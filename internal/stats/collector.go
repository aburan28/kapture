package stats

import (
	"net"
	"strings"
	"time"

	"github.com/kapture-io/kapture/internal/storage"
)

// CollectorConfig sizes the capture statistics collector.
type CollectorConfig struct {
	// WindowDuration is the tumbling flow-window length (default 1m).
	WindowDuration time.Duration
	// TopK is how many heavy hitters to track per dimension (default 10).
	TopK int
	// ExpectedFlows sizes the flow membership filter (default 100k).
	ExpectedFlows int
	// QuantileAccuracy is the DDSketch relative error (default 1%).
	QuantileAccuracy float64
}

// Collector aggregates streaming statistics over the capture path:
// exact tumbling-window counters, HyperLogLog cardinalities for client
// IPs and 5-tuples, Count-Min top-K heavy hitters for paths and
// clients, DDSketch quantiles for body sizes and capture-processing
// latency, and a Bloom filter for flow membership.
type Collector struct {
	Windows *WindowedCounters

	uniqueClients *HyperLogLog
	uniqueFlows   *HyperLogLog
	topPaths      *TopK
	topClients    *TopK
	bodySizes     *DDSketch
	latencies     *DDSketch
	flows         *BloomFilter
}

// NewCollector builds a collector with the given configuration.
func NewCollector(cfg CollectorConfig) *Collector {
	return &Collector{
		Windows:       NewWindowedCounters(cfg.WindowDuration),
		uniqueClients: NewHyperLogLog(),
		uniqueFlows:   NewHyperLogLog(),
		topPaths:      NewTopK(cfg.TopK, 0),
		topClients:    NewTopK(cfg.TopK, 0),
		bodySizes:     NewDDSketch(cfg.QuantileAccuracy),
		latencies:     NewDDSketch(cfg.QuantileAccuracy),
		flows:         NewBloomFilter(cfg.ExpectedFlows, 0.01),
	}
}

// Observe records one captured request and the time the capture path
// spent handling it.
func (c *Collector) Observe(req *storage.CapturedRequest, handleLatency time.Duration) {
	if req == nil {
		return
	}

	clientIP := clientIPOf(req)
	flow := flowKeyOf(req, clientIP)

	newFlow := false
	if flow != "" {
		newFlow = c.flows.AddIfNew(flow)
		c.uniqueFlows.Add(flow)
	}
	if clientIP != "" {
		c.uniqueClients.Add(clientIP)
		c.topClients.Add(clientIP)
	}
	if req.Path != "" {
		c.topPaths.Add(req.Path)
	}
	c.bodySizes.Add(float64(len(req.Body)))
	if handleLatency > 0 {
		c.latencies.Add(float64(handleLatency.Microseconds()))
	}
	c.Windows.Observe(req.Method, req.Protocol, uint64(len(req.Body)), newFlow)
}

// SeenFlow reports whether the flow of req was (probably) captured
// before. No false negatives.
func (c *Collector) SeenFlow(req *storage.CapturedRequest) bool {
	flow := flowKeyOf(req, clientIPOf(req))
	if flow == "" {
		return false
	}
	return c.flows.Contains(flow)
}

// QuantileSummary reports a sketch's key quantiles.
type QuantileSummary struct {
	Count uint64  `json:"count"`
	Mean  float64 `json:"mean"`
	P50   float64 `json:"p50"`
	P90   float64 `json:"p90"`
	P99   float64 `json:"p99"`
	Max   float64 `json:"max"`
}

func summarize(d *DDSketch) QuantileSummary {
	return QuantileSummary{
		Count: d.Count(),
		Mean:  d.Mean(),
		P50:   d.Quantile(0.50),
		P90:   d.Quantile(0.90),
		P99:   d.Quantile(0.99),
		Max:   d.Max(),
	}
}

// Snapshot is a JSON-serialisable view of all statistics.
type Snapshot struct {
	GeneratedAt time.Time `json:"generatedAt"`

	CurrentWindow    *WindowCounts   `json:"currentWindow,omitempty"`
	CompletedWindows []*WindowCounts `json:"completedWindows,omitempty"`

	UniqueClientIPs uint64 `json:"uniqueClientIPs"`
	UniqueFlows     uint64 `json:"uniqueFlows"`

	TopPaths   []TopKEntry `json:"topPaths,omitempty"`
	TopClients []TopKEntry `json:"topClients,omitempty"`

	BodySizeBytes    QuantileSummary `json:"bodySizeBytes"`
	HandleLatencyMys QuantileSummary `json:"handleLatencyMicros"`

	FlowFilterFill float64 `json:"flowFilterFill"`
}

// Snapshot captures the current statistics.
func (c *Collector) Snapshot() Snapshot {
	current, completed := c.Windows.Snapshot()
	return Snapshot{
		GeneratedAt:      time.Now().UTC(),
		CurrentWindow:    current,
		CompletedWindows: completed,
		UniqueClientIPs:  c.uniqueClients.Estimate(),
		UniqueFlows:      c.uniqueFlows.Estimate(),
		TopPaths:         c.topPaths.Top(),
		TopClients:       c.topClients.Top(),
		BodySizeBytes:    summarize(c.bodySizes),
		HandleLatencyMys: summarize(c.latencies),
		FlowFilterFill:   c.flows.FillRatio(),
	}
}

// clientIPOf extracts the client IP from the request's remoteAddr
// metadata ("ip:port").
func clientIPOf(req *storage.CapturedRequest) string {
	addr := req.Metadata["remoteAddr"]
	if addr == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return addr
}

// flowKeyOf approximates the 5-tuple from what a mirrored L7 request
// carries: protocol, client address (IP:port), and destination host.
// The destination IP/port of the original connection are not visible
// past the gateway, so the Host header stands in for the server side.
func flowKeyOf(req *storage.CapturedRequest, clientIP string) string {
	if clientIP == "" {
		return ""
	}
	var b strings.Builder
	b.WriteString(req.Protocol)
	b.WriteByte('|')
	b.WriteString(req.Metadata["remoteAddr"])
	b.WriteByte('|')
	b.WriteString(req.Metadata["host"])
	return b.String()
}
