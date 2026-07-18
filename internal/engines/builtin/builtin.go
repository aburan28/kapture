// Package builtin is the reference implementation of the Kapture replay
// engine ABI: a native Go HTTP sender behind the same contract external
// engines implement. It doubles as the conformance example for engine
// authors — if a behaviour is ambiguous in the ABI doc, this package is
// the tie-breaker.
package builtin

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kapture-io/kapture/internal/plugin/replay"
	"github.com/kapture-io/kapture/internal/storage"
	sdk "github.com/kapture-io/kapture/pkg/replayengine"
	replayenginev1 "github.com/kapture-io/kapture/proto/replayengine/v1"
)

// maxLatencySamples caps the latency reservoir so summaries stay bounded
// for arbitrarily long runs.
const maxLatencySamples = 100_000

// Engine is the builtin HTTP replay engine.
type Engine struct {
	mu       sync.Mutex
	cfg      *replayenginev1.RunConfig
	sender   replay.Sender
	draining atomic.Bool

	// newSender is swappable for tests.
	newSender func(cfg *replayenginev1.RunConfig) (replay.Sender, error)
}

// New creates the builtin engine.
func New() *Engine {
	return &Engine{newSender: defaultSender}
}

func defaultSender(cfg *replayenginev1.RunConfig) (replay.Sender, error) {
	scheme := "http"
	port := int32(80)
	if cfg.GetTarget().GetTls() {
		scheme = "https"
		port = 443
	}
	if cfg.GetTarget().GetPort() != 0 {
		port = cfg.GetTarget().GetPort()
	}
	return replay.NewHTTPSender(replay.HTTPSenderConfig{
		TargetBaseURL: fmt.Sprintf("%s://%s:%d", scheme, cfg.GetTarget().GetHost(), port),
	})
}

func (e *Engine) Describe(_ context.Context) (*replayenginev1.DescribeResponse, error) {
	return &replayenginev1.DescribeResponse{
		Name:         "builtin",
		Version:      "v1",
		AbiVersions:  []string{sdk.ABIVersion},
		Protocols:    []string{"HTTP"},
		Capabilities: []string{sdk.CapabilityPerRequestResults},
	}, nil
}

func (e *Engine) Configure(_ context.Context, cfg *replayenginev1.RunConfig) error {
	if cfg.GetTarget().GetHost() == "" {
		return fmt.Errorf("target host is required")
	}
	sender, err := e.newSender(cfg)
	if err != nil {
		return fmt.Errorf("create sender: %w", err)
	}
	e.mu.Lock()
	e.cfg = cfg
	e.sender = sender
	e.mu.Unlock()
	return nil
}

func (e *Engine) Execute(ctx context.Context, feed <-chan *replayenginev1.FeedItem, events chan<- *replayenginev1.ExecuteResponse) error {
	e.mu.Lock()
	sender := e.sender
	cfg := e.cfg
	e.mu.Unlock()
	if sender == nil {
		return fmt.Errorf("engine not configured")
	}

	concurrency := int(cfg.GetConcurrency())
	if concurrency <= 0 {
		concurrency = 1
	}

	var (
		total, sent, failed atomic.Int64
		latMu               sync.Mutex
		latencies           []time.Duration
		statusMu            sync.Mutex
		statusCodes         = map[string]int64{}
	)
	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range feed {
				if e.draining.Load() {
					continue // drop remaining feed while draining
				}
				total.Add(1)
				result, err := sender.Send(ctx, capturedFromFeedItem(item))

				event := &replayenginev1.RequestResult{RequestId: item.RequestId}
				if err != nil {
					failed.Add(1)
					event.Error = err.Error()
				} else {
					sent.Add(1)
					event.StatusCode = int32(result.StatusCode)
					event.LatencyMs = result.Duration.Milliseconds()

					latMu.Lock()
					if len(latencies) < maxLatencySamples {
						latencies = append(latencies, result.Duration)
					}
					latMu.Unlock()

					statusMu.Lock()
					statusCodes[fmt.Sprintf("%d", result.StatusCode)]++
					statusMu.Unlock()
				}

				select {
				case events <- &replayenginev1.ExecuteResponse{
					Event: &replayenginev1.ExecuteResponse_Result{Result: event},
				}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	wg.Wait()

	elapsed := time.Since(start)
	summary := &replayenginev1.RunSummary{
		TotalRequests:  total.Load(),
		SentRequests:   sent.Load(),
		FailedRequests: failed.Load(),
		StatusCodes:    statusCodes,
	}
	if elapsed > 0 {
		summary.AchievedRps = float64(sent.Load()) / elapsed.Seconds()
	}
	fillLatencyStats(summary, latencies)

	select {
	case events <- &replayenginev1.ExecuteResponse{
		Event: &replayenginev1.ExecuteResponse_Summary{Summary: summary},
	}:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (e *Engine) Drain(_ context.Context) error {
	e.draining.Store(true)
	return nil
}

func fillLatencyStats(summary *replayenginev1.RunSummary, latencies []time.Duration) {
	if len(latencies) == 0 {
		return
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	var totalLat time.Duration
	for _, l := range latencies {
		totalLat += l
	}
	pct := func(p float64) int64 {
		idx := int(float64(len(latencies)-1) * p)
		return latencies[idx].Milliseconds()
	}
	summary.MeanLatencyMs = (totalLat / time.Duration(len(latencies))).Milliseconds()
	summary.P50LatencyMs = pct(0.50)
	summary.P95LatencyMs = pct(0.95)
	summary.P99LatencyMs = pct(0.99)
}

// capturedFromFeedItem converts a wire FeedItem back into the sender's
// request type.
func capturedFromFeedItem(item *replayenginev1.FeedItem) *storage.CapturedRequest {
	req := &storage.CapturedRequest{
		ID:       item.RequestId,
		Method:   item.Method,
		Path:     item.Path,
		Body:     item.Body,
		Protocol: item.Protocol,
		Metadata: item.Metadata,
	}
	if item.CapturedAt != nil {
		req.Timestamp = item.CapturedAt.AsTime()
	}
	if len(item.Headers) > 0 {
		req.Headers = make(map[string][]string, len(item.Headers))
		for k, hv := range item.Headers {
			req.Headers[k] = hv.GetValues()
		}
	}
	return req
}
