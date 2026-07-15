// Package ghz wraps the ghz gRPC load tool as a Kapture replay engine.
//
// Replay semantics — read this before choosing the engine: ghz drives a
// gRPC method at a target rate from a fixed payload; it cannot replay an
// arbitrary sequence of distinct recorded payloads. This adapter therefore
// replays the *shape* of the captured gRPC traffic, not payload-for-payload:
// it counts captured calls per full method, takes the first captured body
// per method as the representative payload (ghz -B binary data), and runs
// ghz once per method with the recorded call count. Use the builtin engine
// for byte-exact replay; use ghz when you want its connection/stream load
// model against gRPC targets.
//
// Rate: the engine is self-paced. Constant mode maps to ghz --rps (split
// evenly across methods); Unlimited omits it. OriginalTiming is rejected
// at Configure — ghz cannot reproduce recorded inter-request gaps.
//
// Method schemas come from gRPC server reflection by default; set
// "protoset" in the engine config for targets without reflection.
//
// Engine config (spec.engine.config):
//
//	{
//	  "binary": "ghz",            // ghz executable, default "ghz" from $PATH
//	  "protoset": "/x.protoset",  // optional: schema without reflection
//	  "insecure": true,           // plaintext gRPC (default: from target.tls)
//	  "args": ["--timeout=5s"]    // extra ghz CLI args
//	}
package ghz

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	sdk "github.com/kapture-io/kapture/pkg/replayengine"
	replayenginev1 "github.com/kapture-io/kapture/proto/replayengine/v1"
)

// execCommand is swappable for tests.
var execCommand = exec.CommandContext

// Config is the engine-specific configuration.
type Config struct {
	Binary   string   `json:"binary,omitempty"`
	Protoset string   `json:"protoset,omitempty"`
	Insecure *bool    `json:"insecure,omitempty"`
	Args     []string `json:"args,omitempty"`
}

// Engine adapts ghz to the replay engine ABI.
type Engine struct {
	cfg      *replayenginev1.RunConfig
	engCfg   Config
	cancelFn atomic.Value // context.CancelFunc for the in-flight ghz run
}

// New creates the ghz engine.
func New() *Engine { return &Engine{} }

func (e *Engine) Describe(_ context.Context) (*replayenginev1.DescribeResponse, error) {
	return &replayenginev1.DescribeResponse{
		Name:         "ghz",
		Version:      "adapter-v1",
		AbiVersions:  []string{sdk.ABIVersion},
		Protocols:    []string{"GRPC"},
		Capabilities: []string{sdk.CapabilityAggregateResults, sdk.CapabilitySelfPaced},
	}, nil
}

func (e *Engine) Configure(_ context.Context, cfg *replayenginev1.RunConfig) error {
	if cfg.GetTarget().GetHost() == "" {
		return fmt.Errorf("target host is required")
	}
	if cfg.GetRate().GetMode() == "OriginalTiming" {
		return fmt.Errorf("the ghz engine cannot reproduce recorded timing; use rate mode Constant or Unlimited, or the builtin engine")
	}
	engCfg := Config{}
	if raw := cfg.GetEngineConfigJson(); len(raw) > 0 {
		if err := json.Unmarshal(raw, &engCfg); err != nil {
			return fmt.Errorf("parse ghz engine config: %w", err)
		}
	}
	if engCfg.Binary == "" {
		engCfg.Binary = "ghz"
	}
	e.cfg = cfg
	e.engCfg = engCfg
	return nil
}

// methodSample accumulates the per-method call shape from the feed.
type methodSample struct {
	method string
	count  int64
	body   []byte // first captured payload for this method
}

func (e *Engine) Execute(ctx context.Context, feed <-chan *replayenginev1.FeedItem, events chan<- *replayenginev1.ExecuteResponse) error {
	if e.cfg == nil {
		return fmt.Errorf("engine not configured")
	}

	// Phase 1: stream the feed into per-method counts + representative
	// payloads. Memory is bounded by the number of distinct methods, not
	// the capture size.
	samples := map[string]*methodSample{}
	for item := range feed {
		method := strings.TrimPrefix(item.Path, "/")
		s := samples[method]
		if s == nil {
			s = &methodSample{method: method, body: item.Body}
			samples[method] = s
		}
		s.count++
	}
	if len(samples) == 0 {
		return e.emitSummary(ctx, events, &replayenginev1.RunSummary{})
	}

	// Deterministic method order.
	methods := make([]string, 0, len(samples))
	for m := range samples {
		methods = append(methods, m)
	}
	sort.Strings(methods)

	workDir, err := os.MkdirTemp("", "kapture-ghz-*")
	if err != nil {
		return fmt.Errorf("create work dir: %w", err)
	}
	defer os.RemoveAll(workDir)

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	e.cancelFn.Store(context.CancelFunc(cancelRun))

	rpsPerMethod := int32(0)
	if e.cfg.GetRate().GetMode() == "Constant" && e.cfg.GetRate().GetRequestsPerSecond() > 0 {
		rpsPerMethod = e.cfg.GetRate().GetRequestsPerSecond() / int32(len(samples))
		if rpsPerMethod < 1 {
			rpsPerMethod = 1
		}
	}

	// Phase 2: one ghz run per method.
	total := &replayenginev1.RunSummary{}
	var weightedLatency int64
	for i, method := range methods {
		s := samples[method]
		report, err := e.runMethod(runCtx, workDir, i, s, rpsPerMethod)
		if err != nil {
			return fmt.Errorf("ghz run for %s: %w", method, err)
		}

		total.TotalRequests += report.Count
		total.SentRequests += report.Count - report.errorCount()
		total.FailedRequests += report.errorCount()
		total.AchievedRps += report.RPS
		weightedLatency += nsToMs(report.Average) * report.Count
		total.P50LatencyMs = maxInt64(total.P50LatencyMs, report.percentileMs(50))
		total.P95LatencyMs = maxInt64(total.P95LatencyMs, report.percentileMs(95))
		total.P99LatencyMs = maxInt64(total.P99LatencyMs, report.percentileMs(99))

		select {
		case events <- &replayenginev1.ExecuteResponse{
			Event: &replayenginev1.ExecuteResponse_Progress{
				Progress: &replayenginev1.Progress{
					Sent:   total.SentRequests,
					Failed: total.FailedRequests,
				},
			},
		}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if total.TotalRequests > 0 {
		total.MeanLatencyMs = weightedLatency / total.TotalRequests
	}
	total.Message = fmt.Sprintf("replayed method mix across %d gRPC methods (representative payloads)", len(methods))

	return e.emitSummary(ctx, events, total)
}

func (e *Engine) Drain(_ context.Context) error {
	if cancel, ok := e.cancelFn.Load().(context.CancelFunc); ok && cancel != nil {
		cancel()
	}
	return nil
}

// runMethod executes one ghz invocation and parses its JSON report.
func (e *Engine) runMethod(ctx context.Context, workDir string, idx int, s *methodSample, rps int32) (*ghzReport, error) {
	dataPath := filepath.Join(workDir, fmt.Sprintf("body-%d.bin", idx))
	if err := os.WriteFile(dataPath, s.body, 0o600); err != nil {
		return nil, fmt.Errorf("write payload: %w", err)
	}

	target := fmt.Sprintf("%s:%d", e.cfg.GetTarget().GetHost(), targetPort(e.cfg))
	args := []string{
		"--call", s.method,
		"--total", strconv.FormatInt(s.count, 10),
		"--concurrency", strconv.Itoa(concurrency(e.cfg)),
		"--binary-file", dataPath,
		"--format", "json",
	}
	if rps > 0 {
		args = append(args, "--rps", strconv.Itoa(int(rps)))
	}
	if e.engCfg.Protoset != "" {
		args = append(args, "--protoset", e.engCfg.Protoset)
	}
	if insecureTarget(e.cfg, e.engCfg) {
		args = append(args, "--insecure")
	}
	args = append(args, e.engCfg.Args...)
	args = append(args, target)

	cmd := execCommand(ctx, e.engCfg.Binary, args...)
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("ghz: %w", err)
	}

	var report ghzReport
	if err := json.Unmarshal(out, &report); err != nil {
		return nil, fmt.Errorf("parse ghz report: %w", err)
	}
	return &report, nil
}

func (e *Engine) emitSummary(ctx context.Context, events chan<- *replayenginev1.ExecuteResponse, summary *replayenginev1.RunSummary) error {
	select {
	case events <- &replayenginev1.ExecuteResponse{
		Event: &replayenginev1.ExecuteResponse_Summary{Summary: summary},
	}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ghzReport is the subset of ghz's JSON report the adapter consumes.
type ghzReport struct {
	Count               int64   `json:"count"`
	Average             float64 `json:"average"` // nanoseconds
	RPS                 float64 `json:"rps"`
	LatencyDistribution []struct {
		Percentage int     `json:"percentage"`
		Latency    float64 `json:"latency"` // nanoseconds
	} `json:"latencyDistribution"`
	ErrorDistribution map[string]int64 `json:"errorDistribution"`
}

func (r *ghzReport) errorCount() int64 {
	var n int64
	for _, c := range r.ErrorDistribution {
		n += c
	}
	return n
}

func (r *ghzReport) percentileMs(p int) int64 {
	for _, d := range r.LatencyDistribution {
		if d.Percentage == p {
			return nsToMs(d.Latency)
		}
	}
	return 0
}

func nsToMs(ns float64) int64 { return int64(ns / 1e6) }

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func targetPort(cfg *replayenginev1.RunConfig) int32 {
	if cfg.GetTarget().GetPort() != 0 {
		return cfg.GetTarget().GetPort()
	}
	return 443
}

func concurrency(cfg *replayenginev1.RunConfig) int {
	if c := int(cfg.GetConcurrency()); c > 0 {
		return c
	}
	return 10
}

// insecureTarget: explicit config wins; otherwise plaintext when the
// target is not TLS.
func insecureTarget(cfg *replayenginev1.RunConfig, engCfg Config) bool {
	if engCfg.Insecure != nil {
		return *engCfg.Insecure
	}
	return !cfg.GetTarget().GetTls()
}
