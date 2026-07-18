// Package k6 wraps Grafana k6 as a Kapture replay engine.
//
// Data flow: the host streams FeedItems over the ABI; the adapter exposes
// them through the same local feed protocol the standalone feed server
// speaks (GET /next, /batch, /status, POST /ack); k6 VUs pull requests
// from the feed and send them to the target. The host paces the ABI
// stream, so the feed — and therefore k6 — naturally reproduces the
// requested rate profile; the capture is never materialised anywhere.
//
// Results come from k6's --summary-export JSON: aggregate counts and
// latency percentiles, no per-request results (the engine advertises the
// aggregate-results capability).
//
// Engine config (spec.engine.config):
//
//	{
//	  "binary": "k6",           // k6 executable, default "k6" from $PATH
//	  "script": "/path/x.js",   // custom script; default: embedded feeder script
//	  "vus": 50,                // default: RunConfig.concurrency
//	  "batchSize": 10,          // feed batch size per VU fetch
//	  "args": ["--quiet"]       // extra k6 CLI args
//	}
package k6

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/kapture-io/kapture/internal/engines/internal/feedbridge"
	"github.com/kapture-io/kapture/internal/plugin/replay"
	sdk "github.com/kapture-io/kapture/pkg/replayengine"
	replayenginev1 "github.com/kapture-io/kapture/proto/replayengine/v1"
)

//go:embed script.js
var defaultScript string

// execCommand is swappable for tests.
var execCommand = exec.CommandContext

// Config is the engine-specific configuration.
type Config struct {
	Binary    string   `json:"binary,omitempty"`
	Script    string   `json:"script,omitempty"`
	VUs       int      `json:"vus,omitempty"`
	BatchSize int      `json:"batchSize,omitempty"`
	Args      []string `json:"args,omitempty"`
}

// Engine adapts k6 to the replay engine ABI.
type Engine struct {
	cfg      *replayenginev1.RunConfig
	engCfg   Config
	draining atomic.Bool
	cancelFn atomic.Value // context.CancelFunc for the running k6 process
}

// New creates the k6 engine.
func New() *Engine { return &Engine{} }

func (e *Engine) Describe(_ context.Context) (*replayenginev1.DescribeResponse, error) {
	return &replayenginev1.DescribeResponse{
		Name:         "k6",
		Version:      "adapter-v1",
		AbiVersions:  []string{sdk.ABIVersion},
		Protocols:    []string{"HTTP"},
		Capabilities: []string{sdk.CapabilityAggregateResults},
	}, nil
}

func (e *Engine) Configure(_ context.Context, cfg *replayenginev1.RunConfig) error {
	if cfg.GetTarget().GetHost() == "" {
		return fmt.Errorf("target host is required")
	}
	engCfg := Config{}
	if raw := cfg.GetEngineConfigJson(); len(raw) > 0 {
		if err := json.Unmarshal(raw, &engCfg); err != nil {
			return fmt.Errorf("parse k6 engine config: %w", err)
		}
	}
	if engCfg.Binary == "" {
		engCfg.Binary = "k6"
	}
	if engCfg.VUs <= 0 {
		engCfg.VUs = int(cfg.GetConcurrency())
		if engCfg.VUs <= 0 {
			engCfg.VUs = 10
		}
	}
	e.cfg = cfg
	e.engCfg = engCfg
	return nil
}

func (e *Engine) Execute(ctx context.Context, feed <-chan *replayenginev1.FeedItem, events chan<- *replayenginev1.ExecuteResponse) error {
	if e.cfg == nil {
		return fmt.Errorf("engine not configured")
	}

	workDir, err := os.MkdirTemp("", "kapture-k6-*")
	if err != nil {
		return fmt.Errorf("create work dir: %w", err)
	}
	defer os.RemoveAll(workDir)

	// Bridge the ABI feed into the local HTTP feed protocol.
	feedServer, err := replay.NewFeedServer(replay.FeedServerConfig{
		Reader:   feedbridge.NewReader(feed),
		RateMode: replay.RateModeUnlimited, // the host already paced the stream
		Addr:     "127.0.0.1:0",
	})
	if err != nil {
		return fmt.Errorf("create feed bridge: %w", err)
	}
	feedCtx, cancelFeed := context.WithCancel(ctx)
	defer cancelFeed()
	if err := feedServer.Start(feedCtx); err != nil {
		return fmt.Errorf("start feed bridge: %w", err)
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = feedServer.Stop(stopCtx)
	}()
	feedURL := "http://" + hostPort(feedServer.Addr())

	// Materialise the script.
	scriptPath := e.engCfg.Script
	if scriptPath == "" {
		scriptPath = filepath.Join(workDir, "replay.js")
		if err := os.WriteFile(scriptPath, []byte(defaultScript), 0o644); err != nil {
			return fmt.Errorf("write k6 script: %w", err)
		}
	}
	summaryPath := filepath.Join(workDir, "summary.json")

	args := []string{
		"run",
		"--vus", strconv.Itoa(e.engCfg.VUs),
		"--summary-export", summaryPath,
		"--summary-trend-stats", "avg,med,p(95),p(99)",
	}
	args = append(args, e.engCfg.Args...)
	args = append(args, scriptPath)

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	e.cancelFn.Store(context.CancelFunc(cancelRun))

	cmd := execCommand(runCtx, e.engCfg.Binary, args...)
	cmd.Env = append(os.Environ(),
		"FEED_URL="+feedURL,
		"TARGET_URL="+targetURL(e.cfg),
		"VUS="+strconv.Itoa(e.engCfg.VUs),
		"BATCH_SIZE="+strconv.Itoa(orDefault(e.engCfg.BatchSize, 10)),
	)
	cmd.Stdout = os.Stderr // k6 output goes to the engine's stderr log
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start k6: %w", err)
	}

	// Periodic progress from the feed bridge while k6 runs.
	progressDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-progressDone:
				return
			case <-ticker.C:
				select {
				case events <- &replayenginev1.ExecuteResponse{
					Event: &replayenginev1.ExecuteResponse_Progress{
						Progress: &replayenginev1.Progress{Sent: feedServer.Served()},
					},
				}:
				case <-runCtx.Done():
					return
				}
			}
		}
	}()

	runErr := cmd.Wait()
	close(progressDone)

	summary, parseErr := parseSummaryExport(summaryPath, feedServer.Served())
	if parseErr != nil {
		if runErr != nil {
			return fmt.Errorf("k6 run failed: %w (and no summary export: %v)", runErr, parseErr)
		}
		return fmt.Errorf("parse k6 summary: %w", parseErr)
	}
	if runErr != nil {
		summary.Message = fmt.Sprintf("k6 exited with error: %v", runErr)
	}

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
	if cancel, ok := e.cancelFn.Load().(context.CancelFunc); ok && cancel != nil {
		cancel()
	}
	return nil
}

// parseSummaryExport converts k6's --summary-export JSON into a RunSummary.
func parseSummaryExport(path string, fedCount int64) (*replayenginev1.RunSummary, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var export struct {
		Metrics map[string]map[string]float64 `json:"metrics"`
	}
	if err := json.Unmarshal(raw, &export); err != nil {
		return nil, fmt.Errorf("decode summary export: %w", err)
	}

	summary := &replayenginev1.RunSummary{TotalRequests: fedCount}

	if reqs, ok := export.Metrics["http_reqs"]; ok {
		count := int64(reqs["count"])
		summary.SentRequests = count
		summary.AchievedRps = reqs["rate"]
	}
	if failedMetric, ok := export.Metrics["http_req_failed"]; ok {
		// Rate metric: "passes" counts samples where the request failed.
		failed := int64(failedMetric["passes"])
		summary.FailedRequests = failed
		if summary.SentRequests >= failed {
			summary.SentRequests -= failed
		}
	}
	if dur, ok := export.Metrics["http_req_duration"]; ok {
		summary.MeanLatencyMs = int64(dur["avg"])
		summary.P50LatencyMs = int64(dur["med"])
		summary.P95LatencyMs = int64(dur["p(95)"])
		summary.P99LatencyMs = int64(dur["p(99)"])
	}
	return summary, nil
}

func targetURL(cfg *replayenginev1.RunConfig) string {
	scheme := "http"
	port := int32(80)
	if cfg.GetTarget().GetTls() {
		scheme = "https"
		port = 443
	}
	if cfg.GetTarget().GetPort() != 0 {
		port = cfg.GetTarget().GetPort()
	}
	return fmt.Sprintf("%s://%s:%d", scheme, cfg.GetTarget().GetHost(), port)
}

func hostPort(addr net.Addr) string {
	if addr == nil {
		return "127.0.0.1:6565"
	}
	return addr.String()
}

func orDefault(v, def int) int {
	if v > 0 {
		return v
	}
	return def
}
