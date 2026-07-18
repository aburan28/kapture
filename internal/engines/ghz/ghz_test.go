package ghz

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	replayenginev1 "github.com/kapture-io/kapture/proto/replayengine/v1"
)

const cannedReport = `{
  "count": 50,
  "average": 8000000,
  "rps": 100.5,
  "latencyDistribution": [
    {"percentage": 50, "latency": 6000000},
    {"percentage": 95, "latency": 20000000},
    {"percentage": 99, "latency": 41000000}
  ],
  "errorDistribution": {"rpc error: unavailable": 2}
}`

// stubGhz writes a shell script that mimics ghz: prints a canned JSON
// report to stdout and appends its argv to a log so tests can assert one
// invocation per method.
func stubGhz(t *testing.T) (binary, argsLog string) {
	t.Helper()
	dir := t.TempDir()
	binary = filepath.Join(dir, "ghz-stub")
	argsLog = filepath.Join(dir, "args.log")

	script := `#!/bin/sh
printf '%s ' "$@" >> "` + argsLog + `"
printf '\n' >> "` + argsLog + `"
cat <<'REPORT'
` + cannedReport + `
REPORT
exit 0
`
	if err := os.WriteFile(binary, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return binary, argsLog
}

func feedOf(items ...*replayenginev1.FeedItem) <-chan *replayenginev1.FeedItem {
	ch := make(chan *replayenginev1.FeedItem, len(items))
	for _, item := range items {
		ch <- item
	}
	close(ch)
	return ch
}

func TestGhzEngine_RunsPerMethodAndAggregates(t *testing.T) {
	binary, argsLog := stubGhz(t)

	engine := New()
	err := engine.Configure(context.Background(), &replayenginev1.RunConfig{
		Target:           &replayenginev1.Target{Host: "grpc.internal", Port: 9090},
		Concurrency:      5,
		Rate:             &replayenginev1.RateHint{Mode: "Unlimited"},
		EngineConfigJson: []byte(`{"binary": "` + binary + `"}`),
	})
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}

	feed := feedOf(
		&replayenginev1.FeedItem{RequestId: "1", Protocol: "GRPC", Path: "/orders.v1.OrderService/Create", Body: []byte{1, 2}},
		&replayenginev1.FeedItem{RequestId: "2", Protocol: "GRPC", Path: "/orders.v1.OrderService/Create", Body: []byte{3, 4}},
		&replayenginev1.FeedItem{RequestId: "3", Protocol: "GRPC", Path: "/orders.v1.OrderService/Get", Body: []byte{5}},
	)
	events := make(chan *replayenginev1.ExecuteResponse, 16)
	if err := engine.Execute(context.Background(), feed, events); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	close(events)

	var summary *replayenginev1.RunSummary
	var progress int
	for ev := range events {
		switch e := ev.Event.(type) {
		case *replayenginev1.ExecuteResponse_Summary:
			summary = e.Summary
		case *replayenginev1.ExecuteResponse_Progress:
			progress++
		}
	}
	if summary == nil {
		t.Fatal("no summary emitted")
	}
	if progress != 2 {
		t.Errorf("got %d progress events, want one per method (2)", progress)
	}

	// Two methods → two canned reports of 50 calls, 2 errors each.
	if summary.TotalRequests != 100 || summary.FailedRequests != 4 || summary.SentRequests != 96 {
		t.Errorf("aggregated counts wrong: %+v", summary)
	}
	if summary.P99LatencyMs != 41 || summary.P95LatencyMs != 20 || summary.P50LatencyMs != 6 {
		t.Errorf("latency percentiles wrong: %+v", summary)
	}
	if summary.MeanLatencyMs != 8 {
		t.Errorf("mean latency = %d, want 8", summary.MeanLatencyMs)
	}

	// Assert per-method invocations: calls, counts, target, insecure.
	raw, err := os.ReadFile(argsLog)
	if err != nil {
		t.Fatalf("stub never ran: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 2 {
		t.Fatalf("ghz invoked %d times, want 2:\n%s", len(lines), raw)
	}
	// Sorted method order: Create before Get.
	if !strings.Contains(lines[0], "--call orders.v1.OrderService/Create") ||
		!strings.Contains(lines[0], "--total 2") {
		t.Errorf("first invocation wrong: %s", lines[0])
	}
	if !strings.Contains(lines[1], "--call orders.v1.OrderService/Get") ||
		!strings.Contains(lines[1], "--total 1") {
		t.Errorf("second invocation wrong: %s", lines[1])
	}
	for _, line := range lines {
		if !strings.Contains(line, "grpc.internal:9090") {
			t.Errorf("target missing: %s", line)
		}
		if !strings.Contains(line, "--insecure") {
			t.Errorf("plaintext target should pass --insecure: %s", line)
		}
		if !strings.Contains(line, "--concurrency 5") {
			t.Errorf("concurrency missing: %s", line)
		}
	}
}

func TestGhzEngine_ConstantRateSplitsRPS(t *testing.T) {
	binary, argsLog := stubGhz(t)

	engine := New()
	err := engine.Configure(context.Background(), &replayenginev1.RunConfig{
		Target:           &replayenginev1.Target{Host: "grpc.internal", Port: 9090},
		Rate:             &replayenginev1.RateHint{Mode: "Constant", RequestsPerSecond: 100},
		EngineConfigJson: []byte(`{"binary": "` + binary + `"}`),
	})
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}

	feed := feedOf(
		&replayenginev1.FeedItem{RequestId: "1", Path: "/svc/A", Body: []byte{1}},
		&replayenginev1.FeedItem{RequestId: "2", Path: "/svc/B", Body: []byte{2}},
	)
	events := make(chan *replayenginev1.ExecuteResponse, 16)
	if err := engine.Execute(context.Background(), feed, events); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	raw, _ := os.ReadFile(argsLog)
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if !strings.Contains(line, "--rps 50") {
			t.Errorf("100 rps over 2 methods should pass --rps 50: %s", line)
		}
	}
}

func TestGhzEngine_RejectsOriginalTiming(t *testing.T) {
	engine := New()
	err := engine.Configure(context.Background(), &replayenginev1.RunConfig{
		Target: &replayenginev1.Target{Host: "grpc.internal"},
		Rate:   &replayenginev1.RateHint{Mode: "OriginalTiming"},
	})
	if err == nil || !strings.Contains(err.Error(), "cannot reproduce recorded timing") {
		t.Errorf("Configure = %v, want OriginalTiming rejection", err)
	}
}

func TestGhzEngine_EmptyFeedEmitsEmptySummary(t *testing.T) {
	engine := New()
	if err := engine.Configure(context.Background(), &replayenginev1.RunConfig{
		Target: &replayenginev1.Target{Host: "grpc.internal"},
	}); err != nil {
		t.Fatalf("Configure: %v", err)
	}

	events := make(chan *replayenginev1.ExecuteResponse, 4)
	if err := engine.Execute(context.Background(), feedOf(), events); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	close(events)

	var summary *replayenginev1.RunSummary
	for ev := range events {
		if s, ok := ev.Event.(*replayenginev1.ExecuteResponse_Summary); ok {
			summary = s.Summary
		}
	}
	if summary == nil || summary.TotalRequests != 0 {
		t.Errorf("empty feed summary wrong: %+v", summary)
	}
}
