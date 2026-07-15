package k6

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	replayenginev1 "github.com/kapture-io/kapture/proto/replayengine/v1"
)

const cannedSummary = `{
  "metrics": {
    "http_reqs": {"count": 120, "rate": 42.5},
    "http_req_failed": {"value": 0.05, "passes": 6, "fails": 114},
    "http_req_duration": {"avg": 12.4, "med": 10.0, "p(95)": 30.2, "p(99)": 55.9}
  }
}`

// stubK6 writes a shell script that mimics `k6 run`: it extracts the
// --summary-export path from its args, dumps a canned summary there, and
// records its argv and env for assertions.
func stubK6(t *testing.T) (binary, argsFile, envFile string) {
	t.Helper()
	dir := t.TempDir()
	binary = filepath.Join(dir, "k6-stub")
	argsFile = filepath.Join(dir, "args.txt")
	envFile = filepath.Join(dir, "env.txt")

	script := `#!/bin/sh
printf '%s\n' "$@" > "` + argsFile + `"
env > "` + envFile + `"
out=""
prev=""
for a in "$@"; do
  if [ "$prev" = "--summary-export" ]; then out="$a"; fi
  prev="$a"
done
cat > "$out" <<'SUMMARY'
` + cannedSummary + `
SUMMARY
exit 0
`
	if err := os.WriteFile(binary, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return binary, argsFile, envFile
}

func TestK6Engine_RunsAndParsesSummary(t *testing.T) {
	binary, argsFile, envFile := stubK6(t)

	engine := New()
	err := engine.Configure(context.Background(), &replayenginev1.RunConfig{
		Target:           &replayenginev1.Target{Host: "staging.internal", Port: 8080},
		Concurrency:      7,
		EngineConfigJson: []byte(`{"binary": "` + binary + `"}`),
	})
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}

	feed := make(chan *replayenginev1.FeedItem)
	close(feed) // the stub does not consume the feed
	events := make(chan *replayenginev1.ExecuteResponse, 16)
	if err := engine.Execute(context.Background(), feed, events); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	close(events)

	var summary *replayenginev1.RunSummary
	for ev := range events {
		if s, ok := ev.Event.(*replayenginev1.ExecuteResponse_Summary); ok {
			summary = s.Summary
		}
	}
	if summary == nil {
		t.Fatal("no summary emitted")
	}
	// 120 http_reqs, 6 failed → 114 sent.
	if summary.SentRequests != 114 || summary.FailedRequests != 6 {
		t.Errorf("counts wrong: %+v", summary)
	}
	if summary.AchievedRps != 42.5 {
		t.Errorf("rps = %v, want 42.5", summary.AchievedRps)
	}
	if summary.P95LatencyMs != 30 || summary.P99LatencyMs != 55 {
		t.Errorf("latency percentiles wrong: %+v", summary)
	}

	// The stub must have been invoked with the VU count and a script.
	args, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatalf("stub never ran: %v", err)
	}
	argStr := string(args)
	for _, want := range []string{"run", "--vus\n7", "--summary-export", "replay.js"} {
		if !containsStr(argStr, want) {
			t.Errorf("k6 args missing %q:\n%s", want, argStr)
		}
	}

	env, err := os.ReadFile(envFile)
	if err != nil {
		t.Fatal(err)
	}
	envStr := string(env)
	for _, want := range []string{"TARGET_URL=http://staging.internal:8080", "FEED_URL=http://127.0.0.1:", "VUS=7"} {
		if !containsStr(envStr, want) {
			t.Errorf("k6 env missing %q", want)
		}
	}
}

func TestK6Engine_CustomVUsAndScript(t *testing.T) {
	binary, argsFile, _ := stubK6(t)
	scriptPath := filepath.Join(t.TempDir(), "custom.js")
	if err := os.WriteFile(scriptPath, []byte("// custom"), 0o644); err != nil {
		t.Fatal(err)
	}

	engine := New()
	err := engine.Configure(context.Background(), &replayenginev1.RunConfig{
		Target:           &replayenginev1.Target{Host: "t", Port: 80},
		Concurrency:      3,
		EngineConfigJson: []byte(`{"binary": "` + binary + `", "vus": 42, "script": "` + scriptPath + `", "args": ["--quiet"]}`),
	})
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}

	feed := make(chan *replayenginev1.FeedItem)
	close(feed)
	events := make(chan *replayenginev1.ExecuteResponse, 16)
	if err := engine.Execute(context.Background(), feed, events); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	args, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatal(err)
	}
	argStr := string(args)
	if !containsStr(argStr, "--vus\n42") {
		t.Errorf("config vus not honored:\n%s", argStr)
	}
	if !containsStr(argStr, "--quiet") {
		t.Errorf("extra args not passed:\n%s", argStr)
	}
	if !containsStr(argStr, scriptPath) {
		t.Errorf("custom script not used:\n%s", argStr)
	}
}

func TestK6Engine_RejectsBadConfig(t *testing.T) {
	engine := New()
	err := engine.Configure(context.Background(), &replayenginev1.RunConfig{
		Target:           &replayenginev1.Target{Host: "t"},
		EngineConfigJson: []byte(`{not json`),
	})
	if err == nil {
		t.Error("Configure accepted malformed engine config")
	}

	if err := engine.Configure(context.Background(), &replayenginev1.RunConfig{}); err == nil {
		t.Error("Configure accepted empty target")
	}
}

func TestParseSummaryExport_MissingFile(t *testing.T) {
	if _, err := parseSummaryExport(filepath.Join(t.TempDir(), "nope.json"), 0); err == nil {
		t.Error("expected error for missing summary file")
	}
}

func containsStr(haystack, needle string) bool {
	return strings.Contains(haystack, needle)
}
