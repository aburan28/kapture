package replayengine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kapture-io/kapture/internal/plugin/replay"
	"github.com/kapture-io/kapture/internal/storage"
	sdk "github.com/kapture-io/kapture/pkg/replayengine"
	replayenginev1 "github.com/kapture-io/kapture/proto/replayengine/v1"
)

// TestMain doubles as the plugin binary: when re-executed with the helper
// env set, it serves a fake engine over the ABI instead of running tests.
func TestMain(m *testing.M) {
	if os.Getenv("KAPTURE_TEST_ENGINE_HELPER") == "1" {
		if err := sdk.Serve(&fakeEngine{}); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// fakeEngine echoes every feed item as a successful result. Its reported
// version and ABI list come from env vars so tests can simulate upgrades
// and incompatible plugins across re-execs.
type fakeEngine struct{}

func (f *fakeEngine) Describe(context.Context) (*replayenginev1.DescribeResponse, error) {
	version := os.Getenv("FAKE_ENGINE_VERSION")
	if version == "" {
		version = "1"
	}
	abi := os.Getenv("FAKE_ENGINE_ABI")
	if abi == "" {
		abi = sdk.ABIVersion
	}
	return &replayenginev1.DescribeResponse{
		Name:         "fake",
		Version:      version,
		AbiVersions:  []string{abi},
		Protocols:    []string{"HTTP"},
		Capabilities: []string{sdk.CapabilityPerRequestResults},
	}, nil
}

func (f *fakeEngine) Configure(_ context.Context, cfg *replayenginev1.RunConfig) error {
	if cfg.GetTarget().GetHost() == "reject-me" {
		return errors.New("rejected by fake engine")
	}
	return nil
}

func (f *fakeEngine) Execute(ctx context.Context, feed <-chan *replayenginev1.FeedItem, events chan<- *replayenginev1.ExecuteResponse) error {
	var count int64
	for item := range feed {
		count++
		events <- &replayenginev1.ExecuteResponse{
			Event: &replayenginev1.ExecuteResponse_Result{Result: &replayenginev1.RequestResult{
				RequestId:  item.RequestId,
				StatusCode: 200,
				LatencyMs:  1,
			}},
		}
	}
	events <- &replayenginev1.ExecuteResponse{
		Event: &replayenginev1.ExecuteResponse_Summary{Summary: &replayenginev1.RunSummary{
			TotalRequests: count,
			SentRequests:  count,
			AchievedRps:   float64(count),
		}},
	}
	return nil
}

func (f *fakeEngine) Drain(context.Context) error { return nil }

// pluginDir sets up a plugin directory whose "fake" engine re-executes
// this test binary in helper mode.
func pluginDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	self, err := os.Executable()
	if err != nil {
		t.Fatalf("finding test binary: %v", err)
	}
	if err := os.Symlink(self, filepath.Join(dir, sdk.PluginBinaryPrefix+"fake")); err != nil {
		t.Fatalf("symlinking plugin: %v", err)
	}
	t.Setenv("KAPTURE_TEST_ENGINE_HELPER", "1")
	return dir
}

func TestSubprocessEngine_LaunchConfigureExecute(t *testing.T) {
	dir := pluginDir(t)
	m := NewManager(dir, nil)
	defer m.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	engine, err := m.Acquire(ctx, "fake")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if engine.Info.Name != "fake" {
		t.Errorf("engine name = %q", engine.Info.Name)
	}

	if err := engine.Configure(ctx, &replayenginev1.RunConfig{
		Target: &replayenginev1.Target{Host: "example.test"},
	}); err != nil {
		t.Fatalf("Configure: %v", err)
	}

	stream, err := engine.Execute(ctx)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	for i := 0; i < 3; i++ {
		err := stream.Send(&replayenginev1.ExecuteRequest{Item: &replayenginev1.FeedItem{
			RequestId: fmt.Sprintf("req-%d", i),
			Method:    "GET",
			Path:      "/x",
		}})
		if err != nil {
			t.Fatalf("Send: %v", err)
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}

	var results, summaries int
	var summary *replayenginev1.RunSummary
	for {
		event, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		switch ev := event.Event.(type) {
		case *replayenginev1.ExecuteResponse_Result:
			results++
		case *replayenginev1.ExecuteResponse_Summary:
			summaries++
			summary = ev.Summary
		}
	}
	if results != 3 || summaries != 1 {
		t.Errorf("got %d results and %d summaries, want 3 and 1", results, summaries)
	}
	if summary.SentRequests != 3 {
		t.Errorf("summary.SentRequests = %d, want 3", summary.SentRequests)
	}
}

func TestSubprocessEngine_ConfigureRejection(t *testing.T) {
	dir := pluginDir(t)
	m := NewManager(dir, nil)
	defer m.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	engine, err := m.Acquire(ctx, "fake")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	err = engine.Configure(ctx, &replayenginev1.RunConfig{
		Target: &replayenginev1.Target{Host: "reject-me"},
	})
	if err == nil || !contains(err.Error(), "rejected by fake engine") {
		t.Errorf("Configure error = %v, want engine rejection", err)
	}
}

func TestLaunch_RejectsIncompatibleABI(t *testing.T) {
	dir := pluginDir(t)
	t.Setenv("FAKE_ENGINE_ABI", "v999")
	m := NewManager(dir, nil)
	defer m.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := m.Acquire(ctx, "fake"); err == nil || !contains(err.Error(), "no common version") {
		t.Errorf("Acquire = %v, want ABI negotiation failure", err)
	}
}

func TestManager_HotReload(t *testing.T) {
	dir := pluginDir(t)
	m := NewManager(dir, nil)
	m.DrainGrace = 100 * time.Millisecond
	defer m.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	t.Setenv("FAKE_ENGINE_VERSION", "1")
	engine, err := m.Acquire(ctx, "fake")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if engine.Info.Version != "1" {
		t.Fatalf("initial version = %q, want 1", engine.Info.Version)
	}

	// Simulate a plugin binary update: the new process reports version 2.
	t.Setenv("FAKE_ENGINE_VERSION", "2")
	if err := m.Reload(ctx, "fake"); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	reloaded, err := m.Acquire(ctx, "fake")
	if err != nil {
		t.Fatalf("Acquire after reload: %v", err)
	}
	if reloaded.Info.Version != "2" {
		t.Errorf("post-reload version = %q, want 2", reloaded.Info.Version)
	}
	if reloaded == engine {
		t.Error("Acquire returned the old engine instance after reload")
	}

	// The old engine still answers until its drain grace expires — runs in
	// flight on it are not cut off by the swap.
	if err := engine.Configure(ctx, &replayenginev1.RunConfig{
		Target: &replayenginev1.Target{Host: "example.test"},
	}); err != nil {
		t.Errorf("old engine unusable immediately after reload: %v", err)
	}
}

func TestManager_ReloadWithoutRunningEngineIsNoop(t *testing.T) {
	dir := pluginDir(t)
	m := NewManager(dir, nil)
	defer m.Close()

	if err := m.Reload(context.Background(), "fake"); err != nil {
		t.Errorf("Reload of non-running engine: %v", err)
	}
}

func TestServe_RefusesWithoutHandshakeEnv(t *testing.T) {
	// Direct execution without the host handshake env must fail fast.
	t.Setenv(sdk.MagicCookieEnv, "")
	err := sdk.Serve(&fakeEngine{})
	if err == nil || !contains(err.Error(), "not meant to be executed directly") {
		t.Errorf("Serve without handshake env = %v, want refusal", err)
	}
}

// --- Feeder through a real subprocess engine ---

// sliceReader is an in-memory replay.Reader.
type sliceReader struct {
	reqs []*storage.CapturedRequest
	idx  int
}

func (r *sliceReader) Open(context.Context, replay.ReadOptions) error { return nil }
func (r *sliceReader) Next(context.Context) (*storage.CapturedRequest, error) {
	if r.idx >= len(r.reqs) {
		return nil, replay.ErrReaderDone
	}
	req := r.reqs[r.idx]
	r.idx++
	return req, nil
}
func (r *sliceReader) Close() error { return nil }

func TestFeeder_StreamsShardSliceThroughEngine(t *testing.T) {
	dir := pluginDir(t)
	m := NewManager(dir, nil)
	defer m.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	engine, err := m.Acquire(ctx, "fake")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if err := engine.Configure(ctx, &replayenginev1.RunConfig{
		Target: &replayenginev1.Target{Host: "example.test"},
	}); err != nil {
		t.Fatalf("Configure: %v", err)
	}

	const total = 100
	reqs := make([]*storage.CapturedRequest, total)
	owned := 0
	for i := range reqs {
		id := fmt.Sprintf("req-%04d", i)
		reqs[i] = &storage.CapturedRequest{ID: id, Method: "GET", Path: "/x", Protocol: "HTTP"}
		if replay.ShardOwns(id, 0, 2) {
			owned++
		}
	}

	feeder, err := NewFeeder(FeederConfig{
		Reader:     &sliceReader{reqs: reqs},
		ShardIndex: 0,
		ShardCount: 2,
		RateMode:   replay.RateModeUnlimited,
	})
	if err != nil {
		t.Fatalf("NewFeeder: %v", err)
	}

	report, err := feeder.Run(ctx, engine)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.SentRequests != int64(owned) {
		t.Errorf("engine received %d requests, want shard-owned %d of %d",
			report.SentRequests, owned, total)
	}
	if owned == 0 || owned == total {
		t.Fatalf("degenerate shard split (%d of %d); test data broken", owned, total)
	}
}

func TestNewFeeder_ValidatesShard(t *testing.T) {
	_, err := NewFeeder(FeederConfig{
		Reader:     &sliceReader{},
		ShardIndex: 5,
		ShardCount: 2,
	})
	if err == nil {
		t.Error("NewFeeder accepted out-of-range shard index")
	}
}

func contains(haystack, needle string) bool {
	return strings.Contains(haystack, needle)
}
