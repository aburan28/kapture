package replay

import (
	"context"
	"fmt"
	"iter"
	"testing"
	"time"

	"github.com/kapture-io/kapture/internal/storage"
)

// mockPlugin is a test double for ReplayPlugin.
type mockPlugin struct {
	name      string
	initCfg   PluginConfig
	requests  []*ReplayRequest
	results   []*ReplayResult
	initErr   error
	processErr error
	closed    bool
}

func newMockPlugin(name string) *mockPlugin {
	return &mockPlugin{name: name}
}

func (m *mockPlugin) Name() string { return m.name }

func (m *mockPlugin) Init(_ context.Context, cfg PluginConfig) error {
	m.initCfg = cfg
	return m.initErr
}

func (m *mockPlugin) ProcessRequest(_ context.Context, req *ReplayRequest) (*ReplayResult, error) {
	m.requests = append(m.requests, req)
	if m.processErr != nil {
		return nil, m.processErr
	}
	result := &ReplayResult{
		RequestID:  req.Original.ID,
		Status:     ReplayStatusSuccess,
		StatusCode: 200,
		Duration:   10 * time.Millisecond,
	}
	if len(m.results) > 0 {
		idx := len(m.requests) - 1
		if idx < len(m.results) {
			result = m.results[idx]
		}
	}
	return result, nil
}

func (m *mockPlugin) Close() error {
	m.closed = true
	return nil
}

// mockReaderFactory produces readers that yield a fixed set of requests.
type mockReaderFactory struct {
	requests []*storage.CapturedRequest
	err      error
}

func (f *mockReaderFactory) NewReader(_ context.Context, captureID string) (storage.Reader, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &mockReader{requests: f.requests}, nil
}

type mockReader struct {
	requests []*storage.CapturedRequest
}

func (r *mockReader) Read(_ context.Context) iter.Seq2[*storage.CapturedRequest, error] {
	return func(yield func(*storage.CapturedRequest, error) bool) {
		for _, req := range r.requests {
			if !yield(req, nil) {
				return
			}
		}
	}
}

func (r *mockReader) Close() error { return nil }

func TestNewEngine(t *testing.T) {
	t.Run("nil reader factory", func(t *testing.T) {
		_, err := NewEngine(EngineConfig{Plugins: []ReplayPlugin{newMockPlugin("test")}})
		if err == nil {
			t.Fatal("expected error for nil reader factory")
		}
	})

	t.Run("no plugins", func(t *testing.T) {
		_, err := NewEngine(EngineConfig{ReaderFactory: &mockReaderFactory{}})
		if err == nil {
			t.Fatal("expected error for empty plugins")
		}
	})

	t.Run("valid config", func(t *testing.T) {
		e, err := NewEngine(EngineConfig{
			ReaderFactory: &mockReaderFactory{},
			Plugins:       []ReplayPlugin{newMockPlugin("test")},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if e == nil {
			t.Fatal("expected non-nil engine")
		}
	})
}

func TestEngineInitPlugins(t *testing.T) {
	plugin := newMockPlugin("test-plugin")
	e, _ := NewEngine(EngineConfig{
		ReaderFactory: &mockReaderFactory{},
		Plugins:       []ReplayPlugin{plugin},
	})

	settings := map[string]map[string]string{
		"test-plugin": {"key": "value"},
	}

	err := e.InitPlugins(context.Background(), "http://target:8080", settings)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plugin.initCfg.TargetURL != "http://target:8080" {
		t.Errorf("expected target URL %q, got %q", "http://target:8080", plugin.initCfg.TargetURL)
	}
	if plugin.initCfg.Settings["key"] != "value" {
		t.Errorf("expected setting key=value, got %v", plugin.initCfg.Settings)
	}
	if plugin.initCfg.Storage == nil {
		t.Error("expected non-nil storage accessor")
	}
}

func TestEngineInitPluginsError(t *testing.T) {
	plugin := newMockPlugin("fail-plugin")
	plugin.initErr = fmt.Errorf("init failed")

	e, _ := NewEngine(EngineConfig{
		ReaderFactory: &mockReaderFactory{},
		Plugins:       []ReplayPlugin{plugin},
	})

	err := e.InitPlugins(context.Background(), "", nil)
	if err == nil {
		t.Fatal("expected error from failing plugin init")
	}
}

func TestEngineReplay(t *testing.T) {
	requests := []*storage.CapturedRequest{
		{ID: "req-1", Method: "GET", Path: "/api/v1/users", Protocol: "HTTP", Timestamp: time.Now()},
		{ID: "req-2", Method: "POST", Path: "/api/v1/orders", Protocol: "HTTP", Timestamp: time.Now()},
		{ID: "req-3", Method: "GET", Path: "/api/v1/health", Protocol: "HTTP", Timestamp: time.Now()},
	}

	plugin := newMockPlugin("test-plugin")
	factory := &mockReaderFactory{requests: requests}

	e, _ := NewEngine(EngineConfig{
		ReaderFactory: factory,
		Plugins:       []ReplayPlugin{plugin},
	})
	_ = e.InitPlugins(context.Background(), "", nil)

	summary, err := e.Replay(context.Background(), "capture-123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if summary.TotalRequests != 3 {
		t.Errorf("expected 3 total requests, got %d", summary.TotalRequests)
	}
	if summary.CaptureID != "capture-123" {
		t.Errorf("expected capture ID %q, got %q", "capture-123", summary.CaptureID)
	}
	if len(plugin.requests) != 3 {
		t.Errorf("expected plugin to receive 3 requests, got %d", len(plugin.requests))
	}

	// Verify sequence numbers
	for i, req := range plugin.requests {
		if req.Sequence != uint64(i) {
			t.Errorf("request %d: expected sequence %d, got %d", i, i, req.Sequence)
		}
		if req.CaptureID != "capture-123" {
			t.Errorf("request %d: expected captureID %q, got %q", i, "capture-123", req.CaptureID)
		}
	}
}

func TestEngineReplayMultiplePlugins(t *testing.T) {
	requests := []*storage.CapturedRequest{
		{ID: "req-1", Method: "GET", Path: "/test", Protocol: "HTTP", Timestamp: time.Now()},
	}

	p1 := newMockPlugin("plugin-a")
	p2 := newMockPlugin("plugin-b")
	factory := &mockReaderFactory{requests: requests}

	e, _ := NewEngine(EngineConfig{
		ReaderFactory: factory,
		Plugins:       []ReplayPlugin{p1, p2},
	})
	_ = e.InitPlugins(context.Background(), "", nil)

	summary, err := e.Replay(context.Background(), "capture-456")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(p1.requests) != 1 {
		t.Errorf("plugin-a: expected 1 request, got %d", len(p1.requests))
	}
	if len(p2.requests) != 1 {
		t.Errorf("plugin-b: expected 1 request, got %d", len(p2.requests))
	}
	if summary.TotalRequests != 1 {
		t.Errorf("expected 1 total request, got %d", summary.TotalRequests)
	}
}

func TestEngineReplayWithPluginError(t *testing.T) {
	requests := []*storage.CapturedRequest{
		{ID: "req-1", Method: "GET", Path: "/test", Protocol: "HTTP", Timestamp: time.Now()},
	}

	plugin := newMockPlugin("error-plugin")
	plugin.processErr = fmt.Errorf("process failed")
	factory := &mockReaderFactory{requests: requests}

	e, _ := NewEngine(EngineConfig{
		ReaderFactory: factory,
		Plugins:       []ReplayPlugin{plugin},
	})

	summary, err := e.Replay(context.Background(), "capture-789")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if summary.TotalFailed != 1 {
		t.Errorf("expected 1 failed, got %d", summary.TotalFailed)
	}
}

func TestEngineReplayCancellation(t *testing.T) {
	requests := make([]*storage.CapturedRequest, 100)
	for i := range requests {
		requests[i] = &storage.CapturedRequest{
			ID:        fmt.Sprintf("req-%d", i),
			Method:    "GET",
			Path:      "/test",
			Protocol:  "HTTP",
			Timestamp: time.Now(),
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	plugin := newMockPlugin("cancel-plugin")
	factory := &mockReaderFactory{requests: requests}

	e, _ := NewEngine(EngineConfig{
		ReaderFactory: factory,
		Plugins:       []ReplayPlugin{plugin},
	})

	// Cancel after a few requests.
	origProcess := plugin.processErr
	_ = origProcess
	cancelAfter := 5
	plugin2 := &cancellingPlugin{
		mockPlugin:  plugin,
		cancelAfter: cancelAfter,
		cancel:      cancel,
	}

	e.plugins = []ReplayPlugin{plugin2}

	_, err := e.Replay(ctx, "capture-cancel")
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
}

type cancellingPlugin struct {
	*mockPlugin
	cancelAfter int
	cancel      context.CancelFunc
	count       int
}

func (c *cancellingPlugin) ProcessRequest(ctx context.Context, req *ReplayRequest) (*ReplayResult, error) {
	c.count++
	if c.count >= c.cancelAfter {
		c.cancel()
	}
	return c.mockPlugin.ProcessRequest(ctx, req)
}

func TestEngineClose(t *testing.T) {
	p1 := newMockPlugin("p1")
	p2 := newMockPlugin("p2")

	e, _ := NewEngine(EngineConfig{
		ReaderFactory: &mockReaderFactory{},
		Plugins:       []ReplayPlugin{p1, p2},
	})

	err := e.Close()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !p1.closed {
		t.Error("expected p1 to be closed")
	}
	if !p2.closed {
		t.Error("expected p2 to be closed")
	}
}

func TestEngineReplayReaderError(t *testing.T) {
	factory := &mockReaderFactory{err: fmt.Errorf("reader creation failed")}
	plugin := newMockPlugin("test")

	e, _ := NewEngine(EngineConfig{
		ReaderFactory: factory,
		Plugins:       []ReplayPlugin{plugin},
	})

	_, err := e.Replay(context.Background(), "bad-capture")
	if err == nil {
		t.Fatal("expected error from reader factory")
	}
}

func TestEngineMetrics(t *testing.T) {
	requests := []*storage.CapturedRequest{
		{ID: "req-1", Method: "GET", Path: "/ok", Protocol: "HTTP", Timestamp: time.Now()},
		{ID: "req-2", Method: "GET", Path: "/skip", Protocol: "HTTP", Timestamp: time.Now()},
	}

	plugin := newMockPlugin("metrics-plugin")
	plugin.results = []*ReplayResult{
		{RequestID: "req-1", Status: ReplayStatusSuccess, StatusCode: 200},
		{RequestID: "req-2", Status: ReplayStatusSkipped},
	}
	factory := &mockReaderFactory{requests: requests}

	e, _ := NewEngine(EngineConfig{
		ReaderFactory: factory,
		Plugins:       []ReplayPlugin{plugin},
	})

	_, _ = e.Replay(context.Background(), "capture-metrics")

	processed, succeeded, _, skipped := e.Metrics()
	if processed != 2 {
		t.Errorf("expected 2 processed, got %d", processed)
	}
	if succeeded != 1 {
		t.Errorf("expected 1 succeeded, got %d", succeeded)
	}
	if skipped != 1 {
		t.Errorf("expected 1 skipped, got %d", skipped)
	}
}
