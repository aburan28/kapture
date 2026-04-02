package replay

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/kapture-io/kapture/internal/storage"
)

// --- test helpers ---

type mockReader struct {
	mu       sync.Mutex
	requests []*storage.CapturedRequest
	index    int
	opened   bool
	closed   bool
}

func (r *mockReader) Open(_ context.Context, _ ReadOptions) error {
	r.opened = true
	return nil
}

func (r *mockReader) Next(_ context.Context) (*storage.CapturedRequest, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.index >= len(r.requests) {
		return nil, ErrReaderDone
	}
	req := r.requests[r.index]
	r.index++
	return req, nil
}

func (r *mockReader) Close() error {
	r.closed = true
	return nil
}

type mockSender struct {
	mu       sync.Mutex
	sent     []*storage.CapturedRequest
	closed   bool
	sendErr  error
	latency  time.Duration
}

func (s *mockSender) Send(_ context.Context, req *storage.CapturedRequest) (*Result, error) {
	if s.sendErr != nil {
		return nil, s.sendErr
	}
	s.mu.Lock()
	s.sent = append(s.sent, req)
	s.mu.Unlock()
	return &Result{
		RequestID:  req.ID,
		StatusCode: 200,
		Duration:   s.latency,
		Timestamp:  time.Now().UTC(),
	}, nil
}

func (s *mockSender) Close() error {
	s.closed = true
	return nil
}

func testRequests(n int) []*storage.CapturedRequest {
	reqs := make([]*storage.CapturedRequest, n)
	base := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	for i := range reqs {
		reqs[i] = &storage.CapturedRequest{
			ID:        "req-" + string(rune('A'+i)),
			Method:    "GET",
			Path:      "/api/test",
			Protocol:  "HTTP",
			Timestamp: base.Add(time.Duration(i) * time.Second),
		}
	}
	return reqs
}

// --- Engine tests ---

func TestEngine_BasicReplay(t *testing.T) {
	reqs := testRequests(5)
	reader := &mockReader{requests: reqs}
	sender := &mockSender{latency: time.Millisecond}

	engine, err := NewEngine(EngineConfig{
		Reader:   reader,
		Sender:   sender,
		RateMode: RateModeUnlimited,
	})
	if err != nil {
		t.Fatal(err)
	}

	summary, err := engine.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if !reader.opened {
		t.Error("reader was not opened")
	}
	if len(sender.sent) != 5 {
		t.Fatalf("expected 5 sent, got %d", len(sender.sent))
	}
	if summary.TotalRequests != 5 {
		t.Errorf("expected 5 total, got %d", summary.TotalRequests)
	}
	if summary.SuccessCount != 5 {
		t.Errorf("expected 5 successes, got %d", summary.SuccessCount)
	}
	if summary.ErrorCount != 0 {
		t.Errorf("expected 0 errors, got %d", summary.ErrorCount)
	}
}

func TestEngine_WithTransformer(t *testing.T) {
	reqs := testRequests(3)
	reader := &mockReader{requests: reqs}
	sender := &mockSender{}

	transformer := NewTransformerFunc("add-header", func(_ context.Context, req *storage.CapturedRequest) (*storage.CapturedRequest, error) {
		cp := *req
		cp.Headers = map[string][]string{"X-Replay": {"true"}}
		return &cp, nil
	})

	engine, err := NewEngine(EngineConfig{
		Reader:       reader,
		Sender:       sender,
		Transformers: []Transformer{transformer},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = engine.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	for _, req := range sender.sent {
		if req.Headers["X-Replay"][0] != "true" {
			t.Error("transformer was not applied")
		}
	}
}

func TestEngine_TransformerFilter(t *testing.T) {
	reqs := testRequests(4)
	reqs[1].Path = "/health"
	reqs[3].Path = "/healthz"

	reader := &mockReader{requests: reqs}
	sender := &mockSender{}

	// Filter out health paths.
	filter := NewTransformerFunc("health-filter", func(_ context.Context, req *storage.CapturedRequest) (*storage.CapturedRequest, error) {
		if req.Path == "/health" || req.Path == "/healthz" {
			return nil, nil
		}
		return req, nil
	})

	engine, err := NewEngine(EngineConfig{
		Reader:       reader,
		Sender:       sender,
		Transformers: []Transformer{filter},
	})
	if err != nil {
		t.Fatal(err)
	}

	summary, err := engine.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if len(sender.sent) != 2 {
		t.Fatalf("expected 2 sent (2 filtered), got %d", len(sender.sent))
	}
	if summary.FilteredCount != 2 {
		t.Errorf("expected 2 filtered, got %d", summary.FilteredCount)
	}
}

func TestEngine_SenderErrors(t *testing.T) {
	reqs := testRequests(3)
	reader := &mockReader{requests: reqs}
	sender := &mockSender{sendErr: errors.New("connection refused")}

	engine, err := NewEngine(EngineConfig{
		Reader: reader,
		Sender: sender,
	})
	if err != nil {
		t.Fatal(err)
	}

	summary, err := engine.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if summary.ErrorCount != 3 {
		t.Errorf("expected 3 errors, got %d", summary.ErrorCount)
	}
	if summary.SuccessCount != 0 {
		t.Errorf("expected 0 successes, got %d", summary.SuccessCount)
	}
}

func TestEngine_Cancellation(t *testing.T) {
	// Create a reader with many requests.
	reqs := testRequests(10)
	reader := &mockReader{requests: reqs}
	sender := &mockSender{latency: 50 * time.Millisecond}

	engine, err := NewEngine(EngineConfig{
		Reader:   reader,
		Sender:   sender,
		RateMode: RateModeConstant,
		RatePerSecond: 2, // Slow rate to allow cancellation.
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err = engine.Run(ctx)
	if err == nil {
		t.Error("expected error from cancelled context")
	}

	// Should have sent fewer than all 10.
	if len(sender.sent) >= 10 {
		t.Error("expected cancellation to stop replay before all requests sent")
	}
}

func TestEngine_Concurrency(t *testing.T) {
	reqs := testRequests(10)
	reader := &mockReader{requests: reqs}
	sender := &mockSender{latency: 10 * time.Millisecond}

	engine, err := NewEngine(EngineConfig{
		Reader:      reader,
		Sender:      sender,
		Concurrency: 4,
	})
	if err != nil {
		t.Fatal(err)
	}

	summary, err := engine.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if summary.TotalRequests != 10 {
		t.Errorf("expected 10 total, got %d", summary.TotalRequests)
	}
}

func TestEngine_DoubleRun(t *testing.T) {
	reqs := testRequests(1)
	reader := &mockReader{requests: reqs}
	sender := &mockSender{}

	engine, err := NewEngine(EngineConfig{
		Reader: reader,
		Sender: sender,
	})
	if err != nil {
		t.Fatal(err)
	}

	// First run succeeds.
	_, err = engine.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
}

func TestEngine_NilReader(t *testing.T) {
	_, err := NewEngine(EngineConfig{
		Sender: &mockSender{},
	})
	if err == nil {
		t.Error("expected error with nil reader")
	}
}

func TestEngine_NilSender(t *testing.T) {
	_, err := NewEngine(EngineConfig{
		Reader: &mockReader{},
	})
	if err == nil {
		t.Error("expected error with nil sender")
	}
}

func TestEngine_Progress(t *testing.T) {
	reqs := testRequests(5)
	reader := &mockReader{requests: reqs}
	sender := &mockSender{}

	engine, err := NewEngine(EngineConfig{
		Reader: reader,
		Sender: sender,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, _ = engine.Run(context.Background())

	sent, errored, filtered := engine.Progress()
	if sent != 5 {
		t.Errorf("expected 5 sent, got %d", sent)
	}
	if errored != 0 {
		t.Errorf("expected 0 errored, got %d", errored)
	}
	if filtered != 0 {
		t.Errorf("expected 0 filtered, got %d", filtered)
	}
}

// --- HeaderRewriter tests ---

func TestHeaderRewriter(t *testing.T) {
	rewriter := &HeaderRewriter{
		Set:    map[string]string{"Authorization": "Bearer new-token"},
		Add:    map[string]string{"X-Replay-ID": "test-123"},
		Remove: []string{"Cookie"},
	}

	req := &storage.CapturedRequest{
		ID:   "test",
		Path: "/api",
		Headers: map[string][]string{
			"Authorization": {"Bearer old-token"},
			"Cookie":        {"session=abc"},
			"Content-Type":  {"application/json"},
		},
	}

	result, err := rewriter.Transform(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	// Set should overwrite.
	if result.Headers["Authorization"][0] != "Bearer new-token" {
		t.Error("Set did not overwrite Authorization")
	}

	// Add should append.
	if result.Headers["X-Replay-ID"][0] != "test-123" {
		t.Error("Add did not add X-Replay-ID")
	}

	// Remove should delete.
	if _, ok := result.Headers["Cookie"]; ok {
		t.Error("Remove did not delete Cookie")
	}

	// Untouched headers should remain.
	if result.Headers["Content-Type"][0] != "application/json" {
		t.Error("Content-Type should be unchanged")
	}

	// Original should be unmodified.
	if req.Headers["Authorization"][0] != "Bearer old-token" {
		t.Error("original was modified")
	}
}

// --- DefaultResultHandler tests ---

func TestDefaultResultHandler_Summary(t *testing.T) {
	handler := NewDefaultResultHandler(nil)
	ctx := context.Background()

	base := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	results := []*Result{
		{RequestID: "1", StatusCode: 200, Duration: 10 * time.Millisecond, Timestamp: base},
		{RequestID: "2", StatusCode: 200, Duration: 20 * time.Millisecond, Timestamp: base.Add(time.Second)},
		{RequestID: "3", StatusCode: 500, Duration: 5 * time.Millisecond, Timestamp: base.Add(2 * time.Second)},
		{RequestID: "4", Error: errors.New("timeout"), Duration: 0, Timestamp: base.Add(3 * time.Second)},
	}

	for _, r := range results {
		if err := handler.HandleResult(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	summary, err := handler.Summarize(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if summary.TotalRequests != 4 {
		t.Errorf("expected 4 total, got %d", summary.TotalRequests)
	}
	if summary.SuccessCount != 3 {
		t.Errorf("expected 3 success, got %d", summary.SuccessCount)
	}
	if summary.ErrorCount != 1 {
		t.Errorf("expected 1 error, got %d", summary.ErrorCount)
	}
	if summary.StatusCodeDist[200] != 2 {
		t.Errorf("expected 2 200s, got %d", summary.StatusCodeDist[200])
	}
	if summary.StatusCodeDist[500] != 1 {
		t.Errorf("expected 1 500, got %d", summary.StatusCodeDist[500])
	}
}

func TestDefaultResultHandler_EmptySummary(t *testing.T) {
	handler := NewDefaultResultHandler(nil)
	summary, err := handler.Summarize(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if summary.TotalRequests != 0 {
		t.Error("expected 0 total requests for empty summary")
	}
}

// --- Registry tests ---

func TestReplayRegistry(t *testing.T) {
	reg := NewRegistry()

	// Register a reader.
	err := reg.RegisterReader("mock", func(_ context.Context, _ map[string]any) (Reader, error) {
		return &mockReader{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Register a sender.
	err = reg.RegisterSender("http", func(_ context.Context, _ map[string]any) (Sender, error) {
		return &mockSender{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Build reader.
	reader, err := reg.BuildReader(context.Background(), "mock", nil)
	if err != nil {
		t.Fatal(err)
	}
	if reader == nil {
		t.Error("expected non-nil reader")
	}

	// Build sender.
	sender, err := reg.BuildSender(context.Background(), "http", nil)
	if err != nil {
		t.Fatal(err)
	}
	if sender == nil {
		t.Error("expected non-nil sender")
	}

	// Unknown should error.
	_, err = reg.BuildReader(context.Background(), "unknown", nil)
	if err == nil {
		t.Error("expected error for unknown reader")
	}
}

func TestReplayRegistry_DuplicateRegistration(t *testing.T) {
	reg := NewRegistry()
	builder := func(_ context.Context, _ map[string]any) (Reader, error) {
		return &mockReader{}, nil
	}
	_ = reg.RegisterReader("dup", builder)
	err := reg.RegisterReader("dup", builder)
	if err == nil {
		t.Error("expected error on duplicate reader registration")
	}
}

// --- TransformerFunc tests ---

func TestTransformerFunc(t *testing.T) {
	called := false
	tf := NewTransformerFunc("test-tf", func(_ context.Context, req *storage.CapturedRequest) (*storage.CapturedRequest, error) {
		called = true
		return req, nil
	})

	if tf.Name() != "test-tf" {
		t.Errorf("expected name test-tf, got %s", tf.Name())
	}

	_, err := tf.Transform(context.Background(), &storage.CapturedRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Error("function was not called")
	}
}

// --- errorCategory tests ---

func TestErrorCategory(t *testing.T) {
	tests := []struct {
		err      error
		expected string
	}{
		{nil, ""},
		{context.Canceled, "canceled"},
		{context.DeadlineExceeded, "deadline_exceeded"},
		{io.EOF, "connection_closed"},
		{errors.New("something"), "unknown"},
	}

	for _, tt := range tests {
		got := errorCategory(tt.err)
		if got != tt.expected {
			t.Errorf("errorCategory(%v) = %q, want %q", tt.err, got, tt.expected)
		}
	}
}

// --- sortDurations tests ---

func TestSortDurations(t *testing.T) {
	ds := []time.Duration{5, 3, 1, 4, 2}
	sortDurations(ds)
	for i := 0; i < len(ds)-1; i++ {
		if ds[i] > ds[i+1] {
			t.Errorf("not sorted at index %d: %v > %v", i, ds[i], ds[i+1])
		}
	}
}
