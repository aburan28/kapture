package payload

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/kapture-io/kapture/internal/storage"
)

// --- test helpers ---

type mockWriter struct {
	mu       sync.Mutex
	written  []*storage.CapturedRequest
	flushed  int
	closed   bool
	writeErr error
}

func (w *mockWriter) Write(_ context.Context, req *storage.CapturedRequest) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.writeErr != nil {
		return w.writeErr
	}
	w.written = append(w.written, req)
	return nil
}

func (w *mockWriter) Flush(_ context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.flushed++
	return nil
}

func (w *mockWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	return nil
}

// passthroughPlugin passes requests through unchanged.
type passthroughPlugin struct{ initCalled, closeCalled bool }

func (p *passthroughPlugin) Name() string                         { return "passthrough" }
func (p *passthroughPlugin) Init(_ context.Context, _ map[string]any) error { p.initCalled = true; return nil }
func (p *passthroughPlugin) Process(_ context.Context, req *storage.CapturedRequest) (*storage.CapturedRequest, error) {
	return req, nil
}
func (p *passthroughPlugin) Close() error { p.closeCalled = true; return nil }

// filterPlugin drops requests matching a path prefix.
type filterPlugin struct{ prefix string }

func (p *filterPlugin) Name() string                         { return "filter" }
func (p *filterPlugin) Init(_ context.Context, _ map[string]any) error { return nil }
func (p *filterPlugin) Process(_ context.Context, req *storage.CapturedRequest) (*storage.CapturedRequest, error) {
	if strings.HasPrefix(req.Path, p.prefix) {
		return nil, nil // filtered
	}
	return req, nil
}
func (p *filterPlugin) Close() error { return nil }

// enrichPlugin adds metadata to requests.
type enrichPlugin struct{ key, value string }

func (p *enrichPlugin) Name() string                         { return "enrich" }
func (p *enrichPlugin) Init(_ context.Context, _ map[string]any) error { return nil }
func (p *enrichPlugin) Process(_ context.Context, req *storage.CapturedRequest) (*storage.CapturedRequest, error) {
	cp := CopyRequest(req)
	if cp.Metadata == nil {
		cp.Metadata = make(map[string]string)
	}
	cp.Metadata[p.key] = p.value
	return cp, nil
}
func (p *enrichPlugin) Close() error { return nil }

// errorPlugin always returns an error.
type errorPlugin struct{}

func (p *errorPlugin) Name() string                         { return "error-plugin" }
func (p *errorPlugin) Init(_ context.Context, _ map[string]any) error { return nil }
func (p *errorPlugin) Process(_ context.Context, _ *storage.CapturedRequest) (*storage.CapturedRequest, error) {
	return nil, errors.New("plugin error")
}
func (p *errorPlugin) Close() error { return nil }

func testReq(path string) *storage.CapturedRequest {
	return &storage.CapturedRequest{
		ID:       "test-id",
		Method:   "GET",
		Path:     path,
		Protocol: "HTTP",
	}
}

// --- Pipeline tests ---

func TestPipeline_Passthrough(t *testing.T) {
	w := &mockWriter{}
	p, err := NewPipeline(PipelineConfig{
		Downstream: w,
		Plugins:    []Plugin{&passthroughPlugin{}},
	})
	if err != nil {
		t.Fatal(err)
	}

	req := testReq("/api/users")
	if err := p.Write(context.Background(), req); err != nil {
		t.Fatal(err)
	}

	if len(w.written) != 1 {
		t.Fatalf("expected 1 written, got %d", len(w.written))
	}
	if w.written[0].Path != "/api/users" {
		t.Errorf("expected path /api/users, got %s", w.written[0].Path)
	}
}

func TestPipeline_Filter(t *testing.T) {
	w := &mockWriter{}
	p, err := NewPipeline(PipelineConfig{
		Downstream: w,
		Plugins:    []Plugin{&filterPlugin{prefix: "/health"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_ = p.Write(ctx, testReq("/healthz"))         // filtered
	_ = p.Write(ctx, testReq("/api/users"))        // passes
	_ = p.Write(ctx, testReq("/health/readiness")) // filtered

	if len(w.written) != 1 {
		t.Fatalf("expected 1 written, got %d", len(w.written))
	}
	if w.written[0].Path != "/api/users" {
		t.Errorf("expected /api/users, got %s", w.written[0].Path)
	}
}

func TestPipeline_Enrich(t *testing.T) {
	w := &mockWriter{}
	p, err := NewPipeline(PipelineConfig{
		Downstream: w,
		Plugins:    []Plugin{&enrichPlugin{key: "env", value: "staging"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	req := testReq("/api/users")
	_ = p.Write(context.Background(), req)

	if len(w.written) != 1 {
		t.Fatal("expected 1 written")
	}
	if w.written[0].Metadata["env"] != "staging" {
		t.Errorf("expected metadata env=staging, got %v", w.written[0].Metadata)
	}
	// Original should be unmodified.
	if req.Metadata != nil {
		t.Error("original request metadata should be nil")
	}
}

func TestPipeline_Chain(t *testing.T) {
	w := &mockWriter{}
	p, err := NewPipeline(PipelineConfig{
		Downstream: w,
		Plugins: []Plugin{
			&filterPlugin{prefix: "/health"},
			&enrichPlugin{key: "processed", value: "true"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_ = p.Write(ctx, testReq("/healthz"))
	_ = p.Write(ctx, testReq("/api/data"))

	if len(w.written) != 1 {
		t.Fatalf("expected 1 written, got %d", len(w.written))
	}
	if w.written[0].Metadata["processed"] != "true" {
		t.Error("enrichment not applied")
	}
}

func TestPipeline_ErrorDrop(t *testing.T) {
	w := &mockWriter{}
	p, err := NewPipeline(PipelineConfig{
		Downstream: w,
		Plugins:    []Plugin{&errorPlugin{}},
		OnError:    ErrorHandlerDrop,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = p.Write(context.Background(), testReq("/api"))
	if err != nil {
		t.Errorf("expected nil error with drop handler, got %v", err)
	}
	if len(w.written) != 0 {
		t.Error("request should have been dropped")
	}
}

func TestPipeline_ErrorSkip(t *testing.T) {
	w := &mockWriter{}
	p, err := NewPipeline(PipelineConfig{
		Downstream: w,
		Plugins: []Plugin{
			&errorPlugin{},
			&enrichPlugin{key: "after-error", value: "yes"},
		},
		OnError: ErrorHandlerSkip,
	})
	if err != nil {
		t.Fatal(err)
	}

	_ = p.Write(context.Background(), testReq("/api"))

	if len(w.written) != 1 {
		t.Fatal("request should have passed through after skipping error plugin")
	}
	if w.written[0].Metadata["after-error"] != "yes" {
		t.Error("subsequent plugin should have run")
	}
}

func TestPipeline_ErrorAbort(t *testing.T) {
	w := &mockWriter{}
	p, err := NewPipeline(PipelineConfig{
		Downstream: w,
		Plugins:    []Plugin{&errorPlugin{}},
		OnError:    ErrorHandlerAbort,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = p.Write(context.Background(), testReq("/api"))
	if err == nil {
		t.Error("expected error with abort handler")
	}
	if len(w.written) != 0 {
		t.Error("request should not have been written")
	}
}

func TestPipeline_NoPlugins(t *testing.T) {
	w := &mockWriter{}
	p, err := NewPipeline(PipelineConfig{Downstream: w})
	if err != nil {
		t.Fatal(err)
	}

	_ = p.Write(context.Background(), testReq("/api"))
	if len(w.written) != 1 {
		t.Error("with no plugins, request should pass through to writer")
	}
}

func TestPipeline_Close(t *testing.T) {
	pl := &passthroughPlugin{}
	w := &mockWriter{}
	p, err := NewPipeline(PipelineConfig{
		Downstream: w,
		Plugins:    []Plugin{pl},
	})
	if err != nil {
		t.Fatal(err)
	}

	_ = p.Close()

	if !pl.closeCalled {
		t.Error("plugin Close not called")
	}
	if !w.closed {
		t.Error("downstream Close not called")
	}

	// Writing after close should fail.
	err = p.Write(context.Background(), testReq("/api"))
	if !errors.Is(err, storage.ErrWriterClosed) {
		t.Errorf("expected ErrWriterClosed, got %v", err)
	}
}

func TestPipeline_Flush(t *testing.T) {
	w := &mockWriter{}
	p, err := NewPipeline(PipelineConfig{Downstream: w})
	if err != nil {
		t.Fatal(err)
	}

	_ = p.Flush(context.Background())
	if w.flushed != 1 {
		t.Errorf("expected 1 flush, got %d", w.flushed)
	}
}

func TestPipeline_NilDownstream(t *testing.T) {
	_, err := NewPipeline(PipelineConfig{})
	if err == nil {
		t.Error("expected error with nil downstream")
	}
}

// --- Registry tests ---

func TestRegistry_RegisterAndBuild(t *testing.T) {
	reg := NewRegistry()
	err := reg.Register("test-plugin", func(_ context.Context, _ map[string]any) (Plugin, error) {
		return &passthroughPlugin{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	plugin, err := reg.Build(context.Background(), "test-plugin", nil)
	if err != nil {
		t.Fatal(err)
	}
	if plugin.Name() != "passthrough" {
		t.Errorf("expected name passthrough, got %s", plugin.Name())
	}
}

func TestRegistry_DuplicateName(t *testing.T) {
	reg := NewRegistry()
	builder := func(_ context.Context, _ map[string]any) (Plugin, error) {
		return &passthroughPlugin{}, nil
	}
	_ = reg.Register("dupe", builder)
	err := reg.Register("dupe", builder)
	if err == nil {
		t.Error("expected error on duplicate registration")
	}
}

func TestRegistry_UnknownPlugin(t *testing.T) {
	reg := NewRegistry()
	_, err := reg.Build(context.Background(), "nonexistent", nil)
	if err == nil {
		t.Error("expected error for unknown plugin")
	}
}

func TestRegistry_Names(t *testing.T) {
	reg := NewRegistry()
	_ = reg.Register("alpha", func(_ context.Context, _ map[string]any) (Plugin, error) {
		return &passthroughPlugin{}, nil
	})
	_ = reg.Register("beta", func(_ context.Context, _ map[string]any) (Plugin, error) {
		return &passthroughPlugin{}, nil
	})

	names := reg.Names()
	if len(names) != 2 {
		t.Fatalf("expected 2 names, got %d", len(names))
	}
}

// --- CopyRequest tests ---

func TestCopyRequest(t *testing.T) {
	orig := &storage.CapturedRequest{
		ID:       "orig",
		Method:   "POST",
		Path:     "/api",
		Headers:  map[string][]string{"Authorization": {"Bearer token"}},
		Body:     []byte(`{"key":"value"}`),
		Metadata: map[string]string{"env": "prod"},
	}

	cp := CopyRequest(orig)

	// Modify copy.
	cp.Headers["Authorization"] = []string{"REDACTED"}
	cp.Body[0] = 'X'
	cp.Metadata["env"] = "test"

	// Verify original is unchanged.
	if orig.Headers["Authorization"][0] != "Bearer token" {
		t.Error("original headers modified")
	}
	if orig.Body[0] != '{' {
		t.Error("original body modified")
	}
	if orig.Metadata["env"] != "prod" {
		t.Error("original metadata modified")
	}
}

func TestCopyRequest_Nil(t *testing.T) {
	if CopyRequest(nil) != nil {
		t.Error("expected nil for nil input")
	}
}
