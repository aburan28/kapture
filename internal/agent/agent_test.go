package agent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/traffic-harvester/traffic-harvester/internal/storage"
)

// ---------------------------------------------------------------------------
// Mock storage.Writer
// ---------------------------------------------------------------------------

type mockWriter struct {
	mu         sync.Mutex
	writes     []*storage.CapturedRequest
	flushCount int
	closeCount int
	writeErr   error
	flushErr   error
	closeErr   error
}

func (m *mockWriter) Write(_ context.Context, req *storage.CapturedRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writeErr != nil {
		return m.writeErr
	}
	m.writes = append(m.writes, req)
	return nil
}

func (m *mockWriter) Flush(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushCount++
	if m.flushErr != nil {
		return m.flushErr
	}
	return nil
}

func (m *mockWriter) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeCount++
	return m.closeErr
}

func (m *mockWriter) getWrites() []*storage.CapturedRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]*storage.CapturedRequest, len(m.writes))
	copy(cp, m.writes)
	return cp
}

func (m *mockWriter) getFlushCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.flushCount
}

func (m *mockWriter) getCloseCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closeCount
}

// errReader is an io.Reader that always returns an error.
type errReader struct{ err error }

func (e *errReader) Read([]byte) (int, error) { return 0, e.err }

// discardLogger returns a logger that discards all output.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// ---------------------------------------------------------------------------
// CaptureHandler tests
// ---------------------------------------------------------------------------

func TestNewCaptureHandler_Defaults(t *testing.T) {
	mw := &mockWriter{}
	h := NewCaptureHandler(CaptureHandlerConfig{Writer: mw})

	if h.maxBodyBytes != 1<<20 {
		t.Errorf("expected default maxBodyBytes 1MB (%d), got %d", 1<<20, h.maxBodyBytes)
	}
	if h.log == nil {
		t.Error("expected non-nil logger when none provided")
	}
}

func TestNewCaptureHandler_CustomValues(t *testing.T) {
	mw := &mockWriter{}
	logger := discardLogger()
	h := NewCaptureHandler(CaptureHandlerConfig{
		Writer:       mw,
		MaxBodyBytes: 512,
		Logger:       logger,
	})

	if h.maxBodyBytes != 512 {
		t.Errorf("expected maxBodyBytes 512, got %d", h.maxBodyBytes)
	}
	if h.log != logger {
		t.Error("expected custom logger to be set")
	}
}

func TestHandle_ValidRequest(t *testing.T) {
	mw := &mockWriter{}
	h := NewCaptureHandler(CaptureHandlerConfig{Writer: mw, MaxBodyBytes: 1024, Logger: discardLogger()})

	req := &CapturedHTTPRequest{
		Method:   "GET",
		Path:     "/api/test",
		Headers:  map[string][]string{"X-Test": {"val"}},
		Body:     strings.NewReader("hello"),
		Protocol: "HTTP",
		Metadata: map[string]string{"key": "value"},
	}

	err := h.Handle(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	writes := mw.getWrites()
	if len(writes) != 1 {
		t.Fatalf("expected 1 write, got %d", len(writes))
	}

	w := writes[0]
	if w.Method != "GET" {
		t.Errorf("expected method GET, got %s", w.Method)
	}
	if w.Path != "/api/test" {
		t.Errorf("expected path /api/test, got %s", w.Path)
	}
	if string(w.Body) != "hello" {
		t.Errorf("expected body 'hello', got %q", w.Body)
	}
	if w.Protocol != "HTTP" {
		t.Errorf("expected protocol HTTP, got %s", w.Protocol)
	}
	if w.ContentLength != 5 {
		t.Errorf("expected content length 5, got %d", w.ContentLength)
	}
	if w.Metadata["key"] != "value" {
		t.Errorf("expected metadata key=value, got %q", w.Metadata["key"])
	}
	if w.ID == "" {
		t.Error("expected non-empty ID")
	}
	if w.Timestamp.IsZero() {
		t.Error("expected non-zero timestamp")
	}
}

func TestHandle_BodyExceedsMaxBytes(t *testing.T) {
	mw := &mockWriter{}
	maxBytes := int64(10)
	h := NewCaptureHandler(CaptureHandlerConfig{Writer: mw, MaxBodyBytes: maxBytes, Logger: discardLogger()})

	body := strings.Repeat("x", 20)
	req := &CapturedHTTPRequest{
		Method:   "POST",
		Path:     "/big",
		Body:     strings.NewReader(body),
		Protocol: "HTTP",
	}

	err := h.Handle(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	writes := mw.getWrites()
	if len(writes) != 1 {
		t.Fatalf("expected 1 write, got %d", len(writes))
	}

	w := writes[0]
	if int64(len(w.Body)) != maxBytes {
		t.Errorf("expected body truncated to %d bytes, got %d", maxBytes, len(w.Body))
	}
	if w.Metadata["bodyTruncated"] != "true" {
		t.Errorf("expected metadata bodyTruncated=true, got %q", w.Metadata["bodyTruncated"])
	}
}

func TestHandle_BodyExactlyAtMaxBytes(t *testing.T) {
	mw := &mockWriter{}
	maxBytes := int64(10)
	h := NewCaptureHandler(CaptureHandlerConfig{Writer: mw, MaxBodyBytes: maxBytes, Logger: discardLogger()})

	body := strings.Repeat("x", 10)
	req := &CapturedHTTPRequest{
		Method:   "POST",
		Path:     "/exact",
		Body:     strings.NewReader(body),
		Protocol: "HTTP",
		Metadata: map[string]string{"existing": "val"},
	}

	err := h.Handle(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	writes := mw.getWrites()
	w := writes[0]
	if int64(len(w.Body)) != maxBytes {
		t.Errorf("expected body length %d, got %d", maxBytes, len(w.Body))
	}
	if _, ok := w.Metadata["bodyTruncated"]; ok {
		t.Error("expected no bodyTruncated metadata for body exactly at limit")
	}
	// Ensure existing metadata is preserved
	if w.Metadata["existing"] != "val" {
		t.Error("expected existing metadata to be preserved")
	}
}

func TestHandle_WriterError_DropsAndReturnsError(t *testing.T) {
	writeErr := errors.New("write failed")
	mw := &mockWriter{writeErr: writeErr}
	h := NewCaptureHandler(CaptureHandlerConfig{Writer: mw, MaxBodyBytes: 1024, Logger: discardLogger()})

	req := &CapturedHTTPRequest{
		Method:   "GET",
		Path:     "/fail",
		Body:     strings.NewReader("data"),
		Protocol: "HTTP",
	}

	err := h.Handle(context.Background(), req)
	if !errors.Is(err, writeErr) {
		t.Fatalf("expected write error, got %v", err)
	}

	total, dropped, _ := h.Metrics()
	if total != 1 {
		t.Errorf("expected total=1, got %d", total)
	}
	if dropped != 1 {
		t.Errorf("expected dropped=1, got %d", dropped)
	}
}

func TestHandle_BodyReadError_DropsAndReturnsError(t *testing.T) {
	mw := &mockWriter{}
	h := NewCaptureHandler(CaptureHandlerConfig{Writer: mw, MaxBodyBytes: 1024, Logger: discardLogger()})

	readErr := errors.New("read failed")
	req := &CapturedHTTPRequest{
		Method:   "POST",
		Path:     "/read-fail",
		Body:     &errReader{err: readErr},
		Protocol: "HTTP",
	}

	err := h.Handle(context.Background(), req)
	if err == nil {
		t.Fatal("expected error from body read failure")
	}

	total, dropped, _ := h.Metrics()
	if total != 1 {
		t.Errorf("expected total=1, got %d", total)
	}
	if dropped != 1 {
		t.Errorf("expected dropped=1, got %d", dropped)
	}

	// No writes should have occurred
	if len(mw.getWrites()) != 0 {
		t.Error("expected no writes after body read error")
	}
}

func TestMetrics_AfterMultipleCalls(t *testing.T) {
	mw := &mockWriter{}
	h := NewCaptureHandler(CaptureHandlerConfig{Writer: mw, MaxBodyBytes: 1024, Logger: discardLogger()})

	// 3 successful requests with 3-byte bodies
	for i := 0; i < 3; i++ {
		req := &CapturedHTTPRequest{
			Method: "GET", Path: "/ok",
			Body: strings.NewReader("abc"), Protocol: "HTTP",
		}
		if err := h.Handle(context.Background(), req); err != nil {
			t.Fatalf("unexpected error on request %d: %v", i, err)
		}
	}

	// 2 failed requests with 3-byte bodies
	mw.mu.Lock()
	mw.writeErr = errors.New("fail")
	mw.mu.Unlock()

	for i := 0; i < 2; i++ {
		req := &CapturedHTTPRequest{
			Method: "GET", Path: "/fail",
			Body: strings.NewReader("def"), Protocol: "HTTP",
		}
		_ = h.Handle(context.Background(), req)
	}

	total, dropped, bytesRecv := h.Metrics()
	if total != 5 {
		t.Errorf("expected total=5, got %d", total)
	}
	if dropped != 2 {
		t.Errorf("expected dropped=2, got %d", dropped)
	}
	// bytesReceived is counted before the write call: 5 * 3 = 15
	if bytesRecv != 15 {
		t.Errorf("expected bytesRecv=15, got %d", bytesRecv)
	}
}

func TestHandle_NilMetadata_CreatedOnTruncation(t *testing.T) {
	mw := &mockWriter{}
	h := NewCaptureHandler(CaptureHandlerConfig{Writer: mw, MaxBodyBytes: 5, Logger: discardLogger()})

	req := &CapturedHTTPRequest{
		Method:   "POST",
		Path:     "/nil-meta",
		Body:     strings.NewReader("exceeds!"),
		Protocol: "HTTP",
		Metadata: nil,
	}

	err := h.Handle(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	w := mw.getWrites()[0]
	if w.Metadata == nil {
		t.Fatal("expected metadata to be initialized on truncation")
	}
	if w.Metadata["bodyTruncated"] != "true" {
		t.Error("expected bodyTruncated=true in metadata")
	}
}

func TestMetrics_InitiallyZero(t *testing.T) {
	mw := &mockWriter{}
	h := NewCaptureHandler(CaptureHandlerConfig{Writer: mw, Logger: discardLogger()})

	total, dropped, bytesRecv := h.Metrics()
	if total != 0 || dropped != 0 || bytesRecv != 0 {
		t.Errorf("expected all zero metrics, got total=%d dropped=%d bytes=%d", total, dropped, bytesRecv)
	}
}

// ---------------------------------------------------------------------------
// BufferedWriter tests
// ---------------------------------------------------------------------------

func TestBufferedWriter_WriteDelegatesToUnderlying(t *testing.T) {
	mw := &mockWriter{}
	bw := NewBufferedWriter(BufferedWriterConfig{
		Writer:        mw,
		BatchSize:     100,
		FlushInterval: time.Hour,
		Logger:        discardLogger(),
	})
	defer bw.Close()

	req := &storage.CapturedRequest{ID: "test-1", Method: "GET", Path: "/delegate"}

	err := bw.Write(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	writes := mw.getWrites()
	if len(writes) != 1 {
		t.Fatalf("expected 1 write, got %d", len(writes))
	}
	if writes[0].ID != "test-1" {
		t.Errorf("expected ID test-1, got %s", writes[0].ID)
	}
}

func TestBufferedWriter_FlushOnBatchSize(t *testing.T) {
	mw := &mockWriter{}
	batchSize := 3
	bw := NewBufferedWriter(BufferedWriterConfig{
		Writer:        mw,
		BatchSize:     batchSize,
		FlushInterval: time.Hour,
		Logger:        discardLogger(),
	})
	defer bw.Close()

	for i := 0; i < batchSize; i++ {
		req := &storage.CapturedRequest{ID: fmt.Sprintf("req-%d", i), Method: "GET", Path: "/batch"}
		if err := bw.Write(context.Background(), req); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	if fc := mw.getFlushCount(); fc < 1 {
		t.Errorf("expected at least 1 flush after batch size reached, got %d", fc)
	}
}

func TestBufferedWriter_NoFlushBelowBatchSize(t *testing.T) {
	mw := &mockWriter{}
	bw := NewBufferedWriter(BufferedWriterConfig{
		Writer:        mw,
		BatchSize:     10,
		FlushInterval: time.Hour,
		Logger:        discardLogger(),
	})
	defer bw.Close()

	for i := 0; i < 5; i++ {
		req := &storage.CapturedRequest{ID: fmt.Sprintf("req-%d", i)}
		if err := bw.Write(context.Background(), req); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	if fc := mw.getFlushCount(); fc != 0 {
		t.Errorf("expected 0 flushes below batch size, got %d", fc)
	}
}

func TestBufferedWriter_WriteReturnsErrWriterClosed(t *testing.T) {
	mw := &mockWriter{}
	bw := NewBufferedWriter(BufferedWriterConfig{
		Writer:        mw,
		BatchSize:     100,
		FlushInterval: time.Hour,
		Logger:        discardLogger(),
	})

	if err := bw.Close(); err != nil {
		t.Fatalf("close error: %v", err)
	}

	req := &storage.CapturedRequest{ID: "after-close"}
	err := bw.Write(context.Background(), req)
	if !errors.Is(err, storage.ErrWriterClosed) {
		t.Errorf("expected ErrWriterClosed, got %v", err)
	}
}

func TestBufferedWriter_FlushCallsUnderlyingFlush(t *testing.T) {
	mw := &mockWriter{}
	bw := NewBufferedWriter(BufferedWriterConfig{
		Writer:        mw,
		BatchSize:     100,
		FlushInterval: time.Hour,
		Logger:        discardLogger(),
	})
	defer bw.Close()

	err := bw.Flush(context.Background())
	if err != nil {
		t.Fatalf("flush error: %v", err)
	}

	if fc := mw.getFlushCount(); fc != 1 {
		t.Errorf("expected 1 flush, got %d", fc)
	}
}

func TestBufferedWriter_FlushPropagatesError(t *testing.T) {
	flushErr := errors.New("flush failed")
	mw := &mockWriter{flushErr: flushErr}
	bw := NewBufferedWriter(BufferedWriterConfig{
		Writer:        mw,
		BatchSize:     100,
		FlushInterval: time.Hour,
		Logger:        discardLogger(),
	})
	defer bw.Close()

	err := bw.Flush(context.Background())
	if !errors.Is(err, flushErr) {
		t.Errorf("expected flush error, got %v", err)
	}
}

func TestBufferedWriter_CloseFlushesAndCloses(t *testing.T) {
	mw := &mockWriter{}
	bw := NewBufferedWriter(BufferedWriterConfig{
		Writer:        mw,
		BatchSize:     100,
		FlushInterval: time.Hour,
		Logger:        discardLogger(),
	})

	// Write some data before closing
	req := &storage.CapturedRequest{ID: "pre-close"}
	if err := bw.Write(context.Background(), req); err != nil {
		t.Fatalf("write error: %v", err)
	}

	if err := bw.Close(); err != nil {
		t.Fatalf("close error: %v", err)
	}

	if fc := mw.getFlushCount(); fc < 1 {
		t.Error("expected at least 1 flush on close")
	}
	if cc := mw.getCloseCount(); cc != 1 {
		t.Errorf("expected 1 close call, got %d", cc)
	}
}

func TestBufferedWriter_CloseWithNoPending(t *testing.T) {
	mw := &mockWriter{}
	bw := NewBufferedWriter(BufferedWriterConfig{
		Writer:        mw,
		BatchSize:     100,
		FlushInterval: time.Hour,
		Logger:        discardLogger(),
	})

	err := bw.Close()
	if err != nil {
		t.Fatalf("close error: %v", err)
	}

	if cc := mw.getCloseCount(); cc != 1 {
		t.Errorf("expected 1 close call, got %d", cc)
	}
}

func TestBufferedWriter_WritePropagatesError(t *testing.T) {
	writeErr := errors.New("write failed")
	mw := &mockWriter{writeErr: writeErr}
	bw := NewBufferedWriter(BufferedWriterConfig{
		Writer:        mw,
		BatchSize:     100,
		FlushInterval: time.Hour,
		Logger:        discardLogger(),
	})
	defer bw.Close()

	req := &storage.CapturedRequest{ID: "fail"}
	err := bw.Write(context.Background(), req)
	if !errors.Is(err, writeErr) {
		t.Errorf("expected write error, got %v", err)
	}
}

func TestBufferedWriter_DefaultConfig(t *testing.T) {
	mw := &mockWriter{}
	bw := NewBufferedWriter(BufferedWriterConfig{Writer: mw})
	defer bw.Close()

	if bw.batchSize != defaultBatchSize {
		t.Errorf("expected default batch size %d, got %d", defaultBatchSize, bw.batchSize)
	}
	if bw.flushInterval != defaultFlushInterval {
		t.Errorf("expected default flush interval %v, got %v", defaultFlushInterval, bw.flushInterval)
	}
}

func TestBufferedWriter_PeriodicFlush(t *testing.T) {
	mw := &mockWriter{}
	bw := NewBufferedWriter(BufferedWriterConfig{
		Writer:        mw,
		BatchSize:     1000,
		FlushInterval: 50 * time.Millisecond,
		Logger:        discardLogger(),
	})
	defer bw.Close()

	req := &storage.CapturedRequest{ID: "periodic"}
	if err := bw.Write(context.Background(), req); err != nil {
		t.Fatalf("write error: %v", err)
	}

	// Wait for the periodic flush to fire
	time.Sleep(200 * time.Millisecond)

	if fc := mw.getFlushCount(); fc < 1 {
		t.Errorf("expected at least 1 periodic flush, got %d", fc)
	}
}

func TestBufferedWriter_PeriodicFlush_NoPendingNoFlush(t *testing.T) {
	mw := &mockWriter{}
	bw := NewBufferedWriter(BufferedWriterConfig{
		Writer:        mw,
		BatchSize:     1000,
		FlushInterval: 50 * time.Millisecond,
		Logger:        discardLogger(),
	})
	defer bw.Close()

	// No writes, just wait for a few ticks
	time.Sleep(200 * time.Millisecond)

	// flushLoop should not call Flush when pending == 0
	if fc := mw.getFlushCount(); fc != 0 {
		t.Errorf("expected 0 flushes with no pending writes, got %d", fc)
	}
}

// ---------------------------------------------------------------------------
// AgentServer HTTP handler tests
// ---------------------------------------------------------------------------

func newTestServer(mw *mockWriter) *AgentServer {
	handler := NewCaptureHandler(CaptureHandlerConfig{
		Writer:       mw,
		MaxBodyBytes: 1024,
		Logger:       discardLogger(),
	})
	return &AgentServer{
		handler: handler,
		log:     discardLogger(),
	}
}

func TestHandleHTTP_Success(t *testing.T) {
	mw := &mockWriter{}
	s := newTestServer(mw)

	body := bytes.NewBufferString(`{"key":"value"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/test?q=1", body)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	s.handleHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	writes := mw.getWrites()
	if len(writes) != 1 {
		t.Fatalf("expected 1 write, got %d", len(writes))
	}

	w := writes[0]
	if w.Method != "POST" {
		t.Errorf("expected method POST, got %s", w.Method)
	}
	if w.Path != "/api/test" {
		t.Errorf("expected path /api/test, got %s", w.Path)
	}
	if w.Protocol != "HTTP" {
		t.Errorf("expected protocol HTTP, got %s", w.Protocol)
	}
	if string(w.Body) != `{"key":"value"}` {
		t.Errorf("unexpected body: %s", w.Body)
	}
	if w.Metadata["query"] != "q=1" {
		t.Errorf("expected query metadata q=1, got %q", w.Metadata["query"])
	}
	if w.Metadata["host"] == "" {
		t.Error("expected host metadata to be set")
	}
}

func TestHandleHTTP_HandlerError_Returns500(t *testing.T) {
	mw := &mockWriter{writeErr: errors.New("storage down")}
	s := newTestServer(mw)

	req := httptest.NewRequest(http.MethodGet, "/fail", nil)
	rec := httptest.NewRecorder()

	s.handleHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", rec.Code)
	}
}

func TestHandleHTTP_EmptyBody(t *testing.T) {
	mw := &mockWriter{}
	s := newTestServer(mw)

	req := httptest.NewRequest(http.MethodGet, "/empty", nil)
	rec := httptest.NewRecorder()

	s.handleHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	writes := mw.getWrites()
	if len(writes) != 1 {
		t.Fatalf("expected 1 write, got %d", len(writes))
	}
	if len(writes[0].Body) != 0 {
		t.Errorf("expected empty body, got %d bytes", len(writes[0].Body))
	}
}

func TestHandleHTTP_PreservesHeaders(t *testing.T) {
	mw := &mockWriter{}
	s := newTestServer(mw)

	req := httptest.NewRequest(http.MethodGet, "/headers", nil)
	req.Header.Set("X-Custom", "custom-value")
	req.Header.Add("X-Multi", "a")
	req.Header.Add("X-Multi", "b")
	rec := httptest.NewRecorder()

	s.handleHTTP(rec, req)

	writes := mw.getWrites()
	w := writes[0]
	if v := w.Headers["X-Custom"]; len(v) != 1 || v[0] != "custom-value" {
		t.Errorf("expected X-Custom=[custom-value], got %v", v)
	}
	if v := w.Headers["X-Multi"]; len(v) != 2 {
		t.Errorf("expected X-Multi with 2 values, got %v", v)
	}
}

func TestHandleHTTP_DifferentMethods(t *testing.T) {
	methods := []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch}

	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			mw := &mockWriter{}
			s := newTestServer(mw)

			req := httptest.NewRequest(method, "/method-test", nil)
			rec := httptest.NewRecorder()

			s.handleHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Errorf("expected 200 for %s, got %d", method, rec.Code)
			}

			writes := mw.getWrites()
			if writes[0].Method != method {
				t.Errorf("expected method %s, got %s", method, writes[0].Method)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Health endpoint tests
// ---------------------------------------------------------------------------

func TestHandleHealth_ReturnsOK(t *testing.T) {
	mw := &mockWriter{}
	s := newTestServer(mw)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	s.handleHealth(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	contentType := rec.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("expected Content-Type application/json, got %s", contentType)
	}

	body := rec.Body.String()
	if body != `{"status":"ok"}` {
		t.Errorf("expected {\"status\":\"ok\"}, got %s", body)
	}
}

// ---------------------------------------------------------------------------
// Metrics endpoint tests
// ---------------------------------------------------------------------------

func TestHandleMetrics_PrometheusFormat(t *testing.T) {
	mw := &mockWriter{}
	s := newTestServer(mw)

	// Generate some metrics: 3 successful + 1 failed
	for i := 0; i < 3; i++ {
		req := &CapturedHTTPRequest{
			Method: "GET", Path: "/m",
			Body: strings.NewReader("data"), Protocol: "HTTP",
		}
		_ = s.handler.Handle(context.Background(), req)
	}

	mw.mu.Lock()
	mw.writeErr = errors.New("fail")
	mw.mu.Unlock()

	failReq := &CapturedHTTPRequest{
		Method: "GET", Path: "/m",
		Body: strings.NewReader("fail"), Protocol: "HTTP",
	}
	_ = s.handler.Handle(context.Background(), failReq)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	s.handleMetrics(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	contentType := rec.Header().Get("Content-Type")
	if contentType != "text/plain" {
		t.Errorf("expected Content-Type text/plain, got %s", contentType)
	}

	body := rec.Body.String()

	expectedLines := []string{
		"# HELP capture_agent_requests_total Total captured requests",
		"# TYPE capture_agent_requests_total counter",
		"capture_agent_requests_total 4",
		"# HELP capture_agent_requests_dropped_total Dropped requests",
		"# TYPE capture_agent_requests_dropped_total counter",
		"capture_agent_requests_dropped_total 1",
		"# HELP capture_agent_bytes_received_total Bytes received",
		"# TYPE capture_agent_bytes_received_total counter",
		"capture_agent_bytes_received_total 16", // 3*4 + 1*4
	}

	for _, expected := range expectedLines {
		if !strings.Contains(body, expected) {
			t.Errorf("metrics output missing %q\nfull body:\n%s", expected, body)
		}
	}
}

func TestHandleMetrics_ZeroValues(t *testing.T) {
	mw := &mockWriter{}
	s := newTestServer(mw)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	s.handleMetrics(rec, req)

	body := rec.Body.String()
	expectedZeros := []string{
		"capture_agent_requests_total 0",
		"capture_agent_requests_dropped_total 0",
		"capture_agent_bytes_received_total 0",
	}
	for _, expected := range expectedZeros {
		if !strings.Contains(body, expected) {
			t.Errorf("expected %q in metrics output, got:\n%s", expected, body)
		}
	}
}
