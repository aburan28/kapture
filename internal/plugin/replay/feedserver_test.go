package replay

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kapture-io/kapture/internal/storage"
)

// helper to build a FeedServer with a started reader for handler testing.
func newTestFeedServer(t *testing.T, reqs []*storage.CapturedRequest) *FeedServer {
	t.Helper()
	reader := &mockReader{requests: reqs}
	server, err := NewFeedServer(FeedServerConfig{
		Reader:     reader,
		BufferSize: 100,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := server.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Stop(context.Background()) })

	// Wait for reader to fill buffer.
	time.Sleep(100 * time.Millisecond)
	return server
}

func TestFeedServer_NextEndpoint(t *testing.T) {
	server := newTestFeedServer(t, testRequests(3))

	// Make HTTP requests to /next via the handler directly.
	for i := 0; i < 3; i++ {
		req := httptest.NewRequest("GET", "/next", nil)
		w := httptest.NewRecorder()
		server.handleNext(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("iteration %d: expected 200, got %d: %s", i, w.Code, w.Body.String())
		}

		var captured storage.CapturedRequest
		if err := json.NewDecoder(w.Body).Decode(&captured); err != nil {
			t.Fatalf("iteration %d: failed to decode response: %v", i, err)
		}
		if captured.ID == "" {
			t.Errorf("iteration %d: expected non-empty ID", i)
		}
	}

	if server.served.Load() != 3 {
		t.Errorf("expected 3 served, got %d", server.served.Load())
	}
}

func TestFeedServer_NextEndpoint_ReplayComplete(t *testing.T) {
	server := newTestFeedServer(t, testRequests(1))

	// Drain the one request.
	req := httptest.NewRequest("GET", "/next", nil)
	w := httptest.NewRecorder()
	server.handleNext(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	// Wait for reader to finish and channel to close.
	time.Sleep(100 * time.Millisecond)

	// Next call should return 410 Gone.
	req = httptest.NewRequest("GET", "/next", nil)
	w = httptest.NewRecorder()
	server.handleNext(w, req)
	if w.Code != http.StatusGone {
		t.Errorf("expected 410 Gone, got %d: %s", w.Code, w.Body.String())
	}
}

func TestFeedServer_BatchEndpoint(t *testing.T) {
	server := newTestFeedServer(t, testRequests(10))

	req := httptest.NewRequest("GET", "/batch?n=5", nil)
	w := httptest.NewRecorder()
	server.handleBatch(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var batch []*storage.CapturedRequest
	if err := json.NewDecoder(w.Body).Decode(&batch); err != nil {
		t.Fatalf("failed to decode batch: %v", err)
	}

	if len(batch) == 0 {
		t.Fatal("expected non-empty batch")
	}
	if len(batch) > 5 {
		t.Errorf("expected at most 5 requests, got %d", len(batch))
	}
}

func TestFeedServer_BatchEndpoint_Clamp(t *testing.T) {
	server := newTestFeedServer(t, testRequests(5))

	// Request more than 1000 — should be clamped.
	req := httptest.NewRequest("GET", "/batch?n=2000", nil)
	w := httptest.NewRecorder()
	server.handleBatch(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestFeedServer_StatusEndpoint(t *testing.T) {
	server := newTestFeedServer(t, testRequests(5))

	req := httptest.NewRequest("GET", "/status", nil)
	w := httptest.NewRecorder()
	server.handleStatus(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var status feedStatus
	if err := json.NewDecoder(w.Body).Decode(&status); err != nil {
		t.Fatalf("failed to decode status: %v", err)
	}

	if status.BufferCap != 100 {
		t.Errorf("expected buffer cap 100, got %d", status.BufferCap)
	}
}

func TestFeedServer_StatusEndpoint_ReaderDone(t *testing.T) {
	server := newTestFeedServer(t, testRequests(1))

	// Drain the request so reader finishes.
	<-server.reqCh
	time.Sleep(100 * time.Millisecond)

	req := httptest.NewRequest("GET", "/status", nil)
	w := httptest.NewRecorder()
	server.handleStatus(w, req)

	var status feedStatus
	json.NewDecoder(w.Body).Decode(&status)

	if !status.ReaderDone {
		t.Error("expected ReaderDone=true after all requests consumed")
	}
}

func TestFeedServer_AckEndpoint(t *testing.T) {
	server := newTestFeedServer(t, testRequests(1))

	body := strings.NewReader(`{"count": 50}`)
	req := httptest.NewRequest("POST", "/ack", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	server.handleAck(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if server.acked.Load() != 50 {
		t.Errorf("expected acked=50, got %d", server.acked.Load())
	}
}

func TestFeedServer_AckEndpoint_NegativeCount(t *testing.T) {
	server := newTestFeedServer(t, testRequests(1))

	body := strings.NewReader(`{"count": -10}`)
	req := httptest.NewRequest("POST", "/ack", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	server.handleAck(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for negative count, got %d", w.Code)
	}
}

func TestFeedServer_AckEndpoint_InvalidBody(t *testing.T) {
	server := newTestFeedServer(t, testRequests(1))

	body := strings.NewReader(`not json`)
	req := httptest.NewRequest("POST", "/ack", body)
	w := httptest.NewRecorder()
	server.handleAck(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid body, got %d", w.Code)
	}
}

func TestFeedServer_NilReaderError(t *testing.T) {
	_, err := NewFeedServer(FeedServerConfig{})
	if err == nil {
		t.Error("expected error with nil reader")
	}
}

func TestFeedServer_ConstantRateTreatedAsUnlimited(t *testing.T) {
	// RateModeConstant is the zero value (iota=0). FeedServer should silently
	// treat it as RateModeUnlimited rather than rejecting it.
	server, err := NewFeedServer(FeedServerConfig{
		Reader:   &mockReader{requests: testRequests(1)},
		RateMode: RateModeConstant,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if server.rateMode != RateModeUnlimited {
		t.Errorf("expected RateModeUnlimited, got %d", server.rateMode)
	}
}

func TestFeedServer_Defaults(t *testing.T) {
	reader := &mockReader{requests: testRequests(1)}

	server, err := NewFeedServer(FeedServerConfig{
		Reader: reader,
	})
	if err != nil {
		t.Fatal(err)
	}

	if server.addr != ":6565" {
		t.Errorf("expected default addr :6565, got %s", server.addr)
	}
	if server.bufferSize != 1000 {
		t.Errorf("expected default buffer 1000, got %d", server.bufferSize)
	}
	if server.timeScale != 1.0 {
		t.Errorf("expected default timeScale 1.0, got %f", server.timeScale)
	}
}

func TestFeedServer_FeedStatusJSON(t *testing.T) {
	status := feedStatus{
		Served:     100,
		Acked:      90,
		BufferLen:  5,
		BufferCap:  10,
		Elapsed:    time.Minute,
		CurrentRPS: 50.5,
		ReaderDone: true,
	}

	data, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}

	var decoded feedStatus
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}

	if decoded.Served != 100 {
		t.Errorf("expected served=100, got %d", decoded.Served)
	}
	if decoded.CurrentRPS != 50.5 {
		t.Errorf("expected currentRps=50.5, got %f", decoded.CurrentRPS)
	}
	if !decoded.ReaderDone {
		t.Error("expected ReaderDone=true")
	}
}
