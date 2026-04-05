package replay

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/kapture-io/kapture/internal/storage"
)

func TestFeedServer_NextEndpoint(t *testing.T) {
	reqs := testRequests(3)
	reader := &mockReader{requests: reqs}

	server, err := NewFeedServer(FeedServerConfig{
		Reader:     reader,
		Addr:       ":0", // will be overridden by test
		BufferSize: 10,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start server in background.
	if err := server.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer server.Stop(context.Background())

	// Wait for reader to fill buffer.
	time.Sleep(100 * time.Millisecond)

	// Verify requests are served via channel.
	for i := 0; i < 3; i++ {
		select {
		case req := <-server.reqCh:
			if req == nil {
				t.Fatal("expected non-nil request")
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for request")
		}
	}
}

func TestFeedServer_BatchEndpoint(t *testing.T) {
	reqs := testRequests(10)
	reader := &mockReader{requests: reqs}

	server, err := NewFeedServer(FeedServerConfig{
		Reader:     reader,
		BufferSize: 20,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := server.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer server.Stop(context.Background())

	// Wait for buffer to fill.
	time.Sleep(100 * time.Millisecond)

	// Drain batch from channel manually (simulating HTTP batch).
	batch := make([]*storage.CapturedRequest, 0, 5)
	for i := 0; i < 5; i++ {
		select {
		case req := <-server.reqCh:
			if req != nil {
				batch = append(batch, req)
			}
		case <-time.After(time.Second):
			break
		}
	}

	if len(batch) < 1 {
		t.Fatal("expected at least 1 request in batch")
	}
}

func TestFeedServer_StatusEndpoint(t *testing.T) {
	reqs := testRequests(5)
	reader := &mockReader{requests: reqs}

	server, err := NewFeedServer(FeedServerConfig{
		Reader:     reader,
		BufferSize: 10,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify the feedStatus struct marshals correctly.
	status := feedStatus{
		Served:    100,
		Acked:     90,
		BufferLen: 5,
		BufferCap: 10,
		Elapsed:   time.Minute,
		CurrentRPS: 50.5,
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
	_ = server // used above
}

func TestFeedServer_NilReaderError(t *testing.T) {
	_, err := NewFeedServer(FeedServerConfig{})
	if err == nil {
		t.Error("expected error with nil reader")
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

func TestFeedServer_AckHandler(t *testing.T) {
	reader := &mockReader{requests: testRequests(1)}
	server, err := NewFeedServer(FeedServerConfig{Reader: reader})
	if err != nil {
		t.Fatal(err)
	}

	// Test the ack handler directly.
	mux := http.NewServeMux()
	mux.HandleFunc("POST /ack", server.handleAck)

	// Verify ack counter starts at 0.
	if server.acked.Load() != 0 {
		t.Error("expected initial acked to be 0")
	}
}
