package spoke

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	hubv1 "github.com/kapture-io/kapture/proto/hub/v1"
)

// ---------------------------------------------------------------------------
// StartHeartbeat
// ---------------------------------------------------------------------------

func TestHubClient_StartHeartbeat_SendsAtInterval(t *testing.T) {
	mock := &mockHubServer{}
	addr, cleanup := startMockServer(t, mock)
	defer cleanup()

	client := newTestClient(t, addr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	if _, err := client.Register(ctx); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	client.StartHeartbeat(ctx, 50*time.Millisecond)

	time.Sleep(200 * time.Millisecond)

	mock.mu.Lock()
	calls := mock.heartbeatCalls
	mock.mu.Unlock()

	if calls < 2 {
		t.Errorf("expected at least 2 heartbeat calls, got %d", calls)
	}
}

func TestHubClient_StartHeartbeat_StopsOnContextCancel(t *testing.T) {
	mock := &mockHubServer{}
	addr, cleanup := startMockServer(t, mock)
	defer cleanup()

	client := newTestClient(t, addr)
	ctx, cancel := context.WithCancel(context.Background())

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	if _, err := client.Register(ctx); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	client.StartHeartbeat(ctx, 50*time.Millisecond)

	// Wait for some heartbeats.
	time.Sleep(150 * time.Millisecond)

	// Cancel context to stop heartbeat.
	cancel()

	// Record current count.
	time.Sleep(50 * time.Millisecond)
	mock.mu.Lock()
	countAfterCancel := mock.heartbeatCalls
	mock.mu.Unlock()

	// Wait and verify no more heartbeats.
	time.Sleep(200 * time.Millisecond)
	mock.mu.Lock()
	countLater := mock.heartbeatCalls
	mock.mu.Unlock()

	if countLater > countAfterCancel+1 { // allow 1 in-flight
		t.Errorf("heartbeats continued after cancel: %d -> %d", countAfterCancel, countLater)
	}
}

func TestHubClient_StartHeartbeat_RestartReplacesOld(t *testing.T) {
	mock := &mockHubServer{}
	addr, cleanup := startMockServer(t, mock)
	defer cleanup()

	client := newTestClient(t, addr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	if _, err := client.Register(ctx); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Start first heartbeat.
	client.StartHeartbeat(ctx, 50*time.Millisecond)
	time.Sleep(100 * time.Millisecond)

	// Start second heartbeat (should replace first).
	client.StartHeartbeat(ctx, 50*time.Millisecond)
	time.Sleep(200 * time.Millisecond)

	// Should not panic and heartbeats should still be working.
	mock.mu.Lock()
	calls := mock.heartbeatCalls
	mock.mu.Unlock()

	if calls < 2 {
		t.Errorf("expected at least 2 heartbeat calls after restart, got %d", calls)
	}
}

// ---------------------------------------------------------------------------
// sendHeartbeat
// ---------------------------------------------------------------------------

func TestHubClient_SendHeartbeat_NilClient(t *testing.T) {
	client := NewHubClient(HubClientConfig{
		HubAddress: "localhost:0",
		SpokeName:  "test",
		ClusterID:  "test",
		Logger:     logr.Discard(),
	})

	// Should not panic when client is nil.
	client.sendHeartbeat(context.Background())
}

// ---------------------------------------------------------------------------
// ReportStatus
// ---------------------------------------------------------------------------

func TestHubClient_ReportStatus_NotConnected(t *testing.T) {
	client := NewHubClient(HubClientConfig{
		HubAddress: "localhost:0",
		SpokeName:  "test",
		ClusterID:  "test",
		Logger:     logr.Discard(),
	})

	err := client.ReportStatus(context.Background(), []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-1", CaptureNamespace: "ns-1"},
	})
	if err == nil {
		t.Error("expected error when not connected")
	}
}

func TestHubClient_ReportStatus_SendsStatuses(t *testing.T) {
	mock := &mockHubServer{}
	addr, cleanup := startMockServer(t, mock)
	defer cleanup()

	client := newTestClient(t, addr)
	ctx := context.Background()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	if _, err := client.Register(ctx); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	statuses := []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-1", CaptureNamespace: "ns-1", Phase: hubv1.CapturePhase_CAPTURE_PHASE_ACTIVE},
		{CaptureName: "cap-2", CaptureNamespace: "ns-2", Phase: hubv1.CapturePhase_CAPTURE_PHASE_PENDING},
	}

	if err := client.ReportStatus(ctx, statuses); err != nil {
		t.Fatalf("ReportStatus failed: %v", err)
	}

	mock.mu.Lock()
	if mock.reportStatusCalls != 1 {
		t.Errorf("expected 1 report call, got %d", mock.reportStatusCalls)
	}
	if len(mock.lastStatuses) != 2 {
		t.Errorf("expected 2 statuses, got %d", len(mock.lastStatuses))
	}
	mock.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Deregister
// ---------------------------------------------------------------------------

func TestHubClient_Deregister_NotConnected(t *testing.T) {
	client := NewHubClient(HubClientConfig{
		HubAddress: "localhost:0",
		SpokeName:  "test",
		ClusterID:  "test",
		Logger:     logr.Discard(),
	})

	// Should be no-op when not connected (client is nil).
	err := client.Deregister(context.Background())
	if err != nil {
		t.Errorf("expected no error, got: %v", err)
	}
}

func TestHubClient_Deregister_SendsRequest(t *testing.T) {
	mock := &mockHubServer{}
	addr, cleanup := startMockServer(t, mock)
	defer cleanup()

	client := newTestClient(t, addr)
	ctx := context.Background()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	if _, err := client.Register(ctx); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	if err := client.Deregister(ctx); err != nil {
		t.Fatalf("Deregister failed: %v", err)
	}

	mock.mu.Lock()
	if mock.deregisterCalls != 1 {
		t.Errorf("expected 1 deregister call, got %d", mock.deregisterCalls)
	}
	if mock.lastSpokeID != "test-spoke" {
		t.Errorf("expected spoke ID test-spoke, got %s", mock.lastSpokeID)
	}
	mock.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------

func TestHubClient_Close_StopsHeartbeatAndDirectives(t *testing.T) {
	mock := &mockHubServer{}
	addr, cleanup := startMockServer(t, mock)
	defer cleanup()

	client := newTestClient(t, addr)
	ctx := context.Background()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	if _, err := client.Register(ctx); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	client.StartHeartbeat(ctx, 50*time.Millisecond)
	time.Sleep(100 * time.Millisecond)

	if err := client.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if client.IsConnected() {
		t.Error("expected not connected after Close")
	}

	// Record count and verify it stops.
	mock.mu.Lock()
	countAtClose := mock.heartbeatCalls
	mock.mu.Unlock()

	time.Sleep(200 * time.Millisecond)

	mock.mu.Lock()
	countLater := mock.heartbeatCalls
	mock.mu.Unlock()

	if countLater > countAtClose+1 {
		t.Errorf("heartbeats continued after Close: %d -> %d", countAtClose, countLater)
	}
}

func TestHubClient_Close_NilConnection(t *testing.T) {
	client := NewHubClient(HubClientConfig{
		HubAddress: "localhost:0",
		SpokeName:  "test",
		ClusterID:  "test",
		Logger:     logr.Discard(),
	})

	err := client.Close()
	if err != nil {
		t.Errorf("expected no error closing unconnected client, got: %v", err)
	}
}

func TestHubClient_Close_Idempotent(t *testing.T) {
	mock := &mockHubServer{}
	addr, cleanup := startMockServer(t, mock)
	defer cleanup()

	client := newTestClient(t, addr)
	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	if err := client.Close(); err != nil {
		t.Fatalf("first Close failed: %v", err)
	}
	// Second close should not panic.
	err := client.Close()
	// May return error from closed conn, but should not panic.
	_ = err
}

// ---------------------------------------------------------------------------
// IsConnected
// ---------------------------------------------------------------------------

func TestHubClient_IsConnected_Lifecycle(t *testing.T) {
	mock := &mockHubServer{}
	addr, cleanup := startMockServer(t, mock)
	defer cleanup()

	client := newTestClient(t, addr)

	if client.IsConnected() {
		t.Error("expected not connected before Connect")
	}

	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	if !client.IsConnected() {
		t.Error("expected connected after Connect")
	}

	_ = client.Close()

	if client.IsConnected() {
		t.Error("expected not connected after Close")
	}
}

// ---------------------------------------------------------------------------
// Register: not connected
// ---------------------------------------------------------------------------

func TestHubClient_Register_NotConnected(t *testing.T) {
	client := NewHubClient(HubClientConfig{
		HubAddress: "localhost:0",
		SpokeName:  "test",
		ClusterID:  "test",
		Logger:     logr.Discard(),
	})

	_, err := client.Register(context.Background())
	if err == nil {
		t.Error("expected error when registering without connection")
	}
}
