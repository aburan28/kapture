package hub

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	hubv1 "github.com/traffic-harvester/traffic-harvester/proto/hub/v1"
)

func newTestServer() *Server {
	return NewServer(":0")
}

// helper to register a spoke and fail the test on error.
func mustRegister(t *testing.T, srv *Server, spokeID, clusterName string) {
	t.Helper()
	_, err := srv.RegisterSpoke(context.Background(), &hubv1.RegisterSpokeRequest{
		SpokeId:     spokeID,
		ClusterName: clusterName,
	})
	if err != nil {
		t.Fatalf("RegisterSpoke(%q) failed: %v", spokeID, err)
	}
}

// helper to report captures and fail the test on error.
func mustReportCaptures(t *testing.T, srv *Server, spokeID string, statuses []*hubv1.CaptureStatusSummary) {
	t.Helper()
	_, err := srv.ReportCaptureStatus(context.Background(), &hubv1.ReportCaptureStatusRequest{
		SpokeId:  spokeID,
		Statuses: statuses,
	})
	if err != nil {
		t.Fatalf("ReportCaptureStatus(%q) failed: %v", spokeID, err)
	}
}

func assertGRPCCode(t *testing.T, err error, expected codes.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error with code %v, got nil", expected)
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != expected {
		t.Errorf("expected code %v, got %v (message: %s)", expected, st.Code(), st.Message())
	}
}

// ---------------------------------------------------------------------------
// RegisterSpoke
// ---------------------------------------------------------------------------

func TestRegisterSpoke_EmptySpokeID(t *testing.T) {
	srv := newTestServer()
	_, err := srv.RegisterSpoke(context.Background(), &hubv1.RegisterSpokeRequest{})
	assertGRPCCode(t, err, codes.InvalidArgument)
}

func TestRegisterSpoke_ValidRegistration(t *testing.T) {
	srv := newTestServer()
	resp, err := srv.RegisterSpoke(context.Background(), &hubv1.RegisterSpokeRequest{
		SpokeId:     "spoke-1",
		ClusterName: "cluster-a",
	})
	if err != nil {
		t.Fatalf("RegisterSpoke failed: %v", err)
	}
	if !resp.Accepted {
		t.Error("expected Accepted=true")
	}
	if resp.HeartbeatIntervalSeconds != DefaultHeartbeatInterval {
		t.Errorf("HeartbeatIntervalSeconds = %d, want %d", resp.HeartbeatIntervalSeconds, DefaultHeartbeatInterval)
	}
	if srv.ConnectedSpokeCount() != 1 {
		t.Errorf("ConnectedSpokeCount = %d, want 1", srv.ConnectedSpokeCount())
	}
}

func TestRegisterSpoke_ReRegistrationOverwrites(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "spoke-1", "cluster-old")

	// Report a capture so the entry has state.
	mustReportCaptures(t, srv, "spoke-1", []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-1", CaptureNamespace: "ns-1"},
	})
	if srv.ActiveCaptureCount() != 1 {
		t.Fatalf("expected 1 capture before re-registration, got %d", srv.ActiveCaptureCount())
	}

	// Re-register same spoke with different cluster name.
	resp, err := srv.RegisterSpoke(ctx, &hubv1.RegisterSpokeRequest{
		SpokeId:     "spoke-1",
		ClusterName: "cluster-new",
	})
	if err != nil {
		t.Fatalf("re-registration failed: %v", err)
	}
	if !resp.Accepted {
		t.Error("expected Accepted=true on re-registration")
	}

	// Old captures should be gone (entry was overwritten).
	if srv.ActiveCaptureCount() != 0 {
		t.Errorf("expected 0 captures after re-registration, got %d", srv.ActiveCaptureCount())
	}

	// Only one spoke should exist.
	if srv.ConnectedSpokeCount() != 1 {
		t.Errorf("ConnectedSpokeCount = %d, want 1", srv.ConnectedSpokeCount())
	}

	// Verify new cluster name via SpokeStatuses.
	statuses := srv.SpokeStatuses()
	if len(statuses) != 1 {
		t.Fatalf("expected 1 spoke status, got %d", len(statuses))
	}
	if statuses[0].Name != "cluster-new" {
		t.Errorf("cluster name = %q, want %q", statuses[0].Name, "cluster-new")
	}
}

// ---------------------------------------------------------------------------
// Heartbeat
// ---------------------------------------------------------------------------

func TestHeartbeat_EmptySpokeID(t *testing.T) {
	srv := newTestServer()
	_, err := srv.Heartbeat(context.Background(), &hubv1.HeartbeatRequest{})
	assertGRPCCode(t, err, codes.InvalidArgument)
}

func TestHeartbeat_UnregisteredSpoke(t *testing.T) {
	srv := newTestServer()
	_, err := srv.Heartbeat(context.Background(), &hubv1.HeartbeatRequest{SpokeId: "unknown"})
	assertGRPCCode(t, err, codes.NotFound)
}

func TestHeartbeat_UpdatesLastHeartbeat(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "spoke-1", "c")

	// Backdate the heartbeat.
	srv.mu.Lock()
	srv.spokes["spoke-1"].lastHeartbeat = time.Now().Add(-time.Minute)
	srv.mu.Unlock()

	before := time.Now()
	resp, err := srv.Heartbeat(ctx, &hubv1.HeartbeatRequest{SpokeId: "spoke-1"})
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}
	if !resp.Acknowledged {
		t.Error("expected Acknowledged=true")
	}

	srv.mu.RLock()
	lastHB := srv.spokes["spoke-1"].lastHeartbeat
	srv.mu.RUnlock()

	if lastHB.Before(before) {
		t.Errorf("lastHeartbeat (%v) should be >= %v", lastHB, before)
	}
}

func TestHeartbeat_UpdatesActiveCapturesAndSummaries(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "spoke-1", "c")

	resp, err := srv.Heartbeat(ctx, &hubv1.HeartbeatRequest{
		SpokeId:        "spoke-1",
		ActiveCaptures: 5,
		CaptureSummaries: []*hubv1.CaptureStatusSummary{
			{CaptureName: "cap-a", CaptureNamespace: "ns-1", Phase: hubv1.CapturePhase_CAPTURE_PHASE_ACTIVE, CapturedRequests: 100},
			{CaptureName: "cap-b", CaptureNamespace: "ns-2", Phase: hubv1.CapturePhase_CAPTURE_PHASE_PENDING, CapturedRequests: 0},
		},
	})
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}
	if !resp.Acknowledged {
		t.Error("expected Acknowledged=true")
	}

	// Verify ActiveCaptures was updated on the spoke info.
	srv.mu.RLock()
	ac := srv.spokes["spoke-1"].info.ActiveCaptures
	numCaptures := len(srv.spokes["spoke-1"].captures)
	srv.mu.RUnlock()

	if ac != 5 {
		t.Errorf("ActiveCaptures = %d, want 5", ac)
	}
	if numCaptures != 2 {
		t.Errorf("captures map has %d entries, want 2", numCaptures)
	}

	// Verify captures are queryable.
	listResp, _ := srv.ListCaptures(ctx, &hubv1.ListCapturesRequest{})
	if len(listResp.Captures) != 2 {
		t.Errorf("ListCaptures returned %d, want 2", len(listResp.Captures))
	}
}

// ---------------------------------------------------------------------------
// DeregisterSpoke
// ---------------------------------------------------------------------------

func TestDeregisterSpoke_EmptySpokeID(t *testing.T) {
	srv := newTestServer()
	_, err := srv.DeregisterSpoke(context.Background(), &hubv1.DeregisterSpokeRequest{})
	assertGRPCCode(t, err, codes.InvalidArgument)
}

func TestDeregisterSpoke_UnregisteredSpoke(t *testing.T) {
	srv := newTestServer()
	_, err := srv.DeregisterSpoke(context.Background(), &hubv1.DeregisterSpokeRequest{SpokeId: "nope"})
	assertGRPCCode(t, err, codes.NotFound)
}

func TestDeregisterSpoke_Valid(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "spoke-1", "c")

	resp, err := srv.DeregisterSpoke(ctx, &hubv1.DeregisterSpokeRequest{SpokeId: "spoke-1"})
	if err != nil {
		t.Fatalf("DeregisterSpoke failed: %v", err)
	}
	if !resp.Acknowledged {
		t.Error("expected Acknowledged=true")
	}
	if srv.ConnectedSpokeCount() != 0 {
		t.Errorf("ConnectedSpokeCount = %d, want 0", srv.ConnectedSpokeCount())
	}

	// Verify heartbeat now fails.
	_, err = srv.Heartbeat(ctx, &hubv1.HeartbeatRequest{SpokeId: "spoke-1"})
	assertGRPCCode(t, err, codes.NotFound)
}

// ---------------------------------------------------------------------------
// ReportCaptureStatus
// ---------------------------------------------------------------------------

func TestReportCaptureStatus_EmptySpokeID(t *testing.T) {
	srv := newTestServer()
	_, err := srv.ReportCaptureStatus(context.Background(), &hubv1.ReportCaptureStatusRequest{
		Statuses: []*hubv1.CaptureStatusSummary{{CaptureName: "c", CaptureNamespace: "n"}},
	})
	assertGRPCCode(t, err, codes.InvalidArgument)
}

func TestReportCaptureStatus_EmptyStatuses(t *testing.T) {
	srv := newTestServer()
	mustRegister(t, srv, "spoke-1", "c")

	_, err := srv.ReportCaptureStatus(context.Background(), &hubv1.ReportCaptureStatusRequest{
		SpokeId:  "spoke-1",
		Statuses: nil,
	})
	assertGRPCCode(t, err, codes.InvalidArgument)
}

func TestReportCaptureStatus_UnregisteredSpoke(t *testing.T) {
	srv := newTestServer()
	_, err := srv.ReportCaptureStatus(context.Background(), &hubv1.ReportCaptureStatusRequest{
		SpokeId:  "unknown",
		Statuses: []*hubv1.CaptureStatusSummary{{CaptureName: "c", CaptureNamespace: "n"}},
	})
	assertGRPCCode(t, err, codes.NotFound)
}

func TestReportCaptureStatus_ValidUpdatesCaptures(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "spoke-1", "c")

	resp, err := srv.ReportCaptureStatus(ctx, &hubv1.ReportCaptureStatusRequest{
		SpokeId: "spoke-1",
		Statuses: []*hubv1.CaptureStatusSummary{
			{CaptureName: "cap-a", CaptureNamespace: "ns-1", Phase: hubv1.CapturePhase_CAPTURE_PHASE_ACTIVE, CapturedRequests: 42},
			{CaptureName: "cap-b", CaptureNamespace: "ns-1", Phase: hubv1.CapturePhase_CAPTURE_PHASE_PENDING},
		},
	})
	if err != nil {
		t.Fatalf("ReportCaptureStatus failed: %v", err)
	}
	if !resp.Acknowledged {
		t.Error("expected Acknowledged=true")
	}

	// ActiveCaptureCount should reflect the two captures.
	if count := srv.ActiveCaptureCount(); count != 2 {
		t.Errorf("ActiveCaptureCount = %d, want 2", count)
	}

	// Verify the spoke's info.ActiveCaptures was updated.
	srv.mu.RLock()
	ac := srv.spokes["spoke-1"].info.ActiveCaptures
	srv.mu.RUnlock()
	if ac != 2 {
		t.Errorf("spoke info ActiveCaptures = %d, want 2", ac)
	}
}

func TestReportCaptureStatus_OverwritesExistingCapture(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "spoke-1", "c")

	mustReportCaptures(t, srv, "spoke-1", []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-a", CaptureNamespace: "ns-1", CapturedRequests: 10},
	})

	// Report updated status for the same capture.
	mustReportCaptures(t, srv, "spoke-1", []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-a", CaptureNamespace: "ns-1", CapturedRequests: 99},
	})

	// Should still be 1 capture, not 2.
	if count := srv.ActiveCaptureCount(); count != 1 {
		t.Errorf("ActiveCaptureCount = %d, want 1", count)
	}

	// Verify the updated value.
	resp, err := srv.GetCaptureStatus(ctx, &hubv1.GetCaptureStatusRequest{
		CaptureName: "cap-a", CaptureNamespace: "ns-1",
	})
	if err != nil {
		t.Fatalf("GetCaptureStatus failed: %v", err)
	}
	if resp.Capture.CapturedRequests != 99 {
		t.Errorf("CapturedRequests = %d, want 99", resp.Capture.CapturedRequests)
	}
}

// ---------------------------------------------------------------------------
// ListCaptures
// ---------------------------------------------------------------------------

func TestListCaptures_EmptyRegistry(t *testing.T) {
	srv := newTestServer()
	resp, err := srv.ListCaptures(context.Background(), &hubv1.ListCapturesRequest{})
	if err != nil {
		t.Fatalf("ListCaptures failed: %v", err)
	}
	if len(resp.Captures) != 0 {
		t.Errorf("expected 0 captures, got %d", len(resp.Captures))
	}
}

func TestListCaptures_AllCapturesAcrossSpokes(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "spoke-1", "c1")
	mustRegister(t, srv, "spoke-2", "c2")

	mustReportCaptures(t, srv, "spoke-1", []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-1", CaptureNamespace: "ns-1"},
		{CaptureName: "cap-2", CaptureNamespace: "ns-1"},
	})
	mustReportCaptures(t, srv, "spoke-2", []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-3", CaptureNamespace: "ns-2"},
	})

	resp, err := srv.ListCaptures(ctx, &hubv1.ListCapturesRequest{})
	if err != nil {
		t.Fatalf("ListCaptures failed: %v", err)
	}
	if len(resp.Captures) != 3 {
		t.Errorf("expected 3 captures, got %d", len(resp.Captures))
	}
}

func TestListCaptures_FilterBySpokeID(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "spoke-1", "c1")
	mustRegister(t, srv, "spoke-2", "c2")

	mustReportCaptures(t, srv, "spoke-1", []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-1", CaptureNamespace: "ns"},
	})
	mustReportCaptures(t, srv, "spoke-2", []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-2", CaptureNamespace: "ns"},
	})

	resp, err := srv.ListCaptures(ctx, &hubv1.ListCapturesRequest{SpokeId: "spoke-1"})
	if err != nil {
		t.Fatalf("ListCaptures failed: %v", err)
	}
	if len(resp.Captures) != 1 {
		t.Errorf("expected 1 capture, got %d", len(resp.Captures))
	}
	if resp.Captures[0].CaptureName != "cap-1" {
		t.Errorf("capture name = %q, want %q", resp.Captures[0].CaptureName, "cap-1")
	}
}

func TestListCaptures_FilterByNamespace(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "spoke-1", "c1")

	mustReportCaptures(t, srv, "spoke-1", []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-1", CaptureNamespace: "ns-alpha"},
		{CaptureName: "cap-2", CaptureNamespace: "ns-beta"},
		{CaptureName: "cap-3", CaptureNamespace: "ns-alpha"},
	})

	resp, err := srv.ListCaptures(ctx, &hubv1.ListCapturesRequest{Namespace: "ns-alpha"})
	if err != nil {
		t.Fatalf("ListCaptures failed: %v", err)
	}
	if len(resp.Captures) != 2 {
		t.Errorf("expected 2 captures in ns-alpha, got %d", len(resp.Captures))
	}
	for _, c := range resp.Captures {
		if c.CaptureNamespace != "ns-alpha" {
			t.Errorf("unexpected namespace %q in filtered results", c.CaptureNamespace)
		}
	}
}

func TestListCaptures_FilterBySpokeAndNamespace(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "spoke-1", "c1")
	mustRegister(t, srv, "spoke-2", "c2")

	mustReportCaptures(t, srv, "spoke-1", []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-1", CaptureNamespace: "ns-a"},
		{CaptureName: "cap-2", CaptureNamespace: "ns-b"},
	})
	mustReportCaptures(t, srv, "spoke-2", []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-3", CaptureNamespace: "ns-a"},
	})

	resp, err := srv.ListCaptures(ctx, &hubv1.ListCapturesRequest{SpokeId: "spoke-1", Namespace: "ns-a"})
	if err != nil {
		t.Fatalf("ListCaptures failed: %v", err)
	}
	if len(resp.Captures) != 1 {
		t.Errorf("expected 1 capture, got %d", len(resp.Captures))
	}
	if len(resp.Captures) == 1 && resp.Captures[0].CaptureName != "cap-1" {
		t.Errorf("capture name = %q, want %q", resp.Captures[0].CaptureName, "cap-1")
	}
}

// ---------------------------------------------------------------------------
// GetCaptureStatus
// ---------------------------------------------------------------------------

func TestGetCaptureStatus_MissingNameOrNamespace(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	// Missing both.
	_, err := srv.GetCaptureStatus(ctx, &hubv1.GetCaptureStatusRequest{})
	assertGRPCCode(t, err, codes.InvalidArgument)

	// Missing namespace.
	_, err = srv.GetCaptureStatus(ctx, &hubv1.GetCaptureStatusRequest{CaptureName: "cap-1"})
	assertGRPCCode(t, err, codes.InvalidArgument)

	// Missing name.
	_, err = srv.GetCaptureStatus(ctx, &hubv1.GetCaptureStatusRequest{CaptureNamespace: "ns-1"})
	assertGRPCCode(t, err, codes.InvalidArgument)
}

func TestGetCaptureStatus_NotFound(t *testing.T) {
	srv := newTestServer()
	_, err := srv.GetCaptureStatus(context.Background(), &hubv1.GetCaptureStatusRequest{
		CaptureName: "missing", CaptureNamespace: "ns",
	})
	assertGRPCCode(t, err, codes.NotFound)
}

func TestGetCaptureStatus_NotFoundOnSpecificSpoke(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "spoke-1", "c")

	_, err := srv.GetCaptureStatus(ctx, &hubv1.GetCaptureStatusRequest{
		CaptureName: "missing", CaptureNamespace: "ns", SpokeId: "spoke-1",
	})
	assertGRPCCode(t, err, codes.NotFound)
}

func TestGetCaptureStatus_NotFoundSpokeNotRegistered(t *testing.T) {
	srv := newTestServer()
	_, err := srv.GetCaptureStatus(context.Background(), &hubv1.GetCaptureStatusRequest{
		CaptureName: "cap-1", CaptureNamespace: "ns", SpokeId: "unknown-spoke",
	})
	assertGRPCCode(t, err, codes.NotFound)
}

func TestGetCaptureStatus_FindsBySpokeID(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "spoke-1", "c1")
	mustRegister(t, srv, "spoke-2", "c2")

	mustReportCaptures(t, srv, "spoke-1", []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-1", CaptureNamespace: "ns-1", CapturedRequests: 10},
	})
	mustReportCaptures(t, srv, "spoke-2", []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-1", CaptureNamespace: "ns-1", CapturedRequests: 20},
	})

	resp, err := srv.GetCaptureStatus(ctx, &hubv1.GetCaptureStatusRequest{
		CaptureName: "cap-1", CaptureNamespace: "ns-1", SpokeId: "spoke-2",
	})
	if err != nil {
		t.Fatalf("GetCaptureStatus failed: %v", err)
	}
	if resp.Capture.SpokeId != "spoke-2" {
		t.Errorf("SpokeId = %q, want %q", resp.Capture.SpokeId, "spoke-2")
	}
	if resp.Capture.CapturedRequests != 20 {
		t.Errorf("CapturedRequests = %d, want 20", resp.Capture.CapturedRequests)
	}
}

func TestGetCaptureStatus_FindsAcrossAllSpokes(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "spoke-1", "c1")

	mustReportCaptures(t, srv, "spoke-1", []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-1", CaptureNamespace: "ns-1", Phase: hubv1.CapturePhase_CAPTURE_PHASE_ACTIVE, CapturedRequests: 77},
	})

	resp, err := srv.GetCaptureStatus(ctx, &hubv1.GetCaptureStatusRequest{
		CaptureName: "cap-1", CaptureNamespace: "ns-1",
	})
	if err != nil {
		t.Fatalf("GetCaptureStatus failed: %v", err)
	}
	if resp.Capture.CapturedRequests != 77 {
		t.Errorf("CapturedRequests = %d, want 77", resp.Capture.CapturedRequests)
	}
	if resp.Capture.SpokeId != "spoke-1" {
		t.Errorf("SpokeId = %q, want %q", resp.Capture.SpokeId, "spoke-1")
	}
}

// ---------------------------------------------------------------------------
// ListSpokes
// ---------------------------------------------------------------------------

func TestListSpokes_EmptyRegistry(t *testing.T) {
	srv := newTestServer()
	resp, err := srv.ListSpokes(context.Background(), &hubv1.ListSpokesRequest{})
	if err != nil {
		t.Fatalf("ListSpokes failed: %v", err)
	}
	if len(resp.Spokes) != 0 {
		t.Errorf("expected 0 spokes, got %d", len(resp.Spokes))
	}
}

func TestListSpokes_ReturnsAllRegistered(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "spoke-1", "cluster-a")
	mustRegister(t, srv, "spoke-2", "cluster-b")

	resp, err := srv.ListSpokes(ctx, &hubv1.ListSpokesRequest{})
	if err != nil {
		t.Fatalf("ListSpokes failed: %v", err)
	}
	if len(resp.Spokes) != 2 {
		t.Errorf("expected 2 spokes, got %d", len(resp.Spokes))
	}

	for _, sp := range resp.Spokes {
		if sp.State != hubv1.SpokeState_SPOKE_STATE_CONNECTED {
			t.Errorf("spoke %s should be connected, got %v", sp.SpokeId, sp.State)
		}
	}
}

func TestListSpokes_MarksStaleAsDisconnected(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "spoke-1", "c1")
	mustRegister(t, srv, "spoke-2", "c2")

	// Backdate spoke-1's heartbeat beyond the timeout.
	srv.mu.Lock()
	srv.spokes["spoke-1"].lastHeartbeat = time.Now().Add(-2 * SpokeTimeout)
	srv.mu.Unlock()

	resp, err := srv.ListSpokes(ctx, &hubv1.ListSpokesRequest{})
	if err != nil {
		t.Fatalf("ListSpokes failed: %v", err)
	}

	stateByID := make(map[string]hubv1.SpokeState)
	for _, sp := range resp.Spokes {
		stateByID[sp.SpokeId] = sp.State
	}

	if stateByID["spoke-1"] != hubv1.SpokeState_SPOKE_STATE_DISCONNECTED {
		t.Errorf("spoke-1 should be disconnected, got %v", stateByID["spoke-1"])
	}
	if stateByID["spoke-2"] != hubv1.SpokeState_SPOKE_STATE_CONNECTED {
		t.Errorf("spoke-2 should be connected, got %v", stateByID["spoke-2"])
	}
}

// ---------------------------------------------------------------------------
// ConnectedSpokeCount
// ---------------------------------------------------------------------------

func TestConnectedSpokeCount_EmptyRegistry(t *testing.T) {
	srv := newTestServer()
	if count := srv.ConnectedSpokeCount(); count != 0 {
		t.Errorf("ConnectedSpokeCount = %d, want 0", count)
	}
}

func TestConnectedSpokeCount_ExcludesTimedOut(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "spoke-1", "c1")
	mustRegister(t, srv, "spoke-2", "c2")
	mustRegister(t, srv, "spoke-3", "c3")

	// Timeout spoke-2 and spoke-3.
	srv.mu.Lock()
	srv.spokes["spoke-2"].lastHeartbeat = time.Now().Add(-2 * SpokeTimeout)
	srv.spokes["spoke-3"].lastHeartbeat = time.Now().Add(-2 * SpokeTimeout)
	srv.mu.Unlock()

	if count := srv.ConnectedSpokeCount(); count != 1 {
		t.Errorf("ConnectedSpokeCount = %d, want 1", count)
	}

	// After heartbeat, spoke-2 should be back.
	_, _ = srv.Heartbeat(ctx, &hubv1.HeartbeatRequest{SpokeId: "spoke-2"})
	if count := srv.ConnectedSpokeCount(); count != 2 {
		t.Errorf("ConnectedSpokeCount = %d, want 2 after heartbeat", count)
	}
}

// ---------------------------------------------------------------------------
// ActiveCaptureCount
// ---------------------------------------------------------------------------

func TestActiveCaptureCount_EmptyRegistry(t *testing.T) {
	srv := newTestServer()
	if count := srv.ActiveCaptureCount(); count != 0 {
		t.Errorf("ActiveCaptureCount = %d, want 0", count)
	}
}

func TestActiveCaptureCount_SumsAcrossSpokes(t *testing.T) {
	srv := newTestServer()

	mustRegister(t, srv, "spoke-1", "c1")
	mustRegister(t, srv, "spoke-2", "c2")

	mustReportCaptures(t, srv, "spoke-1", []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-1", CaptureNamespace: "ns-1"},
		{CaptureName: "cap-2", CaptureNamespace: "ns-1"},
	})
	mustReportCaptures(t, srv, "spoke-2", []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-3", CaptureNamespace: "ns-2"},
	})

	if count := srv.ActiveCaptureCount(); count != 3 {
		t.Errorf("ActiveCaptureCount = %d, want 3", count)
	}
}

func TestActiveCaptureCount_RegisteredSpokeWithNoCaptures(t *testing.T) {
	srv := newTestServer()
	mustRegister(t, srv, "spoke-1", "c1")

	if count := srv.ActiveCaptureCount(); count != 0 {
		t.Errorf("ActiveCaptureCount = %d, want 0", count)
	}
}

// ---------------------------------------------------------------------------
// SpokeStatuses
// ---------------------------------------------------------------------------

func TestSpokeStatuses_EmptyRegistry(t *testing.T) {
	srv := newTestServer()
	statuses := srv.SpokeStatuses()
	if len(statuses) != 0 {
		t.Errorf("expected 0 statuses, got %d", len(statuses))
	}
}

func TestSpokeStatuses_ReturnsSnapshot(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "spoke-1", "cluster-a")

	_, _ = srv.Heartbeat(ctx, &hubv1.HeartbeatRequest{
		SpokeId:        "spoke-1",
		ActiveCaptures: 3,
	})

	snapshots := srv.SpokeStatuses()
	if len(snapshots) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snapshots))
	}
	if snapshots[0].Name != "cluster-a" {
		t.Errorf("Name = %q, want %q", snapshots[0].Name, "cluster-a")
	}
	if snapshots[0].ActiveCaptures != 3 {
		t.Errorf("ActiveCaptures = %d, want 3", snapshots[0].ActiveCaptures)
	}
	if snapshots[0].LastHeartbeat.IsZero() {
		t.Error("LastHeartbeat should not be zero")
	}
}

func TestSpokeStatuses_MultipleSpokes(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "spoke-1", "cluster-a")
	mustRegister(t, srv, "spoke-2", "cluster-b")

	_, _ = srv.Heartbeat(ctx, &hubv1.HeartbeatRequest{SpokeId: "spoke-1", ActiveCaptures: 2})
	_, _ = srv.Heartbeat(ctx, &hubv1.HeartbeatRequest{SpokeId: "spoke-2", ActiveCaptures: 5})

	snapshots := srv.SpokeStatuses()
	if len(snapshots) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(snapshots))
	}

	byName := make(map[string]SpokeStatusSnapshot)
	for _, s := range snapshots {
		byName[s.Name] = s
	}

	if byName["cluster-a"].ActiveCaptures != 2 {
		t.Errorf("cluster-a ActiveCaptures = %d, want 2", byName["cluster-a"].ActiveCaptures)
	}
	if byName["cluster-b"].ActiveCaptures != 5 {
		t.Errorf("cluster-b ActiveCaptures = %d, want 5", byName["cluster-b"].ActiveCaptures)
	}
}
