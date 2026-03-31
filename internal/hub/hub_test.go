package hub

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	hubv1 "github.com/kapture-io/kapture/proto/hub/v1"
)

func newTestServer() *Server {
	return NewServer(":0", "")
}

// helper to register a agent and fail the test on error.
func mustRegister(t *testing.T, srv *Server, agentID, clusterName string) {
	t.Helper()
	_, err := srv.RegisterAgent(context.Background(), &hubv1.RegisterAgentRequest{
		AgentId:     agentID,
		ClusterName: clusterName,
	})
	if err != nil {
		t.Fatalf("RegisterAgent(%q) failed: %v", agentID, err)
	}
}

// helper to report captures and fail the test on error.
func mustReportCaptures(t *testing.T, srv *Server, agentID string, statuses []*hubv1.CaptureStatusSummary) {
	t.Helper()
	for _, status := range statuses {
		if status.Phase == hubv1.CapturePhase_CAPTURE_PHASE_UNSPECIFIED {
			status.Phase = hubv1.CapturePhase_CAPTURE_PHASE_ACTIVE
		}
	}
	_, err := srv.ReportCaptureStatus(context.Background(), &hubv1.ReportCaptureStatusRequest{
		AgentId:  agentID,
		Statuses: statuses,
	})
	if err != nil {
		t.Fatalf("ReportCaptureStatus(%q) failed: %v", agentID, err)
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
// RegisterAgent
// ---------------------------------------------------------------------------

func TestRegisterAgent_EmptyAgentID(t *testing.T) {
	srv := newTestServer()
	_, err := srv.RegisterAgent(context.Background(), &hubv1.RegisterAgentRequest{})
	assertGRPCCode(t, err, codes.InvalidArgument)
}

func TestRegisterAgent_ValidRegistration(t *testing.T) {
	srv := newTestServer()
	resp, err := srv.RegisterAgent(context.Background(), &hubv1.RegisterAgentRequest{
		AgentId:     "agent-1",
		ClusterName: "cluster-a",
	})
	if err != nil {
		t.Fatalf("RegisterAgent failed: %v", err)
	}
	if !resp.Accepted {
		t.Error("expected Accepted=true")
	}
	if resp.HeartbeatIntervalSeconds != DefaultHeartbeatInterval {
		t.Errorf("HeartbeatIntervalSeconds = %d, want %d", resp.HeartbeatIntervalSeconds, DefaultHeartbeatInterval)
	}
	if srv.ConnectedAgentCount() != 1 {
		t.Errorf("ConnectedAgentCount = %d, want 1", srv.ConnectedAgentCount())
	}
}

func TestRegisterAgent_ReRegistrationOverwrites(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "agent-1", "cluster-old")

	// Report a capture so the entry has state.
	mustReportCaptures(t, srv, "agent-1", []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-1", CaptureNamespace: "ns-1"},
	})
	if srv.ActiveCaptureCount() != 1 {
		t.Fatalf("expected 1 capture before re-registration, got %d", srv.ActiveCaptureCount())
	}

	// Re-register same agent with different cluster name.
	resp, err := srv.RegisterAgent(ctx, &hubv1.RegisterAgentRequest{
		AgentId:     "agent-1",
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

	// Only one agent should exist.
	if srv.ConnectedAgentCount() != 1 {
		t.Errorf("ConnectedAgentCount = %d, want 1", srv.ConnectedAgentCount())
	}

	// Verify new cluster name via AgentStatuses.
	statuses := srv.AgentStatuses()
	if len(statuses) != 1 {
		t.Fatalf("expected 1 agent status, got %d", len(statuses))
	}
	if statuses[0].Name != "cluster-new" {
		t.Errorf("cluster name = %q, want %q", statuses[0].Name, "cluster-new")
	}
}

// ---------------------------------------------------------------------------
// Heartbeat
// ---------------------------------------------------------------------------

func TestHeartbeat_EmptyAgentID(t *testing.T) {
	srv := newTestServer()
	_, err := srv.Heartbeat(context.Background(), &hubv1.HeartbeatRequest{})
	assertGRPCCode(t, err, codes.InvalidArgument)
}

func TestHeartbeat_UnregisteredAgent(t *testing.T) {
	srv := newTestServer()
	_, err := srv.Heartbeat(context.Background(), &hubv1.HeartbeatRequest{AgentId: "unknown"})
	assertGRPCCode(t, err, codes.NotFound)
}

func TestHeartbeat_UpdatesLastHeartbeat(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "agent-1", "c")

	// Backdate the heartbeat.
	srv.mu.Lock()
	srv.agents["agent-1"].lastHeartbeat = time.Now().Add(-time.Minute)
	srv.mu.Unlock()

	before := time.Now()
	resp, err := srv.Heartbeat(ctx, &hubv1.HeartbeatRequest{AgentId: "agent-1"})
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}
	if !resp.Acknowledged {
		t.Error("expected Acknowledged=true")
	}

	srv.mu.RLock()
	lastHB := srv.agents["agent-1"].lastHeartbeat
	srv.mu.RUnlock()

	if lastHB.Before(before) {
		t.Errorf("lastHeartbeat (%v) should be >= %v", lastHB, before)
	}
}

func TestHeartbeat_UpdatesActiveCapturesAndSummaries(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "agent-1", "c")

	resp, err := srv.Heartbeat(ctx, &hubv1.HeartbeatRequest{
		AgentId:        "agent-1",
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

	// Verify ActiveCaptures was updated on the agent info.
	srv.mu.RLock()
	ac := srv.agents["agent-1"].info.ActiveCaptures
	numCaptures := len(srv.agents["agent-1"].captures)
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
// DeregisterAgent
// ---------------------------------------------------------------------------

func TestDeregisterAgent_EmptyAgentID(t *testing.T) {
	srv := newTestServer()
	_, err := srv.DeregisterAgent(context.Background(), &hubv1.DeregisterAgentRequest{})
	assertGRPCCode(t, err, codes.InvalidArgument)
}

func TestDeregisterAgent_UnregisteredAgent(t *testing.T) {
	srv := newTestServer()
	_, err := srv.DeregisterAgent(context.Background(), &hubv1.DeregisterAgentRequest{AgentId: "nope"})
	assertGRPCCode(t, err, codes.NotFound)
}

func TestDeregisterAgent_Valid(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "agent-1", "c")

	resp, err := srv.DeregisterAgent(ctx, &hubv1.DeregisterAgentRequest{AgentId: "agent-1"})
	if err != nil {
		t.Fatalf("DeregisterAgent failed: %v", err)
	}
	if !resp.Acknowledged {
		t.Error("expected Acknowledged=true")
	}
	if srv.ConnectedAgentCount() != 0 {
		t.Errorf("ConnectedAgentCount = %d, want 0", srv.ConnectedAgentCount())
	}

	// Verify heartbeat now fails.
	_, err = srv.Heartbeat(ctx, &hubv1.HeartbeatRequest{AgentId: "agent-1"})
	assertGRPCCode(t, err, codes.NotFound)
}

// ---------------------------------------------------------------------------
// ReportCaptureStatus
// ---------------------------------------------------------------------------

func TestReportCaptureStatus_EmptyAgentID(t *testing.T) {
	srv := newTestServer()
	_, err := srv.ReportCaptureStatus(context.Background(), &hubv1.ReportCaptureStatusRequest{
		Statuses: []*hubv1.CaptureStatusSummary{{CaptureName: "c", CaptureNamespace: "n"}},
	})
	assertGRPCCode(t, err, codes.InvalidArgument)
}

func TestReportCaptureStatus_EmptyStatuses(t *testing.T) {
	srv := newTestServer()
	mustRegister(t, srv, "agent-1", "c")

	_, err := srv.ReportCaptureStatus(context.Background(), &hubv1.ReportCaptureStatusRequest{
		AgentId:  "agent-1",
		Statuses: nil,
	})
	assertGRPCCode(t, err, codes.InvalidArgument)
}

func TestReportCaptureStatus_UnregisteredAgent(t *testing.T) {
	srv := newTestServer()
	_, err := srv.ReportCaptureStatus(context.Background(), &hubv1.ReportCaptureStatusRequest{
		AgentId:  "unknown",
		Statuses: []*hubv1.CaptureStatusSummary{{CaptureName: "c", CaptureNamespace: "n"}},
	})
	assertGRPCCode(t, err, codes.NotFound)
}

func TestReportCaptureStatus_ValidUpdatesCaptures(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "agent-1", "c")

	resp, err := srv.ReportCaptureStatus(ctx, &hubv1.ReportCaptureStatusRequest{
		AgentId: "agent-1",
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

	// Verify the agent's info.ActiveCaptures was updated.
	srv.mu.RLock()
	ac := srv.agents["agent-1"].info.ActiveCaptures
	srv.mu.RUnlock()
	if ac != 2 {
		t.Errorf("agent info ActiveCaptures = %d, want 2", ac)
	}
}

func TestReportCaptureStatus_OverwritesExistingCapture(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "agent-1", "c")

	mustReportCaptures(t, srv, "agent-1", []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-a", CaptureNamespace: "ns-1", CapturedRequests: 10},
	})

	// Report updated status for the same capture.
	mustReportCaptures(t, srv, "agent-1", []*hubv1.CaptureStatusSummary{
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

func TestListCaptures_AllCapturesAcrossAgents(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "agent-1", "c1")
	mustRegister(t, srv, "agent-2", "c2")

	mustReportCaptures(t, srv, "agent-1", []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-1", CaptureNamespace: "ns-1"},
		{CaptureName: "cap-2", CaptureNamespace: "ns-1"},
	})
	mustReportCaptures(t, srv, "agent-2", []*hubv1.CaptureStatusSummary{
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

func TestListCaptures_FilterByAgentID(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "agent-1", "c1")
	mustRegister(t, srv, "agent-2", "c2")

	mustReportCaptures(t, srv, "agent-1", []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-1", CaptureNamespace: "ns"},
	})
	mustReportCaptures(t, srv, "agent-2", []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-2", CaptureNamespace: "ns"},
	})

	resp, err := srv.ListCaptures(ctx, &hubv1.ListCapturesRequest{AgentId: "agent-1"})
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

	mustRegister(t, srv, "agent-1", "c1")

	mustReportCaptures(t, srv, "agent-1", []*hubv1.CaptureStatusSummary{
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

func TestListCaptures_FilterByAgentAndNamespace(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "agent-1", "c1")
	mustRegister(t, srv, "agent-2", "c2")

	mustReportCaptures(t, srv, "agent-1", []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-1", CaptureNamespace: "ns-a"},
		{CaptureName: "cap-2", CaptureNamespace: "ns-b"},
	})
	mustReportCaptures(t, srv, "agent-2", []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-3", CaptureNamespace: "ns-a"},
	})

	resp, err := srv.ListCaptures(ctx, &hubv1.ListCapturesRequest{AgentId: "agent-1", Namespace: "ns-a"})
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

func TestGetCaptureStatus_NotFoundOnSpecificAgent(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "agent-1", "c")

	_, err := srv.GetCaptureStatus(ctx, &hubv1.GetCaptureStatusRequest{
		CaptureName: "missing", CaptureNamespace: "ns", AgentId: "agent-1",
	})
	assertGRPCCode(t, err, codes.NotFound)
}

func TestGetCaptureStatus_NotFoundAgentNotRegistered(t *testing.T) {
	srv := newTestServer()
	_, err := srv.GetCaptureStatus(context.Background(), &hubv1.GetCaptureStatusRequest{
		CaptureName: "cap-1", CaptureNamespace: "ns", AgentId: "unknown-agent",
	})
	assertGRPCCode(t, err, codes.NotFound)
}

func TestGetCaptureStatus_FindsByAgentID(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "agent-1", "c1")
	mustRegister(t, srv, "agent-2", "c2")

	mustReportCaptures(t, srv, "agent-1", []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-1", CaptureNamespace: "ns-1", CapturedRequests: 10},
	})
	mustReportCaptures(t, srv, "agent-2", []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-1", CaptureNamespace: "ns-1", CapturedRequests: 20},
	})

	resp, err := srv.GetCaptureStatus(ctx, &hubv1.GetCaptureStatusRequest{
		CaptureName: "cap-1", CaptureNamespace: "ns-1", AgentId: "agent-2",
	})
	if err != nil {
		t.Fatalf("GetCaptureStatus failed: %v", err)
	}
	if resp.Capture.AgentId != "agent-2" {
		t.Errorf("AgentId = %q, want %q", resp.Capture.AgentId, "agent-2")
	}
	if resp.Capture.CapturedRequests != 20 {
		t.Errorf("CapturedRequests = %d, want 20", resp.Capture.CapturedRequests)
	}
}

func TestGetCaptureStatus_FindsAcrossAllAgents(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "agent-1", "c1")

	mustReportCaptures(t, srv, "agent-1", []*hubv1.CaptureStatusSummary{
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
	if resp.Capture.AgentId != "agent-1" {
		t.Errorf("AgentId = %q, want %q", resp.Capture.AgentId, "agent-1")
	}
}

// ---------------------------------------------------------------------------
// ListAgents
// ---------------------------------------------------------------------------

func TestListAgents_EmptyRegistry(t *testing.T) {
	srv := newTestServer()
	resp, err := srv.ListAgents(context.Background(), &hubv1.ListAgentsRequest{})
	if err != nil {
		t.Fatalf("ListAgents failed: %v", err)
	}
	if len(resp.Agents) != 0 {
		t.Errorf("expected 0 agents, got %d", len(resp.Agents))
	}
}

func TestListAgents_ReturnsAllRegistered(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "agent-1", "cluster-a")
	mustRegister(t, srv, "agent-2", "cluster-b")

	resp, err := srv.ListAgents(ctx, &hubv1.ListAgentsRequest{})
	if err != nil {
		t.Fatalf("ListAgents failed: %v", err)
	}
	if len(resp.Agents) != 2 {
		t.Errorf("expected 2 agents, got %d", len(resp.Agents))
	}

	for _, sp := range resp.Agents {
		if sp.State != hubv1.AgentState_AGENT_STATE_CONNECTED {
			t.Errorf("agent %s should be connected, got %v", sp.AgentId, sp.State)
		}
	}
}

func TestListAgents_MarksStaleAsDisconnected(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "agent-1", "c1")
	mustRegister(t, srv, "agent-2", "c2")

	// Backdate agent-1's heartbeat beyond the timeout.
	srv.mu.Lock()
	srv.agents["agent-1"].lastHeartbeat = time.Now().Add(-2 * AgentTimeout)
	srv.mu.Unlock()

	resp, err := srv.ListAgents(ctx, &hubv1.ListAgentsRequest{})
	if err != nil {
		t.Fatalf("ListAgents failed: %v", err)
	}

	stateByID := make(map[string]hubv1.AgentState)
	for _, sp := range resp.Agents {
		stateByID[sp.AgentId] = sp.State
	}

	if stateByID["agent-1"] != hubv1.AgentState_AGENT_STATE_DISCONNECTED {
		t.Errorf("agent-1 should be disconnected, got %v", stateByID["agent-1"])
	}
	if stateByID["agent-2"] != hubv1.AgentState_AGENT_STATE_CONNECTED {
		t.Errorf("agent-2 should be connected, got %v", stateByID["agent-2"])
	}
}

// ---------------------------------------------------------------------------
// ConnectedAgentCount
// ---------------------------------------------------------------------------

func TestConnectedAgentCount_EmptyRegistry(t *testing.T) {
	srv := newTestServer()
	if count := srv.ConnectedAgentCount(); count != 0 {
		t.Errorf("ConnectedAgentCount = %d, want 0", count)
	}
}

func TestConnectedAgentCount_ExcludesTimedOut(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "agent-1", "c1")
	mustRegister(t, srv, "agent-2", "c2")
	mustRegister(t, srv, "agent-3", "c3")

	// Timeout agent-2 and agent-3.
	srv.mu.Lock()
	srv.agents["agent-2"].lastHeartbeat = time.Now().Add(-2 * AgentTimeout)
	srv.agents["agent-3"].lastHeartbeat = time.Now().Add(-2 * AgentTimeout)
	srv.mu.Unlock()

	if count := srv.ConnectedAgentCount(); count != 1 {
		t.Errorf("ConnectedAgentCount = %d, want 1", count)
	}

	// After heartbeat, agent-2 should be back.
	_, _ = srv.Heartbeat(ctx, &hubv1.HeartbeatRequest{AgentId: "agent-2"})
	if count := srv.ConnectedAgentCount(); count != 2 {
		t.Errorf("ConnectedAgentCount = %d, want 2 after heartbeat", count)
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

func TestActiveCaptureCount_SumsAcrossAgents(t *testing.T) {
	srv := newTestServer()

	mustRegister(t, srv, "agent-1", "c1")
	mustRegister(t, srv, "agent-2", "c2")

	mustReportCaptures(t, srv, "agent-1", []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-1", CaptureNamespace: "ns-1"},
		{CaptureName: "cap-2", CaptureNamespace: "ns-1"},
	})
	mustReportCaptures(t, srv, "agent-2", []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-3", CaptureNamespace: "ns-2"},
	})

	if count := srv.ActiveCaptureCount(); count != 3 {
		t.Errorf("ActiveCaptureCount = %d, want 3", count)
	}
}

func TestActiveCaptureCount_RegisteredAgentWithNoCaptures(t *testing.T) {
	srv := newTestServer()
	mustRegister(t, srv, "agent-1", "c1")

	if count := srv.ActiveCaptureCount(); count != 0 {
		t.Errorf("ActiveCaptureCount = %d, want 0", count)
	}
}

// ---------------------------------------------------------------------------
// AgentStatuses
// ---------------------------------------------------------------------------

func TestAgentStatuses_EmptyRegistry(t *testing.T) {
	srv := newTestServer()
	statuses := srv.AgentStatuses()
	if len(statuses) != 0 {
		t.Errorf("expected 0 statuses, got %d", len(statuses))
	}
}

func TestAgentStatuses_ReturnsSnapshot(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "agent-1", "cluster-a")

	_, _ = srv.Heartbeat(ctx, &hubv1.HeartbeatRequest{
		AgentId:        "agent-1",
		ActiveCaptures: 3,
	})

	snapshots := srv.AgentStatuses()
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

func TestAgentStatuses_MultipleAgents(t *testing.T) {
	srv := newTestServer()
	ctx := context.Background()

	mustRegister(t, srv, "agent-1", "cluster-a")
	mustRegister(t, srv, "agent-2", "cluster-b")

	_, _ = srv.Heartbeat(ctx, &hubv1.HeartbeatRequest{AgentId: "agent-1", ActiveCaptures: 2})
	_, _ = srv.Heartbeat(ctx, &hubv1.HeartbeatRequest{AgentId: "agent-2", ActiveCaptures: 5})

	snapshots := srv.AgentStatuses()
	if len(snapshots) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(snapshots))
	}

	byName := make(map[string]AgentStatusSnapshot)
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
