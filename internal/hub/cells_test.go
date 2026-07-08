package hub

import (
	"context"
	"testing"

	hubv1 "github.com/kapture-io/kapture/proto/hub/v1"
)

func registerSpokeInCell(t *testing.T, s *Server, spokeID, cell string) {
	t.Helper()
	resp, err := s.RegisterSpoke(context.Background(), &hubv1.RegisterSpokeRequest{
		SpokeId:     spokeID,
		ClusterName: spokeID,
		Cell:        cell,
	})
	if err != nil {
		t.Fatalf("RegisterSpoke(%s): %v", spokeID, err)
	}
	if !resp.Accepted {
		t.Fatalf("RegisterSpoke(%s) rejected: %s", spokeID, resp.Message)
	}
}

func TestListCells_AggregatesSpokes(t *testing.T) {
	s := NewServer(":0")
	registerSpokeInCell(t, s, "spoke-a1", "cell-a")
	registerSpokeInCell(t, s, "spoke-a2", "cell-a")
	registerSpokeInCell(t, s, "spoke-b1", "cell-b")

	resp, err := s.ListCells(context.Background(), &hubv1.ListCellsRequest{})
	if err != nil {
		t.Fatalf("ListCells: %v", err)
	}
	if len(resp.Cells) != 2 {
		t.Fatalf("got %d cells, want 2", len(resp.Cells))
	}
	// Sorted by name: cell-a first.
	if resp.Cells[0].Name != "cell-a" || resp.Cells[0].TotalSpokes != 2 || resp.Cells[0].ConnectedSpokes != 2 {
		t.Errorf("cell-a aggregate wrong: %+v", resp.Cells[0])
	}
	if resp.Cells[1].Name != "cell-b" || resp.Cells[1].TotalSpokes != 1 {
		t.Errorf("cell-b aggregate wrong: %+v", resp.Cells[1])
	}
}

func TestListSpokes_FiltersByCell(t *testing.T) {
	s := NewServer(":0")
	registerSpokeInCell(t, s, "spoke-a1", "cell-a")
	registerSpokeInCell(t, s, "spoke-b1", "cell-b")

	resp, err := s.ListSpokes(context.Background(), &hubv1.ListSpokesRequest{Cell: "cell-a"})
	if err != nil {
		t.Fatalf("ListSpokes: %v", err)
	}
	if len(resp.Spokes) != 1 || resp.Spokes[0].SpokeId != "spoke-a1" {
		t.Fatalf("cell filter returned wrong spokes: %+v", resp.Spokes)
	}
	if resp.Spokes[0].Cell != "cell-a" {
		t.Errorf("spoke cell = %q, want cell-a", resp.Spokes[0].Cell)
	}
}

func TestSelectSpokes_CellFilterMaxAndDeterminism(t *testing.T) {
	s := NewServer(":0")
	registerSpokeInCell(t, s, "spoke-c", "cell-a")
	registerSpokeInCell(t, s, "spoke-a", "cell-a")
	registerSpokeInCell(t, s, "spoke-b", "cell-b")

	all := s.SelectSpokes(nil, 0)
	if len(all) != 3 {
		t.Fatalf("SelectSpokes(nil) returned %d spokes, want 3", len(all))
	}
	if all[0].SpokeID != "spoke-a" || all[1].SpokeID != "spoke-b" || all[2].SpokeID != "spoke-c" {
		t.Errorf("selection not sorted: %+v", all)
	}

	cellA := s.SelectSpokes([]string{"cell-a"}, 0)
	if len(cellA) != 2 {
		t.Fatalf("SelectSpokes(cell-a) returned %d spokes, want 2", len(cellA))
	}

	capped := s.SelectSpokes(nil, 1)
	if len(capped) != 1 || capped[0].SpokeID != "spoke-a" {
		t.Errorf("SelectSpokes(max=1) = %+v, want [spoke-a]", capped)
	}
}

func TestReportReplayStatus_AndReplayStatuses(t *testing.T) {
	s := NewServer(":0")
	registerSpokeInCell(t, s, "spoke-a", "cell-a")

	summary := &hubv1.ReplayStatusSummary{
		LoadTestName:      "lt",
		LoadTestNamespace: "default",
		ReplayName:        "lt-shard-0",
		ShardIndex:        0,
		ShardCount:        2,
		Phase:             hubv1.ReplayPhase_REPLAY_PHASE_RUNNING,
		SentRequests:      100,
	}
	if _, err := s.ReportReplayStatus(context.Background(), &hubv1.ReportReplayStatusRequest{
		SpokeId:  "spoke-a",
		Statuses: []*hubv1.ReplayStatusSummary{summary},
	}); err != nil {
		t.Fatalf("ReportReplayStatus: %v", err)
	}

	snaps := s.ReplayStatuses("default", "lt")
	if len(snaps) != 1 {
		t.Fatalf("ReplayStatuses returned %d entries, want 1", len(snaps))
	}
	if snaps[0].SpokeID != "spoke-a" || snaps[0].Cell != "cell-a" || snaps[0].Summary.SentRequests != 100 {
		t.Errorf("snapshot wrong: %+v", snaps[0])
	}

	if got := s.ActiveReplayCount(); got != 1 {
		t.Errorf("ActiveReplayCount = %d, want 1", got)
	}

	// Terminal phases stop counting as active.
	summary2 := &hubv1.ReplayStatusSummary{
		LoadTestName:      "lt",
		LoadTestNamespace: "default",
		ReplayName:        "lt-shard-0",
		Phase:             hubv1.ReplayPhase_REPLAY_PHASE_COMPLETED,
	}
	if _, err := s.ReportReplayStatus(context.Background(), &hubv1.ReportReplayStatusRequest{
		SpokeId:  "spoke-a",
		Statuses: []*hubv1.ReplayStatusSummary{summary2},
	}); err != nil {
		t.Fatalf("ReportReplayStatus (terminal): %v", err)
	}
	if got := s.ActiveReplayCount(); got != 0 {
		t.Errorf("ActiveReplayCount after completion = %d, want 0", got)
	}

	s.ClearReplayStatuses("default", "lt")
	if snaps := s.ReplayStatuses("default", "lt"); len(snaps) != 0 {
		t.Errorf("ClearReplayStatuses left %d entries", len(snaps))
	}
}

func TestHeartbeat_CarriesReplaySummaries(t *testing.T) {
	s := NewServer(":0")
	registerSpokeInCell(t, s, "spoke-a", "cell-a")

	_, err := s.Heartbeat(context.Background(), &hubv1.HeartbeatRequest{
		SpokeId: "spoke-a",
		ReplaySummaries: []*hubv1.ReplayStatusSummary{{
			LoadTestName:      "lt",
			LoadTestNamespace: "default",
			ReplayName:        "lt-shard-1",
			Phase:             hubv1.ReplayPhase_REPLAY_PHASE_RUNNING,
		}},
	})
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	if snaps := s.ReplayStatuses("default", "lt"); len(snaps) != 1 {
		t.Fatalf("heartbeat replay summaries not recorded: %d entries", len(snaps))
	}
}

func TestSendReplayDirective_ReachesWatcher(t *testing.T) {
	s := NewServer(":0")
	registerSpokeInCell(t, s, "spoke-a", "cell-a")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := newFakeDirectiveStream(ctx)
	watchDone := make(chan error, 1)
	go func() {
		watchDone <- s.WatchDirectives(&hubv1.WatchDirectivesRequest{SpokeId: "spoke-a"}, stream)
	}()

	directive := &hubv1.ReplayDirective{
		DirectiveId:       "d1",
		Action:            hubv1.ReplayAction_REPLAY_ACTION_START,
		LoadTestName:      "lt",
		LoadTestNamespace: "default",
	}
	if err := s.SendReplayDirective("spoke-a", directive); err != nil {
		t.Fatalf("SendReplayDirective: %v", err)
	}

	resp := <-stream.sent
	rd := resp.GetReplayDirective()
	if rd == nil {
		t.Fatal("streamed response has no replay directive")
	}
	if rd.DirectiveId != "d1" || rd.Action != hubv1.ReplayAction_REPLAY_ACTION_START {
		t.Errorf("streamed directive wrong: %+v", rd)
	}

	cancel()
	<-watchDone
}
