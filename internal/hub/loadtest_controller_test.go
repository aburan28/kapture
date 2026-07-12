package hub

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
	hubv1 "github.com/kapture-io/kapture/proto/hub/v1"
)

func newLoadTest(name string) *capturev1alpha1.CaptureLoadTest {
	return &capturev1alpha1.CaptureLoadTest{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: capturev1alpha1.CaptureLoadTestSpec{
			SourceRef:  capturev1alpha1.TrafficCaptureReference{Name: "orders"},
			StorageRef: capturev1alpha1.CaptureStorageReference{Name: "s3-store"},
			Target:     capturev1alpha1.ReplayTarget{Host: "staging.internal"},
		},
	}
}

func TestPlanDistribution_AssignsDenseShards(t *testing.T) {
	s := NewServer(":0")
	registerSpokeInCell(t, s, "spoke-a", "cell-a")
	registerSpokeInCell(t, s, "spoke-b", "cell-a")

	r := &CaptureLoadTestReconciler{}
	lt := newLoadTest("lt")
	workers := int32(3)
	lt.Spec.Distribution = &capturev1alpha1.LoadTestDistribution{
		WorkersPerSpoke: &workers,
	}

	assignments, err := r.planDistribution(s, lt)
	if err != nil {
		t.Fatalf("planDistribution: %v", err)
	}
	if len(assignments) != 2 {
		t.Fatalf("got %d assignments, want 2", len(assignments))
	}
	if countShards(assignments) != 6 {
		t.Errorf("total shards = %d, want 6", countShards(assignments))
	}

	// Shard indexes must be dense and unique: 0..5.
	seen := map[int32]bool{}
	for _, a := range assignments {
		if len(a.ShardIndexes) != 3 {
			t.Errorf("spoke %s got %d shards, want 3", a.SpokeID, len(a.ShardIndexes))
		}
		for _, idx := range a.ShardIndexes {
			if seen[idx] {
				t.Errorf("shard %d assigned twice", idx)
			}
			seen[idx] = true
		}
	}
	for i := int32(0); i < 6; i++ {
		if !seen[i] {
			t.Errorf("shard %d never assigned", i)
		}
	}
}

func TestPlanDistribution_RespectsCellsAndMaxSpokes(t *testing.T) {
	s := NewServer(":0")
	registerSpokeInCell(t, s, "spoke-a", "cell-a")
	registerSpokeInCell(t, s, "spoke-b", "cell-b")
	registerSpokeInCell(t, s, "spoke-c", "cell-b")

	r := &CaptureLoadTestReconciler{}
	lt := newLoadTest("lt")
	maxSpokes := int32(1)
	lt.Spec.Distribution = &capturev1alpha1.LoadTestDistribution{
		Cells:     []string{"cell-b"},
		MaxSpokes: &maxSpokes,
	}

	assignments, err := r.planDistribution(s, lt)
	if err != nil {
		t.Fatalf("planDistribution: %v", err)
	}
	if len(assignments) != 1 || assignments[0].Cell != "cell-b" {
		t.Fatalf("assignments = %+v, want single cell-b spoke", assignments)
	}
}

func TestPlanDistribution_NoSpokes(t *testing.T) {
	s := NewServer(":0")
	r := &CaptureLoadTestReconciler{}

	if _, err := r.planDistribution(s, newLoadTest("lt")); err == nil {
		t.Fatal("planDistribution with no spokes succeeded, want error")
	}
}

func TestSplitRate_SumsToAggregate(t *testing.T) {
	const totalRPS, shards = int32(1000), int32(7)
	var sum int32
	for i := int32(0); i < shards; i++ {
		sum += splitRate(totalRPS, shards, i)
	}
	if sum != totalRPS {
		t.Errorf("shard rates sum to %d, want %d", sum, totalRPS)
	}
	if got := splitRate(2, 8, 5); got != 1 {
		t.Errorf("splitRate clamps to minimum 1, got %d", got)
	}
}

func TestBuildStartDirective_MapsSpec(t *testing.T) {
	r := &CaptureLoadTestReconciler{}
	lt := newLoadTest("lt")
	lt.Status.TotalShards = 4
	port := int32(8443)
	tls := true
	rps := int32(400)
	lt.Spec.Target.Port = &port
	lt.Spec.Target.TLS = &tls
	lt.Spec.Rate = &capturev1alpha1.ReplayRateConfig{
		Mode:              capturev1alpha1.ReplayRateModeConstant,
		RequestsPerSecond: &rps,
	}

	d := r.buildStartDirective(lt, 2)
	if d.Action != hubv1.ReplayAction_REPLAY_ACTION_START {
		t.Errorf("action = %v, want START", d.Action)
	}
	if d.LoadTestName != "lt" || d.LoadTestNamespace != "default" {
		t.Errorf("load test identity wrong: %s/%s", d.LoadTestNamespace, d.LoadTestName)
	}
	spec := d.Spec
	if spec.SourceCapture != "orders" || spec.StorageRefName != "s3-store" {
		t.Errorf("source/storage wrong: %+v", spec)
	}
	if spec.TargetHost != "staging.internal" || spec.TargetPort != 8443 || !spec.TargetTls {
		t.Errorf("target wrong: %+v", spec)
	}
	if spec.Shard.Index != 2 || spec.Shard.Count != 4 {
		t.Errorf("shard wrong: %+v", spec.Shard)
	}
	if spec.Rate.Mode != string(capturev1alpha1.ReplayRateModeConstant) || spec.Rate.RequestsPerSecond != 100 {
		t.Errorf("rate wrong: %+v (want 400/4=100 rps)", spec.Rate)
	}
}

func TestBuildStartDirective_DefaultsToOriginalTiming(t *testing.T) {
	r := &CaptureLoadTestReconciler{}
	lt := newLoadTest("lt")
	lt.Status.TotalShards = 2

	d := r.buildStartDirective(lt, 0)
	if d.Spec.Rate.Mode != string(capturev1alpha1.ReplayRateModeOriginalTiming) {
		t.Errorf("default rate mode = %q, want OriginalTiming", d.Spec.Rate.Mode)
	}
}

func TestAggregate_RollsUpAndTransitionsPhase(t *testing.T) {
	r := &CaptureLoadTestReconciler{}
	lt := newLoadTest("lt")
	lt.Status.TotalShards = 2
	lt.Status.Assignments = []capturev1alpha1.LoadTestAssignment{
		{SpokeID: "spoke-a", Cell: "cell-a", ShardIndexes: []int32{0}},
		{SpokeID: "spoke-b", Cell: "cell-b", ShardIndexes: []int32{1}},
	}

	running := []ReplayStatusSnapshot{
		{SpokeID: "spoke-a", Cell: "cell-a", Summary: &hubv1.ReplayStatusSummary{
			ShardIndex: 0, Phase: hubv1.ReplayPhase_REPLAY_PHASE_RUNNING,
			SentRequests: 500, FailedRequests: 5, AchievedRps: 50,
		}},
		{SpokeID: "spoke-b", Cell: "cell-b", Summary: &hubv1.ReplayStatusSummary{
			ShardIndex: 1, Phase: hubv1.ReplayPhase_REPLAY_PHASE_RUNNING,
			SentRequests: 400, FailedRequests: 10, AchievedRps: 40,
		}},
	}
	r.aggregate(lt, running)

	if lt.Status.Phase != capturev1alpha1.CaptureLoadTestPhaseRunning {
		t.Errorf("phase = %s, want Running", lt.Status.Phase)
	}
	if lt.Status.SentRequests != 900 || lt.Status.FailedRequests != 15 {
		t.Errorf("totals wrong: sent=%d failed=%d", lt.Status.SentRequests, lt.Status.FailedRequests)
	}
	if lt.Status.AchievedRPS == nil || *lt.Status.AchievedRPS != "90.0" {
		t.Errorf("achievedRPS = %v, want 90.0", lt.Status.AchievedRPS)
	}
	if len(lt.Status.Cells) != 2 {
		t.Fatalf("got %d cell statuses, want 2", len(lt.Status.Cells))
	}
	if lt.Status.Cells[0].Name != "cell-a" || lt.Status.Cells[0].SentRequests != 500 {
		t.Errorf("cell-a rollup wrong: %+v", lt.Status.Cells[0])
	}

	// All shards completed → Completed with completion time.
	done := []ReplayStatusSnapshot{
		{SpokeID: "spoke-a", Cell: "cell-a", Summary: &hubv1.ReplayStatusSummary{
			ShardIndex: 0, Phase: hubv1.ReplayPhase_REPLAY_PHASE_COMPLETED, SentRequests: 1000,
		}},
		{SpokeID: "spoke-b", Cell: "cell-b", Summary: &hubv1.ReplayStatusSummary{
			ShardIndex: 1, Phase: hubv1.ReplayPhase_REPLAY_PHASE_COMPLETED, SentRequests: 1000,
		}},
	}
	r.aggregate(lt, done)
	if lt.Status.Phase != capturev1alpha1.CaptureLoadTestPhaseCompleted {
		t.Errorf("phase = %s, want Completed", lt.Status.Phase)
	}
	if lt.Status.CompletionTime == nil {
		t.Error("completion time not set")
	}

	// One failed shard among terminal shards → Failed.
	lt2 := newLoadTest("lt2")
	lt2.Status.TotalShards = 2
	lt2.Status.Assignments = lt.Status.Assignments
	mixed := []ReplayStatusSnapshot{
		{SpokeID: "spoke-a", Cell: "cell-a", Summary: &hubv1.ReplayStatusSummary{
			ShardIndex: 0, Phase: hubv1.ReplayPhase_REPLAY_PHASE_COMPLETED,
		}},
		{SpokeID: "spoke-b", Cell: "cell-b", Summary: &hubv1.ReplayStatusSummary{
			ShardIndex: 1, Phase: hubv1.ReplayPhase_REPLAY_PHASE_FAILED,
		}},
	}
	r.aggregate(lt2, mixed)
	if lt2.Status.Phase != capturev1alpha1.CaptureLoadTestPhaseFailed {
		t.Errorf("phase = %s, want Failed", lt2.Status.Phase)
	}
}

func TestShouldAbort(t *testing.T) {
	r := &CaptureLoadTestReconciler{}

	// No policy: never aborts.
	lt := newLoadTest("lt")
	lt.Status.Phase = capturev1alpha1.CaptureLoadTestPhaseRunning
	if reason := r.shouldAbort(lt); reason != "" {
		t.Errorf("abort without policy: %q", reason)
	}

	// Error rate above threshold.
	errPercent := int32(5)
	lt.Spec.Abort = &capturev1alpha1.LoadTestAbortPolicy{ErrorPercent: &errPercent}
	lt.Status.SentRequests = 900
	lt.Status.FailedRequests = 100 // 10% of 1000 attempted
	if reason := r.shouldAbort(lt); reason == "" {
		t.Error("expected abort for 10% error rate over 5% threshold")
	}

	// Below the minimum sample size the error rate is not evaluated.
	minSample := int64(10000)
	lt.Spec.Abort.MinSampleRequests = &minSample
	if reason := r.shouldAbort(lt); reason != "" {
		t.Errorf("aborted below min sample size: %q", reason)
	}

	// Max duration exceeded.
	lt2 := newLoadTest("lt2")
	lt2.Status.Phase = capturev1alpha1.CaptureLoadTestPhaseRunning
	started := metav1.NewTime(time.Now().Add(-2 * time.Hour))
	lt2.Status.StartTime = &started
	lt2.Spec.Abort = &capturev1alpha1.LoadTestAbortPolicy{
		MaxDuration: &metav1.Duration{Duration: time.Hour},
	}
	if reason := r.shouldAbort(lt2); reason == "" {
		t.Error("expected abort for exceeded maxDuration")
	}

	// Terminal phases never abort.
	lt2.Status.Phase = capturev1alpha1.CaptureLoadTestPhaseCompleted
	if reason := r.shouldAbort(lt2); reason != "" {
		t.Errorf("aborted terminal load test: %q", reason)
	}
}

// TestStopAllShards_SendsToEveryAssignedSpoke verifies STOP fan-out reaches
// each spoke's directive stream.
func TestStopAllShards_SendsToEveryAssignedSpoke(t *testing.T) {
	s := NewServer(":0")
	registerSpokeInCell(t, s, "spoke-a", "cell-a")
	registerSpokeInCell(t, s, "spoke-b", "cell-a")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	streams := map[string]*fakeDirectiveStream{}
	for _, spoke := range []string{"spoke-a", "spoke-b"} {
		stream := newFakeDirectiveStream(ctx)
		streams[spoke] = stream
		go func(id string, st *fakeDirectiveStream) {
			_ = s.WatchDirectives(&hubv1.WatchDirectivesRequest{SpokeId: id}, st)
		}(spoke, stream)
	}

	r := &CaptureLoadTestReconciler{}
	lt := newLoadTest("lt")
	lt.Status.Assignments = []capturev1alpha1.LoadTestAssignment{
		{SpokeID: "spoke-a", ShardIndexes: []int32{0}},
		{SpokeID: "spoke-b", ShardIndexes: []int32{1}},
	}

	if !r.stopAllShards(s, lt, logr.Discard()) {
		t.Error("stopAllShards reported undelivered directives with healthy spokes")
	}

	for spoke, stream := range streams {
		select {
		case resp := <-stream.sent:
			rd := resp.GetReplayDirective()
			if rd == nil || rd.Action != hubv1.ReplayAction_REPLAY_ACTION_STOP {
				t.Errorf("spoke %s received wrong directive: %+v", spoke, resp)
			}
		case <-time.After(2 * time.Second):
			t.Errorf("spoke %s never received stop directive", spoke)
		}
	}
}

// TestStopAllShards_ReportsUndeliveredOnFullBuffer verifies the coordinator
// learns when a STOP could not be queued so it retries instead of orphaning
// running shards. This is the code-side half of the CleanupAfterDelete
// property in verification/tla/KaptureLoadTest.tla.
func TestStopAllShards_ReportsUndeliveredOnFullBuffer(t *testing.T) {
	s := NewServer(":0")
	registerSpokeInCell(t, s, "spoke-a", "cell-a")

	// Fill the spoke's directive buffer so the STOP cannot be queued.
	for i := 0; i < DirectiveBufferSize; i++ {
		if err := s.SendReplayDirective("spoke-a", &hubv1.ReplayDirective{DirectiveId: "filler"}); err != nil {
			t.Fatalf("filling buffer: %v", err)
		}
	}

	r := &CaptureLoadTestReconciler{}
	lt := newLoadTest("lt")
	lt.Status.Assignments = []capturev1alpha1.LoadTestAssignment{
		{SpokeID: "spoke-a", ShardIndexes: []int32{0}},
	}

	if r.stopAllShards(s, lt, logr.Discard()) {
		t.Fatal("stopAllShards claimed delivery with a full directive buffer")
	}

	// Drain the buffer (as the spoke's directive stream would) and retry:
	// the STOP must now be accepted.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := newFakeDirectiveStream(ctx)
	go func() { _ = s.WatchDirectives(&hubv1.WatchDirectivesRequest{SpokeId: "spoke-a"}, stream) }()
	for i := 0; i < DirectiveBufferSize; i++ {
		select {
		case <-stream.sent:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out draining directive buffer")
		}
	}

	if !r.stopAllShards(s, lt, logr.Discard()) {
		t.Error("stopAllShards still undelivered after buffer drained")
	}
}

// TestStopAllShards_DeregisteredSpokeCountsAsDelivered verifies a vanished
// spoke does not wedge deletion forever.
func TestStopAllShards_DeregisteredSpokeCountsAsDelivered(t *testing.T) {
	s := NewServer(":0")

	r := &CaptureLoadTestReconciler{}
	lt := newLoadTest("lt")
	lt.Status.Assignments = []capturev1alpha1.LoadTestAssignment{
		{SpokeID: "gone-spoke", ShardIndexes: []int32{0}},
	}

	if !r.stopAllShards(s, lt, logr.Discard()) {
		t.Error("stopAllShards blocked on a deregistered spoke")
	}
}
