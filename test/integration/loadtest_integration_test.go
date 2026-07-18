package integration

import (
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
	hubv1 "github.com/kapture-io/kapture/proto/hub/v1"
)

// registerSpoke registers a fake spoke on the in-process hub registry.
func registerSpoke(t *testing.T, spokeID, cell string) {
	t.Helper()
	if _, err := hubServer.RegisterSpoke(ctx, &hubv1.RegisterSpokeRequest{
		SpokeId:     spokeID,
		ClusterName: spokeID,
		Cell:        cell,
	}); err != nil {
		t.Fatalf("registering spoke %s: %v", spokeID, err)
	}
	t.Cleanup(func() {
		_, _ = hubServer.DeregisterSpoke(ctx, &hubv1.DeregisterSpokeRequest{SpokeId: spokeID})
	})
}

func newLoadTest(ns, name, cell string, workersPerSpoke int32) *capturev1alpha1.CaptureLoadTest {
	return &capturev1alpha1.CaptureLoadTest{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: capturev1alpha1.CaptureLoadTestSpec{
			SourceRef:  capturev1alpha1.TrafficCaptureReference{Name: "orders"},
			StorageRef: capturev1alpha1.CaptureStorageReference{Name: "test-storage"},
			Target:     capturev1alpha1.ReplayTarget{Host: "staging.internal"},
			Distribution: &capturev1alpha1.LoadTestDistribution{
				Cells:           []string{cell},
				WorkersPerSpoke: &workersPerSpoke,
			},
		},
	}
}

func getLoadTest(t *testing.T, key types.NamespacedName) *capturev1alpha1.CaptureLoadTest {
	t.Helper()
	lt := &capturev1alpha1.CaptureLoadTest{}
	if err := k8sClient.Get(ctx, key, lt); err != nil {
		t.Fatalf("getting CaptureLoadTest %s: %v", key, err)
	}
	return lt
}

// TestCaptureLoadTestAssignsShardsAcrossSpokes verifies the coordinator
// plans a stable shard assignment over the registered spokes of the
// requested cell against a real API server.
func TestCaptureLoadTestAssignsShardsAcrossSpokes(t *testing.T) {
	ns := createTestNamespace(t)
	cell := "cell-int-a"
	registerSpoke(t, "int-spoke-a1", cell)
	registerSpoke(t, "int-spoke-a2", cell)

	lt := newLoadTest(ns, "assign-test", cell, 2)
	if err := k8sClient.Create(ctx, lt); err != nil {
		t.Fatalf("creating CaptureLoadTest: %v", err)
	}
	key := types.NamespacedName{Namespace: ns, Name: lt.Name}

	waitForCondition(t, 15*time.Second, func() bool {
		got := &capturev1alpha1.CaptureLoadTest{}
		if err := k8sClient.Get(ctx, key, got); err != nil {
			return false
		}
		return len(got.Status.Assignments) == 2
	})

	got := getLoadTest(t, key)
	if got.Status.TotalShards != 4 {
		t.Errorf("totalShards = %d, want 4 (2 spokes x 2 workers)", got.Status.TotalShards)
	}
	if got.Status.AssignedSpokes != 2 {
		t.Errorf("assignedSpokes = %d, want 2", got.Status.AssignedSpokes)
	}
	seen := map[int32]bool{}
	for _, a := range got.Status.Assignments {
		for _, shard := range a.ShardIndexes {
			if seen[shard] {
				t.Errorf("shard %d assigned twice", shard)
			}
			seen[shard] = true
		}
	}
	if len(seen) != 4 {
		t.Errorf("assignments cover %d distinct shards, want 4", len(seen))
	}
	cond := meta.FindStatusCondition(got.Status.Conditions, capturev1alpha1.LoadTestConditionSpokesAssigned)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Errorf("SpokesAssigned condition = %+v, want True", cond)
	}
}

// TestCaptureLoadTestCompletesFromShardReports drives all shards to
// Completed through the hub registry and expects the CR to aggregate the
// counters and reach the Completed phase.
func TestCaptureLoadTestCompletesFromShardReports(t *testing.T) {
	ns := createTestNamespace(t)
	cell := "cell-int-b"
	registerSpoke(t, "int-spoke-b1", cell)

	lt := newLoadTest(ns, "complete-test", cell, 2)
	if err := k8sClient.Create(ctx, lt); err != nil {
		t.Fatalf("creating CaptureLoadTest: %v", err)
	}
	key := types.NamespacedName{Namespace: ns, Name: lt.Name}

	waitForCondition(t, 15*time.Second, func() bool {
		got := &capturev1alpha1.CaptureLoadTest{}
		if err := k8sClient.Get(ctx, key, got); err != nil {
			return false
		}
		return len(got.Status.Assignments) == 1
	})

	for shard := int32(0); shard < 2; shard++ {
		if _, err := hubServer.ReportReplayStatus(ctx, &hubv1.ReportReplayStatusRequest{
			SpokeId: "int-spoke-b1",
			Statuses: []*hubv1.ReplayStatusSummary{{
				LoadTestName:      lt.Name,
				LoadTestNamespace: ns,
				ReplayName:        fmt.Sprintf("%s-shard-%d", lt.Name, shard),
				ShardIndex:        shard,
				ShardCount:        2,
				Phase:             hubv1.ReplayPhase_REPLAY_PHASE_COMPLETED,
				TotalRequests:     50,
				SentRequests:      48,
				FailedRequests:    2,
			}},
		}); err != nil {
			t.Fatalf("reporting shard %d: %v", shard, err)
		}
	}

	waitForCondition(t, 15*time.Second, func() bool {
		got := &capturev1alpha1.CaptureLoadTest{}
		if err := k8sClient.Get(ctx, key, got); err != nil {
			return false
		}
		return got.Status.Phase == capturev1alpha1.CaptureLoadTestPhaseCompleted
	})

	got := getLoadTest(t, key)
	if got.Status.SentRequests != 96 || got.Status.FailedRequests != 4 {
		t.Errorf("aggregated sent/failed = %d/%d, want 96/4",
			got.Status.SentRequests, got.Status.FailedRequests)
	}
	if got.Status.CompletionTime == nil {
		t.Error("completionTime not set on completed load test")
	}
}

// TestCaptureLoadTestTargetDeniedFailsTerminally verifies the safety
// allowlist gates distribution before any shard exists.
func TestCaptureLoadTestTargetDeniedFailsTerminally(t *testing.T) {
	ns := createTestNamespace(t)
	registerSpoke(t, "int-spoke-denied", "cell-int-denied")

	lt := newLoadTest(ns, "denied-test", "cell-int-denied", 1)
	lt.Spec.Target.Host = "prod.example.com"
	lt.Spec.Safety = &capturev1alpha1.ReplaySafety{AllowedHosts: []string{"*.staging.internal"}}
	if err := k8sClient.Create(ctx, lt); err != nil {
		t.Fatalf("creating CaptureLoadTest: %v", err)
	}
	key := types.NamespacedName{Namespace: ns, Name: lt.Name}

	waitForCondition(t, 15*time.Second, func() bool {
		got := &capturev1alpha1.CaptureLoadTest{}
		if err := k8sClient.Get(ctx, key, got); err != nil {
			return false
		}
		return got.Status.Phase == capturev1alpha1.CaptureLoadTestPhaseFailed
	})

	got := getLoadTest(t, key)
	if len(got.Status.Assignments) != 0 {
		t.Errorf("denied load test has %d assignments, want 0", len(got.Status.Assignments))
	}
	cond := meta.FindStatusCondition(got.Status.Conditions, capturev1alpha1.LoadTestConditionTargetAllowed)
	if cond == nil || cond.Status != metav1.ConditionFalse {
		t.Errorf("TargetAllowed condition = %+v, want False", cond)
	}
}

// TestCaptureLoadTestDeletionReleasesFinalizer verifies deletion delivers
// STOP to the assigned spokes and then releases the finalizer.
func TestCaptureLoadTestDeletionReleasesFinalizer(t *testing.T) {
	ns := createTestNamespace(t)
	cell := "cell-int-c"
	registerSpoke(t, "int-spoke-c1", cell)

	lt := newLoadTest(ns, "delete-test", cell, 1)
	if err := k8sClient.Create(ctx, lt); err != nil {
		t.Fatalf("creating CaptureLoadTest: %v", err)
	}
	key := types.NamespacedName{Namespace: ns, Name: lt.Name}

	waitForCondition(t, 15*time.Second, func() bool {
		got := &capturev1alpha1.CaptureLoadTest{}
		if err := k8sClient.Get(ctx, key, got); err != nil {
			return false
		}
		return len(got.Status.Assignments) == 1
	})

	if err := k8sClient.Delete(ctx, getLoadTest(t, key)); err != nil {
		t.Fatalf("deleting CaptureLoadTest: %v", err)
	}
	waitForAbsence(t, &capturev1alpha1.CaptureLoadTest{}, key, 15*time.Second)
}

// TestCaptureLoadTestCELValidation exercises the CRD's CEL rules against
// the real API server: invalid cross-field combinations must be rejected
// at admission.
func TestCaptureLoadTestCELValidation(t *testing.T) {
	ns := createTestNamespace(t)

	constant := capturev1alpha1.ReplayRateModeConstant
	badRate := newLoadTest(ns, "bad-rate", "cell-x", 1)
	badRate.Spec.Rate = &capturev1alpha1.ReplayRateConfig{Mode: constant}
	if err := k8sClient.Create(ctx, badRate); err == nil {
		t.Error("Constant rate without requestsPerSecond was accepted")
		_ = k8sClient.Delete(ctx, badRate)
	}

	badHost := newLoadTest(ns, "bad-host", "cell-x", 1)
	badHost.Spec.Target.Host = "-not-a-host-"
	if err := k8sClient.Create(ctx, badHost); err == nil {
		t.Error("invalid target host was accepted")
		_ = k8sClient.Delete(ctx, badHost)
	}
}
