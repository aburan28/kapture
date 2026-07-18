package integration

import (
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
	"github.com/kapture-io/kapture/internal/spoke"
)

func newTrafficReplay(ns, name, storageName string) *capturev1alpha1.TrafficReplay {
	return &capturev1alpha1.TrafficReplay{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: capturev1alpha1.TrafficReplaySpec{
			SourceRef:  capturev1alpha1.TrafficCaptureReference{Name: "orders"},
			StorageRef: capturev1alpha1.CaptureStorageReference{Name: gwapiv1.ObjectName(storageName)},
			Target:     capturev1alpha1.ReplayTarget{Host: "staging.internal"},
		},
	}
}

// TestTrafficReplayCreatesJob verifies the reconciler materialises an
// owned replay Job for a TrafficReplay with resolvable storage.
func TestTrafficReplayCreatesJob(t *testing.T) {
	ns := createTestNamespace(t)
	storage := newCaptureStorage(ns, "replay-storage")
	if err := k8sClient.Create(ctx, storage); err != nil {
		t.Fatalf("creating CaptureStorage: %v", err)
	}

	tr := newTrafficReplay(ns, "replay-job-test", "replay-storage")
	shard := &capturev1alpha1.ReplayShardSpec{Index: 1, Count: 4}
	tr.Spec.Shard = shard
	if err := k8sClient.Create(ctx, tr); err != nil {
		t.Fatalf("creating TrafficReplay: %v", err)
	}

	job := &batchv1.Job{}
	jobKey := types.NamespacedName{Namespace: ns, Name: spoke.ReplayJobName(tr)}
	waitForObject(t, job, jobKey, 15*time.Second)

	if len(job.OwnerReferences) == 0 || job.OwnerReferences[0].Kind != capturev1alpha1.KindTrafficReplay {
		t.Errorf("replay Job owner references = %+v, want TrafficReplay controller ref", job.OwnerReferences)
	}

	// The shard flags must reach the worker command line.
	args := job.Spec.Template.Spec.Containers[0].Args
	argValue := func(flag string) string {
		for i, a := range args {
			if a == flag && i+1 < len(args) {
				return args[i+1]
			}
		}
		return ""
	}
	if got := argValue("--shard-index"); got != "1" {
		t.Errorf("--shard-index = %q, want 1 (args: %v)", got, args)
	}
	if got := argValue("--shard-count"); got != "4" {
		t.Errorf("--shard-count = %q, want 4 (args: %v)", got, args)
	}

	// Status should leave Pending only when the Job progresses; initially
	// Pending with the JobCreated reason.
	waitForCondition(t, 15*time.Second, func() bool {
		got := &capturev1alpha1.TrafficReplay{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: tr.Name}, got); err != nil {
			return false
		}
		return got.Status.Phase == capturev1alpha1.TrafficReplayPhasePending
	})
}

// TestTrafficReplayTargetDeniedCreatesNoJob verifies the spoke-side safety
// gate: a denied target fails the replay before any Job exists.
func TestTrafficReplayTargetDeniedCreatesNoJob(t *testing.T) {
	ns := createTestNamespace(t)
	storage := newCaptureStorage(ns, "denied-storage")
	if err := k8sClient.Create(ctx, storage); err != nil {
		t.Fatalf("creating CaptureStorage: %v", err)
	}

	tr := newTrafficReplay(ns, "replay-denied-test", "denied-storage")
	tr.Spec.Target.Host = "prod.example.com"
	tr.Spec.Safety = &capturev1alpha1.ReplaySafety{AllowedHosts: []string{"*.staging.internal"}}
	if err := k8sClient.Create(ctx, tr); err != nil {
		t.Fatalf("creating TrafficReplay: %v", err)
	}

	waitForCondition(t, 15*time.Second, func() bool {
		got := &capturev1alpha1.TrafficReplay{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: tr.Name}, got); err != nil {
			return false
		}
		return got.Status.Phase == capturev1alpha1.TrafficReplayPhaseFailed
	})

	job := &batchv1.Job{}
	err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: spoke.ReplayJobName(tr)}, job)
	if err == nil {
		t.Error("replay Job exists for a denied target")
	}
}

// TestTrafficReplayShardCELValidation verifies the CRD's CEL rule rejects
// an out-of-range shard index at admission.
func TestTrafficReplayShardCELValidation(t *testing.T) {
	ns := createTestNamespace(t)

	tr := newTrafficReplay(ns, "bad-shard", "whatever")
	tr.Spec.Shard = &capturev1alpha1.ReplayShardSpec{Index: 4, Count: 4}
	if err := k8sClient.Create(ctx, tr); err == nil {
		t.Error("shard index == count was accepted; CEL rule not enforced")
		_ = k8sClient.Delete(ctx, tr)
	}
}
