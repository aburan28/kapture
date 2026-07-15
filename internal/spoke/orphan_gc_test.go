package spoke

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
	hubv1 "github.com/kapture-io/kapture/proto/hub/v1"
)

func gcReplay(name, loadTest string, age time.Duration) *capturev1alpha1.TrafficReplay {
	tr := &capturev1alpha1.TrafficReplay{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "default",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-age)),
		},
		Spec: capturev1alpha1.TrafficReplaySpec{
			SourceRef:  capturev1alpha1.TrafficCaptureReference{Name: "orders"},
			StorageRef: capturev1alpha1.CaptureStorageReference{Name: "store"},
			Target:     capturev1alpha1.ReplayTarget{Host: "t"},
		},
	}
	if loadTest != "" {
		tr.Spec.LoadTestRef = &capturev1alpha1.LoadTestReference{Name: loadTest}
		tr.Labels = map[string]string{LabelLoadTest: loadTest}
	}
	return tr
}

func TestOrphanShardGC_DeletesOnlyAgedOrphans(t *testing.T) {
	scheme := directiveScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		gcReplay("live-shard", "live-lt", time.Hour),     // load test still exists
		gcReplay("orphan-shard", "gone-lt", time.Hour),   // orphaned and old → GC
		gcReplay("young-orphan", "new-lt", time.Second),  // orphaned but young → keep
		gcReplay("standalone", "", time.Hour),            // user-managed → keep
	).Build()

	gc := &OrphanShardGC{Client: cl, Log: logr.Discard()}
	gc.Collect(context.Background(), []*hubv1.LoadTestKey{
		{Namespace: "default", Name: "live-lt"},
	})

	var remaining capturev1alpha1.TrafficReplayList
	if err := cl.List(context.Background(), &remaining); err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, tr := range remaining.Items {
		names[tr.Name] = true
	}
	if names["orphan-shard"] {
		t.Error("aged orphan shard was not garbage-collected")
	}
	for _, keep := range []string{"live-shard", "young-orphan", "standalone"} {
		if !names[keep] {
			t.Errorf("%s was deleted but must be kept", keep)
		}
	}
}

func TestOrphanShardGC_EmptyActiveListCollectsAgedLoadTestShards(t *testing.T) {
	// An empty-but-complete list means the hub genuinely has no load
	// tests: every aged load-test shard is an orphan.
	scheme := directiveScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		gcReplay("orphan-shard", "gone-lt", time.Hour),
		gcReplay("standalone", "", time.Hour),
	).Build()

	gc := &OrphanShardGC{Client: cl, Log: logr.Discard()}
	gc.Collect(context.Background(), nil)

	var remaining capturev1alpha1.TrafficReplayList
	if err := cl.List(context.Background(), &remaining); err != nil {
		t.Fatal(err)
	}
	if len(remaining.Items) != 1 || remaining.Items[0].Name != "standalone" {
		t.Errorf("remaining = %v, want only the standalone replay", remaining.Items)
	}
}
