package spoke

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
	hubv1 "github.com/kapture-io/kapture/proto/hub/v1"
)

// DefaultOrphanMinAge is how old a load-test shard must be before it can
// be garbage-collected as orphaned. It absorbs the race between a shard
// being created from a directive and the hub's load-test list reflecting
// the (very recently created) CaptureLoadTest.
const DefaultOrphanMinAge = 2 * time.Minute

// OrphanShardGC deletes load-test replay shards whose CaptureLoadTest no
// longer exists on the hub. It is the spoke-side backstop for the one
// delivery gap in the STOP protocol: a hub crash after the finalizer was
// removed but before a queued STOP reached this spoke's directive stream.
// The hub piggybacks its authoritative load-test list on every heartbeat
// response; anything local that references a missing load test is torn
// down here.
//
// Modelled as the OrphanGC action in verification/tla/KaptureLoadTest.tla.
type OrphanShardGC struct {
	Client client.Client
	Log    logr.Logger

	// MinAge guards against acting on shards younger than the list
	// propagation delay. Defaults to DefaultOrphanMinAge.
	MinAge time.Duration
}

// Collect deletes orphaned shards given the hub's complete load-test list.
func (g *OrphanShardGC) Collect(ctx context.Context, active []*hubv1.LoadTestKey) {
	minAge := g.MinAge
	if minAge <= 0 {
		minAge = DefaultOrphanMinAge
	}

	activeSet := make(map[string]bool, len(active))
	for _, key := range active {
		activeSet[key.Namespace+"/"+key.Name] = true
	}

	var replays capturev1alpha1.TrafficReplayList
	if err := g.Client.List(ctx, &replays); err != nil {
		// The manager cache may not be started yet; try again next beat.
		return
	}

	now := time.Now()
	for i := range replays.Items {
		tr := &replays.Items[i]
		if tr.Spec.LoadTestRef == nil {
			continue // standalone replays are user-managed
		}
		if activeSet[tr.Namespace+"/"+tr.Spec.LoadTestRef.Name] {
			continue
		}
		if now.Sub(tr.CreationTimestamp.Time) < minAge {
			continue
		}

		g.Log.Info("garbage-collecting orphaned replay shard",
			"replay", tr.Namespace+"/"+tr.Name,
			"loadTest", tr.Spec.LoadTestRef.Name)
		if err := g.Client.Delete(ctx, tr); client.IgnoreNotFound(err) != nil {
			g.Log.Error(err, "failed to delete orphaned shard",
				"replay", tr.Namespace+"/"+tr.Name)
		}
	}
}
