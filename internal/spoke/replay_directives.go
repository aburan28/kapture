package spoke

import (
	"context"
	"fmt"
	"strconv"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
	hubv1 "github.com/kapture-io/kapture/proto/hub/v1"
)

const (
	// LabelLoadTest marks TrafficReplay resources created from hub load
	// test directives with the owning CaptureLoadTest name.
	LabelLoadTest = "capture.gateway.io/load-test"
	// LabelShardIndex records the shard index a TrafficReplay covers.
	LabelShardIndex = "capture.gateway.io/shard-index"
)

// ReplayDirectiveHandler applies hub-initiated replay directives by creating
// and deleting TrafficReplay resources on the spoke cluster. The local
// TrafficReplay controller then runs the shards as replay-engine Jobs.
type ReplayDirectiveHandler struct {
	Client client.Client
	Log    logr.Logger
}

// Handle applies a single replay directive. Unknown actions are ignored.
func (h *ReplayDirectiveHandler) Handle(ctx context.Context, directive *hubv1.ReplayDirective) error {
	if directive == nil {
		return nil
	}
	switch directive.Action {
	case hubv1.ReplayAction_REPLAY_ACTION_START:
		return h.handleStart(ctx, directive)
	case hubv1.ReplayAction_REPLAY_ACTION_STOP:
		return h.handleStop(ctx, directive)
	default:
		h.Log.Info("ignoring replay directive with unknown action",
			"action", directive.Action, "directiveID", directive.DirectiveId)
		return nil
	}
}

// handleStart upserts the TrafficReplay shard described by the directive.
// Repeated START directives for the same shard are idempotent.
func (h *ReplayDirectiveHandler) handleStart(ctx context.Context, directive *hubv1.ReplayDirective) error {
	if directive.Spec == nil {
		return fmt.Errorf("replay directive %s has no spec", directive.DirectiveId)
	}
	if directive.LoadTestName == "" || directive.LoadTestNamespace == "" {
		return fmt.Errorf("replay directive %s missing load test identity", directive.DirectiveId)
	}

	replay := replayFromDirective(directive)

	existing := &capturev1alpha1.TrafficReplay{}
	key := types.NamespacedName{Namespace: replay.Namespace, Name: replay.Name}
	err := h.Client.Get(ctx, key, existing)
	if apierrors.IsNotFound(err) {
		h.Log.Info("creating replay shard from hub directive",
			"replay", key, "loadTest", directive.LoadTestName)
		return h.Client.Create(ctx, replay)
	}
	if err != nil {
		return err
	}

	// Shard already exists (directive resend); leave the running shard
	// untouched so counts are not reset mid-run.
	return nil
}

// handleStop deletes every TrafficReplay shard belonging to the load test.
func (h *ReplayDirectiveHandler) handleStop(ctx context.Context, directive *hubv1.ReplayDirective) error {
	if directive.LoadTestName == "" || directive.LoadTestNamespace == "" {
		return fmt.Errorf("replay directive %s missing load test identity", directive.DirectiveId)
	}

	h.Log.Info("stopping replay shards from hub directive",
		"loadTest", directive.LoadTestName, "namespace", directive.LoadTestNamespace)

	return h.Client.DeleteAllOf(ctx, &capturev1alpha1.TrafficReplay{},
		client.InNamespace(directive.LoadTestNamespace),
		client.MatchingLabels{LabelLoadTest: directive.LoadTestName},
	)
}

// replayFromDirective maps the proto replay spec onto a TrafficReplay CR.
func replayFromDirective(directive *hubv1.ReplayDirective) *capturev1alpha1.TrafficReplay {
	spec := directive.Spec

	shardIndex := int32(0)
	shardCount := int32(1)
	if spec.Shard != nil {
		shardIndex = spec.Shard.Index
		shardCount = spec.Shard.Count
	}

	replay := &capturev1alpha1.TrafficReplay{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ReplayShardName(directive.LoadTestName, shardIndex),
			Namespace: directive.LoadTestNamespace,
			Labels: map[string]string{
				LabelLoadTest:   directive.LoadTestName,
				LabelShardIndex: strconv.Itoa(int(shardIndex)),
			},
		},
		Spec: capturev1alpha1.TrafficReplaySpec{
			SourceRef:  capturev1alpha1.TrafficCaptureReference{Name: spec.SourceCapture},
			StorageRef: capturev1alpha1.CaptureStorageReference{Name: gwapiv1.ObjectName(spec.StorageRefName)},
			Target: capturev1alpha1.ReplayTarget{
				Host: spec.TargetHost,
			},
			Shard: &capturev1alpha1.ReplayShardSpec{
				Index: shardIndex,
				Count: shardCount,
			},
			LoadTestRef: &capturev1alpha1.LoadTestReference{Name: directive.LoadTestName},
		},
	}

	if spec.TargetPort != 0 {
		port := spec.TargetPort
		replay.Spec.Target.Port = &port
	}
	if spec.TargetTls {
		tls := true
		replay.Spec.Target.TLS = &tls
	}
	if spec.Concurrency > 0 {
		concurrency := spec.Concurrency
		replay.Spec.Concurrency = &concurrency
	}

	if spec.EngineName != "" || len(spec.EngineConfigJson) > 0 {
		engine := &capturev1alpha1.ReplayEngineSpec{Name: spec.EngineName}
		if len(spec.EngineConfigJson) > 0 {
			engine.Config = &runtime.RawExtension{Raw: spec.EngineConfigJson}
		}
		replay.Spec.Engine = engine
	}

	if rate := spec.Rate; rate != nil {
		rateConfig := &capturev1alpha1.ReplayRateConfig{
			Mode: capturev1alpha1.ReplayRateMode(rate.Mode),
		}
		if rate.RequestsPerSecond > 0 {
			rps := rate.RequestsPerSecond
			rateConfig.RequestsPerSecond = &rps
		}
		if rate.TimeScale != "" {
			timeScale := rate.TimeScale
			rateConfig.TimeScale = &timeScale
		}
		replay.Spec.Rate = rateConfig
	}

	if f := spec.Filters; f != nil {
		filters := &capturev1alpha1.ReplayFilters{
			Methods: f.Methods,
		}
		if f.StartTime != nil {
			t := metav1.NewTime(f.StartTime.AsTime())
			filters.StartTime = &t
		}
		if f.EndTime != nil {
			t := metav1.NewTime(f.EndTime.AsTime())
			filters.EndTime = &t
		}
		if f.PathPrefix != "" {
			prefix := f.PathPrefix
			filters.PathPrefix = &prefix
		}
		if f.Limit > 0 {
			limit := f.Limit
			filters.Limit = &limit
		}
		replay.Spec.Filters = filters
	}

	return replay
}

// ReplayShardName returns the deterministic TrafficReplay name for one shard
// of a load test.
func ReplayShardName(loadTestName string, shardIndex int32) string {
	return fmt.Sprintf("%s-shard-%d", loadTestName, shardIndex)
}
