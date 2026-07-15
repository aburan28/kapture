package hub

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
	hubv1 "github.com/kapture-io/kapture/proto/hub/v1"
)

const (
	// LoadTestFinalizer guards spoke-side cleanup of replay shards.
	LoadTestFinalizer = "capture.gateway.io/loadtest-cleanup"

	// defaultLoadTestRequeue is how often an in-flight load test is
	// re-aggregated from spoke reports.
	defaultLoadTestRequeue = 10 * time.Second

	defaultWorkersPerSpoke      int32 = 1
	defaultConcurrencyPerWorker int32 = 10
	defaultAbortMinSample       int64 = 100
)

// CaptureLoadTestReconciler coordinates distributed load tests: it fans a
// CaptureLoadTest out into replay shard directives across the spokes of the
// selected cells, aggregates the shard statuses the spokes report back, and
// enforces the abort policy.
type CaptureLoadTestReconciler struct {
	client.Client
	Log logr.Logger

	// ServerProvider returns the hub gRPC server used to reach spokes.
	// It may return nil while the CaptureHub CR has not started the server.
	ServerProvider func() *Server

	// RequeueInterval overrides the aggregation interval (tests).
	RequeueInterval time.Duration
}

// +kubebuilder:rbac:groups=capture.gateway.io,resources=captureloadtests,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=capture.gateway.io,resources=captureloadtests/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=capture.gateway.io,resources=captureloadtests/finalizers,verbs=update

// SetupWithManager registers the reconciler with the manager.
func (r *CaptureLoadTestReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&capturev1alpha1.CaptureLoadTest{}).
		Complete(r)
}

// Reconcile drives a CaptureLoadTest through its lifecycle.
func (r *CaptureLoadTestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("captureloadtest", req.NamespacedName)

	var lt capturev1alpha1.CaptureLoadTest
	if err := r.Get(ctx, req.NamespacedName, &lt); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	srv := r.server()

	// Deletion: stop all shards on the spokes, then release the finalizer.
	// The finalizer is held until every connected spoke has accepted the
	// STOP directive, so a full directive buffer cannot orphan running
	// shards (verified by the CleanupAfterDelete property in
	// verification/tla/KaptureLoadTest.tla).
	if !lt.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&lt, LoadTestFinalizer) {
			if srv != nil {
				if !r.stopAllShards(srv, &lt, log) {
					log.Info("STOP not yet delivered to all spokes, retrying before finalizer removal")
					return ctrl.Result{RequeueAfter: r.requeueInterval()}, nil
				}
				srv.ClearReplayStatuses(lt.Namespace, lt.Name)
			}
			controllerutil.RemoveFinalizer(&lt, LoadTestFinalizer)
			if err := r.Update(ctx, &lt); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&lt, LoadTestFinalizer) {
		controllerutil.AddFinalizer(&lt, LoadTestFinalizer)
		if err := r.Update(ctx, &lt); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Terminal phases need no further work.
	switch lt.Status.Phase {
	case capturev1alpha1.CaptureLoadTestPhaseCompleted,
		capturev1alpha1.CaptureLoadTestPhaseFailed,
		capturev1alpha1.CaptureLoadTestPhaseAborted:
		return ctrl.Result{}, nil
	}

	if srv == nil {
		log.Info("hub gRPC server not running yet, waiting")
		lt.Status.Phase = capturev1alpha1.CaptureLoadTestPhasePending
		meta.SetStatusCondition(&lt.Status.Conditions, metav1.Condition{
			Type:    capturev1alpha1.LoadTestConditionSpokesAssigned,
			Status:  metav1.ConditionFalse,
			Reason:  "HubNotReady",
			Message: "hub gRPC server is not running",
		})
		if err := r.Status().Update(ctx, &lt); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: r.requeueInterval()}, nil
	}

	// Plan the distribution once; the recorded assignment stays stable for
	// the lifetime of the run.
	if len(lt.Status.Assignments) == 0 {
		planned, err := r.planDistribution(srv, &lt)
		if err != nil {
			log.Info("cannot distribute load test yet", "reason", err.Error())
			lt.Status.Phase = capturev1alpha1.CaptureLoadTestPhasePending
			meta.SetStatusCondition(&lt.Status.Conditions, metav1.Condition{
				Type:    capturev1alpha1.LoadTestConditionSpokesAssigned,
				Status:  metav1.ConditionFalse,
				Reason:  "NoEligibleSpokes",
				Message: err.Error(),
			})
			if err := r.Status().Update(ctx, &lt); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: r.requeueInterval()}, nil
		}

		now := metav1.Now()
		lt.Status.Phase = capturev1alpha1.CaptureLoadTestPhaseDistributing
		lt.Status.StartTime = &now
		lt.Status.Assignments = planned
		lt.Status.AssignedSpokes = int32(len(planned))
		lt.Status.TotalShards = countShards(planned)
		meta.SetStatusCondition(&lt.Status.Conditions, metav1.Condition{
			Type:    capturev1alpha1.LoadTestConditionSpokesAssigned,
			Status:  metav1.ConditionTrue,
			Reason:  "SpokesAssigned",
			Message: fmt.Sprintf("assigned %d shards across %d spokes", lt.Status.TotalShards, len(planned)),
		})
		if err := r.Status().Update(ctx, &lt); err != nil {
			return ctrl.Result{}, err
		}
	}

	// (Re-)send START directives for shards that have not reported status
	// yet. Directives are idempotent upserts on the spoke, so resending
	// after a dropped stream or full buffer is safe.
	statuses := srv.ReplayStatuses(lt.Namespace, lt.Name)
	reported := make(map[int32]bool, len(statuses))
	for _, snap := range statuses {
		reported[snap.Summary.ShardIndex] = true
	}
	for _, assignment := range lt.Status.Assignments {
		for _, shard := range assignment.ShardIndexes {
			if reported[shard] {
				continue
			}
			directive := r.buildStartDirective(&lt, shard)
			if err := srv.SendReplayDirective(assignment.SpokeID, directive); err != nil {
				log.Info("failed to queue replay directive, will retry",
					"spoke", assignment.SpokeID, "shard", shard, "error", err.Error())
			}
		}
	}

	// Aggregate shard progress into the load test status.
	r.aggregate(&lt, statuses)

	// Enforce the abort policy while the run is live. The Aborted phase is
	// only entered once every connected spoke has accepted the STOP
	// directive; until then the reconciler keeps retrying (the abort
	// conditions are monotonic, so the decision is stable across retries).
	if reason := r.shouldAbort(&lt); reason != "" {
		log.Info("aborting load test", "reason", reason)
		if !r.stopAllShards(srv, &lt, log) {
			log.Info("STOP not yet delivered to all spokes, retrying abort")
			meta.SetStatusCondition(&lt.Status.Conditions, metav1.Condition{
				Type:    capturev1alpha1.LoadTestConditionAborted,
				Status:  metav1.ConditionFalse,
				Reason:  "AbortInProgress",
				Message: reason,
			})
			if err := r.Status().Update(ctx, &lt); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: r.requeueInterval()}, nil
		}
		now := metav1.Now()
		lt.Status.Phase = capturev1alpha1.CaptureLoadTestPhaseAborted
		lt.Status.CompletionTime = &now
		meta.SetStatusCondition(&lt.Status.Conditions, metav1.Condition{
			Type:    capturev1alpha1.LoadTestConditionAborted,
			Status:  metav1.ConditionTrue,
			Reason:  "AbortPolicy",
			Message: reason,
		})
		if err := r.Status().Update(ctx, &lt); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	if err := r.Status().Update(ctx, &lt); err != nil {
		return ctrl.Result{}, err
	}

	switch lt.Status.Phase {
	case capturev1alpha1.CaptureLoadTestPhaseCompleted, capturev1alpha1.CaptureLoadTestPhaseFailed:
		return ctrl.Result{}, nil
	default:
		return ctrl.Result{RequeueAfter: r.requeueInterval()}, nil
	}
}

func (r *CaptureLoadTestReconciler) server() *Server {
	if r.ServerProvider == nil {
		return nil
	}
	return r.ServerProvider()
}

func (r *CaptureLoadTestReconciler) requeueInterval() time.Duration {
	if r.RequeueInterval > 0 {
		return r.RequeueInterval
	}
	return defaultLoadTestRequeue
}

// planDistribution selects spokes and assigns shard indexes. Shards are
// numbered densely: spoke i runs shards [i*workers, (i+1)*workers).
func (r *CaptureLoadTestReconciler) planDistribution(srv *Server, lt *capturev1alpha1.CaptureLoadTest) ([]capturev1alpha1.LoadTestAssignment, error) {
	var cells []string
	maxSpokes := 0
	workers := defaultWorkersPerSpoke
	if dist := lt.Spec.Distribution; dist != nil {
		cells = dist.Cells
		if dist.MaxSpokes != nil {
			maxSpokes = int(*dist.MaxSpokes)
		}
		if dist.WorkersPerSpoke != nil {
			workers = *dist.WorkersPerSpoke
		}
	}

	spokes := srv.SelectSpokes(cells, maxSpokes)
	if len(spokes) == 0 {
		if len(cells) > 0 {
			return nil, fmt.Errorf("no connected spokes in cells %v", cells)
		}
		return nil, fmt.Errorf("no connected spokes")
	}

	assignments := make([]capturev1alpha1.LoadTestAssignment, 0, len(spokes))
	next := int32(0)
	for _, spoke := range spokes {
		indexes := make([]int32, 0, workers)
		for w := int32(0); w < workers; w++ {
			indexes = append(indexes, next)
			next++
		}
		assignments = append(assignments, capturev1alpha1.LoadTestAssignment{
			SpokeID:      spoke.SpokeID,
			Cell:         spoke.Cell,
			ShardIndexes: indexes,
		})
	}
	return assignments, nil
}

// buildStartDirective converts the load test spec into a replay directive
// for one shard.
func (r *CaptureLoadTestReconciler) buildStartDirective(lt *capturev1alpha1.CaptureLoadTest, shard int32) *hubv1.ReplayDirective {
	totalShards := lt.Status.TotalShards

	presharded := lt.Spec.Distribution != nil &&
		lt.Spec.Distribution.Presharded != nil && *lt.Spec.Distribution.Presharded

	spec := &hubv1.ReplaySpec{
		SourceCapture:  lt.Spec.SourceRef.Name,
		StorageRefName: string(lt.Spec.StorageRef.Name),
		TargetHost:     lt.Spec.Target.Host,
		Shard:          &hubv1.ReplayShard{Index: shard, Count: totalShards, Presharded: presharded},
		Concurrency:    defaultConcurrencyPerWorker,
	}
	if lt.Spec.Target.Port != nil {
		spec.TargetPort = *lt.Spec.Target.Port
	}
	if lt.Spec.Target.TLS != nil {
		spec.TargetTls = *lt.Spec.Target.TLS
	}
	if dist := lt.Spec.Distribution; dist != nil && dist.ConcurrencyPerWorker != nil {
		spec.Concurrency = *dist.ConcurrencyPerWorker
	}

	if engine := lt.Spec.Engine; engine != nil {
		spec.EngineName = engine.Name
		if engine.Config != nil {
			spec.EngineConfigJson = engine.Config.Raw
		}
	}

	if rate := lt.Spec.Rate; rate != nil {
		protoRate := &hubv1.ReplayRate{Mode: string(rate.Mode)}
		if rate.Mode == capturev1alpha1.ReplayRateModeConstant && rate.RequestsPerSecond != nil {
			protoRate.RequestsPerSecond = splitRate(*rate.RequestsPerSecond, totalShards, shard)
		}
		if rate.TimeScale != nil {
			protoRate.TimeScale = *rate.TimeScale
		}
		spec.Rate = protoRate
	} else {
		// Default to reproducing the recorded timing: each shard replays
		// its subset with original inter-request delays, which yields the
		// recorded aggregate QPS across all shards.
		spec.Rate = &hubv1.ReplayRate{Mode: string(capturev1alpha1.ReplayRateModeOriginalTiming)}
	}

	if f := lt.Spec.Filters; f != nil {
		filters := &hubv1.ReplayDataFilters{
			Methods: f.Methods,
		}
		if f.StartTime != nil {
			filters.StartTime = timestamppb.New(f.StartTime.Time)
		}
		if f.EndTime != nil {
			filters.EndTime = timestamppb.New(f.EndTime.Time)
		}
		if f.PathPrefix != nil {
			filters.PathPrefix = *f.PathPrefix
		}
		if f.Limit != nil {
			filters.Limit = *f.Limit
		}
		spec.Filters = filters
	}

	return &hubv1.ReplayDirective{
		DirectiveId:       fmt.Sprintf("%s-%s-shard-%d", lt.Namespace, lt.Name, shard),
		Action:            hubv1.ReplayAction_REPLAY_ACTION_START,
		LoadTestName:      lt.Name,
		LoadTestNamespace: lt.Namespace,
		Spec:              spec,
	}
}

// stopAllShards sends STOP directives to every assigned spoke and reports
// whether every directive was accepted. A spoke that is no longer
// registered (NotFound) counts as delivered: it cannot be reached, and its
// shards are bounded by the capture size anyway. Any other failure (e.g. a
// full directive buffer) means the caller must retry — STOP directives are
// idempotent, so re-sending to spokes that already received one is safe.
func (r *CaptureLoadTestReconciler) stopAllShards(srv *Server, lt *capturev1alpha1.CaptureLoadTest, log logr.Logger) bool {
	directive := &hubv1.ReplayDirective{
		DirectiveId:       fmt.Sprintf("%s-%s-stop", lt.Namespace, lt.Name),
		Action:            hubv1.ReplayAction_REPLAY_ACTION_STOP,
		LoadTestName:      lt.Name,
		LoadTestNamespace: lt.Namespace,
	}
	allDelivered := true
	for _, assignment := range lt.Status.Assignments {
		err := srv.SendReplayDirective(assignment.SpokeID, directive)
		if err == nil {
			continue
		}
		if status.Code(err) == codes.NotFound {
			log.Info("spoke deregistered, skipping stop directive", "spoke", assignment.SpokeID)
			continue
		}
		log.Info("failed to send stop directive", "spoke", assignment.SpokeID, "error", err.Error())
		allDelivered = false
	}
	return allDelivered
}

// aggregate rolls shard statuses up into the load test status and advances
// the phase when shards start reporting or reach terminal states.
func (r *CaptureLoadTestReconciler) aggregate(lt *capturev1alpha1.CaptureLoadTest, statuses []ReplayStatusSnapshot) {
	var (
		total, sent, failed, filtered int64
		completed, failedShards       int32
		running                       int32
		rps                           float64
	)
	cellTotals := make(map[string]*capturev1alpha1.LoadTestCellStatus)
	for _, assignment := range lt.Status.Assignments {
		cell := cellTotals[assignment.Cell]
		if cell == nil {
			cellTotals[assignment.Cell] = &capturev1alpha1.LoadTestCellStatus{
				Name:   assignment.Cell,
				Spokes: 1,
				Shards: int32(len(assignment.ShardIndexes)),
			}
		} else {
			cell.Spokes++
			cell.Shards += int32(len(assignment.ShardIndexes))
		}
	}

	spokeCells := make(map[string]string, len(lt.Status.Assignments))
	for _, assignment := range lt.Status.Assignments {
		spokeCells[assignment.SpokeID] = assignment.Cell
	}

	for _, snap := range statuses {
		s := snap.Summary
		total += s.TotalRequests
		sent += s.SentRequests
		failed += s.FailedRequests
		filtered += s.FilteredRequests
		rps += s.AchievedRps

		switch s.Phase {
		case hubv1.ReplayPhase_REPLAY_PHASE_COMPLETED:
			completed++
		case hubv1.ReplayPhase_REPLAY_PHASE_FAILED:
			failedShards++
		case hubv1.ReplayPhase_REPLAY_PHASE_RUNNING:
			running++
		}

		if cell := cellTotals[spokeCells[snap.SpokeID]]; cell != nil {
			cell.SentRequests += s.SentRequests
			cell.FailedRequests += s.FailedRequests
		}
	}

	lt.Status.TotalRequests = total
	lt.Status.SentRequests = sent
	lt.Status.FailedRequests = failed
	lt.Status.FilteredRequests = filtered
	lt.Status.CompletedShards = completed
	lt.Status.FailedShards = failedShards
	if rps > 0 {
		formatted := strconv.FormatFloat(rps, 'f', 1, 64)
		lt.Status.AchievedRPS = &formatted
	}

	cells := make([]capturev1alpha1.LoadTestCellStatus, 0, len(cellTotals))
	for _, assignment := range lt.Status.Assignments {
		if cell, ok := cellTotals[assignment.Cell]; ok {
			cells = append(cells, *cell)
			delete(cellTotals, assignment.Cell)
		}
	}
	lt.Status.Cells = cells

	// Phase transitions.
	totalShards := lt.Status.TotalShards
	switch {
	case totalShards > 0 && completed == totalShards:
		lt.Status.Phase = capturev1alpha1.CaptureLoadTestPhaseCompleted
		if lt.Status.CompletionTime == nil {
			now := metav1.Now()
			lt.Status.CompletionTime = &now
		}
	case totalShards > 0 && completed+failedShards == totalShards:
		lt.Status.Phase = capturev1alpha1.CaptureLoadTestPhaseFailed
		if lt.Status.CompletionTime == nil {
			now := metav1.Now()
			lt.Status.CompletionTime = &now
		}
	case running > 0 || completed > 0 || failedShards > 0:
		lt.Status.Phase = capturev1alpha1.CaptureLoadTestPhaseRunning
	}
}

// shouldAbort evaluates the abort policy. It returns a human-readable
// reason, or empty when the run may continue.
func (r *CaptureLoadTestReconciler) shouldAbort(lt *capturev1alpha1.CaptureLoadTest) string {
	policy := lt.Spec.Abort
	if policy == nil {
		return ""
	}
	switch lt.Status.Phase {
	case capturev1alpha1.CaptureLoadTestPhaseDistributing, capturev1alpha1.CaptureLoadTestPhaseRunning:
	default:
		return ""
	}

	if policy.MaxDuration != nil && lt.Status.StartTime != nil {
		if elapsed := time.Since(lt.Status.StartTime.Time); elapsed > policy.MaxDuration.Duration {
			return fmt.Sprintf("run exceeded maxDuration %s (elapsed %s)",
				policy.MaxDuration.Duration, elapsed.Round(time.Second))
		}
	}

	if policy.ErrorPercent != nil {
		minSample := defaultAbortMinSample
		if policy.MinSampleRequests != nil {
			minSample = *policy.MinSampleRequests
		}
		attempted := lt.Status.SentRequests + lt.Status.FailedRequests
		if attempted >= minSample {
			errorPercent := float64(lt.Status.FailedRequests) * 100 / float64(attempted)
			if errorPercent > float64(*policy.ErrorPercent) {
				return fmt.Sprintf("error rate %.1f%% exceeded abort threshold %d%%",
					errorPercent, *policy.ErrorPercent)
			}
		}
	}

	return ""
}

// splitRate divides an aggregate RPS target across shards, giving the
// remainder to the lowest-indexed shards so the shard rates sum exactly to
// the configured aggregate.
func splitRate(totalRPS, shardCount, shard int32) int32 {
	if shardCount <= 0 {
		return totalRPS
	}
	base := totalRPS / shardCount
	if shard < totalRPS%shardCount {
		base++
	}
	if base < 1 {
		base = 1
	}
	return base
}

func countShards(assignments []capturev1alpha1.LoadTestAssignment) int32 {
	var count int32
	for _, a := range assignments {
		count += int32(len(a.ShardIndexes))
	}
	return count
}
