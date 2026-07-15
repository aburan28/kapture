package spoke

import (
	"context"
	"fmt"
	"strconv"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
	"github.com/kapture-io/kapture/internal/plugin/replay"
	"github.com/kapture-io/kapture/internal/safety"
	hubv1 "github.com/kapture-io/kapture/proto/hub/v1"
)

const replayFinalizerName = "capture.gateway.io/replay-cleanup"

// TrafficReplayReconciler runs TrafficReplay resources as replay-engine
// Jobs. For distributed load tests each TrafficReplay is one shard created
// from a hub directive; standalone TrafficReplays work the same way with a
// single implicit shard.
type TrafficReplayReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	HubClient *HubClient // optional: nil when running in standalone mode
}

// +kubebuilder:rbac:groups=capture.gateway.io,resources=trafficreplays,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=capture.gateway.io,resources=trafficreplays/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=capture.gateway.io,resources=trafficreplays/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

// SetupWithManager sets up the controller with the Manager.
func (r *TrafficReplayReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&capturev1alpha1.TrafficReplay{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}

func (r *TrafficReplayReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	tr := &capturev1alpha1.TrafficReplay{}
	if err := r.Get(ctx, req.NamespacedName, tr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !tr.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, tr)
	}

	if !controllerutil.ContainsFinalizer(tr, replayFinalizerName) {
		controllerutil.AddFinalizer(tr, replayFinalizerName)
		if err := r.Update(ctx, tr); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Terminal replays stay terminal.
	switch tr.Status.Phase {
	case capturev1alpha1.TrafficReplayPhaseCompleted, capturev1alpha1.TrafficReplayPhaseFailed:
		return ctrl.Result{}, nil
	}

	// Target safety gate (defense in depth: the hub already checked the
	// load-test target, but user-created replays and stale directives are
	// checked here before any worker exists).
	if tr.Spec.Safety != nil && !safety.HostAllowed(tr.Spec.Target.Host, tr.Spec.Safety.AllowedHosts) {
		logger.Info("replay target denied by safety allowlist",
			"target", tr.Spec.Target.Host, "allowedHosts", tr.Spec.Safety.AllowedHosts)
		return r.setPhase(ctx, tr, capturev1alpha1.TrafficReplayPhaseFailed,
			"TargetDenied", fmt.Sprintf("target host %q matches no pattern in spec.safety.allowedHosts %v",
				tr.Spec.Target.Host, tr.Spec.Safety.AllowedHosts))
	}

	// Resolve the storage backend the replay reads from.
	storage := &capturev1alpha1.CaptureStorage{}
	storageKey := types.NamespacedName{Namespace: tr.Namespace, Name: string(tr.Spec.StorageRef.Name)}
	if err := r.Get(ctx, storageKey, storage); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("CaptureStorage not found, waiting", "storage", storageKey)
			return r.setPhase(ctx, tr, capturev1alpha1.TrafficReplayPhasePending,
				"StorageNotFound", fmt.Sprintf("CaptureStorage %s not found", storageKey))
		}
		return ctrl.Result{}, err
	}

	// Ensure the replay Job exists.
	job := &batchv1.Job{}
	jobKey := types.NamespacedName{Namespace: tr.Namespace, Name: ReplayJobName(tr)}
	err := r.Get(ctx, jobKey, job)
	if apierrors.IsNotFound(err) {
		job, buildErr := BuildReplayJob(tr, storage)
		if buildErr != nil {
			return r.setPhase(ctx, tr, capturev1alpha1.TrafficReplayPhaseFailed,
				"InvalidSpec", buildErr.Error())
		}
		if err := controllerutil.SetControllerReference(tr, job, r.Scheme); err != nil {
			return ctrl.Result{}, fmt.Errorf("set owner reference on Job: %w", err)
		}
		if err := r.Create(ctx, job); err != nil {
			return ctrl.Result{}, fmt.Errorf("create replay Job: %w", err)
		}
		logger.Info("created replay job", "job", jobKey)
		return r.setPhase(ctx, tr, capturev1alpha1.TrafficReplayPhasePending,
			"JobCreated", "replay job created")
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	// Advance phase from Job state.
	switch {
	case job.Status.Succeeded > 0:
		report := r.scrapeRunReport(ctx, job)
		r.applyReport(tr, report)
		now := metav1.Now()
		tr.Status.Phase = capturev1alpha1.TrafficReplayPhaseCompleted
		tr.Status.CompletionTime = &now
		setCondition(&tr.Status.Conditions, metav1.Condition{
			Type:               "Complete",
			Status:             metav1.ConditionTrue,
			Reason:             "ReplayFinished",
			Message:            "replay job completed",
			LastTransitionTime: now,
		})
	case jobFailed(job):
		report := r.scrapeRunReport(ctx, job)
		r.applyReport(tr, report)
		now := metav1.Now()
		tr.Status.Phase = capturev1alpha1.TrafficReplayPhaseFailed
		tr.Status.CompletionTime = &now
		setCondition(&tr.Status.Conditions, metav1.Condition{
			Type:               "Complete",
			Status:             metav1.ConditionFalse,
			Reason:             "ReplayFailed",
			Message:            jobFailureMessage(job),
			LastTransitionTime: now,
		})
	case job.Status.Active > 0:
		tr.Status.Phase = capturev1alpha1.TrafficReplayPhaseRunning
		if tr.Status.StartTime == nil {
			now := metav1.Now()
			tr.Status.StartTime = &now
		}
	default:
		tr.Status.Phase = capturev1alpha1.TrafficReplayPhasePending
	}

	if err := r.Status().Update(ctx, tr); err != nil {
		return ctrl.Result{}, err
	}

	r.reportToHub(ctx, tr)

	switch tr.Status.Phase {
	case capturev1alpha1.TrafficReplayPhaseCompleted, capturev1alpha1.TrafficReplayPhaseFailed:
		return ctrl.Result{}, nil
	default:
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
}

func (r *TrafficReplayReconciler) reconcileDelete(ctx context.Context, tr *capturev1alpha1.TrafficReplay) (ctrl.Result, error) {
	// Delete the Job (and its pods) if it is still around. Owned resources
	// would be garbage collected anyway, but foreground deletion here stops
	// in-flight load promptly.
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name:      ReplayJobName(tr),
		Namespace: tr.Namespace,
	}}
	propagation := metav1.DeletePropagationBackground
	if err := r.Delete(ctx, job, &client.DeleteOptions{PropagationPolicy: &propagation}); client.IgnoreNotFound(err) != nil {
		return ctrl.Result{}, fmt.Errorf("delete replay Job: %w", err)
	}

	controllerutil.RemoveFinalizer(tr, replayFinalizerName)
	if err := r.Update(ctx, tr); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *TrafficReplayReconciler) setPhase(ctx context.Context, tr *capturev1alpha1.TrafficReplay, phase capturev1alpha1.TrafficReplayPhase, reason, message string) (ctrl.Result, error) {
	tr.Status.Phase = phase
	setCondition(&tr.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             conditionStatus(phase == capturev1alpha1.TrafficReplayPhaseRunning),
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.Now(),
	})
	if err := r.Status().Update(ctx, tr); err != nil {
		return ctrl.Result{}, err
	}

	r.reportToHub(ctx, tr)

	if phase == capturev1alpha1.TrafficReplayPhaseFailed {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

// scrapeRunReport reads the RunReport JSON the replay-engine wrote to its
// termination log. Returns nil when no report is available (e.g. the pod
// was OOM-killed before writing it).
func (r *TrafficReplayReconciler) scrapeRunReport(ctx context.Context, job *batchv1.Job) *replay.RunReport {
	logger := log.FromContext(ctx)

	pods := &corev1.PodList{}
	if err := r.List(ctx, pods,
		client.InNamespace(job.Namespace),
		client.MatchingLabels{"job-name": job.Name},
	); err != nil {
		logger.Error(err, "failed to list replay pods for run report")
		return nil
	}

	for i := range pods.Items {
		pod := &pods.Items[i]
		for _, cs := range pod.Status.ContainerStatuses {
			term := cs.State.Terminated
			if term == nil || term.Message == "" {
				continue
			}
			report, err := replay.ParseRunReport([]byte(term.Message))
			if err != nil {
				logger.Info("could not parse replay run report from termination message",
					"pod", pod.Name, "error", err.Error())
				continue
			}
			return report
		}
	}
	return nil
}

// applyReport copies final counts and latencies onto the TrafficReplay status.
func (r *TrafficReplayReconciler) applyReport(tr *capturev1alpha1.TrafficReplay, report *replay.RunReport) {
	if report == nil {
		return
	}
	tr.Status.TotalRequests = report.TotalRequests + report.FilteredRequests
	tr.Status.SentRequests = report.SentRequests
	tr.Status.FailedRequests = report.FailedRequests
	tr.Status.FilteredRequests = report.FilteredRequests

	mean := replay.LatencyString(report.MeanLatencyMs)
	p50 := replay.LatencyString(report.P50LatencyMs)
	p95 := replay.LatencyString(report.P95LatencyMs)
	p99 := replay.LatencyString(report.P99LatencyMs)
	tr.Status.MeanLatency = &mean
	tr.Status.P50Latency = &p50
	tr.Status.P95Latency = &p95
	tr.Status.P99Latency = &p99

	if report.AchievedRPS > 0 {
		rps := strconv.FormatFloat(report.AchievedRPS, 'f', 1, 64)
		tr.Status.AchievedRPS = &rps
	}
}

// reportToHub sends this shard's status to the hub (best-effort).
func (r *TrafficReplayReconciler) reportToHub(ctx context.Context, tr *capturev1alpha1.TrafficReplay) {
	if r.HubClient == nil || !r.HubClient.IsConnected() {
		return
	}
	logger := log.FromContext(ctx)
	summary := ReplaySummaryFromStatus(tr)
	if summary == nil {
		return
	}
	if err := r.HubClient.ReportReplayStatus(ctx, []*hubv1.ReplayStatusSummary{summary}); err != nil {
		logger.Error(err, "failed to report replay status to hub (best-effort)")
	}
}

// ReplaySummaryFromStatus converts a TrafficReplay into the proto summary
// sent to the hub. Returns nil for replays that do not belong to a load
// test (standalone replays are not hub-coordinated).
func ReplaySummaryFromStatus(tr *capturev1alpha1.TrafficReplay) *hubv1.ReplayStatusSummary {
	if tr.Spec.LoadTestRef == nil {
		return nil
	}

	phase := hubv1.ReplayPhase_REPLAY_PHASE_UNSPECIFIED
	switch tr.Status.Phase {
	case capturev1alpha1.TrafficReplayPhasePending:
		phase = hubv1.ReplayPhase_REPLAY_PHASE_PENDING
	case capturev1alpha1.TrafficReplayPhaseRunning:
		phase = hubv1.ReplayPhase_REPLAY_PHASE_RUNNING
	case capturev1alpha1.TrafficReplayPhaseCompleted:
		phase = hubv1.ReplayPhase_REPLAY_PHASE_COMPLETED
	case capturev1alpha1.TrafficReplayPhaseFailed:
		phase = hubv1.ReplayPhase_REPLAY_PHASE_FAILED
	}

	summary := &hubv1.ReplayStatusSummary{
		LoadTestName:      tr.Spec.LoadTestRef.Name,
		LoadTestNamespace: tr.Namespace,
		ReplayName:        tr.Name,
		Phase:             phase,
		TotalRequests:     tr.Status.TotalRequests,
		SentRequests:      tr.Status.SentRequests,
		FailedRequests:    tr.Status.FailedRequests,
		FilteredRequests:  tr.Status.FilteredRequests,
	}
	if tr.Spec.Shard != nil {
		summary.ShardIndex = tr.Spec.Shard.Index
		summary.ShardCount = tr.Spec.Shard.Count
	}
	if tr.Status.AchievedRPS != nil {
		if rps, err := strconv.ParseFloat(*tr.Status.AchievedRPS, 64); err == nil {
			summary.AchievedRps = rps
		}
	}
	summary.P50LatencyMs = latencyMs(tr.Status.P50Latency)
	summary.P95LatencyMs = latencyMs(tr.Status.P95Latency)
	summary.P99LatencyMs = latencyMs(tr.Status.P99Latency)

	return summary
}

func latencyMs(latency *string) int64 {
	if latency == nil {
		return 0
	}
	d, err := time.ParseDuration(*latency)
	if err != nil {
		return 0
	}
	return d.Milliseconds()
}

func jobFailed(job *batchv1.Job) bool {
	for _, cond := range job.Status.Conditions {
		if cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func jobFailureMessage(job *batchv1.Job) string {
	for _, cond := range job.Status.Conditions {
		if cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue {
			if cond.Message != "" {
				return cond.Message
			}
			return string(cond.Reason)
		}
	}
	return "replay job failed"
}
