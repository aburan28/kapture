package spoke

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
	"github.com/kapture-io/kapture/internal/storage"
)

const storageConditionReady = "Ready"

var validatePluginConfig = storage.ValidatePluginConfig

// +kubebuilder:rbac:groups=capture.gateway.io,resources=capturestorages,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=capture.gateway.io,resources=capturestorages/status,verbs=get;update;patch

// CaptureStorageReconciler validates storage backends and reports readiness.
type CaptureStorageReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

func (r *CaptureStorageReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	cs := &capturev1alpha1.CaptureStorage{}
	if err := r.Get(ctx, req.NamespacedName, cs); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if err := validateCaptureStorageSpec(cs.Spec); err != nil {
		logger.Error(err, "capture storage validation failed", "captureStorage", req.NamespacedName)
		if statusErr := r.setStatus(ctx, cs, capturev1alpha1.CaptureStoragePhaseError, "ValidationFailed", err.Error(), metav1.ConditionFalse); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	if err := r.setStatus(ctx, cs, capturev1alpha1.CaptureStoragePhaseReady, "Validated", "storage backend configuration validated", metav1.ConditionTrue); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *CaptureStorageReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&capturev1alpha1.CaptureStorage{}).
		Complete(r)
}

func (r *CaptureStorageReconciler) setStatus(ctx context.Context, cs *capturev1alpha1.CaptureStorage, phase capturev1alpha1.CaptureStoragePhase, reason, message string, status metav1.ConditionStatus) error {
	cs.Status.Phase = phase
	setCondition(&cs.Status.Conditions, metav1.Condition{
		Type:               storageConditionReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: cs.Generation,
		LastTransitionTime: metav1.Now(),
	})
	return r.Status().Update(ctx, cs)
}

func validateCaptureStorageSpec(spec capturev1alpha1.CaptureStorageSpec) error {
	switch spec.Type {
	case capturev1alpha1.CaptureStorageTypeS3:
		if spec.S3 == nil || strings.TrimSpace(spec.S3.Bucket) == "" || strings.TrimSpace(spec.S3.Region) == "" {
			return fmt.Errorf("s3 configuration is incomplete")
		}
	case capturev1alpha1.CaptureStorageTypeGCS:
		if spec.GCS == nil || strings.TrimSpace(spec.GCS.Bucket) == "" {
			return fmt.Errorf("gcs configuration is incomplete")
		}
	case capturev1alpha1.CaptureStorageTypeEFS:
		if spec.EFS == nil || strings.TrimSpace(spec.EFS.FileSystemID) == "" || strings.TrimSpace(spec.EFS.MountPath) == "" {
			return fmt.Errorf("efs configuration is incomplete")
		}
	case capturev1alpha1.CaptureStorageTypeEBS:
		if spec.EBS == nil || strings.TrimSpace(spec.EBS.VolumeID) == "" || strings.TrimSpace(spec.EBS.MountPath) == "" {
			return fmt.Errorf("ebs configuration is incomplete")
		}
	case capturev1alpha1.CaptureStorageTypePlugin:
		if spec.Plugin == nil || strings.TrimSpace(spec.Plugin.Path) == "" {
			return fmt.Errorf("plugin configuration requires plugin.path")
		}

		cfg := storage.PluginConfig{Path: spec.Plugin.Path}
		if spec.Plugin.Symbol != nil {
			cfg.Symbol = strings.TrimSpace(*spec.Plugin.Symbol)
		}
		if spec.Plugin.Config != nil && len(spec.Plugin.Config.Raw) > 0 {
			m := map[string]any{}
			if err := json.Unmarshal(spec.Plugin.Config.Raw, &m); err != nil {
				return fmt.Errorf("decode plugin config: %w", err)
			}
			cfg.Config = m
		}

		if err := validatePluginConfig(cfg); err != nil {
			return fmt.Errorf("validate plugin backend: %w", err)
		}
	default:
		return fmt.Errorf("unsupported storage type %q", spec.Type)
	}

	return nil
}
