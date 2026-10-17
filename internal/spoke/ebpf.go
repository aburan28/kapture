package spoke

import (
	"context"
	"fmt"
	"os"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
)

func isEBPF(tc *capturev1alpha1.TrafficCapture) bool {
	return tc.Spec.Backend == capturev1alpha1.CaptureBackendEBPF
}

func (r *TrafficCaptureReconciler) reconcileEBPF(ctx context.Context, tc *capturev1alpha1.TrafficCapture, storage *capturev1alpha1.CaptureStorage) (ctrl.Result, error) {
	invalid := func(reason, message string) (ctrl.Result, error) {
		return r.setPhaseWithCondition(ctx, tc, capturev1alpha1.TrafficCapturePhaseFailed, reason, message)
	}
	if os.Getenv("EBPF_ENABLED") != "true" {
		return invalid("EBPFDisabled", "enable ebpf.enabled in the spoke Helm release before using the EBPF backend")
	}
	if tc.Spec.TargetRef.Group != "" || tc.Spec.TargetRef.Kind != "Pod" {
		return invalid("InvalidEBPFTarget", "EBPF requires a Pod target")
	}
	if len(tc.Spec.Plugins) > 0 {
		return invalid("UnsupportedEBPFPlugins", "payload plugins are not supported for evidence bundles")
	}
	if storage.Spec.Type == capturev1alpha1.CaptureStorageTypePlugin {
		return invalid("UnsupportedEBPFStorage", "EBPF requires a backend supporting evidence artifacts")
	}
	var secret *gwapiv1.SecretObjectReference
	if storage.Spec.S3 != nil {
		secret = &storage.Spec.S3.CredentialsSecretRef
	}
	if storage.Spec.GCS != nil {
		secret = &storage.Spec.GCS.CredentialsSecretRef
	}
	if secret != nil && secret.Namespace != nil && string(*secret.Namespace) != tc.Namespace {
		return invalid("UnsupportedEBPFCredentials", "EBPF credential Secrets must be in the capture namespace")
	}
	pod := &corev1.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: tc.Namespace, Name: string(tc.Spec.TargetRef.Name)}, pod); err != nil {
		return r.setPhaseWithCondition(ctx, tc, capturev1alpha1.TrafficCapturePhasePending, "TargetPodUnavailable", fmt.Sprintf("target Pod is unavailable: %v", err))
	}
	if pod.Spec.NodeName == "" || pod.UID == "" {
		return r.setPhaseWithCondition(ctx, tc, capturev1alpha1.TrafficCapturePhasePending, "TargetPodUnscheduled", "waiting for target Pod scheduling")
	}
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return r.completeCapture(ctx, tc)
	}
	deploy := BuildEBPFDeployment(tc, storage, pod)
	if err := controllerutil.SetControllerReference(tc, deploy, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.createOrUpdate(ctx, deploy); err != nil {
		return ctrl.Result{}, err
	}
	result, err := r.updateStatus(ctx, tc)
	if err == nil {
		r.reportToHub(ctx, tc)
	}
	return result, err
}

// BuildEBPFDeployment pins a single collector to the workload's node. Recreate
// avoids overlapping sessions during updates; readiness reflects probe attachment.
func BuildEBPFDeployment(tc *capturev1alpha1.TrafficCapture, storage *capturev1alpha1.CaptureStorage, target *corev1.Pod) *appsv1.Deployment {
	d := BuildDeployment(tc, storage)
	replicas := int32(1)
	d.Spec.Replicas = &replicas
	d.Spec.Strategy = appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType}
	spec := &d.Spec.Template.Spec
	spec.NodeName = target.Spec.NodeName
	spec.HostPID = true
	spec.ServiceAccountName = os.Getenv("EBPF_SERVICE_ACCOUNT")
	grace := int64(30)
	spec.TerminationGracePeriodSeconds = &grace
	c := &spec.Containers[0]
	c.Image = os.Getenv("EBPF_AGENT_IMAGE")
	if c.Image == "" {
		c.Image = "kapture/ebpf-agent:dev"
	}
	privileged, nonRoot := true, false
	root := int64(0)
	c.SecurityContext = &corev1.SecurityContext{Privileged: &privileged, RunAsUser: &root, RunAsNonRoot: &nonRoot}
	c.Ports = []corev1.ContainerPort{{Name: "health", ContainerPort: 8081}}
	c.ReadinessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromInt32(8081)}}, PeriodSeconds: 2, FailureThreshold: 2}
	c.Env = append(c.Env, corev1.EnvVar{Name: "CAPTURE_BACKEND", Value: "EBPF"}, corev1.EnvVar{Name: "TARGET_POD_UID", Value: string(target.UID)})
	if tc.Spec.EBPF != nil {
		o := tc.Spec.EBPF
		add := func(name string, value int64) {
			if value != 0 {
				c.Env = append(c.Env, corev1.EnvVar{Name: name, Value: strconv.FormatInt(value, 10)})
			}
		}
		if o.PID != nil {
			add("ECAPTURE_PID", int64(*o.PID))
		}
		add("ECAPTURE_MAP_PAGES", int64(o.MapPages))
		add("L7_MEMORY_BYTES", o.MemoryBytes)
		add("L7_HISTORY_BYTES", o.HistoryBytes)
		add("L7_TRIGGER_STATUS", int64(o.TriggerStatus))
		if o.LibraryPath != "" {
			c.Env = append(c.Env, corev1.EnvVar{Name: "ECAPTURE_LIBRARY", Value: o.LibraryPath})
		}
		if o.History != "" {
			c.Env = append(c.Env, corev1.EnvVar{Name: "L7_HISTORY", Value: o.History})
		}
		if o.PCAP != nil {
			c.Env = append(c.Env, corev1.EnvVar{Name: "L7_PCAP", Value: strconv.FormatBool(*o.PCAP)})
		}
	}
	directory := corev1.HostPathDirectory
	spec.Volumes = []corev1.Volume{{Name: "host-sys", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/sys", Type: &directory}}}}
	c.VolumeMounts = []corev1.VolumeMount{{Name: "host-sys", MountPath: "/sys", ReadOnly: true}}
	mount := ""
	if storage.Spec.EFS != nil {
		mount = storage.Spec.EFS.MountPath
	}
	if storage.Spec.EBS != nil {
		mount = storage.Spec.EBS.MountPath
	}
	if mount != "" {
		create := corev1.HostPathDirectoryOrCreate
		spec.Volumes = append(spec.Volumes, corev1.Volume{Name: "capture-storage", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: mount, Type: &create}}})
		c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{Name: "capture-storage", MountPath: mount})
	}
	if storage.Spec.S3 != nil && storage.Spec.S3.CredentialsSecretRef.Name != "" {
		c.EnvFrom = append(c.EnvFrom, corev1.EnvFromSource{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: string(storage.Spec.S3.CredentialsSecretRef.Name)}}})
	}
	if storage.Spec.GCS != nil && storage.Spec.GCS.CredentialsSecretRef.Name != "" {
		spec.Volumes = append(spec.Volumes, corev1.Volume{Name: "gcs-credentials", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: string(storage.Spec.GCS.CredentialsSecretRef.Name)}}})
		c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{Name: "gcs-credentials", MountPath: "/var/run/kapture-gcp", ReadOnly: true})
		c.Env = append(c.Env, corev1.EnvVar{Name: "GOOGLE_APPLICATION_CREDENTIALS", Value: "/var/run/kapture-gcp/credentials.json"})
	}
	return d
}
