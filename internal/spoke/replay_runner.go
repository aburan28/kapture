package spoke

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
)

const (
	defaultReplayEngineImage = "kapture/replay-engine:latest"

	componentReplayWorker = "replay-worker"

	// replayJobBackoffLimit retries a failed shard a couple of times before
	// the shard is marked failed. Restarted shards re-send data; that is
	// acceptable for load testing.
	replayJobBackoffLimit int32 = 2
)

// ReplayEngineImage can be overridden via the REPLAY_ENGINE_IMAGE env var in
// the spoke deployment.
var ReplayEngineImage = defaultReplayEngineImage

// ReplayJobName returns the Job name for a TrafficReplay.
func ReplayJobName(tr *capturev1alpha1.TrafficReplay) string {
	return fmt.Sprintf("%s-replay", tr.Name)
}

// BuildReplayJob builds the batch Job that runs the replay-engine for one
// TrafficReplay (typically one shard of a distributed load test).
func BuildReplayJob(tr *capturev1alpha1.TrafficReplay, storage *capturev1alpha1.CaptureStorage) (*batchv1.Job, error) {
	args, err := buildReplayArgs(tr, storage)
	if err != nil {
		return nil, err
	}

	labels := map[string]string{
		labelApp:       componentReplayWorker,
		labelInstance:  tr.Name,
		labelComponent: componentReplayWorker,
		labelManagedBy: managedByValue,
	}
	if loadTest, ok := tr.Labels[LabelLoadTest]; ok {
		labels[LabelLoadTest] = loadTest
	}
	if shard, ok := tr.Labels[LabelShardIndex]; ok {
		labels[LabelShardIndex] = shard
	}

	backoffLimit := replayJobBackoffLimit

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ReplayJobName(tr),
			Namespace: tr.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:      "replay-engine",
							Image:     ReplayEngineImage,
							Args:      args,
							Env:       replayEnv(),
							Resources: replayResources(),
						},
					},
				},
			},
		},
	}

	// Filesystem-backed storage needs the capture volume mounted where the
	// reader expects it.
	switch storage.Spec.Type {
	case capturev1alpha1.CaptureStorageTypeEFS:
		if storage.Spec.EFS != nil {
			mountCaptureVolume(job, storage.Spec.EFS.MountPath)
		}
	case capturev1alpha1.CaptureStorageTypeEBS:
		if storage.Spec.EBS != nil {
			mountCaptureVolume(job, storage.Spec.EBS.MountPath)
		}
	}

	return job, nil
}

// buildReplayArgs translates the TrafficReplay spec into replay-engine flags.
func buildReplayArgs(tr *capturev1alpha1.TrafficReplay, storage *capturev1alpha1.CaptureStorage) ([]string, error) {
	args := []string{
		"--capture-id", fmt.Sprintf("%s/%s", tr.Namespace, tr.Spec.SourceRef.Name),
		"--log-format", "json",
	}

	switch storage.Spec.Type {
	case capturev1alpha1.CaptureStorageTypeS3:
		if storage.Spec.S3 == nil {
			return nil, fmt.Errorf("storage %s has type S3 but no s3 config", storage.Name)
		}
		args = append(args, "--storage-type", "s3",
			"--bucket", storage.Spec.S3.Bucket,
			"--region", storage.Spec.S3.Region)
		if storage.Spec.S3.Prefix != nil && *storage.Spec.S3.Prefix != "" {
			args = append(args, "--prefix", *storage.Spec.S3.Prefix)
		}
	case capturev1alpha1.CaptureStorageTypeGCS:
		if storage.Spec.GCS == nil {
			return nil, fmt.Errorf("storage %s has type GCS but no gcs config", storage.Name)
		}
		args = append(args, "--storage-type", "gcs", "--bucket", storage.Spec.GCS.Bucket)
		if storage.Spec.GCS.Prefix != nil && *storage.Spec.GCS.Prefix != "" {
			args = append(args, "--prefix", *storage.Spec.GCS.Prefix)
		}
	case capturev1alpha1.CaptureStorageTypeEFS:
		if storage.Spec.EFS == nil {
			return nil, fmt.Errorf("storage %s has type EFS but no efs config", storage.Name)
		}
		args = append(args, "--storage-type", "efs", "--mount-path", storage.Spec.EFS.MountPath)
	case capturev1alpha1.CaptureStorageTypeEBS:
		if storage.Spec.EBS == nil {
			return nil, fmt.Errorf("storage %s has type EBS but no ebs config", storage.Name)
		}
		args = append(args, "--storage-type", "ebs", "--mount-path", storage.Spec.EBS.MountPath)
	default:
		return nil, fmt.Errorf("storage type %q is not supported for replay", storage.Spec.Type)
	}

	// Target URL.
	scheme := "http"
	port := int32(80)
	if tr.Spec.Target.TLS != nil && *tr.Spec.Target.TLS {
		scheme = "https"
		port = 443
	}
	if tr.Spec.Target.Port != nil {
		port = *tr.Spec.Target.Port
	}
	args = append(args, "--target-url", fmt.Sprintf("%s://%s:%d", scheme, tr.Spec.Target.Host, port))

	// Rate configuration.
	if rate := tr.Spec.Rate; rate != nil {
		switch rate.Mode {
		case capturev1alpha1.ReplayRateModeConstant:
			args = append(args, "--rate-mode", "constant")
			if rate.RequestsPerSecond != nil {
				args = append(args, "--rate-per-second", strconv.Itoa(int(*rate.RequestsPerSecond)))
			}
		case capturev1alpha1.ReplayRateModeOriginalTiming:
			args = append(args, "--rate-mode", "original")
			if rate.TimeScale != nil && *rate.TimeScale != "" {
				args = append(args, "--time-scale", *rate.TimeScale)
			}
		case capturev1alpha1.ReplayRateModeUnlimited:
			args = append(args, "--rate-mode", "unlimited")
		}
	} else {
		args = append(args, "--rate-mode", "original")
	}

	if tr.Spec.Concurrency != nil {
		args = append(args, "--concurrency", strconv.Itoa(int(*tr.Spec.Concurrency)))
	}

	// Sharding for distributed load tests.
	if shard := tr.Spec.Shard; shard != nil {
		args = append(args,
			"--shard-index", strconv.Itoa(int(shard.Index)),
			"--shard-count", strconv.Itoa(int(shard.Count)))
	}

	// Engine selection: non-builtin engines run as plugin subprocesses
	// discovered from the worker's plugin directory.
	if engine := tr.Spec.Engine; engine != nil && engine.Name != "" && engine.Name != "builtin" {
		args = append(args, "--engine", engine.Name)
		if engine.Config != nil && len(engine.Config.Raw) > 0 {
			args = append(args, "--engine-config", string(engine.Config.Raw))
		}
	}

	// Data filters.
	if f := tr.Spec.Filters; f != nil {
		if f.StartTime != nil {
			args = append(args, "--start-time", f.StartTime.UTC().Format(time.RFC3339))
		}
		if f.EndTime != nil {
			args = append(args, "--end-time", f.EndTime.UTC().Format(time.RFC3339))
		}
		if f.PathPrefix != nil && *f.PathPrefix != "" {
			args = append(args, "--path-prefix", *f.PathPrefix)
		}
		if len(f.Methods) > 0 {
			args = append(args, "--methods", strings.Join(f.Methods, ","))
		}
		if f.Limit != nil && *f.Limit > 0 {
			args = append(args, "--limit", strconv.FormatInt(*f.Limit, 10))
		}
	}

	// Write the final RunReport where Kubernetes surfaces it as the
	// container termination message.
	args = append(args, "--summary-path", "/dev/termination-log")

	return args, nil
}

func mountCaptureVolume(job *batchv1.Job, mountPath string) {
	volumeName := "capture-data"
	job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{
		Name: volumeName,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: volumeName,
				ReadOnly:  true,
			},
		},
	})
	job.Spec.Template.Spec.Containers[0].VolumeMounts = append(
		job.Spec.Template.Spec.Containers[0].VolumeMounts,
		corev1.VolumeMount{Name: volumeName, MountPath: mountPath, ReadOnly: true},
	)
}

// replayEnv passes the spoke's plugin directory configuration through to
// replay worker pods so external engines resolve their plugins from the
// same location.
func replayEnv() []corev1.EnvVar {
	if dir := os.Getenv("REPLAY_PLUGIN_DIR"); dir != "" {
		return []corev1.EnvVar{{Name: "REPLAY_PLUGIN_DIR", Value: dir}}
	}
	return nil
}

func replayResources() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("250m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("2"),
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		},
	}
}
