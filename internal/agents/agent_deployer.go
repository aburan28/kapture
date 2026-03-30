package agents

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
)

const (
	defaultCaptureAgentImage       = "kapture/capture-agent:latest"
	defaultStorageSidecarImage     = "kapture/storage-sidecar:latest"
	defaultReplicas          int32 = 1
	defaultMinReplicas       int32 = 1
	defaultMaxReplicas       int32 = 10
	defaultTargetCPU         int32 = 70

	labelApp              = "app.kubernetes.io/name"
	labelInstance         = "app.kubernetes.io/instance"
	labelComponent        = "app.kubernetes.io/component"
	labelManagedBy        = "app.kubernetes.io/managed-by"
	componentCaptureAgent = "capture-agent"
	componentCapturePlugin = "capture-plugin"
	managedByValue        = "kapture"

	captureDataVolumeName = "capture-data"
	captureDataMountPath  = "/capture-data"
)

// CaptureAgentImage can be overridden at build time.
var CaptureAgentImage = defaultCaptureAgentImage

// StorageSidecarImage can be overridden at build time.
var StorageSidecarImage = defaultStorageSidecarImage

// BuildDeployment builds a Deployment for capture agent pods.
func BuildDeployment(tc *capturev1alpha1.TrafficCapture, storage *capturev1alpha1.CaptureStorage) *appsv1.Deployment {
	replicas := defaultReplicas
	if tc.Spec.Agent != nil && tc.Spec.Agent.Replicas != nil {
		replicas = *tc.Spec.Agent.Replicas
	}

	labels := agentLabels(tc)

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      AgentDeploymentName(tc),
			Namespace: tc.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "capture-agent",
							Image: CaptureAgentImage,
							Ports: []corev1.ContainerPort{
								{Name: "http", ContainerPort: 8080, Protocol: corev1.ProtocolTCP},
								{Name: "grpc", ContainerPort: 9090, Protocol: corev1.ProtocolTCP},
							},
							Env:       buildEnvVars(tc, storage),
							Resources: defaultResources(),
						},
					},
				},
			},
		},
	}
}

// BuildPluginDeployment builds a Deployment for a plugin-based capture. It
// deploys the user-provided plugin container alongside a storage sidecar that
// reads capture output from a shared volume and uploads to storage.
func BuildPluginDeployment(tc *capturev1alpha1.TrafficCapture, storage *capturev1alpha1.CaptureStorage, plugin *capturev1alpha1.CapturePlugin) *appsv1.Deployment {
	replicas := defaultReplicas
	if tc.Spec.Agent != nil && tc.Spec.Agent.Replicas != nil {
		replicas = *tc.Spec.Agent.Replicas
	}

	labels := pluginLabels(tc)

	// Determine the output path the plugin writes to.
	outputPath := captureDataMountPath
	if plugin.Spec.Output.Path != "" {
		outputPath = plugin.Spec.Output.Path
	}

	// Build plugin container.
	pluginContainer := corev1.Container{
		Name:  "capture-plugin",
		Image: plugin.Spec.Container.Image,
		VolumeMounts: []corev1.VolumeMount{
			{Name: captureDataVolumeName, MountPath: outputPath},
		},
	}
	if len(plugin.Spec.Container.Command) > 0 {
		pluginContainer.Command = plugin.Spec.Container.Command
	}
	if len(plugin.Spec.Container.Args) > 0 {
		pluginContainer.Args = plugin.Spec.Container.Args
	}
	if len(plugin.Spec.Container.Env) > 0 {
		pluginContainer.Env = append(pluginContainer.Env, plugin.Spec.Container.Env...)
	}
	// Inject capture metadata as env vars.
	pluginContainer.Env = append(pluginContainer.Env,
		corev1.EnvVar{Name: "CAPTURE_ID", Value: fmt.Sprintf("%s/%s", tc.Namespace, tc.Name)},
		corev1.EnvVar{Name: "CAPTURE_OUTPUT_DIR", Value: outputPath},
		corev1.EnvVar{Name: "CAPTURE_OUTPUT_FORMAT", Value: string(plugin.Spec.Output.Format)},
	)
	if plugin.Spec.Container.Resources != nil {
		pluginContainer.Resources = *plugin.Spec.Container.Resources
	} else {
		pluginContainer.Resources = defaultResources()
	}
	if plugin.Spec.Container.SecurityContext != nil {
		pluginContainer.SecurityContext = plugin.Spec.Container.SecurityContext
	}

	// Add ports for mirror-mode plugins.
	for _, p := range plugin.Spec.Ports {
		pluginContainer.Ports = append(pluginContainer.Ports, corev1.ContainerPort{
			Name:          p.Name,
			ContainerPort: p.ContainerPort,
			Protocol:      p.Protocol,
		})
	}

	// Build storage sidecar container.
	sidecarContainer := corev1.Container{
		Name:  "storage-sidecar",
		Image: StorageSidecarImage,
		Args: []string{
			fmt.Sprintf("--watch-dir=%s", outputPath),
			fmt.Sprintf("--format=%s", string(plugin.Spec.Output.Format)),
			fmt.Sprintf("--storage-type=%s", string(storage.Spec.Type)),
			fmt.Sprintf("--capture-id=%s/%s", tc.Namespace, tc.Name),
			"--delete-after-upload=true",
		},
		Env: buildEnvVars(tc, storage),
		VolumeMounts: []corev1.VolumeMount{
			{Name: captureDataVolumeName, MountPath: outputPath},
		},
		Resources: sidecarResources(),
	}

	// Build volumes.
	volumes := []corev1.Volume{
		{
			Name: captureDataVolumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
	}

	// Add user-specified volumes.
	for _, v := range plugin.Spec.Volumes {
		vol := corev1.Volume{Name: v.Name}
		switch {
		case v.EmptyDir != nil:
			vol.VolumeSource = corev1.VolumeSource{EmptyDir: v.EmptyDir}
		case v.HostPath != nil:
			vol.VolumeSource = corev1.VolumeSource{HostPath: v.HostPath}
		case v.ConfigMap != nil:
			vol.VolumeSource = corev1.VolumeSource{ConfigMap: v.ConfigMap}
		case v.Secret != nil:
			vol.VolumeSource = corev1.VolumeSource{Secret: v.Secret}
		}
		volumes = append(volumes, vol)
	}

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      AgentDeploymentName(tc),
			Namespace: tc.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{pluginContainer, sidecarContainer},
					Volumes:    volumes,
				},
			},
		},
	}
}

// BuildPluginService builds a ClusterIP Service for a plugin-based capture.
// For mirror-mode plugins, this exposes the plugin's ports. For standalone
// plugins, this is a no-op (returns nil).
func BuildPluginService(tc *capturev1alpha1.TrafficCapture, plugin *capturev1alpha1.CapturePlugin) *corev1.Service {
	if plugin.Spec.Mode == capturev1alpha1.CapturePluginModeStandalone {
		return nil
	}
	labels := pluginLabels(tc)
	var ports []corev1.ServicePort
	for _, p := range plugin.Spec.Ports {
		ports = append(ports, corev1.ServicePort{
			Name:     p.Name,
			Port:     p.ContainerPort,
			Protocol: p.Protocol,
		})
	}
	// Default ports if none specified for mirror mode.
	if len(ports) == 0 {
		ports = []corev1.ServicePort{
			{Name: "http", Port: 8080, Protocol: corev1.ProtocolTCP},
			{Name: "grpc", Port: 9090, Protocol: corev1.ProtocolTCP},
		}
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      AgentServiceName(tc),
			Namespace: tc.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: labels,
			Ports:    ports,
		},
	}
}

func pluginLabels(tc *capturev1alpha1.TrafficCapture) map[string]string {
	return map[string]string{
		labelApp:       componentCapturePlugin,
		labelInstance:  tc.Name,
		labelComponent: componentCapturePlugin,
		labelManagedBy: managedByValue,
	}
}

func sidecarResources() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("50m"),
			corev1.ResourceMemory: resource.MustParse("64Mi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("200m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
	}
}

// BuildService builds a ClusterIP Service for the capture agent.
func BuildService(tc *capturev1alpha1.TrafficCapture) *corev1.Service {
	labels := agentLabels(tc)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      AgentServiceName(tc),
			Namespace: tc.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: labels,
			Ports: []corev1.ServicePort{
				{Name: "http", Port: 8080, Protocol: corev1.ProtocolTCP},
				{Name: "grpc", Port: 9090, Protocol: corev1.ProtocolTCP},
			},
		},
	}
}

// BuildHPA builds a HorizontalPodAutoscaler for the capture agent Deployment.
func BuildHPA(tc *capturev1alpha1.TrafficCapture) *autoscalingv2.HorizontalPodAutoscaler {
	minReplicas := defaultMinReplicas
	maxReplicas := defaultMaxReplicas
	targetCPU := defaultTargetCPU

	if tc.Spec.Agent != nil {
		if tc.Spec.Agent.MinReplicas != nil {
			minReplicas = *tc.Spec.Agent.MinReplicas
		}
		if tc.Spec.Agent.MaxReplicas != nil {
			maxReplicas = *tc.Spec.Agent.MaxReplicas
		}
		if tc.Spec.Agent.TargetCPU != nil {
			targetCPU = *tc.Spec.Agent.TargetCPU
		}
	}

	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-capture-agent", tc.Name),
			Namespace: tc.Namespace,
			Labels:    agentLabels(tc),
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       AgentDeploymentName(tc),
			},
			MinReplicas: &minReplicas,
			MaxReplicas: maxReplicas,
			Metrics: []autoscalingv2.MetricSpec{
				{
					Type: autoscalingv2.ResourceMetricSourceType,
					Resource: &autoscalingv2.ResourceMetricSource{
						Name: corev1.ResourceCPU,
						Target: autoscalingv2.MetricTarget{
							Type:               autoscalingv2.UtilizationMetricType,
							AverageUtilization: &targetCPU,
						},
					},
				},
			},
		},
	}
}

func agentLabels(tc *capturev1alpha1.TrafficCapture) map[string]string {
	return map[string]string{
		labelApp:       componentCaptureAgent,
		labelInstance:  tc.Name,
		labelComponent: componentCaptureAgent,
		labelManagedBy: managedByValue,
	}
}

func buildEnvVars(tc *capturev1alpha1.TrafficCapture, storage *capturev1alpha1.CaptureStorage) []corev1.EnvVar {
	envs := []corev1.EnvVar{
		{Name: "CAPTURE_ID", Value: fmt.Sprintf("%s/%s", tc.Namespace, tc.Name)},
		{Name: "STORAGE_TYPE", Value: string(storage.Spec.Type)},
	}

	switch storage.Spec.Type {
	case capturev1alpha1.CaptureStorageTypeS3:
		if storage.Spec.S3 != nil {
			envs = append(envs,
				corev1.EnvVar{Name: "S3_BUCKET", Value: storage.Spec.S3.Bucket},
				corev1.EnvVar{Name: "S3_REGION", Value: storage.Spec.S3.Region},
			)
			if storage.Spec.S3.Prefix != nil {
				envs = append(envs, corev1.EnvVar{Name: "S3_PREFIX", Value: *storage.Spec.S3.Prefix})
			}
		}
	case capturev1alpha1.CaptureStorageTypeGCS:
		if storage.Spec.GCS != nil {
			envs = append(envs, corev1.EnvVar{Name: "GCS_BUCKET", Value: storage.Spec.GCS.Bucket})
			if storage.Spec.GCS.Prefix != nil {
				envs = append(envs, corev1.EnvVar{Name: "GCS_PREFIX", Value: *storage.Spec.GCS.Prefix})
			}
		}
	case capturev1alpha1.CaptureStorageTypeEFS:
		if storage.Spec.EFS != nil {
			envs = append(envs,
				corev1.EnvVar{Name: "EFS_FILESYSTEM_ID", Value: storage.Spec.EFS.FileSystemID},
				corev1.EnvVar{Name: "EFS_MOUNT_PATH", Value: storage.Spec.EFS.MountPath},
			)
		}
	case capturev1alpha1.CaptureStorageTypeEBS:
		if storage.Spec.EBS != nil {
			envs = append(envs,
				corev1.EnvVar{Name: "EBS_VOLUME_ID", Value: storage.Spec.EBS.VolumeID},
				corev1.EnvVar{Name: "EBS_MOUNT_PATH", Value: storage.Spec.EBS.MountPath},
			)
		}
	case capturev1alpha1.CaptureStorageTypePlugin:
		if storage.Spec.Plugin != nil {
			envs = append(envs,
				corev1.EnvVar{Name: "PLUGIN_PATH", Value: storage.Spec.Plugin.Path},
			)
			if storage.Spec.Plugin.Symbol != nil {
				envs = append(envs, corev1.EnvVar{Name: "PLUGIN_SYMBOL", Value: *storage.Spec.Plugin.Symbol})
			}
			if storage.Spec.Plugin.Config != nil && len(storage.Spec.Plugin.Config.Raw) > 0 {
				envs = append(envs, corev1.EnvVar{Name: "PLUGIN_CONFIG", Value: string(storage.Spec.Plugin.Config.Raw)})
			}
		}
	}

	// Capture settings
	if tc.Spec.Capture.IncludeHeaders != nil {
		envs = append(envs, corev1.EnvVar{Name: "CAPTURE_INCLUDE_HEADERS", Value: fmt.Sprintf("%v", *tc.Spec.Capture.IncludeHeaders)})
	}
	if tc.Spec.Capture.IncludeBody != nil {
		envs = append(envs, corev1.EnvVar{Name: "CAPTURE_INCLUDE_BODY", Value: fmt.Sprintf("%v", *tc.Spec.Capture.IncludeBody)})
	}
	if tc.Spec.Capture.MaxBodyBytes != nil {
		envs = append(envs, corev1.EnvVar{Name: "CAPTURE_MAX_BODY_BYTES", Value: fmt.Sprintf("%d", *tc.Spec.Capture.MaxBodyBytes)})
	}

	return envs
}

func defaultResources() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("128Mi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("512Mi"),
		},
	}
}
