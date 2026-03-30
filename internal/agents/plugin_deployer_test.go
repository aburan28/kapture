package agents

import (
	"testing"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func newTestPlugin(mode capturev1alpha1.CapturePluginMode) *capturev1alpha1.CapturePlugin {
	return &capturev1alpha1.CapturePlugin{
		ObjectMeta: metav1.ObjectMeta{Name: "test-plugin", Namespace: "default"},
		Spec: capturev1alpha1.CapturePluginSpec{
			Mode: mode,
			Container: capturev1alpha1.PluginContainerSpec{
				Image:   "my-org/tcpdump-capture:v1.0",
				Command: []string{"/capture"},
				Args:    []string{"--format=jsonl"},
				Env: []corev1.EnvVar{
					{Name: "CAPTURE_INTERFACE", Value: "eth0"},
				},
			},
			Ports: []capturev1alpha1.PluginPortSpec{
				{Name: "http", ContainerPort: 8080, Protocol: corev1.ProtocolTCP},
			},
			Output: capturev1alpha1.PluginOutputSpec{
				Type:   capturev1alpha1.PluginOutputTypeVolume,
				Path:   "/capture-data",
				Format: capturev1alpha1.PluginOutputFormatJSONL,
			},
		},
		Status: capturev1alpha1.CapturePluginStatus{
			Phase: capturev1alpha1.CapturePluginPhaseReady,
		},
	}
}

func TestBuildPluginDeployment(t *testing.T) {
	tc := newTestTrafficCapture("capture-1", "default")
	storage := newTestStorage()
	plugin := newTestPlugin(capturev1alpha1.CapturePluginModeStandalone)

	deploy := BuildPluginDeployment(tc, storage, plugin)

	if deploy.Name != "capture-1-capture-agent" {
		t.Errorf("expected deployment name capture-1-capture-agent, got %s", deploy.Name)
	}
	if deploy.Namespace != "default" {
		t.Errorf("expected namespace default, got %s", deploy.Namespace)
	}

	// Should have 2 containers: plugin + sidecar.
	containers := deploy.Spec.Template.Spec.Containers
	if len(containers) != 2 {
		t.Fatalf("expected 2 containers, got %d", len(containers))
	}

	// Plugin container.
	pluginC := containers[0]
	if pluginC.Name != "capture-plugin" {
		t.Errorf("expected container name capture-plugin, got %s", pluginC.Name)
	}
	if pluginC.Image != "my-org/tcpdump-capture:v1.0" {
		t.Errorf("expected plugin image, got %s", pluginC.Image)
	}
	if len(pluginC.Command) != 1 || pluginC.Command[0] != "/capture" {
		t.Errorf("expected command [/capture], got %v", pluginC.Command)
	}
	if len(pluginC.Args) != 1 || pluginC.Args[0] != "--format=jsonl" {
		t.Errorf("expected args [--format=jsonl], got %v", pluginC.Args)
	}

	// Plugin env should include user env + injected capture metadata.
	envMap := make(map[string]string)
	for _, env := range pluginC.Env {
		envMap[env.Name] = env.Value
	}
	if envMap["CAPTURE_INTERFACE"] != "eth0" {
		t.Errorf("expected CAPTURE_INTERFACE=eth0, got %s", envMap["CAPTURE_INTERFACE"])
	}
	if envMap["CAPTURE_ID"] != "default/capture-1" {
		t.Errorf("expected CAPTURE_ID=default/capture-1, got %s", envMap["CAPTURE_ID"])
	}
	if envMap["CAPTURE_OUTPUT_DIR"] != "/capture-data" {
		t.Errorf("expected CAPTURE_OUTPUT_DIR=/capture-data, got %s", envMap["CAPTURE_OUTPUT_DIR"])
	}

	// Plugin should have volume mount.
	if len(pluginC.VolumeMounts) != 1 || pluginC.VolumeMounts[0].Name != captureDataVolumeName {
		t.Errorf("expected capture-data volume mount on plugin container")
	}

	// Sidecar container.
	sidecarC := containers[1]
	if sidecarC.Name != "storage-sidecar" {
		t.Errorf("expected container name storage-sidecar, got %s", sidecarC.Name)
	}
	if sidecarC.Image != StorageSidecarImage {
		t.Errorf("expected sidecar image %s, got %s", StorageSidecarImage, sidecarC.Image)
	}

	// Sidecar should have volume mount.
	if len(sidecarC.VolumeMounts) != 1 || sidecarC.VolumeMounts[0].Name != captureDataVolumeName {
		t.Errorf("expected capture-data volume mount on sidecar container")
	}

	// Deployment should have the shared volume.
	volumes := deploy.Spec.Template.Spec.Volumes
	if len(volumes) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(volumes))
	}
	if volumes[0].Name != captureDataVolumeName {
		t.Errorf("expected volume name %s, got %s", captureDataVolumeName, volumes[0].Name)
	}
	if volumes[0].EmptyDir == nil {
		t.Error("expected emptyDir volume source")
	}
}

func TestBuildPluginDeploymentWithPorts(t *testing.T) {
	tc := newTestTrafficCapture("capture-1", "default")
	storage := newTestStorage()
	plugin := newTestPlugin(capturev1alpha1.CapturePluginModeMirror)

	deploy := BuildPluginDeployment(tc, storage, plugin)

	pluginC := deploy.Spec.Template.Spec.Containers[0]
	if len(pluginC.Ports) != 1 {
		t.Fatalf("expected 1 port, got %d", len(pluginC.Ports))
	}
	if pluginC.Ports[0].ContainerPort != 8080 {
		t.Errorf("expected port 8080, got %d", pluginC.Ports[0].ContainerPort)
	}
}

func TestBuildPluginServiceMirrorMode(t *testing.T) {
	tc := newTestTrafficCapture("capture-1", "default")
	plugin := newTestPlugin(capturev1alpha1.CapturePluginModeMirror)

	svc := BuildPluginService(tc, plugin)
	if svc == nil {
		t.Fatal("expected non-nil service for mirror mode plugin")
	}
	if svc.Name != "capture-1-capture-agent" {
		t.Errorf("expected service name capture-1-capture-agent, got %s", svc.Name)
	}
	if len(svc.Spec.Ports) != 1 {
		t.Fatalf("expected 1 port, got %d", len(svc.Spec.Ports))
	}
	if svc.Spec.Ports[0].Port != 8080 {
		t.Errorf("expected port 8080, got %d", svc.Spec.Ports[0].Port)
	}
}

func TestBuildPluginServiceStandaloneReturnsNil(t *testing.T) {
	tc := newTestTrafficCapture("capture-1", "default")
	plugin := newTestPlugin(capturev1alpha1.CapturePluginModeStandalone)

	svc := BuildPluginService(tc, plugin)
	if svc != nil {
		t.Error("expected nil service for standalone plugin")
	}
}

func TestBuildPluginDeploymentCustomReplicas(t *testing.T) {
	tc := newTestTrafficCapture("capture-1", "default")
	replicas := int32(3)
	tc.Spec.Agent = &capturev1alpha1.AgentScalingSpec{Replicas: &replicas}
	storage := newTestStorage()
	plugin := newTestPlugin(capturev1alpha1.CapturePluginModeStandalone)

	deploy := BuildPluginDeployment(tc, storage, plugin)
	if *deploy.Spec.Replicas != 3 {
		t.Errorf("expected 3 replicas, got %d", *deploy.Spec.Replicas)
	}
}
