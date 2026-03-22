package spoke

import (
	"testing"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
)

// ---------------------------------------------------------------------------
// Helpers (using unique names to avoid conflict with existing test files)
// ---------------------------------------------------------------------------

func makeTC(name, ns string) *capturev1alpha1.TrafficCapture {
	return &capturev1alpha1.TrafficCapture{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: capturev1alpha1.TrafficCaptureSpec{
			TargetRef: gwapiv1.LocalPolicyTargetReference{
				Group: "gateway.networking.k8s.io",
				Kind:  "HTTPRoute",
				Name:  "test-route",
			},
			StorageRef: capturev1alpha1.CaptureStorageReference{
				Name: "test-storage",
			},
			Capture: capturev1alpha1.CaptureSettings{},
		},
	}
}

func makeS3Storage() *capturev1alpha1.CaptureStorage {
	return &capturev1alpha1.CaptureStorage{
		ObjectMeta: metav1.ObjectMeta{Name: "test-storage", Namespace: "default"},
		Spec: capturev1alpha1.CaptureStorageSpec{
			Type: capturev1alpha1.CaptureStorageTypeS3,
			S3: &capturev1alpha1.S3Config{
				Bucket: "my-bucket",
				Region: "us-east-1",
			},
		},
	}
}

func i32(v int32) *int32  { return &v }
func i64(v int64) *int64  { return &v }
func bl(v bool) *bool     { return &v }
func sp(v string) *string { return &v }

func findEnv(envs []corev1.EnvVar, name string) (string, bool) {
	for _, e := range envs {
		if e.Name == name {
			return e.Value, true
		}
	}
	return "", false
}

// ---------------------------------------------------------------------------
// BuildDeployment - comprehensive tests
// ---------------------------------------------------------------------------

func TestBuildDeployment_DefaultReplicasWhenAgentNil(t *testing.T) {
	tc := makeTC("tc1", "ns1")
	tc.Spec.Agent = nil
	dep := BuildDeployment(tc, makeS3Storage())

	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 1 {
		t.Fatalf("expected default replicas=1, got %v", dep.Spec.Replicas)
	}
}

func TestBuildDeployment_CustomReplicasFromAgent(t *testing.T) {
	tc := makeTC("tc1", "ns1")
	tc.Spec.Agent = &capturev1alpha1.AgentScalingSpec{Replicas: i32(7)}
	dep := BuildDeployment(tc, makeS3Storage())

	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 7 {
		t.Fatalf("expected replicas=7, got %v", dep.Spec.Replicas)
	}
}

func TestBuildDeployment_AgentSetButReplicasNil(t *testing.T) {
	tc := makeTC("tc1", "ns1")
	tc.Spec.Agent = &capturev1alpha1.AgentScalingSpec{} // Replicas is nil
	dep := BuildDeployment(tc, makeS3Storage())

	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 1 {
		t.Fatalf("expected default replicas=1 when Agent.Replicas is nil, got %v", dep.Spec.Replicas)
	}
}

func TestBuildDeployment_NameFormat(t *testing.T) {
	tc := makeTC("my-capture", "prod")
	dep := BuildDeployment(tc, makeS3Storage())

	if dep.Name != "my-capture-capture-agent" {
		t.Errorf("name = %q, want %q", dep.Name, "my-capture-capture-agent")
	}
	if dep.Namespace != "prod" {
		t.Errorf("namespace = %q, want %q", dep.Namespace, "prod")
	}
}

func TestBuildDeployment_AllLabelsCorrect(t *testing.T) {
	tc := makeTC("tc1", "ns1")
	dep := BuildDeployment(tc, makeS3Storage())

	want := map[string]string{
		"app.kubernetes.io/name":       "capture-agent",
		"app.kubernetes.io/instance":   "tc1",
		"app.kubernetes.io/component":  "capture-agent",
		"app.kubernetes.io/managed-by": "kapture",
	}

	// Deployment labels
	for k, v := range want {
		if dep.Labels[k] != v {
			t.Errorf("deployment label %s = %q, want %q", k, dep.Labels[k], v)
		}
	}
	// Selector matchLabels
	for k, v := range want {
		if dep.Spec.Selector.MatchLabels[k] != v {
			t.Errorf("selector label %s = %q, want %q", k, dep.Spec.Selector.MatchLabels[k], v)
		}
	}
	// Pod template labels
	for k, v := range want {
		if dep.Spec.Template.Labels[k] != v {
			t.Errorf("pod template label %s = %q, want %q", k, dep.Spec.Template.Labels[k], v)
		}
	}
}

func TestBuildDeployment_ContainerImageAndPorts(t *testing.T) {
	tc := makeTC("tc1", "ns1")
	dep := BuildDeployment(tc, makeS3Storage())

	if len(dep.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("expected 1 container, got %d", len(dep.Spec.Template.Spec.Containers))
	}
	c := dep.Spec.Template.Spec.Containers[0]

	if c.Name != "capture-agent" {
		t.Errorf("container name = %q, want %q", c.Name, "capture-agent")
	}
	if c.Image != CaptureAgentImage {
		t.Errorf("image = %q, want %q", c.Image, CaptureAgentImage)
	}
	if len(c.Ports) != 2 {
		t.Fatalf("expected 2 ports, got %d", len(c.Ports))
	}

	portMap := map[string]int32{}
	for _, p := range c.Ports {
		portMap[p.Name] = p.ContainerPort
	}
	if portMap["http"] != 8080 {
		t.Errorf("http port = %d, want 8080", portMap["http"])
	}
	if portMap["grpc"] != 9090 {
		t.Errorf("grpc port = %d, want 9090", portMap["grpc"])
	}
}

func TestBuildDeployment_DefaultResources(t *testing.T) {
	tc := makeTC("tc1", "ns1")
	dep := BuildDeployment(tc, makeS3Storage())
	c := dep.Spec.Template.Spec.Containers[0]

	checks := []struct {
		desc string
		got  *resource.Quantity
		want resource.Quantity
	}{
		{"CPU request", c.Resources.Requests.Cpu(), resource.MustParse("100m")},
		{"Memory request", c.Resources.Requests.Memory(), resource.MustParse("128Mi")},
		{"CPU limit", c.Resources.Limits.Cpu(), resource.MustParse("500m")},
		{"Memory limit", c.Resources.Limits.Memory(), resource.MustParse("512Mi")},
	}
	for _, ch := range checks {
		if !ch.got.Equal(ch.want) {
			t.Errorf("%s = %s, want %s", ch.desc, ch.got, &ch.want)
		}
	}
}

// ---------------------------------------------------------------------------
// BuildService - comprehensive tests
// ---------------------------------------------------------------------------

func TestBuildService_TypeClusterIP(t *testing.T) {
	tc := makeTC("tc1", "ns1")
	svc := BuildService(tc)

	if svc.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Errorf("type = %v, want ClusterIP", svc.Spec.Type)
	}
}

func TestBuildService_NameAndNamespace(t *testing.T) {
	tc := makeTC("my-cap", "prod")
	svc := BuildService(tc)

	if svc.Name != "my-cap-capture-agent" {
		t.Errorf("name = %q, want %q", svc.Name, "my-cap-capture-agent")
	}
	if svc.Namespace != "prod" {
		t.Errorf("namespace = %q, want %q", svc.Namespace, "prod")
	}
}

func TestBuildService_Ports(t *testing.T) {
	tc := makeTC("tc1", "ns1")
	svc := BuildService(tc)

	portMap := map[string]int32{}
	for _, p := range svc.Spec.Ports {
		portMap[p.Name] = p.Port
	}
	if portMap["http"] != 8080 {
		t.Errorf("http port = %d, want 8080", portMap["http"])
	}
	if portMap["grpc"] != 9090 {
		t.Errorf("grpc port = %d, want 9090", portMap["grpc"])
	}
}

func TestBuildService_SelectorMatchesAgentLabels(t *testing.T) {
	tc := makeTC("tc1", "ns1")
	svc := BuildService(tc)

	want := map[string]string{
		"app.kubernetes.io/name":       "capture-agent",
		"app.kubernetes.io/instance":   "tc1",
		"app.kubernetes.io/component":  "capture-agent",
		"app.kubernetes.io/managed-by": "kapture",
	}
	for k, v := range want {
		if svc.Spec.Selector[k] != v {
			t.Errorf("selector %s = %q, want %q", k, svc.Spec.Selector[k], v)
		}
	}
}

func TestBuildService_Labels(t *testing.T) {
	tc := makeTC("tc1", "ns1")
	svc := BuildService(tc)

	if svc.Labels["app.kubernetes.io/name"] != "capture-agent" {
		t.Errorf("label app name = %q, want %q", svc.Labels["app.kubernetes.io/name"], "capture-agent")
	}
	if svc.Labels["app.kubernetes.io/instance"] != "tc1" {
		t.Errorf("label instance = %q, want %q", svc.Labels["app.kubernetes.io/instance"], "tc1")
	}
}

// ---------------------------------------------------------------------------
// BuildHPA - comprehensive tests
// ---------------------------------------------------------------------------

func TestBuildHPA_DefaultsWhenAgentNil(t *testing.T) {
	tc := makeTC("tc1", "ns1")
	tc.Spec.Agent = nil
	hpa := BuildHPA(tc)

	if hpa.Spec.MinReplicas == nil || *hpa.Spec.MinReplicas != 1 {
		t.Errorf("minReplicas = %v, want 1", hpa.Spec.MinReplicas)
	}
	if hpa.Spec.MaxReplicas != 10 {
		t.Errorf("maxReplicas = %d, want 10", hpa.Spec.MaxReplicas)
	}
	if len(hpa.Spec.Metrics) != 1 {
		t.Fatalf("expected 1 metric, got %d", len(hpa.Spec.Metrics))
	}
	if hpa.Spec.Metrics[0].Resource.Target.AverageUtilization == nil ||
		*hpa.Spec.Metrics[0].Resource.Target.AverageUtilization != 70 {
		t.Errorf("targetCPU = %v, want 70", hpa.Spec.Metrics[0].Resource.Target.AverageUtilization)
	}
}

func TestBuildHPA_CustomMinMaxCPU(t *testing.T) {
	tc := makeTC("tc1", "ns1")
	tc.Spec.Agent = &capturev1alpha1.AgentScalingSpec{
		MinReplicas: i32(2),
		MaxReplicas: i32(25),
		TargetCPU:   i32(90),
	}
	hpa := BuildHPA(tc)

	if *hpa.Spec.MinReplicas != 2 {
		t.Errorf("minReplicas = %d, want 2", *hpa.Spec.MinReplicas)
	}
	if hpa.Spec.MaxReplicas != 25 {
		t.Errorf("maxReplicas = %d, want 25", hpa.Spec.MaxReplicas)
	}
	if *hpa.Spec.Metrics[0].Resource.Target.AverageUtilization != 90 {
		t.Errorf("targetCPU = %d, want 90", *hpa.Spec.Metrics[0].Resource.Target.AverageUtilization)
	}
}

func TestBuildHPA_PartialAgentDefaults(t *testing.T) {
	tc := makeTC("tc1", "ns1")
	tc.Spec.Agent = &capturev1alpha1.AgentScalingSpec{
		MaxReplicas: i32(20),
	}
	hpa := BuildHPA(tc)

	if *hpa.Spec.MinReplicas != 1 {
		t.Errorf("minReplicas = %d, want default 1", *hpa.Spec.MinReplicas)
	}
	if hpa.Spec.MaxReplicas != 20 {
		t.Errorf("maxReplicas = %d, want 20", hpa.Spec.MaxReplicas)
	}
	if *hpa.Spec.Metrics[0].Resource.Target.AverageUtilization != 70 {
		t.Errorf("targetCPU = %d, want default 70", *hpa.Spec.Metrics[0].Resource.Target.AverageUtilization)
	}
}

func TestBuildHPA_ScaleTargetRef(t *testing.T) {
	tc := makeTC("tc1", "ns1")
	hpa := BuildHPA(tc)

	ref := hpa.Spec.ScaleTargetRef
	if ref.APIVersion != "apps/v1" {
		t.Errorf("apiVersion = %q, want %q", ref.APIVersion, "apps/v1")
	}
	if ref.Kind != "Deployment" {
		t.Errorf("kind = %q, want %q", ref.Kind, "Deployment")
	}
	if ref.Name != "tc1-capture-agent" {
		t.Errorf("name = %q, want %q", ref.Name, "tc1-capture-agent")
	}
}

func TestBuildHPA_MetricSpec(t *testing.T) {
	tc := makeTC("tc1", "ns1")
	hpa := BuildHPA(tc)

	if len(hpa.Spec.Metrics) != 1 {
		t.Fatalf("expected 1 metric, got %d", len(hpa.Spec.Metrics))
	}
	m := hpa.Spec.Metrics[0]
	if m.Type != autoscalingv2.ResourceMetricSourceType {
		t.Errorf("metric type = %v, want Resource", m.Type)
	}
	if m.Resource == nil {
		t.Fatal("metric.Resource is nil")
	}
	if m.Resource.Name != corev1.ResourceCPU {
		t.Errorf("resource name = %v, want cpu", m.Resource.Name)
	}
	if m.Resource.Target.Type != autoscalingv2.UtilizationMetricType {
		t.Errorf("target type = %v, want Utilization", m.Resource.Target.Type)
	}
}

func TestBuildHPA_NameAndNamespace(t *testing.T) {
	tc := makeTC("my-cap", "prod")
	hpa := BuildHPA(tc)

	if hpa.Name != "my-cap-capture-agent" {
		t.Errorf("name = %q, want %q", hpa.Name, "my-cap-capture-agent")
	}
	if hpa.Namespace != "prod" {
		t.Errorf("namespace = %q, want %q", hpa.Namespace, "prod")
	}
}

// ---------------------------------------------------------------------------
// buildEnvVars - comprehensive tests
// ---------------------------------------------------------------------------

func TestBuildEnvVars_CaptureIDAndStorageType(t *testing.T) {
	tc := makeTC("cap1", "myns")
	storage := makeS3Storage()
	envs := buildEnvVars(tc, storage)

	if v, ok := findEnv(envs, "CAPTURE_ID"); !ok || v != "myns/cap1" {
		t.Errorf("CAPTURE_ID = %q (found=%v), want %q", v, ok, "myns/cap1")
	}
	if v, ok := findEnv(envs, "STORAGE_TYPE"); !ok || v != "S3" {
		t.Errorf("STORAGE_TYPE = %q (found=%v), want %q", v, ok, "S3")
	}
}

func TestBuildEnvVars_S3Storage(t *testing.T) {
	tc := makeTC("tc1", "ns1")
	storage := makeS3Storage()
	envs := buildEnvVars(tc, storage)

	if v, _ := findEnv(envs, "S3_BUCKET"); v != "my-bucket" {
		t.Errorf("S3_BUCKET = %q, want %q", v, "my-bucket")
	}
	if v, _ := findEnv(envs, "S3_REGION"); v != "us-east-1" {
		t.Errorf("S3_REGION = %q, want %q", v, "us-east-1")
	}
	if _, ok := findEnv(envs, "S3_PREFIX"); ok {
		t.Error("S3_PREFIX should not be set when Prefix is nil")
	}
}

func TestBuildEnvVars_S3StorageWithPrefix(t *testing.T) {
	tc := makeTC("tc1", "ns1")
	storage := makeS3Storage()
	storage.Spec.S3.Prefix = sp("captures/")
	envs := buildEnvVars(tc, storage)

	if v, ok := findEnv(envs, "S3_PREFIX"); !ok || v != "captures/" {
		t.Errorf("S3_PREFIX = %q (found=%v), want %q", v, ok, "captures/")
	}
}

func TestBuildEnvVars_GCSStorage(t *testing.T) {
	tc := makeTC("tc1", "ns1")
	storage := &capturev1alpha1.CaptureStorage{
		Spec: capturev1alpha1.CaptureStorageSpec{
			Type: capturev1alpha1.CaptureStorageTypeGCS,
			GCS:  &capturev1alpha1.GCSConfig{Bucket: "gcs-bucket"},
		},
	}
	envs := buildEnvVars(tc, storage)

	if v, _ := findEnv(envs, "STORAGE_TYPE"); v != "GCS" {
		t.Errorf("STORAGE_TYPE = %q, want GCS", v)
	}
	if v, _ := findEnv(envs, "GCS_BUCKET"); v != "gcs-bucket" {
		t.Errorf("GCS_BUCKET = %q, want %q", v, "gcs-bucket")
	}
	if _, ok := findEnv(envs, "GCS_PREFIX"); ok {
		t.Error("GCS_PREFIX should not be set when Prefix is nil")
	}
}

func TestBuildEnvVars_GCSStorageWithPrefix(t *testing.T) {
	tc := makeTC("tc1", "ns1")
	storage := &capturev1alpha1.CaptureStorage{
		Spec: capturev1alpha1.CaptureStorageSpec{
			Type: capturev1alpha1.CaptureStorageTypeGCS,
			GCS:  &capturev1alpha1.GCSConfig{Bucket: "gcs-bucket", Prefix: sp("data/")},
		},
	}
	envs := buildEnvVars(tc, storage)

	if v, ok := findEnv(envs, "GCS_PREFIX"); !ok || v != "data/" {
		t.Errorf("GCS_PREFIX = %q (found=%v), want %q", v, ok, "data/")
	}
}

func TestBuildEnvVars_EFSStorage(t *testing.T) {
	tc := makeTC("tc1", "ns1")
	storage := &capturev1alpha1.CaptureStorage{
		Spec: capturev1alpha1.CaptureStorageSpec{
			Type: capturev1alpha1.CaptureStorageTypeEFS,
			EFS:  &capturev1alpha1.EFSConfig{FileSystemID: "fs-12345", MountPath: "/mnt/efs"},
		},
	}
	envs := buildEnvVars(tc, storage)

	if v, _ := findEnv(envs, "STORAGE_TYPE"); v != "EFS" {
		t.Errorf("STORAGE_TYPE = %q, want EFS", v)
	}
	if v, _ := findEnv(envs, "EFS_FILESYSTEM_ID"); v != "fs-12345" {
		t.Errorf("EFS_FILESYSTEM_ID = %q, want %q", v, "fs-12345")
	}
	if v, _ := findEnv(envs, "EFS_MOUNT_PATH"); v != "/mnt/efs" {
		t.Errorf("EFS_MOUNT_PATH = %q, want %q", v, "/mnt/efs")
	}
}

func TestBuildEnvVars_EBSStorage(t *testing.T) {
	tc := makeTC("tc1", "ns1")
	storage := &capturev1alpha1.CaptureStorage{
		Spec: capturev1alpha1.CaptureStorageSpec{
			Type: capturev1alpha1.CaptureStorageTypeEBS,
			EBS:  &capturev1alpha1.EBSConfig{VolumeID: "vol-abc", MountPath: "/mnt/ebs"},
		},
	}
	envs := buildEnvVars(tc, storage)

	if v, _ := findEnv(envs, "STORAGE_TYPE"); v != "EBS" {
		t.Errorf("STORAGE_TYPE = %q, want EBS", v)
	}
	if v, _ := findEnv(envs, "EBS_VOLUME_ID"); v != "vol-abc" {
		t.Errorf("EBS_VOLUME_ID = %q, want %q", v, "vol-abc")
	}
	if v, _ := findEnv(envs, "EBS_MOUNT_PATH"); v != "/mnt/ebs" {
		t.Errorf("EBS_MOUNT_PATH = %q, want %q", v, "/mnt/ebs")
	}
}

func TestBuildEnvVars_CaptureSettingsAllSet(t *testing.T) {
	tc := makeTC("tc1", "ns1")
	tc.Spec.Capture = capturev1alpha1.CaptureSettings{
		IncludeHeaders: bl(true),
		IncludeBody:    bl(false),
		MaxBodyBytes:   i64(4096),
	}
	envs := buildEnvVars(tc, makeS3Storage())

	if v, _ := findEnv(envs, "CAPTURE_INCLUDE_HEADERS"); v != "true" {
		t.Errorf("CAPTURE_INCLUDE_HEADERS = %q, want %q", v, "true")
	}
	if v, _ := findEnv(envs, "CAPTURE_INCLUDE_BODY"); v != "false" {
		t.Errorf("CAPTURE_INCLUDE_BODY = %q, want %q", v, "false")
	}
	if v, _ := findEnv(envs, "CAPTURE_MAX_BODY_BYTES"); v != "4096" {
		t.Errorf("CAPTURE_MAX_BODY_BYTES = %q, want %q", v, "4096")
	}
}

func TestBuildEnvVars_CaptureSettingsNone(t *testing.T) {
	tc := makeTC("tc1", "ns1")
	// All capture settings nil by default
	envs := buildEnvVars(tc, makeS3Storage())

	for _, name := range []string{"CAPTURE_INCLUDE_HEADERS", "CAPTURE_INCLUDE_BODY", "CAPTURE_MAX_BODY_BYTES"} {
		if _, ok := findEnv(envs, name); ok {
			t.Errorf("%s should not be set when capture settings fields are nil", name)
		}
	}
}

func TestBuildEnvVars_CaptureSettingsPartial(t *testing.T) {
	tc := makeTC("tc1", "ns1")
	tc.Spec.Capture = capturev1alpha1.CaptureSettings{
		IncludeHeaders: bl(true),
		// IncludeBody and MaxBodyBytes left nil
	}
	envs := buildEnvVars(tc, makeS3Storage())

	if v, ok := findEnv(envs, "CAPTURE_INCLUDE_HEADERS"); !ok || v != "true" {
		t.Errorf("CAPTURE_INCLUDE_HEADERS = %q (found=%v), want %q", v, ok, "true")
	}
	if _, ok := findEnv(envs, "CAPTURE_INCLUDE_BODY"); ok {
		t.Error("CAPTURE_INCLUDE_BODY should not be set")
	}
	if _, ok := findEnv(envs, "CAPTURE_MAX_BODY_BYTES"); ok {
		t.Error("CAPTURE_MAX_BODY_BYTES should not be set")
	}
}

func TestBuildEnvVars_S3NilConfig(t *testing.T) {
	tc := makeTC("tc1", "ns1")
	storage := &capturev1alpha1.CaptureStorage{
		Spec: capturev1alpha1.CaptureStorageSpec{
			Type: capturev1alpha1.CaptureStorageTypeS3,
			S3:   nil, // type says S3 but config is nil
		},
	}
	envs := buildEnvVars(tc, storage)

	// Should still have base env vars but no S3-specific ones
	if _, ok := findEnv(envs, "CAPTURE_ID"); !ok {
		t.Error("CAPTURE_ID missing")
	}
	if _, ok := findEnv(envs, "S3_BUCKET"); ok {
		t.Error("S3_BUCKET should not be set when S3 config is nil")
	}
}

func TestBuildEnvVars_GCSNilConfig(t *testing.T) {
	tc := makeTC("tc1", "ns1")
	storage := &capturev1alpha1.CaptureStorage{
		Spec: capturev1alpha1.CaptureStorageSpec{
			Type: capturev1alpha1.CaptureStorageTypeGCS,
			GCS:  nil,
		},
	}
	envs := buildEnvVars(tc, storage)

	if _, ok := findEnv(envs, "GCS_BUCKET"); ok {
		t.Error("GCS_BUCKET should not be set when GCS config is nil")
	}
}

func TestBuildEnvVars_EFSNilConfig(t *testing.T) {
	tc := makeTC("tc1", "ns1")
	storage := &capturev1alpha1.CaptureStorage{
		Spec: capturev1alpha1.CaptureStorageSpec{
			Type: capturev1alpha1.CaptureStorageTypeEFS,
			EFS:  nil,
		},
	}
	envs := buildEnvVars(tc, storage)

	if _, ok := findEnv(envs, "EFS_FILESYSTEM_ID"); ok {
		t.Error("EFS_FILESYSTEM_ID should not be set when EFS config is nil")
	}
}

func TestBuildEnvVars_EBSNilConfig(t *testing.T) {
	tc := makeTC("tc1", "ns1")
	storage := &capturev1alpha1.CaptureStorage{
		Spec: capturev1alpha1.CaptureStorageSpec{
			Type: capturev1alpha1.CaptureStorageTypeEBS,
			EBS:  nil,
		},
	}
	envs := buildEnvVars(tc, storage)

	if _, ok := findEnv(envs, "EBS_VOLUME_ID"); ok {
		t.Error("EBS_VOLUME_ID should not be set when EBS config is nil")
	}
}

// ---------------------------------------------------------------------------
// Mirror filter helpers - table-driven comprehensive tests
// ---------------------------------------------------------------------------

func TestBuildHTTPMirrorFilter_Fields(t *testing.T) {
	f := buildHTTPMirrorFilter("ns1", "my-svc")

	if f.Type != gwapiv1.HTTPRouteFilterRequestMirror {
		t.Errorf("type = %v, want RequestMirror", f.Type)
	}
	if f.RequestMirror == nil {
		t.Fatal("RequestMirror is nil")
	}
	if string(f.RequestMirror.BackendRef.Name) != "my-svc" {
		t.Errorf("name = %q, want %q", f.RequestMirror.BackendRef.Name, "my-svc")
	}
	if f.RequestMirror.BackendRef.Namespace == nil || string(*f.RequestMirror.BackendRef.Namespace) != "ns1" {
		t.Errorf("namespace = %v, want %q", f.RequestMirror.BackendRef.Namespace, "ns1")
	}
	if f.RequestMirror.BackendRef.Port == nil || *f.RequestMirror.BackendRef.Port != 8080 {
		t.Errorf("port = %v, want 8080", f.RequestMirror.BackendRef.Port)
	}
}

func TestBuildGRPCMirrorFilter_Fields(t *testing.T) {
	f := buildGRPCMirrorFilter("ns2", "grpc-svc")

	if f.Type != gwapiv1.GRPCRouteFilterRequestMirror {
		t.Errorf("type = %v, want RequestMirror", f.Type)
	}
	if f.RequestMirror == nil {
		t.Fatal("RequestMirror is nil")
	}
	if string(f.RequestMirror.BackendRef.Name) != "grpc-svc" {
		t.Errorf("name = %q, want %q", f.RequestMirror.BackendRef.Name, "grpc-svc")
	}
	if f.RequestMirror.BackendRef.Namespace == nil || string(*f.RequestMirror.BackendRef.Namespace) != "ns2" {
		t.Errorf("namespace = %v, want %q", f.RequestMirror.BackendRef.Namespace, "ns2")
	}
	if f.RequestMirror.BackendRef.Port == nil || *f.RequestMirror.BackendRef.Port != 8080 {
		t.Errorf("port = %v, want 8080", f.RequestMirror.BackendRef.Port)
	}
}

func TestHasHTTPMirrorFilter_TableDriven(t *testing.T) {
	mirror := buildHTTPMirrorFilter("ns1", "agent-svc")
	other := gwapiv1.HTTPRouteFilter{Type: gwapiv1.HTTPRouteFilterRequestHeaderModifier}

	tests := []struct {
		name    string
		filters []gwapiv1.HTTPRouteFilter
		svcName string
		want    bool
	}{
		{"found among multiple", []gwapiv1.HTTPRouteFilter{other, mirror}, "agent-svc", true},
		{"wrong name", []gwapiv1.HTTPRouteFilter{mirror}, "other-svc", false},
		{"nil filters", nil, "agent-svc", false},
		{"empty filters", []gwapiv1.HTTPRouteFilter{}, "agent-svc", false},
		{"only non-mirror filters", []gwapiv1.HTTPRouteFilter{other}, "agent-svc", false},
		{"mirror with nil RequestMirror", []gwapiv1.HTTPRouteFilter{
			{Type: gwapiv1.HTTPRouteFilterRequestMirror, RequestMirror: nil},
		}, "agent-svc", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasHTTPMirrorFilter(tt.filters, tt.svcName); got != tt.want {
				t.Errorf("hasHTTPMirrorFilter = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHasGRPCMirrorFilter_TableDriven(t *testing.T) {
	mirror := buildGRPCMirrorFilter("ns1", "agent-svc")
	other := gwapiv1.GRPCRouteFilter{Type: gwapiv1.GRPCRouteFilterRequestHeaderModifier}

	tests := []struct {
		name    string
		filters []gwapiv1.GRPCRouteFilter
		svcName string
		want    bool
	}{
		{"found among multiple", []gwapiv1.GRPCRouteFilter{other, mirror}, "agent-svc", true},
		{"wrong name", []gwapiv1.GRPCRouteFilter{mirror}, "other-svc", false},
		{"nil filters", nil, "agent-svc", false},
		{"empty filters", []gwapiv1.GRPCRouteFilter{}, "agent-svc", false},
		{"only non-mirror filters", []gwapiv1.GRPCRouteFilter{other}, "agent-svc", false},
		{"mirror with nil RequestMirror", []gwapiv1.GRPCRouteFilter{
			{Type: gwapiv1.GRPCRouteFilterRequestMirror, RequestMirror: nil},
		}, "agent-svc", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasGRPCMirrorFilter(tt.filters, tt.svcName); got != tt.want {
				t.Errorf("hasGRPCMirrorFilter = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRemoveHTTPMirrorFilters_TableDriven(t *testing.T) {
	mirror := buildHTTPMirrorFilter("ns1", "agent-svc")
	otherMirror := buildHTTPMirrorFilter("ns1", "other-svc")
	header := gwapiv1.HTTPRouteFilter{Type: gwapiv1.HTTPRouteFilterRequestHeaderModifier}

	tests := []struct {
		name    string
		filters []gwapiv1.HTTPRouteFilter
		svcName string
		wantLen int
	}{
		{"removes matching", []gwapiv1.HTTPRouteFilter{header, mirror}, "agent-svc", 1},
		{"keeps non-matching mirror", []gwapiv1.HTTPRouteFilter{otherMirror, mirror}, "agent-svc", 1},
		{"no match", []gwapiv1.HTTPRouteFilter{header, otherMirror}, "agent-svc", 2},
		{"empty", nil, "agent-svc", 0},
		{"removes all matching", []gwapiv1.HTTPRouteFilter{mirror, mirror}, "agent-svc", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := removeHTTPMirrorFilters(tt.filters, tt.svcName)
			if len(result) != tt.wantLen {
				t.Errorf("len = %d, want %d", len(result), tt.wantLen)
			}
			if hasHTTPMirrorFilter(result, tt.svcName) {
				t.Error("target filter still present after removal")
			}
		})
	}
}

func TestRemoveGRPCMirrorFilters_TableDriven(t *testing.T) {
	mirror := buildGRPCMirrorFilter("ns1", "agent-svc")
	otherMirror := buildGRPCMirrorFilter("ns1", "other-svc")
	header := gwapiv1.GRPCRouteFilter{Type: gwapiv1.GRPCRouteFilterRequestHeaderModifier}

	tests := []struct {
		name    string
		filters []gwapiv1.GRPCRouteFilter
		svcName string
		wantLen int
	}{
		{"removes matching", []gwapiv1.GRPCRouteFilter{header, mirror}, "agent-svc", 1},
		{"keeps non-matching mirror", []gwapiv1.GRPCRouteFilter{otherMirror, mirror}, "agent-svc", 1},
		{"no match", []gwapiv1.GRPCRouteFilter{header, otherMirror}, "agent-svc", 2},
		{"empty", nil, "agent-svc", 0},
		{"removes all matching", []gwapiv1.GRPCRouteFilter{mirror, mirror}, "agent-svc", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := removeGRPCMirrorFilters(tt.filters, tt.svcName)
			if len(result) != tt.wantLen {
				t.Errorf("len = %d, want %d", len(result), tt.wantLen)
			}
			if hasGRPCMirrorFilter(result, tt.svcName) {
				t.Error("target filter still present after removal")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// setAnnotation
// ---------------------------------------------------------------------------

func TestSetAnnotation_NilMap(t *testing.T) {
	route := &gwapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "route1"},
	}
	setAnnotation(route, "ns1/tc1")

	if route.Annotations == nil {
		t.Fatal("annotations should not be nil after setAnnotation")
	}
	if route.Annotations[mirrorFilterAnnotationKey] != "ns1/tc1" {
		t.Errorf("annotation = %q, want %q", route.Annotations[mirrorFilterAnnotationKey], "ns1/tc1")
	}
}

func TestSetAnnotation_ExistingMap(t *testing.T) {
	route := &gwapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "route1",
			Annotations: map[string]string{"existing": "value"},
		},
	}
	setAnnotation(route, "ns1/tc1")

	if route.Annotations["existing"] != "value" {
		t.Error("existing annotation was lost")
	}
	if route.Annotations[mirrorFilterAnnotationKey] != "ns1/tc1" {
		t.Errorf("annotation = %q, want %q", route.Annotations[mirrorFilterAnnotationKey], "ns1/tc1")
	}
}

func TestSetAnnotation_OverwritesExisting(t *testing.T) {
	route := &gwapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "route1",
			Annotations: map[string]string{mirrorFilterAnnotationKey: "old-value"},
		},
	}
	setAnnotation(route, "ns1/tc1")

	if route.Annotations[mirrorFilterAnnotationKey] != "ns1/tc1" {
		t.Errorf("annotation = %q, want %q", route.Annotations[mirrorFilterAnnotationKey], "ns1/tc1")
	}
}

// ---------------------------------------------------------------------------
// AgentServiceName / AgentDeploymentName - format tests
// ---------------------------------------------------------------------------

func TestAgentServiceName_Format(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{"my-cap", "my-cap-capture-agent"},
		{"x", "x-capture-agent"},
		{"long-name-test", "long-name-test-capture-agent"},
	}
	for _, tt := range tests {
		tc := makeTC(tt.name, "ns")
		if got := AgentServiceName(tc); got != tt.want {
			t.Errorf("AgentServiceName(%q) = %q, want %q", tt.name, got, tt.want)
		}
	}
}

func TestAgentDeploymentName_Format(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{"my-cap", "my-cap-capture-agent"},
		{"x", "x-capture-agent"},
		{"long-name-test", "long-name-test-capture-agent"},
	}
	for _, tt := range tests {
		tc := makeTC(tt.name, "ns")
		if got := AgentDeploymentName(tc); got != tt.want {
			t.Errorf("AgentDeploymentName(%q) = %q, want %q", tt.name, got, tt.want)
		}
	}
}
