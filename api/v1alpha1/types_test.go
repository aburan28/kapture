package v1alpha1

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// ---------------------------------------------------------------------------
// GroupVersion constants
// ---------------------------------------------------------------------------

func TestGroupName(t *testing.T) {
	if GroupName != "capture.gateway.io" {
		t.Errorf("expected GroupName %q, got %q", "capture.gateway.io", GroupName)
	}
}

func TestGroupVersion(t *testing.T) {
	want := schema.GroupVersion{Group: "capture.gateway.io", Version: "v1alpha1"}
	if GroupVersion != want {
		t.Errorf("expected GroupVersion %v, got %v", want, GroupVersion)
	}
}

// ---------------------------------------------------------------------------
// Resource() helper
// ---------------------------------------------------------------------------

func TestResource(t *testing.T) {
	gr := Resource("trafficcaptures")
	want := schema.GroupResource{Group: "capture.gateway.io", Resource: "trafficcaptures"}
	if gr != want {
		t.Errorf("expected GroupResource %v, got %v", want, gr)
	}
}

// ---------------------------------------------------------------------------
// Kind constants
// ---------------------------------------------------------------------------

func TestKindConstants(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"KindTrafficCapture", KindTrafficCapture, "TrafficCapture"},
		{"KindCaptureStorage", KindCaptureStorage, "CaptureStorage"},
		{"KindCaptureHub", KindCaptureHub, "CaptureHub"},
		{"KindTrafficReplay", KindTrafficReplay, "TrafficReplay"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("expected %q, got %q", tt.want, tt.got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Phase constants
// ---------------------------------------------------------------------------

func TestTrafficCapturePhases(t *testing.T) {
	tests := []struct {
		phase TrafficCapturePhase
		want  string
	}{
		{TrafficCapturePhasePending, "Pending"},
		{TrafficCapturePhaseActive, "Active"},
		{TrafficCapturePhaseCompleted, "Completed"},
		{TrafficCapturePhaseFailed, "Failed"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if string(tt.phase) != tt.want {
				t.Errorf("expected %q, got %q", tt.want, string(tt.phase))
			}
		})
	}
}

func TestCaptureStoragePhases(t *testing.T) {
	tests := []struct {
		phase CaptureStoragePhase
		want  string
	}{
		{CaptureStoragePhaseReady, "Ready"},
		{CaptureStoragePhaseError, "Error"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if string(tt.phase) != tt.want {
				t.Errorf("expected %q, got %q", tt.want, string(tt.phase))
			}
		})
	}
}

func TestCaptureStorageTypes(t *testing.T) {
	tests := []struct {
		storageType CaptureStorageType
		want        string
	}{
		{CaptureStorageTypeS3, "S3"},
		{CaptureStorageTypeGCS, "GCS"},
		{CaptureStorageTypeEFS, "EFS"},
		{CaptureStorageTypeEBS, "EBS"},
		{CaptureStorageTypePlugin, "Plugin"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if string(tt.storageType) != tt.want {
				t.Errorf("expected %q, got %q", tt.want, string(tt.storageType))
			}
		})
	}
}

func TestTrafficReplayPhases(t *testing.T) {
	tests := []struct {
		phase TrafficReplayPhase
		want  string
	}{
		{TrafficReplayPhasePending, "Pending"},
		{TrafficReplayPhaseRunning, "Running"},
		{TrafficReplayPhaseCompleted, "Completed"},
		{TrafficReplayPhaseFailed, "Failed"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if string(tt.phase) != tt.want {
				t.Errorf("expected %q, got %q", tt.want, string(tt.phase))
			}
		})
	}
}

func TestReplayRateModes(t *testing.T) {
	tests := []struct {
		mode ReplayRateMode
		want string
	}{
		{ReplayRateModeConstant, "Constant"},
		{ReplayRateModeOriginalTiming, "OriginalTiming"},
		{ReplayRateModeUnlimited, "Unlimited"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if string(tt.mode) != tt.want {
				t.Errorf("expected %q, got %q", tt.want, string(tt.mode))
			}
		})
	}
}

func TestHubAuthenticationType(t *testing.T) {
	if string(HubAuthenticationTypeMTLS) != "mTLS" {
		t.Errorf("expected %q, got %q", "mTLS", string(HubAuthenticationTypeMTLS))
	}
}

// ---------------------------------------------------------------------------
// Scheme registration
// ---------------------------------------------------------------------------

func TestSchemeRegistration(t *testing.T) {
	s := runtime.NewScheme()
	if err := AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme failed: %v", err)
	}

	types := []struct {
		obj  runtime.Object
		kind string
	}{
		{&TrafficCapture{}, "TrafficCapture"},
		{&TrafficCaptureList{}, "TrafficCaptureList"},
		{&CaptureStorage{}, "CaptureStorage"},
		{&CaptureStorageList{}, "CaptureStorageList"},
		{&CaptureHub{}, "CaptureHub"},
		{&CaptureHubList{}, "CaptureHubList"},
		{&TrafficReplay{}, "TrafficReplay"},
		{&TrafficReplayList{}, "TrafficReplayList"},
	}

	for _, tt := range types {
		t.Run(tt.kind, func(t *testing.T) {
			gvks, _, err := s.ObjectKinds(tt.obj)
			if err != nil {
				t.Fatalf("type %s not registered: %v", tt.kind, err)
			}
			found := false
			for _, gvk := range gvks {
				if gvk.Group == GroupName && gvk.Version == "v1alpha1" && gvk.Kind == tt.kind {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("expected GVK %s/%s/%s among %v", GroupName, "v1alpha1", tt.kind, gvks)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// DeepCopy round-trip tests
// ---------------------------------------------------------------------------

func ptrString(s string) *string    { return &s }
func ptrInt32(i int32) *int32       { return &i }
func ptrInt64(i int64) *int64       { return &i }
func ptrBool(b bool) *bool          { return &b }

func TestTrafficCaptureDeepCopy(t *testing.T) {
	now := metav1.NewTime(time.Now().Truncate(time.Second))
	qty := resource.MustParse("1Gi")
	original := &TrafficCapture{
		TypeMeta: metav1.TypeMeta{Kind: KindTrafficCapture, APIVersion: GroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-capture",
			Namespace: "default",
		},
		Spec: TrafficCaptureSpec{
			TargetRef: gwapiv1.LocalPolicyTargetReference{
				Group: "gateway.networking.k8s.io",
				Kind:  "HTTPRoute",
				Name:  "my-route",
			},
			StorageRef: CaptureStorageReference{Name: "my-storage"},
			Capture: CaptureSettings{
				IncludeHeaders: ptrBool(true),
				IncludeBody:    ptrBool(false),
				MaxBodyBytes:   ptrInt64(1048576),
				Duration:       ptrString("30m"),
			},
			Filters: &CaptureFilters{
				Headers:    []HeaderMatch{{Name: "x-test", Value: "yes"}},
				PathPrefix: ptrString("/api"),
				Percentage: ptrInt32(50),
			},
			Agent: &AgentScalingSpec{
				Replicas:    ptrInt32(2),
				MinReplicas: ptrInt32(1),
				MaxReplicas: ptrInt32(5),
				TargetCPU:   ptrInt32(80),
			},
			Plugins: []PayloadPluginRef{
				{
					Name:    "redact",
					OnError: ptrString("drop"),
					Config:  &runtime.RawExtension{Raw: []byte(`{"fields":["password"]}`)},
				},
			},
		},
		Status: TrafficCaptureStatus{
			Phase:            TrafficCapturePhaseActive,
			StartTime:        &now,
			CapturedRequests: 42,
			BytesWritten:     &qty,
			AgentStatus: &CaptureAgentStatus{
				ReadyReplicas:   2,
				DesiredReplicas: 2,
			},
			Conditions: []metav1.Condition{
				{
					Type:   "Ready",
					Status: metav1.ConditionTrue,
				},
			},
		},
	}

	copied := original.DeepCopy()

	// Verify copy equals original
	if copied.Name != original.Name {
		t.Error("Name mismatch after DeepCopy")
	}
	if copied.Spec.Capture.Duration == nil || *copied.Spec.Capture.Duration != *original.Spec.Capture.Duration {
		t.Error("Capture.Duration mismatch after DeepCopy")
	}
	if copied.Spec.Filters == nil || len(copied.Spec.Filters.Headers) != 1 {
		t.Error("Filters.Headers mismatch after DeepCopy")
	}
	if copied.Status.Phase != TrafficCapturePhaseActive {
		t.Error("Status.Phase mismatch after DeepCopy")
	}
	if copied.Status.CapturedRequests != 42 {
		t.Error("Status.CapturedRequests mismatch after DeepCopy")
	}
	if len(copied.Spec.Plugins) != 1 || copied.Spec.Plugins[0].Name != "redact" {
		t.Error("Plugins mismatch after DeepCopy")
	}

	// Verify modifying the copy does not affect the original
	*copied.Spec.Capture.Duration = "60m"
	if *original.Spec.Capture.Duration != "30m" {
		t.Error("modifying copy Duration affected original")
	}

	copied.Spec.Filters.Headers[0].Name = "x-changed"
	if original.Spec.Filters.Headers[0].Name != "x-test" {
		t.Error("modifying copy Headers affected original")
	}

	*copied.Spec.Agent.Replicas = 99
	if *original.Spec.Agent.Replicas != 2 {
		t.Error("modifying copy Agent.Replicas affected original")
	}

	copied.Spec.Plugins[0].Config.Raw[0] = 'X'
	if original.Spec.Plugins[0].Config.Raw[0] != '{' {
		t.Error("modifying copy Plugin Config affected original")
	}

	copied.Status.Conditions[0].Type = "NotReady"
	if original.Status.Conditions[0].Type != "Ready" {
		t.Error("modifying copy Conditions affected original")
	}

	// Verify DeepCopyObject returns runtime.Object
	var _ runtime.Object = original.DeepCopyObject()
}

func TestCaptureStorageDeepCopy(t *testing.T) {
	maxSize := resource.MustParse("10Gi")
	original := &CaptureStorage{
		TypeMeta: metav1.TypeMeta{Kind: KindCaptureStorage, APIVersion: GroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-storage",
			Namespace: "default",
		},
		Spec: CaptureStorageSpec{
			Type: CaptureStorageTypeS3,
			S3: &S3Config{
				Bucket: "my-bucket",
				Region: "us-east-1",
				Prefix: ptrString("captures/"),
				CredentialsSecretRef: gwapiv1.SecretObjectReference{
					Name: "s3-creds",
				},
			},
			GCS: &GCSConfig{
				Bucket: "gcs-bucket",
				Prefix: ptrString("data/"),
				CredentialsSecretRef: gwapiv1.SecretObjectReference{
					Name: "gcs-creds",
				},
			},
			EFS: &EFSConfig{
				FileSystemID: "fs-12345",
				MountPath:    "/mnt/efs",
				Directory:    ptrString("captures"),
			},
			EBS: &EBSConfig{
				VolumeID:   "vol-67890",
				MountPath:  "/mnt/ebs",
				Filesystem: ptrString("ext4"),
			},
			Plugin: &PluginConfig{
				Path:   "/plugins/custom.so",
				Symbol: ptrString("NewWriter"),
				Config: &runtime.RawExtension{Raw: []byte(`{"key":"value"}`)},
			},
			Retention: &RetentionPolicy{
				MaxAge:  ptrString("7d"),
				MaxSize: &maxSize,
			},
		},
		Status: CaptureStorageStatus{
			Phase: CaptureStoragePhaseReady,
			Conditions: []metav1.Condition{
				{
					Type:   "Validated",
					Status: metav1.ConditionTrue,
				},
			},
		},
	}

	copied := original.DeepCopy()

	// Verify copy equals original
	if copied.Spec.Type != CaptureStorageTypeS3 {
		t.Error("Type mismatch after DeepCopy")
	}
	if copied.Spec.S3 == nil || copied.Spec.S3.Bucket != "my-bucket" {
		t.Error("S3 config mismatch after DeepCopy")
	}
	if copied.Spec.EFS == nil || copied.Spec.EFS.FileSystemID != "fs-12345" {
		t.Error("EFS config mismatch after DeepCopy")
	}
	if copied.Spec.Plugin == nil || copied.Spec.Plugin.Path != "/plugins/custom.so" {
		t.Error("Plugin config mismatch after DeepCopy")
	}
	if copied.Status.Phase != CaptureStoragePhaseReady {
		t.Error("Status.Phase mismatch after DeepCopy")
	}

	// Verify modifying the copy does not affect the original
	copied.Spec.S3.Bucket = "changed-bucket"
	if original.Spec.S3.Bucket != "my-bucket" {
		t.Error("modifying copy S3.Bucket affected original")
	}

	*copied.Spec.S3.Prefix = "changed/"
	if *original.Spec.S3.Prefix != "captures/" {
		t.Error("modifying copy S3.Prefix affected original")
	}

	*copied.Spec.EFS.Directory = "changed"
	if *original.Spec.EFS.Directory != "captures" {
		t.Error("modifying copy EFS.Directory affected original")
	}

	copied.Spec.Plugin.Config.Raw[0] = 'X'
	if original.Spec.Plugin.Config.Raw[0] != '{' {
		t.Error("modifying copy Plugin.Config affected original")
	}

	*copied.Spec.Retention.MaxAge = "30d"
	if *original.Spec.Retention.MaxAge != "7d" {
		t.Error("modifying copy Retention.MaxAge affected original")
	}

	copied.Status.Conditions[0].Type = "Invalid"
	if original.Status.Conditions[0].Type != "Validated" {
		t.Error("modifying copy Conditions affected original")
	}

	var _ runtime.Object = original.DeepCopyObject()
}

func TestCaptureHubDeepCopy(t *testing.T) {
	now := metav1.NewTime(time.Now().Truncate(time.Second))
	original := &CaptureHub{
		TypeMeta: metav1.TypeMeta{Kind: KindCaptureHub, APIVersion: GroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-hub",
		},
		Spec: CaptureHubSpec{
			GRPCAddress: "hub.example.com:9090",
			TLS: &CaptureHubTLSConfig{
				CertSecretRef: gwapiv1.SecretObjectReference{
					Name: "hub-tls",
				},
			},
			Authentication: &CaptureHubAuthentication{
				Type: HubAuthenticationTypeMTLS,
			},
		},
		Status: CaptureHubStatus{
			ConnectedSpokes: 3,
			ActiveCaptures:  5,
			Spokes: []CaptureHubSpokeStatus{
				{
					Name:           "spoke-1",
					LastHeartbeat:  &now,
					ActiveCaptures: 2,
				},
				{
					Name:           "spoke-2",
					LastHeartbeat:  &now,
					ActiveCaptures: 3,
				},
			},
		},
	}

	copied := original.DeepCopy()

	// Verify copy equals original
	if copied.Spec.GRPCAddress != "hub.example.com:9090" {
		t.Error("GRPCAddress mismatch after DeepCopy")
	}
	if copied.Spec.TLS == nil || string(copied.Spec.TLS.CertSecretRef.Name) != "hub-tls" {
		t.Error("TLS config mismatch after DeepCopy")
	}
	if copied.Spec.Authentication == nil || copied.Spec.Authentication.Type != HubAuthenticationTypeMTLS {
		t.Error("Authentication mismatch after DeepCopy")
	}
	if copied.Status.ConnectedSpokes != 3 {
		t.Error("ConnectedSpokes mismatch after DeepCopy")
	}
	if len(copied.Status.Spokes) != 2 {
		t.Error("Spokes length mismatch after DeepCopy")
	}

	// Verify modifying the copy does not affect the original
	copied.Spec.GRPCAddress = "changed:1234"
	if original.Spec.GRPCAddress != "hub.example.com:9090" {
		t.Error("modifying copy GRPCAddress affected original")
	}

	copied.Status.Spokes[0].Name = "changed-spoke"
	if original.Status.Spokes[0].Name != "spoke-1" {
		t.Error("modifying copy Spokes affected original")
	}

	copied.Status.Spokes[0].ActiveCaptures = 99
	if original.Status.Spokes[0].ActiveCaptures != 2 {
		t.Error("modifying copy Spoke.ActiveCaptures affected original")
	}

	var _ runtime.Object = original.DeepCopyObject()
}

func TestTrafficReplayDeepCopy(t *testing.T) {
	now := metav1.NewTime(time.Now().Truncate(time.Second))
	original := &TrafficReplay{
		TypeMeta: metav1.TypeMeta{Kind: KindTrafficReplay, APIVersion: GroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-replay",
			Namespace: "default",
		},
		Spec: TrafficReplaySpec{
			SourceRef:  TrafficCaptureReference{Name: "my-capture"},
			StorageRef: CaptureStorageReference{Name: "my-storage"},
			Target: ReplayTarget{
				Host: "target.example.com",
				Port: ptrInt32(8080),
				TLS:  ptrBool(true),
			},
			Rate: &ReplayRateConfig{
				Mode:              ReplayRateModeConstant,
				RequestsPerSecond: ptrInt32(100),
				TimeScale:         ptrString("1.0"),
			},
			Filters: &ReplayFilters{
				StartTime:  &now,
				EndTime:    &now,
				PathPrefix: ptrString("/api/v1"),
				Methods:    []string{"GET", "POST"},
				Limit:      ptrInt64(1000),
			},
			Transforms: []ReplayTransformRef{
				{
					Name:   "header-rewriter",
					Config: &runtime.RawExtension{Raw: []byte(`{"add":{"x-replay":"true"}}`)},
				},
			},
			Concurrency: ptrInt32(4),
		},
		Status: TrafficReplayStatus{
			Phase:            TrafficReplayPhaseRunning,
			StartTime:        &now,
			CompletionTime:   &now,
			TotalRequests:    1000,
			SentRequests:     950,
			FailedRequests:   10,
			FilteredRequests: 40,
			MeanLatency:      ptrString("12ms"),
			Conditions: []metav1.Condition{
				{
					Type:   "Progressing",
					Status: metav1.ConditionTrue,
				},
			},
		},
	}

	copied := original.DeepCopy()

	// Verify copy equals original
	if copied.Spec.SourceRef.Name != "my-capture" {
		t.Error("SourceRef mismatch after DeepCopy")
	}
	if copied.Spec.Target.Host != "target.example.com" {
		t.Error("Target.Host mismatch after DeepCopy")
	}
	if copied.Spec.Target.Port == nil || *copied.Spec.Target.Port != 8080 {
		t.Error("Target.Port mismatch after DeepCopy")
	}
	if copied.Spec.Rate == nil || copied.Spec.Rate.Mode != ReplayRateModeConstant {
		t.Error("Rate.Mode mismatch after DeepCopy")
	}
	if copied.Spec.Filters == nil || len(copied.Spec.Filters.Methods) != 2 {
		t.Error("Filters.Methods mismatch after DeepCopy")
	}
	if len(copied.Spec.Transforms) != 1 || copied.Spec.Transforms[0].Name != "header-rewriter" {
		t.Error("Transforms mismatch after DeepCopy")
	}
	if copied.Status.Phase != TrafficReplayPhaseRunning {
		t.Error("Status.Phase mismatch after DeepCopy")
	}
	if copied.Status.SentRequests != 950 {
		t.Error("Status.SentRequests mismatch after DeepCopy")
	}
	if copied.Status.MeanLatency == nil || *copied.Status.MeanLatency != "12ms" {
		t.Error("Status.MeanLatency mismatch after DeepCopy")
	}

	// Verify modifying the copy does not affect the original
	*copied.Spec.Target.Port = 9090
	if *original.Spec.Target.Port != 8080 {
		t.Error("modifying copy Target.Port affected original")
	}

	*copied.Spec.Rate.RequestsPerSecond = 999
	if *original.Spec.Rate.RequestsPerSecond != 100 {
		t.Error("modifying copy Rate.RequestsPerSecond affected original")
	}

	copied.Spec.Filters.Methods[0] = "DELETE"
	if original.Spec.Filters.Methods[0] != "GET" {
		t.Error("modifying copy Filters.Methods affected original")
	}

	*copied.Spec.Filters.Limit = 9999
	if *original.Spec.Filters.Limit != 1000 {
		t.Error("modifying copy Filters.Limit affected original")
	}

	copied.Spec.Transforms[0].Config.Raw[0] = 'X'
	if original.Spec.Transforms[0].Config.Raw[0] != '{' {
		t.Error("modifying copy Transforms Config affected original")
	}

	*copied.Spec.Concurrency = 99
	if *original.Spec.Concurrency != 4 {
		t.Error("modifying copy Concurrency affected original")
	}

	*copied.Status.MeanLatency = "99ms"
	if *original.Status.MeanLatency != "12ms" {
		t.Error("modifying copy MeanLatency affected original")
	}

	copied.Status.Conditions[0].Type = "Failed"
	if original.Status.Conditions[0].Type != "Progressing" {
		t.Error("modifying copy Conditions affected original")
	}

	var _ runtime.Object = original.DeepCopyObject()
}

// ---------------------------------------------------------------------------
// List type DeepCopy
// ---------------------------------------------------------------------------

func TestTrafficCaptureListDeepCopy(t *testing.T) {
	original := &TrafficCaptureList{
		Items: []TrafficCapture{
			{ObjectMeta: metav1.ObjectMeta{Name: "tc-1"}},
			{ObjectMeta: metav1.ObjectMeta{Name: "tc-2"}},
		},
	}
	copied := original.DeepCopy()
	if len(copied.Items) != 2 {
		t.Fatal("Items length mismatch")
	}
	copied.Items[0].Name = "changed"
	if original.Items[0].Name != "tc-1" {
		t.Error("modifying copy Items affected original")
	}
	var _ runtime.Object = original.DeepCopyObject()
}

func TestCaptureStorageListDeepCopy(t *testing.T) {
	original := &CaptureStorageList{
		Items: []CaptureStorage{
			{ObjectMeta: metav1.ObjectMeta{Name: "cs-1"}},
		},
	}
	copied := original.DeepCopy()
	if len(copied.Items) != 1 {
		t.Fatal("Items length mismatch")
	}
	copied.Items[0].Name = "changed"
	if original.Items[0].Name != "cs-1" {
		t.Error("modifying copy Items affected original")
	}
	var _ runtime.Object = original.DeepCopyObject()
}

func TestCaptureHubListDeepCopy(t *testing.T) {
	original := &CaptureHubList{
		Items: []CaptureHub{
			{ObjectMeta: metav1.ObjectMeta{Name: "hub-1"}},
		},
	}
	copied := original.DeepCopy()
	if len(copied.Items) != 1 {
		t.Fatal("Items length mismatch")
	}
	copied.Items[0].Name = "changed"
	if original.Items[0].Name != "hub-1" {
		t.Error("modifying copy Items affected original")
	}
	var _ runtime.Object = original.DeepCopyObject()
}

func TestTrafficReplayListDeepCopy(t *testing.T) {
	original := &TrafficReplayList{
		Items: []TrafficReplay{
			{ObjectMeta: metav1.ObjectMeta{Name: "tr-1"}},
		},
	}
	copied := original.DeepCopy()
	if len(copied.Items) != 1 {
		t.Fatal("Items length mismatch")
	}
	copied.Items[0].Name = "changed"
	if original.Items[0].Name != "tr-1" {
		t.Error("modifying copy Items affected original")
	}
	var _ runtime.Object = original.DeepCopyObject()
}
