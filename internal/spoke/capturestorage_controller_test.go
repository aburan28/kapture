package spoke

import (
	"context"
	"errors"
	"testing"
	"time"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
	"github.com/kapture-io/kapture/internal/storage"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestValidateCaptureStorageSpec_Plugin(t *testing.T) {
	orig := validatePluginConfig
	t.Cleanup(func() { validatePluginConfig = orig })

	called := false
	validatePluginConfig = func(cfg storage.PluginConfig) error {
		called = true
		if cfg.Path != "/plugins/backend.so" {
			t.Fatalf("unexpected plugin path: %q", cfg.Path)
		}
		if cfg.Symbol != "BuildFactory" {
			t.Fatalf("unexpected symbol: %q", cfg.Symbol)
		}
		if got, ok := cfg.Config["bucket"]; !ok || got != "captures" {
			t.Fatalf("unexpected plugin config: %#v", cfg.Config)
		}
		return nil
	}

	raw := &runtime.RawExtension{Raw: []byte(`{"bucket":"captures"}`)}
	symbol := "BuildFactory"
	err := validateCaptureStorageSpec(capturev1alpha1.CaptureStorageSpec{
		Type: capturev1alpha1.CaptureStorageTypePlugin,
		Plugin: &capturev1alpha1.PluginConfig{
			Path:   "/plugins/backend.so",
			Symbol: &symbol,
			Config: raw,
		},
	})
	if err != nil {
		t.Fatalf("validateCaptureStorageSpec() error = %v", err)
	}
	if !called {
		t.Fatal("expected plugin validator to be called")
	}
}

func TestValidateCaptureStorageSpec_PluginError(t *testing.T) {
	orig := validatePluginConfig
	t.Cleanup(func() { validatePluginConfig = orig })

	validatePluginConfig = func(cfg storage.PluginConfig) error {
		return errors.New("boom")
	}

	err := validateCaptureStorageSpec(capturev1alpha1.CaptureStorageSpec{
		Type: capturev1alpha1.CaptureStorageTypePlugin,
		Plugin: &capturev1alpha1.PluginConfig{
			Path: "/plugins/backend.so",
		},
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

// Helper to create a CaptureStorage object for reconciler tests.
func newCaptureStorage(name, namespace string, spec capturev1alpha1.CaptureStorageSpec) *capturev1alpha1.CaptureStorage {
	return &capturev1alpha1.CaptureStorage{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: spec,
	}
}

// Helper to build a CaptureStorageReconciler with a fake client.
func newTestReconciler(cs *capturev1alpha1.CaptureStorage) (*CaptureStorageReconciler, *runtime.Scheme) {
	scheme := testScheme()
	builder := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&capturev1alpha1.CaptureStorage{})
	if cs != nil {
		builder = builder.WithObjects(cs)
	}
	cl := builder.Build()
	return &CaptureStorageReconciler{Client: cl, Scheme: scheme}, scheme
}

func TestCaptureStorageReconcile_ValidS3_SetsReady(t *testing.T) {
	cs := newCaptureStorage("s3-storage", "default", capturev1alpha1.CaptureStorageSpec{
		Type: capturev1alpha1.CaptureStorageTypeS3,
		S3: &capturev1alpha1.S3Config{
			Bucket: "my-bucket",
			Region: "us-east-1",
		},
	})
	r, _ := newTestReconciler(cs)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "s3-storage", Namespace: "default"}}
	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected no requeue, got %v", result.RequeueAfter)
	}

	got := &capturev1alpha1.CaptureStorage{}
	if err := r.Get(context.Background(), req.NamespacedName, got); err != nil {
		t.Fatalf("failed to get CaptureStorage: %v", err)
	}
	if got.Status.Phase != capturev1alpha1.CaptureStoragePhaseReady {
		t.Fatalf("expected phase Ready, got %q", got.Status.Phase)
	}
	if len(got.Status.Conditions) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(got.Status.Conditions))
	}
	if got.Status.Conditions[0].Status != metav1.ConditionTrue {
		t.Fatalf("expected condition True, got %v", got.Status.Conditions[0].Status)
	}
	if got.Status.Conditions[0].Reason != "Validated" {
		t.Fatalf("expected reason Validated, got %q", got.Status.Conditions[0].Reason)
	}
}

func TestCaptureStorageReconcile_InvalidS3_SetsError(t *testing.T) {
	cs := newCaptureStorage("bad-s3", "default", capturev1alpha1.CaptureStorageSpec{
		Type: capturev1alpha1.CaptureStorageTypeS3,
		S3: &capturev1alpha1.S3Config{
			Bucket: "", // missing bucket
			Region: "us-east-1",
		},
	})
	r, _ := newTestReconciler(cs)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "bad-s3", Namespace: "default"}}
	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != 30*time.Second {
		t.Fatalf("expected requeue after 30s, got %v", result.RequeueAfter)
	}

	got := &capturev1alpha1.CaptureStorage{}
	if err := r.Get(context.Background(), req.NamespacedName, got); err != nil {
		t.Fatalf("failed to get CaptureStorage: %v", err)
	}
	if got.Status.Phase != capturev1alpha1.CaptureStoragePhaseError {
		t.Fatalf("expected phase Error, got %q", got.Status.Phase)
	}
	if len(got.Status.Conditions) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(got.Status.Conditions))
	}
	if got.Status.Conditions[0].Status != metav1.ConditionFalse {
		t.Fatalf("expected condition False, got %v", got.Status.Conditions[0].Status)
	}
	if got.Status.Conditions[0].Reason != "ValidationFailed" {
		t.Fatalf("expected reason ValidationFailed, got %q", got.Status.Conditions[0].Reason)
	}
}

func TestCaptureStorageReconcile_NotFound_ReturnsNil(t *testing.T) {
	r, _ := newTestReconciler(nil)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "nonexistent", Namespace: "default"}}
	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile() error = %v, expected nil for not-found", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected no requeue, got %v", result.RequeueAfter)
	}
}

func TestCaptureStorageReconcile_ValidGCS_SetsReady(t *testing.T) {
	cs := newCaptureStorage("gcs-storage", "default", capturev1alpha1.CaptureStorageSpec{
		Type: capturev1alpha1.CaptureStorageTypeGCS,
		GCS: &capturev1alpha1.GCSConfig{
			Bucket: "my-gcs-bucket",
		},
	})
	r, _ := newTestReconciler(cs)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "gcs-storage", Namespace: "default"}}
	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected no requeue, got %v", result.RequeueAfter)
	}

	got := &capturev1alpha1.CaptureStorage{}
	if err := r.Get(context.Background(), req.NamespacedName, got); err != nil {
		t.Fatalf("failed to get CaptureStorage: %v", err)
	}
	if got.Status.Phase != capturev1alpha1.CaptureStoragePhaseReady {
		t.Fatalf("expected phase Ready, got %q", got.Status.Phase)
	}
	if got.Status.Conditions[0].Status != metav1.ConditionTrue {
		t.Fatalf("expected condition True, got %v", got.Status.Conditions[0].Status)
	}
}

func TestCaptureStorageReconcile_ValidEFS_SetsReady(t *testing.T) {
	cs := newCaptureStorage("efs-storage", "default", capturev1alpha1.CaptureStorageSpec{
		Type: capturev1alpha1.CaptureStorageTypeEFS,
		EFS: &capturev1alpha1.EFSConfig{
			FileSystemID: "fs-12345678",
			MountPath:    "/mnt/efs",
		},
	})
	r, _ := newTestReconciler(cs)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "efs-storage", Namespace: "default"}}
	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected no requeue, got %v", result.RequeueAfter)
	}

	got := &capturev1alpha1.CaptureStorage{}
	if err := r.Get(context.Background(), req.NamespacedName, got); err != nil {
		t.Fatalf("failed to get CaptureStorage: %v", err)
	}
	if got.Status.Phase != capturev1alpha1.CaptureStoragePhaseReady {
		t.Fatalf("expected phase Ready, got %q", got.Status.Phase)
	}
	if got.Status.Conditions[0].Status != metav1.ConditionTrue {
		t.Fatalf("expected condition True, got %v", got.Status.Conditions[0].Status)
	}
}

func TestCaptureStorageReconcile_ValidEBS_SetsReady(t *testing.T) {
	cs := newCaptureStorage("ebs-storage", "default", capturev1alpha1.CaptureStorageSpec{
		Type: capturev1alpha1.CaptureStorageTypeEBS,
		EBS: &capturev1alpha1.EBSConfig{
			VolumeID:  "vol-abc123",
			MountPath: "/mnt/ebs",
		},
	})
	r, _ := newTestReconciler(cs)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "ebs-storage", Namespace: "default"}}
	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected no requeue, got %v", result.RequeueAfter)
	}

	got := &capturev1alpha1.CaptureStorage{}
	if err := r.Get(context.Background(), req.NamespacedName, got); err != nil {
		t.Fatalf("failed to get CaptureStorage: %v", err)
	}
	if got.Status.Phase != capturev1alpha1.CaptureStoragePhaseReady {
		t.Fatalf("expected phase Ready, got %q", got.Status.Phase)
	}
	if got.Status.Conditions[0].Status != metav1.ConditionTrue {
		t.Fatalf("expected condition True, got %v", got.Status.Conditions[0].Status)
	}
}

func TestCaptureStorageReconcile_UnsupportedType_SetsError(t *testing.T) {
	cs := newCaptureStorage("bad-type", "default", capturev1alpha1.CaptureStorageSpec{
		Type: capturev1alpha1.CaptureStorageType("FTP"),
	})
	r, _ := newTestReconciler(cs)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "bad-type", Namespace: "default"}}
	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != 30*time.Second {
		t.Fatalf("expected requeue after 30s, got %v", result.RequeueAfter)
	}

	got := &capturev1alpha1.CaptureStorage{}
	if err := r.Get(context.Background(), req.NamespacedName, got); err != nil {
		t.Fatalf("failed to get CaptureStorage: %v", err)
	}
	if got.Status.Phase != capturev1alpha1.CaptureStoragePhaseError {
		t.Fatalf("expected phase Error, got %q", got.Status.Phase)
	}
	if got.Status.Conditions[0].Status != metav1.ConditionFalse {
		t.Fatalf("expected condition False, got %v", got.Status.Conditions[0].Status)
	}
	if got.Status.Conditions[0].Reason != "ValidationFailed" {
		t.Fatalf("expected reason ValidationFailed, got %q", got.Status.Conditions[0].Reason)
	}
}

func TestValidateCaptureStorageSpec_AllTypes(t *testing.T) {
	// Stub out plugin validation for plugin test cases.
	orig := validatePluginConfig
	t.Cleanup(func() { validatePluginConfig = orig })
	validatePluginConfig = func(cfg storage.PluginConfig) error { return nil }

	tests := []struct {
		name    string
		spec    capturev1alpha1.CaptureStorageSpec
		wantErr bool
	}{
		{
			name: "valid S3",
			spec: capturev1alpha1.CaptureStorageSpec{
				Type: capturev1alpha1.CaptureStorageTypeS3,
				S3:   &capturev1alpha1.S3Config{Bucket: "b", Region: "us-east-1"},
			},
			wantErr: false,
		},
		{
			name: "S3 missing bucket",
			spec: capturev1alpha1.CaptureStorageSpec{
				Type: capturev1alpha1.CaptureStorageTypeS3,
				S3:   &capturev1alpha1.S3Config{Bucket: "", Region: "us-east-1"},
			},
			wantErr: true,
		},
		{
			name: "S3 missing region",
			spec: capturev1alpha1.CaptureStorageSpec{
				Type: capturev1alpha1.CaptureStorageTypeS3,
				S3:   &capturev1alpha1.S3Config{Bucket: "b", Region: ""},
			},
			wantErr: true,
		},
		{
			name: "S3 nil config",
			spec: capturev1alpha1.CaptureStorageSpec{
				Type: capturev1alpha1.CaptureStorageTypeS3,
			},
			wantErr: true,
		},
		{
			name: "valid GCS",
			spec: capturev1alpha1.CaptureStorageSpec{
				Type: capturev1alpha1.CaptureStorageTypeGCS,
				GCS:  &capturev1alpha1.GCSConfig{Bucket: "b"},
			},
			wantErr: false,
		},
		{
			name: "GCS missing bucket",
			spec: capturev1alpha1.CaptureStorageSpec{
				Type: capturev1alpha1.CaptureStorageTypeGCS,
				GCS:  &capturev1alpha1.GCSConfig{Bucket: ""},
			},
			wantErr: true,
		},
		{
			name: "GCS nil config",
			spec: capturev1alpha1.CaptureStorageSpec{
				Type: capturev1alpha1.CaptureStorageTypeGCS,
			},
			wantErr: true,
		},
		{
			name: "valid EFS",
			spec: capturev1alpha1.CaptureStorageSpec{
				Type: capturev1alpha1.CaptureStorageTypeEFS,
				EFS:  &capturev1alpha1.EFSConfig{FileSystemID: "fs-123", MountPath: "/mnt"},
			},
			wantErr: false,
		},
		{
			name: "EFS missing fileSystemID",
			spec: capturev1alpha1.CaptureStorageSpec{
				Type: capturev1alpha1.CaptureStorageTypeEFS,
				EFS:  &capturev1alpha1.EFSConfig{FileSystemID: "", MountPath: "/mnt"},
			},
			wantErr: true,
		},
		{
			name: "EFS missing mountPath",
			spec: capturev1alpha1.CaptureStorageSpec{
				Type: capturev1alpha1.CaptureStorageTypeEFS,
				EFS:  &capturev1alpha1.EFSConfig{FileSystemID: "fs-123", MountPath: ""},
			},
			wantErr: true,
		},
		{
			name: "EFS nil config",
			spec: capturev1alpha1.CaptureStorageSpec{
				Type: capturev1alpha1.CaptureStorageTypeEFS,
			},
			wantErr: true,
		},
		{
			name: "valid EBS",
			spec: capturev1alpha1.CaptureStorageSpec{
				Type: capturev1alpha1.CaptureStorageTypeEBS,
				EBS:  &capturev1alpha1.EBSConfig{VolumeID: "vol-123", MountPath: "/mnt"},
			},
			wantErr: false,
		},
		{
			name: "EBS missing volumeID",
			spec: capturev1alpha1.CaptureStorageSpec{
				Type: capturev1alpha1.CaptureStorageTypeEBS,
				EBS:  &capturev1alpha1.EBSConfig{VolumeID: "", MountPath: "/mnt"},
			},
			wantErr: true,
		},
		{
			name: "EBS missing mountPath",
			spec: capturev1alpha1.CaptureStorageSpec{
				Type: capturev1alpha1.CaptureStorageTypeEBS,
				EBS:  &capturev1alpha1.EBSConfig{VolumeID: "vol-123", MountPath: ""},
			},
			wantErr: true,
		},
		{
			name: "EBS nil config",
			spec: capturev1alpha1.CaptureStorageSpec{
				Type: capturev1alpha1.CaptureStorageTypeEBS,
			},
			wantErr: true,
		},
		{
			name: "valid Plugin",
			spec: capturev1alpha1.CaptureStorageSpec{
				Type:   capturev1alpha1.CaptureStorageTypePlugin,
				Plugin: &capturev1alpha1.PluginConfig{Path: "/plugins/backend.so"},
			},
			wantErr: false,
		},
		{
			name: "Plugin missing path",
			spec: capturev1alpha1.CaptureStorageSpec{
				Type:   capturev1alpha1.CaptureStorageTypePlugin,
				Plugin: &capturev1alpha1.PluginConfig{Path: ""},
			},
			wantErr: true,
		},
		{
			name: "Plugin nil config",
			spec: capturev1alpha1.CaptureStorageSpec{
				Type: capturev1alpha1.CaptureStorageTypePlugin,
			},
			wantErr: true,
		},
		{
			name: "unsupported type",
			spec: capturev1alpha1.CaptureStorageSpec{
				Type: capturev1alpha1.CaptureStorageType("FTP"),
			},
			wantErr: true,
		},
		{
			name: "S3 whitespace-only bucket",
			spec: capturev1alpha1.CaptureStorageSpec{
				Type: capturev1alpha1.CaptureStorageTypeS3,
				S3:   &capturev1alpha1.S3Config{Bucket: "   ", Region: "us-east-1"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCaptureStorageSpec(tt.spec)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateCaptureStorageSpec() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
