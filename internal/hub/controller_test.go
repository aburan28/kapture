package hub

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
	hubv1 "github.com/kapture-io/kapture/proto/hub/v1"
)

func hubTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = capturev1alpha1.AddToScheme(s)
	return s
}

func newCaptureHub(name, address string) *capturev1alpha1.CaptureHub {
	return &capturev1alpha1.CaptureHub{
		TypeMeta: metav1.TypeMeta{
			APIVersion: capturev1alpha1.GroupVersion.String(),
			Kind:       capturev1alpha1.KindCaptureHub,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: capturev1alpha1.CaptureHubSpec{
			GRPCAddress: address,
		},
	}
}

// TestReconcile_NotFound_StopsServer verifies that when the CaptureHub CR is
// deleted (not found), the reconciler stops the running gRPC server.
func TestReconcile_NotFound_StopsServer(t *testing.T) {
	scheme := hubTestScheme()

	// No CaptureHub object in the fake client — simulates deletion.
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	r := &CaptureHubReconciler{
		Client: cl,
		Log:    logr.Discard(),
	}

	// Manually start a server so we can verify it gets stopped.
	srv := NewServer(":0")
	ctx, cancel := context.WithCancel(context.Background())
	r.server = srv
	r.cancel = cancel

	go func() {
		_ = srv.Start(ctx)
	}()
	time.Sleep(50 * time.Millisecond)

	// Reconcile a non-existent CR.
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "missing-hub"}}
	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if result.Requeue {
		t.Error("expected no requeue")
	}

	// Server should be nil after deletion handling.
	if r.GetServer() != nil {
		t.Error("expected server to be nil after CR not found")
	}
}

// TestReconcile_CreatesServer verifies that when a CaptureHub CR exists,
// the reconciler starts a gRPC server and updates the CR status.
func TestReconcile_CreatesServer(t *testing.T) {
	scheme := hubTestScheme()
	hub := newCaptureHub("test-hub", ":0")

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(hub).
		WithStatusSubresource(hub).
		Build()

	r := &CaptureHubReconciler{
		Client: cl,
		Log:    logr.Discard(),
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-hub"}}
	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if result.Requeue {
		t.Error("expected no requeue")
	}

	// Give the server goroutine a moment to start.
	time.Sleep(50 * time.Millisecond)

	srv := r.GetServer()
	if srv == nil {
		t.Fatal("expected server to be started")
	}
	if srv.address != ":0" {
		t.Errorf("server address = %q, want %q", srv.address, ":0")
	}

	// Verify status was updated (should be 0 spokes, 0 captures).
	var updated capturev1alpha1.CaptureHub
	if err := cl.Get(context.Background(), types.NamespacedName{Name: "test-hub"}, &updated); err != nil {
		t.Fatalf("failed to get CaptureHub: %v", err)
	}
	if updated.Status.ConnectedSpokes != 0 {
		t.Errorf("ConnectedSpokes = %d, want 0", updated.Status.ConnectedSpokes)
	}
	if updated.Status.ActiveCaptures != 0 {
		t.Errorf("ActiveCaptures = %d, want 0", updated.Status.ActiveCaptures)
	}

	// Cleanup.
	r.stopServer()
}

// TestReconcile_DefaultAddress verifies that when GRPCAddress is empty,
// the reconciler defaults to ":9443".
func TestReconcile_DefaultAddress(t *testing.T) {
	scheme := hubTestScheme()
	hub := newCaptureHub("test-hub", "") // empty address

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(hub).
		WithStatusSubresource(hub).
		Build()

	r := &CaptureHubReconciler{
		Client: cl,
		Log:    logr.Discard(),
	}

	// Call ensureServer directly to verify the default address without
	// needing the full Reconcile (which would also call updateStatus).
	err := r.ensureServer(context.Background(), hub)
	if err != nil {
		t.Fatalf("ensureServer failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	srv := r.GetServer()
	if srv == nil {
		t.Fatal("expected server to be started")
	}
	if srv.address != ":9443" {
		t.Errorf("server address = %q, want %q", srv.address, ":9443")
	}

	// Cleanup.
	r.stopServer()
}

// TestReconcile_ReconfiguresServerOnAddressChange verifies that when the
// GRPCAddress changes, the old server is stopped and a new one is started.
func TestReconcile_ReconfiguresServerOnAddressChange(t *testing.T) {
	scheme := hubTestScheme()
	hub := newCaptureHub("test-hub", ":0")

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(hub).
		WithStatusSubresource(hub).
		Build()

	r := &CaptureHubReconciler{
		Client: cl,
		Log:    logr.Discard(),
	}

	// First reconcile: start server on :0.
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-hub"}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("first reconcile failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	firstServer := r.GetServer()
	if firstServer == nil {
		t.Fatal("expected server after first reconcile")
	}

	// Update the CaptureHub to use a different address.
	var current capturev1alpha1.CaptureHub
	if err := cl.Get(context.Background(), types.NamespacedName{Name: "test-hub"}, &current); err != nil {
		t.Fatalf("failed to get hub: %v", err)
	}
	current.Spec.GRPCAddress = "127.0.0.1:0"
	if err := cl.Update(context.Background(), &current); err != nil {
		t.Fatalf("failed to update hub: %v", err)
	}

	// Second reconcile: should stop old server, start new one.
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	secondServer := r.GetServer()
	if secondServer == nil {
		t.Fatal("expected server after second reconcile")
	}
	if secondServer == firstServer {
		t.Error("expected a new server instance after address change")
	}
	if secondServer.address != "127.0.0.1:0" {
		t.Errorf("server address = %q, want %q", secondServer.address, "127.0.0.1:0")
	}

	// Cleanup.
	r.stopServer()
}

// TestReconcile_NoOpIfServerOnSameAddress verifies that if the server is
// already running on the same address, it is not restarted.
func TestReconcile_NoOpIfServerOnSameAddress(t *testing.T) {
	scheme := hubTestScheme()
	hub := newCaptureHub("test-hub", ":0")

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(hub).
		WithStatusSubresource(hub).
		Build()

	r := &CaptureHubReconciler{
		Client: cl,
		Log:    logr.Discard(),
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-hub"}}

	// First reconcile.
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("first reconcile failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	firstServer := r.GetServer()
	if firstServer == nil {
		t.Fatal("expected server after first reconcile")
	}

	// Second reconcile with same address.
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}

	secondServer := r.GetServer()
	if secondServer != firstServer {
		t.Error("expected same server instance when address did not change")
	}

	// Cleanup.
	r.stopServer()
}

// TestStopServer_Idempotent verifies that calling stopServer when no server
// is running does not panic or error.
func TestStopServer_Idempotent(t *testing.T) {
	r := &CaptureHubReconciler{
		Log: logr.Discard(),
	}

	// Should not panic when no server is running.
	r.stopServer()

	if r.GetServer() != nil {
		t.Error("expected server to remain nil")
	}

	// Call again to verify idempotency.
	r.stopServer()
}

// TestUpdateStatus_SetsConnectedSpokesAndActiveCaptures verifies that
// updateStatus writes aggregated spoke data to the CaptureHub CR status.
func TestUpdateStatus_SetsConnectedSpokesAndActiveCaptures(t *testing.T) {
	scheme := hubTestScheme()
	hub := newCaptureHub("test-hub", ":0")

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(hub).
		WithStatusSubresource(hub).
		Build()

	r := &CaptureHubReconciler{
		Client: cl,
		Log:    logr.Discard(),
	}

	// Start a server and register spokes with captures.
	srv := NewServer(":0")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r.mu.Lock()
	r.server = srv
	r.cancel = cancel
	r.mu.Unlock()

	go func() {
		_ = srv.Start(ctx)
	}()
	time.Sleep(50 * time.Millisecond)

	// Register two spokes with captures.
	mustRegister(t, srv, "spoke-1", "cluster-a")
	mustRegister(t, srv, "spoke-2", "cluster-b")

	mustReportCaptures(t, srv, "spoke-1", []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-1", CaptureNamespace: "ns-1", Phase: hubv1.CapturePhase_CAPTURE_PHASE_ACTIVE, CapturedRequests: 100},
		{CaptureName: "cap-2", CaptureNamespace: "ns-1", Phase: hubv1.CapturePhase_CAPTURE_PHASE_ACTIVE, CapturedRequests: 50},
	})
	mustReportCaptures(t, srv, "spoke-2", []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-3", CaptureNamespace: "ns-2", Phase: hubv1.CapturePhase_CAPTURE_PHASE_PENDING},
	})

	// Re-fetch the hub from the fake client to get the right resource version.
	var current capturev1alpha1.CaptureHub
	if err := cl.Get(context.Background(), types.NamespacedName{Name: "test-hub"}, &current); err != nil {
		t.Fatalf("failed to get hub: %v", err)
	}

	if err := r.updateStatus(context.Background(), &current); err != nil {
		t.Fatalf("updateStatus failed: %v", err)
	}

	// Re-read the CR to verify status was persisted.
	var updated capturev1alpha1.CaptureHub
	if err := cl.Get(context.Background(), types.NamespacedName{Name: "test-hub"}, &updated); err != nil {
		t.Fatalf("failed to get updated hub: %v", err)
	}

	if updated.Status.ConnectedSpokes != 2 {
		t.Errorf("ConnectedSpokes = %d, want 2", updated.Status.ConnectedSpokes)
	}
	if updated.Status.ActiveCaptures != 3 {
		t.Errorf("ActiveCaptures = %d, want 3", updated.Status.ActiveCaptures)
	}
	if len(updated.Status.Spokes) != 2 {
		t.Fatalf("len(Spokes) = %d, want 2", len(updated.Status.Spokes))
	}

	// Verify spoke details.
	spokesByName := make(map[string]capturev1alpha1.CaptureHubSpokeStatus)
	for _, s := range updated.Status.Spokes {
		spokesByName[s.Name] = s
	}

	if s, ok := spokesByName["cluster-a"]; !ok {
		t.Error("expected spoke cluster-a in status")
	} else {
		if s.ActiveCaptures != 2 {
			t.Errorf("cluster-a ActiveCaptures = %d, want 2", s.ActiveCaptures)
		}
		if s.LastHeartbeat == nil {
			t.Error("cluster-a LastHeartbeat should not be nil")
		}
	}

	if s, ok := spokesByName["cluster-b"]; !ok {
		t.Error("expected spoke cluster-b in status")
	} else {
		if s.ActiveCaptures != 1 {
			t.Errorf("cluster-b ActiveCaptures = %d, want 1", s.ActiveCaptures)
		}
		if s.LastHeartbeat == nil {
			t.Error("cluster-b LastHeartbeat should not be nil")
		}
	}

	// Cleanup.
	r.stopServer()
}
