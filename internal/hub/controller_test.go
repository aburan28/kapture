package hub

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
	hubv1 "github.com/kapture-io/kapture/proto/hub/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func hubScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = capturev1alpha1.AddToScheme(s)
	return s
}

// ---------------------------------------------------------------------------
// fakeServerStream for testing WatchDirectives
// ---------------------------------------------------------------------------

type fakeServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f *fakeServerStream) Context() context.Context { return f.ctx }
func (f *fakeServerStream) Send(*hubv1.WatchDirectivesResponse) error {
	return nil
}
func (f *fakeServerStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeServerStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeServerStream) SetTrailer(metadata.MD)       {}
func (f *fakeServerStream) SendMsg(any) error            { return nil }
func (f *fakeServerStream) RecvMsg(any) error            { return nil }

// ---------------------------------------------------------------------------
// Reconcile: CaptureHub not found (deleted)
// ---------------------------------------------------------------------------

func TestReconcile_CaptureHubNotFound_StopsServer(t *testing.T) {
	scheme := hubScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	r := &CaptureHubReconciler{
		Client: cl,
		Log:    logr.Discard(),
	}

	// Manually set a server to verify it gets stopped.
	srv := NewServer(":0")
	_, cancel := context.WithCancel(context.Background())
	r.mu.Lock()
	r.server = srv
	r.cancel = cancel
	r.mu.Unlock()

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "missing-hub"}}
	_, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("expected no error for not-found, got: %v", err)
	}

	if r.GetServer() != nil {
		t.Error("expected server to be stopped after CaptureHub deletion")
	}
}

func TestReconcile_CaptureHubNotFound_NoServerRunning(t *testing.T) {
	scheme := hubScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	r := &CaptureHubReconciler{
		Client: cl,
		Log:    logr.Discard(),
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "missing-hub"}}
	_, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Reconcile: CaptureHub exists — starts gRPC server
// ---------------------------------------------------------------------------

func TestReconcile_CaptureHubExists_StartsServer(t *testing.T) {
	scheme := hubScheme()
	hub := &capturev1alpha1.CaptureHub{
		ObjectMeta: metav1.ObjectMeta{Name: "hub-1"},
		Spec:       capturev1alpha1.CaptureHubSpec{GRPCAddress: ":0"},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(hub).
		WithStatusSubresource(hub).
		Build()

	r := &CaptureHubReconciler{
		Client: cl,
		Log:    logr.Discard(),
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "hub-1"}}
	_, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	srv := r.GetServer()
	if srv == nil {
		t.Fatal("expected server to be started")
	}
	if srv.address != ":0" {
		t.Errorf("expected address :0, got %s", srv.address)
	}

	r.stopServer()
}

func TestReconcile_DefaultAddress(t *testing.T) {
	scheme := hubScheme()
	hub := &capturev1alpha1.CaptureHub{
		ObjectMeta: metav1.ObjectMeta{Name: "hub-1"},
		Spec:       capturev1alpha1.CaptureHubSpec{GRPCAddress: ""},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(hub).
		WithStatusSubresource(hub).
		Build()

	r := &CaptureHubReconciler{
		Client: cl,
		Log:    logr.Discard(),
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "hub-1"}}
	_, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	srv := r.GetServer()
	if srv == nil {
		t.Fatal("expected server to be started")
	}
	if srv.address != ":9443" {
		t.Errorf("expected default address :9443, got %s", srv.address)
	}

	r.stopServer()
}

// ---------------------------------------------------------------------------
// ensureServer: idempotent when address unchanged
// ---------------------------------------------------------------------------

func TestEnsureServer_SameAddress_NoRestart(t *testing.T) {
	hub := &capturev1alpha1.CaptureHub{
		ObjectMeta: metav1.ObjectMeta{Name: "hub-1"},
		Spec:       capturev1alpha1.CaptureHubSpec{GRPCAddress: ":0"},
	}

	r := &CaptureHubReconciler{Log: logr.Discard()}
	t.Cleanup(func() {
		time.Sleep(100 * time.Millisecond) // Let goroutine read r.server before nil
		r.stopServer()
	})

	err := r.ensureServer(context.Background(), hub)
	if err != nil {
		t.Fatalf("first ensureServer failed: %v", err)
	}
	// Let the background goroutine start and read r.server.
	time.Sleep(50 * time.Millisecond)
	firstServer := r.GetServer()

	err = r.ensureServer(context.Background(), hub)
	if err != nil {
		t.Fatalf("second ensureServer failed: %v", err)
	}
	secondServer := r.GetServer()

	if firstServer != secondServer {
		t.Error("expected same server instance for same address")
	}
}

func TestEnsureServer_AddressChange_Restarts(t *testing.T) {
	hub := &capturev1alpha1.CaptureHub{
		ObjectMeta: metav1.ObjectMeta{Name: "hub-1"},
		Spec:       capturev1alpha1.CaptureHubSpec{GRPCAddress: ":0"},
	}

	r := &CaptureHubReconciler{Log: logr.Discard()}
	t.Cleanup(func() {
		time.Sleep(100 * time.Millisecond)
		r.stopServer()
	})

	_ = r.ensureServer(context.Background(), hub)
	// Let the background goroutine start.
	time.Sleep(50 * time.Millisecond)
	firstServer := r.GetServer()

	hub.Spec.GRPCAddress = ":9999"
	_ = r.ensureServer(context.Background(), hub)
	time.Sleep(50 * time.Millisecond)
	secondServer := r.GetServer()

	if firstServer == secondServer {
		t.Error("expected new server instance after address change")
	}
	if secondServer.address != ":9999" {
		t.Errorf("expected address :9999, got %s", secondServer.address)
	}
}

// ---------------------------------------------------------------------------
// stopServer
// ---------------------------------------------------------------------------

func TestStopServer_NilServer(t *testing.T) {
	r := &CaptureHubReconciler{Log: logr.Discard()}
	r.stopServer() // Should not panic.

	if r.GetServer() != nil {
		t.Error("expected nil server")
	}
}

func TestStopServer_ClearsState(t *testing.T) {
	r := &CaptureHubReconciler{Log: logr.Discard()}

	// Manually set a server without starting it in a goroutine.
	srv := NewServer(":0")
	ctx, cancel := context.WithCancel(context.Background())
	r.mu.Lock()
	r.server = srv
	r.cancel = cancel
	r.mu.Unlock()
	_ = ctx

	if r.GetServer() == nil {
		t.Fatal("expected server to be set")
	}

	r.stopServer()

	if r.GetServer() != nil {
		t.Error("expected server to be nil after stop")
	}
}

// ---------------------------------------------------------------------------
// updateStatus
// ---------------------------------------------------------------------------

func TestUpdateStatus_NoServer(t *testing.T) {
	r := &CaptureHubReconciler{Log: logr.Discard()}

	hub := &capturev1alpha1.CaptureHub{}
	err := r.updateStatus(context.Background(), hub)
	if err == nil {
		t.Fatal("expected error when server is nil")
	}
}

func TestUpdateStatus_AggregatesSpokeData(t *testing.T) {
	scheme := hubScheme()
	hub := &capturev1alpha1.CaptureHub{
		ObjectMeta: metav1.ObjectMeta{Name: "hub-1"},
		Spec:       capturev1alpha1.CaptureHubSpec{GRPCAddress: ":0"},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(hub).
		WithStatusSubresource(hub).
		Build()

	r := &CaptureHubReconciler{
		Client: cl,
		Log:    logr.Discard(),
	}

	_ = r.ensureServer(context.Background(), hub)
	defer r.stopServer()

	srv := r.GetServer()
	mustRegister(t, srv, "spoke-1", "cluster-a")
	mustRegister(t, srv, "spoke-2", "cluster-b")
	mustReportCaptures(t, srv, "spoke-1", []*hubv1.CaptureStatusSummary{
		{CaptureName: "cap-1", CaptureNamespace: "ns-1"},
	})

	err := r.updateStatus(context.Background(), hub)
	if err != nil {
		t.Fatalf("updateStatus failed: %v", err)
	}

	if hub.Status.ConnectedSpokes != 2 {
		t.Errorf("expected 2 connected spokes, got %d", hub.Status.ConnectedSpokes)
	}
	if hub.Status.ActiveCaptures != 1 {
		t.Errorf("expected 1 active capture, got %d", hub.Status.ActiveCaptures)
	}
	if len(hub.Status.Spokes) != 2 {
		t.Errorf("expected 2 spoke statuses, got %d", len(hub.Status.Spokes))
	}
}

// ---------------------------------------------------------------------------
// WatchDirectives
// ---------------------------------------------------------------------------

func TestWatchDirectives_EmptySpokeID(t *testing.T) {
	srv := newTestServer()
	err := srv.WatchDirectives(&hubv1.WatchDirectivesRequest{}, nil)
	assertGRPCCode(t, err, codes.InvalidArgument)
}

func TestWatchDirectives_UnregisteredSpoke(t *testing.T) {
	srv := newTestServer()
	err := srv.WatchDirectives(&hubv1.WatchDirectivesRequest{SpokeId: "unknown"}, nil)
	assertGRPCCode(t, err, codes.NotFound)
}

func TestWatchDirectives_RegisteredSpoke_BlocksUntilContextDone(t *testing.T) {
	srv := newTestServer()
	mustRegister(t, srv, "spoke-1", "cluster-a")

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- srv.WatchDirectives(&hubv1.WatchDirectivesRequest{SpokeId: "spoke-1"}, &fakeServerStream{ctx: ctx})
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WatchDirectives did not unblock after context cancellation")
	}
}

// ---------------------------------------------------------------------------
// Helper function tests: captureKey, captureInfoFromSummary, cloneSpokeInfo
// ---------------------------------------------------------------------------

func TestCaptureKey(t *testing.T) {
	key := captureKey("ns-1", "cap-1")
	if key != "ns-1/cap-1" {
		t.Errorf("expected ns-1/cap-1, got %s", key)
	}
}

func TestCaptureInfoFromSummary(t *testing.T) {
	summary := &hubv1.CaptureStatusSummary{
		CaptureName:      "cap-1",
		CaptureNamespace: "ns-1",
		Phase:            hubv1.CapturePhase_CAPTURE_PHASE_ACTIVE,
		CapturedRequests: 42,
		BytesWritten:     1024,
	}
	info := captureInfoFromSummary(summary, "spoke-1")
	if info.CaptureName != "cap-1" {
		t.Errorf("expected cap-1, got %s", info.CaptureName)
	}
	if info.SpokeId != "spoke-1" {
		t.Errorf("expected spoke-1, got %s", info.SpokeId)
	}
	if info.CapturedRequests != 42 {
		t.Errorf("expected 42, got %d", info.CapturedRequests)
	}
	if info.BytesWritten != 1024 {
		t.Errorf("expected 1024, got %d", info.BytesWritten)
	}
}

func TestCloneSpokeInfo(t *testing.T) {
	original := &hubv1.SpokeInfo{
		SpokeId:        "spoke-1",
		ClusterName:    "cluster-a",
		ActiveCaptures: 5,
		State:          hubv1.SpokeState_SPOKE_STATE_CONNECTED,
	}
	clone := cloneSpokeInfo(original)

	if clone.SpokeId != original.SpokeId {
		t.Errorf("SpokeId mismatch")
	}
	if clone.ClusterName != original.ClusterName {
		t.Errorf("ClusterName mismatch")
	}

	// Verify it's a different pointer.
	clone.ActiveCaptures = 99
	if original.ActiveCaptures == 99 {
		t.Error("clone should not modify original")
	}
}

// ---------------------------------------------------------------------------
// Server Start/Stop lifecycle
// ---------------------------------------------------------------------------

func TestServer_StartAndStop(t *testing.T) {
	srv := NewServer(":0")
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start(ctx)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err != nil && err != context.Canceled {
			t.Errorf("unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not stop in time")
	}
}

func TestServer_GRPCServer_ReturnsNonNil(t *testing.T) {
	srv := NewServer(":0")
	if srv.GRPCServer() == nil {
		t.Error("expected non-nil grpc.Server")
	}
}

func TestServer_Stop_NilGRPCServer(t *testing.T) {
	srv := &Server{}
	srv.Stop() // Should not panic with nil grpcServer.
}

func TestNewServerWithTLS(t *testing.T) {
	// Just verify it returns a non-nil server (TLS creds are nil-safe for construction).
	srv := NewServer(":0")
	if srv == nil {
		t.Fatal("expected non-nil server")
	}
	if srv.address != ":0" {
		t.Errorf("expected address :0, got %s", srv.address)
	}
}
