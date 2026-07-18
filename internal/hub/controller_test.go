package hub

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
	hubv1 "github.com/kapture-io/kapture/proto/hub/v1"
)

// freePort reserves an ephemeral port and returns its address. The listener
// is closed so the server can re-listen on it.
func freePort(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close()
	return addr
}

func waitForServerListen(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("server never listened on %s", addr)
}

// TestStop_ReturnsWithActiveWatchStream is a regression test for the hub
// freeze seen in e2e: Stop used a bare GracefulStop, which waits for
// in-flight RPCs, and the long-lived WatchDirectives stream held by a
// connected spoke never returns on its own — so Stop blocked forever while
// the CaptureHub reconciler held its mutex.
func TestStop_ReturnsWithActiveWatchStream(t *testing.T) {
	addr := freePort(t)
	srv := NewServer(addr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = srv.Start(ctx) }()
	waitForServerListen(t, addr)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := hubv1.NewHubServiceClient(conn)

	if _, err := client.RegisterSpoke(ctx, &hubv1.RegisterSpokeRequest{
		SpokeId:     "spoke-1",
		ClusterName: "spoke-1",
	}); err != nil {
		t.Fatalf("RegisterSpoke: %v", err)
	}

	stream, err := client.WatchDirectives(ctx, &hubv1.WatchDirectivesRequest{SpokeId: "spoke-1"})
	if err != nil {
		t.Fatalf("WatchDirectives: %v", err)
	}

	// Prove the server-side handler is running by receiving one directive
	// through the stream before stopping.
	if err := srv.SendDirective("spoke-1", &hubv1.CaptureDirective{
		DirectiveId: "d-1",
		Action:      hubv1.DirectiveAction_DIRECTIVE_ACTION_START,
	}); err != nil {
		t.Fatalf("SendDirective: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("Recv: %v", err)
	}

	stopped := make(chan struct{})
	go func() {
		srv.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(StopGracePeriod + 5*time.Second):
		t.Fatal("Stop did not return with an active WatchDirectives stream")
	}

	// The spoke's stream must be terminated so it reconnects.
	if _, err := stream.Recv(); err == nil {
		t.Fatal("stream still delivering after Stop")
	}
}

func newHubReconciler(t *testing.T, objs ...*capturev1alpha1.CaptureHub) *CaptureHubReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := capturev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	builder := fake.NewClientBuilder().WithScheme(scheme)
	for _, o := range objs {
		builder = builder.WithObjects(o).WithStatusSubresource(o)
	}
	return &CaptureHubReconciler{
		Client: builder.Build(),
		Log:    logr.Discard(),
	}
}

func captureHub(name, address string, created time.Time) *capturev1alpha1.CaptureHub {
	return &capturev1alpha1.CaptureHub{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(created),
		},
		Spec: capturev1alpha1.CaptureHubSpec{GRPCAddress: address},
	}
}

// TestReconcile_SecondCaptureHubDoesNotRetargetServer covers the other half
// of the e2e failure: a newer CaptureHub CR (created by a test, or by
// mistake) must not reconfigure or stop the gRPC server the authoritative
// CR established.
func TestReconcile_SecondCaptureHubDoesNotRetargetServer(t *testing.T) {
	base := time.Now().Add(-time.Hour)
	primary := captureHub("primary", freePort(t), base)
	extra := captureHub("extra", freePort(t), base.Add(time.Minute))

	r := newHubReconciler(t, primary, extra)
	defer r.stopServer()
	ctx := context.Background()

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "primary"}}); err != nil {
		t.Fatalf("reconcile primary: %v", err)
	}
	first := r.GetServer()
	if first == nil {
		t.Fatal("no server after reconciling primary")
	}
	if first.address != primary.Spec.GRPCAddress {
		t.Fatalf("server address = %q, want authoritative %q", first.address, primary.Spec.GRPCAddress)
	}

	// Reconciling the newer CR must leave the running server untouched.
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "extra"}}); err != nil {
		t.Fatalf("reconcile extra: %v", err)
	}
	if got := r.GetServer(); got != first {
		t.Fatal("reconciling a non-authoritative CaptureHub replaced the server")
	}

	// The ignored CR must say so in its status.
	var ignored capturev1alpha1.CaptureHub
	if err := r.Get(ctx, types.NamespacedName{Name: "extra"}, &ignored); err != nil {
		t.Fatal(err)
	}
	cond := meta.FindStatusCondition(ignored.Status.Conditions, capturev1alpha1.CaptureHubConditionActive)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "NotAuthoritative" {
		t.Fatalf("extra CaptureHub Active condition = %+v, want False/NotAuthoritative", cond)
	}

	// Deleting the newer CR must not stop the server either.
	if err := r.Delete(ctx, extra); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "extra"}}); err != nil {
		t.Fatalf("reconcile deleted extra: %v", err)
	}
	if got := r.GetServer(); got != first {
		t.Fatal("deleting a non-authoritative CaptureHub disturbed the server")
	}
}

// TestReconcile_DeletingAuthoritativeHubFailsOver verifies the server
// retargets to the next-oldest CaptureHub when the authoritative one is
// deleted, and stops only when none remain.
func TestReconcile_DeletingAuthoritativeHubFailsOver(t *testing.T) {
	base := time.Now().Add(-time.Hour)
	primary := captureHub("primary", freePort(t), base)
	secondary := captureHub("secondary", freePort(t), base.Add(time.Minute))

	r := newHubReconciler(t, primary, secondary)
	defer r.stopServer()
	ctx := context.Background()

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "primary"}}); err != nil {
		t.Fatalf("reconcile primary: %v", err)
	}

	if err := r.Delete(ctx, primary); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "primary"}}); err != nil {
		t.Fatalf("reconcile after deleting primary: %v", err)
	}
	srv := r.GetServer()
	if srv == nil {
		t.Fatal("server stopped even though a CaptureHub remains")
	}
	if srv.address != secondary.Spec.GRPCAddress {
		t.Fatalf("server address = %q, want failover to %q", srv.address, secondary.Spec.GRPCAddress)
	}

	if err := r.Delete(ctx, secondary); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "secondary"}}); err != nil {
		t.Fatalf("reconcile after deleting secondary: %v", err)
	}
	if r.GetServer() != nil {
		t.Fatal("server still running with no CaptureHub resources")
	}
}

func TestAuthoritativeHub_OldestWinsNameTiebreak(t *testing.T) {
	now := time.Now()
	hubs := []capturev1alpha1.CaptureHub{
		*captureHub("b-newer", ":1", now.Add(time.Minute)),
		*captureHub("z-oldest", ":2", now),
		*captureHub("a-oldest", ":3", now),
	}
	got := authoritativeHub(hubs)
	if got == nil || got.Name != "a-oldest" {
		t.Fatalf("authoritativeHub = %v, want a-oldest", got)
	}
	if authoritativeHub(nil) != nil {
		t.Fatal("authoritativeHub(nil) should be nil")
	}
}
