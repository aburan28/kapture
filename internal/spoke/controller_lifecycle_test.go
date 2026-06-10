package spoke

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
)

func newLifecycleTestRoute() *gwapiv1.HTTPRoute {
	return &gwapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "test-route", Namespace: "default"},
		Spec: gwapiv1.HTTPRouteSpec{
			Rules: []gwapiv1.HTTPRouteRule{
				{
					BackendRefs: []gwapiv1.HTTPBackendRef{
						{BackendRef: gwapiv1.BackendRef{
							BackendObjectReference: gwapiv1.BackendObjectReference{
								Name: "backend-svc",
								Port: ptrTo(gwapiv1.PortNumber(8080)),
							},
						}},
					},
				},
			},
		},
	}
}

func routeHasMirrorFilter(t *testing.T, cl client.Client) bool {
	t.Helper()
	route := &gwapiv1.HTTPRoute{}
	if err := cl.Get(context.Background(), types.NamespacedName{Name: "test-route", Namespace: "default"}, route); err != nil {
		t.Fatalf("failed to get HTTPRoute: %v", err)
	}
	for _, rule := range route.Spec.Rules {
		for _, f := range rule.Filters {
			if f.Type == gwapiv1.HTTPRouteFilterRequestMirror {
				return true
			}
		}
	}
	return false
}

func assertAgentResourcesDeleted(t *testing.T, cl client.Client, tc *capturev1alpha1.TrafficCapture) {
	t.Helper()
	key := types.NamespacedName{Name: AgentDeploymentName(tc), Namespace: tc.Namespace}
	for name, obj := range map[string]client.Object{
		"Deployment": &appsv1.Deployment{},
		"Service":    &corev1.Service{},
		"HPA":        &autoscalingv2.HorizontalPodAutoscaler{},
	} {
		err := cl.Get(context.Background(), key, obj)
		if err == nil {
			t.Errorf("expected %s to be deleted, but it still exists", name)
		} else if !apierrors.IsNotFound(err) {
			t.Errorf("unexpected error getting %s: %v", name, err)
		}
	}
}

func TestReconcile_DurationExpired_StopsMirroringAndRemovesAgents(t *testing.T) {
	scheme := testScheme()
	tc := newTestTrafficCapture("capture-1", "default")
	tc.Spec.Capture.Duration = ptrTo("1m")
	storage := newTestStorage()
	route := newLifecycleTestRoute()

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(tc, storage, route).
		WithStatusSubresource(tc).
		Build()

	r := &TrafficCaptureReconciler{Client: cl, Scheme: scheme}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "capture-1", Namespace: "default"}}

	// First reconcile provisions agents and injects the mirror filter.
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("first reconcile failed: %v", err)
	}
	if !routeHasMirrorFilter(t, cl) {
		t.Fatal("expected mirror filter after first reconcile")
	}

	// Backdate the start time past the configured duration.
	updated := &capturev1alpha1.TrafficCapture{}
	if err := cl.Get(context.Background(), req.NamespacedName, updated); err != nil {
		t.Fatalf("failed to get TrafficCapture: %v", err)
	}
	start := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	updated.Status.Phase = capturev1alpha1.TrafficCapturePhaseActive
	updated.Status.StartTime = &start
	if err := cl.Status().Update(context.Background(), updated); err != nil {
		t.Fatalf("failed to update status: %v", err)
	}

	// Second reconcile must complete the capture and tear everything down.
	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue after completion, got %v", result.RequeueAfter)
	}

	if routeHasMirrorFilter(t, cl) {
		t.Error("expected mirror filter to be removed after duration expiry")
	}
	assertAgentResourcesDeleted(t, cl, updated)

	final := &capturev1alpha1.TrafficCapture{}
	if err := cl.Get(context.Background(), req.NamespacedName, final); err != nil {
		t.Fatalf("failed to get TrafficCapture: %v", err)
	}
	if final.Status.Phase != capturev1alpha1.TrafficCapturePhaseCompleted {
		t.Errorf("phase = %s, want Completed", final.Status.Phase)
	}
	foundCondition := false
	for _, c := range final.Status.Conditions {
		if c.Type == "Ready" && c.Reason == "CaptureCompleted" && c.Status == metav1.ConditionFalse {
			foundCondition = true
		}
	}
	if !foundCondition {
		t.Errorf("expected Ready=False condition with reason CaptureCompleted, got %+v", final.Status.Conditions)
	}
}

func TestReconcile_CompletedCapture_DoesNotReprovision(t *testing.T) {
	scheme := testScheme()
	tc := newTestTrafficCapture("capture-1", "default")
	tc.Finalizers = []string{finalizerName}
	tc.Status.Phase = capturev1alpha1.TrafficCapturePhaseCompleted
	storage := newTestStorage()
	route := newLifecycleTestRoute()

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(tc, storage, route).
		WithStatusSubresource(tc).
		Build()

	r := &TrafficCaptureReconciler{Client: cl, Scheme: scheme}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "capture-1", Namespace: "default"}}

	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue for completed capture, got %v", result.RequeueAfter)
	}

	assertAgentResourcesDeleted(t, cl, tc)
	if routeHasMirrorFilter(t, cl) {
		t.Error("expected no mirror filter for completed capture")
	}
}

func TestReconcile_RequeueShortensNearExpiry(t *testing.T) {
	scheme := testScheme()
	tc := newTestTrafficCapture("capture-1", "default")
	tc.Spec.Capture.Duration = ptrTo("1m")
	storage := newTestStorage()
	route := newLifecycleTestRoute()

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(tc, storage, route).
		WithStatusSubresource(tc).
		Build()

	r := &TrafficCaptureReconciler{Client: cl, Scheme: scheme}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "capture-1", Namespace: "default"}}

	// First reconcile provisions resources.
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("first reconcile failed: %v", err)
	}

	// Backdate start so ~5 seconds remain.
	updated := &capturev1alpha1.TrafficCapture{}
	if err := cl.Get(context.Background(), req.NamespacedName, updated); err != nil {
		t.Fatalf("failed to get TrafficCapture: %v", err)
	}
	start := metav1.NewTime(time.Now().Add(-55 * time.Second))
	updated.Status.StartTime = &start
	if err := cl.Status().Update(context.Background(), updated); err != nil {
		t.Fatalf("failed to update status: %v", err)
	}

	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}
	if result.RequeueAfter <= 0 || result.RequeueAfter > 5*time.Second {
		t.Errorf("RequeueAfter = %v, want within (0, 5s] near expiry", result.RequeueAfter)
	}
}

func TestCaptureExpired(t *testing.T) {
	now := metav1.Now()
	past := metav1.NewTime(time.Now().Add(-10 * time.Minute))

	tests := []struct {
		name        string
		duration    *string
		startTime   *metav1.Time
		wantExpired bool
	}{
		{name: "no duration", duration: nil, startTime: &past, wantExpired: false},
		{name: "no start time", duration: ptrTo("1m"), startTime: nil, wantExpired: false},
		{name: "invalid duration", duration: ptrTo("not-a-duration"), startTime: &past, wantExpired: false},
		{name: "expired", duration: ptrTo("1m"), startTime: &past, wantExpired: true},
		{name: "not expired", duration: ptrTo("1h"), startTime: &now, wantExpired: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := newTestTrafficCapture("capture-1", "default")
			tc.Spec.Capture.Duration = tt.duration
			tc.Status.StartTime = tt.startTime

			expired, remaining := captureExpired(tc)
			if expired != tt.wantExpired {
				t.Errorf("captureExpired() = %v, want %v", expired, tt.wantExpired)
			}
			if tt.name == "not expired" && remaining <= 0 {
				t.Errorf("remaining = %v, want > 0", remaining)
			}
		})
	}
}
