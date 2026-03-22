package spoke

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
)

// ---------------------------------------------------------------------------
// Reconcile: deletion cleanup
// ---------------------------------------------------------------------------

func TestReconcileDelete_RemovesMirrorFilterAndFinalizer(t *testing.T) {
	scheme := testScheme()
	tc := newTestTrafficCapture("capture-1", "default")
	controllerutil.AddFinalizer(tc, finalizerName)
	now := metav1.Now()
	tc.DeletionTimestamp = &now

	route := &gwapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-route",
			Namespace: "default",
			Annotations: map[string]string{
				mirrorFilterAnnotationKey: "default/capture-1",
			},
		},
		Spec: gwapiv1.HTTPRouteSpec{
			Rules: []gwapiv1.HTTPRouteRule{
				{
					Filters: []gwapiv1.HTTPRouteFilter{
						buildHTTPMirrorFilter("default", "capture-1-capture-agent"),
					},
				},
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(tc, route).
		WithStatusSubresource(tc).
		Build()

	r := &TrafficCaptureReconciler{Client: cl, Scheme: scheme}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "capture-1", Namespace: "default"}}
	_, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("reconcileDelete failed: %v", err)
	}

	// After finalizer removal with DeletionTimestamp set, the fake client
	// deletes the object. Verify it's gone (meaning finalizer was removed).
	updatedTC := &capturev1alpha1.TrafficCapture{}
	err = cl.Get(context.Background(), req.NamespacedName, updatedTC)
	if err == nil {
		// Object still exists — check finalizer was removed.
		if controllerutil.ContainsFinalizer(updatedTC, finalizerName) {
			t.Error("expected finalizer to be removed after deletion")
		}
	}
	// If err is NotFound, the object was deleted — that's correct.

	// Verify mirror filter was removed from route.
	updatedRoute := &gwapiv1.HTTPRoute{}
	if err := cl.Get(context.Background(), types.NamespacedName{Name: "test-route", Namespace: "default"}, updatedRoute); err != nil {
		t.Fatalf("failed to get route: %v", err)
	}
	for _, rule := range updatedRoute.Spec.Rules {
		for _, f := range rule.Filters {
			if f.Type == gwapiv1.HTTPRouteFilterRequestMirror &&
				f.RequestMirror != nil &&
				string(f.RequestMirror.BackendRef.Name) == "capture-1-capture-agent" {
				t.Error("expected mirror filter to be removed from route")
			}
		}
	}
}

func TestReconcileDelete_RouteNotFound_ContinuesCleanup(t *testing.T) {
	scheme := testScheme()
	tc := newTestTrafficCapture("capture-1", "default")
	controllerutil.AddFinalizer(tc, finalizerName)
	now := metav1.Now()
	tc.DeletionTimestamp = &now

	// No route in the cluster — removal should not block finalizer removal.
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(tc).
		WithStatusSubresource(tc).
		Build()

	r := &TrafficCaptureReconciler{Client: cl, Scheme: scheme}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "capture-1", Namespace: "default"}}
	_, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("reconcileDelete failed: %v", err)
	}

	// After finalizer removal with DeletionTimestamp set, the fake client
	// may delete the object. Either is acceptable.
	updatedTC := &capturev1alpha1.TrafficCapture{}
	err = cl.Get(context.Background(), req.NamespacedName, updatedTC)
	if err == nil {
		if controllerutil.ContainsFinalizer(updatedTC, finalizerName) {
			t.Error("expected finalizer to be removed even when route is missing")
		}
	}
	// NotFound means object was deleted — finalizer was removed successfully.
}

// ---------------------------------------------------------------------------
// Reconcile: storage not found → Failed phase
// ---------------------------------------------------------------------------

func TestReconcile_StorageNotFound_SetsFailed(t *testing.T) {
	scheme := testScheme()
	tc := newTestTrafficCapture("capture-1", "default")
	route := &gwapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "test-route", Namespace: "default"},
		Spec:       gwapiv1.HTTPRouteSpec{Rules: []gwapiv1.HTTPRouteRule{{}}},
	}

	// No CaptureStorage object.
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(tc, route).
		WithStatusSubresource(tc).
		Build()

	r := &TrafficCaptureReconciler{Client: cl, Scheme: scheme}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "capture-1", Namespace: "default"}}

	// First reconcile adds finalizer.
	_, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	// Second reconcile hits storage not found.
	_, err = r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	updatedTC := &capturev1alpha1.TrafficCapture{}
	if err := cl.Get(context.Background(), req.NamespacedName, updatedTC); err != nil {
		t.Fatalf("failed to get TC: %v", err)
	}
	if updatedTC.Status.Phase != capturev1alpha1.TrafficCapturePhaseFailed {
		t.Errorf("expected phase Failed, got %s", updatedTC.Status.Phase)
	}

	// Check condition.
	found := false
	for _, c := range updatedTC.Status.Conditions {
		if c.Type == "Ready" && c.Reason == "StorageNotFound" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected Ready condition with reason StorageNotFound")
	}
}

// ---------------------------------------------------------------------------
// Reconcile: storage not ready → Pending phase with requeue
// ---------------------------------------------------------------------------

func TestReconcile_StorageNotReady_SetsPending(t *testing.T) {
	scheme := testScheme()
	tc := newTestTrafficCapture("capture-1", "default")
	storage := newTestStorage()
	storage.Status.Phase = capturev1alpha1.CaptureStoragePhaseError // Not ready
	route := &gwapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "test-route", Namespace: "default"},
		Spec:       gwapiv1.HTTPRouteSpec{Rules: []gwapiv1.HTTPRouteRule{{}}},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(tc, storage, route).
		WithStatusSubresource(tc).
		Build()

	r := &TrafficCaptureReconciler{Client: cl, Scheme: scheme}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "capture-1", Namespace: "default"}}

	// First reconcile adds finalizer.
	_, _ = r.Reconcile(context.Background(), req)

	// Second reconcile hits storage not ready.
	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	if result.RequeueAfter != 10*time.Second {
		t.Errorf("expected 10s requeue, got %v", result.RequeueAfter)
	}

	updatedTC := &capturev1alpha1.TrafficCapture{}
	_ = cl.Get(context.Background(), req.NamespacedName, updatedTC)
	if updatedTC.Status.Phase != capturev1alpha1.TrafficCapturePhasePending {
		t.Errorf("expected phase Pending, got %s", updatedTC.Status.Phase)
	}
}

// ---------------------------------------------------------------------------
// Reconcile: duration expired → Completed phase
// ---------------------------------------------------------------------------

func TestReconcile_DurationExpired_SetsCompleted(t *testing.T) {
	scheme := testScheme()
	tc := newTestTrafficCapture("capture-1", "default")
	duration := "1s"
	tc.Spec.Capture.Duration = &duration
	startTime := metav1.NewTime(time.Now().Add(-5 * time.Second))
	tc.Status.StartTime = &startTime

	storage := newTestStorage()
	route := &gwapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "test-route", Namespace: "default"},
		Spec:       gwapiv1.HTTPRouteSpec{Rules: []gwapiv1.HTTPRouteRule{{}}},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(tc, storage, route).
		WithStatusSubresource(tc).
		Build()

	r := &TrafficCaptureReconciler{Client: cl, Scheme: scheme}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "capture-1", Namespace: "default"}}

	// First reconcile adds finalizer.
	_, _ = r.Reconcile(context.Background(), req)

	// Second reconcile detects expiration.
	_, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	updatedTC := &capturev1alpha1.TrafficCapture{}
	_ = cl.Get(context.Background(), req.NamespacedName, updatedTC)
	if updatedTC.Status.Phase != capturev1alpha1.TrafficCapturePhaseCompleted {
		t.Errorf("expected phase Completed, got %s", updatedTC.Status.Phase)
	}
}

func TestReconcile_DurationNotExpired_Continues(t *testing.T) {
	scheme := testScheme()
	tc := newTestTrafficCapture("capture-1", "default")
	duration := "1h"
	tc.Spec.Capture.Duration = &duration
	startTime := metav1.NewTime(time.Now())
	tc.Status.StartTime = &startTime

	storage := newTestStorage()
	route := &gwapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "test-route", Namespace: "default"},
		Spec: gwapiv1.HTTPRouteSpec{
			Rules: []gwapiv1.HTTPRouteRule{
				{BackendRefs: []gwapiv1.HTTPBackendRef{{BackendRef: gwapiv1.BackendRef{BackendObjectReference: gwapiv1.BackendObjectReference{Name: "svc", Port: ptrTo(gwapiv1.PortNumber(80))}}}}},
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(tc, storage, route).
		WithStatusSubresource(tc).
		Build()

	r := &TrafficCaptureReconciler{Client: cl, Scheme: scheme}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "capture-1", Namespace: "default"}}

	// First reconcile adds finalizer, second continues normally.
	_, _ = r.Reconcile(context.Background(), req)
	_, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	updatedTC := &capturev1alpha1.TrafficCapture{}
	_ = cl.Get(context.Background(), req.NamespacedName, updatedTC)
	// Should not be Completed.
	if updatedTC.Status.Phase == capturev1alpha1.TrafficCapturePhaseCompleted {
		t.Error("expected phase NOT to be Completed when duration hasn't expired")
	}
}

// ---------------------------------------------------------------------------
// Reconcile: GRPCRoute target
// ---------------------------------------------------------------------------

func TestReconcile_GRPCRoute_CreatesResourcesAndMirror(t *testing.T) {
	scheme := testScheme()
	tc := newTestTrafficCapture("capture-grpc", "default")
	tc.Spec.TargetRef = gwapiv1.LocalPolicyTargetReference{
		Group: "gateway.networking.k8s.io",
		Kind:  "GRPCRoute",
		Name:  "grpc-route",
	}
	storage := newTestStorage()
	route := &gwapiv1.GRPCRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "grpc-route", Namespace: "default"},
		Spec: gwapiv1.GRPCRouteSpec{
			Rules: []gwapiv1.GRPCRouteRule{{}},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(tc, storage, route).
		WithStatusSubresource(tc).
		Build()

	r := &TrafficCaptureReconciler{Client: cl, Scheme: scheme}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "capture-grpc", Namespace: "default"}}

	// First reconcile adds finalizer.
	_, _ = r.Reconcile(context.Background(), req)

	// Second reconcile creates resources and adds mirror.
	_, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	// Verify mirror filter on GRPCRoute.
	updatedRoute := &gwapiv1.GRPCRoute{}
	if err := cl.Get(context.Background(), types.NamespacedName{Name: "grpc-route", Namespace: "default"}, updatedRoute); err != nil {
		t.Fatalf("failed to get GRPCRoute: %v", err)
	}
	found := false
	for _, rule := range updatedRoute.Spec.Rules {
		for _, f := range rule.Filters {
			if f.Type == gwapiv1.GRPCRouteFilterRequestMirror {
				found = true
			}
		}
	}
	if !found {
		t.Error("expected RequestMirror filter on GRPCRoute")
	}
}

// ---------------------------------------------------------------------------
// Reconcile: unsupported target kind
// ---------------------------------------------------------------------------

func TestReconcile_UnsupportedTargetKind_ReturnsError(t *testing.T) {
	scheme := testScheme()
	tc := newTestTrafficCapture("capture-1", "default")
	tc.Spec.TargetRef.Kind = "TCPRoute" // Unsupported
	storage := newTestStorage()

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(tc, storage).
		WithStatusSubresource(tc).
		Build()

	r := &TrafficCaptureReconciler{Client: cl, Scheme: scheme}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "capture-1", Namespace: "default"}}

	// First reconcile adds finalizer.
	_, _ = r.Reconcile(context.Background(), req)
	// Second reconcile hits unsupported kind.
	_, err := r.Reconcile(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for unsupported target kind")
	}
}

// ---------------------------------------------------------------------------
// Helper function tests
// ---------------------------------------------------------------------------

func TestConditionStatus(t *testing.T) {
	if conditionStatus(true) != metav1.ConditionTrue {
		t.Error("expected ConditionTrue for true")
	}
	if conditionStatus(false) != metav1.ConditionFalse {
		t.Error("expected ConditionFalse for false")
	}
}

func TestSetCondition_AddsNew(t *testing.T) {
	var conditions []metav1.Condition
	setCondition(&conditions, metav1.Condition{
		Type:   "Ready",
		Status: metav1.ConditionTrue,
		Reason: "AllGood",
	})
	if len(conditions) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(conditions))
	}
	if conditions[0].Reason != "AllGood" {
		t.Errorf("expected reason AllGood, got %s", conditions[0].Reason)
	}
}

func TestSetCondition_UpdatesExisting(t *testing.T) {
	conditions := []metav1.Condition{
		{Type: "Ready", Status: metav1.ConditionFalse, Reason: "Old"},
	}
	setCondition(&conditions, metav1.Condition{
		Type:   "Ready",
		Status: metav1.ConditionTrue,
		Reason: "New",
	})
	if len(conditions) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(conditions))
	}
	if conditions[0].Reason != "New" {
		t.Errorf("expected reason New, got %s", conditions[0].Reason)
	}
}

func TestSetCondition_PreservesOtherTypes(t *testing.T) {
	conditions := []metav1.Condition{
		{Type: "Available", Status: metav1.ConditionTrue, Reason: "Keep"},
		{Type: "Ready", Status: metav1.ConditionFalse, Reason: "Old"},
	}
	setCondition(&conditions, metav1.Condition{
		Type:   "Ready",
		Status: metav1.ConditionTrue,
		Reason: "Updated",
	})
	if len(conditions) != 2 {
		t.Fatalf("expected 2 conditions, got %d", len(conditions))
	}
	if conditions[0].Reason != "Keep" {
		t.Error("Available condition should be preserved")
	}
	if conditions[1].Reason != "Updated" {
		t.Error("Ready condition should be updated")
	}
}

// ---------------------------------------------------------------------------
// reportToHub tests
// ---------------------------------------------------------------------------

func TestReportToHub_NilHubClient(t *testing.T) {
	scheme := testScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	r := &TrafficCaptureReconciler{
		Client:    cl,
		Scheme:    scheme,
		HubClient: nil,
	}

	tc := newTestTrafficCapture("capture-1", "default")
	// Should not panic.
	r.reportToHub(context.Background(), tc)
}
