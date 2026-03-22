package spoke

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
)

// ---------------------------------------------------------------------------
// AddMirrorFilter — HTTPRoute
// ---------------------------------------------------------------------------

func TestAddMirrorFilter_HTTPRoute_AddsMirrorToAllRules(t *testing.T) {
	scheme := testScheme()
	route := &gwapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "test-route", Namespace: "default"},
		Spec: gwapiv1.HTTPRouteSpec{
			Rules: []gwapiv1.HTTPRouteRule{
				{}, // Rule 0 — no filters
				{}, // Rule 1 — no filters
			},
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(route).Build()

	tc := newTestTrafficCapture("capture-1", "default")
	err := AddMirrorFilter(context.Background(), cl, tc, "capture-1-capture-agent")
	if err != nil {
		t.Fatalf("AddMirrorFilter failed: %v", err)
	}

	updatedRoute := &gwapiv1.HTTPRoute{}
	_ = cl.Get(context.Background(), types.NamespacedName{Name: "test-route", Namespace: "default"}, updatedRoute)

	for i, rule := range updatedRoute.Spec.Rules {
		found := false
		for _, f := range rule.Filters {
			if f.Type == gwapiv1.HTTPRouteFilterRequestMirror &&
				f.RequestMirror != nil &&
				string(f.RequestMirror.BackendRef.Name) == "capture-1-capture-agent" {
				found = true
			}
		}
		if !found {
			t.Errorf("rule %d: expected mirror filter", i)
		}
	}

	// Check annotation.
	if updatedRoute.Annotations[mirrorFilterAnnotationKey] != "default/capture-1" {
		t.Errorf("expected annotation, got %q", updatedRoute.Annotations[mirrorFilterAnnotationKey])
	}
}

func TestAddMirrorFilter_HTTPRoute_Idempotent(t *testing.T) {
	scheme := testScheme()
	route := &gwapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "test-route", Namespace: "default"},
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

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(route).Build()

	tc := newTestTrafficCapture("capture-1", "default")
	err := AddMirrorFilter(context.Background(), cl, tc, "capture-1-capture-agent")
	if err != nil {
		t.Fatalf("AddMirrorFilter failed: %v", err)
	}

	updatedRoute := &gwapiv1.HTTPRoute{}
	_ = cl.Get(context.Background(), types.NamespacedName{Name: "test-route", Namespace: "default"}, updatedRoute)

	// Should still have exactly 1 mirror filter, not 2.
	mirrorCount := 0
	for _, f := range updatedRoute.Spec.Rules[0].Filters {
		if f.Type == gwapiv1.HTTPRouteFilterRequestMirror {
			mirrorCount++
		}
	}
	if mirrorCount != 1 {
		t.Errorf("expected 1 mirror filter (idempotent), got %d", mirrorCount)
	}
}

func TestAddMirrorFilter_HTTPRoute_RouteNotFound(t *testing.T) {
	scheme := testScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	tc := newTestTrafficCapture("capture-1", "default")
	err := AddMirrorFilter(context.Background(), cl, tc, "capture-1-capture-agent")
	if err == nil {
		t.Fatal("expected error when route not found")
	}
}

// ---------------------------------------------------------------------------
// AddMirrorFilter — GRPCRoute
// ---------------------------------------------------------------------------

func TestAddMirrorFilter_GRPCRoute_AddsMirror(t *testing.T) {
	scheme := testScheme()
	route := &gwapiv1.GRPCRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "grpc-route", Namespace: "default"},
		Spec: gwapiv1.GRPCRouteSpec{
			Rules: []gwapiv1.GRPCRouteRule{{}},
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(route).Build()

	tc := newTestTrafficCapture("capture-1", "default")
	tc.Spec.TargetRef.Kind = "GRPCRoute"
	tc.Spec.TargetRef.Name = "grpc-route"

	err := AddMirrorFilter(context.Background(), cl, tc, "capture-1-capture-agent")
	if err != nil {
		t.Fatalf("AddMirrorFilter failed: %v", err)
	}

	updatedRoute := &gwapiv1.GRPCRoute{}
	_ = cl.Get(context.Background(), types.NamespacedName{Name: "grpc-route", Namespace: "default"}, updatedRoute)

	found := false
	for _, f := range updatedRoute.Spec.Rules[0].Filters {
		if f.Type == gwapiv1.GRPCRouteFilterRequestMirror {
			found = true
		}
	}
	if !found {
		t.Error("expected mirror filter on GRPCRoute")
	}
}

func TestAddMirrorFilter_GRPCRoute_Idempotent(t *testing.T) {
	scheme := testScheme()
	route := &gwapiv1.GRPCRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "grpc-route", Namespace: "default"},
		Spec: gwapiv1.GRPCRouteSpec{
			Rules: []gwapiv1.GRPCRouteRule{
				{
					Filters: []gwapiv1.GRPCRouteFilter{
						buildGRPCMirrorFilter("default", "capture-1-capture-agent"),
					},
				},
			},
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(route).Build()

	tc := newTestTrafficCapture("capture-1", "default")
	tc.Spec.TargetRef.Kind = "GRPCRoute"
	tc.Spec.TargetRef.Name = "grpc-route"

	err := AddMirrorFilter(context.Background(), cl, tc, "capture-1-capture-agent")
	if err != nil {
		t.Fatalf("AddMirrorFilter failed: %v", err)
	}

	updatedRoute := &gwapiv1.GRPCRoute{}
	_ = cl.Get(context.Background(), types.NamespacedName{Name: "grpc-route", Namespace: "default"}, updatedRoute)

	mirrorCount := 0
	for _, f := range updatedRoute.Spec.Rules[0].Filters {
		if f.Type == gwapiv1.GRPCRouteFilterRequestMirror {
			mirrorCount++
		}
	}
	if mirrorCount != 1 {
		t.Errorf("expected 1 mirror filter (idempotent), got %d", mirrorCount)
	}
}

// ---------------------------------------------------------------------------
// AddMirrorFilter — unsupported kind
// ---------------------------------------------------------------------------

func TestAddMirrorFilter_UnsupportedKind(t *testing.T) {
	scheme := testScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	tc := &capturev1alpha1.TrafficCapture{
		ObjectMeta: metav1.ObjectMeta{Name: "capture-1", Namespace: "default"},
		Spec: capturev1alpha1.TrafficCaptureSpec{
			TargetRef: gwapiv1.LocalPolicyTargetReference{
				Kind: "TCPRoute",
				Name: "tcp-route",
			},
		},
	}

	err := AddMirrorFilter(context.Background(), cl, tc, "agent")
	if err == nil {
		t.Fatal("expected error for unsupported kind")
	}
}

// ---------------------------------------------------------------------------
// RemoveMirrorFilter — HTTPRoute
// ---------------------------------------------------------------------------

func TestRemoveMirrorFilter_HTTPRoute_RemovesMirror(t *testing.T) {
	scheme := testScheme()
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
						{Type: gwapiv1.HTTPRouteFilterRequestHeaderModifier},
						buildHTTPMirrorFilter("default", "capture-1-capture-agent"),
					},
				},
			},
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(route).Build()

	tc := newTestTrafficCapture("capture-1", "default")
	err := RemoveMirrorFilter(context.Background(), cl, tc)
	if err != nil {
		t.Fatalf("RemoveMirrorFilter failed: %v", err)
	}

	updatedRoute := &gwapiv1.HTTPRoute{}
	_ = cl.Get(context.Background(), types.NamespacedName{Name: "test-route", Namespace: "default"}, updatedRoute)

	// Mirror filter should be removed.
	for _, f := range updatedRoute.Spec.Rules[0].Filters {
		if f.Type == gwapiv1.HTTPRouteFilterRequestMirror &&
			f.RequestMirror != nil &&
			string(f.RequestMirror.BackendRef.Name) == "capture-1-capture-agent" {
			t.Error("expected mirror filter to be removed")
		}
	}

	// Non-mirror filter should remain.
	if len(updatedRoute.Spec.Rules[0].Filters) != 1 {
		t.Errorf("expected 1 remaining filter, got %d", len(updatedRoute.Spec.Rules[0].Filters))
	}

	// Annotation should be removed.
	if _, ok := updatedRoute.Annotations[mirrorFilterAnnotationKey]; ok {
		t.Error("expected annotation to be removed")
	}
}

func TestRemoveMirrorFilter_HTTPRoute_RouteNotFound_NoError(t *testing.T) {
	scheme := testScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	tc := newTestTrafficCapture("capture-1", "default")
	err := RemoveMirrorFilter(context.Background(), cl, tc)
	if err != nil {
		t.Fatalf("expected no error when route not found, got: %v", err)
	}
}

func TestRemoveMirrorFilter_HTTPRoute_KeepsOtherMirrors(t *testing.T) {
	scheme := testScheme()
	route := &gwapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "test-route", Namespace: "default"},
		Spec: gwapiv1.HTTPRouteSpec{
			Rules: []gwapiv1.HTTPRouteRule{
				{
					Filters: []gwapiv1.HTTPRouteFilter{
						buildHTTPMirrorFilter("default", "capture-1-capture-agent"),
						buildHTTPMirrorFilter("default", "other-agent"),
					},
				},
			},
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(route).Build()

	tc := newTestTrafficCapture("capture-1", "default")
	err := RemoveMirrorFilter(context.Background(), cl, tc)
	if err != nil {
		t.Fatalf("RemoveMirrorFilter failed: %v", err)
	}

	updatedRoute := &gwapiv1.HTTPRoute{}
	_ = cl.Get(context.Background(), types.NamespacedName{Name: "test-route", Namespace: "default"}, updatedRoute)

	if len(updatedRoute.Spec.Rules[0].Filters) != 1 {
		t.Fatalf("expected 1 filter, got %d", len(updatedRoute.Spec.Rules[0].Filters))
	}
	if string(updatedRoute.Spec.Rules[0].Filters[0].RequestMirror.BackendRef.Name) != "other-agent" {
		t.Error("expected other-agent mirror to remain")
	}
}

// ---------------------------------------------------------------------------
// RemoveMirrorFilter — GRPCRoute
// ---------------------------------------------------------------------------

func TestRemoveMirrorFilter_GRPCRoute_RemovesMirror(t *testing.T) {
	scheme := testScheme()
	route := &gwapiv1.GRPCRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "grpc-route",
			Namespace: "default",
			Annotations: map[string]string{
				mirrorFilterAnnotationKey: "default/capture-1",
			},
		},
		Spec: gwapiv1.GRPCRouteSpec{
			Rules: []gwapiv1.GRPCRouteRule{
				{
					Filters: []gwapiv1.GRPCRouteFilter{
						buildGRPCMirrorFilter("default", "capture-1-capture-agent"),
					},
				},
			},
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(route).Build()

	tc := newTestTrafficCapture("capture-1", "default")
	tc.Spec.TargetRef.Kind = "GRPCRoute"
	tc.Spec.TargetRef.Name = "grpc-route"

	err := RemoveMirrorFilter(context.Background(), cl, tc)
	if err != nil {
		t.Fatalf("RemoveMirrorFilter failed: %v", err)
	}

	updatedRoute := &gwapiv1.GRPCRoute{}
	_ = cl.Get(context.Background(), types.NamespacedName{Name: "grpc-route", Namespace: "default"}, updatedRoute)

	if len(updatedRoute.Spec.Rules[0].Filters) != 0 {
		t.Errorf("expected 0 filters after removal, got %d", len(updatedRoute.Spec.Rules[0].Filters))
	}
}

func TestRemoveMirrorFilter_GRPCRoute_RouteNotFound_NoError(t *testing.T) {
	scheme := testScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	tc := newTestTrafficCapture("capture-1", "default")
	tc.Spec.TargetRef.Kind = "GRPCRoute"
	tc.Spec.TargetRef.Name = "missing-route"

	err := RemoveMirrorFilter(context.Background(), cl, tc)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// RemoveMirrorFilter — unsupported kind
// ---------------------------------------------------------------------------

func TestRemoveMirrorFilter_UnsupportedKind(t *testing.T) {
	scheme := testScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	tc := &capturev1alpha1.TrafficCapture{
		ObjectMeta: metav1.ObjectMeta{Name: "capture-1", Namespace: "default"},
		Spec: capturev1alpha1.TrafficCaptureSpec{
			TargetRef: gwapiv1.LocalPolicyTargetReference{Kind: "TCPRoute", Name: "tcp"},
		},
	}

	err := RemoveMirrorFilter(context.Background(), cl, tc)
	if err == nil {
		t.Fatal("expected error for unsupported kind")
	}
}

// ---------------------------------------------------------------------------
// setAnnotation
// ---------------------------------------------------------------------------

func TestSetAnnotation_NilAnnotations(t *testing.T) {
	route := &gwapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "test"},
	}
	setAnnotation(route, "default/capture-1")
	if route.Annotations[mirrorFilterAnnotationKey] != "default/capture-1" {
		t.Error("expected annotation to be set")
	}
}

func TestSetAnnotation_ExistingAnnotations(t *testing.T) {
	route := &gwapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test",
			Annotations: map[string]string{"other": "keep"},
		},
	}
	setAnnotation(route, "default/capture-1")
	if route.Annotations["other"] != "keep" {
		t.Error("expected existing annotations to be preserved")
	}
	if route.Annotations[mirrorFilterAnnotationKey] != "default/capture-1" {
		t.Error("expected annotation to be set")
	}
}

// ---------------------------------------------------------------------------
// mirrorFilterName
// ---------------------------------------------------------------------------

func TestMirrorFilterName(t *testing.T) {
	tc := newTestTrafficCapture("my-capture", "my-ns")
	name := mirrorFilterName(tc)
	if name != "my-ns/my-capture" {
		t.Errorf("expected my-ns/my-capture, got %s", name)
	}
}

// ---------------------------------------------------------------------------
// removeGRPCMirrorFilters: keeps other mirrors
// ---------------------------------------------------------------------------

func TestRemoveGRPCMirrorFilters_KeepsOtherMirrors(t *testing.T) {
	filters := []gwapiv1.GRPCRouteFilter{
		buildGRPCMirrorFilter("default", "agent-1"),
		buildGRPCMirrorFilter("default", "agent-2"),
	}

	result := removeGRPCMirrorFilters(filters, "agent-1")
	if len(result) != 1 {
		t.Fatalf("expected 1 filter, got %d", len(result))
	}
	if string(result[0].RequestMirror.BackendRef.Name) != "agent-2" {
		t.Errorf("expected agent-2 to remain")
	}
}
