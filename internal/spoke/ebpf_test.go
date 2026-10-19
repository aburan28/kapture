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

func TestEBPFReconcilePinsPodScopeWithoutMirrorResources(t *testing.T) {
	t.Setenv("EBPF_ENABLED", "true")
	t.Setenv("EBPF_AGENT_IMAGE", "example/ebpf:tested")
	scheme := testScheme()
	tc := newTestTrafficCapture("ebpf-capture", "default")
	tc.Spec.Backend = capturev1alpha1.CaptureBackendEBPF
	tc.Spec.TargetRef = gwapiv1.LocalPolicyTargetReference{Group: "", Kind: "Pod", Name: "workload"}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "workload", Namespace: "default", UID: types.UID("12345678-1234-1234-1234-123456789012")}, Spec: corev1.PodSpec{NodeName: "node-1"}}
	storage := newTestStorage()
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc, storage, pod).WithStatusSubresource(tc).Build()
	r := &TrafficCaptureReconciler{Client: cl, Scheme: scheme}
	key := client.ObjectKeyFromObject(tc)
	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatal(err)
		}
	}
	d := &appsv1.Deployment{}
	resourceKey := types.NamespacedName{Namespace: tc.Namespace, Name: AgentDeploymentName(tc)}
	if err := cl.Get(context.Background(), resourceKey, d); err != nil {
		t.Fatal(err)
	}
	spec := d.Spec.Template.Spec
	if spec.NodeName != "node-1" || !spec.HostPID || *d.Spec.Replicas != 1 || d.Spec.Strategy.Type != appsv1.RecreateDeploymentStrategyType {
		t.Fatalf("wrong node/lifetime scope: %+v", spec)
	}
	c := spec.Containers[0]
	if c.Image != "example/ebpf:tested" || c.SecurityContext.Privileged == nil || !*c.SecurityContext.Privileged || c.ReadinessProbe == nil {
		t.Fatal("collector capability/readiness wiring missing")
	}
	foundUID := false
	for _, env := range c.Env {
		if env.Name == "TARGET_POD_UID" && env.Value == string(pod.UID) {
			foundUID = true
		}
	}
	if !foundUID {
		t.Fatal("workload UID missing")
	}
	for _, obj := range []client.Object{&corev1.Service{}, &autoscalingv2.HorizontalPodAutoscaler{}} {
		if err := cl.Get(context.Background(), resourceKey, obj); !apierrors.IsNotFound(err) {
			t.Fatalf("unexpected mirror/scaling resource %T: %v", obj, err)
		}
	}
	if len(d.OwnerReferences) != 1 || d.OwnerReferences[0].Name != tc.Name {
		t.Fatal("collector not owned by capture")
	}
	updated := &capturev1alpha1.TrafficCapture{}
	if err := cl.Get(context.Background(), key, updated); err != nil {
		t.Fatal(err)
	}
	start := metav1.NewTime(time.Now().Add(-time.Minute))
	duration := "1s"
	updated.Spec.Capture.Duration = &duration
	if err := cl.Update(context.Background(), updated); err != nil {
		t.Fatal(err)
	}
	updated.Status.StartTime = &start
	if err := cl.Status().Update(context.Background(), updated); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	if err := cl.Get(context.Background(), resourceKey, &appsv1.Deployment{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expired collector remains: %v", err)
	}
}

func TestEBPFRequiresOperatorEnablement(t *testing.T) {
	t.Setenv("EBPF_ENABLED", "")
	scheme := testScheme()
	tc := newTestTrafficCapture("disabled-ebpf", "default")
	tc.Spec.Backend = capturev1alpha1.CaptureBackendEBPF
	tc.Spec.TargetRef = gwapiv1.LocalPolicyTargetReference{Group: "", Kind: "Pod", Name: "workload"}
	storage := newTestStorage()
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc, storage).WithStatusSubresource(tc).Build()
	r := &TrafficCaptureReconciler{Client: cl, Scheme: scheme}
	key := client.ObjectKeyFromObject(tc)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	updated := &capturev1alpha1.TrafficCapture{}
	cl.Get(context.Background(), key, updated)
	if updated.Status.Phase != capturev1alpha1.TrafficCapturePhaseFailed || updated.Status.Conditions[0].Reason != "EBPFDisabled" {
		t.Fatalf("wrong enablement status: %+v", updated.Status)
	}
}
