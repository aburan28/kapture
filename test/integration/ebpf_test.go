package integration

import (
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
)

func TestEBPFAdmissionAndCollectorLifecycle(t *testing.T) {
	t.Setenv("EBPF_ENABLED", "true")
	ns := createTestNamespace(t)
	storage := newCaptureStorage(ns, "ebpf-storage")
	if err := k8sClient.Create(ctx, storage); err != nil {
		t.Fatal(err)
	}
	setStorageReady(t, storage)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "openssl-workload", Namespace: ns}, Spec: corev1.PodSpec{NodeName: "worker-1", Containers: []corev1.Container{{Name: "app", Image: "example.invalid/fixture"}}}}
	if err := k8sClient.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	tc := newTrafficCapture(ns, "ebpf-session", pod.Name, storage.Name)
	tc.Spec.Backend = capturev1alpha1.CaptureBackendEBPF
	tc.Spec.TargetRef = gwapiv1.LocalPolicyTargetReference{Group: "", Kind: "Pod", Name: gwapiv1.ObjectName(pod.Name)}
	tc.Spec.EBPF = &capturev1alpha1.EBPFCaptureSettings{}
	if err := k8sClient.Create(ctx, tc); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(tc), tc); err != nil {
		t.Fatal(err)
	}
	if tc.Spec.EBPF.MapPages != 256 || tc.Spec.EBPF.PCAP == nil || !*tc.Spec.EBPF.PCAP {
		t.Fatalf("EBPF defaults missing: %+v", tc.Spec.EBPF)
	}
	key := types.NamespacedName{Namespace: ns, Name: "ebpf-session-capture-agent"}
	waitForObject(t, &appsv1.Deployment{}, key, 15*time.Second)
	deploy := &appsv1.Deployment{}
	if err := k8sClient.Get(ctx, key, deploy); err != nil {
		t.Fatal(err)
	}
	if deploy.Spec.Template.Spec.NodeName != "worker-1" || !deploy.Spec.Template.Spec.HostPID {
		t.Fatal("collector target scope not reconciled")
	}
	if err := k8sClient.Get(ctx, key, &corev1.Service{}); !apierrors.IsNotFound(err) {
		t.Fatalf("mirror service unexpectedly created: %v", err)
	}
	bad := tc.DeepCopy()
	bad.ResourceVersion = ""
	bad.UID = ""
	bad.Name = "bad-ebpf-target"
	bad.Spec.TargetRef.Kind = "HTTPRoute"
	bad.Spec.TargetRef.Group = "gateway.networking.k8s.io"
	if err := k8sClient.Create(ctx, bad); err == nil {
		t.Fatal("invalid EBPF target admitted")
	}
	updated := tc.DeepCopy()
	updated.Spec.Backend = capturev1alpha1.CaptureBackendMirror
	updated.Spec.EBPF = nil
	if err := k8sClient.Update(ctx, updated); err == nil {
		t.Fatal("backend mutation admitted")
	}
	if err := k8sClient.Delete(ctx, tc); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, 10*time.Second, func() bool {
		return apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(tc), &capturev1alpha1.TrafficCapture{}))
	})
}
