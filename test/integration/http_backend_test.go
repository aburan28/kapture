package integration

import (
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
	"github.com/kapture-io/kapture/internal/spoke"
)

func TestHTTPBackendCRDAndCaptureReplayWiring(t *testing.T) {
	ns := createTestNamespace(t)
	storage := &capturev1alpha1.CaptureStorage{ObjectMeta: metav1.ObjectMeta{Name: "external-backend", Namespace: ns}, Spec: capturev1alpha1.CaptureStorageSpec{Type: capturev1alpha1.CaptureStorageTypeHTTP, HTTP: &capturev1alpha1.HTTPBackendConfig{Endpoint: "https://destination.example.test", TokenSecretRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "destination-token"}, Key: "token"}}}}
	if err := k8sClient.Create(ctx, storage); err != nil {
		t.Fatal(err)
	}
	setStorageReady(t, storage)
	route := newHTTPRoute(ns, "backend-route")
	if err := k8sClient.Create(ctx, route); err != nil {
		t.Fatal(err)
	}
	tc := newTrafficCapture(ns, "backend-capture", route.Name, storage.Name)
	if err := k8sClient.Create(ctx, tc); err != nil {
		t.Fatal(err)
	}
	key := types.NamespacedName{Namespace: ns, Name: spoke.AgentDeploymentName(tc)}
	deploy := &appsv1.Deployment{}
	waitForObject(t, deploy, key, 15*time.Second)
	if err := k8sClient.Get(ctx, key, deploy); err != nil {
		t.Fatal(err)
	}
	tr := newTrafficReplay(ns, "backend-replay", storage.Name)
	if err := k8sClient.Create(ctx, tr); err != nil {
		t.Fatal(err)
	}
	job := &batchv1.Job{}
	jobKey := types.NamespacedName{Namespace: ns, Name: spoke.ReplayJobName(tr)}
	waitForObject(t, job, jobKey, 15*time.Second)
	if err := k8sClient.Get(ctx, jobKey, job); err != nil {
		t.Fatal(err)
	}
	for name, env := range map[string][]corev1.EnvVar{"capture": deploy.Spec.Template.Spec.Containers[0].Env, "replay": job.Spec.Template.Spec.Containers[0].Env} {
		var endpoint, secret bool
		for _, v := range env {
			if v.Name == "BACKEND_URL" {
				endpoint = v.Value == storage.Spec.HTTP.Endpoint
			}
			if v.Name == "BACKEND_TOKEN" {
				secret = v.Value == "" && v.ValueFrom != nil && v.ValueFrom.SecretKeyRef.Name == "destination-token" && v.ValueFrom.SecretKeyRef.Key == "token"
			}
		}
		if !endpoint || !secret {
			t.Errorf("%s lost endpoint or Secret reference", name)
		}
	}
	missing := &capturev1alpha1.CaptureStorage{ObjectMeta: metav1.ObjectMeta{Name: "missing-http", Namespace: ns}, Spec: capturev1alpha1.CaptureStorageSpec{Type: capturev1alpha1.CaptureStorageTypeHTTP}}
	if err := k8sClient.Create(ctx, missing); !apierrors.IsInvalid(err) {
		t.Fatalf("missing backend settings accepted by CRD: %v", err)
	}
	if err := k8sClient.Delete(ctx, tc); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, 15*time.Second, func() bool {
		return apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(tc), &capturev1alpha1.TrafficCapture{}))
	})
}
