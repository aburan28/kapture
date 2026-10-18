package spoke

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
)

func TestHTTPBackendCaptureAndReplayUseSameDestinationAndSecret(t *testing.T) {
	storage := &capturev1alpha1.CaptureStorage{ObjectMeta: metav1.ObjectMeta{Name: "backend", Namespace: "default"}, Spec: capturev1alpha1.CaptureStorageSpec{Type: capturev1alpha1.CaptureStorageTypeHTTP, HTTP: &capturev1alpha1.HTTPBackendConfig{Endpoint: "https://backend.example/v1-prefix", TokenSecretRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "backend-auth"}, Key: "token"}}}}
	if err := validateCaptureStorageSpec(storage.Spec); err != nil {
		t.Fatal(err)
	}
	tc := newTestTrafficCapture("capture", "default")
	deploy := BuildDeployment(tc, storage)
	job, err := BuildReplayJob(makeShardReplay(), storage)
	if err != nil {
		t.Fatal(err)
	}
	for name, env := range map[string][]corev1.EnvVar{"capture": deploy.Spec.Template.Spec.Containers[0].Env, "replay": job.Spec.Template.Spec.Containers[0].Env} {
		var endpoint, credential bool
		for _, v := range env {
			if v.Name == "BACKEND_URL" {
				endpoint = v.Value == storage.Spec.HTTP.Endpoint
			}
			if v.Name == "BACKEND_TOKEN" {
				credential = v.Value == "" && v.ValueFrom != nil && v.ValueFrom.SecretKeyRef.Name == "backend-auth" && v.ValueFrom.SecretKeyRef.Key == "token"
			}
		}
		if !endpoint || !credential {
			t.Errorf("%s lost backend configuration or exposed a credential", name)
		}
	}
	args := strings.Join(job.Spec.Template.Spec.Containers[0].Args, " ")
	if !strings.Contains(args, "--storage-type http --backend-url https://backend.example/v1-prefix") || len(job.Spec.Template.Spec.Volumes) != 0 {
		t.Fatalf("replay coupled to filesystem layout: %v", job.Spec.Template.Spec)
	}
	bad := storage.DeepCopy()
	bad.Spec.HTTP.Endpoint = "https://name:password@backend.example"
	if err := validateCaptureStorageSpec(bad.Spec); err == nil || strings.Contains(err.Error(), "password") {
		t.Fatal("credential-bearing URL accepted or exposed")
	}
}
