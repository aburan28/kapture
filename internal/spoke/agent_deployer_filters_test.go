package spoke

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
)

func findEnvVar(envs []corev1.EnvVar, name string) (string, bool) {
	for _, e := range envs {
		if e.Name == name {
			return e.Value, true
		}
	}
	return "", false
}

func TestBuildEnvVars_FilterEnvVars(t *testing.T) {
	tc := newTestTrafficCapture("capture-1", "default")
	tc.Spec.Filters = &capturev1alpha1.CaptureFilters{
		PathPrefix: ptrTo("/api/"),
		Headers: []capturev1alpha1.HeaderMatch{
			{Name: "x-debug", Value: "true"},
		},
		Percentage: ptrTo(int32(25)),
	}
	storage := newTestStorage()

	envs := buildEnvVars(tc, storage)

	if v, ok := findEnvVar(envs, "FILTER_PATH_PREFIX"); !ok || v != "/api/" {
		t.Errorf("FILTER_PATH_PREFIX = %q (present=%v), want %q", v, ok, "/api/")
	}
	if v, ok := findEnvVar(envs, "FILTER_HEADERS"); !ok || v != `[{"name":"x-debug","value":"true"}]` {
		t.Errorf("FILTER_HEADERS = %q (present=%v), want %q", v, ok, `[{"name":"x-debug","value":"true"}]`)
	}
	if v, ok := findEnvVar(envs, "FILTER_PERCENTAGE"); !ok || v != "25" {
		t.Errorf("FILTER_PERCENTAGE = %q (present=%v), want %q", v, ok, "25")
	}
}

func TestBuildEnvVars_NoFilters(t *testing.T) {
	tc := newTestTrafficCapture("capture-1", "default")
	storage := newTestStorage()

	envs := buildEnvVars(tc, storage)

	for _, name := range []string{"FILTER_PATH_PREFIX", "FILTER_HEADERS", "FILTER_PERCENTAGE"} {
		if _, ok := findEnvVar(envs, name); ok {
			t.Errorf("expected %s to be absent when no filters are configured", name)
		}
	}
}

func TestBuildEnvVars_EmptyPathPrefixOmitted(t *testing.T) {
	tc := newTestTrafficCapture("capture-1", "default")
	tc.Spec.Filters = &capturev1alpha1.CaptureFilters{
		PathPrefix: ptrTo(""),
		Percentage: ptrTo(int32(100)),
	}
	storage := newTestStorage()

	envs := buildEnvVars(tc, storage)

	if _, ok := findEnvVar(envs, "FILTER_PATH_PREFIX"); ok {
		t.Error("expected FILTER_PATH_PREFIX to be omitted for empty prefix")
	}
	if v, ok := findEnvVar(envs, "FILTER_PERCENTAGE"); !ok || v != "100" {
		t.Errorf("FILTER_PERCENTAGE = %q (present=%v), want %q", v, ok, "100")
	}
}
