package agents

import (
	"errors"
	"testing"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
	"github.com/kapture-io/kapture/internal/storage"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestValidateCaptureStorageSpec_Plugin(t *testing.T) {
	orig := validatePluginConfig
	t.Cleanup(func() { validatePluginConfig = orig })

	called := false
	validatePluginConfig = func(cfg storage.PluginConfig) error {
		called = true
		if cfg.Path != "/plugins/backend.so" {
			t.Fatalf("unexpected plugin path: %q", cfg.Path)
		}
		if cfg.Symbol != "BuildFactory" {
			t.Fatalf("unexpected symbol: %q", cfg.Symbol)
		}
		if got, ok := cfg.Config["bucket"]; !ok || got != "captures" {
			t.Fatalf("unexpected plugin config: %#v", cfg.Config)
		}
		return nil
	}

	raw := &runtime.RawExtension{Raw: []byte(`{"bucket":"captures"}`)}
	symbol := "BuildFactory"
	err := validateCaptureStorageSpec(capturev1alpha1.CaptureStorageSpec{
		Type: capturev1alpha1.CaptureStorageTypePlugin,
		Plugin: &capturev1alpha1.PluginConfig{
			Path:   "/plugins/backend.so",
			Symbol: &symbol,
			Config: raw,
		},
	})
	if err != nil {
		t.Fatalf("validateCaptureStorageSpec() error = %v", err)
	}
	if !called {
		t.Fatal("expected plugin validator to be called")
	}
}

func TestValidateCaptureStorageSpec_PluginError(t *testing.T) {
	orig := validatePluginConfig
	t.Cleanup(func() { validatePluginConfig = orig })

	validatePluginConfig = func(cfg storage.PluginConfig) error {
		return errors.New("boom")
	}

	err := validateCaptureStorageSpec(capturev1alpha1.CaptureStorageSpec{
		Type: capturev1alpha1.CaptureStorageTypePlugin,
		Plugin: &capturev1alpha1.PluginConfig{
			Path: "/plugins/backend.so",
		},
	})
	if err == nil {
		t.Fatal("expected error")
	}
}
