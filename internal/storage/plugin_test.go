package storage

import "testing"

func TestAsPluginConfig(t *testing.T) {
	cfg, err := asPluginConfig(PluginConfig{Path: "/tmp/backend.so"})
	if err != nil {
		t.Fatalf("asPluginConfig() error = %v", err)
	}
	if cfg.Path != "/tmp/backend.so" {
		t.Fatalf("path = %q, want /tmp/backend.so", cfg.Path)
	}
}

func TestAsPluginConfigRejectsWrongType(t *testing.T) {
	if _, err := asPluginConfig("bad"); err == nil {
		t.Fatal("expected error for wrong type")
	}
}
