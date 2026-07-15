package replayengine

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCopyPlugins_CopiesWithModes(t *testing.T) {
	src := t.TempDir()
	dst := filepath.Join(t.TempDir(), "plugins")

	if err := os.WriteFile(filepath.Join(src, "kapture-engine-k6"), []byte("binary-1"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "kapture-engine-ghz"), []byte("binary-2"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(src, "subdir"), 0o755); err != nil {
		t.Fatal(err)
	}

	n, err := CopyPlugins(src, dst)
	if err != nil {
		t.Fatalf("CopyPlugins: %v", err)
	}
	if n != 2 {
		t.Errorf("copied %d files, want 2 (directories skipped)", n)
	}

	for _, name := range []string{"kapture-engine-k6", "kapture-engine-ghz"} {
		info, err := os.Stat(filepath.Join(dst, name))
		if err != nil {
			t.Fatalf("%s not installed: %v", name, err)
		}
		if info.Mode().Perm() != 0o755 {
			t.Errorf("%s mode = %v, want 0755 (exec bit preserved)", name, info.Mode().Perm())
		}
	}

	// Re-running (a plugin update) overwrites in place.
	if err := os.WriteFile(filepath.Join(src, "kapture-engine-k6"), []byte("binary-1-v2"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := CopyPlugins(src, dst); err != nil {
		t.Fatalf("CopyPlugins (update): %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dst, "kapture-engine-k6"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "binary-1-v2" {
		t.Errorf("plugin not updated: %q", data)
	}

	// No stray temp files left behind.
	entries, err := os.ReadDir(dst)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !IsPluginBinary(entry.Name()) {
			t.Errorf("unexpected file in plugin dir: %s", entry.Name())
		}
	}
}

func TestCopyPlugins_MissingSource(t *testing.T) {
	if _, err := CopyPlugins(filepath.Join(t.TempDir(), "nope"), t.TempDir()); err == nil {
		t.Error("missing source dir accepted")
	}
}
