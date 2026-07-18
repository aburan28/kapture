package replayengine

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	sdk "github.com/kapture-io/kapture/pkg/replayengine"
)

// CopyPlugins copies every kapture-engine-* binary (and any supporting
// tool binaries) from src to dst, preserving file modes. Files are written
// to a temp name and renamed into place so a watching Manager sees one
// atomic change per plugin instead of a partially written binary — this is
// what makes initContainer- or sidecar-delivered plugin updates safe to
// hot-reload.
func CopyPlugins(src, dst string) (int, error) {
	entries, err := os.ReadDir(src)
	if err != nil {
		return 0, fmt.Errorf("read plugin source %s: %w", src, err)
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return 0, fmt.Errorf("create plugin target %s: %w", dst, err)
	}

	copied := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if err := copyAtomic(filepath.Join(src, entry.Name()), filepath.Join(dst, entry.Name())); err != nil {
			return copied, err
		}
		copied++
	}
	return copied, nil
}

// IsPluginBinary reports whether a file name follows the engine plugin
// naming convention.
func IsPluginBinary(name string) bool {
	return strings.HasPrefix(name, sdk.PluginBinaryPrefix)
}

func copyAtomic(src, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	tmp, err := os.CreateTemp(filepath.Dir(dst), ".plugin-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if _, err := io.Copy(tmp, in); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("copy %s: %w", src, err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := os.Chmod(tmpPath, info.Mode().Perm()); err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, dst); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("install %s: %w", dst, err)
	}
	return nil
}
