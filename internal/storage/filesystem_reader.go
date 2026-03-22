package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// FilesystemObjectStore implements ObjectStore for local filesystem
// storage (EFS and EBS backends).
type FilesystemObjectStore struct {
	mountPath string
}

// FilesystemReaderConfig configures a filesystem-backed ReaderFactory.
type FilesystemReaderConfig struct {
	MountPath string
}

// NewFilesystemReaderFactory creates a ReaderFactory that reads captured data
// from a local filesystem mount (suitable for EFS and EBS backends).
func NewFilesystemReaderFactory(cfg FilesystemReaderConfig) (ReaderFactory, error) {
	if cfg.MountPath == "" {
		return nil, fmt.Errorf("mount path is required")
	}

	store := &FilesystemObjectStore{mountPath: cfg.MountPath}
	return NewObjectStoreReaderFactory(store, ""), nil
}

func (f *FilesystemObjectStore) ListObjects(_ context.Context, prefix string) ([]string, error) {
	searchPath := filepath.Join(f.mountPath, prefix)

	var keys []string
	err := filepath.Walk(searchPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, ".jsonl.gz") {
			keys = append(keys, path)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk directory %q: %w", searchPath, err)
	}

	sort.Strings(keys)
	return keys, nil
}

func (f *FilesystemObjectStore) Download(_ context.Context, key string) (io.ReadCloser, error) {
	file, err := os.Open(key)
	if err != nil {
		return nil, fmt.Errorf("open file %q: %w", key, err)
	}
	return file, nil
}
