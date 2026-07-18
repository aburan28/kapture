package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type EFSConfig struct {
	MountPath      string
	AgentPod       string
	FlushInterval  time.Duration
	MaxBufferBytes int
}

func (EFSConfig) isStorageConfig() {}

type filesystemWriterFactory struct {
	mountPath      string
	agentPod       string
	flushInterval  time.Duration
	maxBufferBytes int
}

type EFSWriterFactory struct{ filesystem *filesystemWriterFactory }

type EFSWriter struct{ *bufferedObjectWriter }

func newFilesystemWriterFactory(mountPath, agentPod string, flushInterval time.Duration, maxBufferBytes int) (*filesystemWriterFactory, error) {
	if mountPath == "" {
		return nil, fmt.Errorf("mount path is required")
	}
	return &filesystemWriterFactory{
		mountPath:      mountPath,
		agentPod:       agentPod,
		flushInterval:  flushInterval,
		maxBufferBytes: maxBufferBytes,
	}, nil
}

func (f *filesystemWriterFactory) putManifest(captureID string, data []byte) error {
	if captureID == "" {
		return fmt.Errorf("capture ID is required")
	}
	path := filepath.Join(f.mountPath, captureID, ManifestObjectName)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create directory for %q: %w", path, err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write manifest %q: %w", path, err)
	}
	return nil
}

func (f *filesystemWriterFactory) newWriter(captureID string) (*bufferedObjectWriter, error) {
	if captureID == "" {
		return nil, fmt.Errorf("capture ID is required")
	}

	upload := func(_ context.Context, objectName string, payload []byte) error {
		if err := os.MkdirAll(filepath.Dir(objectName), 0o755); err != nil {
			return fmt.Errorf("create directory for %q: %w", objectName, err)
		}
		if err := os.WriteFile(objectName, payload, 0o644); err != nil {
			return fmt.Errorf("write file %q: %w", objectName, err)
		}
		return nil
	}

	return newBufferedObjectWriter(
		captureID,
		f.flushInterval,
		f.maxBufferBytes,
		newFilesystemObjectName(f.mountPath, captureID, f.agentPod),
		upload,
	), nil
}

func NewEFSWriterFactory(cfg EFSConfig) (*EFSWriterFactory, error) {
	filesystem, err := newFilesystemWriterFactory(cfg.MountPath, cfg.AgentPod, cfg.FlushInterval, cfg.MaxBufferBytes)
	if err != nil {
		return nil, err
	}
	return &EFSWriterFactory{filesystem: filesystem}, nil
}

// PutManifest stores the dataset manifest for a capture.
func (f *EFSWriterFactory) PutManifest(_ context.Context, captureID string, data []byte) error {
	return f.filesystem.putManifest(captureID, data)
}

func (f *EFSWriterFactory) NewWriter(_ context.Context, captureID string) (Writer, error) {
	writer, err := f.filesystem.newWriter(captureID)
	if err != nil {
		return nil, err
	}
	return &EFSWriter{bufferedObjectWriter: writer}, nil
}
