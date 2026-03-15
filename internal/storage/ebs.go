package storage

import (
	"context"
	"time"
)

type EBSConfig struct {
	MountPath      string
	AgentPod       string
	FlushInterval  time.Duration
	MaxBufferBytes int
}

func (EBSConfig) isStorageConfig() {}

type EBSWriterFactory struct{ filesystem *filesystemWriterFactory }

type EBSWriter struct{ *bufferedObjectWriter }

func NewEBSWriterFactory(cfg EBSConfig) (*EBSWriterFactory, error) {
	filesystem, err := newFilesystemWriterFactory(cfg.MountPath, cfg.AgentPod, cfg.FlushInterval, cfg.MaxBufferBytes)
	if err != nil {
		return nil, err
	}
	return &EBSWriterFactory{filesystem: filesystem}, nil
}

func (f *EBSWriterFactory) NewWriter(_ context.Context, captureID string) (Writer, error) {
	writer, err := f.filesystem.newWriter(captureID)
	if err != nil {
		return nil, err
	}
	return &EBSWriter{bufferedObjectWriter: writer}, nil
}
