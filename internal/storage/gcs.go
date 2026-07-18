package storage

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	gcsstorage "cloud.google.com/go/storage"
)

type gcsObjectWriter interface {
	Write(p []byte) (n int, err error)
	Close() error
	SetContentType(value string)
	SetContentEncoding(value string)
}

type gcsSDKWriter struct{ writer *gcsstorage.Writer }

func (w *gcsSDKWriter) Write(p []byte) (int, error) { return w.writer.Write(p) }
func (w *gcsSDKWriter) Close() error                { return w.writer.Close() }
func (w *gcsSDKWriter) SetContentType(value string) { w.writer.ContentType = value }
func (w *gcsSDKWriter) SetContentEncoding(value string) {
	w.writer.ContentEncoding = value
}

type GCSConfig struct {
	Bucket         string
	Prefix         string
	AgentPod       string
	FlushInterval  time.Duration
	MaxBufferBytes int
	Client         *gcsstorage.Client
	OpenWriter     func(ctx context.Context, bucket, objectName string) (gcsObjectWriter, error)
}

func (GCSConfig) isStorageConfig() {}

type GCSWriterFactory struct{ config GCSConfig }

type GCSWriter struct{ *bufferedObjectWriter }

func NewGCSWriterFactory(ctx context.Context, cfg GCSConfig) (*GCSWriterFactory, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("gcs bucket is required")
	}
	if cfg.OpenWriter == nil {
		if cfg.Client == nil {
			client, err := gcsstorage.NewClient(ctx)
			if err != nil {
				return nil, fmt.Errorf("create gcs client: %w", err)
			}
			cfg.Client = client
		}
		cfg.OpenWriter = func(ctx context.Context, bucket, objectName string) (gcsObjectWriter, error) {
			return &gcsSDKWriter{writer: cfg.Client.Bucket(bucket).Object(objectName).NewWriter(ctx)}, nil
		}
	}
	return &GCSWriterFactory{config: cfg}, nil
}

// PutManifest stores the dataset manifest for a capture under
// "{prefix}/{captureID}/manifest.json".
func (f *GCSWriterFactory) PutManifest(ctx context.Context, captureID string, data []byte) error {
	if captureID == "" {
		return fmt.Errorf("capture ID is required")
	}
	key := path.Join(strings.Trim(f.config.Prefix, "/"), captureID, ManifestObjectName)
	writer, err := f.config.OpenWriter(ctx, f.config.Bucket, key)
	if err != nil {
		return fmt.Errorf("open gcs writer for %q: %w", key, err)
	}
	writer.SetContentType("application/json")
	if _, err := writer.Write(data); err != nil {
		_ = writer.Close()
		return fmt.Errorf("write gcs manifest %q: %w", key, err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close gcs manifest %q: %w", key, err)
	}
	return nil
}

func (f *GCSWriterFactory) NewWriter(_ context.Context, captureID string) (Writer, error) {
	if captureID == "" {
		return nil, fmt.Errorf("capture ID is required")
	}

	upload := func(ctx context.Context, objectName string, payload []byte) error {
		writer, err := f.config.OpenWriter(ctx, f.config.Bucket, objectName)
		if err != nil {
			return fmt.Errorf("open gcs writer for %q: %w", objectName, err)
		}

		writer.SetContentType(contentTypeJSONL)
		writer.SetContentEncoding(contentEncodingGZIP)

		if _, err := writer.Write(payload); err != nil {
			_ = writer.Close()
			return fmt.Errorf("write gcs object %q: %w", objectName, err)
		}
		if err := writer.Close(); err != nil {
			return fmt.Errorf("close gcs object %q: %w", objectName, err)
		}
		return nil
	}

	return &GCSWriter{bufferedObjectWriter: newBufferedObjectWriter(
		captureID,
		f.config.FlushInterval,
		f.config.MaxBufferBytes,
		newCloudObjectName(f.config.Prefix, captureID, f.config.AgentPod),
		upload,
	)}, nil
}
