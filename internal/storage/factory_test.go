package storage

import (
	"context"
	"testing"
)

func TestNewWriterFactorySelectsBackend(t *testing.T) {
	tests := []struct {
		name        string
		storageType string
		config      any
		wantType    any
	}{
		{name: "s3", storageType: "s3", config: S3Config{Bucket: "captures", Client: &fakeS3Client{}}, wantType: &S3WriterFactory{}},
		{name: "gcs", storageType: "gcs", config: GCSConfig{Bucket: "captures", OpenWriter: func(_ context.Context, _, _ string) (gcsObjectWriter, error) { return &fakeGCSWriter{}, nil }}, wantType: &GCSWriterFactory{}},
		{name: "efs", storageType: "efs", config: EFSConfig{MountPath: t.TempDir()}, wantType: &EFSWriterFactory{}},
		{name: "ebs", storageType: "ebs", config: EBSConfig{MountPath: t.TempDir()}, wantType: &EBSWriterFactory{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory, err := NewWriterFactory(tt.storageType, tt.config)
			if err != nil {
				t.Fatalf("NewWriterFactory() error = %v", err)
			}
			switch tt.wantType.(type) {
			case *S3WriterFactory:
				if _, ok := factory.(*S3WriterFactory); !ok {
					t.Fatalf("factory type = %T, want *S3WriterFactory", factory)
				}
			case *GCSWriterFactory:
				if _, ok := factory.(*GCSWriterFactory); !ok {
					t.Fatalf("factory type = %T, want *GCSWriterFactory", factory)
				}
			case *EFSWriterFactory:
				if _, ok := factory.(*EFSWriterFactory); !ok {
					t.Fatalf("factory type = %T, want *EFSWriterFactory", factory)
				}
			case *EBSWriterFactory:
				if _, ok := factory.(*EBSWriterFactory); !ok {
					t.Fatalf("factory type = %T, want *EBSWriterFactory", factory)
				}
			}
		})
	}
}

func TestNewWriterFactoryRejectsUnsupportedType(t *testing.T) {
	if _, err := NewWriterFactory("unknown", nil); err == nil {
		t.Fatal("NewWriterFactory() error = nil, want error")
	}
}
