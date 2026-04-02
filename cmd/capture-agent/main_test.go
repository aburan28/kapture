package main

import (
	"testing"

	"github.com/kapture-io/kapture/internal/storage"
)

func TestParseStorageConfig(t *testing.T) {
	tests := []struct {
		name        string
		storageType string
		rawJSON     string
		want        any
		wantErr     bool
	}{
		// Valid JSON for each storage type
		{
			name:        "valid s3 config",
			storageType: "s3",
			rawJSON:     `{"Bucket":"my-bucket","Region":"us-east-1","Prefix":"captures/"}`,
			want:        storage.S3Config{Bucket: "my-bucket", Region: "us-east-1", Prefix: "captures/"},
		},
		{
			name:        "valid gcs config",
			storageType: "gcs",
			rawJSON:     `{"Bucket":"my-gcs-bucket","Prefix":"data/"}`,
			want:        storage.GCSConfig{Bucket: "my-gcs-bucket", Prefix: "data/"},
		},
		{
			name:        "valid efs config",
			storageType: "efs",
			rawJSON:     `{"MountPath":"/mnt/efs"}`,
			want:        storage.EFSConfig{MountPath: "/mnt/efs"},
		},
		{
			name:        "valid ebs config",
			storageType: "ebs",
			rawJSON:     `{"MountPath":"/mnt/ebs"}`,
			want:        storage.EBSConfig{MountPath: "/mnt/ebs"},
		},
		{
			name:        "valid plugin config",
			storageType: "plugin",
			rawJSON:     `{"Path":"/usr/lib/plugin.so","Symbol":"NewFactory","Config":{"key":"value"}}`,
			want:        storage.PluginConfig{Path: "/usr/lib/plugin.so", Symbol: "NewFactory", Config: map[string]any{"key": "value"}},
		},

		// Empty JSON returns zero-value config
		{
			name:        "empty json s3",
			storageType: "s3",
			rawJSON:     `{}`,
			want:        storage.S3Config{},
		},
		{
			name:        "empty json gcs",
			storageType: "gcs",
			rawJSON:     `{}`,
			want:        storage.GCSConfig{},
		},
		{
			name:        "empty json efs",
			storageType: "efs",
			rawJSON:     `{}`,
			want:        storage.EFSConfig{},
		},
		{
			name:        "empty json ebs",
			storageType: "ebs",
			rawJSON:     `{}`,
			want:        storage.EBSConfig{},
		},
		{
			name:        "empty json plugin",
			storageType: "plugin",
			rawJSON:     `{}`,
			want:        storage.PluginConfig{},
		},

		// Invalid JSON
		{
			name:        "invalid json s3",
			storageType: "s3",
			rawJSON:     `{not valid json}`,
			wantErr:     true,
		},
		{
			name:        "invalid json gcs",
			storageType: "gcs",
			rawJSON:     `{bad`,
			wantErr:     true,
		},
		{
			name:        "invalid json efs",
			storageType: "efs",
			rawJSON:     `!!!`,
			wantErr:     true,
		},
		{
			name:        "invalid json ebs",
			storageType: "ebs",
			rawJSON:     `[1,2,3]`,
			wantErr:     true,
		},
		{
			name:        "invalid json plugin",
			storageType: "plugin",
			rawJSON:     `"just a string"`,
			wantErr:     true,
		},

		// Unsupported storage type
		{
			name:        "unsupported storage type",
			storageType: "azure",
			rawJSON:     `{}`,
			wantErr:     true,
		},
		{
			name:        "empty storage type",
			storageType: "",
			rawJSON:     `{}`,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseStorageConfig(tt.storageType, tt.rawJSON)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (result: %v)", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Compare using formatted strings since some config types contain
			// unexported or interface fields that complicate reflect.DeepEqual.
			switch want := tt.want.(type) {
			case storage.S3Config:
				gotCfg, ok := got.(storage.S3Config)
				if !ok {
					t.Fatalf("expected S3Config, got %T", got)
				}
				if gotCfg.Bucket != want.Bucket || gotCfg.Region != want.Region || gotCfg.Prefix != want.Prefix {
					t.Errorf("got %+v, want %+v", gotCfg, want)
				}
			case storage.GCSConfig:
				gotCfg, ok := got.(storage.GCSConfig)
				if !ok {
					t.Fatalf("expected GCSConfig, got %T", got)
				}
				if gotCfg.Bucket != want.Bucket || gotCfg.Prefix != want.Prefix {
					t.Errorf("got %+v, want %+v", gotCfg, want)
				}
			case storage.EFSConfig:
				gotCfg, ok := got.(storage.EFSConfig)
				if !ok {
					t.Fatalf("expected EFSConfig, got %T", got)
				}
				if gotCfg.MountPath != want.MountPath {
					t.Errorf("got %+v, want %+v", gotCfg, want)
				}
			case storage.EBSConfig:
				gotCfg, ok := got.(storage.EBSConfig)
				if !ok {
					t.Fatalf("expected EBSConfig, got %T", got)
				}
				if gotCfg.MountPath != want.MountPath {
					t.Errorf("got %+v, want %+v", gotCfg, want)
				}
			case storage.PluginConfig:
				gotCfg, ok := got.(storage.PluginConfig)
				if !ok {
					t.Fatalf("expected PluginConfig, got %T", got)
				}
				if gotCfg.Path != want.Path || gotCfg.Symbol != want.Symbol {
					t.Errorf("got %+v, want %+v", gotCfg, want)
				}
			default:
				t.Fatalf("unhandled config type: %T", tt.want)
			}
		})
	}
}
