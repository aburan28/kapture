package storage

import (
	"context"
	"fmt"
	"strings"
)

func NewWriterFactory(storageType string, config any) (WriterFactory, error) {
	switch strings.ToLower(strings.TrimSpace(storageType)) {
	case "s3":
		cfg, err := asS3Config(config)
		if err != nil {
			return nil, err
		}
		return NewS3WriterFactory(context.Background(), cfg)
	case "gcs":
		cfg, err := asGCSConfig(config)
		if err != nil {
			return nil, err
		}
		return NewGCSWriterFactory(context.Background(), cfg)
	case "efs":
		cfg, err := asEFSConfig(config)
		if err != nil {
			return nil, err
		}
		return NewEFSWriterFactory(cfg)
	case "ebs":
		cfg, err := asEBSConfig(config)
		if err != nil {
			return nil, err
		}
		return NewEBSWriterFactory(cfg)
	default:
		return nil, fmt.Errorf("unsupported storage type %q", storageType)
	}
}

func asS3Config(config any) (S3Config, error) {
	switch cfg := config.(type) {
	case S3Config:
		return cfg, nil
	case *S3Config:
		if cfg == nil {
			return S3Config{}, fmt.Errorf("s3 config cannot be nil")
		}
		return *cfg, nil
	default:
		return S3Config{}, fmt.Errorf("invalid config type %T for s3", config)
	}
}

func asGCSConfig(config any) (GCSConfig, error) {
	switch cfg := config.(type) {
	case GCSConfig:
		return cfg, nil
	case *GCSConfig:
		if cfg == nil {
			return GCSConfig{}, fmt.Errorf("gcs config cannot be nil")
		}
		return *cfg, nil
	default:
		return GCSConfig{}, fmt.Errorf("invalid config type %T for gcs", config)
	}
}

func asEFSConfig(config any) (EFSConfig, error) {
	switch cfg := config.(type) {
	case EFSConfig:
		return cfg, nil
	case *EFSConfig:
		if cfg == nil {
			return EFSConfig{}, fmt.Errorf("efs config cannot be nil")
		}
		return *cfg, nil
	default:
		return EFSConfig{}, fmt.Errorf("invalid config type %T for efs", config)
	}
}

func asEBSConfig(config any) (EBSConfig, error) {
	switch cfg := config.(type) {
	case EBSConfig:
		return cfg, nil
	case *EBSConfig:
		if cfg == nil {
			return EBSConfig{}, fmt.Errorf("ebs config cannot be nil")
		}
		return *cfg, nil
	default:
		return EBSConfig{}, fmt.Errorf("invalid config type %T for ebs", config)
	}
}
