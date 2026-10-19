package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// ArtifactWriter stores non-request evidence outside the replay dataset prefix.
// Payload bytes never pass through capture-history relational tables.
type ArtifactWriter interface {
	PutArtifact(context.Context, string, string, []byte, string) (ArtifactRecord, error)
}

func artifactKey(prefix, captureID, name string) (string, error) {
	for _, s := range []string{captureID, name} {
		if s == "" || s == "." || strings.Contains(s, "\\") || strings.HasPrefix(s, "/") || path.Clean(s) != s || s == ".." || strings.HasPrefix(s, "../") {
			return "", fmt.Errorf("invalid artifact path %q", s)
		}
	}
	return path.Join(strings.Trim(prefix, "/"), "evidence", captureID, name), nil
}
func artifactRecord(captureID, key, typ string, data []byte) ArtifactRecord {
	sum := sha256.Sum256(data)
	return ArtifactRecord{CaptureID: captureID, Key: key, ContentType: typ, SizeBytes: int64(len(data)), SHA256: hex.EncodeToString(sum[:]), CreatedAt: time.Now().UTC()}
}

func (f *S3WriterFactory) PutArtifact(ctx context.Context, id, name string, data []byte, typ string) (ArtifactRecord, error) {
	key, err := artifactKey(f.config.Prefix, id, name)
	if err != nil {
		return ArtifactRecord{}, err
	}
	r := artifactRecord(id, key, typ, data)
	r.StorageBackend = "s3"
	r.Bucket = f.config.Bucket
	r.Region = f.config.Region
	r.CaptureName = f.config.CaptureName
	r.CaptureNamespace = f.config.CaptureNamespace
	_, err = f.config.Client.PutObject(ctx, &awss3.PutObjectInput{Bucket: aws.String(f.config.Bucket), Key: aws.String(key), Body: bytes.NewReader(data), ContentType: aws.String(typ)})
	if err != nil {
		return r, fmt.Errorf("put artifact: %w", err)
	}
	if f.config.ArtifactRecorder != nil {
		if err := f.config.ArtifactRecorder.RecordArtifact(ctx, r); err != nil {
			return r, fmt.Errorf("record uploaded artifact %q: %w", key, err)
		}
	}
	return r, nil
}
func (f *GCSWriterFactory) PutArtifact(ctx context.Context, id, name string, data []byte, typ string) (ArtifactRecord, error) {
	key, err := artifactKey(f.config.Prefix, id, name)
	if err != nil {
		return ArtifactRecord{}, err
	}
	r := artifactRecord(id, key, typ, data)
	r.StorageBackend = "gcs"
	r.Bucket = f.config.Bucket
	w, err := f.config.OpenWriter(ctx, f.config.Bucket, key)
	if err != nil {
		return r, err
	}
	w.SetContentType(typ)
	if _, err = w.Write(data); err != nil {
		_ = w.Close()
		return r, err
	}
	return r, w.Close()
}
func (f *filesystemWriterFactory) putArtifact(id, name string, data []byte, typ string) (ArtifactRecord, error) {
	key, err := artifactKey("", id, name)
	if err != nil {
		return ArtifactRecord{}, err
	}
	target := filepath.Join(f.mountPath, filepath.FromSlash(key))
	r := artifactRecord(id, target, typ, data)
	r.StorageBackend = "filesystem"
	if err = os.MkdirAll(filepath.Dir(target), 0700); err != nil {
		return r, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(target), ".artifact-*")
	if err != nil {
		return r, err
	}
	defer os.Remove(tmp.Name())
	if _, err = tmp.Write(data); err != nil {
		_ = tmp.Close()
		return r, err
	}
	if err = tmp.Sync(); err != nil {
		_ = tmp.Close()
		return r, err
	}
	if err = tmp.Close(); err != nil {
		return r, err
	}
	return r, os.Rename(tmp.Name(), target)
}
func (f *EFSWriterFactory) PutArtifact(_ context.Context, id, name string, data []byte, typ string) (ArtifactRecord, error) {
	return f.filesystem.putArtifact(id, name, data, typ)
}
func (f *EBSWriterFactory) PutArtifact(_ context.Context, id, name string, data []byte, typ string) (ArtifactRecord, error) {
	return f.filesystem.putArtifact(id, name, data, typ)
}
