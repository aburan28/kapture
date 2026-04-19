package history

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/kapture-io/kapture/internal/storage"
	_ "github.com/lib/pq"
)

const (
	StatusPending   = "pending"
	StatusRunning   = "running"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
)

type Capture struct {
	ID          string
	Namespace   string
	Name        string
	Status      string
	TargetKind  string
	TargetName  string
	StorageName string
	StorageType string
	StartedAt   *time.Time
	CompletedAt *time.Time
	Error       string
}

type PostgresRepository struct {
	db *sql.DB
}

func NewPostgresRepository(ctx context.Context, databaseURL string) (*PostgresRepository, error) {
	databaseURL = strings.TrimSpace(databaseURL)
	if databaseURL == "" {
		return nil, errors.New("database URL is required")
	}

	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return &PostgresRepository{db: db}, nil
}

func (r *PostgresRepository) Close() error {
	if r == nil || r.db == nil {
		return nil
	}
	return r.db.Close()
}

func (r *PostgresRepository) UpsertCapture(ctx context.Context, capture Capture) error {
	if r == nil || r.db == nil {
		return nil
	}
	if strings.TrimSpace(capture.ID) == "" {
		return errors.New("capture ID is required")
	}
	if strings.TrimSpace(capture.Status) == "" {
		capture.Status = StatusPending
	}

	_, err := r.db.ExecContext(ctx, `
INSERT INTO captures (
  id, namespace, name, status, target_kind, target_name, storage_name,
  storage_type, started_at, completed_at, error, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULLIF($11, ''), now(), now())
ON CONFLICT (id) DO UPDATE SET
  namespace = EXCLUDED.namespace,
  name = EXCLUDED.name,
  status = EXCLUDED.status,
  target_kind = EXCLUDED.target_kind,
  target_name = EXCLUDED.target_name,
  storage_name = EXCLUDED.storage_name,
  storage_type = EXCLUDED.storage_type,
  started_at = COALESCE(captures.started_at, EXCLUDED.started_at),
  completed_at = COALESCE(EXCLUDED.completed_at, captures.completed_at),
  error = EXCLUDED.error,
  updated_at = now()
`, capture.ID, capture.Namespace, capture.Name, capture.Status, capture.TargetKind, capture.TargetName,
		capture.StorageName, capture.StorageType, capture.StartedAt, capture.CompletedAt, capture.Error)
	if err != nil {
		return fmt.Errorf("upsert capture history: %w", err)
	}
	return nil
}

func (r *PostgresRepository) RecordArtifact(ctx context.Context, artifact storage.ArtifactRecord) error {
	if r == nil || r.db == nil {
		return nil
	}
	if strings.TrimSpace(artifact.CaptureID) == "" {
		return errors.New("artifact capture ID is required")
	}
	if strings.TrimSpace(artifact.Bucket) == "" {
		return errors.New("artifact bucket is required")
	}
	if strings.TrimSpace(artifact.Key) == "" {
		return errors.New("artifact key is required")
	}
	if artifact.CreatedAt.IsZero() {
		artifact.CreatedAt = time.Now().UTC()
	}
	if artifact.StorageBackend == "" {
		artifact.StorageBackend = "s3"
	}

	_, err := r.db.ExecContext(ctx, `
INSERT INTO capture_artifacts (
  capture_id, storage_backend, s3_bucket, s3_key, s3_region,
  content_type, content_encoding, size_bytes, sha256, created_at
) VALUES ($1, $2, $3, $4, NULLIF($5, ''), NULLIF($6, ''), NULLIF($7, ''), $8, NULLIF($9, ''), $10)
ON CONFLICT (capture_id, s3_bucket, s3_key) DO UPDATE SET
  storage_backend = EXCLUDED.storage_backend,
  s3_region = EXCLUDED.s3_region,
  content_type = EXCLUDED.content_type,
  content_encoding = EXCLUDED.content_encoding,
  size_bytes = EXCLUDED.size_bytes,
  sha256 = EXCLUDED.sha256,
  created_at = EXCLUDED.created_at
`, artifact.CaptureID, artifact.StorageBackend, artifact.Bucket, artifact.Key, artifact.Region,
		artifact.ContentType, artifact.ContentEncoding, artifact.SizeBytes, artifact.SHA256, artifact.CreatedAt)
	if err != nil {
		return fmt.Errorf("record capture artifact: %w", err)
	}
	return nil
}

var _ storage.ArtifactRecorder = (*PostgresRepository)(nil)
