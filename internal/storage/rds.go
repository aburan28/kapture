package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	// Register the PostgreSQL driver for database/sql.
	_ "github.com/lib/pq"
)

// DefaultRDSTable is the captured-requests table name.
const DefaultRDSTable = "captured_requests"

// rdsManifestTableSuffix names the manifest table relative to the data
// table ("captured_requests" → "captured_requests_manifests").
const rdsManifestTableSuffix = "_manifests"

var rdsTablePattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// RDSConfig configures the PostgreSQL/RDS storage backend. Captured
// requests are stored as rows, making them queryable with SQL and
// replayable like any other backend.
type RDSConfig struct {
	// DSN is the PostgreSQL connection URL.
	DSN string
	// Table overrides the captured-requests table name (identifier-safe
	// names only; validated).
	Table string
	// BatchSize is how many rows are buffered before an insert
	// (default 100).
	BatchSize int
	// FlushInterval bounds how long rows may sit unflushed (default 5s).
	FlushInterval time.Duration

	// DB overrides the connection for tests.
	DB *sql.DB
}

func (RDSConfig) isStorageConfig() {}

// RDSWriterFactory creates writers that persist captured requests to a
// PostgreSQL-compatible database. The schema is created on first use.
type RDSWriterFactory struct {
	db    *sql.DB
	table string
	cfg   RDSConfig
}

// NewRDSWriterFactory connects, verifies the connection, and ensures the
// schema exists.
func NewRDSWriterFactory(ctx context.Context, cfg RDSConfig) (*RDSWriterFactory, error) {
	table := cfg.Table
	if table == "" {
		table = DefaultRDSTable
	}
	if !rdsTablePattern.MatchString(table) {
		return nil, fmt.Errorf("invalid rds table name %q", table)
	}

	db := cfg.DB
	if db == nil {
		if strings.TrimSpace(cfg.DSN) == "" {
			return nil, fmt.Errorf("rds dsn is required")
		}
		var err error
		db, err = sql.Open("postgres", cfg.DSN)
		if err != nil {
			return nil, fmt.Errorf("open rds connection: %w", err)
		}
		if err := db.PingContext(ctx); err != nil {
			db.Close()
			return nil, fmt.Errorf("ping rds: %w", err)
		}
	}

	f := &RDSWriterFactory{db: db, table: table, cfg: cfg}
	if err := f.ensureSchema(ctx); err != nil {
		if cfg.DB == nil {
			db.Close()
		}
		return nil, err
	}
	return f, nil
}

func (f *RDSWriterFactory) ensureSchema(ctx context.Context) error {
	statements := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			capture_id TEXT NOT NULL,
			id TEXT NOT NULL,
			ts TIMESTAMPTZ NOT NULL,
			method TEXT NOT NULL DEFAULT '',
			path TEXT NOT NULL DEFAULT '',
			protocol TEXT NOT NULL DEFAULT '',
			headers JSONB,
			metadata JSONB,
			body BYTEA,
			content_length BIGINT NOT NULL DEFAULT 0,
			PRIMARY KEY (capture_id, id)
		)`, f.table),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_capture_ts ON %s (capture_id, ts)`, f.table, f.table),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s%s (
			capture_id TEXT PRIMARY KEY,
			manifest JSONB NOT NULL
		)`, f.table, rdsManifestTableSuffix),
	}
	for _, stmt := range statements {
		if _, err := f.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("ensure rds schema: %w", err)
		}
	}
	return nil
}

// PutManifest upserts the dataset manifest for a capture.
func (f *RDSWriterFactory) PutManifest(ctx context.Context, captureID string, data []byte) error {
	if captureID == "" {
		return fmt.Errorf("capture ID is required")
	}
	query := fmt.Sprintf(`INSERT INTO %s%s (capture_id, manifest) VALUES ($1, $2)
		ON CONFLICT (capture_id) DO UPDATE SET manifest = EXCLUDED.manifest`,
		f.table, rdsManifestTableSuffix)
	if _, err := f.db.ExecContext(ctx, query, captureID, data); err != nil {
		return fmt.Errorf("put rds manifest: %w", err)
	}
	return nil
}

// NewWriter creates a batching writer for one capture.
func (f *RDSWriterFactory) NewWriter(_ context.Context, captureID string) (Writer, error) {
	if captureID == "" {
		return nil, fmt.Errorf("capture ID is required")
	}
	batchSize := f.cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}
	return &rdsWriter{
		db:            f.db,
		table:         f.table,
		captureID:     captureID,
		batchSize:     batchSize,
		flushInterval: normalizeFlushInterval(f.cfg.FlushInterval),
		lastFlush:     time.Now(),
	}, nil
}

// rdsWriter buffers captured requests and inserts them in batches.
// Inserts are idempotent (ON CONFLICT DO NOTHING on the request ID), so
// retried flushes cannot duplicate rows.
type rdsWriter struct {
	mu            sync.Mutex
	db            *sql.DB
	table         string
	captureID     string
	batchSize     int
	flushInterval time.Duration
	lastFlush     time.Time
	pending       []*CapturedRequest
	closed        bool
}

func (w *rdsWriter) Write(ctx context.Context, req *CapturedRequest) error {
	if req == nil {
		return fmt.Errorf("captured request cannot be nil")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrWriterClosed
	}
	w.pending = append(w.pending, req)
	if len(w.pending) >= w.batchSize || time.Since(w.lastFlush) >= w.flushInterval {
		return w.flushLocked(ctx)
	}
	return nil
}

func (w *rdsWriter) Flush(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrWriterClosed
	}
	return w.flushLocked(ctx)
}

func (w *rdsWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	err := w.flushLocked(context.Background())
	w.closed = true
	return err
}

func (w *rdsWriter) flushLocked(ctx context.Context) error {
	if len(w.pending) == 0 {
		w.lastFlush = time.Now()
		return nil
	}

	const columns = 10
	values := make([]string, 0, len(w.pending))
	args := make([]any, 0, len(w.pending)*columns)
	for i, req := range w.pending {
		base := i * columns
		placeholders := make([]string, columns)
		for j := range placeholders {
			placeholders[j] = fmt.Sprintf("$%d", base+j+1)
		}
		values = append(values, "("+strings.Join(placeholders, ",")+")")

		headers, err := json.Marshal(req.Headers)
		if err != nil {
			return fmt.Errorf("marshal headers: %w", err)
		}
		metadata, err := json.Marshal(req.Metadata)
		if err != nil {
			return fmt.Errorf("marshal metadata: %w", err)
		}
		args = append(args, w.captureID, req.ID, req.Timestamp.UTC(), req.Method,
			req.Path, req.Protocol, headers, metadata, req.Body, req.ContentLength)
	}

	query := fmt.Sprintf(`INSERT INTO %s
		(capture_id, id, ts, method, path, protocol, headers, metadata, body, content_length)
		VALUES %s ON CONFLICT (capture_id, id) DO NOTHING`,
		w.table, strings.Join(values, ","))

	if _, err := w.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("insert captured requests: %w", err)
	}
	w.pending = w.pending[:0]
	w.lastFlush = time.Now()
	return nil
}
