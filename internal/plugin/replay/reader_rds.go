package replay

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	// Register the PostgreSQL driver for database/sql.
	_ "github.com/lib/pq"

	"github.com/kapture-io/kapture/internal/storage"
)

var rdsReaderTablePattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// RDSReaderConfig configures a PostgreSQL/RDS-backed replay Reader.
type RDSReaderConfig struct {
	// DSN is the PostgreSQL connection URL.
	DSN string
	// Table is the captured-requests table (default "captured_requests").
	Table string
	// DB overrides the connection for tests.
	DB     *sql.DB
	Logger *slog.Logger
}

// RDSReader streams captured requests from database rows, ordered by
// capture timestamp. Time bounds are pushed down to SQL; the remaining
// ReadOptions filters run client-side like the other readers.
type RDSReader struct {
	db    *sql.DB
	table string
	log   *slog.Logger

	rows      *sql.Rows
	opts      ReadOptions
	opened    bool
	closed    bool
	totalRead int64
}

// NewRDSReader connects to the database and verifies the connection.
func NewRDSReader(ctx context.Context, cfg RDSReaderConfig) (*RDSReader, error) {
	table := cfg.Table
	if table == "" {
		table = storage.DefaultRDSTable
	}
	if !rdsReaderTablePattern.MatchString(table) {
		return nil, fmt.Errorf("invalid rds table name %q", table)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
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
	return &RDSReader{db: db, table: table, log: cfg.Logger}, nil
}

func (r *RDSReader) Open(ctx context.Context, opts ReadOptions) error {
	if r.opened {
		return fmt.Errorf("reader already opened")
	}
	r.opts = opts
	r.opened = true

	query := fmt.Sprintf(`SELECT id, ts, method, path, protocol, headers, metadata, body, content_length
		FROM %s WHERE capture_id = $1`, r.table)
	args := []any{opts.CaptureID}
	if !opts.StartTime.IsZero() {
		args = append(args, opts.StartTime)
		query += fmt.Sprintf(" AND ts >= $%d", len(args))
	}
	if !opts.EndTime.IsZero() {
		args = append(args, opts.EndTime)
		query += fmt.Sprintf(" AND ts < $%d", len(args))
	}
	query += " ORDER BY ts, id"

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("query captured requests: %w", err)
	}
	r.rows = rows
	r.log.Info("rds reader opened", "table", r.table, "captureID", opts.CaptureID)
	return nil
}

func (r *RDSReader) Next(ctx context.Context) (*storage.CapturedRequest, error) {
	if !r.opened {
		return nil, fmt.Errorf("reader not opened")
	}
	if r.closed || r.rows == nil {
		return nil, ErrReaderDone
	}

	for r.rows.Next() {
		var (
			req           storage.CapturedRequest
			ts            time.Time
			headersJSON   []byte
			metadataJSON  []byte
			contentLength int64
		)
		if err := r.rows.Scan(&req.ID, &ts, &req.Method, &req.Path, &req.Protocol,
			&headersJSON, &metadataJSON, &req.Body, &contentLength); err != nil {
			return nil, fmt.Errorf("scan captured request: %w", err)
		}
		req.Timestamp = ts
		req.ContentLength = contentLength
		if len(headersJSON) > 0 {
			if err := json.Unmarshal(headersJSON, &req.Headers); err != nil {
				r.log.Warn("skipping row with malformed headers", "error", err, "id", req.ID)
				continue
			}
		}
		if len(metadataJSON) > 0 {
			if err := json.Unmarshal(metadataJSON, &req.Metadata); err != nil {
				r.log.Warn("skipping row with malformed metadata", "error", err, "id", req.ID)
				continue
			}
		}

		if !matchesReadOptions(&req, r.opts) {
			continue
		}
		if r.opts.Limit > 0 && r.totalRead >= r.opts.Limit {
			return nil, ErrReaderDone
		}
		r.totalRead++
		return &req, nil
	}
	if err := r.rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate captured requests: %w", err)
	}
	return nil, ErrReaderDone
}

func (r *RDSReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.rows != nil {
		_ = r.rows.Close()
	}
	return r.db.Close()
}

// TotalRead returns the number of requests read so far.
func (r *RDSReader) TotalRead() int64 { return r.totalRead }

// LoadManifest returns the dataset manifest for a capture, or (nil, nil)
// when none exists.
func (r *RDSReader) LoadManifest(ctx context.Context, captureID string) ([]byte, error) {
	query := fmt.Sprintf(`SELECT manifest FROM %s_manifests WHERE capture_id = $1`, r.table)
	var data []byte
	err := r.db.QueryRowContext(ctx, query, captureID).Scan(&data)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		// A missing manifest table means no manifests were ever written.
		if strings.Contains(err.Error(), "does not exist") {
			return nil, nil
		}
		return nil, fmt.Errorf("load rds manifest: %w", err)
	}
	return data, nil
}

func newRDSReaderFromConfig(ctx context.Context, config map[string]any) (*RDSReader, error) {
	cfg := RDSReaderConfig{}
	if v, ok := config["dsn"].(string); ok {
		cfg.DSN = v
	}
	if v, ok := config["table"].(string); ok {
		cfg.Table = v
	}
	return NewRDSReader(ctx, cfg)
}
