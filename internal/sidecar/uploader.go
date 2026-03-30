package sidecar

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/kapture-io/kapture/internal/storage"
)

// Uploader reads capture output files and writes their contents to a
// storage.Writer, then removes the local file.
type Uploader struct {
	writer storage.Writer
	format string // "jsonl" or "pcap"
	delete bool   // delete files after upload
	log    *slog.Logger
}

// UploaderConfig configures an Uploader.
type UploaderConfig struct {
	Writer storage.Writer
	Format string // "jsonl" or "pcap"
	Delete bool
	Logger *slog.Logger
}

// NewUploader creates a new Uploader.
func NewUploader(cfg UploaderConfig) *Uploader {
	if cfg.Format == "" {
		cfg.Format = "jsonl"
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Uploader{
		writer: cfg.Writer,
		format: cfg.Format,
		delete: cfg.Delete,
		log:    cfg.Logger,
	}
}

// Upload reads a file and writes its contents to storage. For JSONL files,
// each line is parsed as a CapturedRequest. For pcap files, each file is
// wrapped as a single CapturedRequest with the raw bytes in the body.
func (u *Uploader) Upload(ctx context.Context, path string) error {
	switch u.format {
	case "jsonl":
		return u.uploadJSONL(ctx, path)
	case "pcap":
		return u.uploadPCAP(ctx, path)
	default:
		return fmt.Errorf("unsupported format: %s", u.format)
	}
}

func (u *Uploader) uploadJSONL(ctx context.Context, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024) // 10MB max line

	var count int
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var req storage.CapturedRequest
		if err := json.Unmarshal(line, &req); err != nil {
			u.log.Warn("skipping malformed JSONL line", "file", path, "error", err)
			continue
		}

		if err := u.writer.Write(ctx, &req); err != nil {
			return fmt.Errorf("write captured request: %w", err)
		}
		count++
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan %s: %w", path, err)
	}

	if err := u.writer.Flush(ctx); err != nil {
		return fmt.Errorf("flush after %s: %w", path, err)
	}

	u.log.Info("uploaded JSONL file", "path", path, "requests", count)

	if u.delete {
		if err := os.Remove(path); err != nil {
			u.log.Warn("failed to delete uploaded file", "path", path, "error", err)
		}
	}

	return nil
}

func (u *Uploader) uploadPCAP(ctx context.Context, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}

	// Wrap raw pcap data as a single CapturedRequest.
	req := &storage.CapturedRequest{
		ID:            filepath.Base(path),
		Method:        "PCAP",
		Path:          path,
		Body:          data,
		ContentLength: int64(len(data)),
		Protocol:      "pcap",
		Metadata: map[string]string{
			"format":   "pcap",
			"filename": filepath.Base(path),
		},
	}

	if err := u.writer.Write(ctx, req); err != nil {
		return fmt.Errorf("write pcap: %w", err)
	}

	if err := u.writer.Flush(ctx); err != nil {
		return fmt.Errorf("flush after pcap: %w", err)
	}

	u.log.Info("uploaded pcap file", "path", path, "bytes", len(data))

	if u.delete {
		if err := os.Remove(path); err != nil {
			u.log.Warn("failed to delete uploaded file", "path", path, "error", err)
		}
	}

	return nil
}
