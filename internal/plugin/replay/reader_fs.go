package replay

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/kapture-io/kapture/internal/storage"
)

// FilesystemReaderConfig configures a filesystem-backed replay Reader.
// Works with both EFS and EBS storage backends.
type FilesystemReaderConfig struct {
	MountPath string
	Logger    *slog.Logger
}

// FilesystemReader streams captured requests from local filesystem JSONL.gz files.
// Suitable for EFS and EBS storage backends where data is locally mounted.
type FilesystemReader struct {
	mountPath string
	log       *slog.Logger

	files     []string
	fileIndex int
	scanner   *bufio.Scanner
	gzReader  *gzip.Reader
	file      *os.File
	opts      ReadOptions
	opened    bool
	closed    bool
	totalRead int64
}

// NewFilesystemReader creates a Reader that streams from a local filesystem path.
func NewFilesystemReader(cfg FilesystemReaderConfig) (*FilesystemReader, error) {
	if cfg.MountPath == "" {
		return nil, fmt.Errorf("mount path is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &FilesystemReader{
		mountPath: cfg.MountPath,
		log:       cfg.Logger,
	}, nil
}

func (r *FilesystemReader) Open(ctx context.Context, opts ReadOptions) error {
	if r.opened {
		return fmt.Errorf("reader already opened")
	}
	r.opts = opts
	r.opened = true

	basePath := r.mountPath
	if opts.CaptureID != "" {
		basePath = filepath.Join(basePath, opts.CaptureID)
	}

	files, err := r.listFiles(basePath, opts)
	if err != nil {
		return fmt.Errorf("list files: %w", err)
	}

	r.files = files
	r.fileIndex = -1
	r.log.Info("filesystem reader opened", "path", basePath, "files", len(files))
	return nil
}

func (r *FilesystemReader) Next(ctx context.Context) (*storage.CapturedRequest, error) {
	if !r.opened {
		return nil, fmt.Errorf("reader not opened")
	}
	if r.closed {
		return nil, ErrReaderDone
	}

	for {
		if r.scanner != nil && r.scanner.Scan() {
			req, err := unmarshalRequest(r.scanner.Bytes())
			if err != nil {
				r.log.Warn("skipping malformed line", "error", err, "file", r.files[r.fileIndex])
				continue
			}
			if !matchesReadOptions(req, r.opts) {
				continue
			}
			if r.opts.Limit > 0 && r.totalRead >= r.opts.Limit {
				return nil, ErrReaderDone
			}
			r.totalRead++
			return req, nil
		}

		if r.scanner != nil {
			if err := r.scanner.Err(); err != nil {
				return nil, fmt.Errorf("scan file %q: %w", r.files[r.fileIndex], err)
			}
		}

		r.closeCurrentFile()
		r.fileIndex++
		if r.fileIndex >= len(r.files) {
			return nil, ErrReaderDone
		}

		if err := r.openFile(r.files[r.fileIndex]); err != nil {
			return nil, fmt.Errorf("open file %q: %w", r.files[r.fileIndex], err)
		}

		r.log.Debug("reading file", "path", r.files[r.fileIndex],
			"progress", fmt.Sprintf("%d/%d", r.fileIndex+1, len(r.files)))
	}
}

func (r *FilesystemReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.closeCurrentFile()
	return nil
}

func (r *FilesystemReader) TotalRead() int64      { return r.totalRead }
func (r *FilesystemReader) FileCount() int        { return len(r.files) }
func (r *FilesystemReader) CurrentFileIndex() int { return r.fileIndex }

// LoadManifest returns the dataset manifest for a capture, or (nil, nil)
// when none exists.
func (r *FilesystemReader) LoadManifest(_ context.Context, captureID string) ([]byte, error) {
	path := filepath.Join(r.mountPath, captureID, storage.ManifestObjectName)
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read manifest %q: %w", path, err)
	}
	return data, nil
}

func (r *FilesystemReader) listFiles(basePath string, opts ReadOptions) ([]string, error) {
	var files []string
	err := filepath.WalkDir(basePath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".jsonl.gz") {
			return nil
		}
		if filterObjectByTime(filepath.ToSlash(path), opts.StartTime, opts.EndTime) {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Strings(files)
	return files, nil
}

func (r *FilesystemReader) openFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}

	// Use buffered reader for filesystem I/O efficiency.
	bufReader := bufio.NewReaderSize(f, 256*1024)

	r.file = f
	r.gzReader, err = gzip.NewReader(bufReader)
	if err != nil {
		f.Close()
		return fmt.Errorf("open gzip reader: %w", err)
	}

	r.scanner = bufio.NewScanner(r.gzReader)
	r.scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	return nil
}

func (r *FilesystemReader) closeCurrentFile() {
	if r.gzReader != nil {
		r.gzReader.Close()
		r.gzReader = nil
	}
	if r.file != nil {
		r.file.Close()
		r.file = nil
	}
	r.scanner = nil
}

// Verify FilesystemReader implements Reader at compile time.
var (
	_ Reader = (*FilesystemReader)(nil)
	_ Reader = (*S3Reader)(nil)
	_ Reader = (*GCSReader)(nil)
)

// ReaderFactory creates a Reader from storage type and configuration.
func NewReaderFromConfig(ctx context.Context, storageType string, config map[string]any) (Reader, error) {
	switch strings.ToLower(strings.TrimSpace(storageType)) {
	case "s3":
		return newS3ReaderFromConfig(ctx, config)
	case "gcs":
		return newGCSReaderFromConfig(ctx, config)
	case "efs", "ebs":
		return newFSReaderFromConfig(config)
	case "rds":
		return newRDSReaderFromConfig(ctx, config)
	default:
		return nil, fmt.Errorf("unsupported storage type for reading: %q", storageType)
	}
}

func newS3ReaderFromConfig(ctx context.Context, config map[string]any) (*S3Reader, error) {
	cfg := S3ReaderConfig{}
	if v, ok := config["bucket"].(string); ok {
		cfg.Bucket = v
	}
	if v, ok := config["region"].(string); ok {
		cfg.Region = v
	}
	if v, ok := config["prefix"].(string); ok {
		cfg.Prefix = v
	}
	return NewS3Reader(ctx, cfg)
}

func newGCSReaderFromConfig(ctx context.Context, config map[string]any) (*GCSReader, error) {
	cfg := GCSReaderConfig{}
	if v, ok := config["bucket"].(string); ok {
		cfg.Bucket = v
	}
	if v, ok := config["prefix"].(string); ok {
		cfg.Prefix = v
	}
	return NewGCSReader(ctx, cfg)
}

func newFSReaderFromConfig(config map[string]any) (*FilesystemReader, error) {
	cfg := FilesystemReaderConfig{}
	if v, ok := config["mountPath"].(string); ok {
		cfg.MountPath = v
	}
	return NewFilesystemReader(cfg)
}

// seekTo returns the index into a sorted slice of object/file keys where
// replay should resume. This is used by checkpoint support to skip already-
// replayed objects.
func seekTo(keys []string, resumeKey string) int {
	if resumeKey == "" {
		return 0
	}
	idx := sort.SearchStrings(keys, resumeKey)
	if idx < len(keys) && keys[idx] == resumeKey {
		// Start from the object after the last completed one.
		return idx + 1
	}
	return idx
}
