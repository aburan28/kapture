package replay

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"sort"

	gcsstorage "cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"github.com/kapture-io/kapture/internal/storage"
)

// GCSReaderConfig configures a GCS-backed replay Reader.
type GCSReaderConfig struct {
	Bucket string
	Prefix string
	Client *gcsstorage.Client
	Logger *slog.Logger
}

// GCSReader streams captured requests from GCS JSONL.gz objects.
type GCSReader struct {
	bucket string
	prefix string
	client *gcsstorage.Client
	log    *slog.Logger

	objects     []string
	objectIndex int
	scanner     *bufio.Scanner
	gzReader    *gzip.Reader
	body        io.ReadCloser
	opts        ReadOptions
	opened      bool
	closed      bool
	totalRead   int64
}

// NewGCSReader creates a Reader that streams from Google Cloud Storage.
func NewGCSReader(ctx context.Context, cfg GCSReaderConfig) (*GCSReader, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("gcs bucket is required")
	}
	if cfg.Client == nil {
		client, err := gcsstorage.NewClient(ctx)
		if err != nil {
			return nil, fmt.Errorf("create gcs client: %w", err)
		}
		cfg.Client = client
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &GCSReader{
		bucket: cfg.Bucket,
		prefix: cfg.Prefix,
		client: cfg.Client,
		log:    cfg.Logger,
	}, nil
}

func (r *GCSReader) Open(ctx context.Context, opts ReadOptions) error {
	if r.opened {
		return fmt.Errorf("reader already opened")
	}
	r.opts = opts
	r.opened = true

	prefix := buildObjectPrefix(r.prefix, opts.CaptureID)
	objects, err := r.listObjects(ctx, prefix, opts)
	if err != nil {
		return fmt.Errorf("list gcs objects: %w", err)
	}

	r.objects = objects
	r.objectIndex = -1
	r.log.Info("gcs reader opened", "bucket", r.bucket, "prefix", prefix, "objects", len(objects))
	return nil
}

func (r *GCSReader) Next(ctx context.Context) (*storage.CapturedRequest, error) {
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
				r.log.Warn("skipping malformed line", "error", err, "object", r.objects[r.objectIndex])
				continue
			}
			if !matchesReadOptions(req, r.opts) {
				continue
			}
			r.totalRead++
			if r.opts.Limit > 0 && r.totalRead > r.opts.Limit {
				return nil, ErrReaderDone
			}
			return req, nil
		}

		if r.scanner != nil {
			if err := r.scanner.Err(); err != nil {
				return nil, fmt.Errorf("scan object %q: %w", r.objects[r.objectIndex], err)
			}
		}

		r.closeCurrentObject()
		r.objectIndex++
		if r.objectIndex >= len(r.objects) {
			return nil, ErrReaderDone
		}

		if err := r.openObject(ctx, r.objects[r.objectIndex]); err != nil {
			return nil, fmt.Errorf("open object %q: %w", r.objects[r.objectIndex], err)
		}

		r.log.Debug("reading object", "key", r.objects[r.objectIndex],
			"progress", fmt.Sprintf("%d/%d", r.objectIndex+1, len(r.objects)))
	}
}

func (r *GCSReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.closeCurrentObject()
	return nil
}

func (r *GCSReader) TotalRead() int64        { return r.totalRead }
func (r *GCSReader) ObjectCount() int         { return len(r.objects) }
func (r *GCSReader) CurrentObjectIndex() int  { return r.objectIndex }

func (r *GCSReader) listObjects(ctx context.Context, prefix string, opts ReadOptions) ([]string, error) {
	var objects []string
	it := r.client.Bucket(r.bucket).Objects(ctx, &gcsstorage.Query{Prefix: prefix})

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		if filterObjectByTime(attrs.Name, opts.StartTime, opts.EndTime) {
			objects = append(objects, attrs.Name)
		}
	}

	sort.Strings(objects)
	return objects, nil
}

func (r *GCSReader) openObject(ctx context.Context, key string) error {
	reader, err := r.client.Bucket(r.bucket).Object(key).NewReader(ctx)
	if err != nil {
		return err
	}

	r.body = reader
	r.gzReader, err = gzip.NewReader(r.body)
	if err != nil {
		r.body.Close()
		return fmt.Errorf("open gzip reader: %w", err)
	}

	r.scanner = bufio.NewScanner(r.gzReader)
	r.scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	return nil
}

func (r *GCSReader) closeCurrentObject() {
	if r.gzReader != nil {
		r.gzReader.Close()
		r.gzReader = nil
	}
	if r.body != nil {
		r.body.Close()
		r.body = nil
	}
	r.scanner = nil
}
