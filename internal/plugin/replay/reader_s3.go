package replay

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/kapture-io/kapture/internal/storage"
)

// s3ListObjectsAPI abstracts S3 listing for testability.
type s3ListObjectsAPI interface {
	ListObjectsV2(ctx context.Context, params *awss3.ListObjectsV2Input, optFns ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error)
}

// s3GetObjectAPI abstracts S3 object retrieval for testability.
type s3GetObjectAPI interface {
	GetObject(ctx context.Context, params *awss3.GetObjectInput, optFns ...func(*awss3.Options)) (*awss3.GetObjectOutput, error)
}

// s3FullAPI combines the S3 interfaces needed by the reader.
type s3FullAPI interface {
	s3ListObjectsAPI
	s3GetObjectAPI
}

// S3ReaderConfig configures an S3-backed replay Reader.
type S3ReaderConfig struct {
	Bucket string
	Region string
	Prefix string
	Client s3FullAPI
	Logger *slog.Logger
}

// S3Reader streams captured requests from S3 JSONL.gz objects.
// It lists all objects under the capture prefix, sorts them by key,
// and streams through them one at a time to bound memory usage.
type S3Reader struct {
	bucket string
	prefix string
	client s3FullAPI
	log    *slog.Logger

	// Runtime state
	objects     []string       // sorted object keys to read
	objectIndex int            // current position in objects slice
	scanner     *bufio.Scanner // line scanner for current object
	gzReader    *gzip.Reader   // decompressor for current object
	body        io.ReadCloser  // raw S3 response body
	opts        ReadOptions
	opened      bool
	closed      bool
	totalRead   int64
}

// NewS3Reader creates a Reader that streams from S3.
func NewS3Reader(ctx context.Context, cfg S3ReaderConfig) (*S3Reader, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3 bucket is required")
	}
	if cfg.Client == nil {
		loadOptions := make([]func(*awscfg.LoadOptions) error, 0, 1)
		if cfg.Region != "" {
			loadOptions = append(loadOptions, awscfg.WithRegion(cfg.Region))
		}
		awsCfg, err := awscfg.LoadDefaultConfig(ctx, loadOptions...)
		if err != nil {
			return nil, fmt.Errorf("load aws config: %w", err)
		}
		cfg.Client = awss3.NewFromConfig(awsCfg)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &S3Reader{
		bucket: cfg.Bucket,
		prefix: cfg.Prefix,
		client: cfg.Client,
		log:    cfg.Logger,
	}, nil
}

func (r *S3Reader) Open(ctx context.Context, opts ReadOptions) error {
	if r.opened {
		return fmt.Errorf("reader already opened")
	}
	r.opts = opts
	r.opened = true

	prefix := buildObjectPrefix(r.prefix, opts.CaptureID)
	objects, err := r.listObjects(ctx, prefix, opts)
	if err != nil {
		return fmt.Errorf("list s3 objects: %w", err)
	}

	r.objects = objects
	r.objectIndex = -1
	r.log.Info("s3 reader opened", "bucket", r.bucket, "prefix", prefix, "objects", len(objects))
	return nil
}

func (r *S3Reader) Next(ctx context.Context) (*storage.CapturedRequest, error) {
	if !r.opened {
		return nil, fmt.Errorf("reader not opened")
	}
	if r.closed {
		return nil, ErrReaderDone
	}

	for {
		// Try to read from current scanner.
		if r.scanner != nil && r.scanner.Scan() {
			req, err := unmarshalRequest(r.scanner.Bytes())
			if err != nil {
				r.log.Warn("skipping malformed line", "error", err, "object", r.objects[r.objectIndex])
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

		// Check for scanner error.
		if r.scanner != nil {
			if err := r.scanner.Err(); err != nil {
				return nil, fmt.Errorf("scan object %q: %w", r.objects[r.objectIndex], err)
			}
		}

		// Move to next object.
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

func (r *S3Reader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.closeCurrentObject()
	return nil
}

// TotalRead returns the number of requests read so far.
func (r *S3Reader) TotalRead() int64 { return r.totalRead }

// ObjectCount returns the total number of objects to read.
func (r *S3Reader) ObjectCount() int { return len(r.objects) }

// CurrentObjectIndex returns the current object index (0-based).
func (r *S3Reader) CurrentObjectIndex() int { return r.objectIndex }

func (r *S3Reader) listObjects(ctx context.Context, prefix string, opts ReadOptions) ([]string, error) {
	var objects []string
	var continuationToken *string

	for {
		input := &awss3.ListObjectsV2Input{
			Bucket:            aws.String(r.bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		}

		output, err := r.client.ListObjectsV2(ctx, input)
		if err != nil {
			return nil, err
		}

		for _, obj := range output.Contents {
			key := aws.ToString(obj.Key)
			if filterObjectByTime(key, opts.StartTime, opts.EndTime) {
				objects = append(objects, key)
			}
		}

		if !aws.ToBool(output.IsTruncated) {
			break
		}
		continuationToken = output.NextContinuationToken
	}

	sort.Strings(objects)
	return objects, nil
}

func (r *S3Reader) openObject(ctx context.Context, key string) error {
	output, err := r.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}

	r.body = output.Body
	r.gzReader, err = gzip.NewReader(r.body)
	if err != nil {
		r.body.Close()
		return fmt.Errorf("open gzip reader: %w", err)
	}

	r.scanner = bufio.NewScanner(r.gzReader)
	// Allow up to 10MB per line for large request bodies.
	r.scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	return nil
}

func (r *S3Reader) closeCurrentObject() {
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

// --- Shared helpers used by all reader implementations ---

// buildObjectPrefix constructs the S3/GCS prefix for listing capture objects.
func buildObjectPrefix(storagePrefix, captureID string) string {
	storagePrefix = strings.Trim(storagePrefix, "/")
	parts := make([]string, 0, 2)
	if storagePrefix != "" {
		parts = append(parts, storagePrefix)
	}
	if captureID != "" {
		parts = append(parts, captureID)
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, "/") + "/"
}

// filterObjectByTime checks if an object key's date component falls within
// the specified time range. Object keys follow the pattern:
// [prefix/]captureID/YYYY-MM-DD/agent-NNNNNN.jsonl.gz
func filterObjectByTime(key string, startTime, endTime time.Time) bool {
	if startTime.IsZero() && endTime.IsZero() {
		return true
	}

	parts := strings.Split(key, "/")
	for _, part := range parts {
		t, err := time.Parse("2006-01-02", part)
		if err != nil {
			continue
		}
		if !startTime.IsZero() && t.Before(startTime.Truncate(24*time.Hour)) {
			return false
		}
		if !endTime.IsZero() && t.After(endTime.Truncate(24*time.Hour)) {
			return false
		}
		return true
	}

	// No date component found — include the object.
	return true
}

// unmarshalRequest parses a single JSONL line into a CapturedRequest.
func unmarshalRequest(line []byte) (*storage.CapturedRequest, error) {
	var req storage.CapturedRequest
	if err := json.Unmarshal(line, &req); err != nil {
		return nil, err
	}
	return &req, nil
}

// matchesReadOptions checks if a request matches the filter criteria.
func matchesReadOptions(req *storage.CapturedRequest, opts ReadOptions) bool {
	if !opts.StartTime.IsZero() && req.Timestamp.Before(opts.StartTime) {
		return false
	}
	if !opts.EndTime.IsZero() && req.Timestamp.After(opts.EndTime) {
		return false
	}
	if opts.PathPrefix != "" && !strings.HasPrefix(req.Path, opts.PathPrefix) {
		return false
	}
	if len(opts.Methods) > 0 {
		found := false
		for _, m := range opts.Methods {
			if strings.EqualFold(req.Method, m) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

