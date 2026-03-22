package storage

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
)

// Reader provides sequential access to captured requests stored in a
// capture session. Implementations are expected to handle decompression
// and deserialization of the underlying gzip-compressed JSONL format.
type Reader interface {
	// Read returns an iterator over captured requests. The iterator yields
	// requests one at a time, allowing callers to process large capture
	// sessions without loading everything into memory. If an error occurs
	// during iteration, the second yield value will be non-nil and iteration
	// will stop.
	Read(ctx context.Context) iter.Seq2[*CapturedRequest, error]

	// Close releases any resources held by the reader.
	Close() error
}

// ReaderFactory creates Reader instances for specific capture sessions.
type ReaderFactory interface {
	NewReader(ctx context.Context, captureID string) (Reader, error)
}

// ObjectLister lists object keys matching a capture session prefix.
type ObjectLister interface {
	ListObjects(ctx context.Context, prefix string) ([]string, error)
}

// ObjectDownloader downloads an object by key and returns its contents.
type ObjectDownloader interface {
	Download(ctx context.Context, key string) (io.ReadCloser, error)
}

// ObjectStore combines listing and downloading for a storage backend.
type ObjectStore interface {
	ObjectLister
	ObjectDownloader
}

// objectStoreReaderFactory creates readers backed by an ObjectStore.
type objectStoreReaderFactory struct {
	store     ObjectStore
	prefix    string
	captureID string
}

// NewObjectStoreReaderFactory creates a ReaderFactory backed by an ObjectStore.
func NewObjectStoreReaderFactory(store ObjectStore, prefix string) ReaderFactory {
	return &objectStoreReaderFactory{store: store, prefix: prefix}
}

func (f *objectStoreReaderFactory) NewReader(_ context.Context, captureID string) (Reader, error) {
	if captureID == "" {
		return nil, fmt.Errorf("capture ID is required")
	}
	return &objectStoreReader{
		store:     f.store,
		prefix:    buildPrefix(f.prefix, captureID),
		captureID: captureID,
	}, nil
}

type objectStoreReader struct {
	store     ObjectStore
	prefix    string
	captureID string
}

func (r *objectStoreReader) Read(ctx context.Context) iter.Seq2[*CapturedRequest, error] {
	return func(yield func(*CapturedRequest, error) bool) {
		keys, err := r.store.ListObjects(ctx, r.prefix)
		if err != nil {
			yield(nil, fmt.Errorf("list objects for capture %q: %w", r.captureID, err))
			return
		}

		for _, key := range keys {
			if ctx.Err() != nil {
				yield(nil, ctx.Err())
				return
			}

			rc, err := r.store.Download(ctx, key)
			if err != nil {
				if !yield(nil, fmt.Errorf("download object %q: %w", key, err)) {
					return
				}
				continue
			}

			if !decodeJSONLGzip(rc, yield) {
				rc.Close()
				return
			}
			rc.Close()
		}
	}
}

func (r *objectStoreReader) Close() error { return nil }

// decodeJSONLGzip reads a gzip-compressed JSONL stream and yields each
// CapturedRequest. Returns false if the caller stopped iteration.
func decodeJSONLGzip(r io.Reader, yield func(*CapturedRequest, error) bool) bool {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return yield(nil, fmt.Errorf("open gzip reader: %w", err))
	}
	defer gz.Close()

	scanner := bufio.NewScanner(gz)
	scanner.Buffer(make([]byte, 0, 256*1024), 10*1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var req CapturedRequest
		if err := json.Unmarshal(line, &req); err != nil {
			if !yield(nil, fmt.Errorf("unmarshal captured request: %w", err)) {
				return false
			}
			continue
		}

		if !yield(&req, nil) {
			return false
		}
	}

	if err := scanner.Err(); err != nil {
		return yield(nil, fmt.Errorf("scan JSONL: %w", err))
	}
	return true
}

func buildPrefix(prefix, captureID string) string {
	if prefix == "" {
		return captureID
	}
	return prefix + "/" + captureID
}
