package storage

import (
	"context"
	"fmt"
	"io"
	"strings"

	gcsstorage "cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// GCSObjectStore implements ObjectStore for Google Cloud Storage.
type GCSObjectStore struct {
	client *gcsstorage.Client
	bucket string
}

// GCSReaderConfig configures a GCS-backed ReaderFactory.
type GCSReaderConfig struct {
	Bucket string
	Prefix string
	Client *gcsstorage.Client
}

// NewGCSReaderFactory creates a ReaderFactory that reads captured data from GCS.
func NewGCSReaderFactory(ctx context.Context, cfg GCSReaderConfig) (ReaderFactory, error) {
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

	store := &GCSObjectStore{client: cfg.Client, bucket: cfg.Bucket}
	return NewObjectStoreReaderFactory(store, strings.Trim(cfg.Prefix, "/")), nil
}

func (g *GCSObjectStore) ListObjects(ctx context.Context, prefix string) ([]string, error) {
	var keys []string

	it := g.client.Bucket(g.bucket).Objects(ctx, &gcsstorage.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("list gcs objects with prefix %q: %w", prefix, err)
		}
		keys = append(keys, attrs.Name)
	}

	return keys, nil
}

func (g *GCSObjectStore) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	reader, err := g.client.Bucket(g.bucket).Object(key).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("read gcs object %q: %w", key, err)
	}
	return reader, nil
}
