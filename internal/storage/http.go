package storage

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/kapture-io/kapture/pkg/backend"
)

type HTTPConfig struct {
	Endpoint         string           `json:"endpoint"`
	Token            string           `json:"-"`
	HTTPClient       *http.Client     `json:"-"`
	ArtifactRecorder ArtifactRecorder `json:"-"`
	CaptureNamespace string           `json:"-"`
	CaptureName      string           `json:"-"`
}

func (HTTPConfig) isStorageConfig() {}

type HTTPWriterFactory struct {
	client *backend.Client
	config HTTPConfig
}

func NewHTTPWriterFactory(ctx context.Context, cfg HTTPConfig) (*HTTPWriterFactory, error) {
	c, err := backend.NewClient(ctx, backend.Config{Endpoint: cfg.Endpoint, Token: cfg.Token, HTTPClient: cfg.HTTPClient})
	if err != nil {
		return nil, err
	}
	if err := c.Require(backend.WriteRequests); err != nil {
		return nil, err
	}
	return &HTTPWriterFactory{client: c, config: cfg}, nil
}

func (f *HTTPWriterFactory) NewWriter(_ context.Context, id string) (Writer, error) {
	if id == "" {
		return nil, fmt.Errorf("capture ID is required")
	}
	return &httpWriter{client: f.client, captureID: id}, nil
}

// ValidateArtifacts makes unsupported evidence fail before attaching probes.
// Request-only destinations remain usable by the default Mirror backend.
func (f *HTTPWriterFactory) ValidateArtifacts() error {
	return f.client.Require(backend.WriteArtifacts)
}

func (f *HTTPWriterFactory) PutArtifact(ctx context.Context, id, name string, data []byte, typ string) (ArtifactRecord, error) {
	r, err := f.client.PutArtifact(ctx, id, name, data, typ)
	if err != nil {
		return r, err
	}
	if r.StorageBackend == "" {
		r.StorageBackend = "http"
	}
	r.CaptureNamespace, r.CaptureName = f.config.CaptureNamespace, f.config.CaptureName
	if f.config.ArtifactRecorder != nil {
		if err := f.config.ArtifactRecorder.RecordArtifact(ctx, r); err != nil {
			return r, fmt.Errorf("record uploaded HTTP artifact metadata: %w", err)
		}
	}
	return r, nil
}

type httpWriter struct {
	mu        sync.Mutex
	client    *backend.Client
	captureID string
	closed    bool
}

func (w *httpWriter) Write(ctx context.Context, r *CapturedRequest) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrWriterClosed
	}
	return w.client.PutRequest(ctx, w.captureID, r)
}

// Each successful HTTP write is already acknowledged durably by the backend.
func (w *httpWriter) Flush(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrWriterClosed
	}
	return ctx.Err()
}
func (w *httpWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	return nil
}
