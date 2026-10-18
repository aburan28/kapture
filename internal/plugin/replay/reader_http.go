package replay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"

	"github.com/kapture-io/kapture/internal/storage"
	"github.com/kapture-io/kapture/pkg/backend"
	"github.com/kapture-io/kapture/pkg/capture"
)

type HTTPReaderConfig struct {
	Endpoint   string
	Token      string
	HTTPClient *http.Client
}

type HTTPReader struct {
	client *backend.Client
	reader capture.Reader
	closed atomic.Bool
}

func NewHTTPReader(ctx context.Context, cfg HTTPReaderConfig) (*HTTPReader, error) {
	c, err := backend.NewClient(ctx, backend.Config{Endpoint: cfg.Endpoint, Token: cfg.Token, HTTPClient: cfg.HTTPClient})
	if err != nil {
		return nil, err
	}
	if err := c.Require(backend.ReadRequests); err != nil {
		return nil, err
	}
	return &HTTPReader{client: c}, nil
}

func (r *HTTPReader) Open(ctx context.Context, opts ReadOptions) error {
	if r.reader != nil || r.closed.Load() {
		return fmt.Errorf("HTTP reader already opened or closed")
	}
	var err error
	r.reader, err = r.client.OpenReplay(ctx, opts)
	return err
}

func (r *HTTPReader) Next(ctx context.Context) (*storage.CapturedRequest, error) {
	if r.reader == nil {
		return nil, fmt.Errorf("HTTP reader not opened")
	}
	if r.closed.Load() {
		return nil, ErrReaderDone
	}
	request, err := r.reader.Next(ctx)
	if errors.Is(err, io.EOF) {
		return nil, ErrReaderDone
	}
	return request, err
}

func (r *HTTPReader) Close() error {
	if r.closed.Swap(true) {
		return nil
	}
	if r.reader != nil {
		return r.reader.Close()
	}
	return nil
}
