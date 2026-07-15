// Package agent implements the capture agent that receives mirrored traffic
// and writes captured request data to pluggable storage backends.
package agent

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/kapture-io/kapture/internal/storage"
)

// CapturedHTTPRequest holds the raw data extracted from an incoming mirrored request.
type CapturedHTTPRequest struct {
	Method   string
	Path     string
	Headers  map[string][]string
	Body     io.Reader
	Protocol string
	Metadata map[string]string
}

// CaptureHandler receives captured requests, enforces limits, and sends them to a writer.
type CaptureHandler struct {
	writer       storage.Writer
	maxBodyBytes int64
	filter       *RequestFilter
	redactor     *HeaderRedactor
	log          *slog.Logger

	// Metrics
	requestsTotal    atomic.Int64
	requestsFiltered atomic.Int64
	requestsDropped  atomic.Int64
	bytesReceived    atomic.Int64
}

// CaptureHandlerConfig configures a CaptureHandler.
type CaptureHandlerConfig struct {
	Writer       storage.Writer
	MaxBodyBytes int64
	// Filter optionally narrows which requests are captured; nil captures all.
	Filter *RequestFilter
	// RedactHeaders lists headers whose values are replaced with
	// RedactedValue before storage. Nil applies DefaultRedactHeaders; an
	// explicit empty (non-nil) slice disables redaction.
	RedactHeaders []string
	Logger        *slog.Logger
}

// NewCaptureHandler creates a CaptureHandler with the given configuration.
func NewCaptureHandler(cfg CaptureHandlerConfig) *CaptureHandler {
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = 1 << 20 // 1MB default
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.RedactHeaders == nil {
		cfg.RedactHeaders = DefaultRedactHeaders
	}
	return &CaptureHandler{
		writer:       cfg.Writer,
		maxBodyBytes: cfg.MaxBodyBytes,
		filter:       cfg.Filter,
		redactor:     NewHeaderRedactor(cfg.RedactHeaders),
		log:          cfg.Logger,
	}
}

// Handle processes a single captured request. It enforces the max body size,
// converts to a CapturedRequest, and writes it to storage. If the write fails,
// the request is dropped and a metric counter is incremented.
func (h *CaptureHandler) Handle(ctx context.Context, req *CapturedHTTPRequest) error {
	h.requestsTotal.Add(1)

	if !h.filter.Matches(req) {
		h.requestsFiltered.Add(1)
		return nil
	}

	body, err := io.ReadAll(io.LimitReader(req.Body, h.maxBodyBytes+1))
	if err != nil {
		h.requestsDropped.Add(1)
		h.log.Warn("failed to read request body", "error", err)
		return err
	}

	truncated := int64(len(body)) > h.maxBodyBytes
	if truncated {
		body = body[:h.maxBodyBytes]
	}

	h.bytesReceived.Add(int64(len(body)))

	// Credential headers must never become durable capture data. Filters
	// have already run, so header-based filtering still sees originals.
	h.redactor.Redact(req.Headers)

	captured := &storage.CapturedRequest{
		ID:            uuid.New().String(),
		Timestamp:     time.Now().UTC(),
		Method:        req.Method,
		Path:          req.Path,
		Headers:       req.Headers,
		Body:          body,
		ContentLength: int64(len(body)),
		Protocol:      req.Protocol,
		Metadata:      req.Metadata,
	}

	if truncated {
		if captured.Metadata == nil {
			captured.Metadata = make(map[string]string)
		}
		captured.Metadata["bodyTruncated"] = "true"
	}

	if err := h.writer.Write(ctx, captured); err != nil {
		h.requestsDropped.Add(1)
		h.log.Warn("failed to write captured request, dropping", "error", err, "path", req.Path)
		return err
	}

	return nil
}

// Metrics returns current handler metrics.
func (h *CaptureHandler) Metrics() (total, filtered, dropped, bytesRecv int64) {
	return h.requestsTotal.Load(), h.requestsFiltered.Load(), h.requestsDropped.Load(), h.bytesReceived.Load()
}
