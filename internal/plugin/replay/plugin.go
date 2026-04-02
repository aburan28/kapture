// Package replay provides an extensible plugin model for replaying captured
// traffic to target services. The model separates three concerns:
//
//   - Reading: A [Reader] loads captured requests from a data source
//   - Transforming: A [Transformer] modifies requests before replay
//   - Sending: A [Sender] delivers requests to a target and reports results
//
// These compose in the [Engine], which orchestrates the full replay lifecycle
// including rate limiting, concurrency control, and progress reporting.
//
// # Interface Design Rationale
//
// Reader, Transformer, and Sender are separate interfaces rather than a single
// monolithic plugin because:
//   - They have independent lifecycles (a Reader might outlive many Senders)
//   - They're independently testable and swappable
//   - Real use cases mix and match (same Reader, different Senders for A/B)
//   - Transformers are optional and chainable
//
// All interfaces are designed to be safe for concurrent use unless documented
// otherwise (Reader is sequential by design — see its docs).
package replay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/kapture-io/kapture/internal/storage"
)

// Reader loads captured requests from a data source. Implementations read from
// storage backends (S3, GCS, filesystem, etc.) and yield CapturedRequests one
// at a time.
//
// Reader is inherently sequential — callers must not call Next concurrently.
// This matches the natural ordering of captured traffic and simplifies
// implementation. Parallelism is handled at the Engine level.
type Reader interface {
	// Open initializes the reader with the given options.
	// Must be called before Next.
	Open(ctx context.Context, opts ReadOptions) error

	// Next returns the next captured request. Returns io.EOF when all
	// requests have been read. After io.EOF, no further calls to Next
	// should be made.
	Next(ctx context.Context) (*storage.CapturedRequest, error)

	// Close releases resources. Safe to call multiple times.
	Close() error
}

// ReadOptions configures what data a Reader should load.
type ReadOptions struct {
	// CaptureID identifies the capture session to replay (e.g., "namespace/name").
	CaptureID string

	// StartTime filters requests to those captured at or after this time.
	// Zero value means no lower bound.
	StartTime time.Time

	// EndTime filters requests to those captured before this time.
	// Zero value means no upper bound.
	EndTime time.Time

	// PathPrefix filters requests to those whose path starts with this prefix.
	PathPrefix string

	// Methods filters to specific HTTP methods (GET, POST, etc.).
	// Empty means all methods.
	Methods []string

	// Limit caps the total number of requests to read. Zero means unlimited.
	Limit int64
}

// Sender delivers a captured request to a target service and reports the result.
// Implementations handle the actual HTTP/gRPC call to the target.
//
// Sender must be safe for concurrent use — the Engine may call Send from
// multiple goroutines for parallel replay.
type Sender interface {
	// Send replays a single captured request to the target and returns the
	// result. The Sender is responsible for constructing the outbound
	// request from the CapturedRequest fields.
	Send(ctx context.Context, req *storage.CapturedRequest) (*Result, error)

	// Close releases resources (e.g., HTTP connection pools).
	Close() error
}

// Result captures the outcome of a single replayed request.
type Result struct {
	// RequestID is the original CapturedRequest.ID.
	RequestID string

	// StatusCode is the HTTP status code from the target (0 if connection failed).
	StatusCode int

	// ResponseHeaders from the target.
	ResponseHeaders http.Header

	// ResponseBody is the (potentially truncated) response body.
	ResponseBody []byte

	// Duration is the round-trip time for the replay request.
	Duration time.Duration

	// Error is non-nil if the request could not be delivered.
	Error error

	// Timestamp is when the replay completed.
	Timestamp time.Time
}

// Transformer modifies a captured request before it is sent to the target.
// Common uses include rewriting URLs, modifying headers, or masking data.
//
// Transformers are optional and chainable. They follow the same contract as
// payload.Plugin.Process: return nil to skip the request, return a modified
// copy to transform it.
//
// Transformer must be safe for concurrent use.
type Transformer interface {
	// Name identifies this transformer for logging and diagnostics.
	Name() string

	// Transform returns a (possibly modified) request, or nil to skip it.
	Transform(ctx context.Context, req *storage.CapturedRequest) (*storage.CapturedRequest, error)
}

// ResultHandler receives replay results for observation, logging, or
// aggregation. It decouples result processing from the replay engine,
// allowing custom reporting without modifying engine code.
//
// ResultHandler must be safe for concurrent use.
type ResultHandler interface {
	// HandleResult processes a single replay result. Implementations should
	// not block for long — the engine calls this synchronously per result.
	HandleResult(ctx context.Context, result *Result) error

	// Summarize returns a final summary after all results are processed.
	// Called once after the replay completes.
	Summarize(ctx context.Context) (*Summary, error)
}

// Summary aggregates replay results for reporting.
type Summary struct {
	TotalRequests   int64
	SuccessCount    int64
	ErrorCount      int64
	FilteredCount   int64
	MeanLatency     time.Duration
	P50Latency      time.Duration
	P95Latency      time.Duration
	P99Latency      time.Duration
	StatusCodeDist  map[int]int64
	StartTime       time.Time
	EndTime         time.Time
	TotalDuration   time.Duration
	BytesSent       int64
	BytesReceived   int64
	ErrorsByType    map[string]int64
}

// Registry is a named collection of plugin constructors for replay components.
type Registry struct {
	mu           sync.RWMutex
	readers      map[string]ReaderBuilder
	senders      map[string]SenderBuilder
	transformers map[string]TransformerBuilder
	handlers     map[string]ResultHandlerBuilder
}

// Builder function types for each component.
type (
	ReaderBuilder        func(ctx context.Context, config map[string]any) (Reader, error)
	SenderBuilder        func(ctx context.Context, config map[string]any) (Sender, error)
	TransformerBuilder   func(ctx context.Context, config map[string]any) (Transformer, error)
	ResultHandlerBuilder func(ctx context.Context, config map[string]any) (ResultHandler, error)
)

// NewRegistry creates an empty replay plugin registry.
func NewRegistry() *Registry {
	return &Registry{
		readers:      make(map[string]ReaderBuilder),
		senders:      make(map[string]SenderBuilder),
		transformers: make(map[string]TransformerBuilder),
		handlers:     make(map[string]ResultHandlerBuilder),
	}
}

// RegisterReader adds a Reader builder under the given name.
func (r *Registry) RegisterReader(name string, builder ReaderBuilder) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.readers[name]; exists {
		return fmt.Errorf("replay reader %q is already registered", name)
	}
	r.readers[name] = builder
	return nil
}

// RegisterSender adds a Sender builder under the given name.
func (r *Registry) RegisterSender(name string, builder SenderBuilder) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.senders[name]; exists {
		return fmt.Errorf("replay sender %q is already registered", name)
	}
	r.senders[name] = builder
	return nil
}

// RegisterTransformer adds a Transformer builder under the given name.
func (r *Registry) RegisterTransformer(name string, builder TransformerBuilder) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.transformers[name]; exists {
		return fmt.Errorf("replay transformer %q is already registered", name)
	}
	r.transformers[name] = builder
	return nil
}

// RegisterResultHandler adds a ResultHandler builder under the given name.
func (r *Registry) RegisterResultHandler(name string, builder ResultHandlerBuilder) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.handlers[name]; exists {
		return fmt.Errorf("replay result handler %q is already registered", name)
	}
	r.handlers[name] = builder
	return nil
}

// BuildReader creates a Reader instance by name with the given config.
func (r *Registry) BuildReader(ctx context.Context, name string, config map[string]any) (Reader, error) {
	r.mu.RLock()
	builder, ok := r.readers[name]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown replay reader %q", name)
	}
	return builder(ctx, config)
}

// BuildSender creates a Sender instance by name with the given config.
func (r *Registry) BuildSender(ctx context.Context, name string, config map[string]any) (Sender, error) {
	r.mu.RLock()
	builder, ok := r.senders[name]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown replay sender %q", name)
	}
	return builder(ctx, config)
}

// BuildTransformer creates a Transformer instance by name with the given config.
func (r *Registry) BuildTransformer(ctx context.Context, name string, config map[string]any) (Transformer, error) {
	r.mu.RLock()
	builder, ok := r.transformers[name]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown replay transformer %q", name)
	}
	return builder(ctx, config)
}

// BuildResultHandler creates a ResultHandler instance by name with the given config.
func (r *Registry) BuildResultHandler(ctx context.Context, name string, config map[string]any) (ResultHandler, error) {
	r.mu.RLock()
	builder, ok := r.handlers[name]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown replay result handler %q", name)
	}
	return builder(ctx, config)
}

// defaultResultHandler is a basic implementation that collects results in memory.
type defaultResultHandler struct {
	mu      sync.Mutex
	results []*Result
	log     *slog.Logger
}

// NewDefaultResultHandler creates a ResultHandler that accumulates results
// in memory and computes summary statistics.
func NewDefaultResultHandler(log *slog.Logger) ResultHandler {
	if log == nil {
		log = slog.Default()
	}
	return &defaultResultHandler{log: log}
}

func (h *defaultResultHandler) HandleResult(_ context.Context, result *Result) error {
	h.mu.Lock()
	h.results = append(h.results, result)
	h.mu.Unlock()
	return nil
}

func (h *defaultResultHandler) Summarize(_ context.Context) (*Summary, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	s := &Summary{
		StatusCodeDist: make(map[int]int64),
		ErrorsByType:   make(map[string]int64),
	}

	if len(h.results) == 0 {
		return s, nil
	}

	s.TotalRequests = int64(len(h.results))
	latencies := make([]time.Duration, 0, len(h.results))

	for _, r := range h.results {
		if r.Error != nil {
			s.ErrorCount++
			s.ErrorsByType[errorCategory(r.Error)]++
		} else {
			s.SuccessCount++
		}
		if r.StatusCode > 0 {
			s.StatusCodeDist[r.StatusCode]++
		}
		latencies = append(latencies, r.Duration)
		s.BytesSent += int64(len(r.ResponseBody))

		if s.StartTime.IsZero() || r.Timestamp.Before(s.StartTime) {
			s.StartTime = r.Timestamp
		}
		if r.Timestamp.After(s.EndTime) {
			s.EndTime = r.Timestamp
		}
	}

	s.TotalDuration = s.EndTime.Sub(s.StartTime)
	s.MeanLatency = meanDuration(latencies)
	s.P50Latency = percentileDuration(latencies, 0.50)
	s.P95Latency = percentileDuration(latencies, 0.95)
	s.P99Latency = percentileDuration(latencies, 0.99)

	return s, nil
}

func errorCategory(err error) string {
	if err == nil {
		return ""
	}
	var netErr interface{ Timeout() bool }
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	if errors.Is(err, io.EOF) {
		return "connection_closed"
	}
	return "unknown"
}

func meanDuration(ds []time.Duration) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	var total time.Duration
	for _, d := range ds {
		total += d
	}
	return total / time.Duration(len(ds))
}

func percentileDuration(ds []time.Duration, p float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	// Simple linear interpolation on unsorted data — acceptable for summaries.
	// For production use with large datasets, a streaming quantile sketch
	// (e.g., t-digest) would be more appropriate.
	sorted := make([]time.Duration, len(ds))
	copy(sorted, ds)
	sortDurations(sorted)

	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func sortDurations(ds []time.Duration) {
	// Simple insertion sort — result sets are typically bounded.
	for i := 1; i < len(ds); i++ {
		key := ds[i]
		j := i - 1
		for j >= 0 && ds[j] > key {
			ds[j+1] = ds[j]
			j--
		}
		ds[j+1] = key
	}
}
