package replay

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kapture-io/kapture/internal/plugin/payload"
	"github.com/kapture-io/kapture/internal/storage"
)

// RateMode controls how the engine paces replay requests.
type RateMode int

const (
	// RateModeConstant replays at a fixed requests-per-second rate.
	RateModeConstant RateMode = iota
	// RateModeOriginalTiming preserves the inter-request delays from the
	// original capture, optionally scaled by a multiplier.
	RateModeOriginalTiming
	// RateModeUnlimited replays as fast as the target can accept.
	RateModeUnlimited
)

// EngineConfig configures the replay Engine.
type EngineConfig struct {
	// Reader loads captured requests. Required.
	Reader Reader

	// Sender delivers requests to the target. Required.
	Sender Sender

	// Transformers are applied in order before sending. Optional.
	Transformers []Transformer

	// ResultHandler receives replay results. If nil, a default
	// in-memory handler is used.
	ResultHandler ResultHandler

	// ReadOptions configures which captured data to replay.
	ReadOptions ReadOptions

	// RateMode controls replay pacing. Defaults to RateModeUnlimited.
	RateMode RateMode

	// RatePerSecond is the target RPS when RateMode is RateModeConstant.
	RatePerSecond float64

	// TimeScale multiplies original inter-request delays when RateMode is
	// RateModeOriginalTiming. 1.0 = real-time, 0.5 = 2x speed, 2.0 = half speed.
	// Defaults to 1.0.
	TimeScale float64

	// Concurrency is the number of parallel senders. Defaults to 1.
	Concurrency int

	// ShardIndex and ShardCount split the capture across multiple engines
	// for distributed load tests. An engine with shard {index: i, count: n}
	// replays only requests whose hashed request ID modulo n equals i, so
	// n engines with the same count cover the capture exactly once.
	// ShardCount <= 1 disables sharding.
	ShardIndex int
	ShardCount int

	// Logger for engine operations. Uses slog.Default() if nil.
	Logger *slog.Logger
}

// Engine orchestrates a traffic replay session. It reads from a Reader,
// applies Transformers, and dispatches to a Sender with configurable
// rate limiting and concurrency.
type Engine struct {
	reader        Reader
	sender        Sender
	transformers  []Transformer
	resultHandler ResultHandler
	readOpts      ReadOptions
	rateMode      RateMode
	ratePerSecond float64
	timeScale     float64
	concurrency   int
	shardIndex    uint64
	shardCount    uint64
	log           *slog.Logger

	// Runtime state
	sent         atomic.Int64
	errors       atomic.Int64
	filtered     atomic.Int64
	shardSkipped atomic.Int64
	running      atomic.Bool
}

// NewEngine creates a replay Engine from the given config.
func NewEngine(cfg EngineConfig) (*Engine, error) {
	if cfg.Reader == nil {
		return nil, errors.New("reader is required")
	}
	if cfg.Sender == nil {
		return nil, errors.New("sender is required")
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.TimeScale <= 0 {
		cfg.TimeScale = 1.0
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.ResultHandler == nil {
		cfg.ResultHandler = NewDefaultResultHandler(cfg.Logger)
	}
	if cfg.ShardCount > 1 {
		if cfg.ShardIndex < 0 || cfg.ShardIndex >= cfg.ShardCount {
			return nil, fmt.Errorf("shard index %d out of range [0, %d)", cfg.ShardIndex, cfg.ShardCount)
		}
	} else {
		cfg.ShardIndex = 0
		cfg.ShardCount = 1
	}

	return &Engine{
		reader:        cfg.Reader,
		sender:        cfg.Sender,
		transformers:  cfg.Transformers,
		resultHandler: cfg.ResultHandler,
		readOpts:      cfg.ReadOptions,
		rateMode:      cfg.RateMode,
		ratePerSecond: cfg.RatePerSecond,
		timeScale:     cfg.TimeScale,
		concurrency:   cfg.Concurrency,
		shardIndex:    uint64(cfg.ShardIndex),
		shardCount:    uint64(cfg.ShardCount),
		log:           cfg.Logger,
	}, nil
}

// Run executes the replay. It blocks until all requests are replayed, an error
// occurs, or the context is cancelled. Returns a Summary of the replay session.
func (e *Engine) Run(ctx context.Context) (*Summary, error) {
	if !e.running.CompareAndSwap(false, true) {
		return nil, errors.New("engine is already running")
	}
	defer e.running.Store(false)

	if err := e.reader.Open(ctx, e.readOpts); err != nil {
		return nil, fmt.Errorf("open reader: %w", err)
	}

	// Feed requests into a channel from the reader.
	reqCh := make(chan *storage.CapturedRequest, e.concurrency*2)
	readErr := make(chan error, 1)

	go func() {
		defer close(reqCh)
		readErr <- e.readLoop(ctx, reqCh)
	}()

	// Fan out to concurrent senders.
	var wg sync.WaitGroup
	for i := 0; i < e.concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.sendLoop(ctx, reqCh)
		}()
	}

	wg.Wait()

	// Check for reader errors.
	if err := <-readErr; err != nil {
		return nil, fmt.Errorf("reader: %w", err)
	}

	summary, err := e.resultHandler.Summarize(ctx)
	if err != nil {
		return nil, err
	}
	// FilteredCount is tracked by the engine (readLoop), not the result
	// handler, because filtered requests never reach the sender.
	summary.FilteredCount = e.filtered.Load()
	return summary, nil
}

// Progress returns current replay progress counters.
func (e *Engine) Progress() (sent, errored, filtered int64) {
	return e.sent.Load(), e.errors.Load(), e.filtered.Load()
}

// ShardSkipped returns how many requests were skipped because they belong
// to other shards.
func (e *Engine) ShardSkipped() int64 {
	return e.shardSkipped.Load()
}

// shardOf deterministically assigns a request ID to a shard using FNV-1a.
// Every engine must use the same function so shards are disjoint and
// exhaustive across workers.
func shardOf(requestID string, shardCount uint64) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(requestID))
	return h.Sum64() % shardCount
}

func (e *Engine) readLoop(ctx context.Context, out chan<- *storage.CapturedRequest) error {
	var prevTimestamp time.Time
	var limiter *time.Ticker

	if e.rateMode == RateModeConstant && e.ratePerSecond > 0 {
		interval := time.Duration(float64(time.Second) / e.ratePerSecond)
		limiter = time.NewTicker(interval)
		defer limiter.Stop()
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		req, err := e.reader.Next(ctx)
		if errors.Is(err, ErrReaderDone) {
			return nil
		}
		if err != nil {
			return err
		}

		// Shard filter: skip requests owned by other shards. Skipped
		// requests do not advance the original-timing clock, so each shard
		// reproduces the recorded timeline of its own subset and all shards
		// together reproduce the recorded aggregate QPS.
		if e.shardCount > 1 && shardOf(req.ID, e.shardCount) != e.shardIndex {
			e.shardSkipped.Add(1)
			continue
		}

		// Apply transformers.
		current := req
		for _, t := range e.transformers {
			current, err = t.Transform(ctx, current)
			if err != nil {
				e.log.Warn("transformer error, skipping request",
					"transformer", t.Name(), "error", err, "requestID", req.ID)
				current = nil
				break
			}
			if current == nil {
				break
			}
		}

		if current == nil {
			e.filtered.Add(1)
			continue
		}

		// Rate limiting.
		switch e.rateMode {
		case RateModeConstant:
			if limiter != nil {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-limiter.C:
				}
			}
		case RateModeOriginalTiming:
			if !prevTimestamp.IsZero() && !req.Timestamp.IsZero() {
				delay := req.Timestamp.Sub(prevTimestamp)
				if delay > 0 {
					scaledDelay := time.Duration(float64(delay) * e.timeScale)
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(scaledDelay):
					}
				}
			}
			prevTimestamp = req.Timestamp
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- current:
		}
	}
}

func (e *Engine) sendLoop(ctx context.Context, in <-chan *storage.CapturedRequest) {
	for req := range in {
		select {
		case <-ctx.Done():
			return
		default:
		}

		result, err := e.sender.Send(ctx, req)
		if err != nil {
			e.errors.Add(1)
			// Use the sender's Result if available (preserves Duration, etc.),
			// otherwise create a minimal one.
			if result == nil {
				result = &Result{
					RequestID: req.ID,
					Error:     err,
					Timestamp: time.Now().UTC(),
				}
			}
		} else {
			e.sent.Add(1)
		}

		if handleErr := e.resultHandler.HandleResult(ctx, result); handleErr != nil {
			e.log.Warn("result handler error", "error", handleErr, "requestID", req.ID)
		}
	}
}

// ErrReaderDone is a sentinel error returned by Reader.Next to indicate
// that all captured requests have been read. Readers should return this
// (or io.EOF) rather than a generic error to signal normal completion.
var ErrReaderDone = errors.New("reader: no more requests")

// TransformerFunc adapts a plain function into the Transformer interface.
// Useful for simple, stateless transformations.
type TransformerFunc struct {
	name string
	fn   func(context.Context, *storage.CapturedRequest) (*storage.CapturedRequest, error)
}

// NewTransformerFunc creates a Transformer from a function.
func NewTransformerFunc(name string, fn func(context.Context, *storage.CapturedRequest) (*storage.CapturedRequest, error)) Transformer {
	return &TransformerFunc{name: name, fn: fn}
}

func (t *TransformerFunc) Name() string { return t.name }
func (t *TransformerFunc) Transform(ctx context.Context, req *storage.CapturedRequest) (*storage.CapturedRequest, error) {
	return t.fn(ctx, req)
}

// HeaderRewriter is a Transformer that adds, removes, or overwrites
// HTTP headers on replayed requests. This is the most common transformation
// for replay scenarios (e.g., replacing auth tokens, adding trace headers).
type HeaderRewriter struct {
	Set    map[string]string // Headers to set (overwrites existing).
	Add    map[string]string // Headers to add (appends to existing).
	Remove []string          // Header names to remove.
}

func (h *HeaderRewriter) Name() string { return "header-rewriter" }

func (h *HeaderRewriter) Transform(_ context.Context, req *storage.CapturedRequest) (*storage.CapturedRequest, error) {
	cp := payload.CopyRequest(req)

	if cp.Headers == nil {
		cp.Headers = make(map[string][]string)
	}

	for _, name := range h.Remove {
		delete(cp.Headers, name)
	}
	for name, value := range h.Set {
		cp.Headers[name] = []string{value}
	}
	for name, value := range h.Add {
		cp.Headers[name] = append(cp.Headers[name], value)
	}

	return cp, nil
}
