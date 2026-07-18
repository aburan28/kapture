package agent

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kapture-io/kapture/internal/storage"
)

// DefaultWriteQueueSize bounds how many captured requests may wait for the
// storage backend before new captures are dropped.
const DefaultWriteQueueSize = 4096

// ErrWriteQueueFull is returned when the storage backend cannot keep up
// and the bounded write queue is at capacity. The capture is dropped —
// mirrored traffic must never be stalled by storage.
var ErrWriteQueueFull = errors.New("capture write queue is full")

// AsyncWriter decouples the capture hot path from storage latency. Handle
// enqueues onto a bounded queue and returns immediately; a single drain
// goroutine performs the actual (potentially slow, mutex-serialised,
// flush-blocking) storage writes. When storage stalls long enough to fill
// the queue, new captures are dropped and counted rather than blocking
// mirrored traffic or growing memory without bound.
type AsyncWriter struct {
	inner storage.Writer
	queue chan *storage.CapturedRequest
	log   *slog.Logger

	drained   chan struct{} // closed when the drain goroutine exits
	closeOnce sync.Once
	closed    atomic.Bool

	enqueued    atomic.Int64
	dropped     atomic.Int64
	writeErrors atomic.Int64
}

// NewAsyncWriter starts the drain goroutine. queueSize <= 0 uses
// DefaultWriteQueueSize.
func NewAsyncWriter(inner storage.Writer, queueSize int, log *slog.Logger) *AsyncWriter {
	if queueSize <= 0 {
		queueSize = DefaultWriteQueueSize
	}
	if log == nil {
		log = slog.Default()
	}
	w := &AsyncWriter{
		inner:   inner,
		queue:   make(chan *storage.CapturedRequest, queueSize),
		log:     log,
		drained: make(chan struct{}),
	}
	go w.drain()
	return w
}

// Write enqueues the request for background storage. It never blocks: a
// full queue drops the request and returns ErrWriteQueueFull.
func (w *AsyncWriter) Write(_ context.Context, req *storage.CapturedRequest) error {
	if w.closed.Load() {
		return storage.ErrWriterClosed
	}
	select {
	case w.queue <- req:
		w.enqueued.Add(1)
		return nil
	default:
		w.dropped.Add(1)
		return ErrWriteQueueFull
	}
}

// Flush waits for the queue to drain, then flushes the underlying writer.
// The context bounds the wait.
func (w *AsyncWriter) Flush(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for len(w.queue) > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-w.drained:
			// Drain goroutine exited (Close already ran); inner flush below.
		case <-ticker.C:
		}
	}
	return w.inner.Flush(ctx)
}

// Close stops accepting writes, drains everything already queued, and
// closes the underlying writer.
func (w *AsyncWriter) Close() error {
	var err error
	w.closeOnce.Do(func() {
		w.closed.Store(true)
		close(w.queue)
		<-w.drained
		err = w.inner.Close()
	})
	return err
}

// QueueDepth reports how many captures are waiting for storage.
func (w *AsyncWriter) QueueDepth() int { return len(w.queue) }

// QueueCapacity reports the bounded queue size.
func (w *AsyncWriter) QueueCapacity() int { return cap(w.queue) }

// Dropped reports captures dropped because the queue was full.
func (w *AsyncWriter) Dropped() int64 { return w.dropped.Load() }

// WriteErrors reports storage write failures from the drain goroutine.
func (w *AsyncWriter) WriteErrors() int64 { return w.writeErrors.Load() }

func (w *AsyncWriter) drain() {
	defer close(w.drained)
	var lastErrLog time.Time
	for req := range w.queue {
		// Detached context: request contexts are cancelled when the
		// mirrored HTTP request finishes, long before this write runs.
		if err := w.inner.Write(context.Background(), req); err != nil {
			w.writeErrors.Add(1)
			// Storage outages affect every queued request; log at most
			// once a second instead of once per drop.
			if time.Since(lastErrLog) >= time.Second {
				lastErrLog = time.Now()
				w.log.Warn("storage write failed, capture lost",
					"error", err, "writeErrors", w.writeErrors.Load())
			}
		}
	}
}
