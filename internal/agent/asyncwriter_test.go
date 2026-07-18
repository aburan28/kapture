package agent

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kapture-io/kapture/internal/storage"
)

// blockableWriter is a storage.Writer whose Write can be stalled.
type blockableWriter struct {
	mu        sync.Mutex
	written   []*storage.CapturedRequest
	block     chan struct{} // when non-nil, Write blocks until closed
	entered   chan struct{} // when non-nil, closed once on first Write entry
	enterOnce sync.Once
	failAll   atomic.Bool
	flushed   atomic.Int64
	closed    atomic.Bool
}

func (w *blockableWriter) Write(_ context.Context, req *storage.CapturedRequest) error {
	if w.entered != nil {
		w.enterOnce.Do(func() { close(w.entered) })
	}
	if w.block != nil {
		<-w.block
	}
	if w.failAll.Load() {
		return errors.New("backend down")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.written = append(w.written, req)
	return nil
}

func (w *blockableWriter) Flush(context.Context) error { w.flushed.Add(1); return nil }
func (w *blockableWriter) Close() error                { w.closed.Store(true); return nil }

func (w *blockableWriter) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.written)
}

func req(id string) *storage.CapturedRequest {
	return &storage.CapturedRequest{ID: id, Timestamp: time.Now(), Method: "GET", Path: "/", Protocol: "HTTP"}
}

func TestAsyncWriter_WritesFlowThrough(t *testing.T) {
	inner := &blockableWriter{}
	w := NewAsyncWriter(inner, 16, nil)

	for i := 0; i < 10; i++ {
		if err := w.Write(context.Background(), req("r")); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got := inner.count(); got != 10 {
		t.Errorf("inner received %d writes, want 10", got)
	}
	if !inner.closed.Load() {
		t.Error("inner writer not closed")
	}
	if w.Dropped() != 0 {
		t.Errorf("dropped = %d, want 0", w.Dropped())
	}
}

// TestAsyncWriter_StalledStorageDropsInsteadOfBlocking is the backpressure
// contract: with the backend stalled and the queue full, Write returns
// ErrWriteQueueFull immediately instead of blocking mirrored traffic.
func TestAsyncWriter_StalledStorageDropsInsteadOfBlocking(t *testing.T) {
	inner := &blockableWriter{block: make(chan struct{}), entered: make(chan struct{})}
	w := NewAsyncWriter(inner, 4, nil)

	// First write is pulled by the drain goroutine, which then stalls
	// inside storage; wait for that so the fill below is deterministic.
	if err := w.Write(context.Background(), req("r")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-inner.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("drain goroutine never reached storage")
	}

	// Fill the queue to capacity behind the stalled write.
	filled := 1
	for i := 0; i < 4; i++ {
		if err := w.Write(context.Background(), req("r")); err != nil {
			t.Fatalf("fill write %d: %v", i, err)
		}
		filled++
	}

	done := make(chan error, 1)
	go func() { done <- w.Write(context.Background(), req("overflow")) }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrWriteQueueFull) {
			t.Fatalf("overflow write returned %v, want ErrWriteQueueFull", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Write blocked on a full queue; capture path must never block on storage")
	}
	if w.Dropped() == 0 {
		t.Error("dropped counter not incremented")
	}

	// Unblock storage: everything queued must still land.
	close(inner.block)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got := inner.count(); got != filled {
		t.Errorf("inner received %d writes after unblocking, want %d", got, filled)
	}
}

func TestAsyncWriter_CountsStorageErrors(t *testing.T) {
	inner := &blockableWriter{}
	inner.failAll.Store(true)
	w := NewAsyncWriter(inner, 8, nil)

	for i := 0; i < 5; i++ {
		_ = w.Write(context.Background(), req("r"))
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got := w.WriteErrors(); got != 5 {
		t.Errorf("writeErrors = %d, want 5", got)
	}
}

func TestAsyncWriter_WriteAfterCloseRejected(t *testing.T) {
	w := NewAsyncWriter(&blockableWriter{}, 4, nil)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Write(context.Background(), req("late")); !errors.Is(err, storage.ErrWriterClosed) {
		t.Fatalf("write after close returned %v, want ErrWriterClosed", err)
	}
	// Close is idempotent.
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAsyncWriter_FlushDrainsQueue(t *testing.T) {
	inner := &blockableWriter{}
	w := NewAsyncWriter(inner, 64, nil)
	defer w.Close()

	for i := 0; i < 20; i++ {
		if err := w.Write(context.Background(), req("r")); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if inner.flushed.Load() == 0 {
		t.Error("inner Flush never called")
	}
}
