package stats

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Sink receives published statistics. The Redis implementation lives in
// redis.go; tests use fakes.
type Sink interface {
	// PublishSnapshot stores the latest full snapshot under key.
	PublishSnapshot(ctx context.Context, key string, payload []byte, ttl time.Duration) error
	// AppendWindow appends one completed flow window to a stream.
	AppendWindow(ctx context.Context, stream string, payload []byte) error
	Close() error
}

// DefaultPublishInterval is how often the latest snapshot is pushed.
const DefaultPublishInterval = 15 * time.Second

// Publisher periodically pushes Collector snapshots to a Sink and
// forwards each completed flow window exactly once. Publishing is
// best-effort: sink errors are logged and never slow the capture path.
type Publisher struct {
	collector *Collector
	sink      Sink
	captureID string
	interval  time.Duration
	log       *slog.Logger

	mu      sync.Mutex
	pending []*WindowCounts
	errors  uint64
}

// NewPublisher wires a collector to a sink. The publisher registers
// itself as the collector's window-roll callback; construct it before
// traffic flows.
func NewPublisher(collector *Collector, sink Sink, captureID string, interval time.Duration, log *slog.Logger) *Publisher {
	if interval <= 0 {
		interval = DefaultPublishInterval
	}
	if log == nil {
		log = slog.Default()
	}
	p := &Publisher{
		collector: collector,
		sink:      sink,
		captureID: captureID,
		interval:  interval,
		log:       log,
	}
	collector.Windows.OnRoll = p.enqueueWindow
	return p
}

// SnapshotKey is the Redis key holding the latest snapshot for a capture.
func SnapshotKey(captureID string) string { return "kapture:stats:" + captureID }

// WindowStream is the Redis stream receiving completed flow windows.
func WindowStream(captureID string) string { return "kapture:stats:" + captureID + ":windows" }

func (p *Publisher) enqueueWindow(w *WindowCounts) {
	p.mu.Lock()
	p.pending = append(p.pending, w)
	// Bound memory if the sink is down for a long time.
	if len(p.pending) > keptWindows {
		p.pending = p.pending[len(p.pending)-keptWindows:]
	}
	p.mu.Unlock()
}

// Run publishes until ctx is cancelled, then makes a final best-effort
// flush and closes the sink.
func (p *Publisher) Run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			p.publish(flushCtx)
			cancel()
			_ = p.sink.Close()
			return
		case <-ticker.C:
			p.publish(ctx)
		}
	}
}

// publish sends queued windows and the current snapshot.
func (p *Publisher) publish(ctx context.Context) {
	p.mu.Lock()
	pending := p.pending
	p.pending = nil
	p.mu.Unlock()

	requeue := pending[:0]
	for _, w := range pending {
		payload, err := json.Marshal(w)
		if err != nil {
			continue
		}
		if err := p.sink.AppendWindow(ctx, WindowStream(p.captureID), payload); err != nil {
			p.noteError("append window", err)
			requeue = append(requeue, w)
		}
	}
	if len(requeue) > 0 {
		p.mu.Lock()
		p.pending = append(requeue, p.pending...)
		p.mu.Unlock()
	}

	snapshot := p.collector.Snapshot()
	payload, err := json.Marshal(snapshot)
	if err != nil {
		p.noteError("marshal snapshot", err)
		return
	}
	// TTL of three intervals: the key disappears shortly after the
	// capture stops publishing instead of going permanently stale.
	if err := p.sink.PublishSnapshot(ctx, SnapshotKey(p.captureID), payload, 3*p.interval); err != nil {
		p.noteError("publish snapshot", err)
	}
}

func (p *Publisher) noteError(op string, err error) {
	p.mu.Lock()
	p.errors++
	count := p.errors
	p.mu.Unlock()
	// Log the first error and then every 20th to avoid flooding while
	// the sink is down.
	if count == 1 || count%20 == 0 {
		p.log.Warn(fmt.Sprintf("stats sink %s failed", op), "error", err, "errors", count)
	}
}
