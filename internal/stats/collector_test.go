package stats

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/kapture-io/kapture/internal/storage"
)

func capturedReq(ip, port, path, method string, bodyLen int) *storage.CapturedRequest {
	return &storage.CapturedRequest{
		ID:        "id",
		Timestamp: time.Now(),
		Method:    method,
		Path:      path,
		Protocol:  "HTTP",
		Body:      make([]byte, bodyLen),
		Metadata: map[string]string{
			"remoteAddr": ip + ":" + port,
			"host":       "api.example.com",
		},
	}
}

func TestCollector_SnapshotAggregatesAllSketches(t *testing.T) {
	c := NewCollector(CollectorConfig{TopK: 3})

	// 50 clients; client-0 sends 10x the traffic to /hot.
	for i := 0; i < 50; i++ {
		ip := fmt.Sprintf("10.0.0.%d", i)
		c.Observe(capturedReq(ip, "1000", "/cold", "GET", 100), time.Millisecond)
	}
	for i := 0; i < 500; i++ {
		c.Observe(capturedReq("10.0.0.0", "1000", "/hot", "POST", 1000), 2*time.Millisecond)
	}

	snap := c.Snapshot()

	if snap.UniqueClientIPs < 45 || snap.UniqueClientIPs > 55 {
		t.Errorf("uniqueClientIPs = %d, want ~50", snap.UniqueClientIPs)
	}
	if snap.UniqueFlows < 45 || snap.UniqueFlows > 55 {
		t.Errorf("uniqueFlows = %d, want ~50", snap.UniqueFlows)
	}
	if len(snap.TopPaths) == 0 || snap.TopPaths[0].Key != "/hot" {
		t.Errorf("top path = %+v, want /hot first", snap.TopPaths)
	}
	if len(snap.TopClients) == 0 || snap.TopClients[0].Key != "10.0.0.0" {
		t.Errorf("top client = %+v, want 10.0.0.0 first", snap.TopClients)
	}
	if snap.CurrentWindow == nil || snap.CurrentWindow.Requests != 550 {
		t.Fatalf("current window = %+v, want 550 requests", snap.CurrentWindow)
	}
	if snap.CurrentWindow.ByMethod["POST"] != 500 {
		t.Errorf("POST count = %d", snap.CurrentWindow.ByMethod["POST"])
	}
	// Body sizes: 500 at 1000 bytes, 50 at 100 → p50 ~1000.
	if p50 := snap.BodySizeBytes.P50; p50 < 950 || p50 > 1050 {
		t.Errorf("body size p50 = %v, want ~1000", p50)
	}
	if snap.HandleLatencyMys.Count != 550 {
		t.Errorf("latency observations = %d", snap.HandleLatencyMys.Count)
	}

	// Membership: seen flow vs never-seen flow.
	if !c.SeenFlow(capturedReq("10.0.0.0", "1000", "/", "GET", 0)) {
		t.Error("known flow reported unseen")
	}
	if c.SeenFlow(capturedReq("192.168.99.99", "1234", "/", "GET", 0)) {
		t.Error("unknown flow reported seen (unlikely false positive)")
	}

	// The snapshot must serialise cleanly for /stats and Redis.
	if _, err := json.Marshal(snap); err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
}

// fakeSink records published payloads and can fail on demand.
type fakeSink struct {
	mu        sync.Mutex
	snapshots [][]byte
	windows   [][]byte
	fail      bool
	closed    bool
}

func (f *fakeSink) PublishSnapshot(_ context.Context, _ string, payload []byte, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return fmt.Errorf("sink down")
	}
	f.snapshots = append(f.snapshots, payload)
	return nil
}

func (f *fakeSink) AppendWindow(_ context.Context, _ string, payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return fmt.Errorf("sink down")
	}
	f.windows = append(f.windows, payload)
	return nil
}

func (f *fakeSink) Close() error { f.mu.Lock(); defer f.mu.Unlock(); f.closed = true; return nil }

func (f *fakeSink) counts() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.snapshots), len(f.windows)
}

func TestPublisher_PublishesSnapshotsAndWindows(t *testing.T) {
	c := NewCollector(CollectorConfig{WindowDuration: time.Minute})
	sink := &fakeSink{}
	p := NewPublisher(c, sink, "ns/cap", time.Hour, slog.Default())

	// Simulate two completed windows via the roll callback.
	c.Windows.OnRoll(&WindowCounts{Requests: 10})
	c.Windows.OnRoll(&WindowCounts{Requests: 20})

	p.publish(context.Background())
	snaps, wins := sink.counts()
	if snaps != 1 || wins != 2 {
		t.Fatalf("published snapshots=%d windows=%d, want 1/2", snaps, wins)
	}

	// Windows are not re-published.
	p.publish(context.Background())
	if _, wins = sink.counts(); wins != 2 {
		t.Errorf("windows re-published: %d", wins)
	}
}

func TestPublisher_RetriesWindowsAfterSinkFailure(t *testing.T) {
	c := NewCollector(CollectorConfig{})
	sink := &fakeSink{fail: true}
	p := NewPublisher(c, sink, "ns/cap", time.Hour, slog.Default())

	c.Windows.OnRoll(&WindowCounts{Requests: 7})
	p.publish(context.Background())
	if _, wins := sink.counts(); wins != 0 {
		t.Fatal("window published while sink down")
	}

	sink.mu.Lock()
	sink.fail = false
	sink.mu.Unlock()
	p.publish(context.Background())
	if _, wins := sink.counts(); wins != 1 {
		t.Errorf("window not retried after sink recovery")
	}
}
