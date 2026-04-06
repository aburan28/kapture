package replay

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/kapture-io/kapture/internal/storage"
)

// timestampRecordingSender captures the wall-clock time of every Send call.
// This allows tests to verify that the engine is pacing correctly.
type timestampRecordingSender struct {
	mu         sync.Mutex
	sendTimes  []time.Time
	requestIDs []string
}

func (s *timestampRecordingSender) Send(_ context.Context, req *storage.CapturedRequest) (*Result, error) {
	now := time.Now()
	s.mu.Lock()
	s.sendTimes = append(s.sendTimes, now)
	s.requestIDs = append(s.requestIDs, req.ID)
	s.mu.Unlock()
	return &Result{
		RequestID:  req.ID,
		StatusCode: 200,
		Duration:   time.Millisecond,
		Timestamp:  now,
	}, nil
}

func (s *timestampRecordingSender) Close() error { return nil }

func (s *timestampRecordingSender) getSendTimes() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]time.Time, len(s.sendTimes))
	copy(cp, s.sendTimes)
	return cp
}

// TestEngine_OriginalTimingFidelity verifies that RateModeOriginalTiming
// preserves the inter-request delays from the captured traffic.
// This is the core test for "as recorded" QPS fidelity.
func TestEngine_OriginalTimingFidelity(t *testing.T) {
	// Create requests with known timestamps: 100ms apart.
	baseTime := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	interval := 100 * time.Millisecond
	numRequests := 8

	reqs := make([]*storage.CapturedRequest, numRequests)
	for i := range reqs {
		reqs[i] = &storage.CapturedRequest{
			ID:        "req-" + string(rune('A'+i)),
			Method:    "GET",
			Path:      "/api/test",
			Protocol:  "HTTP/1.1",
			Timestamp: baseTime.Add(time.Duration(i) * interval),
		}
	}

	reader := &mockReader{requests: reqs}
	sender := &timestampRecordingSender{}

	engine, err := NewEngine(EngineConfig{
		Reader:      reader,
		Sender:      sender,
		RateMode:    RateModeOriginalTiming,
		TimeScale:   1.0, // Real-time: delays should match original
		Concurrency: 1,   // Single sender to ensure strict ordering
	})
	if err != nil {
		t.Fatal(err)
	}

	summary, err := engine.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if summary.TotalRequests != int64(numRequests) {
		t.Fatalf("expected %d requests, got %d", numRequests, summary.TotalRequests)
	}

	sendTimes := sender.getSendTimes()
	if len(sendTimes) != numRequests {
		t.Fatalf("expected %d send times, got %d", numRequests, len(sendTimes))
	}

	// Verify inter-request delays match the expected 100ms interval.
	// Allow generous tolerance for CI environments with race detector overhead.
	tolerance := 80 * time.Millisecond

	for i := 1; i < len(sendTimes); i++ {
		actualDelay := sendTimes[i].Sub(sendTimes[i-1])
		expectedDelay := interval

		diff := absDuration(actualDelay - expectedDelay)
		if diff > tolerance {
			t.Errorf("request %d->%d: expected ~%v delay, got %v (drift: %v)",
				i-1, i, expectedDelay, actualDelay, diff)
		}
	}

	// Verify total elapsed time is approximately (numRequests-1) * interval.
	totalElapsed := sendTimes[len(sendTimes)-1].Sub(sendTimes[0])
	expectedTotal := time.Duration(numRequests-1) * interval

	if absDuration(totalElapsed-expectedTotal) > 150*time.Millisecond*time.Duration(numRequests) {
		t.Errorf("total elapsed: expected ~%v, got %v", expectedTotal, totalElapsed)
	}

	t.Logf("Original timing fidelity: %d requests, expected interval=%v, actual total=%v (expected ~%v)",
		numRequests, interval, totalElapsed, expectedTotal)
}

// TestEngine_OriginalTimingWithTimeScale verifies that TimeScale correctly
// adjusts replay speed. 0.5 = 2x speed, 2.0 = half speed.
func TestEngine_OriginalTimingWithTimeScale(t *testing.T) {
	baseTime := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	interval := 200 * time.Millisecond
	numRequests := 5

	reqs := make([]*storage.CapturedRequest, numRequests)
	for i := range reqs {
		reqs[i] = &storage.CapturedRequest{
			ID:        "req-" + string(rune('A'+i)),
			Method:    "GET",
			Path:      "/api/test",
			Protocol:  "HTTP/1.1",
			Timestamp: baseTime.Add(time.Duration(i) * interval),
		}
	}

	// TimeScale 0.5 = 2x speed: 200ms intervals become 100ms
	reader := &mockReader{requests: reqs}
	sender := &timestampRecordingSender{}

	engine, err := NewEngine(EngineConfig{
		Reader:      reader,
		Sender:      sender,
		RateMode:    RateModeOriginalTiming,
		TimeScale:   0.5,
		Concurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = engine.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	sendTimes := sender.getSendTimes()
	if len(sendTimes) != numRequests {
		t.Fatalf("expected %d send times, got %d", numRequests, len(sendTimes))
	}

	expectedScaledInterval := 100 * time.Millisecond // 200ms * 0.5
	tolerance := 80 * time.Millisecond

	for i := 1; i < len(sendTimes); i++ {
		actualDelay := sendTimes[i].Sub(sendTimes[i-1])
		diff := absDuration(actualDelay - expectedScaledInterval)
		if diff > tolerance {
			t.Errorf("request %d->%d: expected ~%v delay (scaled), got %v (drift: %v)",
				i-1, i, expectedScaledInterval, actualDelay, diff)
		}
	}

	totalElapsed := sendTimes[len(sendTimes)-1].Sub(sendTimes[0])
	expectedTotal := time.Duration(numRequests-1) * expectedScaledInterval

	t.Logf("TimeScale=0.5 fidelity: %d requests, expected interval=%v, actual total=%v (expected ~%v)",
		numRequests, expectedScaledInterval, totalElapsed, expectedTotal)
}

// TestEngine_ConstantRateFidelity verifies RateModeConstant delivers
// requests at the specified RPS.
func TestEngine_ConstantRateFidelity(t *testing.T) {
	numRequests := 10
	targetRPS := 20.0 // 20 req/s = 50ms per request
	expectedInterval := time.Duration(float64(time.Second) / targetRPS)

	baseTime := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	reqs := make([]*storage.CapturedRequest, numRequests)
	for i := range reqs {
		reqs[i] = &storage.CapturedRequest{
			ID:        "req-" + string(rune('A'+i)),
			Method:    "GET",
			Path:      "/api/test",
			Protocol:  "HTTP/1.1",
			Timestamp: baseTime.Add(time.Duration(i) * time.Millisecond), // original timing doesn't matter
		}
	}

	reader := &mockReader{requests: reqs}
	sender := &timestampRecordingSender{}

	engine, err := NewEngine(EngineConfig{
		Reader:        reader,
		Sender:        sender,
		RateMode:      RateModeConstant,
		RatePerSecond: targetRPS,
		Concurrency:   1,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = engine.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	sendTimes := sender.getSendTimes()
	if len(sendTimes) != numRequests {
		t.Fatalf("expected %d send times, got %d", numRequests, len(sendTimes))
	}

	// Verify approximate RPS.
	totalElapsed := sendTimes[len(sendTimes)-1].Sub(sendTimes[0])
	actualRPS := float64(numRequests-1) / totalElapsed.Seconds()

	// Allow 50% tolerance for constant rate (timer granularity + CI overhead).
	if math.Abs(actualRPS-targetRPS)/targetRPS > 0.50 {
		t.Errorf("expected RPS ~%.1f, got %.1f (total elapsed: %v)",
			targetRPS, actualRPS, totalElapsed)
	}

	// Verify individual intervals are approximately correct.
	tolerance := 40 * time.Millisecond
	for i := 1; i < len(sendTimes); i++ {
		actualDelay := sendTimes[i].Sub(sendTimes[i-1])
		diff := absDuration(actualDelay - expectedInterval)
		if diff > tolerance {
			t.Errorf("request %d->%d: expected ~%v interval, got %v (drift: %v)",
				i-1, i, expectedInterval, actualDelay, diff)
		}
	}

	t.Logf("Constant rate fidelity: target=%.1f RPS, actual=%.1f RPS, total=%v",
		targetRPS, actualRPS, totalElapsed)
}

// TestEngine_UnlimitedMode verifies that RateModeUnlimited sends
// as fast as possible (no artificial delays).
func TestEngine_UnlimitedMode(t *testing.T) {
	numRequests := 50
	baseTime := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	reqs := make([]*storage.CapturedRequest, numRequests)
	for i := range reqs {
		reqs[i] = &storage.CapturedRequest{
			ID:        "req-" + string(rune('A'+i%26)),
			Method:    "GET",
			Path:      "/api/test",
			Protocol:  "HTTP/1.1",
			Timestamp: baseTime.Add(time.Duration(i) * time.Second), // 1s apart originally
		}
	}

	reader := &mockReader{requests: reqs}
	sender := &timestampRecordingSender{}

	engine, err := NewEngine(EngineConfig{
		Reader:      reader,
		Sender:      sender,
		RateMode:    RateModeUnlimited,
		Concurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = engine.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	sendTimes := sender.getSendTimes()
	totalElapsed := sendTimes[len(sendTimes)-1].Sub(sendTimes[0])

	// Unlimited should complete much faster than original timing (49 seconds).
	// With no network I/O, it should finish in well under 1 second.
	if totalElapsed > 2*time.Second {
		t.Errorf("unlimited mode too slow: %v for %d requests (original was %v apart)",
			totalElapsed, numRequests, time.Second)
	}

	t.Logf("Unlimited mode: %d requests in %v", numRequests, totalElapsed)
}

// TestEngine_VariableIntervalFidelity tests with realistic variable
// inter-request delays to simulate bursty traffic patterns.
func TestEngine_VariableIntervalFidelity(t *testing.T) {
	baseTime := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	// Simulate bursty traffic: some requests close together, some far apart.
	timestamps := []time.Duration{
		0,
		50 * time.Millisecond,   // 50ms gap
		60 * time.Millisecond,   // 10ms gap (burst)
		70 * time.Millisecond,   // 10ms gap (burst)
		270 * time.Millisecond,  // 200ms gap (quiet period)
		320 * time.Millisecond,  // 50ms gap
	}

	reqs := make([]*storage.CapturedRequest, len(timestamps))
	for i, ts := range timestamps {
		reqs[i] = &storage.CapturedRequest{
			ID:        "req-" + string(rune('A'+i)),
			Method:    "GET",
			Path:      "/api/test",
			Protocol:  "HTTP/1.1",
			Timestamp: baseTime.Add(ts),
		}
	}

	reader := &mockReader{requests: reqs}
	sender := &timestampRecordingSender{}

	engine, err := NewEngine(EngineConfig{
		Reader:      reader,
		Sender:      sender,
		RateMode:    RateModeOriginalTiming,
		TimeScale:   1.0,
		Concurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = engine.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	sendTimes := sender.getSendTimes()
	tolerance := 60 * time.Millisecond

	// Verify each inter-request delay matches the original.
	for i := 1; i < len(sendTimes); i++ {
		actualDelay := sendTimes[i].Sub(sendTimes[i-1])
		expectedDelay := timestamps[i] - timestamps[i-1]

		diff := absDuration(actualDelay - expectedDelay)
		if diff > tolerance {
			t.Errorf("request %d->%d: expected ~%v delay, got %v (drift: %v)",
				i-1, i, expectedDelay, actualDelay, diff)
		}
	}

	t.Logf("Variable interval fidelity: %d requests with bursty pattern verified", len(reqs))
}

// TestFeedServer_OriginalTimingPacing verifies the feed server's readLoop
// paces requests according to original timing.
func TestFeedServer_OriginalTimingPacing(t *testing.T) {
	baseTime := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	interval := 100 * time.Millisecond
	numRequests := 4

	reqs := make([]*storage.CapturedRequest, numRequests)
	for i := range reqs {
		reqs[i] = &storage.CapturedRequest{
			ID:        "req-" + string(rune('A'+i)),
			Method:    "GET",
			Path:      "/api/test",
			Protocol:  "HTTP/1.1",
			Timestamp: baseTime.Add(time.Duration(i) * interval),
		}
	}

	reader := &mockReader{requests: reqs}
	server, err := NewFeedServer(FeedServerConfig{
		Reader:     reader,
		RateMode:   RateModeOriginalTiming,
		TimeScale:  1.0,
		BufferSize: 10,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := server.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer server.Stop(context.Background())

	// Read requests and record when each arrives.
	receiveTimes := make([]time.Time, 0, numRequests)
	for i := 0; i < numRequests; i++ {
		select {
		case _, ok := <-server.reqCh:
			if !ok {
				break
			}
			receiveTimes = append(receiveTimes, time.Now())
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for request from feed server")
		}
	}

	if len(receiveTimes) != numRequests {
		t.Fatalf("expected %d requests, got %d", numRequests, len(receiveTimes))
	}

	// Verify pacing: intervals should be ~100ms.
	tolerance := 80 * time.Millisecond
	for i := 1; i < len(receiveTimes); i++ {
		actualDelay := receiveTimes[i].Sub(receiveTimes[i-1])
		diff := absDuration(actualDelay - interval)
		if diff > tolerance {
			t.Errorf("feed request %d->%d: expected ~%v delay, got %v",
				i-1, i, interval, actualDelay)
		}
	}

	t.Logf("Feed server pacing: %d requests with ~%v intervals verified", numRequests, interval)
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
