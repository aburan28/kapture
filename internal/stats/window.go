package stats

import (
	"sync"
	"time"
)

// DefaultWindowDuration is the tumbling flow-window length.
const DefaultWindowDuration = time.Minute

// keptWindows bounds how many completed windows are retained in memory.
const keptWindows = 15

// WindowCounts are the exact counters accumulated over one flow window.
type WindowCounts struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end,omitzero"`

	Requests  uint64            `json:"requests"`
	Bytes     uint64            `json:"bytes"`
	MeanBytes float64           `json:"meanBytes"`
	ByMethod  map[string]uint64 `json:"byMethod,omitempty"`
	ByProto   map[string]uint64 `json:"byProtocol,omitempty"`
	// NewFlows counts 5-tuples first seen (per Bloom filter) this window.
	NewFlows uint64 `json:"newFlows"`
}

func newWindowCounts(start time.Time) *WindowCounts {
	return &WindowCounts{
		Start:    start,
		ByMethod: make(map[string]uint64),
		ByProto:  make(map[string]uint64),
	}
}

func (w *WindowCounts) finalize(end time.Time) {
	w.End = end
	if w.Requests > 0 {
		w.MeanBytes = float64(w.Bytes) / float64(w.Requests)
	}
}

// WindowedCounters maintains plain counters over tumbling windows. The
// current window rolls when an observation arrives past its end; a roll
// callback (if set) receives each completed window exactly once.
type WindowedCounters struct {
	mu        sync.Mutex
	duration  time.Duration
	current   *WindowCounts
	completed []*WindowCounts
	now       func() time.Time

	// OnRoll, when set, is called with each completed window (outside
	// the lock). Set before the first observation.
	OnRoll func(*WindowCounts)
}

// NewWindowedCounters creates tumbling-window counters (0 = default 1m).
func NewWindowedCounters(duration time.Duration) *WindowedCounters {
	if duration <= 0 {
		duration = DefaultWindowDuration
	}
	return &WindowedCounters{duration: duration, now: time.Now}
}

// Observe adds one request with its byte size to the current window.
func (w *WindowedCounters) Observe(method, protocol string, bytes uint64, newFlow bool) {
	now := w.now()

	w.mu.Lock()
	rolled := w.rollLocked(now)
	if w.current == nil {
		w.current = newWindowCounts(now.Truncate(w.duration))
	}
	w.current.Requests++
	w.current.Bytes += bytes
	if method != "" {
		w.current.ByMethod[method]++
	}
	if protocol != "" {
		w.current.ByProto[protocol]++
	}
	if newFlow {
		w.current.NewFlows++
	}
	w.mu.Unlock()

	if rolled != nil && w.OnRoll != nil {
		w.OnRoll(rolled)
	}
}

// rollLocked closes the current window if now is past its end, returning
// the completed window (or nil).
func (w *WindowedCounters) rollLocked(now time.Time) *WindowCounts {
	if w.current == nil || now.Before(w.current.Start.Add(w.duration)) {
		return nil
	}
	done := w.current
	done.finalize(done.Start.Add(w.duration))
	w.completed = append(w.completed, done)
	if len(w.completed) > keptWindows {
		w.completed = w.completed[len(w.completed)-keptWindows:]
	}
	w.current = newWindowCounts(now.Truncate(w.duration))
	return done
}

// Snapshot returns a copy of the in-progress window (nil if empty) and
// the retained completed windows, oldest first.
func (w *WindowedCounters) Snapshot() (current *WindowCounts, completed []*WindowCounts) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.current != nil {
		c := *w.current
		c.ByMethod = copyMap(w.current.ByMethod)
		c.ByProto = copyMap(w.current.ByProto)
		if c.Requests > 0 {
			c.MeanBytes = float64(c.Bytes) / float64(c.Requests)
		}
		current = &c
	}
	completed = append([]*WindowCounts(nil), w.completed...)
	return current, completed
}

func copyMap(m map[string]uint64) map[string]uint64 {
	out := make(map[string]uint64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
