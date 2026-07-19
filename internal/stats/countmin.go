package stats

import (
	"sort"
	"sync"
)

const (
	cmDepth        = 4
	defaultCMWidth = 2048
)

// TopKEntry is one heavy hitter with its estimated count.
type TopKEntry struct {
	Key   string `json:"key"`
	Count uint64 `json:"count"`
}

// TopK tracks the approximately-heaviest keys in a stream using a
// Count-Min sketch for frequency estimates and a bounded candidate set
// for identity. Estimates never undercount (CMS overestimates only), so
// true heavy hitters cannot be evicted by noise once established.
type TopK struct {
	mu    sync.Mutex
	k     int
	width int
	table [cmDepth][]uint64

	// candidates holds the current top-k keys with their CMS estimates.
	candidates map[string]uint64
	// minEstimate is the smallest estimate in candidates (maintained
	// lazily on eviction).
	minEstimate uint64
}

// NewTopK tracks the k heaviest keys with a Count-Min sketch of the
// given width per row (0 = default).
func NewTopK(k, width int) *TopK {
	if k <= 0 {
		k = 10
	}
	if width <= 0 {
		width = defaultCMWidth
	}
	t := &TopK{k: k, width: width, candidates: make(map[string]uint64, k+1)}
	for i := range t.table {
		t.table[i] = make([]uint64, width)
	}
	return t
}

// Add records one occurrence of key.
func (t *TopK) Add(key string) {
	h1, h2 := splitHash(fnv1a64(key))

	t.mu.Lock()
	defer t.mu.Unlock()

	// Count-Min update, tracking the new estimate (row minimum).
	estimate := ^uint64(0)
	for row := 0; row < cmDepth; row++ {
		idx := (h1 + uint64(row)*h2) % uint64(t.width)
		t.table[row][idx]++
		if v := t.table[row][idx]; v < estimate {
			estimate = v
		}
	}

	if _, ok := t.candidates[key]; ok {
		t.candidates[key] = estimate
		return
	}
	if len(t.candidates) < t.k {
		t.candidates[key] = estimate
		return
	}
	if estimate <= t.minEstimateLocked() {
		return
	}
	// Evict the current minimum candidate.
	var evictKey string
	evictVal := ^uint64(0)
	for k, v := range t.candidates {
		if v < evictVal {
			evictKey, evictVal = k, v
		}
	}
	delete(t.candidates, evictKey)
	t.candidates[key] = estimate
}

func (t *TopK) minEstimateLocked() uint64 {
	min := ^uint64(0)
	for _, v := range t.candidates {
		if v < min {
			min = v
		}
	}
	if min == ^uint64(0) {
		return 0
	}
	return min
}

// Estimate returns the CMS frequency estimate for key (an upper bound
// that matches the true count with high probability).
func (t *TopK) Estimate(key string) uint64 {
	h1, h2 := splitHash(fnv1a64(key))
	t.mu.Lock()
	defer t.mu.Unlock()
	estimate := ^uint64(0)
	for row := 0; row < cmDepth; row++ {
		idx := (h1 + uint64(row)*h2) % uint64(t.width)
		if v := t.table[row][idx]; v < estimate {
			estimate = v
		}
	}
	return estimate
}

// Top returns the tracked heavy hitters, heaviest first.
func (t *TopK) Top() []TopKEntry {
	t.mu.Lock()
	entries := make([]TopKEntry, 0, len(t.candidates))
	for k, v := range t.candidates {
		entries = append(entries, TopKEntry{Key: k, Count: v})
	}
	t.mu.Unlock()

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Count != entries[j].Count {
			return entries[i].Count > entries[j].Count
		}
		return entries[i].Key < entries[j].Key
	})
	return entries
}
