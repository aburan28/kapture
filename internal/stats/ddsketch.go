package stats

import (
	"math"
	"sort"
	"sync"
)

// DDSketch is a quantile sketch with a relative-error guarantee: any
// returned quantile q satisfies |q - trueQ| <= alpha * trueQ. Buckets
// grow logarithmically, so the sketch stays small for any value range
// (sizes, latencies). Sparse map storage; zero and negative values land
// in a dedicated bucket (negatives clamp to zero — sizes and durations
// are non-negative by construction).
type DDSketch struct {
	mu       sync.Mutex
	gamma    float64
	logGamma float64
	buckets  map[int]uint64
	zeroes   uint64
	count    uint64
	sum      float64
	max      float64
}

// NewDDSketch builds a sketch with the given relative accuracy
// (0 < alpha < 1; 0 = default 1%).
func NewDDSketch(alpha float64) *DDSketch {
	if alpha <= 0 || alpha >= 1 {
		alpha = 0.01
	}
	gamma := (1 + alpha) / (1 - alpha)
	return &DDSketch{
		gamma:    gamma,
		logGamma: math.Log(gamma),
		buckets:  make(map[int]uint64),
	}
}

// Add records one observation.
func (d *DDSketch) Add(v float64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.count++
	if v > 0 {
		d.sum += v
	}
	if v > d.max {
		d.max = v
	}
	if v <= 0 {
		d.zeroes++
		return
	}
	idx := int(math.Ceil(math.Log(v) / d.logGamma))
	d.buckets[idx]++
}

// Quantile returns the value at quantile q in [0, 1].
func (d *DDSketch) Quantile(q float64) float64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.count == 0 {
		return 0
	}
	if q < 0 {
		q = 0
	}
	if q > 1 {
		q = 1
	}

	rank := uint64(q * float64(d.count-1))
	if rank < d.zeroes {
		return 0
	}

	keys := make([]int, 0, len(d.buckets))
	for k := range d.buckets {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	cumulative := d.zeroes
	for _, k := range keys {
		cumulative += d.buckets[k]
		if cumulative > rank {
			// Midpoint estimate of the bucket [gamma^(k-1), gamma^k].
			return 2 * math.Pow(d.gamma, float64(k)) / (d.gamma + 1)
		}
	}
	return d.max
}

// Count returns the number of observations.
func (d *DDSketch) Count() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.count
}

// Mean returns the exact mean of positive observations (zeroes included
// in the denominator).
func (d *DDSketch) Mean() float64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.count == 0 {
		return 0
	}
	return d.sum / float64(d.count)
}

// Max returns the largest observation seen.
func (d *DDSketch) Max() float64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.max
}
