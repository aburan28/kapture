package stats

import (
	"math"
	"math/bits"
	"sync"
)

// hllPrecision is the register-index width. 2^12 = 4096 registers give a
// standard error of ~1.6% in 4 KiB — plenty for per-capture cardinality.
const hllPrecision = 12

const hllRegisters = 1 << hllPrecision

// HyperLogLog estimates the number of distinct strings added. Standard
// HLL with linear-counting small-range correction.
type HyperLogLog struct {
	mu        sync.Mutex
	registers [hllRegisters]uint8
}

// NewHyperLogLog returns an empty estimator.
func NewHyperLogLog() *HyperLogLog { return &HyperLogLog{} }

// Add records one occurrence of s.
func (h *HyperLogLog) Add(s string) {
	x := fnv1a64(s)
	idx := x >> (64 - hllPrecision)
	// Rank: position of the leftmost 1 in the remaining bits, 1-based.
	rest := x << hllPrecision
	rank := uint8(bits.LeadingZeros64(rest)) + 1
	maxRank := uint8(64 - hllPrecision + 1)
	if rank > maxRank {
		rank = maxRank
	}

	h.mu.Lock()
	if rank > h.registers[idx] {
		h.registers[idx] = rank
	}
	h.mu.Unlock()
}

// Estimate returns the approximate distinct count.
func (h *HyperLogLog) Estimate() uint64 {
	h.mu.Lock()
	var sum float64
	zeros := 0
	for _, r := range h.registers {
		sum += 1 / float64(uint64(1)<<r)
		if r == 0 {
			zeros++
		}
	}
	h.mu.Unlock()

	m := float64(hllRegisters)
	alpha := 0.7213 / (1 + 1.079/m)
	estimate := alpha * m * m / sum

	// Small-range correction: linear counting.
	if estimate <= 2.5*m && zeros > 0 {
		estimate = m * math.Log(m/float64(zeros))
	}
	return uint64(estimate + 0.5)
}

// Merge folds other into h (union of the two multisets' distinct items).
func (h *HyperLogLog) Merge(other *HyperLogLog) {
	other.mu.Lock()
	snapshot := other.registers
	other.mu.Unlock()

	h.mu.Lock()
	for i, r := range snapshot {
		if r > h.registers[i] {
			h.registers[i] = r
		}
	}
	h.mu.Unlock()
}
