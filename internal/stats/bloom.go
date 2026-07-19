package stats

import (
	"math"
	"math/bits"
	"sync"
)

// BloomFilter answers approximate set membership: Contains never returns
// a false negative, and false positives stay near the configured rate
// until the filter fills past its designed capacity.
type BloomFilter struct {
	mu     sync.Mutex
	bits   []uint64
	m      uint64 // total bits
	k      int    // hash functions
	setOps uint64 // Add calls (for fill reporting)
}

// NewBloomFilter sizes the filter for expectedItems at the target
// false-positive probability (0 < fpp < 1; defaults 100k items, 1%).
func NewBloomFilter(expectedItems int, fpp float64) *BloomFilter {
	if expectedItems <= 0 {
		expectedItems = 100_000
	}
	if fpp <= 0 || fpp >= 1 {
		fpp = 0.01
	}
	n := float64(expectedItems)
	m := math.Ceil(-n * math.Log(fpp) / (math.Ln2 * math.Ln2))
	k := int(math.Round(m / n * math.Ln2))
	if k < 1 {
		k = 1
	}
	mBits := uint64(m)
	if mBits < 64 {
		mBits = 64
	}
	return &BloomFilter{
		bits: make([]uint64, (mBits+63)/64),
		m:    mBits,
		k:    k,
	}
}

// Add inserts s into the set.
func (b *BloomFilter) Add(s string) {
	h1, h2 := splitHash(fnv1a64(s))
	b.mu.Lock()
	for i := 0; i < b.k; i++ {
		pos := (h1 + uint64(i)*h2) % b.m
		b.bits[pos/64] |= 1 << (pos % 64)
	}
	b.setOps++
	b.mu.Unlock()
}

// Contains reports whether s may be in the set (no false negatives).
func (b *BloomFilter) Contains(s string) bool {
	h1, h2 := splitHash(fnv1a64(s))
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := 0; i < b.k; i++ {
		pos := (h1 + uint64(i)*h2) % b.m
		if b.bits[pos/64]&(1<<(pos%64)) == 0 {
			return false
		}
	}
	return true
}

// AddIfNew inserts s and reports whether it was (probably) new: false
// means s was definitely already... probably present. Used to count
// first-seen flows cheaply.
func (b *BloomFilter) AddIfNew(s string) bool {
	h1, h2 := splitHash(fnv1a64(s))
	b.mu.Lock()
	defer b.mu.Unlock()
	present := true
	for i := 0; i < b.k; i++ {
		pos := (h1 + uint64(i)*h2) % b.m
		word, bit := pos/64, uint64(1)<<(pos%64)
		if b.bits[word]&bit == 0 {
			present = false
			b.bits[word] |= bit
		}
	}
	b.setOps++
	return !present
}

// FillRatio reports the fraction of set bits — above ~0.5 the
// false-positive rate degrades beyond the design point.
func (b *BloomFilter) FillRatio() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	var set uint64
	for _, w := range b.bits {
		set += uint64(bits.OnesCount64(w))
	}
	return float64(set) / float64(b.m)
}
