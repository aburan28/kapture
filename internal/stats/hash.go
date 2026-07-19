// Package stats provides streaming statistics for the capture path:
// exact counters over tumbling flow windows, and sublinear sketches for
// everything that cannot be tracked exactly at line rate — cardinality
// (HyperLogLog), heavy hitters (Count-Min + top-K), quantiles
// (DDSketch), and membership (Bloom filter).
//
// All structures are safe for concurrent use unless noted and are sized
// for per-agent use: a Collector costs a few hundred KiB regardless of
// traffic volume.
package stats

// fnv1a64 hashes a string with FNV-1a (64-bit) and runs the result
// through a SplitMix64 finalizer. Raw FNV-1a has weak avalanche into the
// high bits for short keys that differ only near the end — fatal for
// HyperLogLog, which indexes registers by the top bits — so every sketch
// consumes the finalized value.
func fnv1a64(s string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	return mix64(h)
}

// mix64 is the SplitMix64 finalizer: full avalanche, bijective.
func mix64(h uint64) uint64 {
	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	h ^= h >> 31
	return h
}

// splitHash expands one 64-bit hash into two independent-enough values
// for double hashing (h1 + i*h2 schemes).
func splitHash(h uint64) (uint64, uint64) {
	h2 := mix64(h ^ 0x9e3779b97f4a7c15)
	if h2 == 0 {
		h2 = 1
	}
	return h, h2
}
