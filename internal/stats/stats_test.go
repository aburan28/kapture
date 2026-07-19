package stats

import (
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
	"time"
)

func TestHyperLogLog_AccuracyAcrossScales(t *testing.T) {
	for _, n := range []int{100, 1000, 50_000, 500_000} {
		hll := NewHyperLogLog()
		for i := 0; i < n; i++ {
			hll.Add(fmt.Sprintf("item-%d", i))
		}
		got := float64(hll.Estimate())
		errRatio := math.Abs(got-float64(n)) / float64(n)
		// Standard error at p=12 is ~1.6%; allow 5% headroom.
		if errRatio > 0.05 {
			t.Errorf("n=%d: estimate %v off by %.1f%%", n, got, errRatio*100)
		}
	}
}

func TestHyperLogLog_DuplicatesDontInflate(t *testing.T) {
	hll := NewHyperLogLog()
	for round := 0; round < 10; round++ {
		for i := 0; i < 1000; i++ {
			hll.Add(fmt.Sprintf("dup-%d", i))
		}
	}
	got := float64(hll.Estimate())
	if math.Abs(got-1000)/1000 > 0.05 {
		t.Errorf("estimate %v for 1000 distinct items added 10x", got)
	}
}

func TestHyperLogLog_Merge(t *testing.T) {
	a, b := NewHyperLogLog(), NewHyperLogLog()
	for i := 0; i < 5000; i++ {
		a.Add(fmt.Sprintf("a-%d", i))
		b.Add(fmt.Sprintf("b-%d", i))
	}
	a.Merge(b)
	got := float64(a.Estimate())
	if math.Abs(got-10000)/10000 > 0.05 {
		t.Errorf("merged estimate %v, want ~10000", got)
	}
}

func TestTopK_FindsHeavyHitters(t *testing.T) {
	topk := NewTopK(5, 0)
	rng := rand.New(rand.NewPCG(1, 2))

	// 5 heavy keys at 10k each buried in 100k noise singletons.
	for i := 0; i < 10_000; i++ {
		for h := 0; h < 5; h++ {
			topk.Add(fmt.Sprintf("heavy-%d", h))
		}
		for n := 0; n < 10; n++ {
			topk.Add(fmt.Sprintf("noise-%d", rng.IntN(1_000_000)))
		}
	}

	top := topk.Top()
	if len(top) != 5 {
		t.Fatalf("top returned %d entries, want 5", len(top))
	}
	found := map[string]bool{}
	for _, e := range top {
		found[e.Key] = true
		// CMS never undercounts; overcount should be small relative to
		// the heavy count.
		if e.Count < 10_000 || e.Count > 12_000 {
			t.Errorf("heavy hitter %s count %d, want [10000, 12000]", e.Key, e.Count)
		}
	}
	for h := 0; h < 5; h++ {
		if !found[fmt.Sprintf("heavy-%d", h)] {
			t.Errorf("heavy-%d missing from top-5: %v", h, top)
		}
	}
}

func TestTopK_EstimateNeverUndercounts(t *testing.T) {
	topk := NewTopK(3, 512)
	for i := 0; i < 500; i++ {
		topk.Add("exact")
	}
	if got := topk.Estimate("exact"); got < 500 {
		t.Errorf("estimate %d undercounts true 500", got)
	}
}

func TestDDSketch_RelativeErrorAcrossMagnitudes(t *testing.T) {
	d := NewDDSketch(0.01)
	// Log-uniform values across 6 orders of magnitude.
	rng := rand.New(rand.NewPCG(3, 4))
	values := make([]float64, 100_000)
	for i := range values {
		values[i] = math.Pow(10, rng.Float64()*6)
		d.Add(values[i])
	}
	slices.Sort(values)

	for _, q := range []float64{0.01, 0.25, 0.5, 0.75, 0.95, 0.99} {
		want := values[int(q*float64(len(values)-1))]
		got := d.Quantile(q)
		if math.Abs(got-want)/want > 0.02 {
			t.Errorf("q%.2f = %v, true %v: relative error %.3f exceeds 2%%",
				q, got, want, math.Abs(got-want)/want)
		}
	}
}

func TestDDSketch_Quantiles(t *testing.T) {
	d := NewDDSketch(0.01)
	// Values 1..10000 uniformly.
	for i := 1; i <= 10_000; i++ {
		d.Add(float64(i))
	}
	checks := map[float64]float64{0.5: 5000, 0.9: 9000, 0.99: 9900}
	for q, want := range checks {
		got := d.Quantile(q)
		if math.Abs(got-want)/want > 0.03 {
			t.Errorf("q%.2f = %.0f, want ~%.0f (±3%%)", q, got, want)
		}
	}
	if d.Count() != 10_000 {
		t.Errorf("count = %d", d.Count())
	}
	if mean := d.Mean(); math.Abs(mean-5000.5) > 1 {
		t.Errorf("mean = %v, want 5000.5", mean)
	}
	if max := d.Max(); max != 10_000 {
		t.Errorf("max = %v", max)
	}
}

func TestDDSketch_ZeroesAndEmpty(t *testing.T) {
	d := NewDDSketch(0.01)
	if d.Quantile(0.5) != 0 {
		t.Error("empty sketch quantile != 0")
	}
	for i := 0; i < 10; i++ {
		d.Add(0)
	}
	d.Add(100)
	if got := d.Quantile(0.5); got != 0 {
		t.Errorf("median of mostly-zero data = %v, want 0", got)
	}
	if got := d.Quantile(1.0); math.Abs(got-100) > 2 {
		t.Errorf("p100 = %v, want ~100", got)
	}
}

func TestBloomFilter_NoFalseNegatives(t *testing.T) {
	b := NewBloomFilter(10_000, 0.01)
	for i := 0; i < 10_000; i++ {
		b.Add(fmt.Sprintf("member-%d", i))
	}
	for i := 0; i < 10_000; i++ {
		if !b.Contains(fmt.Sprintf("member-%d", i)) {
			t.Fatalf("false negative for member-%d", i)
		}
	}
}

func TestBloomFilter_FalsePositiveRate(t *testing.T) {
	b := NewBloomFilter(10_000, 0.01)
	for i := 0; i < 10_000; i++ {
		b.Add(fmt.Sprintf("member-%d", i))
	}
	fp := 0
	const probes = 20_000
	for i := 0; i < probes; i++ {
		if b.Contains(fmt.Sprintf("nonmember-%d", i)) {
			fp++
		}
	}
	if rate := float64(fp) / probes; rate > 0.03 {
		t.Errorf("false positive rate %.3f, want < 0.03", rate)
	}
}

func TestBloomFilter_AddIfNew(t *testing.T) {
	b := NewBloomFilter(1000, 0.01)
	if !b.AddIfNew("flow-1") {
		t.Error("first add reported not-new")
	}
	if b.AddIfNew("flow-1") {
		t.Error("second add reported new")
	}
}

func TestWindowedCounters_RollsAndAggregates(t *testing.T) {
	w := NewWindowedCounters(time.Minute)
	base := time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC)
	now := base
	w.now = func() time.Time { return now }

	var rolled []*WindowCounts
	w.OnRoll = func(c *WindowCounts) { rolled = append(rolled, c) }

	w.Observe("GET", "HTTP", 100, true)
	w.Observe("POST", "HTTP", 300, false)

	current, completed := w.Snapshot()
	if current == nil || current.Requests != 2 || current.Bytes != 400 {
		t.Fatalf("current window = %+v", current)
	}
	if current.MeanBytes != 200 {
		t.Errorf("mean bytes = %v, want 200", current.MeanBytes)
	}
	if len(completed) != 0 {
		t.Fatalf("completed windows before roll: %d", len(completed))
	}

	// Advance past the window: next observation rolls it.
	now = base.Add(90 * time.Second)
	w.Observe("GET", "gRPC", 50, false)

	current, completed = w.Snapshot()
	if len(completed) != 1 || len(rolled) != 1 {
		t.Fatalf("completed=%d rolled=%d, want 1/1", len(completed), len(rolled))
	}
	done := completed[0]
	if done.Requests != 2 || done.ByMethod["GET"] != 1 || done.ByMethod["POST"] != 1 || done.NewFlows != 1 {
		t.Errorf("rolled window = %+v", done)
	}
	if !done.End.Equal(base.Add(time.Minute)) {
		t.Errorf("window end = %v", done.End)
	}
	if current == nil || current.Requests != 1 || current.ByProto["gRPC"] != 1 {
		t.Errorf("new current window = %+v", current)
	}
}
