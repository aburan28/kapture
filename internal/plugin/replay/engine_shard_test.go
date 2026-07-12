package replay

import (
	"context"
	"fmt"
	"testing"

	"github.com/kapture-io/kapture/internal/storage"
)

func shardRequests(n int) []*storage.CapturedRequest {
	reqs := make([]*storage.CapturedRequest, n)
	for i := range reqs {
		reqs[i] = &storage.CapturedRequest{
			ID:       fmt.Sprintf("req-%04d", i),
			Method:   "GET",
			Path:     "/api/test",
			Protocol: "HTTP",
		}
	}
	return reqs
}

// TestEngine_ShardsAreDisjointAndExhaustive replays the same capture through
// every shard of a 4-way split and verifies each request is sent by exactly
// one shard.
func TestEngine_ShardsAreDisjointAndExhaustive(t *testing.T) {
	const total = 200
	const shards = 4

	seen := make(map[string]int)
	var sentPerShard []int

	for shard := 0; shard < shards; shard++ {
		reader := &mockReader{requests: shardRequests(total)}
		sender := &mockSender{}

		engine, err := NewEngine(EngineConfig{
			Reader:     reader,
			Sender:     sender,
			ShardIndex: shard,
			ShardCount: shards,
		})
		if err != nil {
			t.Fatalf("NewEngine shard %d: %v", shard, err)
		}

		if _, err := engine.Run(context.Background()); err != nil {
			t.Fatalf("Run shard %d: %v", shard, err)
		}

		if got := engine.ShardSkipped() + int64(len(sender.sent)); got != total {
			t.Errorf("shard %d: sent+skipped = %d, want %d", shard, got, total)
		}

		sentPerShard = append(sentPerShard, len(sender.sent))
		for _, req := range sender.sent {
			seen[req.ID]++
		}
	}

	if len(seen) != total {
		t.Fatalf("shards covered %d distinct requests, want %d", len(seen), total)
	}
	for id, count := range seen {
		if count != 1 {
			t.Errorf("request %s replayed by %d shards, want exactly 1", id, count)
		}
	}

	// FNV-1a should spread requests reasonably; no shard may be empty for
	// 200 requests over 4 shards.
	for shard, sent := range sentPerShard {
		if sent == 0 {
			t.Errorf("shard %d sent no requests; hash distribution is broken", shard)
		}
	}
}

// TestEngine_NoShardingSendsEverything verifies ShardCount <= 1 leaves the
// engine unsharded.
func TestEngine_NoShardingSendsEverything(t *testing.T) {
	reader := &mockReader{requests: shardRequests(50)}
	sender := &mockSender{}

	engine, err := NewEngine(EngineConfig{Reader: reader, Sender: sender})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	if _, err := engine.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(sender.sent) != 50 {
		t.Errorf("sent %d requests, want 50", len(sender.sent))
	}
	if engine.ShardSkipped() != 0 {
		t.Errorf("skipped %d requests without sharding, want 0", engine.ShardSkipped())
	}
}

// TestEngine_InvalidShardConfig rejects out-of-range shard indexes.
func TestEngine_InvalidShardConfig(t *testing.T) {
	for _, tc := range []struct{ index, count int }{
		{index: 4, count: 4},
		{index: -1, count: 4},
		{index: 100, count: 4},
	} {
		_, err := NewEngine(EngineConfig{
			Reader:     &mockReader{},
			Sender:     &mockSender{},
			ShardIndex: tc.index,
			ShardCount: tc.count,
		})
		if err == nil {
			t.Errorf("NewEngine(index=%d, count=%d) succeeded, want error", tc.index, tc.count)
		}
	}
}

// TestShardOf_Deterministic confirms the shard function is stable, which is
// what makes independent workers' shards disjoint.
func TestShardOf_Deterministic(t *testing.T) {
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("req-%d", i)
		first := shardOf(id, 7)
		for j := 0; j < 5; j++ {
			if got := shardOf(id, 7); got != first {
				t.Fatalf("shardOf(%q) not deterministic: %d then %d", id, first, got)
			}
		}
		if first >= 7 {
			t.Fatalf("shardOf(%q, 7) = %d, out of range", id, first)
		}
	}
}
