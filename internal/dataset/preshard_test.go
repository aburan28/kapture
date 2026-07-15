package dataset

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/kapture-io/kapture/internal/plugin/replay"
	"github.com/kapture-io/kapture/internal/storage"
)

// memReader is an in-memory capture source.
type memReader struct {
	reqs []*storage.CapturedRequest
	idx  int
}

func (r *memReader) Open(context.Context, replay.ReadOptions) error { return nil }
func (r *memReader) Next(context.Context) (*storage.CapturedRequest, error) {
	if r.idx >= len(r.reqs) {
		return nil, replay.ErrReaderDone
	}
	req := r.reqs[r.idx]
	r.idx++
	return req, nil
}
func (r *memReader) Close() error { return nil }

func testCapture(n int) []*storage.CapturedRequest {
	reqs := make([]*storage.CapturedRequest, n)
	base := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	for i := range reqs {
		reqs[i] = &storage.CapturedRequest{
			ID:        fmt.Sprintf("req-%05d", i),
			Timestamp: base.Add(time.Duration(i) * time.Millisecond),
			Method:    "GET",
			Path:      fmt.Sprintf("/api/%d", i),
			Protocol:  "HTTP",
		}
	}
	return reqs
}

// TestPreshard_RoundTripThroughRealStorage preshards through the real EFS
// writer and reads every slice back with the real filesystem reader: the
// slices must be disjoint, exhaustive, hash-consistent, and located
// exactly where the replay Job runner will look for them.
func TestPreshard_RoundTripThroughRealStorage(t *testing.T) {
	const total = 500
	const shards = 4
	mount := t.TempDir()

	factory, err := storage.NewWriterFactory("efs", storage.EFSConfig{
		MountPath: mount,
		AgentPod:  "preshard-test",
	})
	if err != nil {
		t.Fatal(err)
	}

	reqs := testCapture(total)
	result, err := Preshard(context.Background(), &memReader{reqs: reqs},
		replay.ReadOptions{CaptureID: "prod/orders"}, factory, "prod/orders", shards)
	if err != nil {
		t.Fatalf("Preshard: %v", err)
	}
	if result.Total != total {
		t.Errorf("total = %d, want %d", result.Total, total)
	}

	seen := map[string]int{}
	var sliceSum int64
	for i := 0; i < shards; i++ {
		sliceID := ShardSliceCaptureID("prod/orders", i, shards)
		if result.ShardCounts[i] == 0 {
			t.Errorf("slice %d is empty; hash distribution broken", i)
		}
		sliceSum += result.ShardCounts[i]

		reader, err := replay.NewFilesystemReader(replay.FilesystemReaderConfig{MountPath: mount})
		if err != nil {
			t.Fatal(err)
		}
		if err := reader.Open(context.Background(), replay.ReadOptions{CaptureID: sliceID}); err != nil {
			t.Fatalf("open slice %d (%s): %v", i, sliceID, err)
		}

		var read int64
		for {
			req, err := reader.Next(context.Background())
			if errors.Is(err, replay.ErrReaderDone) {
				break
			}
			if err != nil {
				t.Fatalf("read slice %d: %v", i, err)
			}
			read++
			seen[req.ID]++
			// Every request in slice i must be owned by shard i under the
			// data plane's hash — the preshard layout and the runtime
			// filter agree by construction.
			if !replay.ShardOwns(req.ID, i, shards) {
				t.Errorf("request %s in slice %d but hash-owned by shard %d",
					req.ID, i, replay.ShardIndexOf(req.ID, shards))
			}
		}
		reader.Close()

		if read != result.ShardCounts[i] {
			t.Errorf("slice %d: read %d requests, reported %d", i, read, result.ShardCounts[i])
		}
	}

	if sliceSum != total {
		t.Errorf("slice counts sum to %d, want %d", sliceSum, total)
	}
	if len(seen) != total {
		t.Fatalf("slices cover %d distinct requests, want %d", len(seen), total)
	}
	for id, count := range seen {
		if count != 1 {
			t.Errorf("request %s appears in %d slices, want exactly 1", id, count)
		}
	}
}

func TestPreshard_Validation(t *testing.T) {
	factory, err := storage.NewWriterFactory("efs", storage.EFSConfig{
		MountPath: t.TempDir(), AgentPod: "x",
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := Preshard(context.Background(), &memReader{}, replay.ReadOptions{}, factory, "c", 1); err == nil {
		t.Error("shard count 1 accepted")
	}
	if _, err := Preshard(context.Background(), &memReader{}, replay.ReadOptions{}, factory, "", 2); err == nil {
		t.Error("empty capture ID accepted")
	}
}

func TestShardSliceCaptureID(t *testing.T) {
	got := ShardSliceCaptureID("prod/orders", 3, 16)
	if got != "prod/orders/shards/3-of-16" {
		t.Errorf("ShardSliceCaptureID = %q", got)
	}
}
