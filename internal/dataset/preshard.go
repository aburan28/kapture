// Package dataset transforms raw captures into replay-optimised layouts.
//
// Preshard is the first such transform: it re-partitions a capture into N
// per-shard slices using the same FNV-1a assignment the replay data plane
// filters by (replay.ShardOwns / ShardIndexOf). A distributed load test
// over a presharded capture reads shardCount× less data from storage —
// each worker fetches only its own slice instead of streaming the full
// capture and discarding (N-1)/N of it.
//
// Slice layout reuses the standard capture object layout under a nested
// capture ID, so every existing reader and writer works unchanged:
//
//	{prefix}/{captureID}/shards/{index}-of-{count}/{date}/{writer}-{seq}.jsonl.gz
//
// The pipeline is streaming end to end: one pass over the source, one
// buffered writer per shard, no full-capture materialisation.
package dataset

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/kapture-io/kapture/internal/plugin/replay"
	"github.com/kapture-io/kapture/internal/storage"
)

// ShardSliceCaptureID returns the capture ID under which one shard slice
// of a presharded capture is stored. The replay Job runner derives the
// same path when spec.shard.presharded is set — the format is a
// compatibility contract between the preshard CLI and the runner.
func ShardSliceCaptureID(captureID string, index, count int) string {
	return fmt.Sprintf("%s/shards/%d-of-%d", captureID, index, count)
}

// PreshardResult reports what a preshard run wrote.
type PreshardResult struct {
	Total       int64
	ShardCounts []int64
}

// Preshard streams the capture selected by reader/readOpts once and
// routes every request to its owning shard's writer. Writers are created
// through the standard factory, so any storage backend works and the
// slices are readable by the unmodified replay readers.
func Preshard(ctx context.Context, reader replay.Reader, readOpts replay.ReadOptions, factory storage.WriterFactory, captureID string, shardCount int) (*PreshardResult, error) {
	if shardCount < 2 {
		return nil, fmt.Errorf("shard count must be >= 2, got %d", shardCount)
	}
	if captureID == "" {
		return nil, errors.New("capture ID is required")
	}

	if err := reader.Open(ctx, readOpts); err != nil {
		return nil, fmt.Errorf("open reader: %w", err)
	}

	writers := make([]storage.Writer, shardCount)
	for i := range writers {
		w, err := factory.NewWriter(ctx, ShardSliceCaptureID(captureID, i, shardCount))
		if err != nil {
			closeWriters(writers[:i])
			return nil, fmt.Errorf("create shard %d writer: %w", i, err)
		}
		writers[i] = w
	}

	result := &PreshardResult{ShardCounts: make([]int64, shardCount)}
	for {
		req, err := reader.Next(ctx)
		if errors.Is(err, replay.ErrReaderDone) || errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			closeWriters(writers)
			return nil, fmt.Errorf("read capture: %w", err)
		}

		shard := replay.ShardIndexOf(req.ID, shardCount)
		if err := writers[shard].Write(ctx, req); err != nil {
			closeWriters(writers)
			return nil, fmt.Errorf("write shard %d: %w", shard, err)
		}
		result.ShardCounts[shard]++
		result.Total++
	}

	// Flush and close every slice; publish is only complete when all
	// writers closed cleanly.
	for i, w := range writers {
		if err := w.Flush(ctx); err != nil {
			closeWriters(writers)
			return nil, fmt.Errorf("flush shard %d: %w", i, err)
		}
		if err := w.Close(); err != nil {
			closeWriters(writers[i+1:])
			return nil, fmt.Errorf("close shard %d: %w", i, err)
		}
	}
	return result, nil
}

func closeWriters(writers []storage.Writer) {
	for _, w := range writers {
		if w != nil {
			_ = w.Close()
		}
	}
}
