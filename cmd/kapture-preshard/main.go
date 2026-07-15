// Command kapture-preshard re-partitions a capture into per-shard slices
// so distributed load tests read shardCount× less data from storage: each
// replay worker fetches only its own slice instead of streaming the full
// capture and discarding the rest.
//
// Run it once per (capture, shard count) before a large load test —
// typically as a Kubernetes Job using the replay-engine image (the binary
// ships at /kapture-preshard):
//
//	kapture-preshard \
//	  --storage-type s3 --bucket my-captures --region us-east-1 \
//	  --capture-id prod/orders --shards 32
//
// Then set distribution.presharded: true (and the matching shard count via
// spokes × workersPerSpoke) on the CaptureLoadTest, or shard.presharded on
// a manual TrafficReplay. Workers read
// {captureID}/shards/{i}-of-{n} with no shard filtering.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/kapture-io/kapture/internal/dataset"
	"github.com/kapture-io/kapture/internal/plugin/replay"
	"github.com/kapture-io/kapture/internal/storage"
)

func main() {
	var (
		storageType = flag.String("storage-type", "", "Storage backend: s3, gcs, efs, ebs")
		bucket      = flag.String("bucket", "", "S3/GCS bucket name")
		region      = flag.String("region", "", "AWS region (S3 only)")
		prefix      = flag.String("prefix", "", "Storage prefix/path")
		mountPath   = flag.String("mount-path", "", "Filesystem mount path (EFS/EBS)")
		captureID   = flag.String("capture-id", "", "Capture ID to preshard (required)")
		shards      = flag.Int("shards", 0, "Number of shard slices to produce (required, >= 2)")
	)
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, *storageType, *bucket, *region, *prefix, *mountPath, *captureID, *shards); err != nil {
		fmt.Fprintln(os.Stderr, "preshard failed:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, storageType, bucket, region, prefix, mountPath, captureID string, shards int) error {
	if storageType == "" || captureID == "" || shards < 2 {
		return fmt.Errorf("--storage-type, --capture-id, and --shards >= 2 are required")
	}

	readerConfig := map[string]any{}
	if bucket != "" {
		readerConfig["bucket"] = bucket
	}
	if region != "" {
		readerConfig["region"] = region
	}
	if prefix != "" {
		readerConfig["prefix"] = prefix
	}
	if mountPath != "" {
		readerConfig["mountPath"] = mountPath
	}

	reader, err := replay.NewReaderFromConfig(ctx, storageType, readerConfig)
	if err != nil {
		return fmt.Errorf("create reader: %w", err)
	}
	defer reader.Close()

	factory, err := newWriterFactory(storageType, bucket, region, prefix, mountPath)
	if err != nil {
		return fmt.Errorf("create writer factory: %w", err)
	}

	result, err := dataset.Preshard(ctx, reader, replay.ReadOptions{CaptureID: captureID}, factory, captureID, shards)
	if err != nil {
		return err
	}

	fmt.Printf("presharded %d requests from %q into %d slices:\n", result.Total, captureID, shards)
	for i, count := range result.ShardCounts {
		fmt.Printf("  %s: %d requests\n", dataset.ShardSliceCaptureID(captureID, i, shards), count)
	}
	return nil
}

func newWriterFactory(storageType, bucket, region, prefix, mountPath string) (storage.WriterFactory, error) {
	writerName := "kapture-preshard"
	switch storageType {
	case "s3":
		return storage.NewWriterFactory("s3", storage.S3Config{
			Bucket: bucket, Region: region, Prefix: prefix, AgentPod: writerName,
		})
	case "gcs":
		return storage.NewWriterFactory("gcs", storage.GCSConfig{
			Bucket: bucket, Prefix: prefix, AgentPod: writerName,
		})
	case "efs":
		return storage.NewWriterFactory("efs", storage.EFSConfig{
			MountPath: mountPath, AgentPod: writerName,
		})
	case "ebs":
		return storage.NewWriterFactory("ebs", storage.EBSConfig{
			MountPath: mountPath, AgentPod: writerName,
		})
	default:
		return nil, fmt.Errorf("unsupported storage type %q", storageType)
	}
}
