package storage

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestEFSWriterWritesCompressedFile(t *testing.T) {
	mountPath := t.TempDir()
	factory, err := NewEFSWriterFactory(EFSConfig{MountPath: mountPath, AgentPod: "agent-c"})
	if err != nil {
		t.Fatalf("NewEFSWriterFactory() error = %v", err)
	}

	writer, err := factory.NewWriter(context.Background(), "capture-3")
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}

	fixedNow := time.Date(2026, 3, 15, 14, 0, 0, 0, time.UTC)
	efsWriter := writer.(*EFSWriter)
	efsWriter.now = func() time.Time { return fixedNow }

	if err := writer.Write(context.Background(), &CapturedRequest{ID: "req-3", Timestamp: fixedNow, Method: "PUT", Path: "/files", Protocol: "HTTP"}); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	target := filepath.Join(mountPath, "capture-3", "2026-03-15", "agent-c-000001.jsonl.gz")
	payload, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", target, err)
	}
	decoded := decompressPayload(t, payload)
	if !strings.Contains(decoded, `"id":"req-3"`) || !strings.Contains(decoded, `"path":"/files"`) {
		t.Fatalf("unexpected payload %q", decoded)
	}
}
