package storage

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestEBSWriterUsesFilesystemBackend(t *testing.T) {
	mountPath := t.TempDir()
	factory, err := NewEBSWriterFactory(EBSConfig{MountPath: mountPath, AgentPod: "agent-d"})
	if err != nil {
		t.Fatalf("NewEBSWriterFactory() error = %v", err)
	}

	writer, err := factory.NewWriter(context.Background(), "capture-4")
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}

	fixedNow := time.Date(2026, 3, 15, 15, 0, 0, 0, time.UTC)
	ebsWriter := writer.(*EBSWriter)
	ebsWriter.now = func() time.Time { return fixedNow }

	if err := writer.Write(context.Background(), &CapturedRequest{ID: "req-4", Timestamp: fixedNow, Method: "PATCH", Path: "/block", Protocol: "HTTP"}); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := writer.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	target := filepath.Join(mountPath, "capture-4", "2026-03-15", "agent-d-000001.jsonl.gz")
	payload, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", target, err)
	}
	decoded := decompressPayload(t, payload)
	if !strings.Contains(decoded, `"id":"req-4"`) || !strings.Contains(decoded, `"path":"/block"`) {
		t.Fatalf("unexpected payload %q", decoded)
	}
}
