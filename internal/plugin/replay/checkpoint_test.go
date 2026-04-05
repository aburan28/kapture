package replay

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCheckpointer_SaveAndLoad(t *testing.T) {
	dir := t.TempDir()

	cp, err := NewCheckpointer(CheckpointerConfig{
		Dir:      dir,
		Interval: time.Hour, // won't auto-flush in test
	})
	if err != nil {
		t.Fatal(err)
	}

	initial := &Checkpoint{
		ReplayID:     "test-replay-1",
		CaptureID:    "prod/api-gateway",
		RequestsSent: 0,
		StartTime:    time.Now().UTC(),
	}

	cp.Start(initial)

	// Update checkpoint.
	cp.Update(func(c *Checkpoint) {
		c.RequestsSent = 1000
		c.LastObjectKey = "prefix/capture/2026-01-15/agent-000010.jsonl.gz"
		c.LastRequestID = "req-xyz"
	})

	// Stop triggers a flush.
	if err := cp.Stop(); err != nil {
		t.Fatal(err)
	}

	// Load it back.
	loaded, err := cp.Load("test-replay-1")
	if err != nil {
		t.Fatal(err)
	}

	if loaded == nil {
		t.Fatal("expected non-nil checkpoint")
	}
	if loaded.RequestsSent != 1000 {
		t.Errorf("expected 1000 sent, got %d", loaded.RequestsSent)
	}
	if loaded.LastObjectKey != "prefix/capture/2026-01-15/agent-000010.jsonl.gz" {
		t.Errorf("unexpected LastObjectKey: %s", loaded.LastObjectKey)
	}
	if loaded.CaptureID != "prod/api-gateway" {
		t.Errorf("unexpected CaptureID: %s", loaded.CaptureID)
	}
}

func TestCheckpointer_LoadNonexistent(t *testing.T) {
	dir := t.TempDir()

	cp, err := NewCheckpointer(CheckpointerConfig{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}

	loaded, err := cp.Load("does-not-exist")
	if err != nil {
		t.Fatal(err)
	}
	if loaded != nil {
		t.Error("expected nil for nonexistent checkpoint")
	}
}

func TestCheckpointer_Delete(t *testing.T) {
	dir := t.TempDir()

	cp, err := NewCheckpointer(CheckpointerConfig{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}

	// Create a checkpoint file manually.
	path := filepath.Join(dir, "replay-delete-me.checkpoint.json")
	if err := os.WriteFile(path, []byte(`{"replayId":"delete-me"}`), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := cp.Delete("delete-me"); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("checkpoint file should have been deleted")
	}
}

func TestCheckpointer_DeleteNonexistent(t *testing.T) {
	dir := t.TempDir()

	cp, err := NewCheckpointer(CheckpointerConfig{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}

	// Should not error on nonexistent file.
	if err := cp.Delete("nonexistent"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestCheckpointer_DirRequired(t *testing.T) {
	_, err := NewCheckpointer(CheckpointerConfig{})
	if err == nil {
		t.Error("expected error with empty dir")
	}
}

func TestCheckpointer_AtomicWrite(t *testing.T) {
	dir := t.TempDir()

	cp, err := NewCheckpointer(CheckpointerConfig{
		Dir:      dir,
		Interval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}

	initial := &Checkpoint{
		ReplayID:  "atomic-test",
		CaptureID: "test",
	}

	cp.Start(initial)

	// Multiple rapid updates.
	for i := 0; i < 100; i++ {
		cp.Update(func(c *Checkpoint) {
			c.RequestsSent++
		})
	}

	if err := cp.Stop(); err != nil {
		t.Fatal(err)
	}

	loaded, err := cp.Load("atomic-test")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.RequestsSent != 100 {
		t.Errorf("expected 100, got %d", loaded.RequestsSent)
	}

	// Verify no .tmp file left behind.
	tmpPath := filepath.Join(dir, "replay-atomic-test.checkpoint.json.tmp")
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Error("tmp file should not exist after successful write")
	}
}
