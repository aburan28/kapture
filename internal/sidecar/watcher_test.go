package sidecar

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestWatcherProcessesStableFiles(t *testing.T) {
	dir := t.TempDir()

	var mu sync.Mutex
	var processed []string

	handler := func(_ context.Context, path string) error {
		mu.Lock()
		defer mu.Unlock()
		processed = append(processed, filepath.Base(path))
		return nil
	}

	w := NewWatcher(WatcherConfig{
		Dir:          dir,
		Extensions:   []string{".jsonl"},
		PollInterval: 50 * time.Millisecond,
		StableAge:    100 * time.Millisecond,
		Handler:      handler,
	})

	// Write a file and backdate its mod time so it's immediately stable.
	f := filepath.Join(dir, "test.jsonl")
	if err := os.WriteFile(f, []byte(`{"id":"1"}`), 0644); err != nil {
		t.Fatal(err)
	}
	past := time.Now().Add(-1 * time.Second)
	os.Chtimes(f, past, past)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	_ = w.Run(ctx)

	mu.Lock()
	defer mu.Unlock()
	if len(processed) != 1 {
		t.Fatalf("expected 1 processed file, got %d", len(processed))
	}
	if processed[0] != "test.jsonl" {
		t.Errorf("expected test.jsonl, got %s", processed[0])
	}
}

func TestWatcherSkipsNonMatchingExtensions(t *testing.T) {
	dir := t.TempDir()

	var mu sync.Mutex
	var processed []string

	handler := func(_ context.Context, path string) error {
		mu.Lock()
		defer mu.Unlock()
		processed = append(processed, filepath.Base(path))
		return nil
	}

	w := NewWatcher(WatcherConfig{
		Dir:          dir,
		Extensions:   []string{".jsonl"},
		PollInterval: 50 * time.Millisecond,
		StableAge:    100 * time.Millisecond,
		Handler:      handler,
	})

	// Write a .txt file (should be ignored) and a .jsonl file.
	past := time.Now().Add(-1 * time.Second)

	txt := filepath.Join(dir, "ignore.txt")
	os.WriteFile(txt, []byte("nope"), 0644)
	os.Chtimes(txt, past, past)

	jsonl := filepath.Join(dir, "good.jsonl")
	os.WriteFile(jsonl, []byte(`{"id":"1"}`), 0644)
	os.Chtimes(jsonl, past, past)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	_ = w.Run(ctx)

	mu.Lock()
	defer mu.Unlock()
	if len(processed) != 1 {
		t.Fatalf("expected 1 processed file, got %d", len(processed))
	}
	if processed[0] != "good.jsonl" {
		t.Errorf("expected good.jsonl, got %s", processed[0])
	}
}

func TestWatcherDoesNotReprocessFiles(t *testing.T) {
	dir := t.TempDir()

	var mu sync.Mutex
	count := 0

	handler := func(_ context.Context, path string) error {
		mu.Lock()
		defer mu.Unlock()
		count++
		return nil
	}

	w := NewWatcher(WatcherConfig{
		Dir:          dir,
		Extensions:   []string{".jsonl"},
		PollInterval: 50 * time.Millisecond,
		StableAge:    50 * time.Millisecond,
		Handler:      handler,
	})

	f := filepath.Join(dir, "test.jsonl")
	os.WriteFile(f, []byte(`{"id":"1"}`), 0644)
	past := time.Now().Add(-1 * time.Second)
	os.Chtimes(f, past, past)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	_ = w.Run(ctx)

	mu.Lock()
	defer mu.Unlock()
	if count != 1 {
		t.Errorf("expected handler called once, got %d", count)
	}
}
