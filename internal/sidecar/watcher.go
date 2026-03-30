// Package sidecar implements the storage sidecar that watches for capture
// output files and uploads them to the configured storage backend.
package sidecar

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	defaultPollInterval = 2 * time.Second
	// Files are considered ready when they haven't been modified for this long.
	defaultStableAge = 3 * time.Second
)

// FileHandler is called when a new completed file is detected.
type FileHandler func(ctx context.Context, path string) error

// Watcher polls a directory for new files and invokes a handler for each
// completed file. A file is "completed" when it hasn't been modified for
// a configurable stable age.
type Watcher struct {
	dir          string
	extensions   []string
	pollInterval time.Duration
	stableAge    time.Duration
	handler      FileHandler
	log          *slog.Logger

	mu        sync.Mutex
	processed map[string]struct{}
}

// WatcherConfig configures a Watcher.
type WatcherConfig struct {
	Dir          string
	Extensions   []string // e.g. [".jsonl", ".pcap"]
	PollInterval time.Duration
	StableAge    time.Duration
	Handler      FileHandler
	Logger       *slog.Logger
}

// NewWatcher creates a new directory Watcher.
func NewWatcher(cfg WatcherConfig) *Watcher {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.StableAge <= 0 {
		cfg.StableAge = defaultStableAge
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Watcher{
		dir:          cfg.Dir,
		extensions:   cfg.Extensions,
		pollInterval: cfg.PollInterval,
		stableAge:    cfg.StableAge,
		handler:      cfg.Handler,
		log:          cfg.Logger,
		processed:    make(map[string]struct{}),
	}
}

// Run starts the polling loop and blocks until ctx is cancelled.
func (w *Watcher) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Final sweep to pick up any remaining files.
			w.sweep(context.Background())
			return ctx.Err()
		case <-ticker.C:
			w.sweep(ctx)
		}
	}
}

func (w *Watcher) sweep(ctx context.Context) {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		w.log.Warn("failed to read watch directory", "dir", w.dir, "error", err)
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !w.matchesExtension(name) {
			continue
		}

		w.mu.Lock()
		_, done := w.processed[name]
		w.mu.Unlock()
		if done {
			continue
		}

		fullPath := filepath.Join(w.dir, name)
		info, err := entry.Info()
		if err != nil {
			continue
		}

		// Wait for file to be stable (not recently modified).
		if time.Since(info.ModTime()) < w.stableAge {
			continue
		}

		if err := w.handler(ctx, fullPath); err != nil {
			w.log.Warn("handler failed for file", "path", fullPath, "error", err)
			continue
		}

		w.mu.Lock()
		w.processed[name] = struct{}{}
		w.mu.Unlock()

		w.log.Info("processed file", "path", fullPath)
	}
}

func (w *Watcher) matchesExtension(name string) bool {
	if len(w.extensions) == 0 {
		return true
	}
	for _, ext := range w.extensions {
		if strings.HasSuffix(name, ext) {
			return true
		}
	}
	return false
}
