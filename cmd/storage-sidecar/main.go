package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kapture-io/kapture/internal/sidecar"
	"github.com/kapture-io/kapture/internal/storage"
)

func main() {
	var (
		watchDir      = flag.String("watch-dir", "/capture-data", "Directory to watch for capture output files")
		format        = flag.String("format", "jsonl", "Output format: jsonl or pcap")
		storageType   = flag.String("storage-type", "efs", "Storage backend type (s3, gcs, efs, ebs, plugin)")
		storageConfig = flag.String("storage-config", "{}", "Storage backend config as JSON")
		captureID     = flag.String("capture-id", "", "Capture ID for storage path naming")
		pollInterval  = flag.Duration("poll-interval", 2*time.Second, "How often to check for new files")
		stableAge     = flag.Duration("stable-age", 3*time.Second, "How long a file must be unmodified before processing")
		deleteAfter   = flag.Bool("delete-after-upload", true, "Delete files after successful upload")
		healthPort    = flag.Int("health-port", 8082, "Health check listen port")
	)
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if *captureID == "" {
		log.Error("--capture-id is required")
		os.Exit(1)
	}

	// Parse storage config.
	var configMap map[string]any
	if err := json.Unmarshal([]byte(*storageConfig), &configMap); err != nil {
		log.Error("invalid --storage-config JSON", "error", err)
		os.Exit(1)
	}

	// Create storage writer factory.
	factory, err := storage.NewWriterFactory(*storageType, configMap)
	if err != nil {
		log.Error("failed to create storage writer factory", "error", err, "type", *storageType)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	writer, err := factory.NewWriter(ctx, *captureID)
	if err != nil {
		log.Error("failed to create storage writer", "error", err)
		os.Exit(1)
	}
	defer writer.Close()

	// Determine file extensions to watch.
	var extensions []string
	switch *format {
	case "jsonl":
		extensions = []string{".jsonl", ".json", ".ndjson"}
	case "pcap":
		extensions = []string{".pcap", ".pcapng"}
	}

	// Create uploader.
	uploader := sidecar.NewUploader(sidecar.UploaderConfig{
		Writer: writer,
		Format: *format,
		Delete: *deleteAfter,
		Logger: log,
	})

	// Create watcher.
	watcher := sidecar.NewWatcher(sidecar.WatcherConfig{
		Dir:          *watchDir,
		Extensions:   extensions,
		PollInterval: *pollInterval,
		StableAge:    *stableAge,
		Handler:      uploader.Upload,
		Logger:       log,
	})

	// Start health server.
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status":"ok"}`)
	})
	healthServer := &http.Server{
		Addr:              fmt.Sprintf(":%d", *healthPort),
		Handler:           healthMux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Info("starting sidecar health server", "port", *healthPort)
		if err := healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("health server failed", "error", err)
		}
	}()

	// Handle shutdown signals.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Info("received signal, shutting down", "signal", sig)
		cancel()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = healthServer.Shutdown(shutdownCtx)
	}()

	log.Info("starting storage sidecar",
		"watchDir", *watchDir,
		"format", *format,
		"storageType", *storageType,
		"captureID", *captureID,
	)

	if err := watcher.Run(ctx); err != nil && ctx.Err() == nil {
		log.Error("watcher failed", "error", err)
		os.Exit(1)
	}

	log.Info("storage sidecar shutdown complete")
}
