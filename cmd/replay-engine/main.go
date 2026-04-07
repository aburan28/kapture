// Command replay-engine orchestrates replaying captured Kapture traffic
// against a target service. It reads compressed JSONL data from any supported
// storage backend (S3, GCS, EFS, EBS), applies optional transformers, and
// replays at the original recorded QPS or a configurable rate.
//
// Designed for large captures (100GB+) and multi-day replay sessions with
// automatic checkpointing and resume support.
//
// Usage:
//
//	# Direct replay (engine controls rate + sends requests):
//	replay-engine \
//	  --storage-type s3 --bucket my-captures \
//	  --capture-id production/api-gateway \
//	  --target-url http://staging.internal:8080 \
//	  --rate-mode original --concurrency 50
//
//	# Feed server mode (k6/external generator sends requests):
//	replay-engine \
//	  --storage-type s3 --bucket my-captures \
//	  --capture-id production/api-gateway \
//	  --serve --addr :6565 --rate-mode original
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/kapture-io/kapture/internal/plugin/replay"
)

type config struct {
	// Storage
	storageType string
	bucket      string
	region      string
	prefix      string
	mountPath   string

	// Capture selection
	captureID  string
	startTime  string
	endTime    string
	pathPrefix string
	methods    string
	limit      int64

	// Replay target (direct mode)
	targetURL       string
	overrideHeaders string

	// Feed server mode
	serve bool
	addr  string

	// Engine
	rateMode      string
	ratePerSecond float64
	timeScale     float64
	concurrency   int

	// Checkpoint
	checkpointDir string
	replayID      string
	resume        bool

	// Output
	logLevel  string
	logFormat string
}

func main() {
	cfg := parseFlags()

	logger := setupLogger(cfg.logLevel, cfg.logFormat)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, cfg, logger); err != nil {
		logger.Error("replay failed", "error", err)
		os.Exit(1)
	}
}

func parseFlags() config {
	var cfg config

	// Storage flags.
	flag.StringVar(&cfg.storageType, "storage-type", "", "Storage backend: s3, gcs, efs, ebs")
	flag.StringVar(&cfg.bucket, "bucket", "", "S3/GCS bucket name")
	flag.StringVar(&cfg.region, "region", "", "AWS region (S3 only)")
	flag.StringVar(&cfg.prefix, "prefix", "", "Storage prefix/path")
	flag.StringVar(&cfg.mountPath, "mount-path", "", "Filesystem mount path (EFS/EBS)")

	// Capture selection flags.
	flag.StringVar(&cfg.captureID, "capture-id", "", "Capture ID to replay (required)")
	flag.StringVar(&cfg.startTime, "start-time", "", "Start time filter (RFC3339)")
	flag.StringVar(&cfg.endTime, "end-time", "", "End time filter (RFC3339)")
	flag.StringVar(&cfg.pathPrefix, "path-prefix", "", "Filter by path prefix")
	flag.StringVar(&cfg.methods, "methods", "", "Filter by HTTP methods (comma-separated)")
	flag.Int64Var(&cfg.limit, "limit", 0, "Max requests to replay (0=unlimited)")

	// Target flags (direct mode).
	flag.StringVar(&cfg.targetURL, "target-url", "", "Target service URL (required in direct mode)")
	flag.StringVar(&cfg.overrideHeaders, "override-headers", "", "Headers to override (key=value,key=value)")

	// Feed server flags.
	flag.BoolVar(&cfg.serve, "serve", false, "Run as feed server for external load generators (k6)")
	flag.StringVar(&cfg.addr, "addr", ":6565", "Feed server listen address (--serve mode)")

	// Engine flags.
	flag.StringVar(&cfg.rateMode, "rate-mode", "original", "Rate mode: original, constant, unlimited")
	flag.Float64Var(&cfg.ratePerSecond, "rate-per-second", 0, "Target RPS (constant mode)")
	flag.Float64Var(&cfg.timeScale, "time-scale", 1.0, "Time scaling (original mode, 1.0=realtime)")
	flag.IntVar(&cfg.concurrency, "concurrency", 10, "Number of parallel senders")

	// Checkpoint flags.
	flag.StringVar(&cfg.checkpointDir, "checkpoint-dir", "/tmp/kapture-checkpoints", "Checkpoint directory")
	flag.StringVar(&cfg.replayID, "replay-id", "", "Replay session ID (required with --resume, auto-generated otherwise)")
	flag.BoolVar(&cfg.resume, "resume", false, "Resume from last checkpoint (requires --replay-id)")

	// Output flags.
	flag.StringVar(&cfg.logLevel, "log-level", "info", "Log level: debug, info, warn, error")
	flag.StringVar(&cfg.logFormat, "log-format", "text", "Log format: text, json")

	flag.Parse()
	return cfg
}

func run(ctx context.Context, cfg config, logger *slog.Logger) error {
	if cfg.captureID == "" {
		return fmt.Errorf("--capture-id is required")
	}
	if cfg.storageType == "" {
		return fmt.Errorf("--storage-type is required")
	}

	// Validate mode: either --serve or --target-url must be specified.
	if !cfg.serve && cfg.targetURL == "" {
		return fmt.Errorf("either --target-url (direct mode) or --serve (feed server mode) is required")
	}

	// Validate --resume requires --replay-id to find the right checkpoint.
	if cfg.resume {
		return fmt.Errorf("--resume is not yet implemented: checkpoint state is recorded but reader seek-to-position is not wired up; track progress via checkpoints and manually set --start-time on restart")
	}

	// Generate replay ID if not provided.
	if cfg.replayID == "" {
		cfg.replayID = uuid.New().String()[:8]
	}

	logger.Info("starting replay engine",
		"replayId", cfg.replayID,
		"captureId", cfg.captureID,
		"storageType", cfg.storageType,
		"rateMode", cfg.rateMode,
		"serve", cfg.serve)

	// Build reader from storage config.
	readerConfig := buildReaderConfig(cfg)
	reader, err := replay.NewReaderFromConfig(ctx, cfg.storageType, readerConfig)
	if err != nil {
		return fmt.Errorf("create reader: %w", err)
	}
	defer reader.Close()

	// Feed server mode: start HTTP server and block.
	if cfg.serve {
		return runFeedServer(ctx, cfg, reader, logger)
	}

	// Direct mode: use the replay engine.
	return runDirectReplay(ctx, cfg, reader, logger)
}

func runFeedServer(ctx context.Context, cfg config, reader replay.Reader, logger *slog.Logger) error {
	rateMode, err := parseRateMode(cfg.rateMode)
	if err != nil {
		return err
	}

	readOpts, err := buildReadOptions(cfg)
	if err != nil {
		return err
	}

	server, err := replay.NewFeedServer(replay.FeedServerConfig{
		Reader:     reader,
		ReadOpts:   readOpts,
		RateMode:   rateMode,
		TimeScale:  cfg.timeScale,
		Addr:       cfg.addr,
		BufferSize: cfg.concurrency * 100,
		Logger:     logger,
	})
	if err != nil {
		return fmt.Errorf("create feed server: %w", err)
	}

	if err := server.Start(ctx); err != nil {
		return fmt.Errorf("start feed server: %w", err)
	}

	logger.Info("feed server running", "addr", cfg.addr)

	// Block until signal.
	<-ctx.Done()
	logger.Info("shutting down feed server")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return server.Stop(shutdownCtx)
}

func runDirectReplay(ctx context.Context, cfg config, reader replay.Reader, logger *slog.Logger) error {
	// Build HTTP sender.
	sender, err := replay.NewHTTPSender(replay.HTTPSenderConfig{
		TargetBaseURL:   cfg.targetURL,
		OverrideHeaders: parseOverrideHeaders(cfg.overrideHeaders),
	})
	if err != nil {
		return fmt.Errorf("create sender: %w", err)
	}
	defer sender.Close()

	// Set up checkpointer.
	checkpointer, err := replay.NewCheckpointer(replay.CheckpointerConfig{
		Dir:    cfg.checkpointDir,
		Logger: logger,
	})
	if err != nil {
		return fmt.Errorf("create checkpointer: %w", err)
	}

	// Build read options.
	readOpts, err := buildReadOptions(cfg)
	if err != nil {
		return err
	}

	// Build result handler with checkpointing.
	baseHandler := replay.NewDefaultResultHandler(logger)
	resultHandler := replay.NewCheckpointResultHandler(baseHandler, checkpointer)

	// Build engine.
	rateMode, err := parseRateMode(cfg.rateMode)
	if err != nil {
		return err
	}

	engine, err := replay.NewEngine(replay.EngineConfig{
		Reader:        reader,
		Sender:        sender,
		ResultHandler: resultHandler,
		ReadOptions:   readOpts,
		RateMode:      rateMode,
		RatePerSecond: cfg.ratePerSecond,
		TimeScale:     cfg.timeScale,
		Concurrency:   cfg.concurrency,
		Logger:        logger,
	})
	if err != nil {
		return fmt.Errorf("create engine: %w", err)
	}

	// Start checkpoint.
	cp := &replay.Checkpoint{
		ReplayID:  cfg.replayID,
		CaptureID: cfg.captureID,
		StartTime: time.Now().UTC(),
		EngineParams: replay.CheckpointEngineParams{
			RateMode:      rateMode,
			RatePerSecond: cfg.ratePerSecond,
			TimeScale:     cfg.timeScale,
			Concurrency:   cfg.concurrency,
			StorageType:   cfg.storageType,
		},
	}
	checkpointer.Start(cp)

	// Start periodic progress reporting.
	go reportProgress(ctx, engine, logger)

	// Run the replay.
	summary, err := engine.Run(ctx)

	// Stop checkpointer regardless of error.
	if stopErr := checkpointer.Stop(); stopErr != nil {
		logger.Warn("checkpoint stop error", "error", stopErr)
	}

	if err != nil {
		return fmt.Errorf("replay engine: %w", err)
	}

	printSummary(summary, logger)

	// Clean up checkpoint on successful completion.
	if summary.ErrorCount == 0 {
		if delErr := checkpointer.Delete(cfg.replayID); delErr != nil {
			logger.Warn("failed to delete checkpoint", "error", delErr)
		}
	}

	return nil
}

func buildReaderConfig(cfg config) map[string]any {
	m := map[string]any{}
	if cfg.bucket != "" {
		m["bucket"] = cfg.bucket
	}
	if cfg.region != "" {
		m["region"] = cfg.region
	}
	if cfg.prefix != "" {
		m["prefix"] = cfg.prefix
	}
	if cfg.mountPath != "" {
		m["mountPath"] = cfg.mountPath
	}
	return m
}

func buildReadOptions(cfg config) (replay.ReadOptions, error) {
	opts := replay.ReadOptions{
		CaptureID:  cfg.captureID,
		PathPrefix: cfg.pathPrefix,
		Limit:      cfg.limit,
	}
	if cfg.startTime != "" {
		t, err := time.Parse(time.RFC3339, cfg.startTime)
		if err != nil {
			return opts, fmt.Errorf("parse start-time: %w", err)
		}
		opts.StartTime = t
	}
	if cfg.endTime != "" {
		t, err := time.Parse(time.RFC3339, cfg.endTime)
		if err != nil {
			return opts, fmt.Errorf("parse end-time: %w", err)
		}
		opts.EndTime = t
	}
	if cfg.methods != "" {
		methods := strings.Split(cfg.methods, ",")
		for i := range methods {
			methods[i] = strings.TrimSpace(methods[i])
		}
		opts.Methods = methods
	}
	return opts, nil
}

func parseRateMode(mode string) (replay.RateMode, error) {
	switch strings.ToLower(mode) {
	case "original", "recorded":
		return replay.RateModeOriginalTiming, nil
	case "constant":
		return replay.RateModeConstant, nil
	case "unlimited", "max":
		return replay.RateModeUnlimited, nil
	default:
		return 0, fmt.Errorf("unknown rate mode %q (use: original, constant, unlimited)", mode)
	}
}

func parseOverrideHeaders(headers string) map[string]string {
	if headers == "" {
		return nil
	}
	m := make(map[string]string)
	for _, pair := range strings.Split(headers, ",") {
		k, v, ok := strings.Cut(pair, "=")
		if ok {
			m[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	return m
}

func reportProgress(ctx context.Context, engine *replay.Engine, logger *slog.Logger) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sent, errored, filtered := engine.Progress()
			logger.Info("replay progress",
				"sent", sent,
				"errored", errored,
				"filtered", filtered)
		}
	}
}

func printSummary(s *replay.Summary, logger *slog.Logger) {
	logger.Info("replay complete",
		"totalRequests", s.TotalRequests,
		"success", s.SuccessCount,
		"errors", s.ErrorCount,
		"filtered", s.FilteredCount,
		"duration", s.TotalDuration,
		"meanLatency", s.MeanLatency,
		"p50Latency", s.P50Latency,
		"p95Latency", s.P95Latency,
		"p99Latency", s.P99Latency)

	if len(s.StatusCodeDist) > 0 {
		logger.Info("status code distribution", "codes", s.StatusCodeDist)
	}
	if len(s.ErrorsByType) > 0 {
		logger.Info("error distribution", "errors", s.ErrorsByType)
	}
}

func setupLogger(level, format string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: lvl}
	var handler slog.Handler
	if format == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}
	return slog.New(handler)
}
