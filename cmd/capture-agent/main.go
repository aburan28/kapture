package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kapture-io/kapture/internal/agent"
	"github.com/kapture-io/kapture/internal/storage"
)

func main() {
	var (
		httpPort      = flag.Int("http-port", 8080, "HTTP sink listen port")
		grpcPort      = flag.Int("grpc-port", 9090, "gRPC sink listen port")
		healthPort    = flag.Int("health-port", 8081, "Health/metrics listen port")
		storageType   = flag.String("storage-type", "efs", "Storage backend type (s3, gcs, efs, ebs, plugin)")
		storageConfig = flag.String("storage-config", "{}", "Storage backend config as JSON")
		captureID     = flag.String("capture-id", "", "Capture ID for file naming")
		maxBodyBytes  = flag.Int64("max-body-bytes", 1<<20, "Max request body size in bytes")
		batchSize     = flag.Int("batch-size", 100, "Buffered writer batch size")
		flushInterval = flag.Duration("flush-interval", 5*time.Second, "Buffered writer flush interval")

		// Proxy-mode flags
		mode          = flag.String("mode", "mirror", "Capture mode: mirror (passive sink) or proxy (inline reverse proxy)")
		upstreamHTTP  = flag.String("upstream-http", "", "Upstream HTTP base URL for proxy mode (e.g. http://my-service:8080)")
		upstreamGRPC  = flag.String("upstream-grpc", "", "Upstream gRPC address for proxy mode (e.g. my-service:9090)")

		// Health check flags
		healthCheckInterval = flag.Duration("health-check-interval", 30*time.Second, "Interval between in-flight health checks")
	)
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if *captureID == "" {
		log.Error("--capture-id is required")
		os.Exit(1)
	}

	captureMode := agent.CaptureMode(*mode)
	if captureMode == agent.CaptureModeProxy && *upstreamHTTP == "" && *upstreamGRPC == "" {
		log.Error("proxy mode requires at least one of --upstream-http or --upstream-grpc")
		os.Exit(1)
	}

	// Parse storage config JSON into the correct type
	storageCfg, err := parseStorageConfig(*storageType, *storageConfig)
	if err != nil {
		log.Error("failed to parse storage config", "error", err)
		os.Exit(1)
	}

	factory, err := storage.NewWriterFactory(*storageType, storageCfg)
	if err != nil {
		log.Error("failed to create storage writer factory", "error", err)
		os.Exit(1)
	}

	writer, err := factory.NewWriter(context.Background(), *captureID)
	if err != nil {
		log.Error("failed to create storage writer", "error", err)
		os.Exit(1)
	}

	bufferedWriter := agent.NewBufferedWriter(agent.BufferedWriterConfig{
		Writer:        writer,
		BatchSize:     *batchSize,
		FlushInterval: *flushInterval,
		Logger:        log,
	})

	handler := agent.NewCaptureHandler(agent.CaptureHandlerConfig{
		Writer:       bufferedWriter,
		MaxBodyBytes: *maxBodyBytes,
		Logger:       log,
	})

	// Set up health checker with pre-flight and in-flight probes.
	healthChecker := agent.NewHealthChecker(agent.HealthCheckerConfig{
		WriterFactory: factory,
		StorageType:   *storageType,
		UpstreamAddr:  *upstreamHTTP,
		Logger:        log,
	})

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Run pre-flight checks before accepting traffic.
	if err := healthChecker.RunPreflightChecks(ctx); err != nil {
		log.Error("pre-flight health checks failed", "error", err)
		os.Exit(1)
	}

	// Start background in-flight health checks.
	go healthChecker.StartBackgroundChecks(ctx, *healthCheckInterval)

	server := agent.NewAgentServer(agent.AgentServerConfig{
		HTTPPort:      *httpPort,
		GRPCPort:      *grpcPort,
		HealthPort:    *healthPort,
		Handler:       handler,
		Logger:        log,
		Mode:          captureMode,
		UpstreamHTTP:  *upstreamHTTP,
		UpstreamGRPC:  *upstreamGRPC,
		HealthChecker: healthChecker,
	})

	log.Info("starting capture agent",
		"captureID", *captureID,
		"httpPort", *httpPort,
		"grpcPort", *grpcPort,
		"healthPort", *healthPort,
		"storageType", *storageType,
		"mode", *mode,
		"upstreamHTTP", *upstreamHTTP,
		"upstreamGRPC", *upstreamGRPC,
	)

	if err := server.Start(ctx); err != nil {
		log.Error("server exited with error", "error", err)
	}

	log.Info("shutting down buffered writer")
	if err := bufferedWriter.Close(); err != nil {
		log.Error("buffered writer close error", "error", err)
	}

	log.Info("capture agent stopped")
}

func parseStorageConfig(storageType, rawJSON string) (any, error) {
	switch storageType {
	case "s3":
		var cfg storage.S3Config
		if err := json.Unmarshal([]byte(rawJSON), &cfg); err != nil {
			return nil, fmt.Errorf("parse s3 config: %w", err)
		}
		return cfg, nil
	case "gcs":
		var cfg storage.GCSConfig
		if err := json.Unmarshal([]byte(rawJSON), &cfg); err != nil {
			return nil, fmt.Errorf("parse gcs config: %w", err)
		}
		return cfg, nil
	case "efs":
		var cfg storage.EFSConfig
		if err := json.Unmarshal([]byte(rawJSON), &cfg); err != nil {
			return nil, fmt.Errorf("parse efs config: %w", err)
		}
		return cfg, nil
	case "ebs":
		var cfg storage.EBSConfig
		if err := json.Unmarshal([]byte(rawJSON), &cfg); err != nil {
			return nil, fmt.Errorf("parse ebs config: %w", err)
		}
		return cfg, nil
	case "plugin":
		var cfg storage.PluginConfig
		if err := json.Unmarshal([]byte(rawJSON), &cfg); err != nil {
			return nil, fmt.Errorf("parse plugin config: %w", err)
		}
		return cfg, nil
	default:
		return nil, fmt.Errorf("unsupported storage type %q", storageType)
	}
}
