package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/kapture-io/kapture/internal/agent"
	"github.com/kapture-io/kapture/internal/history"
	"github.com/kapture-io/kapture/internal/storage"
)

func main() {
	var (
		httpPort         = flag.Int("http-port", 8080, "HTTP sink listen port")
		grpcPort         = flag.Int("grpc-port", 9090, "gRPC sink listen port")
		healthPort       = flag.Int("health-port", 8081, "Health/metrics listen port")
		storageType      = flag.String("storage-type", envOr("STORAGE_TYPE", "efs"), "Storage backend type (s3, gcs, efs, ebs, plugin)")
		storageConfig    = flag.String("storage-config", envOr("STORAGE_CONFIG", "{}"), "Storage backend config as JSON")
		captureID        = flag.String("capture-id", envOr("CAPTURE_ID", ""), "Capture ID for file naming")
		captureNamespace = flag.String("capture-namespace", envOr("CAPTURE_NAMESPACE", ""), "TrafficCapture namespace for history records")
		captureName      = flag.String("capture-name", envOr("CAPTURE_NAME", ""), "TrafficCapture name for history records")
		targetKind       = flag.String("target-kind", envOr("CAPTURE_TARGET_KIND", ""), "Captured Gateway API route kind")
		targetName       = flag.String("target-name", envOr("CAPTURE_TARGET_NAME", ""), "Captured Gateway API route name")
		storageName      = flag.String("storage-name", envOr("CAPTURE_STORAGE_NAME", ""), "CaptureStorage name")
		databaseURL      = flag.String("database-url", envOr("DATABASE_URL", ""), "PostgreSQL/RDS connection URL for capture history")
		maxBodyBytes     = flag.Int64("max-body-bytes", 1<<20, "Max request body size in bytes")
		batchSize        = flag.Int("batch-size", 100, "Buffered writer batch size")
		flushInterval    = flag.Duration("flush-interval", 5*time.Second, "Buffered writer flush interval")
	)
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if *captureID == "" {
		log.Error("--capture-id or CAPTURE_ID is required")
		os.Exit(1)
	}

	if *captureNamespace == "" || *captureName == "" {
		ns, name := splitCaptureID(*captureID)
		if *captureNamespace == "" {
			*captureNamespace = ns
		}
		if *captureName == "" {
			*captureName = name
		}
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Parse storage config JSON into the correct type, with env var fallbacks
	// supplied by the spoke controller deployment builder.
	storageCfg, err := parseStorageConfig(*storageType, *storageConfig)
	if err != nil {
		log.Error("failed to parse storage config", "error", err)
		os.Exit(1)
	}

	var historyRepo *history.PostgresRepository
	if strings.TrimSpace(*databaseURL) != "" {
		historyRepo, err = history.NewPostgresRepository(ctx, *databaseURL)
		if err != nil {
			log.Error("failed to connect capture history database", "error", err)
			os.Exit(1)
		}
		defer historyRepo.Close()

		now := time.Now().UTC()
		if err := historyRepo.UpsertCapture(ctx, history.Capture{
			ID:          *captureID,
			Namespace:   *captureNamespace,
			Name:        *captureName,
			Status:      history.StatusRunning,
			TargetKind:  *targetKind,
			TargetName:  *targetName,
			StorageName: *storageName,
			StorageType: strings.ToLower(*storageType),
			StartedAt:   &now,
		}); err != nil {
			log.Error("failed to record capture start", "error", err)
			os.Exit(1)
		}
	}

	storageCfg = attachHistoryRecorder(storageCfg, historyRepo, *captureNamespace, *captureName)

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

	server := agent.NewAgentServer(agent.AgentServerConfig{
		HTTPPort:   *httpPort,
		GRPCPort:   *grpcPort,
		HealthPort: *healthPort,
		Handler:    handler,
		Logger:     log,
	})

	log.Info("starting capture agent",
		"captureID", *captureID,
		"httpPort", *httpPort,
		"grpcPort", *grpcPort,
		"healthPort", *healthPort,
		"storageType", *storageType,
		"historyEnabled", historyRepo != nil,
	)

	serverErr := server.Start(ctx)
	if serverErr != nil {
		log.Error("server exited with error", "error", serverErr)
	}

	log.Info("shutting down buffered writer")
	if err := bufferedWriter.Close(); err != nil {
		log.Error("buffered writer close error", "error", err)
		if serverErr == nil {
			serverErr = err
		}
	}

	if historyRepo != nil {
		now := time.Now().UTC()
		status := history.StatusCompleted
		errMsg := ""
		if serverErr != nil {
			status = history.StatusFailed
			errMsg = serverErr.Error()
		}
		if err := historyRepo.UpsertCapture(context.Background(), history.Capture{
			ID:          *captureID,
			Namespace:   *captureNamespace,
			Name:        *captureName,
			Status:      status,
			TargetKind:  *targetKind,
			TargetName:  *targetName,
			StorageName: *storageName,
			StorageType: strings.ToLower(*storageType),
			CompletedAt: &now,
			Error:       errMsg,
		}); err != nil {
			log.Error("failed to record capture completion", "error", err)
		}
	}

	log.Info("capture agent stopped")
	if serverErr != nil {
		os.Exit(1)
	}
}

func parseStorageConfig(storageType, rawJSON string) (any, error) {
	switch strings.ToLower(strings.TrimSpace(storageType)) {
	case "s3":
		var cfg storage.S3Config
		if err := unmarshalOptionalConfig(rawJSON, &cfg); err != nil {
			return nil, fmt.Errorf("parse s3 config: %w", err)
		}
		cfg.Bucket = firstNonEmpty(cfg.Bucket, os.Getenv("S3_BUCKET"))
		cfg.Region = firstNonEmpty(cfg.Region, os.Getenv("S3_REGION"))
		cfg.Prefix = firstNonEmpty(cfg.Prefix, os.Getenv("S3_PREFIX"))
		cfg.AgentPod = firstNonEmpty(cfg.AgentPod, os.Getenv("POD_NAME"), os.Getenv("HOSTNAME"))
		return cfg, nil
	case "gcs":
		var cfg storage.GCSConfig
		if err := unmarshalOptionalConfig(rawJSON, &cfg); err != nil {
			return nil, fmt.Errorf("parse gcs config: %w", err)
		}
		cfg.Bucket = firstNonEmpty(cfg.Bucket, os.Getenv("GCS_BUCKET"))
		cfg.Prefix = firstNonEmpty(cfg.Prefix, os.Getenv("GCS_PREFIX"))
		cfg.AgentPod = firstNonEmpty(cfg.AgentPod, os.Getenv("POD_NAME"), os.Getenv("HOSTNAME"))
		return cfg, nil
	case "efs":
		var cfg storage.EFSConfig
		if err := unmarshalOptionalConfig(rawJSON, &cfg); err != nil {
			return nil, fmt.Errorf("parse efs config: %w", err)
		}
		cfg.MountPath = firstNonEmpty(cfg.MountPath, os.Getenv("EFS_MOUNT_PATH"))
		cfg.AgentPod = firstNonEmpty(cfg.AgentPod, os.Getenv("POD_NAME"), os.Getenv("HOSTNAME"))
		return cfg, nil
	case "ebs":
		var cfg storage.EBSConfig
		if err := unmarshalOptionalConfig(rawJSON, &cfg); err != nil {
			return nil, fmt.Errorf("parse ebs config: %w", err)
		}
		cfg.MountPath = firstNonEmpty(cfg.MountPath, os.Getenv("EBS_MOUNT_PATH"))
		cfg.AgentPod = firstNonEmpty(cfg.AgentPod, os.Getenv("POD_NAME"), os.Getenv("HOSTNAME"))
		return cfg, nil
	case "plugin":
		var cfg storage.PluginConfig
		if err := unmarshalOptionalConfig(rawJSON, &cfg); err != nil {
			return nil, fmt.Errorf("parse plugin config: %w", err)
		}
		cfg.Path = firstNonEmpty(cfg.Path, os.Getenv("PLUGIN_PATH"))
		cfg.AgentPod = firstNonEmpty(cfg.AgentPod, os.Getenv("POD_NAME"), os.Getenv("HOSTNAME"))
		return cfg, nil
	default:
		return nil, fmt.Errorf("unsupported storage type %q", storageType)
	}
}

func unmarshalOptionalConfig(rawJSON string, target any) error {
	rawJSON = strings.TrimSpace(rawJSON)
	if rawJSON == "" || rawJSON == "{}" {
		return nil
	}
	return json.Unmarshal([]byte(rawJSON), target)
}

func attachHistoryRecorder(config any, recorder storage.ArtifactRecorder, namespace, name string) any {
	switch cfg := config.(type) {
	case storage.S3Config:
		cfg.ArtifactRecorder = recorder
		cfg.CaptureNamespace = namespace
		cfg.CaptureName = name
		return cfg
	default:
		return config
	}
}

func splitCaptureID(captureID string) (string, string) {
	namespace, name, ok := strings.Cut(captureID, "/")
	if !ok {
		return "", captureID
	}
	return namespace, name
}

func envOr(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
