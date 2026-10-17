package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/kapture-io/kapture/internal/agent"
	"github.com/kapture-io/kapture/internal/ebpf"
	"github.com/kapture-io/kapture/internal/history"
	"github.com/kapture-io/kapture/internal/l7"
	"github.com/kapture-io/kapture/internal/storage"
)

const historyUpdateTimeout = 5 * time.Second

func main() {
	var (
		backend          = flag.String("backend", envOr("CAPTURE_BACKEND", "Mirror"), "Capture backend: Mirror or EBPF")
		ecaptureBinary   = flag.String("ecapture-binary", envOr("ECAPTURE_BINARY", "/ecapture"), "Pinned eCapture executable")
		ecapturePID      = flag.Int("ecapture-pid", envIntOr("ECAPTURE_PID", 0), "Optional host PID within the target pod; required outside Kubernetes")
		ecaptureLibrary  = flag.String("ecapture-library", envOr("ECAPTURE_LIBRARY", ""), "Optional OpenSSL library path; otherwise discover from process maps")
		ecaptureURL      = flag.String("ecapture-url", "", "Consume an already running typed eCaptureQ endpoint")
		l7Input          = flag.String("l7-input", "", "Read bounded JSONL observations for offline reconstruction")
		l7Memory         = flag.Int("l7-memory-bytes", envIntOr("L7_MEMORY_BYTES", 128<<20), "Maximum parser memory budget")
		l7History        = flag.Duration("l7-history", envDurationOr("L7_HISTORY", 30*time.Second), "Pre-trigger exchange history window")
		l7HistoryBytes   = flag.Int("l7-history-bytes", envIntOr("L7_HISTORY_BYTES", 32<<20), "Maximum retained exchange history bytes")
		l7Trigger        = flag.Int("l7-trigger-status", envIntOr("L7_TRIGGER_STATUS", 0), "HTTP status trigger minimum; zero captures every exchange")
		l7PCAP           = flag.Bool("l7-pcap", envOr("L7_PCAP", "true") == "true", "Export a labeled, reconstructed plaintext PCAPNG per exchange")
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
		maxBodyBytes     = flag.Int64("max-body-bytes", int64(envIntOr("CAPTURE_MAX_BODY_BYTES", 1<<20)), "Max request body size in bytes")
		batchSize        = flag.Int("batch-size", 100, "Buffered writer batch size")
		flushInterval    = flag.Duration("flush-interval", 5*time.Second, "Buffered writer flush interval")
		filterPathPrefix = flag.String("filter-path-prefix", envOr("FILTER_PATH_PREFIX", ""), "Only capture requests whose path starts with this prefix")
		filterHeaders    = flag.String("filter-headers", envOr("FILTER_HEADERS", ""), `Header filters as JSON array, e.g. [{"name":"x-debug","value":"true"}]`)
		filterPercentage = flag.Int("filter-percentage", envIntOr("FILTER_PERCENTAGE", 100), "Percentage of requests to capture (0-100)")
		writeQueueSize   = flag.Int("write-queue-size", envIntOr("CAPTURE_WRITE_QUEUE_SIZE", agent.DefaultWriteQueueSize), "Bounded write queue size between capture and storage; new captures are dropped (and counted) when full")
	)
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if *backend != "Mirror" && *backend != "EBPF" {
		log.Error("backend must be Mirror or EBPF")
		os.Exit(1)
	}
	if *backend == "EBPF" && (*maxBodyBytes < 0 || *maxBodyBytes > 64<<20 || *l7Memory < 1<<20 || *l7HistoryBytes < 1<<20 || *l7Trigger < 0 || *l7Trigger > 599 || *l7History < 0) {
		log.Error("invalid capture size, trigger or retention limit")
		os.Exit(1)
	}

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

	if strings.TrimSpace(*databaseURL) != "" && (strings.TrimSpace(*captureNamespace) == "" || strings.TrimSpace(*captureName) == "") {
		log.Error("capture namespace and name are required when capture history is enabled", "captureID", *captureID)
		os.Exit(1)
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

	// The async queue keeps the capture hot path non-blocking: storage
	// stalls fill the bounded queue and then drop (counted) instead of
	// stalling mirrored traffic.
	asyncWriter := agent.NewAsyncWriter(bufferedWriter, *writeQueueSize, log)

	headerFilters, err := agent.ParseHeaderFilters(*filterHeaders)
	if err != nil {
		log.Error("failed to parse header filters", "error", err)
		os.Exit(1)
	}
	var requestFilter *agent.RequestFilter
	if *filterPathPrefix != "" || len(headerFilters) > 0 || *filterPercentage < 100 {
		requestFilter = agent.NewRequestFilter(agent.RequestFilterConfig{
			PathPrefix: *filterPathPrefix,
			Headers:    headerFilters,
			Percentage: *filterPercentage,
		})
	}

	handler := agent.NewCaptureHandler(agent.CaptureHandlerConfig{
		Writer:        asyncWriter,
		MaxBodyBytes:  *maxBodyBytes,
		Filter:        requestFilter,
		RedactHeaders: parseRedactHeaders(os.Getenv("CAPTURE_REDACT_HEADERS")),
		Logger:        log,
	})

	server := agent.NewAgentServer(agent.AgentServerConfig{
		HTTPPort:   *httpPort,
		GRPCPort:   *grpcPort,
		HealthPort: *healthPort,
		Handler:    handler,
		Queue:      asyncWriter,
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

	var serverErr error
	if *backend == "EBPF" {
		artifacts, ok := factory.(storage.ArtifactWriter)
		if !ok {
			serverErr = fmt.Errorf("storage backend does not support L7 evidence artifacts")
		} else {
			serverErr = ebpf.Run(ctx, ebpf.Config{CaptureID: *captureID, Binary: *ecaptureBinary, PID: *ecapturePID, Library: *ecaptureLibrary, PodUID: os.Getenv("TARGET_POD_UID"), MapPages: envIntOr("ECAPTURE_MAP_PAGES", 256), SourceURL: *ecaptureURL, InputFile: *l7Input, HealthAddress: fmt.Sprintf(":%d", *healthPort), Engine: l7.Config{MaxBodyBytes: max(1, int(*maxBodyBytes)), MaxMemoryBytes: *l7Memory}, Export: l7.ExportConfig{CaptureID: *captureID, Writer: bufferedWriter, Artifacts: artifacts, Filter: requestFilter, RedactHeaders: l7.RedactionList(os.Getenv("CAPTURE_REDACT_HEADERS")), IncludeHeaders: envOr("CAPTURE_INCLUDE_HEADERS", "true") == "true", IncludeBody: *maxBodyBytes > 0 && envOr("CAPTURE_INCLUDE_BODY", "true") == "true", PCAP: *l7PCAP, History: *l7History, HistoryBytes: *l7HistoryBytes, TriggerStatus: *l7Trigger}})
		}
	} else {
		serverErr = server.Start(ctx)
	}
	if serverErr != nil {
		log.Error("server exited with error", "error", serverErr)
	}

	log.Info("shutting down capture write pipeline")
	// Closing the async writer drains its queue and closes the buffered
	// writer underneath.
	if err := asyncWriter.Close(); err != nil {
		log.Error("write pipeline close error", "error", err)
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
		historyCtx, historyCancel := context.WithTimeout(context.Background(), historyUpdateTimeout)
		defer historyCancel()
		if err := historyRepo.UpsertCapture(historyCtx, history.Capture{
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

// parseRedactHeaders interprets CAPTURE_REDACT_HEADERS: unset/empty keeps
// the agent's default credential redaction, "none" disables all redaction,
// anything else is a comma-separated list of extra headers to redact on
// top of the defaults.
func parseRedactHeaders(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil // handler applies DefaultRedactHeaders
	}
	if strings.EqualFold(value, "none") {
		return []string{} // non-nil empty: redaction disabled
	}
	headers := append([]string{}, agent.DefaultRedactHeaders...)
	for _, h := range strings.Split(value, ",") {
		if h = strings.TrimSpace(h); h != "" {
			headers = append(headers, h)
		}
	}
	return headers
}

func envOr(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envIntOr(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envDurationOr(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return -1
	}
	return d
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
