// Package main implements a replay plugin that replays captured gRPC traffic
// using ghz (https://ghz.sh), a gRPC benchmarking and load testing tool.
// For HTTP traffic it falls back to direct HTTP replay.
//
// The plugin collects captured gRPC requests and generates ghz-compatible
// JSON call data files, then optionally invokes ghz for load testing.
//
// Build as a Go plugin:
//
//	go build -buildmode=plugin -o ghz.so ./examples/plugins/ghz
//
// Settings:
//
//	output_dir         - Directory for generated ghz call data files (default: "ghz-data")
//	concurrency        - Number of concurrent ghz workers (default: "50")
//	total_requests     - Total number of requests ghz should send (default: "1000")
//	connections         - Number of gRPC connections (default: "1")
//	timeout            - Per-request timeout (default: "30s")
//	insecure           - Skip TLS verification (default: "false")
//	proto_path         - Path to .proto files for reflection (optional)
//	import_paths       - Comma-separated proto import paths (optional)
//	metadata           - Additional gRPC metadata as key=value pairs, comma-separated (optional)
//	http_timeout       - Timeout for fallback HTTP replay (default: "30s")
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/kapture-io/kapture/internal/replay"
	"github.com/kapture-io/kapture/internal/storage"
)

type ghzPlugin struct {
	log       *slog.Logger
	mu        sync.Mutex
	targetURL string
	client    *http.Client

	// ghz settings
	outputDir     string
	concurrency   string
	totalRequests string
	connections   string
	timeout       string
	insecure      bool
	protoPath     string
	importPaths   []string
	metadata      map[string]string

	// Collected gRPC calls, keyed by gRPC service method.
	grpcCalls map[string][]grpcCallData
}

// grpcCallData holds a single gRPC call's data for ghz replay.
type grpcCallData struct {
	RequestID string            `json:"-"`
	Payload   json.RawMessage   `json:"payload"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// ghzConfig is the JSON structure ghz accepts for batch configuration.
type ghzConfig struct {
	Call          string            `json:"call"`
	Host         string            `json:"host"`
	Proto        string            `json:"proto,omitempty"`
	ImportPaths  []string          `json:"import-paths,omitempty"`
	Insecure     bool              `json:"insecure"`
	Concurrency  string            `json:"concurrency"`
	TotalReqs    string            `json:"total"`
	Connections  string            `json:"connections"`
	Timeout      string            `json:"timeout"`
	Metadata     map[string]string `json:"metadata,omitempty"`
	DataFile     string            `json:"data-file"`
	RequestCount int               `json:"request-count"`
}

// NewReplayPlugin is the exported constructor required by the plugin contract.
func NewReplayPlugin() (replay.ReplayPlugin, error) {
	return &ghzPlugin{
		log:           slog.Default(),
		outputDir:     "ghz-data",
		concurrency:   "50",
		totalRequests: "1000",
		connections:   "1",
		timeout:       "30s",
		grpcCalls:     make(map[string][]grpcCallData),
		metadata:      make(map[string]string),
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}, nil
}

func (p *ghzPlugin) Name() string { return "ghz" }

func (p *ghzPlugin) Init(_ context.Context, cfg replay.PluginConfig) error {
	p.targetURL = cfg.TargetURL

	if v, ok := cfg.Settings["output_dir"]; ok {
		p.outputDir = v
	}
	if v, ok := cfg.Settings["concurrency"]; ok {
		p.concurrency = v
	}
	if v, ok := cfg.Settings["total_requests"]; ok {
		p.totalRequests = v
	}
	if v, ok := cfg.Settings["connections"]; ok {
		p.connections = v
	}
	if v, ok := cfg.Settings["timeout"]; ok {
		p.timeout = v
	}
	if cfg.Settings["insecure"] == "true" {
		p.insecure = true
	}
	if v, ok := cfg.Settings["proto_path"]; ok {
		p.protoPath = v
	}
	if v, ok := cfg.Settings["import_paths"]; ok {
		p.importPaths = strings.Split(v, ",")
	}
	if v, ok := cfg.Settings["metadata"]; ok {
		for _, pair := range strings.Split(v, ",") {
			kv := strings.SplitN(strings.TrimSpace(pair), "=", 2)
			if len(kv) == 2 {
				p.metadata[kv[0]] = kv[1]
			}
		}
	}
	if v, ok := cfg.Settings["http_timeout"]; ok {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("parse http_timeout %q: %w", v, err)
		}
		p.client.Timeout = d
	}

	p.log.Info("ghz plugin initialized",
		"targetURL", p.targetURL,
		"outputDir", p.outputDir,
		"concurrency", p.concurrency,
		"totalRequests", p.totalRequests,
	)
	return nil
}

func (p *ghzPlugin) ProcessRequest(ctx context.Context, req *replay.ReplayRequest) (*replay.ReplayResult, error) {
	orig := req.Original

	if orig.Protocol == "gRPC" {
		return p.processGRPC(req)
	}
	return p.processHTTP(ctx, orig)
}

// processGRPC collects gRPC request data for batch ghz replay.
func (p *ghzPlugin) processGRPC(req *replay.ReplayRequest) (*replay.ReplayResult, error) {
	orig := req.Original

	// Extract the gRPC method from the path (e.g., "/package.Service/Method").
	method := strings.TrimPrefix(orig.Path, "/")
	if method == "" {
		return &replay.ReplayResult{
			RequestID: orig.ID,
			Status:    replay.ReplayStatusSkipped,
			Metadata:  map[string]string{"reason": "empty_grpc_method"},
		}, nil
	}

	// The body is the serialized protobuf message. For ghz we store it
	// as a JSON payload. If the body is valid JSON (e.g., from gRPC-JSON
	// transcoding), use it directly; otherwise base64-encode the raw bytes.
	var payload json.RawMessage
	if json.Valid(orig.Body) {
		payload = orig.Body
	} else {
		// Wrap raw protobuf bytes as a JSON string for ghz binary data mode.
		encoded, _ := json.Marshal(orig.Body)
		payload = encoded
	}

	// Collect gRPC metadata from the captured request headers.
	md := make(map[string]string)
	for k, vals := range orig.Headers {
		lower := strings.ToLower(k)
		// Skip HTTP/2 pseudo-headers and standard gRPC headers.
		if strings.HasPrefix(lower, ":") || lower == "content-type" || lower == "te" || lower == "user-agent" {
			continue
		}
		if len(vals) > 0 {
			md[k] = vals[0]
		}
	}

	call := grpcCallData{
		RequestID: orig.ID,
		Payload:   payload,
		Metadata:  md,
	}

	p.mu.Lock()
	p.grpcCalls[method] = append(p.grpcCalls[method], call)
	p.mu.Unlock()

	return &replay.ReplayResult{
		RequestID: orig.ID,
		Status:    replay.ReplayStatusSuccess,
		Metadata: map[string]string{
			"action":      "collected_grpc",
			"grpc_method": method,
			"sequence":    fmt.Sprintf("%d", req.Sequence),
		},
	}, nil
}

// processHTTP replays HTTP requests directly against the target.
func (p *ghzPlugin) processHTTP(ctx context.Context, orig *storage.CapturedRequest) (*replay.ReplayResult, error) {
	if p.targetURL == "" {
		return &replay.ReplayResult{
			RequestID: orig.ID,
			Status:    replay.ReplayStatusSkipped,
			Metadata:  map[string]string{"reason": "no_target_url"},
		}, nil
	}

	url := p.targetURL + orig.Path

	httpReq, err := http.NewRequestWithContext(ctx, orig.Method, url, bytes.NewReader(orig.Body))
	if err != nil {
		return &replay.ReplayResult{
			RequestID: orig.ID,
			Status:    replay.ReplayStatusError,
			Error:     fmt.Sprintf("create http request: %v", err),
		}, nil
	}

	for k, vals := range orig.Headers {
		for _, v := range vals {
			httpReq.Header.Add(k, v)
		}
	}

	start := time.Now()
	resp, err := p.client.Do(httpReq)
	duration := time.Since(start)

	if err != nil {
		return &replay.ReplayResult{
			RequestID: orig.ID,
			Status:    replay.ReplayStatusError,
			Duration:  duration,
			Error:     fmt.Sprintf("send http request: %v", err),
		}, nil
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	return &replay.ReplayResult{
		RequestID:       orig.ID,
		Status:          replay.ReplayStatusSuccess,
		StatusCode:      resp.StatusCode,
		Duration:        duration,
		ResponseHeaders: resp.Header,
		ResponseBody:    body,
	}, nil
}

// Close writes collected gRPC call data to disk as ghz-compatible files.
func (p *ghzPlugin) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.client.CloseIdleConnections()

	if len(p.grpcCalls) == 0 {
		p.log.Info("ghz plugin: no gRPC requests collected")
		return nil
	}

	if err := os.MkdirAll(p.outputDir, 0o755); err != nil {
		return fmt.Errorf("create output directory %q: %w", p.outputDir, err)
	}

	host := p.targetURL
	if host == "" {
		host = "localhost:50051"
	}
	// Strip scheme for gRPC host.
	host = strings.TrimPrefix(host, "http://")
	host = strings.TrimPrefix(host, "https://")

	var configs []ghzConfig

	for method, calls := range p.grpcCalls {
		// Write the call data as a JSON array file.
		payloads := make([]json.RawMessage, len(calls))
		for i, c := range calls {
			payloads[i] = c.Payload
		}

		safeName := strings.ReplaceAll(method, "/", "_")
		dataFileName := fmt.Sprintf("%s_data.json", safeName)
		dataFilePath := filepath.Join(p.outputDir, dataFileName)

		data, err := json.MarshalIndent(payloads, "", "  ")
		if err != nil {
			return fmt.Errorf("marshal call data for %q: %w", method, err)
		}

		if err := os.WriteFile(dataFilePath, data, 0o644); err != nil {
			return fmt.Errorf("write call data file %q: %w", dataFilePath, err)
		}

		cfg := ghzConfig{
			Call:         method,
			Host:         host,
			Proto:        p.protoPath,
			ImportPaths:  p.importPaths,
			Insecure:     p.insecure,
			Concurrency:  p.concurrency,
			TotalReqs:    p.totalRequests,
			Connections:  p.connections,
			Timeout:      p.timeout,
			DataFile:     dataFilePath,
			RequestCount: len(calls),
		}
		if len(p.metadata) > 0 {
			cfg.Metadata = p.metadata
		}

		configs = append(configs, cfg)

		p.log.Info("ghz call data written",
			"method", method,
			"requests", len(calls),
			"dataFile", dataFilePath,
		)
	}

	// Write a summary config file.
	configPath := filepath.Join(p.outputDir, "ghz-config.json")
	configData, err := json.MarshalIndent(configs, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal ghz config: %w", err)
	}
	if err := os.WriteFile(configPath, configData, 0o644); err != nil {
		return fmt.Errorf("write ghz config %q: %w", configPath, err)
	}

	p.log.Info("ghz plugin complete",
		"configFile", configPath,
		"methods", len(p.grpcCalls),
		"totalCalls", p.totalCallCount(),
	)
	return nil
}

func (p *ghzPlugin) totalCallCount() int {
	total := 0
	for _, calls := range p.grpcCalls {
		total += len(calls)
	}
	return total
}
