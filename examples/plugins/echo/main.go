// Package main implements an example replay plugin that logs each request
// and optionally replays it against a target service via HTTP.
//
// Build as a Go plugin:
//
//	go build -buildmode=plugin -o echo.so ./examples/plugins/echo
//
// The resulting echo.so can be loaded by the kapture replay engine.
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/kapture-io/kapture/internal/replay"
	"github.com/kapture-io/kapture/internal/storage"
)

// echoPlugin logs each replayed request and optionally sends it to a target.
type echoPlugin struct {
	log       *slog.Logger
	targetURL string
	client    *http.Client
	storage   replay.StorageAccessor
	dryRun    bool
}

// NewReplayPlugin is the exported constructor required by the plugin contract.
// The replay engine looks up this symbol when loading the .so file.
func NewReplayPlugin() (replay.ReplayPlugin, error) {
	return &echoPlugin{
		log: slog.Default(),
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}, nil
}

func (p *echoPlugin) Name() string { return "echo" }

func (p *echoPlugin) Init(_ context.Context, cfg replay.PluginConfig) error {
	p.targetURL = cfg.TargetURL
	p.storage = cfg.Storage

	if cfg.Settings["dry_run"] == "true" {
		p.dryRun = true
	}

	if timeout, ok := cfg.Settings["timeout"]; ok {
		d, err := time.ParseDuration(timeout)
		if err != nil {
			return fmt.Errorf("parse timeout %q: %w", timeout, err)
		}
		p.client.Timeout = d
	}

	p.log.Info("echo plugin initialized",
		"targetURL", p.targetURL,
		"dryRun", p.dryRun,
	)
	return nil
}

func (p *echoPlugin) ProcessRequest(ctx context.Context, req *replay.ReplayRequest) (*replay.ReplayResult, error) {
	orig := req.Original

	p.log.Info("processing request",
		"sequence", req.Sequence,
		"method", orig.Method,
		"path", orig.Path,
		"protocol", orig.Protocol,
		"captureID", req.CaptureID,
	)

	if p.dryRun || p.targetURL == "" {
		return &replay.ReplayResult{
			RequestID: orig.ID,
			Status:    replay.ReplayStatusSkipped,
			Metadata: map[string]string{
				"reason": "dry_run",
				"method": orig.Method,
				"path":   orig.Path,
			},
		}, nil
	}

	return p.sendRequest(ctx, orig)
}

func (p *echoPlugin) sendRequest(ctx context.Context, orig *storage.CapturedRequest) (*replay.ReplayResult, error) {
	url := p.targetURL + orig.Path

	httpReq, err := http.NewRequestWithContext(ctx, orig.Method, url, bytes.NewReader(orig.Body))
	if err != nil {
		return &replay.ReplayResult{
			RequestID: orig.ID,
			Status:    replay.ReplayStatusError,
			Error:     fmt.Sprintf("create request: %v", err),
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
			Error:     fmt.Sprintf("send request: %v", err),
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

func (p *echoPlugin) Close() error {
	p.client.CloseIdleConnections()
	p.log.Info("echo plugin closed")
	return nil
}
