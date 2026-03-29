// Package main implements a replay plugin that generates k6 load test scripts
// from captured HTTP traffic. Instead of replaying requests directly, it
// writes a k6-compatible JavaScript test script that can be used for load
// testing the target service.
//
// Build as a Go plugin:
//
//	go build -buildmode=plugin -o k6.so ./examples/plugins/k6
//
// Settings:
//
//	output_path  - Path to write the generated k6 script (default: "replay.js")
//	vus          - Number of virtual users for the k6 test (default: "10")
//	duration     - Duration of the k6 test run (default: "30s")
//	threshold    - p95 response time threshold in ms (default: "500")
//	sleep_range  - Random sleep range between requests in seconds (default: "1,3")
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/kapture-io/kapture/internal/replay"
)

type k6Plugin struct {
	log        *slog.Logger
	mu         sync.Mutex
	outputPath string
	vus        string
	duration   string
	threshold  string
	sleepRange string
	targetURL  string
	requests   []k6Request
}

type k6Request struct {
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body,omitempty"`
}

// NewReplayPlugin is the exported constructor required by the plugin contract.
func NewReplayPlugin() (replay.ReplayPlugin, error) {
	return &k6Plugin{
		log:        slog.Default(),
		outputPath: "replay.js",
		vus:        "10",
		duration:   "30s",
		threshold:  "500",
		sleepRange: "1,3",
	}, nil
}

func (p *k6Plugin) Name() string { return "k6" }

func (p *k6Plugin) Init(_ context.Context, cfg replay.PluginConfig) error {
	p.targetURL = cfg.TargetURL

	if v, ok := cfg.Settings["output_path"]; ok {
		p.outputPath = v
	}
	if v, ok := cfg.Settings["vus"]; ok {
		p.vus = v
	}
	if v, ok := cfg.Settings["duration"]; ok {
		p.duration = v
	}
	if v, ok := cfg.Settings["threshold"]; ok {
		p.threshold = v
	}
	if v, ok := cfg.Settings["sleep_range"]; ok {
		p.sleepRange = v
	}

	p.log.Info("k6 plugin initialized",
		"outputPath", p.outputPath,
		"targetURL", p.targetURL,
		"vus", p.vus,
		"duration", p.duration,
	)
	return nil
}

func (p *k6Plugin) ProcessRequest(_ context.Context, req *replay.ReplayRequest) (*replay.ReplayResult, error) {
	orig := req.Original

	if orig.Protocol == "gRPC" {
		return &replay.ReplayResult{
			RequestID: orig.ID,
			Status:    replay.ReplayStatusSkipped,
			Metadata:  map[string]string{"reason": "grpc_not_supported"},
		}, nil
	}

	// Flatten headers to single-value for k6 script simplicity.
	headers := make(map[string]string, len(orig.Headers))
	for k, vals := range orig.Headers {
		lower := strings.ToLower(k)
		// Skip hop-by-hop and host headers that k6 manages.
		if lower == "host" || lower == "transfer-encoding" || lower == "connection" {
			continue
		}
		if len(vals) > 0 {
			headers[k] = vals[0]
		}
	}

	var body string
	if len(orig.Body) > 0 {
		body = base64.StdEncoding.EncodeToString(orig.Body)
	}

	p.mu.Lock()
	p.requests = append(p.requests, k6Request{
		Method:  orig.Method,
		Path:    orig.Path,
		Headers: headers,
		Body:    body,
	})
	p.mu.Unlock()

	return &replay.ReplayResult{
		RequestID: orig.ID,
		Status:    replay.ReplayStatusSuccess,
		Metadata: map[string]string{
			"action":   "collected",
			"sequence": fmt.Sprintf("%d", req.Sequence),
		},
	}, nil
}

func (p *k6Plugin) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.requests) == 0 {
		p.log.Info("k6 plugin: no requests collected, skipping script generation")
		return nil
	}

	script, err := p.generateScript()
	if err != nil {
		return fmt.Errorf("generate k6 script: %w", err)
	}

	if err := os.WriteFile(p.outputPath, []byte(script), 0o644); err != nil {
		return fmt.Errorf("write k6 script to %q: %w", p.outputPath, err)
	}

	p.log.Info("k6 script generated",
		"path", p.outputPath,
		"requests", len(p.requests),
	)
	return nil
}

func (p *k6Plugin) generateScript() (string, error) {
	requestsJSON, err := json.MarshalIndent(p.requests, "", "  ")
	if err != nil {
		return "", err
	}

	sleepParts := strings.SplitN(p.sleepRange, ",", 2)
	sleepMin, sleepMax := "1", "3"
	if len(sleepParts) == 2 {
		sleepMin = strings.TrimSpace(sleepParts[0])
		sleepMax = strings.TrimSpace(sleepParts[1])
	}

	targetURL := p.targetURL
	if targetURL == "" {
		targetURL = "http://localhost:8080"
	}

	return fmt.Sprintf(`// Auto-generated k6 load test script from kapture replay
// Generated: %s
// Requests: %d
//
// Run with: k6 run %s

import http from 'k6/http';
import { check, sleep } from 'k6';
import encoding from 'k6/encoding';

export const options = {
  vus: %s,
  duration: '%s',
  thresholds: {
    http_req_duration: ['p(95)<%s'],
  },
};

const BASE_URL = __ENV.TARGET_URL || '%s';

const requests = %s;

export default function () {
  for (const req of requests) {
    const url = BASE_URL + req.path;
    const params = { headers: req.headers };
    let body = null;

    if (req.body) {
      body = encoding.b64decode(req.body, 'std', 's');
    }

    const res = http.request(req.method, url, body, params);

    check(res, {
      'status is not 5xx': (r) => r.status < 500,
    });

    sleep(Math.random() * (%s - %s) + %s);
  }
}
`,
		time.Now().UTC().Format(time.RFC3339),
		len(p.requests),
		p.outputPath,
		p.vus,
		p.duration,
		p.threshold,
		targetURL,
		string(requestsJSON),
		sleepMax, sleepMin, sleepMin,
	), nil
}
