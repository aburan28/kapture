package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kapture-io/kapture/internal/storage"
)

// HealthStatus represents the overall health state of the capture service.
type HealthStatus string

const (
	HealthStatusOK       HealthStatus = "ok"
	HealthStatusDegraded HealthStatus = "degraded"
	HealthStatusNotReady HealthStatus = "not_ready"
)

// CheckResult is the outcome of a single health check probe.
type CheckResult struct {
	Name    string       `json:"name"`
	Status  HealthStatus `json:"status"`
	Message string       `json:"message,omitempty"`
	Latency string       `json:"latency,omitempty"`
}

// HealthReport is returned by the health endpoints.
type HealthReport struct {
	Status    HealthStatus  `json:"status"`
	Timestamp string        `json:"timestamp"`
	Checks    []CheckResult `json:"checks"`
}

// HealthChecker manages pre-flight and in-flight health checks for the capture
// service. Pre-flight checks run once at startup to verify the service is ready
// to accept traffic. In-flight checks run periodically to ensure continued
// health during operation.
type HealthChecker struct {
	mu sync.RWMutex

	// Pre-flight state: becomes true once all startup checks pass.
	preflightPassed atomic.Bool

	// Storage writer for write-path probes.
	writerFactory storage.WriterFactory
	storageType   string

	// Upstream target for proxy-mode connectivity checks.
	upstreamAddr string

	// In-flight tracking.
	lastCheckTime   time.Time
	lastCheckReport *HealthReport

	log *slog.Logger
}

// HealthCheckerConfig configures the HealthChecker.
type HealthCheckerConfig struct {
	WriterFactory storage.WriterFactory
	StorageType   string
	UpstreamAddr  string
	Logger        *slog.Logger
}

// NewHealthChecker creates a HealthChecker.
func NewHealthChecker(cfg HealthCheckerConfig) *HealthChecker {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &HealthChecker{
		writerFactory: cfg.WriterFactory,
		storageType:   cfg.StorageType,
		upstreamAddr:  cfg.UpstreamAddr,
		log:           cfg.Logger,
	}
}

// RunPreflightChecks executes all startup checks and marks the service as ready
// when they pass. Returns an error if any critical check fails.
func (hc *HealthChecker) RunPreflightChecks(ctx context.Context) error {
	hc.log.Info("running pre-flight health checks")

	var checks []CheckResult

	// 1. Storage connectivity check.
	storageCheck := hc.checkStorageConnectivity(ctx)
	checks = append(checks, storageCheck)
	if storageCheck.Status != HealthStatusOK {
		hc.log.Error("pre-flight storage check failed", "message", storageCheck.Message)
		return fmt.Errorf("pre-flight storage check failed: %s", storageCheck.Message)
	}

	// 2. Upstream connectivity check (only in proxy mode).
	if hc.upstreamAddr != "" {
		upstreamCheck := hc.checkUpstreamConnectivity(ctx)
		checks = append(checks, upstreamCheck)
		if upstreamCheck.Status == HealthStatusNotReady {
			hc.log.Error("pre-flight upstream check failed", "message", upstreamCheck.Message)
			return fmt.Errorf("pre-flight upstream check failed: %s", upstreamCheck.Message)
		}
	}

	hc.mu.Lock()
	hc.lastCheckTime = time.Now().UTC()
	hc.lastCheckReport = &HealthReport{
		Status:    HealthStatusOK,
		Timestamp: hc.lastCheckTime.Format(time.RFC3339),
		Checks:    checks,
	}
	hc.mu.Unlock()

	hc.preflightPassed.Store(true)
	hc.log.Info("pre-flight health checks passed", "checks", len(checks))
	return nil
}

// RunInFlightCheck executes a single round of in-flight health checks and
// caches the result. Called periodically by the background loop.
func (hc *HealthChecker) RunInFlightCheck(ctx context.Context) *HealthReport {
	var checks []CheckResult
	overall := HealthStatusOK

	// Storage check.
	storageCheck := hc.checkStorageConnectivity(ctx)
	checks = append(checks, storageCheck)
	if storageCheck.Status != HealthStatusOK {
		overall = HealthStatusDegraded
	}

	// Upstream check (proxy mode only).
	if hc.upstreamAddr != "" {
		upstreamCheck := hc.checkUpstreamConnectivity(ctx)
		checks = append(checks, upstreamCheck)
		if upstreamCheck.Status == HealthStatusNotReady {
			overall = HealthStatusDegraded
		}
	}

	report := &HealthReport{
		Status:    overall,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Checks:    checks,
	}

	hc.mu.Lock()
	hc.lastCheckTime = time.Now().UTC()
	hc.lastCheckReport = report
	hc.mu.Unlock()

	return report
}

// StartBackgroundChecks runs in-flight checks at the given interval until ctx
// is cancelled.
func (hc *HealthChecker) StartBackgroundChecks(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			report := hc.RunInFlightCheck(ctx)
			if report.Status != HealthStatusOK {
				hc.log.Warn("in-flight health check degraded", "status", report.Status)
			}
		}
	}
}

// IsReady returns true if pre-flight checks have passed.
func (hc *HealthChecker) IsReady() bool {
	return hc.preflightPassed.Load()
}

// LastReport returns the most recent cached health report.
func (hc *HealthChecker) LastReport() *HealthReport {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	return hc.lastCheckReport
}

// HandleReadyz serves the readiness probe endpoint. Returns 503 if pre-flight
// checks have not passed, 200 otherwise.
func (hc *HealthChecker) HandleReadyz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if !hc.IsReady() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(HealthReport{
			Status:    HealthStatusNotReady,
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			Checks:    []CheckResult{{Name: "preflight", Status: HealthStatusNotReady, Message: "pre-flight checks have not passed"}},
		})
		return
	}

	report := hc.LastReport()
	if report == nil {
		report = &HealthReport{
			Status:    HealthStatusOK,
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		}
	}

	if report.Status == HealthStatusOK {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(report)
}

// HandleLivez serves the liveness probe endpoint. Returns 200 as long as the
// process is running.
func (hc *HealthChecker) HandleLivez(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(HealthReport{
		Status:    HealthStatusOK,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Checks:    []CheckResult{{Name: "process", Status: HealthStatusOK, Message: "alive"}},
	})
}

// HandleHealthDetail serves a detailed health report with all check results.
func (hc *HealthChecker) HandleHealthDetail(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	report := hc.RunInFlightCheck(r.Context())

	if report.Status == HealthStatusOK {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(report)
}

// checkStorageConnectivity verifies that the storage backend is reachable by
// creating a probe writer and immediately closing it.
func (hc *HealthChecker) checkStorageConnectivity(ctx context.Context) CheckResult {
	if hc.writerFactory == nil {
		return CheckResult{
			Name:    "storage",
			Status:  HealthStatusNotReady,
			Message: "no writer factory configured",
		}
	}

	start := time.Now()
	probeWriter, err := hc.writerFactory.NewWriter(ctx, "__healthcheck__")
	if err != nil {
		return CheckResult{
			Name:    "storage",
			Status:  HealthStatusNotReady,
			Message: fmt.Sprintf("failed to create probe writer: %v", err),
			Latency: time.Since(start).String(),
		}
	}
	_ = probeWriter.Close()

	return CheckResult{
		Name:    "storage",
		Status:  HealthStatusOK,
		Message: fmt.Sprintf("storage backend %q reachable", hc.storageType),
		Latency: time.Since(start).String(),
	}
}

// checkUpstreamConnectivity verifies the upstream target is reachable via an
// HTTP HEAD request (proxy mode only).
func (hc *HealthChecker) checkUpstreamConnectivity(ctx context.Context) CheckResult {
	if hc.upstreamAddr == "" {
		return CheckResult{
			Name:   "upstream",
			Status: HealthStatusOK,
		}
	}

	start := time.Now()
	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, hc.upstreamAddr, nil)
	if err != nil {
		return CheckResult{
			Name:    "upstream",
			Status:  HealthStatusNotReady,
			Message: fmt.Sprintf("failed to build probe request: %v", err),
			Latency: time.Since(start).String(),
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		return CheckResult{
			Name:    "upstream",
			Status:  HealthStatusNotReady,
			Message: fmt.Sprintf("upstream %q unreachable: %v", hc.upstreamAddr, err),
			Latency: time.Since(start).String(),
		}
	}
	_ = resp.Body.Close()

	return CheckResult{
		Name:    "upstream",
		Status:  HealthStatusOK,
		Message: fmt.Sprintf("upstream %q reachable (HTTP %d)", hc.upstreamAddr, resp.StatusCode),
		Latency: time.Since(start).String(),
	}
}
