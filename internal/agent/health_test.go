package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kapture-io/kapture/internal/storage"
)

func TestHealthChecker_PreflightPasses_WithNilFactory(t *testing.T) {
	hc := NewHealthChecker(HealthCheckerConfig{
		WriterFactory: nil,
		Logger:        discardLogger(),
	})

	err := hc.RunPreflightChecks(context.Background())
	if err == nil {
		t.Fatal("expected pre-flight to fail when no writer factory is configured")
	}
	if hc.IsReady() {
		t.Error("expected IsReady to be false after failed preflight")
	}
}

func TestHealthChecker_PreflightPasses_WithMockFactory(t *testing.T) {
	mw := &mockWriter{}
	factory := &mockWriterFactory{writer: mw}

	hc := NewHealthChecker(HealthCheckerConfig{
		WriterFactory: factory,
		StorageType:   "mock",
		Logger:        discardLogger(),
	})

	err := hc.RunPreflightChecks(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hc.IsReady() {
		t.Error("expected IsReady to be true after successful preflight")
	}
}

func TestHealthChecker_PreflightPasses_WithUpstream(t *testing.T) {
	// Spin up a tiny upstream server.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	mw := &mockWriter{}
	factory := &mockWriterFactory{writer: mw}

	hc := NewHealthChecker(HealthCheckerConfig{
		WriterFactory: factory,
		StorageType:   "mock",
		UpstreamAddr:  upstream.URL,
		Logger:        discardLogger(),
	})

	err := hc.RunPreflightChecks(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	report := hc.LastReport()
	if report == nil {
		t.Fatal("expected non-nil report")
	}
	if len(report.Checks) != 2 {
		t.Errorf("expected 2 checks (storage + upstream), got %d", len(report.Checks))
	}
}

func TestHealthChecker_InFlightCheck_Degraded(t *testing.T) {
	// No writer factory → storage check fails → degraded.
	hc := NewHealthChecker(HealthCheckerConfig{
		WriterFactory: nil,
		Logger:        discardLogger(),
	})

	report := hc.RunInFlightCheck(context.Background())
	if report.Status != HealthStatusDegraded {
		t.Errorf("expected degraded status, got %s", report.Status)
	}
}

func TestHealthChecker_InFlightCheck_OK(t *testing.T) {
	mw := &mockWriter{}
	factory := &mockWriterFactory{writer: mw}

	hc := NewHealthChecker(HealthCheckerConfig{
		WriterFactory: factory,
		StorageType:   "mock",
		Logger:        discardLogger(),
	})

	report := hc.RunInFlightCheck(context.Background())
	if report.Status != HealthStatusOK {
		t.Errorf("expected OK status, got %s", report.Status)
	}
}

func TestHealthChecker_HandleReadyz_NotReady(t *testing.T) {
	hc := NewHealthChecker(HealthCheckerConfig{
		WriterFactory: nil,
		Logger:        discardLogger(),
	})

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	hc.HandleReadyz(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", rec.Code)
	}

	var report HealthReport
	if err := json.NewDecoder(rec.Body).Decode(&report); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if report.Status != HealthStatusNotReady {
		t.Errorf("expected not_ready status, got %s", report.Status)
	}
}

func TestHealthChecker_HandleReadyz_Ready(t *testing.T) {
	mw := &mockWriter{}
	factory := &mockWriterFactory{writer: mw}

	hc := NewHealthChecker(HealthCheckerConfig{
		WriterFactory: factory,
		StorageType:   "mock",
		Logger:        discardLogger(),
	})

	_ = hc.RunPreflightChecks(context.Background())

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	hc.HandleReadyz(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestHealthChecker_HandleLivez(t *testing.T) {
	hc := NewHealthChecker(HealthCheckerConfig{Logger: discardLogger()})

	req := httptest.NewRequest(http.MethodGet, "/livez", nil)
	rec := httptest.NewRecorder()

	hc.HandleLivez(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}

	var report HealthReport
	if err := json.NewDecoder(rec.Body).Decode(&report); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if report.Status != HealthStatusOK {
		t.Errorf("expected ok, got %s", report.Status)
	}
}

func TestHealthChecker_HandleHealthDetail(t *testing.T) {
	mw := &mockWriter{}
	factory := &mockWriterFactory{writer: mw}

	hc := NewHealthChecker(HealthCheckerConfig{
		WriterFactory: factory,
		StorageType:   "mock",
		Logger:        discardLogger(),
	})

	req := httptest.NewRequest(http.MethodGet, "/healthz/detail", nil)
	rec := httptest.NewRecorder()

	hc.HandleHealthDetail(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}

	var report HealthReport
	if err := json.NewDecoder(rec.Body).Decode(&report); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if len(report.Checks) == 0 {
		t.Error("expected at least one check in detailed report")
	}
}

func TestHealthChecker_BackgroundChecks_Cancellation(t *testing.T) {
	mw := &mockWriter{}
	factory := &mockWriterFactory{writer: mw}

	hc := NewHealthChecker(HealthCheckerConfig{
		WriterFactory: factory,
		StorageType:   "mock",
		Logger:        discardLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		hc.StartBackgroundChecks(ctx, 10*time.Millisecond)
		close(done)
	}()

	// Let a few checks run.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Background checks stopped as expected.
	case <-time.After(2 * time.Second):
		t.Fatal("background checks did not stop after context cancellation")
	}
}

// ---------------------------------------------------------------------------
// Mock WriterFactory
// ---------------------------------------------------------------------------

type mockWriterFactory struct {
	writer *mockWriter
	err    error
}

func (f *mockWriterFactory) NewWriter(_ context.Context, _ string) (storage.Writer, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.writer, nil
}
