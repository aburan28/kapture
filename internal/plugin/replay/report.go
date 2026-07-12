package replay

import (
	"encoding/json"
	"os"
	"time"
)

// RunReport is the compact, machine-readable result of a replay run. The
// replay-engine writes it as JSON to its termination log (or any file) so
// the spoke controller can scrape final counts and latencies from the pod
// without a side channel.
type RunReport struct {
	TotalRequests    int64   `json:"totalRequests"`
	SentRequests     int64   `json:"sentRequests"`
	FailedRequests   int64   `json:"failedRequests"`
	FilteredRequests int64   `json:"filteredRequests"`
	DurationMs       int64   `json:"durationMs"`
	AchievedRPS      float64 `json:"achievedRPS"`
	MeanLatencyMs    int64   `json:"meanLatencyMs"`
	P50LatencyMs     int64   `json:"p50LatencyMs"`
	P95LatencyMs     int64   `json:"p95LatencyMs"`
	P99LatencyMs     int64   `json:"p99LatencyMs"`
}

// NewRunReport converts an engine Summary into a RunReport.
func NewRunReport(s *Summary) *RunReport {
	report := &RunReport{
		TotalRequests:    s.TotalRequests,
		SentRequests:     s.SuccessCount,
		FailedRequests:   s.ErrorCount,
		FilteredRequests: s.FilteredCount,
		DurationMs:       s.TotalDuration.Milliseconds(),
		MeanLatencyMs:    s.MeanLatency.Milliseconds(),
		P50LatencyMs:     s.P50Latency.Milliseconds(),
		P95LatencyMs:     s.P95Latency.Milliseconds(),
		P99LatencyMs:     s.P99Latency.Milliseconds(),
	}
	if s.TotalDuration > 0 {
		report.AchievedRPS = float64(s.SuccessCount) / s.TotalDuration.Seconds()
	}
	return report
}

// WriteFile serialises the report as a single JSON line to path. Kubernetes
// termination logs are capped at 4KiB; RunReport stays well under that.
func (r *RunReport) WriteFile(path string) error {
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// ParseRunReport decodes a RunReport from JSON produced by WriteFile.
func ParseRunReport(data []byte) (*RunReport, error) {
	var report RunReport
	if err := json.Unmarshal(data, &report); err != nil {
		return nil, err
	}
	return &report, nil
}

// LatencyString formats a millisecond latency as a duration string for CRD
// status fields (e.g. "42ms").
func LatencyString(ms int64) string {
	return (time.Duration(ms) * time.Millisecond).String()
}
