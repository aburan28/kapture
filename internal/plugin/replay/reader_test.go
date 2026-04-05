package replay

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kapture-io/kapture/internal/storage"
)

// --- Helper functions ---

func writeJSONLGZ(t *testing.T, path string, requests []*storage.CapturedRequest) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	for _, req := range requests {
		line, _ := json.Marshal(req)
		buf.Write(line)
		buf.WriteByte('\n')
	}
	compressed := compressToGzip(t, buf.Bytes())
	if err := os.WriteFile(path, compressed, 0o644); err != nil {
		t.Fatal(err)
	}
}

func compressToGzip(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func sampleRequests(n int, baseTime time.Time) []*storage.CapturedRequest {
	reqs := make([]*storage.CapturedRequest, n)
	for i := range reqs {
		reqs[i] = &storage.CapturedRequest{
			ID:        "req-" + string(rune('A'+i)),
			Timestamp: baseTime.Add(time.Duration(i) * time.Second),
			Method:    "GET",
			Path:      "/api/test",
			Protocol:  "HTTP/1.1",
			Headers:   map[string][]string{"Accept": {"application/json"}},
			Body:      []byte(`{"test": true}`),
		}
	}
	return reqs
}

// --- FilesystemReader tests ---

func TestFilesystemReader_BasicRead(t *testing.T) {
	dir := t.TempDir()
	baseTime := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	captureID := "test-capture"

	// Create files matching the storage layout:
	// mountPath/captureID/YYYY-MM-DD/agent-NNNNNN.jsonl.gz
	dateDir := filepath.Join(dir, captureID, "2026-01-15")
	reqs := sampleRequests(5, baseTime)
	writeJSONLGZ(t, filepath.Join(dateDir, "agent-pod-000001.jsonl.gz"), reqs[:3])
	writeJSONLGZ(t, filepath.Join(dateDir, "agent-pod-000002.jsonl.gz"), reqs[3:])

	reader, err := NewFilesystemReader(FilesystemReaderConfig{MountPath: dir})
	if err != nil {
		t.Fatal(err)
	}

	if err := reader.Open(context.Background(), ReadOptions{CaptureID: captureID}); err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	var got []*storage.CapturedRequest
	for {
		req, err := reader.Next(context.Background())
		if err == ErrReaderDone {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, req)
	}

	if len(got) != 5 {
		t.Fatalf("expected 5 requests, got %d", len(got))
	}
	if reader.TotalRead() != 5 {
		t.Errorf("expected TotalRead() = 5, got %d", reader.TotalRead())
	}
	if reader.FileCount() != 2 {
		t.Errorf("expected FileCount() = 2, got %d", reader.FileCount())
	}
}

func TestFilesystemReader_TimeFilter(t *testing.T) {
	dir := t.TempDir()
	captureID := "test-capture"

	// Create files for two days.
	jan15 := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	jan16 := time.Date(2026, 1, 16, 10, 0, 0, 0, time.UTC)

	writeJSONLGZ(t,
		filepath.Join(dir, captureID, "2026-01-15", "agent-000001.jsonl.gz"),
		sampleRequests(3, jan15))
	writeJSONLGZ(t,
		filepath.Join(dir, captureID, "2026-01-16", "agent-000001.jsonl.gz"),
		sampleRequests(2, jan16))

	reader, err := NewFilesystemReader(FilesystemReaderConfig{MountPath: dir})
	if err != nil {
		t.Fatal(err)
	}

	// Only read Jan 16.
	opts := ReadOptions{
		CaptureID: captureID,
		StartTime: jan16,
	}
	if err := reader.Open(context.Background(), opts); err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	count := 0
	for {
		_, err := reader.Next(context.Background())
		if err == ErrReaderDone {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		count++
	}

	if count != 2 {
		t.Fatalf("expected 2 requests from Jan 16 only, got %d", count)
	}
}

func TestFilesystemReader_MethodFilter(t *testing.T) {
	dir := t.TempDir()
	captureID := "test-capture"
	baseTime := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)

	reqs := []*storage.CapturedRequest{
		{ID: "1", Method: "GET", Path: "/api/a", Timestamp: baseTime, Protocol: "HTTP/1.1"},
		{ID: "2", Method: "POST", Path: "/api/b", Timestamp: baseTime.Add(time.Second), Protocol: "HTTP/1.1"},
		{ID: "3", Method: "GET", Path: "/api/c", Timestamp: baseTime.Add(2 * time.Second), Protocol: "HTTP/1.1"},
		{ID: "4", Method: "DELETE", Path: "/api/d", Timestamp: baseTime.Add(3 * time.Second), Protocol: "HTTP/1.1"},
	}

	writeJSONLGZ(t,
		filepath.Join(dir, captureID, "2026-01-15", "agent-000001.jsonl.gz"),
		reqs)

	reader, err := NewFilesystemReader(FilesystemReaderConfig{MountPath: dir})
	if err != nil {
		t.Fatal(err)
	}

	opts := ReadOptions{
		CaptureID: captureID,
		Methods:   []string{"POST", "DELETE"},
	}
	if err := reader.Open(context.Background(), opts); err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	var got []*storage.CapturedRequest
	for {
		req, err := reader.Next(context.Background())
		if err == ErrReaderDone {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, req)
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 requests, got %d", len(got))
	}
	if got[0].Method != "POST" || got[1].Method != "DELETE" {
		t.Errorf("unexpected methods: %s, %s", got[0].Method, got[1].Method)
	}
}

func TestFilesystemReader_PathPrefixFilter(t *testing.T) {
	dir := t.TempDir()
	captureID := "test-capture"
	baseTime := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)

	reqs := []*storage.CapturedRequest{
		{ID: "1", Method: "GET", Path: "/api/users", Timestamp: baseTime, Protocol: "HTTP/1.1"},
		{ID: "2", Method: "GET", Path: "/health", Timestamp: baseTime.Add(time.Second), Protocol: "HTTP/1.1"},
		{ID: "3", Method: "GET", Path: "/api/orders", Timestamp: baseTime.Add(2 * time.Second), Protocol: "HTTP/1.1"},
	}

	writeJSONLGZ(t,
		filepath.Join(dir, captureID, "2026-01-15", "agent-000001.jsonl.gz"),
		reqs)

	reader, err := NewFilesystemReader(FilesystemReaderConfig{MountPath: dir})
	if err != nil {
		t.Fatal(err)
	}

	opts := ReadOptions{
		CaptureID:  captureID,
		PathPrefix: "/api/",
	}
	if err := reader.Open(context.Background(), opts); err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	count := 0
	for {
		_, err := reader.Next(context.Background())
		if err == ErrReaderDone {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		count++
	}

	if count != 2 {
		t.Fatalf("expected 2 requests with /api/ prefix, got %d", count)
	}
}

func TestFilesystemReader_Limit(t *testing.T) {
	dir := t.TempDir()
	captureID := "test-capture"
	baseTime := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)

	writeJSONLGZ(t,
		filepath.Join(dir, captureID, "2026-01-15", "agent-000001.jsonl.gz"),
		sampleRequests(10, baseTime))

	reader, err := NewFilesystemReader(FilesystemReaderConfig{MountPath: dir})
	if err != nil {
		t.Fatal(err)
	}

	opts := ReadOptions{CaptureID: captureID, Limit: 3}
	if err := reader.Open(context.Background(), opts); err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	count := 0
	for {
		_, err := reader.Next(context.Background())
		if err == ErrReaderDone {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		count++
	}

	if count != 3 {
		t.Fatalf("expected 3 requests (limit=3), got %d", count)
	}
}

func TestFilesystemReader_EmptyDirectory(t *testing.T) {
	dir := t.TempDir()
	captureID := "nonexistent-capture"

	reader, err := NewFilesystemReader(FilesystemReaderConfig{MountPath: dir})
	if err != nil {
		t.Fatal(err)
	}

	// Open should fail because the capture directory doesn't exist.
	err = reader.Open(context.Background(), ReadOptions{CaptureID: captureID})
	if err == nil {
		defer reader.Close()
		// If it opens successfully (some implementations may), Next should return Done immediately.
		_, nextErr := reader.Next(context.Background())
		if nextErr != ErrReaderDone {
			t.Fatalf("expected ErrReaderDone for empty dir, got %v", nextErr)
		}
	}
}

func TestFilesystemReader_MountPathRequired(t *testing.T) {
	_, err := NewFilesystemReader(FilesystemReaderConfig{})
	if err == nil {
		t.Error("expected error with empty mount path")
	}
}

func TestFilesystemReader_MultipleFiles(t *testing.T) {
	dir := t.TempDir()
	captureID := "multi-file"
	baseTime := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)

	// Create 5 files to simulate high-throughput capture with multiple agents.
	for i := 0; i < 5; i++ {
		writeJSONLGZ(t,
			filepath.Join(dir, captureID, "2026-01-15",
				"agent-pod-"+string(rune('a'+i))+"-000001.jsonl.gz"),
			sampleRequests(3, baseTime.Add(time.Duration(i)*time.Minute)))
	}

	reader, err := NewFilesystemReader(FilesystemReaderConfig{MountPath: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Open(context.Background(), ReadOptions{CaptureID: captureID}); err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	count := 0
	for {
		_, err := reader.Next(context.Background())
		if err == ErrReaderDone {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		count++
	}

	if count != 15 {
		t.Fatalf("expected 15 requests from 5 files, got %d", count)
	}
	if reader.FileCount() != 5 {
		t.Errorf("expected 5 files, got %d", reader.FileCount())
	}
}

// --- Shared helper tests ---

func TestBuildObjectPrefix(t *testing.T) {
	tests := []struct {
		prefix    string
		captureID string
		want      string
	}{
		{"", "", ""},
		{"", "my-capture", "my-capture/"},
		{"prefix", "my-capture", "prefix/my-capture/"},
		{"/prefix/", "my-capture", "prefix/my-capture/"},
		{"prefix", "", "prefix/"},
	}

	for _, tt := range tests {
		got := buildObjectPrefix(tt.prefix, tt.captureID)
		if got != tt.want {
			t.Errorf("buildObjectPrefix(%q, %q) = %q, want %q", tt.prefix, tt.captureID, got, tt.want)
		}
	}
}

func TestFilterObjectByTime(t *testing.T) {
	start := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 1, 17, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		key   string
		start time.Time
		end   time.Time
		want  bool
	}{
		// No time filter — always match.
		{"prefix/capture/2026-01-14/agent-000001.jsonl.gz", time.Time{}, time.Time{}, true},
		// Within range.
		{"prefix/capture/2026-01-15/agent-000001.jsonl.gz", start, end, true},
		{"prefix/capture/2026-01-16/agent-000001.jsonl.gz", start, end, true},
		// Before range.
		{"prefix/capture/2026-01-14/agent-000001.jsonl.gz", start, end, false},
		// After range.
		{"prefix/capture/2026-01-18/agent-000001.jsonl.gz", start, end, false},
		// No date in key — include by default.
		{"prefix/capture/agent-000001.jsonl.gz", start, end, true},
	}

	for _, tt := range tests {
		got := filterObjectByTime(tt.key, tt.start, tt.end)
		if got != tt.want {
			t.Errorf("filterObjectByTime(%q, ...) = %v, want %v", tt.key, got, tt.want)
		}
	}
}

func TestUnmarshalRequest(t *testing.T) {
	req := &storage.CapturedRequest{
		ID:       "test-id",
		Method:   "POST",
		Path:     "/api/test",
		Protocol: "HTTP/1.1",
	}
	line, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}

	got, err := unmarshalRequest(line)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != req.ID || got.Method != req.Method || got.Path != req.Path {
		t.Errorf("unmarshal mismatch: got %+v", got)
	}
}

func TestUnmarshalRequest_Invalid(t *testing.T) {
	_, err := unmarshalRequest([]byte("not json"))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestMatchesReadOptions(t *testing.T) {
	baseTime := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	req := &storage.CapturedRequest{
		ID:        "1",
		Method:    "GET",
		Path:      "/api/users",
		Timestamp: baseTime,
	}

	tests := []struct {
		name string
		opts ReadOptions
		want bool
	}{
		{"no filters", ReadOptions{}, true},
		{"matching path prefix", ReadOptions{PathPrefix: "/api/"}, true},
		{"non-matching path prefix", ReadOptions{PathPrefix: "/health"}, false},
		{"matching method", ReadOptions{Methods: []string{"GET"}}, true},
		{"non-matching method", ReadOptions{Methods: []string{"POST"}}, false},
		{"in time range", ReadOptions{
			StartTime: baseTime.Add(-time.Hour),
			EndTime:   baseTime.Add(time.Hour),
		}, true},
		{"before start time", ReadOptions{
			StartTime: baseTime.Add(time.Hour),
		}, false},
		{"after end time", ReadOptions{
			EndTime: baseTime.Add(-time.Hour),
		}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchesReadOptions(req, tt.opts)
			if got != tt.want {
				t.Errorf("matchesReadOptions() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeekTo(t *testing.T) {
	keys := []string{"a.jsonl.gz", "b.jsonl.gz", "c.jsonl.gz", "d.jsonl.gz"}

	tests := []struct {
		resumeKey string
		want      int
	}{
		{"", 0},
		{"a.jsonl.gz", 1},
		{"b.jsonl.gz", 2},
		{"d.jsonl.gz", 4},
		{"ab.jsonl.gz", 1}, // between a and b
		{"z.jsonl.gz", 4},  // past end
	}

	for _, tt := range tests {
		got := seekTo(keys, tt.resumeKey)
		if got != tt.want {
			t.Errorf("seekTo(%q) = %d, want %d", tt.resumeKey, got, tt.want)
		}
	}
}

func TestNewReaderFromConfig_UnsupportedType(t *testing.T) {
	_, err := NewReaderFromConfig(context.Background(), "unknown", nil)
	if err == nil {
		t.Error("expected error for unsupported storage type")
	}
}

