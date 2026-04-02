package storage

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// marshalRequestLine edge-case tests
// ---------------------------------------------------------------------------

func TestMarshalRequestLine_NilRequest(t *testing.T) {
	_, err := marshalRequestLine(nil)
	if err == nil {
		t.Fatal("expected error for nil request, got nil")
	}
	if !strings.Contains(err.Error(), "nil") {
		t.Fatalf("error should mention nil, got: %v", err)
	}
}

func TestMarshalRequestLine_MinimalFields(t *testing.T) {
	req := &CapturedRequest{
		ID:       "min-1",
		Method:   "GET",
		Path:     "/",
		Protocol: "HTTP/1.1",
	}
	line, err := marshalRequestLine(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decoded CapturedRequest
	if err := json.Unmarshal(line[:len(line)-1], &decoded); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if decoded.ID != "min-1" {
		t.Fatalf("got ID %q, want %q", decoded.ID, "min-1")
	}
	if decoded.Headers != nil {
		t.Fatalf("expected nil headers for minimal request, got %v", decoded.Headers)
	}
	if decoded.Body != nil {
		t.Fatalf("expected nil body for minimal request, got %v", decoded.Body)
	}
	if decoded.Metadata != nil {
		t.Fatalf("expected nil metadata for minimal request, got %v", decoded.Metadata)
	}
	if decoded.ContentLength != 0 {
		t.Fatalf("expected zero content length, got %d", decoded.ContentLength)
	}
}

func TestMarshalRequestLine_WithAllFields(t *testing.T) {
	ts := time.Date(2026, 3, 15, 10, 30, 0, 0, time.UTC)
	req := &CapturedRequest{
		ID:        "full-1",
		Timestamp: ts,
		Method:    "POST",
		Path:      "/api/v1/data",
		Headers: map[string][]string{
			"Content-Type":  {"application/json"},
			"Authorization": {"Bearer token123"},
			"X-Multi":       {"val1", "val2"},
		},
		Body:          []byte(`{"key":"value"}`),
		ContentLength: 15,
		Protocol:      "HTTP/2.0",
		Metadata: map[string]string{
			"cluster": "us-east-1",
			"env":     "staging",
		},
	}

	line, err := marshalRequestLine(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decoded CapturedRequest
	if err := json.Unmarshal(line[:len(line)-1], &decoded); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if decoded.ID != "full-1" {
		t.Fatalf("got ID %q, want %q", decoded.ID, "full-1")
	}
	if !decoded.Timestamp.Equal(ts) {
		t.Fatalf("got timestamp %v, want %v", decoded.Timestamp, ts)
	}
	if decoded.Method != "POST" {
		t.Fatalf("got method %q, want %q", decoded.Method, "POST")
	}
	if decoded.Path != "/api/v1/data" {
		t.Fatalf("got path %q, want %q", decoded.Path, "/api/v1/data")
	}
	if len(decoded.Headers) != 3 {
		t.Fatalf("got %d header keys, want 3", len(decoded.Headers))
	}
	if decoded.Headers["X-Multi"][1] != "val2" {
		t.Fatalf("multi-value header mismatch: %v", decoded.Headers["X-Multi"])
	}
	if string(decoded.Body) != `{"key":"value"}` {
		t.Fatalf("body mismatch: %s", decoded.Body)
	}
	if decoded.ContentLength != 15 {
		t.Fatalf("got content length %d, want 15", decoded.ContentLength)
	}
	if decoded.Protocol != "HTTP/2.0" {
		t.Fatalf("got protocol %q, want %q", decoded.Protocol, "HTTP/2.0")
	}
	if decoded.Metadata["cluster"] != "us-east-1" {
		t.Fatalf("metadata mismatch: %v", decoded.Metadata)
	}
}

func TestMarshalRequestLine_EndsWithNewline(t *testing.T) {
	req := &CapturedRequest{ID: "nl-1", Method: "GET", Path: "/"}
	line, err := marshalRequestLine(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(line) == 0 {
		t.Fatal("expected non-empty output")
	}
	if line[len(line)-1] != '\n' {
		t.Fatalf("expected trailing newline, last byte is %q", line[len(line)-1])
	}
	// Ensure there is exactly one trailing newline (no double newlines).
	trimmed := bytes.TrimRight(line, "\n")
	if len(line)-len(trimmed) != 1 {
		t.Fatalf("expected exactly 1 trailing newline, got %d", len(line)-len(trimmed))
	}
}

// ---------------------------------------------------------------------------
// compressGZIP edge-case tests
// ---------------------------------------------------------------------------

func TestCompressGZIP_RoundTrip(t *testing.T) {
	original := []byte("round-trip test with special chars: <>&\"' \t\n\x00 end")
	compressed, err := compressGZIP(original)
	if err != nil {
		t.Fatalf("compress failed: %v", err)
	}

	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatalf("gzip.NewReader failed: %v", err)
	}
	defer reader.Close()

	decompressed, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read decompressed failed: %v", err)
	}

	if !bytes.Equal(original, decompressed) {
		t.Fatalf("round-trip mismatch:\n  original:     %q\n  decompressed: %q", original, decompressed)
	}
}

func TestCompressGZIP_EmptyPayload(t *testing.T) {
	compressed, err := compressGZIP([]byte{})
	if err != nil {
		t.Fatalf("compress empty payload failed: %v", err)
	}

	// Result must be valid gzip.
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatalf("gzip.NewReader failed on empty payload output: %v", err)
	}
	defer reader.Close()

	decompressed, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read decompressed failed: %v", err)
	}
	if len(decompressed) != 0 {
		t.Fatalf("expected empty decompressed output, got %d bytes", len(decompressed))
	}
}

// ---------------------------------------------------------------------------
// normalizeFlushInterval edge-case tests
// ---------------------------------------------------------------------------

func TestNormalizeFlushInterval_Defaults(t *testing.T) {
	cases := []struct {
		name  string
		input time.Duration
	}{
		{"zero", 0},
		{"negative one second", -1 * time.Second},
		{"negative large", -999 * time.Hour},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := normalizeFlushInterval(tc.input)
			if got != defaultFlushInterval {
				t.Fatalf("normalizeFlushInterval(%v) = %v, want %v", tc.input, got, defaultFlushInterval)
			}
		})
	}
}

func TestNormalizeFlushInterval_CustomValue(t *testing.T) {
	custom := 42 * time.Second
	got := normalizeFlushInterval(custom)
	if got != custom {
		t.Fatalf("normalizeFlushInterval(%v) = %v, want %v", custom, got, custom)
	}
}

// ---------------------------------------------------------------------------
// normalizeMaxBufferBytes edge-case tests
// ---------------------------------------------------------------------------

func TestNormalizeMaxBufferBytes_Defaults(t *testing.T) {
	cases := []struct {
		name  string
		input int
	}{
		{"zero", 0},
		{"negative one", -1},
		{"large negative", -999999},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := normalizeMaxBufferBytes(tc.input)
			if got != defaultMaxBufferSize {
				t.Fatalf("normalizeMaxBufferBytes(%d) = %d, want %d", tc.input, got, defaultMaxBufferSize)
			}
		})
	}
}

func TestNormalizeMaxBufferBytes_CustomValue(t *testing.T) {
	custom := 8192
	got := normalizeMaxBufferBytes(custom)
	if got != custom {
		t.Fatalf("normalizeMaxBufferBytes(%d) = %d, want %d", custom, got, custom)
	}
}

// ---------------------------------------------------------------------------
// objectFileName edge-case tests
// ---------------------------------------------------------------------------

func TestObjectFileName_WithPod(t *testing.T) {
	got := objectFileName("my-pod", 1)
	expected := "my-pod-000001.jsonl.gz"
	if got != expected {
		t.Fatalf("got %q, want %q", got, expected)
	}
}

func TestObjectFileName_EmptyPod(t *testing.T) {
	got := objectFileName("", 1)
	expected := "unknown-agent-000001.jsonl.gz"
	if got != expected {
		t.Fatalf("got %q, want %q", got, expected)
	}
}

func TestObjectFileName_WhitespacePod(t *testing.T) {
	cases := []string{"  ", "\t", " \t\n "}
	for _, pod := range cases {
		got := objectFileName(pod, 1)
		expected := "unknown-agent-000001.jsonl.gz"
		if got != expected {
			t.Fatalf("objectFileName(%q, 1) = %q, want %q", pod, got, expected)
		}
	}
}

// ---------------------------------------------------------------------------
// newCloudObjectName edge-case tests
// ---------------------------------------------------------------------------

func TestNewCloudObjectName_WithPrefix(t *testing.T) {
	ts := time.Date(2026, 4, 2, 8, 0, 0, 0, time.UTC)
	fn := newCloudObjectName("captures", "cap-123", "agent-pod")
	got := fn(ts, 1)
	expected := "captures/cap-123/2026-04-02/agent-pod-000001.jsonl.gz"
	if got != expected {
		t.Fatalf("got %q, want %q", got, expected)
	}
}

func TestNewCloudObjectName_EmptyPrefix(t *testing.T) {
	ts := time.Date(2026, 4, 2, 8, 0, 0, 0, time.UTC)
	fn := newCloudObjectName("", "cap-123", "agent-pod")
	got := fn(ts, 1)
	expected := "cap-123/2026-04-02/agent-pod-000001.jsonl.gz"
	if got != expected {
		t.Fatalf("got %q, want %q", got, expected)
	}
	// Must not start with a slash.
	if strings.HasPrefix(got, "/") {
		t.Fatalf("empty prefix should not produce leading slash, got %q", got)
	}
}

// ---------------------------------------------------------------------------
// newFilesystemObjectName tests
// ---------------------------------------------------------------------------

func TestNewFilesystemObjectName(t *testing.T) {
	ts := time.Date(2026, 4, 2, 8, 0, 0, 0, time.UTC)
	fn := newFilesystemObjectName("/mnt/capture", "cap-456", "worker-pod")
	got := fn(ts, 3)
	expected := filepath.Join("/mnt/capture", "cap-456", "2026-04-02", "worker-pod-000003.jsonl.gz")
	if got != expected {
		t.Fatalf("got %q, want %q", got, expected)
	}
}

// ---------------------------------------------------------------------------
// bufferedObjectWriter edge-case tests
// ---------------------------------------------------------------------------

// helper to create a writer with a mock upload function that records calls.
type uploadRecord struct {
	objectName string
	payload    []byte
}

func newTestWriter(bufSize int, flushInterval time.Duration) (*bufferedObjectWriter, *[]uploadRecord) {
	var uploads []uploadRecord
	w := newBufferedObjectWriter(
		"test-capture",
		flushInterval,
		bufSize,
		func(ts time.Time, seq uint64) string {
			return fmt.Sprintf("test-capture/%s/agent-%06d.jsonl.gz", ts.UTC().Format("2006-01-02"), seq)
		},
		func(_ context.Context, name string, payload []byte) error {
			uploads = append(uploads, uploadRecord{
				objectName: name,
				payload:    append([]byte{}, payload...),
			})
			return nil
		},
	)
	return w, &uploads
}

func TestBufferedObjectWriter_WriteAndFlush(t *testing.T) {
	w, uploads := newTestWriter(10*1024*1024, time.Hour)

	req := &CapturedRequest{ID: "w1", Method: "PUT", Path: "/resource", Protocol: "HTTP/1.1"}
	if err := w.Write(context.Background(), req); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	// Nothing uploaded yet (buffer not full, interval not elapsed).
	if len(*uploads) != 0 {
		t.Fatalf("expected 0 uploads before flush, got %d", len(*uploads))
	}

	if err := w.Flush(context.Background()); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	if len(*uploads) != 1 {
		t.Fatalf("expected 1 upload after flush, got %d", len(*uploads))
	}

	// Verify uploaded payload is valid gzip containing the request ID.
	reader, err := gzip.NewReader(bytes.NewReader((*uploads)[0].payload))
	if err != nil {
		t.Fatalf("gzip.NewReader failed: %v", err)
	}
	decompressed, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		t.Fatalf("decompress failed: %v", err)
	}
	if !bytes.Contains(decompressed, []byte(`"w1"`)) {
		t.Fatalf("decompressed payload missing request ID, got: %s", decompressed)
	}
}

func TestBufferedObjectWriter_AutoFlushOnSize(t *testing.T) {
	// Use a very small buffer size so a single write exceeds it.
	w, uploads := newTestWriter(10, time.Hour)

	req := &CapturedRequest{ID: "big-req", Method: "GET", Path: "/exceeds-tiny-buffer"}
	if err := w.Write(context.Background(), req); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if len(*uploads) != 1 {
		t.Fatalf("expected auto-flush after exceeding buffer, got %d uploads", len(*uploads))
	}
}

func TestBufferedObjectWriter_Close_FlushesRemaining(t *testing.T) {
	w, uploads := newTestWriter(10*1024*1024, time.Hour)

	req := &CapturedRequest{ID: "close-flush", Method: "DELETE", Path: "/item/1"}
	if err := w.Write(context.Background(), req); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if len(*uploads) != 0 {
		t.Fatal("expected no uploads before close")
	}

	if err := w.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	if len(*uploads) != 1 {
		t.Fatalf("expected close to flush remaining data, got %d uploads", len(*uploads))
	}

	// Verify the flushed data contains our request.
	reader, err := gzip.NewReader(bytes.NewReader((*uploads)[0].payload))
	if err != nil {
		t.Fatalf("gzip.NewReader failed: %v", err)
	}
	decompressed, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		t.Fatalf("decompress failed: %v", err)
	}
	if !bytes.Contains(decompressed, []byte(`"close-flush"`)) {
		t.Fatalf("flushed data missing request ID, got: %s", decompressed)
	}
}

func TestBufferedObjectWriter_WriteAfterClose(t *testing.T) {
	w, _ := newTestWriter(10*1024*1024, time.Hour)
	if err := w.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	err := w.Write(context.Background(), &CapturedRequest{ID: "late", Method: "GET", Path: "/"})
	if !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("expected ErrWriterClosed, got %v", err)
	}
}

func TestBufferedObjectWriter_FlushAfterClose(t *testing.T) {
	w, _ := newTestWriter(10*1024*1024, time.Hour)
	if err := w.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	err := w.Flush(context.Background())
	if !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("expected ErrWriterClosed, got %v", err)
	}
}

func TestBufferedObjectWriter_CloseIdempotent(t *testing.T) {
	w, uploads := newTestWriter(10*1024*1024, time.Hour)

	// Write some data so the first close actually flushes.
	req := &CapturedRequest{ID: "idem", Method: "GET", Path: "/"}
	if err := w.Write(context.Background(), req); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("first close failed: %v", err)
	}
	firstUploadCount := len(*uploads)

	// Second close should be a no-op, no error, no additional uploads.
	if err := w.Close(); err != nil {
		t.Fatalf("second close should not error, got: %v", err)
	}
	if len(*uploads) != firstUploadCount {
		t.Fatalf("second close caused additional uploads: before=%d, after=%d", firstUploadCount, len(*uploads))
	}

	// Third close for good measure.
	if err := w.Close(); err != nil {
		t.Fatalf("third close should not error, got: %v", err)
	}
}
