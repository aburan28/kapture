package storage

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// ---------------------------------------------------------------------------
// Mocks / fakes
// ---------------------------------------------------------------------------

type mockS3Client struct {
	mu      sync.Mutex
	calls   []s3PutCall
	err     error
}

type s3PutCall struct {
	Bucket string
	Key    string
	Body   []byte
}

func (m *mockS3Client) PutObject(_ context.Context, params *awss3.PutObjectInput, _ ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	body, _ := io.ReadAll(params.Body)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, s3PutCall{
		Bucket: deref(params.Bucket),
		Key:    deref(params.Key),
		Body:   body,
	})
	return &awss3.PutObjectOutput{}, m.err
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

type mockGCSObjectWriter struct {
	mu              sync.Mutex
	data            []byte
	closed          bool
	contentType     string
	contentEncoding string
	writeErr        error
	closeErr        error
}

func (w *mockGCSObjectWriter) Write(p []byte) (int, error) {
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.data = append(w.data, p...)
	return len(p), nil
}

func (w *mockGCSObjectWriter) Close() error {
	w.closed = true
	return w.closeErr
}

func (w *mockGCSObjectWriter) SetContentType(v string)     { w.contentType = v }
func (w *mockGCSObjectWriter) SetContentEncoding(v string)  { w.contentEncoding = v }

// ---------------------------------------------------------------------------
// interface.go tests
// ---------------------------------------------------------------------------

func TestMarshalRequestLine(t *testing.T) {
	t.Run("nil request returns error", func(t *testing.T) {
		_, err := marshalRequestLine(nil)
		if err == nil {
			t.Fatal("expected error for nil request")
		}
	})

	t.Run("valid request returns JSON newline", func(t *testing.T) {
		req := &CapturedRequest{
			ID:     "abc",
			Method: "GET",
			Path:   "/foo",
		}
		line, err := marshalRequestLine(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if line[len(line)-1] != '\n' {
			t.Fatal("expected trailing newline")
		}
		var decoded CapturedRequest
		if err := json.Unmarshal(line[:len(line)-1], &decoded); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		if decoded.ID != "abc" || decoded.Method != "GET" || decoded.Path != "/foo" {
			t.Fatalf("unexpected decoded value: %+v", decoded)
		}
	})
}

func TestCompressGZIP(t *testing.T) {
	original := []byte("hello world, this is a test payload for gzip compression")
	compressed, err := compressGZIP(original)
	if err != nil {
		t.Fatalf("compress failed: %v", err)
	}

	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatalf("gzip reader failed: %v", err)
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

func TestNormalizeFlushInterval(t *testing.T) {
	tests := []struct {
		name     string
		input    time.Duration
		expected time.Duration
	}{
		{"zero returns default", 0, defaultFlushInterval},
		{"negative returns default", -1 * time.Second, defaultFlushInterval},
		{"positive passthrough", 10 * time.Second, 10 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeFlushInterval(tt.input)
			if got != tt.expected {
				t.Fatalf("got %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestNormalizeMaxBufferBytes(t *testing.T) {
	tests := []struct {
		name     string
		input    int
		expected int
	}{
		{"zero returns default", 0, defaultMaxBufferSize},
		{"negative returns default", -100, defaultMaxBufferSize},
		{"positive passthrough", 4096, 4096},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeMaxBufferBytes(tt.input)
			if got != tt.expected {
				t.Fatalf("got %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestObjectFileName(t *testing.T) {
	tests := []struct {
		name     string
		agentPod string
		seq      uint64
		expected string
	}{
		{"empty pod", "", 1, "unknown-agent-000001.jsonl.gz"},
		{"whitespace pod", "  ", 1, "unknown-agent-000001.jsonl.gz"},
		{"valid pod", "my-pod", 42, "my-pod-000042.jsonl.gz"},
		{"sequence formatting", "pod", 1, "pod-000001.jsonl.gz"},
		{"large sequence", "pod", 999999, "pod-999999.jsonl.gz"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := objectFileName(tt.agentPod, tt.seq)
			if got != tt.expected {
				t.Fatalf("got %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestNewCloudObjectName(t *testing.T) {
	ts := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)

	t.Run("without prefix", func(t *testing.T) {
		fn := newCloudObjectName("", "capture-1", "pod-a")
		got := fn(ts, 1)
		expected := "capture-1/2025-06-15/pod-a-000001.jsonl.gz"
		if got != expected {
			t.Fatalf("got %q, want %q", got, expected)
		}
	})

	t.Run("with prefix", func(t *testing.T) {
		fn := newCloudObjectName("my-prefix", "capture-1", "pod-a")
		got := fn(ts, 3)
		expected := "my-prefix/capture-1/2025-06-15/pod-a-000003.jsonl.gz"
		if got != expected {
			t.Fatalf("got %q, want %q", got, expected)
		}
	})

	t.Run("prefix with slashes is trimmed", func(t *testing.T) {
		fn := newCloudObjectName("/my-prefix/", "capture-1", "pod-a")
		got := fn(ts, 1)
		expected := "my-prefix/capture-1/2025-06-15/pod-a-000001.jsonl.gz"
		if got != expected {
			t.Fatalf("got %q, want %q", got, expected)
		}
	})
}

func TestNewFilesystemObjectName(t *testing.T) {
	ts := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	fn := newFilesystemObjectName("/mnt/data", "capture-1", "pod-a")
	got := fn(ts, 2)
	expected := filepath.Join("/mnt/data", "capture-1", "2025-06-15", "pod-a-000002.jsonl.gz")
	if got != expected {
		t.Fatalf("got %q, want %q", got, expected)
	}
}

// ---------------------------------------------------------------------------
// bufferedObjectWriter tests
// ---------------------------------------------------------------------------

func TestBufferedObjectWriter_WriteBuffersData(t *testing.T) {
	var uploads [][]byte
	w := newBufferedObjectWriter(
		"cap-1",
		time.Hour, // large interval so no time-based flush
		10*1024*1024, // large buffer
		func(ts time.Time, seq uint64) string { return fmt.Sprintf("obj-%d", seq) },
		func(_ context.Context, _ string, payload []byte) error {
			uploads = append(uploads, append([]byte{}, payload...))
			return nil
		},
	)

	req := &CapturedRequest{ID: "1", Method: "GET", Path: "/"}
	if err := w.Write(context.Background(), req); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if len(uploads) != 0 {
		t.Fatal("expected no uploads yet (data should be buffered)")
	}
}

func TestBufferedObjectWriter_AutoFlushOnSize(t *testing.T) {
	var uploads int
	w := newBufferedObjectWriter(
		"cap-1",
		time.Hour,
		50, // tiny buffer to trigger auto-flush
		func(ts time.Time, seq uint64) string { return fmt.Sprintf("obj-%d", seq) },
		func(_ context.Context, _ string, _ []byte) error {
			uploads++
			return nil
		},
	)

	req := &CapturedRequest{ID: "1", Method: "GET", Path: "/this-is-a-long-path-to-exceed-buffer-size"}
	if err := w.Write(context.Background(), req); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if uploads != 1 {
		t.Fatalf("expected 1 upload after exceeding buffer, got %d", uploads)
	}
}

func TestBufferedObjectWriter_FlushEmptyBuffer(t *testing.T) {
	var uploads int
	w := newBufferedObjectWriter(
		"cap-1",
		time.Hour,
		10*1024*1024,
		func(ts time.Time, seq uint64) string { return "obj" },
		func(_ context.Context, _ string, _ []byte) error {
			uploads++
			return nil
		},
	)

	if err := w.Flush(context.Background()); err != nil {
		t.Fatalf("flush empty failed: %v", err)
	}
	if uploads != 0 {
		t.Fatal("expected no uploads for empty buffer flush")
	}
}

func TestBufferedObjectWriter_FlushWithData(t *testing.T) {
	var uploadedPayloads [][]byte
	w := newBufferedObjectWriter(
		"cap-1",
		time.Hour,
		10*1024*1024,
		func(ts time.Time, seq uint64) string { return fmt.Sprintf("obj-%d", seq) },
		func(_ context.Context, _ string, payload []byte) error {
			uploadedPayloads = append(uploadedPayloads, append([]byte{}, payload...))
			return nil
		},
	)

	req := &CapturedRequest{ID: "flush-test", Method: "POST", Path: "/data"}
	if err := w.Write(context.Background(), req); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if err := w.Flush(context.Background()); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	if len(uploadedPayloads) != 1 {
		t.Fatalf("expected 1 upload, got %d", len(uploadedPayloads))
	}

	// Verify uploaded data is valid gzip containing JSONL
	reader, err := gzip.NewReader(bytes.NewReader(uploadedPayloads[0]))
	if err != nil {
		t.Fatalf("gzip reader failed: %v", err)
	}
	decompressed, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("decompress failed: %v", err)
	}
	reader.Close()

	if !bytes.Contains(decompressed, []byte(`"flush-test"`)) {
		t.Fatalf("decompressed data does not contain expected content: %s", decompressed)
	}
}

func TestBufferedObjectWriter_CloseFlushesAndMarksClosed(t *testing.T) {
	var uploads int
	w := newBufferedObjectWriter(
		"cap-1",
		time.Hour,
		10*1024*1024,
		func(ts time.Time, seq uint64) string { return "obj" },
		func(_ context.Context, _ string, _ []byte) error {
			uploads++
			return nil
		},
	)

	req := &CapturedRequest{ID: "close-test", Method: "GET", Path: "/"}
	if err := w.Write(context.Background(), req); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	if uploads != 1 {
		t.Fatalf("expected 1 upload on close, got %d", uploads)
	}
}

func TestBufferedObjectWriter_WriteAfterCloseReturnsError(t *testing.T) {
	w := newBufferedObjectWriter(
		"cap-1",
		time.Hour,
		10*1024*1024,
		func(ts time.Time, seq uint64) string { return "obj" },
		func(_ context.Context, _ string, _ []byte) error { return nil },
	)
	_ = w.Close()

	err := w.Write(context.Background(), &CapturedRequest{ID: "x", Method: "GET", Path: "/"})
	if !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("expected ErrWriterClosed, got %v", err)
	}
}

func TestBufferedObjectWriter_FlushAfterCloseReturnsError(t *testing.T) {
	w := newBufferedObjectWriter(
		"cap-1",
		time.Hour,
		10*1024*1024,
		func(ts time.Time, seq uint64) string { return "obj" },
		func(_ context.Context, _ string, _ []byte) error { return nil },
	)
	_ = w.Close()

	err := w.Flush(context.Background())
	if !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("expected ErrWriterClosed, got %v", err)
	}
}

func TestBufferedObjectWriter_SequenceIncrements(t *testing.T) {
	var objectNames []string
	w := newBufferedObjectWriter(
		"cap-1",
		time.Hour,
		50, // small buffer to auto-flush
		func(ts time.Time, seq uint64) string {
			name := fmt.Sprintf("obj-%d", seq)
			objectNames = append(objectNames, name)
			return name
		},
		func(_ context.Context, _ string, _ []byte) error { return nil },
	)

	for i := 0; i < 3; i++ {
		req := &CapturedRequest{ID: fmt.Sprintf("req-%d", i), Method: "GET", Path: "/this-is-a-long-path-to-exceed-buffer"}
		if err := w.Write(context.Background(), req); err != nil {
			t.Fatalf("write %d failed: %v", i, err)
		}
	}

	if len(objectNames) < 2 {
		t.Fatalf("expected at least 2 flushes, got %d", len(objectNames))
	}

	// Verify sequences are monotonically increasing
	for i := 1; i < len(objectNames); i++ {
		if objectNames[i] <= objectNames[i-1] {
			t.Fatalf("object names not monotonically increasing: %v", objectNames)
		}
	}
}

func TestBufferedObjectWriter_TimeBasedFlush(t *testing.T) {
	var uploads int
	fakeNow := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	w := newBufferedObjectWriter(
		"cap-1",
		1*time.Second, // 1s flush interval
		10*1024*1024,
		func(ts time.Time, seq uint64) string { return "obj" },
		func(_ context.Context, _ string, _ []byte) error {
			uploads++
			return nil
		},
	)
	// Override the now function and lastFlush to control time
	w.now = func() time.Time { return fakeNow }
	w.lastFlush = fakeNow

	// First write: no flush (within interval)
	req := &CapturedRequest{ID: "1", Method: "GET", Path: "/"}
	if err := w.Write(context.Background(), req); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if uploads != 0 {
		t.Fatal("should not have flushed yet")
	}

	// Advance time past the flush interval
	fakeNow = fakeNow.Add(2 * time.Second)
	req = &CapturedRequest{ID: "2", Method: "GET", Path: "/"}
	if err := w.Write(context.Background(), req); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if uploads != 1 {
		t.Fatalf("expected 1 time-based flush, got %d", uploads)
	}
}

// ---------------------------------------------------------------------------
// factory.go tests
// ---------------------------------------------------------------------------

func TestAsS3Config(t *testing.T) {
	t.Run("value type", func(t *testing.T) {
		cfg := S3Config{Bucket: "b"}
		got, err := asS3Config(cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.Bucket != "b" {
			t.Fatalf("got bucket %q, want %q", got.Bucket, "b")
		}
	})

	t.Run("pointer type", func(t *testing.T) {
		cfg := &S3Config{Bucket: "b"}
		got, err := asS3Config(cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.Bucket != "b" {
			t.Fatalf("got bucket %q, want %q", got.Bucket, "b")
		}
	})

	t.Run("nil pointer", func(t *testing.T) {
		var cfg *S3Config
		_, err := asS3Config(cfg)
		if err == nil {
			t.Fatal("expected error for nil pointer")
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		_, err := asS3Config("not a config")
		if err == nil {
			t.Fatal("expected error for wrong type")
		}
	})
}

func TestAsGCSConfig(t *testing.T) {
	t.Run("value type", func(t *testing.T) {
		cfg := GCSConfig{Bucket: "b"}
		got, err := asGCSConfig(cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.Bucket != "b" {
			t.Fatalf("got bucket %q", got.Bucket)
		}
	})

	t.Run("pointer type", func(t *testing.T) {
		cfg := &GCSConfig{Bucket: "b"}
		got, err := asGCSConfig(cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.Bucket != "b" {
			t.Fatalf("got bucket %q", got.Bucket)
		}
	})

	t.Run("nil pointer", func(t *testing.T) {
		var cfg *GCSConfig
		_, err := asGCSConfig(cfg)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		_, err := asGCSConfig(42)
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestAsEFSConfig(t *testing.T) {
	t.Run("value type", func(t *testing.T) {
		cfg := EFSConfig{MountPath: "/mnt"}
		got, err := asEFSConfig(cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.MountPath != "/mnt" {
			t.Fatalf("got %q", got.MountPath)
		}
	})

	t.Run("pointer type", func(t *testing.T) {
		cfg := &EFSConfig{MountPath: "/mnt"}
		got, err := asEFSConfig(cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.MountPath != "/mnt" {
			t.Fatalf("got %q", got.MountPath)
		}
	})

	t.Run("nil pointer", func(t *testing.T) {
		var cfg *EFSConfig
		_, err := asEFSConfig(cfg)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		_, err := asEFSConfig(true)
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestAsEBSConfig(t *testing.T) {
	t.Run("value type", func(t *testing.T) {
		cfg := EBSConfig{MountPath: "/mnt"}
		got, err := asEBSConfig(cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.MountPath != "/mnt" {
			t.Fatalf("got %q", got.MountPath)
		}
	})

	t.Run("pointer type", func(t *testing.T) {
		cfg := &EBSConfig{MountPath: "/mnt"}
		got, err := asEBSConfig(cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.MountPath != "/mnt" {
			t.Fatalf("got %q", got.MountPath)
		}
	})

	t.Run("nil pointer", func(t *testing.T) {
		var cfg *EBSConfig
		_, err := asEBSConfig(cfg)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		_, err := asEBSConfig(3.14)
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestNewWriterFactory_UnsupportedType(t *testing.T) {
	_, err := NewWriterFactory("unknown", nil)
	if err == nil {
		t.Fatal("expected error for unsupported type")
	}
	if !strings.Contains(err.Error(), "unsupported storage type") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewWriterFactory_CaseInsensitive(t *testing.T) {
	// Use EFS since it doesn't require external clients
	dir := t.TempDir()

	for _, storageType := range []string{"EFS", "Efs", "efs", " EFS "} {
		t.Run(storageType, func(t *testing.T) {
			cfg := EFSConfig{MountPath: dir}
			factory, err := NewWriterFactory(storageType, cfg)
			if err != nil {
				t.Fatalf("NewWriterFactory(%q) error: %v", storageType, err)
			}
			if factory == nil {
				t.Fatal("expected non-nil factory")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// s3.go tests
// ---------------------------------------------------------------------------

func TestNewS3WriterFactory_EmptyBucket(t *testing.T) {
	_, err := NewS3WriterFactory(context.Background(), S3Config{
		Bucket: "",
		Client: &mockS3Client{},
	})
	if err == nil {
		t.Fatal("expected error for empty bucket")
	}
}

func TestNewS3WriterFactory_ValidConfig(t *testing.T) {
	factory, err := NewS3WriterFactory(context.Background(), S3Config{
		Bucket: "my-bucket",
		Client: &mockS3Client{},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if factory == nil {
		t.Fatal("expected non-nil factory")
	}
}

func TestS3WriterFactory_NewWriter_EmptyCaptureID(t *testing.T) {
	factory, _ := NewS3WriterFactory(context.Background(), S3Config{
		Bucket: "my-bucket",
		Client: &mockS3Client{},
	})
	_, err := factory.NewWriter(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty captureID")
	}
}

func TestS3WriterFactory_NewWriter_Valid(t *testing.T) {
	factory, _ := NewS3WriterFactory(context.Background(), S3Config{
		Bucket: "my-bucket",
		Client: &mockS3Client{},
	})
	writer, err := factory.NewWriter(context.Background(), "cap-123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if writer == nil {
		t.Fatal("expected non-nil writer")
	}
}

func TestS3Writer_Write(t *testing.T) {
	mock := &mockS3Client{}
	factory, _ := NewS3WriterFactory(context.Background(), S3Config{
		Bucket:         "test-bucket",
		Prefix:         "prefix",
		AgentPod:       "pod-1",
		Client:         mock,
		MaxBufferBytes: 50, // small to trigger flush
	})

	writer, _ := factory.NewWriter(context.Background(), "cap-1")
	req := &CapturedRequest{ID: "s3-test", Method: "PUT", Path: "/very-long-path-to-exceed-tiny-buffer"}
	if err := writer.Write(context.Background(), req); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()
	if len(mock.calls) != 1 {
		t.Fatalf("expected 1 PutObject call, got %d", len(mock.calls))
	}
	if mock.calls[0].Bucket != "test-bucket" {
		t.Fatalf("unexpected bucket: %s", mock.calls[0].Bucket)
	}
	if !strings.HasPrefix(mock.calls[0].Key, "prefix/cap-1/") {
		t.Fatalf("unexpected key: %s", mock.calls[0].Key)
	}

	// Verify the body is valid gzip
	reader, err := gzip.NewReader(bytes.NewReader(mock.calls[0].Body))
	if err != nil {
		t.Fatalf("gzip read failed: %v", err)
	}
	data, _ := io.ReadAll(reader)
	reader.Close()
	if !bytes.Contains(data, []byte(`"s3-test"`)) {
		t.Fatalf("decompressed data missing expected content: %s", data)
	}
}

func TestS3Writer_PutObjectError(t *testing.T) {
	mock := &mockS3Client{err: fmt.Errorf("access denied")}
	factory, _ := NewS3WriterFactory(context.Background(), S3Config{
		Bucket:         "test-bucket",
		Client:         mock,
		MaxBufferBytes: 50,
	})

	writer, _ := factory.NewWriter(context.Background(), "cap-1")
	req := &CapturedRequest{ID: "err-test", Method: "GET", Path: "/long-path-to-exceed-buffer-limit"}
	err := writer.Write(context.Background(), req)
	if err == nil {
		t.Fatal("expected error from PutObject failure")
	}
	if !strings.Contains(err.Error(), "access denied") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// gcs.go tests
// ---------------------------------------------------------------------------

func TestNewGCSWriterFactory_EmptyBucket(t *testing.T) {
	_, err := NewGCSWriterFactory(context.Background(), GCSConfig{
		Bucket: "",
		OpenWriter: func(ctx context.Context, bucket, objectName string) (gcsObjectWriter, error) {
			return &mockGCSObjectWriter{}, nil
		},
	})
	if err == nil {
		t.Fatal("expected error for empty bucket")
	}
}

func TestNewGCSWriterFactory_ValidConfig(t *testing.T) {
	factory, err := NewGCSWriterFactory(context.Background(), GCSConfig{
		Bucket: "my-bucket",
		OpenWriter: func(ctx context.Context, bucket, objectName string) (gcsObjectWriter, error) {
			return &mockGCSObjectWriter{}, nil
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if factory == nil {
		t.Fatal("expected non-nil factory")
	}
}

func TestGCSWriterFactory_NewWriter_EmptyCaptureID(t *testing.T) {
	factory, _ := NewGCSWriterFactory(context.Background(), GCSConfig{
		Bucket: "my-bucket",
		OpenWriter: func(ctx context.Context, bucket, objectName string) (gcsObjectWriter, error) {
			return &mockGCSObjectWriter{}, nil
		},
	})
	_, err := factory.NewWriter(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty captureID")
	}
}

func TestGCSWriterFactory_NewWriter_Valid(t *testing.T) {
	factory, _ := NewGCSWriterFactory(context.Background(), GCSConfig{
		Bucket: "my-bucket",
		OpenWriter: func(ctx context.Context, bucket, objectName string) (gcsObjectWriter, error) {
			return &mockGCSObjectWriter{}, nil
		},
	})
	writer, err := factory.NewWriter(context.Background(), "cap-456")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if writer == nil {
		t.Fatal("expected non-nil writer")
	}
}

func TestGCSWriter_Write(t *testing.T) {
	var mu sync.Mutex
	var writers []*mockGCSObjectWriter

	factory, _ := NewGCSWriterFactory(context.Background(), GCSConfig{
		Bucket:         "test-bucket",
		Prefix:         "gcs-prefix",
		AgentPod:       "gcs-pod",
		MaxBufferBytes: 50,
		OpenWriter: func(ctx context.Context, bucket, objectName string) (gcsObjectWriter, error) {
			w := &mockGCSObjectWriter{}
			mu.Lock()
			writers = append(writers, w)
			mu.Unlock()
			return w, nil
		},
	})

	writer, _ := factory.NewWriter(context.Background(), "cap-gcs")
	req := &CapturedRequest{ID: "gcs-test", Method: "DELETE", Path: "/a-long-enough-path-to-trigger-flush"}
	if err := writer.Write(context.Background(), req); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(writers) != 1 {
		t.Fatalf("expected 1 OpenWriter call, got %d", len(writers))
	}
	w := writers[0]
	if w.contentType != contentTypeJSONL {
		t.Fatalf("unexpected content type: %s", w.contentType)
	}
	if w.contentEncoding != contentEncodingGZIP {
		t.Fatalf("unexpected content encoding: %s", w.contentEncoding)
	}
	if !w.closed {
		t.Fatal("expected writer to be closed")
	}

	// Verify data is valid gzip
	reader, err := gzip.NewReader(bytes.NewReader(w.data))
	if err != nil {
		t.Fatalf("gzip read failed: %v", err)
	}
	data, _ := io.ReadAll(reader)
	reader.Close()
	if !bytes.Contains(data, []byte(`"gcs-test"`)) {
		t.Fatalf("decompressed data missing expected content: %s", data)
	}
}

func TestGCSWriter_OpenWriterError(t *testing.T) {
	factory, _ := NewGCSWriterFactory(context.Background(), GCSConfig{
		Bucket:         "test-bucket",
		MaxBufferBytes: 50,
		OpenWriter: func(ctx context.Context, bucket, objectName string) (gcsObjectWriter, error) {
			return nil, fmt.Errorf("permission denied")
		},
	})

	writer, _ := factory.NewWriter(context.Background(), "cap-err")
	req := &CapturedRequest{ID: "err", Method: "GET", Path: "/long-path-to-force-buffer-flush-on-write"}
	err := writer.Write(context.Background(), req)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGCSWriter_WriteError(t *testing.T) {
	factory, _ := NewGCSWriterFactory(context.Background(), GCSConfig{
		Bucket:         "test-bucket",
		MaxBufferBytes: 50,
		OpenWriter: func(ctx context.Context, bucket, objectName string) (gcsObjectWriter, error) {
			return &mockGCSObjectWriter{writeErr: fmt.Errorf("write failed")}, nil
		},
	})

	writer, _ := factory.NewWriter(context.Background(), "cap-err2")
	req := &CapturedRequest{ID: "err2", Method: "GET", Path: "/long-path-to-force-buffer-flush-here-too"}
	err := writer.Write(context.Background(), req)
	if err == nil {
		t.Fatal("expected error")
	}
}

// ---------------------------------------------------------------------------
// efs.go / ebs.go tests
// ---------------------------------------------------------------------------

func TestNewEFSWriterFactory_EmptyMountPath(t *testing.T) {
	_, err := NewEFSWriterFactory(EFSConfig{MountPath: ""})
	if err == nil {
		t.Fatal("expected error for empty mount path")
	}
}

func TestNewEFSWriterFactory_Valid(t *testing.T) {
	factory, err := NewEFSWriterFactory(EFSConfig{MountPath: "/mnt/efs"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if factory == nil {
		t.Fatal("expected non-nil factory")
	}
}

func TestNewEBSWriterFactory_EmptyMountPath(t *testing.T) {
	_, err := NewEBSWriterFactory(EBSConfig{MountPath: ""})
	if err == nil {
		t.Fatal("expected error for empty mount path")
	}
}

func TestNewEBSWriterFactory_Valid(t *testing.T) {
	factory, err := NewEBSWriterFactory(EBSConfig{MountPath: "/mnt/ebs"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if factory == nil {
		t.Fatal("expected non-nil factory")
	}
}

func TestEFSWriter_EmptyCaptureID(t *testing.T) {
	factory, _ := NewEFSWriterFactory(EFSConfig{MountPath: t.TempDir()})
	_, err := factory.NewWriter(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty captureID")
	}
}

func TestEBSWriter_EmptyCaptureID(t *testing.T) {
	factory, _ := NewEBSWriterFactory(EBSConfig{MountPath: t.TempDir()})
	_, err := factory.NewWriter(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty captureID")
	}
}

func testFilesystemWriter(t *testing.T, factoryFn func(dir string) (WriterFactory, error)) {
	t.Helper()
	dir := t.TempDir()
	factory, err := factoryFn(dir)
	if err != nil {
		t.Fatalf("factory creation failed: %v", err)
	}

	writer, err := factory.NewWriter(context.Background(), "cap-fs")
	if err != nil {
		t.Fatalf("new writer failed: %v", err)
	}

	req := &CapturedRequest{
		ID:     "fs-test",
		Method: "GET",
		Path:   "/hello",
	}
	if err := writer.Write(context.Background(), req); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if err := writer.Flush(context.Background()); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	// Find the written file
	var found string
	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, ".jsonl.gz") {
			found = path
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk failed: %v", err)
	}
	if found == "" {
		t.Fatal("no .jsonl.gz file found on disk")
	}

	// Verify the file contains valid gzip data with our request
	data, err := os.ReadFile(found)
	if err != nil {
		t.Fatalf("read file failed: %v", err)
	}

	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("gzip reader failed: %v", err)
	}
	decompressed, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("decompress failed: %v", err)
	}
	reader.Close()

	if !bytes.Contains(decompressed, []byte(`"fs-test"`)) {
		t.Fatalf("file content missing expected data: %s", decompressed)
	}

	// Verify it's valid JSONL
	var decoded CapturedRequest
	if err := json.Unmarshal(bytes.TrimSpace(decompressed), &decoded); err != nil {
		t.Fatalf("invalid JSON in file: %v", err)
	}
	if decoded.ID != "fs-test" {
		t.Fatalf("unexpected ID: %s", decoded.ID)
	}
}

func TestEFSWriter_FilesystemWrite(t *testing.T) {
	testFilesystemWriter(t, func(dir string) (WriterFactory, error) {
		return NewEFSWriterFactory(EFSConfig{
			MountPath: dir,
			AgentPod:  "test-pod",
		})
	})
}

func TestEBSWriter_FilesystemWrite(t *testing.T) {
	testFilesystemWriter(t, func(dir string) (WriterFactory, error) {
		return NewEBSWriterFactory(EBSConfig{
			MountPath: dir,
			AgentPod:  "test-pod",
		})
	})
}

func TestFilesystemWriter_MultipleFlushes(t *testing.T) {
	dir := t.TempDir()
	factory, _ := NewEFSWriterFactory(EFSConfig{
		MountPath:      dir,
		AgentPod:       "multi-pod",
		MaxBufferBytes: 50, // small to auto-flush
	})

	writer, _ := factory.NewWriter(context.Background(), "cap-multi")

	for i := 0; i < 5; i++ {
		req := &CapturedRequest{
			ID:     fmt.Sprintf("req-%d", i),
			Method: "GET",
			Path:   "/this-is-a-path-long-enough-to-trigger-flush",
		}
		if err := writer.Write(context.Background(), req); err != nil {
			t.Fatalf("write %d failed: %v", i, err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Count files created
	var fileCount int
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, ".jsonl.gz") {
			fileCount++
		}
		return nil
	})
	if fileCount < 2 {
		t.Fatalf("expected multiple files from multiple flushes, got %d", fileCount)
	}
}

// ---------------------------------------------------------------------------
// NewWriterFactory integration with type assertion helpers
// ---------------------------------------------------------------------------

func TestNewWriterFactory_S3(t *testing.T) {
	factory, err := NewWriterFactory("s3", S3Config{
		Bucket: "test",
		Client: &mockS3Client{},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if factory == nil {
		t.Fatal("expected non-nil factory")
	}
}

func TestNewWriterFactory_GCS(t *testing.T) {
	factory, err := NewWriterFactory("gcs", GCSConfig{
		Bucket: "test",
		OpenWriter: func(ctx context.Context, bucket, objectName string) (gcsObjectWriter, error) {
			return &mockGCSObjectWriter{}, nil
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if factory == nil {
		t.Fatal("expected non-nil factory")
	}
}

func TestNewWriterFactory_EFS(t *testing.T) {
	factory, err := NewWriterFactory("efs", EFSConfig{MountPath: t.TempDir()})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if factory == nil {
		t.Fatal("expected non-nil factory")
	}
}

func TestNewWriterFactory_EBS(t *testing.T) {
	factory, err := NewWriterFactory("ebs", EBSConfig{MountPath: t.TempDir()})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if factory == nil {
		t.Fatal("expected non-nil factory")
	}
}

func TestNewWriterFactory_WrongConfigType(t *testing.T) {
	_, err := NewWriterFactory("s3", GCSConfig{Bucket: "b"})
	if err == nil {
		t.Fatal("expected error for wrong config type")
	}
}

// ---------------------------------------------------------------------------
// Close idempotency
// ---------------------------------------------------------------------------

func TestBufferedObjectWriter_CloseIdempotent(t *testing.T) {
	w := newBufferedObjectWriter(
		"cap-1",
		time.Hour,
		10*1024*1024,
		func(ts time.Time, seq uint64) string { return "obj" },
		func(_ context.Context, _ string, _ []byte) error { return nil },
	)

	if err := w.Close(); err != nil {
		t.Fatalf("first close failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second close should succeed (idempotent), got: %v", err)
	}
}
