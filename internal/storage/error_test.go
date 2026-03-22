package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// ---------------------------------------------------------------------------
// S3: upload failure
// ---------------------------------------------------------------------------

type errorS3Client struct {
	err error
}

func (e *errorS3Client) PutObject(_ context.Context, _ *awss3.PutObjectInput, _ ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	return nil, e.err
}

func TestS3Writer_UploadFailure(t *testing.T) {
	uploadErr := errors.New("access denied")

	factory, err := NewS3WriterFactory(context.Background(), S3Config{
		Bucket:   "captures",
		AgentPod: "agent-a",
		Client:   &errorS3Client{err: uploadErr},
	})
	if err != nil {
		t.Fatalf("NewS3WriterFactory() error = %v", err)
	}

	writer, err := factory.NewWriter(context.Background(), "capture-1")
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}

	req := &CapturedRequest{ID: "req-1", Timestamp: time.Now(), Method: "GET", Path: "/test", Protocol: "HTTP"}
	if err := writer.Write(context.Background(), req); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	err = writer.Flush(context.Background())
	if err == nil {
		t.Fatal("expected error from S3 upload failure")
	}
}

// ---------------------------------------------------------------------------
// S3: empty bucket validation
// ---------------------------------------------------------------------------

func TestS3WriterFactory_EmptyBucket(t *testing.T) {
	_, err := NewS3WriterFactory(context.Background(), S3Config{Bucket: ""})
	if err == nil {
		t.Fatal("expected error for empty bucket")
	}
}

func TestS3Writer_EmptyCaptureID(t *testing.T) {
	factory, err := NewS3WriterFactory(context.Background(), S3Config{
		Bucket: "test",
		Client: &fakeS3Client{},
	})
	if err != nil {
		t.Fatalf("NewS3WriterFactory() error = %v", err)
	}

	_, err = factory.NewWriter(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty capture ID")
	}
}

// ---------------------------------------------------------------------------
// GCS: writer open failure
// ---------------------------------------------------------------------------

func TestGCSWriter_OpenWriterFailure(t *testing.T) {
	openErr := errors.New("gcs open failed")

	factory, err := NewGCSWriterFactory(context.Background(), GCSConfig{
		Bucket: "captures",
		OpenWriter: func(_ context.Context, _, _ string) (gcsObjectWriter, error) {
			return nil, openErr
		},
	})
	if err != nil {
		t.Fatalf("NewGCSWriterFactory() error = %v", err)
	}

	writer, err := factory.NewWriter(context.Background(), "capture-1")
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}

	req := &CapturedRequest{ID: "req-1", Timestamp: time.Now(), Method: "GET", Path: "/test", Protocol: "HTTP"}
	_ = writer.Write(context.Background(), req)

	err = writer.Flush(context.Background())
	if err == nil {
		t.Fatal("expected error from GCS open failure")
	}
}

// ---------------------------------------------------------------------------
// GCS: write failure
// ---------------------------------------------------------------------------

type failingGCSWriter struct {
	writeErr error
	closeErr error
}

func (f *failingGCSWriter) Write([]byte) (int, error)  { return 0, f.writeErr }
func (f *failingGCSWriter) Close() error                { return f.closeErr }
func (f *failingGCSWriter) SetContentType(string)       {}
func (f *failingGCSWriter) SetContentEncoding(string)   {}

func TestGCSWriter_WriteFailure(t *testing.T) {
	writeErr := errors.New("gcs write failed")

	factory, err := NewGCSWriterFactory(context.Background(), GCSConfig{
		Bucket: "captures",
		OpenWriter: func(_ context.Context, _, _ string) (gcsObjectWriter, error) {
			return &failingGCSWriter{writeErr: writeErr}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewGCSWriterFactory() error = %v", err)
	}

	writer, err := factory.NewWriter(context.Background(), "capture-1")
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}

	req := &CapturedRequest{ID: "req-1", Timestamp: time.Now(), Method: "GET", Path: "/test", Protocol: "HTTP"}
	_ = writer.Write(context.Background(), req)

	err = writer.Flush(context.Background())
	if err == nil {
		t.Fatal("expected error from GCS write failure")
	}
}

// ---------------------------------------------------------------------------
// GCS: close failure
// ---------------------------------------------------------------------------

func TestGCSWriter_CloseFailure(t *testing.T) {
	closeErr := errors.New("gcs close failed")

	factory, err := NewGCSWriterFactory(context.Background(), GCSConfig{
		Bucket: "captures",
		OpenWriter: func(_ context.Context, _, _ string) (gcsObjectWriter, error) {
			return &failingGCSWriter{closeErr: closeErr}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewGCSWriterFactory() error = %v", err)
	}

	writer, err := factory.NewWriter(context.Background(), "capture-1")
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}

	req := &CapturedRequest{ID: "req-1", Timestamp: time.Now(), Method: "GET", Path: "/test", Protocol: "HTTP"}
	_ = writer.Write(context.Background(), req)

	err = writer.Flush(context.Background())
	if err == nil {
		t.Fatal("expected error from GCS close failure")
	}
}

// ---------------------------------------------------------------------------
// GCS: empty bucket validation
// ---------------------------------------------------------------------------

func TestGCSWriterFactory_EmptyBucket(t *testing.T) {
	_, err := NewGCSWriterFactory(context.Background(), GCSConfig{Bucket: ""})
	if err == nil {
		t.Fatal("expected error for empty bucket")
	}
}

func TestGCSWriter_EmptyCaptureID(t *testing.T) {
	factory, err := NewGCSWriterFactory(context.Background(), GCSConfig{
		Bucket: "test",
		OpenWriter: func(_ context.Context, _, _ string) (gcsObjectWriter, error) {
			return &fakeGCSWriter{}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewGCSWriterFactory() error = %v", err)
	}

	_, err = factory.NewWriter(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty capture ID")
	}
}

// ---------------------------------------------------------------------------
// EFS: validation
// ---------------------------------------------------------------------------

func TestEFSWriterFactory_EmptyMountPath(t *testing.T) {
	_, err := NewEFSWriterFactory(EFSConfig{MountPath: ""})
	if err == nil {
		t.Fatal("expected error for empty mount path")
	}
}

func TestEFSWriter_EmptyCaptureID(t *testing.T) {
	factory, err := NewEFSWriterFactory(EFSConfig{MountPath: t.TempDir()})
	if err != nil {
		t.Fatalf("NewEFSWriterFactory() error = %v", err)
	}

	_, err = factory.NewWriter(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty capture ID")
	}
}

// ---------------------------------------------------------------------------
// EBS: validation
// ---------------------------------------------------------------------------

func TestEBSWriterFactory_EmptyMountPath(t *testing.T) {
	_, err := NewEBSWriterFactory(EBSConfig{MountPath: ""})
	if err == nil {
		t.Fatal("expected error for empty mount path")
	}
}

func TestEBSWriter_EmptyCaptureID(t *testing.T) {
	factory, err := NewEBSWriterFactory(EBSConfig{MountPath: t.TempDir()})
	if err != nil {
		t.Fatalf("NewEBSWriterFactory() error = %v", err)
	}

	_, err = factory.NewWriter(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty capture ID")
	}
}

// ---------------------------------------------------------------------------
// Factory: config type validation
// ---------------------------------------------------------------------------

func TestAsS3Config_NilPointer(t *testing.T) {
	_, err := asS3Config((*S3Config)(nil))
	if err == nil {
		t.Fatal("expected error for nil S3Config pointer")
	}
}

func TestAsS3Config_InvalidType(t *testing.T) {
	_, err := asS3Config("invalid")
	if err == nil {
		t.Fatal("expected error for invalid config type")
	}
}

func TestAsS3Config_ValueType(t *testing.T) {
	cfg, err := asS3Config(S3Config{Bucket: "test"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Bucket != "test" {
		t.Errorf("expected bucket test, got %s", cfg.Bucket)
	}
}

func TestAsS3Config_PointerType(t *testing.T) {
	cfg, err := asS3Config(&S3Config{Bucket: "test"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Bucket != "test" {
		t.Errorf("expected bucket test, got %s", cfg.Bucket)
	}
}

func TestAsGCSConfig_NilPointer(t *testing.T) {
	_, err := asGCSConfig((*GCSConfig)(nil))
	if err == nil {
		t.Fatal("expected error for nil GCSConfig pointer")
	}
}

func TestAsGCSConfig_InvalidType(t *testing.T) {
	_, err := asGCSConfig(42)
	if err == nil {
		t.Fatal("expected error for invalid config type")
	}
}

func TestAsEFSConfig_NilPointer(t *testing.T) {
	_, err := asEFSConfig((*EFSConfig)(nil))
	if err == nil {
		t.Fatal("expected error for nil EFSConfig pointer")
	}
}

func TestAsEFSConfig_InvalidType(t *testing.T) {
	_, err := asEFSConfig(false)
	if err == nil {
		t.Fatal("expected error for invalid config type")
	}
}

func TestAsEBSConfig_NilPointer(t *testing.T) {
	_, err := asEBSConfig((*EBSConfig)(nil))
	if err == nil {
		t.Fatal("expected error for nil EBSConfig pointer")
	}
}

func TestAsEBSConfig_InvalidType(t *testing.T) {
	_, err := asEBSConfig([]byte{})
	if err == nil {
		t.Fatal("expected error for invalid config type")
	}
}

// ---------------------------------------------------------------------------
// bufferedObjectWriter: write/flush after close
// ---------------------------------------------------------------------------

func TestBufferedObjectWriter_WriteAfterClose(t *testing.T) {
	w := newBufferedObjectWriter("cap-1", time.Hour, 10*1024*1024,
		func(_ time.Time, _ uint64) string { return "test-obj" },
		func(_ context.Context, _ string, _ []byte) error { return nil },
	)

	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	req := &CapturedRequest{ID: "req-1", Method: "GET", Path: "/test", Protocol: "HTTP", Timestamp: time.Now()}
	err := w.Write(context.Background(), req)
	if !errors.Is(err, ErrWriterClosed) {
		t.Errorf("expected ErrWriterClosed, got %v", err)
	}
}

func TestBufferedObjectWriter_FlushAfterClose(t *testing.T) {
	w := newBufferedObjectWriter("cap-1", time.Hour, 10*1024*1024,
		func(_ time.Time, _ uint64) string { return "test-obj" },
		func(_ context.Context, _ string, _ []byte) error { return nil },
	)

	_ = w.Close()

	err := w.Flush(context.Background())
	if !errors.Is(err, ErrWriterClosed) {
		t.Errorf("expected ErrWriterClosed, got %v", err)
	}
}

func TestBufferedObjectWriter_CloseIdempotent(t *testing.T) {
	w := newBufferedObjectWriter("cap-1", time.Hour, 10*1024*1024,
		func(_ time.Time, _ uint64) string { return "test-obj" },
		func(_ context.Context, _ string, _ []byte) error { return nil },
	)

	if err := w.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

// ---------------------------------------------------------------------------
// bufferedObjectWriter: sequence increments
// ---------------------------------------------------------------------------

func TestBufferedObjectWriter_SequenceIncrements(t *testing.T) {
	var keys []string
	w := newBufferedObjectWriter("cap-1", time.Hour, 10*1024*1024,
		func(_ time.Time, seq uint64) string {
			key := objectFileName("agent", seq)
			keys = append(keys, key)
			return key
		},
		func(_ context.Context, _ string, _ []byte) error { return nil },
	)

	for i := 0; i < 3; i++ {
		req := &CapturedRequest{ID: "req", Method: "GET", Path: "/test", Protocol: "HTTP", Timestamp: time.Now()}
		_ = w.Write(context.Background(), req)
		_ = w.Flush(context.Background())
	}

	if len(keys) != 3 {
		t.Fatalf("expected 3 uploads, got %d", len(keys))
	}
	if keys[0] == keys[1] || keys[1] == keys[2] {
		t.Errorf("expected unique keys, got %v", keys)
	}
}

// ---------------------------------------------------------------------------
// bufferedObjectWriter: flush with empty buffer is no-op
// ---------------------------------------------------------------------------

func TestBufferedObjectWriter_FlushEmptyBuffer(t *testing.T) {
	callCount := 0
	w := newBufferedObjectWriter("cap-1", time.Hour, 10*1024*1024,
		func(_ time.Time, _ uint64) string { return "obj" },
		func(_ context.Context, _ string, _ []byte) error {
			callCount++
			return nil
		},
	)

	err := w.Flush(context.Background())
	if err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if callCount != 0 {
		t.Errorf("expected no upload on empty flush, got %d", callCount)
	}
}

// ---------------------------------------------------------------------------
// bufferedObjectWriter: upload error
// ---------------------------------------------------------------------------

func TestBufferedObjectWriter_UploadError(t *testing.T) {
	uploadErr := errors.New("upload failed")
	w := newBufferedObjectWriter("cap-1", time.Hour, 10*1024*1024,
		func(_ time.Time, _ uint64) string { return "obj" },
		func(_ context.Context, _ string, _ []byte) error { return uploadErr },
	)

	req := &CapturedRequest{ID: "req", Method: "GET", Path: "/test", Protocol: "HTTP", Timestamp: time.Now()}
	_ = w.Write(context.Background(), req)

	err := w.Flush(context.Background())
	if err == nil {
		t.Fatal("expected upload error")
	}
}

// ---------------------------------------------------------------------------
// marshalRequestLine: nil request
// ---------------------------------------------------------------------------

func TestMarshalRequestLine_Nil(t *testing.T) {
	_, err := marshalRequestLine(nil)
	if err == nil {
		t.Fatal("expected error for nil request")
	}
}
