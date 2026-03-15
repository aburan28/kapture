package storage

import (
	"context"
	"strings"
	"testing"
	"time"
)

type fakeGCSWriter struct {
	contentType     string
	contentEncoding string
	payload         []byte
	closed          bool
}

func (w *fakeGCSWriter) Write(p []byte) (int, error) {
	w.payload = append(w.payload, p...)
	return len(p), nil
}
func (w *fakeGCSWriter) Close() error                    { w.closed = true; return nil }
func (w *fakeGCSWriter) SetContentType(value string)     { w.contentType = value }
func (w *fakeGCSWriter) SetContentEncoding(value string) { w.contentEncoding = value }

func TestGCSWriterFlushesJSONLGzipObject(t *testing.T) {
	var bucketName, objectName string
	var createdWriter *fakeGCSWriter

	factory, err := NewGCSWriterFactory(context.Background(), GCSConfig{
		Bucket:   "captures",
		Prefix:   "prefix",
		AgentPod: "agent-b",
		OpenWriter: func(_ context.Context, bucket, object string) (gcsObjectWriter, error) {
			bucketName = bucket
			objectName = object
			createdWriter = &fakeGCSWriter{}
			return createdWriter, nil
		},
	})
	if err != nil {
		t.Fatalf("NewGCSWriterFactory() error = %v", err)
	}

	writer, err := factory.NewWriter(context.Background(), "capture-2")
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}

	fixedNow := time.Date(2026, 3, 15, 13, 0, 0, 0, time.UTC)
	gcsWriter := writer.(*GCSWriter)
	gcsWriter.now = func() time.Time { return fixedNow }

	if err := writer.Write(context.Background(), &CapturedRequest{ID: "req-2", Timestamp: fixedNow, Method: "POST", Path: "/grpc.Service/Call", Protocol: "gRPC"}); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := writer.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	if bucketName != "captures" || objectName != "prefix/capture-2/2026-03-15/agent-b-000001.jsonl.gz" {
		t.Fatalf("unexpected object destination bucket=%q object=%q", bucketName, objectName)
	}
	if createdWriter == nil || !createdWriter.closed {
		t.Fatalf("writer not closed")
	}
	if createdWriter.contentType != contentTypeJSONL || createdWriter.contentEncoding != contentEncodingGZIP {
		t.Fatalf("unexpected content settings type=%q encoding=%q", createdWriter.contentType, createdWriter.contentEncoding)
	}
	decoded := decompressPayload(t, createdWriter.payload)
	if !strings.Contains(decoded, `"protocol":"gRPC"`) || !strings.Contains(decoded, `"path":"/grpc.Service/Call"`) {
		t.Fatalf("unexpected payload %q", decoded)
	}
}
