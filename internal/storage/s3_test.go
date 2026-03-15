package storage

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

type fakeS3Client struct {
	bucket          string
	key             string
	contentType     string
	contentEncoding string
	body            []byte
	callCount       int
}

func (f *fakeS3Client) PutObject(_ context.Context, params *awss3.PutObjectInput, _ ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	f.callCount++
	f.bucket = aws.ToString(params.Bucket)
	f.key = aws.ToString(params.Key)
	f.contentType = aws.ToString(params.ContentType)
	f.contentEncoding = aws.ToString(params.ContentEncoding)
	body, err := io.ReadAll(params.Body)
	if err != nil {
		return nil, err
	}
	f.body = body
	return &awss3.PutObjectOutput{}, nil
}

func TestS3WriterFlushesJSONLGzipObject(t *testing.T) {
	factory, err := NewS3WriterFactory(context.Background(), S3Config{
		Bucket:   "captures",
		Prefix:   "prefix",
		AgentPod: "agent-a",
		Client:   &fakeS3Client{},
	})
	if err != nil {
		t.Fatalf("NewS3WriterFactory() error = %v", err)
	}

	writer, err := factory.NewWriter(context.Background(), "capture-1")
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}

	fixedNow := time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC)
	s3Writer := writer.(*S3Writer)
	s3Writer.now = func() time.Time { return fixedNow }

	req := &CapturedRequest{ID: "req-1", Timestamp: fixedNow, Method: "GET", Path: "/orders", Protocol: "HTTP", Body: []byte("hello")}
	if err := writer.Write(context.Background(), req); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := writer.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	client := factory.config.Client.(*fakeS3Client)
	if client.callCount != 1 {
		t.Fatalf("PutObject() calls = %d, want 1", client.callCount)
	}
	if client.bucket != "captures" || client.key != "prefix/capture-1/2026-03-15/agent-a-000001.jsonl.gz" {
		t.Fatalf("unexpected upload target bucket=%q key=%q", client.bucket, client.key)
	}
	if client.contentType != contentTypeJSONL || client.contentEncoding != contentEncodingGZIP {
		t.Fatalf("unexpected content metadata type=%q encoding=%q", client.contentType, client.contentEncoding)
	}

	lines := strings.Split(strings.TrimSpace(decompressPayload(t, client.body)), "\n")
	if len(lines) != 1 {
		t.Fatalf("line count = %d, want 1", len(lines))
	}

	var got CapturedRequest
	if err := json.Unmarshal([]byte(lines[0]), &got); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if got.ID != req.ID || got.Path != req.Path || got.Protocol != req.Protocol || !bytes.Equal(got.Body, req.Body) {
		t.Fatalf("unexpected request payload: %+v", got)
	}
}

func decompressPayload(t *testing.T, payload []byte) string {
	t.Helper()
	reader, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("gzip.NewReader() error = %v", err)
	}
	defer reader.Close()
	decoded, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	return string(decoded)
}
