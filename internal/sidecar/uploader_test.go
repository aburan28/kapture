package sidecar

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/kapture-io/kapture/internal/storage"
)

// mockWriter records all writes for testing.
type mockWriter struct {
	mu       sync.Mutex
	requests []*storage.CapturedRequest
	flushed  int
}

func (m *mockWriter) Write(_ context.Context, req *storage.CapturedRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests = append(m.requests, req)
	return nil
}

func (m *mockWriter) Flush(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushed++
	return nil
}

func (m *mockWriter) Close() error { return nil }

func TestUploaderJSONL(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.jsonl")

	content := `{"id":"req-1","method":"GET","path":"/foo","protocol":"HTTP"}
{"id":"req-2","method":"POST","path":"/bar","protocol":"HTTP"}
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	w := &mockWriter{}
	u := NewUploader(UploaderConfig{
		Writer: w,
		Format: "jsonl",
		Delete: true,
	})

	if err := u.Upload(context.Background(), path); err != nil {
		t.Fatal(err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.requests) != 2 {
		t.Fatalf("expected 2 requests, got %d", len(w.requests))
	}
	if w.requests[0].ID != "req-1" {
		t.Errorf("expected req-1, got %s", w.requests[0].ID)
	}
	if w.requests[1].ID != "req-2" {
		t.Errorf("expected req-2, got %s", w.requests[1].ID)
	}
	if w.flushed != 1 {
		t.Errorf("expected 1 flush, got %d", w.flushed)
	}

	// File should be deleted.
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("expected file to be deleted after upload")
	}
}

func TestUploaderJSONLNoDelete(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.jsonl")

	content := `{"id":"req-1","method":"GET","path":"/foo","protocol":"HTTP"}
`
	os.WriteFile(path, []byte(content), 0644)

	w := &mockWriter{}
	u := NewUploader(UploaderConfig{
		Writer: w,
		Format: "jsonl",
		Delete: false,
	})

	if err := u.Upload(context.Background(), path); err != nil {
		t.Fatal(err)
	}

	// File should still exist.
	if _, err := os.Stat(path); err != nil {
		t.Error("expected file to still exist when delete=false")
	}
}

func TestUploaderPCAP(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.pcap")

	data := []byte{0xd4, 0xc3, 0xb2, 0xa1, 0x02, 0x00} // fake pcap header bytes
	os.WriteFile(path, data, 0644)

	w := &mockWriter{}
	u := NewUploader(UploaderConfig{
		Writer: w,
		Format: "pcap",
		Delete: true,
	})

	if err := u.Upload(context.Background(), path); err != nil {
		t.Fatal(err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.requests) != 1 {
		t.Fatalf("expected 1 request, got %d", len(w.requests))
	}
	if w.requests[0].Protocol != "pcap" {
		t.Errorf("expected protocol pcap, got %s", w.requests[0].Protocol)
	}
	if len(w.requests[0].Body) != len(data) {
		t.Errorf("expected %d bytes, got %d", len(data), len(w.requests[0].Body))
	}
}

func TestUploaderSkipsMalformedLines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.jsonl")

	content := `not json
{"id":"req-1","method":"GET","path":"/foo","protocol":"HTTP"}
also not json
`
	os.WriteFile(path, []byte(content), 0644)

	w := &mockWriter{}
	u := NewUploader(UploaderConfig{
		Writer: w,
		Format: "jsonl",
		Delete: false,
	})

	if err := u.Upload(context.Background(), path); err != nil {
		t.Fatal(err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.requests) != 1 {
		t.Fatalf("expected 1 valid request (skipping malformed), got %d", len(w.requests))
	}
}
