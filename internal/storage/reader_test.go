package storage

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

// memoryObjectStore is an in-memory ObjectStore for testing.
type memoryObjectStore struct {
	objects map[string][]byte
	listErr error
	dlErr   error
}

func newMemoryObjectStore() *memoryObjectStore {
	return &memoryObjectStore{objects: make(map[string][]byte)}
}

func (m *memoryObjectStore) put(key string, data []byte) {
	m.objects[key] = data
}

func (m *memoryObjectStore) putRequests(key string, requests ...*CapturedRequest) error {
	var buf bytes.Buffer
	for _, req := range requests {
		line, err := json.Marshal(req)
		if err != nil {
			return err
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}

	compressed, err := compressGZIP(buf.Bytes())
	if err != nil {
		return err
	}
	m.objects[key] = compressed
	return nil
}

func (m *memoryObjectStore) ListObjects(_ context.Context, prefix string) ([]string, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	var keys []string
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func (m *memoryObjectStore) Download(_ context.Context, key string) (io.ReadCloser, error) {
	if m.dlErr != nil {
		return nil, m.dlErr
	}
	data, ok := m.objects[key]
	if !ok {
		return nil, fmt.Errorf("object %q not found", key)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func TestObjectStoreReaderFactory(t *testing.T) {
	store := newMemoryObjectStore()
	factory := NewObjectStoreReaderFactory(store, "prefix")

	t.Run("empty capture ID", func(t *testing.T) {
		_, err := factory.NewReader(context.Background(), "")
		if err == nil {
			t.Fatal("expected error for empty capture ID")
		}
	})

	t.Run("valid capture ID", func(t *testing.T) {
		reader, err := factory.NewReader(context.Background(), "capture-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if reader == nil {
			t.Fatal("expected non-nil reader")
		}
	})
}

func TestObjectStoreReaderRead(t *testing.T) {
	store := newMemoryObjectStore()

	req1 := &CapturedRequest{
		ID: "req-1", Method: "GET", Path: "/api/v1/users",
		Protocol: "HTTP", Timestamp: time.Now().UTC(),
	}
	req2 := &CapturedRequest{
		ID: "req-2", Method: "POST", Path: "/api/v1/orders",
		Protocol: "HTTP", Timestamp: time.Now().UTC(),
		Body: []byte(`{"item":"widget"}`),
	}

	if err := store.putRequests("prefix/capture-1/obj1.jsonl.gz", req1, req2); err != nil {
		t.Fatalf("setup: %v", err)
	}

	factory := NewObjectStoreReaderFactory(store, "prefix")
	reader, _ := factory.NewReader(context.Background(), "capture-1")

	var got []*CapturedRequest
	for req, err := range reader.Read(context.Background()) {
		if err != nil {
			t.Fatalf("unexpected read error: %v", err)
		}
		got = append(got, req)
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 requests, got %d", len(got))
	}
	if got[0].ID != "req-1" {
		t.Errorf("expected req-1, got %s", got[0].ID)
	}
	if got[1].ID != "req-2" {
		t.Errorf("expected req-2, got %s", got[1].ID)
	}
	if string(got[1].Body) != `{"item":"widget"}` {
		t.Errorf("unexpected body: %s", got[1].Body)
	}
}

func TestObjectStoreReaderMultipleObjects(t *testing.T) {
	store := newMemoryObjectStore()

	req1 := &CapturedRequest{ID: "req-1", Method: "GET", Path: "/a", Protocol: "HTTP", Timestamp: time.Now().UTC()}
	req2 := &CapturedRequest{ID: "req-2", Method: "GET", Path: "/b", Protocol: "HTTP", Timestamp: time.Now().UTC()}
	req3 := &CapturedRequest{ID: "req-3", Method: "GET", Path: "/c", Protocol: "HTTP", Timestamp: time.Now().UTC()}

	_ = store.putRequests("test/cap-2/obj1.jsonl.gz", req1)
	_ = store.putRequests("test/cap-2/obj2.jsonl.gz", req2, req3)

	factory := NewObjectStoreReaderFactory(store, "test")
	reader, _ := factory.NewReader(context.Background(), "cap-2")

	var count int
	for _, err := range reader.Read(context.Background()) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		count++
	}

	if count != 3 {
		t.Errorf("expected 3 requests across 2 objects, got %d", count)
	}
}

func TestObjectStoreReaderListError(t *testing.T) {
	store := newMemoryObjectStore()
	store.listErr = fmt.Errorf("list failed")

	factory := NewObjectStoreReaderFactory(store, "")
	reader, _ := factory.NewReader(context.Background(), "cap-3")

	var gotErr error
	for _, err := range reader.Read(context.Background()) {
		if err != nil {
			gotErr = err
			break
		}
	}

	if gotErr == nil {
		t.Fatal("expected error from listing")
	}
}

func TestObjectStoreReaderDownloadError(t *testing.T) {
	store := newMemoryObjectStore()
	// Put a key but make downloads fail.
	store.objects["cap-4/obj.jsonl.gz"] = nil
	store.dlErr = fmt.Errorf("download failed")

	factory := NewObjectStoreReaderFactory(store, "")
	reader, _ := factory.NewReader(context.Background(), "cap-4")

	var gotErr error
	for _, err := range reader.Read(context.Background()) {
		if err != nil {
			gotErr = err
			break
		}
	}

	if gotErr == nil {
		t.Fatal("expected error from download")
	}
}

func TestObjectStoreReaderContextCancellation(t *testing.T) {
	store := newMemoryObjectStore()

	for i := 0; i < 10; i++ {
		req := &CapturedRequest{ID: fmt.Sprintf("req-%d", i), Method: "GET", Path: "/test", Protocol: "HTTP", Timestamp: time.Now().UTC()}
		_ = store.putRequests(fmt.Sprintf("cap-5/obj%d.jsonl.gz", i), req)
	}

	factory := NewObjectStoreReaderFactory(store, "")
	reader, _ := factory.NewReader(context.Background(), "cap-5")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	var gotErr error
	for _, err := range reader.Read(ctx) {
		if err != nil {
			gotErr = err
			break
		}
	}

	if gotErr == nil {
		t.Fatal("expected context cancellation error")
	}
}

func TestDecodeJSONLGzip(t *testing.T) {
	t.Run("invalid gzip", func(t *testing.T) {
		r := bytes.NewReader([]byte("not gzip data"))
		var gotErr error
		decodeJSONLGzip(r, func(req *CapturedRequest, err error) bool {
			if err != nil {
				gotErr = err
				return false
			}
			return true
		})
		if gotErr == nil {
			t.Fatal("expected error for invalid gzip")
		}
	})

	t.Run("invalid JSON line", func(t *testing.T) {
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		_, _ = gz.Write([]byte("not json\n"))
		_ = gz.Close()

		var gotErr error
		decodeJSONLGzip(&buf, func(req *CapturedRequest, err error) bool {
			if err != nil {
				gotErr = err
				return true // Continue to test error recovery.
			}
			return true
		})
		if gotErr == nil {
			t.Fatal("expected error for invalid JSON")
		}
	})

	t.Run("empty lines skipped", func(t *testing.T) {
		req := &CapturedRequest{ID: "req-1", Method: "GET", Path: "/test", Protocol: "HTTP"}
		line, _ := json.Marshal(req)

		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		_, _ = gz.Write([]byte("\n"))
		_, _ = gz.Write(line)
		_, _ = gz.Write([]byte("\n\n"))
		_ = gz.Close()

		var count int
		decodeJSONLGzip(&buf, func(req *CapturedRequest, err error) bool {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			count++
			return true
		})
		if count != 1 {
			t.Errorf("expected 1 request, got %d", count)
		}
	})
}

func TestBuildPrefix(t *testing.T) {
	tests := []struct {
		prefix    string
		captureID string
		want      string
	}{
		{"", "cap-1", "cap-1"},
		{"my-prefix", "cap-1", "my-prefix/cap-1"},
		{"nested/prefix", "cap-2", "nested/prefix/cap-2"},
	}

	for _, tt := range tests {
		got := buildPrefix(tt.prefix, tt.captureID)
		if got != tt.want {
			t.Errorf("buildPrefix(%q, %q) = %q, want %q", tt.prefix, tt.captureID, got, tt.want)
		}
	}
}
