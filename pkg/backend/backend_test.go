package backend_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kapture-io/kapture/pkg/backend"
	"github.com/kapture-io/kapture/pkg/backend/filesystem"
	"github.com/kapture-io/kapture/pkg/capture"
)

func descriptor(w http.ResponseWriter, capabilities ...backend.Capability) {
	_ = json.NewEncoder(w).Encode(backend.Descriptor{Version: backend.Version, Name: "test", Capabilities: capabilities})
}

func TestHTTPBackendPersistsBinaryRecordsAndStreamsSelectedReplay(t *testing.T) {
	store, err := filesystem.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	handler := backend.NewHandler(store)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer destination-token" {
			w.WriteHeader(401)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	ctx := context.Background()
	client, err := backend.NewClient(ctx, backend.Config{Endpoint: server.URL, Token: "destination-token"})
	if err != nil {
		t.Fatal(err)
	}
	body := []byte{0, 255, 0, 1}
	first := &capture.Request{ID: "one", Timestamp: time.Unix(1700000000, 3), Method: "POST", Path: "/a?q=1", Headers: map[string][]string{"Authorization": {"[REDACTED]"}}, Body: body, Protocol: "HTTP"}
	second := *first
	second.ID, second.Path, second.Timestamp = "two", "/b", first.Timestamp.Add(time.Second)
	for _, request := range []*capture.Request{&second, first, first} {
		if err := client.PutRequest(ctx, "app/capture", request); err != nil {
			t.Fatal(err)
		}
	}
	reader, err := client.OpenReplay(ctx, capture.ReadOptions{CaptureID: "app/capture", StartTime: first.Timestamp, EndTime: second.Timestamp, Methods: []string{"POST"}, PathPrefix: "/a", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	r, err := reader.Next(ctx)
	if err != nil || r.ID != first.ID || r.Path != first.Path || !r.Timestamp.Equal(first.Timestamp) || !bytes.Equal(r.Body, body) {
		t.Fatalf("record changed: %+v %v", r, err)
	}
	if _, err := reader.Next(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("missing terminal: %v", err)
	}
	artifact, err := client.PutArtifact(ctx, "app/capture", "epoch/exchange.pcapng", body, "application/x-pcapng")
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(artifact.Key)
	if err != nil || !bytes.Equal(data, body) || artifact.SHA256 == "" || artifact.SizeBytes != int64(len(body)) {
		t.Fatalf("artifact changed: %+v %v", artifact, err)
	}
	conflict := *first
	conflict.Body = []byte("different")
	if err := client.PutRequest(ctx, "app/capture", &conflict); err == nil {
		t.Fatal("immutable record overwritten")
	}
	reader2, err := client.OpenReplay(ctx, capture.ReadOptions{CaptureID: "app/capture"})
	if err != nil {
		t.Fatal(err)
	}
	defer reader2.Close()
	for _, id := range []string{"one", "two"} {
		got, err := reader2.Next(ctx)
		if err != nil || got.ID != id {
			t.Fatalf("order/deduplication failed: %+v %v", got, err)
		}
	}
	if _, err := reader2.Next(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("duplicate or missing end: %v", err)
	}
}

func TestReplayStreamCannotTurnTruncationIntoSuccessfulEOF(t *testing.T) {
	for _, test := range []struct{ name, wire string }{
		{"truncated", ""},
		{"count mismatch", "{\"end\":true,\"count\":2}\n"},
		{"source error", "{\"error\":\"secret-must-not-be-logged\"}\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/capabilities" {
					descriptor(w, backend.ReadRequests)
					return
				}
				io.WriteString(w, test.wire)
			}))
			defer server.Close()
			client, err := backend.NewClient(context.Background(), backend.Config{Endpoint: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			reader, err := client.OpenReplay(context.Background(), capture.ReadOptions{CaptureID: "capture"})
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			if _, err := reader.Next(context.Background()); err == nil || errors.Is(err, io.EOF) || strings.Contains(err.Error(), "secret-must-not-be-logged") {
				t.Fatalf("stream failure hidden or leaked: %v", err)
			}
		})
	}
}

func TestReplayNextCancellationInterruptsBlockedNetworkRead(t *testing.T) {
	serverDone := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/capabilities" {
			descriptor(w, backend.ReadRequests)
			return
		}
		defer close(serverDone)
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	reading := make(chan struct{})
	client, err := backend.NewClient(context.Background(), backend.Config{Endpoint: server.URL, HTTPClient: &http.Client{Transport: notifyTransport{reading: reading}}})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := client.OpenReplay(context.Background(), capture.ReadOptions{CaptureID: "capture"})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := reader.Next(ctx); done <- err }()
	select {
	case <-reading:
	case <-time.After(3 * time.Second):
		t.Fatal("client never entered the network read")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled read succeeded")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancellation did not interrupt read")
	}
	reader.Close()
	select {
	case <-serverDone:
	case <-time.After(3 * time.Second):
		t.Fatal("server connection remained open")
	}
}

type notifyTransport struct{ reading chan struct{} }

func (t notifyTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := http.DefaultTransport.RoundTrip(r)
	if err == nil && strings.HasSuffix(r.URL.Path, "/requests") {
		resp.Body = &notifyBody{ReadCloser: resp.Body, reading: t.reading}
	}
	return resp, err
}

type notifyBody struct {
	io.ReadCloser
	reading chan struct{}
	once    sync.Once
}

func (r *notifyBody) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.reading) })
	return r.ReadCloser.Read(p)
}

func TestCapabilitiesAndRedirectsAreEnforcedBeforeSendingData(t *testing.T) {
	var redirected atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1) }))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/capabilities" {
			descriptor(w, backend.WriteRequests)
			return
		}
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	client, err := backend.NewClient(context.Background(), backend.Config{Endpoint: server.URL, Token: "private-destination-token"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.OpenReplay(context.Background(), capture.ReadOptions{CaptureID: "capture"}); err == nil {
		t.Fatal("write-only backend accepted for replay")
	}
	if _, err := client.PutArtifact(context.Background(), "capture", "a.json", nil, "application/json"); err == nil {
		t.Fatal("missing evidence capability hidden")
	}
	err = client.PutRequest(context.Background(), "capture", &capture.Request{ID: "r"})
	if err == nil || redirected.Load() != 0 || strings.Contains(err.Error(), "private-destination-token") {
		t.Fatalf("redirect followed or token leaked: %v", err)
	}
	for _, bad := range []string{"file:///tmp/data", "https://user:secret@example.test", "https://example.test?token=secret"} {
		if err := backend.ValidateEndpoint(bad); err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatalf("invalid endpoint accepted or exposed: %v", err)
		}
	}
}
