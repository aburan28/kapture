package agent

import (
	"bytes"
	"context"
	"testing"

	"github.com/kapture-io/kapture/internal/storage"
)

// captureWriter records written requests.
type captureWriter struct {
	written []*storage.CapturedRequest
}

func (w *captureWriter) Write(_ context.Context, req *storage.CapturedRequest) error {
	w.written = append(w.written, req)
	return nil
}
func (w *captureWriter) Flush(context.Context) error { return nil }
func (w *captureWriter) Close() error                { return nil }

func TestHeaderRedactor_Redact(t *testing.T) {
	r := NewHeaderRedactor([]string{"Authorization", "x-api-key"})

	headers := map[string][]string{
		"Authorization": {"Bearer secret-token"},
		"X-Api-Key":     {"key-1", "key-2"}, // canonicalised match
		"Content-Type":  {"application/json"},
	}
	if n := r.Redact(headers); n != 2 {
		t.Errorf("redacted %d headers, want 2", n)
	}
	if headers["Authorization"][0] != RedactedValue {
		t.Errorf("Authorization not redacted: %v", headers["Authorization"])
	}
	for _, v := range headers["X-Api-Key"] {
		if v != RedactedValue {
			t.Errorf("X-Api-Key value not redacted: %v", headers["X-Api-Key"])
		}
	}
	if headers["Content-Type"][0] != "application/json" {
		t.Errorf("non-credential header modified: %v", headers["Content-Type"])
	}
}

func TestCaptureHandler_RedactsCredentialsByDefault(t *testing.T) {
	writer := &captureWriter{}
	handler := NewCaptureHandler(CaptureHandlerConfig{Writer: writer})

	err := handler.Handle(context.Background(), &CapturedHTTPRequest{
		Method: "GET",
		Path:   "/api",
		Headers: map[string][]string{
			"Authorization": {"Bearer live-secret"},
			"Cookie":        {"session=abc"},
			"Accept":        {"application/json"},
		},
		Body:     bytes.NewReader(nil),
		Protocol: "HTTP",
	})
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(writer.written) != 1 {
		t.Fatalf("wrote %d requests, want 1", len(writer.written))
	}

	stored := writer.written[0]
	if stored.Headers["Authorization"][0] != RedactedValue {
		t.Errorf("stored Authorization = %q; credential became durable data", stored.Headers["Authorization"][0])
	}
	if stored.Headers["Cookie"][0] != RedactedValue {
		t.Errorf("stored Cookie = %q; credential became durable data", stored.Headers["Cookie"][0])
	}
	if stored.Headers["Accept"][0] != "application/json" {
		t.Errorf("non-credential header modified: %v", stored.Headers["Accept"])
	}
}

func TestCaptureHandler_RedactionCanBeDisabled(t *testing.T) {
	writer := &captureWriter{}
	handler := NewCaptureHandler(CaptureHandlerConfig{
		Writer:        writer,
		RedactHeaders: []string{}, // explicit empty: disabled
	})

	err := handler.Handle(context.Background(), &CapturedHTTPRequest{
		Method: "GET",
		Path:   "/api",
		Headers: map[string][]string{
			"Authorization": {"Bearer live-secret"},
		},
		Body:     bytes.NewReader(nil),
		Protocol: "HTTP",
	})
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if writer.written[0].Headers["Authorization"][0] != "Bearer live-secret" {
		t.Errorf("redaction ran despite explicit opt-out")
	}
}
