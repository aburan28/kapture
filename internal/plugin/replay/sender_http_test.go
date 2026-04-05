package replay

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kapture-io/kapture/internal/storage"
)

func TestHTTPSender_BasicSend(t *testing.T) {
	var received *http.Request
	var receivedBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received = r
		body := make([]byte, 1024)
		n, _ := r.Body.Read(body)
		receivedBody = body[:n]
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	sender, err := NewHTTPSender(HTTPSenderConfig{
		TargetBaseURL: server.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	req := &storage.CapturedRequest{
		ID:       "test-1",
		Method:   "POST",
		Path:     "/api/users",
		Protocol: "HTTP/1.1",
		Headers: map[string][]string{
			"Content-Type": {"application/json"},
			"X-Custom":     {"value1"},
		},
		Body: []byte(`{"name":"test"}`),
	}

	result, err := sender.Send(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	if result.StatusCode != 200 {
		t.Errorf("expected status 200, got %d", result.StatusCode)
	}
	if result.RequestID != "test-1" {
		t.Errorf("expected request ID test-1, got %s", result.RequestID)
	}
	if result.Duration <= 0 {
		t.Error("expected positive duration")
	}
	if result.Error != nil {
		t.Errorf("unexpected error: %v", result.Error)
	}

	// Verify request was sent correctly.
	if received.Method != "POST" {
		t.Errorf("expected POST, got %s", received.Method)
	}
	if received.URL.Path != "/api/users" {
		t.Errorf("expected /api/users, got %s", received.URL.Path)
	}
	if received.Header.Get("Content-Type") != "application/json" {
		t.Errorf("expected Content-Type application/json")
	}
	if received.Header.Get("X-Kapture-Replay") != "true" {
		t.Error("expected X-Kapture-Replay header")
	}
	if string(receivedBody) != `{"name":"test"}` {
		t.Errorf("unexpected body: %s", string(receivedBody))
	}
}

func TestHTTPSender_OverrideHeaders(t *testing.T) {
	var received *http.Request

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received = r
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sender, err := NewHTTPSender(HTTPSenderConfig{
		TargetBaseURL: server.URL,
		OverrideHeaders: map[string]string{
			"Authorization": "Bearer new-token",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	req := &storage.CapturedRequest{
		ID:     "test-2",
		Method: "GET",
		Path:   "/api/protected",
		Headers: map[string][]string{
			"Authorization": {"Bearer old-token"},
		},
	}

	result, err := sender.Send(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if result.StatusCode != 200 {
		t.Errorf("expected 200, got %d", result.StatusCode)
	}

	// Override should win.
	if received.Header.Get("Authorization") != "Bearer new-token" {
		t.Errorf("expected overridden auth header, got %s", received.Header.Get("Authorization"))
	}
}

func TestHTTPSender_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal error"))
	}))
	defer server.Close()

	sender, err := NewHTTPSender(HTTPSenderConfig{
		TargetBaseURL: server.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	req := &storage.CapturedRequest{
		ID:     "test-3",
		Method: "GET",
		Path:   "/api/fail",
	}

	result, err := sender.Send(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	// Server error is captured in the result, not returned as an error.
	if result.StatusCode != 500 {
		t.Errorf("expected 500, got %d", result.StatusCode)
	}
	if result.Error != nil {
		t.Errorf("expected nil error for server-side errors, got %v", result.Error)
	}
}

func TestHTTPSender_ConnectionError(t *testing.T) {
	sender, err := NewHTTPSender(HTTPSenderConfig{
		TargetBaseURL: "http://localhost:1", // unlikely to be listening
		Client: &http.Client{
			Timeout: 100 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	req := &storage.CapturedRequest{
		ID:     "test-4",
		Method: "GET",
		Path:   "/api/test",
	}

	result, err := sender.Send(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	// Connection errors are captured in the result.
	if result.Error == nil {
		t.Error("expected error for connection failure")
	}
	if result.StatusCode != 0 {
		t.Errorf("expected 0 status for connection error, got %d", result.StatusCode)
	}
}

func TestHTTPSender_TargetURLRequired(t *testing.T) {
	_, err := NewHTTPSender(HTTPSenderConfig{})
	if err == nil {
		t.Error("expected error with empty target URL")
	}
}

func TestHTTPSender_AllHTTPMethods(t *testing.T) {
	methods := []string{"GET", "POST", "PUT", "PATCH", "DELETE"}

	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			var gotMethod string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotMethod = r.Method
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			sender, err := NewHTTPSender(HTTPSenderConfig{TargetBaseURL: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			defer sender.Close()

			req := &storage.CapturedRequest{
				ID:     "test-method",
				Method: method,
				Path:   "/api/test",
			}

			result, err := sender.Send(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			if result.StatusCode != 200 {
				t.Errorf("expected 200, got %d", result.StatusCode)
			}
			if gotMethod != method {
				t.Errorf("expected method %s, got %s", method, gotMethod)
			}
		})
	}
}
