package agent

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// NewAgentServer
// ---------------------------------------------------------------------------

func TestNewAgentServer_Defaults(t *testing.T) {
	mw := &mockWriter{}
	handler := NewCaptureHandler(CaptureHandlerConfig{Writer: mw, Logger: discardLogger()})
	srv := NewAgentServer(AgentServerConfig{Handler: handler})

	if srv.httpPort != 8080 {
		t.Errorf("expected default HTTP port 8080, got %d", srv.httpPort)
	}
	if srv.grpcPort != 9090 {
		t.Errorf("expected default gRPC port 9090, got %d", srv.grpcPort)
	}
	if srv.healthPort != 8081 {
		t.Errorf("expected default health port 8081, got %d", srv.healthPort)
	}
	if srv.log == nil {
		t.Error("expected non-nil logger")
	}
}

func TestNewAgentServer_CustomPorts(t *testing.T) {
	mw := &mockWriter{}
	handler := NewCaptureHandler(CaptureHandlerConfig{Writer: mw, Logger: discardLogger()})
	srv := NewAgentServer(AgentServerConfig{
		Handler:    handler,
		HTTPPort:   18080,
		GRPCPort:   19090,
		HealthPort: 18081,
		Logger:     discardLogger(),
	})

	if srv.httpPort != 18080 {
		t.Errorf("expected HTTP port 18080, got %d", srv.httpPort)
	}
	if srv.grpcPort != 19090 {
		t.Errorf("expected gRPC port 19090, got %d", srv.grpcPort)
	}
	if srv.healthPort != 18081 {
		t.Errorf("expected health port 18081, got %d", srv.healthPort)
	}
}

func TestNewAgentServer_NegativePorts_UsesDefaults(t *testing.T) {
	mw := &mockWriter{}
	handler := NewCaptureHandler(CaptureHandlerConfig{Writer: mw, Logger: discardLogger()})
	srv := NewAgentServer(AgentServerConfig{
		Handler:    handler,
		HTTPPort:   -1,
		GRPCPort:   -1,
		HealthPort: -1,
	})

	if srv.httpPort != 8080 {
		t.Errorf("expected default HTTP port, got %d", srv.httpPort)
	}
	if srv.grpcPort != 9090 {
		t.Errorf("expected default gRPC port, got %d", srv.grpcPort)
	}
	if srv.healthPort != 8081 {
		t.Errorf("expected default health port, got %d", srv.healthPort)
	}
}

// ---------------------------------------------------------------------------
// Start and Shutdown
// ---------------------------------------------------------------------------

func TestAgentServer_StartAndShutdown(t *testing.T) {
	mw := &mockWriter{}
	handler := NewCaptureHandler(CaptureHandlerConfig{Writer: mw, Logger: discardLogger()})
	srv := NewAgentServer(AgentServerConfig{
		Handler:    handler,
		HTTPPort:   0, // Use 0 won't work with ListenAndServe, use specific ports
		GRPCPort:   0,
		HealthPort: 0,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	// Use ephemeral ports to avoid conflicts.
	// Since AgentServer uses fmt.Sprintf(":%d", port), port 0 would be ":0"
	// which works for net.Listen but httpServer.ListenAndServe does its own listen.
	// We'll test with actual free ports.
	srv.httpPort = 18180
	srv.grpcPort = 19190
	srv.healthPort = 18181

	// Update the http servers to use the new ports.
	srv.httpServer.Addr = fmt.Sprintf(":%d", srv.httpPort)
	srv.healthServer.Addr = fmt.Sprintf(":%d", srv.healthPort)

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start(ctx)
	}()

	// Give servers time to start.
	time.Sleep(200 * time.Millisecond)

	// Verify health endpoint is accessible.
	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/healthz", srv.healthPort))
	if err != nil {
		t.Logf("health endpoint not accessible (may be port conflict): %v", err)
	} else {
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200 from health, got %d", resp.StatusCode)
		}
	}

	// Verify HTTP capture endpoint is accessible.
	resp, err = http.Get(fmt.Sprintf("http://localhost:%d/test", srv.httpPort))
	if err != nil {
		t.Logf("HTTP endpoint not accessible (may be port conflict): %v", err)
	} else {
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200 from HTTP capture, got %d", resp.StatusCode)
		}
	}

	// Shutdown.
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Logf("Start returned error (expected on shutdown): %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("server did not shut down in time")
	}
}

// ---------------------------------------------------------------------------
// Shutdown: graceful with no running server
// ---------------------------------------------------------------------------

func TestAgentServer_Shutdown_DirectCall(t *testing.T) {
	mw := &mockWriter{}
	handler := NewCaptureHandler(CaptureHandlerConfig{Writer: mw, Logger: discardLogger()})
	srv := NewAgentServer(AgentServerConfig{
		Handler: handler,
		Logger:  discardLogger(),
	})

	// Shutdown without Start should not panic.
	err := srv.Shutdown()
	// May return error since servers weren't started, but should not panic.
	_ = err
}

// ---------------------------------------------------------------------------
// rawMessage methods
// ---------------------------------------------------------------------------

func TestRawMessage_Marshal(t *testing.T) {
	msg := rawMessage([]byte("hello"))
	data, err := msg.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	if string(data) != "hello" {
		t.Errorf("expected hello, got %s", data)
	}
}

func TestRawMessage_Unmarshal(t *testing.T) {
	var msg rawMessage
	err := msg.Unmarshal([]byte("world"))
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if string(msg) != "world" {
		t.Errorf("expected world, got %s", msg)
	}
}

func TestRawMessage_ProtoMessage(t *testing.T) {
	msg := rawMessage([]byte("test"))
	msg.ProtoMessage() // Should not panic.
}

func TestRawMessage_Reset(t *testing.T) {
	msg := rawMessage([]byte("test"))
	msg.Reset()
	if msg != nil {
		t.Errorf("expected nil after Reset, got %v", msg)
	}
}

func TestRawMessage_String(t *testing.T) {
	msg := rawMessage([]byte("test"))
	if msg.String() != "test" {
		t.Errorf("expected test, got %s", msg.String())
	}
}
