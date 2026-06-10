package spoke

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/kapture-io/kapture/internal/hub"
	hubv1 "github.com/kapture-io/kapture/proto/hub/v1"
)

// startRealHub starts a real hub gRPC server on a random port and returns it
// with its address. The server is stopped via t.Cleanup.
func startRealHub(t *testing.T) (*hub.Server, string) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	srv := hub.NewServer(lis.Addr().String())
	go func() {
		_ = srv.GRPCServer().Serve(lis)
	}()
	t.Cleanup(srv.Stop)

	return srv, lis.Addr().String()
}

// TestHubClient_WatchDirectives_EndToEnd exercises the full hub-to-spoke
// directive path over a real gRPC connection: the spoke registers, opens the
// directive stream, and receives a directive queued on the hub.
func TestHubClient_WatchDirectives_EndToEnd(t *testing.T) {
	hubSrv, addr := startRealHub(t)

	client := NewHubClient(HubClientConfig{
		HubAddress: addr,
		SpokeName:  "spoke-e2e",
		ClusterID:  "cluster-e2e",
		Logger:     logr.Discard(),
	})
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	if _, err := client.Register(ctx); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	received := make(chan *hubv1.CaptureDirective, 8)
	client.OnDirective = func(resp *hubv1.WatchDirectivesResponse) {
		received <- resp.GetDirective()
	}
	client.WatchDirectives(ctx)

	directive := &hubv1.CaptureDirective{
		DirectiveId:      "e2e-1",
		Action:           hubv1.DirectiveAction_DIRECTIVE_ACTION_STOP,
		CaptureName:      "cap-1",
		CaptureNamespace: "ns-1",
	}

	// The stream is opened asynchronously; retry until the directive is
	// queued and delivered.
	deadline := time.After(5 * time.Second)
	for {
		_ = hubSrv.SendDirective("spoke-e2e", directive)
		select {
		case got := <-received:
			if got.GetDirectiveId() != "e2e-1" {
				t.Fatalf("DirectiveId = %q, want %q", got.GetDirectiveId(), "e2e-1")
			}
			if got.GetAction() != hubv1.DirectiveAction_DIRECTIVE_ACTION_STOP {
				t.Fatalf("Action = %v, want STOP", got.GetAction())
			}
			return
		case <-deadline:
			t.Fatal("timed out waiting for directive to reach spoke")
		case <-time.After(50 * time.Millisecond):
			// retry
		}
	}
}
