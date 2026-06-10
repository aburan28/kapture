package hub

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	hubv1 "github.com/kapture-io/kapture/proto/hub/v1"
)

// fakeDirectiveStream implements grpc.ServerStreamingServer[hubv1.WatchDirectivesResponse]
// for driving WatchDirectives directly in tests.
type fakeDirectiveStream struct {
	grpc.ServerStream
	ctx  context.Context
	sent chan *hubv1.WatchDirectivesResponse
}

func newFakeDirectiveStream(ctx context.Context) *fakeDirectiveStream {
	return &fakeDirectiveStream{ctx: ctx, sent: make(chan *hubv1.WatchDirectivesResponse, 64)}
}

func (f *fakeDirectiveStream) Context() context.Context { return f.ctx }

func (f *fakeDirectiveStream) Send(resp *hubv1.WatchDirectivesResponse) error {
	f.sent <- resp
	return nil
}

// receiveDirective waits for the next streamed directive or fails the test.
func (f *fakeDirectiveStream) receiveDirective(t *testing.T) *hubv1.CaptureDirective {
	t.Helper()
	select {
	case resp := <-f.sent:
		if resp.GetDirective() == nil {
			t.Fatal("received response with nil directive")
		}
		return resp.GetDirective()
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for directive")
		return nil
	}
}

func testDirective(id string) *hubv1.CaptureDirective {
	return &hubv1.CaptureDirective{
		DirectiveId:      id,
		Action:           hubv1.DirectiveAction_DIRECTIVE_ACTION_START,
		CaptureName:      "cap-1",
		CaptureNamespace: "ns-1",
	}
}

// watchAsync runs WatchDirectives in a goroutine and returns a channel with its result.
func watchAsync(srv *Server, spokeID string, stream *fakeDirectiveStream) <-chan error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.WatchDirectives(&hubv1.WatchDirectivesRequest{SpokeId: spokeID}, stream)
	}()
	return errCh
}

func waitForWatchResult(t *testing.T, errCh <-chan error) error {
	t.Helper()
	select {
	case err := <-errCh:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for WatchDirectives to return")
		return nil
	}
}

// ---------------------------------------------------------------------------
// WatchDirectives validation
// ---------------------------------------------------------------------------

func TestWatchDirectives_EmptySpokeID(t *testing.T) {
	srv := newTestServer()
	stream := newFakeDirectiveStream(context.Background())
	err := srv.WatchDirectives(&hubv1.WatchDirectivesRequest{}, stream)
	assertGRPCCode(t, err, codes.InvalidArgument)
}

func TestWatchDirectives_UnregisteredSpoke(t *testing.T) {
	srv := newTestServer()
	stream := newFakeDirectiveStream(context.Background())
	err := srv.WatchDirectives(&hubv1.WatchDirectivesRequest{SpokeId: "ghost"}, stream)
	assertGRPCCode(t, err, codes.NotFound)
}

func TestWatchDirectives_ClientDisconnectEndsStream(t *testing.T) {
	srv := newTestServer()
	mustRegister(t, srv, "spoke-1", "cluster-a")

	ctx, cancel := context.WithCancel(context.Background())
	stream := newFakeDirectiveStream(ctx)
	errCh := watchAsync(srv, "spoke-1", stream)

	cancel()
	if err := waitForWatchResult(t, errCh); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// SendDirective
// ---------------------------------------------------------------------------

func TestSendDirective_Validation(t *testing.T) {
	srv := newTestServer()
	mustRegister(t, srv, "spoke-1", "cluster-a")

	assertGRPCCode(t, srv.SendDirective("", testDirective("d1")), codes.InvalidArgument)
	assertGRPCCode(t, srv.SendDirective("spoke-1", nil), codes.InvalidArgument)
	assertGRPCCode(t, srv.SendDirective("ghost", testDirective("d1")), codes.NotFound)
}

func TestSendDirective_QueuedBeforeWatchIsDelivered(t *testing.T) {
	srv := newTestServer()
	mustRegister(t, srv, "spoke-1", "cluster-a")

	if err := srv.SendDirective("spoke-1", testDirective("queued-1")); err != nil {
		t.Fatalf("SendDirective failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := newFakeDirectiveStream(ctx)
	watchAsync(srv, "spoke-1", stream)

	d := stream.receiveDirective(t)
	if d.DirectiveId != "queued-1" {
		t.Errorf("DirectiveId = %q, want %q", d.DirectiveId, "queued-1")
	}
	if d.Action != hubv1.DirectiveAction_DIRECTIVE_ACTION_START {
		t.Errorf("Action = %v, want START", d.Action)
	}
}

func TestSendDirective_DeliveredWhileWatching(t *testing.T) {
	srv := newTestServer()
	mustRegister(t, srv, "spoke-1", "cluster-a")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := newFakeDirectiveStream(ctx)
	watchAsync(srv, "spoke-1", stream)

	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("d-%d", i)
		if err := srv.SendDirective("spoke-1", testDirective(id)); err != nil {
			t.Fatalf("SendDirective(%s) failed: %v", id, err)
		}
		if got := stream.receiveDirective(t).DirectiveId; got != id {
			t.Errorf("DirectiveId = %q, want %q", got, id)
		}
	}
}

func TestSendDirective_BufferFull(t *testing.T) {
	srv := newTestServer()
	mustRegister(t, srv, "spoke-1", "cluster-a")

	// No watcher: fill the buffer.
	for i := 0; i < DirectiveBufferSize; i++ {
		if err := srv.SendDirective("spoke-1", testDirective(fmt.Sprintf("d-%d", i))); err != nil {
			t.Fatalf("SendDirective(%d) failed: %v", i, err)
		}
	}
	assertGRPCCode(t, srv.SendDirective("spoke-1", testDirective("overflow")), codes.ResourceExhausted)
}

// ---------------------------------------------------------------------------
// Stream termination on deregistration / re-registration
// ---------------------------------------------------------------------------

func TestWatchDirectives_DeregisterTerminatesStream(t *testing.T) {
	srv := newTestServer()
	mustRegister(t, srv, "spoke-1", "cluster-a")

	stream := newFakeDirectiveStream(context.Background())
	errCh := watchAsync(srv, "spoke-1", stream)

	// Give the watcher a moment to enter its select loop, then deregister.
	time.Sleep(20 * time.Millisecond)
	if _, err := srv.DeregisterSpoke(context.Background(), &hubv1.DeregisterSpokeRequest{SpokeId: "spoke-1"}); err != nil {
		t.Fatalf("DeregisterSpoke failed: %v", err)
	}

	assertGRPCCode(t, waitForWatchResult(t, errCh), codes.Aborted)
}

func TestWatchDirectives_ReRegisterTerminatesOldStream(t *testing.T) {
	srv := newTestServer()
	mustRegister(t, srv, "spoke-1", "cluster-a")

	stream := newFakeDirectiveStream(context.Background())
	errCh := watchAsync(srv, "spoke-1", stream)

	time.Sleep(20 * time.Millisecond)
	mustRegister(t, srv, "spoke-1", "cluster-a")

	assertGRPCCode(t, waitForWatchResult(t, errCh), codes.Aborted)

	// Directives sent after re-registration go to the fresh entry.
	if err := srv.SendDirective("spoke-1", testDirective("after-reregister")); err != nil {
		t.Fatalf("SendDirective after re-register failed: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	newStream := newFakeDirectiveStream(ctx)
	watchAsync(srv, "spoke-1", newStream)
	if got := newStream.receiveDirective(t).DirectiveId; got != "after-reregister" {
		t.Errorf("DirectiveId = %q, want %q", got, "after-reregister")
	}
}

// ---------------------------------------------------------------------------
// BroadcastDirective
// ---------------------------------------------------------------------------

func TestBroadcastDirective_QueuesToAllSpokes(t *testing.T) {
	srv := newTestServer()
	mustRegister(t, srv, "spoke-1", "cluster-a")
	mustRegister(t, srv, "spoke-2", "cluster-b")

	if queued := srv.BroadcastDirective(testDirective("bcast")); queued != 2 {
		t.Fatalf("BroadcastDirective queued = %d, want 2", queued)
	}

	for _, spokeID := range []string{"spoke-1", "spoke-2"} {
		ctx, cancel := context.WithCancel(context.Background())
		stream := newFakeDirectiveStream(ctx)
		watchAsync(srv, spokeID, stream)
		if got := stream.receiveDirective(t).DirectiveId; got != "bcast" {
			t.Errorf("spoke %s: DirectiveId = %q, want %q", spokeID, got, "bcast")
		}
		cancel()
	}
}

func TestBroadcastDirective_NilDirective(t *testing.T) {
	srv := newTestServer()
	mustRegister(t, srv, "spoke-1", "cluster-a")
	if queued := srv.BroadcastDirective(nil); queued != 0 {
		t.Errorf("BroadcastDirective(nil) queued = %d, want 0", queued)
	}
}

func TestBroadcastDirective_SkipsFullBuffers(t *testing.T) {
	srv := newTestServer()
	mustRegister(t, srv, "spoke-full", "cluster-a")
	mustRegister(t, srv, "spoke-free", "cluster-b")

	for i := 0; i < DirectiveBufferSize; i++ {
		if err := srv.SendDirective("spoke-full", testDirective(fmt.Sprintf("fill-%d", i))); err != nil {
			t.Fatalf("SendDirective(%d) failed: %v", i, err)
		}
	}

	if queued := srv.BroadcastDirective(testDirective("bcast")); queued != 1 {
		t.Errorf("BroadcastDirective queued = %d, want 1 (full buffer skipped)", queued)
	}
}
