package replayengine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"google.golang.org/grpc"

	replayenginev1 "github.com/kapture-io/kapture/proto/replayengine/v1"
)

// feedBuffer is the per-run buffer between the gRPC receive loop and the
// engine's Execute. Small on purpose: backpressure must reach the host so
// captures are streamed, never accumulated.
const feedBuffer = 256

// Serve runs an Engine as a Kapture replay-engine subprocess. It refuses to
// start unless the host handshake environment is present, listens on a unix
// socket, prints the handshake line to stdout, and serves until the host
// disconnects or the process receives SIGINT/SIGTERM.
//
// Typical engine main:
//
//	func main() {
//		if err := replayengine.Serve(newMyEngine()); err != nil {
//			fmt.Fprintln(os.Stderr, err)
//			os.Exit(1)
//		}
//	}
func Serve(engine Engine) error {
	if os.Getenv(MagicCookieEnv) != MagicCookieValue {
		return fmt.Errorf(
			"this binary is a Kapture replay-engine plugin and is not meant to be executed directly; "+
				"it is launched by the Kapture replay host (missing %s handshake environment)", MagicCookieEnv)
	}

	socketDir := os.Getenv(SocketDirEnv)
	if socketDir == "" {
		socketDir = os.TempDir()
	}
	socketPath := filepath.Join(socketDir, fmt.Sprintf("kapture-engine-%d.sock", os.Getpid()))
	// Remove a stale socket from a previous PID-reused run.
	_ = os.Remove(socketPath)

	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", socketPath, err)
	}
	defer os.Remove(socketPath)

	server := grpc.NewServer()
	replayenginev1.RegisterReplayEngineServiceServer(server, &engineServer{engine: engine})

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()

	// Handshake: exactly one line on stdout, then nothing else. Engines
	// must log to stderr.
	fmt.Fprintf(os.Stdout, "%s|%s|unix|%s|grpc\n",
		HandshakePrefix, HandshakeFormatVersion, socketPath)

	return server.Serve(lis)
}

// engineServer adapts an Engine to the generated gRPC service.
type engineServer struct {
	replayenginev1.UnimplementedReplayEngineServiceServer
	engine Engine
}

func (s *engineServer) Describe(ctx context.Context, _ *replayenginev1.DescribeRequest) (*replayenginev1.DescribeResponse, error) {
	return s.engine.Describe(ctx)
}

func (s *engineServer) Configure(ctx context.Context, req *replayenginev1.ConfigureRequest) (*replayenginev1.ConfigureResponse, error) {
	if err := s.engine.Configure(ctx, req.GetConfig()); err != nil {
		return &replayenginev1.ConfigureResponse{Accepted: false, Message: err.Error()}, nil
	}
	return &replayenginev1.ConfigureResponse{Accepted: true}, nil
}

func (s *engineServer) Drain(ctx context.Context, _ *replayenginev1.DrainRequest) (*replayenginev1.DrainResponse, error) {
	if err := s.engine.Drain(ctx); err != nil {
		return nil, err
	}
	return &replayenginev1.DrainResponse{Acknowledged: true}, nil
}

// Execute bridges the bidi stream to the Engine's channel-based Execute.
// The receive loop applies backpressure naturally: it only reads from the
// stream as fast as the engine drains the feed channel.
func (s *engineServer) Execute(stream grpc.BidiStreamingServer[replayenginev1.ExecuteRequest, replayenginev1.ExecuteResponse]) error {
	ctx := stream.Context()

	feed := make(chan *replayenginev1.FeedItem, feedBuffer)
	events := make(chan *replayenginev1.ExecuteResponse, feedBuffer)

	var wg sync.WaitGroup
	var recvErr error

	// Receive loop: host → engine feed.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(feed)
		for {
			req, err := stream.Recv()
			if err != nil {
				// io.EOF is the host half-closing after the last item;
				// anything else surfaces after Execute returns.
				if !errors.Is(err, io.EOF) {
					recvErr = err
				}
				return
			}
			if item := req.GetItem(); item != nil {
				select {
				case feed <- item:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	// Send loop: engine events → host.
	var sendErr error
	var sendWg sync.WaitGroup
	sendWg.Add(1)
	go func() {
		defer sendWg.Done()
		for event := range events {
			if sendErr != nil {
				continue // drain remaining events after a send failure
			}
			if err := stream.Send(event); err != nil {
				sendErr = err
			}
		}
	}()

	execErr := s.engine.Execute(ctx, feed, events)
	close(events)
	sendWg.Wait()
	wg.Wait()

	switch {
	case execErr != nil:
		return execErr
	case sendErr != nil:
		return sendErr
	default:
		return recvErr
	}
}
