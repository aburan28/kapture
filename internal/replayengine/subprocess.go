// Package replayengine implements the host side of the Kapture replay
// engine ABI: launching engine plugins as gRPC subprocesses, hot-reloading
// them, and streaming shard-filtered capture data through them.
//
// The engine side of the contract lives in pkg/replayengine; the wire
// protocol in proto/replayengine/v1; the full spec in
// docs/replay-engine-abi.md.
package replayengine

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	sdk "github.com/kapture-io/kapture/pkg/replayengine"
	replayenginev1 "github.com/kapture-io/kapture/proto/replayengine/v1"
)

// HandshakeTimeout is how long the host waits for the subprocess to print
// its handshake line.
const HandshakeTimeout = 15 * time.Second

// HostABIVersions lists the ABI generations this host speaks, in
// preference order.
var HostABIVersions = []string{sdk.ABIVersion}

// SubprocessEngine is a replay engine running as a child process, driven
// over gRPC on a unix socket.
type SubprocessEngine struct {
	Info *replayenginev1.DescribeResponse

	cmd    *exec.Cmd
	conn   *grpc.ClientConn
	client replayenginev1.ReplayEngineServiceClient
	log    *slog.Logger

	closeOnce sync.Once
	closeErr  error
}

// Launch starts the plugin binary at path, performs the stdout handshake,
// connects over the advertised unix socket, and negotiates the ABI version
// via Describe. The returned engine is ready for Configure/Execute.
func Launch(ctx context.Context, path string, log *slog.Logger) (*SubprocessEngine, error) {
	if log == nil {
		log = slog.Default()
	}

	socketDir, err := os.MkdirTemp("", "kapture-engine-*")
	if err != nil {
		return nil, fmt.Errorf("create socket dir: %w", err)
	}

	cmd := exec.Command(path)
	cmd.Env = append(os.Environ(),
		sdk.MagicCookieEnv+"="+sdk.MagicCookieValue,
		sdk.SocketDirEnv+"="+socketDir,
	)
	cmd.Stderr = os.Stderr // engine logs pass through

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start engine %s: %w", path, err)
	}

	addr, err := readHandshake(ctx, stdout)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, fmt.Errorf("engine %s handshake: %w", path, err)
	}

	conn, err := grpc.NewClient("unix://"+addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, fmt.Errorf("dial engine socket: %w", err)
	}

	engine := &SubprocessEngine{
		cmd:    cmd,
		conn:   conn,
		client: replayenginev1.NewReplayEngineServiceClient(conn),
		log:    log.With("engine-binary", path),
	}

	describeCtx, cancel := context.WithTimeout(ctx, HandshakeTimeout)
	defer cancel()
	info, err := engine.client.Describe(describeCtx, &replayenginev1.DescribeRequest{
		HostAbiVersions: HostABIVersions,
	})
	if err != nil {
		engine.Close()
		return nil, fmt.Errorf("describe engine %s: %w", path, err)
	}
	if _, ok := sdk.SupportsABIVersion(HostABIVersions, info.AbiVersions); !ok {
		engine.Close()
		return nil, fmt.Errorf("engine %s speaks ABI %v, host speaks %v: no common version",
			info.Name, info.AbiVersions, HostABIVersions)
	}
	engine.Info = info

	log.Info("replay engine launched",
		"name", info.Name, "version", info.Version,
		"protocols", info.Protocols, "capabilities", info.Capabilities)
	return engine, nil
}

// readHandshake reads the single handshake line from the engine's stdout:
//
//	KAPTURE-ENGINE|1|unix|/path/to/engine.sock|grpc
func readHandshake(ctx context.Context, stdout interface{ Read([]byte) (int, error) }) (string, error) {
	type result struct {
		line string
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		if scanner.Scan() {
			ch <- result{line: scanner.Text()}
			// Keep draining stdout so the child never blocks on a full
			// pipe if it misbehaves and keeps writing.
			for scanner.Scan() {
			}
			return
		}
		if err := scanner.Err(); err != nil {
			ch <- result{err: err}
			return
		}
		ch <- result{err: fmt.Errorf("engine exited before handshake")}
	}()

	timeout := time.NewTimer(HandshakeTimeout)
	defer timeout.Stop()

	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-timeout.C:
		return "", fmt.Errorf("no handshake line within %s", HandshakeTimeout)
	case res := <-ch:
		if res.err != nil {
			return "", res.err
		}
		parts := strings.Split(strings.TrimSpace(res.line), "|")
		if len(parts) != 5 || parts[0] != sdk.HandshakePrefix {
			return "", fmt.Errorf("malformed handshake line %q", res.line)
		}
		if parts[1] != sdk.HandshakeFormatVersion {
			return "", fmt.Errorf("unsupported handshake format version %q", parts[1])
		}
		if parts[2] != "unix" || parts[4] != "grpc" {
			return "", fmt.Errorf("unsupported transport %q/%q", parts[2], parts[4])
		}
		return parts[3], nil
	}
}

// Configure prepares the engine for a run.
func (e *SubprocessEngine) Configure(ctx context.Context, cfg *replayenginev1.RunConfig) error {
	resp, err := e.client.Configure(ctx, &replayenginev1.ConfigureRequest{Config: cfg})
	if err != nil {
		return fmt.Errorf("configure engine: %w", err)
	}
	if !resp.Accepted {
		return fmt.Errorf("engine rejected configuration: %s", resp.Message)
	}
	return nil
}

// Execute opens the bidi run stream.
func (e *SubprocessEngine) Execute(ctx context.Context) (grpc.BidiStreamingClient[replayenginev1.ExecuteRequest, replayenginev1.ExecuteResponse], error) {
	return e.client.Execute(ctx)
}

// Drain asks the engine to finish in-flight work.
func (e *SubprocessEngine) Drain(ctx context.Context, grace time.Duration) error {
	_, err := e.client.Drain(ctx, &replayenginev1.DrainRequest{GracePeriodMs: grace.Milliseconds()})
	return err
}

// SelfPaced reports whether the engine paces itself from the RateHint.
func (e *SubprocessEngine) SelfPaced() bool {
	if e.Info == nil {
		return false
	}
	for _, c := range e.Info.Capabilities {
		if c == sdk.CapabilitySelfPaced {
			return true
		}
	}
	return false
}

// Close terminates the connection and the subprocess. Safe to call more
// than once.
func (e *SubprocessEngine) Close() error {
	e.closeOnce.Do(func() {
		if e.conn != nil {
			_ = e.conn.Close()
		}
		if e.cmd != nil && e.cmd.Process != nil {
			// SIGTERM first (Serve installs a graceful handler), then
			// escalate if the engine will not exit.
			_ = e.cmd.Process.Signal(os.Interrupt)
			done := make(chan error, 1)
			go func() { done <- e.cmd.Wait() }()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				_ = e.cmd.Process.Kill()
				<-done
			}
		}
	})
	return e.closeErr
}
