package ecapture

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/net/websocket"
)

type Process struct {
	PID     int
	Start   string
	Library string
}

// Discover uses pod cgroups, not command substrings, to scope host probes. Only
// processes mapping OpenSSL are selected. Reading /proc/PID/root follows that
// process's mount namespace, so container libraries need not exist in the agent.
func Discover(procRoot, podUID string, pid int, library string) ([]Process, error) {
	var ids []int
	if pid > 0 {
		ids = []int{pid}
	} else {
		if len(podUID) < 32 {
			return nil, fmt.Errorf("a pod UID or explicit PID is required")
		}
		entries, err := os.ReadDir(procRoot)
		if err != nil {
			return nil, err
		}
		for _, v := range entries {
			n, err := strconv.Atoi(v.Name())
			if err == nil && n > 0 {
				ids = append(ids, n)
			}
		}
	}
	sort.Ints(ids)
	var found []Process
	for _, id := range ids {
		base := filepath.Join(procRoot, strconv.Itoa(id))
		if podUID != "" {
			b, err := os.ReadFile(filepath.Join(base, "cgroup"))
			if err != nil {
				continue
			}
			normalized := strings.ReplaceAll(string(b), "_", "-")
			marker := "pod" + strings.ReplaceAll(podUID, "_", "-")
			pos := strings.Index(normalized, marker)
			if pos < 0 {
				continue
			}
			end := pos + len(marker)
			if end < len(normalized) && !strings.ContainsRune("./\n", rune(normalized[end])) {
				continue
			}
		}
		start, err := processStart(base)
		if err != nil {
			continue
		}
		lib := library
		if lib == "" {
			b, err := os.ReadFile(filepath.Join(base, "maps"))
			if err != nil {
				continue
			}
			for _, line := range strings.Split(string(b), "\n") {
				fields := strings.Fields(line)
				if len(fields) < 6 {
					continue
				}
				candidate := fields[len(fields)-1]
				if strings.HasPrefix(candidate, "/") && strings.Contains(filepath.Base(candidate), "libssl") {
					lib = filepath.Join(base, "root", candidate)
					break
				}
			}
		}
		if lib != "" {
			found = append(found, Process{PID: id, Start: start, Library: lib})
		}
	}
	return found, nil
}
func processStart(base string) (string, error) {
	b, err := os.ReadFile(filepath.Join(base, "stat"))
	if err != nil {
		return "", err
	}
	end := strings.LastIndexByte(string(b), ')')
	if end < 0 {
		return "", fmt.Errorf("invalid proc stat")
	}
	fields := strings.Fields(string(b[end+1:]))
	if len(fields) < 20 {
		return "", fmt.Errorf("short proc stat")
	}
	return fields[19], nil
}
func Alive(root string, p Process, podUID string) bool {
	found, err := Discover(root, podUID, p.PID, p.Library)
	return err == nil && len(found) == 1 && found[0].Start == p.Start
}

// Receive consumes one connected producer epoch; reconnect requires a new epoch.
// ready is called only after the producer reports successful probe attachment.
func Receive(ctx context.Context, url string, handle func(Entry) error, ready func()) error {
	config, err := websocket.NewConfig(url, "http://localhost/")
	if err != nil {
		return err
	}
	config.Dialer = &net.Dialer{Timeout: time.Second}
	var conn *websocket.Conn
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	for conn == nil {
		conn, err = websocket.DialConfig(config)
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("eCaptureQ did not become available: %w", err)
		case <-time.After(100 * time.Millisecond):
		}
	}
	defer conn.Close()
	conn.MaxPayloadBytes = MaxMessageBytes
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	attached := false
	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	for {
		if attached {
			_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		}
		var b []byte
		if err = websocket.Message.Receive(conn, &b); err != nil {
			return err
		}
		entry, err := Decode(b)
		if err != nil {
			return err
		}
		if !attached && strings.Contains(strings.ToLower(entry.ProcessLog), "probe started successfully") {
			attached = true
			_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
			if ready != nil {
				ready()
			}
		}
		if entry.Event != nil && !attached {
			return fmt.Errorf("producer emitted data before attachment readiness")
		}
		if err := handle(entry); err != nil {
			return err
		}
	}
}

func Collect(ctx context.Context, binary, procRoot, podUID string, p Process, mapPages int, handle func(Entry) error, ready func()) error {
	if mapPages < 8 || mapPages > 8192 {
		return fmt.Errorf("map pages must be between 8 and 8192")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	address := listener.Addr().String()
	_ = listener.Close()
	args := []string{"tls", "-m", "text", "--pid", strconv.Itoa(p.PID), "--libssl", p.Library, "--mapsize", strconv.Itoa(mapPages), "--ecaptureq", "ws://" + address + "/", "--eventaddr", "/dev/null"}
	cmd := exec.Command(binary, args...)
	// A pipe drained into a bounded buffer avoids persisting captured logs.
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err = cmd.Start(); err != nil {
		return err
	}
	var diagnostic strings.Builder
	drain := make(chan struct{})
	go func() {
		defer close(drain)
		scanner := bufio.NewScanner(stderr)
		scanner.Buffer(make([]byte, 4096), 65536)
		for scanner.Scan() {
			if diagnostic.Len() < 4096 {
				diagnostic.WriteString(scanner.Text())
				diagnostic.WriteByte('\n')
			}
		}
	}()
	wait := make(chan error, 1)
	go func() { wait <- cmd.Wait() }()
	sourceCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- Receive(sourceCtx, "ws://"+address+"/", func(entry Entry) error {
			if !Alive(procRoot, p, podUID) {
				return fmt.Errorf("target process lifetime changed")
			}
			if entry.Event != nil && entry.Event.PID != int64(p.PID) {
				return fmt.Errorf("producer emitted an event outside its process scope")
			}
			return handle(entry)
		}, ready)
	}()
	var cause error
	exited := false
	select {
	case cause = <-result:
	case cause = <-wait:
		exited = true
		cancel()
		<-result
	case <-ctx.Done():
		cause = ctx.Err()
		cancel()
		<-result
	}
	cancel()
	if !exited {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-wait:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			<-wait
		}
	}
	<-drain
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if cause == nil {
		cause = fmt.Errorf("collector exited")
	}
	// Avoid returning arbitrary stderr: it is producer-controlled. A bounded
	// readiness error and process exit are enough for operator diagnostics.
	return fmt.Errorf("eCapture PID %d stopped: %w", p.PID, cause)
}
