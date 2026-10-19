package ebpf

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kapture-io/kapture/internal/l7"
	replay "github.com/kapture-io/kapture/internal/plugin/replay"
	"github.com/kapture-io/kapture/internal/storage"
)

// Run explicitly on a Linux BPF host. Once selected, missing prerequisites are
// failures, never skips. Standard unit tests do not attempt privileged attachment.
func TestNativeOpenSSLCapture(t *testing.T) {
	if os.Getenv("KAPTURE_NATIVE_EBPF") != "1" {
		t.Skip("set KAPTURE_NATIVE_EBPF=1 on a root Linux BPF host")
	}
	fixture := os.Getenv("ECAPTURE_FIXTURE_BINARY")
	if fixture == "" {
		t.Fatal("ECAPTURE_FIXTURE_BINARY must select the compiled test/fixtures/openssl-server.c fixture")
	}
	if _, err := os.Stat(fixture); err != nil {
		t.Fatal(err)
	}
	t.Run("ssl_bio", func(t *testing.T) { testNativeOpenSSLCapture(t, "") })
	t.Run("worker_thread_ex", func(t *testing.T) { testNativeOpenSSLCapture(t, fixture) })
}

func testNativeOpenSSLCapture(t *testing.T, fixture string) {
	if os.Geteuid() != 0 {
		t.Fatal("native capture requires root")
	}
	if _, err := os.Stat("/sys/kernel/btf/vmlinux"); err != nil {
		t.Fatalf("native CO-RE capture requires kernel BTF: %v", err)
	}
	binary := os.Getenv("ECAPTURE_BINARY")
	if binary == "" {
		binary = "/ecapture"
	}
	if _, err := os.Stat(binary); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	cert, key := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if out, err := exec.Command("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=localhost", "-days", "1", "-keyout", key, "-out", cert).CombinedOutput(); err != nil {
		t.Fatalf("fixture certificate: %v: %s", err, out)
	}
	if err := os.WriteFile(filepath.Join(dir, "fixture"), []byte("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 6\r\nSet-Cookie: native-canary\r\n\r\nnative"), 0600); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	server := exec.Command("openssl", "s_server", "-quiet", "-accept", address, "-cert", cert, "-key", key, "-HTTP")
	if fixture != "" {
		server = exec.Command(fixture, "-accept", address, "-cert", cert, "-key", key)
	}
	server.Dir = dir
	server.Stdout = io.Discard
	server.Stderr = io.Discard
	if err = server.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Process.Kill(); _ = server.Wait() }()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	output := t.TempDir()
	factory, _ := storage.NewEFSWriterFactory(storage.EFSConfig{MountPath: output, AgentPod: "native", MaxBufferBytes: 1})
	writer, _ := factory.NewWriter(ctx, "native/capture")
	defer writer.Close()
	health, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	healthAddress := health.Addr().String()
	health.Close()
	done := make(chan error, 1)
	var logMu sync.Mutex
	var producerLogs []string
	processLog := func(line string) {
		logMu.Lock()
		defer logMu.Unlock()
		if len(line) > 8192 {
			line = line[len(line)-8192:]
		}
		if len(producerLogs) < 64 {
			producerLogs = append(producerLogs, line)
		}
	}
	go func() {
		done <- Run(ctx, Config{CaptureID: "native/capture", Binary: binary, PID: server.Process.Pid, HealthAddress: healthAddress, ProcessLog: processLog, Export: l7.ExportConfig{CaptureID: "native/capture", Writer: writer, Artifacts: factory, IncludeHeaders: true, IncludeBody: true, PCAP: true}})
	}()
	defer func() {
		if !t.Failed() {
			return
		}
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
		logMu.Lock()
		for _, line := range producerLogs {
			t.Logf("fixture producer: %s", line)
		}
		logMu.Unlock()
		// Only operational diagnostics, counters and quality are printed; headers/bodies never
		// enter CI diagnostics, even when a producer or decoder fails.
		files, _ := filepath.Glob(filepath.Join(output, "evidence/native/capture", "*", "*.json"))
		for _, file := range files {
			data, _ := os.ReadFile(file)
			if filepath.Base(filepath.Dir(file)) == "sessions" {
				t.Logf("capture session: %s", data)
				continue
			}
			var x l7.Exchange
			if json.Unmarshal(data, &x) == nil {
				t.Logf("exchange quality: requestComplete=%t responseComplete=%t status=%d quality=%+v", x.Request.Complete, x.Response.Complete, x.Response.Status, x.Quality)
			}
		}
	}()
	ready := false
	for !ready {
		select {
		case err := <-done:
			t.Fatalf("collector failed before readiness: %v", err)
		case <-ctx.Done():
			t.Fatal("collector readiness timed out")
		default:
		}
		response, err := http.Get("http://" + healthAddress + "/healthz")
		if err == nil {
			ready = response.StatusCode == 200
			response.Body.Close()
		}
		if !ready {
			time.Sleep(100 * time.Millisecond)
		}
	}
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, DisableKeepAlives: true}} // isolated self-signed fixture only
	defer client.CloseIdleConnections()
	for i := 0; i < 3; i++ {
		request, _ := http.NewRequestWithContext(ctx, "GET", "https://"+address+"/fixture", nil)
		request.Header.Set("Authorization", "native-canary")
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || string(body) != "native" {
			t.Fatalf("fixture response differs: %q %v", body, err)
		}
	}
	loadCaptured := func(readCtx context.Context) []*storage.CapturedRequest {
		reader, _ := replay.NewFilesystemReader(replay.FilesystemReaderConfig{MountPath: output})
		defer reader.Close()
		if reader.Open(readCtx, replay.ReadOptions{CaptureID: "native/capture"}) != nil {
			return nil
		}
		var records []*storage.CapturedRequest
		for {
			request, err := reader.Next(readCtx)
			if err != nil {
				return records
			}
			records = append(records, request)
		}
	}
	var captured []*storage.CapturedRequest
	lastDiagnostic := time.Now()
	for len(captured) < 3 {
		captured = loadCaptured(ctx)
		if len(captured) >= 3 {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("collector failed: %v", err)
		case <-ctx.Done():
			t.Fatal("actual BPF observations never produced replay data")
		case <-time.After(100 * time.Millisecond):
			if time.Since(lastDiagnostic) > 5*time.Second {
				lastDiagnostic = time.Now()
				metricsClient := &http.Client{Timeout: time.Second}
				if metrics, err := metricsClient.Get("http://" + healthAddress + "/metrics"); err == nil {
					data, _ := io.ReadAll(io.LimitReader(metrics.Body, 4096))
					metrics.Body.Close()
					t.Logf("native counters: %s", data)
				}
			}
		}
	}
	for _, request := range captured {
		if request.Path != "/fixture" || http.Header(request.Headers).Get("Authorization") != "[REDACTED]" {
			t.Fatal("native request or redaction is incorrect")
		}
		if request.Metadata["captureIntegrity"] != "unverified" {
			t.Fatal("native producer overclaimed integrity")
		}
	}
	files, _ := filepath.Glob(filepath.Join(output, "evidence/native/capture", "*", "*.json"))
	if len(files) == 0 {
		t.Fatal("native evidence absent")
	}
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), "native-canary") {
			t.Fatal("native credential leaked")
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("native shutdown: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("collector did not stop")
	}
	finalCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	captured = loadCaptured(finalCtx)
	ids := map[string]bool{}
	for _, request := range captured {
		ids[request.ID] = true
	}
	if len(captured) != 3 || len(ids) != 3 {
		t.Fatalf("expected three independent exchanges without wrapper duplication: records=%d identities=%d", len(captured), len(ids))
	}
	t.Logf("actual OpenSSL BPF capture produced %d evidence files and %d independent replay requests", len(files), len(captured))
}
