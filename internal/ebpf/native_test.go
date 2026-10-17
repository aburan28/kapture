package ebpf

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
	go func() {
		done <- Run(ctx, Config{CaptureID: "native/capture", Binary: binary, PID: server.Process.Pid, HealthAddress: healthAddress, Export: l7.ExportConfig{CaptureID: "native/capture", Writer: writer, Artifacts: factory, IncludeHeaders: true, IncludeBody: true, PCAP: true}})
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
	var captured *storage.CapturedRequest
	for captured == nil {
		reader, _ := replay.NewFilesystemReader(replay.FilesystemReaderConfig{MountPath: output})
		if err := reader.Open(ctx, replay.ReadOptions{CaptureID: "native/capture"}); err == nil {
			captured, _ = reader.Next(ctx)
		}
		reader.Close()
		if captured != nil {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("collector failed: %v", err)
		case <-ctx.Done():
			t.Fatal("actual BPF observations never produced replay data")
		case <-time.After(100 * time.Millisecond):
		}
	}
	if captured.Path != "/fixture" || http.Header(captured.Headers).Get("Authorization") != "[REDACTED]" {
		t.Fatalf("native capture is incorrect: %+v", captured)
	}
	if captured.Metadata["captureIntegrity"] != "unverified" {
		t.Fatal("native producer overclaimed integrity")
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
	t.Logf("actual OpenSSL BPF capture produced %d evidence files and replay request %s", len(files), captured.ID)
}
