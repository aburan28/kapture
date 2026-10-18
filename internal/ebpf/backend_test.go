package ebpf

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kapture-io/kapture/internal/engines/builtin"
	"github.com/kapture-io/kapture/internal/l7"
	replay "github.com/kapture-io/kapture/internal/plugin/replay"
	hostengine "github.com/kapture-io/kapture/internal/replayengine"
	"github.com/kapture-io/kapture/internal/storage"
	"github.com/kapture-io/kapture/pkg/backend"
	"github.com/kapture-io/kapture/pkg/backend/filesystem"
	sdk "github.com/kapture-io/kapture/pkg/replayengine"
	replayv1 "github.com/kapture-io/kapture/proto/replayengine/v1"
)

func TestMain(m *testing.M) {
	if os.Getenv("KAPTURE_BACKEND_ENGINE_HELPER") == "1" {
		if os.Getenv("BACKEND_TOKEN") != "" {
			fmt.Fprintln(os.Stderr, "replay engine received a destination credential")
			os.Exit(1)
		}
		if err := sdk.Serve(builtin.New()); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestPluggableBackendCaptureToIndependentReplayEngine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	root := t.TempDir()
	store, err := filesystem.New(root)
	if err != nil {
		t.Fatal(err)
	}
	handler := backend.NewHandler(store)
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer backend-credential" {
			w.WriteHeader(401)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	defer destination.Close()
	factory, err := storage.NewHTTPWriterFactory(ctx, storage.HTTPConfig{Endpoint: destination.URL, Token: "backend-credential"})
	if err != nil {
		t.Fatal(err)
	}
	writer, err := factory.NewWriter(ctx, "app/l7")
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	input := filepath.Join(t.TempDir(), "observations.jsonl")
	var observations bytes.Buffer
	encoder := json.NewEncoder(&observations)
	for i, data := range [][]byte{[]byte("POST /verify?q=retained HTTP/1.1\r\nHost: source\r\nAuthorization: capture-canary\r\nContent-Length: 3\r\n\r\nabc"), []byte("HTTP/1.1 204 No Content\r\n\r\n")} {
		lane := "write"
		if i == 1 {
			lane = "read"
		}
		if err := encoder.Encode(l7.Observation{Epoch: "fixture", Flow: "flow", Lane: lane, Sequence: uint64(i + 1), Timestamp: time.Unix(1700000000, int64(i)), Data: data, OriginalLength: len(data), Verified: true}); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(input, observations.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	if err := Run(ctx, Config{CaptureID: "app/l7", InputFile: input, Export: l7.ExportConfig{CaptureID: "app/l7", Writer: writer, Artifacts: factory, IncludeHeaders: true, IncludeBody: true, PCAP: true}}); err != nil {
		t.Fatal(err)
	}
	var evidence, records int
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(data, []byte("capture-canary")) || bytes.Contains(data, []byte("backend-credential")) {
			t.Fatal("credentials entered destination data")
		}
		if strings.Contains(path, string(os.PathSeparator)+"evidence"+string(os.PathSeparator)) {
			evidence++
		} else {
			records++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if evidence != 3 || records != 1 {
		t.Fatalf("expected exchange, PCAP, session and replay record: evidence=%d records=%d", evidence, records)
	}
	reader, err := replay.NewReaderFromConfig(ctx, "http", map[string]any{"endpoint": destination.URL, "token": "backend-credential"})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.URL.RequestURI() != "/verify?q=retained" || string(body) != "abc" || r.Header.Get("Authorization") != "[REDACTED]" {
			t.Errorf("replay bytes/policy changed: path=%s body=%q", r.URL.RequestURI(), body)
		}
		w.WriteHeader(204)
	}))
	defer target.Close()
	t.Setenv("KAPTURE_BACKEND_ENGINE_HELPER", "1")
	t.Setenv("BACKEND_TOKEN", "backend-credential")
	self, _ := os.Executable()
	engine, err := hostengine.Launch(ctx, self, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	u, _ := url.Parse(target.URL)
	host, port, _ := net.SplitHostPort(u.Host)
	n, _ := strconv.Atoi(port)
	if err := engine.Configure(ctx, &replayv1.RunConfig{RunId: "backend-test", Target: &replayv1.Target{Host: host, Port: int32(n)}, Concurrency: 1, Rate: &replayv1.RateHint{Mode: "Unlimited"}}); err != nil {
		t.Fatal(err)
	}
	feeder, err := hostengine.NewFeeder(hostengine.FeederConfig{Reader: reader, ReadOptions: replay.ReadOptions{CaptureID: "app/l7"}, RateMode: replay.RateModeUnlimited})
	if err != nil {
		t.Fatal(err)
	}
	report, err := feeder.Run(ctx, engine)
	if err != nil || report.SentRequests != 1 || report.FailedRequests != 0 {
		t.Fatalf("independent engine replay failed: %+v %v", report, err)
	}
	// The same destination also serves the in-process engine, whose completion
	// sentinel differs from the public HTTP reader's terminal io.EOF.
	directReader, err := replay.NewHTTPReader(ctx, replay.HTTPReaderConfig{Endpoint: destination.URL, Token: "backend-credential"})
	if err != nil {
		t.Fatal(err)
	}
	defer directReader.Close()
	sender, err := replay.NewHTTPSender(replay.HTTPSenderConfig{TargetBaseURL: target.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	direct, err := replay.NewEngine(replay.EngineConfig{Reader: directReader, Sender: sender, ReadOptions: replay.ReadOptions{CaptureID: "app/l7"}, RateMode: replay.RateModeUnlimited})
	if err != nil {
		t.Fatal(err)
	}
	result, err := direct.Run(ctx)
	if err != nil || result.SuccessCount != 1 {
		t.Fatalf("direct replay failed: %+v %v", result, err)
	}
}

type requestOnlyBackend struct{ *filesystem.Store }

func (b requestOnlyBackend) Describe(ctx context.Context) (backend.Descriptor, error) {
	d, err := b.Store.Describe(ctx)
	d.Capabilities = []backend.Capability{backend.WriteRequests}
	return d, err
}

func TestL7RejectsRequestOnlyBackendBeforeOpeningCaptureInput(t *testing.T) {
	store, err := filesystem.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(backend.NewHandler(requestOnlyBackend{store}))
	defer server.Close()
	factory, err := storage.NewHTTPWriterFactory(context.Background(), storage.HTTPConfig{Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	writer, err := factory.NewWriter(context.Background(), "app/l7")
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	err = Run(context.Background(), Config{CaptureID: "app/l7", InputFile: "/missing-observation-input", Export: l7.ExportConfig{CaptureID: "app/l7", Writer: writer, Artifacts: factory}})
	if err == nil || !strings.Contains(err.Error(), string(backend.WriteArtifacts)) {
		t.Fatalf("missing evidence capability did not fail before capture: %v", err)
	}
}
