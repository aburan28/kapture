package ebpf

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kapture-io/kapture/internal/l7"
	replay "github.com/kapture-io/kapture/internal/plugin/replay"
	"github.com/kapture-io/kapture/internal/storage"
	"golang.org/x/net/websocket"
	"google.golang.org/protobuf/encoding/protowire"
	"strings"
)

// This runs the real exporter and existing replay reader/sender. The source is
// explicitly a recorded observation fixture, not a claim of live BPF execution.
func TestObservationToEvidenceToReplayEndToEnd(t *testing.T) {
	dir := t.TempDir()
	factory, err := storage.NewEFSWriterFactory(storage.EFSConfig{MountPath: dir, AgentPod: "ebpf-test"})
	if err != nil {
		t.Fatal(err)
	}
	writer, err := factory.NewWriter(context.Background(), "ns/capture")
	if err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(t.TempDir(), "observations.jsonl")
	file, err := os.Create(input)
	if err != nil {
		t.Fatal(err)
	}
	encoder := json.NewEncoder(file)
	start := time.Unix(1700000000, 0)
	request := []byte("POST /echo?query=retained HTTP/1.1\r\nHost: original\r\nAuthorization: canary-secret\r\nContent-Length: 3\r\n\r\nabc")
	response := []byte("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
	for i, p := range [][]byte{request, response} {
		lane := "write"
		if i == 1 {
			lane = "read"
		}
		err = encoder.Encode(l7.Observation{Epoch: "fixture", Flow: "connection", Lane: lane, Sequence: uint64(i + 1), ID: lane, Timestamp: start.Add(time.Duration(i) * time.Millisecond), Data: p, OriginalLength: len(p), Verified: true})
		if err != nil {
			t.Fatal(err)
		}
	}
	file.Close()
	err = Run(context.Background(), Config{CaptureID: "ns/capture", InputFile: input, Export: l7.ExportConfig{CaptureID: "ns/capture", Writer: writer, Artifacts: factory, IncludeHeaders: true, IncludeBody: true, PCAP: true}})
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	reader, _ := replay.NewFilesystemReader(replay.FilesystemReaderConfig{MountPath: dir})
	defer reader.Close()
	if err = reader.Open(context.Background(), replay.ReadOptions{CaptureID: "ns/capture"}); err != nil {
		t.Fatal(err)
	}
	captured, err := reader.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !captured.Timestamp.Equal(start) || string(captured.Body) != "abc" || http.Header(captured.Headers).Get("Authorization") != "[REDACTED]" {
		t.Fatalf("projection changed original bytes/time or leaked credentials: %+v", captured)
	}
	got := make(chan string, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { got <- r.URL.RequestURI(); w.WriteHeader(204) }))
	defer target.Close()
	sender, _ := replay.NewHTTPSender(replay.HTTPSenderConfig{TargetBaseURL: target.URL})
	defer sender.Close()
	result, err := sender.Send(context.Background(), captured)
	if err != nil || result.StatusCode != 204 {
		t.Fatalf("replay failed: %+v %v", result, err)
	}
	if <-got != "/echo?query=retained" {
		t.Fatal("query string lost in replay")
	}
	pcaps, _ := filepath.Glob(filepath.Join(dir, "evidence/ns/capture", "*", "*.pcapng"))
	if len(pcaps) != 1 {
		t.Fatal("PCAPNG missing")
	}
	if _, err = reader.Next(context.Background()); err != replay.ErrReaderDone {
		t.Fatalf("evidence entered replay dataset: %v", err)
	}
}

type cancelAfterWrite struct {
	storage.Writer
	cancel context.CancelFunc
}

func (w cancelAfterWrite) Write(ctx context.Context, r *storage.CapturedRequest) error {
	if err := w.Writer.Write(ctx, r); err != nil {
		return err
	}
	w.cancel()
	return nil
}

func TestTypedWebSocketToStoredExchange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	field := func(b []byte, n protowire.Number, v []byte) []byte {
		return protowire.AppendBytes(protowire.AppendTag(b, n, protowire.BytesType), v)
	}
	integer := func(b []byte, n protowire.Number, v uint64) []byte {
		return protowire.AppendVarint(protowire.AppendTag(b, n, protowire.VarintType), v)
	}
	server := httptest.NewServer(websocket.Handler(func(ws *websocket.Conn) {
		defer ws.Close()
		ready := field(integer(nil, 1, 1), 4, []byte("OpenSSL probe started successfully"))
		if err := websocket.Message.Send(ws, ready); err != nil {
			return
		}
		for i, p := range [][]byte{[]byte("GET /typed HTTP/1.1\r\nHost: fixture\r\n\r\n"), []byte("HTTP/1.1 204 No Content\r\n\r\n")} {
			lane := "write"
			if i == 1 {
				lane = "read"
			}
			var event []byte
			event = field(event, 2, []byte(lane))
			event = field(event, 3, []byte("127.0.0.1"))
			event = integer(event, 4, 30000)
			event = field(event, 5, []byte("127.0.0.1"))
			event = integer(event, 6, 8443)
			event = integer(event, 7, 42)
			event = integer(event, 10, uint64(len(p)))
			event = field(event, 11, p)
			event = integer(event, 12, 1)
			event = field(event, 14, []byte(lane))
			event = integer(event, 15, uint64(len(p)))
			event = integer(event, 17, uint64(i+1))
			if err := websocket.Message.Send(ws, field(integer(nil, 1, 2), 2, event)); err != nil {
				return
			}
		}
		<-ctx.Done()
	}))
	defer server.Close()
	dir := t.TempDir()
	factory, _ := storage.NewEFSWriterFactory(storage.EFSConfig{MountPath: dir, MaxBufferBytes: 1})
	writer, _ := factory.NewWriter(ctx, "typed")
	defer writer.Close()
	err := Run(ctx, Config{CaptureID: "typed", SourceURL: "ws" + strings.TrimPrefix(server.URL, "http") + "/", Export: l7.ExportConfig{CaptureID: "typed", Writer: cancelAfterWrite{writer, cancel}, Artifacts: factory, IncludeHeaders: true, IncludeBody: true}})
	if err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "evidence/typed", "*", "*.json"))
	var exchanges []string
	for _, file := range files {
		if filepath.Base(filepath.Dir(file)) != "sessions" {
			exchanges = append(exchanges, file)
		}
	}
	if len(exchanges) != 1 {
		t.Fatalf("typed protocol did not yield evidence: %v", files)
	}
	data, _ := os.ReadFile(exchanges[0])
	var exchange l7.Exchange
	if err = json.Unmarshal(data, &exchange); err != nil {
		t.Fatal(err)
	}
	if exchange.Request.Path != "/typed" || exchange.Quality.CaptureIntegrity != "unverified" || !exchange.Quality.FramingComplete {
		t.Fatalf("typed source integrity/framing incorrect: %+v", exchange)
	}
}
