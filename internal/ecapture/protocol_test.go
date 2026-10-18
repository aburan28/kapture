package ecapture

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/websocket"
	"google.golang.org/protobuf/encoding/protowire"
)

func wireBytes(b []byte, n protowire.Number, v []byte) []byte {
	b = protowire.AppendTag(b, n, protowire.BytesType)
	return protowire.AppendBytes(b, v)
}
func wireInt(b []byte, n protowire.Number, v uint64) []byte {
	b = protowire.AppendTag(b, n, protowire.VarintType)
	return protowire.AppendVarint(b, v)
}
func eventWire(payload []byte) []byte {
	var event []byte
	event = wireBytes(event, 2, []byte("pid_tid_timestamp"))
	event = wireBytes(event, 3, []byte("127.0.0.1"))
	event = wireInt(event, 4, 30000)
	event = wireBytes(event, 5, []byte("127.0.0.1"))
	event = wireInt(event, 6, 8443)
	event = wireInt(event, 7, 42)
	event = wireInt(event, 10, uint64(len(payload)))
	event = wireBytes(event, 11, payload)
	event = wireInt(event, 12, 1)
	event = wireBytes(event, 14, []byte("write"))
	event = wireInt(event, 15, uint64(len(payload)))
	event = wireBytes(event, 16, []byte("socket-lifetime"))
	event = wireInt(event, 17, 1)
	return wireBytes(wireInt(nil, 1, 2), 2, event)
}

func TestTypedProtocolPreservesBinaryAndRejectsMalformedFields(t *testing.T) {
	payload := []byte{0, 1, 2, 255}
	b := eventWire(payload)
	b = wireBytes(b, 99, []byte("future-field"))
	entry, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if entry.Event == nil || !bytes.Equal(entry.Event.Payload, payload) || entry.Event.StreamID != "socket-lifetime" {
		t.Fatal("binary payload changed")
	}
	o, err := entry.Event.Observation("epoch", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if o.Verified || o.Sequence != 1 || o.Flow == "" {
		t.Fatal("unsupported integrity assertion")
	}
	for _, bad := range [][]byte{b[:len(b)-1], {0x12, 0xff, 0xff}, wireInt(nil, 2, 1)} {
		if _, err = Decode(bad); err == nil {
			t.Fatalf("malformed protobuf accepted: %x", bad)
		}
	}
	entry.Event.Payload = make([]byte, 16384)
	entry.Event.OriginalLength = 16384
	entry.Event.Length = 16384
	o, err = entry.Event.Observation("e", time.Now())
	if err != nil || o.Gap == "" {
		t.Fatal("payload cap not exposed")
	}
}

func TestProtocolHonorsLastOneofPayload(t *testing.T) {
	data := wireBytes(eventWire([]byte("payload")), 3, nil)
	if _, err := Decode(data); err == nil {
		t.Fatal("an overwritten event payload was accepted")
	}
	data = wireBytes(eventWire([]byte("payload")), 4, []byte("log"))
	if _, err := Decode(data); err == nil {
		t.Fatal("a run log was accepted as an event payload")
	}
}

func TestSocketIdentitySeparatesReusedTuples(t *testing.T) {
	e := &Event{PID: 42, SourceIP: "127.0.0.1", DestinationIP: "127.0.0.2", SourcePort: 443, DestinationPort: 30000, Format: 1, Direction: "read", StreamID: "socket-lifetime-one"}
	first, err := e.Observation("producer", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	e.StreamID = "socket-lifetime-two"
	second, err := e.Observation("producer", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if first.Flow == second.Flow {
		t.Fatal("different socket lifetimes merged by their reused TCP tuple")
	}
	e.StreamID = strings.Repeat("x", 129)
	if _, err := e.Observation("producer", time.Now()); err == nil {
		t.Fatal("unbounded stream identity accepted")
	}
}

func TestReceiveWaitsForAttachmentAndCancelsBlockedRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	attached := make(chan struct{})
	serverDone := make(chan struct{})
	server := httptest.NewServer(websocket.Handler(func(ws *websocket.Conn) {
		defer close(serverDone)
		defer ws.Close()
		_ = websocket.Message.Send(ws, wireBytes(wireInt(nil, 1, 1), 4, []byte("OpenSSL probe started successfully")))
		_ = websocket.Message.Send(ws, eventWire([]byte("GET / HTTP/1.1\r\n\r\n")))
		<-ctx.Done()
	}))
	defer server.Close()
	done := make(chan error, 1)
	go func() {
		done <- Receive(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/", func(entry Entry) error {
			if entry.Event != nil {
				cancel()
			}
			return nil
		}, func() { close(attached) })
	}()
	select {
	case <-attached:
	case <-time.After(3 * time.Second):
		t.Fatal("producer readiness not observed")
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("closed source returned success")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancellation did not close websocket")
	}
	<-serverDone
}

func TestDiscoverScopesPodUIDAndDetectsPIDReuse(t *testing.T) {
	root := t.TempDir()
	uid := "12345678-1234-1234-1234-123456789012"
	create := func(pid int, pod, start string) {
		base := filepath.Join(root, strconv.Itoa(pid))
		if err := os.MkdirAll(base, 0700); err != nil {
			t.Fatal(err)
		}
		fields := make([]string, 20)
		for i := range fields {
			fields[i] = "0"
		}
		fields[0] = "S"
		fields[19] = start
		os.WriteFile(filepath.Join(base, "stat"), []byte(strconv.Itoa(pid)+" (name with spaces) "+strings.Join(fields, " ")), 0600)
		os.WriteFile(filepath.Join(base, "cgroup"), []byte("0::/kubepods.slice/kubepods-pod"+strings.ReplaceAll(pod, "-", "_")+".slice/container.scope"), 0600)
		os.WriteFile(filepath.Join(base, "maps"), []byte("1000-2000 r-xp 0 00:01 1 /usr/lib/libssl.so.3\n"), 0600)
	}
	create(42, uid, "100")
	create(43, "87654321-1234-1234-1234-123456789012", "100")
	create(44, uid+"extra", "100")
	processes, err := Discover(root, uid, 0, "")
	if err != nil || len(processes) != 1 || processes[0].PID != 42 {
		t.Fatalf("wrong process scope: %+v %v", processes, err)
	}
	if !Alive(root, processes[0], uid) {
		t.Fatal("live identity rejected")
	}
	create(42, uid, "101")
	if Alive(root, processes[0], uid) {
		t.Fatal("reused PID accepted")
	}
	if _, err = Discover(root, "", 0, ""); err == nil {
		t.Fatal("unscoped collection allowed")
	}
}

func TestReceiveRejectsEventBeforeReady(t *testing.T) {
	server := httptest.NewServer(websocket.Handler(func(ws *websocket.Conn) { defer ws.Close(); _ = websocket.Message.Send(ws, eventWire(nil)) }))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := Receive(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/", func(Entry) error { return errors.New("should not dispatch") }, nil)
	if err == nil || !strings.Contains(err.Error(), "before attachment") {
		t.Fatalf("unexpected readiness result: %v", err)
	}
}
