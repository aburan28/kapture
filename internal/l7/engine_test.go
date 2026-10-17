package l7

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

type fixture struct {
	t        *testing.T
	engine   *Engine
	sequence uint64
	out      []*Exchange
}

func newFixture(t *testing.T, cfg Config) *fixture {
	t.Helper()
	f := &fixture{t: t}
	f.engine = NewEngine(cfg, func(x *Exchange) error { f.out = append(f.out, x); return nil })
	return f
}
func (f *fixture) feed(lane string, p []byte) {
	f.t.Helper()
	f.sequence++
	if err := f.engine.Feed(Observation{Epoch: "epoch", Flow: "connection", Lane: lane, Sequence: f.sequence, ID: strings.Repeat("x", 2), Timestamp: time.Unix(1700000000, int64(f.sequence)), Data: p, OriginalLength: len(p), Verified: true}); err != nil {
		f.t.Fatal(err)
	}
}
func (f *fixture) fragmented(lane string, p []byte) {
	f.t.Helper()
	for len(p) > 0 {
		n := min(len(p), 7)
		f.feed(lane, p[:n])
		p = p[n:]
	}
}

func TestHTTP1ChunkedPipeliningAndServerDirection(t *testing.T) {
	f := newFixture(t, Config{})
	f.fragmented("read", []byte("POST /a?q=1 HTTP/1.1\r\nHost: service\r\nTransfer-Encoding: chunked\r\nTrailer: X-End\r\n\r\n3\r\nabc\r\n0\r\nX-End: yes\r\n\r\nGET /b HTTP/1.1\r\nHost: service\r\n\r\n"))
	f.fragmented("write", []byte("HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTrailer: X-End\r\n\r\n4\r\ndefg\r\n0\r\nX-End: done\r\n\r\nHTTP/1.1 204 No Content\r\n\r\n"))
	if len(f.out) != 2 {
		t.Fatalf("got %d exchanges; decoder: %v", len(f.out), f.engine.lastError)
	}
	x := f.out[0]
	if x.Request.Path != "/a?q=1" || string(x.Request.Body) != "abc" || x.Request.Trailers.Get("X-End") != "yes" || string(x.Response.Body) != "defg" || x.Response.Trailers.Get("X-End") != "done" {
		t.Fatalf("wrong exchange: %+v", x)
	}
	if !x.Quality.FramingComplete || !x.ReplayEligible() || len(x.Quality.Reasons) > 0 {
		t.Fatalf("quality: %+v", x.Quality)
	}
	if f.out[1].Response.Status != 204 || f.out[0].ID == f.out[1].ID {
		t.Fatal("pipeline correlation failed")
	}
}

func TestHTTP1BodyTruncationDoesNotBreakFollowingMessage(t *testing.T) {
	f := newFixture(t, Config{MaxBodyBytes: 2})
	f.feed("write", []byte("POST /big HTTP/1.1\r\nContent-Length: 4\r\n\r\n1234GET /small HTTP/1.1\r\n\r\n"))
	f.feed("read", []byte("HTTP/1.1 204 No Content\r\n\r\nHTTP/1.1 204 No Content\r\n\r\n"))
	if len(f.out) != 2 || !f.out[0].Request.BodyTruncated || f.out[0].Request.BodyLength != 4 || f.out[0].ReplayEligible() || !f.out[1].ReplayEligible() {
		t.Fatalf("wrong truncation/correlation: %+v", f.out)
	}
}

func TestKnownDecoderLossRemainsVisibleAfterFlowReappears(t *testing.T) {
	f := newFixture(t, Config{MaxBufferBytes: 128})
	f.feed("write", bytes.Repeat([]byte{'x'}, 129))
	f.feed("write", []byte("GET /later HTTP/1.1\r\n\r\n"))
	f.feed("read", []byte("HTTP/1.1 204 No Content\r\n\r\n"))
	if len(f.out) != 1 || f.out[0].ReplayEligible() || len(f.out[0].Quality.Reasons) == 0 {
		t.Fatal("flow eviction erased known capture loss")
	}
}

func TestHPACKHeaderExpansionHasBoundedEmission(t *testing.T) {
	d := newHeaderDecoder()
	if _, err := d.decode(bytes.Repeat([]byte{0x82}, 65536)); err == nil {
		t.Fatal("unbounded indexed header expansion accepted")
	}
}

func TestHTTP1CloseDelimitedResponseNeedsExplicitClose(t *testing.T) {
	f := newFixture(t, Config{})
	f.feed("write", []byte("GET / HTTP/1.1\r\n\r\n"))
	f.feed("read", []byte("HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nabc"))
	if len(f.out) != 0 {
		t.Fatal("premature EOF treated as message end")
	}
	f.sequence++
	err := f.engine.Feed(Observation{Epoch: "epoch", Flow: "connection", Lane: "read", Sequence: f.sequence, Timestamp: time.Unix(1700000001, 0), Close: true, Verified: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(f.out) != 1 || string(f.out[0].Response.Body) != "abc" || !f.out[0].Response.Complete {
		t.Fatal("close-delimited response missing")
	}
}

func headerBlock(t *testing.T, enc *hpack.Encoder, buffer *bytes.Buffer, fields ...hpack.HeaderField) []byte {
	t.Helper()
	buffer.Reset()
	for _, v := range fields {
		if err := enc.WriteField(v); err != nil {
			t.Fatal(err)
		}
	}
	return append([]byte(nil), buffer.Bytes()...)
}
func frames(t *testing.T, fn func(*http2.Framer) error) []byte {
	t.Helper()
	var b bytes.Buffer
	if err := fn(http2.NewFramer(&b, nil)); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func TestHTTP2CheckpointRestoresOlderDynamicHeaders(t *testing.T) {
	f := newFixture(t, Config{})
	f.fragmented("write", []byte(http2.ClientPreface))
	f.feed("read", frames(t, func(fr *http2.Framer) error { return fr.WriteSettings() }))
	var client, server bytes.Buffer
	ce, se := hpack.NewEncoder(&client), hpack.NewEncoder(&server)
	for _, stream := range []uint32{1, 3} {
		block := headerBlock(t, ce, &client, hpack.HeaderField{Name: ":method", Value: "GET"}, hpack.HeaderField{Name: ":path", Value: "/selected"}, hpack.HeaderField{Name: "x-context", Value: "introduced-before-history-window"})
		mid := len(block) / 2
		f.fragmented("write", frames(t, func(fr *http2.Framer) error {
			if err := fr.WriteHeaders(http2.HeadersFrameParam{StreamID: stream, BlockFragment: block[:mid], EndStream: true}); err != nil {
				return err
			}
			return fr.WriteContinuation(stream, true, block[mid:])
		}))
		response := headerBlock(t, se, &server, hpack.HeaderField{Name: ":status", Value: "200"})
		f.feed("read", frames(t, func(fr *http2.Framer) error {
			return fr.WriteHeaders(http2.HeadersFrameParam{StreamID: stream, BlockFragment: response, EndStream: true, EndHeaders: true})
		}))
	}
	if len(f.out) != 2 || f.out[1].Request.Headers.Get("X-Context") == "" {
		t.Fatalf("missing reconstructed headers: %+v; decoder: %v", f.out, f.engine.lastError)
	}
	proof := f.out[1].HeaderProofs[0]
	if len(proof.Before.Entries) == 0 {
		t.Fatal("missing dynamic-table checkpoint")
	}
	decoder, err := RestoreHeaderState(proof.Before)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decoder.DecodeFull(proof.Block)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, h := range decoded {
		if h.Name == "x-context" && h.Value == "introduced-before-history-window" {
			found = true
		}
	}
	if !found {
		t.Fatalf("checkpoint did not restore the selected exchange: %+v", decoded)
	}
}

func TestGRPCMultiplexingAndTrailersOnlyFailure(t *testing.T) {
	f := newFixture(t, Config{})
	f.feed("write", []byte(http2.ClientPreface))
	var cb, sb bytes.Buffer
	ce, se := hpack.NewEncoder(&cb), hpack.NewEncoder(&sb)
	for _, id := range []uint32{1, 3} {
		block := headerBlock(t, ce, &cb, hpack.HeaderField{Name: ":method", Value: "POST"}, hpack.HeaderField{Name: ":path", Value: "/example.Service/Call"}, hpack.HeaderField{Name: "content-type", Value: "application/grpc"})
		f.feed("write", frames(t, func(fr *http2.Framer) error {
			return fr.WriteHeaders(http2.HeadersFrameParam{StreamID: id, BlockFragment: block, EndHeaders: true})
		}))
	}
	message := []byte{0, 0, 0, 0, 3, 'a', 'b', 'c'}
	for _, id := range []uint32{3, 1} {
		f.feed("write", frames(t, func(fr *http2.Framer) error { return fr.WriteData(id, true, message) }))
		response := headerBlock(t, se, &sb, hpack.HeaderField{Name: ":status", Value: "200"}, hpack.HeaderField{Name: "grpc-status", Value: "13"})
		f.feed("read", frames(t, func(fr *http2.Framer) error {
			return fr.WriteHeaders(http2.HeadersFrameParam{StreamID: id, BlockFragment: response, EndStream: true, EndHeaders: true})
		}))
	}
	if len(f.out) != 2 || f.out[0].StreamID != 3 || f.out[1].StreamID != 1 {
		t.Fatal("streams were conflated")
	}
	if f.out[0].Protocol != "gRPC" || len(f.out[0].Request.GRPC) != 1 || string(f.out[0].Request.GRPC[0].Data) != "abc" || f.out[0].Response.Headers.Get("Grpc-Status") != "13" {
		t.Fatalf("gRPC reconstruction: %+v", f.out[0])
	}
}

func TestLossAndLimitsNeverYieldFalseCompleteness(t *testing.T) {
	f := newFixture(t, Config{MaxMemoryBytes: 4096})
	f.feed("write", []byte("GET /pending HTTP/1.1\r\n\r\n"))
	f.sequence++ // drop one producer event
	f.feed("read", []byte("HTTP/1.1 204 No Content\r\n\r\n"))
	if len(f.out) != 1 || f.out[0].Quality.FramingComplete || f.out[0].ReplayEligible() {
		t.Fatalf("loss was hidden: %+v", f.out)
	}
	if err := f.engine.Close(); err != nil {
		t.Fatal(err)
	}
	if f.engine.MemoryBytes() != 0 {
		t.Fatal("state not released")
	}
	for i := 0; i < 20; i++ {
		f.feed("write", []byte("POST /big HTTP/1.1\r\nContent-Length: 10000\r\n\r\n"+strings.Repeat("a", 5000)))
	}
	if f.engine.MemoryBytes() > 4096 {
		t.Fatal("global memory limit not enforced")
	}
}

func TestStorageFailureIsNotSwallowedByDecoder(t *testing.T) {
	failure := errors.New("storage unavailable")
	e := NewEngine(Config{}, func(*Exchange) error { return failure })
	for i, v := range []struct{ lane, body string }{{"write", "GET / HTTP/1.1\r\n\r\n"}, {"read", "HTTP/1.1 204 No Content\r\n\r\n"}} {
		err := e.Feed(Observation{Epoch: "e", Flow: "f", Lane: v.lane, Sequence: uint64(i + 1), Timestamp: time.Now(), Data: []byte(v.body), OriginalLength: len(v.body)})
		if i == 1 && !errors.Is(err, failure) {
			t.Fatalf("storage error lost: %v", err)
		}
	}
}

func TestSyntheticPCAPHasValidBlocksAndChecksums(t *testing.T) {
	x := &Exchange{ID: "example", Protocol: "HTTP/1.1", StartedAt: time.Unix(1700000000, 0), Request: Message{Method: "GET", Path: "/a?q=1", Headers: http.Header{}}, Response: Message{Status: 200, Body: []byte("hello")}}
	p, err := SyntheticPCAP(x)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(p, []byte("Synthetic plaintext")) || !bytes.Contains(p, []byte("X-Kapture-Reconstructed")) {
		t.Fatal("synthetic provenance absent")
	}
	packets := 0
	for len(p) > 0 {
		if len(p) < 12 {
			t.Fatal("short block")
		}
		n := int(binary.LittleEndian.Uint32(p[4:8]))
		if n < 12 || n > len(p) || binary.LittleEndian.Uint32(p[n-4:n]) != uint32(n) {
			t.Fatal("invalid block length")
		}
		if binary.LittleEndian.Uint32(p[:4]) == 6 {
			size := int(binary.LittleEndian.Uint32(p[20:24]))
			packet := p[28 : 28+size]
			if checksum(packet[:20]) != 0 {
				t.Fatal("bad IPv4 checksum")
			}
			pseudo := append([]byte(nil), packet[12:20]...)
			pseudo = append(pseudo, 0, 6, byte((len(packet)-20)>>8), byte(len(packet)-20))
			pseudo = append(pseudo, packet[20:]...)
			if checksum(pseudo) != 0 {
				t.Fatal("bad TCP checksum")
			}
			packets++
		}
		p = p[n:]
	}
	if packets < 7 {
		t.Fatal("missing TCP lifecycle")
	}
}
