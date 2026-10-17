package l7

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

// These benchmark user-space reconstruction, including checkpoint construction.
// They do not measure BPF attachment, TLS extraction, or storage durability.
func BenchmarkHTTP1Reconstruction(b *testing.B) {
	for _, size := range []int{128, 98304} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			request := []byte(fmt.Sprintf("POST /bench HTTP/1.1\r\nHost: benchmark\r\nContent-Length: %d\r\n\r\n%s", size, bytes.Repeat([]byte{'x'}, size)))
			response := []byte("HTTP/1.1 204 No Content\r\n\r\n")
			now := time.Unix(1700000000, 0)
			b.SetBytes(int64(len(request) + len(response)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				count := 0
				e := NewEngine(Config{}, func(*Exchange) error { count++; return nil })
				for j, p := range [][]byte{request, response} {
					lane := "write"
					if j == 1 {
						lane = "read"
					}
					if err := e.Feed(Observation{Epoch: "b", Flow: "c", Lane: lane, Sequence: uint64(j + 1), Timestamp: now, Data: p, OriginalLength: len(p), Verified: true}); err != nil {
						b.Fatal(err)
					}
				}
				if count != 1 {
					b.Fatalf("reconstructed %d exchanges", count)
				}
			}
		})
	}
}

func BenchmarkHTTP2CheckpointReconstruction(b *testing.B) {
	var request, response, cb, sb bytes.Buffer
	request.WriteString(http2.ClientPreface)
	ce, se := hpack.NewEncoder(&cb), hpack.NewEncoder(&sb)
	cf, sf := http2.NewFramer(&request, nil), http2.NewFramer(&response, nil)
	for i := uint32(1); i < 64; i += 2 {
		cb.Reset()
		ce.WriteField(hpack.HeaderField{Name: ":method", Value: "GET"})
		ce.WriteField(hpack.HeaderField{Name: ":path", Value: "/checkpoint"})
		ce.WriteField(hpack.HeaderField{Name: "x-context", Value: "state-shared-across-streams"})
		cf.WriteHeaders(http2.HeadersFrameParam{StreamID: i, BlockFragment: cb.Bytes(), EndStream: true, EndHeaders: true})
		sb.Reset()
		se.WriteField(hpack.HeaderField{Name: ":status", Value: "204"})
		sf.WriteHeaders(http2.HeadersFrameParam{StreamID: i, BlockFragment: sb.Bytes(), EndStream: true, EndHeaders: true})
	}
	b.SetBytes(int64(request.Len() + response.Len()))
	b.ReportAllocs()
	now := time.Unix(1700000000, 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		e := NewEngine(Config{}, func(*Exchange) error { count++; return nil })
		for j, p := range [][]byte{request.Bytes(), response.Bytes()} {
			lane := "write"
			if j == 1 {
				lane = "read"
			}
			if err := e.Feed(Observation{Epoch: "b", Flow: "c", Lane: lane, Sequence: uint64(j + 1), Timestamp: now, Data: p, OriginalLength: len(p), Verified: true}); err != nil {
				b.Fatal(err)
			}
		}
		if count != 32 {
			b.Fatalf("reconstructed %d exchanges", count)
		}
	}
}
