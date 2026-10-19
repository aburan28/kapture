// l7-bench measures reconstruction only, excluding acquisition and storage.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/kapture-io/kapture/internal/l7"
)

func main() {
	size := flag.Int("body", 128, "Request body bytes")
	iterations := flag.Int("iterations", 20000, "New connections per trial")
	flag.Parse()
	if *size < 0 || *size > 1<<20 || *iterations <= 0 {
		log.Fatal("invalid body size or iteration count")
	}
	request := []byte(fmt.Sprintf("POST /bench HTTP/1.1\r\nHost: benchmark\r\nContent-Length: %d\r\n\r\n%s", *size, bytes.Repeat([]byte{'x'}, *size)))
	response := []byte("HTTP/1.1 204 No Content\r\n\r\n")
	now := time.Unix(1700000000, 0)
	count := 0
	start := time.Now()
	for i := 0; i < *iterations; i++ {
		engine := l7.NewEngine(l7.Config{}, func(x *l7.Exchange) error {
			if x.Request.BodyLength != int64(*size) || !x.Quality.FramingComplete || x.Response.Status != 204 {
				log.Fatal("incorrect reconstruction")
			}
			count++
			return nil
		})
		for j, data := range [][]byte{request, response} {
			lane := "write"
			if j == 1 {
				lane = "read"
			}
			if err := engine.Feed(l7.Observation{Epoch: "b", Flow: "c", Lane: lane, Sequence: uint64(j + 1), Timestamp: now, Data: data, OriginalLength: len(data), Verified: true}); err != nil {
				log.Fatal(err)
			}
		}
	}
	elapsed := time.Since(start)
	if count != *iterations {
		log.Fatalf("got %d exchanges, expected %d", count, *iterations)
	}
	fmt.Printf("kapture,http1,%d,%d,%d,%d,%.2f\n", *size, *iterations, count, len(request)+len(response), float64(elapsed.Nanoseconds())/float64(*iterations))
}
