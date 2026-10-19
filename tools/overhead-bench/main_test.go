package main

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/kapture-io/kapture/internal/storage"
)

type testWriter struct{ err error }

func (w testWriter) Write(context.Context, *storage.CapturedRequest) error { return w.err }
func (w testWriter) Flush(context.Context) error                           { return nil }
func (w testWriter) Close() error                                          { return nil }

func TestCaptureOracleRejectsDuplicatesAndInvalidBodies(t *testing.T) {
	c := &counters{seen: map[uint64]bool{}}
	w := &oracleWriter{Writer: testWriter{}, stats: c, size: 3}
	r := &storage.CapturedRequest{Method: "POST", Path: "/bench", Body: []byte("xxx"), Headers: http.Header{"X-Bench-Id": {"1"}, "X-Bench-Phase": {"measure"}, "Authorization": {"[REDACTED]"}}}
	ctx := context.Background()
	if err := w.Write(ctx, r); err != nil {
		t.Fatal(err)
	}
	if err := w.Write(ctx, r); err != nil {
		t.Fatal(err)
	}
	r.Body = []byte("bad")
	r.Headers["X-Bench-Id"] = []string{"2"}
	_ = w.Write(ctx, r)
	r.Body = []byte("xxx")
	r.Headers["X-Bench-Phase"] = []string{"warmup"}
	_ = w.Write(ctx, r)
	if c.Exchanges != 1 || c.IDSum != 1 || c.Duplicates != 1 || c.Invalid != 1 {
		t.Fatalf("oracle accepted incorrect traffic: %+v", c)
	}
}

func TestCaptureOracleDoesNotCountFailedStorageWrites(t *testing.T) {
	c := &counters{seen: map[uint64]bool{}}
	w := &oracleWriter{Writer: testWriter{err: errors.New("disk full")}, stats: c, size: 3}
	if err := w.Write(context.Background(), &storage.CapturedRequest{}); err == nil {
		t.Fatal("write failure hidden")
	}
	if c.Exchanges != 0 || c.Invalid != 0 {
		t.Fatal("failed storage was counted as captured")
	}
}
