package builtin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"

	replayenginev1 "github.com/kapture-io/kapture/proto/replayengine/v1"
)

func runEngine(t *testing.T, engine *Engine, items []*replayenginev1.FeedItem) []*replayenginev1.ExecuteResponse {
	t.Helper()

	feed := make(chan *replayenginev1.FeedItem, len(items))
	for _, item := range items {
		feed <- item
	}
	close(feed)

	events := make(chan *replayenginev1.ExecuteResponse, len(items)+8)
	if err := engine.Execute(context.Background(), feed, events); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	close(events)

	var collected []*replayenginev1.ExecuteResponse
	for ev := range events {
		collected = append(collected, ev)
	}
	return collected
}

func configureAgainst(t *testing.T, engine *Engine, serverURL string) {
	t.Helper()
	u, err := url.Parse(serverURL)
	if err != nil {
		t.Fatal(err)
	}
	port, _ := strconv.Atoi(u.Port())
	err = engine.Configure(context.Background(), &replayenginev1.RunConfig{
		Target:      &replayenginev1.Target{Host: u.Hostname(), Port: int32(port)},
		Concurrency: 4,
	})
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}
}

func TestBuiltinEngine_SendsAndSummarizes(t *testing.T) {
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.Header.Get("X-Kapture-Replay") != "true" {
			t.Errorf("missing replay marker header")
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	engine := New()
	configureAgainst(t, engine, server.URL)

	items := []*replayenginev1.FeedItem{
		{RequestId: "a", Method: "GET", Path: "/one"},
		{RequestId: "b", Method: "POST", Path: "/two", Body: []byte("hello")},
		{RequestId: "c", Method: "GET", Path: "/three"},
	}
	events := runEngine(t, engine, items)

	var results, summaries int
	var summary *replayenginev1.RunSummary
	for _, ev := range events {
		switch e := ev.Event.(type) {
		case *replayenginev1.ExecuteResponse_Result:
			results++
			if e.Result.StatusCode != 200 || e.Result.Error != "" {
				t.Errorf("unexpected result: %+v", e.Result)
			}
		case *replayenginev1.ExecuteResponse_Summary:
			summaries++
			summary = e.Summary
		}
	}
	if results != 3 || summaries != 1 {
		t.Fatalf("got %d results / %d summaries, want 3 / 1", results, summaries)
	}
	if summary.SentRequests != 3 || summary.FailedRequests != 0 {
		t.Errorf("summary counts wrong: %+v", summary)
	}
	if summary.StatusCodes["200"] != 3 {
		t.Errorf("status code distribution wrong: %v", summary.StatusCodes)
	}
	if hits.Load() != 3 {
		t.Errorf("target received %d requests, want 3", hits.Load())
	}
}

func TestBuiltinEngine_CountsFailures(t *testing.T) {
	engine := New()
	// Unroutable target: every send fails.
	err := engine.Configure(context.Background(), &replayenginev1.RunConfig{
		Target:      &replayenginev1.Target{Host: "127.0.0.1", Port: 1},
		Concurrency: 2,
	})
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}

	events := runEngine(t, engine, []*replayenginev1.FeedItem{
		{RequestId: "a", Method: "GET", Path: "/x"},
		{RequestId: "b", Method: "GET", Path: "/y"},
	})

	var summary *replayenginev1.RunSummary
	for _, ev := range events {
		if s, ok := ev.Event.(*replayenginev1.ExecuteResponse_Summary); ok {
			summary = s.Summary
		}
	}
	if summary == nil {
		t.Fatal("no summary emitted")
	}
	if summary.FailedRequests != 2 || summary.SentRequests != 0 {
		t.Errorf("summary = %+v, want 2 failures", summary)
	}
}

func TestBuiltinEngine_RequiresConfigure(t *testing.T) {
	engine := New()
	feed := make(chan *replayenginev1.FeedItem)
	close(feed)
	events := make(chan *replayenginev1.ExecuteResponse, 1)
	if err := engine.Execute(context.Background(), feed, events); err == nil {
		t.Error("Execute without Configure succeeded")
	}
}
