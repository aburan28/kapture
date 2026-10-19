// backend-server runs the filesystem reference implementation of the public
// backend contract. Custom providers implement pkg/backend's small interfaces.
package main

import (
	"context"
	"crypto/subtle"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kapture-io/kapture/pkg/backend"
	"github.com/kapture-io/kapture/pkg/backend/filesystem"
)

func main() {
	address := flag.String("addr", "127.0.0.1:9080", "Backend HTTP listen address")
	directory := flag.String("data-dir", "", "Private directory for durable capture data (required)")
	flag.Parse()
	token := os.Getenv("BACKEND_TOKEN")
	host, _, err := net.SplitHostPort(*address)
	ip := net.ParseIP(host)
	if err != nil || (token == "" && host != "localhost" && (ip == nil || !ip.IsLoopback())) {
		slog.Error("non-loopback listeners require BACKEND_TOKEN and a valid address")
		os.Exit(1)
	}
	store, err := filesystem.New(*directory)
	if err != nil {
		slog.Error("cannot open backend storage", "error", err)
		os.Exit(1)
	}
	handler := backend.NewHandler(store)
	authenticated := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		handler.ServeHTTP(w, r)
	})
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	server := &http.Server{Addr: *address, Handler: authenticated, ReadHeaderTimeout: 5 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}
	go func() {
		<-ctx.Done()
		shutdownCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		_ = server.Shutdown(shutdownCtx)
	}()
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("backend server failed", "error", err)
		os.Exit(1)
	}
}
