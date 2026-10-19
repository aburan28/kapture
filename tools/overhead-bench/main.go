// Command overhead-bench supplies the load generator and measured capture
// processes for tools/overhead/run.py. Benchmark fixtures never target users.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/kapture-io/kapture/internal/agent"
	"github.com/kapture-io/kapture/internal/ebpf"
	"github.com/kapture-io/kapture/internal/ecapture"
	"github.com/kapture-io/kapture/internal/l7"
	"github.com/kapture-io/kapture/internal/storage"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return errors.New("expected load or collect subcommand")
	}
	if os.Args[1] == "load" {
		return load(os.Args[2:])
	}
	if os.Args[1] == "collect" {
		return collect(os.Args[2:])
	}
	return errors.New("unknown subcommand")
}

func load(args []string) error {
	f := flag.NewFlagSet("load", flag.ContinueOnError)
	url := f.String("url", "https://localhost:18443/bench", "isolated target")
	cert := f.String("cert", "", "fixture CA certificate")
	duration := f.Duration("duration", 6*time.Second, "offering interval")
	concurrency := f.Int("concurrency", 16, "workers / maximum in-flight requests")
	rate := f.Int("rate", 0, "fixed offered requests/sec; zero uses closed-loop saturation")
	size := f.Int("body", 128, "request bytes")
	phase := f.String("phase", "measure", "oracle cohort")
	if err := f.Parse(args); err != nil {
		return err
	}
	if *duration <= 0 || *duration > time.Minute || *concurrency < 1 || *concurrency > 256 || *size < 1 || *size > 16300 || *rate < 0 {
		return errors.New("invalid load options")
	}
	pem, err := os.ReadFile(*cert)
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return errors.New("invalid fixture certificate")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13}, MaxIdleConnsPerHost: *concurrency, MaxConnsPerHost: *concurrency, ForceAttemptHTTP2: false}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	body := bytes.Repeat([]byte("x"), *size)
	digest := sha256.Sum256(body)
	wantHash := hex.EncodeToString(digest[:])
	var seq, failures, success, missed atomic.Uint64
	var mu sync.Mutex
	var latency, service []int64
	var firstError string
	start := time.Now()
	end := start.Add(*duration)
	perform := func(scheduled time.Time) {
		id := strconv.FormatUint(seq.Add(1), 10)
		r, _ := http.NewRequest("POST", *url, bytes.NewReader(body))
		r.Header.Set("X-Bench-Id", id)
		r.Header.Set("X-Bench-Phase", *phase)
		r.Header.Set("Authorization", "synthetic-benchmark-canary")
		began := time.Now()
		response, requestErr := client.Do(r)
		if requestErr == nil {
			_, requestErr = io.Copy(io.Discard, response.Body)
			response.Body.Close()
			if requestErr == nil && (response.StatusCode != 204 || response.Header.Get("X-Bench-Id") != id || response.Header.Get("X-Bench-SHA256") != wantHash || response.TLS == nil || response.TLS.Version != tls.VersionTLS13) {
				requestErr = errors.New("response oracle mismatch")
			}
		}
		finished := time.Now()
		if requestErr != nil {
			failures.Add(1)
			mu.Lock()
			if firstError == "" {
				firstError = requestErr.Error()
			}
			mu.Unlock()
			return
		}
		success.Add(1)
		mu.Lock()
		latency = append(latency, finished.Sub(scheduled).Nanoseconds())
		service = append(service, finished.Sub(began).Nanoseconds())
		mu.Unlock()
	}
	var wg sync.WaitGroup
	if *rate == 0 {
		for i := 0; i < *concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for time.Now().Before(end) {
					perform(time.Now())
				}
			}()
		}
	} else {
		jobs := make(chan time.Time, *concurrency)
		for i := 0; i < *concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for scheduled := range jobs {
					perform(scheduled)
				}
			}()
		}
		// Schedule from the original deadline; latency includes scheduler/queue
		// delay and every missed slot is reported, avoiding coordinated omission.
		for n := 0; ; n++ {
			scheduled := start.Add(time.Duration(n) * time.Second / time.Duration(*rate))
			if !scheduled.Before(end) {
				break
			}
			if delay := time.Until(scheduled); delay > 0 {
				time.Sleep(delay)
			}
			select {
			case jobs <- scheduled:
			default:
				missed.Add(1)
			}
		}
		close(jobs)
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	quantile := func(values []int64, p float64) float64 {
		if len(values) == 0 {
			return 0
		}
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		i := int(float64(len(values)-1) * p)
		return float64(values[i]) / 1e6
	}
	result := map[string]any{"success": success.Load(), "errors": failures.Load(), "missed": missed.Load(), "attempts": seq.Load(), "offering_seconds": duration.Seconds(), "elapsed_seconds": elapsed, "rps": float64(success.Load()) / elapsed, "p50_ms": quantile(latency, .5), "p95_ms": quantile(latency, .95), "p99_ms": quantile(latency, .99), "service_p95_ms": quantile(service, .95), "first_error": firstError}
	return json.NewEncoder(os.Stdout).Encode(result)
}

type counters struct {
	mu           sync.Mutex
	Ready        bool   `json:"ready"`
	Events       uint64 `json:"events"`
	Loss         uint64 `json:"loss"`
	Unattributed uint64 `json:"unattributed"`
	Exchanges    uint64 `json:"exchanges"`
	Invalid      uint64 `json:"invalid"`
	Duplicates   uint64 `json:"duplicates"`
	IDSum        uint64 `json:"id_sum"`
	seen         map[uint64]bool
}

func (c *counters) observe(id string, phase string, valid bool) {
	if phase != "measure" {
		return
	}
	n, err := strconv.ParseUint(id, 10, 64)
	c.mu.Lock()
	defer c.mu.Unlock()
	if !valid || err != nil || n == 0 {
		c.Invalid++
		return
	}
	if c.seen[n] {
		c.Duplicates++
		return
	}
	c.seen[n] = true
	c.Exchanges++
	c.IDSum += n
}

type oracleWriter struct {
	storage.Writer
	stats *counters
	size  int
}

func (w *oracleWriter) Write(ctx context.Context, r *storage.CapturedRequest) error {
	if err := w.Writer.Write(ctx, r); err != nil {
		return err
	}
	h := http.Header(r.Headers)
	w.stats.observe(h.Get("X-Bench-Id"), h.Get("X-Bench-Phase"), r.Method == "POST" && r.Path == "/bench" && len(r.Body) == w.size && bytes.Equal(r.Body, bytes.Repeat([]byte("x"), w.size)) && h.Get("Authorization") == "[REDACTED]")
	return nil
}

func collect(args []string) error {
	f := flag.NewFlagSet("collect", flag.ContinueOnError)
	mode := f.String("mode", "go", "acquire, go, rust, export, or mirror")
	pid := f.Int("pid", 0, "OpenSSL application PID")
	producer := f.String("ecapture", "/ecapture", "pinned producer")
	rust := f.String("rust", "", "Rust parser executable")
	dir := f.String("output", "", "private artifact directory")
	size := f.Int("body", 128, "oracle body bytes")
	statusPort := f.Int("status-port", 18101, "benchmark status")
	resultFile := f.String("result", "", "final oracle counters JSON")
	if err := f.Parse(args); err != nil {
		return err
	}
	if *mode != "acquire" && *mode != "go" && *mode != "rust" && *mode != "export" && *mode != "mirror" {
		return errors.New("invalid collector mode")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	s := &counters{seen: map[uint64]bool{}}
	defer func() {
		if *resultFile != "" {
			s.mu.Lock()
			defer s.mu.Unlock()
			b, _ := json.Marshal(s)
			_ = os.WriteFile(*resultFile, b, 0600)
		}
	}()
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		_ = json.NewEncoder(w).Encode(s)
	})
	status := &http.Server{Addr: fmt.Sprintf("127.0.0.1:%d", *statusPort), Handler: mux, ReadHeaderTimeout: time.Second}
	go func() {
		if err := status.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(os.Stderr, err)
			cancel()
		}
	}()
	defer status.Close()
	if *mode == "export" || *mode == "mirror" {
		factory, err := storage.NewEFSWriterFactory(storage.EFSConfig{MountPath: *dir, AgentPod: "benchmark", MaxBufferBytes: 1 << 20, FlushInterval: time.Second})
		if err != nil {
			return err
		}
		inner, err := factory.NewWriter(ctx, "benchmark/capture")
		if err != nil {
			return err
		}
		writer := &oracleWriter{Writer: inner, stats: s, size: *size}
		log := slog.New(slog.NewJSONHandler(io.Discard, nil))
		buffered := agent.NewBufferedWriter(agent.BufferedWriterConfig{Writer: writer, BatchSize: 100, FlushInterval: time.Second, Logger: log})
		queue := agent.NewAsyncWriter(buffered, 1024, log)
		defer queue.Close()
		if *mode == "mirror" {
			handler := agent.NewCaptureHandler(agent.CaptureHandlerConfig{Writer: queue, MaxBodyBytes: 1 << 20, Logger: log})
			server := agent.NewAgentServer(agent.AgentServerConfig{HTTPPort: 18080, GRPCPort: 18090, HealthPort: 18081, Handler: handler, Queue: queue, Logger: log})
			s.mu.Lock()
			s.Ready = true
			s.mu.Unlock()
			return server.Start(ctx)
		}
		// Exercise the production pipeline, including typed acquisition, bounded
		// reconstruction, redaction, synthetic PCAPNG and synced artifact writes.
		go func() {
			for ctx.Err() == nil {
				resp, err := http.Get("http://127.0.0.1:18102/healthz")
				if err == nil {
					ok := resp.StatusCode == 200
					resp.Body.Close()
					if ok {
						s.mu.Lock()
						s.Ready = true
						s.mu.Unlock()
						return
					}
				}
				time.Sleep(50 * time.Millisecond)
			}
		}()
		return ebpf.Run(ctx, ebpf.Config{CaptureID: "benchmark/capture", Binary: *producer, PID: *pid, HealthAddress: "127.0.0.1:18102", Engine: l7.Config{}, Export: l7.ExportConfig{CaptureID: "benchmark/capture", Writer: queue, Artifacts: factory, IncludeHeaders: true, IncludeBody: true, PCAP: true}})
	}
	var engine *l7.Engine
	if *mode == "go" {
		want := sha256.Sum256(bytes.Repeat([]byte("x"), *size))
		digest := hex.EncodeToString(want[:])
		engine = l7.NewEngine(l7.Config{}, func(x *l7.Exchange) error {
			s.observe(x.Request.Headers.Get("X-Bench-Id"), x.Request.Headers.Get("X-Bench-Phase"), x.Request.Complete && x.Response.Complete && x.Response.Status == 204 && x.Request.Method == "POST" && x.Request.Path == "/bench" && len(x.Request.Body) == *size && bytes.Equal(x.Request.Body, bytes.Repeat([]byte("x"), *size)) && x.Response.Headers.Get("X-Bench-Id") == x.Request.Headers.Get("X-Bench-Id") && x.Response.Headers.Get("X-Bench-SHA256") == digest && len(x.Quality.Reasons) == 0)
			return nil
		})
	}
	var input io.WriteCloser
	var rustCmd *exec.Cmd
	var rustDone chan error
	if *mode == "rust" {
		want := sha256.Sum256(bytes.Repeat([]byte("x"), *size))
		rustCmd = exec.Command(*rust, strconv.Itoa(*size), hex.EncodeToString(want[:]))
		rustCmd.Stderr = os.Stderr
		var err error
		input, err = rustCmd.StdinPipe()
		if err != nil {
			return err
		}
		out, err := rustCmd.StdoutPipe()
		if err != nil {
			return err
		}
		if err = rustCmd.Start(); err != nil {
			return err
		}
		rustDone = make(chan error, 1)
		go func() {
			reader := bufio.NewReader(out)
			var b [10]byte
			for {
				_, err := io.ReadFull(reader, b[:])
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					rustDone <- err
					return
				}
				s.observe(strconv.FormatUint(binaryLittle(b[:8]), 10), map[bool]string{true: "measure", false: "warmup"}[b[9] == 1], b[8] == 1)
			}
			rustDone <- rustCmd.Wait()
		}()
	}
	processes, err := ecapture.Discover("/proc", "", *pid, "")
	if err != nil {
		return err
	}
	if len(processes) != 1 {
		return errors.New("cannot discover application OpenSSL")
	}
	err = ecapture.Collect(ctx, *producer, "/proc", "", processes[0], 256, func(entry ecapture.Entry) error {
		if ecapture.IsLossLog(entry.ProcessLog) {
			s.mu.Lock()
			s.Loss++
			s.mu.Unlock()
			if engine != nil {
				return engine.Loss("benchmark", "producer_reported_loss")
			}
		}
		if entry.Event == nil {
			return nil
		}
		o, err := entry.Event.Observation("benchmark", time.Now().UTC())
		s.mu.Lock()
		s.Events++
		if err != nil {
			s.Unattributed++
		}
		s.mu.Unlock()
		if err != nil {
			return nil
		}
		if engine != nil {
			return engine.Feed(o)
		}
		if input != nil {
			var header [7]byte
			binary.LittleEndian.PutUint16(header[:2], uint16(len(o.Flow)))
			binary.LittleEndian.PutUint32(header[2:6], uint32(len(o.Data)))
			if o.Lane == "write" {
				header[6] = 1
			}
			// One bounded write per observation: explicit IPC is included in the
			// Rust variant's acquisition + reconstruction cost.
			frame := append(header[:], []byte(o.Flow)...)
			frame = append(frame, o.Data...)
			_, err = input.Write(frame)
			return err
		}
		return nil
	}, func() { s.mu.Lock(); s.Ready = true; s.mu.Unlock() }, func(line string) { fmt.Fprintln(os.Stderr, line) })
	if input != nil {
		_ = input.Close()
		if rustErr := <-rustDone; rustErr != nil {
			return rustErr
		}
	}
	if engine != nil {
		if endErr := engine.EndEpoch("benchmark"); endErr != nil {
			return endErr
		}
	}
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func binaryLittle(b []byte) uint64 { return binary.LittleEndian.Uint64(b) }
