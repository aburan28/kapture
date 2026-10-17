// Package ebpf wires the eCapture producer to L7 reconstruction and storage.
package ebpf

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/kapture-io/kapture/internal/ecapture"
	"github.com/kapture-io/kapture/internal/l7"
	"github.com/kapture-io/kapture/internal/storage"
)

type Config struct {
	CaptureID     string
	Binary        string
	ProcRoot      string
	PodUID        string
	PID           int
	Library       string
	MapPages      int
	SourceURL     string
	InputFile     string
	HealthAddress string
	Engine        l7.Config
	Export        l7.ExportConfig
}
type record struct {
	epoch       string
	observation *l7.Observation
	reason      string
	ready       bool
	ended       bool
	err         error
}
type stats struct {
	ready                                 atomic.Int64
	events, unattributed, loss            atomic.Uint64
	exchanges, requests, retained, memory atomic.Uint64
	decodeErrors                          atomic.Uint64
}

func Run(ctx context.Context, cfg Config) error {
	if cfg.ProcRoot == "" {
		cfg.ProcRoot = "/proc"
	}
	if cfg.Binary == "" {
		cfg.Binary = "/ecapture"
	}
	if cfg.MapPages == 0 {
		cfg.MapPages = 256
	}
	if cfg.SourceURL != "" && cfg.InputFile != "" {
		return fmt.Errorf("source URL and offline input are mutually exclusive")
	}
	if cfg.InputFile == "" && cfg.SourceURL == "" {
		if _, err := exec.LookPath(cfg.Binary); err != nil {
			return fmt.Errorf("eCapture executable: %w", err)
		}
		if cfg.PodUID == "" && cfg.PID <= 0 {
			return fmt.Errorf("pod UID or --ecapture-pid is required")
		}
	}
	if cfg.PID < 0 || cfg.MapPages < 8 || cfg.MapPages > 8192 {
		return fmt.Errorf("invalid PID or perf map size")
	}
	exporter, err := l7.NewExporter(cfg.Export)
	if err != nil {
		return err
	}
	storeCtx := ctx
	engine := l7.NewEngine(cfg.Engine, func(x *l7.Exchange) error { return exporter.Accept(storeCtx, x) })
	s := &stats{}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if cfg.HealthAddress != "" {
		listener, err := net.Listen("tcp", cfg.HealthAddress)
		if err != nil {
			return err
		}
		server := healthServer(s)
		go server.Serve(listener)
		defer func() {
			shutdown, stop := context.WithTimeout(context.Background(), 3*time.Second)
			defer stop()
			_ = server.Shutdown(shutdown)
		}()
	}
	var wg sync.WaitGroup
	records := make(chan record, 128)
	send := func(r record) error {
		select {
		case records <- r:
			return nil
		case <-runCtx.Done():
			return runCtx.Err()
		}
	}
	start := func(epoch string, source func(context.Context) error) {
		wg.Add(1)
		go func() { defer wg.Done(); err := source(runCtx); _ = send(record{epoch: epoch, ended: true, err: err}) }()
	}
	defer func() { cancel(); wg.Wait() }()
	handle := func(epoch string) func(ecapture.Entry) error {
		return func(entry ecapture.Entry) error {
			if ecapture.IsLossLog(entry.ProcessLog) {
				return send(record{epoch: epoch, reason: "producer_reported_loss"})
			}
			if entry.Event == nil {
				return nil
			}
			o, err := entry.Event.Observation(epoch, time.Now().UTC())
			if err != nil {
				s.unattributed.Add(1)
				return send(record{epoch: epoch, reason: "unattributed_or_unsupported_event"})
			}
			return send(record{epoch: epoch, observation: &o})
		}
	}
	if cfg.InputFile != "" {
		start("fixture", func(ctx context.Context) error {
			file, err := os.Open(cfg.InputFile)
			if err != nil {
				return err
			}
			defer file.Close()
			scanner := bufio.NewScanner(file)
			scanner.Buffer(make([]byte, 4096), ecapture.MaxMessageBytes)
			s.ready.Store(1)
			for scanner.Scan() {
				var o l7.Observation
				if err = json.Unmarshal(scanner.Bytes(), &o); err != nil {
					return err
				}
				if err = send(record{observation: &o}); err != nil {
					return err
				}
			}
			return scanner.Err()
		})
	} else if cfg.SourceURL != "" {
		epoch := uuid.NewString()
		start(epoch, func(ctx context.Context) error {
			return ecapture.Receive(ctx, cfg.SourceURL, handle(epoch), func() { _ = send(record{epoch: epoch, ready: true}) })
		})
	}
	active := map[int]struct {
		process ecapture.Process
		cancel  context.CancelFunc
	}{}
	scan := func() error {
		if cfg.SourceURL != "" || cfg.InputFile != "" {
			return nil
		}
		processes, err := ecapture.Discover(cfg.ProcRoot, cfg.PodUID, cfg.PID, cfg.Library)
		if err != nil {
			return err
		}
		if len(processes) > 8 {
			return fmt.Errorf("target exceeds eight OpenSSL processes; narrow the capture PID")
		}
		for pid, v := range active {
			if !ecapture.Alive(cfg.ProcRoot, v.process, cfg.PodUID) {
				v.cancel()
				delete(active, pid)
			}
		}
		for _, p := range processes {
			if _, ok := active[p.PID]; ok {
				continue
			}
			p := p
			epoch := uuid.NewString()
			sourceCtx, stop := context.WithCancel(runCtx)
			active[p.PID] = struct {
				process ecapture.Process
				cancel  context.CancelFunc
			}{p, stop}
			start(epoch, func(_ context.Context) error {
				return ecapture.Collect(sourceCtx, cfg.Binary, cfg.ProcRoot, cfg.PodUID, p, cfg.MapPages, handle(epoch), func() { _ = send(record{epoch: epoch, ready: true}) })
			})
		}
		return nil
	}
	if err = scan(); err != nil {
		return err
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	readyEpoch := map[string]bool{}
	var runErr error
loop:
	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				runErr = ctx.Err()
			}
			break loop
		case <-ticker.C:
			if err = scan(); err != nil {
				runErr = err
				break loop
			}
			if err = engine.Expire(time.Now().UTC()); err != nil {
				runErr = err
				break loop
			}
		case r := <-records:
			if r.ready && !readyEpoch[r.epoch] {
				readyEpoch[r.epoch] = true
				s.ready.Add(1)
			}
			if r.observation != nil {
				s.events.Add(1)
				if err = engine.Feed(*r.observation); err != nil {
					runErr = err
					break loop
				}
			}
			if r.reason != "" {
				s.loss.Add(1)
				if err = engine.Loss(r.epoch, r.reason); err != nil {
					runErr = err
					break loop
				}
			}
			if r.ended {
				if readyEpoch[r.epoch] {
					delete(readyEpoch, r.epoch)
					s.ready.Add(-1)
				}
				if err = engine.EndEpoch(r.epoch); err != nil {
					runErr = err
					break loop
				}
				if cfg.InputFile != "" || cfg.SourceURL != "" {
					if ctx.Err() == nil {
						runErr = r.err
					}
					break loop
				}
				// Fail the container rather than silently stop collecting a PID.
				if r.err != nil && !errors.Is(r.err, context.Canceled) {
					runErr = r.err
					break loop
				}
			}
		}
		s.exchanges.Store(exporter.Exchanges)
		s.requests.Store(exporter.Requests)
		s.retained.Store(uint64(exporter.MemoryBytes()))
		s.memory.Store(uint64(engine.MemoryBytes()))
		s.decodeErrors.Store(engine.DecodeErrors)
	}
	cancel()
	wg.Wait()
	finalCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
	defer stop()
	storeCtx = finalCtx
	// Producers are joined, so this queue is now finite. Preserve accepted
	// observations on SIGTERM without waiting for new source records.
drain:
	for {
		select {
		case r := <-records:
			if r.observation != nil {
				s.events.Add(1)
				if err := engine.Feed(*r.observation); err != nil && runErr == nil {
					runErr = err
				}
			}
			if r.reason != "" {
				s.loss.Add(1)
				if err := engine.Loss(r.epoch, r.reason); err != nil && runErr == nil {
					runErr = err
				}
			}
		default:
			break drain
		}
	}
	if err := engine.Close(); err != nil && runErr == nil {
		runErr = err
	}
	if err := cfg.Export.Writer.Flush(finalCtx); err != nil && runErr == nil {
		runErr = err
	}
	manifest := map[string]any{"formatVersion": 1, "captureID": cfg.CaptureID, "producerRevision": ecapture.Revision, "exchanges": exporter.Exchanges, "replayRequests": exporter.Requests, "observations": s.events.Load(), "lossIntervals": s.loss.Load(), "decodeErrors": engine.DecodeErrors, "unattributedEvents": s.unattributed.Load(), "retentionEvictions": exporter.Evictions, "captureIntegrity": "unverified", "stoppedAt": time.Now().UTC(), "failed": runErr != nil}
	data, _ := json.Marshal(manifest)
	if _, err := cfg.Export.Artifacts.PutArtifact(finalCtx, cfg.CaptureID, "sessions/"+uuid.NewString()+".json", data, "application/json"); err != nil && runErr == nil {
		runErr = err
	}
	return runErr
}

func healthServer(s *stats) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if s.ready.Load() == 0 {
			http.Error(w, "waiting for an attached eCapture producer", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(w, "kapture_ebpf_producers_ready %d\nkapture_ebpf_observations_total %d\nkapture_ebpf_loss_intervals_total %d\nkapture_ebpf_unattributed_total %d\nkapture_ebpf_exchanges_total %d\nkapture_ebpf_replay_requests_total %d\nkapture_ebpf_retained_bytes %d\nkapture_ebpf_parser_bytes %d\n", s.ready.Load(), s.events.Load(), s.loss.Load(), s.unattributed.Load(), s.exchanges.Load(), s.requests.Load(), s.retained.Load(), s.memory.Load())
		fmt.Fprintf(w, "kapture_ebpf_decode_errors_total %d\n", s.decodeErrors.Load())
	})
	return &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
}

var _ storage.ArtifactWriter = (*storage.EFSWriterFactory)(nil)
