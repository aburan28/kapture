package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kapture-io/kapture/internal/storage"
)

// FeedServer exposes captured requests over HTTP so external load generators
// (k6, Locust, Gatling, etc.) can consume them. It maintains a sliding window
// buffer and paces requests according to the original capture timing.
//
// Endpoints:
//
//	GET /next          — returns the next request as JSON (blocks until available)
//	GET /batch?n=100   — returns up to N requests as a JSON array
//	GET /status        — returns current replay progress
//	POST /ack          — acknowledges a batch was sent (for checkpointing)
type FeedServer struct {
	reader    Reader
	readOpts  ReadOptions
	rateMode  RateMode
	timeScale float64
	log       *slog.Logger
	addr      string

	// Buffer for prefetched requests.
	bufferSize int
	reqCh      chan *storage.CapturedRequest
	readErr    atomic.Value // stores error
	readerDone atomic.Bool  // set when readLoop exits (success or error)

	// Progress tracking.
	served    atomic.Int64
	acked     atomic.Int64
	startTime time.Time

	server   *http.Server
	cancelFn context.CancelFunc // cancels the internal context to unblock reader
	done     chan struct{}
	once     sync.Once
}

// FeedServerConfig configures the FeedServer.
type FeedServerConfig struct {
	Reader     Reader
	ReadOpts   ReadOptions
	RateMode   RateMode
	TimeScale  float64
	Addr       string // listen address, default ":6565"
	BufferSize int    // prefetch buffer, default 1000
	Logger     *slog.Logger
}

// NewFeedServer creates an HTTP server that feeds requests to external consumers.
// Only RateModeOriginalTiming and RateModeUnlimited are supported; constant-rate
// pacing should be handled by the external load generator (e.g., k6 VU count).
// The zero value of RateMode (RateModeConstant) is treated as RateModeUnlimited.
func NewFeedServer(cfg FeedServerConfig) (*FeedServer, error) {
	if cfg.Reader == nil {
		return nil, fmt.Errorf("reader is required")
	}
	// The zero value of RateMode is RateModeConstant (iota=0). Treat it as
	// unlimited when no explicit mode is set. Only reject explicit constant-rate
	// configuration (RateModeConstant with a non-zero RatePerSecond would be
	// set via the Engine, not the FeedServer).
	if cfg.RateMode == RateModeConstant {
		cfg.RateMode = RateModeUnlimited
	}
	if cfg.Addr == "" {
		cfg.Addr = ":6565"
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 1000
	}
	if cfg.TimeScale <= 0 {
		cfg.TimeScale = 1.0
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &FeedServer{
		reader:     cfg.Reader,
		readOpts:   cfg.ReadOpts,
		rateMode:   cfg.RateMode,
		timeScale:  cfg.TimeScale,
		log:        cfg.Logger,
		addr:       cfg.Addr,
		bufferSize: cfg.BufferSize,
		reqCh:      make(chan *storage.CapturedRequest, cfg.BufferSize),
		done:       make(chan struct{}),
	}, nil
}

// Start opens the reader and begins serving requests.
func (s *FeedServer) Start(ctx context.Context) error {
	if err := s.reader.Open(ctx, s.readOpts); err != nil {
		return fmt.Errorf("open reader: %w", err)
	}

	s.startTime = time.Now()

	// Create an internal cancellable context so Stop() can unblock the reader.
	innerCtx, cancel := context.WithCancel(ctx)
	s.cancelFn = cancel

	// Start background reader goroutine.
	go s.readLoop(innerCtx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /next", s.handleNext)
	mux.HandleFunc("GET /batch", s.handleBatch)
	mux.HandleFunc("GET /status", s.handleStatus)
	mux.HandleFunc("POST /ack", s.handleAck)

	s.server = &http.Server{
		Addr:    s.addr,
		Handler: mux,
	}

	s.log.Info("feed server starting", "addr", s.addr)
	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.log.Error("feed server error", "error", err)
		}
	}()

	return nil
}

// Stop gracefully shuts down the feed server and closes the reader.
func (s *FeedServer) Stop(ctx context.Context) error {
	s.once.Do(func() {
		close(s.done)
		// Cancel the internal context to unblock reader.Next() if it's in-flight.
		if s.cancelFn != nil {
			s.cancelFn()
		}
	})

	var serverErr error
	if s.server != nil {
		serverErr = s.server.Shutdown(ctx)
	}

	// Close the reader to release storage resources (open files, S3/GCS bodies).
	if err := s.reader.Close(); err != nil {
		s.log.Warn("reader close error", "error", err)
	}

	return serverErr
}

func (s *FeedServer) readLoop(ctx context.Context) {
	defer func() {
		s.readerDone.Store(true)
		close(s.reqCh)
	}()

	var prevTimestamp time.Time
	var delayTimer *time.Timer

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		default:
		}

		req, err := s.reader.Next(ctx)
		if err != nil {
			if err != ErrReaderDone {
				s.readErr.Store(err)
			}
			return
		}

		// Apply original timing delay if configured, using a reusable timer
		// to avoid allocating a new one per request.
		if s.rateMode == RateModeOriginalTiming && !prevTimestamp.IsZero() && !req.Timestamp.IsZero() {
			delay := req.Timestamp.Sub(prevTimestamp)
			if delay > 0 {
				scaledDelay := time.Duration(float64(delay) * s.timeScale)
				if delayTimer == nil {
					delayTimer = time.NewTimer(scaledDelay)
				} else {
					delayTimer.Reset(scaledDelay)
				}
				select {
				case <-ctx.Done():
					delayTimer.Stop()
					return
				case <-s.done:
					delayTimer.Stop()
					return
				case <-delayTimer.C:
				}
			}
		}
		prevTimestamp = req.Timestamp

		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		case s.reqCh <- req:
		}
	}
}

func (s *FeedServer) handleNext(w http.ResponseWriter, r *http.Request) {
	// Use select to respect client cancellation and avoid consuming a request
	// that can't be delivered (the client may have disconnected).
	select {
	case req, ok := <-s.reqCh:
		if !ok {
			if errVal := s.readErr.Load(); errVal != nil {
				http.Error(w, fmt.Sprintf("reader error: %v", errVal), http.StatusInternalServerError)
				return
			}
			http.Error(w, "replay complete", http.StatusGone)
			return
		}
		s.served.Add(1)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(req)
	case <-r.Context().Done():
		http.Error(w, "client disconnected", http.StatusRequestTimeout)
	}
}

func (s *FeedServer) handleBatch(w http.ResponseWriter, r *http.Request) {
	n := 100
	if q := r.URL.Query().Get("n"); q != "" {
		if parsed, err := strconv.Atoi(q); err == nil {
			n = parsed
		}
	}
	if n <= 0 {
		n = 1
	}
	if n > 1000 {
		n = 1000
	}

	batch := s.collectBatch(n)
	s.served.Add(int64(len(batch)))

	if len(batch) == 0 {
		if errVal := s.readErr.Load(); errVal != nil {
			http.Error(w, fmt.Sprintf("reader error: %v", errVal), http.StatusInternalServerError)
			return
		}
		http.Error(w, "replay complete", http.StatusGone)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(batch)
}

// collectBatch reads up to n requests from the channel. It blocks for the
// first request but returns immediately if the buffer is empty after that.
func (s *FeedServer) collectBatch(n int) []*storage.CapturedRequest {
	batch := make([]*storage.CapturedRequest, 0, n)
	for i := 0; i < n; i++ {
		if i == 0 {
			// First request: block until available.
			req, ok := <-s.reqCh
			if !ok {
				return batch
			}
			batch = append(batch, req)
		} else {
			// Subsequent requests: non-blocking drain.
			select {
			case req, ok := <-s.reqCh:
				if !ok {
					return batch
				}
				batch = append(batch, req)
			default:
				return batch
			}
		}
	}
	return batch
}

type feedStatus struct {
	Served      int64         `json:"served"`
	Acked       int64         `json:"acked"`
	BufferLen   int           `json:"bufferLen"`
	BufferCap   int           `json:"bufferCap"`
	Elapsed     time.Duration `json:"elapsed"`
	CurrentRPS  float64       `json:"currentRps"`
	ReaderDone  bool          `json:"readerDone"`
	ReaderError string        `json:"readerError,omitempty"`
}

func (s *FeedServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	elapsed := time.Since(s.startTime)
	served := s.served.Load()

	status := feedStatus{
		Served:     served,
		Acked:      s.acked.Load(),
		BufferLen:  len(s.reqCh),
		BufferCap:  cap(s.reqCh),
		Elapsed:    elapsed,
		ReaderDone: s.readerDone.Load(),
	}

	if elapsed > 0 {
		status.CurrentRPS = float64(served) / elapsed.Seconds()
	}

	if errVal := s.readErr.Load(); errVal != nil {
		status.ReaderError = fmt.Sprintf("%v", errVal)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (s *FeedServer) handleAck(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Count int64 `json:"count"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if body.Count < 0 {
		http.Error(w, "count must be >= 0", http.StatusBadRequest)
		return
	}
	s.acked.Add(body.Count)
	w.WriteHeader(http.StatusOK)
}
