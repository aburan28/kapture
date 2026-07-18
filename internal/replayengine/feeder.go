package replayengine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/kapture-io/kapture/internal/plugin/replay"
	"github.com/kapture-io/kapture/internal/storage"
	replayenginev1 "github.com/kapture-io/kapture/proto/replayengine/v1"
)

// FeederConfig configures a streaming run through an external engine.
type FeederConfig struct {
	// Reader streams captured requests from the storage backend. Readers
	// are already incremental (object-by-object, line-by-line); the
	// feeder preserves that property end to end.
	Reader replay.Reader

	// ReadOptions select the capture slice (capture ID, time range,
	// filters, limit).
	ReadOptions replay.ReadOptions

	// ShardIndex/ShardCount restrict the feed to one deterministic slice
	// of the capture. ShardCount <= 1 disables sharding.
	ShardIndex int
	ShardCount int

	// RateMode/RatePerSecond/TimeScale pace the feed. Ignored when the
	// engine advertises the self-paced capability — then items are pushed
	// as fast as gRPC flow control allows and pacing is the engine's job.
	RateMode      replay.RateMode
	RatePerSecond float64
	TimeScale     float64

	Logger *slog.Logger
}

// Feeder drives one replay run through a SubprocessEngine: it streams the
// shard's slice of the capture into the engine's Execute stream and folds
// the engine's events into a RunReport.
//
// Memory stays bounded no matter how large the capture is: the reader
// yields one request at a time, and stream.Send blocks on gRPC flow
// control when the engine falls behind, which in turn stops the reader.
// Nothing is ever buffered beyond the transport windows and the engine
// SDK's small feed channel.
type Feeder struct {
	cfg FeederConfig
	log *slog.Logger

	fed     atomic.Int64 // items sent to the engine
	skipped atomic.Int64 // items owned by other shards
}

// Fed returns how many items were streamed to the engine.
func (f *Feeder) Fed() int64 { return f.fed.Load() }

// Skipped returns how many items were skipped by the shard filter.
func (f *Feeder) Skipped() int64 { return f.skipped.Load() }

// NewFeeder validates the config and creates a Feeder.
func NewFeeder(cfg FeederConfig) (*Feeder, error) {
	if cfg.Reader == nil {
		return nil, errors.New("reader is required")
	}
	if cfg.ShardCount > 1 && (cfg.ShardIndex < 0 || cfg.ShardIndex >= cfg.ShardCount) {
		return nil, fmt.Errorf("shard index %d out of range [0, %d)", cfg.ShardIndex, cfg.ShardCount)
	}
	if cfg.TimeScale <= 0 {
		cfg.TimeScale = 1.0
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Feeder{cfg: cfg, log: cfg.Logger}, nil
}

// Run executes the replay through the engine and returns the final report.
func (f *Feeder) Run(ctx context.Context, engine *SubprocessEngine) (*replay.RunReport, error) {
	if err := f.cfg.Reader.Open(ctx, f.cfg.ReadOptions); err != nil {
		return nil, fmt.Errorf("open reader: %w", err)
	}

	stream, err := engine.Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("open execute stream: %w", err)
	}

	// Host-side pacing is skipped for self-paced engines.
	pace := !engine.SelfPaced()

	feedErr := make(chan error, 1)
	go func() {
		feedErr <- f.feed(ctx, stream, pace)
	}()

	report, recvErr := f.collect(stream)
	sendErr := <-feedErr

	if recvErr != nil {
		return nil, recvErr
	}
	if sendErr != nil {
		return nil, sendErr
	}
	return report, nil
}

// feed streams the shard's slice into the engine, then half-closes.
func (f *Feeder) feed(ctx context.Context, stream interface {
	Send(*replayenginev1.ExecuteRequest) error
	CloseSend() error
}, pace bool) error {
	defer func() {
		if err := stream.CloseSend(); err != nil {
			f.log.Warn("close send failed", "error", err)
		}
	}()

	var baseTime, prevTimestamp time.Time
	var limiter *time.Ticker
	if pace && f.cfg.RateMode == replay.RateModeConstant && f.cfg.RatePerSecond > 0 {
		limiter = time.NewTicker(time.Duration(float64(time.Second) / f.cfg.RatePerSecond))
		defer limiter.Stop()
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		req, err := f.cfg.Reader.Next(ctx)
		if errors.Is(err, replay.ErrReaderDone) || errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("reader: %w", err)
		}

		// Shard filter first: skipped requests never advance the pacing
		// clock, so each shard reproduces the timeline of its own subset
		// (same semantics as the builtin engine's readLoop).
		if !replay.ShardOwns(req.ID, f.cfg.ShardIndex, f.cfg.ShardCount) {
			f.skipped.Add(1)
			continue
		}

		if baseTime.IsZero() && !req.Timestamp.IsZero() {
			baseTime = req.Timestamp
		}

		if pace {
			switch f.cfg.RateMode {
			case replay.RateModeConstant:
				if limiter != nil {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-limiter.C:
					}
				}
			case replay.RateModeOriginalTiming:
				if !prevTimestamp.IsZero() && !req.Timestamp.IsZero() {
					if delay := req.Timestamp.Sub(prevTimestamp); delay > 0 {
						scaled := time.Duration(float64(delay) * f.cfg.TimeScale)
						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-time.After(scaled):
						}
					}
				}
				prevTimestamp = req.Timestamp
			}
		}

		// Send blocks on gRPC flow control when the engine lags — this is
		// the backpressure that keeps streaming replays bounded.
		if err := stream.Send(&replayenginev1.ExecuteRequest{
			Item: feedItemFromCaptured(req, baseTime),
		}); err != nil {
			return fmt.Errorf("send feed item: %w", err)
		}
		f.fed.Add(1)
	}
}

// collect folds engine events into a RunReport. The engine's final
// RunSummary is authoritative; per-request results and progress events
// keep host-side counters live for progress logging while the run is
// going.
func (f *Feeder) collect(stream interface {
	Recv() (*replayenginev1.ExecuteResponse, error)
}) (*replay.RunReport, error) {
	report := &replay.RunReport{}
	var sawSummary bool
	var sent, failed int64
	lastLog := time.Now()

	for {
		event, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			if !sawSummary {
				return nil, errors.New("engine closed stream without a final RunSummary")
			}
			return report, nil
		}
		if err != nil {
			return nil, fmt.Errorf("receive engine event: %w", err)
		}

		switch ev := event.Event.(type) {
		case *replayenginev1.ExecuteResponse_Result:
			if ev.Result.Error == "" {
				sent++
			} else {
				failed++
			}
		case *replayenginev1.ExecuteResponse_Progress:
			sent = ev.Progress.Sent
			failed = ev.Progress.Failed
		case *replayenginev1.ExecuteResponse_Summary:
			s := ev.Summary
			sawSummary = true
			report.TotalRequests = s.TotalRequests
			report.SentRequests = s.SentRequests
			report.FailedRequests = s.FailedRequests
			report.AchievedRPS = s.AchievedRps
			report.MeanLatencyMs = s.MeanLatencyMs
			report.P50LatencyMs = s.P50LatencyMs
			report.P95LatencyMs = s.P95LatencyMs
			report.P99LatencyMs = s.P99LatencyMs
		}

		if time.Since(lastLog) >= 10*time.Second {
			f.log.Info("replay progress", "sent", sent, "failed", failed)
			lastLog = time.Now()
		}
	}
}

// feedItemFromCaptured converts a stored request into the wire FeedItem.
func feedItemFromCaptured(req *storage.CapturedRequest, baseTime time.Time) *replayenginev1.FeedItem {
	item := &replayenginev1.FeedItem{
		RequestId: req.ID,
		Protocol:  req.Protocol,
		Method:    req.Method,
		Path:      req.Path,
		Body:      req.Body,
		Metadata:  req.Metadata,
	}
	if !req.Timestamp.IsZero() {
		item.CapturedAt = timestamppb.New(req.Timestamp)
		if !baseTime.IsZero() {
			item.SourceOffsetNs = req.Timestamp.Sub(baseTime).Nanoseconds()
		}
	}
	if len(req.Headers) > 0 {
		item.Headers = make(map[string]*replayenginev1.HeaderValues, len(req.Headers))
		for k, vals := range req.Headers {
			item.Headers[k] = &replayenginev1.HeaderValues{Values: vals}
		}
	}
	return item
}
