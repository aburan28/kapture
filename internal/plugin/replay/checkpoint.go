package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Checkpoint records replay progress so a multi-day session can be resumed
// after a crash, restart, or intentional pause. It is written atomically to
// a local file at configurable intervals.
type Checkpoint struct {
	// ReplayID uniquely identifies this replay session.
	ReplayID string `json:"replayId"`

	// CaptureID is the source capture being replayed.
	CaptureID string `json:"captureId"`

	// LastObjectKey is the storage key of the last fully consumed object.
	// On resume, reading starts from the object after this one.
	LastObjectKey string `json:"lastObjectKey"`

	// LastRequestID is the ID of the last successfully sent request
	// within the current object.
	LastRequestID string `json:"lastRequestId"`

	// RequestsSent is the cumulative count of successfully sent requests.
	RequestsSent int64 `json:"requestsSent"`

	// RequestsErrored is the cumulative count of failed requests.
	RequestsErrored int64 `json:"requestsErrored"`

	// RequestsFiltered is the cumulative count of filtered/skipped requests.
	RequestsFiltered int64 `json:"requestsFiltered"`

	// BytesProcessed is the approximate total bytes read from storage.
	BytesProcessed int64 `json:"bytesProcessed"`

	// StartTime is when the replay session originally started.
	StartTime time.Time `json:"startTime"`

	// LastUpdate is when this checkpoint was last written.
	LastUpdate time.Time `json:"lastUpdate"`

	// ElapsedDuration is the cumulative wall-clock time spent replaying
	// (excludes paused/stopped periods).
	ElapsedDuration time.Duration `json:"elapsedDuration"`

	// EngineConfig captures key engine parameters for validation on resume.
	EngineParams CheckpointEngineParams `json:"engineParams"`
}

// CheckpointEngineParams stores engine configuration for resume validation.
type CheckpointEngineParams struct {
	RateMode      RateMode `json:"rateMode"`
	RatePerSecond float64  `json:"ratePerSecond,omitempty"`
	TimeScale     float64  `json:"timeScale,omitempty"`
	Concurrency   int      `json:"concurrency"`
	StorageType   string   `json:"storageType"`
}

// CheckpointerConfig configures a Checkpointer.
type CheckpointerConfig struct {
	// Dir is the directory where checkpoint files are stored.
	Dir string

	// Interval is how often checkpoints are flushed to disk.
	// Defaults to 30 seconds.
	Interval time.Duration

	// Logger for checkpoint operations.
	Logger *slog.Logger
}

// Checkpointer manages reading and writing replay checkpoints.
// It is safe for concurrent use.
type Checkpointer struct {
	dir      string
	interval time.Duration
	log      *slog.Logger

	mu         sync.Mutex
	current    *Checkpoint
	dirty      bool
	stopCh     chan struct{}
	stoppedCh  chan struct{}
}

// NewCheckpointer creates a Checkpointer that persists progress to disk.
func NewCheckpointer(cfg CheckpointerConfig) (*Checkpointer, error) {
	if cfg.Dir == "" {
		return nil, fmt.Errorf("checkpoint directory is required")
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("create checkpoint dir: %w", err)
	}
	return &Checkpointer{
		dir:      cfg.Dir,
		interval: cfg.Interval,
		log:      cfg.Logger,
	}, nil
}

// Load reads a checkpoint for the given replay ID. Returns nil if no
// checkpoint exists.
func (c *Checkpointer) Load(replayID string) (*Checkpoint, error) {
	path := c.checkpointPath(replayID)
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read checkpoint: %w", err)
	}

	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("unmarshal checkpoint: %w", err)
	}

	c.log.Info("loaded checkpoint",
		"replayId", cp.ReplayID,
		"lastObject", cp.LastObjectKey,
		"requestsSent", cp.RequestsSent,
		"lastUpdate", cp.LastUpdate)
	return &cp, nil
}

// Start begins periodic checkpoint flushing. Call Update() to record
// progress; the checkpointer will write to disk at the configured interval.
func (c *Checkpointer) Start(initial *Checkpoint) {
	c.mu.Lock()
	c.current = initial
	c.stopCh = make(chan struct{})
	c.stoppedCh = make(chan struct{})
	c.mu.Unlock()

	go c.flushLoop()
}

// Update records new progress. The checkpoint is written to disk on the
// next flush interval.
func (c *Checkpointer) Update(fn func(cp *Checkpoint)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.current != nil {
		fn(c.current)
		c.current.LastUpdate = time.Now().UTC()
		c.dirty = true
	}
}

// Stop flushes any pending checkpoint and stops the periodic flusher.
func (c *Checkpointer) Stop() error {
	c.mu.Lock()
	if c.stopCh == nil {
		c.mu.Unlock()
		return nil
	}
	close(c.stopCh)
	stoppedCh := c.stoppedCh
	c.mu.Unlock()

	<-stoppedCh

	// Final flush.
	return c.flush()
}

// Delete removes the checkpoint file for a completed replay.
func (c *Checkpointer) Delete(replayID string) error {
	path := c.checkpointPath(replayID)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (c *Checkpointer) flushLoop() {
	defer close(c.stoppedCh)
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			if err := c.flush(); err != nil {
				c.log.Warn("checkpoint flush failed", "error", err)
			}
		}
	}
}

func (c *Checkpointer) flush() error {
	c.mu.Lock()
	if !c.dirty || c.current == nil {
		c.mu.Unlock()
		return nil
	}
	// Copy checkpoint under lock.
	cp := *c.current
	c.dirty = false
	c.mu.Unlock()

	return c.writeCheckpoint(&cp)
}

func (c *Checkpointer) writeCheckpoint(cp *Checkpoint) error {
	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}

	path := c.checkpointPath(cp.ReplayID)
	tmpPath := path + ".tmp"

	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("write checkpoint tmp: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename checkpoint: %w", err)
	}

	c.log.Debug("checkpoint flushed",
		"replayId", cp.ReplayID,
		"requestsSent", cp.RequestsSent,
		"lastObject", cp.LastObjectKey)
	return nil
}

func (c *Checkpointer) checkpointPath(replayID string) string {
	return filepath.Join(c.dir, fmt.Sprintf("replay-%s.checkpoint.json", replayID))
}

// CheckpointResultHandler wraps a ResultHandler and updates the Checkpointer
// on every successful result. This integrates checkpointing into the replay
// engine without modifying engine code.
type CheckpointResultHandler struct {
	inner        ResultHandler
	checkpointer *Checkpointer
}

// NewCheckpointResultHandler wraps an inner ResultHandler to also track progress.
func NewCheckpointResultHandler(inner ResultHandler, checkpointer *Checkpointer) *CheckpointResultHandler {
	return &CheckpointResultHandler{inner: inner, checkpointer: checkpointer}
}

func (h *CheckpointResultHandler) HandleResult(ctx context.Context, result *Result) error {
	if err := h.inner.HandleResult(ctx, result); err != nil {
		return err
	}

	h.checkpointer.Update(func(cp *Checkpoint) {
		if result.Error != nil {
			cp.RequestsErrored++
		} else {
			cp.RequestsSent++
			cp.LastRequestID = result.RequestID
		}
	})

	return nil
}

func (h *CheckpointResultHandler) Summarize(ctx context.Context) (*Summary, error) {
	return h.inner.Summarize(ctx)
}
