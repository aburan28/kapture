package replay

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/kapture-io/kapture/internal/storage"
)

// Engine reads captured requests from storage and feeds them through
// loaded replay plugins.
type Engine struct {
	reader  storage.ReaderFactory
	plugins []ReplayPlugin
	log     *slog.Logger

	processed atomic.Int64
	succeeded atomic.Int64
	failed    atomic.Int64
	skipped   atomic.Int64
}

// EngineConfig configures the replay engine.
type EngineConfig struct {
	// ReaderFactory provides access to captured request data.
	ReaderFactory storage.ReaderFactory

	// Plugins are the loaded replay plugins to process requests through.
	Plugins []ReplayPlugin

	// Logger is the structured logger. Defaults to slog.Default().
	Logger *slog.Logger
}

// NewEngine creates a replay engine with the given configuration.
func NewEngine(cfg EngineConfig) (*Engine, error) {
	if cfg.ReaderFactory == nil {
		return nil, fmt.Errorf("reader factory is required")
	}
	if len(cfg.Plugins) == 0 {
		return nil, fmt.Errorf("at least one plugin is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &Engine{
		reader:  cfg.ReaderFactory,
		plugins: cfg.Plugins,
		log:     cfg.Logger,
	}, nil
}

// storageAccessor implements StorageAccessor by wrapping a ReaderFactory.
type storageAccessor struct {
	factory storage.ReaderFactory
}

func (s *storageAccessor) ReadCapture(ctx context.Context, captureID string) (storage.Reader, error) {
	return s.factory.NewReader(ctx, captureID)
}

// InitPlugins initializes all plugins with the given configuration.
func (e *Engine) InitPlugins(ctx context.Context, targetURL string, settings map[string]map[string]string) error {
	accessor := &storageAccessor{factory: e.reader}

	for _, p := range e.plugins {
		pluginSettings := settings[p.Name()]
		if pluginSettings == nil {
			pluginSettings = make(map[string]string)
		}

		cfg := PluginConfig{
			Settings:  pluginSettings,
			Storage:   accessor,
			TargetURL: targetURL,
		}

		if err := p.Init(ctx, cfg); err != nil {
			return fmt.Errorf("init plugin %q: %w", p.Name(), err)
		}

		e.log.Info("initialized plugin", "plugin", p.Name())
	}

	return nil
}

// Replay reads all captured requests for the given capture ID and processes
// them through all loaded plugins sequentially.
func (e *Engine) Replay(ctx context.Context, captureID string) (*ReplaySummary, error) {
	reader, err := e.reader.NewReader(ctx, captureID)
	if err != nil {
		return nil, fmt.Errorf("create reader for capture %q: %w", captureID, err)
	}
	defer reader.Close()

	e.log.Info("starting replay", "captureID", captureID, "plugins", pluginNames(e.plugins))

	var sequence uint64
	var results []*PluginResult

	for req, readErr := range reader.Read(ctx) {
		if readErr != nil {
			e.log.Warn("error reading captured request", "error", readErr, "captureID", captureID)
			continue
		}

		replayReq := &ReplayRequest{
			Original:  req,
			Sequence:  sequence,
			CaptureID: captureID,
		}

		for _, p := range e.plugins {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}

			result, err := p.ProcessRequest(ctx, replayReq)
			if err != nil {
				e.failed.Add(1)
				e.log.Warn("plugin error processing request",
					"plugin", p.Name(),
					"requestID", req.ID,
					"error", err,
				)
				continue
			}

			e.processed.Add(1)
			switch result.Status {
			case ReplayStatusSuccess:
				e.succeeded.Add(1)
			case ReplayStatusError:
				e.failed.Add(1)
			case ReplayStatusSkipped:
				e.skipped.Add(1)
			}

			results = append(results, &PluginResult{
				PluginName: p.Name(),
				Result:     result,
			})
		}

		sequence++
	}

	summary := &ReplaySummary{
		CaptureID:      captureID,
		TotalRequests:  sequence,
		PluginResults:  results,
		TotalProcessed: e.processed.Load(),
		TotalSucceeded: e.succeeded.Load(),
		TotalFailed:    e.failed.Load(),
		TotalSkipped:   e.skipped.Load(),
	}

	e.log.Info("replay complete",
		"captureID", captureID,
		"requests", sequence,
		"processed", summary.TotalProcessed,
		"succeeded", summary.TotalSucceeded,
		"failed", summary.TotalFailed,
		"skipped", summary.TotalSkipped,
	)

	return summary, nil
}

// Close shuts down all plugins.
func (e *Engine) Close() error {
	var mu sync.Mutex
	var errs []error

	for _, p := range e.plugins {
		if err := p.Close(); err != nil {
			mu.Lock()
			errs = append(errs, fmt.Errorf("close plugin %q: %w", p.Name(), err))
			mu.Unlock()
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing plugins: %v", errs)
	}
	return nil
}

// Metrics returns current engine metrics.
func (e *Engine) Metrics() (processed, succeeded, failed, skipped int64) {
	return e.processed.Load(), e.succeeded.Load(), e.failed.Load(), e.skipped.Load()
}

// PluginResult pairs a plugin name with its result for a single request.
type PluginResult struct {
	PluginName string
	Result     *ReplayResult
}

// ReplaySummary holds aggregate results for a complete replay session.
type ReplaySummary struct {
	CaptureID      string
	TotalRequests  uint64
	PluginResults  []*PluginResult
	TotalProcessed int64
	TotalSucceeded int64
	TotalFailed    int64
	TotalSkipped   int64
}

func pluginNames(plugins []ReplayPlugin) []string {
	names := make([]string, len(plugins))
	for i, p := range plugins {
		names[i] = p.Name()
	}
	return names
}
