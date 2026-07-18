package replayengine

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	sdk "github.com/kapture-io/kapture/pkg/replayengine"
)

// reloadDebounce coalesces the burst of fsnotify events a plugin update
// produces (write, chmod, rename) into one reload.
const reloadDebounce = 500 * time.Millisecond

// Manager discovers replay-engine plugins in a directory, launches them as
// gRPC subprocesses on demand, and hot-reloads them when their binaries
// change on disk.
//
// Discovery convention: an engine named "k6" is the executable
// "kapture-engine-k6" inside PluginDir.
//
// Hot reload semantics: when a plugin binary is replaced (the usual flow is
// a sidecar or operator writing a new binary into the shared plugin
// volume), the manager launches the new binary, swaps it in for subsequent
// Acquire calls, and asks the old process to drain before closing it. Runs
// already executing on the old process finish there; new runs get the new
// engine. There is no in-run handover.
type Manager struct {
	PluginDir string
	Log       *slog.Logger

	// DrainGrace is how long a replaced engine gets to finish in-flight
	// work before its process is closed.
	DrainGrace time.Duration

	mu      sync.Mutex
	engines map[string]*SubprocessEngine

	watcher   *fsnotify.Watcher
	watchStop chan struct{}
	pending   map[string]*time.Timer // engine name -> debounce timer
}

// NewManager creates a Manager over the given plugin directory.
func NewManager(pluginDir string, log *slog.Logger) *Manager {
	if log == nil {
		log = slog.Default()
	}
	return &Manager{
		PluginDir:  pluginDir,
		Log:        log.With("component", "engine-manager"),
		DrainGrace: 30 * time.Second,
		engines:    make(map[string]*SubprocessEngine),
		pending:    make(map[string]*time.Timer),
	}
}

// PluginPath returns the expected binary path for an engine name.
func (m *Manager) PluginPath(name string) string {
	return filepath.Join(m.PluginDir, sdk.PluginBinaryPrefix+name)
}

// Acquire returns a running engine for name, launching it on first use.
func (m *Manager) Acquire(ctx context.Context, name string) (*SubprocessEngine, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if engine, ok := m.engines[name]; ok {
		return engine, nil
	}

	path := m.PluginPath(name)
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("engine plugin %q not found at %s: %w", name, path, err)
	}

	engine, err := Launch(ctx, path, m.Log)
	if err != nil {
		return nil, err
	}
	if engine.Info.Name != name {
		engine.Close()
		return nil, fmt.Errorf("plugin %s identifies as %q, expected %q", path, engine.Info.Name, name)
	}

	m.engines[name] = engine
	return engine, nil
}

// Reload replaces the running engine for name with a freshly launched
// process from the (possibly updated) plugin binary. The old process is
// drained and closed in the background. Reload of an engine that is not
// currently running is a no-op: the next Acquire launches the new binary
// anyway.
func (m *Manager) Reload(ctx context.Context, name string) error {
	m.mu.Lock()
	old, running := m.engines[name]
	if !running {
		m.mu.Unlock()
		return nil
	}

	fresh, err := Launch(ctx, m.PluginPath(name), m.Log)
	if err != nil {
		m.mu.Unlock()
		return fmt.Errorf("hot reload of engine %q failed, keeping old process: %w", name, err)
	}
	if fresh.Info.Name != name {
		fresh.Close()
		m.mu.Unlock()
		return fmt.Errorf("reloaded plugin identifies as %q, expected %q", fresh.Info.Name, name)
	}
	m.engines[name] = fresh
	m.mu.Unlock()

	m.Log.Info("engine hot-reloaded",
		"engine", name, "oldVersion", old.Info.Version, "newVersion", fresh.Info.Version)

	// Drain and close the old process off the critical path.
	grace := m.DrainGrace
	go func() {
		drainCtx, cancel := context.WithTimeout(context.Background(), grace)
		defer cancel()
		_ = old.Drain(drainCtx, grace)
		<-drainCtx.Done()
		old.Close()
	}()

	return nil
}

// Watch starts watching the plugin directory and hot-reloads engines whose
// binaries change. Returns immediately; call Close to stop.
func (m *Manager) Watch(ctx context.Context) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("create plugin watcher: %w", err)
	}
	if err := watcher.Add(m.PluginDir); err != nil {
		watcher.Close()
		return fmt.Errorf("watch plugin dir %s: %w", m.PluginDir, err)
	}

	m.mu.Lock()
	m.watcher = watcher
	m.watchStop = make(chan struct{})
	stop := m.watchStop
	m.mu.Unlock()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename) == 0 {
					continue
				}
				base := filepath.Base(event.Name)
				if len(base) <= len(sdk.PluginBinaryPrefix) || base[:len(sdk.PluginBinaryPrefix)] != sdk.PluginBinaryPrefix {
					continue
				}
				name := base[len(sdk.PluginBinaryPrefix):]
				m.scheduleReload(ctx, name)
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				m.Log.Error("plugin watcher error", "error", err)
			}
		}
	}()

	m.Log.Info("watching plugin directory for hot reload", "dir", m.PluginDir)
	return nil
}

// scheduleReload debounces reloads per engine.
func (m *Manager) scheduleReload(ctx context.Context, name string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if timer, ok := m.pending[name]; ok {
		timer.Stop()
	}
	m.pending[name] = time.AfterFunc(reloadDebounce, func() {
		m.mu.Lock()
		delete(m.pending, name)
		m.mu.Unlock()
		if err := m.Reload(ctx, name); err != nil {
			m.Log.Error("hot reload failed", "engine", name, "error", err)
		}
	})
}

// Close stops the watcher and shuts down every running engine.
func (m *Manager) Close() {
	m.mu.Lock()
	if m.watchStop != nil {
		close(m.watchStop)
		m.watchStop = nil
	}
	if m.watcher != nil {
		m.watcher.Close()
		m.watcher = nil
	}
	for _, timer := range m.pending {
		timer.Stop()
	}
	engines := m.engines
	m.engines = make(map[string]*SubprocessEngine)
	m.mu.Unlock()

	for _, engine := range engines {
		engine.Close()
	}
}
