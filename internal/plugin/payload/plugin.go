// Package payload provides an extensible plugin model for processing captured
// traffic payloads. Plugins form a middleware pipeline between the capture agent
// and storage, enabling transformation, filtering, enrichment, and observation
// of captured requests without modifying core capture logic.
//
// # Interface Design
//
// The plugin model uses a chain-of-responsibility pattern. Each plugin receives
// a CapturedRequest and returns either:
//   - A (possibly modified) request to pass downstream
//   - nil to filter/drop the request from the pipeline
//
// Plugins are composable: multiple plugins execute in registration order,
// and the output of one becomes the input to the next.
//
// # Usage
//
//	pipeline := payload.NewPipeline(storageWriter,
//	    redactPlugin,
//	    metricsPlugin,
//	    filterPlugin,
//	)
//	// pipeline implements storage.Writer — drop-in replacement
//	err := pipeline.Write(ctx, capturedReq)
package payload

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/kapture-io/kapture/internal/plugin/validation"
	"github.com/kapture-io/kapture/internal/storage"
)

// Plugin processes captured requests inline in the capture pipeline.
// Implementations must be safe for concurrent use.
type Plugin interface {
	// Name returns a unique, human-readable identifier for this plugin.
	Name() string

	// Init initializes the plugin with arbitrary configuration.
	// Called once before any calls to Process.
	Init(ctx context.Context, config map[string]any) error

	// Process handles a single captured request. Implementations may:
	//   - Return the request unmodified (passthrough/observation)
	//   - Return a modified copy (transformation/enrichment)
	//   - Return (nil, nil) to drop the request (filtering)
	//   - Return (nil, error) to signal a processing failure
	//
	// Plugins MUST NOT mutate the input request directly. Return a copy if
	// modifications are needed. This ensures earlier pipeline stages and
	// concurrent readers are not affected.
	Process(ctx context.Context, req *storage.CapturedRequest) (*storage.CapturedRequest, error)

	// Close releases any resources held by the plugin.
	// Called once during shutdown, after all Process calls complete.
	Close() error
}

// ErrorHandler defines how the pipeline handles plugin errors.
type ErrorHandler int

const (
	// ErrorHandlerDrop drops the request on plugin error and continues.
	ErrorHandlerDrop ErrorHandler = iota
	// ErrorHandlerSkip skips the failing plugin and passes the request to the next.
	ErrorHandlerSkip
	// ErrorHandlerAbort stops processing and returns the error to the caller.
	ErrorHandlerAbort
)

// PipelineConfig configures a Pipeline.
type PipelineConfig struct {
	// Downstream is the storage writer that receives processed requests.
	Downstream storage.Writer

	// Plugins are executed in order for each captured request.
	Plugins []Plugin

	// OnError controls behavior when a plugin returns an error.
	// Defaults to ErrorHandlerDrop.
	OnError ErrorHandler

	// Logger for pipeline operations. Uses slog.Default() if nil.
	Logger *slog.Logger
}

// Pipeline chains payload plugins before a downstream storage.Writer.
// It implements storage.Writer so it can be used as a drop-in replacement
// anywhere a Writer is expected.
type Pipeline struct {
	downstream storage.Writer
	plugins    []Plugin
	onError    ErrorHandler
	log        *slog.Logger

	mu     sync.RWMutex
	closed bool
}

// Compile-time check: Pipeline implements storage.Writer.
var _ storage.Writer = (*Pipeline)(nil)

// NewPipeline creates a Pipeline from the given config.
func NewPipeline(cfg PipelineConfig) (*Pipeline, error) {
	if cfg.Downstream == nil {
		return nil, errors.New("downstream writer is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Pipeline{
		downstream: cfg.Downstream,
		plugins:    cfg.Plugins,
		onError:    cfg.OnError,
		log:        cfg.Logger,
	}, nil
}

// Write runs the request through each plugin in order, then writes the
// result to the downstream writer. If any plugin filters the request
// (returns nil), writing is skipped.
func (p *Pipeline) Write(ctx context.Context, req *storage.CapturedRequest) error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return storage.ErrWriterClosed
	}
	p.mu.RUnlock()

	current := req
	for _, pl := range p.plugins {
		result, err := pl.Process(ctx, current)
		if err != nil {
			switch p.onError {
			case ErrorHandlerAbort:
				return fmt.Errorf("plugin %q: %w", pl.Name(), err)
			case ErrorHandlerSkip:
				p.log.Warn("plugin error, skipping plugin",
					"plugin", pl.Name(), "error", err, "path", req.Path)
				continue
			default: // ErrorHandlerDrop
				p.log.Warn("plugin error, dropping request",
					"plugin", pl.Name(), "error", err, "path", req.Path)
				return nil
			}
		}
		if result == nil {
			p.log.Debug("request filtered by plugin",
				"plugin", pl.Name(), "path", req.Path)
			return nil
		}
		current = result
	}

	return p.downstream.Write(ctx, current)
}

// Flush delegates to the downstream writer.
func (p *Pipeline) Flush(ctx context.Context) error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return storage.ErrWriterClosed
	}
	p.mu.RUnlock()
	return p.downstream.Flush(ctx)
}

// Close shuts down all plugins (in reverse order) then closes the downstream writer.
func (p *Pipeline) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()

	var errs []error
	// Close plugins in reverse order (last registered, first closed).
	for i := len(p.plugins) - 1; i >= 0; i-- {
		if err := p.plugins[i].Close(); err != nil {
			errs = append(errs, fmt.Errorf("close plugin %q: %w", p.plugins[i].Name(), err))
		}
	}
	if err := p.downstream.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close downstream: %w", err))
	}
	return errors.Join(errs...)
}

// Registry is a named collection of plugin constructors. It allows plugins
// to be registered by name and instantiated from configuration at runtime.
// It embeds a [validation.Registry] for cross-plugin constraint validation.
type Registry struct {
	mu         sync.RWMutex
	builders   map[string]Builder
	Validation *validation.Registry
}

// Builder constructs a Plugin from configuration. This is the function
// signature that plugin authors implement and register.
type Builder func(ctx context.Context, config map[string]any) (Plugin, error)

// NewRegistry creates an empty plugin registry with an embedded validation registry.
func NewRegistry() *Registry {
	return &Registry{
		builders:   make(map[string]Builder),
		Validation: validation.NewRegistry(),
	}
}

// Register adds a plugin builder under the given name.
// Returns an error if the name is already registered.
func (r *Registry) Register(name string, builder Builder) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.builders[name]; exists {
		return fmt.Errorf("payload plugin %q is already registered", name)
	}
	r.builders[name] = builder
	return nil
}

// RegisterWithConstraints adds a plugin builder and its validation constraints.
// This is the preferred registration method for plugins that declare cross-cutting
// concerns (conflicts, ordering, resource claims).
func (r *Registry) RegisterWithConstraints(name string, builder Builder, constraints *validation.Constraints) error {
	if err := r.Register(name, builder); err != nil {
		return err
	}
	if err := r.Validation.Register(name, constraints); err != nil {
		// Roll back the builder registration on constraint failure.
		r.mu.Lock()
		delete(r.builders, name)
		r.mu.Unlock()
		return err
	}
	return nil
}

// ValidatePipeline checks an ordered list of plugin references against all
// registered constraints. Returns nil if the pipeline is valid.
func (r *Registry) ValidatePipeline(refs []validation.PluginRef) []*validation.ValidationError {
	return r.Validation.ValidatePipeline(refs)
}

// Build creates a plugin instance by name with the given config.
func (r *Registry) Build(ctx context.Context, name string, config map[string]any) (Plugin, error) {
	r.mu.RLock()
	builder, ok := r.builders[name]
	r.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unknown payload plugin %q", name)
	}
	plugin, err := builder(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("build payload plugin %q: %w", name, err)
	}
	return plugin, nil
}

// Names returns all registered plugin names.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.builders))
	for name := range r.builders {
		names = append(names, name)
	}
	return names
}

// CopyRequest creates a shallow copy of a CapturedRequest with deep-copied
// mutable fields (Headers, Metadata). Use this in plugin implementations
// when you need to modify a request without affecting the original.
func CopyRequest(req *storage.CapturedRequest) *storage.CapturedRequest {
	if req == nil {
		return nil
	}
	cp := *req

	if req.Headers != nil {
		cp.Headers = make(map[string][]string, len(req.Headers))
		for k, v := range req.Headers {
			vals := make([]string, len(v))
			copy(vals, v)
			cp.Headers[k] = vals
		}
	}

	if req.Body != nil {
		cp.Body = make([]byte, len(req.Body))
		copy(cp.Body, req.Body)
	}

	if req.Metadata != nil {
		cp.Metadata = make(map[string]string, len(req.Metadata))
		for k, v := range req.Metadata {
			cp.Metadata[k] = v
		}
	}

	return &cp
}
