// Package replay provides a Go plugin system for replaying captured traffic.
//
// Plugin developers build their plugins as Go plugin shared objects (.so files)
// and export a constructor function. The replay engine loads these plugins at
// runtime, reads captured requests from storage, and feeds them through the
// plugin pipeline.
//
// # Stable Plugin Interface
//
// Plugin developers must implement the [ReplayPlugin] interface and export a
// constructor via the [PluginConstructor] function signature. The compiled .so
// file must export a symbol named "NewReplayPlugin" with this signature.
//
// # Storage Interaction
//
// Plugins receive a [StorageAccessor] during initialization, giving them
// read-only access to the capture storage backend. This allows plugins to
// read captured requests directly when they need random access beyond what
// the sequential replay engine provides.
package replay

import (
	"context"
	"net/http"
	"time"

	"github.com/kapture-io/kapture/internal/storage"
)

// PluginConstructor is the function signature that plugin .so files must
// export as the symbol "NewReplayPlugin". The engine calls this to create
// a plugin instance.
type PluginConstructor func() (ReplayPlugin, error)

// ReplayPlugin is the stable interface that Go plugin developers implement
// to process captured traffic for replay. Implementations must be safe for
// concurrent use if the engine is configured with multiple workers.
//
// Lifecycle: NewReplayPlugin() → Init() → [ProcessRequest()...] → Close()
type ReplayPlugin interface {
	// Name returns a human-readable identifier for this plugin.
	// Must be unique across loaded plugins.
	Name() string

	// Init initializes the plugin with runtime configuration and a handle
	// to the storage backend. Called once before any ProcessRequest calls.
	Init(ctx context.Context, cfg PluginConfig) error

	// ProcessRequest handles a single captured request for replay.
	// The plugin decides how to replay the request (e.g., send to a target
	// service, transform it, compare responses, etc.) and returns a result.
	ProcessRequest(ctx context.Context, req *ReplayRequest) (*ReplayResult, error)

	// Close releases any resources held by the plugin. Called once after
	// all requests have been processed or on engine shutdown.
	Close() error
}

// PluginConfig holds runtime configuration passed to a plugin during Init.
type PluginConfig struct {
	// Settings contains user-provided key-value configuration for the plugin.
	Settings map[string]string

	// Storage provides read-only access to the capture storage backend.
	// Plugins can use this to read captured requests directly.
	Storage StorageAccessor

	// TargetURL is the base URL of the service to replay traffic against.
	// Plugins that send HTTP requests should use this as the target.
	TargetURL string
}

// StorageAccessor provides read-only access to captured request data.
// This is passed to plugins during initialization so they can read from
// the same storage backend the capture was written to.
type StorageAccessor interface {
	// ReadCapture returns a reader for a specific capture session.
	ReadCapture(ctx context.Context, captureID string) (storage.Reader, error)
}

// ReplayRequest wraps a captured request with replay-specific metadata.
type ReplayRequest struct {
	// Original is the captured request as read from storage.
	Original *storage.CapturedRequest

	// Sequence is the position of this request in the replay stream (0-indexed).
	Sequence uint64

	// CaptureID identifies the capture session this request belongs to.
	CaptureID string
}

// ReplayResult holds the outcome of processing a single request through a plugin.
type ReplayResult struct {
	// RequestID is the ID of the original captured request.
	RequestID string

	// Status indicates the outcome of the replay attempt.
	Status ReplayStatus

	// StatusCode is the HTTP status code from the replayed response, if applicable.
	StatusCode int

	// Duration is how long the replay request took.
	Duration time.Duration

	// ResponseHeaders contains response headers from the replayed request.
	ResponseHeaders http.Header

	// ResponseBody contains the response body from the replayed request.
	// May be nil or truncated for large responses.
	ResponseBody []byte

	// Error contains an error message if the replay failed.
	Error string

	// Metadata holds arbitrary plugin-specific data about the result.
	Metadata map[string]string
}

// ReplayStatus represents the outcome of a replay attempt.
type ReplayStatus string

const (
	// ReplayStatusSuccess indicates the request was replayed successfully.
	ReplayStatusSuccess ReplayStatus = "success"

	// ReplayStatusError indicates the replay encountered an error.
	ReplayStatusError ReplayStatus = "error"

	// ReplayStatusSkipped indicates the plugin chose to skip this request.
	ReplayStatusSkipped ReplayStatus = "skipped"

	// ReplayStatusMismatch indicates the response differed from expectations.
	ReplayStatusMismatch ReplayStatus = "mismatch"
)
