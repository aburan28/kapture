// Package replayengine is the SDK for building Kapture replay engines.
//
// A replay engine is the process that turns captured requests into real
// load: the builtin HTTP sender, a k6 or ghz wrapper, or any third-party
// generator. Engines implement the [Engine] interface and call [Serve] in
// their main function; Kapture's replay host launches them as gRPC
// subprocesses, streams already-sharded, already-paced requests to them,
// and collects their results.
//
// The full ABI contract — handshake, versioning, streaming semantics,
// packaging, and hot reload — is documented in docs/replay-engine-abi.md.
// The wire protocol lives in proto/replayengine/v1.
package replayengine

import (
	"context"

	replayenginev1 "github.com/kapture-io/kapture/proto/replayengine/v1"
)

const (
	// ABIVersion is the ABI generation this SDK speaks. Hosts and engines
	// negotiate the first common version during Describe.
	ABIVersion = "v1"

	// HandshakePrefix starts the single handshake line an engine prints to
	// stdout after its gRPC server is listening:
	//
	//	KAPTURE-ENGINE|1|unix|/path/to/engine.sock|grpc
	//
	// Fields: prefix, handshake format version, network, address, protocol.
	HandshakePrefix = "KAPTURE-ENGINE"

	// HandshakeFormatVersion is the version of the handshake line format
	// (not the ABI version — that is negotiated via Describe).
	HandshakeFormatVersion = "1"

	// MagicCookieEnv must be set to MagicCookieValue in the engine's
	// environment. Serve refuses to start without it, so plugin binaries
	// executed directly by a user fail with a clear message instead of
	// hanging waiting for a host.
	MagicCookieEnv   = "KAPTURE_ENGINE_MAGIC"
	MagicCookieValue = "kapture-replay-engine"

	// SocketDirEnv, when set by the host, tells Serve where to create the
	// engine's unix socket. Defaults to the OS temp directory.
	SocketDirEnv = "KAPTURE_ENGINE_SOCKET_DIR"

	// PluginBinaryPrefix is the required plugin binary naming convention:
	// an engine named "k6" is discovered as "kapture-engine-k6" in the
	// host's plugin directory.
	PluginBinaryPrefix = "kapture-engine-"
)

// Capability flags an engine may advertise in DescribeResponse.
const (
	// CapabilityPerRequestResults: the engine emits a RequestResult event
	// for every FeedItem.
	CapabilityPerRequestResults = "per-request-results"
	// CapabilityAggregateResults: the engine reports Progress/RunSummary
	// aggregates only.
	CapabilityAggregateResults = "aggregate-results"
	// CapabilitySelfPaced: the engine applies its own pacing from the
	// RateHint and per-item source offsets; the host feeds as fast as
	// gRPC flow control allows instead of pacing the stream.
	CapabilitySelfPaced = "self-paced"
)

// Engine is the interface a replay engine implements. Serve adapts it to
// the gRPC ABI; the Kapture host drives the same shape from the other side.
type Engine interface {
	// Describe returns the engine identity, supported ABI versions,
	// protocols, and capabilities.
	Describe(ctx context.Context) (*replayenginev1.DescribeResponse, error)

	// Configure prepares the engine for a run. It is called exactly once
	// before Execute; invalid configuration must be rejected here.
	Configure(ctx context.Context, cfg *replayenginev1.RunConfig) error

	// Execute performs the run. It consumes feed until the channel is
	// closed (the capture slice is exhausted) or ctx is cancelled, sends
	// events (results, progress, and a final RunSummary) into events, and
	// returns. The engine must emit exactly one RunSummary as its last
	// event. Execute must not close either channel.
	Execute(ctx context.Context, feed <-chan *replayenginev1.FeedItem, events chan<- *replayenginev1.ExecuteResponse) error

	// Drain asks the engine to stop accepting new work and finish
	// in-flight requests. It may be called concurrently with Execute and
	// must be idempotent.
	Drain(ctx context.Context) error
}

// SupportsABIVersion reports whether two version lists share a version,
// returning the first common one in hostPreference order.
func SupportsABIVersion(hostPreference, engineVersions []string) (string, bool) {
	engineSet := make(map[string]bool, len(engineVersions))
	for _, v := range engineVersions {
		engineSet[v] = true
	}
	for _, v := range hostPreference {
		if engineSet[v] {
			return v, true
		}
	}
	return "", false
}
