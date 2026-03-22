# Go Plugin Model Opportunities in Kapture

This note identifies where Kapture can adopt a Go plugin model (using `plugin.Open` or an in-process registry plugin pattern) to reduce core churn while adding extensibility.

## Why Kapture is a good fit

Kapture already has strong interface boundaries (`storage.Writer`, `storage.WriterFactory`) and centralized construction points. Those are ideal seams for loading runtime-provided implementations.

## 1) Storage backends (highest ROI)

### Current state

- Storage backend selection is a static `switch` in `internal/storage/factory.go` (`NewWriterFactory`).
- The capture agent separately parses backend-specific JSON in another static `switch` (`parseStorageConfig` in `cmd/capture-agent/main.go`).

### Plugin opportunity

Introduce a backend plugin contract so new backends (for example Azure Blob, Kafka, NATS, local object store variants) can be added without changing core Kapture code.

Suggested contract:

- `type StorageBackendPlugin interface {`
  - `Type() string`
  - `ParseConfig(raw json.RawMessage) (any, error)`
  - `NewFactory(ctx context.Context, cfg any) (storage.WriterFactory, error)`
- `}`

Core changes:

1. Add a plugin registry in `internal/storage` (map of backend type -> constructor).
2. Keep built-ins registered by default.
3. Optionally load `.so` plugins at startup and call an exported `Register(registry)` symbol.
4. Replace both static switches with registry lookups.

### Benefits

- Eliminates duplicate backend switches in two places.
- Enables third-party backend distribution on independent release cycles.
- Reduces risk of regressions in core when adding new backend support.

### Risks/notes

- Native Go `.so` plugins are Linux/ELF and Go-version sensitive; this project should likely support both:
  - **Dynamic `.so` mode** for controlled environments, and
  - **Static registry mode** for portability/testing.

## 2) Capture pipeline processors (headers/body transforms, redaction, sampling)

### Current state

- `CaptureHandler.Handle` performs all processing inline: read body, truncate, map to `CapturedRequest`, write.

### Plugin opportunity

Define a middleware-like processor chain:

- `type CaptureProcessor interface {`
  - `Name() string`
  - `Process(ctx context.Context, req *storage.CapturedRequest) (*storage.CapturedRequest, error)`
- `}`

Potential processors as plugins:

- PII redaction
- Header allow/deny-listing
- Body parsing and field hashing
- Advanced sampling/filtering
- Protocol-specific enrichment

### Benefits

- Lets users customize compliance and observability behavior per deployment.
- Keeps `CaptureHandler` focused on ingestion reliability.

## 3) Protocol ingestion adapters (beyond HTTP + gRPC)

### Current state

- `AgentServer` hardcodes HTTP and gRPC listeners/handlers.

### Plugin opportunity

Add a protocol adapter interface:

- `type IngestionAdapter interface {`
  - `Name() string`
  - `Start(ctx context.Context, sink CaptureSink) error`
  - `Shutdown(ctx context.Context) error`
- `}`

Adapters could support WebSocket, custom TCP, or message queue mirror inputs without modifying `internal/agent/server.go`.

### Benefits

- Decouples transport concerns from capture persistence.
- Opens path for protocol experiments outside core.

## 4) Route mutation strategy plugins in spoke controller

### Current state

- Spoke logic appears designed around Gateway API RequestMirror-driven behavior.

### Plugin opportunity

Abstract mirror/mutation strategy so environments can provide custom route mutation logic (for example gateway-vendor-specific annotations or alternative policy resources).

Interface sketch:

- `type RouteMutationStrategy interface {`
  - `Kind() string // HTTPRoute, GRPCRoute, ...`
  - `Apply(ctx context.Context, target client.Object, captureSpec ...) error`
  - `Remove(ctx context.Context, target client.Object, captureSpec ...) error`
- `}`

### Benefits

- Helps multi-platform support without complicating core reconcile paths.

## 5) Deployment policy plugins for capture agents

### Current state

- Deployment/HPA/service assembly is hardcoded in `internal/spoke/agent_deployer.go`.

### Plugin opportunity

Add policy hooks for:

- sidecar injection
- node affinity/tolerations
- security context defaults
- env var augmentation

This can be a simpler registry plugin (no `.so` required initially).

## Recommended implementation order

1. **Storage backend registry/plugins** (clearest seam, immediate value).
2. **Capture processors** (compliance/security use-cases).
3. **Deployment policy hooks** (platform customization).
4. **Protocol adapters** (as demand appears).
5. **Route mutation strategies** (advanced multi-gateway ecosystems).

## Practical rollout plan

### Phase 1: Static plugin architecture (no `plugin.Open` yet)

- Add registries/interfaces.
- Move built-ins to self-registration.
- Add tests that register fake plugins at runtime.

### Phase 2: Optional dynamic loading

- Add `--plugin-dir` or `PLUGIN_DIR`.
- Load `.so` files and invoke exported registration symbols.
- Emit clear startup diagnostics for incompatible plugins.

### Phase 3: Hardening

- Version the plugin APIs (semver handshake).
- Add plugin capability discovery in logs/metrics.
- Add conformance tests for third-party plugin packs.
