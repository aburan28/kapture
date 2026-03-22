# Kapture Test Coverage Analysis

## Overview

This document analyzes the current test coverage of the Kapture codebase and identifies areas where test coverage should be improved. The analysis covers unit tests, integration tests, and end-to-end tests across all packages.

### Estimated Coverage by Package

| Package | Est. Coverage | Status |
|---------|--------------|--------|
| `internal/agent` | ~70% | Good — main handlers covered |
| `internal/hub` (gRPC) | ~90% | Excellent — streaming RPCs untested |
| `internal/hub` (controller) | ~10% | **Critical gap** |
| `internal/spoke` (reconciler) | ~30% | **Critical gap** |
| `internal/spoke` (mirror) | ~50% | Partial |
| `internal/spoke` (hub_client) | ~20% | Background tasks untested |
| `internal/storage` | ~65% | Error paths missing |
| `api/v1alpha1` | N/A | Generated types — no custom logic |
| `cmd/*` | 0% | Expected (integration-tested) |
| `test/integration` | ~40% | Basic scenarios only |
| `test/e2e` | ~20% | Minimal lifecycle coverage |

---

## Critical Gaps (High Priority)

### 1. Hub Controller Reconciler — `internal/hub/controller.go`

The `CaptureHubReconciler.Reconcile()` method has **no test coverage at all**. This is the core control loop for the hub component.

**Untested functions:**
- `Reconcile()` — multi-step reconciliation of CaptureHub resources
- `ensureServer()` — server lifecycle management (start/restart)
- `stopServer()` — graceful cleanup
- `updateStatus()` — status subresource updates
- `SetupWithManager()` — controller-runtime wiring

**Recommended tests:**
- Reconcile creates a gRPC server when a CaptureHub resource is created
- Reconcile updates the server when CaptureHub spec changes
- Reconcile stops the server when CaptureHub is deleted
- Status reflects the correct number of connected spokes and active captures
- Error handling when server fails to start (port conflict, TLS misconfiguration)

---

### 2. Spoke Reconciler Error Paths — `internal/spoke/controller.go`

The `TrafficCaptureReconciler.Reconcile()` has basic happy-path tests but is missing coverage for error handling, deletion, and edge cases.

**Untested functions/paths:**
- `reconcileDelete()` — finalizer cleanup logic
- `updateStatus()` — status subresource updates
- `setPhase()` / `setPhaseWithCondition()` — phase transitions
- `createOrUpdate()` — resource ownership and update conflicts
- `reportToHub()` — hub communication during reconciliation
- Duration expiration logic (capture timeout)
- Storage not found / not ready error paths
- Mirror filter addition failures

**Recommended tests:**
- Reconcile handles TrafficCapture deletion (finalizer removes mirror filters, agent deployment)
- Reconcile transitions to `Failed` phase when CaptureStorage is missing
- Reconcile transitions to `Expired` when duration elapses
- Reconcile retries on transient hub communication failures
- Status conditions are set correctly for each phase transition
- Resource ownership labels/annotations are set correctly

---

### 3. Hub Client Background Tasks — `internal/spoke/hub_client.go`

The `HubClient` has tests for `Connect()` and `Register()` but none of the long-running background operations are tested.

**Untested functions:**
- `StartHeartbeat()` — background goroutine that sends periodic heartbeats
- `sendHeartbeat()` — individual heartbeat with capture summaries
- `ReportStatus()` — capture status reporting to the hub
- `WatchDirectives()` — streaming RPC that receives hub directives
- `watchDirectivesLoop()` — reconnection logic for directive stream
- `Deregister()` — spoke deregistration
- `Close()` — connection and goroutine cleanup
- `IsConnected()` — connection state checking

**Recommended tests:**
- Heartbeat is sent at the configured interval
- Heartbeat includes current capture summaries
- Heartbeat stops when context is cancelled
- WatchDirectives reconnects on stream error
- Close() stops heartbeat and deregisters before closing connection
- ReportStatus sends correct capture statuses to hub
- IsConnected returns false after Close()

---

### 4. Storage Backend Error Scenarios — `internal/storage/`

The storage package has good happy-path coverage but is missing error-path and edge-case tests across all backends.

**Missing error tests:**
- **S3:** `PutObject` failures (network error, access denied, bucket not found)
- **GCS:** Writer open failures, write failures mid-stream, close failures
- **EFS/EBS:** Directory creation failures, file write permission errors, disk full scenarios
- **All backends:** Config validation (nil/empty required fields)

**Missing edge-case tests:**
- Concurrent writes from multiple goroutines
- Buffer exactly at max size boundary
- Multiple sequential flushes (sequence number incrementing)
- Flush with zero pending bytes (no-op behavior)
- Writer reuse after Close() returns an error

**Untested factory functions:**
- `asS3Config()`, `asGCSConfig()`, `asEFSConfig()`, `asEBSConfig()` — type conversion helpers in `factory.go`

---

### 5. Agent Server Lifecycle — `internal/agent/server.go`

The HTTP handler methods are well-tested, but server lifecycle management is not.

**Untested functions:**
- `NewAgentServer()` — direct construction (only used indirectly)
- `Start()` — goroutine startup, listener binding, error channel handling
- `Shutdown()` — graceful shutdown with partial failure handling
- `handleGRPC()` — gRPC unknown service handler (stream forwarding)

**Recommended tests:**
- Start() binds to the configured address and serves requests
- Shutdown() stops accepting new connections and drains existing ones
- Shutdown() returns errors if underlying server shutdown fails
- handleGRPC() properly forwards unknown gRPC service calls
- Concurrent Start()/Shutdown() doesn't race

---

## Medium Priority Gaps

### 6. Mirror Filter Operations — `internal/spoke/mirror.go`

The filter building and detection helpers are tested, but the actual add/remove operations on routes are not.

**Untested:**
- `AddMirrorFilter()` — top-level add operation
- `RemoveMirrorFilter()` — top-level remove operation
- `addHTTPRouteMirror()` / `removeHTTPRouteMirror()` — HTTP route mutation
- `addGRPCRouteMirror()` / `removeGRPCRouteMirror()` — gRPC route mutation
- Route not found scenarios
- Route with existing mirror filters from other sources

**Recommended tests:**
- AddMirrorFilter adds a mirror to an HTTPRoute with no existing filters
- AddMirrorFilter is idempotent (doesn't duplicate on re-add)
- RemoveMirrorFilter removes only kapture-owned mirrors, leaving others intact
- GRPCRoute mirror operations mirror the HTTPRoute behavior
- Error when target route does not exist

---

### 7. Hub gRPC Streaming — `internal/hub/grpc_server.go`

`WatchDirectives()` is a server-streaming RPC that is completely untested.

**Recommended tests:**
- WatchDirectives sends initial directive set on connect
- WatchDirectives sends updates when captures change
- WatchDirectives handles client disconnect gracefully
- WatchDirectives rejects empty spoke ID

---

### 8. Helper Functions Without Direct Tests

Several utility functions lack direct test coverage:

- **Hub:** `captureKey()`, `captureInfoFromSummary()`, `cloneSpokeInfo()` (grpc_server.go)
- **Spoke:** `conditionStatus()`, `setCondition()` (controller.go)
- **Spoke:** `agentLabels()`, `defaultResources()` (agent_deployer.go)
- **Mirror:** `setAnnotation()`, `mirrorFilterName()` (mirror.go)

While some are tested indirectly, direct tests would catch regressions more precisely.

---

## Integration & E2E Test Gaps

### Integration Tests (`test/integration/`)

**Currently covered:**
- TrafficCapture creation triggers Deployment/Service/HPA creation
- Basic resource reconciliation

**Missing scenarios:**
- CaptureStorage controller lifecycle and status
- TrafficCapture duration expiration and automatic cleanup
- Multiple concurrent TrafficCaptures
- TrafficCapture update (spec change triggers re-reconciliation)
- Error recovery (e.g., manually deleted child resources get re-created)
- Mirror filter injection into existing HTTPRoute/GRPCRoute resources

### E2E Tests (`test/e2e/`)

**Currently covered:**
- Basic TrafficCapture full lifecycle

**Missing scenarios:**
- Hub-spoke registration and heartbeat
- Multi-spoke topology
- Actual traffic capture data flow (mirror → agent → storage)
- CaptureStorage with real backend credentials (S3/GCS)
- Capture expiration and cleanup
- Spoke disconnection and reconnection
- Hub failover behavior

---

## Recommendations (Prioritized)

1. **Add unit tests for `CaptureHubReconciler.Reconcile()`** — This is the hub's core logic with zero coverage.
2. **Add unit tests for spoke `reconcileDelete()` and error paths** — Deletion cleanup is critical for resource hygiene.
3. **Add unit tests for `HubClient` background tasks** — Heartbeat and directive watching are essential for hub-spoke communication.
4. **Add error-path tests for storage backends** — Failures in production storage writes need graceful handling.
5. **Add unit tests for `AgentServer.Start()/Shutdown()`** — Server lifecycle bugs can cause resource leaks.
6. **Add tests for mirror filter add/remove operations** — These mutate Gateway API resources and need correctness guarantees.
7. **Add integration tests for duration expiration** — Time-based capture cleanup is a key feature.
8. **Expand e2e tests for multi-spoke and data flow** — End-to-end validation of the capture pipeline.
