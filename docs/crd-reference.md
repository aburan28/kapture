# CRD Reference

API group `capture.gateway.io/v1alpha1`. Five resources:

| Kind | Scope | Runs on | Purpose |
|------|-------|---------|---------|
| [TrafficCapture](#trafficcapture) | Namespaced | spoke | Mirror route traffic into storage |
| [CaptureStorage](#capturestorage) | Namespaced | spoke | Storage backend configuration |
| [CaptureHub](#capturehub) | Cluster | hub | Hub gRPC server + fleet view |
| [TrafficReplay](#trafficreplay) | Namespaced | spoke | One replay run (or load-test shard) |
| [CaptureLoadTest](#captureloadtest) | Namespaced | hub | Distributed load test across cells |

Cross-field rules are enforced at admission by CEL validation; the
constraints called out below are rejected by the API server, not at
runtime.

## TrafficCapture

Captures traffic flowing through a Gateway API route by injecting a
`RequestMirror` filter that duplicates requests to capture agent pods.

### Spec

| Field | Type | Description |
|-------|------|-------------|
| `targetRef` | LocalPolicyTargetReference | Route to capture: `group` (gateway.networking.k8s.io), `kind` (`HTTPRoute` or `GRPCRoute`), `name` (same namespace) |
| `storageRef.name` | string | CaptureStorage in the same namespace |
| `capture.includeHeaders` | bool | Store request headers |
| `capture.includeBody` | bool | Store request bodies |
| `capture.maxBodyBytes` | int64 ≥ 0 | Truncate bodies beyond this size (default 1 MiB; truncation is marked in metadata) |
| `capture.duration` | Go duration string | Stop capturing after this long |
| `capture.redactHeaders` | []string | Extra headers to redact on top of the default credential set (Authorization, Proxy-Authorization, Cookie, Set-Cookie, X-Api-Key, X-Auth-Token) |
| `capture.disableHeaderRedaction` | bool | Turn off ALL redaction — captured credentials become durable data and are re-sent on replay; only for trusted pipelines |
| `filters.headers` | []{name, value} | Only capture requests with these exact header values |
| `filters.pathPrefix` | string | Only capture matching paths |
| `filters.percentage` | 0–100 | Uniform sample rate |
| `agent.replicas` / `minReplicas` / `maxReplicas` / `targetCPU` | int32 | Agent scaling; min/max drive an HPA. `minReplicas` ≤ `maxReplicas` (CEL) |
| `payloadPlugins` | []{name, onError, config} | Payload pipeline plugins; `onError` ∈ drop/skip/abort |

### Status

`phase` (Pending/Active/Completed/Failed), `startTime`,
`capturedRequests`, `bytesWritten`, `agentStatus.readyReplicas`/
`desiredReplicas`, `conditions`.

Captured data lands under the dataset ID `<namespace>/<name>` in the
storage backend, as gzip JSONL objects.

## CaptureStorage

Validated storage backend configuration referenced by captures,
replays, and load tests. `spec.type` selects a backend and the matching
config block is required (CEL).

| Type | Block | Required fields |
|------|-------|-----------------|
| `S3` | `s3` | `bucket` (3–63 chars), `region`, `credentialsSecretRef`; optional `prefix` |
| `GCS` | `gcs` | `bucket`, `credentialsSecretRef`; optional `prefix` |
| `EFS` | `efs` | `fileSystemID`, `mountPath` (absolute); optional `directory` |
| `EBS` | `ebs` | `volumeID`, `mountPath` (absolute); optional `filesystem` |
| `RDS` | `rds` | `connectionSecretRef` (Secret with a `dsn` key holding a PostgreSQL URL); optional `table`. Captured requests become SQL-queryable rows |
| `Plugin` | `plugin` | `path`; optional `symbol`, `config` (JSON) |

Optional `retention.maxAge` / `retention.maxSize`. Status: `phase`
(Ready/Error) + `Ready` condition with the validation failure message.

## CaptureHub

Cluster-scoped hub configuration. The hub gRPC server follows the
**oldest** CaptureHub when several exist (name as tiebreak); the others
carry an `Active: False / NotAuthoritative` condition.

### Spec

| Field | Type | Description |
|-------|------|-------------|
| `grpcAddress` | string | Listen address in Go `net.Listen` form (`":9443"`); pattern-validated |
| `tls.certSecretRef` | SecretObjectReference | Secret with `tls.crt`, `tls.key`, and (for mTLS) `ca.crt`. Content changes rotate in place — see [tls-rotation.md](tls-rotation.md) |
| `authentication.type` | `mTLS` | Require client certificates signed by `ca.crt`; binds each spoke's ID to its certificate CN/DNS SAN. Requires `tls` (CEL) |

### Status

`connectedSpokes`, `activeCaptures`, `activeReplays`, `spokes[]`
(name, cell, lastHeartbeat, per-spoke counts), `cells[]` (per-cell
aggregation), `conditions` (`Active`).

## TrafficReplay

One replay run on a spoke: reads a captured dataset from storage and
sends it at a target via a replay-engine Job. Distributed load tests
create one TrafficReplay per shard (labeled
`capture.gateway.io/load-test`); user-created replays work identically.

### Spec

| Field | Type | Description |
|-------|------|-------------|
| `sourceRef.name` | string | TrafficCapture whose dataset to replay |
| `storageRef.name` | string | CaptureStorage to read from |
| `target.host` | hostname/IP | Replay destination (validated shape) |
| `target.port` | 1–65535 | Default 80 (443 with TLS) |
| `target.tls` | bool | HTTPS/TLS to the target |
| `rate.mode` | `Unlimited` \| `Constant` \| `OriginalTiming` | Pacing. `Constant` requires `requestsPerSecond` (CEL); `timeScale` only valid with `OriginalTiming` (CEL) |
| `rate.requestsPerSecond` | int32 ≥ 1 | Target RPS for Constant mode |
| `rate.timeScale` | decimal string | Multiplies recorded inter-request delays (0.5 = 2× speed) |
| `filters.startTime`/`endTime` | timestamps | Time-window the dataset; start < end (CEL) |
| `filters.pathPrefix` / `methods` / `limit` | | Narrow what is replayed; `limit` is per shard |
| `shard.index` / `shard.count` | int32 | Deterministic slice of the capture: this worker replays requests whose FNV-1a hash mod `count` equals `index`. `index < count` (CEL) |
| `shard.presharded` | bool | Read the pre-partitioned slice `{captureID}/shards/{index}-of-{count}` instead of filtering the full capture |
| `loadTestRef.name` | string | Set by the spoke controller on shards created from hub directives |
| `engine.name` | string | `builtin` (default), `k6`, `ghz`, or any installed `kapture-engine-<name>` plugin |
| `engine.config` | JSON | Passed verbatim to the engine's Configure call |
| `safety.allowedHosts` | []pattern | Exact hostnames or `*.suffix` wildcards; a target matching no pattern fails the replay before any traffic is sent. Empty = no restriction |
| `concurrency` | int32 ≥ 1 | Parallel senders in the worker |
| `transforms` | []{name, config} | Request transformers applied in order |

### Status

`phase` (Pending/Running/Completed/Failed), `startTime`,
`completionTime`, `totalRequests`, `sentRequests`, `failedRequests`,
`filteredRequests`, `meanLatency`, `p50Latency`, `p95Latency`,
`p99Latency`, `achievedRPS`, `conditions`.

## CaptureLoadTest

Hub-side distributed load test: fans one captured dataset out as replay
shards across the spokes of selected cells, aggregates progress, and
enforces abort policy. Deletion stops all shards on all spokes before
the finalizer is released.

### Spec

`sourceRef`, `storageRef`, `target`, `rate`, `filters`, `engine`, and
`safety` have the same shapes as TrafficReplay. Notes:

- `storageRef` names a CaptureStorage that must exist (same
  namespace/name) on **every participating spoke**.
- `rate.mode: Constant` is the **aggregate** rate — the hub divides
  `requestsPerSecond` across all workers.
- `safety.allowedHosts` is enforced by the hub before distribution and
  again by every spoke (defense in depth).

| Field | Type | Description |
|-------|------|-------------|
| `distribution.cells` | []string | Restrict to spokes registered in these cells; empty = all connected spokes |
| `distribution.maxSpokes` | int32 ≥ 1 | Cap participating spokes |
| `distribution.workersPerSpoke` | int32 ≥ 1 (default 1) | Shards per spoke; total shards = spokes × workersPerSpoke |
| `distribution.concurrencyPerWorker` | int32 ≥ 1 (default 10) | Parallel senders per worker |
| `distribution.presharded` | bool | Dataset was pre-partitioned by `kapture-preshard` into exactly totalShards slices |
| `abort.maxDuration` | duration | Abort runs exceeding this |
| `abort.errorPercent` | 1–100 | Abort when failed/sent exceeds this percentage |
| `abort.minSampleRequests` | int64 ≥ 1 (default 100) | Attempts before errorPercent is evaluated |

### Status

`phase` (Pending/Distributing/Running/Completed/Failed/Aborted),
`startTime`, `completionTime`, `totalShards`, `assignedSpokes`,
`completedShards`, `failedShards`, aggregate request counters,
`achievedRPS`, `assignments[]` (spoke → shard indexes, decided once at
distribution and stable for the run), `cells[]` (per-cell progress),
`conditions` (`SpokesAssigned`, `TargetAllowed`, `Aborted`).
