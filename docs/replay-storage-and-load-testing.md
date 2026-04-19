# Replay Storage and Load Testing Design

This document proposes a replay-oriented storage format and execution model for
large-scale Kapture load tests. It assumes the current capture path remains
compatible with the existing gzip-compressed JSONL writer, while adding the
manifest and index data required by replay agents to load large captures,
reproduce recorded QPS, scale pods predictably, and validate a run before and
during execution.

## Goals

- Preserve the current raw capture object format as the durable source of truth.
- Add a versioned replay dataset format that is cheap to discover, shard, and
  stream from object storage.
- Treat captured data size as a core design constraint, with explicit capture
  budgets, compaction, and bundle sizing.
- Use an aggregation and dataset-handler pipeline to transform raw capture data
  into replay bundles consumed by replay sidecars.
- Keep load-test tools data-agnostic: they should not list storage, open
  bundles, parse manifests, prefetch bodies, or understand dataset layout.
- Support replay at recorded QPS, plus explicit speed-up, slow-down, and fixed
  QPS modes.
- Scale replay workers before the run needs capacity, not only after CPU rises.
- Make validation a first-class gate for dataset integrity, cluster capacity,
  target safety, and live run health.

## Current State

Kapture currently writes captured requests as gzip-compressed JSONL under:

```text
{prefix}/{captureID}/{YYYY-MM-DD}/{pod-name}-{sequence}.jsonl.gz
```

Each JSON line is the JSON form of `storage.CapturedRequest`:

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2025-01-15T10:30:00Z",
  "method": "POST",
  "path": "/api/v2/users",
  "headers": {"content-type": ["application/json"]},
  "body": "eyJ1c2VybmFtZSI6ICJ0ZXN0In0=",
  "contentLength": 28,
  "protocol": "HTTP",
  "metadata": {"host": "api.example.com", "query": "page=1"}
}
```

The Go `[]byte` body already serializes as base64 in JSON. HTTP query strings
are currently stored in `metadata.query`; the recorded `path` is only
`URL.Path`. The format is adequate for capture durability, but replay should
not depend on listing and scanning every raw object before a run. Raw capture
objects can also be the wrong shape for replay: they may be too small, too
numerous, too body-heavy, or distributed by capture pod rather than by replay
time window.

## Proposed Format: Kapture Replay Dataset v1

A replay dataset is a bundled and indexed view over one capture. It is created
by an aggregation and dataset-handler pipeline at first, and can later be
emitted directly by capture agents or a background compactor.

```text
{prefix}/kapture/v1/datasets/{datasetID}/
  manifest.json
  bundles/
    date=2025-01-15/hour=10/bundle-000001/
      bundle.jsonl.gz
      bundle.index.json
    date=2025-01-15/hour=10/bundle-000002/
      bundle.jsonl.gz
      bundle.index.json
  indexes/
    bundles.jsonl.gz
    time-buckets.jsonl.gz
    route-stats.json
    size-report.json
  validation/
    prepare-report.json
```

`datasetID` should be a stable, object-path-safe identifier. The manifest keeps
the original namespace/name capture identity so we do not encode Kubernetes
names directly into object paths.

### Manifest

`manifest.json` is the single object every controller reads first.

Path resolution is intentionally simple in v1. `storage.prefix` is the
canonical dataset root, and `manifest.json` must live directly under that root.
All relative paths in the manifest, bundle index, time bucket index, leases,
validation reports, and body refs are resolved by joining them to
`storage.prefix`. Producers should not emit leading slashes in relative paths,
and consumers should not resolve paths relative to the manifest object's parent
independently from `storage.prefix`.

```json
{
  "schemaVersion": "kapture.replay.dataset.v1",
  "datasetID": "orders-prod-20250115-103000",
  "capture": {
    "namespace": "prod",
    "name": "orders",
    "uid": "9ab1c2d3",
    "startedAt": "2025-01-15T10:30:00Z",
    "endedAt": "2025-01-15T11:30:00Z"
  },
  "storage": {
    "backend": "S3",
    "bucket": "kapture-captures",
    "prefix": "captures/kapture/v1/datasets/orders-prod-20250115-103000"
  },
  "bundlePolicy": {
    "targetCompressedBytes": 134217728,
    "maxCompressedBytes": 268435456,
    "maxTimeSpanMs": 10000,
    "externalBodyThresholdBytes": 1048576
  },
  "records": {
    "count": 180000000,
    "compressedBytes": 740000000000,
    "uncompressedBytes": 2100000000000,
    "bodyBytes": 900000000000
  },
  "timing": {
    "baseTime": "2025-01-15T10:30:00Z",
    "durationMs": 3600000,
    "peakQPS": 125000,
    "p50QPS": 44000,
    "bucketWidthMs": 1000
  },
  "indexes": {
    "bundles": "indexes/bundles.jsonl.gz",
    "timeBuckets": "indexes/time-buckets.jsonl.gz",
    "routeStats": "indexes/route-stats.json",
    "sizeReport": "indexes/size-report.json"
  },
  "validation": {
    "status": "Passed",
    "preparedAt": "2025-01-15T12:00:00Z",
    "report": "validation/prepare-report.json"
  }
}
```

### Replay Record

Bundles are still gzip-compressed JSONL, but each line is normalized for replay
scheduling. Bundle records can be produced from today's
`storage.CapturedRequest` without changing the capture agent.

```json
{
  "schemaVersion": "kapture.replay.request.v1",
  "requestID": "550e8400-e29b-41d4-a716-446655440000",
  "sourceTimestamp": "2025-01-15T10:30:00.123456Z",
  "sourceOffsetNs": 123456000,
  "protocol": "HTTP",
  "method": "POST",
  "scheme": "https",
  "authority": "api.example.com",
  "path": "/api/v2/users",
  "query": "page=1",
  "headers": {"content-type": ["application/json"]},
  "body": {
    "encoding": "base64",
    "inline": "eyJ1c2VybmFtZSI6ICJ0ZXN0In0=",
    "contentLength": 28,
    "sha256": "95d5..."
  },
  "routing": {
    "routeKey": "POST /api/v2/users",
    "partitionKey": "api.example.com|/api/v2/users"
  },
  "metadata": {
    "capturedProtocol": "HTTP"
  }
}
```

`metadata` remains a freeform string map for source capture metadata. For
compatibility with today's capture agent, `bodyTruncated` should only appear in
metadata when truncation happened, with value `"true"`; absence means false.

For very large bodies, use an external body reference instead of inline base64:

```json
{
  "body": {
    "encoding": "raw",
    "ref": "bodies/sha256/95/d5/95d5...",
    "storageEncoding": "identity",
    "contentLength": 8388608,
    "sha256": "95d5..."
  }
}
```

Inline bodies keep small request replay fast. External body references prevent a
few large requests from forcing every scheduler buffer to hold huge JSON lines.
The prepare job should default to inline bodies below 1 MiB and external bodies
above that threshold.

For external body refs, `encoding: raw` means the referenced object contains the
exact request body bytes, not base64 text. `storageEncoding` must be `identity`
for v1 body objects; if future formats add compression, they need a new explicit
storage encoding. `contentLength` and `sha256` are computed over the exact bytes
the sidecar will send after resolving the reference.

## Data Size Management

Large captures can fail long before replay starts if Kapture writes too many
small objects, stores bodies that are too large, or forces replay pods to
prefetch more data than memory and storage bandwidth allow. Size management
needs gates at capture time and again during dataset aggregation.

Capture-time controls:

- `includeBody`, `maxBodyBytes`, sampling, and route filters set the first
  storage budget.
- Body capture should support content-type and path policies so binary uploads,
  media, and other high-volume bodies can be skipped or externally referenced.
- Headers should be allowlisted for replay; volatile and credential headers
  should be dropped or transformed before they become durable replay data.
- Capture status should eventually report raw object count, compressed bytes,
  uncompressed bytes, body bytes, truncated body count, and dropped request
  count.

Dataset-time controls:

- Aggregation compacts raw pod-oriented JSONL objects into replay-oriented
  bundles sized for sidecar prefetch.
- Bundle indexes let sidecars seek by time bucket without scanning the full
  bundle.
- Large bodies are deduplicated by SHA and stored once under `bodies/`.
- The dataset handler writes `indexes/size-report.json` with top routes by
  byte volume, body-size histograms, compression ratio, object count, and
  projected replay prefetch memory.
- Preflight rejects runs whose bundle read bandwidth, body cache size, or
  decompressed prefetch window exceeds configured budgets.

Default bundle targets should be:

- 128 MiB target compressed bundle size.
- 256 MiB maximum compressed bundle size.
- 10 seconds maximum source time span per high-QPS bundle.
- 1 MiB threshold for moving bodies to external body refs.
- 10,000 raw objects maximum per aggregation shard before reduce/compact.

## Compatibility and Migration

The raw capture writer does not need to change for v1. Existing captures can be
prepared into replay datasets by reading today's object layout and writing the
new dataset tree elsewhere in the same storage backend.

Compatibility rules:

- Treat raw JSONL files as immutable source data.
- Keep `storage.CapturedRequest` decoding as the fallback input parser.
- Recover HTTP authority from `metadata.host` and query strings from
  `metadata.query` when normalizing old records.
- Preserve the original raw object URI and line number in prepare reports for
  debugging malformed records.
- Publish `manifest.json` only after all bundles, body refs, and indexes are
  durably written.

Later, capture agents can write small per-object metadata sidecars with record
counts, first/last timestamps, checksums, and byte totals. That would make
preparation faster without changing the raw request line format.

## Aggregation and Dataset Handler Pattern

The aggregation layer converts capture-shaped data into replay-shaped bundles.
The dataset handler owns this process and presents a stable dataset contract to
the replay controller and replay sidecars.

Pipeline stages:

1. `RawObjectScanner`: lists raw capture objects once and records object size,
   date, capture pod, and checksum.
2. `Aggregator`: streams raw JSONL, normalizes requests, applies redaction and
   body policies, and emits temporary records partitioned by source time.
3. `BundleBuilder`: compacts temporary records into bounded replay bundles and
   writes per-bundle indexes.
4. `DatasetHandler`: publishes global indexes, size reports, validation
   reports, and the final manifest.
5. `ReplaySidecar`: receives bundle leases, prefetches assigned bundles, and
   feeds ready-to-send records to the local load-test tool or scheduler.

This pattern keeps the replay sidecar simple: it does not list raw storage, run
global aggregation, or infer dataset shape. It consumes already-validated
bundles and reports progress against bundle leases.

## Load-Test Tool Boundary

Load-test tools should not load replay data. They should not receive storage
credentials, manifest URIs, bundle paths, object-store clients, body caches, or
dataset parsing code. Those responsibilities belong to Kapture's dataset
handler, controller, and replay sidecar.

The load-test tool contract should be local and data-agnostic:

- Kapture sidecar owns bundle leases, prefetch, decompression, body refs,
  buffering, recorded-time scheduling, and progress checkpoints.
- The load-test tool receives already-materialized requests from a localhost API,
  stdin stream, shared-memory queue, or adapter library.
- The tool returns send result, latency, status code, and error metadata to the
  sidecar.
- The sidecar translates those results into lease progress, bucket accounting,
  lag metrics, and abort signals.
- Replacing the load-test tool should not require a new dataset format or new
  object-storage loader.

This makes tools like a custom Go sender, k6, Vegeta, or a protocol-specific
executor interchangeable at the send layer. Kapture remains responsible for the
hard parts: data placement, loading, replay timing, validation, and accounting.

### Bundle Index

`indexes/bundles.jsonl.gz` contains one line per bundle.

```json
{
  "bundleID": "date=2025-01-15/hour=10/bundle-000001",
  "uri": "bundles/date=2025-01-15/hour=10/bundle-000001/bundle.jsonl.gz",
  "indexURI": "bundles/date=2025-01-15/hour=10/bundle-000001/bundle.index.json",
  "firstOffsetNs": 123456000,
  "lastOffsetNs": 6123456000,
  "firstTimestamp": "2025-01-15T10:30:00.123456Z",
  "lastTimestamp": "2025-01-15T10:30:06.123456Z",
  "recordCount": 125000,
  "compressedBytes": 67108864,
  "uncompressedBytes": 201326592,
  "sha256": "2f8a...",
  "routeCounts": {"POST /api/v2/users": 104000, "GET /health": 21000}
}
```

Recommended bundle targets:

- 64-256 MiB compressed object size.
- 2-10 seconds of peak traffic per bundle for high-QPS captures.
- Records sorted by `sourceOffsetNs` within each bundle.
- Bundle boundaries aligned to time buckets when possible.

### Time Bucket Index

`indexes/time-buckets.jsonl.gz` is the replay scheduler's main input.

```json
{
  "bucketStartOffsetMs": 6000,
  "bucketWidthMs": 1000,
  "requestCount": 125000,
  "bodyBytes": 740000000,
  "compressedBytes": 260000000,
  "bundles": [
    {
      "bundleID": "date=2025-01-15/hour=10/bundle-000001",
      "recordCount": 52000
    },
    {
      "bundleID": "date=2025-01-15/hour=10/bundle-000002",
      "recordCount": 73000
    }
  ],
  "requiredAgents": 42
}
```

The controller uses this index to scale ahead of future demand and to issue
leases. Agents use per-record `sourceOffsetNs` for precise send timing inside a
bucket.

## Prepare Job

The prepare job runs the aggregation and dataset-handler pipeline:

1. Discover raw objects from the capture prefix once.
2. Stream and decompress JSONL without loading the full capture.
3. Normalize records into replay request schema.
4. Sort or merge by `timestamp` where raw pod files overlap.
5. Apply body, header, redaction, and deduplication policies.
6. Write replay bundles, external body objects, and indexes.
7. Compute counts, byte totals, histograms, checksums, and QPS percentiles.
8. Write `indexes/size-report.json` and `validation/prepare-report.json`.
9. Atomically publish `manifest.json` last.

The prepare job can be implemented as a Kubernetes Job. For very large
captures, split it into map/reduce stages:

- Map: each pod converts a slice of raw objects into temporary normalized
  bundles and local summaries.
- Reduce: merge by time bucket, compact to final bundle sizes, build global
  indexes, and publish the manifest.

## Replay API Shape

The repo does not yet define replay CRDs. The minimum useful API is:

```yaml
apiVersion: capture.gateway.io/v1alpha1
kind: ReplayDataset
metadata:
  name: orders-prod
spec:
  storageRef:
    name: my-storage
  manifestURI: captures/kapture/v1/datasets/orders-prod-20250115-103000/manifest.json
status:
  phase: Ready
  totalRequests: 180000000
  peakQPS: 125000
  validation: Passed
```

```yaml
apiVersion: capture.gateway.io/v1alpha1
kind: TrafficReplay
metadata:
  name: orders-prod-replay
spec:
  datasetRef:
    name: orders-prod
  target:
    baseURL: https://staging-api.example.com
    allowedHosts:
      - staging-api.example.com
  schedule:
    mode: Recorded
    timeScale: "1x"
    qpsScale: "1x"
    startBarrier: true
  agents:
    minReplicas: 10
    maxReplicas: 500
    targetQPSPerPod: 3000
    prefetchWindow: 30s
  validation:
    preflightRequired: true
    abortOnErrorRate: "5%"
    abortOnScheduleLag: 5s
```

The controller should create replay agent Deployments and a lightweight
`ReplayLease` resource or equivalent control-plane stream. The lease model is
what prevents duplicate sends and lets work move between pods.

## Replay Leases

A lease is the unit of ownership for replay work. It should cover a bounded time
range and a bounded set of bundle refs.

```json
{
  "leaseID": "orders-prod-replay-000042",
  "runID": "orders-prod-replay-20250115T130000Z",
  "epoch": 7,
  "bucketStartOffsetMs": 6000,
  "bucketEndOffsetMs": 7000,
  "bundles": [
    {
      "bundleID": "date=2025-01-15/hour=10/bundle-000001",
      "uri": "bundles/date=2025-01-15/hour=10/bundle-000001/bundle.jsonl.gz"
    }
  ],
  "expectedRequests": 52000,
  "expectedQPS": 52000,
  "assignedAgent": "orders-prod-replay-agent-17",
  "expiresAt": "2025-01-15T13:00:30Z"
}
```

Lease rules:

- A lease is assigned to at most one warmed agent at a time.
- Agents heartbeat lease progress with sent count, next due offset, queue depth,
  and schedule lag.
- Expired leases return to the controller and are reissued with a higher epoch.
- Agents must reject stale leases whose epoch is lower than the latest epoch
  seen for the lease ID.
- Completed leases are checkpointed before the controller advances final run
  counts.

This gives the controller a simple accounting model: expected requests are the
sum of issued leases, completed requests are the sum of completed lease
checkpoints, and any gap is visible before the run finishes.

## Loading Large Datasets

Replay agents and load-test tools should never list the capture bucket or read
the full manifest tree independently. The expected flow is:

1. Controller reads `manifest.json` and indexes.
2. Controller assigns agents leases for upcoming time buckets and bundle refs.
3. Replay sidecars prefetch only leased bundles within `prefetchWindow`.
4. Sidecars stream-decompress JSONL into a bounded priority queue ordered by
   `sourceOffsetNs`.
5. Sidecars hand ready records to the colocated load-test tool through the local
   data-agnostic contract.
6. Sidecars drop decompressed records after send completion.
7. Sidecars cache external bodies by SHA in `emptyDir` or an optional PVC.

Default memory bounds per agent should be derived from:

```text
memory >= max(prefetch_window_seconds * assigned_qps * average_record_bytes, minimum_buffer)
```

If the calculated buffer is too large, preflight should increase the pod count,
shorten the prefetch window, or reject the run.

## Achieving Recorded QPS

Recorded mode uses virtual time:

```text
due_time = run_start_time + duration(sourceOffsetNs / timeScale)
```

`sourceOffsetNs` is always an integer nanosecond offset from `timing.baseTime`.
`timeScale` is a positive numeric multiplier parsed from strings such as `1x`,
`2x`, or `0.5x`; `2x` halves the recorded interval, and `0.5x` doubles it. Time
bucket offsets use milliseconds only for compact indexes and lease boundaries;
controllers and sidecars must convert bucket offsets to nanoseconds before
comparing them with per-record `sourceOffsetNs`.

`qpsScale` controls how many records from each bucket are sent:

- `1x`: send every record at recorded timing.
- `<1x`: deterministically sample by request ID within each bucket.
- `>1x`: either loop the bucket data or duplicate with synthetic request IDs.
  This must be explicit because it changes traffic uniqueness.

Each replay sidecar maintains:

- A monotonic clock.
- A per-lease priority queue ordered by due time.
- A local token bucket for its assigned bucket rate.
- Schedule lag metrics: `now - due_time` at send.

The load-test tool should only see requests when they are due or nearly due. It
should not compute global replay time, own token buckets, or decide which bundle
to read next.

The controller maintains:

- The global run start barrier.
- Bucket-level expected request counts.
- Current and lookahead required agents from the time bucket index.
- Rebalancing when an agent reports sustained lag.

Recorded-QPS success should be measured per time bucket:

```text
abs(actual_count - expected_count) / expected_count <= allowed_bucket_error
p99(schedule_lag) <= allowed_schedule_lag
```

## Scaling Replay Pods

CPU HPA alone is too reactive for recorded-QPS replay. Use planned scaling from
the time bucket index:

```text
required_pods = ceil(peak_bucket_qps / measured_qps_per_pod * headroom)
```

The controller should support two scaling signals:

- Planned replicas: based on upcoming bucket QPS and bytes for the next
  1-5 minutes.
- Reactive replicas: based on schedule lag, queue depth, error rate, and CPU.

Preflight should run a short calibration job against either a noop endpoint or
the real target at a safe low rate to estimate `measured_qps_per_pod`. If no
calibration exists, use a conservative configured default and require manual
override for high-QPS runs.

Replay agents should expose readiness states:

- `Ready`: process can accept a lease.
- `Warmed`: initial bundles and external bodies for first leases are cached.
- `Running`: start barrier has been released.
- `Draining`: no new leases; finishing assigned sends.

The run should not begin until the required first-window pods are `Warmed`.

## Run Lifecycle

Large-scale replay should move through explicit phases:

1. `Admitted`: CRD accepted and target safety policy parsed.
2. `Validated`: dataset, target, cluster, and storage capacity checks passed.
3. `Scaling`: controller has created the required first-window agents.
4. `Warming`: agents are downloading initial leased bundles.
5. `Running`: start barrier released and leases are actively replaying.
6. `Draining`: no new leases are issued; assigned leases finish or expire.
7. `Completed` or `Failed`: final report is written and agents are scaled down.

Every phase should have a status condition with a machine-readable reason and a
human-readable message. That keeps failed preflight and aborted in-flight runs
debuggable from `kubectl describe`.

## Preflight Validation

Preflight is a blocking gate. It should write a report and set conditions on
`TrafficReplay`.

Dataset checks:

- Manifest schema version is supported.
- Bundle and body objects exist and are readable.
- Bundle checksums match.
- Record counts and byte totals match index totals.
- Records in every bundle are ordered by `sourceOffsetNs`.
- Required replay fields are present: protocol, method, authority or target
  override, path, body length, and body hash.
- No unknown protocols unless explicitly allowed.

Capacity checks:

- Peak QPS, request bytes, and response discard cost fit configured pod budget.
- Required pods fit namespace quota and cluster allocatable CPU/memory.
- Object storage read bandwidth and request rate fit provider limits.
- Target host allowlist matches the replay target.
- NetworkPolicy, DNS, TLS, and auth configuration are valid.
- The first N seconds of data can be prefetched within the warmup window.

Safety checks:

- Target URL is not the original production host unless explicitly allowed.
- Destructive HTTP methods can be blocked or require an override.
- Header rewrite policy removes captured credentials unless explicitly allowed.
- Max run duration, max total requests, and max spend estimates are bounded.

## In-Flight Validation

Replay agents emit metrics per pod and per lease:

- `replay_requests_expected_total`
- `replay_requests_sent_total`
- `replay_requests_failed_total`
- `replay_schedule_lag_seconds`
- `replay_send_latency_seconds`
- `replay_agent_queue_depth`
- `replay_storage_prefetch_bytes_total`
- `replay_storage_prefetch_errors_total`
- `replay_lease_lag_seconds`

The controller aggregates bucket-level health:

- Expected vs sent count per bucket.
- P50/P95/P99 schedule lag.
- Error rate by route and status code.
- Active, warmed, lagging, and failed agents.
- Bundle prefetch throughput and cache hit rate.

Abort conditions should be explicit in the `TrafficReplay` spec. Defaults:

- More than 5 percent send failures for 3 consecutive buckets.
- P99 schedule lag greater than 5 seconds for 3 consecutive buckets.
- Any lease has no heartbeat for 30 seconds.
- Target 5xx rate exceeds configured threshold.
- Object storage read errors exceed 1 percent.

The final run report should include the same bucket-level metrics used during
the run, plus lease retries, aborted lease IDs, pod scale history, and the
manifest digest that was replayed.

## Implementation Plan

1. Add replay dataset preparation as an offline Job and CLI path.
2. Define `ReplayDataset` and `TrafficReplay` CRDs with status conditions.
3. Implement manifest/index readers and preflight validation.
4. Implement aggregation, dataset handler, and bundle writing.
5. Implement replay controller leases and replay sidecar streaming scheduler.
6. Add planned scaling from the time bucket index.
7. Add in-flight metrics, abort policy, and final run report.
8. Teach capture agents or a compactor to emit bundle metadata sidecars
   directly.

## Future: Sensor Connectivity Pattern

Hub connectivity should use the same dynamic-registration idea as spokes, but
generalized for sensors. The hub should not keep a hardcoded list of expected
sensors. Instead, sensors self-identify, advertise capabilities, and heartbeat.

Proposed shape:

- `RegisterSensor`: sensor sends ID, type, version, namespace, pod, node,
  owner reference, and capabilities.
- `SensorHeartbeat`: sensor reports connection health, last successful hub RPC,
  observed latency, active leases or captures, and local readiness.
- `DeregisterSensor`: graceful shutdown removes the live registry entry.
- Hub registry is keyed by sensor ID and expires entries by heartbeat timeout.
- Hub status and query APIs expose discovered sensors and their latest health.

Initial sensor types:

- `spoke-controller`: proves controller-to-hub connectivity.
- `capture-agent`: proves capture pods can reach the hub when hub reporting is
  needed.
- `replay-sidecar`: proves replay workers can reach the hub before start
  barriers and lease assignment.
- `storage-probe`: proves storage credentials and object paths are usable from
  the workload namespace.

This should be implemented later as a generic registration service or as an
extension of the existing hub gRPC service. Either way, sensors are discovered
by registration and liveness, not configured statically in the hub.

## Open Questions

- Whether gRPC replay needs unary-only support first, or whether streaming RPCs
  should be represented in v1.
- Whether response capture should be added for validation beyond send success.
- How much PII and credential redaction should happen at capture time versus
  prepare time.
- Which object store should be the first reference implementation for large
  replay datasets.
- Whether bundle indexes should include byte offsets for seekable gzip formats,
  or whether bundles should stay small enough for simple streaming gzip.
