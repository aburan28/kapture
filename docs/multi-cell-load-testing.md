# Multi-Cell Distributed Load Testing

Kapture can replay a recorded capture as a distributed load test that runs
across many spoke clusters at once, coordinated by the hub. This document
describes the cell model, the `CaptureLoadTest` resource, how work is
sharded, and how results flow back to the hub.

For the storage-format roadmap (replay datasets, bundles, leases) see
[replay-storage-and-load-testing.md](replay-storage-and-load-testing.md).
This document covers what is implemented today on top of the raw JSONL
capture format.

## Cells

A **cell** is a named grouping of spoke clusters — typically an availability
zone, region, or deployment ring. Spokes declare their cell when they
register with the hub:

```bash
spoke --hub-address hub.capture-system.svc:9443 --cell cell-us-east-1a
# or via env: CELL_NAME=cell-us-east-1a
# or via Helm: --set spoke.cell=cell-us-east-1a
```

The hub tracks cells in its registry and exposes them:

- `ListCells` gRPC RPC returns per-cell aggregates (connected spokes, active
  captures, active replay shards).
- `ListSpokes` and `ListCaptures` accept a `cell` filter.
- The `CaptureHub` status lists per-cell aggregates under `status.cells`.

Cells are dynamic: the hub never needs a static cell inventory. A cell
exists exactly as long as at least one spoke registers under it.

## CaptureLoadTest

`CaptureLoadTest` is a namespaced CRD reconciled **on the hub cluster**. It
fans one capture out into replay shards across the spokes of the selected
cells:

```yaml
apiVersion: capture.gateway.io/v1alpha1
kind: CaptureLoadTest
metadata:
  name: orders-peak-replay
  namespace: prod
spec:
  sourceRef:
    name: orders            # TrafficCapture whose data is replayed
  storageRef:
    name: s3-captures       # CaptureStorage resolved on each spoke
  target:
    host: staging-api.internal
    port: 8443
    tls: true
  rate:
    mode: Constant          # Constant | OriginalTiming | Unlimited
    requestsPerSecond: 50000   # aggregate across ALL shards
  filters:
    pathPrefix: /api
    methods: ["GET", "POST"]
  distribution:
    cells: ["cell-us-east-1a", "cell-us-east-1b"]  # empty = all cells
    maxSpokes: 20
    workersPerSpoke: 4
    concurrencyPerWorker: 50
  abort:
    maxDuration: 30m
    errorPercent: 5
    minSampleRequests: 1000
```

### Lifecycle

```
Pending ──► Distributing ──► Running ──► Completed
                 │               │
                 │               ├──► Failed   (some shards failed)
                 └───────────────┴──► Aborted  (abort policy tripped)
```

1. **Pending** — the hub is waiting for eligible connected spokes in the
   requested cells. The `SpokesAssigned` condition explains what is missing.
2. **Distributing** — the hub selected spokes, assigned shard indexes, and
   is sending START directives over each spoke's directive stream. The
   assignment is recorded in `status.assignments` and stays stable for the
   run. Directives are idempotent and re-sent until every shard reports.
3. **Running** — shards are executing; the hub aggregates their reports into
   `status` every ~10 seconds (totals, per-cell rollups, achieved RPS).
4. **Completed / Failed / Aborted** — terminal. Deleting the load test sends
   STOP directives that clean up the shard resources on every spoke.

### Sharding

The capture is split into `spokes × workersPerSpoke` shards. Each shard
replays the subset of requests whose FNV-1a-hashed request ID modulo the
shard count equals its shard index, so shards are disjoint and together
cover the capture exactly once — no coordination between workers is needed
at replay time, which is what makes the fan-out scale to arbitrarily large
captures.

Rate behaviour per mode:

- **Constant**: `spec.rate.requestsPerSecond` is the aggregate target. The
  hub divides it across shards (remainder goes to the lowest shard indexes),
  so shard rates sum exactly to the configured aggregate.
- **OriginalTiming** (default): each shard preserves the recorded
  inter-request delays of its own subset. Because shards partition the
  recorded timeline, all shards together reproduce the recorded aggregate
  QPS — including bursts. `timeScale` speeds up or slows down every shard.
- **Unlimited**: every shard sends as fast as the target accepts.

### Replay engines

Each shard runs a replay engine. The default `builtin` engine is the
native byte-exact HTTP sender; `spec.engine` selects an alternative that
runs as a gRPC subprocess plugin on the replay worker — `k6` and `ghz`
ship out of the box, and any binary implementing the engine ABI plugs in
the same way (including hot reload of plugin binaries without restarting
workers). Engines receive an already-sharded, already-paced request
stream; they never touch storage. See
[replay-engine-abi.md](replay-engine-abi.md) for the contract, the OOTB
engine matrix, and packaging.

### On the spoke

Each START directive is materialised as a `TrafficReplay` resource named
`<loadtest>-shard-<n>` in the load test's namespace, labelled
`capture.gateway.io/load-test` and `capture.gateway.io/shard-index`. The
spoke's TrafficReplay controller runs each shard as a Kubernetes **Job**
executing the `replay-engine` binary with `--shard-index/--shard-count` and
the storage/target/rate flags derived from the spec.

When the Job finishes, the replay engine writes a JSON run report to
`/dev/termination-log`; the spoke controller scrapes it into the
`TrafficReplay` status (counts, achieved RPS, p50/p95/p99 latencies) and
reports the shard summary to the hub via `ReportReplayStatus`. Shard
summaries are also piggybacked on every heartbeat, so the hub's view heals
automatically after lost reports or hub restarts.

`TrafficReplay` resources created directly by users (without
`spec.loadTestRef`) behave the same way but are not reported to the hub.

### Abort policy

The hub enforces `spec.abort` while the run is live:

- `maxDuration` — wall-clock limit from the run's start time.
- `errorPercent` — aborts when failed requests exceed this percentage of
  attempted requests, evaluated only after `minSampleRequests` attempts
  (default 100) so a slow start cannot trip it.

An abort sends STOP directives to every assigned spoke, deletes the shard
resources, and records the reason in the `Aborted` condition.

### Observability

```bash
# Hub cluster
kubectl get captureloadtests -A
kubectl describe captureloadtest orders-peak-replay -n prod

# Per-cell / per-spoke placement and progress
kubectl get captureloadtest orders-peak-replay -n prod -o jsonpath='{.status.cells}'

# Spoke cluster: individual shards
kubectl get trafficreplays -n prod -l capture.gateway.io/load-test=orders-peak-replay
```

The hub also exposes the same data over gRPC (`ListReplays`, filterable by
load test and cell) for dashboards and the UI.

## Hub-Spoke protocol additions

The `hub.v1.HubService` gRPC API gained:

| RPC / field | Purpose |
|-------------|---------|
| `RegisterSpokeRequest.cell`, `SpokeInfo.cell` | Cell membership |
| `ListCells` | Per-cell aggregates |
| `ListReplays` | Shard statuses across the fleet |
| `WatchDirectivesResponse.replay_directive` | START/STOP replay shards |
| `HeartbeatRequest.replay_summaries` | Piggybacked shard progress |
| `ReportReplayStatus` | Immediate shard status reporting |

Directives flow hub → spoke over the existing `WatchDirectives` stream;
status flows spoke → hub via heartbeats and explicit reports. The hub keeps
the registry in memory — a hub restart loses in-flight shard summaries, but
heartbeats repopulate them within one interval, and the recorded
`status.assignments` on the CaptureLoadTest let the coordinator re-send
directives idempotently.

Heartbeat responses also carry the hub's authoritative CaptureLoadTest
list: spokes garbage-collect local shards whose load test no longer exists
(`internal/spoke/orphan_gc.go`), so even a STOP directive lost in a hub
crash window cannot leave orphaned load generators running. This is the
`OrphanGC` action in the TLA+ model.

## Verification

Two layers of machine-checked verification back the sharding and
coordination claims above:

- **TLA+ model checking** (`verification/tla/`, run by
  `.github/workflows/tla.yaml` and `make verify-tla`): `Sharding.tla`
  checks that shard slices are disjoint and exhaustive for *every*
  possible hash assignment, including across Job retries;
  `KaptureLoadTest.tla` checks the hub-spoke coordination protocol under
  full directive buffers, duplicate directive resends, shard failures,
  and a hub restart — including that deletion and abort can never orphan
  running shards. See `verification/tla/README.md` for the property
  catalogue and the explicit modelling assumptions.
- **End-to-end tests** (`test/e2e/loadtest_test.go`, run by the e2e
  workflow against a Kind cluster): a real `CaptureLoadTest` fans out
  across shards on a cell-registered spoke, replaying a seeded capture
  against a counting sink. The sink's request counter must equal the
  seeded request count exactly — a duplicate shard slice would push it
  over, a dropped slice under — and per-shard sent counts must sum to the
  same total. The suite also covers cell targeting, `Pending` behaviour
  for empty cells, and shard cleanup on deletion.

## Sizing guidance

- One replay worker (shard) with `concurrencyPerWorker: 50` sustains roughly
  1-5k RPS for small-body HTTP traffic, dominated by target latency; use
  `workersPerSpoke` to add parallel readers on large spokes.
- By default every worker streams and decompresses the **full** capture
  and skips requests owned by other shards, so worker count multiplies
  storage read bandwidth: budget `shards × compressed-capture-size` of
  object-store egress per run. **Preshard large captures to remove this
  cost** (next section).
- Prefer more spokes over more workers per spoke when the target is remote:
  cells spread source load and avoid a single egress bottleneck.

## Pre-sharded captures

For large captures, run `kapture-preshard` once before the load test to
re-partition the capture into per-shard slices using the same FNV-1a
assignment the runtime filter uses:

```bash
# ships at /kapture-preshard in the replay-engine image; run as a Job
kapture-preshard \
  --storage-type s3 --bucket my-captures --region us-east-1 \
  --capture-id prod/orders --shards 32
```

This writes `{captureID}/shards/{i}-of-{n}/...` slices in the standard
capture layout (single streaming pass, any storage backend). Then set:

```yaml
spec:
  distribution:
    presharded: true      # workers read only their own slice
    maxSpokes: 8
    workersPerSpoke: 4    # spokes × workers must equal --shards (32)
```

Each worker now reads `1/n` of the capture with no hash filtering —
aggregate storage egress drops from `n × capture-size` to `1 ×
capture-size`. The slice/filter equivalence (a request lands in slice `i`
iff the runtime filter would keep it on shard `i`) is enforced by both
using `replay.ShardOwns`, and covered by a round-trip test through real
storage writers and readers.

## Securing the hub-spoke channel (mTLS)

The hub gRPC server takes its certificates from the CaptureHub CR:

```yaml
apiVersion: capture.gateway.io/v1alpha1
kind: CaptureHub
metadata:
  name: global-hub
spec:
  grpcAddress: ":9443"
  tls:
    certSecretRef:
      name: hub-tls          # tls.crt/tls.key + ca.crt (client CA)
  authentication:
    type: mTLS               # spokes must present certs signed by ca.crt
```

Spokes mount their client certificate secret via Helm:

```bash
--set spoke.hub.tls.secretName=spoke-hub-tls \
--set spoke.hub.tls.serverName=kapture-hub.capture-system.svc
```

Without `authentication.type: mTLS` the hub serves TLS without requiring
client certificates. Certificate rotation currently requires changing the
secret name (or restarting the hub); in-place rotation is future work.

With mTLS enabled, the hub also **binds spoke identity to the
certificate**: a spoke may only register, heartbeat, watch directives, or
report status as the `spoke_id` matching its client certificate's
CommonName or a DNS SAN. Issue one certificate per spoke with the spoke
name as CN — a shared fleet certificate would let any spoke impersonate
any other.

## Target safety and credential handling

- **Target allowlist**: set `spec.safety.allowedHosts` on a
  CaptureLoadTest (or TrafficReplay) to fence where replayed traffic may
  go — exact hostnames or `*.subdomain` wildcards. The hub fails the load
  test with a `TargetAllowed: False` condition before distributing any
  shard, and every spoke re-checks before creating a worker Job. This is
  the guard against pointing recorded production traffic back at
  production.
- **Credential redaction**: capture agents replace the values of
  credential headers (Authorization, Proxy-Authorization, Cookie,
  Set-Cookie, X-Api-Key, X-Auth-Token) with `[REDACTED]` before requests
  become durable capture data. Add more via
  `spec.capture.redactHeaders`; opt out (trusted pipelines only) with
  `spec.capture.disableHeaderRedaction: true`. Replays inject fresh
  credentials via engine header overrides instead of resending recorded
  ones.
- **Empty-replay preflight**: replay workers fail (instead of completing
  with zero requests) when the capture ID resolves to no data — the
  usual causes are a typo or a `presharded: true` run whose slice layout
  was never built with `kapture-preshard`. Override with
  `--fail-on-empty=false` for intentionally-empty replays.
