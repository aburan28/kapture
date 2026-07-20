# Quickstart

This walkthrough goes from an empty cluster to a distributed load test
replaying captured production traffic: install → capture → preshard →
load test → results.

Prerequisites:

- A Kubernetes cluster (1.29+) with the [Gateway API](https://gateway-api.sigs.k8s.io/) CRDs and a
  Gateway implementation that supports the `RequestMirror` filter
- Helm 3
- Traffic flowing through an `HTTPRoute` or `GRPCRoute`

## 1. Install

Single-cluster (hub and spoke in one cluster):

```bash
helm install kapture charts/kapture \
  --namespace capture-system --create-namespace
```

Multi-cluster: install the hub in the central cluster
(`--set spoke.enabled=false`), then a spoke in each workload cluster
pointing at it:

```bash
helm install kapture charts/kapture \
  --namespace capture-system --create-namespace \
  --set hub.enabled=false \
  --set spoke.hub.address=hub.example.internal:9443 \
  --set spoke.cell=cell-us-east
```

`spoke.cell` groups spokes into named cells (availability zones,
deployment rings); load tests target cells. For mTLS between hub and
spokes see [tls-rotation.md](tls-rotation.md).

Create the hub configuration (cluster-scoped, hub cluster only):

```yaml
apiVersion: capture.gateway.io/v1alpha1
kind: CaptureHub
metadata:
  name: kapture-hub
spec:
  grpcAddress: ":9443"
```

## 2. Capture traffic

Define where captured data goes, then what to capture:

```yaml
apiVersion: capture.gateway.io/v1alpha1
kind: CaptureStorage
metadata:
  name: orders-storage
  namespace: shop
spec:
  type: S3
  s3:
    bucket: kapture-data
    region: us-east-1
    credentialsSecretRef:
      name: s3-credentials
---
apiVersion: capture.gateway.io/v1alpha1
kind: TrafficCapture
metadata:
  name: orders-capture
  namespace: shop
spec:
  targetRef:
    group: gateway.networking.k8s.io
    kind: HTTPRoute
    name: orders-route
  storageRef:
    name: orders-storage
  capture:
    includeHeaders: true
    includeBody: true
    maxBodyBytes: 1048576
  filters:
    percentage: 100        # sample rate; lower for very hot routes
```

The spoke controller injects a `RequestMirror` filter into the route and
runs capture agent pods that write compressed JSONL to storage (or SQL
rows with the `RDS` storage type, backed by PostgreSQL/Aurora).
Credential headers (Authorization, Cookie, …) are redacted by default.

Each agent also maintains streaming statistics over the capture —
per-window counters, unique clients/flows (HyperLogLog), heavy-hitter
paths and IPs (Count-Min), body-size and latency quantiles (DDSketch) —
served at `:8081/stats`, on the dashboard's Analytics page, and —
with `--set stats.redis.addr=redis.observability.svc:6379` on the
spoke's Helm install — published to Redis
(`kapture:stats:{captureID}` snapshots plus a `...:windows` stream).
Watch progress:

```bash
kubectl get trafficcapture -n shop
kubectl get capturestorage -n shop
```

When done, delete the TrafficCapture (the mirror filter is removed; the
data stays in storage). The capture's dataset ID is
`<namespace>/<name>`, e.g. `shop/orders-capture`.

## 3. Preshard the dataset (recommended for load tests)

Presharding splits a capture into per-worker slices once, so N replay
workers each read 1/N of the data instead of all of it:

```bash
kapture-preshard \
  --storage-type s3 --s3-bucket kapture-data --s3-region us-east-1 \
  --capture-id shop/orders-capture \
  --shard-count 8
```

This writes `shop/orders-capture/shards/{i}-of-8/...` slices plus a
manifest per slice (record count, checksum) that replays verify before
streaming. The shard count must equal the load test's total workers
(participating spokes × `workersPerSpoke`).

## 4. Run a distributed load test

On the hub cluster:

```yaml
apiVersion: capture.gateway.io/v1alpha1
kind: CaptureLoadTest
metadata:
  name: orders-peak
  namespace: shop
spec:
  sourceRef:
    name: orders-capture
  storageRef:
    name: orders-storage      # must exist on every participating spoke
  target:
    host: orders.staging.internal
    port: 8080
  rate:
    mode: Constant            # aggregate rate, divided across workers
    requestsPerSecond: 2000
  distribution:
    cells: ["cell-us-east", "cell-eu-west"]
    workersPerSpoke: 2
    concurrencyPerWorker: 25
    presharded: true          # read the preshard layout from step 3
  engine:
    name: k6                  # builtin | k6 | ghz | any installed plugin
  safety:
    allowedHosts: ["*.staging.internal"]
  abort:
    maxDuration: 30m
    errorPercent: 10
```

The hub fans shards out to the spokes of the selected cells; each spoke
runs replay-engine Jobs; shard progress streams back on heartbeats.
`safety.allowedHosts` rejects the run before any traffic is generated
if the target is not allowlisted — set it. The `abort` policy stops all
shards fleet-wide when breached.

## 5. Read results

```bash
kubectl get captureloadtest orders-peak -n shop -o wide
kubectl describe captureloadtest orders-peak -n shop   # Events tell the story
```

The status carries aggregate counters (`sentRequests`,
`failedRequests`, `achievedRPS`), per-cell breakdowns, and the
spoke → shard assignment map. Per-shard latency percentiles
(P50/P95/P99) are on each spoke's TrafficReplay status:

```bash
kubectl get trafficreplays -n shop -l capture.gateway.io/load-test=orders-peak
```

The web dashboard (`ui/`) shows the same data live: load tests, shard
progress, cells, and replays. Deleting the CaptureLoadTest stops all
shards on all spokes first, then releases its finalizer.

## Standalone replays

For a single-cluster replay without hub coordination, create a
TrafficReplay directly on a spoke cluster — same fields, one worker (or
an explicit `shard` for manual sharding).

## Next steps

- [crd-reference.md](crd-reference.md) — every field of the five CRDs
- [troubleshooting.md](troubleshooting.md) — common failure modes
- [multi-cell-load-testing.md](multi-cell-load-testing.md) — architecture
- [replay-engine-abi.md](replay-engine-abi.md) — writing engine plugins
