# Kapture

A Kubernetes traffic capture controller that uses **Gateway API RequestMirror filters** for non-invasive, declarative traffic recording across single or multi-cluster environments.

## Overview

Kapture intercepts live HTTP and gRPC traffic flowing through Gateway API routes by injecting `RequestMirror` filters that duplicate requests to capture agent pods. Captured data is written to pluggable storage backends (S3, GCS, EFS, EBS) in compressed JSONL format.

**Documentation:**
[Quickstart](docs/quickstart.md) ·
[CRD Reference](docs/crd-reference.md) ·
[Troubleshooting](docs/troubleshooting.md) ·
[Multi-Cell Load Testing](docs/multi-cell-load-testing.md) ·
[Replay Engine ABI](docs/replay-engine-abi.md) ·
[TLS & Rotation](docs/tls-rotation.md)

### Key Features

- **Non-invasive capture** via Gateway API RequestMirror -- no sidecars, no code changes
- **Hub-spoke architecture** for multi-cluster orchestration
- **Pluggable storage** -- S3, GCS, EFS, EBS backends
- **Auto-scaling** capture agents via HPA
- **Declarative** -- everything is a Kubernetes Custom Resource
- **Graceful lifecycle** -- finalizers ensure cleanup on deletion

## Architecture

```
                    ┌─────────────────────────────────┐
                    │           Hub Cluster            │
                    │                                  │
                    │  ┌──────────────────────────┐   │
                    │  │    CaptureHub Controller  │   │
                    │  │    (gRPC Server :9443)    │   │
                    │  └──────────┬───────────────┘   │
                    └─────────────┼───────────────────┘
                          gRPC    │   (register, heartbeat,
                                  │    report status)
              ┌───────────────────┼───────────────────────┐
              │                   │                        │
    ┌─────────▼─────────┐   ┌────▼──────────────┐   ┌────▼──────────────┐
    │   Spoke Cluster A  │   │  Spoke Cluster B   │   │  Spoke Cluster C   │
    │                    │   │                    │   │                    │
    │ ┌────────────────┐ │   │ ┌────────────────┐ │   │ ┌────────────────┐ │
    │ │Spoke Controller│ │   │ │Spoke Controller│ │   │ │Spoke Controller│ │
    │ └───────┬────────┘ │   │ └───────┬────────┘ │   │ └───────┬────────┘ │
    │         │          │   │         │          │   │         │          │
    │    Reconciles:     │   │    Reconciles:     │   │    Reconciles:     │
    │  - Deployment      │   │  - Deployment      │   │  - Deployment      │
    │  - Service         │   │  - Service         │   │  - Service         │
    │  - HPA             │   │  - HPA             │   │  - HPA             │
    │  - Mirror Filter   │   │  - Mirror Filter   │   │  - Mirror Filter   │
    └────────────────────┘   └────────────────────┘   └────────────────────┘
```

### Request Flow

```
Client → Gateway → HTTPRoute ──┬──→ Backend Service (original)
                                │
                                └──→ Capture Agent (mirrored copy)
                                        │
                                        ▼
                                   Storage Backend
                                   (S3/GCS/EFS/EBS)
```

1. A `TrafficCapture` CR is created referencing an `HTTPRoute` (or `GRPCRoute`) and a `CaptureStorage`
2. The spoke controller creates a capture agent `Deployment`, `Service`, and `HPA`
3. A `RequestMirror` filter is injected into the target route
4. Mirrored traffic flows to capture agents, which batch and write to storage
5. On deletion, the finalizer removes the mirror filter and owned resources are garbage collected

For a detailed end-to-end sequence diagram of provisioning, mirror filter
injection, active capture, and finalizer-guarded teardown, open
[docs/design/capture-lifecycle.html](docs/design/capture-lifecycle.html) in a browser.

## Components

### Hub Controller (`cmd/hub`)

Central orchestration point for multi-cluster deployments. Manages the `CaptureHub` CR and runs a gRPC server that:

- Accepts spoke registrations
- Monitors spoke health via heartbeats (30s interval, 90s timeout)
- Aggregates capture status across all spokes
- Provides query APIs (ListCaptures, GetCaptureStatus, ListSpokes)

### Spoke Controller (`cmd/spoke`)

Per-cluster controller that reconciles `TrafficCapture` CRs. For each capture it:

- Validates the referenced `CaptureStorage` exists and is `Ready`
- Creates/updates a capture agent `Deployment` with environment-based configuration
- Creates/updates a `ClusterIP` Service for the agent
- Creates/updates an `HPA` with CPU-based autoscaling
- Injects a `RequestMirror` filter into the target `HTTPRoute` or `GRPCRoute`
- Reports status to the hub (best-effort, non-blocking)

The spoke can run standalone (without a hub) or connected to a hub for multi-cluster visibility.

### Capture Agent (`cmd/capture-agent`)

A multi-protocol sink server that receives mirrored traffic:

- **HTTP sink** (port 8080) -- catches all mirrored HTTP requests
- **gRPC sink** (port 9090) -- catches all mirrored gRPC calls via `UnknownServiceHandler`
- **Health/metrics** (port 8081) -- `/healthz` and `/metrics` endpoints

Captured requests are batched via a `BufferedWriter` (configurable batch size and flush interval) and written to storage as gzip-compressed JSONL files.

## Custom Resources

### TrafficCapture (namespaced)

Defines a traffic capture request.

```yaml
apiVersion: capture.gateway.io/v1alpha1
kind: TrafficCapture
metadata:
  name: my-capture
  namespace: default
spec:
  targetRef:
    group: gateway.networking.k8s.io
    kind: HTTPRoute          # or GRPCRoute
    name: my-route
  storageRef:
    name: my-storage
  capture:
    includeHeaders: true
    includeBody: true
    maxBodyBytes: 1048576    # 1MB
    duration: "1h"           # optional auto-stop
  filters:                   # optional
    headers:
      - name: x-debug
        value: "true"
    pathPrefix: "/api/v2"
    percentage: 50
  agent:                     # optional scaling config
    replicas: 2
    minReplicas: 1
    maxReplicas: 10
    targetCPU: 70
```

**Status fields:** `phase` (Pending/Active/Completed/Failed), `startTime`, `capturedRequests`, `bytesWritten`, `agentStatus`, `conditions`

### CaptureStorage (namespaced)

Defines a reusable storage backend.

```yaml
apiVersion: capture.gateway.io/v1alpha1
kind: CaptureStorage
metadata:
  name: my-s3-storage
spec:
  type: S3                   # S3, GCS, EFS, or EBS
  s3:
    bucket: my-capture-bucket
    region: us-east-1
    prefix: captures/
    credentialsSecretRef:
      name: s3-credentials
  retention:
    maxAge: "7d"
    maxSize: "100Gi"
```

**Storage types:**

| Type | Required fields |
|------|----------------|
| S3 | `bucket`, `region`, `credentialsSecretRef` |
| GCS | `bucket`, `credentialsSecretRef` |
| EFS | `fileSystemID`, `mountPath` |
| EBS | `volumeID`, `mountPath` |

### CaptureHub (cluster-scoped)

Singleton hub configuration for multi-cluster mode.

```yaml
apiVersion: capture.gateway.io/v1alpha1
kind: CaptureHub
metadata:
  name: global-hub
spec:
  grpcAddress: ":9443"
  tls:
    certSecretRef:
      name: hub-tls-cert
  authentication:
    type: mTLS
```

**Status fields:** `connectedSpokes`, `activeCaptures`, `activeReplays`, `spokes[]` (name, cell, lastHeartbeat, activeCaptures, activeReplays), `cells[]` (per-cell aggregates)

### TrafficReplay (namespaced)

Replays captured data against a target service. Runs as a `replay-engine`
Job on the spoke cluster. Usually created automatically as one shard of a
`CaptureLoadTest`, but can also be created directly for standalone replays.

```yaml
apiVersion: capture.gateway.io/v1alpha1
kind: TrafficReplay
metadata:
  name: orders-replay
  namespace: prod
spec:
  sourceRef:
    name: orders
  storageRef:
    name: s3-captures
  target:
    host: staging-api.internal
    port: 8443
    tls: true
  rate:
    mode: OriginalTiming    # Constant | OriginalTiming | Unlimited
  concurrency: 50
```

### Replay engines

Replays and load tests run through pluggable **replay engines** behind a
versioned gRPC ABI ([docs/replay-engine-abi.md](docs/replay-engine-abi.md)).
The `builtin` engine (byte-exact HTTP sender) is the default; `k6` and
`ghz` adapters ship out of the box (`make build-engines`), and third-party
engines plug in as `kapture-engine-<name>` subprocess binaries with hot
reload. Kapture streams already-sharded, already-paced requests to the
engine — engines never touch storage, and captures are never downloaded
wholesale. Select per replay or load test:

```yaml
spec:
  engine:
    name: k6
    config:
      vus: 200
```

### CaptureLoadTest (namespaced, hub cluster)

Distributed load test that replays one capture from many spoke clusters at
once. The hub selects connected spokes (optionally restricted to cells),
splits the capture into deterministic hash shards, sends replay directives
over the spoke gRPC streams, and aggregates shard progress into the status.
See [docs/multi-cell-load-testing.md](docs/multi-cell-load-testing.md).

```yaml
apiVersion: capture.gateway.io/v1alpha1
kind: CaptureLoadTest
metadata:
  name: orders-peak-replay
  namespace: prod
spec:
  sourceRef:
    name: orders
  storageRef:
    name: s3-captures
  target:
    host: staging-api.internal
  rate:
    mode: Constant
    requestsPerSecond: 50000     # aggregate across all shards
  distribution:
    cells: ["cell-us-east-1a", "cell-us-east-1b"]
    workersPerSpoke: 4
    concurrencyPerWorker: 50
  abort:
    maxDuration: 30m
    errorPercent: 5
```

## Captured Data Format

Requests are stored as gzip-compressed JSONL files at:

```
{prefix}/{captureID}/{YYYY-MM-DD}/{pod-name}-{sequence}.jsonl.gz
```

Each line is a JSON object:

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
  "metadata": {"host": "api.example.com", "bodyTruncated": "false"}
}
```

For the proposed replay-oriented dataset format and large-scale load testing
plan, see [Replay Storage and Load Testing Design](docs/replay-storage-and-load-testing.md).

## Installation

### Prerequisites

- Kubernetes 1.28+
- [Gateway API CRDs](https://gateway-api.sigs.k8s.io/) installed
- Helm 3.x

### Install Gateway API CRDs

```bash
kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.2.1/standard-install.yaml
```

### Install via Helm

```bash
# Add CRDs
kubectl apply -f config/crd/bases/

# Install the chart
helm install kapture charts/kapture \
  --namespace capture-system \
  --create-namespace \
  --set hub.enabled=true \
  --set spoke.enabled=true \
  --set spoke.hub.address=kapture-hub.capture-system.svc:9443
```

### Helm Values

| Key | Default | Description |
|-----|---------|-------------|
| `installCRDs` | `true` | Install CRDs with chart |
| `hub.enabled` | `true` | Deploy hub controller |
| `hub.image.repository` | `kapture/hub` | Hub image |
| `hub.image.tag` | `dev` | Hub image tag |
| `hub.replicas` | `1` | Hub replicas |
| `hub.service.type` | `ClusterIP` | Hub service type |
| `hub.service.port` | `9443` | Hub gRPC port |
| `hub.tls.secretName` | `""` | TLS secret for hub |
| `hub.irsa.enabled` | `false` | Add `eks.amazonaws.com/role-arn` to hub ServiceAccount |
| `hub.irsa.roleArn` | `""` | AWS IAM role ARN for hub IRSA |
| `spoke.enabled` | `true` | Deploy spoke controller |
| `spoke.image.repository` | `kapture/spoke` | Spoke image |
| `spoke.image.tag` | `dev` | Spoke image tag |
| `spoke.replicas` | `1` | Spoke replicas |
| `spoke.hub.address` | `""` | Hub gRPC address (empty = standalone) |
| `spoke.irsa.enabled` | `false` | Add `eks.amazonaws.com/role-arn` to spoke ServiceAccount |
| `spoke.irsa.roleArn` | `""` | AWS IAM role ARN for spoke IRSA |
| `agent.image.repository` | `kapture/agent` | Capture agent image |
| `agent.image.tag` | `dev` | Agent image tag |

### AWS IRSA Example (EKS)

Use IRSA to grant hub/spoke pods AWS permissions without static secrets:

```bash
helm install kapture charts/kapture \
  --namespace capture-system \
  --create-namespace \
  --set hub.irsa.enabled=true \
  --set hub.irsa.roleArn=arn:aws:iam::123456789012:role/kapture-hub \
  --set spoke.irsa.enabled=true \
  --set spoke.irsa.roleArn=arn:aws:iam::123456789012:role/kapture-spoke
```

### Standalone Mode

If you only need single-cluster capture without hub orchestration:

```bash
helm install kapture charts/kapture \
  --namespace capture-system \
  --create-namespace \
  --set hub.enabled=false \
  --set spoke.enabled=true
```

## Quick Start

1. **Create a storage backend:**

```yaml
apiVersion: capture.gateway.io/v1alpha1
kind: CaptureStorage
metadata:
  name: my-storage
spec:
  type: S3
  s3:
    bucket: my-capture-bucket
    region: us-east-1
    credentialsSecretRef:
      name: s3-creds
```

2. **Start capturing traffic:**

```yaml
apiVersion: capture.gateway.io/v1alpha1
kind: TrafficCapture
metadata:
  name: my-capture
spec:
  targetRef:
    group: gateway.networking.k8s.io
    kind: HTTPRoute
    name: my-route
  storageRef:
    name: my-storage
  capture:
    includeHeaders: true
    includeBody: true
```

3. **Monitor the capture:**

```bash
kubectl get trafficcaptures
kubectl describe trafficcapture my-capture
```

4. **Stop capturing:**

```bash
kubectl delete trafficcapture my-capture
```

The finalizer automatically removes the mirror filter from the route and owned resources (Deployment, Service, HPA) are garbage collected.

## Development

### Prerequisites

- Go 1.25+
- Docker
- [buf](https://buf.build/) (for protobuf generation)
- [Helm](https://helm.sh/) (for chart testing)

### Build

```bash
# Build all binaries
make build

# Build Docker images
make docker-build

# Generate protobuf code
make generate-proto
```

### Project Structure

```
.
├── api/v1alpha1/              # CRD type definitions
│   ├── trafficcapture_types.go
│   ├── capturestorage_types.go
│   ├── capturehub_types.go
│   └── zz_generated.deepcopy.go
├── cmd/
│   ├── hub/                   # Hub controller binary
│   ├── spoke/                 # Spoke controller binary
│   └── capture-agent/         # Capture agent binary
├── internal/
│   ├── hub/                   # Hub reconciler + gRPC server
│   ├── spoke/                 # Spoke reconciler + mirror logic
│   ├── agent/                 # Capture handler + buffered writer
│   └── storage/               # Pluggable storage backends
├── proto/hub/v1/              # gRPC protobuf definitions
├── config/crd/bases/          # Generated CRD YAML
├── charts/kapture/  # Helm chart
├── test/
│   ├── integration/           # envtest-based integration tests
│   └── e2e/                   # End-to-end tests (Kind cluster)
├── .github/workflows/
│   ├── ci.yaml                # Unit/integration/lint/helm CI
│   └── e2e.yaml               # E2E tests on Kind cluster
├── Dockerfile.{hub,spoke,agent}
└── Makefile
```

## Testing

The project has four layers of testing:

### Unit Tests

Fast, isolated tests for all packages with mocked dependencies.

```bash
go test ./internal/... -v -race
```

**187 tests** covering:

| Package | Tests | What's tested |
|---------|-------|---------------|
| `internal/storage` | 55 | Buffered writer, JSONL marshaling, gzip compression, S3/GCS/EFS/EBS writers, factory dispatch |
| `internal/agent` | 30 | CaptureHandler (body truncation, metrics), BufferedWriter (batching, flush), HTTP/health/metrics handlers |
| `internal/spoke` | 66 | BuildDeployment/Service/HPA, env var generation, mirror filter build/detect/remove, annotations |
| `internal/hub` | 36 | All gRPC RPCs, spoke registry, heartbeat tracking, timeout detection, status aggregation |

### Integration Tests

Use [envtest](https://book.kubebuilder.io/reference/envtest) to run a real Kubernetes API server with the spoke controller.

```bash
# Install envtest binaries (one-time)
go run sigs.k8s.io/controller-runtime/tools/setup-envtest@latest use 1.31.0 --bin-dir ./testbin

# Run integration tests
KUBEBUILDER_ASSETS=./testbin/k8s/1.31.0-$(go env GOOS)-$(go env GOARCH) \
  go test ./test/integration/... -v -race -timeout 300s
```

**6 tests** covering:
- CaptureStorage CRUD for all types
- CaptureStorage status updates
- TrafficCapture creates Deployment, Service, HPA, and mirror filter
- TrafficCapture deletion cleanup (finalizer removes mirror filter)
- TrafficCapture fails when storage not found

### Helm Unit Tests

Use [helm-unittest](https://github.com/helm-unittest/helm-unittest) to test chart templates.

```bash
# Install plugin (one-time)
helm plugin install --verify=false https://github.com/helm-unittest/helm-unittest.git

# Run tests
helm unittest charts/kapture
```

**17 tests** covering hub and spoke Deployments, Services, ServiceAccounts, RBAC, TLS, conditional rendering.

### End-to-End Tests

Full lifecycle tests running against a real Kind cluster with all controllers deployed.

```bash
# Create Kind cluster + deploy (see .github/workflows/e2e.yaml for full setup)
kind create cluster --name kapture-e2e
kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.2.1/standard-install.yaml
kubectl apply -f config/crd/bases/

# Build and load images
docker build -f Dockerfile.hub -t kapture/hub:e2e .
docker build -f Dockerfile.spoke -t kapture/spoke:e2e .
docker build -f Dockerfile.agent -t kapture/agent:e2e .
kind load docker-image kapture/hub:e2e --name kapture-e2e
kind load docker-image kapture/spoke:e2e --name kapture-e2e
kind load docker-image kapture/agent:e2e --name kapture-e2e

# Deploy via Helm
helm install kapture charts/kapture \
  --namespace capture-system --create-namespace \
  --set hub.image.tag=e2e --set hub.image.pullPolicy=Never \
  --set spoke.image.tag=e2e --set spoke.image.pullPolicy=Never \
  --set agent.image.tag=e2e --set agent.image.pullPolicy=Never \
  --set spoke.hub.address=kapture-hub.capture-system.svc:9443

# Run e2e tests
go test ./test/e2e/... -v -timeout 600s
```

**8 tests** covering:
- Full TrafficCapture lifecycle (create, verify all resources, delete, verify cleanup)
- Missing storage fails with condition
- Storage not-ready blocks with pending, progresses after ready
- All storage types CRUD
- GRPCRoute mirror filter injection and removal
- Capture agent health endpoint
- CaptureHub status updates
- Multiple concurrent captures in same namespace

## CI/CD

### CI Pipeline (`.github/workflows/ci.yaml`)

Runs on every push to `main` and all PRs:

| Job | Description |
|-----|-------------|
| `lint` | `go vet ./...` |
| `unit-test` | Unit tests with race detector + coverage upload |
| `integration-test` | envtest-based integration tests |
| `helm-test` | Helm chart unit tests |
| `build` | Compilation check (depends on lint + unit-test) |

### E2E Pipeline (`.github/workflows/e2e.yaml`)

Runs on every push to `main` and all PRs:

1. Creates a 2-node Kind cluster
2. Installs Gateway API and kapture CRDs
3. Builds all 3 Docker images
4. Loads images into Kind
5. Deploys via Helm with hub-spoke connectivity
6. Runs the full e2e test suite
7. Collects controller logs, events, and CR dumps on failure

Includes concurrency control (cancels previous runs on same PR) and a 30-minute timeout.

## Hub-Spoke gRPC Protocol

The hub and spoke communicate via gRPC (defined in `proto/hub/v1/hub.proto`):

| RPC | Direction | Description |
|-----|-----------|-------------|
| `RegisterSpoke` | Spoke -> Hub | Register with cluster info, receive heartbeat interval |
| `Heartbeat` | Spoke -> Hub | Periodic health signal with capture summaries |
| `DeregisterSpoke` | Spoke -> Hub | Graceful shutdown deregistration |
| `WatchDirectives` | Hub -> Spoke (stream) | Server-streaming for future hub-initiated commands |
| `ReportCaptureStatus` | Spoke -> Hub | Push capture status snapshots |
| `ListCaptures` | Query | List captures, optionally filtered by spoke/namespace |
| `GetCaptureStatus` | Query | Get status of a specific capture |
| `ListSpokes` | Query | List all registered spokes with health state |

## RBAC

### Hub Controller

- `capture.gateway.io` CaptureHubs: get, list, watch, update, patch (+ status)
- `capture.gateway.io` TrafficCaptures: get, list, watch (+ status read)
- Events: create, patch

### Spoke Controller

- `capture.gateway.io` TrafficCaptures: full CRUD (+ status, finalizers)
- `capture.gateway.io` CaptureStorages: get, list, watch (+ status)
- `gateway.networking.k8s.io` HTTPRoutes/GRPCRoutes: get, list, watch, update, patch
- `apps/v1` Deployments: full CRUD
- `core/v1` Services: full CRUD
- `autoscaling/v2` HorizontalPodAutoscalers: full CRUD
- Events: create, patch

## License

See [LICENSE](LICENSE) for details.
