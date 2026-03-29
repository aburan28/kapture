# CLAUDE.md - Kapture Project Guide

## Project Overview

Kapture is a Kubernetes traffic capture controller that enables non-invasive, declarative traffic recording using **Gateway API RequestMirror filters**. It uses a hub-spoke architecture for multi-cluster orchestration, duplicating live HTTP/gRPC traffic to capture agent pods without sidecars or code changes. Captured data is written to pluggable storage backends in compressed JSONL format.

## Architecture

```
Hub Cluster (central orchestration via gRPC)
  └── Spoke Clusters (per-cluster controllers)
       └── Capture Agents (traffic sink servers)
            └── Storage Backends (S3/GCS/EFS/EBS/Plugin)
```

Three Custom Resources:
- **TrafficCapture** (namespaced) - Defines what traffic to capture from HTTPRoute/GRPCRoute
- **CaptureStorage** (namespaced) - Configures storage backend (S3, GCS, EFS, EBS, Plugin)
- **CaptureHub** (cluster-scoped) - Central hub for multi-cluster coordination

## Directory Structure

```
api/v1alpha1/          # CRD type definitions (TrafficCapture, CaptureStorage, CaptureHub)
cmd/
  hub/                 # Hub controller entrypoint
  spoke/               # Spoke controller entrypoint
  capture-agent/       # Capture agent entrypoint
internal/
  hub/                 # Hub gRPC server + CaptureHub reconciler
  spoke/               # Spoke controller, mirror injection, agent deployer, hub client
  agent/               # HTTP/gRPC capture handler, buffered writer, server
  storage/             # Storage backends (s3, gcs, efs, ebs, plugin) + factory
proto/hub/v1/          # Protobuf definitions and generated gRPC stubs
config/crd/bases/      # Generated CRD YAML manifests
charts/kapture/        # Helm chart (templates, values, CRDs, unit tests)
test/
  integration/         # envtest-based integration tests
  e2e/                 # Kind cluster end-to-end tests
ui/                    # Next.js web dashboard (React 19, Tailwind CSS)
.github/workflows/     # CI (ci.yaml) and E2E (e2e.yaml) pipelines
```

## Tech Stack

- **Go 1.26** with controller-runtime v0.23, gateway-api v1.5, gRPC v1.79
- **Protobuf** via Buf for hub-spoke communication
- **Helm 3** for Kubernetes deployment
- **Next.js 16** / React 19 / TypeScript 5.9 / Tailwind CSS 4 for the UI
- **Vitest** for UI tests, **Go test** + **envtest** for backend tests

## Common Commands

### Build & Lint
```bash
make build              # go build ./...
make lint               # go vet ./...
make generate           # Regenerate protobuf + CRD manifests
make generate-proto     # buf lint + buf generate
make docker-build       # Build hub, spoke, agent Docker images
```

### Testing
```bash
# Unit tests (Go backend)
go test ./internal/... -v -race

# Integration tests (requires envtest binaries)
KUBEBUILDER_ASSETS=$(go run sigs.k8s.io/controller-runtime/tools/setup-envtest@latest use 1.31.0 --bin-dir ./testbin -p path)
go test ./test/integration/... -v -race -timeout 300s

# Helm unit tests
helm unittest charts/kapture

# E2E tests (requires Kind cluster with Gateway API CRDs)
go test ./test/e2e/... -v -timeout 30m

# UI tests
cd ui && npm test
```

### UI Development
```bash
cd ui
npm install
npm run dev             # Start dev server with Turbopack
npm run build           # Production build
npm run test            # Run Vitest suite
```

## CI Pipeline

Defined in `.github/workflows/ci.yaml` (runs on push to main + all PRs):
1. **lint** - `go vet ./...`
2. **unit-test** - Go tests with race detector + coverage
3. **integration-test** - envtest-based tests (300s timeout)
4. **helm-test** - Helm unittest plugin
5. **build** - `go build ./...` (depends on lint + unit-test)

E2E pipeline in `.github/workflows/e2e.yaml` deploys to a Kind cluster.

## Code Conventions

### Go Patterns
- **Kubebuilder markers** for RBAC, validation, CRD generation (e.g., `// +kubebuilder:rbac:...`)
- **controller-runtime reconciliation** pattern with `Reconcile(ctx, req) (Result, error)`
- **Interface-based storage** - all backends implement `storage.Writer`
- **Factory pattern** for storage backend creation (`storage.NewWriterFactory()`)
- **Finalizers** for cleanup (constant: `capture.gateway.io/cleanup`)
- **Condition-based status** using `metav1.Condition`
- **OwnerReferences** on child resources (Deployment/Service/HPA owned by TrafficCapture)
- **zap logging** via controller-runtime's zapr adapter

### Protobuf
- Service definitions in `proto/hub/v1/hub.proto`
- Generated code committed to repo (`hub.pb.go`, `hub_grpc.pb.go`)
- Buf used for linting (STANDARD rules) and generation

### Naming
- CRD API group: `capture.gateway.io`
- CRD API version: `v1alpha1`
- Go module: `github.com/kapture-io/kapture`
- Docker images: `kapture/hub`, `kapture/spoke`, `kapture/agent`

### Testing Conventions
- Unit tests colocated with source files (`*_test.go`)
- Integration tests in `test/integration/` using envtest
- E2E tests in `test/e2e/` using Kind
- Helm tests in `charts/kapture/tests/`
- UI tests in `ui/src/` using Vitest + React Testing Library
- Always use `-race` flag for Go tests

## Key Files

| File | Purpose |
|------|---------|
| `api/v1alpha1/*_types.go` | CRD type definitions - edit these to change API |
| `api/v1alpha1/zz_generated.deepcopy.go` | Auto-generated - do not edit manually |
| `proto/hub/v1/hub.proto` | gRPC service definition - regenerate after changes |
| `internal/spoke/controller.go` | Main spoke reconcilers (TrafficCapture + CaptureStorage) |
| `internal/spoke/mirror.go` | HTTPRoute/GRPCRoute RequestMirror filter injection |
| `internal/hub/grpc_server.go` | Hub gRPC service implementation |
| `internal/storage/interface.go` | Storage Writer interface definition |
| `internal/storage/factory.go` | Storage backend factory |
| `charts/kapture/values.yaml` | Helm chart default values |
| `config/crd/bases/*.yaml` | Generated CRD manifests - regenerate after API changes |

## Development Notes

- After modifying `api/v1alpha1/*_types.go`, run `make generate` to update deepcopy functions and CRD manifests
- After modifying `proto/hub/v1/hub.proto`, run `make generate-proto` to regenerate Go stubs
- CaptureHub is cluster-scoped; TrafficCapture and CaptureStorage are namespaced
- Docker images use distroless base with nonroot user (security-first)
- The UI supports a mock mode for local development without a live hub connection
