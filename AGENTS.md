# AGENTS.md

Guidance for AI coding agents and maintainers working in this repository.

## Repository Scope

These instructions apply to the entire repository. If a more specific `AGENTS.md` exists in a subdirectory, follow the more specific file for files under that path while still honoring the root-level testing and safety requirements here.

## Project Overview

Kapture is a Kubernetes traffic capture system built around Gateway API `RequestMirror` filters. The main components are:

- `cmd/hub`: hub controller and gRPC server for multi-cluster coordination.
- `cmd/spoke`: spoke controller that reconciles `TrafficCapture` resources and manages capture agents.
- `cmd/capture-agent`: HTTP/gRPC sink that writes captured request batches to storage.
- `api/v1alpha1`: Kubernetes API types for `TrafficCapture`, `CaptureStorage`, and `CaptureHub`.
- `internal/agent`: request capture handlers and buffered writing.
- `internal/storage`: pluggable storage writers such as S3, GCS, EFS, and EBS.
- `internal/spoke`: Kubernetes resource construction and Gateway API mirror filter logic.
- `internal/hub`: spoke registry, hub RPCs, and capture status aggregation.
- `charts/kapture`: Helm chart and chart tests.
- `config/crd/bases`: generated CRD manifests.
- `test/integration`: envtest-based controller tests.
- `test/e2e`: Kind-based full system tests.

## Development Principles

- Keep changes narrow and consistent with existing package boundaries.
- Prefer existing helper functions and types before adding new abstractions.
- Do not store captured payload bytes, request bodies, or PCAP data in relational databases. Store large capture artifacts in object/file storage such as S3, and store only capture history, metadata, status, and artifact locations in PostgreSQL/RDS.
- Treat Kubernetes custom resources as the desired-state API. Use external persistence only for query/history data that should outlive controller reconciliation history.
- Preserve backward compatibility for CRD fields, Helm values, and environment variables unless a migration is explicitly part of the task.
- Keep controller operations idempotent. Reconciliation code should tolerate repeated execution, partially created resources, and missing optional dependencies.
- Make storage writes observable and testable without requiring real cloud services in unit tests.
- Avoid broad refactors while fixing focused bugs or CI failures.

## Required Checks Before Finishing

Always run the relevant tests before handing work back. For most code changes, run the full local check set below. If a check cannot run because of missing local tools, unavailable Docker/Kubernetes, or network restrictions, say exactly which command could not run and why.

Minimum checks for normal Go changes:

```bash
go mod download
go vet ./...
go test ./internal/... -v -race -coverprofile=coverage.out
go build ./...
```

Run integration tests when changing API types, reconcilers, generated Kubernetes resources, status handling, or controller behavior:

```bash
go mod download
go run sigs.k8s.io/controller-runtime/tools/setup-envtest@latest use 1.31.0 --bin-dir ./testbin
KUBEBUILDER_ASSETS=./testbin/k8s/1.31.0-$(go env GOOS)-$(go env GOARCH) go test ./test/integration/... -v -race -timeout 300s
```

Run Helm tests when changing anything under `charts/kapture`:

```bash
helm plugin install --verify=false https://github.com/helm-unittest/helm-unittest.git
helm unittest charts/kapture
```

Run e2e tests when changing controller lifecycle behavior, Gateway API mirror filter behavior, Dockerfiles, Helm deployment behavior, cross-component wiring, or capture-agent runtime configuration:

```bash
go mod download
go test ./test/e2e/... -v -timeout 600s -count=1
```

The GitHub Actions checks that must pass on pull requests are:

- `CI / lint`: `go mod download`, then `go vet ./...`
- `CI / unit-test`: `go mod download`, then `go test ./internal/... -v -race -coverprofile=coverage.out`
- `CI / integration-test`: envtest integration suite
- `CI / helm-test`: `helm unittest charts/kapture`
- `CI / build`: `go mod download`, then `go build ./...`
- `E2E Tests / e2e`: Kind cluster, Helm deploy, then `go test ./test/e2e/... -v -timeout 600s -count=1`

If a PR check fails, inspect the failing job log before changing code. Fix the first concrete compiler, vet, test, or runtime error rather than guessing from the check name.

## Code Generation

- Run `make generate-proto` after changing files under `proto/`.
- Keep generated protobuf output in sync with `.proto` changes.
- If API type changes require generated Kubernetes code or CRD updates, update generated manifests and mention the generation command used. The current `make generate` target is a placeholder, so verify the actual generator command before relying on it.

## Go Conventions

- Format Go code with `gofmt`.
- Keep imports tidy and let `go test` or `go mod tidy` reveal module metadata drift.
- Use `context.Context` for operations that can block or call external services.
- Prefer table-driven tests for storage, agent, and reconciler helpers where several variants share setup.
- Do not require cloud credentials in unit tests. Use fake clients, local interfaces, or recorders.
- Keep package-level interfaces small and owned by the consumer when possible.

## Kubernetes and Controller Guidance

- Keep reconcilers idempotent and side-effect aware.
- When adding environment variables to agent deployments, update tests that assert deployment shape.
- When adding Helm values, update `charts/kapture/values.yaml`, templates, and Helm unit tests or snapshots as appropriate.
- When changing CRD-facing API types, update examples, docs, generated CRDs, and tests.
- Preserve owner references, labels, and selectors used by existing tests and cleanup logic.
- Be careful with Secrets. Prefer `valueFrom.secretKeyRef` for sensitive values such as database URLs or cloud credentials.

## Storage and Capture History Guidance

- Captured request artifacts belong in object/file storage, typically S3.
- PostgreSQL/RDS should store durable history such as capture identity, namespace, target reference, storage reference, status, timestamps, error text, and artifact metadata.
- Artifact metadata may include backend type, bucket, object key, region, content type, encoding, byte size, checksum, creation time, and expiration time.
- Database writes should not block or corrupt capture artifact writes. If an artifact upload succeeds but history recording fails, surface the error clearly and keep enough context to retry or diagnose.
- Avoid putting database credentials into CRDs. Prefer Helm-provided environment variables or Kubernetes Secrets mounted into controller/agent pods.

## Documentation Expectations

Update documentation when behavior, configuration, operational steps, or storage semantics change. Good candidates are:

- `README.md` for user-facing setup and architecture changes.
- `docs/` for deeper operational guides and migration notes.
- Helm values tables when chart configuration changes.
- Example manifests when API shape or recommended configuration changes.

## Pull Request Notes

When finishing work, summarize:

- What changed.
- Which checks were run, with exact commands.
- Which checks were not run and why.
- Any migration, Helm value, Secret, or operational follow-up required.

Do not mark work as complete when required checks have not been run or inspected unless you clearly call out the limitation and the next action needed.
