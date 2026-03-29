# Proposal 001: Database Storage Backend & Traffic Replay Controller

**Author:** @aburan28
**Status:** Draft
**Created:** 2026-03-29
**Branch:** `claude/add-database-replay-support-PFqIs`

---

## Summary

This proposal introduces two new capabilities to Kapture:

1. **Database storage backend** — a first-class `Database` storage type (targeting RDS/PostgreSQL) so captured traffic can be stored in a relational database rather than object storage alone.
2. **Traffic replay controller** — a new `TrafficReplay` CRD and controller that reads captured traffic from storage and replays it against target endpoints using a plugin-based model, with built-in support for **ghz** (gRPC) and **k6** (HTTP) as replay engines.

Both features build on Kapture's existing architecture: the storage `WriterFactory` interface for (1) and a new replay plugin interface for (2).

---

## Motivation

### Database Storage

Current storage backends (S3, GCS, EFS, EBS) are optimized for bulk writes of compressed JSONL files. While efficient for archival, they make it difficult to:

- Query captured traffic by field (method, path, headers, status, time range)
- Build dashboards and analytics over captured data
- Correlate captures across multiple sessions
- Support retention policies with fine-grained TTL per row

A relational database (e.g., Amazon RDS PostgreSQL) provides indexed, queryable storage that complements the existing object backends.

### Traffic Replay

Captured traffic is currently write-only — there is no way to replay it. Traffic replay enables:

- **Load testing** — replay production traffic patterns against staging environments
- **Regression testing** — verify new deployments handle the same traffic correctly
- **Performance benchmarking** — measure latency/throughput under realistic load
- **gRPC load testing** — replay captured gRPC calls using ghz
- **HTTP load testing** — replay captured HTTP requests using k6

A plugin-based model allows users to bring their own replay engines while Kapture provides the orchestration, scheduling, and status reporting.

---

## Design

### Part 1: Database Storage Backend

#### 1.1 CRD Changes — `CaptureStorage`

Add a new `Database` storage type and `DatabaseConfig` to `CaptureStorageSpec`:

```go
// capturestorage_types.go

// +kubebuilder:validation:Enum=S3;GCS;EFS;EBS;Plugin;Database
type CaptureStorageType string

const (
    // ... existing types ...
    CaptureStorageTypeDatabase CaptureStorageType = "Database"
)

// DatabaseConfig defines relational database storage settings.
type DatabaseConfig struct {
    // Driver is the database driver (e.g., "postgres", "mysql").
    // +kubebuilder:validation:Enum=postgres;mysql
    Driver string `json:"driver"`
    // Host is the database endpoint (e.g., RDS endpoint).
    Host string `json:"host"`
    // Port is the database port.
    // +kubebuilder:validation:Minimum=1
    // +kubebuilder:validation:Maximum=65535
    Port int32 `json:"port"`
    // DatabaseName is the target database name.
    DatabaseName string `json:"databaseName"`
    // Table is the table name for captured requests.
    // +optional
    Table *string `json:"table,omitempty"`
    // CredentialsSecretRef references a Secret with "username" and "password" keys.
    CredentialsSecretRef gwapiv1.SecretObjectReference `json:"credentialsSecretRef"`
    // TLS configures the database TLS settings.
    // +optional
    TLS *DatabaseTLSConfig `json:"tls,omitempty"`
    // MaxConnections sets the connection pool maximum.
    // +optional
    // +kubebuilder:validation:Minimum=1
    MaxConnections *int32 `json:"maxConnections,omitempty"`
}

type DatabaseTLSConfig struct {
    // Enabled controls whether TLS is used.
    Enabled bool `json:"enabled"`
    // CASecretRef references a Secret containing the CA certificate.
    // +optional
    CASecretRef *gwapiv1.SecretObjectReference `json:"caSecretRef,omitempty"`
    // Mode sets the TLS verification mode (e.g., "verify-full", "require").
    // +optional
    // +kubebuilder:validation:Enum=require;verify-ca;verify-full
    Mode *string `json:"mode,omitempty"`
}
```

Updated `CaptureStorageSpec`:
```go
type CaptureStorageSpec struct {
    Type CaptureStorageType `json:"type"`
    // ... existing fields ...
    // +optional
    Database *DatabaseConfig `json:"database,omitempty"`
    // +optional
    Retention *RetentionPolicy `json:"retention,omitempty"`
}
```

#### 1.2 Database Schema

The capture agent will auto-create the table on first write if it doesn't exist:

```sql
CREATE TABLE IF NOT EXISTS captured_requests (
    id          UUID PRIMARY KEY,
    capture_id  TEXT NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL,
    method      TEXT NOT NULL,
    path        TEXT NOT NULL,
    headers     JSONB,
    body        BYTEA,
    content_length BIGINT,
    protocol    TEXT NOT NULL,
    metadata    JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_captured_requests_capture_id ON captured_requests (capture_id);
CREATE INDEX idx_captured_requests_timestamp ON captured_requests (timestamp);
CREATE INDEX idx_captured_requests_method_path ON captured_requests (method, path);
CREATE INDEX idx_captured_requests_protocol ON captured_requests (protocol);
```

Retention enforcement via a periodic cleanup:
```sql
DELETE FROM captured_requests WHERE created_at < NOW() - INTERVAL '<maxAge>';
```

#### 1.3 Storage Implementation

New file: `internal/storage/database.go`

```go
type DatabaseWriterFactory struct {
    db        *sql.DB
    tableName string
}

func NewDatabaseWriterFactory(ctx context.Context, cfg DatabaseConfig) (*DatabaseWriterFactory, error)
func (f *DatabaseWriterFactory) NewWriter(ctx context.Context, captureID string) (Writer, error)
```

The `DatabaseWriter` will:
- Batch inserts using `COPY` (Postgres) or multi-row `INSERT` for efficiency
- Use the same `Writer` interface (`Write`, `Flush`, `Close`)
- Flush on batch size (default 500 rows) or time interval (default 2s)
- Use connection pooling via `database/sql` with configurable pool size

#### 1.4 Factory Registration

Update `internal/storage/factory.go` to add the `"database"` case:

```go
case "database":
    cfg, err := asDatabaseConfig(config)
    if err != nil {
        return nil, err
    }
    return NewDatabaseWriterFactory(context.Background(), cfg)
```

#### 1.5 Dependencies

Add to `go.mod`:
- `github.com/lib/pq` (PostgreSQL driver) or `github.com/jackc/pgx/v5` (preferred, pure Go)
- `github.com/go-sql-driver/mysql` (MySQL driver, optional)

**Recommendation:** Use `pgx/v5` as the primary driver. MySQL support can follow in a later proposal.

---

### Part 2: Traffic Replay Controller

#### 2.1 New CRD — `TrafficReplay`

```go
// api/v1alpha1/trafficreplay_types.go

// +kubebuilder:validation:Enum=Pending;Running;Completed;Failed
type TrafficReplayPhase string

const (
    TrafficReplayPhasePending   TrafficReplayPhase = "Pending"
    TrafficReplayPhaseRunning   TrafficReplayPhase = "Running"
    TrafficReplayPhaseCompleted TrafficReplayPhase = "Completed"
    TrafficReplayPhaseFailed    TrafficReplayPhase = "Failed"
)

// +kubebuilder:validation:Enum=ghz;k6;plugin
type ReplayEngineType string

const (
    ReplayEngineGhz    ReplayEngineType = "ghz"
    ReplayEngineK6     ReplayEngineType = "k6"
    ReplayEnginePlugin ReplayEngineType = "plugin"
)

// TrafficReplaySpec defines the desired state of TrafficReplay.
type TrafficReplaySpec struct {
    // SourceRef references the CaptureStorage containing captured traffic.
    SourceRef CaptureStorageReference `json:"sourceRef"`
    // CaptureID identifies which capture session to replay.
    // +optional — if omitted, replays all captures in the storage.
    CaptureID *string `json:"captureID,omitempty"`
    // Target is the endpoint to send replayed traffic to.
    Target ReplayTarget `json:"target"`
    // Engine selects the replay engine.
    Engine ReplayEngineSpec `json:"engine"`
    // Schedule is an optional cron expression for recurring replays.
    // +optional
    Schedule *string `json:"schedule,omitempty"`
    // Filters narrow which captured requests are replayed.
    // +optional
    Filters *ReplayFilters `json:"filters,omitempty"`
    // RateLimit controls replay throughput.
    // +optional
    RateLimit *ReplayRateLimit `json:"rateLimit,omitempty"`
}

// ReplayTarget defines where replayed traffic is sent.
type ReplayTarget struct {
    // Host is the target hostname or address.
    Host string `json:"host"`
    // Port is the target port.
    // +optional
    Port *int32 `json:"port,omitempty"`
    // TLS enables TLS for the replay connection.
    // +optional
    TLS *bool `json:"tls,omitempty"`
}

// ReplayEngineSpec configures the replay engine.
type ReplayEngineSpec struct {
    // Type selects the engine: ghz, k6, or plugin.
    Type ReplayEngineType `json:"type"`
    // Ghz configures the ghz gRPC load testing engine.
    // +optional
    Ghz *GhzEngineConfig `json:"ghz,omitempty"`
    // K6 configures the k6 HTTP load testing engine.
    // +optional
    K6 *K6EngineConfig `json:"k6,omitempty"`
    // Plugin configures a custom replay engine plugin.
    // +optional
    Plugin *ReplayPluginConfig `json:"plugin,omitempty"`
}

// GhzEngineConfig configures ghz for gRPC replay.
type GhzEngineConfig struct {
    // Image is the ghz container image.
    // +optional
    Image *string `json:"image,omitempty"`
    // Concurrency is the number of concurrent workers.
    // +optional
    Concurrency *int32 `json:"concurrency,omitempty"`
    // TotalRequests is the total number of requests to replay.
    // If 0, replays all captured requests.
    // +optional
    TotalRequests *int64 `json:"totalRequests,omitempty"`
    // Duration is the maximum replay duration (e.g., "30s", "5m").
    // +optional
    Duration *string `json:"duration,omitempty"`
    // Connections is the number of gRPC connections.
    // +optional
    Connections *int32 `json:"connections,omitempty"`
    // ExtraArgs are additional CLI flags passed to ghz.
    // +optional
    ExtraArgs []string `json:"extraArgs,omitempty"`
}

// K6EngineConfig configures k6 for HTTP replay.
type K6EngineConfig struct {
    // Image is the k6 container image.
    // +optional
    Image *string `json:"image,omitempty"`
    // VUs is the number of virtual users.
    // +optional
    VUs *int32 `json:"vus,omitempty"`
    // Duration is the test duration (e.g., "30s", "5m").
    // +optional
    Duration *string `json:"duration,omitempty"`
    // Iterations is the total number of iterations.
    // +optional
    Iterations *int64 `json:"iterations,omitempty"`
    // Thresholds define pass/fail criteria (e.g., "http_req_duration{p(95)}<200").
    // +optional
    Thresholds map[string]string `json:"thresholds,omitempty"`
    // ExtraArgs are additional CLI flags passed to k6.
    // +optional
    ExtraArgs []string `json:"extraArgs,omitempty"`
}

// ReplayPluginConfig configures a custom replay engine.
type ReplayPluginConfig struct {
    // Image is the container image for the plugin.
    Image string `json:"image"`
    // Command overrides the container entrypoint.
    // +optional
    Command []string `json:"command,omitempty"`
    // Args are arguments passed to the container.
    // +optional
    Args []string `json:"args,omitempty"`
    // Env are additional environment variables.
    // +optional
    Env map[string]string `json:"env,omitempty"`
}

// ReplayFilters narrow which captured requests are replayed.
type ReplayFilters struct {
    // Methods filters by HTTP method (e.g., ["GET", "POST"]).
    // +optional
    Methods []string `json:"methods,omitempty"`
    // PathPrefix filters by URL path prefix.
    // +optional
    PathPrefix *string `json:"pathPrefix,omitempty"`
    // Protocol filters by protocol ("HTTP" or "gRPC").
    // +optional
    Protocol *string `json:"protocol,omitempty"`
    // TimeRange filters by capture timestamp.
    // +optional
    TimeRange *TimeRangeFilter `json:"timeRange,omitempty"`
}

type TimeRangeFilter struct {
    // +optional
    After *metav1.Time `json:"after,omitempty"`
    // +optional
    Before *metav1.Time `json:"before,omitempty"`
}

// ReplayRateLimit controls replay throughput.
type ReplayRateLimit struct {
    // RequestsPerSecond caps the replay rate.
    // +optional
    RequestsPerSecond *int32 `json:"requestsPerSecond,omitempty"`
    // SpeedMultiplier replays at N× the original rate (e.g., 2.0 = 2× speed).
    // +optional
    SpeedMultiplier *string `json:"speedMultiplier,omitempty"`
}

// TrafficReplayStatus defines the observed state of TrafficReplay.
type TrafficReplayStatus struct {
    // +optional
    Phase TrafficReplayPhase `json:"phase,omitempty"`
    // +optional
    StartTime *metav1.Time `json:"startTime,omitempty"`
    // +optional
    CompletionTime *metav1.Time `json:"completionTime,omitempty"`
    // +optional
    RepliedRequests int64 `json:"repliedRequests,omitempty"`
    // +optional
    FailedRequests int64 `json:"failedRequests,omitempty"`
    // +optional
    Results *ReplayResults `json:"results,omitempty"`
    // +optional
    JobRef *corev1.ObjectReference `json:"jobRef,omitempty"`
    // +optional
    Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// ReplayResults holds aggregated metrics from the replay run.
type ReplayResults struct {
    // AverageLatency in milliseconds.
    // +optional
    AverageLatencyMs *int64 `json:"averageLatencyMs,omitempty"`
    // P50 latency in milliseconds.
    // +optional
    P50LatencyMs *int64 `json:"p50LatencyMs,omitempty"`
    // P95 latency in milliseconds.
    // +optional
    P95LatencyMs *int64 `json:"p95LatencyMs,omitempty"`
    // P99 latency in milliseconds.
    // +optional
    P99LatencyMs *int64 `json:"p99LatencyMs,omitempty"`
    // RequestsPerSecond achieved during replay.
    // +optional
    RequestsPerSecond *float64 `json:"requestsPerSecond,omitempty"`
    // ErrorRate as a percentage.
    // +optional
    ErrorRate *float64 `json:"errorRate,omitempty"`
}
```

#### 2.2 Controller Architecture

New controller: `internal/replay/controller.go`

The `TrafficReplayReconciler` manages the replay lifecycle:

```
TrafficReplay CR created
    │
    ▼
[Pending] ─── Validate sourceRef, resolve storage config
    │
    ▼
[Running] ─── Create Kubernetes Job with replay engine container
    │           │
    │           ├── ghz:  Job runs ghz container with captured gRPC payloads
    │           ├── k6:   Job runs k6 container with generated JS script
    │           └── plugin: Job runs user-provided container image
    │
    ▼
[Completed/Failed] ─── Parse Job output, update status with ReplayResults
```

**Key design decisions:**

1. **Job-based execution** — Each replay runs as a Kubernetes `Job` (not a long-lived Deployment). This gives us built-in retry, backoff, and completion tracking.

2. **Data loading** — An init container reads captured requests from storage (S3/GCS/DB/etc.) and writes them to a shared `emptyDir` volume in the format expected by the replay engine.

3. **Engine adapters** — Each built-in engine has an adapter in `internal/replay/engine/` that:
   - Converts `CapturedRequest` data into the engine's input format
   - Generates the Job spec (container image, args, volumes)
   - Parses engine output into `ReplayResults`

```
internal/replay/
├── controller.go          # TrafficReplayReconciler
├── job_builder.go         # Kubernetes Job spec construction
├── data_loader.go         # Init container for loading captures
├── results_parser.go      # Parse engine output → ReplayResults
└── engine/
    ├── interface.go       # ReplayEngine interface
    ├── ghz.go             # ghz adapter
    ├── k6.go              # k6 adapter
    └── plugin.go          # Custom plugin adapter
```

#### 2.3 Replay Engine Interface

```go
// internal/replay/engine/interface.go

// ReplayEngine adapts captured traffic for a specific replay tool.
type ReplayEngine interface {
    // Name returns the engine identifier.
    Name() string
    // PrepareData converts captured requests into the engine's input format.
    // Returns the data to mount and the expected file path inside the container.
    PrepareData(ctx context.Context, requests []*storage.CapturedRequest) ([]byte, string, error)
    // JobSpec returns the container spec for the replay Job.
    JobSpec(target ReplayTarget, dataPath string, config any) (*corev1.Container, error)
    // ParseResults extracts replay metrics from Job logs.
    ParseResults(logs string) (*ReplayResults, error)
}
```

#### 2.4 ghz Engine Adapter

ghz accepts a JSON data file and proto descriptors. The adapter:

1. Filters captured requests to `protocol == "gRPC"`
2. Extracts method names from paths (e.g., `/pkg.Service/Method`)
3. Writes request bodies as JSON payloads
4. Generates ghz CLI args: `--call`, `--data-file`, `--concurrency`, `--connections`, etc.

```yaml
# Example TrafficReplay for gRPC
apiVersion: capture.gateway.io/v1alpha1
kind: TrafficReplay
metadata:
  name: grpc-load-test
spec:
  sourceRef:
    name: my-db-storage
  captureID: "capture-abc123"
  target:
    host: grpc-staging.example.com
    port: 443
    tls: true
  engine:
    type: ghz
    ghz:
      concurrency: 50
      connections: 10
      duration: "5m"
  filters:
    protocol: "gRPC"
```

#### 2.5 k6 Engine Adapter

k6 uses JavaScript test scripts. The adapter:

1. Filters captured requests to `protocol == "HTTP"`
2. Generates a k6 script that iterates over captured requests
3. Maps headers, body, and method from captured data
4. Applies VU/duration/threshold configuration

```yaml
# Example TrafficReplay for HTTP
apiVersion: capture.gateway.io/v1alpha1
kind: TrafficReplay
metadata:
  name: http-load-test
spec:
  sourceRef:
    name: my-db-storage
  target:
    host: api-staging.example.com
    port: 443
    tls: true
  engine:
    type: k6
    k6:
      vus: 100
      duration: "10m"
      thresholds:
        http_req_duration(p(95)): "< 200"
  rateLimit:
    requestsPerSecond: 500
  filters:
    methods: ["GET", "POST"]
    pathPrefix: "/api/v2"
```

#### 2.6 Plugin Engine Adapter

For custom engines, users provide a container image. Kapture mounts captured data at a well-known path (`/data/captured-requests.jsonl`) and the user's container processes it.

```yaml
# Example TrafficReplay with custom engine
apiVersion: capture.gateway.io/v1alpha1
kind: TrafficReplay
metadata:
  name: custom-replay
spec:
  sourceRef:
    name: my-s3-storage
  target:
    host: api-staging.example.com
  engine:
    type: plugin
    plugin:
      image: myorg/custom-replay-engine:v1
      env:
        CONCURRENCY: "20"
        OUTPUT_FORMAT: "json"
```

**Plugin contract:**
- Input: JSONL file at `/data/captured-requests.jsonl` (one `CapturedRequest` JSON per line)
- Target info via env vars: `REPLAY_TARGET_HOST`, `REPLAY_TARGET_PORT`, `REPLAY_TARGET_TLS`
- Output: JSON results to stdout matching `ReplayResults` schema (best-effort parsing)
- Exit code 0 = success, non-zero = failure

---

### Part 3: Scheduled Replays (Optional / Future)

The `schedule` field on `TrafficReplaySpec` supports cron expressions for recurring replays. The controller creates a `CronJob` instead of a `Job` when `schedule` is set. This is noted here for completeness but can be deferred to a follow-up proposal.

---

## File Changes Summary

### New Files

| File | Description |
|------|-------------|
| `api/v1alpha1/trafficreplay_types.go` | TrafficReplay CRD types |
| `internal/storage/database.go` | Database WriterFactory + Writer |
| `internal/storage/database_test.go` | Unit tests for database storage |
| `internal/replay/controller.go` | TrafficReplay reconciler |
| `internal/replay/job_builder.go` | Job/CronJob spec construction |
| `internal/replay/data_loader.go` | Capture data loading from storage |
| `internal/replay/results_parser.go` | Engine output → ReplayResults |
| `internal/replay/engine/interface.go` | ReplayEngine interface |
| `internal/replay/engine/ghz.go` | ghz adapter |
| `internal/replay/engine/k6.go` | k6 adapter |
| `internal/replay/engine/plugin.go` | Custom plugin adapter |
| `internal/replay/engine/*_test.go` | Engine adapter tests |
| `internal/replay/controller_test.go` | Controller unit tests |
| `cmd/spoke/main.go` | Register TrafficReplay controller |
| `config/crd/bases/capture.gateway.io_trafficreplays.yaml` | Generated CRD |
| `test/integration/replay_test.go` | Integration tests |

### Modified Files

| File | Change |
|------|--------|
| `api/v1alpha1/capturestorage_types.go` | Add `Database` type + `DatabaseConfig` |
| `api/v1alpha1/zz_generated.deepcopy.go` | Regenerated |
| `internal/storage/factory.go` | Add `"database"` case |
| `go.mod` / `go.sum` | Add `pgx/v5` dependency |
| `config/crd/bases/capture.gateway.io_capturestorages.yaml` | Regenerated with DB fields |
| `charts/kapture/` | Helm chart updates for new CRD + RBAC |

---

## Alternatives Considered

### Database Storage

1. **Use the existing Plugin system for database support** — Rejected because database storage is a common enough need to warrant first-class support. Plugin-based DB storage would require users to build and ship a `.so` file, which is fragile.

2. **Use an ORM (GORM, Ent)** — Rejected in favor of raw `pgx` + `database/sql`. The schema is simple and stable; an ORM adds unnecessary abstraction and dependency weight.

3. **Support all database engines immediately** — Rejected. Start with PostgreSQL (most common on RDS), add MySQL/Aurora support based on demand.

### Traffic Replay

1. **In-process replay (Go HTTP/gRPC clients)** — Rejected. Using established tools (ghz, k6) provides better reporting, protocol handling, and community support. Users already know these tools.

2. **Operator-managed Deployments instead of Jobs** — Rejected. Replays are finite tasks; Jobs are the correct Kubernetes primitive for run-to-completion workloads.

3. **CRD-less approach (CLI-only replay)** — Rejected. A CRD-based approach is consistent with Kapture's declarative model and enables scheduling, status tracking, and hub visibility.

---

## Migration & Backwards Compatibility

- All changes are additive. No existing CRD fields are modified or removed.
- The `Database` storage type is opt-in; existing `CaptureStorage` resources continue to work unchanged.
- `TrafficReplay` is a new CRD with no impact on existing `TrafficCapture` resources.
- The spoke controller gains an additional reconciler but existing reconcile loops are untouched.

---

## Security Considerations

- Database credentials are stored in Kubernetes Secrets (same pattern as S3/GCS `credentialsSecretRef`).
- TLS to the database is configurable and recommended for production.
- Replay Jobs run with the same RBAC as capture agents — no elevated privileges.
- k6 scripts are generated by the controller (not user-supplied), preventing script injection.
- Plugin replay containers are user-provided images; users are responsible for image security.

---

## Open Questions

1. **Should the database writer support `COPY` protocol for PostgreSQL bulk inserts, or use batched `INSERT` statements?** `COPY` is ~5× faster but requires `pgx` specific APIs. Recommendation: use `COPY` with a fallback to batched `INSERT`.

2. **Should replay results be stored back into the database storage?** This would enable historical replay analysis. Recommendation: yes, as a separate `replay_results` table, but defer to a follow-up.

3. **Should the `TrafficReplay` controller live in the spoke or in the hub?** Recommendation: spoke controller, since replays execute locally. The hub can aggregate replay status via the existing `ReportCaptureStatus` pattern.

4. **Container image defaults for ghz/k6** — Should we pin specific versions or use `latest`? Recommendation: pin to known-good versions with override via `image` field.

---

## Implementation Plan

### Phase 1: Database Storage Backend
1. Add `DatabaseConfig` types to `api/v1alpha1/capturestorage_types.go`
2. Implement `internal/storage/database.go` (writer + factory)
3. Update `internal/storage/factory.go`
4. Add `pgx/v5` dependency
5. Regenerate CRDs and deepcopy
6. Unit tests + integration tests
7. Update Helm chart

### Phase 2: Traffic Replay Controller
1. Add `TrafficReplay` CRD types
2. Implement `internal/replay/engine/interface.go`
3. Implement ghz adapter
4. Implement k6 adapter
5. Implement plugin adapter
6. Implement `TrafficReplayReconciler`
7. Register controller in spoke `main.go`
8. Regenerate CRDs and deepcopy
9. Unit tests + integration tests
10. Update Helm chart with RBAC for Jobs

### Phase 3: Polish & Documentation
1. E2E tests with Kind cluster
2. Update README with replay examples
3. Helm chart documentation
