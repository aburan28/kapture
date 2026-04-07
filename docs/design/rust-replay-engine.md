# Rust Replay Engine — Design Proposal

**Status:** Proposed  
**Author:** Kapture Team  
**Date:** 2026-04-05  
**Target:** High-performance traffic replay for 100GB+ captures at recorded QPS

## 1. Motivation

The existing Go-based replay engine (using the `replay.Engine` + k6 integration)
handles moderate workloads well, but for extreme-scale scenarios — 100GB+ captures,
sustained multi-day replays at >100K QPS, or replay across hundreds of target
endpoints — a purpose-built Rust engine offers key advantages:

- **Zero-cost abstractions** — No GC pauses during sustained high-throughput replay
- **Predictable tail latency** — Critical for "as recorded" QPS fidelity
- **Memory efficiency** — Streaming 100GB datasets with bounded RSS (<100MB)
- **Async I/O** — tokio's work-stealing scheduler for massive connection concurrency
- **Single static binary** — Simplified deployment as a Kubernetes Job or CronJob

### When to use Rust vs k6

| Dimension | k6 Engine | Rust Engine |
|-----------|-----------|-------------|
| Dataset size | <10GB | 10GB–1TB+ |
| Target QPS | <50K | 50K–1M+ |
| Duration | Hours | Days to weeks |
| Tail latency SLA | Relaxed | Strict (p99 <5ms jitter) |
| Team familiarity | JavaScript | Rust or ops-focused |
| Customization | k6 scripts + JS | Rust plugins or WASM |

## 2. Architecture Overview

```
┌──────────────────────────────────────────────────────┐
│                   Rust Replay Engine                  │
│                                                      │
│  ┌────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │   Storage   │  │    Timing    │  │   Sender     │ │
│  │   Reader    │──│    Engine    │──│   Pool       │ │
│  │  (Streamer) │  │  (Pacer)    │  │  (hyper)     │ │
│  └────────────┘  └──────────────┘  └──────────────┘ │
│        │                │                  │         │
│  ┌─────┴────┐    ┌──────┴──────┐   ┌──────┴──────┐ │
│  │Checkpoint │    │  Metrics    │   │  Result     │ │
│  │ Manager   │    │ (Prometheus)│   │  Collector  │ │
│  └──────────┘    └─────────────┘   └─────────────┘ │
│                                                      │
│  ┌─────────────────────────────────────────────────┐ │
│  │          gRPC Control Plane (optional)           │ │
│  │    pause / resume / scale / progress / abort     │ │
│  └─────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────┘
```

### Core Components

1. **Storage Reader (Streamer)** — Async streaming from S3/GCS/filesystem
2. **Timing Engine (Pacer)** — Token-bucket + timestamp-based pacing
3. **Sender Pool** — hyper-based HTTP/2 connection pool with backpressure
4. **Checkpoint Manager** — Atomic progress persistence for resume
5. **Metrics Exporter** — Prometheus-compatible metrics endpoint
6. **Result Collector** — HdrHistogram-based latency tracking
7. **Control Plane** — Optional gRPC API for runtime control

## 3. Detailed Design

### 3.1 Storage Reader

The reader streams JSONL.gz objects from cloud storage without loading the full
dataset into memory. It uses a **prefetch pipeline** with bounded memory:

```rust
/// Configuration for the storage reader.
pub struct ReaderConfig {
    pub storage_type: StorageType,
    pub bucket: Option<String>,
    pub prefix: Option<String>,
    pub mount_path: Option<PathBuf>,
    pub capture_id: String,
    
    /// Number of objects to prefetch concurrently.
    /// Each prefetched object is decompressed in a background task.
    pub prefetch_count: usize,  // default: 4
    
    /// Maximum memory budget for the prefetch buffer.
    /// The reader pauses prefetching when this limit is reached.
    pub max_prefetch_bytes: usize,  // default: 256MB
    
    /// Filters applied during reading.
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub path_prefix: Option<String>,
    pub methods: Option<Vec<String>>,
}

pub enum StorageType {
    S3 { region: Option<String> },
    Gcs,
    Filesystem,
}
```

**Prefetch Pipeline:**

```
Object List ──► Prefetch Queue (4 concurrent) ──► Decompress ──► Line Buffer ──► Channel
                   │                                    │
                   │  aws-sdk-rust / cloud-storage      │  flate2 async
                   │  Range requests for parallelism    │  Streaming decompression
                   ▼                                    ▼
            Bounded buffer (256MB)              Parsed CapturedRequest
```

Key design decisions:
- **Object-level parallelism**: Prefetch N objects concurrently while the pacer
  consumes from the current one. This hides S3/GCS latency.
- **Streaming decompression**: Use `async-compression` with `flate2` to decompress
  on-the-fly. Never hold a full decompressed object in memory.
- **Backpressure**: If the sender pool can't keep up, the bounded channel blocks
  the reader, which in turn pauses prefetching. Memory stays bounded.
- **S3 range requests**: For very large individual objects (>100MB), use
  range-based reads to parallelize download within a single object.

```rust
#[async_trait]
pub trait StorageReader: Send + Sync {
    /// List all objects matching the capture prefix and time range.
    async fn list_objects(&self, opts: &ReadOptions) -> Result<Vec<ObjectInfo>>;
    
    /// Open a streaming reader for a specific object.
    async fn open_object(&self, key: &str) -> Result<Pin<Box<dyn AsyncRead + Send>>>;
}

pub struct S3Reader {
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
}

pub struct GcsReader {
    client: cloud_storage::Client,
    bucket: String,
    prefix: String,
}

pub struct FilesystemReader {
    mount_path: PathBuf,
}
```

### 3.2 Timing Engine (Pacer)

The pacer is the most critical component for achieving "as recorded" QPS.
It supports three modes:

#### Original Timing Mode

Preserves the exact inter-request delays from the capture, scaled by a
configurable factor:

```rust
pub struct OriginalTimingPacer {
    time_scale: f64,        // 1.0 = real-time, 0.5 = 2x speed
    prev_timestamp: Option<DateTime<Utc>>,
    drift_correction: DriftCorrector,
}

impl OriginalTimingPacer {
    /// Returns the delay before the next request should be sent.
    /// Uses drift correction to prevent cumulative timing errors
    /// over multi-day runs.
    pub fn next_delay(&mut self, req_timestamp: DateTime<Utc>) -> Duration {
        let delay = match self.prev_timestamp {
            Some(prev) => {
                let raw_delay = req_timestamp - prev;
                let scaled = raw_delay.mul_f64(self.time_scale);
                self.drift_correction.correct(scaled)
            }
            None => Duration::ZERO,
        };
        self.prev_timestamp = Some(req_timestamp);
        delay
    }
}
```

**Drift correction** is essential for multi-day replays. Without it, small
timing errors accumulate — after 3 days, the replay could be minutes off from
the recorded timeline. The corrector tracks wall-clock vs. expected time and
applies micro-adjustments:

```rust
pub struct DriftCorrector {
    /// Wall-clock time when replay started.
    wall_start: Instant,
    /// Capture timestamp of the first request.
    capture_start: DateTime<Utc>,
    /// Maximum drift before correction kicks in.
    max_drift: Duration,  // default: 100ms
}

impl DriftCorrector {
    pub fn correct(&self, scheduled_delay: Duration) -> Duration {
        let wall_elapsed = self.wall_start.elapsed();
        let capture_elapsed = /* current capture time - capture_start */;
        let drift = wall_elapsed.as_secs_f64() - capture_elapsed.as_secs_f64();
        
        if drift.abs() > self.max_drift.as_secs_f64() {
            // Adjust delay to reduce drift
            let correction = Duration::from_secs_f64(drift * 0.1);
            if drift > 0.0 {
                scheduled_delay.saturating_sub(correction)
            } else {
                scheduled_delay + correction
            }
        } else {
            scheduled_delay
        }
    }
}
```

#### Constant Rate Mode

Uses a **token bucket** for precise QPS targeting:

```rust
pub struct TokenBucketPacer {
    rate: f64,               // requests per second
    tokens: AtomicU64,       // available tokens (fixed-point)
    last_refill: AtomicU64,  // nanosecond timestamp
    burst: u64,              // max burst size
}
```

#### Unlimited Mode

No pacing — sends as fast as the sender pool allows. Useful for stress testing.

### 3.3 Sender Pool

The sender pool manages HTTP connections to the target service with precise
backpressure control:

```rust
pub struct SenderPool {
    client: hyper_util::client::legacy::Client<
        hyper_rustls::HttpsConnector<hyper::client::connect::HttpConnector>,
        http_body_util::Full<Bytes>,
    >,
    target_base_url: Uri,
    concurrency: usize,
    semaphore: Arc<Semaphore>,  // bounds in-flight requests
    metrics: Arc<SenderMetrics>,
}

pub struct SenderPoolConfig {
    pub target_url: String,
    pub concurrency: usize,          // default: 100
    pub connection_pool_size: usize,  // default: 200
    pub request_timeout: Duration,    // default: 30s
    pub http2: bool,                  // default: true
    pub override_headers: HashMap<String, String>,
}
```

Key design decisions:
- **HTTP/2 multiplexing**: Multiple requests over a single connection reduces
  connection overhead at high QPS.
- **Semaphore-based concurrency**: Bounds in-flight requests to prevent
  overwhelming the target. The semaphore is separate from the connection pool
  to allow fine-grained control.
- **Backpressure propagation**: When all semaphore permits are held, the pacer
  blocks, which blocks the reader, creating end-to-end flow control.

```rust
impl SenderPool {
    pub async fn send(&self, req: CapturedRequest) -> Result<ReplayResult> {
        let _permit = self.semaphore.acquire().await?;
        
        let http_req = self.build_request(&req)?;
        let start = Instant::now();
        
        let result = match tokio::time::timeout(
            self.request_timeout,
            self.client.request(http_req),
        ).await {
            Ok(Ok(resp)) => {
                let status = resp.status().as_u16();
                let body = read_limited_body(resp, 4096).await;
                ReplayResult {
                    request_id: req.id,
                    status_code: status,
                    duration: start.elapsed(),
                    error: None,
                }
            }
            Ok(Err(e)) => ReplayResult {
                request_id: req.id,
                status_code: 0,
                duration: start.elapsed(),
                error: Some(e.to_string()),
            },
            Err(_) => ReplayResult {
                request_id: req.id,
                status_code: 0,
                duration: start.elapsed(),
                error: Some("request timeout".into()),
            },
        };
        
        self.metrics.record(&result);
        Ok(result)
    }
}
```

### 3.4 Checkpoint Manager

For multi-day replays, crash recovery is essential. The checkpoint manager
persists progress atomically:

```rust
pub struct CheckpointManager {
    dir: PathBuf,
    replay_id: String,
    interval: Duration,  // default: 30s
    state: Arc<RwLock<CheckpointState>>,
}

#[derive(Serialize, Deserialize)]
pub struct CheckpointState {
    pub replay_id: String,
    pub capture_id: String,
    pub last_object_key: String,
    pub last_request_id: String,
    pub requests_sent: u64,
    pub requests_errored: u64,
    pub bytes_processed: u64,
    pub start_time: DateTime<Utc>,
    pub last_update: DateTime<Utc>,
    pub elapsed: Duration,
}
```

Checkpoints use **write-ahead** with atomic rename:
1. Serialize state to `checkpoint.tmp`
2. `fsync` the temp file
3. `rename` to `checkpoint.json` (atomic on POSIX)

On resume:
1. Read checkpoint
2. Seek storage reader to `last_object_key`
3. Skip requests until `last_request_id` within that object
4. Resume normal replay

### 3.5 Metrics & Observability

```rust
pub struct ReplayMetrics {
    // Counters
    pub requests_sent: IntCounter,
    pub requests_errored: IntCounter,
    pub requests_filtered: IntCounter,
    pub bytes_read: IntCounter,
    
    // Histograms (HdrHistogram for accurate percentiles)
    pub request_latency: Histogram,
    pub feed_read_latency: Histogram,
    
    // Gauges
    pub inflight_requests: IntGauge,
    pub prefetch_buffer_bytes: IntGauge,
    pub current_rps: Gauge,
    pub drift_seconds: Gauge,
    
    // Status code distribution
    pub status_codes: IntCounterVec,  // labeled by status code
}
```

Metrics are exposed via:
- **Prometheus `/metrics` endpoint** on a configurable port
- **Structured JSON logs** via `tracing` + `tracing-subscriber`
- **OpenTelemetry traces** (optional) for distributed tracing through the target

### 3.6 Control Plane (Optional gRPC)

For long-running replays, a control plane enables runtime management:

```protobuf
syntax = "proto3";
package kapture.replay.v1;

service ReplayControl {
    // Get current replay status and progress.
    rpc GetStatus(GetStatusRequest) returns (ReplayStatus);
    
    // Pause the replay (stops sending but keeps reader position).
    rpc Pause(PauseRequest) returns (PauseResponse);
    
    // Resume a paused replay.
    rpc Resume(ResumeRequest) returns (ResumeResponse);
    
    // Adjust replay parameters at runtime.
    rpc UpdateConfig(UpdateConfigRequest) returns (UpdateConfigResponse);
    
    // Gracefully abort the replay.
    rpc Abort(AbortRequest) returns (AbortResponse);
    
    // Stream real-time progress events.
    rpc WatchProgress(WatchProgressRequest) returns (stream ProgressEvent);
}

message UpdateConfigRequest {
    optional double rate_per_second = 1;
    optional double time_scale = 2;
    optional uint32 concurrency = 3;
}
```

This integrates with Kubernetes: deploy the replay engine as a StatefulSet,
use the gRPC API from the Kapture hub for centralized orchestration.

## 4. Data Flow

```
                         Bounded Memory (~256MB)
                    ┌────────────────────────────┐
                    │                            │
S3/GCS/FS ──► Prefetcher ──► Decompressor ──► Line Parser ──► Channel(1024)
                                                                    │
                                                              ┌─────┴─────┐
                                                              │   Pacer   │
                                                              │ (timing)  │
                                                              └─────┬─────┘
                                                                    │
                                                    ┌───────────────┼───────────────┐
                                                    │               │               │
                                                    ▼               ▼               ▼
                                               Sender 1        Sender 2      Sender N
                                               (permit)        (permit)      (permit)
                                                    │               │               │
                                                    ▼               ▼               ▼
                                              Target Service (HTTP/gRPC)
                                                    │               │               │
                                                    └───────────────┼───────────────┘
                                                                    │
                                                              Result Collector
                                                              (HdrHistogram)
                                                                    │
                                                              Checkpoint +
                                                              Prometheus
```

**Memory budget for 100GB dataset:**

| Component | Memory | Notes |
|-----------|--------|-------|
| Prefetch buffer | 256 MB | 4 objects × ~64MB each |
| Line parse buffer | 10 MB | Single JSONL line buffer |
| Channel buffer | ~50 MB | 1024 CapturedRequests |
| Connection pool | ~20 MB | 200 HTTP/2 connections |
| Metrics/histograms | ~5 MB | HdrHistogram with 3 sig figs |
| **Total RSS** | **~341 MB** | **Constant regardless of dataset size** |

## 5. Crate Dependencies

```toml
[dependencies]
# Async runtime
tokio = { version = "1", features = ["full"] }
tokio-stream = "0.1"

# HTTP client
hyper = { version = "1", features = ["client", "http1", "http2"] }
hyper-util = { version = "0.1", features = ["client-legacy", "tokio"] }
hyper-rustls = { version = "0.27", features = ["http2"] }
http-body-util = "0.1"

# Cloud storage
aws-sdk-s3 = "1"
aws-config = "1"
cloud-storage = "0.11"

# Serialization
serde = { version = "1", features = ["derive"] }
serde_json = "1"

# Compression
async-compression = { version = "0.4", features = ["tokio", "gzip"] }

# Metrics & observability
prometheus = "0.13"
hdrhistogram = "7"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["json"] }
opentelemetry = { version = "0.24", optional = true }

# gRPC (optional control plane)
tonic = { version = "0.12", optional = true }
prost = { version = "0.13", optional = true }

# Utilities
chrono = { version = "0.4", features = ["serde"] }
clap = { version = "4", features = ["derive"] }
uuid = { version = "1", features = ["v4"] }
anyhow = "1"
```

## 6. CLI Interface

```
kapture-replay [OPTIONS] --storage-type <TYPE> --capture-id <ID> --target-url <URL>

Options:
  Storage:
    --storage-type <s3|gcs|efs|ebs>    Storage backend type
    --bucket <BUCKET>                   S3/GCS bucket name
    --region <REGION>                   AWS region
    --prefix <PREFIX>                   Storage prefix
    --mount-path <PATH>                 Filesystem mount path (EFS/EBS)

  Capture Selection:
    --capture-id <ID>                   Capture session to replay
    --start-time <RFC3339>              Start time filter
    --end-time <RFC3339>                End time filter
    --path-prefix <PREFIX>              Path prefix filter
    --methods <GET,POST,...>            HTTP method filter
    --limit <N>                         Max requests to replay

  Replay Configuration:
    --rate-mode <original|constant|unlimited>  Pacing mode [default: original]
    --rate-per-second <N>               Target QPS (constant mode)
    --time-scale <FLOAT>                Time multiplier (original mode) [default: 1.0]
    --concurrency <N>                   Parallel senders [default: 100]
    --http2                             Use HTTP/2 [default: true]
    --request-timeout <DURATION>        Per-request timeout [default: 30s]

  Checkpoint & Resume:
    --checkpoint-dir <PATH>             Checkpoint directory [default: /var/lib/kapture]
    --replay-id <ID>                    Session ID (auto-generated if empty)
    --resume                            Resume from checkpoint

  Observability:
    --metrics-port <PORT>               Prometheus metrics port [default: 9090]
    --log-level <LEVEL>                 Log level [default: info]
    --log-format <text|json>            Log format [default: json]
    --enable-tracing                    Enable OpenTelemetry tracing

  Control Plane:
    --grpc-port <PORT>                  gRPC control plane port [default: 9091]
    --enable-control-plane              Enable gRPC control plane
```

## 7. Kubernetes Deployment

### As a Job (one-shot replay)

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: kapture-replay-prod-20260401
spec:
  backoffLimit: 3
  activeDeadlineSeconds: 604800  # 7 days
  template:
    spec:
      serviceAccountName: kapture-replay
      containers:
      - name: replay
        image: kapture/replay-engine-rust:latest
        args:
        - --storage-type=s3
        - --bucket=kapture-captures-prod
        - --capture-id=prod/api-gateway
        - --target-url=http://staging-api.internal:8080
        - --rate-mode=original
        - --concurrency=100
        - --checkpoint-dir=/checkpoints
        - --resume
        resources:
          requests:
            memory: 512Mi
            cpu: "2"
          limits:
            memory: 1Gi
            cpu: "4"
        volumeMounts:
        - name: checkpoints
          mountPath: /checkpoints
        ports:
        - name: metrics
          containerPort: 9090
      volumes:
      - name: checkpoints
        persistentVolumeClaim:
          claimName: replay-checkpoints
      restartPolicy: OnFailure
```

### As a StatefulSet (managed replay with control plane)

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kapture-replay
spec:
  replicas: 1
  serviceName: kapture-replay
  template:
    spec:
      containers:
      - name: replay
        image: kapture/replay-engine-rust:latest
        args:
        - --enable-control-plane
        - --grpc-port=9091
        - --storage-type=s3
        - --bucket=kapture-captures
        - --capture-id=prod/api-gateway
        - --target-url=http://staging:8080
        - --rate-mode=original
        - --concurrency=200
        - --resume
        ports:
        - name: metrics
          containerPort: 9090
        - name: grpc
          containerPort: 9091
  volumeClaimTemplates:
  - metadata:
      name: checkpoints
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 1Gi
```

## 8. Performance Targets

| Metric | Target | Notes |
|--------|--------|-------|
| Max sustained QPS | 500,000 | Single instance, HTTP/2 |
| Memory (100GB dataset) | <512 MB RSS | Streaming, no full load |
| p99 timing jitter | <5 ms | Original timing mode |
| Checkpoint interval | 30 s | Configurable |
| Resume time | <10 s | From crash to first request |
| Startup time | <5 s | Object listing + first prefetch |

## 9. Testing Strategy

1. **Unit tests**: Each component in isolation (reader, pacer, sender, checkpoint)
2. **Integration tests**: Full pipeline with `mockito` HTTP server
3. **Property tests**: `proptest` for pacer timing accuracy
4. **Benchmark tests**: `criterion` for throughput measurement
5. **Chaos tests**: Kill/restart during replay to validate checkpoint resume
6. **Conformance tests**: Compare Rust engine output against Go engine for same input

## 10. Implementation Phases

### Phase 1: Core Engine (4 weeks)
- Storage readers (S3, filesystem)
- Timing engine (all three modes)
- HTTP sender pool
- CLI interface
- Basic checkpoint/resume

### Phase 2: Production Hardening (3 weeks)
- GCS reader
- Prometheus metrics
- Structured logging
- Drift correction
- HTTP/2 support
- Docker image + Helm chart

### Phase 3: Control Plane (2 weeks)
- gRPC service
- Runtime config updates
- Progress streaming
- Integration with Kapture hub

### Phase 4: Advanced Features (2 weeks)
- WASM-based request transformers
- Response validation/diffing
- Distributed replay (multiple instances splitting the dataset)
- OpenTelemetry tracing

## 11. Open Questions

1. **Distributed replay**: Should multiple engine instances split a single capture?
   This would require a coordinator (Kapture hub) to assign object ranges.
   
2. **gRPC replay**: The current design focuses on HTTP. gRPC replay requires
   understanding protobuf schemas — should we support this via reflection or
   require schema registration?

3. **Response comparison**: Should the engine support comparing replay responses
   against captured responses (regression testing)? This requires capturing
   responses during the original traffic capture.

4. **WASM transformers**: Plugin transformers via WebAssembly would allow safe,
   sandboxed request modification. Is the complexity justified vs. a simpler
   config-driven approach?
