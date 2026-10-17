# eCapture L7 capture backend

Kapture can collect plaintext TLS observations from a pinned eCapture process,
reconstruct HTTP/1.1 and HTTP/2/gRPC exchanges, and write L7 evidence plus replay
datasets to S3, GCS, EFS, or EBS. Gateway RequestMirror capture remains the default.

## Install and capture a pod

Build the optional image and make it available to the workload cluster:

```bash
docker build -f Dockerfile.ebpf -t your-registry/kapture-ebpf:dev .
docker push your-registry/kapture-ebpf:dev
helm upgrade --install kapture charts/kapture \
  --namespace capture-system --create-namespace \
  --set ebpf.enabled=true \
  --set ebpf.image.repository=your-registry/kapture-ebpf \
  --set ebpf.image.tag=dev
kubectl apply -f config/crd/bases/
```

Use an existing `CaptureStorage` in the same namespace, then select the target
Pod explicitly. The controller waits for scheduling and pins one collector
Deployment to that Pod's node. No mirror filter, Service, or HPA is created.

```yaml
apiVersion: capture.gateway.io/v1alpha1
kind: TrafficCapture
metadata:
  name: openssl-session
  namespace: app
spec:
  backend: EBPF
  targetRef:
    group: ""
    kind: Pod
    name: openssl-service-0
  storageRef:
    name: captures
  capture:
    includeHeaders: true
    includeBody: true
    maxBodyBytes: 1048576
    duration: 10m
  ebpf:
    mapPages: 256
    memoryBytes: 134217728
    historyBytes: 33554432
    history: 30s
    triggerStatus: 500
    pcap: true
```

`triggerStatus: 0` exports every matching exchange. An HTTP error at or above a
nonzero threshold, or a nonzero gRPC status, exports recent exchanges retained on
that same connection. Path/header/percentage filters still apply. Idle history
and parser state are bounded; eviction is counted and pending incomplete
exchanges carry an explicit reason.

The backend choice is immutable. Create a new TrafficCapture to switch between
Mirror and EBPF. `spec.agent` autoscaling and payload plugins are rejected for
this backend. Deletion and duration expiry stop the collector; normal termination
allows bounded final flushing. A node crash cannot guarantee that final flush.

## Host and workload requirements

The provided image builds eCapture at
`afcc9345ff8f09f96811a158cf768d284b4c7e1b` with CO-RE assets. It requires a Linux
host with usable kernel BTF at `/sys/kernel/btf/vmlinux`, working BPF/perf/uprobe
facilities, and an OpenSSL workload supported by that revision. The collector
runs as root with `hostPID` and a privileged security context, and mounts host
`/sys` read-only. Cluster admission must permit that explicitly enabled workload.

Process discovery matches the target Pod UID in host cgroups and libraries in
`/proc/PID/maps`; command-name matching is not used. At most eight OpenSSL
processes are collected per Pod. Optional `ebpf.pid` narrows this to one host PID
that must belong to the Pod; `ebpf.libraryPath` overrides library discovery.
Process start-time checks stop a lease when its PID is reused. A process without
an attributable network tuple is counted and excluded from reconstruction.

Readiness requires an eCaptureQ connection and the producer's successful probe
attachment message. It remains false while no eligible process exists. Collector
exit, protocol corruption, or a broken source fails the agent rather than
silently declaring an idle session healthy.

Go TLS, GnuTLS, kTLS, QUIC, custom BIO transport attribution, and a shared
per-node collector are not implemented in this backend. The container bundles
OpenSSL support through eCapture; it does not claim all TLS runtimes work.

EFS/EBS paths must already be mounted on the selected node; the collector mounts
the configured `mountPath` into its Pod. S3 credential Secrets use standard
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment keys. GCS credential
Secrets contain `credentials.json`. An existing workload-identity service account
can be selected using `ebpf.serviceAccountName` in Helm values.
Credential Secrets must be in the capture namespace.

## Evidence, redaction, and replay

Ordinary replay request objects retain Kapture's existing layout:

```text
{prefix}/{captureID}/{date}/{agent}-{sequence}.jsonl.gz
```

Evidence is deliberately stored outside that dataset prefix:

```text
{prefix}/evidence/{captureID}/{epochHash}/{exchangeID}.json
{prefix}/evidence/{captureID}/{epochHash}/{exchangeID}.pcapng
{prefix}/evidence/{captureID}/sessions/{sessionID}.json
```

The exchange JSON contains request/response headers, trailers, bounded bodies,
gRPC message envelopes, stream IDs, observation references, quality fields, and
HPACK table checkpoints taken before each selected field block. Checkpoints are
bounded dynamic tables, not unbounded connection prefixes. For unredacted proofs,
`RestoreHeaderState` reproduces their indices in a standard HPACK decoder.

Default credential-header redaction applies to requests, responses, trailers,
checkpoints, and synthetic captures. Encoded HPACK blocks are omitted when a
header-redaction policy applies, because they could otherwise retain credentials.
Such proofs are marked `derived`. `includeHeaders: false` also removes checkpoint
entries; `includeBody: false` removes bodies and gRPC payloads. Disabling header
redaction uses the existing `capture.disableHeaderRedaction` setting.

The PCAPNG is an explicitly labeled **reconstructed plaintext capture** with a
synthetic TCP handshake and sequence space. HTTP/2/gRPC exchanges get a fresh
HPACK context and stream 1; their original stream IDs remain in JSON and the
interface comment. This makes a selected exchange independently inspectable in
Wireshark. The PCAPNG does not preserve original ciphertext packets, original TCP
segmentation, RTT, ACK timing, or TLS-record attribution. Raw packet-plus-secret
retention remains a separate, unimplemented part of the longer-term design.

HTTP requests enter the replay dataset only when independently framed, not body
truncated, and free of known gaps. Original IDs, observation timestamps, body
bytes, and query strings are retained. Single observed uncompressed gRPC request
messages with at most one response message are projected as existing `gRPC`
records with the five-byte envelope removed; use the ghz engine's documented
method-mix semantics and schemas. Streaming/compressed gRPC stays inspectable in
the evidence bundle and is not automatically projected for replay.

## Interpreting quality

`framingComplete` means the request and response message boundaries were observed.
`captureIntegrity` remains `unverified` for upstream eCaptureQ. That producer does
not export socket-generation proofs or all pre-publication loss/original-length
information. `identity` is therefore inferred from producer epoch, PID and tuple;
`packetAttribution` is unavailable. Do not equate a replayable request with a
forensically complete wire capture.

Sequence discontinuities, reported perf loss, oversized upstream events, missing
decoder state, and limits invalidate affected pending exchanges. The pinned
OpenSSL event caps payloads at 16 KiB; an event reaching that cap conservatively
taints its flow because a wrapper cannot establish whether a tail was lost.
Unknown HPACK gaps invalidate decoding, rather than guessing later headers.
Decoder errors and state-limit eviction conservatively taint the producer epoch,
so reappearing tuples cannot silently discard a known loss warning.

Producer timestamps can be derived from monotonic values; if they are not usable
UTC timestamps, the adapter explicitly uses collector receive time. Fine-grained
network latency cannot be inferred from those receive timestamps. Initial
HTTP/1.1 reconstruction buffers messages up to 4 MiB; larger or memory-limited
flows become incomplete. HTTP/2 bodies stream into per-message bounded buffers.
Close-delimited HTTP/1 responses require an explicit close observation; upstream
eCaptureQ does not forward socket closes, so those responses remain incomplete.

## CLI and offline reconstruction

The same capture agent supports an explicit PID outside Kubernetes:

```bash
sudo ./kapture-agent --backend EBPF \
  --ecapture-binary /path/to/ecapture --ecapture-pid 1234 \
  --capture-id local/session --storage-type efs \
  --storage-config '{"MountPath":"/tmp/kapture-captures"}'
```

Use `--ecapture-url ws://127.0.0.1:28257/` to consume a producer already running
`ecapture tls -m text --eventaddr /dev/null --ecaptureq ws://127.0.0.1:28257/`.
The connection is not silently rejoined midstream after failure.

`--l7-input observations.jsonl` reconstructs recorded `l7.Observation` JSONL
without attaching probes. This is also the deterministic test boundary; it is
not a live-capture mode. Each record includes epoch, flow, read/write lane,
producer sequence, ID, timestamp, originalLength, and base64-encoded data.

The health endpoint is `/healthz` on port 8081. `/metrics` includes attached
producer count, observations, loss intervals, unattributed events, exchanges,
replay requests, retained bytes, and parser bytes. Session manifests retain
summary counters and the pinned producer revision. Artifact payloads never enter
the relational capture-history database.

## Validation and benchmarks

```bash
go test ./internal/l7 ./internal/ecapture ./internal/ebpf ./internal/spoke -race
make generate-deepcopy generate-crds
helm unittest charts/kapture
make benchmark-l7
make benchmark-flowscope # Go 1.26 and Rust 1.97.0; pinned Rust FlowScope
```

`TestNativeOpenSSLCapture` is selected with `KAPTURE_NATIVE_EBPF=1`. It creates an
OpenSSL TLS server and verifies actual probe observations, evidence redaction,
replay output, and shutdown. Missing root/BTF/probes are failures once selected.
The `eBPF Native Capture` workflow builds the pinned image and runs this test
with a host PID namespace on an isolated Linux runner.

The end-to-end recorded-observation test uses the real evidence writer and
existing replay reader/sender. Typed protocol tests exercise WebSocket/protobuf,
readiness, cancellation, malformed frames, and process lifetime checks. Neither
is a substitute for the native test on a BPF-capable host. See the
[comparison protocol](design/ebpf-l7-capture-comparison.md) for acquisition and
FlowScope comparisons; user-space benchmarks do not establish kernel overhead.
