# eCapture and FlowScope: L7 Capture Comparison and Measurement Protocol

**Status:** Source comparison and analytical model completed. An initial
eCaptureQ backend and user-space benchmarks now exist; live comparative capture
measurements have not been run. See the [backend guide](../ebpf-capture.md).

**Date:** 2026-10-17

This accompanies the [Loom design proposal](ebpf-l7-capture.md). The recommended
implementation uses eCapture as the capture foundation and adds dependency-aware
retention, reconstruction provenance, and Kapture integration.

## Baselines and source pins

Two different projects named FlowScope are relevant. Cover both until the
intended comparator is identified; they evaluate different parts of the system.

| Project | Inspected revision | Role |
| --- | --- | --- |
| [gojue/ecapture](https://github.com/gojue/ecapture/tree/afcc9345ff8f09f96811a158cf768d284b4c7e1b) | `afcc9345ff8f09f96811a158cf768d284b4c7e1b` | eBPF capture implementation baseline |
| [emmericp/FlowScope](https://github.com/emmericp/FlowScope/tree/4a1f0f458b2f85aa92e3d941ce8c3f35b75bef7e) | `4a1f0f458b2f85aa92e3d941ce8c3f35b75bef7e` | Triggered high-speed packet-history recorder |
| [p13marc/flowscope](https://github.com/p13marc/flowscope/tree/97d9d8e10edd4a5ab2a31cdff7cc674993e354e2) | `97d9d8e10edd4a5ab2a31cdff7cc674993e354e2` | Rust TCP/L7 reconstruction library, version 0.25.3 at this pin |

These revisions make the assessment reproducible. They are observations of the
sources inspected on this date, not permanent statements about future releases.

## Capability comparison

This table compares source-backed capabilities with proposed behavior. It is
not a performance ranking or a claim that an unlisted feature is impossible.

| Dimension | eCapture, pinned | FlowScope packet recorder | flowscope Rust library | Proposed Kapture/Loom addition |
| --- | --- | --- | --- | --- |
| Capture boundary | Host eBPF, TLS/runtime probes, packet path | libmoon/DPDK packet ingest | Caller supplies frames or stream bytes | Reuse eCapture and add workload/session lifecycle |
| TLS visibility | Plaintext and secret capture for supported adapters | No host TLS-secret extraction path established by the reviewed architecture | TLS handshake metadata; HTTP parsers need plaintext input | Prefer captured packets plus usable secrets; retain plaintext fallback |
| L7 parsing | Existing HTTP/1 and HTTP/2 display parsers; PCAP can use Wireshark | Lua analyzers and per-flow dumping | TCP reassembly, HTTP/2/HPACK and gRPC stream/status helpers | Complete exchange materialization with source references |
| Triggered history | Inspected PCAP path records packet/secret streams | Existing QQ history buffer and triggered flow dumps | Caller owns capture history and persistence | Select an exchange and retain its decoder dependencies |
| Loss/ambiguity | Perf loss signals; bounded plaintext-event ABI | Capture/queue/dump behavior depends on module and deployment | Explicit stream gaps, stop reasons, and configurable overlap handling | Propagate capture, identity, and decoder uncertainty into each exchange |
| Deployment | Host agent with required BPF/probe access | Dedicated packet-I/O setup and supported NICs | In-process Rust library with no required async runtime | Opt-in Kapture backend plus object/file artifacts and request projection |

Sources for the eCapture rows are its
[OpenSSL event definition](https://github.com/gojue/ecapture/blob/afcc9345ff8f09f96811a158cf768d284b4c7e1b/internal/probe/openssl/event.go),
[kernel OpenSSL probe](https://github.com/gojue/ecapture/blob/afcc9345ff8f09f96811a158cf768d284b4c7e1b/kern/openssl.h),
[PCAPNG session](https://github.com/gojue/ecapture/blob/afcc9345ff8f09f96811a158cf768d284b4c7e1b/internal/output/pcapng/session.go),
and [HTTP/2 request parser](https://github.com/gojue/ecapture/blob/afcc9345ff8f09f96811a158cf768d284b4c7e1b/pkg/event_processor/http2_request.go).

The pinned HTTP/2 request display parser explicitly lacks HEADERS/CONTINUATION
assembly and relies on preface-based detection. This is a limit of that path,
not proof that eCapture's packet-plus-secret output cannot be decoded correctly
by Wireshark. The OpenSSL plaintext event has a 16 KiB payload capacity; validate
full-operation completeness rather than treating each event as an entire call.

The packet recorder's
[architecture](https://github.com/emmericp/FlowScope/blob/4a1f0f458b2f85aa92e3d941ce8c3f35b75bef7e/README.md)
already separates early analysis from delayed dumping, preserves buffered
history, and handles expiration rules with delayed consumers. Triggered history
capture is therefore established prior work.

The Rust library's
[HTTP/2 module](https://github.com/p13marc/flowscope/blob/97d9d8e10edd4a5ab2a31cdff7cc674993e354e2/src/http2/mod.rs),
[session adapter](https://github.com/p13marc/flowscope/blob/97d9d8e10edd4a5ab2a31cdff7cc674993e354e2/src/http2/session.rs),
and [reassembler](https://github.com/p13marc/flowscope/blob/97d9d8e10edd4a5ab2a31cdff7cc674993e354e2/src/reassembler.rs)
already cover stream IDs, HPACK, continuation frames, gRPC trailers, and explicit
gaps. Tolerating a missing HTTP/2 preface does not reconstruct unknown earlier
HPACK state. Its [bounded-memory guide](https://github.com/p13marc/flowscope/blob/97d9d8e10edd4a5ab2a31cdff7cc674993e354e2/docs/bounded-memory.md)
also distinguishes default per-parser caps from optional cross-flow caps.

The resulting design direction is to reuse eCapture's observation machinery,
evaluate existing parsers before replacing them, and test Loom's proposed
dependency-complete export. A Rust parser sidecar is an option to evaluate, not
an assumption that Kapture should be rewritten or that IPC is free.

## What has actually been measured

The local feasibility check observed Linux `6.18.55` on x86_64, effective UID
1000, and both effective and bounding capability sets equal to zero. BTF,
tracing, and hugepage paths checked by the preflight were absent. `ecapture`,
`clang`, `bpftool`, `tshark`, `tcpdump`, `cargo`, and `rustc` were unavailable.

This prevents a native live eBPF trial and the Rust-library benchmarks in this
workspace at the time of the initial design review. No DPDK/NIC testbed was
established. The subsequent implementation adds a runnable eCaptureQ backend;
its validation is documented separately. The retention calculation below was executed, but it is
an analytical scenario, not a capture benchmark.

| Result category | Current evidence |
| --- | --- |
| Architecture and API comparison | Completed against the pinned sources above |
| Logical retained-byte model | Calculated below with reproducible inputs |
| eCapture application overhead and capture loss | Not measured here |
| FlowScope capture throughput | Not rerun here |
| Rust flowscope parser throughput | HTTP/1 exchange reconstruction measured below; other protocols not benchmarked |
| Loom live capture quality, CPU, memory, or throughput | Requires a BPF-capable test host; user-space fixture tests and benchmarks are separate |

Do not populate unrun results with zero or claim a speedup from these rows.

The FlowScope authors
[reported 120 Gbit/s capture to memory with 128-byte packets](https://www.net.in.tum.de/fileadmin/bibtex/publications/papers/FlowScope.pdf)
on their documented multi-NIC, dual-Xeon testbed. This is a published external
result, not a measurement in this repository or a TLS/L7 decoding benchmark.
It does not establish a throughput target already met by eBPF or Loom.

## Implemented HTTP/1 parser comparison

The initial implementation was measured on 2026-10-17 against the pinned Rust
`p13marc/flowscope` revision above. This is **reconstruction only**: identical
plaintext POST requests with fixed-length bodies and a 204 response, one new
connection per exchange, with exact exchange counts, request body lengths, and
response status checked by both harnesses. No BPF, TLS, TCP reassembly, filtering,
artifact persistence, redaction, or replay delivery is timed.

| Request body | Kapture median µs/exchange (range) | Rust FlowScope median µs/exchange (range) |
| --- | ---: | ---: |
| 128 B | 15.61 (13.28–20.26) | 1.16 (1.08–1.29) |
| 96 KiB | 152.06 (143.18–290.17) | 36.27 (35.23–49.94) |

Each row has five sequential trials, alternating implementation order. Small-body
trials contain 200,000 exchanges and large-body trials 20,000; every trial
reconstructed the expected count. The host reports an Intel Xeon Platinum 8573C,
five visible CPUs, a four-CPU cgroup quota, and 16 GiB memory. Go 1.26.0 uses
`GOMAXPROCS=1`; Rust 1.97.0 uses the release profile. This is a shared virtual
workspace, without CPU affinity or a controlled frequency governor. The ranges
show meaningful scheduling noise; treat these as initial observations.

FlowScope's `HttpExchangeParser` is faster in this workload. The implementations
also produce different evidence: Kapture constructs observation references,
source-quality state, identities, and bounded multi-flow state; the Rust harness
constructs FlowScope's HTTP exchange objects. The test does not isolate the cost
of those differences, compare HTTP/2 checkpoints, or establish live-capture
performance. It gives no measured speed advantage to Kapture.

Reproduce with Go 1.26 and Rust 1.97.0 installed:

```bash
make benchmark-flowscope > comparison.csv
```

The [harness](../../tools/benchmark-l7.sh) pins FlowScope and its dependency lock
file. [Raw trials](../benchmarks/2026-10-17-http1.csv) are in execution order,
with four rows per trial. `make benchmark-l7` additionally measures the Go
HTTP/2 checkpoint path and allocations; it is not a cross-tool HTTP/2 benchmark.

## Reproducible retained-byte model

Consider one multiplexed connection containing 1,000 exchanges, each with 1 MiB
of body data and 1 KiB of header/control data. Compare retaining that whole
connection with retaining selected bodies, all header/control data, and a 64 KiB
checkpoint. Keeping all control data is a deliberately conservative dependency
policy for this example.

```text
whole_connection = exchanges * (body_bytes + control_bytes)
dependency_bundle = selected * body_bytes
                  + exchanges * control_bytes
                  + checkpoint_bytes
```

| Selected exchanges | Whole-connection bytes | Dependency-bundle bytes | Logical reduction |
| ---: | ---: | ---: | ---: |
| 1 / 1,000 | 1,049,600,000 | 2,138,112 | 99.7963% |
| 10 / 1,000 | 1,049,600,000 | 11,575,296 | 98.8972% |
| 100 / 1,000 | 1,049,600,000 | 105,947,136 | 89.9060% |
| 1,000 / 1,000 | 1,049,600,000 | 1,049,665,536 | -0.0062% |

Reproduce the calculation with Python 3:

```python
exchanges = 1000
body_bytes = 1024 * 1024
control_bytes = 1024
checkpoint_bytes = 64 * 1024
whole = exchanges * (body_bytes + control_bytes)
for selected in (1, 10, 100, 1000):
    kept = selected * body_bytes + exchanges * control_bytes + checkpoint_bytes
    print(selected, whole, kept, f"{100 * (1 - kept / whole):.4f}%")
```

This excludes packet/TLS overhead, indexing, allocator overhead, duplicate
representations, compression, and checkpoint variability. It models a retention
policy, not any tool's actual disk output. Retaining everything slightly
increases storage because of the checkpoint. It demonstrates neither CPU savings
nor a unique advantage over another tool extended with the same retention policy.

## Benchmark scopes

Measure three scopes separately so acquisition throughput is not mistaken for
TLS visibility or L7 correctness.

1. **Acquisition:** Compare unmodified eCapture packet/secret capture with the
   eCapture-based collector before decoding. Evaluate the DPDK FlowScope recorder
   on a dedicated mirrored-feed setup with equivalent packet lengths, filters,
   offered load, capture budget, and output durability. Record deployment
   differences; an off-host sensor and an in-host agent affect application CPU
   differently. Report wire rate and captured-byte rate separately.
2. **Reconstruction:** Feed identical plaintext and timing fixtures to the
   eCapture-derived parser path, Rust flowscope, and Loom. Account for adapters,
   serialization, IPC, and output construction separately. A parser supplied
   plaintext has not performed TLS capture or decryption.
3. **Retention and export:** Apply the same selected-exchange query and byte
   budget to full-connection retention and dependency-preserving retention.
   Measure independently replayable exchange yield, retained bytes, checkpoint
   cost, and reconstruction latency. Do not equate omitted bodies with complete
   exchange recovery.

For an end-to-end comparison, include acquisition, decryption, decoding,
retention, artifact writing, and verification for every contender. If a component
requires external key logs, count their acquisition cost or state that they were
provided. Unsupported cases remain explicit coverage results.

## Workloads and correctness oracle

| Workload | What it resolves |
| --- | --- |
| OpenSSL HTTP/1.1, new connection per request, 96 KiB body | Reproduce the upstream eCapture baseline before adding features |
| HTTP/1.1 keepalive, chunked bodies, pipelining, and partial I/O | Framing, ordering, accepted lengths, and reuse |
| HTTP/2 with 1, 32, and 256 concurrent streams | Multiplexing, stream identity, and bounded-state behavior |
| Dynamic HPACK entries older than the body-history window; split continuation blocks | Decoder dependency preservation and standalone selected-exchange export |
| gRPC unary/streaming and trailers-only errors with HTTP 200 | Message boundaries and application status rather than HTTP status alone |
| TLS 1.2/1.3, large API calls, and supported runtime adapters | Event-size limits, key coverage, and per-adapter completeness |
| Late attach, unknown header gap, overflow, reordering, FD reuse, and collector restart | Honest uncertainty, identity lifetime, and state recovery |
| Sparse selection on long-lived connections | Retention benefit and checkpoint cost |

Use controlled endpoint transcripts containing unique exchange IDs, exact body
lengths and hashes, request/response headers, trailers, and stream IDs where
available. Markers establish presence, not full-body correctness. Compare whole
body hashes and framing too. Apply equivalent redaction to the oracle and all
semantic outputs; do not conflate intended redaction with capture loss.

## Metrics and decision rules

| Metric | Definition |
| --- | --- |
| Complete-exchange recovery | Exact supported exchanges / expected supported exchanges, with the support set declared before the run |
| False completeness | Exchanges labeled complete that differ from ground truth; acceptable count is zero |
| Attribution accuracy | Correct connection/stream bindings / verified bindings; report unknown bindings separately |
| Loss | Endpoint bytes/messages missing, perf lost samples, copy failures, and unknown intervals; report all, not just reader drops |
| Application impact | Paired throughput change and p50/p95/p99 latency changes versus no instrumentation |
| Resource cost | Collector and decoder CPU, workload CPU delta, peak RSS, BPF/perf-map allocation, spool use, and disk/network output |
| Capture capacity | Sustained offered rate with declared loss and completeness, expressed in both packets/s and bytes/s |
| Retention efficiency | Persisted bytes per independently reconstructable selected exchange, including checkpoints and indexes |
| Export latency | Trigger-to-durable-bundle and bundle-to-verified-exchange latency |

Use the same machine class, kernel, runtime/library builds, workload seed,
offload settings, redaction, body limits, and storage sink across comparable
runs. Record CPU pinning, NUMA placement, map sizes, selected features, and all
commit IDs. Measure the entire process tree plus kernel buffers; RSS alone
misses eBPF/per-CPU allocations.

Run at least five paired trials per operating point after warmup, alternate
baseline/capture order where the harness permits, and report dispersion along
with per-run loss. Keep upstream behavior unchanged when reproducing its
baseline. Use separate steady-load and saturation runs: overload must produce
reported loss or incompleteness rather than an apparently fast complete result.
Reject a claimed improvement if it increases false completeness or silently
shrinks the supported workload set.

## Starting with maintained upstream harnesses

eCapture's pinned [benchmark documentation](https://github.com/gojue/ecapture/blob/afcc9345ff8f09f96811a158cf768d284b4c7e1b/docs/performance-benchmarks.md)
and [TLS harness](https://github.com/gojue/ecapture/blob/afcc9345ff8f09f96811a158cf768d284b4c7e1b/test/benchmark/tls/run_benchmark.sh)
already provide paired uninstrumented/text-capture trials, marker loss, perf
loss, application latency, and collector CPU/RSS. Reuse them first on a suitable
Linux host; these commands were not run in this workspace:

```bash
git clone https://github.com/gojue/ecapture.git /tmp/kapture-ecapture-baseline
cd /tmp/kapture-ecapture-baseline
git checkout afcc9345ff8f09f96811a158cf768d284b4c7e1b
git submodule update --init --recursive
make clean
make all
sudo env BENCH_RUNS=5 BENCH_FAIL_ON_LOSS=1 make benchmark-tls
```

The upstream harness covers short-lived HTTP/1.1 text capture; it does not
establish HTTP/2 correctness, PCAPNG-mode overhead, or complete body recovery.
Add those workloads and modes explicitly when implementing Loom's measurement
adapter. Keep the original upstream run as the control.

Its `BENCH_MAP_PAGES` setting is pages per CPU, not bytes. The documented default
of 8,192 pages means 32 MiB per CPU per perf reader on 4 KiB-page hosts. Record
this allocation when comparing collectors and respect the test host's memory
budget. Do not compare this configuration with a smaller buffer without saying so.

Rust flowscope's [performance guide](https://github.com/p13marc/flowscope/blob/97d9d8e10edd4a5ab2a31cdff7cc674993e354e2/docs/performance.md)
provides Criterion layer benchmarks. On the pinned checkout, with its required
Rust toolchain installed, start with:

```bash
cargo bench --locked --all-features --bench reassembler
cargo bench --locked --all-features --bench session_driver
```

These measure specific user-space paths, not NIC capture, host probe overhead,
or a full HTTP/2/gRPC reconstruction pipeline. Use the shared workload fixtures
for the latter. Follow the packet recorder's pinned README and published
MoonGen testbed for the DPDK acquisition comparison; preserve its NIC/module
configuration in the result manifest.

Until comparative runs on an appropriate test host are completed, the
performance conclusion remains open. The current recommendation rests on code
reuse and capability fit, not an unmeasured claim that Loom is faster.
