# Application capture overhead benchmark

`run.py` measures the effect on the application, not just parser execution time.
It uses a real Linux kernel; QEMU software-emulation timings are unsuitable for
this experiment. The default experiment runs 126 trials (roughly 20 minutes plus
builds): three repeats, three application profiles, two load patterns, seven
capture configurations. It writes every raw row before continuing.

## Measured configurations

| Mode | Acquisition and output | Comparison baseline |
| --- | --- | --- |
| `none` | Direct HTTPS, no capture | Application baseline |
| `ebpf-acquire` | Patched eCapture, typed WebSocket decode, process-scope checks, drain observations | `none` |
| `ebpf-go` | Same acquisition plus Kapture L7 reconstruction; validate and discard exchanges | `none`; incremental cost over acquisition |
| `ebpf-rust` | Same acquisition, bounded binary pipe bridge, pinned Rust FlowScope HTTP exchange parser; validate and discard | `none`; bridge and IPC included |
| `ebpf-export` | Production Kapture pipeline: redacted request/response JSON, synthetic PCAPNG, replay projections, synced local artifact files | `none`; incremental cost over reconstruction |
| `envoy` | One Envoy TLS-terminating proxy, TLS upstream to the application, no mirroring | `none` |
| `envoy-mirror` | Same Envoy plus 100% asynchronous request mirroring into the production Kapture sink and local request storage | `envoy` for incremental mirroring; `none` for total proxy and capture cost |

The pinned Rust FlowScope is a library, not an independent TLS acquisition
mechanism. Its passive HTTP parser needs plaintext. This experiment supplies the
same eBPF plaintext to both parsers. It does not label encrypted packet capture
as successful HTTP capture or omit the adapter's CPU, memory, and IPC.

The mirror path captures requests. eBPF reconstruction also captures responses.
The parser-only modes do not persist evidence. The acquisition mode does not
verify L7 completeness. These differences matter when comparing their costs.
The Envoy experiment measures the local mirroring data plane. It does not
measure a full Istio installation, a second sidecar, Kubernetes networking,
remote sink traffic, a control plane, or production mTLS certificate management.

## Workload and accounting

All configurations use the same OpenSSL application binary, TLS 1.3 (AES-128-GCM on the application leg), HTTP/1.1
POSTs, 16 persistent client workers, exact request IDs, and deterministic bodies.
Profiles: 128-byte POST; 8-KiB POST; 8-KiB POST with 64 additional SHA256 body
hashes. There is no application delay or dependency service. Every response
returns its request ID and body digest; errors and missed arrival slots are
reported. The load result records the negotiated TLS cipher suites. Exported request/response IDs, hashes, bodies, framing and redaction are checked again from the actual evidence files after the measured interval. Each trial starts fresh processes and includes a one-second warmup.

Fixed load offers 100 requests/sec for six seconds. A request's latency starts
at its scheduled arrival, including queue/scheduler delay; missed arrivals are
not omitted. Saturation uses a separate six-second closed-loop run with 16
concurrent clients. Its latency is explicitly a closed-loop distribution; it is
not a latency-at-equal-load comparison. HTTP/TLS connection startup is included
and amortized over each run. Captured bodies stay below the probe's 16-KiB cap.

The app is pinned to one CPU and the load generator to another. Capture and
proxy processes use the remaining CPUs. On a two-CPU host capture shares the
app's CPU, explicitly recorded in metadata. The workflow requires cgroup v2
accounting: application, collector (including producer and Rust children), proxy,
and load generator have separate groups. CPU deltas exclude warmup and drain.
CPU executed by probes in application context is charged to the application.
CPU microseconds/request and extra CPU cores are both reported. Sampled anonymous
memory and cgroup peak total memory are separate; the latter includes file cache
and may include kernel allocations. Neither is Go/Rust heap size alone.

Modes are shuffled with a reproducible seed within each repetition. Report
medians and ranges; three repeats on one shared CI runner cannot establish
production percentiles or confidence intervals. No scheduler/frequency governor
or physical-host isolation is assumed. Do not infer CPU savings from lower
captured counts. Every fixed-load L7 run must reproduce all unique IDs and exact
bodies. Saturation rows retain measured capture yield and loss counters, including
overruns. Lossy rows cannot establish complete-capture capacity.

Local EFS-configured storage is a filesystem here, not a measured AWS EFS mount.
Evidence artifacts use the production fsync behavior; request projections use
the production batching/gzip path. Network/object storage and its latency are
outside this experiment. Capture source integrity remains `unverified` even
when the synthetic oracle matches. All traffic, certificates and credentials
are generated fixtures. The private key and payload artifacts are removed;
raw counters, synthetic operational logs and version metadata remain.

## Run

Requires Linux x86-64 with BTF, root/BPF permissions, writable cgroup v2, Go 1.26,
Rust 1.97, OpenSSL 3.0 development headers, a C compiler, and Envoy 1.36.2. The
[workflow](../../.github/workflows/overhead.yaml) pins the Envoy image digest and
builds the patched producer from `Dockerfile.ebpf` at the exact eCapture revision.

```sh
go build -o /tmp/kapture-overhead-bench ./tools/overhead-bench
cc -O2 -Wall -Wextra -Werror -pthread tools/overhead/openssl-app.c \
  -lssl -lcrypto -o /tmp/kapture-overhead-app
cargo +1.97.0 build --release --locked --manifest-path tools/flowscope-bench/Cargo.toml
python3 tools/overhead/check.py tools/flowscope-bench/target/release/kapture-flowscope-live
sudo python3 tools/overhead/run.py --require-cgroup --output /tmp/application-impact \
  --app /tmp/kapture-overhead-app --helper /tmp/kapture-overhead-bench \
  --ecapture /path/to/patched/ecapture \
  --rust "$PWD/tools/flowscope-bench/target/release/kapture-flowscope-live" --envoy /path/to/envoy
```

`metadata.json` records exact source and binary hashes, host, affinity and
parameters. `trials.jsonl` retains nested CPU/memory and capture counters,
`trials.csv` is flattened, and `summary.md` is the initial measured table.
Capture evidence is deleted after each trial unless `--keep-captures` is set.
Missing prerequisites fail selected native modes; there is no silent fallback.
