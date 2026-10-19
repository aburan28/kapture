# Application overhead measurements

Source: `de8e87a888448d0be341ab417919d3d334fcd306`; Linux `6.18.55`; INTEL(R) XEON(R) PLATINUM 8573C. CPU placement: {'application': [0], 'load': [1], 'capture': [2, 3, 4]}. Accounting: process-cpu-clock; summed RssAnon/RSS. Three short repetitions on one shared native Linux host; ranges below are observed minima and maxima, not confidence intervals.

CPU deltas compare each mode with the no-capture trial from the same repetition and workload. Latency uses a fixed offered rate; saturation throughput is a separate closed-loop experiment. Acquisition alone has no L7 completeness oracle. The Rust row includes the common eBPF feed and pipe adapter. The mirror row captures requests; eBPF L7 also reconstructs responses. Local file export includes JSON, synthetic PCAPNG and replay projections.

Measured configurations: none, envoy, envoy-mirror.

**No eBPF overhead was measured on this host.** Native BPF comparisons remain pending on a BPF-capable runner. These results compare only no capture, one Envoy proxy and Envoy request mirroring; they must not be combined with timing from another host as a common-host comparison.

## small

At 100 requests/sec; values are median (min–max). CPU µs/request includes probe work in application context. Extra CPU includes producer, parser, adapter, storage and proxy as applicable. Memory is sampled anonymous memory, including startup/warmup, not a steady-state heap guarantee.

| Configuration | App CPU µs/request | App CPU change % | Total app+capture CPU change % | Extra CPU mcores | Extra anonymous MiB | p95 latency ms | Unique L7 capture yield |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| No capture | 92.7 (79.5–96.7) | 0.0 (0.0–0.0) | 0.0 (0.0–0.0) | 0.0 (0.0–0.0) | 0.0 (0.0–0.0) | 1.338 (1.316–1.468) | — |
| Envoy, no mirror | 77.9 (76.6–80.9) | -12.7 (-20.8–-2.0) | 317.5 (287.9–372.4) | 29.6 (27.7–32.5) | 14.1 (14.1–14.2) | 1.832 (1.773–1.871) | — |
| Envoy + request mirror | 75.4 (63.6–99.8) | -20.1 (-22.0–7.7) | 796.9 (715.5–932.8) | 70.4 (64.1–85.5) | 24.0 (23.8–24.2) | 1.845 (1.805–3.260) | 100.00 (100.00–100.00)% |

Saturation, 16 persistent workers. A low capture yield means the row is not complete-capture capacity. Loss can make capture appear less expensive by discarding work.

| Configuration | App throughput requests/sec | Throughput change % vs no capture | Capture yield % | Trials meeting every capture oracle |
| --- | ---: | ---: | ---: | ---: |
| No capture | 22770.7 (21677.9–25192.1) | 0.0 (0.0–0.0) | — | — |
| Envoy, no mirror | 12795.5 (8917.8–15335.0) | -43.8 (-58.9–-39.1) | — | — |
| Envoy + request mirror | 9533.0 (6802.4–10062.1) | -60.1 (-68.6–-58.1) | 100.00 (100.00–100.00) | 3/3 |

Incremental mirroring against the existing Envoy path: paired total app+proxy+capture CPU change **95.3 (89.9–166.3)%**; p95 latency change **1.8 (-1.4–77.9)%**; saturation throughput change **-25.5 (-34.4–-23.7)%**. These differ from the cost of introducing Envoy and mirroring together.

## upload

At 100 requests/sec; values are median (min–max). CPU µs/request includes probe work in application context. Extra CPU includes producer, parser, adapter, storage and proxy as applicable. Memory is sampled anonymous memory, including startup/warmup, not a steady-state heap guarantee.

| Configuration | App CPU µs/request | App CPU change % | Total app+capture CPU change % | Extra CPU mcores | Extra anonymous MiB | p95 latency ms | Unique L7 capture yield |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| No capture | 103.5 (97.3–162.2) | 0.0 (0.0–0.0) | 0.0 (0.0–0.0) | 0.0 (0.0–0.0) | 0.0 (0.0–0.0) | 1.291 (1.287–2.263) | — |
| Envoy, no mirror | 86.4 (86.4–86.6) | -16.3 (-46.7–-11.2) | 273.0 (168.1–313.0) | 31.3 (29.8–34.6) | 14.2 (14.2–14.2) | 1.775 (1.754–1.924) | — |
| Envoy + request mirror | 90.0 (84.2–93.9) | -18.6 (-42.1–-7.4) | 750.1 (526.9–862.3) | 83.7 (78.7–91.1) | 27.6 (27.4–28.1) | 1.794 (1.762–2.503) | 100.00 (100.00–100.00)% |

Saturation, 16 persistent workers. A low capture yield means the row is not complete-capture capacity. Loss can make capture appear less expensive by discarding work.

| Configuration | App throughput requests/sec | Throughput change % vs no capture | Capture yield % | Trials meeting every capture oracle |
| --- | ---: | ---: | ---: | ---: |
| No capture | 19324.8 (18951.3–19620.6) | 0.0 (0.0–0.0) | — | — |
| Envoy, no mirror | 12488.6 (10158.3–12709.9) | -35.4 (-46.4–-35.2) | — | — |
| Envoy + request mirror | 6993.3 (5379.4–7965.7) | -64.4 (-71.6–-58.8) | 100.00 (100.00–100.00) | 3/3 |

Incremental mirroring against the existing Envoy path: paired total app+proxy+capture CPU change **133.0 (127.9–133.8)%**; p95 latency change **1.1 (0.5–30.1)%**; saturation throughput change **-45.0 (-47.0–-36.2)%**. These differ from the cost of introducing Envoy and mirroring together.

## cpu

At 100 requests/sec; values are median (min–max). CPU µs/request includes probe work in application context. Extra CPU includes producer, parser, adapter, storage and proxy as applicable. Memory is sampled anonymous memory, including startup/warmup, not a steady-state heap guarantee.

| Configuration | App CPU µs/request | App CPU change % | Total app+capture CPU change % | Extra CPU mcores | Extra anonymous MiB | p95 latency ms | Unique L7 capture yield |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| No capture | 544.1 (541.6–566.5) | 0.0 (0.0–0.0) | 0.0 (0.0–0.0) | 0.0 (0.0–0.0) | 0.0 (0.0–0.0) | 1.806 (1.751–1.813) | — |
| Envoy, no mirror | 500.9 (500.8–509.0) | -7.9 (-11.6–-6.0) | 49.4 (41.3–50.2) | 29.9 (29.8–31.5) | 14.2 (14.2–14.3) | 2.157 (2.151–2.206) | — |
| Envoy + request mirror | 531.4 (518.6–564.0) | -2.3 (-4.2–-0.4) | 176.3 (141.1–187.6) | 96.7 (78.6–101.7) | 27.5 (26.6–27.7) | 3.199 (2.999–4.080) | 100.00 (100.00–100.00)% |

Saturation, 16 persistent workers. A low capture yield means the row is not complete-capture capacity. Loss can make capture appear less expensive by discarding work.

| Configuration | App throughput requests/sec | Throughput change % vs no capture | Capture yield % | Trials meeting every capture oracle |
| --- | ---: | ---: | ---: | ---: |
| No capture | 2094.3 (2089.6–2109.8) | 0.0 (0.0–0.0) | — | — |
| Envoy, no mirror | 2100.1 (1888.6–2100.3) | 0.3 (-10.5–0.5) | — | — |
| Envoy + request mirror | 2113.0 (1996.7–2119.2) | 1.1 (-5.4–1.2) | 100.00 (100.00–100.00) | 3/3 |

Incremental mirroring against the existing Envoy path: paired total app+proxy+capture CPU change **84.9 (70.6–91.5)%**; p95 latency change **48.3 (36.0–89.7)%**; saturation throughput change **0.9 (0.6–5.7)%**. These differ from the cost of introducing Envoy and mirroring together.


Limitations: HTTP/1.1 over OpenSSL TLS 1.3 only; deterministic compressible bodies; no remote object store, full service-mesh deployment or second sidecar. CPU-work profile adds 64 SHA256 body passes. Process startup and warmup are excluded from CPU deltas; capture drain CPU is preserved separately in raw rows. Short shared-host trials do not establish steady-state memory, production tail latency, safe peak capture capacity or overhead for HTTP/2/gRPC and other runtimes.
