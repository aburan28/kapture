#!/usr/bin/env python3
"""Summarize native overhead trials, preserving paired baselines and fidelity.

Usage: python3 tools/analyze-overhead.py /path/to/raw --output /path/to/report
Add --plot to export a standalone SVG with matplotlib (optional dependency).
"""
import argparse
import gzip
import json
import statistics as stats
from pathlib import Path

MODES = ["none", "ebpf-acquire", "ebpf-go", "ebpf-rust", "ebpf-export", "envoy", "envoy-mirror"]
LABELS = {"none": "No capture", "ebpf-acquire": "eBPF acquisition", "ebpf-go": "eBPF + Go L7", "ebpf-rust": "eBPF + Rust L7 + IPC", "ebpf-export": "eBPF + Go L7 + files", "envoy": "Envoy, no mirror", "envoy-mirror": "Envoy + request mirror"}
PROFILES = ["small", "upload", "cpu"]


def metric(row, name):
    resources = row["resources"]
    app = resources["application"]["usage_usec"]
    extra = sum(v["usage_usec"] for k, v in resources.items() if k in ["collector", "proxy"])
    if name == "app_cpu_us":
        return app / row["success"]
    if name == "extra_cpu_us":
        return extra / row["success"]
    if name == "total_cpu_us":
        return (app + extra) / row["success"]
    if name == "extra_mcores":
        return extra / 1000 / row["resource_interval_seconds"]
    if name == "extra_anon_mib":
        return sum(v["peak_anon_bytes"] for k, v in resources.items() if k in ["collector", "proxy"]) / 2**20
    return row[name]


def describe(values):
    return {"median": stats.median(values), "min": min(values), "max": max(values)}


def fmt(value, digits=1):
    return f"{value['median']:.{digits}f} ({value['min']:.{digits}f}–{value['max']:.{digits}f})"


def analyze(rows, metadata):
    if metadata["accounting"] not in ["cgroup-v2", "process-cpu-clock; summed RssAnon/RSS"]:
        raise ValueError("publishable CPU comparisons require native high-resolution CPU accounting")
    index = {(r["profile"], r["load"], r["mode"], r["trial"]): r for r in rows}
    if len(index) != len(rows):
        raise ValueError("duplicate experiment cells")
    trials = metadata["parameters"]["trials"]
    modes = metadata["parameters"]["modes"].split(",")
    if "none" not in modes or any(m not in MODES for m in modes):
        raise ValueError("invalid configured modes or missing no-capture baseline")
    expected = len(PROFILES) * len(modes) * 2 * trials
    if len(index) != expected or trials < 3:
        raise ValueError(f"incomplete experiment: {len(index)}/{expected} rows or fewer than three repeats")
    if any(not r["application_ok"] for r in rows):
        raise ValueError("application errors or missed slots; inspect raw rows before reporting overhead")
    output = []
    for profile in PROFILES:
        for load in ["fixed", "saturation"]:
            for mode in modes:
                group = [index[profile, load, mode, n] for n in range(1, trials + 1)]
                result = {"profile": profile, "load": load, "mode": mode}
                for name in ["rps", "p50_ms", "p95_ms", "p99_ms", "service_p95_ms", "app_cpu_us", "extra_cpu_us", "total_cpu_us", "extra_mcores", "extra_anon_mib"]:
                    result[name] = describe([metric(r, name) for r in group])
                for baseline in (["none", "envoy"] if mode == "envoy-mirror" else ["none"]):
                    comparisons = {}
                    for name in ["rps", "p95_ms", "app_cpu_us", "total_cpu_us"]:
                        changes = []
                        for row in group:
                            base = index[profile, load, baseline, row["trial"]]
                            denominator = metric(base, name)
                            if denominator <= 0:
                                raise ValueError("invalid zero baseline")
                            changes.append(100 * (metric(row, name) / denominator - 1))
                        comparisons[name + "_change_pct"] = describe(changes)
                        comparisons[name + "_change_absolute"] = describe([metric(row, name) - metric(index[profile, load, baseline, row["trial"]], name) for row in group])
                    result["relative_to_" + baseline] = comparisons
                yields = [r["capture_yield"] for r in group if r.get("capture_yield") is not None]
                result["capture_yield_pct"] = describe([100*y for y in yields]) if yields else None
                result["complete_trials"] = sum(bool(r.get("capture_complete")) for r in group)
                result["trials"] = trials
                output.append(result)
    return output


def markdown(results, metadata):
    index = {(r["profile"], r["load"], r["mode"]): r for r in results}
    modes = [m for m in MODES if any(r["mode"] == m for r in results)]
    lines = ["# Application overhead measurements", "", f"Source: `{metadata['source_sha']}`; Linux `{metadata['kernel']}`; {metadata['cpu_model']}. CPU placement: {metadata['affinity']}. Accounting: {metadata['accounting']}. Three short repetitions on one shared native Linux host; ranges below are observed minima and maxima, not confidence intervals.", "", "CPU deltas compare each mode with the no-capture trial from the same repetition and workload. Latency uses a fixed offered rate; saturation throughput is a separate closed-loop experiment. Acquisition alone has no L7 completeness oracle. The Rust row includes the common eBPF feed and pipe adapter. The mirror row captures requests; eBPF L7 also reconstructs responses. Local file export includes JSON, synthetic PCAPNG and replay projections.", ""]
    lines += ["Measured configurations: " + ", ".join(modes) + ".", ""]
    if not any(mode.startswith("ebpf-") for mode in modes):
        lines += ["**No eBPF overhead was measured on this host.** Native BPF comparisons remain pending on a BPF-capable runner. These results compare only no capture, one Envoy proxy and Envoy request mirroring; they must not be combined with timing from another host as a common-host comparison.", ""]
    for profile in PROFILES:
        lines += [f"## {profile}", "", "At 100 requests/sec; values are median (min–max). CPU µs/request includes probe work in application context. Extra CPU includes producer, parser, adapter, storage and proxy as applicable. Memory is sampled anonymous memory, including startup/warmup, not a steady-state heap guarantee.", "", "| Configuration | App CPU µs/request | App CPU change % | Total app+capture CPU change % | Extra CPU mcores | Extra anonymous MiB | p95 latency ms | Unique L7 capture yield |", "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
        for mode in modes:
            r = index[profile, "fixed", mode]
            baseline = r["relative_to_none"]
            coverage = fmt(r["capture_yield_pct"], 2) + "%" if r["capture_yield_pct"] else "not checked" if mode == "ebpf-acquire" else "—"
            lines.append(f"| {LABELS[mode]} | {fmt(r['app_cpu_us'])} | {fmt(baseline['app_cpu_us_change_pct'])} | {fmt(baseline['total_cpu_us_change_pct'])} | {fmt(r['extra_mcores'])} | {fmt(r['extra_anon_mib'])} | {fmt(r['p95_ms'],3)} | {coverage} |")
        lines += ["", "Saturation, 16 persistent workers. A low capture yield means the row is not complete-capture capacity. Loss can make capture appear less expensive by discarding work.", "", "| Configuration | App throughput requests/sec | Throughput change % vs no capture | Capture yield % | Trials meeting every capture oracle |", "| --- | ---: | ---: | ---: | ---: |"]
        for mode in modes:
            r = index[profile, "saturation", mode]
            coverage = fmt(r["capture_yield_pct"], 2) if r["capture_yield_pct"] else "not checked" if mode == "ebpf-acquire" else "—"
            complete = f"{r['complete_trials']}/{r['trials']}" if r["capture_yield_pct"] else "—"
            lines.append(f"| {LABELS[mode]} | {fmt(r['rps'])} | {fmt(r['relative_to_none']['rps_change_pct'])} | {coverage} | {complete} |")
        mirror = index[profile, "fixed", "envoy-mirror"]["relative_to_envoy"]
        saturated = index[profile, "saturation", "envoy-mirror"]["relative_to_envoy"]
        lines += ["", f"Incremental mirroring against the existing Envoy path: paired total app+proxy+capture CPU change **{fmt(mirror['total_cpu_us_change_pct'])}%**; p95 latency change **{fmt(mirror['p95_ms_change_pct'])}%**; saturation throughput change **{fmt(saturated['rps_change_pct'])}%**. These differ from the cost of introducing Envoy and mirroring together.", ""]
    failed = [r for r in results if r["load"] == "fixed" and r["mode"] not in ["none", "envoy", "ebpf-acquire"] and r["complete_trials"] != r["trials"]]
    if failed:
        lines += ["## Fixed-load fidelity failures", "", "Some fixed-load rows failed an oracle. Their numbers are diagnostic measurements and cannot support a complete-capture overhead claim:", ""]
        lines += [f"- {r['profile']}: {r['mode']} ({r['complete_trials']}/{r['trials']} complete)." for r in failed]
    lines += ["", "Limitations: HTTP/1.1 over OpenSSL TLS 1.3 only; deterministic compressible bodies; no remote object store, full service-mesh deployment or second sidecar. CPU-work profile adds 64 SHA256 body passes. Process startup and warmup are excluded from CPU deltas; capture drain CPU is preserved separately in raw rows. Short shared-host trials do not establish steady-state memory, production tail latency, safe peak capture capacity or overhead for HTTP/2/gRPC and other runtimes."]
    return "\n".join(lines) + "\n"


def plot(results, metadata, destination):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    index = {(r["profile"], r["load"], r["mode"]): r for r in results}
    fig, axes = plt.subplots(1, 3, figsize=(15, 5.8), sharey=True)
    modes = [m for m in MODES if any(r["mode"] == m for r in results)]
    colors = ["#325a83", "#d78b31"]
    for axis, profile in zip(axes, PROFILES):
        rows = [index[profile, "fixed", m] for m in modes]
        app = [r["app_cpu_us"]["median"] for r in rows]
        extra = [r["extra_cpu_us"]["median"] for r in rows]
        y = list(range(len(modes)))
        axis.barh(y, app, color=colors[0], label="Application (includes probe CPU)")
        axis.barh(y, extra, left=app, color=colors[1], label="Collector + proxy")
        for n, row in enumerate(rows):
            total = row["total_cpu_us"]
            axis.plot([total["min"], total["max"]], [n, n], color="#172432", linewidth=1.4)
        axis.set_title({"small": "128-byte POST", "upload": "8-KiB POST", "cpu": "8-KiB POST + CPU work"}[profile])
        axis.set_xlabel("CPU µs per completed application request")
        axis.grid(axis="x", alpha=.2)
        axis.set_axisbelow(True)
        axis.spines[["top", "right"]].set_visible(False)
    axes[0].set_yticks(range(len(modes)), [LABELS[m] for m in modes])
    axes[0].invert_yaxis()
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=2, frameon=False, bbox_to_anchor=(.55, .04))
    fig.suptitle("Application and capture CPU at 100 requests/sec", fontsize=17, x=.55)
    fig.text(.55, .005, "3 trials; bars: component medians; lines: total CPU observed range. One native Linux host. See capture-yield tables.", ha="center", fontsize=9)
    fig.tight_layout(rect=(0, .12, 1, .93))
    fig.savefig(destination, bbox_inches="tight")
    destination.write_text("\n".join(line.rstrip() for line in destination.read_text().splitlines()) + "\n")
    fig.savefig(destination.with_suffix(".png"), bbox_inches="tight", dpi=160)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("raw", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--plot", action="store_true")
    args = parser.parse_args()
    metadata = json.loads((args.raw / "metadata.json").read_text())
    source = args.raw / "trials.jsonl"
    data = source.read_text() if source.exists() else gzip.decompress((args.raw / "trials.jsonl.gz").read_bytes()).decode()
    rows = [json.loads(line) for line in data.splitlines()]
    results = analyze(rows, metadata)
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "analysis.json").write_text(json.dumps(results, indent=2) + "\n")
    (args.output / "report.md").write_text(markdown(results, metadata))
    if args.plot:
        plot(results, metadata, args.output / "cpu-overhead.svg")
    print(f"Analyzed {len(rows)} native trials into {args.output}")


if __name__ == "__main__":
    main()
