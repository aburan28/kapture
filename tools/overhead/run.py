#!/usr/bin/env python3
"""Native application-impact benchmark. All traffic and credentials are synthetic.

Runs the same TLS 1.3 application with independently accounted application,
capture and proxy processes. QEMU timing is deliberately not used. Only Python's
standard library is required; see README.md for tool and host prerequisites.
"""
import argparse
import base64
import csv
import hashlib
import json
import os
import platform
import random
import shutil
import signal
import socket
import statistics
import subprocess
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MODES = ["none", "ebpf-acquire", "ebpf-go", "ebpf-rust", "ebpf-export", "envoy", "envoy-mirror"]
PROFILES = [("small", 128, 0), ("upload", 8192, 0), ("cpu", 8192, 64)]
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def get_json(url):
    with HTTP.open(url, timeout=1) as response:
        return json.load(response)


def wait_ready(check, processes, seconds=35):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        for role in processes:
            if role.process.poll() is not None:
                raise RuntimeError(f"{role.name} exited ({role.process.returncode}); inspect {role.log_path}")
        try:
            if check():
                return
        except (OSError, ValueError):
            pass
        time.sleep(.05)
    raise RuntimeError("fixture or collector readiness timed out")


def listening(port):
    with socket.create_connection(("127.0.0.1", port), timeout=.2):
        return True


def proc_group(session):
    usage, rss = 0, 0
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            fields = (entry / "stat").read_text().rsplit(") ", 1)[1].split()
            if int(fields[3]) == session:
                usage += int(fields[11]) + int(fields[12])
                rss += int(fields[21]) * os.sysconf("SC_PAGE_SIZE")
        except (OSError, ValueError, IndexError):
            pass
    return {"usage_usec": usage * 1e6 / os.sysconf("SC_CLK_TCK"), "anon_bytes": rss, "memory_bytes": rss}


def kv_file(path):
    return {k: int(v) for k, v in (line.split() for line in path.read_text().splitlines())}


class Role:
    def __init__(self, name, command, cpus, directory, cgroup_root):
        self.name = name
        self.log_path = directory / f"{name}.log"
        self.log = self.log_path.open("wb")
        self.group = cgroup_root / (directory.name + "-" + name) if cgroup_root else None
        if self.group:
            self.group.mkdir()

        def prepare():
            os.sched_setaffinity(0, cpus)
            if self.group:
                (self.group / "cgroup.procs").write_text(str(os.getpid()))

        self.process = subprocess.Popen(command, stdout=self.log, stderr=subprocess.STDOUT,
                                        start_new_session=True, preexec_fn=prepare)
        self.peak_anon = 0
        self.peak_memory = 0

    def stats(self):
        if self.group:
            data = kv_file(self.group / "cpu.stat")
            memory = kv_file(self.group / "memory.stat")
            data["anon_bytes"] = memory.get("anon", 0)
            data["memory_bytes"] = int((self.group / "memory.current").read_text())
            data["peak_memory_bytes"] = int((self.group / "memory.peak").read_text())
        else:
            data = proc_group(self.process.pid)
        self.peak_anon = max(self.peak_anon, data["anon_bytes"])
        self.peak_memory = max(self.peak_memory, data.get("peak_memory_bytes", data["memory_bytes"]))
        return data

    def stop(self):
        if self.process.poll() is None:
            # Signal the owning collector first: it closes the producer and
            # Rust pipe and drains queued writes before exiting.
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=12)
            except subprocess.TimeoutExpired:
                os.killpg(self.process.pid, signal.SIGKILL)
                self.process.wait(timeout=5)
        self.log.close()

    def cleanup(self):
        if self.group:
            try:
                self.group.rmdir()
            except OSError:
                # A failed child must not survive into the next trial.
                for pid in (self.group / "cgroup.procs").read_text().split():
                    try:
                        os.kill(int(pid), signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                time.sleep(.05)
                self.group.rmdir()


def envoy_config(cert, key, mirror):
    tls = {"tls_params": {"tls_minimum_protocol_version": "TLSv1_3", "tls_maximum_protocol_version": "TLSv1_3"}}
    downstream = {**tls, "tls_certificates": [{"certificate_chain": {"filename": str(cert)}, "private_key": {"filename": str(key)}}]}
    upstream = {**tls, "validation_context": {"trusted_ca": {"filename": str(cert)}}}
    route = {"cluster": "app", "timeout": "5s"}
    if mirror:
        route["request_mirror_policies"] = [{"cluster": "capture", "runtime_fraction": {"default_value": {"numerator": 100, "denominator": "HUNDRED"}}}]
    def cluster(name, port):
        return {"name": name, "type": "STATIC", "connect_timeout": "1s", "load_assignment": {"cluster_name": name, "endpoints": [{"lb_endpoints": [{"endpoint": {"address": {"socket_address": {"address": "127.0.0.1", "port_value": port}}}}]}]}, "circuit_breakers": {"thresholds": [{"max_connections": 1024, "max_pending_requests": 10000, "max_requests": 10000}]}}
    app = cluster("app", 18443)
    app["transport_socket"] = {"name": "envoy.transport_sockets.tls", "typed_config": {"@type": "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.UpstreamTlsContext", "sni": "localhost", "common_tls_context": upstream}}
    return {
        "admin": {"address": {"socket_address": {"address": "127.0.0.1", "port_value": 19001}}},
        "static_resources": {
            "listeners": [{"name": "https", "address": {"socket_address": {"address": "127.0.0.1", "port_value": 18444}}, "filter_chains": [{"transport_socket": {"name": "envoy.transport_sockets.tls", "typed_config": {"@type": "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.DownstreamTlsContext", "common_tls_context": downstream}}, "filters": [{"name": "envoy.filters.network.http_connection_manager", "typed_config": {"@type": "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager", "stat_prefix": "bench", "codec_type": "HTTP1", "route_config": {"name": "routes", "virtual_hosts": [{"name": "bench", "domains": ["*"], "routes": [{"match": {"prefix": "/"}, "route": route}]}]}, "http_filters": [{"name": "envoy.filters.http.router", "typed_config": {"@type": "type.googleapis.com/envoy.extensions.filters.http.router.v3.Router"}}]}}]}]}],
            "clusters": [app] + ([cluster("capture", 18080)] if mirror else [])}}


def verify_evidence(artifacts, size, expected):
    """Outside the measured interval, verify durable request/response pairs."""
    seen = set()
    invalid, duplicates = 0, 0
    digest = hashlib.sha256(b"x" * size).hexdigest()
    for path in artifacts.glob("evidence/benchmark/capture/*/*.json"):
        if path.parent.name == "sessions":
            continue
        exchange = json.loads(path.read_text())
        request, response = exchange["request"], exchange["response"]
        headers = {k.lower(): v[0] for k, v in request.get("headers", {}).items() if v}
        returned = {k.lower(): v[0] for k, v in response.get("headers", {}).items() if v}
        if headers.get("x-bench-phase") == "warmup":
            continue
        try:
            ident = int(headers.get("x-bench-id", "0"))
            valid = (ident > 0 and headers.get("x-bench-phase") == "measure"
                     and request.get("method") == "POST" and request.get("path") == "/bench"
                     and request.get("complete") and response.get("complete")
                     and base64.b64decode(request.get("body", ""), validate=True) == b"x" * size
                     and response.get("status") == 204
                     and returned.get("x-bench-id") == str(ident)
                     and returned.get("x-bench-sha256") == digest
                     and headers.get("authorization") == "[REDACTED]"
                     and exchange["quality"]["framingComplete"]
                     and not exchange["quality"].get("reasons"))
        except (ValueError, KeyError):
            valid = False
        if not valid:
            invalid += 1
        elif ident in seen:
            duplicates += 1
        else:
            seen.add(ident)
    total = sum(seen)
    return {"exchanges": len(seen), "invalid": invalid, "duplicates": duplicates,
            "id_sum": total, "complete": len(seen) == expected and total == expected * (expected + 1) // 2 and invalid == 0 and duplicates == 0}


def trial(args, mode, profile, load_kind, repetition, ordinal, cert, key, cpus, cgroups):
    name, body, work = profile
    directory = args.output / f"{ordinal:03d}-{name}-{load_kind}-{mode}-r{repetition}"
    directory.mkdir()
    artifacts = directory / "artifacts"
    artifacts.mkdir()
    roles = []
    result = {"ordinal": ordinal, "mode": mode, "profile": name, "body_bytes": body, "hash_rounds": work, "load": load_kind, "trial": repetition}
    try:
        app = Role("application", [args.app, "18443", str(cert), str(key), str(work)], cpus["application"], directory, cgroups)
        roles.append(app)
        wait_ready(lambda: listening(18443), roles)
        collector = None
        if mode.startswith("ebpf-") or mode == "envoy-mirror":
            collector_mode = "mirror" if mode == "envoy-mirror" else {"ebpf-acquire": "acquire", "ebpf-go": "go", "ebpf-rust": "rust", "ebpf-export": "export"}[mode]
            collector = Role("collector", [args.helper, "collect", "-mode", collector_mode, "-pid", str(app.process.pid), "-ecapture", args.ecapture, "-rust", args.rust, "-body", str(body), "-output", str(artifacts), "-result", str(directory / "oracle.json")], cpus["capture"], directory, cgroups)
            roles.append(collector)
            wait_ready(lambda: get_json("http://127.0.0.1:18101/status")["ready"], roles)
            if mode == "envoy-mirror":
                wait_ready(lambda: listening(18080), roles)
        if mode.startswith("envoy"):
            config = directory / "envoy.json"
            config.write_text(json.dumps(envoy_config(cert, key, mode == "envoy-mirror")))
            proxy = Role("proxy", [args.envoy, "-c", str(config), "--concurrency", "1", "--log-level", "error", "--disable-hot-restart"], cpus["capture"], directory, cgroups)
            roles.append(proxy)
            wait_ready(lambda: listening(18444), roles)
        target = "https://localhost:" + ("18444" if mode.startswith("envoy") else "18443") + "/bench"
        common = [args.helper, "load", "-url", target, "-cert", str(cert), "-body", str(body), "-concurrency", str(args.concurrency)]
        warm = Role("warmup", common + ["-duration", f"{args.warmup}s", "-rate", "100", "-phase", "warmup"], cpus["load"], directory, cgroups)
        warm.process.wait(timeout=15)
        warm.stop()
        warm_result = json.loads(warm.log_path.read_text())
        warm.cleanup()
        if warm_result["errors"] or warm_result["missed"]:
            raise RuntimeError(f"warmup failed: {warm_result}")
        before = {r.name: r.stats() for r in roles}
        begun = time.monotonic()
        load = Role("load", common + ["-duration", f"{args.seconds}s", "-rate", str(args.rate if load_kind == "fixed" else 0)], cpus["load"], directory, cgroups)
        roles.append(load)
        while load.process.poll() is None:
            for r in roles:
                r.stats()
                if r is not load and r.process.poll() is not None:
                    raise RuntimeError(f"{r.name} failed during the measured load; see {r.log_path}")
            time.sleep(.1)
        interval = time.monotonic() - begun
        after = {r.name: r.stats() for r in roles}
        load.stop()
        if load.process.returncode:
            raise RuntimeError("load process failed; inspect " + str(load.log_path))
        load_result = json.loads(load.log_path.read_text())
        result.update(load_result)
        result["resource_interval_seconds"] = interval
        result["resources"] = {}
        for role in roles:
            end_stats = after[role.name]
            delta = {key: end_stats.get(key, 0) - before.get(role.name, {}).get(key, 0) for key in ["usage_usec", "user_usec", "system_usec", "nr_throttled", "throttled_usec"]}
            delta.update({"peak_anon_bytes": role.peak_anon, "peak_memory_bytes": role.peak_memory})
            result["resources"][role.name] = delta
        expected = result["success"]
        if collector:
            deadline = time.monotonic() + args.drain
            while time.monotonic() < deadline:
                status = get_json("http://127.0.0.1:18101/status")
                if mode == "ebpf-acquire" or status["exchanges"] >= expected:
                    break
                time.sleep(.1)
            if mode == "ebpf-export":
                with HTTP.open("http://127.0.0.1:18102/metrics", timeout=1) as response:
                    (directory / "production-metrics.txt").write_bytes(response.read())
            if mode.startswith("envoy"):
                result["envoy_stats"] = get_json("http://127.0.0.1:19001/stats?format=json")
            result["drain_seconds"] = max(0, time.monotonic() - begun - interval)
            result["collector_drain_cpu_usec"] = max(0, collector.stats()["usage_usec"] - after["collector"]["usage_usec"])
            collector.stop()
            if collector.process.returncode != 0:
                raise RuntimeError("collector did not shut down cleanly; see " + str(collector.log_path))
            oracle = json.loads((directory / "oracle.json").read_text())
            result["capture"] = oracle
            result["capture_yield"] = oracle["exchanges"] / expected if expected and mode != "ebpf-acquire" else None
            result["capture_complete"] = mode != "ebpf-acquire" and oracle["exchanges"] == expected and oracle["id_sum"] == expected * (expected + 1) // 2 and oracle["duplicates"] == 0 and oracle["invalid"] == 0 and oracle["loss"] == 0 and oracle["unattributed"] == 0
            sessions = list(artifacts.glob("evidence/benchmark/capture/sessions/*.json"))
            if sessions:
                result["production_session"] = json.loads(sessions[-1].read_text())
                session = result["production_session"]
                result["capture_complete"] &= not session["failed"] and session["decodeErrors"] == 0 and session["lossIntervals"] == 0 and session["unattributedEvents"] == 0
            if mode == "ebpf-export":
                result["evidence_validation"] = verify_evidence(artifacts, body, expected)
                result["capture_complete"] &= result["evidence_validation"]["complete"]
                result["capture_yield"] = min(oracle["exchanges"], result["evidence_validation"]["exchanges"]) / expected if expected else 0
            result["artifact_bytes"] = sum(p.stat().st_size for p in artifacts.rglob("*") if p.is_file())
        result["application_ok"] = result["errors"] == 0 and result["missed"] == 0 and expected > 0
        return result
    finally:
        for role in reversed(roles):
            role.stop()
        for role in roles:
            role.cleanup()
        if not args.keep_captures:
            shutil.rmtree(artifacts)


def summarize(rows, output):
    flat = []
    for row in rows:
        item = {k: v for k, v in row.items() if not isinstance(v, (dict, list))}
        for role, resource in row.get("resources", {}).items():
            for key, value in resource.items():
                item[f"{role}_{key}"] = value
        flat.append(item)
    keys = sorted(set().union(*(r.keys() for r in flat)))
    with (output / "trials.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, keys); writer.writeheader(); writer.writerows(flat)
    lines = ["# Measured application impact", "", "Initial observations on one shared native Linux runner; medians and ranges are descriptive, not a production capacity guarantee.", "", "| Profile | Load | Mode | RPS median (min–max) | p95 ms median | App CPU µs/request | Extra CPU cores | Capture yield |", "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |"]
    for profile, _, _ in PROFILES:
        for kind in ["fixed", "saturation"]:
            for mode in MODES:
                group = [r for r in rows if r["profile"] == profile and r["load"] == kind and r["mode"] == mode]
                if not group:
                    continue
                rates = [r["rps"] for r in group]
                cpu = [r["resources"]["application"]["usage_usec"] / r["success"] for r in group if r["success"]]
                extra = [sum(v["usage_usec"] for k, v in r["resources"].items() if k in ["collector", "proxy"]) / 1e6 / r["resource_interval_seconds"] for r in group]
                yields = [r["capture_yield"] for r in group if r.get("capture_yield") is not None]
                coverage = f"{100 * statistics.median(yields):.2f}%" if yields else "—"
                lines.append(f"| {profile} | {kind} | {mode} | {statistics.median(rates):.1f} ({min(rates):.1f}–{max(rates):.1f}) | {statistics.median(r['p95_ms'] for r in group):.3f} | {statistics.median(cpu):.1f} | {statistics.median(extra):.3f} | {coverage} |")
    (output / "summary.md").write_text("\n".join(lines) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    for name in ["app", "helper", "ecapture", "rust", "envoy"]:
        parser.add_argument("--" + name, default="/" + name)
    parser.add_argument("--modes", default=",".join(MODES))
    parser.add_argument("--profiles", default="small,upload,cpu")
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--seconds", type=float, default=6)
    parser.add_argument("--warmup", type=float, default=1)
    parser.add_argument("--drain", type=float, default=3)
    parser.add_argument("--rate", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--require-cgroup", action="store_true")
    parser.add_argument("--keep-captures", action="store_true")
    args = parser.parse_args()
    modes = args.modes.split(",")
    if any(m not in MODES for m in modes) or not 1 <= args.trials <= 20 or not .5 <= args.seconds <= 60:
        parser.error("invalid benchmark options")
    if platform.system() != "Linux" or platform.machine() != "x86_64":
        parser.error("this native OpenSSL benchmark requires Linux x86-64")
    if any(m.startswith("ebpf-") for m in modes) and (os.geteuid() != 0 or not Path("/sys/kernel/btf/vmlinux").is_file()):
        parser.error("native eBPF modes require root and kernel BTF; do not substitute emulator timings")
    available = sorted(os.sched_getaffinity(0))
    if len(available) < 2:
        parser.error("at least two available CPUs required")
    cpus = {"application": [available[0]], "load": [available[1]], "capture": available[2:] or [available[0]]}
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    cert, key = args.output / "fixture.crt", args.output / "fixture.key"
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1", "-keyout", str(key), "-out", str(cert)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
    cgroups = Path("/sys/fs/cgroup") / ("kapture-overhead-" + str(os.getpid()))
    try:
        cgroups.mkdir()
        (cgroups / "cgroup.subtree_control").write_text("+cpu +memory")
    except OSError:
        try:
            cgroups.rmdir()
        except OSError:
            pass
        cgroups = None
    if args.require_cgroup and cgroups is None:
        parser.error("writable cgroup v2 CPU/memory accounting required")
    model = next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown")
    metadata = {"date_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "kernel": platform.release(), "cpu_model": model, "available_cpus": available, "affinity": cpus, "cpu_isolation": "distinct roles" if len(available) >= 3 else "capture/proxy share application CPU", "accounting": "cgroup-v2" if cgroups else "proc-stat (10ms tick; RSS sum)", "parameters": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()}, "source_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(), "flowscope_revision": "97d9d8e10edd4a5ab2a31cdff7cc674993e354e2", "ecapture_revision": "afcc9345ff8f09f96811a158cf768d284b4c7e1b", "binaries": {}}
    for name in ["app", "helper", "ecapture", "rust", "envoy"]:
        binary = Path(getattr(args, name))
        if binary.is_file():
            metadata["binaries"][name] = hashlib.sha256(binary.read_bytes()).hexdigest()
    metadata["host_limits"] = {}
    for name in ["cpu.max", "memory.max"]:
        limit = Path("/sys/fs/cgroup") / name
        if limit.is_file():
            metadata["host_limits"][name] = limit.read_text().strip()
    metadata["versions"] = {}
    for name, command in {"openssl": ["openssl", "version"], "envoy": [args.envoy, "--version"]}.items():
        try:
            metadata["versions"][name] = subprocess.check_output(command, text=True, stderr=subprocess.STDOUT).strip()
        except (OSError, subprocess.CalledProcessError):
            pass
    (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    rows = []
    rng = random.Random(20261019)
    try:
        for repetition in range(1, args.trials + 1):
            cases = [(mode, profile, kind) for profile in PROFILES if profile[0] in args.profiles.split(",") for kind in ["fixed", "saturation"] for mode in modes]
            rng.shuffle(cases)
            for mode, profile, kind in cases:
                row = trial(args, mode, profile, kind, repetition, len(rows) + 1, cert, key, cpus, cgroups)
                rows.append(row)
                with (args.output / "trials.jsonl").open("a") as f:
                    f.write(json.dumps(row) + "\n")
                summarize(rows, args.output)
                print(json.dumps({k: row.get(k) for k in ["ordinal", "mode", "profile", "load", "trial", "rps", "p95_ms", "application_ok", "capture_yield", "capture_complete"]}), flush=True)
    finally:
        key.unlink(missing_ok=True)
        if cgroups:
            cgroups.rmdir()
    # Saturation may legitimately overrun capture; preserve those rows and
    # show yield. Fixed-load capture failures must fail the experiment.
    if any(not r["application_ok"] or (r["load"] == "fixed" and r["mode"] not in ["none", "envoy", "ebpf-acquire"] and not r.get("capture_complete")) for r in rows):
        raise SystemExit("application or fixed-load capture oracle failed; raw results retained")


if __name__ == "__main__":
    main()
