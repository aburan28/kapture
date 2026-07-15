# Replay Engine ABI (v1)

The replay engine ABI is the contract between the **Kapture replay host**
(the `replay-engine` worker binary that owns storage access, sharding, and
pacing) and a **replay engine** (the process that actually generates load:
the builtin sender, k6, ghz, or any third-party generator).

- Wire protocol: `proto/replayengine/v1/replayengine.proto`
- Engine SDK (Go): `pkg/replayengine` — implement `Engine`, call `Serve`
- Host runtime: `internal/replayengine` — subprocess launcher, hot-reload
  manager, streaming feeder
- Reference implementation: `internal/engines/builtin` (ties break here
  when this document is ambiguous)

## Design boundary

The split follows the load-test tool boundary from
`docs/replay-storage-and-load-testing.md`:

| Host owns | Engine owns |
|-----------|-------------|
| Storage access & credentials | Turning FeedItems into real traffic |
| Streaming reads (never full downloads) | Connection/VU/worker model |
| Shard filtering (FNV-1a, `replay.ShardOwns`) | Protocol specifics (HTTP, gRPC) |
| Pacing (unless engine is self-paced) | Result measurement |
| Result aggregation & the RunReport | Per-tool configuration |

Engines never receive storage credentials, bucket layouts, or shard math.
Replacing the engine never requires a new dataset format or storage loader.

## Process model

Engines run as **gRPC subprocesses**, discovered from a plugin directory:

1. **Discovery.** An engine named `k6` is the executable
   `kapture-engine-k6` in the plugin directory (`--plugin-dir`, env
   `REPLAY_PLUGIN_DIR`, Helm `replayEngine.pluginDir`).
2. **Launch.** The host starts the binary with two env vars:
   `KAPTURE_ENGINE_MAGIC=kapture-replay-engine` (refuse to serve without
   it — a user running the plugin directly gets a clear error instead of a
   hang) and `KAPTURE_ENGINE_SOCKET_DIR` (where to create the socket).
3. **Handshake.** The engine listens on a unix socket and prints exactly
   one line to stdout, then never writes to stdout again (logs go to
   stderr):

   ```
   KAPTURE-ENGINE|1|unix|/path/to/engine.sock|grpc
   ```

   Fields: prefix, handshake-format version, network, address, protocol.
   The host times out after 15s without this line.
4. **Version negotiation.** The host calls `Describe` with its supported
   ABI versions (`["v1"]`); the engine answers with its own list. The
   first common entry (host preference order) wins; no common entry kills
   the launch. New ABI generations are additive: an engine may support
   `["v1","v2"]` during a migration.
5. **Identity check.** `DescribeResponse.name` must equal the plugin
   binary suffix; mismatches are rejected.

## Run lifecycle

```
Describe ──► Configure ──► Execute (bidi stream) ──► exit / next run
                              ▲
                            Drain (concurrent, idempotent)
```

- `Configure` is called exactly once per run, before `Execute`. Engines
  must validate everything here and fail fast; `ConfigureResponse.accepted
  = false` carries a human-readable reason into the TrafficReplay status.
- `Execute` is the run: the host streams `FeedItem`s (client → server) and
  the engine streams `ExecuteEvent`s back (server → client). The host
  half-closes the feed when the capture slice is exhausted; the engine
  finishes in-flight work, emits **exactly one final `RunSummary`**, and
  closes its side. A stream that closes without a summary fails the run.
- `Drain` may arrive at any time (hot reload, abort): stop pulling new
  feed items, finish what is in flight. Must be idempotent.

## Streaming and backpressure

Captures are **streamed, never downloaded**: storage readers yield one
request at a time (S3/GCS objects are stream-decompressed), the feeder
sends them into the stream one at a time, and `Send` blocks on gRPC flow
control when the engine falls behind — which stops the reader. Nothing is
buffered beyond the transport windows and the SDK's small feed channel
(256 items). An engine that wants bounded memory gets it for free by
reading the feed at its send rate; an engine must **never** try to slurp
the whole feed before starting (ghz is the sanctioned exception — see its
package doc — and it buffers per-method *aggregates*, not requests).

## Pacing contract

`RunConfig.rate` carries the rate hint (`Constant` with per-shard RPS,
`OriginalTiming` with a time scale, or `Unlimited`).

- Default: the **host paces the feed**. Items arrive when they are due;
  the engine sends them promptly and does no pacing of its own (this is
  how the k6 adapter reproduces recorded QPS: VUs block on the feed).
- Engines advertising the `self-paced` capability receive the feed as
  fast as flow control allows and must implement the rate hint themselves
  (`FeedItem.source_offset_ns` rebuilds the recorded timeline). Engines
  that cannot honour a mode must reject it in `Configure` — the ghz
  adapter rejects `OriginalTiming` rather than silently distorting it.

## Sharding contract

The host filters the feed to the worker's shard before anything reaches
the engine (`replay.ShardOwns`, FNV-1a over the request ID — the property
model-checked in `verification/tla/Sharding.tla`). `RunConfig.shard_index
/ shard_count` are informational. Engines must not re-shard, sample, or
deduplicate the feed; doing so breaks the fleet-wide disjoint-and-
exhaustive guarantee.

## Results contract

Engines advertise one (or both) result capabilities:

- `per-request-results`: one `RequestResult` per FeedItem (builtin).
- `aggregate-results`: periodic `Progress` (cumulative counters) plus the
  final `RunSummary` (k6, ghz).

The final `RunSummary` is authoritative for the TrafficReplay status and
the hub's load-test aggregation: counts, achieved RPS, and latency
percentiles (milliseconds). Engines that cannot measure a field leave it
zero rather than guessing.

## Hot reload

The host watches the plugin directory (fsnotify, 500ms debounce). When a
plugin binary changes:

1. The new binary is launched and handshaked. If it fails, the old
   process **stays** — a bad rollout never takes down a working engine.
2. New runs are routed to the new process.
3. The old process gets `Drain`, a grace period (default 30s), then
   termination. Runs already executing on it finish there; there is no
   in-run handover.

Because plugins are separate processes, a reload never restarts the
replay worker, and an engine crash can never corrupt host state — the
run fails with the stream error and the Job retries.

## Selecting engines

```yaml
apiVersion: capture.gateway.io/v1alpha1
kind: CaptureLoadTest        # same field on TrafficReplay
spec:
  engine:
    name: k6                 # builtin (default) | k6 | ghz | <plugin>
    config:                  # engine-specific, passed through verbatim
      vus: 200
      args: ["--quiet"]
```

The hub forwards `engine` inside replay directives; spokes stamp it on the
shard TrafficReplays; the Job runner turns it into
`--engine k6 --engine-config '{...}'`.

## Out-of-the-box engines

| Engine | Protocols | Pacing | Results | Notes |
|--------|-----------|--------|---------|-------|
| `builtin` | HTTP | host-paced (all modes) | per-request | byte-exact replay; the reference implementation |
| `k6` | HTTP | host-paced via feed | aggregate (summary export) | VUs pull from the adapter's local feed endpoint (`/next`, `/batch`); requires the `k6` binary |
| `ghz` | gRPC | self-paced (`Constant`/`Unlimited` only) | aggregate (JSON report) | replays the captured **method mix** with representative payloads, not byte-exact sequences; uses server reflection or a `protoset` |

Build them with `make build-engines` and place the binaries in the plugin
directory (`replayEngine.pluginDir`).

## Conformance checklist for engine authors

- [ ] Binary named `kapture-engine-<name>`; `Describe.name` matches.
- [ ] Refuses direct execution without `KAPTURE_ENGINE_MAGIC` (automatic
      with `pkg/replayengine.Serve`).
- [ ] One handshake line on stdout; all logging on stderr.
- [ ] Advertises accurate `abi_versions`, `protocols`, `capabilities`.
- [ ] Rejects unsupported rate modes in `Configure` instead of distorting
      them.
- [ ] Consumes the feed incrementally; bounded memory regardless of
      capture size.
- [ ] Never re-shards, samples, or deduplicates the feed.
- [ ] Emits exactly one final `RunSummary`, even on failure paths that
      still hold the stream.
- [ ] `Drain` is idempotent and stops new work promptly.

The subprocess tests in `internal/replayengine/subprocess_test.go` show a
minimal conforming engine (`fakeEngine`) exercised through the real ABI.
