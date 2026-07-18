# TLA+ Specifications for Kapture

Machine-checked models of the two correctness-critical pieces of Kapture's
distributed load testing: the data-plane sharding function and the hub-spoke
coordination protocol. Both are checked with TLC (the TLA+ model checker)
on every push via `.github/workflows/tla.yaml`, and locally with
`make verify-tla`.

These are model checks over small but adversarial configurations (bounded
spokes, buffer capacity 1, hub restarts), not TLAPS deductive proofs. Every
property below is exhaustively verified for all interleavings of the
modelled configuration.

## Specs

### `Sharding.tla` — shard filter correctness

Models `shardOf` and the read-loop shard filter in
`internal/plugin/replay/engine.go`, plus Job retries
(`internal/spoke/replay_runner.go`, `backoffLimit`). The hash function is
an arbitrary total function chosen nondeterministically, so TLC checks
every possible assignment — subsuming FNV-1a mod N, whose only relied-upon
property is that it is a deterministic total function.

| Property | Kind | Meaning |
|----------|------|---------|
| `OnlyOwnerSends` | safety | a request is only ever sent by its owning shard, even across Job retries |
| `AtMostOneShardSends` | safety | shard slices are pairwise **disjoint** |
| `CrashFreeExactlyOnce` | safety | with no Job retries, fleet-wide delivery is exactly-once |
| `Termination` | liveness | every worker finishes |
| `Coverage` | liveness | shard slices are **exhaustive**: every request is sent at least once |

`AtMostOneShardSends` + `Coverage` together are the "disjoint and
exhaustive" claim in `docs/multi-cell-load-testing.md`. Job retries make
within-shard delivery at-least-once, which is why exactly-once is stated
conditionally on a crash-free run — the model is honest about that.

### `KaptureLoadTest.tla` — coordination protocol

Models one `CaptureLoadTest` run across
`internal/hub/loadtest_controller.go`, the bounded per-spoke directive
buffers in `internal/hub/grpc_server.go`, and the spoke-side directive
handling in `internal/spoke/replay_directives.go`. Modelled failures:
full directive buffers (`ResourceExhausted`), duplicate START resends,
shard Job failures, and a hub restart that wipes the in-memory replay
registry and the coordinator's STOP-delivery bookkeeping.

| Property | Kind | Meaning |
|----------|------|---------|
| `StartOnlyToOwner` | safety | START directives only reach the assigned spoke |
| `NoStartAfterStop` | safety | per-spoke FIFO ⇒ a STOP is never followed by a START, so stopped runs cannot resurrect shards |
| `CompletionSound` | safety | `Completed` is only entered when every shard reported success |
| `AbortSound` | safety | `Aborted` is only entered after STOP reached every assigned spoke |
| `FinalizeSound` | safety | the finalizer is only removed after STOP reached every assigned spoke |
| `Termination` | liveness | every run reaches a terminal phase, despite full buffers and a hub restart |
| `CleanupAfterDelete` | liveness | deletion eventually stops every shard — no orphaned load |
| `AbortCleansUp` | liveness | abort eventually stops every shard |

**This model found a real bug**: the original `stopAllShards` sent STOP
once, logged failures, and let the finalizer/abort proceed — with a full
directive buffer that orphans running shards, violating
`CleanupAfterDelete` and `AbortCleansUp`. The fix (retry STOP until every
connected spoke accepted it, holding the finalizer / the Aborted
transition until then) is what the `AbortSound`/`FinalizeSound` action
properties pin down.

## Explicit modelling assumptions

Directives accepted into a spoke's buffer are eventually delivered: the
buffer survives the modelled hub restart. In reality a hub crash in the
window between queueing a STOP and the spoke's gRPC stream consuming it
would lose the directive after the finalizer was already removed. That
window is now closed by **spoke-side orphan GC**
(`internal/spoke/orphan_gc.go`, the `OrphanGC` action in the model):
heartbeat responses carry the hub's authoritative CaptureLoadTest list,
and shards referencing a load test that no longer exists are deleted
locally — so `CleanupAfterDelete` holds even if a queued STOP is lost
after finalization. The STOP-delivery-before-finalize rule remains the
primary mechanism (prompt cleanup); GC is the backstop (bounded by the
heartbeat interval plus the GC minimum age).

Spoke crash/deregistration remains out of scope for STOP delivery: a
deregistered spoke's shards are bounded by the capture size, and the same
orphan GC removes them when the spoke reconnects.

## Running

```bash
make verify-tla          # downloads tla2tools.jar, runs TLC on both specs
# or directly:
./hack/verify-tla.sh
```

TLC is run with `-deadlock` disabled-deadlock checking because terminal
states (run finished, finalizer removed) intentionally have no enabled
actions.

## Keeping specs and code in sync

The specs mirror specific functions; if you change them, re-check the
model (CI does) and update the model when behaviour changes:

| Spec element | Code |
|--------------|------|
| `Sharding!Process` shard filter | `engine.go` `readLoop` + `shardOf` |
| `Sharding!Restart` | `replay_runner.go` `replayJobBackoffLimit` |
| `KaptureLoadTest!SendStart` resend guard | `loadtest_controller.go` reported-shard resend loop |
| `KaptureLoadTest!QueueStop`/`EnterAborted`/`Finalize` | `loadtest_controller.go` `stopAllShards` retry + abort/delete paths |
| `KaptureLoadTest!DeliverDirective` | `replay_directives.go` `Handle` (idempotent START, label-scoped STOP) |
| `KaptureLoadTest!Report` | `replay_controller.go` `reportToHub` + heartbeat piggyback |
| `KaptureLoadTest!HubRestart` | in-memory `Server` registry loss |
| `BufferCapacity` | `grpc_server.go` `DirectiveBufferSize` |
