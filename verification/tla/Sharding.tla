------------------------------ MODULE Sharding ------------------------------
(***************************************************************************)
(* Data-plane sharding of Kapture's replay engine.                        *)
(*                                                                         *)
(* Code under verification:                                                *)
(*   internal/plugin/replay/engine.go   (shardOf, readLoop shard filter)   *)
(*   internal/spoke/replay_runner.go    (Job backoffLimit retries)         *)
(*                                                                         *)
(* Each replay worker (shard) streams the FULL capture and sends only the  *)
(* requests whose hashed ID maps to its own shard index.  Workers share no *)
(* state at replay time.  A worker's Job may fail and be retried up to     *)
(* MaxRestarts times (batch Job backoffLimit); a retry re-reads the        *)
(* capture from the beginning.                                             *)
(*                                                                         *)
(* The hash function is modelled as an ARBITRARY total function from       *)
(* requests to shard indexes, chosen nondeterministically at Init.  TLC    *)
(* therefore checks the properties for EVERY possible hash assignment,     *)
(* which subsumes FNV-1a mod N: the only property of shardOf the code      *)
(* relies on is that it is a deterministic total function, mirrored here   *)
(* by `hash` being immutable after Init.                                   *)
(*                                                                         *)
(* Checked properties:                                                     *)
(*   OnlyOwnerSends        A request is only ever sent by the shard that   *)
(*                         owns it (safety; holds even across retries).    *)
(*   AtMostOneShardSends   Shards are pairwise disjoint (safety).          *)
(*   CrashFreeExactlyOnce  With no Job retries, every request is sent at   *)
(*                         most once fleet-wide (safety).                  *)
(*   Termination           All shards finish (liveness).                   *)
(*   Coverage              Every request is sent at least once, i.e. the   *)
(*                         shards are exhaustive (liveness).               *)
(*                                                                         *)
(* Together, AtMostOneShardSends + Coverage are the "disjoint and          *)
(* exhaustive" claim from docs/multi-cell-load-testing.md.  Retries make   *)
(* delivery at-least-once WITHIN a shard, which is why exactly-once is     *)
(* stated conditionally on a crash-free run.                               *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Requests,     \* set of captured request IDs (model values)
    ShardCount,   \* number of replay workers
    MaxRestarts   \* Job backoffLimit: retries per shard

ASSUME ShardCount \in Nat \ {0}
ASSUME MaxRestarts \in Nat

Shards == 0 .. ShardCount - 1

VARIABLES
    hash,       \* [Requests -> Shards]: abstract shardOf() assignment
    remaining,  \* [Shards -> SUBSET Requests]: unread input in this attempt
    done,       \* [Shards -> BOOLEAN]: worker finished
    restarts,   \* [Shards -> 0..MaxRestarts]: Job retries used
    sentBy      \* [Requests -> [Shards -> Nat]]: sends per (request, shard)

vars == <<hash, remaining, done, restarts, sentBy>>

TypeOK ==
    /\ hash \in [Requests -> Shards]
    /\ remaining \in [Shards -> SUBSET Requests]
    /\ done \in [Shards -> BOOLEAN]
    /\ restarts \in [Shards -> 0..MaxRestarts]
    /\ sentBy \in [Requests -> [Shards -> 0..(MaxRestarts + 1)]]

Init ==
    /\ hash \in [Requests -> Shards]
    /\ remaining = [sh \in Shards |-> Requests]
    /\ done = [sh \in Shards |-> FALSE]
    /\ restarts = [sh \in Shards |-> 0]
    /\ sentBy = [r \in Requests |-> [sh \in Shards |-> 0]]

(* readLoop: consume one input record; send it iff this shard owns it.    *)
Process(sh) ==
    /\ ~done[sh]
    /\ \E r \in remaining[sh] :
        /\ remaining' = [remaining EXCEPT ![sh] = @ \ {r}]
        /\ sentBy' = IF hash[r] = sh
                     THEN [sentBy EXCEPT ![r][sh] = @ + 1]
                     ELSE sentBy
    /\ UNCHANGED <<hash, done, restarts>>

(* Worker exits successfully once its input is exhausted.                  *)
Complete(sh) ==
    /\ ~done[sh]
    /\ remaining[sh] = {}
    /\ done' = [done EXCEPT ![sh] = TRUE]
    /\ UNCHANGED <<hash, remaining, restarts, sentBy>>

(* Job retry: the worker crashes at an arbitrary point and a fresh pod     *)
(* re-reads the capture from the beginning.  Sends already made are not    *)
(* undone — this is what makes within-shard delivery at-least-once.        *)
Restart(sh) ==
    /\ ~done[sh]
    /\ restarts[sh] < MaxRestarts
    /\ restarts' = [restarts EXCEPT ![sh] = @ + 1]
    /\ remaining' = [remaining EXCEPT ![sh] = Requests]
    /\ UNCHANGED <<hash, done, sentBy>>

Next == \E sh \in Shards : Process(sh) \/ Complete(sh) \/ Restart(sh)

Fairness ==
    \A sh \in Shards : WF_vars(Process(sh)) /\ WF_vars(Complete(sh))

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
(* Safety *)

(* Only the owner shard ever sends a request — the shard filter never      *)
(* leaks another shard's slice, even across retries.                       *)
OnlyOwnerSends ==
    \A r \in Requests : \A sh \in Shards :
        sentBy[r][sh] > 0 => hash[r] = sh

(* Shards are pairwise disjoint: no request is sent by two shards.         *)
AtMostOneShardSends ==
    \A r \in Requests :
        Cardinality({sh \in Shards : sentBy[r][sh] > 0}) <= 1

(* In a crash-free run every request is sent at most once fleet-wide.      *)
CrashFreeExactlyOnce ==
    (\A sh \in Shards : restarts[sh] = 0) =>
        \A r \in Requests : sentBy[r][hash[r]] <= 1

-----------------------------------------------------------------------------
(* Liveness *)

AllDone == \A sh \in Shards : done[sh]

Termination == <>AllDone

(* The shards are exhaustive: every request is eventually sent (at least   *)
(* once) by its owner.                                                     *)
Coverage == <>(\A r \in Requests : sentBy[r][hash[r]] >= 1)

=============================================================================
