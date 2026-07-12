--------------------------- MODULE KaptureLoadTest ---------------------------
(***************************************************************************)
(* Hub-spoke coordination protocol for one CaptureLoadTest.                *)
(*                                                                         *)
(* Code under verification:                                                *)
(*   internal/hub/loadtest_controller.go  (plan, resend, aggregate,        *)
(*                                         abort, delete/finalizer)        *)
(*   internal/hub/grpc_server.go          (bounded per-spoke directive     *)
(*                                         buffers, replay registry)       *)
(*   internal/spoke/replay_directives.go  (idempotent START upsert, STOP   *)
(*                                         deletes by load-test label)     *)
(*                                                                         *)
(* Modelled failure modes:                                                 *)
(*   - Bounded per-spoke directive buffers: a send can fail with           *)
(*     ResourceExhausted, which the coordinator must survive by retrying.  *)
(*   - Duplicate START directives (the coordinator resends until a shard   *)
(*     reports); the spoke handles them as idempotent upserts.             *)
(*   - Shard Jobs failing instead of completing.                           *)
(*   - Hub restart: the in-memory replay registry (hubView) is lost and    *)
(*     must be repopulated by spoke heartbeats; the coordinator's          *)
(*     delivery bookkeeping for STOP is also lost and re-derived.          *)
(*                                                                         *)
(* Modelling assumption (documented in verification/tla/README.md):        *)
(* directives accepted into a spoke's buffer are eventually delivered —    *)
(* the buffer survives in the model where in reality a hub crash in the    *)
(* window between queueing a STOP and the spoke's stream consuming it      *)
(* would drop it after the finalizer was already removed.  Closing that    *)
(* window needs delivery acknowledgements and is listed as future work.    *)
(*                                                                         *)
(* Checked properties:                                                     *)
(*   NoStartAfterStop      Per-spoke FIFO ordering means a STOP is never   *)
(*                         followed by a buffered START, so a stopped      *)
(*                         load test cannot resurrect shards (safety).     *)
(*   StartOnlyToOwner      START directives only go to the spoke that      *)
(*                         owns the shard (safety).                        *)
(*   CompletionSound       The Completed phase is only entered when every  *)
(*                         shard reported success (safety).                *)
(*   AbortSound /          Aborted phase / finalizer removal only after    *)
(*   FinalizeSound         STOP was accepted by every assigned spoke       *)
(*                         (safety; this is the property that forced the   *)
(*                         stopAllShards retry fix).                       *)
(*   Termination           Every run reaches a terminal phase, even with   *)
(*                         full buffers and a hub restart (liveness).      *)
(*   CleanupAfterDelete    Deletion eventually removes every shard from    *)
(*                         every spoke — no orphaned load (liveness).      *)
(*   AbortCleansUp         Abort eventually stops every shard (liveness).  *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    Spokes,          \* connected spoke clusters (model values)
    WorkersPerSpoke, \* shards per spoke
    BufferCapacity,  \* per-spoke directive buffer size (DirectiveBufferSize)
    MaxHubRestarts   \* bound on modelled hub restarts

ASSUME WorkersPerSpoke \in Nat \ {0}
ASSUME BufferCapacity \in Nat \ {0}
ASSUME MaxHubRestarts \in Nat

NumShards == Cardinality(Spokes) * WorkersPerSpoke
Shards == 0 .. NumShards - 1

ShardStates == {"None", "Running", "Completed", "Failed"}
ViewStates  == {"Unknown", "Running", "Completed", "Failed"}
Phases      == {"Distributing", "Running", "Completed", "Failed", "Aborted"}

StartDirective(sh) == [kind |-> "START", shard |-> sh]
StopDirective     == [kind |-> "STOP", shard |-> 0]  \* shard unused for STOP

VARIABLES
    assign,        \* [Shards -> Spokes]: the recorded assignment (status.assignments)
    phase,         \* CaptureLoadTest status.phase
    buf,           \* [Spokes -> Seq(directive)]: per-spoke FIFO directive buffer
    shardState,    \* [Shards -> ShardStates]: TrafficReplay/Job state on the spokes
    hubView,       \* [Shards -> ViewStates]: hub's in-memory replay registry
    aborting,      \* abort policy tripped
    deleted,       \* deletion requested (deletionTimestamp set)
    finalized,     \* finalizer removed; hub involvement over
    stopDelivered, \* [Spokes -> BOOLEAN]: STOP accepted into the spoke's buffer
    hubRestarts    \* number of modelled hub restarts so far

vars == <<assign, phase, buf, shardState, hubView, aborting, deleted,
          finalized, stopDelivered, hubRestarts>>

AssignedSpokes == {assign[sh] : sh \in Shards}

TypeOK ==
    /\ assign \in [Shards -> Spokes]
    /\ phase \in Phases
    /\ buf \in [Spokes -> Seq([kind : {"START", "STOP"}, shard : Shards])]
    /\ \A sp \in Spokes : Len(buf[sp]) <= BufferCapacity
    /\ shardState \in [Shards -> ShardStates]
    /\ hubView \in [Shards -> ViewStates]
    /\ aborting \in BOOLEAN
    /\ deleted \in BOOLEAN
    /\ finalized \in BOOLEAN
    /\ stopDelivered \in [Spokes -> BOOLEAN]
    /\ hubRestarts \in 0 .. MaxHubRestarts

(* The plan is any total shard->spoke function.  The Go planner produces   *)
(* one specific dense assignment; verifying over every total function      *)
(* covers it and any future planner.                                       *)
Init ==
    /\ assign \in [Shards -> Spokes]
    /\ phase = "Distributing"
    /\ buf = [sp \in Spokes |-> <<>>]
    /\ shardState = [sh \in Shards |-> "None"]
    /\ hubView = [sh \in Shards |-> "Unknown"]
    /\ aborting = FALSE
    /\ deleted = FALSE
    /\ finalized = FALSE
    /\ stopDelivered = [sp \in Spokes |-> FALSE]
    /\ hubRestarts = 0

-----------------------------------------------------------------------------
(* Hub coordinator actions (loadtest_controller.go)                        *)

(* Resend START for every shard that has not reported yet.  Blocked when   *)
(* the spoke's buffer is full (SendReplayDirective -> ResourceExhausted);  *)
(* the reconcile loop simply retries later.                                *)
SendStart(sh) ==
    /\ phase \in {"Distributing", "Running"}
    /\ ~deleted /\ ~aborting
    /\ hubView[sh] = "Unknown"
    /\ Len(buf[assign[sh]]) < BufferCapacity
    /\ buf' = [buf EXCEPT ![assign[sh]] = Append(@, StartDirective(sh))]
    /\ UNCHANGED <<assign, phase, shardState, hubView, aborting, deleted,
                   finalized, stopDelivered, hubRestarts>>

(* Aggregation: phase follows the reported shard states.                   *)
AggRunning ==
    /\ phase = "Distributing"
    /\ \E sh \in Shards : hubView[sh] # "Unknown"
    /\ phase' = "Running"
    /\ UNCHANGED <<assign, buf, shardState, hubView, aborting, deleted,
                   finalized, stopDelivered, hubRestarts>>

AggCompleted ==
    /\ phase \in {"Distributing", "Running"}
    /\ ~aborting /\ ~deleted
    /\ \A sh \in Shards : hubView[sh] = "Completed"
    /\ phase' = "Completed"
    /\ UNCHANGED <<assign, buf, shardState, hubView, aborting, deleted,
                   finalized, stopDelivered, hubRestarts>>

AggFailed ==
    /\ phase \in {"Distributing", "Running"}
    /\ ~aborting /\ ~deleted
    /\ \A sh \in Shards : hubView[sh] \in {"Completed", "Failed"}
    /\ \E sh \in Shards : hubView[sh] = "Failed"
    /\ phase' = "Failed"
    /\ UNCHANGED <<assign, buf, shardState, hubView, aborting, deleted,
                   finalized, stopDelivered, hubRestarts>>

(* The abort policy trips (maxDuration / errorPercent).  The trigger is    *)
(* modelled as fully nondeterministic; its conditions are monotonic in     *)
(* the implementation so a tripped abort stays tripped.                    *)
TriggerAbort ==
    /\ phase \in {"Distributing", "Running"}
    /\ ~deleted /\ ~aborting
    /\ aborting' = TRUE
    /\ UNCHANGED <<assign, phase, buf, shardState, hubView, deleted,
                   finalized, stopDelivered, hubRestarts>>

(* stopAllShards: queue STOP to one assigned spoke.  Retried until every   *)
(* spoke accepted it — a full buffer just means trying again later.        *)
QueueStop(sp) ==
    /\ aborting \/ deleted
    /\ ~finalized
    /\ sp \in AssignedSpokes
    /\ ~stopDelivered[sp]
    /\ Len(buf[sp]) < BufferCapacity
    /\ buf' = [buf EXCEPT ![sp] = Append(@, StopDirective)]
    /\ stopDelivered' = [stopDelivered EXCEPT ![sp] = TRUE]
    /\ UNCHANGED <<assign, phase, shardState, hubView, aborting, deleted,
                   finalized, hubRestarts>>

(* The Aborted phase is only entered once STOP reached every assigned      *)
(* spoke (the stopAllShards retry fix).                                    *)
EnterAborted ==
    /\ aborting
    /\ phase \in {"Distributing", "Running"}
    /\ \A sp \in AssignedSpokes : stopDelivered[sp]
    /\ phase' = "Aborted"
    /\ UNCHANGED <<assign, buf, shardState, hubView, aborting, deleted,
                   finalized, stopDelivered, hubRestarts>>

(* The user deletes the CaptureLoadTest.                                   *)
RequestDelete ==
    /\ ~deleted
    /\ deleted' = TRUE
    /\ UNCHANGED <<assign, phase, buf, shardState, hubView, aborting,
                   finalized, stopDelivered, hubRestarts>>

(* Finalizer removal: only after STOP reached every assigned spoke.        *)
(* ClearReplayStatuses wipes the hub's registry for this load test.        *)
Finalize ==
    /\ deleted
    /\ ~finalized
    /\ \A sp \in AssignedSpokes : stopDelivered[sp]
    /\ finalized' = TRUE
    /\ hubView' = [sh \in Shards |-> "Unknown"]
    /\ UNCHANGED <<assign, phase, buf, shardState, aborting, deleted,
                   stopDelivered, hubRestarts>>

(* Hub restart: the in-memory replay registry and the reconciler's         *)
(* delivery bookkeeping are lost; the CR (phase, assignments, deletion,    *)
(* finalizer) survives in etcd.  Heartbeats repopulate the registry and    *)
(* the deletion/abort paths re-send STOP (idempotent on the spoke).        *)
HubRestart ==
    /\ hubRestarts < MaxHubRestarts
    /\ ~finalized
    /\ hubRestarts' = hubRestarts + 1
    /\ hubView' = [sh \in Shards |-> "Unknown"]
    /\ stopDelivered' = [sp \in Spokes |-> FALSE]
    /\ UNCHANGED <<assign, phase, buf, shardState, aborting, deleted,
                   finalized>>

-----------------------------------------------------------------------------
(* Spoke actions (replay_directives.go / replay_controller.go)             *)

(* Consume the head of the FIFO directive buffer.  START is an idempotent  *)
(* upsert; STOP deletes every shard of this load test on the spoke.        *)
DeliverDirective(sp) ==
    /\ Len(buf[sp]) > 0
    /\ LET d == Head(buf[sp]) IN
        /\ buf' = [buf EXCEPT ![sp] = Tail(@)]
        /\ IF d.kind = "START"
           THEN shardState' = IF shardState[d.shard] = "None"
                              THEN [shardState EXCEPT ![d.shard] = "Running"]
                              ELSE shardState
           ELSE shardState' = [sh \in Shards |->
                                  IF assign[sh] = sp THEN "None" ELSE shardState[sh]]
    /\ UNCHANGED <<assign, phase, hubView, aborting, deleted, finalized,
                   stopDelivered, hubRestarts>>

(* The replay Job finishes, successfully or not.                           *)
ShardTerminate(sh) ==
    /\ shardState[sh] = "Running"
    /\ \E outcome \in {"Completed", "Failed"} :
        shardState' = [shardState EXCEPT ![sh] = outcome]
    /\ UNCHANGED <<assign, phase, buf, hubView, aborting, deleted, finalized,
                   stopDelivered, hubRestarts>>

(* Status reporting (ReportReplayStatus + heartbeat piggyback): the hub's  *)
(* view converges to the spoke-side truth.                                 *)
Report(sh) ==
    /\ shardState[sh] # "None"
    /\ hubView[sh] # shardState[sh]
    /\ hubView' = [hubView EXCEPT ![sh] = shardState[sh]]
    /\ UNCHANGED <<assign, phase, buf, shardState, aborting, deleted,
                   finalized, stopDelivered, hubRestarts>>

-----------------------------------------------------------------------------

Next ==
    \/ \E sh \in Shards : SendStart(sh) \/ ShardTerminate(sh) \/ Report(sh)
    \/ \E sp \in Spokes : DeliverDirective(sp) \/ QueueStop(sp)
    \/ AggRunning \/ AggCompleted \/ AggFailed
    \/ TriggerAbort \/ EnterAborted
    \/ RequestDelete \/ Finalize
    \/ HubRestart

(* SendStart and QueueStop have flickering enablement (buffers fill and    *)
(* drain), so they get strong fairness — matching the reconcile loop that  *)
(* retries them every RequeueInterval regardless of transient failures.    *)
Fairness ==
    /\ \A sp \in Spokes : WF_vars(DeliverDirective(sp))
    /\ \A sp \in Spokes : SF_vars(QueueStop(sp))
    /\ \A sh \in Shards : SF_vars(SendStart(sh))
    /\ \A sh \in Shards : WF_vars(ShardTerminate(sh))
    /\ \A sh \in Shards : WF_vars(Report(sh))
    /\ WF_vars(AggRunning) /\ WF_vars(AggCompleted) /\ WF_vars(AggFailed)
    /\ WF_vars(EnterAborted) /\ WF_vars(Finalize)

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
(* Safety *)

(* START directives only target the owning spoke, so shard indexes stay    *)
(* disjoint across the fleet at the directive level.                       *)
StartOnlyToOwner ==
    \A sp \in Spokes : \A i \in 1 .. Len(buf[sp]) :
        buf[sp][i].kind = "START" => assign[buf[sp][i].shard] = sp

(* FIFO buffers never hold a START behind a STOP: a stopped load test      *)
(* cannot have shards resurrected by a late START.                         *)
NoStartAfterStop ==
    \A sp \in Spokes : \A i, j \in 1 .. Len(buf[sp]) :
        (i < j /\ buf[sp][i].kind = "STOP") => buf[sp][j].kind = "STOP"

(* Phase transitions are sound with respect to the hub's view.             *)
CompletionSound ==
    [][ (phase' = "Completed" /\ phase # "Completed") =>
            \A sh \in Shards : hubView[sh] = "Completed" ]_vars

AbortSound ==
    [][ (phase' = "Aborted" /\ phase # "Aborted") =>
            \A sp \in AssignedSpokes : stopDelivered[sp] ]_vars

FinalizeSound ==
    [][ (finalized' /\ ~finalized) =>
            \A sp \in AssignedSpokes : stopDelivered[sp] ]_vars

-----------------------------------------------------------------------------
(* Liveness *)

Terminal == phase \in {"Completed", "Failed", "Aborted"} \/ finalized

Termination == <>Terminal

(* Deleting the load test eventually removes every shard from every       *)
(* spoke: no orphaned replay load.                                         *)
CleanupAfterDelete ==
    deleted ~> (\A sh \in Shards : shardState[sh] = "None")

(* Aborting eventually stops every shard.                                  *)
AbortCleansUp ==
    aborting ~> (\A sh \in Shards : shardState[sh] = "None")

=============================================================================
