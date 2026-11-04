----------------------- MODULE ClusterFailoverPlanning -----------------------
(***************************************************************************)
(* Two-partition model of how a Raft leader reassigns partitions when more *)
(* than one member is gone (gh #300 review, gh #280). The one-partition     *)
(* model in ClusterRestartRejoin cannot show this.                          *)
(*                                                                         *)
(* Code each action abstracts (crates/mqdb-cluster/src/cluster):           *)
(*   Fail      raft/coordinator/replication.rs                             *)
(*             reassign_partitions_for_dead_node                           *)
(*   FailTogether  two deaths reach the Raft task within the batch window  *)
(*             (1.5 s with no new death), in either order                  *)
(*   Commit    the proposals reach the applied partition map               *)
(*   LoseTail  leadership changes and the unapplied proposals are          *)
(*             overwritten; this model keeps the members as they were,     *)
(*             where a new leader in the code has its own view of them     *)
(*   Scan      scan_partition_map_for_departed_nodes, at leader start and  *)
(*             every 30 s                                                  *)
(*   Rejoin    handle_node_alive, a member is back                         *)
(*   Rebalance rebalancer.rs redistribute_primaries: move a primary to     *)
(*             one of its replicas                                         *)
(*                                                                         *)
(* Constants:                                                              *)
(*   PlanFromProposals  TRUE: a failover reads the applied map plus         *)
(*                      unapplied proposals. FALSE: the applied map. It     *)
(*                      has no effect on the main cfg, which passes with    *)
(*                      it FALSE too, so this model is not evidence for     *)
(*                      planned-map reads; unit tests cover those.          *)
(*   HolderOnly         TRUE: promote only a live mapped replica, otherwise *)
(*                      leave the partition on its holder. FALSE: fall back *)
(*                      to the first alive node, which holds no copy.       *)
(*   LiveTargetsOnly    TRUE: rebalance only targets a live replica.        *)
(*   RepairScan         FALSE: Scan never runs.                             *)
(*   ScanReplicas       FALSE: Scan looks at primaries only.                *)
(*   AddReplicas        TRUE: L can replace a replica or fill an empty slot *)
(*                      with no copy, with no death needed, as the          *)
(*                      rebalancer does (rebalancer.rs:275-296), and can    *)
(*                      then be promoted. The map does not record who       *)
(*                      synced. The model has no replaced-replica source    *)
(*                      (snapshot.rs:375-377): only the mapped holder.      *)
(*   WaitForData        TRUE: TakeRole refuses a node mapped primary with   *)
(*                      no copy while its mapped source is alive (gh #273). *)
(*                      FALSE: it can serve at once. The code also takes    *)
(*                      the role with a live source after 30 s without a    *)
(*                      finished catchup and after 90 s on a source never   *)
(*                      heard from; those takes are not modelled.           *)
(*   Together           TRUE: two nodes can die within the window.          *)
(*   BatchDeaths        TRUE: deaths in the window are all out of the alive *)
(*                      set before any is processed. FALSE: the first is    *)
(*                      processed while the second still counts as alive.   *)
(*   KeepDeadReplica    TRUE: a dead replica entry stays while the primary  *)
(*                      is also gone. FALSE: it is always removed.          *)
(*                                                                         *)
(* Abstractions: one leader identity, L, which stands for a live majority: *)
(* Commit, Scan and Fail run with L alone, and loss of a majority is not    *)
(* covered. Proposals are applied in order. InvPrimaryWasAHolder means      *)
(* "the primary is not L". A and B are assumed to keep their copies, and in *)
(* this model only L can lack one; the code has other ways to lack one      *)
(* (#287, a wiped disk). ClusterEmptyPrimary and ClusterPromotedTransfer    *)
(* model the transfer itself. The time a detection takes is not modelled.   *)
(*                                                                         *)
(* Expected, verified for these constants only:                             *)
(*   ClusterFailoverPlanning: all pass                                      *)
(*   _no_holder_rule: fails InvPrimaryWasAHolder                            *)
(*   _stale: passes. It turns RepairScan and the planned-map reads off and   *)
(*       checks InvNoStrandedPrimary only, with no lost proposals. Planning  *)
(*       matters for epochs, which this model lacks; see the unit tests     *)
(*   _departed: fails InvRebalanceTargetLive, the gh #280 panic             *)
(*   _new_replica: fails InvPrimaryWasAHolder: the map can still name a node *)
(*       with no copy.                                                      *)
(*   _new_replica_nowait: WaitForData off, fails InvNoEmptyServe.           *)
(*   _new_replica_wait: passes InvNoEmptyServe.                             *)
(*   _new_replica_no_source: fails InvNoEmptyTake: L, a replica with no     *)
(*       copy, is promoted after the source died, so it never waits and     *)
(*       takes the role empty. The trace has no wait followed by the source *)
(*       dying; that path is not separately shown.                          *)
(*   _no_scan: fails Repaired, a lost failover is never redone              *)
(*   _scan_primaries_only: fails Repaired, a lost replica entry stays       *)
(*   _no_batch_no_keep: BatchDeaths and KeepDeadReplica off, fails           *)
(*       HolderServes at the same state count as _no_batch, so KeepDeadReplica *)
(*       adds nothing once batching is off. This is not main: main also has  *)
(*       HolderOnly, PlanFromProposals and LiveTargetsOnly off.              *)
(*   _holder_serves_fails3: MaxFails 3, fails HolderServes; see below.      *)
(*   _no_batch: fails HolderServes, promoted to a node dying in the window  *)
(*   _no_keep: fails HolderServes, the replica returns first with no entry  *)
(*                                                                         *)
(* This model has no writes, so it says nothing about who holds an acked    *)
(* write. Replication is asynchronous: a primary acks before its replica    *)
(* applies, so holders dying together can differ, and promoting either can  *)
(* lose acked writes. HolderServes only says a partition whose map entries  *)
(* named a node when both holders died gets a live primary once that node   *)
(* is back. An earlier version tracked current[p] and                       *)
(* InvServingPrimaryIsCurrent; a review refuted them as a measure of acked  *)
(* writes and they are gone.                                                *)
(*                                                                         *)
(* Scope of HolderServes: two holders of an RF=2 partition dying together   *)
(* as the first failure, with a live majority. It holds only because        *)
(* MaxFails 2 makes FailTogether the only failure on that path, and named   *)
(* never clears. A further failure breaks it (_holder_serves_fails3): a     *)
(* holder that returns and dies again strands the partition on it. A second *)
(* death after the window is also outside it: the first holder has left the *)
(* map and the partition stays at 503 until the second returns or a forced  *)
(* rebalance.                                                               *)
(*                                                                         *)
(* Model restriction: Rejoin requires proposals = <<>>, so a node does not  *)
(* return while a proposal is unapplied. The code has no such guard.        *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS L, A, B,
          PlanFromProposals, HolderOnly, LiveTargetsOnly, RepairScan, ScanReplicas, AddReplicas,
          Together, BatchDeaths, KeepDeadReplica, WaitForData,
          MaxFails, MaxProposals, MaxLost

VARIABLES applied, proposals, members, failsLeft, proposalsLeft, lostLeft, deadTarget, named, returned,
          filled, takenEmpty

vars == <<applied, proposals, members, failsLeft, proposalsLeft, lostLeft, deadTarget, named, returned,
          filled, takenEmpty>>

Nodes == {L, A, B}
Partitions == {1, 2}
None == "none"
InitialMap ==
    [p \in Partitions |->
        IF p = 1 THEN [primary |-> A, replica |-> B] ELSE [primary |-> B, replica |-> A]]

RECURSIVE SetToSeq(_)
SetToSeq(set) ==
    IF set = {} THEN <<>>
    ELSE LET x == CHOOSE e \in set : TRUE
         IN <<x>> \o SetToSeq(set \ {x})

Apply(map, proposal) ==
    [map EXCEPT ![proposal.p] = [primary |-> proposal.primary, replica |-> proposal.replica]]

RECURSIVE Replay(_, _)
Replay(map, list) ==
    IF list = <<>> THEN map ELSE Replay(Apply(map, Head(list)), Tail(list))

Planned == Replay(applied, proposals)
Base == IF PlanFromProposals THEN Planned ELSE applied

Init ==
    /\ applied = InitialMap
    /\ proposals = <<>>
    /\ members = Nodes
    /\ failsLeft = MaxFails
    /\ proposalsLeft = MaxProposals
    /\ lostLeft = MaxLost
    /\ deadTarget = FALSE
    /\ named = [p \in Partitions |-> {}]
    /\ returned = [p \in Partitions |-> FALSE]
    /\ takenEmpty = "no"
    /\ filled = [p \in Partitions |-> FALSE]

\* The repair one departed node d needs on partition p, or None.
Repair(map, p, d, alive) ==
    LET a == map[p]
    IN CASE a.primary = d /\ a.replica \in alive ->
                [p |-> p, primary |-> a.replica, replica |-> None]
         [] a.primary = d /\ HolderOnly -> None
         [] a.primary = d ->
                [p |-> p, primary |-> L, replica |-> None]
         [] a.replica = d /\ KeepDeadReplica /\ a.primary \notin alive -> None
         [] a.replica = d ->
                [p |-> p, primary |-> a.primary, replica |-> None]
         [] OTHER -> None

Repairs(map, d, alive) == {Repair(map, p, d, alive) : p \in Partitions} \ {None}

Fail(d) ==
    /\ d \in members
    /\ d # L
    /\ failsLeft > 0
    /\ failsLeft' = failsLeft - 1
    /\ LET alive == members \ {d}
           repairs == Repairs(Base, d, alive)
       IN /\ Cardinality(repairs) <= proposalsLeft
          /\ proposalsLeft' = proposalsLeft - Cardinality(repairs)
          /\ proposals' = proposals \o SetToSeq(repairs)
          /\ members' = alive
    /\ UNCHANGED <<applied, lostLeft, deadTarget, named, returned, filled, takenEmpty>>

\* Both deaths reach the task within the window, d1 first.
FailTogether(d1, d2) ==
    /\ Together
    /\ d1 # d2
    /\ {d1, d2} \subseteq members
    /\ L \notin {d1, d2}
    /\ failsLeft >= 2
    /\ failsLeft' = failsLeft - 2
    /\ LET alive == members \ {d1, d2}
           first == Repairs(Base, d1, IF BatchDeaths THEN alive ELSE members \ {d1})
           afterFirst == IF PlanFromProposals THEN Replay(Base, SetToSeq(first)) ELSE Base
           second == Repairs(afterFirst, d2, alive)
           count == Cardinality(first) + Cardinality(second)
       IN /\ count <= proposalsLeft
          /\ proposalsLeft' = proposalsLeft - count
          /\ proposals' = proposals \o SetToSeq(first) \o SetToSeq(second)
          /\ members' = alive
    /\ named' = [p \in Partitions |->
                    IF {Base[p].primary, Base[p].replica} = {d1, d2} THEN {d1, d2} ELSE named[p]]
    /\ UNCHANGED <<applied, lostLeft, deadTarget, returned, filled, takenEmpty>>

Commit ==
    /\ proposals # <<>>
    /\ applied' = Apply(applied, Head(proposals))
    /\ proposals' = Tail(proposals)
    /\ returned' = [p \in Partitions |->
                       returned[p] /\ applied'[p].primary \notin members /\ applied'[p].replica \in members]
    /\ UNCHANGED <<members, failsLeft, proposalsLeft, lostLeft, deadTarget, named, filled, takenEmpty>>

LoseTail ==
    /\ proposals # <<>>
    /\ lostLeft > 0
    /\ lostLeft' = lostLeft - 1
    /\ proposals' = <<>>
    /\ UNCHANGED <<applied, members, failsLeft, proposalsLeft, deadTarget, named, returned, filled, takenEmpty>>

Departed(n) == n \in Nodes /\ n \notin members

ScanTargets ==
    {n \in Nodes : Departed(n) /\
        \E p \in Partitions :
            Planned[p].primary = n \/ (ScanReplicas /\ Planned[p].replica = n)}

ScanRepairs ==
    {r \in UNION {Repairs(Planned, n, members) : n \in ScanTargets} :
        ScanReplicas \/ Planned[r.p].primary \in ScanTargets}

Scan ==
    /\ RepairScan
    /\ proposals = <<>>
    /\ ScanRepairs # {}
    /\ Cardinality(ScanRepairs) <= proposalsLeft
    /\ proposalsLeft' = proposalsLeft - Cardinality(ScanRepairs)
    /\ proposals' = SetToSeq(ScanRepairs)
    /\ UNCHANGED <<applied, members, failsLeft, lostLeft, deadTarget, named, returned, filled, takenEmpty>>

Rejoin(n) ==
    /\ Departed(n)
    /\ proposals = <<>>
    /\ members' = members \cup {n}
    /\ returned' = [p \in Partitions |->
                       returned[p] \/ (applied[p].replica = n /\ applied[p].primary \notin members)]
    /\ UNCHANGED <<applied, proposals, failsLeft, proposalsLeft, lostLeft, deadTarget, named, filled, takenEmpty>>

Rebalance(p) ==
    /\ proposalsLeft > 0
    /\ proposalsLeft' = proposalsLeft - 1
    /\ LET a == Base[p]
       IN /\ a.replica # None
          /\ a.primary \in members
          /\ (LiveTargetsOnly => a.replica \in members)
          /\ deadTarget' = (deadTarget \/ a.replica \notin members)
          /\ proposals' = Append(proposals, [p |-> p, primary |-> a.replica, replica |-> a.primary])
    /\ UNCHANGED <<applied, members, failsLeft, lostLeft, named, returned, filled, takenEmpty>>

\* The rebalancer gives a partition a new replica with no copy yet. L plays that node.
\* _new_replica fails on the promotion that follows, with or without a failure first, so
\* the 30 s hold after a death in the code, which is not modelled, would not stop it.
AddReplica(p) ==
    /\ AddReplicas
    /\ proposalsLeft > 0
    /\ proposalsLeft' = proposalsLeft - 1
    /\ Base[p].replica # L
    /\ Base[p].primary \in members
    /\ Base[p].primary # L
    /\ proposals' = Append(proposals, [p |-> p, primary |-> Base[p].primary, replica |-> L])
    /\ UNCHANGED <<applied, members, failsLeft, lostLeft, deadTarget, named, returned, filled, takenEmpty>>

\* The snapshot for a node with no copy arrives while a holder of it is live and mapped.
Transfer(p) ==
    /\ AddReplicas
    /\ ~filled[p]
    /\ L \in {applied[p].primary, applied[p].replica}
    /\ ({applied[p].primary, applied[p].replica} \ {L}) \cap members # {}
    /\ filled' = [filled EXCEPT ![p] = TRUE]
    /\ UNCHANGED <<applied, proposals, members, failsLeft, proposalsLeft, lostLeft, deadTarget, named, returned, takenEmpty>>

\* A node mapped primary with no copy takes the role. takenEmpty records whether its
\* mapped source was alive at that moment.
TakeRole(p) ==
    /\ AddReplicas
    /\ takenEmpty = "no"
    /\ applied[p].primary = L
    /\ ~filled[p]
    /\ LET sourceLive == applied[p].replica \in members \ {L}
       IN /\ (WaitForData => ~sourceLive)
          /\ takenEmpty' = IF sourceLive THEN "source_alive" ELSE "source_dead"
    /\ UNCHANGED <<applied, proposals, members, failsLeft, proposalsLeft, lostLeft, deadTarget, named, returned, filled>>

Next ==
    \/ \E p \in Partitions : TakeRole(p)
    \/ \E p \in Partitions : Transfer(p)
    \/ \E p \in Partitions : AddReplica(p)
    \/ \E d \in Nodes : Fail(d)
    \/ \E d1, d2 \in Nodes : FailTogether(d1, d2)
    \/ Commit
    \/ LoseTail
    \/ Scan
    \/ \E n \in Nodes : Rejoin(n)
    \/ \E p \in Partitions : Rebalance(p)

Spec == Init /\ [][Next]_vars /\ WF_vars(Commit) /\ WF_vars(Scan)

\* The primary is not L, the node with no copy.
InvPrimaryWasAHolder == \A p \in Partitions : applied[p].primary \in {A, B}

\* Once a failover is applied and no proposal is lost, a partition does not rest
\* on a departed node while its mapped replica is alive.
InvNoStrandedPrimary ==
    (proposals = <<>> /\ lostLeft = MaxLost) =>
        \A p \in Partitions :
            (Departed(applied[p].primary) /\ ~returned[p]) => applied[p].replica \notin members

\* Rebalance never moves a primary to a replica outside the members (gh #280).
InvRebalanceTargetLive == ~deadTarget

NothingToRepair ==
    /\ proposals = <<>>
    /\ \A p \in Partitions :
        /\ Departed(applied[p].primary) => applied[p].replica \notin members
        /\ ~Departed(applied[p].replica) \/ (KeepDeadReplica /\ Departed(applied[p].primary))

\* After failures stop, every repairable entry is repaired, whatever was lost.
Repaired == <>[](NothingToRepair \/ proposalsLeft < 2)

\* Every node the map named when both holders died: once one is back, p has a live primary.
ServedByAHolderBack ==
    \A p \in Partitions :
        (named[p] \cap members # {}) => applied[p].primary \in members

\* No manual rebalance is needed, in either order of the two deaths and either order of
\* return, within the scope in the header. This is about the map only, not acked writes.
HolderServes == <>[](ServedByAHolderBack \/ proposalsLeft < 2)

\* No node with no copy takes the role while its source is alive.
InvNoEmptyServe == takenEmpty # "source_alive"

\* No node with no copy takes the role at all.
InvNoEmptyTake == takenEmpty = "no"
=============================================================================
