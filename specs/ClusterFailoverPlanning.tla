----------------------- MODULE ClusterFailoverPlanning -----------------------
(***************************************************************************)
(* Two-partition model of how a Raft leader reassigns partitions when more *)
(* than one member is gone (gh #300 review, gh #280). The one-partition     *)
(* model in ClusterRestartRejoin cannot show this.                          *)
(*                                                                         *)
(* Code each action abstracts (crates/mqdb-cluster/src/cluster):           *)
(*   Fail      raft/coordinator/replication.rs                             *)
(*             reassign_partitions_for_dead_node                           *)
(*   Commit    the proposals reach the applied partition map               *)
(*   LoseTail  leadership changes and the unapplied proposals are          *)
(*             overwritten; the dead nodes stay out of the members         *)
(*   Scan      scan_partition_map_for_dead_primaries, at leader start and  *)
(*             every 30 s                                                  *)
(*   Rejoin    handle_node_alive, a member is back                         *)
(*   Rebalance rebalancer.rs redistribute_primaries: move a primary to     *)
(*             one of its replicas                                         *)
(*                                                                         *)
(* Constants:                                                              *)
(*   PlanFromProposals  TRUE: a failover reads the applied map plus         *)
(*                      unapplied proposals. FALSE: the applied map.        *)
(*   HolderOnly         TRUE: promote only a live mapped replica, otherwise *)
(*                      leave the partition on its holder. FALSE: fall back *)
(*                      to the first alive node, which holds no copy.       *)
(*   LiveTargetsOnly    TRUE: rebalance only targets a live replica.        *)
(*   RepairScan         FALSE: Scan never runs.                             *)
(*   ScanReplicas       FALSE: Scan looks at primaries only.                *)
(*                                                                         *)
(* Abstractions: one leader identity; proposals applied in order; A and B   *)
(* are the only nodes holding either partition, so InvPrimaryWasAHolder     *)
(* reads as "never promote L". A holder is assumed to keep its copy, so     *)
(* gh #273 and #287 are out of scope. Replica addition is not modelled. The *)
(* time a detection takes is not modelled.                                  *)
(*                                                                         *)
(* Expected, verified for these constants only:                             *)
(*   ClusterFailoverPlanning: all pass                                      *)
(*   _no_holder_rule: fails InvPrimaryWasAHolder                            *)
(*   _stale: passes; with the holder rule, reading the applied map breaks   *)
(*       none of these invariants. The planning matters for epochs, which   *)
(*       this model lacks, and is covered by unit tests                     *)
(*   _departed: fails InvRebalanceTargetLive, the gh #280 panic             *)
(*   _no_scan: fails Repaired, a lost failover is never redone              *)
(*   _scan_primaries_only: fails Repaired, a lost replica entry stays       *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS L, A, B,
          PlanFromProposals, HolderOnly, LiveTargetsOnly, RepairScan, ScanReplicas,
          MaxFails, MaxProposals, MaxLost

VARIABLES applied, proposals, members, failsLeft, proposalsLeft, lostLeft, deadTarget

vars == <<applied, proposals, members, failsLeft, proposalsLeft, lostLeft, deadTarget>>

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

\* The repair one departed node d needs on partition p, or None.
Repair(map, p, d, alive) ==
    LET a == map[p]
    IN CASE a.primary = d /\ a.replica \in alive ->
                [p |-> p, primary |-> a.replica, replica |-> None]
         [] a.primary = d /\ HolderOnly -> None
         [] a.primary = d ->
                [p |-> p, primary |-> L, replica |-> None]
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
    /\ UNCHANGED <<applied, lostLeft, deadTarget>>

Commit ==
    /\ proposals # <<>>
    /\ applied' = Apply(applied, Head(proposals))
    /\ proposals' = Tail(proposals)
    /\ UNCHANGED <<members, failsLeft, proposalsLeft, lostLeft, deadTarget>>

LoseTail ==
    /\ proposals # <<>>
    /\ lostLeft > 0
    /\ lostLeft' = lostLeft - 1
    /\ proposals' = <<>>
    /\ UNCHANGED <<applied, members, failsLeft, proposalsLeft, deadTarget>>

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
    /\ UNCHANGED <<applied, members, failsLeft, lostLeft, deadTarget>>

Rejoin(n) ==
    /\ Departed(n)
    /\ proposals = <<>>
    /\ members' = members \cup {n}
    /\ UNCHANGED <<applied, proposals, failsLeft, proposalsLeft, lostLeft, deadTarget>>

Rebalance(p) ==
    /\ proposalsLeft > 0
    /\ proposalsLeft' = proposalsLeft - 1
    /\ LET a == Base[p]
       IN /\ a.replica # None
          /\ a.primary \in members
          /\ (LiveTargetsOnly => a.replica \in members)
          /\ deadTarget' = (deadTarget \/ a.replica \notin members)
          /\ proposals' = Append(proposals, [p |-> p, primary |-> a.replica, replica |-> a.primary])
    /\ UNCHANGED <<applied, members, failsLeft, lostLeft>>

Next ==
    \/ \E d \in Nodes : Fail(d)
    \/ Commit
    \/ LoseTail
    \/ Scan
    \/ \E n \in Nodes : Rejoin(n)
    \/ \E p \in Partitions : Rebalance(p)

Spec == Init /\ [][Next]_vars /\ WF_vars(Commit) /\ WF_vars(Scan)

\* A partition is only ever served by a node that holds its records.
InvPrimaryWasAHolder == \A p \in Partitions : applied[p].primary \in {A, B}

\* Once a failover is applied and no proposal is lost, a partition does not rest
\* on a departed node while its mapped replica is alive.
InvNoStrandedPrimary ==
    (proposals = <<>> /\ lostLeft = MaxLost) =>
        \A p \in Partitions :
            Departed(applied[p].primary) => applied[p].replica \notin members

\* Rebalance never moves a primary to a replica outside the members (gh #280).
InvRebalanceTargetLive == ~deadTarget

NothingToRepair ==
    /\ proposals = <<>>
    /\ \A p \in Partitions :
        /\ Departed(applied[p].primary) => applied[p].replica \notin members
        /\ ~Departed(applied[p].replica)

\* After failures stop, every repairable entry is repaired, whatever was lost.
Repaired == <>[](NothingToRepair \/ proposalsLeft < 2)
=============================================================================
