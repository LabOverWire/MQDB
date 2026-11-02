----------------------- MODULE ClusterFailoverPlanning -----------------------
(***************************************************************************)
(* Two-partition model of how a Raft leader reassigns partitions when more *)
(* than one member is gone (gh #300 review, gh #280). The one-partition     *)
(* model in ClusterRestartRejoin cannot show this.                          *)
(*                                                                         *)
(* Code each action abstracts (crates/mqdb-cluster/src/cluster):           *)
(*   Fail      raft/coordinator/replication.rs                             *)
(*             reassign_partitions_for_dead_node, one proposal per         *)
(*             partition the dead node holds                               *)
(*   Commit    the proposals reach the applied partition map               *)
(*   Rejoin    handle_node_alive, a member is back                         *)
(*   Rebalance rebalancer.rs redistribute_primaries: move a primary to     *)
(*             one of its replicas                                         *)
(*                                                                         *)
(* Constants:                                                              *)
(*   PlanFromProposals  TRUE: each failover reads the applied map plus      *)
(*                      proposals not yet applied. FALSE: the applied map.  *)
(*   LiveTargetsOnly    TRUE: rebalance only targets a replica that is a    *)
(*                      member. FALSE: as rebalancer.rs was (gh #280).      *)
(*   RepairScan         TRUE: a failover also repairs any partition on a    *)
(*                      departed node. FALSE: as the scan was, skipping a   *)
(*                      node already processed.                             *)
(*                                                                         *)
(* Abstractions: Raft is one leader that stays up, with proposals applied   *)
(* in order; there is no data, so promotion without data (gh #273) is out   *)
(* of scope. A detection is one Fail action; the time it takes is not       *)
(* modelled, so this says nothing about how long to wait before failing a  *)
(* member over, only about what the failover computes.                      *)
(*                                                                         *)
(* Expected, verified for these constants only:                             *)
(*   ClusterFailoverPlanning: all pass                                      *)
(*   _stale: fails InvNoDeadPrimary, failovers read the applied map         *)
(*   _stale_scan: fails InvNoDeadPrimary, a scan reading the applied map    *)
(*       does not rescue a failover reading it                              *)
(*   _departed: fails InvRebalanceTargetLive, the gh #280 panic             *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS L, A, B,
          PlanFromProposals, LiveTargetsOnly, RepairScan, MaxFails, MaxProposals

VARIABLES applied, proposals, members, failed, failsLeft, proposalsLeft, deadTarget

vars == <<applied, proposals, members, failed, failsLeft, proposalsLeft, deadTarget>>

Nodes == {L, A, B}
Leader == L
Partitions == {1, 2}
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

None == "none"

Init ==
    /\ applied = InitialMap
    /\ proposals = <<>>
    /\ members = Nodes
    /\ failed = {}
    /\ failsLeft = MaxFails
    /\ proposalsLeft = MaxProposals
    /\ deadTarget = FALSE

\* The proposals one failover of d produces, read from Base as the code does.
Reassign(p, d, alive) ==
    LET a == Base[p]
    IN CASE a.primary = d /\ a.replica \in alive ->
                [p |-> p, primary |-> a.replica, replica |-> None]
         [] a.primary = d ->
                [p |-> p, primary |-> CHOOSE n \in alive : TRUE, replica |-> None]
         [] a.replica = d ->
                [p |-> p, primary |-> a.primary, replica |-> None]
         [] OTHER -> None

Fail(d) ==
    /\ d \in members
    /\ d # Leader
    /\ failsLeft > 0
    /\ failsLeft' = failsLeft - 1
    /\ LET alive == members \ {d}
           touched == {Reassign(p, d, alive) : p \in Partitions} \ {None}
           onDeparted == IF RepairScan
                         THEN {[p |-> p, primary |-> CHOOSE n \in alive : TRUE, replica |-> None] :
                               p \in {q \in Partitions :
                                        Base[q].primary \notin alive /\ Base[q].primary # d}}
                         ELSE {}
           byPartition == touched \cup onDeparted
           one == {CHOOSE x \in byPartition : x.p = p : p \in {y.p : y \in byPartition}}
       IN /\ Cardinality(one) <= proposalsLeft
          /\ proposalsLeft' = proposalsLeft - Cardinality(one)
          /\ proposals' = proposals \o SetToSeq(one)
    /\ members' = members \ {d}
    /\ failed' = failed \cup {d}
    /\ UNCHANGED <<applied, deadTarget>>

Commit ==
    /\ proposals # <<>>
    /\ applied' = Apply(applied, Head(proposals))
    /\ proposals' = Tail(proposals)
    /\ UNCHANGED <<members, failed, failsLeft, proposalsLeft, deadTarget>>

Rejoin(n) ==
    /\ n \in failed
    /\ n \notin members
    /\ members' = members \cup {n}
    /\ failed' = failed \ {n}
    /\ UNCHANGED <<applied, proposals, failsLeft, proposalsLeft, deadTarget>>

Rebalance(p) ==
    /\ proposalsLeft > 0
    /\ proposalsLeft' = proposalsLeft - 1
    /\ LET a == Base[p]
       IN /\ a.replica # None
          /\ a.primary # a.replica
          /\ (LiveTargetsOnly => a.replica \in members)
          /\ deadTarget' = (deadTarget \/ a.replica \notin members)
          /\ proposals' = Append(proposals, [p |-> p, primary |-> a.replica, replica |-> a.primary])
    /\ UNCHANGED <<applied, members, failed, failsLeft>>

Next ==
    \/ \E d \in Nodes : Fail(d)
    \/ Commit
    \/ \E n \in Nodes : Rejoin(n)
    \/ \E p \in Partitions : Rebalance(p)

Spec == Init /\ [][Next]_vars

Settled == proposals = <<>>

\* Once every proposal is applied, no partition rests on a node that is gone.
InvNoDeadPrimary ==
    (Settled /\ failed # {}) => \A p \in Partitions : applied[p].primary \notin failed

\* Rebalance never moves a primary to a replica outside the members (gh #280).
InvRebalanceTargetLive == ~deadTarget
=============================================================================
