----------------------- MODULE ClusterFailoverPlanning -----------------------
(***************************************************************************)
(* Two-partition model of how a Raft leader reassigns partitions when more *)
(* than one member is gone (gh #300 review, gh #280). The one-partition    *)
(* model in ClusterRestartRejoin cannot show this.                         *)
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
(*   Scan      scan_partition_map_for_departed_nodes: after each failover  *)
(*             batch, at a new leader's first ready tick when it can commit *)
(*             and deaths have settled, and on the 30 s periodic check     *)
(*             (election.rs). The model has no timing for any of these.    *)
(*   Rejoin    handle_node_alive, a member is back; see the restriction    *)
(*             below                                                       *)
(*   Rebalance rebalancer.rs redistribute_primaries: move a primary to     *)
(*             one of its replicas                                         *)
(*   Hear      the first heartbeat from an unheard member: handle_node_alive *)
(*             takes it out of unheard_members and asks for a rescan at the *)
(*             next settled tick that can commit; Scan stands for that too *)
(*                                                                         *)
(* Constants:                                                              *)
(*   PlanFromProposals  TRUE: a failover reads the applied map plus        *)
(*                      unapplied proposals. FALSE: the applied map. The   *)
(*                      main cfg gives the same verdict with it FALSE, at  *)
(*                      9016 states instead of 4440, so this model is not  *)
(*                      evidence for planned-map reads in general; unit    *)
(*                      tests cover those. _departed passes with it TRUE   *)
(*                      only at MaxLost 0 (795 states); at MaxLost 1 it     *)
(*                      fails at 29 states. LiveTargetsOnly is what        *)
(*                      protects.                                          *)
(*   HolderOnly         TRUE: promote only a live mapped replica, otherwise *)
(*                      leave the partition on its holder. FALSE: fall back *)
(*                      to the first alive node, which holds no copy.      *)
(*   LiveTargetsOnly    TRUE: rebalance only targets a live replica.       *)
(*   RepairScan         FALSE: Scan never runs.                            *)
(*   ScanReplicas       FALSE: Scan looks at primaries only.               *)
(*   AddReplicas        TRUE: L fills an empty slot or replaces a live     *)
(*                      replica, with no copy and no death needed, as the  *)
(*                      rebalancer does (rebalancer.rs:172, :217-296), and *)
(*                      can then be promoted. A dead replica cannot be     *)
(*                      replaced. The map does not record who synced.      *)
(*                      The dead-replica guard is inert in the shipped     *)
(*                      cfgs (same states without it).                     *)
(*                      replaced[p] stands for the replaced-replica source *)
(*                      (snapshot.rs:375-377). It differs from the code:   *)
(*                      the code records at map apply, keeps the first, and *)
(*                      drops it with the wait; here it is set at proposal, *)
(*                      keeps the last, is cleared only by LoseTail (no    *)
(*                      AddReplicas cfg has MaxLost above 0, so that is    *)
(*                      unreachable in shipped cfgs), and is always known: *)
(*                      a node whose first map is the move, or that        *)
(*                      restarted (from reading), has no memory of it and  *)
(*                      takes the role with the replaced replica alive, see *)
(*                      a_node_whose_first_map_is_the_move_takes_the_role_ *)
(*                      with_the_replaced_replica_alive.                   *)
(*   WaitForData        TRUE: TakeRole refuses a node mapped primary with  *)
(*                      no copy while its mapped replica or its replaced   *)
(*                      replica is alive, and while the map still names a  *)
(*                      replica: a dead mapped source alone does not end   *)
(*                      the wait, the map update does (gh #273). FALSE: it *)
(*                      can serve at once. Not modelled: the old primary   *)
(*                      and a kept source also count as live sources in the *)
(*                      code; the code takes the role inside the apply of  *)
(*                      (L,[]), where the model takes it in a later step,  *)
(*                      so source_dead can label a take with a holder back; *)
(*                      Dead is each node's own view in the code and one   *)
(*                      global view here; a take 30 s after an import with *)
(*                      no finished catchup; and a take after 90 s on a    *)
(*                      source never heard from.                           *)
(*                      The replica = None conjunct removes 279 states on  *)
(*                      _new_replica_wait with no change of verdict.       *)
(*   Together           TRUE: two nodes can die within the window.         *)
(*   BatchDeaths        TRUE: deaths in the window are all out of the alive *)
(*                      set before any is processed. FALSE: the first is   *)
(*                      processed while the second still counts as alive.  *)
(*   KeepDeadReplica    TRUE: a dead replica entry stays while the primary *)
(*                      is also gone. FALSE: it is always removed.         *)
(*   RestoredUnheard    TRUE: B starts as a member not heard from. It      *)
(*                      stays unheard until Hear, or Fail, which here      *)
(*                      stands for the verdict on a node never heard from, *)
(*                      30 s in the code. A node already heard gets 15 s;  *)
(*                      the model has no timing. The spec assumes one of   *)
(*                      them comes: the                                    *)
(*                      fairness on Hear or Fail. Without it Repaired      *)
(*                      fails at 7432 states. That form is closer to the   *)
(*                      code and changes no result: fairness on Hear alone *)
(*                      gives the same 7432 states and 17410 transitions.  *)
(*                      In the code that verdict covers restored voters    *)
(*                      (init.rs) and a member a snapshot adds or lists    *)
(*                      again (forward_members_to_expect, expect_again);   *)
(*                      that path is shown by tests in raft_task.rs, not   *)
(*                      here. The model has no votes. Only B starts        *)
(*                      unheard. Symmetry between A and B is not exact:    *)
(*                      with {A} unheard, run by hand, the same properties *)
(*                      pass at 7454 states against 7432. With {A, B}      *)
(*                      unheard they pass at 8269. No cfg ships either.    *)
(*   SkipUnheard        TRUE: failover and scan never target an unheard    *)
(*                      node (live_targets), and rebalance does not when   *)
(*                      LiveTargetsOnly is also on. FALSE: they can, which *)
(*                      is the defect found on 2b5c1a6.                    *)
(*   MaxFails, MaxLost  budgets of failures and of lost tails.             *)
(*   MaxProposals       budget of proposals across the whole behaviour, to *)
(*                      keep the state space finite. Repaired and          *)
(*                      NamedNodeBackGivesLivePrimary each have the escape *)
(*                      proposalsLeft < 2, so a behaviour that spends the  *)
(*                      budget satisfies them without repairing anything.  *)
(*                                                                         *)
(* Variant cfgs may flip several flags at once; a cfg shows a single guard *)
(* only where the Expected list says so. This checker, unlike TLC, checks  *)
(* every Inv* operator by name when a cfg lists no INVARIANTS: to see a    *)
(* property alone, list one invariant that passes, such as                 *)
(* InvRebalanceTargetLive.                                                 *)
(*                                                                         *)
(* Abstractions: one leader identity, L, which stands for a live majority: *)
(* Commit, Scan and Fail run with L alone, and loss of a majority is not   *)
(* covered. Proposals are applied in order. InvPrimaryWasAHolder means     *)
(* "the primary is not L", and holds structurally with HolderOnly on and   *)
(* AddReplicas off. A and B are assumed to keep their copies, and in       *)
(* this model only L can lack one; the code has other ways to lack one     *)
(* (#287, a wiped disk). ClusterEmptyPrimary and ClusterPromotedTransfer   *)
(* model the transfer itself. The time a detection takes is not modelled.  *)
(*                                                                         *)
(* Expected, verified for these constants only:                            *)
(*   ClusterFailoverPlanning: all pass                                     *)
(*   _no_holder_rule: fails InvPrimaryWasAHolder                           *)
(*   _stale: passes. It turns RepairScan and the planned-map reads off and *)
(*       checks InvNoStrandedPrimary only, with no lost proposals. Planning *)
(*       matters for epochs, which this model lacks; see the unit tests    *)
(*   _departed: fails InvRebalanceTargetLive, the gh #280 panic            *)
(*   _new_replica: fails InvPrimaryWasAHolder: the map can still name a    *)
(*       node with no copy.                                                *)
(*   _new_replica_nowait: WaitForData off and MaxFails 1, where _wait has  *)
(*       2, fails InvNoEmptyServe.                                         *)
(*   _new_replica_wait: passes InvNoEmptyServe, which restates the guard.  *)
(*   _new_replica_no_source: fails InvNoEmptyTake: L replaces live B, then *)
(*       A and B die, so L is promoted with no live or replaced source and *)
(*       takes the role empty. A wait followed by the source dying is not  *)
(*       separately shown.                                                 *)
(*   _no_scan: fails Repaired: with one leader identity a lost failover    *)
(*       is not redone. In the code a different leader redoes it from its  *)
(*       own queued death; a re-elected node depends on the scan, and a    *)
(*       restarted node with an empty queue may too (not tested).          *)
(*   _no_scan_live_primary: RepairScan off, fails                          *)
(*       NamedNodeBackGivesLivePrimary: the scan is what promotes a        *)
(*       returning replica.                                                *)
(*   _scan_primaries_only: fails Repaired, a lost replica entry stays      *)
(*   _no_batch_no_keep: BatchDeaths and KeepDeadReplica off, fails         *)
(*       NamedNodeBackGivesLivePrimary at the same state count as _no_batch *)
(*       at MaxLost 0. That equality does not hold at MaxLost 1. This is   *)
(*       not main: main also has HolderOnly, PlanFromProposals and         *)
(*       LiveTargetsOnly off.                                              *)
(*   _live_primary_fails3: MaxFails 3, fails                               *)
(*       NamedNodeBackGivesLivePrimary; see below.                          *)
(*   _no_batch: fails NamedNodeBackGivesLivePrimary, promoted to a node     *)
(*       dying in the window                                                *)
(*   _no_keep: fails NamedNodeBackGivesLivePrimary, the replica returns     *)
(*       first with no entry                                                *)
(*   _restored_unheard: passes with B unheard at the start and every        *)
(*       property checked, InvNoPromotionOfUnheard included. Off that cfg:  *)
(*       BatchDeaths or KeepDeadReplica off fails                           *)
(*       NamedNodeBackGivesLivePrimary, but those traces take B out of      *)
(*       unheard in the first step and repeat _no_batch and _no_keep, so    *)
(*       they add nothing about an unheard voter. RepairScan off fails      *)
(*       Repaired and does use the unheard path, but Scan stands for both   *)
(*       the rescan on Hear and the periodic check, so it cannot separate   *)
(*       them. This shows the map only, with no timing.                     *)
(*   _promotes_unheard: SkipUnheard off, fails InvNoPromotionOfUnheard: A   *)
(*       dies and B is proposed primary with no heartbeat from it. That     *)
(*       invariant restates the guard: see below.                           *)
(*                                                                         *)
(* This model has no writes, so it says nothing about who holds an acked   *)
(* write. Replication is asynchronous: a primary acks before its replica   *)
(* applies, so holders dying together can differ, and promoting either can *)
(* lose acked writes. NamedNodeBackGivesLivePrimary only says a partition  *)
(* whose map entries named a node when both holders died has a live mapped *)
(* primary once that node is back. It does not say that primary holds the  *)
(* partition: the main cfg with HolderOnly FALSE and InvPrimaryWasAHolder *)
(* dropped from the check passes it, by promoting L. The holder rule is    *)
(* needed only for InvPrimaryWasAHolder.                                   *)
(* An earlier version tracked current[p] and InvServingPrimaryIsCurrent; a *)
(* review refuted them as a measure of acked writes and they are gone.     *)
(*                                                                         *)
(* Scope of NamedNodeBackGivesLivePrimary: two holders of an RF=2 partition *)
(* dying together as the first failure, with a live majority. It holds only *)
(* because MaxFails 2 makes FailTogether the only failure on that path, and *)
(* named never clears. A further failure breaks it (_live_primary_fails3): *)
(* a holder that returns and dies again strands the partition on it. A     *)
(* second death after the window is also outside it: the first holder has  *)
(* left the map, and the partition stays at 503 until the still-mapped     *)
(* holder returns or a forced rebalance.                                   *)
(*                                                                         *)
(* Model restriction: Rejoin requires proposals = <<>>, so a node does not *)
(* return while a proposal is unapplied. The code has no such guard: there *)
(* a partition can rest on a departed primary with its replica back until  *)
(* the next scan. With InvNoStrandedPrimary checked alone and the guard    *)
(* dropped, it fails on the main cfg, _stale, _restored_unheard,           *)
(* _promotes_unheard, _live_primary_fails3 and _no_scan_live_primary; it   *)
(* passes on the other _no_* and _new_replica cfgs, so it depends on the   *)
(* guard only for those six. As shipped no cfg fails it, but _departed     *)
(* does not list it; checked alone there, it fails at 194 states even with *)
(* the guard. Hear has no such guard: it marks the planned map instead.    *)
(*                                                                         *)
(* InvRebalanceTargetLive, InvNoEmptyServe and InvNoPromotionOfUnheard     *)
(* restate guards, so their passes add no evidence; only the cfgs that     *)
(* turn the guard off show anything. InvNoPromotionOfUnheard shows no cost *)
(* of promoting an unheard node: with SkipUnheard off and only this        *)
(* invariant removed from _restored_unheard, everything else passes (6850  *)
(* states). It does not                                                    *)
(* see Rebalance either: with LiveTargetsOnly off and it alone checked, it *)
(* passes. The evidence for not targeting an unheard node is the unit      *)
(* tests. InvNoStrandedPrimary gives none on the unheard path: it treats   *)
(* an unheard replica as not there, so it says nothing about one, and it   *)
(* fails at 3 states if that exclusion is removed. LiveTargetsOnly is what *)
(* protects InvRebalanceTargetLive. At                                     *)
(* _departed's other settings, MaxLost 0, it also takes PlanFromProposals  *)
(* off for a failure to appear; at MaxLost 1 it fails either way.          *)
(*                                                                         *)
(* Not covered: data, acked writes, what a 503 answers, repeat failures,   *)
(* loss of a majority, timing, and more than two partitions or holders.    *)
(* L never dies. takenEmpty is one-shot and global across both partitions, *)
(* so the model stops looking at takes after the first. Transfer can fire  *)
(* after a take, which the code does not rely on. An unheard voter comes   *)
(* only from restart here; in the code a member a snapshot from the leader *)
(* first names, or lists again after its verdict, is unheard too.          *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS L, A, B,
          PlanFromProposals, HolderOnly, LiveTargetsOnly, RepairScan, ScanReplicas, AddReplicas,
          Together, BatchDeaths, KeepDeadReplica, WaitForData, RestoredUnheard, SkipUnheard,
          MaxFails, MaxProposals, MaxLost

VARIABLES applied, proposals, members, failsLeft, proposalsLeft, lostLeft, deadTarget, named, returned,
          filled, takenEmpty, replaced, unheard, promotedUnheard

vars == <<applied, proposals, members, failsLeft, proposalsLeft, lostLeft, deadTarget, named, returned,
          filled, takenEmpty, replaced, unheard, promotedUnheard>>

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
    /\ replaced = [p \in Partitions |-> None]
    /\ unheard = IF RestoredUnheard THEN {B} ELSE {}
    /\ promotedUnheard = FALSE

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

\* A promotion target: a member, except one not heard from yet.
Targets(nodes) == IF SkipUnheard THEN nodes \ unheard ELSE nodes

\* Whether a batch of repairs promotes a node not heard from.
PromotesUnheard(map, repairs) ==
    \E r \in repairs : r.primary # map[r.p].primary /\ r.primary \in unheard

Fail(d) ==
    /\ d \in members
    /\ d # L
    /\ failsLeft > 0
    /\ failsLeft' = failsLeft - 1
    /\ LET alive == Targets(members \ {d})
           repairs == Repairs(Base, d, alive)
       IN /\ Cardinality(repairs) <= proposalsLeft
          /\ proposalsLeft' = proposalsLeft - Cardinality(repairs)
          /\ proposals' = proposals \o SetToSeq(repairs)
          /\ members' = members \ {d}
          /\ promotedUnheard' = (promotedUnheard \/ PromotesUnheard(Base, repairs))
    /\ unheard' = unheard \ {d}
    /\ UNCHANGED <<applied, lostLeft, deadTarget, named, returned, filled, takenEmpty, replaced>>

\* Both deaths reach the task within the window, d1 first.
FailTogether(d1, d2) ==
    /\ Together
    /\ d1 # d2
    /\ {d1, d2} \subseteq members
    /\ L \notin {d1, d2}
    /\ failsLeft >= 2
    /\ failsLeft' = failsLeft - 2
    /\ LET alive == Targets(members \ {d1, d2})
           first == Repairs(Base, d1, IF BatchDeaths THEN alive ELSE Targets(members \ {d1}))
           afterFirst == IF PlanFromProposals THEN Replay(Base, SetToSeq(first)) ELSE Base
           second == Repairs(afterFirst, d2, alive)
           count == Cardinality(first) + Cardinality(second)
       IN /\ count <= proposalsLeft
          /\ proposalsLeft' = proposalsLeft - count
          /\ proposals' = proposals \o SetToSeq(first) \o SetToSeq(second)
          /\ members' = members \ {d1, d2}
          /\ promotedUnheard' = (promotedUnheard \/ PromotesUnheard(Base, first) \/ PromotesUnheard(afterFirst, second))
    /\ unheard' = unheard \ {d1, d2}
    /\ named' = [p \in Partitions |->
                    IF {Base[p].primary, Base[p].replica} = {d1, d2} THEN {d1, d2} ELSE named[p]]
    /\ UNCHANGED <<applied, lostLeft, deadTarget, returned, filled, takenEmpty, replaced>>

Commit ==
    /\ proposals # <<>>
    /\ applied' = Apply(applied, Head(proposals))
    /\ proposals' = Tail(proposals)
    /\ returned' = [p \in Partitions |->
                       returned[p] /\ applied'[p].primary \notin members /\ applied'[p].replica \in members]
    /\ UNCHANGED <<members, failsLeft, proposalsLeft, lostLeft, deadTarget, named, filled, takenEmpty, replaced, unheard, promotedUnheard>>

LoseTail ==
    /\ proposals # <<>>
    /\ lostLeft > 0
    /\ lostLeft' = lostLeft - 1
    /\ proposals' = <<>>
    /\ replaced' = [p \in Partitions |-> None]
    /\ UNCHANGED <<applied, members, failsLeft, proposalsLeft, deadTarget, named, returned, filled, takenEmpty, unheard, promotedUnheard>>

Departed(n) == n \in Nodes /\ n \notin members

ScanTargets ==
    {n \in Nodes : Departed(n) /\
        \E p \in Partitions :
            Planned[p].primary = n \/ (ScanReplicas /\ Planned[p].replica = n)}

ScanRepairs ==
    {r \in UNION {Repairs(Planned, n, Targets(members)) : n \in ScanTargets} :
        ScanReplicas \/ Planned[r.p].primary \in ScanTargets}

Scan ==
    /\ RepairScan
    /\ proposals = <<>>
    /\ ScanRepairs # {}
    /\ Cardinality(ScanRepairs) <= proposalsLeft
    /\ proposalsLeft' = proposalsLeft - Cardinality(ScanRepairs)
    /\ proposals' = SetToSeq(ScanRepairs)
    /\ promotedUnheard' = (promotedUnheard \/ PromotesUnheard(Planned, ScanRepairs))
    /\ UNCHANGED <<applied, members, failsLeft, lostLeft, deadTarget, named, returned, filled, takenEmpty, replaced, unheard>>

Rejoin(n) ==
    /\ Departed(n)
    /\ proposals = <<>>
    /\ members' = members \cup {n}
    /\ returned' = [p \in Partitions |->
                       returned[p] \/ (applied[p].replica = n /\ applied[p].primary \notin members)]
    /\ UNCHANGED <<applied, proposals, failsLeft, proposalsLeft, lostLeft, deadTarget, named, filled, takenEmpty, replaced, unheard, promotedUnheard>>

Rebalance(p) ==
    /\ proposalsLeft > 0
    /\ proposalsLeft' = proposalsLeft - 1
    /\ LET a == Base[p]
       IN /\ a.replica # None
          /\ a.primary \in members
          /\ (LiveTargetsOnly => a.replica \in Targets(members))
          /\ deadTarget' = (deadTarget \/ a.replica \notin members)
          /\ proposals' = Append(proposals, [p |-> p, primary |-> a.replica, replica |-> a.primary])
    /\ UNCHANGED <<applied, members, failsLeft, lostLeft, named, returned, filled, takenEmpty, replaced, unheard, promotedUnheard>>

\* The rebalancer gives a partition a new replica with no copy yet. L plays that node.
\* _new_replica fails on the promotion that follows, with or without a failure first, so
\* the 30 s hold after a death in the code, which is not modelled, would not stop it.
AddReplica(p) ==
    /\ AddReplicas
    /\ proposalsLeft > 0
    /\ proposalsLeft' = proposalsLeft - 1
    /\ Base[p].replica # L
    /\ Base[p].replica = None \/ Base[p].replica \in members
    /\ Base[p].primary \in members
    /\ Base[p].primary # L
    /\ proposals' = Append(proposals, [p |-> p, primary |-> Base[p].primary, replica |-> L])
    /\ replaced' = IF Base[p].replica = None THEN replaced ELSE [replaced EXCEPT ![p] = Base[p].replica]
    /\ UNCHANGED <<applied, members, failsLeft, lostLeft, deadTarget, named, returned, filled, takenEmpty, unheard, promotedUnheard>>

\* The snapshot for a node with no copy arrives from a live mapped holder. Only a node
\* mapped primary, waiting to take the role, asks its replaced replica.
Transfer(p) ==
    /\ AddReplicas
    /\ ~filled[p]
    /\ L \in {applied[p].primary, applied[p].replica}
    /\ (({applied[p].primary, applied[p].replica}
         \cup IF applied[p].primary = L THEN {replaced[p]} ELSE {}) \ {L}) \cap members # {}
    /\ filled' = [filled EXCEPT ![p] = TRUE]
    /\ UNCHANGED <<applied, proposals, members, failsLeft, proposalsLeft, lostLeft, deadTarget, named, returned, takenEmpty, replaced, unheard, promotedUnheard>>

\* A node mapped primary with no copy takes the role. takenEmpty records whether a source
\* was alive at that moment. With WaitForData it needs the map to name no replica: a dead
\* mapped source alone does not end the wait.
TakeRole(p) ==
    /\ AddReplicas
    /\ takenEmpty = "no"
    /\ applied[p].primary = L
    /\ ~filled[p]
    /\ LET sourceLive == ({applied[p].replica, replaced[p]} \ {L}) \cap members # {}
       IN /\ (WaitForData => (~sourceLive /\ applied[p].replica = None))
          /\ takenEmpty' = IF sourceLive THEN "source_alive" ELSE "source_dead"
    /\ UNCHANGED <<applied, proposals, members, failsLeft, proposalsLeft, lostLeft, deadTarget, named, returned, filled, replaced, unheard, promotedUnheard>>

\* A member's first heartbeat. The code rescans at the next settled tick.
Hear(n) ==
    /\ n \in unheard
    /\ n \in members
    /\ unheard' = unheard \ {n}
    /\ returned' = [p \in Partitions |->
                       returned[p] \/ (Planned[p].replica = n /\ Planned[p].primary \notin members)]
    /\ UNCHANGED <<applied, proposals, members, failsLeft, proposalsLeft, lostLeft, deadTarget, named, filled, takenEmpty, replaced, promotedUnheard>>

Next ==
    \/ \E n \in Nodes : Hear(n)
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

Spec == Init /\ [][Next]_vars /\ WF_vars(Commit) /\ WF_vars(Scan) /\ WF_vars(\E n \in unheard : Hear(n) \/ Fail(n))

\* The primary is not L, the node with no copy.
InvPrimaryWasAHolder == \A p \in Partitions : applied[p].primary \in {A, B}

\* Once a failover is applied and no proposal is lost, a partition does not rest on a
\* departed node while its mapped replica is a member heard from, except a replica that
\* returned or was first heard since.
\* This holds only under the Rejoin restriction in the header; the code can reach that
\* state until the next scan. No shipped cfg fails it.
InvNoStrandedPrimary ==
    (proposals = <<>> /\ lostLeft = MaxLost) =>
        \A p \in Partitions :
            (Departed(applied[p].primary) /\ ~returned[p]) => applied[p].replica \notin (members \ unheard)

\* Rebalance never moves a primary to a replica outside the members (gh #280).
InvRebalanceTargetLive == ~deadTarget

NothingToRepair ==
    /\ proposals = <<>>
    /\ \A p \in Partitions :
        /\ Departed(applied[p].primary) => applied[p].replica \notin (members \ unheard)
        /\ ~Departed(applied[p].replica) \/ (KeepDeadReplica /\ Departed(applied[p].primary))

\* After failures stop, every repairable entry is repaired, whatever was lost.
Repaired == <>[](NothingToRepair \/ proposalsLeft < 2)

\* Once a node the map named when both holders died is back, p has a live mapped primary.
\* That primary need not hold anything: this is true with HolderOnly FALSE.
LivePrimaryIfNamedNodeBack ==
    \A p \in Partitions :
        (named[p] \cap members # {}) => applied[p].primary \in members

\* No manual rebalance is needed, in either order of the two deaths and either order of
\* return, within the scope in the header. This is about the map only, not acked writes.
NamedNodeBackGivesLivePrimary == <>[](LivePrimaryIfNamedNodeBack \/ proposalsLeft < 2)

\* No failover or scan promotes a member not heard from.
InvNoPromotionOfUnheard == ~promotedUnheard

\* No node with no copy takes the role while a source is alive. This restates the guard.
InvNoEmptyServe == takenEmpty # "source_alive"

\* No node with no copy takes the role at all.
InvNoEmptyTake == takenEmpty = "no"
=============================================================================
