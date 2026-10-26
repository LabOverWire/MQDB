------------------------- MODULE ClusterPartitionInit -------------------------
(***************************************************************************)
(* PARTITION MAP INITIALIZATION across Raft leader changes (gh #216).       *)
(*                                                                          *)
(* The committed Raft log holds "init" entries, which assign every         *)
(* partition from scratch at epoch 1, and "move" entries, which change     *)
(* assignments already made. A node applies the log in order; a new       *)
(* leader holds every committed entry but may not have applied them yet.  *)
(*                                                                          *)
(* VERIFIED FACTS MODELLED (Design = "current"):                            *)
(*  - the Raft task initializes when it is leader and its own flag is      *)
(*    unset; the flag starts unset on every node and is set only by        *)
(*    initializing (raft_task.rs handle_tick, initialize_partitions)        *)
(*  - on becoming leader, the coordinator also initializes if the map it  *)
(*    has applied assigns no partition (raft/coordinator/election.rs tick, *)
(*    replication.rs trigger_rebalance_internal)                            *)
(*                                                                          *)
(* Design = "oneflag": only the Raft task path, still guarded by its flag. *)
(* Design = "fixed": one path, taken only by a leader that has applied     *)
(* the whole log and finds no init entry in it.                            *)
(***************************************************************************)
EXTENDS Naturals, Sequences

CONSTANTS MaxLog, MaxElections, MaxRestarts, Design

Nodes == {1, 2, 3}
None == 0

VARIABLES log, applied, leader, flag, elections, restarts

vars == <<log, applied, leader, flag, elections, restarts>>

HasInit(s) == \E i \in DOMAIN s : s[i] = "init"
Inits == {i \in DOMAIN log : log[i] = "init"}
AppliedPrefix(n) == SubSeq(log, 1, applied[n])

TypeOK ==
    /\ log \in Seq({"init", "move"})
    /\ Len(log) <= MaxLog
    /\ applied \in [Nodes -> 0..MaxLog]
    /\ leader \in Nodes \cup {None}
    /\ flag \in [Nodes -> BOOLEAN]
    /\ elections \in 0..MaxElections
    /\ restarts \in 0..MaxRestarts

Init ==
    /\ log = <<>>
    /\ applied = [n \in Nodes |-> 0]
    /\ leader = None
    /\ flag = [n \in Nodes |-> FALSE]
    /\ elections = 0
    /\ restarts = 0

Propose(e) == log' = Append(log, e)

BecomeLeader(n) ==
    /\ elections < MaxElections
    /\ leader # n
    /\ elections' = elections + 1
    /\ leader' = n
    /\ IF Design = "current" /\ ~HasInit(AppliedPrefix(n)) /\ Len(log) < MaxLog
         THEN Propose("init")
         ELSE UNCHANGED log
    /\ UNCHANGED <<applied, flag, restarts>>

TaskInit(n) ==
    /\ leader = n
    /\ Len(log) < MaxLog
    /\ IF Design = "fixed"
         THEN /\ applied[n] = Len(log)
              /\ ~HasInit(log)
              /\ UNCHANGED flag
         ELSE /\ ~flag[n]
              /\ flag' = [flag EXCEPT ![n] = TRUE]
    /\ Propose("init")
    /\ UNCHANGED <<applied, leader, elections, restarts>>

Move(n) ==
    /\ leader = n
    /\ Len(log) < MaxLog
    /\ HasInit(AppliedPrefix(n))
    /\ Propose("move")
    /\ UNCHANGED <<applied, leader, flag, elections, restarts>>

Apply(n) ==
    /\ applied[n] < Len(log)
    /\ applied' = [applied EXCEPT ![n] = @ + 1]
    /\ UNCHANGED <<log, leader, flag, elections, restarts>>

Restart(n) ==
    /\ restarts < MaxRestarts
    /\ restarts' = restarts + 1
    /\ applied' = [applied EXCEPT ![n] = 0]
    /\ flag' = [flag EXCEPT ![n] = FALSE]
    /\ leader' = IF leader = n THEN None ELSE leader
    /\ UNCHANGED <<log, elections>>

Next ==
    \E n \in Nodes :
        \/ BecomeLeader(n)
        \/ TaskInit(n)
        \/ Move(n)
        \/ Apply(n)
        \/ Restart(n)

Spec == Init /\ [][Next]_vars

InitAtMostOnce == \A i, j \in Inits : i = j

=============================================================================
