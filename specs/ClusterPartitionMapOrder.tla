----------------------- MODULE ClusterPartitionMapOrder -----------------------
(***************************************************************************)
(* PARTITION MAP ORDER across nodes for one partition (gh #216).            *)
(*                                                                          *)
(* The Raft log holds the partition's assignments in commit order. An      *)
(* entry is named by its log position, so a higher position is a newer     *)
(* assignment. Each node applies the committed log in order to its Raft    *)
(* map (raft) and the node controller copies that map into its own        *)
(* (ctrl), which decides the node's role.                                  *)
(*                                                                          *)
(* VERIFIED FACTS MODELLED (Design = "current"):                            *)
(*  - every node broadcasts each update it applies from the log            *)
(*    (raft/coordinator/replication.rs apply_command)                       *)
(*  - a node that receives a copy applies it to its controller map         *)
(*    whatever it held, and passes it to its Raft map as an external       *)
(*    update (node_controller/mod.rs handle_partition_update_received,     *)
(*    raft_task.rs ExternalUpdate, coordinator apply_external_update)       *)
(*  - the controller copies the Raft map into its own whenever they        *)
(*    differ (cluster_agent/event_loop.rs handle_tick)                      *)
(*  - a restarted node applies its log again from the start, and           *)
(*    broadcasts every entry again                                          *)
(*  - copies travel on one ordered control stream per peer, so copies     *)
(*    from one sender arrive in order; copies from different senders       *)
(*    interleave in any order (quic_transport.rs peer_writer_task)         *)
(*  - a broadcast reaches only the peers connected at that moment and a    *)
(*    full queue drops the copy, so any copy can be lost                   *)
(*    (quic_transport.rs broadcast, send_to_peer)                           *)
(*                                                                          *)
(* Design = "fixed": nodes no longer broadcast or apply copies; the        *)
(* controller follows only its own Raft map.                               *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS MaxLog, MaxRestarts, MaxLoss, Design

Nodes == {1, 2, 3}
Broadcasts == Design = "current"

VARIABLES committed, applied, raft, ctrl, msgs, restarts, losses

vars == <<committed, applied, raft, ctrl, msgs, restarts, losses>>

TypeOK ==
    /\ committed \in 0..MaxLog
    /\ applied \in [Nodes -> 0..MaxLog]
    /\ raft \in [Nodes -> 0..MaxLog]
    /\ ctrl \in [Nodes -> 0..MaxLog]
    /\ \A f \in Nodes, t \in Nodes : \A i \in DOMAIN msgs[f][t] : msgs[f][t][i] \in 1..MaxLog
    /\ restarts \in 0..MaxRestarts
    /\ losses \in 0..MaxLoss

Init ==
    /\ committed = 0
    /\ applied = [n \in Nodes |-> 0]
    /\ raft = [n \in Nodes |-> 0]
    /\ ctrl = [n \in Nodes |-> 0]
    /\ msgs = [f \in Nodes |-> [t \in Nodes |-> <<>>]]
    /\ restarts = 0
    /\ losses = 0

Commit ==
    /\ committed < MaxLog
    /\ committed' = committed + 1
    /\ UNCHANGED <<applied, raft, ctrl, msgs, restarts, losses>>

Apply(n) ==
    /\ applied[n] < committed
    /\ applied' = [applied EXCEPT ![n] = @ + 1]
    /\ raft' = [raft EXCEPT ![n] = applied[n] + 1]
    /\ msgs' = IF Broadcasts
                 THEN [msgs EXCEPT ![n] = [t \in Nodes |->
                          IF t = n THEN @[t] ELSE Append(@[t], applied[n] + 1)]]
                 ELSE msgs
    /\ UNCHANGED <<committed, ctrl, restarts, losses>>

Receive(f, t) ==
    /\ msgs[f][t] # <<>>
    /\ ctrl' = [ctrl EXCEPT ![t] = Head(msgs[f][t])]
    /\ raft' = [raft EXCEPT ![t] = Head(msgs[f][t])]
    /\ msgs' = [msgs EXCEPT ![f][t] = Tail(@)]
    /\ UNCHANGED <<committed, applied, restarts, losses>>

Sync(n) ==
    /\ ctrl[n] # raft[n]
    /\ ctrl' = [ctrl EXCEPT ![n] = raft[n]]
    /\ UNCHANGED <<committed, applied, raft, msgs, restarts, losses>>

Restart(n) ==
    /\ restarts < MaxRestarts
    /\ restarts' = restarts + 1
    /\ applied' = [applied EXCEPT ![n] = 0]
    /\ raft' = [raft EXCEPT ![n] = 0]
    /\ ctrl' = [ctrl EXCEPT ![n] = 0]
    /\ msgs' = [f \in Nodes |-> [t \in Nodes |-> IF t = n THEN <<>> ELSE msgs[f][t]]]
    /\ UNCHANGED <<committed, losses>>

Lose(f, t) ==
    /\ losses < MaxLoss
    /\ msgs[f][t] # <<>>
    /\ losses' = losses + 1
    /\ msgs' = [msgs EXCEPT ![f][t] = Tail(@)]
    /\ UNCHANGED <<committed, applied, raft, ctrl, restarts>>

Next ==
    \/ Commit
    \/ \E n \in Nodes : Apply(n) \/ Sync(n) \/ Restart(n)
    \/ \E f, t \in Nodes : Receive(f, t) \/ Lose(f, t)

Spec == Init /\ [][Next]_vars

Settled ==
    /\ committed = MaxLog
    /\ \A f, t \in Nodes : msgs[f][t] = <<>>
    /\ \A n \in Nodes : applied[n] = committed /\ ctrl[n] = raft[n]

AgreeOnceSettled == Settled => \A n \in Nodes : ctrl[n] = committed

NoRewind == [][\A n \in Nodes : ctrl'[n] = 0 \/ ctrl'[n] >= ctrl[n]]_vars

=============================================================================
