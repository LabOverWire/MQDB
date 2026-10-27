------------------------- MODULE ClusterForwardedWrite -------------------------
(***************************************************************************)
(* FORWARDED INTERNAL WRITES across partition reassignments (gh #221).     *)
(*                                                                          *)
(* The Raft log assigns one partition's primary, epoch by epoch. Each node *)
(* applies the log on its own tick, so at any moment nodes can be at       *)
(* different epochs. A node that issues an internal write (session, mqtt   *)
(* subscription) applies it when its map names itself as primary, and      *)
(* otherwise sends it, without waiting for a reply, to the node its map    *)
(* names.                                                                   *)
(*                                                                          *)
(* VERIFIED FACTS MODELLED (Design = "current"):                            *)
(*  - write_or_forward_impl forwards to partition_map.primary with no ack  *)
(*    (replication_ops.rs, change_events.rs forward_write_with_change_event)*)
(*  - handle_write_request drops a write when the receiver's map does not  *)
(*    name it primary ("received write request but not primary")           *)
(*  - the event loop applies the Raft map only on its tick                 *)
(*    (event_loop.rs handle_tick, apply_raft_partition_map)                *)
(*                                                                          *)
(* Design = "fixed": the forwarder stamps the epoch of its map on the      *)
(* write. A receiver that is not primary holds the write while its own     *)
(* epoch is older than the stamp, and otherwise forwards it to the node    *)
(* its map names, stamped with its own epoch. A write whose stamp equals   *)
(* the receiver's epoch is never received by a node that is not primary:   *)
(* the CASE has no arm for it, so TLC reports an error if it happens.      *)
(*                                                                          *)
(* NoLoss: once every write is issued, every node has applied the whole    *)
(* log, nothing is in flight and nothing is held, every write is stored.   *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Assign, Writes, Design

Nodes == {1, 2, 3}
None == 0
MaxEpoch == Len(Assign)

VARIABLES applied, issued, msgs, held, stored

vars == <<applied, issued, msgs, held, stored>>

Quiescent ==
    /\ issued = Writes
    /\ \A n \in Nodes : applied[n] = MaxEpoch
    /\ msgs = {}
    /\ \A n \in Nodes : held[n] = {}

PrimaryAt(e) == IF e = 0 THEN None ELSE Assign[e]
Believed(n) == PrimaryAt(applied[n])

Msg == [w : Writes, to : Nodes, ep : 0..MaxEpoch]

TypeOK ==
    /\ applied \in [Nodes -> 0..MaxEpoch]
    /\ issued \subseteq Writes
    /\ msgs \subseteq Msg
    /\ held \in [Nodes -> SUBSET [w : Writes, ep : 0..MaxEpoch]]
    /\ stored \subseteq Writes

Init ==
    /\ applied = [n \in Nodes |-> 0]
    /\ issued = {}
    /\ msgs = {}
    /\ held = [n \in Nodes |-> {}]
    /\ stored = {}

Route(n, w) ==
    IF Believed(n) = n
    THEN /\ stored' = stored \cup {w}
         /\ UNCHANGED msgs
    ELSE /\ msgs' = msgs \cup {[w |-> w, to |-> Believed(n), ep |-> applied[n]]}
         /\ UNCHANGED stored

Advance(n) ==
    /\ applied[n] < MaxEpoch
    /\ applied' = [applied EXCEPT ![n] = @ + 1]
    /\ UNCHANGED <<issued, msgs, held, stored>>

Issue(n, w) ==
    /\ w \notin issued
    /\ Believed(n) # None
    /\ issued' = issued \cup {w}
    /\ Route(n, w)
    /\ UNCHANGED <<applied, held>>

Receive(m) ==
    LET n == m.to IN
    /\ m \in msgs
    /\ CASE Believed(n) = n ->
                /\ stored' = stored \cup {m.w}
                /\ msgs' = msgs \ {m}
                /\ UNCHANGED held
         [] Design = "current" ->
                /\ msgs' = msgs \ {m}
                /\ UNCHANGED <<held, stored>>
         [] m.ep > applied[n] ->
                /\ held' = [held EXCEPT ![n] = @ \cup {[w |-> m.w, ep |-> m.ep]}]
                /\ msgs' = msgs \ {m}
                /\ UNCHANGED stored
         [] m.ep < applied[n] ->
                /\ msgs' = (msgs \ {m}) \cup
                       {[w |-> m.w, to |-> Believed(n), ep |-> applied[n]]}
                /\ UNCHANGED <<held, stored>>
    /\ UNCHANGED <<applied, issued>>

Release(n, h) ==
    /\ h \in held[n]
    /\ h.ep <= applied[n]
    /\ held' = [held EXCEPT ![n] = @ \ {h}]
    /\ IF Believed(n) = n
       THEN /\ stored' = stored \cup {h.w}
            /\ UNCHANGED msgs
       ELSE /\ msgs' = msgs \cup {[w |-> h.w, to |-> Believed(n), ep |-> applied[n]]}
            /\ UNCHANGED stored
    /\ UNCHANGED <<applied, issued>>

Done == Quiescent /\ UNCHANGED vars

Next ==
    \/ Done
    \/ \E n \in Nodes : Advance(n)
    \/ \E n \in Nodes, w \in Writes : Issue(n, w)
    \/ \E m \in msgs : Receive(m)
    \/ \E n \in Nodes : \E h \in held[n] : Release(n, h)

Fairness ==
    /\ \A n \in Nodes : WF_vars(Advance(n))
    /\ \A n \in Nodes, w \in Writes : WF_vars(Issue(n, w))
    /\ \A m \in Msg : WF_vars(Receive(m))
    /\ \A n \in Nodes : \A e \in 0..MaxEpoch, w \in Writes :
           WF_vars(Release(n, [w |-> w, ep |-> e]))

Spec == Init /\ [][Next]_vars /\ Fairness

EventuallyQuiescent == <>[]Quiescent

NoLoss == Quiescent => stored = Writes

HeldBounded == \A n \in Nodes : Cardinality(held[n]) <= Cardinality(Writes)

=============================================================================
