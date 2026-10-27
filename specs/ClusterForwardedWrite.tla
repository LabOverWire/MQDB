------------------------- MODULE ClusterForwardedWrite -------------------------
(***************************************************************************)
(* FORWARDED INTERNAL WRITES across partition reassignments (gh #221).     *)
(*                                                                          *)
(* The Raft log assigns one partition's primary, epoch by epoch. Each node *)
(* applies the log on its own tick, so nodes can be at different epochs,   *)
(* and a node can apply several epochs at once. Origin issues writes for   *)
(* one key in order: it applies each when its map names itself primary,    *)
(* and otherwise sends it to the node its map names. Each pair of nodes    *)
(* has one ordered channel (one QUIC stream per peer).                     *)
(*                                                                          *)
(* VERIFIED FACTS MODELLED (Design = "current"):                            *)
(*  - write_or_forward_impl forwards to partition_map.primary with no ack  *)
(*  - handle_write_request drops a write when the receiver is not primary  *)
(*  - the event loop applies the Raft map only on its tick                 *)
(*                                                                          *)
(* Design = "hold": the forwarder stamps the epoch of its map on the       *)
(* write. A receiver holds a write stamped newer than its own epoch, even  *)
(* if it is primary now, and routes it again in the same step that applies *)
(* the newer map: applied if primary, dropped otherwise. A write stamped   *)
(* at or below the receiver's epoch is applied if it is primary and        *)
(* dropped otherwise; it is never sent on, since a second hop could land   *)
(* after a newer write Origin sent straight to the primary.                *)
(*                                                                          *)
(* Ordered: every node applies Origin's writes in issue order.             *)
(* Delivered: with Lagging, Origin issues only when its map is at least as *)
(* new as every node's, and a node applies one epoch at a time and never  *)
(* past the stamp of a write already on its way to it: the receiver lags.  *)
(* Every write is then applied somewhere. The current design drops writes  *)
(* there; the hold design does not. A write that reaches a node already    *)
(* past its stamp (the sender was behind) is still dropped by both.        *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Assign, K, Origin, Design, Lagging

Nodes == {1, 2, 3}
None == 0
MaxEpoch == Len(Assign)

VARIABLES applied, issued, chan, held, stored

vars == <<applied, issued, chan, held, stored>>

PrimaryAt(e) == IF e = 0 THEN None ELSE Assign[e]
Believed(n) == PrimaryAt(applied[n])

Quiescent ==
    /\ issued = K
    /\ \A n \in Nodes : applied[n] = MaxEpoch
    /\ \A a, b \in Nodes : chan[<<a, b>>] = <<>>
    /\ \A n \in Nodes : held[n] = <<>>

Init ==
    /\ applied = [n \in Nodes |-> 0]
    /\ issued = 0
    /\ chan = [p \in Nodes \X Nodes |-> <<>>]
    /\ held = [n \in Nodes |-> <<>>]
    /\ stored = [n \in Nodes |-> <<>>]

Store(n, ws) == stored' = [stored EXCEPT ![n] = @ \o ws]

Advance(n, e) ==
    LET ready == SelectSeq(held[n], LAMBDA h : h.ep <= e)
    IN /\ applied[n] < e
       /\ Lagging =>
             /\ e = applied[n] + 1
             /\ \A f \in Nodes : \A i \in DOMAIN chan[<<f, n>>] : chan[<<f, n>>][i].ep >= e
       /\ applied' = [applied EXCEPT ![n] = e]
       /\ held' = [held EXCEPT ![n] = SelectSeq(@, LAMBDA h : h.ep > e)]
       /\ IF PrimaryAt(e) = n
          THEN Store(n, [i \in 1..Len(ready) |-> ready[i].w])
          ELSE UNCHANGED stored
       /\ UNCHANGED <<issued, chan>>

Issue ==
    /\ issued < K
    /\ Believed(Origin) # None
    /\ Lagging => \A n \in Nodes : applied[n] <= applied[Origin]
    /\ issued' = issued + 1
    /\ IF Believed(Origin) = Origin
       THEN /\ Store(Origin, <<issued + 1>>)
            /\ UNCHANGED chan
       ELSE /\ chan' = [chan EXCEPT ![<<Origin, Believed(Origin)>>] =
                  Append(@, [w |-> issued + 1, ep |-> applied[Origin]])]
            /\ UNCHANGED stored
    /\ UNCHANGED <<applied, held>>

Receive(from, n) ==
    /\ chan[<<from, n>>] # <<>>
    /\ LET m == Head(chan[<<from, n>>])
       IN /\ chan' = [chan EXCEPT ![<<from, n>>] = Tail(@)]
          /\ CASE Design = "hold" /\ m.ep > applied[n] ->
                    /\ held' = [held EXCEPT ![n] = Append(@, m)]
                    /\ UNCHANGED stored
             [] Believed(n) = n ->
                    /\ Store(n, <<m.w>>)
                    /\ UNCHANGED held
             [] OTHER ->
                    UNCHANGED <<held, stored>>
    /\ UNCHANGED <<applied, issued>>

Done == Quiescent /\ UNCHANGED vars

Next ==
    \/ Done
    \/ \E n \in Nodes, e \in 1..MaxEpoch : Advance(n, e)
    \/ Issue
    \/ \E a, b \in Nodes : Receive(a, b)

Spec == Init /\ [][Next]_vars

Ordered ==
    \A n \in Nodes : \A i, j \in DOMAIN stored[n] :
        i < j => stored[n][i] < stored[n][j]

Delivered ==
    Quiescent => \A w \in 1..K : \E n \in Nodes, i \in DOMAIN stored[n] : stored[n][i] = w

=============================================================================
