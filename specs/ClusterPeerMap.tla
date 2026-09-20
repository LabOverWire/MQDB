---- MODULE ClusterPeerMap ----
(***************************************************************************)
(* The QUIC transport's peer-connection map, `peers: HashMap<NodeId,       *)
(* PeerConnection>` in cluster/quic_transport.rs. Issue #140.              *)
(*                                                                          *)
(* Modelled from the code: an entry is created by exactly two sites, the   *)
(* outbound dial and the inbound accept, both inserting under the peer's    *)
(* NodeId, so `insert` REPLACES and the displaced PeerConnection is        *)
(* dropped. Dropping it drops its SendStream, which quinn `finish()`es, so  *)
(* the replaced generation can no longer send. Each connection's receiver   *)
(* task is detached and is NOT stopped by the replacement, so an old        *)
(* receiver can end long after a newer connection took the slot. Today      *)
(* nothing ever removes an entry.                                           *)
(*                                                                          *)
(* slot[p]  which connection generation occupies p's map entry (0 = empty) *)
(* usable   generations whose send stream still works                      *)
(* running  generations whose receiver task is still running               *)
(*                                                                          *)
(* SendBreaks models the remote's receiver task ending: its RecvStream     *)
(* drop calls stop() on OUR send stream, so our generation stops being     *)
(* usable while our own receiver still runs and the map entry is untouched. *)
(*                                                                          *)
(* Invariants:                                                              *)
(*   InvNoDeadLink  the map never advertises a generation that cannot send. *)
(*                  This is what direct_peers() reports, so a violation     *)
(*                  means the mesh warning calls an unreachable peer linked.*)
(*   InvNoLiveDrop  a usable connection is never dropped from the map.      *)
(*                                                                          *)
(* Configurations, and what the checker found. Cleanup is triggered ONLY by *)
(* the receiver task ending, because that is the only death signal the code *)
(* has:                                                                     *)
(*   _current  RemoveOnDeath=FALSE            today's code.                 *)
(*             InvNoDeadLink VIOLATED - a dead peer stays linked forever,   *)
(*             and there is no re-dial, so it never recovers.               *)
(*   _naive    RemoveOnDeath=TRUE, unguarded  remove by key on receiver     *)
(*             exit. InvNoLiveDrop VIOLATED two ways: it drops a link whose *)
(*             send side still works, and on a reconnect the OLD receiver's *)
(*             cleanup deletes the NEWER entry.                             *)
(*   _guarded  RemoveOnDeath=TRUE, guarded    remove only if the slot still *)
(*             holds my generation. Fixes the reconnect case but            *)
(*             InvNoLiveDrop is STILL VIOLATED: the guard asks "is the slot *)
(*             mine", not "is this link dead", so it removes a connection   *)
(*             whose send stream is healthy.                                *)
(*                                                                          *)
(* Conclusion: receiver exit is the wrong trigger. A receiver ends whenever *)
(* the remote drops its SEND stream - which it does on every replacement,   *)
(* so this is routine, not rare - while our send stream to that peer may be *)
(* perfectly usable. Removal must be driven by SEND failure (or a liveness  *)
(* probe), guarded by a per-connection id, and paired with a re-dial.       *)
(*                                                                          *)
(* What this spec CANNOT settle. InvNoDeadLink is unachievable by any async *)
(* implementation: a break and its cleanup are separate events, so there is *)
(* always a transient window. Guarded removal with no re-dial also turns a  *)
(* stale entry into a permanently absent one, and both outcomes satisfy the *)
(* safety invariants equally. Telling a real fix from an emptied map needs  *)
(* a Redial action and a liveness property, which this spec does not have.  *)
(*                                                                          *)
(* Caveat: the guard needs a per-connection identity PeerConnection lacks.  *)
(* Use a monotonic counter, NOT quinn's Connection::stable_id, which is a   *)
(* reusable heap pointer and would reintroduce ABA in a compare-and-remove. *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Peers, MaxGen, RemoveOnDeath, GuardRemoval

Gen == 1..MaxGen
NoGen == 0

VARIABLES slot, owner, allocated, running, usable

vars == <<slot, owner, allocated, running, usable>>

TypeOK ==
    /\ slot \in [Peers -> Gen \cup {NoGen}]
    /\ owner \in [Gen -> Peers]
    /\ allocated \subseteq Gen
    /\ running \subseteq Gen
    /\ usable \subseteq Gen

Init ==
    /\ slot = [p \in Peers |-> NoGen]
    /\ owner = [g \in Gen |-> CHOOSE p \in Peers : TRUE]
    /\ allocated = {}
    /\ running = {}
    /\ usable = {}

Connect(p, g) ==
    /\ g \notin allocated
    /\ allocated' = allocated \cup {g}
    /\ owner' = [owner EXCEPT ![g] = p]
    /\ running' = running \cup {g}
    /\ usable' = (usable \ {slot[p]}) \cup {g}
    /\ slot' = [slot EXCEPT ![p] = g]

RemovedSlot(g) ==
    IF ~RemoveOnDeath
        THEN slot
        ELSE IF GuardRemoval
            THEN IF slot[owner[g]] = g
                    THEN [slot EXCEPT ![owner[g]] = NoGen]
                    ELSE slot
            ELSE [slot EXCEPT ![owner[g]] = NoGen]

SendBreaks(g) ==
    /\ g \in usable
    /\ usable' = usable \ {g}
    /\ UNCHANGED <<slot, owner, allocated, running>>

ReceiverEnds(g) ==
    /\ g \in running
    /\ running' = running \ {g}
    /\ slot' = RemovedSlot(g)
    /\ UNCHANGED <<owner, allocated, usable>>

Next ==
    \/ \E p \in Peers, g \in Gen : Connect(p, g)
    \/ \E g \in Gen : SendBreaks(g)
    \/ \E g \in Gen : ReceiverEnds(g)

Spec == Init /\ [][Next]_vars

InvNoDeadLink ==
    \A p \in Peers : slot[p] # NoGen => slot[p] \in usable

InvNoLiveDrop ==
    \A p \in Peers :
        (\E g \in usable : owner[g] = p) => slot[p] # NoGen

====
