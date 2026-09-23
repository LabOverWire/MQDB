------------------------- MODULE ClusterSubReconcile -------------------------
(***************************************************************************)
(* Design model for gh #141: SubscriptionCache::reconcile vs a missed      *)
(* topic-index broadcast. One client, subscribed at its partition primary  *)
(* P. The durable subscription record reaches the replica R by partition    *)
(* replication; the topic-index entry reaches every other node only by a    *)
(* one-hop broadcast (routing.rs handle_topic_subscribe). apply_subscription*)
(* never touches the index. Replication and the index broadcast share the   *)
(* P->R bulk lane (FIFO). Any in-flight message, replication writes         *)
(* included, can be dropped (Drop). O never holds a record. S is a former   *)
(* holder (ex-owner, or the node the client used to connect through): it    *)
(* receives replication until LoseOwnership, then keeps a stale copy that   *)
(* nothing deletes (SubscriptionCache::clear_partition has no production    *)
(* caller), while still receiving index broadcasts.                        *)
(*                                                                          *)
(* Mode "old"  = main before #151: for every client present in the cache,   *)
(*               the record is overwritten with the local index.            *)
(* Mode "new"  = #151: the index gains every recorded topic; the record is  *)
(*               never modified. Restrict = only for partitions this node   *)
(*               currently holds (primary or replica).                     *)
(* Mode "none" = no reconcile (shows the liveness property is non-vacuous). *)
(*                                                                          *)
(* The loss/resurrection flags are computed from the record change of every *)
(* Reconcile step in every mode. In mode "new" the record never changes, so *)
(* those two invariants hold by construction; the checker results that      *)
(* matter for "new" are RoutingRepaired and the ghost invariants.           *)
(*                                                                          *)
(* Configs:                                                                 *)
(*   ClusterSubReconcile.cfg     new, Restrict -> loss/resurrection/stale-  *)
(*        copy-ghost invariants and RoutingRepaired hold (MaxOps = 2, the   *)
(*        largest bound where the liveness check is exhaustive)            *)
(*   *_oldloss.cfg               old -> InvNoReconcileLoss violated         *)
(*   *_oldresurrect.cfg          old -> InvNoResurrection violated          *)
(*   *_norepair.cfg              none -> RoutingRepaired violated           *)
(*   *_nonholder.cfg             new -> NonHolderIndexed violated: a node   *)
(*        that holds no record and misses the broadcast is never repaired   *)
(*        (tracked in gh #140)                                              *)
(*   *_unrestricted.cfg          new, no Restrict -> InvNoStaleCopyGhost    *)
(*        violated: a former holder's stale copy recreates an index entry   *)
(*        for a topic the client dropped                                    *)
(*   *_holderghost.cfg           new, Restrict -> InvNoHolderGhost violated:*)
(*        a holder whose record lags the index (replication delete delayed  *)
(*        or dropped behind a delivered unsubscribe broadcast) re-adds an   *)
(*        entry; the index is never pruned, so it stays. Accepted: the      *)
(*        receiving node only delivers to its local subscribers, so the     *)
(*        cost is a wasted forward, not a misdelivery                       *)
(*                                                                          *)
(* The old-mode guard rec[n] # {} under-approximates the real code, where   *)
(* the replica keeps an empty snapshot after the last unsubscribe; it can   *)
(* only hide old-mode violations, never create them.                        *)
(*                                                                          *)
(* NOT MODELLED: the on-disk copy and restart recovery; explicit promotion  *)
(* (reconcile after takeover is a Reconcile step); snapshot import of       *)
(* TOPIC_INDEX (an extra repair source for topic-partition holders);        *)
(* replication catch-up of a dropped write; a client connected to a node  *)
(* other than P, where the record and the broadcast take different paths   *)
(* and are not FIFO-ordered. Bounded by MaxOps.                            *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Topics, MaxOps, Mode, Lossy, Restrict

ASSUME Mode \in {"old", "new", "none"}
ASSUME MaxOps \in Nat /\ Lossy \in BOOLEAN /\ Restrict \in BOOLEAN

Nodes == {"P", "R", "O", "S"}
Holders == {"P", "R"}
Cached == {"P", "R", "S"}
Remote == {"R", "O", "S"}

Msg == [k : {"repl", "idx"}, op : {"add", "del"}, t : Topics]

VARIABLES truth, rec, idx, ch, ops, sHolds, lost, resurrected, staleGhost, holderGhost

vars == <<truth, rec, idx, ch, ops, sHolds, lost, resurrected, staleGhost, holderGhost>>

TypeOK ==
    /\ truth \subseteq Topics
    /\ rec \in [Cached -> SUBSET Topics]
    /\ idx \in [Nodes -> SUBSET Topics]
    /\ \A n \in Remote : ch[n] \in Seq(Msg)
    /\ ops \in 0..MaxOps
    /\ sHolds \in BOOLEAN
    /\ lost \in BOOLEAN /\ resurrected \in BOOLEAN
    /\ staleGhost \in BOOLEAN /\ holderGhost \in BOOLEAN

Init ==
    /\ truth = {}
    /\ rec = [n \in Cached |-> {}]
    /\ idx = [n \in Nodes |-> {}]
    /\ ch = [n \in Remote |-> <<>>]
    /\ ops = 0
    /\ sHolds = TRUE
    /\ lost = FALSE
    /\ resurrected = FALSE
    /\ staleGhost = FALSE
    /\ holderGhost = FALSE

Holds(n) == n \in Holders \/ (n = "S" /\ sHolds)

Apply(s, op, t) == IF op = "add" THEN s \cup {t} ELSE s \ {t}

ReplMsg(op, t) == [k |-> "repl", op |-> op, t |-> t]
IdxMsg(op, t) == [k |-> "idx", op |-> op, t |-> t]

Emit(op, t) ==
    [n \in Remote |->
        CASE n = "R" -> ch[n] \o <<ReplMsg(op, t), IdxMsg(op, t)>>
          [] n = "S" -> IF sHolds
                          THEN ch[n] \o <<ReplMsg(op, t), IdxMsg(op, t)>>
                          ELSE Append(ch[n], IdxMsg(op, t))
          [] OTHER -> Append(ch[n], IdxMsg(op, t))]

Change(op, t) ==
    /\ ops < MaxOps
    /\ truth' = Apply(truth, op, t)
    /\ rec' = [rec EXCEPT !["P"] = Apply(@, op, t)]
    /\ idx' = [idx EXCEPT !["P"] = Apply(@, op, t)]
    /\ ch' = Emit(op, t)
    /\ ops' = ops + 1
    /\ UNCHANGED <<sHolds, lost, resurrected, staleGhost, holderGhost>>

Sub(t) == t \notin truth /\ Change("add", t)
Unsub(t) == t \in truth /\ Change("del", t)

LoseOwnership ==
    /\ sHolds
    /\ sHolds' = FALSE
    /\ ch' = [ch EXCEPT !["S"] = SelectSeq(@, LAMBDA m : m.k = "idx")]
    /\ UNCHANGED <<truth, rec, idx, ops, lost, resurrected, staleGhost, holderGhost>>

Deliver(n) ==
    /\ ch[n] # <<>>
    /\ LET m == Head(ch[n]) IN
         /\ ch' = [ch EXCEPT ![n] = Tail(@)]
         /\ IF m.k = "repl"
              THEN /\ rec' = [rec EXCEPT ![n] = Apply(@, m.op, m.t)]
                   /\ UNCHANGED idx
              ELSE /\ idx' = [idx EXCEPT ![n] = Apply(@, m.op, m.t)]
                   /\ UNCHANGED rec
    /\ UNCHANGED <<truth, ops, sHolds, lost, resurrected, staleGhost, holderGhost>>

Drop(n, i) ==
    /\ Lossy
    /\ i \in 1..Len(ch[n])
    /\ ch' = [ch EXCEPT ![n] = SubSeq(@, 1, i - 1) \o SubSeq(@, i + 1, Len(@))]
    /\ UNCHANGED <<truth, rec, idx, ops, sHolds, lost, resurrected, staleGhost, holderGhost>>

NewRec(n) == IF Mode = "old" THEN idx[n] ELSE rec[n]
NewIdx(n) == IF Mode = "old" THEN idx[n] ELSE idx[n] \cup rec[n]

Reconcile(n) ==
    /\ Mode # "none"
    /\ rec[n] # {}
    /\ (Mode = "new" /\ Restrict) => Holds(n)
    /\ rec' = [rec EXCEPT ![n] = NewRec(n)]
    /\ idx' = [idx EXCEPT ![n] = NewIdx(n)]
    /\ lost' = (lost \/ (Holds(n) /\ (rec[n] \ NewRec(n)) \cap truth # {}))
    /\ resurrected' = (resurrected \/ (Holds(n) /\ (NewRec(n) \ rec[n]) \ truth # {}))
    /\ staleGhost' = (staleGhost \/ (~Holds(n) /\ (NewIdx(n) \ idx[n]) \ truth # {}))
    /\ holderGhost' = (holderGhost \/ (Holds(n) /\ (NewIdx(n) \ idx[n]) \ truth # {}))
    /\ UNCHANGED <<truth, ch, ops, sHolds>>

Next ==
    \/ \E t \in Topics : Sub(t) \/ Unsub(t)
    \/ LoseOwnership
    \/ \E n \in Remote : Deliver(n)
    \/ \E n \in Remote : \E i \in 1..Len(ch[n]) : Drop(n, i)
    \/ \E n \in Cached : Reconcile(n)

Spec ==
    /\ Init /\ [][Next]_vars
    /\ \A n \in Remote : WF_vars(Deliver(n))
    /\ \A n \in Holders : WF_vars(Reconcile(n))

----------------------------------------------------------------------------
\* SAFETY on the record of a current holder: reconcile never deletes a
\* subscription the client still holds, and never brings back a dropped one.
InvNoReconcileLoss == ~lost
InvNoResurrection == ~resurrected

\* SAFETY on the index: a node that no longer holds the partition never
\* recreates an entry for a topic the client dropped.
InvNoStaleCopyGhost == ~staleGhost
InvNoHolderGhost == ~holderGhost

\* LIVENESS: every node that holds the record ends up with every recorded
\* topic in its index (routing on that node is repaired).
HoldersIndexed == \A n \in Holders : rec[n] \subseteq idx[n]
RoutingRepaired == <>[]HoldersIndexed

\* SCOPE: a node holding no record is never repaired if it missed a broadcast.
NonHolderIndexed == <>[](truth \subseteq idx["O"])
=============================================================================
