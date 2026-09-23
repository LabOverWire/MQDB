------------------------- MODULE ClusterSubReconcile -------------------------
(***************************************************************************)
(* Design model for gh #141: SubscriptionCache::reconcile vs a missed      *)
(* topic-index broadcast. One client, subscribed at its partition primary  *)
(* P. The durable subscription record reaches the replica R by partition    *)
(* replication; the topic-index entry reaches every other node only by a    *)
(* one-hop broadcast (routing.rs handle_topic_subscribe). apply_subscription*)
(* never touches the index. Replication and the index broadcast share the   *)
(* P->R bulk lane (FIFO, lossy under backpressure). O holds no record.      *)
(*                                                                          *)
(* Mode "old"  = current reconcile: for a client present in the cache, the  *)
(*               record is overwritten with the local index.                *)
(* Mode "new"  = proposed: the index gains every recorded topic; the record *)
(*               is never modified by reconcile.                            *)
(* Mode "none" = no reconcile (shows the liveness property is non-vacuous). *)
(*                                                                          *)
(* Configs:                                                                 *)
(*   ClusterSubReconcile.cfg  new  -> safety holds, holders' index repaired *)
(*   *_oldloss.cfg            old  -> InvNoReconcileLoss violated           *)
(*   *_oldresurrect.cfg       old  -> InvNoResurrection violated            *)
(*   *_norepair.cfg           none -> RoutingRepaired violated              *)
(*   *_nonholder.cfg          new  -> NonHolderIndexed violated: a node     *)
(*        that holds no record and misses the broadcast is NOT repaired by  *)
(*        this fix (residual gap, out of scope, tracked separately).        *)
(*                                                                          *)
(* NOT MODELLED: the on-disk copy (reconcile edits memory, and memory feeds *)
(* export_partition); explicit promotion (reconcile after takeover is just  *)
(* a Reconcile step); loss of the replicated write itself (async RF=2       *)
(* durability, independent of reconcile). Bounded by MaxOps.                *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Topics, MaxOps, Mode, Lossy

ASSUME Mode \in {"old", "new", "none"}
ASSUME MaxOps \in Nat /\ Lossy \in BOOLEAN

Nodes == {"P", "R", "O"}
Holders == {"P", "R"}
Remote == {"R", "O"}

Msg == [k : {"repl", "idx"}, op : {"add", "del"}, t : Topics]

VARIABLES truth, rec, idx, ch, ops, lost, resurrected

vars == <<truth, rec, idx, ch, ops, lost, resurrected>>

TypeOK ==
    /\ truth \subseteq Topics
    /\ rec \in [Holders -> SUBSET Topics]
    /\ idx \in [Nodes -> SUBSET Topics]
    /\ \A n \in Remote : ch[n] \in Seq(Msg)
    /\ ops \in 0..MaxOps
    /\ lost \in BOOLEAN /\ resurrected \in BOOLEAN

Init ==
    /\ truth = {}
    /\ rec = [n \in Holders |-> {}]
    /\ idx = [n \in Nodes |-> {}]
    /\ ch = [n \in Remote |-> <<>>]
    /\ ops = 0
    /\ lost = FALSE
    /\ resurrected = FALSE

Apply(s, op, t) == IF op = "add" THEN s \cup {t} ELSE s \ {t}

Emit(op, t) ==
    [ch EXCEPT !["R"] = Append(Append(@, [k |-> "repl", op |-> op, t |-> t]),
                                          [k |-> "idx", op |-> op, t |-> t]),
               !["O"] = Append(@, [k |-> "idx", op |-> op, t |-> t])]

Change(op, t) ==
    /\ ops < MaxOps
    /\ truth' = Apply(truth, op, t)
    /\ rec' = [rec EXCEPT !["P"] = Apply(@, op, t)]
    /\ idx' = [idx EXCEPT !["P"] = Apply(@, op, t)]
    /\ ch' = Emit(op, t)
    /\ ops' = ops + 1
    /\ UNCHANGED <<lost, resurrected>>

Sub(t) == t \notin truth /\ Change("add", t)
Unsub(t) == t \in truth /\ Change("del", t)

Deliver(n) ==
    /\ ch[n] # <<>>
    /\ LET m == Head(ch[n]) IN
         /\ ch' = [ch EXCEPT ![n] = Tail(@)]
         /\ IF m.k = "repl"
              THEN /\ rec' = [rec EXCEPT ![n] = Apply(@, m.op, m.t)]
                   /\ UNCHANGED idx
              ELSE /\ idx' = [idx EXCEPT ![n] = Apply(@, m.op, m.t)]
                   /\ UNCHANGED rec
    /\ UNCHANGED <<truth, ops, lost, resurrected>>

Drop(n, i) ==
    /\ Lossy
    /\ i \in 1..Len(ch[n])
    /\ ch' = [ch EXCEPT ![n] = SubSeq(@, 1, i - 1) \o SubSeq(@, i + 1, Len(@))]
    /\ UNCHANGED <<truth, rec, idx, ops, lost, resurrected>>

\* The real reconcile iterates only clients present in the cache, so it
\* does nothing for a node whose record for the client is empty.
Reconcile(n) ==
    /\ Mode # "none"
    /\ rec[n] # {}
    /\ IF Mode = "old"
         THEN /\ rec' = [rec EXCEPT ![n] = idx[n]]
              /\ lost' = (lost \/ ((rec[n] \ idx[n]) \cap truth # {}))
              /\ resurrected' = (resurrected \/ ((idx[n] \ rec[n]) \ truth # {}))
              /\ UNCHANGED idx
         ELSE /\ idx' = [idx EXCEPT ![n] = @ \cup rec[n]]
              /\ UNCHANGED <<rec, lost, resurrected>>
    /\ UNCHANGED <<truth, ch, ops>>

Next ==
    \/ \E t \in Topics : Sub(t) \/ Unsub(t)
    \/ \E n \in Remote : Deliver(n)
    \/ \E n \in Remote : \E i \in 1..Len(ch[n]) : Drop(n, i)
    \/ \E n \in Holders : Reconcile(n)

Spec ==
    /\ Init /\ [][Next]_vars
    /\ \A n \in Remote : WF_vars(Deliver(n))
    /\ \A n \in Holders : WF_vars(Reconcile(n))

----------------------------------------------------------------------------
\* SAFETY: reconcile never deletes a subscription the client still holds,
\* and never brings back one the client dropped.
InvNoReconcileLoss == ~lost
InvNoResurrection == ~resurrected

\* LIVENESS: every node that holds the record ends up with every recorded
\* topic in its index (routing on that node is repaired).
HoldersIndexed == \A n \in Holders : rec[n] \subseteq idx[n]
RoutingRepaired == <>[]HoldersIndexed

\* SCOPE: a node holding no record is never repaired if it missed a broadcast.
NonHolderIndexed == <>[](truth \subseteq idx["O"])
=============================================================================
