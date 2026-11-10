----------------------- MODULE ClusterSnapshotReplace -----------------------
(***************************************************************************)
(* Design model for gh #308: what a partition snapshot import does to the  *)
(* replica's stores, given the writes delivered while the transfer runs.   *)
(*                                                                          *)
(* One primary P, one replica R that is AwaitingSnapshot from the start    *)
(* (it already sent its request). Three kinds of key:                       *)
(*   DKeys  partition records written through sequenced Writes. R refuses  *)
(*          them while AwaitingSnapshot (not_replica); after the import it  *)
(*          takes the snapshot sequence and catches up from P's write log.  *)
(*   UKeys  partition keys written unsequenced (UniqueReplicate): R applies *)
(*          them whatever its role.                                         *)
(*   GKeys  global keys (schemas, constraints): exported whole by every    *)
(*          partition snapshot, broadcast unsequenced (WriteRequest), R    *)
(*          applies them whatever its role.                                 *)
(* R's partition keys start with arbitrary stale content (a node that held *)
(* the partition before); its global keys start equal to P's.               *)
(* P -> R is one FIFO channel; the snapshot travels apart, so it is         *)
(* exported at any point and imported at any later point.                   *)
(*                                                                          *)
(* Import (Replace, Protect):                                               *)
(*   main      merge: a snapshot entry overwrites, an absent one keeps R's  *)
(*   Replace   a partition key takes the snapshot value (absent = deleted); *)
(*             a global key is merged as on main                            *)
(*   Protect   a key R wrote after sending its request keeps R's value      *)
(*                                                                          *)
(* InvConverged: once R is a replica, the channel is drained and R holds   *)
(* P's sequence, R's store equals P's store (no ghost, no resurrection, no *)
(* undone update).                                                          *)
(*                                                                          *)
(* Configs (MaxWrites = 3 unless noted):                                  *)
(*   ClusterSnapshotReplace.cfg        Replace + Protect: holds, 422,829   *)
(*                                     states.                              *)
(*   ClusterSnapshotReplace_wide.cfg   same, MaxWrites = 4: holds,         *)
(*                                     2,889,666 states.                    *)
(*   ClusterSnapshotReplace_reach.cfg  same, checks NeverQuiescentAfter-   *)
(*                                     Writes: violated, so the quiescent  *)
(*                                     states InvConverged speaks about    *)
(*                                     are reached after every write.      *)
(*   ClusterSnapshotReplace_main.cfg   neither: violated, a stale partition *)
(*                                     key survives the import.             *)
(*   ClusterSnapshotReplace_main_global.cfg neither, global key only,      *)
(*                                     MaxWrites = 2: violated, a global   *)
(*                                     delete taken during the transfer is *)
(*                                     undone.                              *)
(*   ClusterSnapshotReplace_noprotect.cfg Replace only: violated, an       *)
(*                                     unsequenced write taken after the   *)
(*                                     export is undone.                    *)
(*   ClusterSnapshotReplace_noreplace.cfg Protect only: violated, a stale  *)
(*                                     partition key survives.              *)
(*                                                                          *)
(* NOT MODELLED: global writes from an origin other than P (R can then be  *)
(* ahead of the snapshot source before its transfer starts); lost global   *)
(* broadcasts; write-log eviction; transfer retries; topic index and       *)
(* wildcard entries (merged, rebuilt from subscriptions). Bounded         *)
(* constants: verified for these sizes, not proven for all.              *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS DKeys, UKeys, GKeys, Vals, MaxWrites, Replace, Protect

None == 0
Keys == DKeys \cup UKeys \cup GKeys
PartKeys == DKeys \cup UKeys
Values == Vals \cup {None}

VARIABLES pstore, pseq, plog, writes, chan, snap, rstore, rrole, rseq, touched

vars == <<pstore, pseq, plog, writes, chan, snap, rstore, rrole, rseq, touched>>

Msg == [t : {"seq", "unseq"}, k : Keys, v : Values, q : 0..MaxWrites]

TypeOK ==
    /\ pstore \in [Keys -> Values]
    /\ rstore \in [Keys -> Values]
    /\ pseq \in 0..MaxWrites
    /\ writes \in 0..MaxWrites
    /\ chan \in Seq(Msg)
    /\ rrole \in {"awaiting", "replica"}
    /\ rseq \in 0..MaxWrites
    /\ touched \subseteq Keys
    /\ snap.taken \in BOOLEAN

Init ==
    /\ pstore \in [Keys -> Values]
    /\ rstore \in [Keys -> Values]
    /\ \A g \in GKeys : rstore[g] = pstore[g]
    /\ pseq = 0
    /\ plog = <<>>
    /\ writes = 0
    /\ chan = <<>>
    /\ snap = [taken |-> FALSE, s |-> [k \in Keys |-> None], q |-> 0]
    /\ rrole = "awaiting"
    /\ rseq = 0
    /\ touched = {}

PrimaryWrite(k, v) ==
    /\ writes < MaxWrites
    /\ pstore[k] # v
    /\ pstore' = [pstore EXCEPT ![k] = v]
    /\ writes' = writes + 1
    /\ IF k \in DKeys
         THEN /\ pseq' = pseq + 1
              /\ plog' = Append(plog, [k |-> k, v |-> v])
              /\ chan' = Append(chan, [t |-> "seq", k |-> k, v |-> v, q |-> pseq + 1])
         ELSE /\ chan' = Append(chan, [t |-> "unseq", k |-> k, v |-> v, q |-> 0])
              /\ UNCHANGED <<pseq, plog>>
    /\ UNCHANGED <<snap, rstore, rrole, rseq, touched>>

Export ==
    /\ ~snap.taken
    /\ snap' = [taken |-> TRUE, s |-> pstore, q |-> pseq]
    /\ UNCHANGED <<pstore, pseq, plog, writes, chan, rstore, rrole, rseq, touched>>

Kept(k) == Protect /\ k \in touched

ImportValue(k) ==
    IF Kept(k) THEN rstore[k]
    ELSE IF Replace /\ k \in PartKeys THEN snap.s[k]
    ELSE IF snap.s[k] # None THEN snap.s[k]
    ELSE rstore[k]

Import ==
    /\ rrole = "awaiting"
    /\ snap.taken
    /\ rstore' = [k \in Keys |-> ImportValue(k)]
    /\ rrole' = "replica"
    /\ rseq' = snap.q
    /\ touched' = {}
    /\ UNCHANGED <<pstore, pseq, plog, writes, chan, snap>>

DeliverUnsequenced ==
    /\ chan # <<>>
    /\ Head(chan).t = "unseq"
    /\ chan' = Tail(chan)
    /\ rstore' = [rstore EXCEPT ![Head(chan).k] = Head(chan).v]
    /\ touched' = IF rrole = "awaiting" THEN touched \cup {Head(chan).k} ELSE touched
    /\ UNCHANGED <<pstore, pseq, plog, writes, snap, rrole, rseq>>

DeliverSequenced ==
    /\ chan # <<>>
    /\ Head(chan).t = "seq"
    /\ chan' = Tail(chan)
    /\ IF rrole = "replica" /\ Head(chan).q = rseq + 1
         THEN /\ rstore' = [rstore EXCEPT ![Head(chan).k] = Head(chan).v]
              /\ rseq' = Head(chan).q
         ELSE UNCHANGED <<rstore, rseq>>
    /\ UNCHANGED <<pstore, pseq, plog, writes, snap, rrole, touched>>

RECURSIVE ApplyFrom(_, _, _)
ApplyFrom(store, from, to) ==
    IF from > to THEN store
    ELSE ApplyFrom([store EXCEPT ![plog[from].k] = plog[from].v], from + 1, to)

CatchUp ==
    /\ rrole = "replica"
    /\ rseq < pseq
    /\ rstore' = ApplyFrom(rstore, rseq + 1, pseq)
    /\ rseq' = pseq
    /\ UNCHANGED <<pstore, pseq, plog, writes, chan, snap, rrole, touched>>

Next ==
    \/ \E k \in Keys, v \in Values : PrimaryWrite(k, v)
    \/ Export
    \/ Import
    \/ DeliverUnsequenced
    \/ DeliverSequenced
    \/ CatchUp

Spec == Init /\ [][Next]_vars

Quiescent == rrole = "replica" /\ chan = <<>> /\ rseq = pseq

InvConverged == Quiescent => rstore = pstore

NeverQuiescentAfterWrites == ~(Quiescent /\ writes = MaxWrites)
=============================================================================
