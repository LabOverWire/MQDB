----------------------- MODULE ClusterSnapshotReplace -----------------------
(***************************************************************************)
(* Design model for gh #308: what a partition snapshot import does to the  *)
(* replica's stores, given the writes delivered while the transfer runs,   *)
(* and whether a restarted primary keeps the writes it acked.              *)
(*                                                                          *)
(* One primary P, one replica R that is AwaitingSnapshot from the start    *)
(* (its transfer window is open). Three kinds of key:                       *)
(*   DKeys  partition records written through sequenced Writes. R refuses  *)
(*          them while AwaitingSnapshot (not_replica); after the import it  *)
(*          takes the snapshot sequence and catches up from P's write log.  *)
(*   UKeys  unique claims, written unsequenced (UniqueReplicate): R applies *)
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
(*   Replace   a DKey takes the snapshot value (absent = deleted); UKeys   *)
(*             and GKeys are merged as on main                              *)
(*   Protect   a UKey or GKey R wrote while its window was open keeps R's   *)
(*             value                                                        *)
(*                                                                          *)
(* Restart: P starts holding data it imported and becomes primary          *)
(* (BecomePrimary) before it takes writes or serves R. P may restart once  *)
(* at any point, also before BecomePrimary (a crash between the import and *)
(* the marker write). On restart P trusts its disk iff it holds a held-    *)
(* primary marker whose epoch lies in primary_since..epoch, as             *)
(* HeldPrimaries::trusts. MarkerAfterImport = TRUE writes the marker on    *)
(* BecomePrimary and on Rebalance (become_primary at the new epoch); FALSE *)
(* is the previous rule, which skipped it for an imported partition. A P   *)
(* that does not trust its disk imports R's store with the same rule if R  *)
(* is a replica, else keeps its own (it takes the role without data).      *)
(* P takes no writes after the restart.                                   *)
(*                                                                          *)
(* InvConverged: once R is a replica, the channel is drained and R holds   *)
(* P's sequence, R's store equals P's store (no ghost, no resurrection, no *)
(* undone update), except a UKey or GKey P never wrote in the run and does *)
(* not hold: merge keeps such a stale entry on R by design.                *)
(* InvAckedKept: after the restart P still holds the value of every key   *)
(* it wrote as primary.                                                    *)
(*                                                                          *)
(* Configs (MaxWrites = 3 unless noted; all explore the full space):      *)
(*   ClusterSnapshotReplace.cfg        Replace + Protect: holds, 461,376   *)
(*                                     states.                              *)
(*   ClusterSnapshotReplace_wide.cfg   same, MaxWrites = 4: holds,         *)
(*                                     3,180,195 states.                    *)
(*   ClusterSnapshotReplace_reach.cfg  same, checks NeverQuiescentAfter-   *)
(*                                     Writes: violated, so the quiescent  *)
(*                                     states InvConverged speaks about    *)
(*                                     are reached after every write.      *)
(*   ClusterSnapshotReplace_main.cfg   neither: violated, a stale DKey     *)
(*                                     survives the import.                 *)
(*   ClusterSnapshotReplace_main_global.cfg neither, global key only,      *)
(*                                     MaxWrites = 2: violated, a global   *)
(*                                     delete taken during the transfer is *)
(*                                     undone.                              *)
(*   ClusterSnapshotReplace_noprotect.cfg Replace only: violated, a unique *)
(*                                     delete taken after the export is    *)
(*                                     undone.                              *)
(*   ClusterSnapshotReplace_noreplace.cfg Protect only: violated, a stale  *)
(*                                     DKey survives.                       *)
(*   ClusterSnapshotReplace_restart.cfg Replace + Protect + Restart,       *)
(*                                     MarkerAfterImport, MaxEpoch = 2:    *)
(*                                     InvConverged and InvAckedKept hold, *)
(*                                     1,846,476 states.                    *)
(*   ClusterSnapshotReplace_restart_prev.cfg same, no marker after an      *)
(*                                     import: InvAckedKept violated, an   *)
(*                                     acked write R had not applied is    *)
(*                                     lost.                                *)
(*   ClusterSnapshotReplace_restart_main.cfg merge, no marker after an     *)
(*                                     import: InvAckedKept violated, an   *)
(*                                     acked unique update is rolled back  *)
(*                                     to R's older value.                  *)
(*                                                                          *)
(* NOT MODELLED: more than one writer. Unique claims are made by the       *)
(* unique coordinator and fanned out to every node, and schemas or        *)
(* constraints by any node, so R can hold an entry the snapshot source    *)
(* lacks or has older (unit tests cover the cases this PR changes:        *)
(* a_unique_claim_the_source_lacks_is_kept_by_import, a_global_write_      *)
(* while_the_request_cannot_be_sent_survives_import). Also: unique seals; *)
(* a primary that loses and regains the role (primary_since moves, so the *)
(* marker range is only exercised by same-primary rebalances here); lost  *)
(* broadcasts; write-log eviction (a_catchup_past_the_evicted_log_falls_  *)
(* back_to_a_new_snapshot); transfer retries and window overflow          *)
(* (a_delete_protected_before_the_window_fills_stays_deleted); topic index *)
(* and wildcard entries (merged, rebuilt from subscriptions). Bounded     *)
(* constants: verified for these sizes, not proven for all.               *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS DKeys, UKeys, GKeys, Vals, MaxWrites, Replace, Protect, Restart,
          MarkerAfterImport, MaxEpoch

None == 0
Keys == DKeys \cup UKeys \cup GKeys
Values == Vals \cup {None}

VARIABLES pstore, pseq, plog, writes, chan, snap, rstore, rrole, rseq, touched,
          restarted, acked, pprimary, marker, epoch, wrote

vars == <<pstore, pseq, plog, writes, chan, snap, rstore, rrole, rseq, touched,
          restarted, acked, pprimary, marker, epoch, wrote>>

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
    /\ restarted \in BOOLEAN
    /\ acked \in [Keys -> Values]
    /\ pprimary \in BOOLEAN
    /\ marker \in 0..MaxEpoch
    /\ epoch \in 1..MaxEpoch
    /\ wrote \subseteq Keys

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
    /\ restarted = FALSE
    /\ acked = [k \in Keys |-> None]
    /\ pprimary = ~Restart
    /\ marker = 0
    /\ epoch = 1
    /\ wrote = {}

PrimaryWrite(k, v) ==
    /\ pprimary
    /\ ~restarted
    /\ writes < MaxWrites
    /\ wrote' = wrote \cup {k}
    /\ pstore[k] # v
    /\ pstore' = [pstore EXCEPT ![k] = v]
    /\ writes' = writes + 1
    /\ IF k \in DKeys
         THEN /\ pseq' = pseq + 1
              /\ plog' = Append(plog, [k |-> k, v |-> v])
              /\ chan' = Append(chan, [t |-> "seq", k |-> k, v |-> v, q |-> pseq + 1])
         ELSE /\ chan' = Append(chan, [t |-> "unseq", k |-> k, v |-> v, q |-> 0])
              /\ UNCHANGED <<pseq, plog>>
    /\ UNCHANGED <<snap, rstore, rrole, rseq, touched, restarted, acked, pprimary, marker, epoch>>

Export ==
    /\ pprimary
    /\ ~snap.taken
    /\ snap' = [taken |-> TRUE, s |-> pstore, q |-> pseq]
    /\ UNCHANGED <<pstore, pseq, plog, writes, chan, rstore, rrole, rseq, touched, restarted, acked, pprimary, marker, epoch, wrote>>

Kept(k) == Protect /\ k \in touched

ImportValue(k) ==
    IF Kept(k) THEN rstore[k]
    ELSE IF Replace /\ k \in DKeys THEN snap.s[k]
    ELSE IF snap.s[k] # None THEN snap.s[k]
    ELSE rstore[k]

Import ==
    /\ rrole = "awaiting"
    /\ snap.taken
    /\ rstore' = [k \in Keys |-> ImportValue(k)]
    /\ rrole' = "replica"
    /\ rseq' = snap.q
    /\ touched' = {}
    /\ UNCHANGED <<pstore, pseq, plog, writes, chan, snap, restarted, acked, pprimary, marker, epoch, wrote>>

DeliverUnsequenced ==
    /\ chan # <<>>
    /\ Head(chan).t = "unseq"
    /\ chan' = Tail(chan)
    /\ rstore' = [rstore EXCEPT ![Head(chan).k] = Head(chan).v]
    /\ touched' = IF rrole = "awaiting" THEN touched \cup {Head(chan).k} ELSE touched
    /\ UNCHANGED <<pstore, pseq, plog, writes, snap, rrole, rseq, restarted, acked, pprimary, marker, epoch, wrote>>

DeliverSequenced ==
    /\ chan # <<>>
    /\ Head(chan).t = "seq"
    /\ chan' = Tail(chan)
    /\ IF rrole = "replica" /\ Head(chan).q = rseq + 1
         THEN /\ rstore' = [rstore EXCEPT ![Head(chan).k] = Head(chan).v]
              /\ rseq' = Head(chan).q
         ELSE UNCHANGED <<rstore, rseq>>
    /\ UNCHANGED <<pstore, pseq, plog, writes, snap, rrole, touched, restarted, acked, pprimary, marker, epoch, wrote>>

RECURSIVE ApplyFrom(_, _, _)
ApplyFrom(store, from, to) ==
    IF from > to THEN store
    ELSE ApplyFrom([store EXCEPT ![plog[from].k] = plog[from].v], from + 1, to)

CatchUp ==
    /\ rrole = "replica"
    /\ rseq < pseq
    /\ rstore' = ApplyFrom(rstore, rseq + 1, pseq)
    /\ rseq' = pseq
    /\ UNCHANGED <<pstore, pseq, plog, writes, chan, snap, rrole, touched, restarted, acked, pprimary, marker, epoch, wrote>>

RestartValue(k) ==
    IF Replace /\ k \in DKeys THEN rstore[k]
    ELSE IF rstore[k] # None THEN rstore[k]
    ELSE pstore[k]

BecomePrimary ==
    /\ Restart
    /\ ~pprimary
    /\ ~restarted
    /\ pprimary' = TRUE
    /\ marker' = IF MarkerAfterImport THEN epoch ELSE 0
    /\ UNCHANGED <<pstore, pseq, plog, writes, chan, snap, rstore, rrole, rseq, touched,
                   restarted, acked, epoch, wrote>>

Rebalance ==
    /\ Restart
    /\ ~restarted
    /\ epoch < MaxEpoch
    /\ epoch' = epoch + 1
    /\ marker' = IF pprimary /\ MarkerAfterImport THEN epoch + 1 ELSE marker
    /\ UNCHANGED <<pstore, pseq, plog, writes, chan, snap, rstore, rrole, rseq, touched,
                   restarted, acked, pprimary, wrote>>

PrimarySince == 1

Trusted == marker # 0 /\ PrimarySince <= marker /\ marker <= epoch

PrimaryRestart ==
    /\ Restart
    /\ ~restarted
    /\ restarted' = TRUE
    /\ acked' = pstore
    /\ pstore' = IF Trusted \/ rrole # "replica" THEN pstore
                  ELSE [k \in Keys |-> RestartValue(k)]
    /\ UNCHANGED <<pseq, plog, writes, chan, snap, rstore, rrole, rseq, touched,
                   pprimary, marker, epoch, wrote>>

Next ==
    \/ \E k \in Keys, v \in Values : PrimaryWrite(k, v)
    \/ Export
    \/ Import
    \/ DeliverUnsequenced
    \/ DeliverSequenced
    \/ CatchUp
    \/ PrimaryRestart
    \/ BecomePrimary
    \/ Rebalance

Spec == Init /\ [][Next]_vars

Quiescent == rrole = "replica" /\ chan = <<>> /\ rseq = pseq

InvConverged ==
    Quiescent =>
        \A k \in Keys :
            \/ rstore[k] = pstore[k]
            \/ /\ k \in UKeys \cup GKeys
               /\ pstore[k] = None
               /\ k \notin wrote

InvAckedKept == restarted => \A k \in wrote : pstore[k] = acked[k]

NeverQuiescentAfterWrites == ~(Quiescent /\ writes = MaxWrites)
=============================================================================
