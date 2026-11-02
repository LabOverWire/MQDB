----------------------- MODULE ClusterSnapshotCatchup -----------------------
(***************************************************************************)
(* Design model for gh #268: what a replica does after a snapshot import   *)
(* to collect the writes it refused while it was awaiting that snapshot.   *)
(* ClusterSnapshotTransfer covers the chunks; here an export is one chunk.  *)
(*                                                                          *)
(* One primary P, one replica R, one partition.                             *)
(*   bulk   P->R FIFO: live writes, catchup pieces, end markers. Pieces and *)
(*          markers can be lost (inbox full); live writes cannot, so the    *)
(*          lost-live-write behaviour of gh #309 stays out of the result.   *)
(*   chunks P->R lossy FIFO, independent of bulk, so a chunk can overtake   *)
(*          bulk.                                                           *)
(*   req    R->P lossy FIFO: snapshot requests, catchup requests, gap acks. *)
(*                                                                          *)
(* Primary (handle_catchup_request, gap ack path):                          *)
(*   * a catchup request gets the log range as pieces, then a marker with   *)
(*     covers = pSeq; the range starts at the oldest entry the log holds;   *)
(*   * a gap ack gets pieces only.                                          *)
(*                                                                          *)
(* Replica (handle_catchup_response, handle_snapshot_chunk):                *)
(*   * awaiting a snapshot it refuses writes, pieces and markers;           *)
(*   * an import sets rSeq = S, opens an exchange asked from S+1;           *)
(*   * a piece starting past rSeq+1 is rejected, as is a piece hit by       *)
(*     RejectPiece, which stands for a stale epoch or a failed store: both  *)
(*     switch to awaiting a new snapshot with rSeq = 0;                     *)
(*   * a marker with next <= covers asks again if rSeq moved since the ask, *)
(*     else takes a snapshot; with next > covers it finishes unless a gap   *)
(*     remains, which takes a snapshot;                                     *)
(*   * UseIdentity: a marker answering another exchange is ignored.         *)
(*   * while catching, a timer asks again.                                  *)
(*                                                                          *)
(* Timers: RetryCatchup and RetrySnapshot fire when nothing is in flight.   *)
(* Premature firings are bounded by MaxPremature.                           *)
(*                                                                          *)
(* Configs:                                                                 *)
(*   ClusterSnapshotCatchup_noid.cfg   UseIdentity=FALSE, the code before   *)
(*                                     the marker carried answers_from:     *)
(*                                     InvCoveredOrRetrying violated, by a  *)
(*                                     marker from before the last import.  *)
(*   ClusterSnapshotCatchup_noid_live.cfg  same, liveness only: holds.      *)
(*                                     Writes are never lost here, so this  *)
(*                                     cannot show a replica stuck behind.  *)
(*                                     Inference, not a result: every write *)
(*                                     it lacks follows that marker on the  *)
(*                                     FIFO lane, so it stays behind only   *)
(*                                     if one of them is lost.              *)
(*   ClusterSnapshotCatchup.cfg        UseIdentity=TRUE: TypeOK,            *)
(*                                     InvAppliedPrefix,                    *)
(*                                     InvCoveredOrRetrying and             *)
(*                                     EventuallyConverged hold.            *)
(*   ClusterSnapshotCatchup_wide.cfg   same four, with more loss, a smaller *)
(*                                     log and split pieces.                *)
(*   ClusterSnapshotCatchup_noloss.cfg no loss, no rejection, no eviction:  *)
(*                                     those four and InvOneSnapshot hold,  *)
(*                                     for these constants.                 *)
(*   ClusterSnapshotCatchup_loss.cfg   one loss: InvOneSnapshot violated, a *)
(*                                     lost first piece costs a snapshot.   *)
(*                                                                          *)
(* NOT MODELLED: epochs and failover. The one test for a replica ahead of   *)
(* its primary sets the sequence directly; it has no restart or failover.   *)
(* Values: a write at or below rSeq changes no state here. Tests check that *)
(* a duplicate piece and a queued write a chunk overtook do not roll a      *)
(* record back, and that a write from a primary restarted at a lower        *)
(* sequence is stored. Also lost live writes (gh #309); the 5 s and 30 s    *)
(* timer lengths; old nodes. A gap ack or its pieces are never lost here,   *)
(* since main has no retry for them either. Bounded constants: verified    *)
(* for these sizes, not proven for all.                                     *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS MaxSeq, LogCap, PieceSize, MaxLoss, MaxReject, MaxPremature, UseIdentity

VARIABLES
    pSeq, bulk, chunks, req,
    role, rSeq, pending, applied, requested,
    catching, askedFrom, horizon,
    imports, losses, rejects, premature

vars == <<pSeq, bulk, chunks, req, role, rSeq, pending, applied, requested,
          catching, askedFrom, horizon, imports, losses, rejects, premature>>
primary == <<pSeq>>
counters == <<imports, losses, rejects, premature>>

Max(a, b) == IF a > b THEN a ELSE b
Min(a, b) == IF a < b THEN a ELSE b

Oldest == Max(1, pSeq - LogCap + 1)

RECURSIVE Pieces(_, _, _)
Pieces(lo, hi, kind) ==
    IF lo > hi THEN <<>>
    ELSE LET top == Min(hi, lo + PieceSize - 1)
         IN <<[t |-> "piece", a |-> lo, b |-> top, x |-> kind]>>
            \o Pieces(top + 1, hi, kind)

Init ==
    /\ pSeq = 0 /\ bulk = <<>> /\ chunks = <<>>
    /\ req = <<[t |-> "snap", from |-> 0]>>
    /\ role = "Awaiting" /\ rSeq = 0 /\ pending = {} /\ applied = {}
    /\ requested = TRUE /\ catching = FALSE /\ askedFrom = 0 /\ horizon = 0
    /\ imports = 0 /\ losses = 0 /\ rejects = 0 /\ premature = 0

(***************************** primary *************************************)
Write ==
    /\ pSeq < MaxSeq
    /\ pSeq' = pSeq + 1
    /\ bulk' = Append(bulk, [t |-> "write", s |-> pSeq + 1])
    /\ UNCHANGED <<chunks, req, role, rSeq, pending, applied, requested,
                   catching, askedFrom, horizon, counters>>

PrimaryRecv ==
    /\ req /= <<>>
    /\ LET m == Head(req) IN
       /\ req' = Tail(req)
       /\ IF m.t = "snap"
          THEN /\ chunks' = Append(chunks, pSeq)
               /\ UNCHANGED bulk
          ELSE /\ LET ps == Pieces(Max(m.from, Oldest), pSeq, m.t)
                  IN bulk' = bulk \o ps \o
                       (IF m.t = "catchup"
                        THEN <<[t |-> "marker", covers |-> pSeq,
                                answers |-> m.from]>>
                        ELSE <<>>)
               /\ UNCHANGED chunks
    /\ UNCHANGED <<pSeq, role, rSeq, pending, applied, requested, catching,
                   askedFrom, horizon, counters>>

(***************************** replica *************************************)
\* Switching to a snapshot: rSeq = 0, stored data stays.
ToSnapshot(rest) ==
    /\ role' = "Awaiting" /\ rSeq' = 0 /\ pending' = {}
    /\ requested' = TRUE /\ catching' = FALSE
    /\ req' = Append(req, [t |-> "snap", from |-> 0])
    /\ bulk' = rest
    /\ UNCHANGED <<pSeq, chunks, applied, askedFrom, horizon, imports, losses,
                   premature>>

Ask(rest) ==
    /\ askedFrom' = rSeq + 1
    /\ req' = Append(req, [t |-> "catchup", from |-> rSeq + 1])
    /\ bulk' = rest
    /\ UNCHANGED <<pSeq, chunks, role, rSeq, pending, applied, requested,
                   catching, horizon, counters>>

Drop(rest) ==
    /\ bulk' = rest
    /\ UNCHANGED <<pSeq, chunks, req, role, rSeq, pending, applied, requested,
                   catching, askedFrom, horizon, counters>>

ApplyThrough(top, rest) ==
    /\ rSeq' = top
    /\ applied' = applied \cup ((rSeq + 1)..top)
    /\ pending' = {p \in pending : p > top}
    /\ bulk' = rest
    /\ UNCHANGED <<pSeq, chunks, req, role, requested, catching, askedFrom,
                   horizon, counters>>

ReplicaWrite(m, rest) ==
    IF m.s <= rSeq THEN Drop(rest)
    ELSE IF m.s = rSeq + 1 THEN ApplyThrough(m.s, rest)
    ELSE /\ pending' = pending \cup {m.s}
         /\ req' = Append(req, [t |-> "gap", from |-> rSeq + 1])
         /\ bulk' = rest
         /\ UNCHANGED <<pSeq, chunks, role, rSeq, applied, requested, catching,
                        askedFrom, horizon, counters>>

ReplicaPiece(m, rest) ==
    IF m.b <= rSeq THEN Drop(rest)
    ELSE IF m.a > rSeq + 1 THEN ToSnapshot(rest) /\ UNCHANGED rejects
    ELSE ApplyThrough(m.b, rest)

ReplicaMarker(m, rest) ==
    IF ~catching
    THEN IF pending /= {} THEN ToSnapshot(rest) /\ UNCHANGED rejects
         ELSE Drop(rest)
    ELSE IF UseIdentity /\ m.answers /= askedFrom THEN Drop(rest)
    ELSE IF rSeq + 1 <= m.covers
         THEN IF rSeq + 1 > askedFrom THEN Ask(rest)
              ELSE ToSnapshot(rest) /\ UNCHANGED rejects
    ELSE IF pending /= {} THEN ToSnapshot(rest) /\ UNCHANGED rejects
    ELSE /\ catching' = FALSE
         /\ bulk' = rest
         /\ UNCHANGED <<pSeq, chunks, req, role, rSeq, pending, applied,
                        requested, askedFrom, horizon, counters>>

ReplicaBulk ==
    /\ bulk /= <<>>
    /\ LET m == Head(bulk)
           rest == Tail(bulk)
       IN IF role = "Awaiting" THEN Drop(rest)
          ELSE CASE m.t = "write"  -> ReplicaWrite(m, rest)
               []   m.t = "marker" -> ReplicaMarker(m, rest)
               []   OTHER          -> ReplicaPiece(m, rest)

\* A piece refused for a reason other than a hole: stale epoch, failed store.
RejectPiece ==
    /\ rejects < MaxReject
    /\ role = "Replica"
    /\ bulk /= <<>>
    /\ Head(bulk).t = "piece"
    /\ Head(bulk).b > rSeq
    /\ rejects' = rejects + 1
    /\ ToSnapshot(Tail(bulk))

ReplicaChunk ==
    /\ chunks /= <<>>
    /\ chunks' = Tail(chunks)
    /\ IF role = "Awaiting" /\ requested
       THEN LET s == Head(chunks) IN
            /\ role' = "Replica" /\ rSeq' = s /\ pending' = {}
            /\ applied' = applied \cup (1..s)
            /\ requested' = FALSE
            /\ catching' = TRUE /\ askedFrom' = s + 1
            /\ horizon' = pSeq
            /\ imports' = imports + 1
            /\ req' = Append(req, [t |-> "catchup", from |-> s + 1])
            /\ UNCHANGED <<pSeq, bulk, losses, rejects, premature>>
       ELSE UNCHANGED <<pSeq, bulk, req, role, rSeq, pending, applied, requested,
                        catching, askedFrom, horizon, counters>>

Idle == bulk = <<>> /\ chunks = <<>> /\ req = <<>>

AskAgain ==
    /\ catching
    /\ askedFrom' = rSeq + 1
    /\ req' = Append(req, [t |-> "catchup", from |-> rSeq + 1])
    /\ UNCHANGED <<pSeq, bulk, chunks, role, rSeq, pending, applied, requested,
                   catching, horizon>>

RetryCatchup == Idle /\ AskAgain /\ UNCHANGED counters

PrematureCatchup ==
    /\ premature < MaxPremature /\ ~Idle
    /\ AskAgain
    /\ premature' = premature + 1
    /\ UNCHANGED <<imports, losses, rejects>>

RetrySnapshot ==
    /\ Idle /\ role = "Awaiting" /\ requested
    /\ req' = Append(req, [t |-> "snap", from |-> 0])
    /\ UNCHANGED <<pSeq, bulk, chunks, role, rSeq, pending, applied, requested,
                   catching, askedFrom, horizon, counters>>

(******************************* loss **************************************)
LoseBulk ==
    /\ losses < MaxLoss /\ bulk /= <<>>
    /\ \/ Head(bulk).t = "marker"
       \/ Head(bulk).t = "piece" /\ Head(bulk).x = "catchup"
    /\ bulk' = Tail(bulk)
    /\ losses' = losses + 1
    /\ UNCHANGED <<pSeq, chunks, req, role, rSeq, pending, applied, requested,
                   catching, askedFrom, horizon, imports, rejects, premature>>

LoseReq ==
    /\ losses < MaxLoss /\ req /= <<>>
    /\ Head(req).t /= "gap"
    /\ req' = Tail(req)
    /\ losses' = losses + 1
    /\ UNCHANGED <<pSeq, bulk, chunks, role, rSeq, pending, applied, requested,
                   catching, askedFrom, horizon, imports, rejects, premature>>

LoseChunk ==
    /\ losses < MaxLoss /\ chunks /= <<>>
    /\ chunks' = Tail(chunks)
    /\ losses' = losses + 1
    /\ UNCHANGED <<pSeq, bulk, req, role, rSeq, pending, applied, requested,
                   catching, askedFrom, horizon, imports, rejects, premature>>

Next ==
    \/ Write \/ PrimaryRecv \/ ReplicaBulk \/ RejectPiece \/ ReplicaChunk
    \/ RetryCatchup \/ PrematureCatchup \/ RetrySnapshot
    \/ LoseBulk \/ LoseReq \/ LoseChunk

Fairness ==
    /\ WF_vars(Write) /\ WF_vars(PrimaryRecv) /\ WF_vars(ReplicaBulk)
    /\ WF_vars(ReplicaChunk) /\ WF_vars(RetryCatchup) /\ WF_vars(RetrySnapshot)

Spec == Init /\ [][Next]_vars /\ Fairness

(**************************** properties ***********************************)
TypeOK ==
    /\ pSeq \in 0..MaxSeq /\ rSeq \in 0..MaxSeq /\ horizon \in 0..MaxSeq
    /\ role \in {"Awaiting", "Replica"}
    /\ catching \in BOOLEAN /\ requested \in BOOLEAN
    /\ pending \subseteq 1..MaxSeq /\ applied \subseteq 1..MaxSeq

\* A replica that has stopped catching up holds what the primary had at import.
InvCoveredOrRetrying == (role = "Replica" /\ ~catching) => rSeq >= horizon

\* The sequence never passes what the store holds.
InvAppliedPrefix == (1..rSeq) \subseteq applied

\* Without loss, rejection or eviction one snapshot is enough.
InvOneSnapshot == imports <= 1

Converged == pSeq = MaxSeq /\ role = "Replica" /\ rSeq = MaxSeq
EventuallyConverged == <>[]Converged
=============================================================================
