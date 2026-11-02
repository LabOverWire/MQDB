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
(*   chunks P->R lossy FIFO, separate from bulk. UseFence: a chunk cannot   *)
(*          pass bulk frames queued before its export, which is the writer  *)
(*          holding chunk 0 back; it can pass anything queued later. An     *)
(*          export is one chunk, standing for chunk 0 of a transfer: the    *)
(*          later chunks follow it and import only happens after them.      *)
(*          The fence is an element placed in bulk at export, not the       *)
(*          writer's per-connection counter, so this model cannot show a    *)
(*          bug in that counter or what replacing a connection does to it.  *)
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
(* Configs (all UseFence=TRUE and UseIdentity=TRUE unless stated):         *)
(*   ClusterSnapshotCatchup.cfg        TypeOK, InvAppliedPrefix,            *)
(*                                     InvCoveredOrRetrying,                *)
(*                                     InvNoOvertakenWrite and              *)
(*                                     EventuallyConverged hold.            *)
(*   ClusterSnapshotCatchup_wide.cfg   same five, with more loss, a smaller *)
(*                                     log and split pieces.                *)
(*   ClusterSnapshotCatchup_noloss.cfg no loss, no rejection, no eviction:  *)
(*                                     those five and InvOneSnapshot hold,  *)
(*                                     for these constants.                 *)
(*   ClusterSnapshotCatchup_loss.cfg   one loss: InvOneSnapshot violated, a *)
(*                                     lost first piece costs a snapshot.   *)
(*   ClusterSnapshotCatchup_nofence.cfg UseFence=FALSE:                     *)
(*                                     InvNoOvertakenWrite violated.        *)
(*   ClusterSnapshotCatchup_noid.cfg   UseFence=FALSE, UseIdentity=FALSE:   *)
(*                                     InvCoveredOrRetrying violated, by a  *)
(*                                     marker from before the last import.  *)
(*   ClusterSnapshotCatchup_noid_live.cfg  same, liveness only: holds.      *)
(*                                     Writes are never lost here, so this  *)
(*                                     cannot show a replica stuck behind.  *)
(*   ClusterSnapshotCatchup_noid_fence.cfg UseIdentity=FALSE with the fence: *)
(*                                     all five hold. On one stream the     *)
(*                                     fence alone stops a late marker, so  *)
(*                                     UseIdentity is for a marker on a     *)
(*                                     replaced connection, not modelled.   *)
(*                                                                          *)
(* NOT MODELLED: epochs and failover. Tests have a replica ahead of its     *)
(* primary two ways, by setting the sequence directly and with a second     *)
(* primary at a lower sequence; neither is a real restart. Values: a write  *)
(* at or below rSeq changes no state here, though the code stores it as     *)
(* main does; tests check a duplicate catchup piece does not roll a record  *)
(* back and that a write from a lower-sequence primary is stored. Also lost *)
(* live writes (gh #309); bounded catchup requests, which only the          *)
(* test-only tick() sends; a connection replaced with writes still on the   *)
(* old stream; timer lengths; old nodes. A gap ack or its pieces are never  *)
(* lost here,                                                               *)
(* since main has no retry for them either. Bounded constants: verified    *)
(* for these sizes, not proven for all.                                     *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS MaxSeq, LogCap, PieceSize, MaxLoss, MaxReject, MaxPremature, UseIdentity, UseFence

VARIABLES
    pSeq, bulk, chunks, req,
    role, rSeq, pending, applied, requested,
    catching, askedFrom, horizon,
    imports, losses, rejects, premature, exports, importedAt

vars == <<pSeq, bulk, chunks, req, role, rSeq, pending, applied, requested,
          catching, askedFrom, horizon, imports, losses, rejects, premature,
          exports, importedAt>>
primary == <<pSeq>>
counters == <<imports, losses, rejects, premature>>
fence == <<exports, importedAt>>

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
    /\ exports = 0 /\ importedAt = 0

(***************************** primary *************************************)
Write ==
    /\ pSeq < MaxSeq
    /\ pSeq' = pSeq + 1
    /\ bulk' = Append(bulk, [t |-> "write", s |-> pSeq + 1])
    /\ UNCHANGED <<exports, importedAt, chunks, req, role, rSeq, pending, applied, requested,
                   catching, askedFrom, horizon, counters>>

PrimaryRecv ==
    /\ req /= <<>>
    /\ LET m == Head(req) IN
       /\ req' = Tail(req)
       /\ IF m.t = "snap"
          THEN /\ exports' = exports + 1
               /\ chunks' = Append(chunks, [s |-> pSeq, id |-> exports + 1])
               /\ bulk' = IF UseFence
                          THEN Append(bulk, [t |-> "fence", id |-> exports + 1])
                          ELSE bulk
          ELSE /\ LET ps == Pieces(Max(m.from, Oldest), pSeq, m.t)
                  IN bulk' = bulk \o ps \o
                       (IF m.t = "catchup"
                        THEN <<[t |-> "marker", covers |-> pSeq,
                                answers |-> m.from]>>
                        ELSE <<>>)
               /\ UNCHANGED <<chunks, exports>>
    /\ UNCHANGED <<importedAt, pSeq, role, rSeq, pending, applied, requested, catching,
                   askedFrom, horizon, counters>>

(***************************** replica *************************************)
\* Switching to a snapshot: rSeq = 0, stored data stays.
ToSnapshot(rest) ==
    /\ role' = "Awaiting" /\ rSeq' = 0 /\ pending' = {}
    /\ requested' = TRUE /\ catching' = FALSE
    /\ req' = Append(req, [t |-> "snap", from |-> 0])
    /\ bulk' = rest
    /\ UNCHANGED <<exports, importedAt, pSeq, chunks, applied, askedFrom, horizon, imports, losses,
                   premature>>

Ask(rest) ==
    /\ askedFrom' = rSeq + 1
    /\ req' = Append(req, [t |-> "catchup", from |-> rSeq + 1])
    /\ bulk' = rest
    /\ UNCHANGED <<exports, importedAt, pSeq, chunks, role, rSeq, pending, applied, requested,
                   catching, horizon, counters>>

Drop(rest) ==
    /\ bulk' = rest
    /\ UNCHANGED <<exports, importedAt, pSeq, chunks, req, role, rSeq, pending, applied, requested,
                   catching, askedFrom, horizon, counters>>

ApplyThrough(top, rest) ==
    /\ rSeq' = top
    /\ applied' = applied \cup ((rSeq + 1)..top)
    /\ pending' = {p \in pending : p > top}
    /\ bulk' = rest
    /\ UNCHANGED <<exports, importedAt, pSeq, chunks, req, role, requested, catching, askedFrom,
                   horizon, counters>>

ReplicaWrite(m, rest) ==
    IF m.s <= rSeq THEN Drop(rest)
    ELSE IF m.s = rSeq + 1 THEN ApplyThrough(m.s, rest)
    ELSE /\ pending' = pending \cup {m.s}
         /\ req' = Append(req, [t |-> "gap", from |-> rSeq + 1])
         /\ bulk' = rest
         /\ UNCHANGED <<exports, importedAt, pSeq, chunks, role, rSeq, applied, requested, catching,
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
         /\ UNCHANGED <<exports, importedAt, pSeq, chunks, req, role, rSeq, pending, applied,
                        requested, askedFrom, horizon, counters>>

ReplicaBulk ==
    /\ bulk /= <<>>
    /\ LET m == Head(bulk)
           rest == Tail(bulk)
       IN IF role = "Awaiting" THEN Drop(rest)
          ELSE CASE m.t = "fence"  -> Drop(rest)
               []   m.t = "write"  -> ReplicaWrite(m, rest)
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

FenceAhead(id) ==
    \E i \in 1..Len(bulk) : bulk[i].t = "fence" /\ bulk[i].id = id

ReplicaChunk ==
    /\ chunks /= <<>>
    /\ ~FenceAhead(Head(chunks).id)
    /\ chunks' = Tail(chunks)
    /\ IF role = "Awaiting" /\ requested
       THEN LET s == Head(chunks).s IN
            /\ role' = "Replica" /\ rSeq' = s /\ pending' = {}
            /\ applied' = applied \cup (1..s)
            /\ requested' = FALSE
            /\ catching' = TRUE /\ askedFrom' = s + 1
            /\ horizon' = pSeq
            /\ imports' = imports + 1
            /\ importedAt' = s
            /\ req' = Append(req, [t |-> "catchup", from |-> s + 1])
            /\ UNCHANGED <<exports, pSeq, bulk, losses, rejects, premature>>
       ELSE UNCHANGED <<exports, importedAt, pSeq, bulk, req, role, rSeq, pending, applied, requested,
                        catching, askedFrom, horizon, counters>>

Idle == bulk = <<>> /\ chunks = <<>> /\ req = <<>>

AskAgain ==
    /\ catching
    /\ askedFrom' = rSeq + 1
    /\ req' = Append(req, [t |-> "catchup", from |-> rSeq + 1])
    /\ UNCHANGED <<exports, importedAt, pSeq, bulk, chunks, role, rSeq, pending, applied, requested,
                   catching, horizon>>

RetryCatchup == Idle /\ AskAgain /\ UNCHANGED counters

PrematureCatchup ==
    /\ premature < MaxPremature /\ ~Idle
    /\ AskAgain
    /\ premature' = premature + 1
    /\ UNCHANGED <<exports, importedAt, imports, losses, rejects>>

RetrySnapshot ==
    /\ Idle /\ role = "Awaiting" /\ requested
    /\ req' = Append(req, [t |-> "snap", from |-> 0])
    /\ UNCHANGED <<exports, importedAt, pSeq, bulk, chunks, role, rSeq, pending, applied, requested,
                   catching, askedFrom, horizon, counters>>

(******************************* loss **************************************)
LoseBulk ==
    /\ losses < MaxLoss /\ bulk /= <<>>
    /\ \/ Head(bulk).t = "marker"
       \/ Head(bulk).t = "piece" /\ Head(bulk).x = "catchup"
    /\ bulk' = Tail(bulk)
    /\ losses' = losses + 1
    /\ UNCHANGED <<exports, importedAt, pSeq, chunks, req, role, rSeq, pending, applied, requested,
                   catching, askedFrom, horizon, imports, rejects, premature>>

LoseReq ==
    /\ losses < MaxLoss /\ req /= <<>>
    /\ Head(req).t /= "gap"
    /\ req' = Tail(req)
    /\ losses' = losses + 1
    /\ UNCHANGED <<exports, importedAt, pSeq, bulk, chunks, role, rSeq, pending, applied, requested,
                   catching, askedFrom, horizon, imports, rejects, premature>>

LoseChunk ==
    /\ losses < MaxLoss /\ chunks /= <<>>
    /\ chunks' = Tail(chunks)
    /\ losses' = losses + 1
    /\ UNCHANGED <<exports, importedAt, pSeq, bulk, req, role, rSeq, pending, applied, requested,
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

\* No write at or below the last import is still queued: a chunk cannot pass a
\* write queued before its export.
InvNoOvertakenWrite ==
    \A i \in 1..Len(bulk) :
        bulk[i].t = "write" => bulk[i].s > importedAt

Converged == pSeq = MaxSeq /\ role = "Replica" /\ rSeq = MaxSeq
EventuallyConverged == <>[]Converged
=============================================================================
