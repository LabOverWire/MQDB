----------------------- MODULE ClusterSnapshotTransfer -----------------------
(***************************************************************************)
(* Design model for gh #268: multi-chunk partition snapshot transfer.      *)
(*                                                                          *)
(* One receiver R wants one partition snapshot. Senders are the nodes R     *)
(* may request it from; the partition primary can move between them        *)
(* (Reassign). Each sender->R link is a FIFO channel (one QUIC stream per   *)
(* peer, bulk lane) that may LOSE messages (inbox full drop, a connection   *)
(* replaced mid-transfer). R->sender requests travel on a lossy FIFO too.   *)
(*                                                                          *)
(* Sender (handle_snapshot_request + paced pump):                           *)
(*   * a request REPLACES any transfer to R with a fresh export (new         *)
(*     transfer id, fresh chunk count) and starts at chunk 0;                *)
(*   * chunks are queued in index order; a full queue only delays (never     *)
(*     skips) a chunk (Cap);                                                *)
(*   * a "cancel" (SnapshotComplete failed from R) drops the transfer.       *)
(*   SendAll = FALSE is main: only chunk 0 is ever sent.                     *)
(*                                                                          *)
(* Receiver (handle_snapshot_chunk + drive_snapshots):                      *)
(*   * one outstanding request per partition, remembering its source;       *)
(*     chunks from any other node are dropped;                              *)
(*   * chunk 0 starts a fresh builder; a later chunk is accepted only if it  *)
(*     extends the builder in order (same transfer id when UseTransferId,    *)
(*     same total, index == next); a duplicate/stale chunk is dropped; a     *)
(*     gap in the same transfer aborts (clear + cancel to the sender);       *)
(*   * StallTimeout: when nothing more can arrive the request is abandoned  *)
(*     and re-issued to the current primary (Retry). PrematureTimeout        *)
(*     models a slow sender tripping the timer while chunks are still in    *)
(*     flight.                                                              *)
(*   Retry = FALSE is main: the production loop never retries.               *)
(*                                                                          *)
(* Configs:                                                                 *)
(*   ClusterSnapshotTransfer.cfg        fixed design: InvWholeImport holds, *)
(*                                      EventuallyImported holds.           *)
(*   ClusterSnapshotTransfer_wide.cfg   fixed design, MaxLoss=2,            *)
(*                                      MaxExports=4: both properties hold. *)
(*   ClusterSnapshotTransfer_main.cfg   SendAll=FALSE, Retry=FALSE, no      *)
(*                                      loss -> EventuallyImported violated *)
(*                                      (stuck after chunk 0 of a 2-chunk   *)
(*                                      export).                            *)
(*   ClusterSnapshotTransfer_noid.cfg   UseTransferId=FALSE -> InvWhole-    *)
(*                                      Import violated: a retried request  *)
(*                                      plus one lost chunk 0 splices two    *)
(*                                      exports into one import.             *)
(*   ClusterSnapshotTransfer_noretry.cfg Retry=FALSE with sending fixed ->  *)
(*                                      EventuallyImported violated after a *)
(*                                      single lost chunk.                  *)
(*                                                                          *)
(* NOT MODELLED: the content of an export (an import is "whole" when all   *)
(* its chunks come from one export); sequence numbers and role changes on  *)
(* import; more than one partition (transfers for different partitions     *)
(* are independent state keyed by partition); legacy senders, which only    *)
(* ever send chunk 0 (a single-chunk transfer from them is exactly a        *)
(* SendAll transfer with total 1). Bounded constants: verified for these   *)
(* sizes, not proven for all.                                               *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    Senders,
    MaxChunks,
    Cap,
    MaxLoss,
    MaxPremature,
    MaxReassign,
    MaxExports,
    SendAll,
    Retry,
    UseTransferId

None == 0

VARIABLES
    out,
    chan,
    reqChan,
    req,
    bld,
    primary,
    exports,
    requests,
    losses,
    premature,
    reassigns,
    imports,
    done

vars == <<out, chan, reqChan, req, bld, primary, exports, requests, losses,
          premature, reassigns, imports, done>>

Chunk == [id : 1..MaxExports, idx : 0..(MaxChunks - 1), total : 1..MaxChunks]

NoTransfer == [active |-> FALSE, id |-> 0, next |-> 0, total |-> 1]
NoBuilder == [active |-> FALSE, id |-> 0, next |-> 0, total |-> 1, ids |-> {}]

TypeOK ==
    /\ out \in [Senders -> [active : BOOLEAN, id : 0..MaxExports,
                            next : 0..MaxChunks, total : 1..MaxChunks]]
    /\ \A s \in Senders : chan[s] \in Seq(Chunk) /\ Len(chan[s]) <= Cap
    /\ \A s \in Senders : reqChan[s] \in Seq({"req", "cancel"})
    /\ req \in Senders \cup {None}
    /\ bld.active \in BOOLEAN
    /\ primary \in Senders
    /\ exports \in 0..MaxExports
    /\ done \in BOOLEAN

Init ==
    /\ out = [s \in Senders |-> NoTransfer]
    /\ chan = [s \in Senders |-> <<>>]
    /\ reqChan = [s \in Senders |-> <<>>]
    /\ req = None
    /\ bld = NoBuilder
    /\ primary \in Senders
    /\ exports = 0
    /\ requests = 0
    /\ losses = 0
    /\ premature = 0
    /\ reassigns = 0
    /\ imports = {}
    /\ done = FALSE

Request ==
    /\ ~done
    /\ req = None
    /\ Retry \/ requests = 0
    /\ requests < MaxExports
    /\ req' = primary
    /\ reqChan' = [reqChan EXCEPT ![primary] = Append(@, "req")]
    /\ requests' = requests + 1
    /\ bld' = NoBuilder
    /\ UNCHANGED <<out, chan, primary, exports, losses, premature, reassigns,
                   imports, done>>

SenderRecv(s) ==
    /\ reqChan[s] # <<>>
    /\ reqChan' = [reqChan EXCEPT ![s] = Tail(@)]
    /\ IF Head(reqChan[s]) = "req"
       THEN /\ exports < MaxExports
            /\ \E t \in 1..MaxChunks :
                 out' = [out EXCEPT ![s] = [active |-> TRUE, id |-> exports + 1,
                                            next |-> 0, total |-> t]]
            /\ exports' = exports + 1
       ELSE /\ out' = [out EXCEPT ![s] = NoTransfer]
            /\ UNCHANGED exports
    /\ UNCHANGED <<chan, req, bld, primary, requests, losses, premature,
                   reassigns, imports, done>>

CanSend(s) ==
    /\ out[s].active
    /\ SendAll \/ out[s].next = 0

SendChunk(s) ==
    /\ CanSend(s)
    /\ Len(chan[s]) < Cap
    /\ chan' = [chan EXCEPT ![s] = Append(@, [id |-> out[s].id,
                                               idx |-> out[s].next,
                                               total |-> out[s].total])]
    /\ out' = [out EXCEPT ![s] =
                 IF @.next + 1 >= @.total THEN NoTransfer
                 ELSE [@ EXCEPT !.next = @ + 1]]
    /\ UNCHANGED <<reqChan, req, bld, primary, exports, requests, losses,
                   premature, reassigns, imports, done>>

Import(ids) ==
    /\ imports' = imports \cup {ids}
    /\ done' = TRUE
    /\ req' = None
    /\ bld' = NoBuilder
    /\ UNCHANGED reqChan

Abort(s) ==
    /\ req' = None
    /\ bld' = NoBuilder
    /\ reqChan' = [reqChan EXCEPT ![s] = Append(@, "cancel")]
    /\ UNCHANGED <<imports, done>>

Drop ==
    UNCHANGED <<req, bld, reqChan, imports, done>>

SameTransfer(c) == ~UseTransferId \/ c.id = bld.id

ReceiverRecv(s) ==
    /\ chan[s] # <<>>
    /\ LET c == Head(chan[s]) IN
       /\ chan' = [chan EXCEPT ![s] = Tail(@)]
       /\ IF req # s THEN Drop
          ELSE IF c.idx = 0 THEN
              IF c.total = 1 THEN Import({c.id})
              ELSE /\ bld' = [active |-> TRUE, id |-> c.id, next |-> 1,
                              total |-> c.total, ids |-> {c.id}]
                   /\ UNCHANGED <<req, reqChan, imports, done>>
          ELSE IF ~bld.active THEN Drop
          ELSE IF ~SameTransfer(c) THEN Drop
          ELSE IF c.total # bld.total THEN Abort(s)
          ELSE IF c.idx < bld.next THEN Drop
          ELSE IF c.idx > bld.next THEN Abort(s)
          ELSE IF c.idx + 1 = bld.total THEN Import(bld.ids \cup {c.id})
          ELSE /\ bld' = [bld EXCEPT !.next = @ + 1, !.ids = @ \cup {c.id}]
               /\ UNCHANGED <<req, reqChan, imports, done>>
    /\ UNCHANGED <<out, primary, exports, requests, losses, premature,
                   reassigns>>

Stalled ==
    /\ req # None
    /\ chan[req] = <<>>
    /\ reqChan[req] = <<>>
    /\ ~CanSend(req)

StallTimeout ==
    /\ Retry
    /\ Stalled
    /\ req' = None
    /\ bld' = NoBuilder
    /\ UNCHANGED <<out, chan, reqChan, primary, exports, requests, losses,
                   premature, reassigns, imports, done>>

PrematureTimeout ==
    /\ Retry
    /\ req # None
    /\ premature < MaxPremature
    /\ premature' = premature + 1
    /\ req' = None
    /\ bld' = NoBuilder
    /\ UNCHANGED <<out, chan, reqChan, primary, exports, requests, losses,
                   reassigns, imports, done>>

LoseChunk(s) ==
    /\ losses < MaxLoss
    /\ \E i \in 1..Len(chan[s]) :
         chan' = [chan EXCEPT ![s] = SubSeq(@, 1, i - 1) \o SubSeq(@, i + 1, Len(@))]
    /\ losses' = losses + 1
    /\ UNCHANGED <<out, reqChan, req, bld, primary, exports, requests,
                   premature, reassigns, imports, done>>

LoseRequest(s) ==
    /\ losses < MaxLoss
    /\ \E i \in 1..Len(reqChan[s]) :
         reqChan' = [reqChan EXCEPT ![s] = SubSeq(@, 1, i - 1) \o SubSeq(@, i + 1, Len(@))]
    /\ losses' = losses + 1
    /\ UNCHANGED <<out, chan, req, bld, primary, exports, requests,
                   premature, reassigns, imports, done>>

Reassign ==
    /\ reassigns < MaxReassign
    /\ \E p \in Senders \ {primary} : primary' = p
    /\ reassigns' = reassigns + 1
    /\ UNCHANGED <<out, chan, reqChan, req, bld, exports, requests, losses,
                   premature, imports, done>>

Next ==
    \/ Request
    \/ \E s \in Senders : SenderRecv(s) \/ SendChunk(s) \/ ReceiverRecv(s)
    \/ StallTimeout
    \/ PrematureTimeout
    \/ \E s \in Senders : LoseChunk(s) \/ LoseRequest(s)
    \/ Reassign

Fairness ==
    /\ WF_vars(Request)
    /\ \A s \in Senders : WF_vars(SenderRecv(s))
    /\ \A s \in Senders : WF_vars(SendChunk(s))
    /\ \A s \in Senders : WF_vars(ReceiverRecv(s))
    /\ WF_vars(StallTimeout)

Spec == Init /\ [][Next]_vars /\ Fairness

InvWholeImport == \A ids \in imports : Cardinality(ids) = 1

EventuallyImported == <>done
==============================================================================
