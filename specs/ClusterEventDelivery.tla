------------------------- MODULE ClusterEventDelivery -------------------------
(***************************************************************************)
(* CHANGE-EVENT DELIVERY for one partition across a primary failover      *)
(* (gh #199, "events can be lost").                                         *)
(*                                                                          *)
(* Node 1 starts as primary, node 2 is its replica, S is a node with a      *)
(* subscriber. A write id is 10 * writer's epoch + a counter, standing for *)
(* the code's (epoch, seq): the new primary's writes order after the old   *)
(* one's, including writes a cut-off old primary keeps making.             *)
(*                                                                          *)
(* VERIFIED FACTS MODELLED (Design = "current"):                            *)
(*  - data and outbox entry commit in one batch, on the primary only        *)
(*    (session_ops.rs replicate_write_async_with_extra; the Write sent to   *)
(*    the replica carries no outbox entry)                                  *)
(*  - the entry is removed right after the event is handed to the send      *)
(*    queue (db_ops.rs publish_and_deliver_change_event), whether or not    *)
(*    the send succeeded: forward failure only warns, the local queue       *)
(*    drops when full (quic_transport.rs enqueue_local_publish), and the    *)
(*    receiving node drops when its inbox is full                           *)
(*  - replication is async; a crash loses writes not yet replicated         *)
(*  - on startup a node replays its whole outbox before it knows which      *)
(*    partitions it holds (event_loop.rs recover_pending_outbox)            *)
(*                                                                          *)
(* Design = "acked" (candidate):                                            *)
(*  - the outbox entry rides with the replication write                     *)
(*  - a node that believes it is primary sends pending events in order,     *)
(*    one at a time per target, and removes one only on the target's ack,   *)
(*    then tells the replica                                                *)
(*  - the target delivers an event only if its id is above the last one it *)
(*    delivered, and acks either way                                        *)
(*  - a node that is no longer primary stops sending and keeps its copy    *)
(*                                                                          *)
(* Zombies = TRUE lets the replica be promoted while node 1 is still        *)
(* running and cut off from it; node 1 keeps writing and sending until it  *)
(* learns. Its writes after the cut are lost by async replication in any   *)
(* design, so NoLostWriteAfterFailover is only checked without zombies.    *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS MaxWrites, MaxLoss, MaxNet, Design, Zombies

Nodes == {1, 2}
Acked == Design = "acked"

VARIABLES primary, epoch, alive, believes, restarted, hist, pending, repl,
          net, acks, inflight, delivered, lastW, nextId, losses, lost

vars == <<primary, epoch, alive, believes, restarted, hist, pending, repl,
          net, acks, inflight, delivered, lastW, nextId, losses, lost>>

Min(s) == CHOOSE x \in s : \A y \in s : x <= y
Range(s) == {s[i] : i \in DOMAIN s}
Remove(s, i) == SubSeq(s, 1, i - 1) \o SubSeq(s, i + 1, Len(s))
DeliveredIds == {delivered[i].w : i \in DOMAIN delivered}
Ids == {10 * e + n : e \in Nodes, n \in 1..MaxWrites}
WriterEpoch(p) == p

TypeOK ==
    /\ primary \in Nodes
    /\ epoch \in {1, 2}
    /\ nextId \in 1..(MaxWrites + 1)
    /\ losses \in 0..MaxLoss
    /\ lastW \in {0} \cup Ids
    /\ \A n \in Nodes : pending[n] \subseteq Ids
    /\ \A n \in Nodes : inflight[n] \in {0} \cup Ids
    /\ Len(net) <= MaxNet

Init ==
    /\ primary = 1
    /\ epoch = 1
    /\ alive = [n \in Nodes |-> TRUE]
    /\ believes = [n \in Nodes |-> n = 1]
    /\ restarted = [n \in Nodes |-> FALSE]
    /\ hist = [n \in Nodes |-> <<>>]
    /\ pending = [n \in Nodes |-> {}]
    /\ repl = <<>>
    /\ net = <<>>
    /\ acks = <<>>
    /\ inflight = [n \in Nodes |-> 0]
    /\ delivered = <<>>
    /\ lastW = 0
    /\ nextId = 1
    /\ losses = 0
    /\ lost = {}

Write(p) ==
    LET w == 10 * WriterEpoch(p) + nextId
        zombie == p = 1 /\ primary = 2
    IN /\ nextId <= MaxWrites
       /\ alive[p]
       /\ believes[p]
       /\ Acked \/ pending[p] = {}
       /\ hist' = [hist EXCEPT ![p] = Append(@, w)]
       /\ pending' = [pending EXCEPT ![p] = @ \cup {w}]
       /\ repl' = IF p = 1 /\ primary = 1
                  THEN Append(repl, [kind |-> "write", w |-> w])
                  ELSE repl
       /\ lost' = IF zombie THEN lost \cup {w} ELSE lost
       /\ nextId' = nextId + 1
       /\ UNCHANGED <<primary, epoch, alive, believes, restarted, net, acks,
                      inflight, delivered, lastW, losses>>

Replicate ==
    /\ repl # <<>>
    /\ LET m == Head(repl)
           isWrite == m.kind = "write"
       IN /\ hist' = IF isWrite THEN [hist EXCEPT ![2] = Append(@, m.w)] ELSE hist
          /\ pending' = CASE isWrite /\ Acked -> [pending EXCEPT ![2] = @ \cup {m.w}]
                          [] isWrite -> pending
                          [] OTHER -> [pending EXCEPT ![2] = @ \ {m.w}]
    /\ repl' = Tail(repl)
    /\ UNCHANGED <<primary, epoch, alive, believes, restarted, net, acks,
                   inflight, delivered, lastW, nextId, losses, lost>>

CurrentSend(p) ==
    /\ ~Acked
    /\ alive[p]
    /\ \E w \in pending[p] :
         /\ restarted[p] \/ w = Min(pending[p])
         /\ pending' = [pending EXCEPT ![p] = @ \ {w}]
         /\ \/ /\ Len(net) < MaxNet
               /\ net' = Append(net, [w |-> w, from |-> p])
               /\ UNCHANGED losses
            \/ /\ losses < MaxLoss
               /\ losses' = losses + 1
               /\ UNCHANGED net
    /\ UNCHANGED <<primary, epoch, alive, believes, restarted, hist, repl, acks,
                   inflight, delivered, lastW, nextId, lost>>

AckedSend(p) ==
    /\ Acked
    /\ alive[p]
    /\ believes[p]
    /\ inflight[p] = 0
    /\ pending[p] # {}
    /\ Len(net) < MaxNet
    /\ net' = Append(net, [w |-> Min(pending[p]), from |-> p])
    /\ inflight' = [inflight EXCEPT ![p] = Min(pending[p])]
    /\ UNCHANGED <<primary, epoch, alive, believes, restarted, hist, pending,
                   repl, acks, delivered, lastW, nextId, losses, lost>>

Resend(p) ==
    /\ Acked
    /\ alive[p]
    /\ believes[p]
    /\ inflight[p] # 0
    /\ Len(net) < MaxNet
    /\ \A i \in DOMAIN net : net[i].from # p
    /\ \A i \in DOMAIN acks : acks[i].to # p
    /\ net' = Append(net, [w |-> inflight[p], from |-> p])
    /\ UNCHANGED <<primary, epoch, alive, believes, restarted, hist, pending,
                   repl, acks, inflight, delivered, lastW, nextId,
                   losses, lost>>

Receive ==
    /\ net # <<>>
    /\ LET m == Head(net)
           newer == m.w > lastW
           fresh == ~Acked \/ newer
       IN /\ delivered' = IF fresh
                          THEN Append(delivered, [w |-> m.w, e |-> epoch])
                          ELSE delivered
          /\ lastW' = IF newer THEN m.w ELSE lastW
          /\ acks' = IF Acked THEN Append(acks, [w |-> m.w, to |-> m.from])
                              ELSE acks
    /\ net' = Tail(net)
    /\ UNCHANGED <<primary, epoch, alive, believes, restarted, hist, pending,
                   repl, inflight, nextId, losses, lost>>

ReceiveAck ==
    /\ acks # <<>>
    /\ LET a == Head(acks)
           p == a.to
           match == alive[p] /\ believes[p] /\ inflight[p] = a.w
       IN /\ inflight' = IF match THEN [inflight EXCEPT ![p] = 0] ELSE inflight
          /\ pending' = IF match THEN [pending EXCEPT ![p] = @ \ {a.w}] ELSE pending
          /\ repl' = IF match /\ p = 1 /\ primary = 1
                     THEN Append(repl, [kind |-> "prune", w |-> a.w])
                     ELSE repl
    /\ acks' = Tail(acks)
    /\ UNCHANGED <<primary, epoch, alive, believes, restarted, hist, net,
                   delivered, lastW, nextId, losses, lost>>

DropEvent ==
    /\ losses < MaxLoss
    /\ \E i \in DOMAIN net : net' = Remove(net, i)
    /\ losses' = losses + 1
    /\ UNCHANGED <<primary, epoch, alive, believes, restarted, hist, pending,
                   repl, acks, inflight, delivered, lastW, nextId, lost>>

DropAck ==
    /\ Acked
    /\ losses < MaxLoss
    /\ \E i \in DOMAIN acks : acks' = Remove(acks, i)
    /\ losses' = losses + 1
    /\ UNCHANGED <<primary, epoch, alive, believes, restarted, hist, pending,
                   repl, net, inflight, delivered, lastW, nextId, lost>>

Crash ==
    /\ primary = 1
    /\ alive[1]
    /\ ~restarted[1]
    /\ alive' = [alive EXCEPT ![1] = FALSE]
    /\ believes' = [believes EXCEPT ![1] = FALSE]
    /\ repl' = <<>>
    /\ net' = SelectSeq(net, LAMBDA m : m.from # 1)
    /\ acks' = SelectSeq(acks, LAMBDA a : a.to # 1)
    /\ inflight' = [inflight EXCEPT ![1] = 0]
    /\ UNCHANGED <<primary, epoch, restarted, hist, pending, delivered,
                   lastW, nextId, losses, lost>>

Promote ==
    /\ primary = 1
    /\ ~alive[1] \/ Zombies
    /\ primary' = 2
    /\ epoch' = 2
    /\ believes' = [believes EXCEPT ![2] = TRUE]
    /\ repl' = <<>>
    /\ lost' = Range(hist[1]) \ Range(hist[2])
    /\ UNCHANGED <<alive, restarted, hist, pending, net, acks, inflight,
                   delivered, lastW, nextId, losses>>

Demote ==
    /\ primary = 2
    /\ alive[1]
    /\ believes[1]
    /\ believes' = [believes EXCEPT ![1] = FALSE]
    /\ hist' = [hist EXCEPT ![1] = hist[2]]
    /\ inflight' = [inflight EXCEPT ![1] = 0]
    /\ UNCHANGED <<primary, epoch, alive, restarted, pending, repl, net, acks,
                   delivered,
                   lastW, nextId, losses, lost>>

Restart ==
    /\ ~alive[1]
    /\ alive' = [alive EXCEPT ![1] = TRUE]
    /\ believes' = [believes EXCEPT ![1] = primary = 1]
    /\ restarted' = [restarted EXCEPT ![1] = TRUE]
    /\ hist' = IF primary = 2 THEN [hist EXCEPT ![1] = hist[2]] ELSE hist
    /\ UNCHANGED <<primary, epoch, pending, repl, net, acks, inflight, delivered,
                   lastW, nextId, losses, lost>>

Next ==
    \/ \E p \in Nodes : Write(p) \/ CurrentSend(p) \/ AckedSend(p) \/ Resend(p)
    \/ Replicate
    \/ Receive
    \/ ReceiveAck
    \/ DropEvent
    \/ DropAck
    \/ Crash
    \/ Promote
    \/ Demote
    \/ Restart

Fairness ==
    /\ WF_vars(Replicate)
    /\ WF_vars(Receive)
    /\ WF_vars(ReceiveAck)
    /\ WF_vars(Demote)
    /\ WF_vars(Promote /\ ~alive[1])
    /\ \A p \in Nodes : WF_vars(CurrentSend(p))
    /\ \A p \in Nodes : WF_vars(AckedSend(p))
    /\ \A p \in Nodes : WF_vars(Resend(p))

Spec == Init /\ [][Next]_vars /\ Fairness

Surviving(i) == delivered[i].w \notin lost

InOrder ==
    \A i, j \in DOMAIN delivered :
        (i < j /\ Surviving(i) /\ Surviving(j)) => delivered[i].w < delivered[j].w

NoDuplicates ==
    \A i, j \in DOMAIN delivered : i < j => delivered[i].w # delivered[j].w

NoLostWriteAfterFailover ==
    \A i \in DOMAIN delivered : delivered[i].w \in lost => delivered[i].e = 1

AllDelivered == Range(hist[primary]) \subseteq DeliveredIds

EveryCommittedEventDelivered == <>[]AllDelivered

==============================================================================
