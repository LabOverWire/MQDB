---------------------------- MODULE PrimaryMoveWindow ----------------------------
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Keys, MaxWrites, Forward, Refuse, Resync, RefuseHeld, MaxSwaps

Nodes == {"A", "B"}
Other(n) == IF n = "A" THEN "B" ELSE "A"
PrimaryAt(e) == IF e = 1 THEN "A" ELSE "B"

VARIABLES mapEpoch, role, rEpoch, seq, store, wlog, pending, superseded,
          dirty, snapAsked, deferred, held, chan, nextW, acked, rheld, swaps

vars == <<mapEpoch, role, rEpoch, seq, store, wlog, pending, superseded,
          dirty, snapAsked, deferred, held, chan, nextW, acked, rheld, swaps>>

MapPrimary(n) == PrimaryAt(mapEpoch[n]) = n

Init ==
    /\ mapEpoch = [n \in Nodes |-> 1]
    /\ role = [n \in Nodes |-> IF n = "A" THEN "P" ELSE "R"]
    /\ rEpoch = [n \in Nodes |-> 1]
    /\ seq = [n \in Nodes |-> 0]
    /\ store = [n \in Nodes |-> [k \in Keys |-> 0]]
    /\ wlog = [n \in Nodes |-> {}]
    /\ pending = [n \in Nodes |-> {}]
    /\ superseded = [n \in Nodes |-> {}]
    /\ dirty = [n \in Nodes |-> FALSE]
    /\ snapAsked = [n \in Nodes |-> FALSE]
    /\ deferred = [n \in Nodes |-> FALSE]
    /\ held = [n \in Nodes |-> <<>>]
    /\ chan = [p \in Nodes \X Nodes |-> <<>>]
    /\ nextW = 1
    /\ acked = <<>>
    /\ rheld = [n \in Nodes |-> {}]
    /\ swaps = 0

WriteMsg(e, s, k, v) == [type |-> "Write", epoch |-> e, seq |-> s, k |-> k, v |-> v]
AckMsg(st, e, s) == [type |-> "Ack", status |-> st, epoch |-> e, seq |-> s]

BySeq(S) == [i \in 1..Cardinality(S) |->
               CHOOSE x \in S : Cardinality({y \in S : y.seq < x.seq}) = i - 1]

Refusals(S, e) == [i \in 1..Cardinality(S) |-> AckMsg("Stale", e, BySeq(S)[i].seq)]

PrimaryWrite(n, k, v) ==
    LET s == seq[n] + 1
    IN /\ store' = [store EXCEPT ![n][k] = v]
       /\ seq' = [seq EXCEPT ![n] = s]
       /\ wlog' = [wlog EXCEPT ![n] = @ \cup {[seq |-> s, epoch |-> rEpoch[n], k |-> k, v |-> v]}]
       /\ pending' = [pending EXCEPT ![n] = @ \cup {[seq |-> s, epoch |-> rEpoch[n]]}]

Unchanged1 == UNCHANGED <<mapEpoch, role, superseded, dirty, snapAsked, deferred>>
Unchanged2 == UNCHANGED <<rheld, swaps>>

ClientWrite(n, k) ==
    /\ nextW <= MaxWrites
    /\ MapPrimary(n) /\ role[n] = "P"
    /\ PrimaryWrite(n, k, nextW)
    /\ chan' = [chan EXCEPT ![<<n, Other(n)>>] = Append(@, WriteMsg(rEpoch[n], seq[n] + 1, k, nextW))]
    /\ acked' = Append(acked, [k |-> k, v |-> nextW])
    /\ nextW' = nextW + 1
    /\ UNCHANGED <<rEpoch, held>> /\ Unchanged1 /\ Unchanged2

NeedsResync(n) == Resync /\ (pending[n] /= {} \/ superseded[n] /= {} \/ dirty[n])

ApplyMap(n) ==
    /\ mapEpoch[n] = 1
    /\ mapEpoch' = [mapEpoch EXCEPT ![n] = 2]
    /\ rEpoch' = [rEpoch EXCEPT ![n] = 2]
    /\ role' = [role EXCEPT ![n] = IF PrimaryAt(2) = n THEN "P" ELSE "R"]
    /\ snapAsked' = [snapAsked EXCEPT ![n] = PrimaryAt(2) /= n /\ NeedsResync(n)]
    /\ rheld' = [rheld EXCEPT ![n] = {}]
    /\ chan' = IF RefuseHeld /\ PrimaryAt(2) = n
               THEN [chan EXCEPT ![<<n, Other(n)>>] = @ \o Refusals(rheld[n], 2)]
               ELSE chan
    /\ UNCHANGED <<seq, store, wlog, pending, superseded, dirty, deferred, held, nextW, acked, swaps>>

SwapWrites(from, to) ==
    /\ swaps < MaxSwaps
    /\ Len(chan[<<from, to>>]) >= 2
    /\ chan[<<from, to>>][1].type = "Write" /\ chan[<<from, to>>][2].type = "Write"
    /\ chan' = [chan EXCEPT ![<<from, to>>] =
                  <<@[2], @[1]>> \o SubSeq(@, 3, Len(@))]
    /\ swaps' = swaps + 1
    /\ UNCHANGED <<mapEpoch, role, rEpoch, seq, store, wlog, pending, superseded, dirty,
                   snapAsked, deferred, held, nextW, acked, rheld>>

ReceiveWrite(from, to) ==
    /\ chan[<<from, to>>] /= <<>>
    /\ Head(chan[<<from, to>>]).type = "Write"
    /\ LET m == Head(chan[<<from, to>>])
           rest == Tail(chan[<<from, to>>])
           newer == m.epoch > rEpoch[to]
           sq == IF newer THEN 0 ELSE seq[to]
           kept == IF newer THEN {} ELSE rheld[to]
           ns == IF m.seq = sq + 1 THEN m.seq ELSE sq
           reply(a) == [chan EXCEPT ![<<from, to>>] = rest, ![<<to, from>>] = Append(@, a)]
       IN IF role[to] = "W"
          THEN /\ chan' = reply(AckMsg("NotReplica", rEpoch[to], m.seq))
               /\ UNCHANGED <<mapEpoch, role, rEpoch, seq, store, wlog, pending, superseded, dirty, snapAsked, deferred, held, nextW, acked, rheld>>
          ELSE IF m.epoch < rEpoch[to]
          THEN /\ chan' = reply(AckMsg("Stale", rEpoch[to], m.seq))
               /\ UNCHANGED <<mapEpoch, role, rEpoch, seq, store, wlog, pending, superseded, dirty, snapAsked, deferred, held, nextW, acked, rheld>>
          ELSE IF Refuse /\ role[to] = "P"
          THEN /\ chan' = reply(AckMsg("NotReplica", rEpoch[to], m.seq))
               /\ dirty' = [dirty EXCEPT ![to] = TRUE]
               /\ UNCHANGED <<mapEpoch, role, rEpoch, seq, store, wlog, pending, superseded, snapAsked, deferred, held, nextW, acked, rheld>>
          ELSE IF m.seq <= sq + 1
          THEN /\ rEpoch' = [rEpoch EXCEPT ![to] = m.epoch]
               /\ seq' = [seq EXCEPT ![to] = ns]
               /\ store' = [store EXCEPT ![to][m.k] = m.v]
               /\ wlog' = [wlog EXCEPT ![to] = @ \cup {[seq |-> m.seq, epoch |-> m.epoch, k |-> m.k, v |-> m.v]}]
               /\ rheld' = [rheld EXCEPT ![to] = {h \in kept : h.seq > ns}]
               /\ chan' = reply(AckMsg("Ok", m.epoch, m.seq))
               /\ UNCHANGED <<mapEpoch, role, pending, superseded, dirty, snapAsked, deferred, held, nextW, acked>>
          ELSE /\ rEpoch' = [rEpoch EXCEPT ![to] = m.epoch]
               /\ seq' = [seq EXCEPT ![to] = sq]
               /\ rheld' = [rheld EXCEPT ![to] = kept \cup {m}]
               /\ chan' = reply(AckMsg("Gap", m.epoch, sq + 1))
               /\ UNCHANGED <<mapEpoch, role, store, wlog, pending, superseded, dirty, snapAsked, deferred, held, nextW, acked>>
    /\ UNCHANGED swaps

OrderedFrom(n, fromSeq) ==
    LET es == {e \in wlog[n] : e.seq >= fromSeq /\ e.epoch = rEpoch[n]}
    IN [i \in 1..Cardinality(es) |->
          LET e == CHOOSE x \in es : Cardinality({y \in es : y.seq < x.seq}) = i - 1
          IN WriteMsg(e.epoch, e.seq, e.k, e.v)]

ReceiveAck(from, to) ==
    /\ chan[<<from, to>>] /= <<>>
    /\ Head(chan[<<from, to>>]).type = "Ack"
    /\ LET m == Head(chan[<<from, to>>])
           rest == Tail(chan[<<from, to>>])
           trackers == {p \in pending[to] : p.seq = m.seq}
           resolved == IF m.status = "Gap" THEN {} ELSE trackers
           stale == {p \in resolved : m.status = "Stale"}
           refused == {[k |-> e.k, v |-> e.v] : e \in {x \in wlog[to] : \E p \in stale : x.seq = p.seq /\ x.epoch = p.epoch}}
       IN /\ pending' = [pending EXCEPT ![to] = @ \ resolved]
          /\ superseded' = [superseded EXCEPT ![to] = IF Forward THEN @ \cup refused ELSE @]
          /\ chan' = IF m.status = "Gap" /\ role[to] = "P"
                     THEN [chan EXCEPT ![<<from, to>>] = rest,
                                       ![<<to, from>>] = Append(@, [type |-> "Catchup", writes |-> OrderedFrom(to, m.seq)])]
                     ELSE [chan EXCEPT ![<<from, to>>] = rest]
          /\ UNCHANGED <<mapEpoch, role, rEpoch, seq, store, wlog, dirty, snapAsked, deferred, held, nextW, acked>>
    /\ Unchanged2

ReceiveCatchup(from, to) ==
    /\ chan[<<from, to>>] /= <<>>
    /\ Head(chan[<<from, to>>]).type = "Catchup"
    /\ LET m == Head(chan[<<from, to>>])
           rest == Tail(chan[<<from, to>>])
           fresh == SelectSeq(m.writes, LAMBDA w : w.epoch = rEpoch[to] /\ w.seq > seq[to])
           old == SelectSeq(m.writes, LAMBDA w : w.epoch < rEpoch[to])
           app == {fresh[i] : i \in 1..Len(fresh)}
           top == IF app = {} THEN seq[to] ELSE (CHOOSE w \in app : \A x \in app : x.seq <= w.seq).seq
           lastFor(k) == CHOOSE w \in app : w.k = k /\ \A x \in app : x.k = k => x.seq <= w.seq
       IN IF role[to] = "R"
          THEN /\ store' = [store EXCEPT ![to] =
                              [k \in Keys |-> IF \E w \in app : w.k = k THEN lastFor(k).v ELSE store[to][k]]]
               /\ seq' = [seq EXCEPT ![to] = top]
               /\ wlog' = [wlog EXCEPT ![to] = @ \cup {[seq |-> w.seq, epoch |-> w.epoch, k |-> w.k, v |-> w.v] : w \in app}]
               /\ rheld' = [rheld EXCEPT ![to] = {h \in @ : h.seq > top}]
               /\ chan' = [chan EXCEPT ![<<from, to>>] = rest,
                                       ![<<to, from>>] = @ \o [i \in 1..Len(fresh) |-> AckMsg("Ok", rEpoch[to], fresh[i].seq)]]
          ELSE IF RefuseHeld /\ role[to] = "P"
          THEN /\ chan' = [chan EXCEPT ![<<from, to>>] = rest,
                                       ![<<to, from>>] = @ \o [i \in 1..Len(old) |-> AckMsg("Stale", rEpoch[to], old[i].seq)]]
               /\ UNCHANGED <<store, seq, wlog, rheld>>
          ELSE /\ chan' = [chan EXCEPT ![<<from, to>>] = rest]
               /\ UNCHANGED <<store, seq, wlog, rheld>>
    /\ UNCHANGED <<mapEpoch, role, rEpoch, pending, superseded, dirty, snapAsked, deferred, held, nextW, acked, swaps>>

ForwardSuperseded(n) ==
    /\ ~MapPrimary(n)
    /\ \E w \in superseded[n] :
         /\ superseded' = [superseded EXCEPT ![n] = @ \ {w}]
         /\ chan' = [chan EXCEPT ![<<n, Other(n)>>] =
                       Append(@, [type |-> "WriteRequest", epoch |-> mapEpoch[n], k |-> w.k, v |-> w.v])]
    /\ UNCHANGED <<mapEpoch, role, rEpoch, seq, store, wlog, pending, dirty, snapAsked, deferred, held, nextW, acked>>
    /\ Unchanged2

RequestSnapshot(n) ==
    /\ role[n] = "R" /\ snapAsked[n]
    /\ pending[n] = {} /\ superseded[n] = {}
    /\ snapAsked' = [snapAsked EXCEPT ![n] = FALSE]
    /\ role' = [role EXCEPT ![n] = "W"]
    /\ chan' = [chan EXCEPT ![<<n, Other(n)>>] = Append(@, [type |-> "SnapReq"])]
    /\ UNCHANGED <<mapEpoch, rEpoch, seq, store, wlog, pending, superseded, dirty, deferred, held, nextW, acked>>
    /\ Unchanged2

SnapMsg(n) == [type |-> "Snap", store |-> store[n], seq |-> seq[n], epoch |-> rEpoch[n]]

ReceiveSnapReq(from, to) ==
    /\ chan[<<from, to>>] /= <<>>
    /\ Head(chan[<<from, to>>]).type = "SnapReq"
    /\ IF role[to] = "P"
       THEN chan' = [chan EXCEPT ![<<from, to>>] = Tail(@), ![<<to, from>>] = Append(@, SnapMsg(to))]
            /\ UNCHANGED deferred
       ELSE chan' = [chan EXCEPT ![<<from, to>>] = Tail(@)]
            /\ deferred' = [deferred EXCEPT ![to] = TRUE]
    /\ UNCHANGED <<mapEpoch, role, rEpoch, seq, store, wlog, pending, superseded, dirty, snapAsked, held, nextW, acked>>
    /\ Unchanged2

ServeDeferred(n) ==
    /\ deferred[n] /\ role[n] = "P"
    /\ deferred' = [deferred EXCEPT ![n] = FALSE]
    /\ chan' = [chan EXCEPT ![<<n, Other(n)>>] = Append(@, SnapMsg(n))]
    /\ UNCHANGED <<mapEpoch, role, rEpoch, seq, store, wlog, pending, superseded, dirty, snapAsked, held, nextW, acked>>
    /\ Unchanged2

ReceiveSnap(from, to) ==
    /\ chan[<<from, to>>] /= <<>>
    /\ Head(chan[<<from, to>>]).type = "Snap"
    /\ LET m == Head(chan[<<from, to>>])
       IN /\ chan' = [chan EXCEPT ![<<from, to>>] = Tail(@)]
          /\ IF role[to] = "W"
             THEN /\ store' = [store EXCEPT ![to] = m.store]
                  /\ seq' = [seq EXCEPT ![to] = m.seq]
                  /\ rEpoch' = [rEpoch EXCEPT ![to] = m.epoch]
                  /\ role' = [role EXCEPT ![to] = "R"]
                  /\ dirty' = [dirty EXCEPT ![to] = FALSE]
                  /\ rheld' = [rheld EXCEPT ![to] = {}]
             ELSE UNCHANGED <<store, seq, rEpoch, role, dirty, rheld>>
    /\ UNCHANGED <<mapEpoch, wlog, pending, superseded, snapAsked, deferred, held, nextW, acked, swaps>>

ReceiveWriteRequest(from, to) ==
    /\ chan[<<from, to>>] /= <<>>
    /\ Head(chan[<<from, to>>]).type = "WriteRequest"
    /\ LET m == Head(chan[<<from, to>>])
       IN IF m.epoch > mapEpoch[to]
          THEN /\ held' = [held EXCEPT ![to] = Append(@, m)]
               /\ chan' = [chan EXCEPT ![<<from, to>>] = Tail(@)]
               /\ UNCHANGED <<rEpoch, seq, store, wlog, pending>>
          ELSE IF MapPrimary(to) /\ role[to] = "P" /\ m.epoch = mapEpoch[to]
               THEN /\ PrimaryWrite(to, m.k, m.v)
                    /\ chan' = [chan EXCEPT ![<<from, to>>] = Tail(@),
                                            ![<<to, from>>] = Append(@, WriteMsg(rEpoch[to], seq[to] + 1, m.k, m.v))]
                    /\ UNCHANGED <<rEpoch, held>>
               ELSE /\ chan' = [chan EXCEPT ![<<from, to>>] = Tail(@)]
                    /\ UNCHANGED <<rEpoch, seq, store, wlog, pending, held>>
    /\ UNCHANGED <<nextW, acked>> /\ Unchanged1 /\ Unchanged2

ReleaseHeld(n) ==
    /\ held[n] /= <<>>
    /\ Head(held[n]).epoch <= mapEpoch[n]
    /\ LET m == Head(held[n])
       IN /\ held' = [held EXCEPT ![n] = Tail(@)]
          /\ IF MapPrimary(n) /\ role[n] = "P" /\ m.epoch = mapEpoch[n]
             THEN /\ PrimaryWrite(n, m.k, m.v)
                  /\ chan' = [chan EXCEPT ![<<n, Other(n)>>] = Append(@, WriteMsg(rEpoch[n], seq[n] + 1, m.k, m.v))]
             ELSE UNCHANGED <<seq, store, wlog, pending, chan>>
    /\ UNCHANGED <<rEpoch, nextW, acked>> /\ Unchanged1 /\ Unchanged2

Next ==
    \/ \E n \in Nodes, k \in Keys : ClientWrite(n, k)
    \/ \E n \in Nodes : ApplyMap(n)
    \/ \E n \in Nodes : SwapWrites(Other(n), n)
    \/ \E n \in Nodes : ReceiveWrite(Other(n), n)
    \/ \E n \in Nodes : ReceiveAck(Other(n), n)
    \/ \E n \in Nodes : ReceiveCatchup(Other(n), n)
    \/ \E n \in Nodes : ReceiveWriteRequest(Other(n), n)
    \/ \E n \in Nodes : ForwardSuperseded(n)
    \/ \E n \in Nodes : ReleaseHeld(n)
    \/ \E n \in Nodes : RequestSnapshot(n)
    \/ \E n \in Nodes : ReceiveSnapReq(Other(n), n)
    \/ \E n \in Nodes : ServeDeferred(n)
    \/ \E n \in Nodes : ReceiveSnap(Other(n), n)

Spec == Init /\ [][Next]_vars

Quiescent ==
    /\ \A n \in Nodes : mapEpoch[n] = 2
    /\ \A p \in Nodes \X Nodes : chan[p] = <<>>
    /\ \A n \in Nodes : superseded[n] = {} /\ held[n] = <<>> /\ ~deferred[n]
    /\ role["A"] /= "W" /\ ~snapAsked["A"]

WrittenKeys == {acked[i].k : i \in 1..Len(acked)}

LastAcked(k) == acked[CHOOSE i \in 1..Len(acked) :
                        acked[i].k = k /\ \A j \in 1..Len(acked) : acked[j].k = k => j <= i].v

InvNoLostWrite == Quiescent => \A k \in WrittenKeys : store["B"][k] /= 0

InvConverged == Quiescent => store["A"] = store["B"]

InvFinalIsAnAckedWrite == Quiescent =>
    \A k \in WrittenKeys : \E i \in 1..Len(acked) : acked[i].k = k /\ acked[i].v = store["B"][k]

InvLastWriterWins == Quiescent => \A k \in WrittenKeys : store["B"][k] = LastAcked(k)

InvTerminalIsQuiescent == (~ENABLED Next) => Quiescent

=============================================================================
