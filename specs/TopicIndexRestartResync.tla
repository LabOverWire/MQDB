---------------------------- MODULE TopicIndexRestartResync ----------------------------
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Nodes, Clients, Topics, Crashable, NoNode,
          MaxOps, MaxCrashes, MaxDrops,
          Resync, LocTargets, StampGuard, DeleteCheck, AskBack, Retry, ResumePush, StaleDisk

Entries == Clients \X Topics
Pairs == {p \in Nodes \X Nodes : p[1] # p[2]}
Absent == [node |-> NoNode, ts |-> 0]

VARIABLES up, links, conn, clock, loc, idx, view, chan, pending, outstanding, ops, crashes, drops, ever, want, sess

vars == <<up, links, conn, clock, loc, idx, view, chan, pending, outstanding, ops, crashes, drops, ever, want, sess>>

Linked(n, m) == {n, m} \in links

SubsOf(n, c) == {t \in Topics : <<c, t>> \in idx[n]}

OwnClients(n) == {c \in Clients : loc[n][c].node = n}

Listing(n) == {[c |-> c, ts |-> loc[n][c].ts, subs |-> SubsOf(n, c)] : c \in OwnClients(n)}

LocReceivers(n) ==
    IF LocTargets = "alive" THEN {m \in Nodes \ {n} : view[n][m] /\ Linked(n, m)}
    ELSE {m \in Nodes \ {n} : Linked(n, m)}

TopicReceivers(n) == {m \in Nodes \ {n} : Linked(n, m)}

SendTo(ch, from, targets, msg) ==
    [p \in Pairs |-> IF p[1] = from /\ p[2] \in targets THEN Append(ch[p], msg) ELSE ch[p]]

Settled ==
    /\ \A p \in Pairs : chan[p] = <<>>
    /\ \A n \in Nodes : up[n] => pending[n] = {}
    /\ \A n, m \in Nodes : (up[n] /\ n # m) => (view[n][m] = up[m] /\ (up[m] => Linked(n, m)))
    /\ Retry => \A n \in Nodes : up[n] => outstanding[n] = {}

Init ==
    /\ up = [n \in Nodes |-> TRUE]
    /\ links = {{n, m} : n \in Nodes, m \in Nodes \ {n}}
    /\ conn = [c \in Clients |-> NoNode]
    /\ clock = 0
    /\ loc = [n \in Nodes |-> [c \in Clients |-> Absent]]
    /\ idx = [n \in Nodes |-> {}]
    /\ view = [n \in Nodes |-> [m \in Nodes |-> m # n]]
    /\ chan = [p \in Pairs |-> <<>>]
    /\ pending = [n \in Nodes |-> {}]
    /\ outstanding = [n \in Nodes |-> {}]
    /\ ops = 0
    /\ crashes = 0
    /\ drops = 0
    /\ ever = {}
    /\ want = [c \in Clients |-> {}]
    /\ sess = [c \in Clients |-> NoNode]

StampedSet(n, c, node, ts) == [loc EXCEPT ![n][c] = [node |-> node, ts |-> ts]]

AcceptsLocation(current, ts) ==
    StampGuard = "none" \/ current.node = NoNode \/ current.ts <= ts

AcceptsDelete(current, node, ts) ==
    ~DeleteCheck \/ (current.node = node /\ current.ts <= ts)

Resumes(c, n) == sess[c] = n

Connect(c, n) ==
    /\ ops < MaxOps
    /\ conn[c] = NoNode
    /\ up[n]
    /\ clock' = clock + 1
    /\ conn' = [conn EXCEPT ![c] = n]
    /\ sess' = [sess EXCEPT ![c] = n]
    /\ loc' = StampedSet(n, c, n, clock + 1)
    /\ LET located == [kind |-> "loc", c |-> c, node |-> n, ts |-> clock + 1]
           pushed == [kind |-> "push",
                      list |-> {[c |-> c, ts |-> clock + 1, subs |-> SubsOf(n, c)]}]
           resubscribed == [kind |-> "subs", c |-> c, ts |-> want[c]]
       IN IF Resumes(c, n)
          THEN /\ idx' = idx
               /\ chan' = IF ResumePush
                          THEN SendTo(SendTo(chan, n, LocReceivers(n), located),
                                      n, TopicReceivers(n), pushed)
                          ELSE SendTo(chan, n, LocReceivers(n), located)
          ELSE /\ idx' = [idx EXCEPT ![n] = @ \cup {<<c, t>> : t \in want[c]}]
               /\ chan' = SendTo(SendTo(chan, n, LocReceivers(n), located),
                                 n, TopicReceivers(n), resubscribed)
    /\ ops' = ops + 1
    /\ UNCHANGED <<want, ever, links, up, view, pending, outstanding, crashes, drops>>

Disconnect(c) ==
    LET n == conn[c] IN
    /\ ops < MaxOps
    /\ n # NoNode
    /\ clock' = clock + 1
    /\ conn' = [conn EXCEPT ![c] = NoNode]
    /\ loc' = IF AcceptsDelete(loc[n][c], n, clock + 1)
              THEN [loc EXCEPT ![n][c] = Absent] ELSE loc
    /\ chan' = SendTo(chan, n, LocReceivers(n), [kind |-> "del", c |-> c, node |-> n, ts |-> clock + 1])
    /\ ops' = ops + 1
    /\ UNCHANGED <<want, sess, ever, links, up, idx, view, pending, outstanding, crashes, drops>>

Sub(c, t) ==
    LET n == conn[c] IN
    /\ ops < MaxOps
    /\ n # NoNode
    /\ t \notin want[c]
    /\ idx' = [idx EXCEPT ![n] = @ \cup {<<c, t>>}]
    /\ chan' = SendTo(chan, n, TopicReceivers(n), [kind |-> "sub", c |-> c, t |-> t])
    /\ ops' = ops + 1
    /\ ever' = ever \cup {<<c, t>>}
    /\ want' = [want EXCEPT ![c] = @ \cup {t}]
    /\ UNCHANGED <<sess, links, up, conn, clock, loc, view, pending, outstanding, crashes, drops>>

Unsub(c, t) ==
    LET n == conn[c] IN
    /\ ops < MaxOps
    /\ n # NoNode
    /\ t \in want[c]
    /\ idx' = [idx EXCEPT ![n] = @ \ {<<c, t>>}]
    /\ chan' = SendTo(chan, n, TopicReceivers(n), [kind |-> "unsub", c |-> c, t |-> t])
    /\ ops' = ops + 1
    /\ want' = [want EXCEPT ![c] = @ \ {t}]
    /\ UNCHANGED <<sess, ever, links, up, conn, clock, loc, view, pending, outstanding, crashes, drops>>

Crash(n) ==
    /\ n \in Crashable
    /\ up[n]
    /\ crashes < MaxCrashes
    /\ up' = [up EXCEPT ![n] = FALSE]
    /\ links' = {l \in links : n \notin l}
    /\ conn' = [c \in Clients |-> IF conn[c] = n THEN NoNode ELSE conn[c]]
    /\ sess' = [c \in Clients |-> IF sess[c] = n THEN NoNode ELSE sess[c]]
    /\ loc' = [loc EXCEPT ![n] = [c \in Clients |-> Absent]]
    /\ view' = [view EXCEPT ![n] = [m \in Nodes |-> FALSE]]
    /\ chan' = [p \in Pairs |-> IF p[2] = n THEN <<>> ELSE chan[p]]
    /\ pending' = [pending EXCEPT ![n] = {}]
    /\ outstanding' = [outstanding EXCEPT ![n] = {}]
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<want, ever, clock, idx, ops, drops>>

Restart(n) ==
    /\ ~up[n]
    /\ \E disk \in IF StaleDisk THEN SUBSET ever ELSE {{}} :
          idx' = [idx EXCEPT ![n] = disk]
    /\ up' = [up EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<want, sess, ever, links, conn, clock, loc, view, chan, pending, outstanding, ops, crashes, drops>>

LinkUp(n, m) ==
    /\ n # m
    /\ up[n]
    /\ up[m]
    /\ ~Linked(n, m)
    /\ links' = links \cup {{n, m}}
    /\ UNCHANGED <<want, sess, ever, up, conn, clock, loc, idx, view, chan, pending, outstanding, ops, crashes, drops>>

Heartbeat(n, m) ==
    /\ n # m
    /\ Linked(n, m)
    /\ ~view[n][m]
    /\ view' = [view EXCEPT ![n][m] = TRUE]
    /\ pending' = IF Resync THEN [pending EXCEPT ![n] = @ \cup {[peer |-> m, back |-> AskBack]}] ELSE pending
    /\ UNCHANGED <<want, sess, ever, links, up, conn, clock, loc, idx, chan, outstanding, ops, crashes, drops>>

DetectDeath(n, m) ==
    /\ n # m
    /\ up[n]
    /\ ~up[m]
    /\ view[n][m]
    /\ view' = [view EXCEPT ![n][m] = FALSE]
    /\ pending' = [pending EXCEPT ![n] = {q \in @ : q.peer # m}]
    /\ outstanding' = [outstanding EXCEPT ![n] = {q \in @ : q.peer # m}]
    /\ UNCHANGED <<want, sess, ever, links, up, conn, clock, loc, idx, chan, ops, crashes, drops>>

SendRequest(n, m) ==
    /\ Linked(n, m)
    /\ \E q \in pending[n] :
          /\ q.peer = m
          /\ chan' = [chan EXCEPT ![<<n, m>>] = Append(@, [kind |-> "req", back |-> q.back])]
          /\ pending' = [pending EXCEPT ![n] = @ \ {q}]
          /\ outstanding' = [outstanding EXCEPT ![n] = {o \in @ : o.peer # m} \cup {q}]
    /\ UNCHANGED <<want, sess, ever, links, up, conn, clock, loc, idx, view, ops, crashes, drops>>

InFlight(n, m) ==
    \/ \E i \in 1..Len(chan[<<n, m>>]) : chan[<<n, m>>][i].kind = "req"
    \/ \E i \in 1..Len(chan[<<m, n>>]) : chan[<<m, n>>][i].kind = "reply"

ReAsk(n, m) ==
    /\ Retry
    /\ Linked(n, m)
    /\ view[n][m]
    /\ ~InFlight(n, m)
    /\ \E q \in outstanding[n] :
          /\ q.peer = m
          /\ chan' = [chan EXCEPT ![<<n, m>>] = Append(@, [kind |-> "req", back |-> q.back])]
    /\ UNCHANGED <<want, sess, ever, links, up, conn, clock, loc, idx, view, pending, outstanding, ops, crashes, drops>>

Lost(n, m) ==
    /\ drops < MaxDrops
    /\ chan[<<n, m>>] # <<>>
    /\ Head(chan[<<n, m>>]).kind \in {"req", "reply"}
    /\ chan' = [chan EXCEPT ![<<n, m>>] = Tail(@)]
    /\ drops' = drops + 1
    /\ UNCHANGED <<want, sess, ever, links, up, conn, clock, loc, idx, view, pending, outstanding, ops, crashes>>

Takes(to, e) ==
    /\ loc[to][e.c].node # to
    /\ AcceptsLocation(loc[to][e.c], e.ts)

TakenNames(to, list) == {e.c : e \in {x \in list : Takes(to, x)}}

ReplacedIdx(to, list) ==
    {x \in idx[to] : x[1] \notin TakenNames(to, list)}
    \cup UNION {{<<e.c, t>> : t \in e.subs} : e \in {x \in list : Takes(to, x)}}

ReplacedLoc(to, from, list) ==
    [c \in Clients |->
        IF c \in TakenNames(to, list)
        THEN [node |-> from, ts |-> (CHOOSE e \in list : e.c = c).ts]
        ELSE loc[to][c]]

Receive(from, to) ==
    /\ chan[<<from, to>>] # <<>>
    /\ LET msg == Head(chan[<<from, to>>])
           rest == [chan EXCEPT ![<<from, to>>] = Tail(@)]
       IN
       CASE msg.kind = "sub" ->
              /\ idx' = [idx EXCEPT ![to] = @ \cup {<<msg.c, msg.t>>}]
              /\ chan' = rest
              /\ UNCHANGED <<want, sess, ever, loc, outstanding, pending>>
         [] msg.kind = "subs" ->
              /\ idx' = [idx EXCEPT ![to] = @ \cup {<<msg.c, t>> : t \in msg.ts}]
              /\ chan' = rest
              /\ UNCHANGED <<want, sess, ever, loc, outstanding, pending>>
         [] msg.kind = "unsub" ->
              /\ idx' = [idx EXCEPT ![to] = @ \ {<<msg.c, msg.t>>}]
              /\ chan' = rest
              /\ UNCHANGED <<want, sess, ever, loc, outstanding, pending>>
         [] msg.kind = "loc" ->
              /\ loc' = IF AcceptsLocation(loc[to][msg.c], msg.ts)
                        THEN StampedSet(to, msg.c, msg.node, msg.ts) ELSE loc
              /\ chan' = rest
              /\ UNCHANGED <<want, sess, ever, idx, outstanding, pending>>
         [] msg.kind = "del" ->
              /\ loc' = IF AcceptsDelete(loc[to][msg.c], msg.node, msg.ts)
                        THEN [loc EXCEPT ![to][msg.c] = Absent] ELSE loc
              /\ chan' = rest
              /\ UNCHANGED <<want, sess, ever, idx, outstanding, pending>>
         [] msg.kind = "req" ->
              /\ chan' = [rest EXCEPT ![<<to, from>>] =
                            Append(@, [kind |-> "reply", list |-> Listing(to)])]
              /\ pending' = IF msg.back
                             THEN [pending EXCEPT ![to] = @ \cup {[peer |-> from, back |-> FALSE]}]
                             ELSE pending
              /\ UNCHANGED <<want, sess, ever, idx, loc, outstanding>>
         [] msg.kind \in {"reply", "push"} ->
              /\ idx' = [idx EXCEPT ![to] = ReplacedIdx(to, msg.list)]
              /\ loc' = [loc EXCEPT ![to] = ReplacedLoc(to, from, msg.list)]
              /\ chan' = rest
              /\ outstanding' = IF msg.kind = "reply"
                                THEN [outstanding EXCEPT ![to] = {o \in @ : o.peer # from}]
                                ELSE outstanding
              /\ pending' = pending
    /\ UNCHANGED <<want, sess, ever, links, up, conn, clock, view, ops, crashes, drops>>

Next ==
    \/ \E c \in Clients, n \in Nodes : Connect(c, n)
    \/ \E c \in Clients : Disconnect(c)
    \/ \E c \in Clients, t \in Topics : Sub(c, t) \/ Unsub(c, t)
    \/ \E n \in Nodes : Crash(n) \/ Restart(n)
    \/ \E n, m \in Nodes : n # m /\ (LinkUp(n, m) \/ Heartbeat(n, m) \/ DetectDeath(n, m)
                                     \/ SendRequest(n, m) \/ ReAsk(n, m)
                                     \/ Lost(n, m) \/ Receive(n, m))

Spec == Init /\ [][Next]_vars

InvConverged ==
    Settled =>
        \A n \in Nodes, c \in Clients :
            (up[n] /\ conn[c] # NoNode) =>
                /\ loc[n][c].node = conn[c]
                /\ want[c] \subseteq SubsOf(n, c)

InvNoInventedEntries == \A n \in Nodes : idx[n] \subseteq ever
=============================================================================
