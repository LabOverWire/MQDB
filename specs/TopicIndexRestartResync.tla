---------------------------- MODULE TopicIndexRestartResync ----------------------------
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Nodes, Clients, Topics, Crashable, NoNode,
          MaxOps, MaxCrashes, MaxDrops, MaxAsks, MaxChan,
          Resync, Claims, Supersede, AntiEntropy, LinkedTargets, ReleaseOnDeath,
          RetainOnComplete, AskBack, Retry, Announce, StaleDisk, FalseSuspicion,
          UniqueRounds

Entries == Clients \X Topics
Pairs == {p \in Nodes \X Nodes : p[1] # p[2]}

VARIABLES up, links, view, dead, conn, sess, want, resub, loc, idx, chan,
          pending, outstanding, asks, ops, crashes, drops, ever, dirty, boot

vars == <<up, links, view, dead, conn, sess, want, resub, loc, idx, chan,
          pending, outstanding, asks, ops, crashes, drops, ever, dirty, boot>>

Linked(n, m) == {n, m} \in links

SubsOf(n, c) == {t \in Topics : <<c, t>> \in idx[n]}

Listing(n) == {[c |-> c, subs |-> SubsOf(n, c)] : c \in {x \in Clients : n \in loc[n][x]}}

LocReceivers(n) ==
    IF LinkedTargets THEN {m \in Nodes \ {n} : Linked(n, m)}
    ELSE {m \in Nodes \ {n} : Linked(n, m) /\ view[n][m]}

TopicReceivers(n) == {m \in Nodes \ {n} : Linked(n, m)}

SendTo(ch, from, targets, msg) ==
    [p \in Pairs |-> IF p[1] = from /\ p[2] \in targets THEN Append(ch[p], msg) ELSE ch[p]]

Claimed(set, node) == IF Claims THEN set \cup {node} ELSE {node}

Connected(set, node) == IF Supersede \/ ~Claims THEN {node} ELSE set \cup {node}

ClaimedAt(to, set, node) == IF ReleaseOnDeath /\ dead[to][node] THEN set ELSE Claimed(set, node)

ConnectedAt(to, set, node) ==
    IF ReleaseOnDeath /\ dead[to][node] THEN set
    ELSE IF Claims THEN Connected(set, node) \cup (set \cap {to})
    ELSE Connected(set, node)

Released(set, node) == IF Claims THEN set \ {node} ELSE {}

Settled ==
    /\ \A p \in Pairs : chan[p] = <<>>
    /\ \A n \in Nodes : up[n] => pending[n] = {}
    /\ \A n, m \in Nodes : (up[n] /\ n # m) => (view[n][m] = up[m] /\ (up[m] => Linked(n, m)))
    /\ \A c \in Clients : ~resub[c]
    /\ Retry => \A n \in Nodes : up[n] => outstanding[n] = {}
    /\ AntiEntropy => \A n \in Nodes : up[n] => ~dirty[n]

Init ==
    /\ up = [n \in Nodes |-> TRUE]
    /\ links = {{n, m} : n \in Nodes, m \in Nodes \ {n}}
    /\ view = [n \in Nodes |-> [m \in Nodes |-> m # n]]
    /\ dead = [n \in Nodes |-> [m \in Nodes |-> FALSE]]
    /\ conn = [c \in Clients |-> NoNode]
    /\ sess = [c \in Clients |-> NoNode]
    /\ want = [c \in Clients |-> {}]
    /\ resub = [c \in Clients |-> FALSE]
    /\ loc = [n \in Nodes |-> [c \in Clients |-> {}]]
    /\ idx = [n \in Nodes |-> {}]
    /\ chan = [p \in Pairs |-> <<>>]
    /\ pending = [n \in Nodes |-> {}]
    /\ outstanding = [n \in Nodes |-> {}]
    /\ asks = [n \in Nodes |-> [m \in Nodes |-> 0]]
    /\ ops = 0
    /\ crashes = 0
    /\ drops = 0
    /\ ever = {}
    /\ dirty = [n \in Nodes |-> FALSE]
    /\ boot = [n \in Nodes |-> 0]

Connect(c, n, clean) ==
    LET old == conn[c]
        resumed == ~clean /\ sess[c] = n
        located == [kind |-> "loc", c |-> c, node |-> n]
        released == [kind |-> "del", c |-> c, node |-> old]
        announced == [kind |-> "announce", list |-> {[c |-> c, subs |-> SubsOf(n, c)]}]
        afterOld == IF old # NoNode /\ old # n
                    THEN SendTo(chan, old, LocReceivers(old), released)
                    ELSE chan
        afterLoc == SendTo(afterOld, n, LocReceivers(n), located)
    IN
    /\ ops < MaxOps
    /\ up[n]
    /\ conn[c] # n
    /\ conn' = [conn EXCEPT ![c] = n]
    /\ sess' = [sess EXCEPT ![c] = n]
    /\ resub' = [resub EXCEPT ![c] = ~resumed]
    /\ loc' = [m \in Nodes |->
                 IF m = n THEN [loc[m] EXCEPT ![c] = Connected(@, n)]
                 ELSE IF m = old THEN [loc[m] EXCEPT ![c] = Released(@, old)]
                 ELSE loc[m]]
    /\ chan' = IF Announce /\ ~clean
               THEN SendTo(afterLoc, n, TopicReceivers(n), announced)
               ELSE afterLoc
    /\ ops' = ops + 1
    /\ UNCHANGED <<dirty, up, links, view, dead, want, idx, pending, outstanding, asks, crashes, drops, ever, boot>>

Resubscribe(c) ==
    LET n == conn[c] IN
    /\ resub[c]
    /\ n # NoNode
    /\ idx' = [idx EXCEPT ![n] = @ \cup {<<c, t>> : t \in want[c]}]
    /\ chan' = SendTo(chan, n, TopicReceivers(n), [kind |-> "subs", c |-> c, ts |-> want[c]])
    /\ resub' = [resub EXCEPT ![c] = FALSE]
    /\ UNCHANGED <<dirty, up, links, view, dead, conn, sess, want, loc, pending, outstanding, asks, ops, crashes, drops, ever, boot>>

Disconnect(c) ==
    LET n == conn[c] IN
    /\ ops < MaxOps
    /\ n # NoNode
    /\ conn' = [conn EXCEPT ![c] = NoNode]
    /\ resub' = [resub EXCEPT ![c] = FALSE]
    /\ loc' = [loc EXCEPT ![n][c] = Released(@, n)]
    /\ chan' = SendTo(chan, n, LocReceivers(n), [kind |-> "del", c |-> c, node |-> n])
    /\ ops' = ops + 1
    /\ UNCHANGED <<dirty, up, links, view, dead, sess, want, idx, pending, outstanding, asks, crashes, drops, ever, boot>>

Sub(c, t) ==
    LET n == conn[c] IN
    /\ ops < MaxOps
    /\ n # NoNode
    /\ t \notin want[c]
    /\ want' = [want EXCEPT ![c] = @ \cup {t}]
    /\ idx' = [idx EXCEPT ![n] = @ \cup {<<c, t>>}]
    /\ chan' = SendTo(chan, n, TopicReceivers(n), [kind |-> "sub", c |-> c, t |-> t])
    /\ ever' = ever \cup {<<c, t>>}
    /\ ops' = ops + 1
    /\ UNCHANGED <<dirty, up, links, view, dead, conn, sess, resub, loc, pending, outstanding, asks, crashes, drops, boot>>

Unsub(c, t) ==
    LET n == conn[c] IN
    /\ ops < MaxOps
    /\ n # NoNode
    /\ t \in want[c]
    /\ want' = [want EXCEPT ![c] = @ \ {t}]
    /\ idx' = [idx EXCEPT ![n] = @ \ {<<c, t>>}]
    /\ chan' = SendTo(chan, n, TopicReceivers(n), [kind |-> "unsub", c |-> c, t |-> t])
    /\ ops' = ops + 1
    /\ UNCHANGED <<dirty, up, links, view, dead, conn, sess, resub, loc, pending, outstanding, asks, crashes, drops, ever, boot>>

Crash(n) ==
    /\ n \in Crashable
    /\ up[n]
    /\ crashes < MaxCrashes
    /\ up' = [up EXCEPT ![n] = FALSE]
    /\ links' = {l \in links : n \notin l}
    /\ view' = [view EXCEPT ![n] = [m \in Nodes |-> FALSE]]
    /\ dead' = [dead EXCEPT ![n] = [m \in Nodes |-> FALSE]]
    /\ dirty' = [dirty EXCEPT ![n] = FALSE]
    /\ conn' = [c \in Clients |-> IF conn[c] = n THEN NoNode ELSE conn[c]]
    /\ sess' = [c \in Clients |-> IF sess[c] = n THEN NoNode ELSE sess[c]]
    /\ resub' = [c \in Clients |-> IF conn[c] = n THEN FALSE ELSE resub[c]]
    /\ loc' = [loc EXCEPT ![n] = [c \in Clients |-> {}]]
    /\ chan' = [p \in Pairs |-> IF p[2] = n THEN <<>> ELSE chan[p]]
    /\ pending' = [pending EXCEPT ![n] = {}]
    /\ outstanding' = [outstanding EXCEPT ![n] = {}]
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<want, idx, asks, ops, drops, ever, boot>>

Restart(n) ==
    /\ ~up[n]
    /\ \E disk \in IF StaleDisk THEN SUBSET ever ELSE {{}} :
          idx' = [idx EXCEPT ![n] = disk]
    /\ up' = [up EXCEPT ![n] = TRUE]
    /\ boot' = [boot EXCEPT ![n] = @ + 1]
    /\ UNCHANGED <<dirty, links, view, dead, conn, sess, want, resub, loc, chan, pending, outstanding, asks, ops, crashes, drops, ever>>

Heartbeat(n, m) ==
    /\ n # m
    /\ up[n]
    /\ up[m]
    /\ ~view[n][m]
    /\ links' = links \cup {{n, m}}
    /\ view' = [view EXCEPT ![n][m] = TRUE]
    /\ dead' = [dead EXCEPT ![n][m] = FALSE]
    /\ pending' = IF Resync THEN [pending EXCEPT ![n] = @ \cup {[peer |-> m, back |-> AskBack]}] ELSE pending
    /\ UNCHANGED <<dirty, up, conn, sess, want, resub, loc, idx, chan, outstanding, asks, ops, crashes, drops, ever, boot>>

DetectDeath(n, m) ==
    /\ n # m
    /\ up[n]
    /\ (FalseSuspicion \/ ~up[m])
    /\ view[n][m]
    /\ view' = [view EXCEPT ![n][m] = FALSE]
    /\ dead' = [dead EXCEPT ![n][m] = TRUE]
    /\ loc' = IF ReleaseOnDeath
              THEN [loc EXCEPT ![n] = [c \in Clients |-> Released(@[c], m)]]
              ELSE loc
    /\ pending' = [pending EXCEPT ![n] = {q \in @ : q.peer # m}]
    /\ outstanding' = [outstanding EXCEPT ![n] = {q \in @ : q.peer # m}]
    /\ UNCHANGED <<dirty, up, links, conn, sess, want, resub, idx, chan, asks, ops, crashes, drops, ever, boot>>

SendRequest(n, m) ==
    /\ Linked(n, m)
    /\ \E q \in pending[n] :
          /\ q.peer = m
          /\ chan' = [chan EXCEPT ![<<n, m>>] = Append(@, [kind |-> "req", back |-> q.back, boot |-> boot[n]])]
          /\ pending' = [pending EXCEPT ![n] = @ \ {q}]
          /\ outstanding' = [outstanding EXCEPT ![n] = {o \in @ : o.peer # m} \cup {q}]
          /\ asks' = [asks EXCEPT ![n][m] = 1]
    /\ UNCHANGED <<dirty, up, links, view, dead, conn, sess, want, resub, loc, idx, ops, crashes, drops, ever, boot>>

InFlight(n, m) ==
    \/ \E i \in 1..Len(chan[<<n, m>>]) : chan[<<n, m>>][i].kind = "req"
    \/ \E i \in 1..Len(chan[<<m, n>>]) : chan[<<m, n>>][i].kind = "reply"

ReAsk(n, m) ==
    /\ Retry
    /\ Linked(n, m)
    /\ asks[n][m] < MaxAsks
    /\ \E q \in outstanding[n] :
          /\ q.peer = m
          /\ chan' = [chan EXCEPT ![<<n, m>>] = Append(@, [kind |-> "req", back |-> q.back, boot |-> boot[n]])]
    /\ asks' = [asks EXCEPT ![n][m] = @ + 1]
    /\ UNCHANGED <<dirty, up, links, view, dead, conn, sess, want, resub, loc, idx, pending, outstanding, ops, crashes, drops, ever, boot>>

GiveUp(n, m) ==
    /\ asks[n][m] >= MaxAsks
    /\ \E q \in outstanding[n] : q.peer = m
    /\ outstanding' = [outstanding EXCEPT ![n] = {o \in @ : o.peer # m}]
    /\ UNCHANGED <<dirty, up, links, view, dead, conn, sess, want, resub, loc, idx, chan, pending, asks, ops, crashes, drops, ever, boot>>

Lost(n, m) ==
    /\ drops < MaxDrops
    /\ chan[<<n, m>>] # <<>>
    /\ Head(chan[<<n, m>>]).kind \in {"req", "reply", "del"}
    /\ chan' = [chan EXCEPT ![<<n, m>>] = Tail(@)]
    /\ drops' = drops + 1
    /\ dirty' = IF Head(chan[<<n, m>>]).kind = "del" THEN [dirty EXCEPT ![m] = TRUE] ELSE dirty
    /\ UNCHANGED <<up, links, view, dead, conn, sess, want, resub, loc, idx, pending, outstanding, asks, ops, crashes, ever, boot>>

Named(list) == {e.c : e \in list}

AfterClaims(to, from, list) ==
    [c \in Clients |-> IF c \in Named(list) THEN ClaimedAt(to, loc[to][c], from) ELSE loc[to][c]]

Replaces(to, from, claims, c) == to \notin claims[c] /\ claims[c] = {from}

Adds(to, claims, c) == to \notin claims[c]

Applied(to, from, list, replacing) ==
    LET claims == AfterClaims(to, from, list)
        replaced == {e.c : e \in {x \in list : replacing /\ Replaces(to, from, claims, x.c)}}
        added == {x \in list : Adds(to, claims, x.c)}
    IN [ loc |-> claims,
         idx |-> {x \in idx[to] : x[1] \notin replaced}
                 \cup UNION {{<<e.c, t>> : t \in e.subs} : e \in added} ]

Completed(to, from, claims, list) ==
    IF RetainOnComplete
    THEN [c \in Clients |-> IF c \in Named(list) THEN claims[c] ELSE Released(claims[c], from)]
    ELSE claims

Receive(from, to) ==
    /\ chan[<<from, to>>] # <<>>
    /\ LET msg == Head(chan[<<from, to>>])
           rest == [chan EXCEPT ![<<from, to>>] = Tail(@)]
       IN
       CASE msg.kind = "sub" ->
              /\ idx' = [idx EXCEPT ![to] = @ \cup {<<msg.c, msg.t>>}]
              /\ chan' = rest
              /\ UNCHANGED <<loc, pending, outstanding, dirty>>
         [] msg.kind = "subs" ->
              /\ idx' = [idx EXCEPT ![to] = @ \cup {<<msg.c, t>> : t \in msg.ts}]
              /\ chan' = rest
              /\ UNCHANGED <<loc, pending, outstanding, dirty>>
         [] msg.kind = "unsub" ->
              /\ idx' = [idx EXCEPT ![to] = @ \ {<<msg.c, msg.t>>}]
              /\ chan' = rest
              /\ UNCHANGED <<loc, pending, outstanding, dirty>>
         [] msg.kind = "loc" ->
              /\ loc' = [loc EXCEPT ![to][msg.c] = ConnectedAt(to, @, msg.node)]
              /\ chan' = rest
              /\ dirty' = [dirty EXCEPT ![to] = TRUE]
              /\ UNCHANGED <<idx, pending, outstanding>>
         [] msg.kind = "del" ->
              /\ loc' = [loc EXCEPT ![to][msg.c] = Released(@, msg.node)]
              /\ chan' = rest
              /\ dirty' = [dirty EXCEPT ![to] = TRUE]
              /\ UNCHANGED <<idx, pending, outstanding>>
         [] msg.kind = "req" ->
              /\ chan' = [rest EXCEPT ![<<to, from>>] =
                            Append(@, [kind |-> "reply", list |-> Listing(to), boot |-> msg.boot])]
              /\ pending' = IF msg.back
                            THEN [pending EXCEPT ![to] = @ \cup {[peer |-> from, back |-> FALSE]}]
                            ELSE pending
              /\ UNCHANGED <<idx, loc, outstanding, dirty>>
         [] msg.kind = "reply" ->
              /\ LET r == Applied(to, from, msg.list, TRUE)
                     current == ~UniqueRounds \/ msg.boot = boot[to]
                 IN /\ idx' = [idx EXCEPT ![to] = r.idx]
                    /\ loc' = [loc EXCEPT ![to] =
                                 IF current THEN Completed(to, from, r.loc, msg.list) ELSE r.loc]
                    /\ outstanding' = IF current
                                      THEN [outstanding EXCEPT ![to] = {o \in @ : o.peer # from}]
                                      ELSE outstanding
              /\ chan' = rest
              /\ UNCHANGED <<pending, dirty>>
         [] msg.kind = "announce" ->
              /\ LET r == Applied(to, from, msg.list, FALSE) IN
                    /\ idx' = [idx EXCEPT ![to] = r.idx]
                    /\ loc' = [loc EXCEPT ![to] = r.loc]
              /\ chan' = rest
              /\ UNCHANGED <<pending, outstanding, dirty>>
    /\ UNCHANGED <<up, links, view, dead, conn, sess, want, resub, asks, ops, crashes, drops, ever, boot>>

Refresh(n) ==
    /\ AntiEntropy
    /\ up[n]
    /\ dirty[n]
    /\ dirty' = [dirty EXCEPT ![n] = FALSE]
    /\ pending' = [pending EXCEPT ![n] =
                     @ \cup {[peer |-> m, back |-> FALSE] : m \in {x \in Nodes \ {n} : Linked(n, x)}}]
    /\ UNCHANGED <<up, links, view, dead, conn, sess, want, resub, loc, idx, chan,
                   outstanding, asks, ops, crashes, drops, ever, boot>>

Next ==
    \/ \E c \in Clients, n \in Nodes, clean \in BOOLEAN : Connect(c, n, clean)
    \/ \E c \in Clients : Disconnect(c) \/ Resubscribe(c)
    \/ \E c \in Clients, t \in Topics : Sub(c, t) \/ Unsub(c, t)
    \/ \E n \in Nodes : Crash(n) \/ Restart(n) \/ Refresh(n)
    \/ \E n, m \in Nodes : n # m /\ (Heartbeat(n, m) \/ DetectDeath(n, m)
                                     \/ SendRequest(n, m) \/ ReAsk(n, m) \/ GiveUp(n, m)
                                     \/ Lost(n, m) \/ Receive(n, m))

Spec == Init /\ [][Next]_vars

ChannelBound == \A p \in Pairs : Len(chan[p]) <= MaxChan

InvConverged ==
    Settled =>
        \A n \in Nodes, c \in Clients :
            up[n] =>
                /\ loc[n][c] = IF conn[c] = NoNode THEN {} ELSE {conn[c]}
                /\ (conn[c] # NoNode => want[c] \subseteq SubsOf(n, c))

InvNoInventedEntries == \A n \in Nodes : idx[n] \subseteq ever
=============================================================================
