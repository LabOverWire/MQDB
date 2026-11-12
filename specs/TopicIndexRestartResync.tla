---------------------------- MODULE TopicIndexRestartResync ----------------------------
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Nodes, Clients, Topics, Crashable, NoNode,
          MaxOps, MaxCrashes, MaxDrops, MaxAsks, MaxChan,
          Resync, Claims, Supersede, AntiEntropy, LinkedTargets, ReleaseOnDeath,
          RetainOnComplete, AskBack, Retry, Announce, StaleDisk, FalseSuspicion,
          UniqueRounds, AnsweredStaysRefreshed, Digest, GatedRefresh, ParkOnDeath

Entries == Clients \X Topics
Pairs == {p \in Nodes \X Nodes : p[1] # p[2]}

VARIABLES up, links, view, dead, conn, sess, want, resub, loc, idx, chan,
          pending, outstanding, asks, ops, crashes, drops, ever, dirty, boot,
          answered, silent, parked

vars == <<up, links, view, dead, conn, sess, want, resub, loc, idx, chan,
          pending, outstanding, asks, ops, crashes, drops, ever, dirty, boot,
          answered, silent, parked>>

Linked(n, m) == {n, m} \in links

SubsOf(n, c) == {t \in Topics : <<c, t>> \in idx[n]}

Rank == CHOOSE f \in [Nodes -> 1..Cardinality(Nodes)] : \A x, y \in Nodes : x # y => f[x] # f[y]

Newer(x, y) == x.o > y.o \/ (x.o = y.o /\ Rank[x.n] > Rank[y.n])

NodesOf(set) == {x.n : x \in set}

OrdOf(set, node) == IF \E x \in set : x.n = node THEN (CHOOSE x \in set : x.n = node).o ELSE 0

TopOrd(set) == IF set = {} THEN 0 ELSE (CHOOSE x \in set : \A y \in set : x.o >= y.o).o

Newest(set) == CHOOSE x \in set : \A y \in set : y = x \/ Newer(x, y)

Listing(n) == {[c |-> c, subs |-> SubsOf(n, c), o |-> OrdOf(loc[n][c], n)] :
                  c \in {x \in Clients : n \in NodesOf(loc[n][x])}}

LocReceivers(n) ==
    IF LinkedTargets THEN {m \in Nodes \ {n} : Linked(n, m)}
    ELSE {m \in Nodes \ {n} : Linked(n, m) /\ view[n][m]}

TopicReceivers(n) == {m \in Nodes \ {n} : Linked(n, m)}

SendTo(ch, from, targets, msg) ==
    [p \in Pairs |-> IF p[1] = from /\ p[2] \in targets THEN Append(ch[p], msg) ELSE ch[p]]

Without(set, node) == {x \in set : x.n # node}

Claimed(set, node, o) ==
    IF Claims THEN Without(set, node) \cup {[n |-> node, o |-> o]} ELSE {[n |-> node, o |-> o]}

ClaimedAt(to, set, node, o) == IF ReleaseOnDeath /\ dead[to][node] THEN set ELSE Claimed(set, node, o)

ConnectedAt(to, set, node, o) ==
    LET new == [n |-> node, o |-> o] IN
    IF ReleaseOnDeath /\ dead[to][node] THEN set
    ELSE IF ~Claims THEN {new}
    ELSE IF Supersede THEN {x \in Without(set, node) : x.n = to \/ Newer(x, new)} \cup {new}
    ELSE Without(set, node) \cup {new}

Released(set, node) == IF Claims THEN Without(set, node) ELSE {}

ReleasedUpTo(set, node, o) == IF Claims THEN {x \in set : x.n # node \/ x.o > o} ELSE {}

AskerView(n, m) == {[c |-> c, subs |-> SubsOf(n, c), o |-> OrdOf(loc[n][c], m)] :
                      c \in {x \in Clients : m \in NodesOf(loc[n][x])}}

Request(n, m, back) ==
    [kind |-> "req", back |-> back, boot |-> boot[n],
     view |-> IF Digest THEN AskerView(n, m) ELSE {}]

AllPeers(n) == [m \in Nodes |-> m # n]

Settled ==
    /\ \A p \in Pairs : chan[p] = <<>>
    /\ \A n \in Nodes : up[n] => pending[n] = {}
    /\ \A n, m \in Nodes : (up[n] /\ n # m) => (view[n][m] = up[m] /\ (up[m] => Linked(n, m)))
    /\ \A c \in Clients : ~resub[c]
    /\ Retry => \A n \in Nodes : up[n] => outstanding[n] = {}
    /\ \A n, m \in Nodes : (up[n] /\ up[m] /\ n # m) => ~(silent[n][m] /\ ~answered[n][m])
    /\ AntiEntropy => \A n, m \in Nodes : (up[n] /\ up[m] /\ n # m /\ dirty[n][m]) => silent[n][m]

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
    /\ dirty = [n \in Nodes |-> [m \in Nodes |-> FALSE]]
    /\ answered = [n \in Nodes |-> [m \in Nodes |-> FALSE]]
    /\ silent = [n \in Nodes |-> [m \in Nodes |-> FALSE]]
    /\ parked = [n \in Nodes |-> [m \in Nodes |-> {}]]
    /\ boot = [n \in Nodes |-> 0]

Connect(c, n, clean) ==
    LET old == conn[c]
        resumed == ~clean /\ sess[c] = n
        ord == TopOrd(loc[n][c]) + 1
        located == [kind |-> "loc", c |-> c, node |-> n, o |-> ord]
        released == [kind |-> "del", c |-> c, node |-> old, o |-> OrdOf(loc[old][c], old)]
        announced == [kind |-> "announce", list |-> {[c |-> c, subs |-> SubsOf(n, c), o |-> ord]}]
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
                 IF m = n THEN [loc[m] EXCEPT ![c] = IF Supersede \/ ~Claims
                                                     THEN {[n |-> n, o |-> ord]}
                                                     ELSE Without(@, n) \cup {[n |-> n, o |-> ord]}]
                 ELSE IF m = old THEN [loc[m] EXCEPT ![c] = Released(@, old)]
                 ELSE loc[m]]
    /\ chan' = IF Announce /\ ~clean
               THEN SendTo(afterLoc, n, TopicReceivers(n), announced)
               ELSE afterLoc
    /\ ops' = ops + 1
    /\ UNCHANGED <<dirty, up, links, view, dead, want, idx, pending, outstanding, asks, crashes, drops, ever, boot, answered, silent, parked>>

Resubscribe(c) ==
    LET n == conn[c] IN
    /\ resub[c]
    /\ n # NoNode
    /\ idx' = [idx EXCEPT ![n] = @ \cup {<<c, t>> : t \in want[c]}]
    /\ chan' = SendTo(chan, n, TopicReceivers(n), [kind |-> "subs", c |-> c, ts |-> want[c]])
    /\ resub' = [resub EXCEPT ![c] = FALSE]
    /\ UNCHANGED <<dirty, up, links, view, dead, conn, sess, want, loc, pending, outstanding, asks, ops, crashes, drops, ever, boot, answered, silent, parked>>

Disconnect(c) ==
    LET n == conn[c] IN
    /\ ops < MaxOps
    /\ n # NoNode
    /\ conn' = [conn EXCEPT ![c] = NoNode]
    /\ resub' = [resub EXCEPT ![c] = FALSE]
    /\ loc' = [loc EXCEPT ![n][c] = Released(@, n)]
    /\ chan' = SendTo(chan, n, LocReceivers(n), [kind |-> "del", c |-> c, node |-> n, o |-> OrdOf(loc[n][c], n)])
    /\ ops' = ops + 1
    /\ UNCHANGED <<dirty, up, links, view, dead, sess, want, idx, pending, outstanding, asks, crashes, drops, ever, boot, answered, silent, parked>>

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
    /\ UNCHANGED <<dirty, up, links, view, dead, conn, sess, resub, loc, pending, outstanding, asks, crashes, drops, boot, answered, silent, parked>>

Unsub(c, t) ==
    LET n == conn[c] IN
    /\ ops < MaxOps
    /\ n # NoNode
    /\ t \in want[c]
    /\ want' = [want EXCEPT ![c] = @ \ {t}]
    /\ idx' = [idx EXCEPT ![n] = @ \ {<<c, t>>}]
    /\ chan' = SendTo(chan, n, TopicReceivers(n), [kind |-> "unsub", c |-> c, t |-> t])
    /\ ops' = ops + 1
    /\ UNCHANGED <<dirty, up, links, view, dead, conn, sess, resub, loc, pending, outstanding, asks, crashes, drops, ever, boot, answered, silent, parked>>

Crash(n) ==
    /\ n \in Crashable
    /\ up[n]
    /\ crashes < MaxCrashes
    /\ up' = [up EXCEPT ![n] = FALSE]
    /\ links' = {l \in links : n \notin l}
    /\ view' = [view EXCEPT ![n] = [m \in Nodes |-> FALSE]]
    /\ dead' = [dead EXCEPT ![n] = [m \in Nodes |-> FALSE]]
    /\ dirty' = [m \in Nodes |-> IF m = n THEN [x \in Nodes |-> FALSE] ELSE [dirty[m] EXCEPT ![n] = TRUE]]
    /\ answered' = [answered EXCEPT ![n] = [m \in Nodes |-> FALSE]]
    /\ silent' = [silent EXCEPT ![n] = [m \in Nodes |-> FALSE]]
    /\ parked' = [parked EXCEPT ![n] = [m \in Nodes |-> {}]]
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
    /\ chan' = [p \in Pairs |-> IF p[2] = n THEN <<>> ELSE chan[p]]
    /\ UNCHANGED <<dirty, links, view, dead, conn, sess, want, resub, loc, pending, outstanding, asks, ops, crashes, drops, ever, answered, silent, parked>>

Heartbeat(n, m) ==
    /\ n # m
    /\ up[n]
    /\ up[m]
    /\ ~view[n][m]
    /\ links' = links \cup {{n, m}}
    /\ view' = [view EXCEPT ![n][m] = TRUE]
    /\ dead' = [dead EXCEPT ![n][m] = FALSE]
    /\ pending' = IF Resync THEN [pending EXCEPT ![n] = @ \cup {[peer |-> m, back |-> AskBack]}] ELSE pending
    /\ silent' = [silent EXCEPT ![n][m] = FALSE]
    /\ loc' = [loc EXCEPT ![n] = [c \in Clients |->
                 LET back == {e.x : e \in {p \in parked[n][m] : p.c = c}}
                 IN IF back # {} /\ ~\E y \in @[c] : \E x \in back : Newer(y, x)
                    THEN Without(@[c], m) \cup back
                    ELSE @[c]]]
    /\ parked' = [parked EXCEPT ![n][m] = {}]
    /\ UNCHANGED <<dirty, up, conn, sess, want, resub, idx, chan, outstanding, asks, ops, crashes, drops, ever, boot, answered>>

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
    /\ dirty' = [dirty EXCEPT ![n][m] = FALSE]
    /\ answered' = [answered EXCEPT ![n][m] = FALSE]
    /\ silent' = [silent EXCEPT ![n][m] = FALSE]
    /\ parked' = IF ReleaseOnDeath /\ ParkOnDeath
                 THEN [parked EXCEPT ![n][m] =
                         {[c |-> c, x |-> x] : c \in Clients, x \in {y \in loc[n][c] : y.n = m}}]
                 ELSE parked
    /\ UNCHANGED <<up, links, conn, sess, want, resub, idx, chan, asks, ops, crashes, drops, ever, boot>>

SendRequest(n, m) ==
    /\ Linked(n, m)
    /\ \E q \in pending[n] :
          /\ q.peer = m
          /\ chan' = [chan EXCEPT ![<<n, m>>] = Append(@, Request(n, m, q.back))]
          /\ pending' = [pending EXCEPT ![n] = @ \ {q}]
          /\ outstanding' = [outstanding EXCEPT ![n] = {o \in @ : o.peer # m} \cup {q}]
          /\ asks' = [asks EXCEPT ![n][m] = 1]
    /\ UNCHANGED <<dirty, up, links, view, dead, conn, sess, want, resub, loc, idx, ops, crashes, drops, ever, boot, answered, silent, parked>>

InFlight(n, m) ==
    \/ \E i \in 1..Len(chan[<<n, m>>]) : chan[<<n, m>>][i].kind = "req"
    \/ \E i \in 1..Len(chan[<<m, n>>]) : chan[<<m, n>>][i].kind = "reply"

ReAsk(n, m) ==
    /\ Retry
    /\ Linked(n, m)
    /\ asks[n][m] < MaxAsks
    /\ \E q \in outstanding[n] :
          /\ q.peer = m
          /\ chan' = [chan EXCEPT ![<<n, m>>] = Append(@, Request(n, m, q.back))]
    /\ asks' = [asks EXCEPT ![n][m] = @ + 1]
    /\ UNCHANGED <<dirty, up, links, view, dead, conn, sess, want, resub, loc, idx, pending, outstanding, ops, crashes, drops, ever, boot, answered, silent, parked>>

GiveUp(n, m) ==
    /\ asks[n][m] >= MaxAsks
    /\ \E q \in outstanding[n] : q.peer = m
    /\ outstanding' = [outstanding EXCEPT ![n] = {o \in @ : o.peer # m}]
    /\ silent' = IF ~AnsweredStaysRefreshed \/ ~answered[n][m]
                 THEN [silent EXCEPT ![n][m] = TRUE]
                 ELSE silent
    /\ UNCHANGED <<dirty, up, links, view, dead, conn, sess, want, resub, loc, idx, chan, pending, asks, ops, crashes, drops, ever, boot, answered, parked>>

Lost(n, m) ==
    /\ drops < MaxDrops
    /\ chan[<<n, m>>] # <<>>
    /\ Head(chan[<<n, m>>]).kind \in {"req", "reply", "same", "del", "loc"}
    /\ chan' = [chan EXCEPT ![<<n, m>>] = Tail(@)]
    /\ drops' = drops + 1
    /\ dirty' = IF Head(chan[<<n, m>>]).kind \in {"del", "loc"}
              THEN [dirty EXCEPT ![m] = AllPeers(m), ![n][m] = TRUE]
              ELSE [dirty EXCEPT ![m][n] = TRUE, ![n][m] = TRUE]
    /\ UNCHANGED <<up, links, view, dead, conn, sess, want, resub, loc, idx, pending, outstanding, asks, ops, crashes, ever, boot, answered, silent, parked>>

Named(list) == {e.c : e \in list}

AfterClaims(to, from, list) ==
    [c \in Clients |-> IF c \in Named(list)
                       THEN ClaimedAt(to, loc[to][c], from, (CHOOSE e \in list : e.c = c).o)
                       ELSE loc[to][c]]

Outranks(to, from, claims, e) ==
    /\ claims[e.c] # {}
    /\ Newest(claims[e.c]).n = to
    /\ Newer(Newest(claims[e.c]), [n |-> from, o |-> e.o])

Replaces(to, from, claims, c) == NodesOf(claims[c]) = {from}

Applied(to, from, list, replacing) ==
    LET claims == AfterClaims(to, from, list)
        added == {x \in list : ~Outranks(to, from, claims, x)}
        replaced == {e.c : e \in {x \in added : replacing /\ Replaces(to, from, claims, x.c)}}
    IN [ loc |-> claims,
         idx |-> {x \in idx[to] : x[1] \notin replaced}
                 \cup UNION {{<<e.c, t>> : t \in e.subs} : e \in added} ]

Completed(to, from, claims, list) ==
    IF RetainOnComplete
    THEN [c \in Clients |-> IF c \in Named(list) THEN claims[c] ELSE Released(claims[c], from)]
    ELSE claims

Answered(to, from, current) ==
    IF current
    THEN /\ outstanding' = [outstanding EXCEPT ![to] = {o \in @ : o.peer # from}]
         /\ dirty' = [dirty EXCEPT ![to][from] = FALSE]
         /\ answered' = [answered EXCEPT ![to][from] = TRUE]
         /\ silent' = [silent EXCEPT ![to][from] = FALSE]
    ELSE UNCHANGED <<outstanding, dirty, answered, silent>>

Receive(from, to) ==
    /\ up[to]
    /\ chan[<<from, to>>] # <<>>
    /\ LET msg == Head(chan[<<from, to>>])
           rest == [chan EXCEPT ![<<from, to>>] = Tail(@)]
       IN
       CASE msg.kind = "sub" ->
              /\ idx' = [idx EXCEPT ![to] = @ \cup {<<msg.c, msg.t>>}]
              /\ chan' = rest
              /\ UNCHANGED <<loc, pending, outstanding, dirty, answered, silent, parked>>
         [] msg.kind = "subs" ->
              /\ idx' = [idx EXCEPT ![to] = @ \cup {<<msg.c, t>> : t \in msg.ts}]
              /\ chan' = rest
              /\ UNCHANGED <<loc, pending, outstanding, dirty, answered, silent, parked>>
         [] msg.kind = "unsub" ->
              /\ idx' = [idx EXCEPT ![to] = @ \ {<<msg.c, msg.t>>}]
              /\ chan' = rest
              /\ UNCHANGED <<loc, pending, outstanding, dirty, answered, silent, parked>>
         [] msg.kind = "loc" ->
              /\ loc' = [loc EXCEPT ![to][msg.c] = ConnectedAt(to, @, msg.node, msg.o)]
              /\ chan' = rest
              /\ dirty' = [dirty EXCEPT ![to] = AllPeers(to)]
              /\ UNCHANGED <<idx, pending, outstanding, answered, silent, parked>>
         [] msg.kind = "del" ->
              /\ loc' = [loc EXCEPT ![to][msg.c] = ReleasedUpTo(@, msg.node, msg.o)]
              /\ parked' = [parked EXCEPT ![to][msg.node] =
                               {p \in @ : p.c # msg.c \/ p.x.o > msg.o}]
              /\ chan' = rest
              /\ dirty' = [dirty EXCEPT ![to] = AllPeers(to)]
              /\ UNCHANGED <<idx, pending, outstanding, answered, silent>>
         [] msg.kind = "req" ->
              /\ chan' = [rest EXCEPT ![<<to, from>>] =
                            Append(@, IF Digest /\ msg.view = Listing(to)
                                      THEN [kind |-> "same", boot |-> msg.boot, view |-> msg.view]
                                      ELSE [kind |-> "reply", list |-> Listing(to), boot |-> msg.boot])]
              /\ pending' = IF msg.back
                            THEN [pending EXCEPT ![to] = @ \cup {[peer |-> from, back |-> FALSE]}]
                            ELSE pending
              /\ UNCHANGED <<idx, loc, outstanding, dirty, answered, silent, parked>>
         [] msg.kind = "reply" ->
              /\ LET current == ~UniqueRounds \/ msg.boot = boot[to]
                     r == Applied(to, from, msg.list, TRUE)
                 IN IF current
                    THEN /\ idx' = [idx EXCEPT ![to] = r.idx]
                         /\ loc' = [loc EXCEPT ![to] = Completed(to, from, r.loc, msg.list)]
                         /\ Answered(to, from, TRUE)
                    ELSE /\ UNCHANGED <<idx, loc>>
                         /\ Answered(to, from, FALSE)
              /\ chan' = rest
              /\ UNCHANGED <<pending, parked>>
         [] msg.kind = "same" ->
              /\ IF (~UniqueRounds \/ msg.boot = boot[to]) /\ msg.view # AskerView(to, from)
                 THEN /\ answered' = [answered EXCEPT ![to][from] = TRUE]
                      /\ silent' = [silent EXCEPT ![to][from] = FALSE]
                      /\ UNCHANGED <<outstanding, dirty>>
                 ELSE Answered(to, from, ~UniqueRounds \/ msg.boot = boot[to])
              /\ chan' = rest
              /\ UNCHANGED <<idx, loc, pending, parked>>
         [] msg.kind = "announce" ->
              /\ LET r == Applied(to, from, msg.list, FALSE) IN
                    /\ idx' = [idx EXCEPT ![to] = r.idx]
                    /\ loc' = [loc EXCEPT ![to] = r.loc]
              /\ chan' = rest
              /\ UNCHANGED <<pending, outstanding, dirty, answered, silent, parked>>
    /\ UNCHANGED <<up, links, view, dead, conn, sess, want, resub, asks, ops, crashes, drops, ever, boot, parked>>

Refresh(n, m) ==
    /\ AntiEntropy
    /\ up[n]
    /\ Linked(n, m)
    /\ (GatedRefresh => dirty[n][m])
    /\ ~silent[n][m]
    /\ \A q \in pending[n] \cup outstanding[n] : q.peer # m
    /\ pending' = [pending EXCEPT ![n] = @ \cup {[peer |-> m, back |-> FALSE]}]
    /\ UNCHANGED <<dirty, up, links, view, dead, conn, sess, want, resub, loc, idx, chan,
                   outstanding, asks, ops, crashes, drops, ever, boot, answered, silent, parked>>

Next ==
    \/ \E c \in Clients, n \in Nodes, clean \in BOOLEAN : Connect(c, n, clean)
    \/ \E c \in Clients : Disconnect(c) \/ Resubscribe(c)
    \/ \E c \in Clients, t \in Topics : Sub(c, t) \/ Unsub(c, t)
    \/ \E n \in Nodes : Crash(n) \/ Restart(n)
    \/ \E n, m \in Nodes : n # m /\ (Heartbeat(n, m) \/ DetectDeath(n, m)
                                     \/ SendRequest(n, m) \/ ReAsk(n, m) \/ GiveUp(n, m)
                                     \/ Lost(n, m) \/ Receive(n, m) \/ Refresh(n, m))

Spec == Init /\ [][Next]_vars

ChannelBound == \A p \in Pairs : Len(chan[p]) <= MaxChan

InvConverged ==
    Settled =>
        \A n \in Nodes, c \in Clients :
            up[n] =>
                /\ NodesOf(loc[n][c]) = IF conn[c] = NoNode THEN {} ELSE {conn[c]}
                /\ (conn[c] # NoNode => want[c] \subseteq SubsOf(n, c))

InvNoInventedEntries == \A n \in Nodes : idx[n] \subseteq ever
=============================================================================
