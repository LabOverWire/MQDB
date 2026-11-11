---------------------------- MODULE TopicIndexRestartResync ----------------------------
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Nodes, Clients, Topics, Ownership, Crashable, MaxOps, MaxCrashes, Mode, StaleDisk

Owner == [c \in Clients |-> CHOOSE n \in Nodes : <<c, n>> \in Ownership]

Entries == Clients \X Topics
Pairs == {p \in Nodes \X Nodes : p[1] # p[2]}

VARIABLES up, idx, chan, pending, ops, crashes

vars == <<up, idx, chan, pending, ops, crashes>>

Truth == {e \in Entries : e \in idx[Owner[e[1]]]}

Quiescent == (\A p \in Pairs : chan[p] = <<>>) /\ (\A n \in Nodes : pending[n] = {})

Init ==
    /\ up = [n \in Nodes |-> TRUE]
    /\ idx = [n \in Nodes |-> {}]
    /\ chan = [p \in Pairs |-> <<>>]
    /\ pending = [n \in Nodes |-> {}]
    /\ ops = 0
    /\ crashes = 0

Broadcast(from, msg) ==
    [p \in Pairs |->
        IF p[1] = from /\ up[p[2]] THEN Append(chan[p], msg) ELSE chan[p]]

Sub(c, t) ==
    LET o == Owner[c] IN
    /\ ops < MaxOps
    /\ <<c, t>> \notin idx[o]
    /\ idx' = [idx EXCEPT ![o] = @ \cup {<<c, t>>}]
    /\ chan' = Broadcast(o, [kind |-> "sub", e |-> <<c, t>>])
    /\ ops' = ops + 1
    /\ UNCHANGED <<up, pending, crashes>>

Unsub(c, t) ==
    LET o == Owner[c] IN
    /\ ops < MaxOps
    /\ <<c, t>> \in idx[o]
    /\ idx' = [idx EXCEPT ![o] = @ \ {<<c, t>>}]
    /\ chan' = Broadcast(o, [kind |-> "unsub", e |-> <<c, t>>])
    /\ ops' = ops + 1
    /\ UNCHANGED <<up, pending, crashes>>

Crash(n) ==
    /\ n \in Crashable
    /\ up[n]
    /\ crashes < MaxCrashes
    /\ up' = [up EXCEPT ![n] = FALSE]
    /\ idx' = [idx EXCEPT ![n] = {}]
    /\ chan' = [p \in Pairs |-> IF n \in {p[1], p[2]} THEN <<>> ELSE chan[p]]
    /\ pending' = [pending EXCEPT ![n] = {}]
    /\ crashes' = crashes + 1
    /\ UNCHANGED ops

Restart(n) ==
    /\ ~up[n]
    /\ \E disk \in IF StaleDisk THEN SUBSET Entries ELSE {{}} :
          idx' = [idx EXCEPT ![n] = disk]
    /\ up' = [up EXCEPT ![n] = TRUE]
    /\ pending' = [pending EXCEPT ![n] = IF Mode = "none" THEN {} ELSE Nodes \ {n}]
    /\ UNCHANGED <<chan, ops, crashes>>

SendRequest(n, m) ==
    /\ up[n]
    /\ up[m]
    /\ m \in pending[n]
    /\ chan' = [chan EXCEPT ![<<n, m>>] = Append(@, [kind |-> "req"])]
    /\ pending' = [pending EXCEPT ![n] = @ \ {m}]
    /\ UNCHANGED <<up, idx, ops, crashes>>

Replayed(n) ==
    IF Mode \in {"own", "replace"} THEN {e \in idx[n] : Owner[e[1]] = n} ELSE idx[n]

Applied(to, from, set) ==
    IF Mode = "replace"
    THEN {e \in idx[to] : Owner[e[1]] # from} \cup set
    ELSE idx[to] \cup set

Receive(from, to) ==
    /\ chan[<<from, to>>] # <<>>
    /\ LET msg == Head(chan[<<from, to>>])
           rest == [chan EXCEPT ![<<from, to>>] = Tail(@)]
       IN
       CASE msg.kind = "sub" ->
              /\ idx' = [idx EXCEPT ![to] = @ \cup {msg.e}]
              /\ chan' = rest
         [] msg.kind = "unsub" ->
              /\ idx' = [idx EXCEPT ![to] = @ \ {msg.e}]
              /\ chan' = rest
         [] msg.kind = "req" ->
              /\ idx' = idx
              /\ chan' = [rest EXCEPT ![<<to, from>>] =
                            Append(@, [kind |-> "replay", set |-> Replayed(to)])]
         [] msg.kind = "replay" ->
              /\ idx' = [idx EXCEPT ![to] = Applied(to, from, msg.set)]
              /\ chan' = rest
    /\ UNCHANGED <<up, pending, ops, crashes>>

Next ==
    \/ \E c \in Clients, t \in Topics : Sub(c, t) \/ Unsub(c, t)
    \/ \E n \in Nodes : Crash(n) \/ Restart(n)
    \/ \E n, m \in Nodes : n # m /\ (SendRequest(n, m) \/ Receive(n, m))

Spec == Init /\ [][Next]_vars

InvConverged == Quiescent => \A n \in Nodes : up[n] => idx[n] = Truth

InvNoResurrection == Quiescent => \A n \in Nodes : up[n] => idx[n] \subseteq Truth
=============================================================================
