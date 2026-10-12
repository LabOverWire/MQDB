------------------------- MODULE ClusterRetainedSync -------------------------
(***************************************************************************)
(* Design model for the retained-message sync loop between a topic's       *)
(* partition primary P, its replica R, and a node O that holds neither.    *)
(* One retained topic. Every node runs an MQTT broker with its own retained *)
(* copy (broker); P and R also hold the replicated retained store (store).  *)
(*                                                                          *)
(* A change to a node's broker copy fires on_retained_set later (events),   *)
(* which turns it into a replicated write unless it is recognised as the  *)
(* echo of a sync: on P the write is applied and replicated to R; elsewhere *)
(* it is applied locally and sent to P as a write request. P applies a      *)
(* write request, syncs it into its own broker, and replicates it to R. R   *)
(* applies a replicated write and syncs it into its broker. A sync is a     *)
(* token on the node's local publish queue (localq); applying it reads a    *)
(* value (ReadLocal) and later writes it into the broker (WriteLocal),      *)
(* which fires on_retained_set again. A subscribe on O fetches P's stored   *)
(* copy and injects it the same way (deliver_retained_messages). Links are *)
(* FIFO and lossless.                                                      *)
(*                                                                          *)
(* Mode "old"     = main: one time-based flag per node                      *)
(*   (synced_retained_topics). Syncing a set raises it, syncing a clear     *)
(*   does not; while it is raised, on_retained_set ignores every change and *)
(*   every sync is skipped. Expire lowers it at any time (the 5 s TTL).    *)
(* Mode "new"     = echo by value: before queueing a value, the node records *)
(*   it as a pending echo; on_retained_set consumes a pending echo of the   *)
(*   same value and replicates anything else. Syncs are never skipped.      *)
(* Mode "refresh" = a holder queues a refresh of the topic after every store *)
(*   change, its own client changes included; applying it reads the store  *)
(*   at that moment, skips the write if the broker already holds the value, *)
(*   and otherwise records the echo just before writing.                    *)
(* Sequential = each node applies one queued sync at a time (FALSE: reads   *)
(*   and writes of several syncs interleave, as when every local publish is *)
(*   a separately spawned task).                                            *)
(*                                                                          *)
(* Configs:                                                                 *)
(*   ClusterRetainedSync.cfg       refresh, sequential -> all invariants     *)
(*        hold                                                              *)
(*   *_concurrent.cfg              refresh, concurrent -> InvConverged       *)
(*        violated: an older read is written after a newer one              *)
(*   *_echoonly.cfg                new -> InvConverged violated: a sync      *)
(*        queued before a newer client change on the same node overwrites   *)
(*        it                                                                *)
(*   *_oldloop.cfg                 old -> InvWriteBound violated: a clear    *)
(*        bounces between P and R and the writes keep growing               *)
(*   *_oldstale.cfg                old -> InvConverged violated: a change    *)
(*        within the TTL is ignored or skipped and a broker keeps the old  *)
(*        value                                                             *)
(*                                                                          *)
(* NOT MODELLED: O's broker copy is not kept in sync by replication (it only *)
(* receives writes its own clients make or a subscribe fetches), so          *)
(* convergence is checked for P and R only; message drops, local queue       *)
(* overflow, restarts, catch-up, partition moves, QoS, and the retained     *)
(* limits. Bounded by MaxOps client publishes and subscribes.               *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Vals, MaxOps, Mode, Sequential, WriteBound

Empty == "empty"
Refresh == "refresh"
Payloads == Vals \cup {Empty}
Nodes == {"P", "R", "O"}
Holders == {"P", "R"}
Requesters == {"R", "O"}

VARIABLES broker, store, events, localq, inflight, reqs, repl, synced, echo, ops, writes

vars == <<broker, store, events, localq, inflight, reqs, repl, synced, echo, ops, writes>>

NoEcho == [p \in Payloads |-> 0]

Init ==
    /\ broker = [n \in Nodes |-> Empty]
    /\ store = [n \in Holders |-> Empty]
    /\ events = [n \in Nodes |-> <<>>]
    /\ localq = [n \in Nodes |-> <<>>]
    /\ inflight = [n \in Nodes |-> <<>>]
    /\ reqs = [n \in Requesters |-> <<>>]
    /\ repl = <<>>
    /\ synced = [n \in Nodes |-> FALSE]
    /\ echo = [n \in Nodes |-> NoEcho]
    /\ ops = 0
    /\ writes = 0

RemoveAt(s, i) == SubSeq(s, 1, i - 1) \o SubSeq(s, i + 1, Len(s))

OldInject(n, v) ==
    IF synced[n]
    THEN UNCHANGED <<localq, synced, echo>>
    ELSE /\ localq' = [localq EXCEPT ![n] = Append(@, v)]
         /\ synced' = (IF v = Empty THEN synced ELSE [synced EXCEPT ![n] = TRUE])
         /\ UNCHANGED echo

NewInject(n, v) ==
    /\ localq' = [localq EXCEPT ![n] = Append(@, v)]
    /\ echo' = [echo EXCEPT ![n][v] = @ + 1]
    /\ UNCHANGED synced

RefreshInject(n, v) ==
    /\ localq' = [localq EXCEPT ![n] = Append(@, Refresh)]
    /\ UNCHANGED <<echo, synced>>

Inject(n, v) ==
    CASE Mode = "old" -> OldInject(n, v)
      [] Mode = "new" -> NewInject(n, v)
      [] Mode = "refresh" -> RefreshInject(n, v)

ClientPublish(n, v) ==
    /\ ops < MaxOps
    /\ broker' = [broker EXCEPT ![n] = v]
    /\ events' = [events EXCEPT ![n] = Append(@, v)]
    /\ ops' = ops + 1
    /\ UNCHANGED <<store, localq, inflight, reqs, repl, synced, echo, writes>>

OldDeliver ==
    IF synced["O"]
    THEN UNCHANGED <<localq, synced, echo>>
    ELSE /\ localq' = [localq EXCEPT !["O"] = Append(@, store["P"])]
         /\ synced' = [synced EXCEPT !["O"] = TRUE]
         /\ UNCHANGED echo

SubscribeOnO ==
    /\ ops < MaxOps
    /\ store["P"] # Empty
    /\ ops' = ops + 1
    /\ CASE Mode = "old" -> OldDeliver
         [] Mode = "new" -> NewInject("O", store["P"])
         [] Mode = "refresh" -> (/\ localq' = [localq EXCEPT !["O"] = Append(@, store["P"])]
                                 /\ UNCHANGED <<echo, synced>>)
    /\ UNCHANGED <<broker, store, events, inflight, reqs, repl, writes>>

SelfRefresh(n) ==
    localq' = IF Mode = "refresh" /\ n \in Holders
              THEN [localq EXCEPT ![n] = Append(@, Refresh)]
              ELSE localq

Replicate(n, v) ==
    /\ writes' = writes + 1
    /\ SelfRefresh(n)
    /\ IF n = "P"
       THEN /\ store' = [store EXCEPT !["P"] = v]
            /\ repl' = Append(repl, v)
            /\ UNCHANGED reqs
       ELSE /\ store' = (IF n \in Holders THEN [store EXCEPT ![n] = v] ELSE store)
            /\ reqs' = [reqs EXCEPT ![n] = Append(@, v)]
            /\ UNCHANGED repl

Suppressed(n, v) ==
    IF Mode = "old" THEN synced[n] ELSE echo[n][v] > 0

OnRetainedSet(n) ==
    /\ events[n] # <<>>
    /\ LET v == Head(events[n]) IN
       /\ events' = [events EXCEPT ![n] = Tail(@)]
       /\ IF Suppressed(n, v)
          THEN /\ echo' = (IF Mode = "old" THEN echo ELSE [echo EXCEPT ![n][v] = @ - 1])
               /\ UNCHANGED <<store, reqs, repl, writes, localq>>
          ELSE /\ Replicate(n, v)
               /\ UNCHANGED echo
    /\ UNCHANGED <<broker, inflight, synced, ops>>

HandleRequest(src) ==
    /\ reqs[src] # <<>>
    /\ LET v == Head(reqs[src]) IN
       /\ reqs' = [reqs EXCEPT ![src] = Tail(@)]
       /\ store' = [store EXCEPT !["P"] = v]
       /\ repl' = Append(repl, v)
       /\ Inject("P", v)
    /\ UNCHANGED <<broker, events, inflight, ops, writes>>

HandleReplication ==
    /\ repl # <<>>
    /\ LET v == Head(repl) IN
       /\ repl' = Tail(repl)
       /\ store' = [store EXCEPT !["R"] = v]
       /\ Inject("R", v)
    /\ UNCHANGED <<broker, events, inflight, reqs, ops, writes>>

ReadLocal(n) ==
    /\ localq[n] # <<>>
    /\ Sequential => inflight[n] = <<>>
    /\ LET token == Head(localq[n])
           v == IF token = Refresh THEN store[n] ELSE token
       IN /\ localq' = [localq EXCEPT ![n] = Tail(@)]
          /\ inflight' = [inflight EXCEPT ![n] = Append(@, v)]
    /\ UNCHANGED <<broker, store, events, reqs, repl, synced, echo, ops, writes>>

WriteLocal(n) ==
    /\ inflight[n] # <<>>
    /\ \E i \in 1..Len(inflight[n]) :
        LET v == inflight[n][i] IN
        /\ inflight' = [inflight EXCEPT ![n] = RemoveAt(@, i)]
        /\ IF Mode = "refresh" /\ broker[n] = v
           THEN UNCHANGED <<broker, events, echo>>
           ELSE /\ broker' = [broker EXCEPT ![n] = v]
                /\ events' = [events EXCEPT ![n] = Append(@, v)]
                /\ echo' = (IF Mode = "refresh" THEN [echo EXCEPT ![n][v] = @ + 1] ELSE echo)
    /\ UNCHANGED <<store, localq, reqs, repl, synced, ops, writes>>

Expire(n) ==
    /\ Mode = "old"
    /\ synced[n]
    /\ synced' = [synced EXCEPT ![n] = FALSE]
    /\ UNCHANGED <<broker, store, events, localq, inflight, reqs, repl, echo, ops, writes>>

Next ==
    \/ \E n \in Nodes, v \in Payloads : ClientPublish(n, v)
    \/ SubscribeOnO
    \/ \E n \in Nodes : OnRetainedSet(n)
    \/ \E src \in Requesters : HandleRequest(src)
    \/ HandleReplication
    \/ \E n \in Nodes : ReadLocal(n)
    \/ \E n \in Nodes : WriteLocal(n)
    \/ \E n \in Nodes : Expire(n)

Spec == Init /\ [][Next]_vars

Quiescent ==
    /\ \A n \in Nodes : events[n] = <<>> /\ localq[n] = <<>> /\ inflight[n] = <<>>
    /\ \A n \in Requesters : reqs[n] = <<>>
    /\ repl = <<>>

TypeOK_Mode == Mode \in {"old", "new", "refresh"}

TypeOK ==
    /\ broker \in [Nodes -> Payloads]
    /\ store \in [Holders -> Payloads]
    /\ synced \in [Nodes -> BOOLEAN]
    /\ ops \in 0..MaxOps

InvWriteBound == writes <= WriteBound

InvConverged ==
    Quiescent =>
        /\ store["R"] = store["P"]
        /\ broker["P"] = store["P"]
        /\ broker["R"] = store["P"]

InvNoEchoLeft ==
    (Mode # "old" /\ Quiescent) => \A n \in Nodes : echo[n] = NoEcho

=============================================================================
