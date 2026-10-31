------------------------- MODULE ClusterShareRouting -------------------------
(***************************************************************************)
(* Design model for routing one $share group across cluster nodes (#191).  *)
(* Each member lives on a fixed home node and is absent (not subscribed),  *)
(* online, or offline (a persistent session that keeps its subscription).  *)
(* Every node runs an mqtt5 router that, for a group it serves, delivers   *)
(* to an online local member, else queues for an offline local member,     *)
(* else does nothing. Each counts as one delivery.                          *)
(*                                                                          *)
(* A publish on origin o runs in three steps:                               *)
(*   Decide      o's event handler reads o's view of the members and picks *)
(*               at most one remote node to serve the group, unless it      *)
(*               believes o already serves it (Rule).                       *)
(*   LocalRoute  o's own router routes the publish as it always does, so it *)
(*               serves the group if o has any member at that moment.       *)
(*   Receive     the chosen node routes the forwarded copy with only the    *)
(*               assigned group, serving it if it has a member then.        *)
(* Views lag the truth and catch up one member at a time (Sync).            *)
(*                                                                          *)
(* Rule "online" = skip remote assignment if o's view has an online local   *)
(*                 member (the plan on #191).                               *)
(* Rule "any"    = skip if o's view has any local member, online or offline.*)
(* LocalExact    = o's view of its own members is always the truth.         *)
(* RemoteOffline = o may assign a remote node whose only member is offline. *)
(*                 MQDB cannot: an offline client's location is dropped on  *)
(*                 disconnect, so a group with only remote offline members  *)
(*                 is not served across nodes.                              *)
(*                                                                          *)
(* Configs:                                                                 *)
(*   ClusterShareRouting.cfg          any, exact, no remote offline -> all   *)
(*        invariants hold (the implementation)                              *)
(*   *_remoteoffline.cfg              any, exact, remote offline -> all      *)
(*        invariants hold                                                   *)
(*   *_online.cfg                     online, exact -> InvNoDuplicate        *)
(*        violated: o queues for its offline member and also assigns a     *)
(*        remote online member                                              *)
(*   *_lag.cfg                        any, lagging local view ->             *)
(*        InvNoDuplicate violated: o's view misses its own new member       *)
(*   *_change.cfg                     also checks InvNoDuplicateUnderChange, *)
(*        expected to fail: a member subscribing between Decide and         *)
(*        LocalRoute is served twice.                                       *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS NodeCount, MemberCount, Pubs, Rule, LocalExact, RemoteOffline, MaxChanges

Nodes == 1..NodeCount
Members == 1..MemberCount
Home == [m \in Members |-> ((m - 1) % NodeCount) + 1]

States == {"absent", "online", "offline"}

VARIABLES state, view, phase, origin, assigned, deliveries, consistent,
          changed, changes, hadMember

vars == <<state, view, phase, origin, assigned, deliveries, consistent,
          changed, changes, hadMember>>

None == "none"

MembersAt(n) == {m \in Members : Home[m] = n}

Present(s, m) == s[m] # "absent"

Reachable(o) ==
    \E m \in Members :
        \/ Home[m] = o /\ Present(state, m)
        \/ state[m] = "online"
        \/ RemoteOffline /\ Present(state, m)

Serves(s, n) == \E m \in MembersAt(n) : Present(s, m)

ViewOf(n) == IF LocalExact
             THEN [m \in Members |-> IF Home[m] = n THEN state[m] ELSE view[n][m]]
             ELSE view[n]

OriginSkips(v, n) ==
    IF Rule = "online"
    THEN \E m \in MembersAt(n) : v[m] = "online"
    ELSE \E m \in MembersAt(n) : Present(v, m)

OnlineRemotes(v, n) == {r \in Nodes \ {n} : \E m \in MembersAt(r) : v[m] = "online"}
PresentRemotes(v, n) == {r \in Nodes \ {n} : \E m \in MembersAt(r) : Present(v, m)}

Choices(v, n) ==
    IF OriginSkips(v, n) THEN {None}
    ELSE IF OnlineRemotes(v, n) # {} THEN OnlineRemotes(v, n)
    ELSE IF RemoteOffline /\ PresentRemotes(v, n) # {} THEN PresentRemotes(v, n)
    ELSE {None}

TypeOK ==
    /\ state \in [Members -> States]
    /\ view \in [Nodes -> [Members -> States]]
    /\ phase \in [Pubs -> {"idle", "decided", "routed", "done"}]
    /\ origin \in [Pubs -> Nodes \cup {None}]
    /\ assigned \in [Pubs -> Nodes \cup {None}]
    /\ deliveries \in [Pubs -> 0..3]
    /\ consistent \in [Pubs -> BOOLEAN]
    /\ changed \in [Pubs -> BOOLEAN]
    /\ changes \in 0..MaxChanges
    /\ hadMember \in [Pubs -> BOOLEAN]

Init ==
    /\ state \in [Members -> States]
    /\ view = [n \in Nodes |-> state]
    /\ phase = [p \in Pubs |-> "idle"]
    /\ origin = [p \in Pubs |-> None]
    /\ assigned = [p \in Pubs |-> None]
    /\ deliveries = [p \in Pubs |-> 0]
    /\ consistent = [p \in Pubs |-> FALSE]
    /\ changed = [p \in Pubs |-> FALSE]
    /\ changes = 0
    /\ hadMember = [p \in Pubs |-> FALSE]

Change(m, s) ==
    /\ changes < MaxChanges
    /\ s # state[m]
    /\ state' = [state EXCEPT ![m] = s]
    /\ changed' = [p \in Pubs |-> changed[p] \/ phase[p] \in {"decided", "routed"}]
    /\ changes' = changes + 1
    /\ UNCHANGED <<view, phase, origin, assigned, deliveries, consistent, hadMember>>

Sync(n, m) ==
    /\ view[n][m] # state[m]
    /\ view' = [view EXCEPT ![n][m] = state[m]]
    /\ UNCHANGED <<state, phase, origin, assigned, deliveries, consistent,
                   changed, changes, hadMember>>

Decide(p, o) ==
    /\ phase[p] = "idle"
    /\ \E r \in Choices(ViewOf(o), o) :
        /\ assigned' = [assigned EXCEPT ![p] = r]
    /\ phase' = [phase EXCEPT ![p] = "decided"]
    /\ origin' = [origin EXCEPT ![p] = o]
    /\ consistent' = [consistent EXCEPT ![p] = (ViewOf(o) = state)]
    /\ hadMember' = [hadMember EXCEPT ![p] = Reachable(o)]
    /\ UNCHANGED <<state, view, deliveries, changed, changes>>

LocalRoute(p) ==
    /\ phase[p] = "decided"
    /\ phase' = [phase EXCEPT ![p] = "routed"]
    /\ deliveries' = [deliveries EXCEPT ![p] =
                        @ + (IF Serves(state, origin[p]) THEN 1 ELSE 0)]
    /\ UNCHANGED <<state, view, origin, assigned, consistent, changed, changes,
                   hadMember>>

Receive(p) ==
    /\ phase[p] = "routed"
    /\ phase' = [phase EXCEPT ![p] = "done"]
    /\ deliveries' = [deliveries EXCEPT ![p] =
                        @ + (IF assigned[p] # None /\ Serves(state, assigned[p])
                             THEN 1 ELSE 0)]
    /\ UNCHANGED <<state, view, origin, assigned, consistent, changed, changes,
                   hadMember>>

Next ==
    \/ \E m \in Members, s \in States : Change(m, s)
    \/ \E n \in Nodes, m \in Members : Sync(n, m)
    \/ \E p \in Pubs, o \in Nodes : Decide(p, o)
    \/ \E p \in Pubs : LocalRoute(p)
    \/ \E p \in Pubs : Receive(p)

Spec == Init /\ [][Next]_vars

InvNoDuplicate ==
    \A p \in Pubs : ~changed[p] => deliveries[p] <= 1

InvNoDuplicateUnderChange ==
    \A p \in Pubs : deliveries[p] <= 1

InvServedWhenStable ==
    \A p \in Pubs :
        (phase[p] = "done" /\ consistent[p] /\ ~changed[p] /\ hadMember[p])
            => deliveries[p] = 1
=============================================================================
