------------------------- MODULE ClusterSessionRouting -------------------------
(***************************************************************************)
(* Can a client that is connected and subscribed be reached from every     *)
(* node once the cluster has processed all events?                        *)
(*                                                                         *)
(* One client, nodes 1..N. Each node runs an mqtt5 broker with node-local  *)
(* in-memory session storage, and fires connect / subscribe / disconnect   *)
(* events into the cluster handler (broker_events.rs). Events from one     *)
(* connection arrive in order. Events from different connections on the   *)
(* same node do not: a taken-over connection fires its disconnect from its *)
(* own task, before or after the new connection's connect. Connect events  *)
(* on one node ARE handled in connection order: mqtt5 fires connect right  *)
(* after CONNACK in the connection's task, so an older connection is       *)
(* already queued on the controller lock (a fair tokio RwLock) before the  *)
(* client can reconnect. This is an assumption grounded in that code, not  *)
(* something the model checks.                                            *)
(*                                                                         *)
(* Cluster state:                                                          *)
(*   rec[n]  node n's local copy of the session record (connected, node,   *)
(*           clean). Node 1 is the partition primary: a write from         *)
(*           another node also lands there. Session deletes are local      *)
(*           only, as in the code.                                         *)
(*   loc[n]  node n's ClientLocations entry. Inserts and deletes apply     *)
(*           locally, then travel to every other node; messages from one   *)
(*           sender arrive in order, messages from different senders do    *)
(*           not.                                                          *)
(*   tidx    TopicIndex has the client's subscription. Keyed by client,    *)
(*           not node, and applied atomically (not modelled as messages).  *)
(*                                                                         *)
(* Routing on node k (routing.rs resolve_connected_node): loc[k], else the *)
(* local record's node when it says connected.                            *)
(*                                                                         *)
(* Code:                                                                   *)
(*   "main"  mqtt5 0.45: the connect event's clean_start is "no session    *)
(*           was resumed", and the handler uses it both to clear on connect*)
(*           and as the record's clean flag. Connect returns early when a  *)
(*           local record exists. Disconnect clears subscriptions when the *)
(*           record is clean, and deletes the location unconditionally.    *)
(*   "bump"  mqtt5 0.47: clean_start is the flag the client sent. Handler  *)
(*           unchanged.                                                    *)
(*   "fix"   Connect updates an existing record instead of returning, and  *)
(*           always inserts the location. The record is clean when the     *)
(*           session expiry is 0. A disconnect from a connection that was  *)
(*           taken over on the same node is ignored. "Taken over"          *)
(*           uses a per-node count of handled connects minus disconnects,  *)
(*           like presence's LiveConnections: a disconnect that leaves the *)
(*           count above zero is ignored.                                  *)
(*                                                                         *)
(* Sequential = TRUE only lets the client reconnect once every node has   *)
(* noticed the old connection drop and the cluster has handled every      *)
(* event and location message: a reconnect after a disconnect, the common *)
(* case. Sequential = FALSE adds same-node takeover (the client is back    *)
(* before the node noticed it leave) and late detection on another node.  *)
(*                                                                         *)
(* ClientNodes is where the client may connect. A single node isolates     *)
(* reconnecting to the same node. Several nodes add moving between nodes, *)
(* where each node's mqtt5 broker keeps its own session and the cluster    *)
(* does not reconcile them.                                               *)
(*                                                                         *)
(* RESULTS (RoutableWhenQuiet, N = 3):                                     *)
(*   ClusterSessionRouting.cfg          fix,  Sequential, client on node 2 *)
(*                                      -> holds                          *)
(*   ClusterSessionRouting_primary.cfg  fix,  Sequential, on node 1        *)
(*                                      -> holds                          *)
(*   ClusterSessionRouting_bump.cfg     bump, Sequential, on node 2        *)
(*                                      -> violated: reconnect to a node   *)
(*                                         holding the record returns early*)
(*   ClusterSessionRouting_main.cfg     main, Sequential, on node 2        *)
(*                                      -> violated: a first persistent    *)
(*                                         connect is marked clean, its    *)
(*                                         disconnect clears subscriptions,*)
(*                                         the resumed session never       *)
(*                                         resubscribes                    *)
(*   ClusterSessionRouting_takeover.cfg fix,  not Sequential, on node 2    *)
(*                                      -> violated when the displaced     *)
(*                                         connection's disconnect is      *)
(*                                         handled before the new connect: *)
(*                                         the count is 0 then, and mqtt5  *)
(*                                         reports no takeover reason      *)
(*   ClusterSessionRouting_roaming.cfg  fix,  Sequential, nodes 1 and 2    *)
(*                                      -> violated: a clean session on one*)
(*                                         node clears TopicIndex entries  *)
(*                                         another node's stored session   *)
(*                                         still relies on                 *)
(* The last two fail on main too and are left for a follow-up.            *)
(*                                                                         *)
(* NOT MODELLED: replication lag between the primary and replicas (writes  *)
(* land on the primary atomically), TopicIndex broadcast ordering, the     *)
(* expiry sweep (it is dormant: nothing sets the record's expiry), mqtt5   *)
(* local session expiry, explicit unsubscribe, node crashes.               *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS N, MaxConns, Code, Sequential, ClientNodes

Nodes == 1..N
Primary == 1
NoRec == [conn |-> FALSE, node |-> 0, clean |-> FALSE, present |-> FALSE]

VARIABLES
    gen, cconn, live, stored, bsubs, info, subd,
    events, seq, rec, loc, locq, tidx, cnt

vars == <<gen, cconn, live, stored, bsubs, info, subd,
          events, seq, rec, loc, locq, tidx, cnt>>

Init ==
    /\ gen = 0
    /\ cconn = 0
    /\ live = [n \in Nodes |-> 0]
    /\ stored = [n \in Nodes |-> FALSE]
    /\ bsubs = [n \in Nodes |-> FALSE]
    /\ info = [g \in 1..MaxConns |-> [node |-> 0, cs |-> FALSE, pers |-> FALSE]]
    /\ subd = {}
    /\ events = {}
    /\ seq = 0
    /\ rec = [n \in Nodes |-> NoRec]
    /\ loc = [n \in Nodes |-> 0]
    /\ locq = {}
    /\ tidx = FALSE
    /\ cnt = [n \in Nodes |-> 0]

ClientNode == IF cconn = 0 THEN 0 ELSE info[cconn].node

Emit(e) ==
    /\ events' = events \cup {[e EXCEPT !.seq = seq]}
    /\ seq' = seq + 1

Connect(n, cs, pers) ==
    /\ cconn = 0
    /\ gen < MaxConns
    /\ Sequential => (events = {} /\ locq = {} /\ \A k \in Nodes : live[k] = 0)
    /\ LET g == gen + 1
           takeover == live[n] # 0
           present == ~cs /\ (stored[n] \/ takeover)
           connEv == [kind |-> "conn", node |-> n, gen |-> g, cs |-> cs,
                      pers |-> pers, present |-> present, seq |-> 0]
           discEv == [kind |-> "disc", node |-> n, gen |-> live[n], cs |-> FALSE,
                      pers |-> FALSE, present |-> FALSE, seq |-> 0]
       IN /\ gen' = g
          /\ cconn' = g
          /\ live' = [live EXCEPT ![n] = g]
          /\ stored' = [stored EXCEPT ![n] = FALSE]
          /\ bsubs' = [bsubs EXCEPT ![n] = present /\ bsubs[n]]
          /\ info' = [info EXCEPT ![g] = [node |-> n, cs |-> cs, pers |-> pers]]
          /\ IF takeover
               THEN /\ events' = events \cup {[discEv EXCEPT !.seq = seq],
                                               [connEv EXCEPT !.seq = seq + 1]}
                    /\ seq' = seq + 2
               ELSE Emit(connEv)
    /\ UNCHANGED <<subd, rec, loc, locq, tidx, cnt>>

Subscribe ==
    /\ cconn # 0
    /\ cconn \notin subd
    /\ ~bsubs[info[cconn].node]
    /\ subd' = subd \cup {cconn}
    /\ bsubs' = [bsubs EXCEPT ![info[cconn].node] = TRUE]
    /\ Emit([kind |-> "sub", node |-> info[cconn].node, gen |-> cconn, cs |-> FALSE,
             pers |-> FALSE, present |-> FALSE, seq |-> 0])
    /\ UNCHANGED <<gen, cconn, live, stored, info, rec, loc, locq, tidx, cnt>>

ClientDrop ==
    /\ cconn # 0
    /\ cconn' = 0
    /\ UNCHANGED <<gen, live, stored, bsubs, info, subd, events, seq, rec, loc, locq, tidx, cnt>>

Detect(n) ==
    /\ live[n] # 0
    /\ live[n] # cconn
    /\ LET g == live[n] IN
       /\ live' = [live EXCEPT ![n] = 0]
       /\ stored' = [stored EXCEPT ![n] = info[g].pers]
       /\ bsubs' = [bsubs EXCEPT ![n] = info[g].pers /\ bsubs[n]]
       /\ Emit([kind |-> "disc", node |-> n, gen |-> g, cs |-> FALSE,
                pers |-> FALSE, present |-> FALSE, seq |-> 0])
    /\ UNCHANGED <<gen, cconn, info, subd, rec, loc, locq, tidx, cnt>>

Ready(e) ==
    /\ ~\E o \in events : o.gen = e.gen /\ o.node = e.node /\ o.seq < e.seq
    /\ e.kind = "conn" => ~\E o \in events : o.kind = "conn" /\ o.node = e.node /\ o.gen < e.gen

SendLoc(n, op) ==
    locq' = locq \cup {[from |-> n, to |-> k, op |-> op, seq |-> seq] : k \in Nodes \ {n}}

WriteRec(n, r) ==
    rec' = IF n = Primary THEN [rec EXCEPT ![n] = r]
           ELSE [rec EXCEPT ![n] = r, ![Primary] = r]

ReportedCs(e) == IF Code = "main" THEN ~e.present ELSE e.cs

CleanFlag(e) == IF Code = "fix" THEN ~e.pers ELSE ReportedCs(e)

HandleConn(e) ==
    LET n == e.node
        cntAfter == [cnt EXCEPT ![n] = cnt[n] + 1]
        cleared == ReportedCs(e) /\ rec[n].present
        recAfterClear == IF cleared THEN NoRec ELSE rec[n]
        newRec == [conn |-> TRUE, node |-> n, clean |-> CleanFlag(e), present |-> TRUE]
    IN /\ cnt' = cntAfter
       /\ tidx' = IF cleared THEN FALSE ELSE tidx
       /\ IF Code # "fix" /\ recAfterClear.present
            THEN /\ rec' = [rec EXCEPT ![n] = recAfterClear]
                 /\ UNCHANGED <<loc, locq>>
            ELSE /\ WriteRec(n, newRec)
                 /\ loc' = [loc EXCEPT ![n] = n]
                 /\ SendLoc(n, "ins")

HandleSub(e) ==
    /\ tidx' = TRUE
    /\ UNCHANGED <<rec, loc, locq, cnt>>

HandleDisc(e) ==
    LET n == e.node
        displaced == Code = "fix" /\ cnt[n] > 1
    IN /\ cnt' = [cnt EXCEPT ![n] = IF cnt[n] > 0 THEN cnt[n] - 1 ELSE 0]
       /\ IF displaced \/ ~rec[n].present
            THEN UNCHANGED <<rec, loc, locq, tidx>>
            ELSE /\ IF rec[n].clean
                      THEN /\ tidx' = FALSE
                           /\ rec' = [rec EXCEPT ![n] = NoRec]
                      ELSE /\ WriteRec(n, [rec[n] EXCEPT !.conn = FALSE, !.node = n])
                           /\ UNCHANGED tidx
                 /\ loc' = [loc EXCEPT ![n] = 0]
                 /\ SendLoc(n, "del")

Handle(e) ==
    /\ e \in events
    /\ Ready(e)
    /\ events' = events \ {e}
    /\ seq' = seq + 1
    /\ CASE e.kind = "conn" -> HandleConn(e)
         [] e.kind = "sub"  -> HandleSub(e)
         [] e.kind = "disc" -> HandleDisc(e)
    /\ UNCHANGED <<gen, cconn, live, stored, bsubs, info, subd>>

LocReady(m) == ~\E o \in locq : o.from = m.from /\ o.to = m.to /\ o.seq < m.seq

DeliverLoc(m) ==
    /\ m \in locq
    /\ LocReady(m)
    /\ locq' = locq \ {m}
    /\ loc' = [loc EXCEPT ![m.to] =
                 IF m.op = "ins" THEN m.from ELSE 0]
    /\ UNCHANGED <<gen, cconn, live, stored, bsubs, info, subd, events, seq, rec, tidx, cnt>>

Next ==
    \/ \E n \in ClientNodes, cs \in BOOLEAN, pers \in BOOLEAN : Connect(n, cs, pers)
    \/ Subscribe
    \/ ClientDrop
    \/ \E n \in Nodes : Detect(n)
    \/ \E e \in events : Handle(e)
    \/ \E m \in locq : DeliverLoc(m)

Spec == Init /\ [][Next]_vars

Resolve(k) ==
    IF loc[k] # 0 THEN loc[k]
    ELSE IF rec[k].present /\ rec[k].conn THEN rec[k].node ELSE 0

Quiet ==
    /\ events = {}
    /\ locq = {}
    /\ \A n \in Nodes : live[n] = 0 \/ live[n] = cconn

RoutableWhenQuiet ==
    (Quiet /\ cconn # 0 /\ bsubs[ClientNode]) =>
        (tidx /\ \A k \in Nodes : Resolve(k) = ClientNode)

TypeOK ==
    /\ gen \in 0..MaxConns
    /\ cconn \in 0..MaxConns
    /\ tidx \in BOOLEAN
=============================================================================
