------------------------ MODULE ClusterRestartRejoin ------------------------
(***************************************************************************)
(* Cross-node model of gh #300: a node restarted with its data and no      *)
(* --peers elects itself and serves reads that miss acked writes.          *)
(*                                                                         *)
(* Code each action abstracts (crates/mqdb-cluster/src):                   *)
(*   Restart   raft/node.rs create_with_storage + raft/state.rs recover:   *)
(*             term and log survive, commit is the snapshot point, peers   *)
(*             are empty. cluster_agent/init.rs adds only --peers.         *)
(*   ElectAlone raft/node.rs can_start_election: no peers => allowed once  *)
(*             startup_grace_period_ms passed; quorum of one.              *)
(*   Elect     quorum_size = div_ceil(peers, 2) + 1; votes travel only     *)
(*             over links (quic_transport send => NodeNotFound otherwise). *)
(*   Dial      quic_transport redial + dial; driven by the event loop's    *)
(*             redial tick. Only a node with the address dials: --peers,   *)
(*             or with PersistBook a node that has been linked to it.      *)
(*   Failover  raft/coordinator/replication.rs                             *)
(*             reassign_partitions_for_dead_node.                          *)
(*   Join      trigger_rebalance_for_new_node, reduced to naming the node. *)
(*                                                                         *)
(* Abstractions:                                                           *)
(*   One partition. A log entry is a map version naming a primary and the  *)
(*   nodes the whole map names. An acked write is the (index, term) of     *)
(*   the entry its primary served under. A node serving as primary holds   *)
(*   a write iff that entry is in its applied log, so snapshot hand-off    *)
(*   between holders is assumed to work: #301/#273 are NOT modelled here.  *)
(*   Elections are atomic. Links are symmetric because one dial opens one  *)
(*   bidirectional stream. A link is usable or gone: stale peer-map        *)
(*   entries (ClusterRedial) are not modelled. A heartbeat is taken as     *)
(*   arriving with the link, so voters grow on Dial. A new leader counts   *)
(*   as having heard only after it commits (RaftNode::map_is_current).     *)
(*   Death verdicts are    *)
(*   one set, not per observer: a verdict on x stands until every other    *)
(*   node has a link to x, so each survivor still dials x.                 *)
(*   Time is coarse: Tick is about 5 s, Grace 2 (10 s), Detect 3 (15 s).   *)
(*   RedialPeriod 3 stands in for the 60 s tick: only "longer than Grace"  *)
(*   matters, and 12 adds states but no behaviour. A due dial is urgent:   *)
(*   Tick waits for it, unless Isolate (a network fault) blocks the node.  *)
(*                                                                         *)
(* Not modelled: forgetting a learned address, the unseen-member timeout,  *)
(* and a whole-log replay of more than one map (NamedSince is the union).   *)
(* Each failing cfg names one invariant or property because the checker     *)
(* stops at the first violation. Verified for the cfg's constants only, and *)
(* see the PR for the run results. Expected:                                *)
(*   _today_authority, _today_complete, _today_acked: the #300 failure      *)
(*   _sequential_today, _sequential_lasttail: fails InvNoLoneLeader; voters *)
(*       taken from the last entry let an uncommitted failover tail name    *)
(*       only the restarting node                                           *)
(*   _sequential_tail, _sequential, _sequential_converge: the fix passes    *)
(*   _fastredial_fail, _book_fail: redial or book alone need the guard      *)
(*   _guard_noredial: fails Converge, the guard needs some redial           *)
(*   _guard_slow, _nobook, _killany: pass                                   *)
(*   _nogate: fails InvNoIsolatedDisruption, a node with no links raises    *)
(*       its term                                                           *)
(*   _followerlink: fails InvNoRejoinDisruption: known limit, needs pre-vote *)
(*       (gh #314)                                                          *)
(*   _nohold: fails InvAckedVisible, a restored map serves stale            *)
(*   _single_neveralone: fails Converge, a blanket guard breaks one node    *)
(*   _single, _firstboot, _firstboot_single: pass                           *)
(*   _stale_voters, _stale_voters_authority: known limit, two nodes down    *)
(*       while others joined reach a majority of their old set and can      *)
(*       overwrite committed entries; needs committed membership (gh #157)  *)
(*   _stale_voters_today: the same failure without the guard                *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    n1, n2, n3, n4, n5, Nodes,
    Formed,
    RestoreVoters, VotersFromWholeTail, LinkGate, NeverAlone, LaggingStart,
    RedialPeriod, RedialUnlinked, Redial, PersistBook,
    RestoreSnapshot, HoldMap,
    Grace, Detect,
    KillSet, KillOrder, Writes, Isolations, MaxKills, MaxTerm, MaxLog

VARIABLES
    up, term, log, commit, role, voters, links, heard,
    graceLeft, dialWait, detectLeft, detected, unseen, isolatedLeft,
    killsLeft, isolationsLeft, acked, committed, deposed, bumped, learned, failedElection

vars == <<up, term, log, commit, role, voters, links, heard,
          graceLeft, dialWait, detectLeft, detected, unseen, isolatedLeft,
          killsLeft, isolationsLeft, acked, committed, deposed, bumped, learned, failedElection>>

Min(a, b) == IF a < b THEN a ELSE b
Max(a, b) == IF a > b THEN a ELSE b

Rank(n) == IF n = n1 THEN 1 ELSE IF n = n2 THEN 2 ELSE IF n = n3 THEN 3 ELSE IF n = n4 THEN 4 ELSE 5
Configured == [n \in Nodes |-> {m \in Nodes : Rank(m) < Rank(n)}]

Book(a) == IF PersistBook THEN Configured[a] \cup learned[a] ELSE Configured[a]
Linked(a, b) == {a, b} \in links
LinkedVoters(n) == {v \in voters[n] : Linked(n, v)}
Quorum(n) == (Cardinality(voters[n]) + 1) \div 2 + 1
Reachable(n) == {v \in LinkedVoters(n) : term[v] <= term[n]}
CanLink(n) == Cardinality(LinkedVoters(n)) + 1 >= Quorum(n)
CanCommit(n) == Cardinality(Reachable(n)) + 1 >= Quorum(n)
Named(s) == IF s = <<>> THEN {} ELSE s[Len(s)].named
PrimaryOf(s) == s[Len(s)].primary
Applied(n) == SubSeq(log[n], 1, commit[n])
SnapshotPoint(n) == IF RestoreSnapshot THEN commit[n] ELSE 0
NamedSince(n) ==
    UNION {log[n][i].named : i \in Max(SnapshotPoint(n), 1)..Len(log[n])}
RestoredVoters(n) ==
    IF ~RestoreVoters THEN {}
    ELSE IF VotersFromWholeTail THEN NamedSince(n) \ {n}
    ELSE Named(log[n]) \ {n}
Leaders == {n \in up : role[n] = "leader"}

UpToDate(v, n) ==
    LET lastTerm(s) == IF s = <<>> THEN 0 ELSE s[Len(s)].term
    IN \/ lastTerm(log[n]) > lastTerm(log[v])
       \/ /\ lastTerm(log[n]) = lastTerm(log[v])
          /\ Len(log[n]) >= Len(log[v])

Serving(n) ==
    /\ n \in up
    /\ commit[n] > 0
    /\ PrimaryOf(Applied(n)) = n
    /\ (HoldMap => heard[n])

LeaderLinked(n) == \E l \in Leaders : l # n /\ Linked(n, l) /\ term[l] >= term[n]

Target(a, b) ==
    /\ a # b
    /\ b \in Book(a)
    /\ ~Linked(a, b)
    /\ \/ b \in unseen[a]
       \/ /\ Redial
          /\ (b \in detected \/ RedialUnlinked)

EarlyNames == {n1, n2, n3}
EarlyEntry == [term |-> 1, primary |-> n3, named |-> EarlyNames]
LateEntry == [term |-> 1, primary |-> n3, named |-> Nodes]
Lagged == {n1, n2}
FormedEntry == [term |-> 1, primary |-> n1, named |-> Nodes]

InitialLog(n) ==
    CASE LaggingStart /\ n \in Lagged -> <<EarlyEntry>>
      [] LaggingStart -> <<EarlyEntry, LateEntry>>
      [] Formed -> <<FormedEntry>>
      [] OTHER -> <<>>

Init ==
    /\ up = CASE LaggingStart -> Nodes \ Lagged
              [] Formed -> Nodes
              [] OTHER -> {}
    /\ term = [n \in Nodes |-> IF Formed \/ LaggingStart THEN 1 ELSE 0]
    /\ log = [n \in Nodes |-> InitialLog(n)]
    /\ commit = [n \in Nodes |-> Len(InitialLog(n))]
    /\ role = [n \in Nodes |->
                 IF (LaggingStart /\ n = n3) \/ (~LaggingStart /\ Formed /\ n = n1)
                 THEN "leader" ELSE "follower"]
    /\ voters = [n \in Nodes |->
                  IF LaggingStart /\ n \in Lagged THEN EarlyNames \ {n}
                  ELSE IF Formed \/ LaggingStart THEN Nodes \ {n}
                  ELSE {}]
    /\ links = IF LaggingStart
               THEN {{a, b} : a \in Nodes \ Lagged, b \in Nodes \ Lagged} \ {{a} : a \in Nodes}
               ELSE IF Formed THEN {{a, b} : a \in Nodes, b \in Nodes} \ {{a} : a \in Nodes}
               ELSE {}
    /\ heard = [n \in Nodes |-> Formed \/ LaggingStart]
    /\ graceLeft = [n \in Nodes |-> 0]
    /\ dialWait = [n \in Nodes |-> RedialPeriod]
    /\ detectLeft = [n \in Nodes |-> 0]
    /\ detected = {}
    /\ unseen = [n \in Nodes |-> {}]
    /\ isolatedLeft = [n \in Nodes |-> 0]
    /\ killsLeft = MaxKills
    /\ isolationsLeft = Isolations
    /\ acked = {}
    /\ committed = IF LaggingStart
                   THEN {[idx |-> 1, term |-> 1, at |-> 1], [idx |-> 2, term |-> 1, at |-> 1]}
                   ELSE IF Formed THEN {[idx |-> 1, term |-> 1, at |-> 1]}
                   ELSE {}
    /\ deposed = FALSE
    /\ bumped = {}
    /\ learned = [n \in Nodes |->
                   IF LaggingStart /\ n \in Lagged THEN EarlyNames \ {n}
                   ELSE IF Formed \/ LaggingStart THEN Nodes \ {n}
                   ELSE {}]
    /\ failedElection = FALSE

Kill(n) ==
    /\ n \in KillSet
    /\ n \in up
    /\ killsLeft > 0
    /\ KillOrder # <<>> => n = KillOrder[MaxKills - killsLeft + 1]
    /\ killsLeft > 0
    /\ killsLeft' = killsLeft - 1
    /\ up' = up \ {n}
    /\ role' = [role EXCEPT ![n] = "follower"]
    /\ links' = {l \in links : n \notin l}
    /\ detectLeft' = [detectLeft EXCEPT ![n] = Detect]
    /\ UNCHANGED <<term, log, commit, voters, heard, graceLeft, dialWait,
                   detected, unseen, isolatedLeft, isolationsLeft, acked, committed, deposed, bumped, learned, failedElection>>

Restart(n) ==
    /\ n \notin up
    /\ up' = up \cup {n}
    /\ role' = [role EXCEPT ![n] = "follower"]
    /\ commit' = [commit EXCEPT ![n] = IF RestoreSnapshot THEN @ ELSE 0]
    /\ voters' = [voters EXCEPT ![n] =
                    Configured[n] \cup RestoredVoters(n)]
    /\ heard' = [heard EXCEPT ![n] = FALSE]
    /\ graceLeft' = [graceLeft EXCEPT ![n] = Grace]
    /\ dialWait' = [dialWait EXCEPT ![n] = 0]
    /\ unseen' = [unseen EXCEPT ![n] = Nodes \ {n}]
    /\ UNCHANGED <<term, log, links, detectLeft, detected, isolatedLeft, killsLeft,
                   isolationsLeft, acked, committed, deposed, bumped, learned, failedElection>>

Dial(a, b) ==
    /\ {a, b} \subseteq up
    /\ dialWait[a] = 0
    /\ Target(a, b)
    /\ isolatedLeft[a] = 0
    /\ isolatedLeft[b] = 0
    /\ links' = links \cup {{a, b}}
    /\ voters' = [voters EXCEPT ![a] = @ \cup {b}, ![b] = @ \cup {a}]
    /\ learned' = [learned EXCEPT ![a] = @ \cup {b}, ![b] = @ \cup {a}]
    /\ unseen' = [unseen EXCEPT ![a] = @ \ {b}, ![b] = @ \ {a}]
    /\ LET fullyLinked == {x \in {a, b} : \A y \in Nodes \ {x} : {x, y} \in links'}
       IN /\ detected' = detected \ fullyLinked
          /\ detectLeft' = [x \in Nodes |-> IF x \in fullyLinked THEN 0 ELSE detectLeft[x]]
    /\ UNCHANGED <<up, term, log, commit, role, heard, graceLeft, dialWait,
                   isolatedLeft, killsLeft, isolationsLeft, acked, committed, deposed, bumped, failedElection>>

\* A network fault: no dial reaches n until the grace period has run out.
Isolate(n) ==
    /\ n \in up
    /\ isolationsLeft > 0
    /\ {l \in links : n \in l} = {}
    /\ isolationsLeft' = isolationsLeft - 1
    /\ isolatedLeft' = [isolatedLeft EXCEPT ![n] = Grace + 1]
    /\ UNCHANGED <<up, term, log, commit, role, voters, links, heard, graceLeft,
                   dialWait, detectLeft, detected, unseen, killsLeft, acked,
                   committed, deposed, bumped, learned, failedElection>>

DialDue ==
    \E a, b \in up :
        dialWait[a] = 0 /\ Target(a, b) /\ isolatedLeft[a] = 0 /\ isolatedLeft[b] = 0

Tick ==
    /\ ~DialDue
    /\ graceLeft' = [n \in Nodes |-> IF graceLeft[n] > 0 THEN graceLeft[n] - 1 ELSE 0]
    /\ detectLeft' = [n \in Nodes |-> IF detectLeft[n] > 0 THEN detectLeft[n] - 1 ELSE 0]
    /\ isolatedLeft' = [n \in Nodes |-> IF isolatedLeft[n] > 0 THEN isolatedLeft[n] - 1 ELSE 0]
    /\ LET fired == {n \in Nodes : detectLeft[n] = 1}
       IN /\ detected' = detected \cup fired
          /\ dialWait' = [n \in Nodes |->
                            IF fired # {} \/ dialWait[n] = 1 THEN 0
                            ELSE IF dialWait[n] = 0 THEN RedialPeriod
                            ELSE dialWait[n] - 1]
    /\ UNCHANGED <<up, term, log, commit, role, voters, links, heard, unseen,
                   killsLeft, isolationsLeft, acked, committed, deposed, bumped, learned, failedElection>>

BecomeLeader(n, voteSet) ==
    LET newTerm == term[n] + 1
        priorPrimary == IF log[n] = <<>> THEN n ELSE PrimaryOf(log[n])
        named == IF log[n] = <<>> THEN {n} \cup LinkedVoters(n) ELSE Named(log[n])
    IN /\ term' = [v \in Nodes |-> IF v \in voteSet THEN newTerm ELSE term[v]]
       /\ role' = [role EXCEPT ![n] = "leader"]
       /\ log' = [log EXCEPT ![n] = Append(@, [term |-> newTerm, primary |-> priorPrimary, named |-> named])]

ElectAlone(n) ==
    /\ n \in up
    /\ role[n] = "follower"
    /\ voters[n] = {}
    /\ graceLeft[n] = 0
    /\ term[n] < MaxTerm
    /\ Len(log[n]) < MaxLog
    /\ NeverAlone => log[n] = <<>>
    /\ BecomeLeader(n, {n})
    /\ heard' = [heard EXCEPT ![n] = TRUE]
    /\ commit' = [commit EXCEPT ![n] = Len(log'[n])]
    /\ committed' = committed \cup {[idx |-> i, term |-> log'[n][i].term, at |-> term'[n]] : i \in 1..Len(log'[n])}
    /\ UNCHANGED <<up, voters, links, graceLeft, dialWait, detectLeft, detected,
                   unseen, isolatedLeft, killsLeft, isolationsLeft, acked, deposed, bumped, learned, failedElection>>

Elect(n) ==
    /\ n \in up
    /\ role[n] = "follower"
    /\ voters[n] # {}
    /\ ~LeaderLinked(n)
    /\ term[n] < MaxTerm
    /\ Len(log[n]) < MaxLog
    /\ \E voteSet \in SUBSET LinkedVoters(n) :
        /\ Cardinality(voteSet) + 1 >= Quorum(n)
        /\ \A v \in voteSet : term[v] <= term[n] /\ role[v] = "follower" /\ UpToDate(v, n)
        /\ BecomeLeader(n, voteSet \cup {n})
    /\ UNCHANGED <<heard, up, commit, voters, links, graceLeft, dialWait, detectLeft,
                   detected, unseen, isolatedLeft, killsLeft, isolationsLeft, acked,
                   committed, deposed, bumped, learned, failedElection>>

Bump(n) ==
    /\ ~LinkGate
    /\ n \in up
    /\ role[n] = "follower"
    /\ voters[n] # {}
    /\ ~CanCommit(n)
    /\ ~LeaderLinked(n)
    /\ term[n] < MaxTerm
    /\ term' = [term EXCEPT ![n] = @ + 1]
    /\ bumped' = bumped \cup {n}
    /\ UNCHANGED <<up, log, commit, role, voters, links, heard, graceLeft, dialWait,
                   detectLeft, detected, unseen, isolatedLeft, killsLeft, isolationsLeft,
                   acked, committed, deposed, learned, failedElection>>

\* A node that links to a follower, reaches a quorum of links, and loses: the
\* follower's vote is refused or the node never hears the leader first. Its raised
\* term still reaches the leader through that follower. raft/node.rs start_election
\* has no pre-vote (gh #314), so the link gate cannot stop this. Bounded to one per
\* behaviour so that MaxTerm limits the state space rather than leadership.
FailedElect(n) ==
    /\ ~failedElection
    /\ n \in up
    /\ ~heard[n]
    /\ Leaders # {}
    /\ role[n] = "follower"
    /\ voters[n] # {}
    /\ CanLink(n)
    /\ ~LeaderLinked(n)
    /\ term[n] < MaxTerm
    /\ term' = [term EXCEPT ![n] = @ + 1]
    /\ failedElection' = TRUE
    /\ UNCHANGED <<up, log, commit, role, voters, links, heard, graceLeft, dialWait,
                   detectLeft, detected, unseen, isolatedLeft, killsLeft, isolationsLeft,
                   acked, committed, deposed, bumped, learned>>

StepDown(a, b) ==
    /\ a \in Leaders
    /\ b \in up
    /\ Linked(a, b)
    /\ term[b] > term[a]
    /\ role' = [role EXCEPT ![a] = "follower"]
    /\ term' = [term EXCEPT ![a] = term[b]]
    /\ deposed' = (deposed \/ b \in bumped)
    /\ UNCHANGED <<up, log, commit, voters, links, heard, graceLeft, dialWait,
                   detectLeft, detected, unseen, isolatedLeft, killsLeft, isolationsLeft,
                   acked, committed, bumped, learned, failedElection>>

Replicate(l, f) ==
    /\ l \in Leaders
    /\ f \in up
    /\ f # l
    /\ Linked(l, f)
    /\ term[l] >= term[f]
    /\ (log[f] # log[l] \/ commit[f] # Min(Max(commit[f], commit[l]), Len(log[l])) \/ (~heard[f] /\ commit[l] = Len(log[l])) \/ term[f] # term[l] \/ role[f] # "follower")
    /\ log' = [log EXCEPT ![f] = log[l]]
    /\ term' = [term EXCEPT ![f] = term[l]]
    /\ role' = [role EXCEPT ![f] = "follower"]
    /\ commit' = [commit EXCEPT ![f] = Min(Max(commit[f], commit[l]), Len(log[l]))]
    /\ heard' = [heard EXCEPT ![f] = @ \/ commit[l] = Len(log[l])]
    /\ bumped' = bumped \ {f}
    /\ UNCHANGED <<up, voters, links, graceLeft, dialWait, detectLeft, detected,
                   unseen, isolatedLeft, killsLeft, isolationsLeft, acked, committed, deposed, learned, failedElection>>

Commit(l) ==
    /\ l \in Leaders
    /\ commit[l] < Len(log[l])
    /\ Cardinality({v \in Reachable(l) : log[v] = log[l]}) + 1 >= Quorum(l)
    /\ commit' = [commit EXCEPT ![l] = Len(log[l])]
    /\ heard' = [heard EXCEPT ![l] = TRUE]
    /\ committed' = committed \cup {[idx |-> i, term |-> log[l][i].term, at |-> term[l]] : i \in 1..Len(log[l])}
    /\ UNCHANGED <<up, term, log, role, voters, links, graceLeft, dialWait,
                   detectLeft, detected, unseen, isolatedLeft, killsLeft, isolationsLeft,
                   acked, deposed, bumped, learned, failedElection>>

Propose(l, primary, named) ==
    /\ log' = [log EXCEPT ![l] = Append(@, [term |-> term[l], primary |-> primary, named |-> named])]
    /\ UNCHANGED <<up, term, commit, role, voters, links, heard, graceLeft, dialWait,
                   detectLeft, detected, unseen, isolatedLeft, killsLeft, isolationsLeft,
                   acked, committed, deposed, bumped, learned, failedElection>>

Failover(l, d) ==
    /\ l \in Leaders
    /\ Len(log[l]) < MaxLog
    /\ d \in detected
    /\ ~Linked(l, d)
    /\ d \in Named(log[l])
    /\ \E q \in LinkedVoters(l) \cup {l} :
        /\ q \notin detected
        /\ IF PrimaryOf(log[l]) = d
           THEN Propose(l, q, Named(log[l]) \ {d})
           ELSE Propose(l, PrimaryOf(log[l]), Named(log[l]) \ {d})

Join(l, n) ==
    /\ l \in Leaders
    /\ commit[l] = Len(log[l])
    /\ Len(log[l]) < MaxLog
    /\ n \in up
    /\ Linked(l, n)
    /\ n \notin Named(log[l])
    /\ Propose(l, PrimaryOf(log[l]), Named(log[l]) \cup {n})

Write(p) ==
    /\ Writes
    /\ Serving(p)
    /\ [idx |-> commit[p], term |-> log[p][commit[p]].term] \notin acked
    /\ acked' = acked \cup {[idx |-> commit[p], term |-> log[p][commit[p]].term]}
    /\ UNCHANGED <<up, term, log, commit, role, voters, links, heard, graceLeft,
                   dialWait, detectLeft, detected, unseen, isolatedLeft, killsLeft,
                   isolationsLeft, committed, deposed, bumped, learned, failedElection>>

Next ==
    \/ \E n \in Nodes :
        \/ Kill(n) \/ Restart(n) \/ Isolate(n) \/ ElectAlone(n) \/ Elect(n) \/ Bump(n) \/ FailedElect(n) \/ Commit(n) \/ Write(n)
    \/ \E a, b \in Nodes :
        \/ Dial(a, b) \/ StepDown(a, b) \/ Replicate(a, b)
        \/ Failover(a, b) \/ Join(a, b)
    \/ Tick

Fairness ==
    /\ WF_vars(Tick)
    /\ \A n \in Nodes :
        /\ WF_vars(Restart(n)) /\ WF_vars(ElectAlone(n)) /\ WF_vars(Elect(n)) /\ WF_vars(Commit(n))
    /\ \A a, b \in Nodes :
        /\ WF_vars(Dial(a, b)) /\ WF_vars(StepDown(a, b)) /\ WF_vars(Replicate(a, b))
        /\ WF_vars(Failover(a, b)) /\ WF_vars(Join(a, b))

Spec == Init /\ [][Next]_vars /\ Fairness

------------------------------------------------------------------------------
TypeOK ==
    /\ up \subseteq Nodes
    /\ role \in [Nodes -> {"leader", "follower"}]
    /\ term \in [Nodes -> 0..MaxTerm]
    /\ graceLeft \in [Nodes -> 0..Grace]
    /\ dialWait \in [Nodes -> 0..RedialPeriod]
    /\ detectLeft \in [Nodes -> 0..Detect]

\* No two live leaders that can both commit: this is the "second leader".
InvSingleAuthority ==
    \A a, b \in Leaders : (a # b /\ CanCommit(a) /\ CanCommit(b)) => FALSE

\* Raft leader completeness: an entry committed in a term is in the log of every
\* leader of that term or later. "at" is the committing leader's term, since a
\* deposed leader of an earlier term may legitimately still be running.
InvLeaderComplete ==
    \A l \in Leaders, c \in committed :
        c.at <= term[l] =>
            /\ c.idx <= Len(log[l])
            /\ log[l][c.idx].term = c.term

\* Every acked write is held by any node that serves as primary. A miss is a 404.
\* A node linked to a live leader is exempt: it applies the next AppendEntries
\* (at most one 500 ms heartbeat away), which is the ordinary lag of an async
\* partition map on every follower, not the unbounded window of #300.
InvAckedVisible ==
    \A n \in Nodes, w \in acked :
        (Serving(n) /\ ~LeaderLinked(n)) =>
            \E i \in 1..commit[n] : i = w.idx /\ log[n][i].term = w.term

\* A node with links to no quorum raises no term, so it cannot depose a leader.
InvNoIsolatedDisruption == ~deposed

\* A returning node does not raise the term at all while a leader is working. This
\* does NOT hold for the fix: see FailedElect and _followerlink. It needs pre-vote.
InvNoRejoinDisruption == ~deposed /\ ~failedElection

\* A node whose log holds a committed entry naming another node never leads alone.
InvNoLoneLeader ==
    \A l \in Leaders :
        voters[l] = {} =>
            \A c \in committed :
                (c.idx <= Len(log[l]) /\ log[l][c.idx].term = c.term) =>
                    log[l][c.idx].named \subseteq {l}

Settled ==
    /\ up = Nodes
    /\ Cardinality(Leaders) = 1
    /\ \A a, b \in Nodes : a # b => Linked(a, b)
    /\ \A a, b \in Nodes : log[a] = log[b] /\ commit[a] = Len(log[a]) /\ term[a] = term[b]

\* Every node ends up linked to every other under one leader with the same map.
Converge == <>[]Settled
=============================================================================
