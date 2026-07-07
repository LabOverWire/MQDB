---------------------- MODULE ClusterUniqueQuorum ----------------------
\* Option C: epoch-fenced, quorum-durable primary-backup for the unique claim.
\* No per-create Raft: the metadata layer (abstracted by NewEpoch) only hands a single
\* primary a fresh monotonic epoch. Fencing + durability come from the replication path:
\*   - a primary must SEAL at promotion (read a majority + promise its epoch) before serving;
\*   - reserve/commit are quorum-durable (a majority accept, gated by the epoch promise).
\* Single contended value (v1) modelled implicitly. Records = concurrent claimers.
\*
\* Sealing = TRUE  -> the full mechanism: expect NoOversell to hold.
\* Sealing = FALSE -> primaries serve without the read-quorum seal: expect oversell
\*                    (a promoted primary never learns the existing reservation).

EXTENDS Naturals, FiniteSets

CONSTANTS Records, Nodes, MaxEpoch, Sealing, Fence, RequireMajority

NULL == CHOOSE x : x \notin (Records \cup Nodes)

Majorities == {Q \in SUBSET Nodes : 2 * Cardinality(Q) > Cardinality(Nodes)}

\* The eligible quorum sets. RequireMajority=FALSE lets any non-empty set act as a "quorum"
\* (non-intersecting quorums) -- used to show majority intersection is load-bearing.
QuorumSets == IF RequireMajority THEN Majorities ELSE {Q \in SUBSET Nodes : Q # {}}

VARIABLES
    promised,     \* [Nodes -> 0..MaxEpoch] : highest epoch a replica has promised
    accepted,     \* [Nodes -> NULL | [epoch, owner, committed]] : reservation the replica holds
    actingEpoch,  \* [Nodes -> 0..MaxEpoch] : epoch at which a node believes itself primary (0 = none)
    sealedAt,     \* [Nodes -> 0..MaxEpoch] : epoch a node has sealed (read-quorum + promise)
    leaderView,   \* [Nodes -> Records | NULL] : owner the sealed primary believes holds v1
    curEpoch,     \* 0..MaxEpoch : highest epoch handed out by the metadata layer
    recordHeld,   \* [Records -> BOOLEAN] : durable record written for v1
    phase         \* [Records -> {"idle","routed","written","done","failed"}]

vars == << promised, accepted, actingEpoch, sealedAt, leaderView, curEpoch, recordHeld, phase >>

Phases == {"idle", "routed", "written", "done", "failed"}
ResvType == [epoch : 1..MaxEpoch, owner : Records, committed : BOOLEAN]

TypeOK ==
    /\ promised     \in [Nodes -> 0..MaxEpoch]
    /\ accepted     \in [Nodes -> (ResvType \cup {NULL})]
    /\ actingEpoch  \in [Nodes -> 0..MaxEpoch]
    /\ sealedAt     \in [Nodes -> 0..MaxEpoch]
    /\ leaderView   \in [Nodes -> Records \cup {NULL}]
    /\ curEpoch     \in 0..MaxEpoch
    /\ recordHeld   \in [Records -> BOOLEAN]
    /\ phase        \in [Records -> Phases]

Init ==
    /\ promised     = [n \in Nodes |-> 0]
    /\ accepted     = [n \in Nodes |-> NULL]
    /\ actingEpoch  = [n \in Nodes |-> 0]
    /\ sealedAt     = [n \in Nodes |-> 0]
    /\ leaderView   = [n \in Nodes |-> NULL]
    /\ curEpoch     = 0
    /\ recordHeld   = [r \in Records |-> FALSE]
    /\ phase        = [r \in Records |-> "idle"]

\* The epoch-fence guard: an accept/seal at epoch e is allowed only if no node in Q has
\* promised a higher epoch. Fence=FALSE drops it -- used to show fencing is load-bearing.
FenceOk(Q, e) == Fence => (\A q \in Q : promised[q] <= e)

\* Highest-epoch reservation a read-quorum Q discovers (the Paxos "learn" step).
Owner(q) == IF accepted[q] = NULL THEN NULL ELSE accepted[q].owner
Ep(q)    == IF accepted[q] = NULL THEN 0    ELSE accepted[q].epoch

LearnView(Q) ==
    LET S == {q \in Q : accepted[q] # NULL}
    IN IF S = {} THEN NULL
       ELSE Owner(CHOOSE q \in S : \A o \in S : Ep(q) >= Ep(o))

\* Metadata layer elects n as primary at a fresh, strictly-higher epoch.
NewEpoch(n) ==
    /\ curEpoch < MaxEpoch
    /\ curEpoch' = curEpoch + 1
    /\ actingEpoch' = [actingEpoch EXCEPT ![n] = curEpoch + 1]
    /\ UNCHANGED << promised, accepted, sealedAt, leaderView, recordHeld, phase >>

\* Promotion seal: read a majority + promise this epoch, before serving.
Seal(n, Q) ==
    /\ Sealing
    /\ actingEpoch[n] > 0
    /\ sealedAt[n] < actingEpoch[n]
    /\ Q \in QuorumSets
    /\ FenceOk(Q, actingEpoch[n])
    /\ promised'   = [m \in Nodes |-> IF m \in Q THEN actingEpoch[n] ELSE promised[m]]
    /\ leaderView' = [leaderView EXCEPT ![n] = LearnView(Q)]
    /\ sealedAt'   = [sealedAt EXCEPT ![n] = actingEpoch[n]]
    /\ UNCHANGED << accepted, actingEpoch, curEpoch, recordHeld, phase >>

\* Reserve v1 for record r: quorum accept at the primary's epoch.
Reserve(n, r, Q) ==
    /\ phase[r] = "idle"
    /\ actingEpoch[n] > 0
    /\ (Sealing => sealedAt[n] = actingEpoch[n])
    /\ leaderView[n] = NULL
    /\ Q \in QuorumSets
    /\ FenceOk(Q, actingEpoch[n])
    /\ accepted'   = [m \in Nodes |-> IF m \in Q
                          THEN [epoch |-> actingEpoch[n], owner |-> r, committed |-> FALSE]
                          ELSE accepted[m]]
    /\ promised'   = [m \in Nodes |-> IF m \in Q THEN actingEpoch[n] ELSE promised[m]]
    /\ leaderView' = [leaderView EXCEPT ![n] = r]
    /\ phase'      = [phase EXCEPT ![r] = "routed"]
    /\ UNCHANGED << actingEpoch, sealedAt, curEpoch, recordHeld >>

\* The durable record is written only after a successful majority reserve.
WriteRecord(r) ==
    /\ phase[r] = "routed"
    /\ recordHeld' = [recordHeld EXCEPT ![r] = TRUE]
    /\ phase'      = [phase EXCEPT ![r] = "written"]
    /\ UNCHANGED << promised, accepted, actingEpoch, sealedAt, leaderView, curEpoch >>

\* Commit promotes the reservation to permanent via a quorum accept.
Commit(n, r, Q) ==
    /\ phase[r] = "written"
    /\ actingEpoch[n] > 0
    /\ (Sealing => sealedAt[n] = actingEpoch[n])
    /\ leaderView[n] = r
    /\ Q \in QuorumSets
    /\ FenceOk(Q, actingEpoch[n])
    /\ accepted' = [m \in Nodes |-> IF m \in Q
                        THEN [epoch |-> actingEpoch[n], owner |-> r, committed |-> TRUE]
                        ELSE accepted[m]]
    /\ promised' = [m \in Nodes |-> IF m \in Q THEN actingEpoch[n] ELSE promised[m]]
    /\ phase'    = [phase EXCEPT ![r] = "done"]
    /\ UNCHANGED << actingEpoch, sealedAt, leaderView, curEpoch, recordHeld >>

Next ==
    \/ \E n \in Nodes : NewEpoch(n)
    \/ \E n \in Nodes, Q \in QuorumSets : Seal(n, Q)
    \/ \E n \in Nodes, r \in Records, Q \in QuorumSets : Reserve(n, r, Q)
    \/ \E r \in Records : WriteRecord(r)
    \/ \E n \in Nodes, r \in Records, Q \in QuorumSets : Commit(n, r, Q)

Spec == Init /\ [][Next]_vars

NoOversell == Cardinality({r \in Records : recordHeld[r]}) <= 1

========================================================================
