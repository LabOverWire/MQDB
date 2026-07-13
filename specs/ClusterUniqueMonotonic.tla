---------------------- MODULE ClusterUniqueMonotonic ----------------------
\* Faithful model of the IMPLEMENTED Phase 2 (no leaderView seal-transfer):
\*   - Seal promises the epoch at a majority but does NOT learn/merge reservations.
\*   - The primary serves a reserve using only its LOCAL view (its own accepted slot):
\*       PrimaryCasOk(n,r): the primary's slot is empty or already owned by r.
\*     A new primary that MISSED an earlier reserve has accepted[n]=NULL -> CAS passes.
\*   - Members apply under the epoch fence PLUS the monotonic-committed guard
\*       MonotonicOk(Q,r): no member in Q holds a COMMITTED claim for a different owner.
\* Question: is (seal-promise + primary-local-CAS + epoch-fence + monotonic-committed guard),
\* WITHOUT leaderView reservation-learning, enough for NoOversell?
\*
\* RESULT: NO. NoOversell is VIOLATED even with `n \in Q` (primary applies locally). Counterexample:
\*   n1@e1 reserves r1 at {n1,n3}; n2 promoted to e2, seals at {n1,n2} but MISSED r1 (not in its
\*   quorum) and the seal does not transfer reservations; n2 reserves r2 at {n1,n2} -- its local CAS
\*   sees NULL and n1 holds r1 only UNCOMMITTED so the monotonic guard permits the overwrite; both
\*   records are written. This proves the seal MUST learn existing reservations (leaderView / the
\*   d-2 seal-reservation-transfer). The monotonic-committed guard alone is NOT a sufficient
\*   substitute; ClusterUniqueQuorum.tla (with leaderView) is the model that holds NoOversell.

EXTENDS Naturals, FiniteSets

CONSTANTS Records, Nodes, MaxEpoch

NULL == CHOOSE x : x \notin (Records \cup Nodes)

Majorities == {Q \in SUBSET Nodes : 2 * Cardinality(Q) > Cardinality(Nodes)}

VARIABLES
    promised,     \* [Nodes -> 0..MaxEpoch]
    accepted,     \* [Nodes -> NULL | [epoch, owner, committed]]
    actingEpoch,  \* [Nodes -> 0..MaxEpoch]
    sealedAt,     \* [Nodes -> 0..MaxEpoch]
    curEpoch,
    recordHeld,
    phase

vars == << promised, accepted, actingEpoch, sealedAt, curEpoch, recordHeld, phase >>

Phases == {"idle", "routed", "written", "done", "failed"}
ResvType == [epoch : 1..MaxEpoch, owner : Records, committed : BOOLEAN]

TypeOK ==
    /\ promised     \in [Nodes -> 0..MaxEpoch]
    /\ accepted     \in [Nodes -> (ResvType \cup {NULL})]
    /\ actingEpoch  \in [Nodes -> 0..MaxEpoch]
    /\ sealedAt     \in [Nodes -> 0..MaxEpoch]
    /\ curEpoch     \in 0..MaxEpoch
    /\ recordHeld   \in [Records -> BOOLEAN]
    /\ phase        \in [Records -> Phases]

Init ==
    /\ promised     = [n \in Nodes |-> 0]
    /\ accepted     = [n \in Nodes |-> NULL]
    /\ actingEpoch  = [n \in Nodes |-> 0]
    /\ sealedAt     = [n \in Nodes |-> 0]
    /\ curEpoch     = 0
    /\ recordHeld   = [r \in Records |-> FALSE]
    /\ phase        = [r \in Records |-> "idle"]

FenceOk(Q, e) == \A q \in Q : promised[q] <= e

\* Primary's local CAS: its own slot must be free or already its own claim (missed writes => NULL).
PrimaryCasOk(n, r) == accepted[n] = NULL \/ accepted[n].owner = r

\* Member monotonic guard: a member never lets a COMMITTED value be reassigned to a different owner.
MonotonicOk(Q, r) ==
    \A m \in Q : accepted[m] = NULL \/ ~accepted[m].committed \/ accepted[m].owner = r

NewEpoch(n) ==
    /\ curEpoch < MaxEpoch
    /\ curEpoch' = curEpoch + 1
    /\ actingEpoch' = [actingEpoch EXCEPT ![n] = curEpoch + 1]
    /\ UNCHANGED << promised, accepted, sealedAt, recordHeld, phase >>

\* Seal: promise this epoch at a majority. NO reservation learning (unlike ClusterUniqueQuorum).
Seal(n, Q) ==
    /\ actingEpoch[n] > 0
    /\ sealedAt[n] < actingEpoch[n]
    /\ Q \in Majorities
    /\ FenceOk(Q, actingEpoch[n])
    /\ promised'   = [m \in Nodes |-> IF m \in Q THEN actingEpoch[n] ELSE promised[m]]
    /\ sealedAt'   = [sealedAt EXCEPT ![n] = actingEpoch[n]]
    /\ UNCHANGED << accepted, actingEpoch, curEpoch, recordHeld, phase >>

Reserve(n, r, Q) ==
    /\ phase[r] = "idle"
    /\ actingEpoch[n] > 0
    /\ sealedAt[n] = actingEpoch[n]
    /\ n \in Q
    /\ PrimaryCasOk(n, r)
    /\ Q \in Majorities
    /\ FenceOk(Q, actingEpoch[n])
    /\ MonotonicOk(Q, r)
    /\ accepted'   = [m \in Nodes |-> IF m \in Q
                          THEN [epoch |-> actingEpoch[n], owner |-> r, committed |-> FALSE]
                          ELSE accepted[m]]
    /\ promised'   = [m \in Nodes |-> IF m \in Q THEN actingEpoch[n] ELSE promised[m]]
    /\ phase'      = [phase EXCEPT ![r] = "routed"]
    /\ UNCHANGED << actingEpoch, sealedAt, curEpoch, recordHeld >>

WriteRecord(r) ==
    /\ phase[r] = "routed"
    /\ recordHeld' = [recordHeld EXCEPT ![r] = TRUE]
    /\ phase'      = [phase EXCEPT ![r] = "written"]
    /\ UNCHANGED << promised, accepted, actingEpoch, sealedAt, curEpoch >>

Commit(n, r, Q) ==
    /\ phase[r] = "written"
    /\ actingEpoch[n] > 0
    /\ sealedAt[n] = actingEpoch[n]
    /\ n \in Q
    /\ PrimaryCasOk(n, r)
    /\ Q \in Majorities
    /\ FenceOk(Q, actingEpoch[n])
    /\ MonotonicOk(Q, r)
    /\ accepted' = [m \in Nodes |-> IF m \in Q
                        THEN [epoch |-> actingEpoch[n], owner |-> r, committed |-> TRUE]
                        ELSE accepted[m]]
    /\ promised' = [m \in Nodes |-> IF m \in Q THEN actingEpoch[n] ELSE promised[m]]
    /\ phase'    = [phase EXCEPT ![r] = "done"]
    /\ UNCHANGED << actingEpoch, sealedAt, curEpoch, recordHeld >>

Next ==
    \/ \E n \in Nodes : NewEpoch(n)
    \/ \E n \in Nodes, Q \in Majorities : Seal(n, Q)
    \/ \E n \in Nodes, r \in Records, Q \in Majorities : Reserve(n, r, Q)
    \/ \E r \in Records : WriteRecord(r)
    \/ \E n \in Nodes, r \in Records, Q \in Majorities : Commit(n, r, Q)

Spec == Init /\ [][Next]_vars

NoOversell == Cardinality({r \in Records : recordHeld[r]}) <= 1

========================================================================
