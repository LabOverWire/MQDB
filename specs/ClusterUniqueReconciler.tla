--------------------- MODULE ClusterUniqueReconciler ---------------------
\* Models the Phase 1 reconciler: a leader-driven repair loop that reconciles the
\* (durable) unique claim against the durable record. Focus: the reconciler must NEVER
\* cause oversell. Its subtle hazard is a TOCTOU -- reclaiming an uncommitted, record-less
\* claim exactly while the coordinator is about to write the record.
\*
\* Switch ClaimConditionalWrite:
\*   TRUE  -> the record write verifies the claim is still held (CAS): expect NoOversell to hold
\*            even under an aggressive reconciler.
\*   FALSE -> the record write is unconditional (today's db_create): expect oversell.
\*
\* Single contended value, modelled implicitly. Records = concurrent claimers.

EXTENDS Naturals, FiniteSets

CONSTANTS Records, ClaimConditionalWrite

NULL == CHOOSE x : x \notin Records

VARIABLES
    claimOwner,      \* Records \cup {NULL} : holder of the (durable) claim for the value
    claimCommitted,  \* BOOLEAN : claim promoted to permanent
    recordExists,    \* [Records -> BOOLEAN] : durable record written on the data partition
    phase            \* [Records -> {"idle","reserved","recorded","done","failed","abandoned"}]

vars == << claimOwner, claimCommitted, recordExists, phase >>

Phases == {"idle", "reserved", "recorded", "done", "failed", "abandoned"}

TypeOK ==
    /\ claimOwner     \in Records \cup {NULL}
    /\ claimCommitted \in BOOLEAN
    /\ recordExists   \in [Records -> BOOLEAN]
    /\ phase          \in [Records -> Phases]

Init ==
    /\ claimOwner     = NULL
    /\ claimCommitted = FALSE
    /\ recordExists   = [r \in Records |-> FALSE]
    /\ phase          = [r \in Records |-> "idle"]

Reserve(r) ==
    /\ phase[r] = "idle"
    /\ claimOwner = NULL
    /\ claimOwner' = r
    /\ phase'      = [phase EXCEPT ![r] = "reserved"]
    /\ UNCHANGED << claimCommitted, recordExists >>

\* The record write. Claim-conditional (CAS) when the switch is on: it only persists the
\* record if this record still holds the claim -- so a reconciler that reclaimed the claim
\* fences a late write.
WriteRecord(r) ==
    /\ phase[r] = "reserved"
    /\ (ClaimConditionalWrite => claimOwner = r)
    /\ recordExists' = [recordExists EXCEPT ![r] = TRUE]
    /\ phase'        = [phase EXCEPT ![r] = "recorded"]
    /\ UNCHANGED << claimOwner, claimCommitted >>

\* The claim-conditional write refused: the claim was reclaimed out from under it.
WriteRecordFenced(r) ==
    /\ ClaimConditionalWrite
    /\ phase[r] = "reserved"
    /\ claimOwner # r
    /\ phase' = [phase EXCEPT ![r] = "failed"]
    /\ UNCHANGED << claimOwner, claimCommitted, recordExists >>

Commit(r) ==
    /\ phase[r] = "recorded"
    /\ claimOwner = r
    /\ ~claimCommitted
    /\ claimCommitted' = TRUE
    /\ phase'          = [phase EXCEPT ![r] = "done"]
    /\ UNCHANGED << claimOwner, recordExists >>

\* Coordinator dies after reserving, before writing the record.
Abandon(r) ==
    /\ phase[r] = "reserved"
    /\ phase' = [phase EXCEPT ![r] = "abandoned"]
    /\ UNCHANGED << claimOwner, claimCommitted, recordExists >>

\* --- Reconciler (leader-driven), reconciling the claim to the durable record ---

\* Repair a lost commit: an uncommitted claim whose record exists is promoted.
ReconcilePromote ==
    /\ claimOwner # NULL
    /\ ~claimCommitted
    /\ recordExists[claimOwner]
    /\ claimCommitted' = TRUE
    /\ UNCHANGED << claimOwner, recordExists, phase >>

\* Reclaim an abandoned claim: uncommitted, with no durable record for the owner.
\* Modelled AGGRESSIVELY -- allowed regardless of the owner's phase (worst case: fires
\* while the owner is still "reserved", racing a late WriteRecord).
ReconcileReclaim ==
    /\ claimOwner # NULL
    /\ ~claimCommitted
    /\ ~recordExists[claimOwner]
    /\ claimOwner' = NULL
    /\ UNCHANGED << claimCommitted, recordExists, phase >>

Next ==
    \/ \E r \in Records : Reserve(r)
    \/ \E r \in Records : WriteRecord(r)
    \/ \E r \in Records : WriteRecordFenced(r)
    \/ \E r \in Records : Commit(r)
    \/ \E r \in Records : Abandon(r)
    \/ ReconcilePromote
    \/ ReconcileReclaim

Spec == Init /\ [][Next]_vars

NoOversell == Cardinality({r \in Records : recordExists[r]}) <= 1

\* A committed claim's owner is the unique record holder.
CommittedConsistent ==
    claimCommitted /\ claimOwner # NULL =>
        \A r \in Records : (recordExists[r] => r = claimOwner)

==========================================================================
