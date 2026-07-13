------------------------- MODULE ClusterUnique -------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS Records, Values, AllowLostCommit

NULL == CHOOSE x : x \notin (Records \cup Values)

VARIABLES
    resvOwner,      \* Values -> Records \cup {NULL} : reservation holder at the value-primary
    resvCommitted,  \* Values -> BOOLEAN : reservation promoted to permanent
    recordVal,      \* Records -> Values \cup {NULL} : the record written on the id-primary
    target,         \* Records -> Values \cup {NULL} : value this writer is claiming
    phase           \* Records -> {"idle","reserved","written","done","failed"}

vars == << resvOwner, resvCommitted, recordVal, target, phase >>

Phases == {"idle", "reserved", "written", "done", "failed"}

TypeOK ==
    /\ resvOwner     \in [Values  -> Records \cup {NULL}]
    /\ resvCommitted \in [Values  -> BOOLEAN]
    /\ recordVal     \in [Records -> Values  \cup {NULL}]
    /\ target        \in [Records -> Values  \cup {NULL}]
    /\ phase         \in [Records -> Phases]

Init ==
    /\ resvOwner     = [v \in Values  |-> NULL]
    /\ resvCommitted = [v \in Values  |-> FALSE]
    /\ recordVal     = [r \in Records |-> NULL]
    /\ target        = [r \in Records |-> NULL]
    /\ phase         = [r \in Records |-> "idle"]

Reserve(r, v) ==
    /\ phase[r] = "idle"
    /\ resvOwner[v] = NULL
    /\ resvOwner' = [resvOwner EXCEPT ![v] = r]
    /\ target'    = [target    EXCEPT ![r] = v]
    /\ phase'     = [phase     EXCEPT ![r] = "reserved"]
    /\ UNCHANGED << resvCommitted, recordVal >>

ReserveConflict(r, v) ==
    /\ phase[r] = "idle"
    /\ resvOwner[v] # NULL
    /\ phase' = [phase EXCEPT ![r] = "failed"]
    /\ UNCHANGED << resvOwner, resvCommitted, recordVal, target >>

WriteRecord(r) ==
    /\ phase[r] = "reserved"
    /\ recordVal' = [recordVal EXCEPT ![r] = target[r]]
    /\ phase'     = [phase     EXCEPT ![r] = "written"]
    /\ UNCHANGED << resvOwner, resvCommitted, target >>

CommitArrive(r) ==
    /\ phase[r] = "written"
    /\ resvOwner[target[r]] = r
    /\ ~resvCommitted[target[r]]
    /\ resvCommitted' = [resvCommitted EXCEPT ![target[r]] = TRUE]
    /\ phase'         = [phase         EXCEPT ![r] = "done"]
    /\ UNCHANGED << resvOwner, recordVal, target >>

CommitLost(r) ==
    /\ phase[r] = "written"
    /\ resvOwner[target[r]] # r
    /\ phase' = [phase EXCEPT ![r] = "failed"]
    /\ UNCHANGED << resvOwner, resvCommitted, recordVal, target >>

CanExpire(v) ==
    \/ AllowLostCommit
    \/ \A r \in Records : (resvOwner[v] = r => phase[r] \notin {"reserved", "written"})

Expire(v) ==
    /\ resvOwner[v] # NULL
    /\ ~resvCommitted[v]
    /\ CanExpire(v)
    /\ resvOwner' = [resvOwner EXCEPT ![v] = NULL]
    /\ UNCHANGED << resvCommitted, recordVal, target, phase >>

Next ==
    \/ \E r \in Records, v \in Values : Reserve(r, v)
    \/ \E r \in Records, v \in Values : ReserveConflict(r, v)
    \/ \E r \in Records : WriteRecord(r)
    \/ \E r \in Records : CommitArrive(r)
    \/ \E r \in Records : CommitLost(r)
    \/ \E v \in Values : Expire(v)

Spec == Init /\ [][Next]_vars

NoDuplicateValue ==
    \A v \in Values : Cardinality({r \in Records : recordVal[r] = v}) <= 1

CommittedIsExclusive ==
    \A v \in Values : resvCommitted[v] =>
        \A r \in Records : (recordVal[r] = v => resvOwner[v] = r)

========================================================================
