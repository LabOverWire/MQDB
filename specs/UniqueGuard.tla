-------------------------- MODULE UniqueGuard --------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS Records, Values, AtomicUpdate

NULL == CHOOSE x : x \notin (Records \cup Values)

VARIABLES
    guard,      \* Values -> Records \cup {NULL} : owner of each unique-value guard key
    recordVal,  \* Records -> Values \cup {NULL} : value each row's data declares (NULL = row absent)
    obs         \* Records -> Values \cup {NULL} : a non-atomic update's observed-free target

vars == << guard, recordVal, obs >>

TypeOK ==
    /\ guard     \in [Values  -> Records \cup {NULL}]
    /\ recordVal \in [Records -> Values  \cup {NULL}]
    /\ obs       \in [Records -> Values  \cup {NULL}]

Init ==
    /\ guard     = [v \in Values  |-> NULL]
    /\ recordVal = [r \in Records |-> NULL]
    /\ obs       = [r \in Records |-> NULL]

Exists(r) == recordVal[r] # NULL

Create(r, v) ==
    /\ ~Exists(r)
    /\ obs[r] = NULL
    /\ guard[v] = NULL
    /\ guard'     = [guard     EXCEPT ![v] = r]
    /\ recordVal' = [recordVal EXCEPT ![r] = v]
    /\ UNCHANGED obs

Delete(r) ==
    /\ Exists(r)
    /\ obs[r] = NULL
    /\ guard'     = [guard     EXCEPT ![recordVal[r]] = NULL]
    /\ recordVal' = [recordVal EXCEPT ![r] = NULL]
    /\ UNCHANGED obs

UpdateAtomic(r, vNew) ==
    /\ AtomicUpdate
    /\ Exists(r)
    /\ obs[r] = NULL
    /\ vNew # recordVal[r]
    /\ guard[vNew] = NULL
    /\ guard'     = [guard EXCEPT ![vNew] = r, ![recordVal[r]] = NULL]
    /\ recordVal' = [recordVal EXCEPT ![r] = vNew]
    /\ UNCHANGED obs

ObserveNew(r, vNew) ==
    /\ ~AtomicUpdate
    /\ Exists(r)
    /\ obs[r] = NULL
    /\ vNew # recordVal[r]
    /\ guard[vNew] = NULL
    /\ obs' = [obs EXCEPT ![r] = vNew]
    /\ UNCHANGED << guard, recordVal >>

CommitUpdate(r) ==
    /\ ~AtomicUpdate
    /\ obs[r] # NULL
    /\ Exists(r)
    /\ guard'     = [guard EXCEPT ![obs[r]] = r, ![recordVal[r]] = NULL]
    /\ recordVal' = [recordVal EXCEPT ![r] = obs[r]]
    /\ obs'       = [obs EXCEPT ![r] = NULL]

Next ==
    \/ \E r \in Records, v \in Values : Create(r, v)
    \/ \E r \in Records : Delete(r)
    \/ \E r \in Records, v \in Values : UpdateAtomic(r, v)
    \/ \E r \in Records, v \in Values : ObserveNew(r, v)
    \/ \E r \in Records : CommitUpdate(r)

Spec == Init /\ [][Next]_vars

NoDoubleClaim ==
    \A v \in Values : Cardinality({r \in Records : recordVal[r] = v}) <= 1

GuardMatchesData ==
    /\ \A r \in Records : Exists(r) => guard[recordVal[r]] = r
    /\ \A v \in Values  : guard[v] # NULL => recordVal[guard[v]] = v

========================================================================
