----------------------- MODULE ClusterUniqueFix -----------------------
\* Proposed fix: the unique claim is a durable, single-authoritative, consensus-backed CAS
\* (not a per-node in-memory lock). Node crash / failover / dual-belief no longer create
\* divergent or lost claim state. TTL reclaims only ABANDONED claims (owner failed before a
\* durable record), never an active or committed one.
\*
\* Same adversarial events as ClusterUniqueV2 (Crash, PromoteEmpty, DualBelieve, Expire) —
\* the claim state is immune to them, so NoOversell must hold.

EXTENDS Naturals, FiniteSets

CONSTANTS Records, Values, Nodes, Durable

NULL == CHOOSE x : x \notin (Records \cup Values \cup Nodes)

VARIABLES
    believes,       \* [Nodes -> BOOLEAN] : node-local primary belief (adversarial; irrelevant to the claim)
    claimOwner,     \* [Values -> Records \cup {NULL}] : single authoritative durable claim
    claimCommitted, \* [Values -> BOOLEAN]
    recordVal,      \* [Records -> Values \cup {NULL}] : durable record
    target,         \* [Records -> Values \cup {NULL}]
    phase           \* [Records -> {"idle","routed","written","done","failed"}]

vars == << believes, claimOwner, claimCommitted, recordVal, target, phase >>

Phases == {"idle", "routed", "written", "done", "failed"}

TypeOK ==
    /\ believes       \in [Nodes -> BOOLEAN]
    /\ claimOwner      \in [Values -> Records \cup {NULL}]
    /\ claimCommitted  \in [Values -> BOOLEAN]
    /\ recordVal       \in [Records -> Values \cup {NULL}]
    /\ target          \in [Records -> Values \cup {NULL}]
    /\ phase           \in [Records -> Phases]

InitialPrimary == CHOOSE n \in Nodes : TRUE

Init ==
    /\ believes       = [n \in Nodes |-> n = InitialPrimary]
    /\ claimOwner      = [v \in Values |-> NULL]
    /\ claimCommitted  = [v \in Values |-> FALSE]
    /\ recordVal       = [r \in Records |-> NULL]
    /\ target          = [r \in Records |-> NULL]
    /\ phase           = [r \in Records |-> "idle"]

\* Reserve is a consensus CAS on the single authoritative claim. A minority-partitioned node
\* cannot complete it (no quorum) -> there is only ever one claim slot per value.
Reserve(r, v) ==
    /\ phase[r] = "idle"
    /\ claimOwner[v] = NULL
    /\ claimOwner' = [claimOwner EXCEPT ![v] = r]
    /\ target'     = [target     EXCEPT ![r] = v]
    /\ phase'      = [phase      EXCEPT ![r] = "routed"]
    /\ UNCHANGED << believes, claimCommitted, recordVal >>

ReserveConflict(r, v) ==
    /\ phase[r] = "idle"
    /\ claimOwner[v] # NULL
    /\ phase' = [phase EXCEPT ![r] = "failed"]
    /\ UNCHANGED << believes, claimOwner, claimCommitted, recordVal, target >>

WriteRecord(r) ==
    /\ phase[r] = "routed"
    /\ recordVal' = [recordVal EXCEPT ![r] = target[r]]
    /\ phase'     = [phase     EXCEPT ![r] = "written"]
    /\ UNCHANGED << believes, claimOwner, claimCommitted, target >>

\* Reliable commit through the same consensus object.
Commit(r) ==
    /\ phase[r] = "written"
    /\ claimOwner[target[r]] = r
    /\ ~claimCommitted[target[r]]
    /\ claimCommitted' = [claimCommitted EXCEPT ![target[r]] = TRUE]
    /\ phase'          = [phase EXCEPT ![r] = "done"]
    /\ UNCHANGED << believes, claimOwner, recordVal, target >>

\* Owner crashes AFTER reserve but BEFORE writing a durable record: the claim is abandoned.
Abandon(r) ==
    /\ phase[r] = "routed"
    /\ phase' = [phase EXCEPT ![r] = "failed"]
    /\ UNCHANGED << believes, claimOwner, claimCommitted, recordVal, target >>

\* TTL reclaims ONLY an abandoned, uncommitted claim (owner failed with no durable record).
\* It can never steal an active (routed/written) or committed claim.
Expire(v) ==
    /\ claimOwner[v] # NULL
    /\ ~claimCommitted[v]
    /\ phase[claimOwner[v]] = "failed"
    /\ claimOwner' = [claimOwner EXCEPT ![v] = NULL]
    /\ UNCHANGED << believes, claimCommitted, recordVal, target, phase >>

\* --- Adversarial node events: manipulate belief only; claim state is consensus-durable. ---

\* Durable=TRUE: the authoritative claim survives crash (consensus/quorum durability).
\* Durable=FALSE: the claim is lost on crash (fenced but NOT durable) -- reintroduces class A.
Crash(n) ==
    /\ believes[n]
    /\ believes' = [believes EXCEPT ![n] = FALSE]
    /\ claimOwner'     = IF Durable THEN claimOwner ELSE [v \in Values |-> NULL]
    /\ claimCommitted' = IF Durable THEN claimCommitted ELSE [v \in Values |-> FALSE]
    /\ UNCHANGED << recordVal, target, phase >>

PromoteEmpty(n) ==
    /\ ~believes[n]
    /\ believes' = [believes EXCEPT ![n] = TRUE]
    /\ UNCHANGED << claimOwner, claimCommitted, recordVal, target, phase >>

DualBelieve(n) ==
    /\ ~believes[n]
    /\ \E m \in Nodes : m # n /\ believes[m]
    /\ believes' = [believes EXCEPT ![n] = TRUE]
    /\ UNCHANGED << claimOwner, claimCommitted, recordVal, target, phase >>

Next ==
    \/ \E r \in Records, v \in Values : Reserve(r, v)
    \/ \E r \in Records, v \in Values : ReserveConflict(r, v)
    \/ \E r \in Records : WriteRecord(r)
    \/ \E r \in Records : Commit(r)
    \/ \E r \in Records : Abandon(r)
    \/ \E v \in Values : Expire(v)
    \/ \E n \in Nodes : Crash(n)
    \/ \E n \in Nodes : PromoteEmpty(n)
    \/ \E n \in Nodes : DualBelieve(n)

Spec == Init /\ [][Next]_vars

NoOversell ==
    \A v \in Values : Cardinality({r \in Records : recordVal[r] = v}) <= 1

\* A committed claim implies its record is the unique holder of that value.
CommittedExclusive ==
    \A v \in Values : claimCommitted[v] =>
        \A r \in Records : (recordVal[r] = v => claimOwner[v] = r)

\* --- Liveness: no value is wedged forever ---
\* The system makes progress (records write + commit) and the GC reaps abandoned claims.
\* Crash/Promote/DualBelieve/Abandon are failure events and are deliberately NOT forced.
Fairness ==
    /\ WF_vars(\E r \in Records : WriteRecord(r))
    /\ WF_vars(\E r \in Records : Commit(r))
    /\ WF_vars(\E v \in Values : Expire(v))

FairSpec == Init /\ [][Next]_vars /\ Fairness

\* Every claimed-but-uncommitted value eventually either commits or is released.
Resolves ==
    \A v \in Values :
        (claimOwner[v] # NULL /\ ~claimCommitted[v])
            ~> (claimCommitted[v] \/ claimOwner[v] = NULL)

\* Negative control: a free value need NOT ever become committed (nobody is forced to claim).
\* This MUST be reported as a liveness violation, proving leads-to is really being evaluated.
NegControl ==
    \A v \in Values : (claimOwner[v] = NULL) ~> claimCommitted[v]

========================================================================
