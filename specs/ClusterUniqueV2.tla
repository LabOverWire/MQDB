------------------------ MODULE ClusterUniqueV2 ------------------------
\* Faithful model of the CURRENT cluster unique-constraint protocol.
\* Reservation = fragile per-node in-memory lock, decoupled from the durable record.
\* Reproduces oversell class A (durability/TTL loss) and class B (dual-primary / handoff).

EXTENDS Naturals, FiniteSets

CONSTANTS Records, Values, Nodes

NULL == CHOOSE x : x \notin (Records \cup Values \cup Nodes)

VARIABLES
    believes,       \* [Nodes -> BOOLEAN] : node currently acts as primary for the value-partition
    resvOwner,      \* [Nodes -> [Values -> Records \cup {NULL}]] : per-node in-memory reservation holder
    resvCommitted,  \* [Nodes -> [Values -> BOOLEAN]] : per-node committed flag
    recordVal,      \* [Records -> Values \cup {NULL}] : durable, replicated record (data partition)
    target,         \* [Records -> Values \cup {NULL}] : value the record is claiming
    resvNode,       \* [Records -> Nodes \cup {NULL}] : node the record reserved on
    phase           \* [Records -> {"idle","routed","written","done","failed"}]

vars == << believes, resvOwner, resvCommitted, recordVal, target, resvNode, phase >>

Phases == {"idle", "routed", "written", "done", "failed"}

TypeOK ==
    /\ believes      \in [Nodes -> BOOLEAN]
    /\ resvOwner     \in [Nodes -> [Values -> Records \cup {NULL}]]
    /\ resvCommitted \in [Nodes -> [Values -> BOOLEAN]]
    /\ recordVal     \in [Records -> Values \cup {NULL}]
    /\ target        \in [Records -> Values \cup {NULL}]
    /\ resvNode      \in [Records -> Nodes \cup {NULL}]
    /\ phase         \in [Records -> Phases]

InitialPrimary == CHOOSE n \in Nodes : TRUE

Init ==
    /\ believes      = [n \in Nodes |-> n = InitialPrimary]
    /\ resvOwner     = [n \in Nodes |-> [v \in Values |-> NULL]]
    /\ resvCommitted = [n \in Nodes |-> [v \in Values |-> FALSE]]
    /\ recordVal     = [r \in Records |-> NULL]
    /\ target        = [r \in Records |-> NULL]
    /\ resvNode      = [r \in Records |-> NULL]
    /\ phase         = [r \in Records |-> "idle"]

\* A create routes to a node it believes is primary and reserves there (atomic check-and-set,
\* but only against THAT node's local store).
Reserve(r, v, n) ==
    /\ phase[r] = "idle"
    /\ believes[n]
    /\ resvOwner[n][v] = NULL
    /\ resvOwner'  = [resvOwner  EXCEPT ![n][v] = r]
    /\ target'     = [target     EXCEPT ![r] = v]
    /\ resvNode'   = [resvNode   EXCEPT ![r] = n]
    /\ phase'      = [phase      EXCEPT ![r] = "routed"]
    /\ UNCHANGED << believes, resvCommitted, recordVal >>

ReserveConflict(r, v, n) ==
    /\ phase[r] = "idle"
    /\ believes[n]
    /\ resvOwner[n][v] # NULL
    /\ phase' = [phase EXCEPT ![r] = "failed"]
    /\ UNCHANGED << believes, resvOwner, resvCommitted, recordVal, target, resvNode >>

\* The durable record is written on the data partition (persisted + replicated).
WriteRecord(r) ==
    /\ phase[r] = "routed"
    /\ recordVal' = [recordVal EXCEPT ![r] = target[r]]
    /\ phase'     = [phase     EXCEPT ![r] = "written"]
    /\ UNCHANGED << believes, resvOwner, resvCommitted, target, resvNode >>

\* Commit succeeds only if the reservation is still held by this record on its node.
Commit(r) ==
    /\ phase[r] = "written"
    /\ resvNode[r] # NULL
    /\ resvOwner[resvNode[r]][target[r]] = r
    /\ ~resvCommitted[resvNode[r]][target[r]]
    /\ resvCommitted' = [resvCommitted EXCEPT ![resvNode[r]][target[r]] = TRUE]
    /\ phase'         = [phase EXCEPT ![r] = "done"]
    /\ UNCHANGED << believes, resvOwner, recordVal, target, resvNode >>

\* Commit lands but the reservation was lost/taken: fire-and-forget, silently dropped.
\* The durable record REMAINS (never rolled back).
CommitLost(r) ==
    /\ phase[r] = "written"
    /\ resvNode[r] # NULL
    /\ resvOwner[resvNode[r]][target[r]] # r
    /\ phase' = [phase EXCEPT ![r] = "failed"]
    /\ UNCHANGED << believes, resvOwner, resvCommitted, recordVal, target, resvNode >>

\* --- Failure / distributed events ---

\* TTL expiry of an uncommitted reservation (hourly cleanup, in-memory).
Expire(n, v) ==
    /\ resvOwner[n][v] # NULL
    /\ ~resvCommitted[n][v]
    /\ resvOwner' = [resvOwner EXCEPT ![n][v] = NULL]
    /\ UNCHANGED << believes, resvCommitted, recordVal, target, resvNode, phase >>

\* Node crash: in-memory reservations lost (not durable / not fsync'd / not replicated),
\* node stops believing it is primary.
Crash(n) ==
    /\ believes[n]
    /\ resvOwner'     = [resvOwner     EXCEPT ![n] = [v \in Values |-> NULL]]
    /\ resvCommitted' = [resvCommitted EXCEPT ![n] = [v \in Values |-> FALSE]]
    /\ believes'      = [believes      EXCEPT ![n] = FALSE]
    /\ UNCHANGED << recordVal, target, resvNode, phase >>

\* Failover / rebalance grants primary belief to another node WITHOUT transferring
\* the (async-replicated / in-memory) reservation state: promotion gap.
PromoteEmpty(n) ==
    /\ ~believes[n]
    /\ believes' = [believes EXCEPT ![n] = TRUE]
    /\ UNCHANGED << resvOwner, resvCommitted, recordVal, target, resvNode, phase >>

\* Stale-map / partition window: a second node also believes it is primary (dual-primary),
\* while the first still believes. No reservation state is shared between them.
DualBelieve(n) ==
    /\ ~believes[n]
    /\ \E m \in Nodes : m # n /\ believes[m]
    /\ believes' = [believes EXCEPT ![n] = TRUE]
    /\ UNCHANGED << resvOwner, resvCommitted, recordVal, target, resvNode, phase >>

Next ==
    \/ \E r \in Records, v \in Values, n \in Nodes : Reserve(r, v, n)
    \/ \E r \in Records, v \in Values, n \in Nodes : ReserveConflict(r, v, n)
    \/ \E r \in Records : WriteRecord(r)
    \/ \E r \in Records : Commit(r)
    \/ \E r \in Records : CommitLost(r)
    \/ \E n \in Nodes, v \in Values : Expire(n, v)
    \/ \E n \in Nodes : Crash(n)
    \/ \E n \in Nodes : PromoteEmpty(n)
    \/ \E n \in Nodes : DualBelieve(n)

Spec == Init /\ [][Next]_vars

\* At most one durable record per unique value.
NoOversell ==
    \A v \in Values : Cardinality({r \in Records : recordVal[r] = v}) <= 1

========================================================================
