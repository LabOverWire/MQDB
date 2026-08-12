-------------------------- MODULE ClusterShareAccess --------------------------
\* Confidentiality of share-grant access checks over an asynchronously
\* replicated store (GitHub #75, cluster parity for diagram sharing).
\*
\* The existing DiagramSharing.tla proves the sharing DECISION function over one
\* consistent grant table. The cluster stores grants in a partitioned,
\* asynchronously replicated store and repurposes that store as an ACL, so the
\* decision is graded against a possibly-stale local copy. This spec models the
\* INTERSECTION the existing specs never touch.
\*
\* One (resource, grantee) grant cell lives on the resource's partition,
\* replicated across Nodes. The PRIMARY applies writes synchronously (committed
\* and store[primary] move together); replicas catch up lazily (Replicate). An
\* access-check Read is served from some node's LOCAL copy. We ask: after an
\* unshare is committed (committed = Absent), can a serving node still grant the
\* revoked principal because its local copy is a stale Present?
\*
\* Design forks this check decides:
\*   ReadMode    \in {"ReplicaServe", "PrimaryOnly"}   -- serve reads on any replica, or force to primary
\*   PromoteGate \in {"Ungated", "DrainGated"}         -- promote any replica, or only a caught-up one
\* Expected results (run per mode via the constants override):
\*   ReplicaServe , *            -> InvNoStaleGrantLeak VIOLATED (standing window, no failover needed)
\*   PrimaryOnly  , Ungated      -> VIOLATED (a promoted-but-behind primary serves a stale grant)
\*   PrimaryOnly  , DrainGated   -> HOLDS (both mitigations are required together)

EXTENDS Naturals

CONSTANTS Nodes, ReadMode, PromoteGate

Present == "present"
Absent  == "absent"
GrantState == {Present, Absent}

VARIABLES store, primary, committed

vars == <<store, primary, committed>>

TypeOK ==
    /\ store \in [Nodes -> GrantState]
    /\ primary \in Nodes
    /\ committed \in GrantState

Init ==
    /\ store = [n \in Nodes |-> Absent]
    /\ primary \in Nodes
    /\ committed = Absent

\* The primary applies writes synchronously.
Share ==
    /\ committed' = Present
    /\ store' = [store EXCEPT ![primary] = Present]
    /\ UNCHANGED primary

Unshare ==
    /\ committed' = Absent
    /\ store' = [store EXCEPT ![primary] = Absent]
    /\ UNCHANGED primary

\* A replica lazily catches up to the primary's current value.
Replicate(n) ==
    /\ n # primary
    /\ store' = [store EXCEPT ![n] = store[primary]]
    /\ UNCHANGED <<primary, committed>>

\* Failover. A drain-gated promotion may only pick a candidate that has caught
\* up to the current primary (store[n] = store[primary]).
CanPromote(n) ==
    \/ PromoteGate = "Ungated"
    \/ store[n] = store[primary]

Promote(n) ==
    /\ CanPromote(n)
    /\ primary' = n
    /\ UNCHANGED <<store, committed>>

Next ==
    \/ Share
    \/ Unshare
    \/ \E n \in Nodes : Replicate(n)
    \/ \E n \in Nodes : Promote(n)

Spec == Init /\ [][Next]_vars

\* Which nodes serve access-check reads.
ServingNodes == IF ReadMode = "PrimaryOnly" THEN {primary} ELSE Nodes

\* Confidentiality: once a grant is revoked (committed = Absent), no serving
\* node may still hold a stale Present that would grant the revoked principal.
InvNoStaleGrantLeak ==
    committed = Absent => \A n \in ServingNodes : store[n] = Absent

=============================================================================
