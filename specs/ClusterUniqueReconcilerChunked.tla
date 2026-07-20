------------------ MODULE ClusterUniqueReconcilerChunked ------------------
\* Distributed model of the unique reconciler's collect->apply staleness, built to answer one
\* question: is making COLLECT yield its lock periodically (chunked collect) safe?
\*
\* The reconciler has TWO sites for a given (record, value):
\*   * the DATA site  -- holds the durable record; runs collect + the record_owns_value recheck
\*                       (crates/.../node_controller/unique.rs:1101-1179).
\*   * the VALUE site  -- holds the unique claim (keyed by value); applies reasserts with NO recheck
\*                       (unique.rs:451-463 handle_unique_reassert_request -> reassert_unique_claim).
\* They are the SAME node only when unique_partition(value) == data_partition(record). Because those
\* hash independently, the cross-site case is the common one.
\*
\* Both the reconciler's reassert AND a record delete's claim release travel DATA->VALUE and can be
\* REORDERED (fire-and-forget / forwarded writes, no mutual FIFO). reassertMsgs / releaseMsgs are the
\* in-flight sets, delivered in any order.
\*
\* Modeling the collect->apply staleness (the thing chunking changes): a hint may be CAPTURED
\* (CollectHint) whenever the record owns the value, and APPLIED (ApplyHint) later after arbitrary
\* interleaving. That nondeterministic gap is a superset of atomic-collect (today) and chunked-collect
\* (proposed). ApplyHint re-runs the recheck on the DATA site (under the write lock, so recheck+send
\* are atomic w.r.t. local writes) -- exactly record_owns_value before the reassert.
\*
\* Switches:
\*   Colocated = TRUE  -> value site == data site: recheck and reassert are one atomic step, and a
\*                        delete's release is applied inline. This is the LOCAL reconcile path.
\*             = FALSE -> separate sites: recheck on data, reassert crosses the network to the value
\*                        site (applied with no recheck), release also crosses and may reorder.
\*   RecheckOwnership = TRUE -> ApplyHint drops a hint whose record no longer owns the value
\*                              (record_owns_value). FALSE = negative control.
\*
\* Result (see the three .cfg files):
\*   local  (Colocated=TRUE,  Recheck=TRUE)  -> SAFE: chunking the collect adds no wedge; the
\*                                              apply-time recheck (under the controller write lock,
\*                                              atomic with the send decision) closes the widened
\*                                              collect->apply gap. This is the answer to the fix.
\*   norecheck (Colocated=TRUE, Recheck=FALSE) -> WEDGE: the recheck is load-bearing.
\*   remote (Colocated=FALSE, Recheck=TRUE)  -> WEDGE reachable IN THE MODEL: a release overtakes an
\*                                              in-flight reassert, re-Establishing a committed claim
\*                                              for a value the record no longer owns.
\*
\* Quorum resolution of the remote case (what the code actually does):
\*   - reassert and release for a value travel DATA->VALUE and, for a STABLE primary, over the SAME
\*     ordered per-peer QUIC stream, sent from the same single-threaded controller loop -> FIFO, so a
\*     later release cannot overtake an earlier reassert. The remote wedge as traced is therefore an
\*     ARTIFACT for a stable primary (the "no mutual FIFO" premise below is only true across a
\*     primary FAILOVER, where the two messages come from different source nodes and there is no
\*     epoch fence on handle_unique_reassert_request/handle_unique_release_request). So the remote
\*     cfg documents a real FRAGILITY -- the value site has no recheck and no epoch fence, and only
\*     message ordering prevents the wedge -- exercisable only under failover of the data primary.
\*   - The reorder trigger via record DELETE does not exist at all: cluster-mode delete
\*     (db_delete_replicated) emits NO unique release. That is a SEPARATE, confirmed pre-existing bug
\*     (agent mode releases on delete via release_unique_guards + test delete_releases_unique_guard;
\*     the cluster delete path never does), which permanently leaks the unique value. DeleteRecord
\*     below therefore models UPDATE-AWAY / agent-mode-delete release semantics, not cluster delete.
\*   - Neither the fragility nor the delete-leak is caused or widened by chunking the collect: the
\*     send->deliver gap is independent of the data-node-local collect->apply window.
\*
\* Single contended value V, modelled implicitly. Create is collapsed to one atomic step (reserve
\* only when the claim is free, then write+commit) -- the reserve/write/commit/CAS interleavings and
\* their oversell are covered by ClusterUniqueReconciler.tla; here the focus is the reconciler wedge.

EXTENDS Naturals, FiniteSets

\* DeleteReleases = FALSE reproduces the confirmed cluster bug (delete never releases the claim);
\* TRUE models the fix (delete emits a release, as agent mode and update-away already do).
CONSTANTS Records, Colocated, RecheckOwnership, DeleteReleases

NULL == CHOOSE x : x \notin Records

VARIABLES
    recordExists,    \* [Records -> BOOLEAN] : durable record present (DATA site)
    hasV,            \* [Records -> BOOLEAN] : record's unique field currently equals V (DATA site)
    claimOwner,      \* Records \cup {NULL}  : owner of the claim for V (VALUE site)
    claimCommitted,  \* BOOLEAN              : claim promoted to permanent (VALUE site)
    pendingHints,    \* SUBSET Records : reassert hints captured by collect, not yet applied (DATA)
    reassertMsgs,    \* SUBSET Records : in-flight reassert(r) requests DATA->VALUE
    releaseMsgs      \* SUBSET Records : in-flight release(r) requests DATA->VALUE (from delete/update)

vars == << recordExists, hasV, claimOwner, claimCommitted,
           pendingHints, reassertMsgs, releaseMsgs >>

Owns(r) == recordExists[r] /\ hasV[r]

\* Effect of reassert(V, r) on the claim (UniqueStore::reassert, unique_store.rs:287-312):
\*   absent -> Establish committed(r); owned-by-r -> commit; owned-by-other -> no-op (Conflict/Pending).
ReassertOwner(r)     == IF claimOwner = NULL THEN r ELSE claimOwner
ReassertCommitted(r) == IF claimOwner = NULL \/ claimOwner = r THEN TRUE ELSE claimCommitted

\* Effect of release(V, r): removes the claim iff owned by r (unique_store.rs:252-267).
ReleaseOwner(r)     == IF claimOwner = r THEN NULL  ELSE claimOwner
ReleaseCommitted(r) == IF claimOwner = r THEN FALSE ELSE claimCommitted

TypeOK ==
    /\ recordExists   \in [Records -> BOOLEAN]
    /\ hasV           \in [Records -> BOOLEAN]
    /\ claimOwner     \in Records \cup {NULL}
    /\ claimCommitted \in BOOLEAN
    /\ pendingHints   \in SUBSET Records
    /\ reassertMsgs   \in SUBSET Records
    /\ releaseMsgs    \in SUBSET Records

Init ==
    /\ recordExists   = [r \in Records |-> FALSE]
    /\ hasV           = [r \in Records |-> FALSE]
    /\ claimOwner     = NULL
    /\ claimCommitted = FALSE
    /\ pendingHints   = {}
    /\ reassertMsgs   = {}
    /\ releaseMsgs    = {}

\* Create r: succeeds only when the claim for V is free (reserve would otherwise Conflict). Collapses
\* reserve->write->commit into one atomic step (see header). Establishes record + committed claim.
CreateRecord(r) ==
    /\ ~recordExists[r]
    /\ claimOwner = NULL
    /\ recordExists'   = [recordExists EXCEPT ![r] = TRUE]
    /\ hasV'           = [hasV EXCEPT ![r] = TRUE]
    /\ claimOwner'     = r
    /\ claimCommitted' = TRUE
    /\ UNCHANGED << pendingHints, reassertMsgs, releaseMsgs >>

\* Delete r: remove the record, then release its claim for V. Inline when co-located, else the release
\* is forwarded to the value site (may reorder vs a reassert).
DeleteRecord(r) ==
    /\ recordExists[r]
    /\ recordExists' = [recordExists EXCEPT ![r] = FALSE]
    /\ hasV'         = [hasV EXCEPT ![r] = FALSE]
    /\ IF ~DeleteReleases
         THEN UNCHANGED << claimOwner, claimCommitted, releaseMsgs >>  \* current cluster bug: no release
         ELSE IF Colocated
           THEN /\ claimOwner'     = ReleaseOwner(r)
                /\ claimCommitted' = ReleaseCommitted(r)
                /\ UNCHANGED releaseMsgs
           ELSE /\ releaseMsgs' = releaseMsgs \cup {r}
                /\ UNCHANGED << claimOwner, claimCommitted >>
    /\ UNCHANGED << pendingHints, reassertMsgs >>

\* Update r's unique field away from V: record persists, releases r's claim for V (same routing).
UpdateAway(r) ==
    /\ recordExists[r]
    /\ hasV[r]
    /\ hasV' = [hasV EXCEPT ![r] = FALSE]
    /\ IF Colocated
         THEN /\ claimOwner'     = ReleaseOwner(r)
              /\ claimCommitted' = ReleaseCommitted(r)
              /\ UNCHANGED releaseMsgs
         ELSE /\ releaseMsgs' = releaseMsgs \cup {r}
              /\ UNCHANGED << claimOwner, claimCommitted >>
    /\ UNCHANGED << recordExists, pendingHints, reassertMsgs >>

\* Reconciler COLLECT: capture one hint whenever the record currently owns V (staggered capture across
\* records = the yielding collect). Data-site read only.
CollectHint(r) ==
    /\ Owns(r)
    /\ r \notin pendingHints
    /\ pendingHints' = pendingHints \cup {r}
    /\ UNCHANGED << recordExists, hasV, claimOwner, claimCommitted, reassertMsgs, releaseMsgs >>

\* Reconciler APPLY: record_owns_value recheck on the DATA site. If it passes (or the recheck is
\* switched off), reassert -- inline when co-located, else send to the value site.
ApplyHint(r) ==
    /\ r \in pendingHints
    /\ pendingHints' = pendingHints \ {r}
    /\ IF RecheckOwnership /\ ~Owns(r)
         THEN UNCHANGED << claimOwner, claimCommitted, reassertMsgs >>      \* recheck skip
         ELSE IF Colocated
                THEN /\ claimOwner'     = ReassertOwner(r)                  \* recheck+reassert atomic
                     /\ claimCommitted' = ReassertCommitted(r)
                     /\ UNCHANGED reassertMsgs
                ELSE /\ reassertMsgs' = reassertMsgs \cup {r}               \* crosses network, no recheck there
                     /\ UNCHANGED << claimOwner, claimCommitted >>
    /\ UNCHANGED << recordExists, hasV, releaseMsgs >>

\* VALUE site applies a delivered reassert -- NO recheck (it holds no record).
DeliverReassert(r) ==
    /\ r \in reassertMsgs
    /\ reassertMsgs' = reassertMsgs \ {r}
    /\ claimOwner'     = ReassertOwner(r)
    /\ claimCommitted' = ReassertCommitted(r)
    /\ UNCHANGED << recordExists, hasV, pendingHints, releaseMsgs >>

\* VALUE site applies a delivered release.
DeliverRelease(r) ==
    /\ r \in releaseMsgs
    /\ releaseMsgs' = releaseMsgs \ {r}
    /\ claimOwner'     = ReleaseOwner(r)
    /\ claimCommitted' = ReleaseCommitted(r)
    /\ UNCHANGED << recordExists, hasV, pendingHints, reassertMsgs >>

Next ==
    \/ \E r \in Records : CreateRecord(r)
    \/ \E r \in Records : DeleteRecord(r)
    \/ \E r \in Records : UpdateAway(r)
    \/ \E r \in Records : CollectHint(r)
    \/ \E r \in Records : ApplyHint(r)
    \/ \E r \in Records : DeliverReassert(r)
    \/ \E r \in Records : DeliverRelease(r)

Spec == Init /\ [][Next]_vars

\* ---- Invariants ----

\* At most one durable record holds value V (the unique constraint itself).
NoOversell == Cardinality({r \in Records : Owns(r)}) <= 1

\* A committed claim must be backed by a live record that owns V, OR a release for its owner must be
\* in flight that will clean it up. A state that commits a claim for a non-owning record with NO
\* pending release is a PERMANENT wedge: TTL never reclaims a committed claim, and the record-driven
\* reconciler will never revisit a gone record. This is the property the apply-time recheck protects.
ClaimBackedOrReleasing ==
    (claimCommitted /\ claimOwner # NULL /\ ~Owns(claimOwner))
        => (claimOwner \in releaseMsgs)

===========================================================================
