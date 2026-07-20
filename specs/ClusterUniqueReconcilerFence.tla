-------------------- MODULE ClusterUniqueReconcilerFence --------------------
\* Models the epoch fence that closes the failover reorder wedge left open by
\* ClusterUniqueReconcilerChunked.tla (remote cfg).
\*
\* The wedge: the reconciler's reassert (data-partition primary -> value-partition primary) and a
\* delete/update's release travel as independent fire-and-forget messages. For a STABLE primary they
\* share one FIFO stream so they cannot reorder; across a data-primary FAILOVER the in-flight reassert
\* (from the deposed primary) and the release (from the new primary) take different streams and can
\* reorder, so the stale reassert lands after the release and re-Establishes a committed claim for a
\* record that is gone -> permanent wedge. The value site applies both with NO recheck and NO fence
\* today (handle_unique_reassert_request / handle_unique_release_request).
\*
\* The fence: stamp every claim-mutating message with the DATA-PARTITION EPOCH at send time. The value
\* site keeps a per-partition high-water mark (fenceEpoch) and rejects any message whose epoch is
\* BELOW it, bumping it to the max on every accepted message. A deposed primary's reassert carries the
\* old epoch and is dropped once the new primary's higher-epoch release has been applied. Repair is
\* preserved: the periodic reconciler re-issues a current-epoch reassert on its next cycle.
\*
\* Fenced = TRUE models the fix; FALSE reproduces today's unfenced reorder wedge.
\* Single contended value V and single data partition (one monotonic epoch lineage), modelled
\* implicitly. Records = claimers. Create is atomic co-located (its oversell is covered elsewhere);
\* the focus is the reassert/release reorder across failover.

EXTENDS Naturals, FiniteSets

CONSTANTS Records, Fenced, MaxEpoch

NULL == CHOOSE x : x \notin Records

VARIABLES
    recordExists,    \* [Records -> BOOLEAN] : durable record present (data site)
    hasV,            \* [Records -> BOOLEAN] : record's unique field currently equals V (data site)
    claimOwner,      \* Records \cup {NULL}  : owner of the claim for V (value site)
    claimCommitted,  \* BOOLEAN              : claim promoted to permanent (value site)
    dataEpoch,       \* 1..MaxEpoch : current epoch of the data partition (bumped by failover)
    fenceEpoch,      \* 0..MaxEpoch : value site's per-partition high-water mark
    pendingHints,    \* SUBSET Records : reassert hints captured by collect, not yet applied
    reassertMsgs,    \* SUBSET (Records \X (1..MaxEpoch)) : in-flight epoch-stamped reasserts
    releaseMsgs      \* SUBSET (Records \X (1..MaxEpoch)) : in-flight epoch-stamped releases

vars == << recordExists, hasV, claimOwner, claimCommitted, dataEpoch, fenceEpoch,
           pendingHints, reassertMsgs, releaseMsgs >>

Owns(r) == recordExists[r] /\ hasV[r]
Epochs  == 1..MaxEpoch

ReassertOwner(r)     == IF claimOwner = NULL THEN r ELSE claimOwner
ReassertCommitted(r) == IF claimOwner = NULL \/ claimOwner = r THEN TRUE ELSE claimCommitted
ReleaseOwner(r)      == IF claimOwner = r THEN NULL  ELSE claimOwner
ReleaseCommitted(r)  == IF claimOwner = r THEN FALSE ELSE claimCommitted

\* A message is accepted iff the fence is off or its epoch is at least the high-water mark.
Accept(e) == (~Fenced) \/ (e >= fenceEpoch)
Bump(e)   == IF e > fenceEpoch THEN e ELSE fenceEpoch

TypeOK ==
    /\ recordExists   \in [Records -> BOOLEAN]
    /\ hasV           \in [Records -> BOOLEAN]
    /\ claimOwner     \in Records \cup {NULL}
    /\ claimCommitted \in BOOLEAN
    /\ dataEpoch      \in Epochs
    /\ fenceEpoch     \in 0..MaxEpoch
    /\ pendingHints   \in SUBSET Records
    /\ reassertMsgs   \in SUBSET (Records \X Epochs)
    /\ releaseMsgs    \in SUBSET (Records \X Epochs)

Init ==
    /\ recordExists   = [r \in Records |-> FALSE]
    /\ hasV           = [r \in Records |-> FALSE]
    /\ claimOwner     = NULL
    /\ claimCommitted = FALSE
    /\ dataEpoch      = 1
    /\ fenceEpoch     = 0
    /\ pendingHints   = {}
    /\ reassertMsgs   = {}
    /\ releaseMsgs    = {}

\* Atomic co-located create: only when the claim for V is free AND no durable record already owns V
\* (the data-layer unique constraint; a claim lost to value-primary failover does not license a
\* second owner -- that claim-loss/oversell hazard is out of this spec's scope).
CreateRecord(r) ==
    /\ ~recordExists[r]
    /\ claimOwner = NULL
    /\ \A other \in Records : ~Owns(other)
    /\ recordExists'   = [recordExists EXCEPT ![r] = TRUE]
    /\ hasV'           = [hasV EXCEPT ![r] = TRUE]
    /\ claimOwner'     = r
    /\ claimCommitted' = TRUE
    /\ UNCHANGED << dataEpoch, fenceEpoch, pendingHints, reassertMsgs, releaseMsgs >>

\* Value-primary failover drops the claim while the durable record persists -- the exact state a
\* reassert Establish is meant to repair.
LoseClaim ==
    /\ claimOwner # NULL
    /\ Owns(claimOwner)
    /\ claimOwner'     = NULL
    /\ claimCommitted' = FALSE
    /\ UNCHANGED << recordExists, hasV, dataEpoch, fenceEpoch, pendingHints, reassertMsgs, releaseMsgs >>

\* Delete r: record gone; release forwarded to the value site, stamped with the current epoch.
DeleteRecord(r) ==
    /\ recordExists[r]
    /\ recordExists' = [recordExists EXCEPT ![r] = FALSE]
    /\ hasV'         = [hasV EXCEPT ![r] = FALSE]
    /\ releaseMsgs'  = releaseMsgs \cup {<<r, dataEpoch>>}
    /\ UNCHANGED << claimOwner, claimCommitted, dataEpoch, fenceEpoch, pendingHints, reassertMsgs >>

\* Update r's field away from V: record persists, releases V, stamped with the current epoch.
UpdateAway(r) ==
    /\ recordExists[r]
    /\ hasV[r]
    /\ hasV'        = [hasV EXCEPT ![r] = FALSE]
    /\ releaseMsgs' = releaseMsgs \cup {<<r, dataEpoch>>}
    /\ UNCHANGED << recordExists, claimOwner, claimCommitted, dataEpoch, fenceEpoch, pendingHints, reassertMsgs >>

\* Data-primary failover bumps the partition epoch.
Failover ==
    /\ dataEpoch < MaxEpoch
    /\ dataEpoch' = dataEpoch + 1
    /\ UNCHANGED << recordExists, hasV, claimOwner, claimCommitted, fenceEpoch, pendingHints, reassertMsgs, releaseMsgs >>

CollectHint(r) ==
    /\ Owns(r)
    /\ r \notin pendingHints
    /\ pendingHints' = pendingHints \cup {r}
    /\ UNCHANGED << recordExists, hasV, claimOwner, claimCommitted, dataEpoch, fenceEpoch, reassertMsgs, releaseMsgs >>

\* Apply: record_owns_value recheck on the data site; if it passes, send an epoch-stamped reassert.
ApplyHint(r) ==
    /\ r \in pendingHints
    /\ pendingHints' = pendingHints \ {r}
    /\ IF Owns(r)
         THEN reassertMsgs' = reassertMsgs \cup {<<r, dataEpoch>>}
         ELSE UNCHANGED reassertMsgs
    /\ UNCHANGED << recordExists, hasV, claimOwner, claimCommitted, dataEpoch, fenceEpoch, releaseMsgs >>

\* Value site applies a delivered reassert -- no record recheck, only the epoch fence.
DeliverReassert(r, e) ==
    /\ <<r, e>> \in reassertMsgs
    /\ reassertMsgs' = reassertMsgs \ {<<r, e>>}
    /\ IF Accept(e)
         THEN /\ claimOwner'     = ReassertOwner(r)
              /\ claimCommitted' = ReassertCommitted(r)
              /\ fenceEpoch'     = Bump(e)
         ELSE UNCHANGED << claimOwner, claimCommitted, fenceEpoch >>
    /\ UNCHANGED << recordExists, hasV, dataEpoch, pendingHints, releaseMsgs >>

\* Value site applies a delivered release. A release is ALWAYS applied and is NOT fenced: it removes
\* the claim only when the committed owner's record_id matches (ReleaseOwner), so a stale release for
\* a superseded record is a harmless no-op and can never drop another record's claim. It still bumps
\* the high-water mark so a later stale reassert is rejected. Same-epoch messages share one FIFO
\* stream and the apply recheck forbids a reassert after the record's delete, so a same-(record,epoch)
\* reassert was sent first and must be consumed first; only cross-epoch (failover) reorder remains.
DeliverRelease(r, e) ==
    /\ <<r, e>> \in releaseMsgs
    /\ <<r, e>> \notin reassertMsgs
    /\ releaseMsgs'    = releaseMsgs \ {<<r, e>>}
    /\ claimOwner'     = ReleaseOwner(r)
    /\ claimCommitted' = ReleaseCommitted(r)
    /\ fenceEpoch'     = Bump(e)
    /\ UNCHANGED << recordExists, hasV, dataEpoch, pendingHints, reassertMsgs >>

Next ==
    \/ \E r \in Records : CreateRecord(r)
    \/ LoseClaim
    \/ \E r \in Records : DeleteRecord(r)
    \/ \E r \in Records : UpdateAway(r)
    \/ Failover
    \/ \E r \in Records : CollectHint(r)
    \/ \E r \in Records : ApplyHint(r)
    \/ \E r \in Records, e \in Epochs : DeliverReassert(r, e)
    \/ \E r \in Records, e \in Epochs : DeliverRelease(r, e)

Spec == Init /\ [][Next]_vars

NoOversell == Cardinality({r \in Records : Owns(r)}) <= 1

\* A committed claim for a record that no longer owns V is a permanent wedge unless a release for it
\* is still in flight to clean it up.
ClaimBackedOrReleasing ==
    (claimCommitted /\ claimOwner # NULL /\ ~Owns(claimOwner))
        => (\E e \in Epochs : <<claimOwner, e>> \in releaseMsgs)

===========================================================================
