------------------- MODULE ClusterUniqueReconfigV5 -------------------
\* v5 models the CONCRETE, IMPLEMENTABLE fix and distinguishes it from a tempting-but-wrong variant.
\* The abstract joint-consensus cfg (v3/v4 JointConsensus=TRUE) proves "keep the reservation on a
\* majority of the current group" is safe, but hides HOW. Two realizations are possible in the code:
\*
\*   GATED (the correct fix): a newly-joined node is admitted to the unique quorum group -- counted in
\*   unique_majority() and eligible in the seal quorum -- ONLY after outstanding uncommitted reservations
\*   have been carried to it. Admission is atomic with the carry (Admit sets eff'=eff∪{m} together with
\*   holders'=holders∪{m}). The majority/seal denominator is the EFFECTIVE group `eff`, which lags the
\*   raw partition-map group `grp` until the carry completes. This is joint consensus for the DB_UNIQUE
\*   keyspace: the enlarged config is not active for unique quorum until the reservation transitions in.
\*
\*   UNGATED (the trap): re-replicate outstanding reservations to new members as a SEPARATE, best-effort
\*   action (EagerTransfer), while the raw group `grp` is used for majority/seal immediately on join.
\*   A seal can interleave between the join and the transfer -> the sealing majority of the enlarged
\*   group misses the holders -> oversell. This models "just re-replicate on membership change" without
\*   gating the effective group.
\*
\* Gated = TRUE  -> expect SAFE.
\* Gated = FALSE -> expect NoOversell VIOLATED (Add -> Seal -> EagerTransfer, the race the fix must close).
\*
\* Liveness is enforced as in v4: `up` (alive) set, Crash, and every quorum op gated on a live majority.

EXTENDS Naturals, FiniteSets

CONSTANTS Nodes, InitGroup, Gated

VARIABLES
    grp,          \* raw unique quorum group = partition_map.all_nodes() ∪ self
    eff,          \* effective unique group: nodes admitted (reservation-covered) for quorum purposes
    up,           \* alive nodes
    holders,      \* nodes durably holding the reservation
    reserved, committed, recordExists, sealed, learned, oversold

vars == << grp, eff, up, holders, reserved, committed, recordExists, sealed, learned, oversold >>

IsMaj(S, C) == Cardinality(S \cap C) * 2 > Cardinality(C)

\* The group used for the unique majority / seal denominator. GATED lags to the admitted set.
EffGroup == IF Gated THEN eff ELSE grp
Quorum == IsMaj(up, EffGroup)

TypeOK ==
    /\ grp \in SUBSET Nodes /\ eff \in SUBSET Nodes /\ up \in SUBSET Nodes /\ holders \in SUBSET Nodes
    /\ reserved \in BOOLEAN /\ committed \in BOOLEAN /\ recordExists \in BOOLEAN
    /\ sealed \in BOOLEAN /\ learned \in BOOLEAN /\ oversold \in BOOLEAN

Init ==
    /\ grp = InitGroup /\ eff = InitGroup /\ up = InitGroup /\ holders = {}
    /\ reserved = FALSE /\ committed = FALSE /\ recordExists = FALSE
    /\ sealed = FALSE /\ learned = FALSE /\ oversold = FALSE

Crash ==
    /\ \E n \in up : up' = up \ {n}
    /\ UNCHANGED << grp, eff, holders, reserved, committed, recordExists, sealed, learned, oversold >>

\* Reserve fans out to the effective group; durable at a live majority of it.
Reserve ==
    /\ ~reserved /\ Quorum
    /\ \E S \in SUBSET (up \cap EffGroup) : IsMaj(S, EffGroup) /\ holders' = S
    /\ reserved' = TRUE
    /\ UNCHANGED << grp, eff, up, committed, recordExists, sealed, learned, oversold >>

Commit ==
    /\ reserved /\ ~committed /\ Quorum
    /\ committed' = TRUE /\ recordExists' = TRUE
    /\ UNCHANGED << grp, eff, up, holders, reserved, sealed, learned, oversold >>

Reconcile ==
    /\ recordExists /\ Quorum
    /\ \E S \in SUBSET (up \cap EffGroup) : IsMaj(S, EffGroup) /\ holders' = holders \cup S
    /\ UNCHANGED << grp, eff, up, reserved, committed, recordExists, sealed, learned, oversold >>

\* A node joins the raw partition-map group (alive). Not yet admitted to the effective unique group.
AddNode ==
    /\ \E m \in Nodes \ grp : grp' = grp \cup {m} /\ up' = up \cup {m}
    /\ UNCHANGED << eff, holders, reserved, committed, recordExists, sealed, learned, oversold >>

\* GATED: admit a joined, live node into the effective group, ATOMICALLY carrying the reservation.
Admit ==
    /\ Gated
    /\ \E m \in (grp \cap up) \ eff :
         /\ eff' = eff \cup {m}
         /\ holders' = IF reserved THEN holders \cup {m} ELSE holders
    /\ UNCHANGED << grp, up, reserved, committed, recordExists, sealed, learned, oversold >>

\* UNGATED trap: re-replicate to a live majority of the raw group as a SEPARATE best-effort action.
EagerTransfer ==
    /\ ~Gated /\ reserved /\ Quorum
    /\ \E S \in SUBSET (up \cap grp) : IsMaj(S, grp) /\ holders' = holders \cup S
    /\ UNCHANGED << grp, eff, up, reserved, committed, recordExists, sealed, learned, oversold >>

\* Remove a DEAD node, gated on a live effective-majority; GATED also preserves reservation coverage.
RemoveNode ==
    /\ \E r \in grp :
         /\ r \notin up
         /\ IsMaj(up, EffGroup)
         /\ (Gated => (~reserved \/ IsMaj(holders \ {r}, eff \ {r})))
         /\ grp' = grp \ {r} /\ eff' = eff \ {r} /\ holders' = holders \ {r}
    /\ UNCHANGED << up, reserved, committed, recordExists, sealed, learned, oversold >>

SealMarkSealed ==
    /\ reserved /\ ~sealed /\ Quorum
    /\ \E p \in (up \cap EffGroup), SQ \in SUBSET (up \cap EffGroup) :
         /\ p \in SQ /\ IsMaj(SQ, EffGroup)
         /\ learned' = (SQ \cap holders # {})
    /\ sealed' = TRUE
    /\ UNCHANGED << grp, eff, up, holders, reserved, committed, recordExists, oversold >>

SealLearnLate ==
    /\ sealed /\ ~learned /\ (EffGroup \cap holders \cap up # {})
    /\ learned' = TRUE
    /\ UNCHANGED << grp, eff, up, holders, reserved, committed, recordExists, sealed, oversold >>

\* Genuinely-unbackstopped oversell = uncommitted only (committed has AlreadyCommitted/Conflict + reconciler).
SecondReserve ==
    /\ sealed /\ reserved /\ ~learned /\ ~committed /\ Quorum
    /\ oversold' = TRUE
    /\ UNCHANGED << grp, eff, up, holders, reserved, committed, recordExists, sealed, learned >>

Next ==
    \/ Crash \/ Reserve \/ Commit \/ Reconcile
    \/ AddNode \/ Admit \/ EagerTransfer \/ RemoveNode
    \/ SealMarkSealed \/ SealLearnLate \/ SecondReserve

Spec == Init /\ [][Next]_vars

NoOversell == ~oversold
NoUnrecoverableStrand == (reserved /\ (EffGroup \cap holders = {})) => recordExists

======================================================================
