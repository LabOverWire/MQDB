------------------- MODULE ClusterUniqueReconfigV4 -------------------
\* v4 closes v3's liveness gap: v3's RemoveNode could drop a majority of the config directly, which
\* raft forbids (you cannot commit a config change, elect a primary, or seal without a live majority).
\* v4 tracks the ALIVE set `up`, models Crash, and gates every quorum operation (RemoveNode, Reserve,
\* Seal, Reconcile, Commit) on a live majority of the current config. Question: does the strand/oversell
\* survive when the holder-majority can leave only via the LEGAL grow-then-shed path?
\*
\* A node "leaves the group" physically = it Crashes (leaves `up`), then a live-majority rebalance
\* Removes it from the config (`grp`). Removing a dead node needs a live majority of the current config,
\* so a majority cannot vanish directly -- the config must GROW first, then shed the old majority.
\*
\* JointConsensus = FALSE (today) / TRUE (fix: keep the reservation on a majority of the current group).

EXTENDS Naturals, FiniteSets

CONSTANTS Nodes, InitGroup, JointConsensus

VARIABLES
    grp, up, holders, reserved, committed, recordExists, sealed, learned, oversold

vars == << grp, up, holders, reserved, committed, recordExists, sealed, learned, oversold >>

IsMaj(S, C) == Cardinality(S \cap C) * 2 > Cardinality(C)
Quorum == IsMaj(up, grp)         \* a live majority of the current config exists
Durable == (~reserved) \/ IsMaj(holders, grp)

TypeOK ==
    /\ grp \in SUBSET Nodes /\ up \in SUBSET Nodes /\ holders \in SUBSET Nodes
    /\ reserved \in BOOLEAN /\ committed \in BOOLEAN /\ recordExists \in BOOLEAN
    /\ sealed \in BOOLEAN /\ learned \in BOOLEAN /\ oversold \in BOOLEAN

Init ==
    /\ grp = InitGroup /\ up = InitGroup /\ holders = {}
    /\ reserved = FALSE /\ committed = FALSE /\ recordExists = FALSE
    /\ sealed = FALSE /\ learned = FALSE /\ oversold = FALSE

\* A node goes down (still in the config until a rebalance removes it).
Crash ==
    /\ \E n \in up : up' = up \ {n}
    /\ UNCHANGED << grp, holders, reserved, committed, recordExists, sealed, learned, oversold >>

\* Reserve: acked by a live majority; holders are those (live) ackers.
Reserve ==
    /\ ~reserved /\ Quorum
    /\ \E S \in SUBSET up : IsMaj(S, grp) /\ holders' = S
    /\ reserved' = TRUE
    /\ UNCHANGED << grp, up, committed, recordExists, sealed, learned, oversold >>

Commit ==
    /\ reserved /\ ~committed /\ Quorum
    /\ committed' = TRUE /\ recordExists' = TRUE
    /\ UNCHANGED << grp, up, holders, reserved, sealed, learned, oversold >>

\* The ~10s reconciler reasserts a committed claim (from the durable record) to a live majority.
Reconcile ==
    /\ recordExists /\ Quorum
    /\ \E S \in SUBSET up : IsMaj(S, grp) /\ holders' = holders \cup S
    /\ UNCHANGED << grp, up, reserved, committed, recordExists, sealed, learned, oversold >>

AddWithData ==
    /\ \E m \in Nodes \ grp :
         /\ (reserved => holders # {})
         /\ grp' = grp \cup {m} /\ up' = up \cup {m}
         /\ holders' = IF reserved THEN holders \cup {m} ELSE holders
    /\ UNCHANGED << reserved, committed, recordExists, sealed, learned, oversold >>

AddNoData ==
    /\ \E m \in Nodes \ grp :
         /\ (JointConsensus => (~reserved \/ IsMaj(holders, grp \cup {m})))
         /\ grp' = grp \cup {m} /\ up' = up \cup {m}
    /\ UNCHANGED << holders, reserved, committed, recordExists, sealed, learned, oversold >>

\* Rebalance a DEAD node out of the config -- requires a live majority of the current config to commit.
RemoveNode ==
    /\ \E r \in grp :
         /\ r \notin up
         /\ IsMaj(up, grp)
         /\ (JointConsensus => (~reserved \/ IsMaj(holders \ {r}, grp \ {r})))
         /\ grp' = grp \ {r} /\ holders' = holders \ {r}
    /\ UNCHANGED << up, reserved, committed, recordExists, sealed, learned, oversold >>

Transfer ==
    /\ JointConsensus /\ reserved /\ Quorum
    /\ \E S \in SUBSET up : IsMaj(S, grp) /\ holders' = holders \cup S
    /\ UNCHANGED << grp, up, reserved, committed, recordExists, sealed, learned, oversold >>

\* Seal requires a live majority (to elect and seal); the sealing majority is of live responders.
SealMarkSealed ==
    /\ reserved /\ ~sealed /\ Quorum
    /\ \E p \in up, SQ \in SUBSET up :
         /\ p \in SQ /\ IsMaj(SQ, grp)
         /\ learned' = (SQ \cap holders # {})
    /\ sealed' = TRUE
    /\ UNCHANGED << grp, up, holders, reserved, committed, recordExists, oversold >>

\* A late response from a live in-group holder merges.
SealLearnLate ==
    /\ sealed /\ ~learned /\ (grp \cap holders \cap up # {})
    /\ learned' = TRUE
    /\ UNCHANGED << grp, up, holders, reserved, committed, recordExists, sealed, oversold >>

\* Fidelity (audit): the oversold flag is gated on ~committed. A COMMITTED first claim has a
\* commit-time backstop the code enforces but this model omits (apply_replicated -> AlreadyCommitted,
\* reassert -> Conflict, unique_store.rs), and the ~10s record-driven reconciler re-establishes it.
\* The genuinely UNBACKSTOPPED oversell is exactly the uncommitted-reservation window.
SecondReserve ==
    /\ sealed /\ reserved /\ ~learned /\ ~committed /\ Quorum
    /\ oversold' = TRUE
    /\ UNCHANGED << grp, up, holders, reserved, committed, recordExists, sealed, learned >>

Next ==
    \/ Crash \/ Reserve \/ Commit \/ Reconcile
    \/ AddWithData \/ AddNoData \/ RemoveNode \/ Transfer
    \/ SealMarkSealed \/ SealLearnLate \/ SecondReserve

Spec == Init /\ [][Next]_vars

NoOversell == ~oversold
\* A reservation whose holders are all gone from the config must be recoverable from a durable record.
NoUnrecoverableStrand == (reserved /\ (grp \cap holders = {})) => recordExists

======================================================================
