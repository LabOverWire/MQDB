------------------- MODULE ClusterUniqueReconfigV7 -------------------
\* v7 models the finding-1 residual that V5/V6 did NOT cover: a node's in-memory voter set can become
\* EMPTY (process restart / pre-adoption), and today `unique_voter_group()` falls back to the FULL
\* membership when the voter set is empty. If a promoting primary seals from that fallback while a
\* reservation is held on a smaller established (paced) voter set, the seal's majority of the larger
\* membership can miss the holders and oversell.
\*
\* The proposed cheap fix: fail the seal CLOSED on an empty voter set instead of falling back to full
\* membership — the promoting primary must first ADOPT the established voter set (arrives within one
\* heartbeat) before it can seal. This model checks both behaviours.
\*
\* FailClosed = FALSE  -> the shipped fallback. Expect NoOversell VIOLATED (restart -> seal over grown
\*                        membership misses the holders).
\* FailClosed = TRUE   -> the fix. Expect SAFE (seal is disabled while voters is empty; after adopting
\*                        the established set, a seal majority of it still intersects the holders).

EXTENDS Naturals, FiniteSets

CONSTANTS Nodes, InitMembers, FailClosed

VARIABLES
    member,      \* full partition-map membership (can grow)
    voters,      \* this node's in-memory unique-voter set (emptied by restart, restored by adopt)
    estab,       \* the established voter set a reservation was made a majority of (adopt restores this)
    holders,     \* reservation holders (a majority of estab)
    reserved, committed, sealed, learned, oversold

vars == << member, voters, estab, holders, reserved, committed, sealed, learned, oversold >>

IsMaj(S, C) == Cardinality(S \cap C) * 2 > Cardinality(C)

\* The denominator a seal computes its majority over. Empty voters -> fallback to full membership
\* (shipped) or nothing (the fix, which disables the seal = fail closed).
SealGroup == IF voters # {} THEN voters
             ELSE IF FailClosed THEN {} ELSE member

TypeOK ==
    /\ member \in SUBSET Nodes /\ voters \in SUBSET Nodes /\ estab \in SUBSET Nodes
    /\ holders \in SUBSET Nodes
    /\ reserved \in BOOLEAN /\ committed \in BOOLEAN
    /\ sealed \in BOOLEAN /\ learned \in BOOLEAN /\ oversold \in BOOLEAN

Init ==
    /\ member = InitMembers /\ voters = InitMembers /\ estab = InitMembers /\ holders = {}
    /\ reserved = FALSE /\ committed = FALSE
    /\ sealed = FALSE /\ learned = FALSE /\ oversold = FALSE

\* A reservation durable on a majority of the CURRENT (non-empty) voter set; remember that set as the
\* one the reservation is a majority of.
Reserve ==
    /\ ~reserved /\ voters # {}
    /\ \E S \in SUBSET voters : IsMaj(S, voters) /\ holders' = S /\ estab' = voters
    /\ reserved' = TRUE
    /\ UNCHANGED << member, voters, committed, sealed, learned, oversold >>

Commit ==
    /\ reserved /\ ~committed
    /\ committed' = TRUE
    /\ UNCHANGED << member, voters, estab, holders, reserved, sealed, learned, oversold >>

\* Cluster grows: a new node joins the membership but is not yet a voter (the paced lag). This makes
\* the empty-voters fallback (= member) strictly larger than the established voter set.
Grow ==
    /\ \E m \in Nodes \ member : member' = member \cup {m}
    /\ UNCHANGED << voters, estab, holders, reserved, committed, sealed, learned, oversold >>

\* A node loses its in-memory voter set (process restart / before adopting the gossiped set).
Restart ==
    /\ voters # {}
    /\ voters' = {}
    /\ UNCHANGED << member, estab, holders, reserved, committed, sealed, learned, oversold >>

\* The node adopts the established voter set from the leader's gossip (arrives within a heartbeat).
Adopt ==
    /\ voters = {}
    /\ voters' = estab
    /\ UNCHANGED << member, estab, holders, reserved, committed, sealed, learned, oversold >>

\* A promoting primary seals over a majority of the seal group and learns iff it intersects holders.
\* When FailClosed and voters is empty, SealGroup = {} so no majority exists -> the seal cannot fire.
Seal ==
    /\ reserved /\ ~sealed
    /\ SealGroup # {}
    /\ \E SQ \in SUBSET SealGroup : IsMaj(SQ, SealGroup) /\ learned' = (SQ \cap holders # {})
    /\ sealed' = TRUE
    /\ UNCHANGED << member, voters, estab, holders, reserved, committed, oversold >>

SealLearnLate ==
    /\ sealed /\ ~learned /\ (SealGroup \cap holders # {})
    /\ learned' = TRUE
    /\ UNCHANGED << member, voters, estab, holders, reserved, committed, sealed, oversold >>

SecondReserve ==
    /\ sealed /\ reserved /\ ~learned /\ ~committed
    /\ oversold' = TRUE
    /\ UNCHANGED << member, voters, estab, holders, reserved, committed, sealed, learned >>

Next ==
    \/ Reserve \/ Commit \/ Grow \/ Restart \/ Adopt
    \/ Seal \/ SealLearnLate \/ SecondReserve

Spec == Init /\ [][Next]_vars

NoOversell == ~oversold

======================================================================
