------------------- MODULE ClusterUniqueReconfigV6 -------------------
\* v6 models the ACTUAL SHIPPED mechanism, which differs from v5's abstract atomic-carry Admit.
\* The implementation does NOT re-replicate a reservation when it promotes/removes a voter. Instead it
\* relies on TWO facts proven here:
\*   (1) SINGLE-STEP SAFETY: a reservation is durable on a majority of the voter set at reserve time;
\*       changing the voter set by ONE node (add or remove) still leaves every seal-majority of the new
\*       set intersecting the holders (a majority of the old set), so the seal still learns it.
\*   (2) PACING: the leader changes the voter set at most one node per interval, and the interval
\*       exceeds the reserve->commit timeout, so an UNCOMMITTED reservation's window overlaps AT MOST
\*       ONE voter change. (Committed reservations are recovered by the record-driven reconciler.)
\*
\* So the safety question: with NO carry, is NoOversell preserved when at most ONE voter change occurs
\* between a reserve and its seal? And is it VIOLATED if TWO changes are allowed (pacing removed)?
\*
\* Paced = TRUE  -> at most one voter change while uncommitted. Expect SAFE.
\* Paced = FALSE -> unbounded voter changes while uncommitted. Expect NoOversell VIOLATED (proves pacing
\*                  is load-bearing, not decorative).

EXTENDS Naturals, FiniteSets

CONSTANTS Nodes, InitVoters, Paced

VARIABLES
    eff,          \* the current unique-voter set (majority/seal denominator)
    holders,      \* nodes durably holding the reservation (majority of eff AT RESERVE TIME)
    reserved, committed, sealed, learned, oversold,
    changes       \* voter changes since the reservation was taken (while uncommitted)

vars == << eff, holders, reserved, committed, sealed, learned, oversold, changes >>

IsMaj(S, C) == Cardinality(S \cap C) * 2 > Cardinality(C)

\* A voter change is admissible now. Under pacing, at most ONE change may occur while a reservation is
\* uncommitted; once committed (or absent) changes are unconstrained.
MayChange == (~Paced) \/ (~reserved) \/ committed \/ (changes = 0)
BumpChanges == IF reserved /\ ~committed THEN changes + 1 ELSE changes

TypeOK ==
    /\ eff \in SUBSET Nodes /\ holders \in SUBSET Nodes
    /\ reserved \in BOOLEAN /\ committed \in BOOLEAN
    /\ sealed \in BOOLEAN /\ learned \in BOOLEAN /\ oversold \in BOOLEAN
    /\ changes \in Nat

Init ==
    /\ eff = InitVoters /\ holders = {}
    /\ reserved = FALSE /\ committed = FALSE
    /\ sealed = FALSE /\ learned = FALSE /\ oversold = FALSE
    /\ changes = 0

\* Reserve is durable on a majority of the CURRENT voter set.
Reserve ==
    /\ ~reserved
    /\ \E S \in SUBSET eff : IsMaj(S, eff) /\ holders' = S
    /\ reserved' = TRUE /\ changes' = 0
    /\ UNCHANGED << eff, committed, sealed, learned, oversold >>

Commit ==
    /\ reserved /\ ~committed
    /\ committed' = TRUE
    /\ UNCHANGED << eff, holders, reserved, sealed, learned, oversold, changes >>

\* Promote one voter WITHOUT carrying the reservation (holders unchanged) — the shipped behaviour.
AddOneVoter ==
    /\ MayChange
    /\ \E m \in Nodes \ eff :
         /\ eff' = eff \cup {m}
    /\ changes' = BumpChanges
    /\ UNCHANGED << holders, reserved, committed, sealed, learned, oversold >>

\* Remove one voter (a departed node); the reservation is not re-replicated first.
RemoveOneVoter ==
    /\ MayChange
    /\ \E r \in eff :
         /\ Cardinality(eff) > 1
         /\ eff' = eff \ {r}
         /\ holders' = holders \ {r}
    /\ changes' = BumpChanges
    /\ UNCHANGED << reserved, committed, sealed, learned, oversold >>

\* A promoting primary seals against a majority of the current voter set and learns iff it intersects.
SealMarkSealed ==
    /\ reserved /\ ~sealed
    /\ \E p \in eff, SQ \in SUBSET eff :
         /\ p \in SQ /\ IsMaj(SQ, eff)
         /\ learned' = (SQ \cap holders # {})
    /\ sealed' = TRUE
    /\ UNCHANGED << eff, holders, reserved, committed, oversold, changes >>

SealLearnLate ==
    /\ sealed /\ ~learned /\ (eff \cap holders # {})
    /\ learned' = TRUE
    /\ UNCHANGED << eff, holders, reserved, committed, sealed, oversold, changes >>

SecondReserve ==
    /\ sealed /\ reserved /\ ~learned /\ ~committed
    /\ oversold' = TRUE
    /\ UNCHANGED << eff, holders, reserved, committed, sealed, learned, changes >>

Next ==
    \/ Reserve \/ Commit \/ AddOneVoter \/ RemoveOneVoter
    \/ SealMarkSealed \/ SealLearnLate \/ SecondReserve

Spec == Init /\ [][Next]_vars

\* Keep the state space finite: bound the change counter (the unpaced case would otherwise grow it
\* without limit). Three is well past the one-change boundary that pacing enforces.
ChangeBound == changes <= 3

NoOversell == ~oversold

======================================================================
