-------------------------------- MODULE CascadeClosure --------------------------------
EXTENDS Integers, FiniteSets

CONSTANTS d1, d2, d3
Diagrams == {d1, d2, d3}
Root     == d1

VARIABLES refs, shareable, visited, frontier, skipped
vars == <<refs, shareable, visited, frontier, skipped>>

RECURSIVE ReachSet(_)
ReachSet(S) ==
    LET S2 == S \cup {y \in shareable : \E x \in S : <<x, y>> \in refs}
    IN IF S2 = S THEN S ELSE ReachSet(S2)

Reachable == ReachSet({Root})

ForeignNeighbours == {y \in Diagrams \ shareable : \E x \in Reachable : <<x, y>> \in refs}

Init ==
    /\ refs \in SUBSET (Diagrams \X Diagrams)
    /\ shareable \in {S \in SUBSET Diagrams : Root \in S}
    /\ visited = {}
    /\ frontier = {Root}
    /\ skipped = {}

Step ==
    /\ frontier # {}
    /\ \E d \in frontier :
        LET nv   == visited \cup {d}
            nbrs == {y \in Diagrams : <<d, y>> \in refs}
        IN /\ visited'  = nv
           /\ frontier' = (frontier \ {d}) \cup ((nbrs \cap shareable) \ nv)
           /\ skipped'  = skipped \cup (nbrs \ shareable)
           /\ UNCHANGED <<refs, shareable>>

Next == Step
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ refs \subseteq (Diagrams \X Diagrams)
    /\ shareable \subseteq Diagrams
    /\ visited \subseteq Diagrams
    /\ frontier \subseteq Diagrams
    /\ skipped \subseteq Diagrams

InvGrantsOnlyShareable == visited \subseteq shareable
InvCascadeSound == visited \subseteq Reachable
InvFrontierSound == frontier \subseteq Reachable
InvSkippedNeverGranted == skipped \cap visited = {}
InvCascadeComplete == (frontier = {}) => (visited = Reachable)
InvSkippedComplete == (frontier = {}) => (skipped = ForeignNeighbours)

=============================================================================
