-------------------------------- MODULE CascadeClosure --------------------------------
\* Verifies the transitive-share cascade (docs/design/diagram-sharing.md):
\* sharing a root diagram must grant exactly the diagrams reachable from it via
\* diagram->diagram references -- no more (over-sharing = leak), no less
\* (under-sharing = broken functionality) -- and must terminate on cyclic graphs.
\*
\* The BFS (visited/frontier, visited-guarded) is checked against an independent
\* fixpoint reachability, over EVERY possible reference graph on 3 diagrams.

EXTENDS Integers, FiniteSets

CONSTANTS d1, d2, d3
Diagrams == {d1, d2, d3}
Root     == d1

VARIABLES refs, visited, frontier
vars == <<refs, visited, frontier>>

\* independent definition of reachability: least fixpoint of the neighbour step
RECURSIVE ReachSet(_)
ReachSet(S) ==
    LET S2 == S \cup {y \in Diagrams : \E x \in S : <<x, y>> \in refs}
    IN IF S2 = S THEN S ELSE ReachSet(S2)

Reachable == ReachSet({Root})

Init ==
    /\ refs \in SUBSET (Diagrams \X Diagrams)   \* every graph, incl. cycles & self-loops
    /\ visited = {}
    /\ frontier = {Root}

\* visit one frontier node; enqueue its not-yet-visited neighbours (cycle-safe)
Step ==
    /\ frontier # {}
    /\ \E d \in frontier :
        LET nv   == visited \cup {d}
            nbrs == {y \in Diagrams : <<d, y>> \in refs}
        IN /\ visited'  = nv
           /\ frontier' = (frontier \ {d}) \cup (nbrs \ nv)
           /\ UNCHANGED refs

Next == Step
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ refs \subseteq (Diagrams \X Diagrams)
    /\ visited \subseteq Diagrams
    /\ frontier \subseteq Diagrams

\* never grant outside the reference closure (no over-sharing / leak)
InvCascadeSound == visited \subseteq Reachable
\* frontier stays within the closure too
InvFrontierSound == frontier \subseteq Reachable
\* on termination, granted set equals the closure exactly (no under-sharing)
InvCascadeComplete == (frontier = {}) => (visited = Reachable)

=============================================================================
