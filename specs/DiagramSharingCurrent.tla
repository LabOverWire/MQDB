----------------------------- MODULE DiagramSharingCurrent -----------------------------
\* The CURRENT (pre-feature) behavior, modeled to demonstrate that the security
\* invariants in DiagramSharing.tla have teeth: today's behavior VIOLATES them.
\* Verified facts encoded here:
\*   - diagrams: read/update/delete gated to owner (transport_execute.rs:72-129)
\*   - child entities: NO access control at all (types.rs:144-155) -> see/edit = TRUE
\*   - events: broadcast to every authenticated subscriber (dispatcher.rs:72-82)
\* Expectation: InvEventConfidentiality and the child invariants FAIL.

EXTENDS Integers, FiniteSets

CONSTANTS u1, u2, adm, d1, d2, c1

Users    == {u1, u2}
Admins   == {adm}
Diagrams == {d1, d2}
Children == {c1}
Parent   == (c1 :> d1)

Resources == Diagrams \cup Children
Readers   == Users \cup Admins

VARIABLES owner
vars == <<owner>>

IsAdmin(u) == u \in Admins

SeeDiagram(u, d)  == IsAdmin(u) \/ u = owner[d]
EditDiagram(u, d) == IsAdmin(u) \/ u = owner[d]

\* children are ungated today
CanSee(u, x)  == IF x \in Diagrams THEN SeeDiagram(u, x)  ELSE TRUE
CanEdit(u, x) == IF x \in Diagrams THEN EditDiagram(u, x) ELSE TRUE

\* events broadcast to every authenticated principal
Delivers(u, x) == u \in (Users \cup Admins)

Init == owner \in [Diagrams -> Users]

Transfer(d, u) ==
    /\ owner' = [owner EXCEPT ![d] = u]

Next == \E d \in Diagrams, u \in Users : Transfer(d, u)

Spec == Init /\ [][Next]_vars

TypeOK == owner \in [Diagrams -> Users]

\* Same property as the proposed spec: receiving an event implies being able to read.
InvEventConfidentiality ==
    \A u \in Readers, x \in Resources : Delivers(u, x) => CanSee(u, x)

InvChildNeedsParentSee ==
    \A u \in Readers, x \in Children : CanSee(u, x) => CanSee(u, Parent[x])

InvChildNeedsParentEdit ==
    \A u \in Readers, x \in Children : CanEdit(u, x) => CanEdit(u, Parent[x])

=============================================================================
