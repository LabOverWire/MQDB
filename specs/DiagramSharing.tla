-------------------------------- MODULE DiagramSharing --------------------------------
\* Authorization core of the diagram-sharing design (docs/design/diagram-sharing.md).
\* Verifies the PROPOSED design: named view/edit grants + public grants + pending
\* grants + child-entity derivation + recipient-scoped event routing. The parent
\* mapping is a VARIABLE here and may be reparented arbitrarily, to test whether
\* derived child access can leak when a child moves between diagrams.
\*
\* Threat model: authenticated users are mutually untrusting tenants.
\* Assumption NOT modeled (taken as given): mqtt-lib's %u ACL correctly isolates
\* per-user event topics, so a user only receives events published to their own
\* namespace (or the public namespace). Under that assumption "u receives event for
\* x" == "u in Recipients(x) OR x is public" == Delivers(u, x) below.

EXTENDS Integers, FiniteSets

CONSTANTS u1, u2, adm, d1, d2, c1, PUBLIC, PENDING, ANON

Users    == {u1, u2}            \* regular principals: can own and be grantees
Admins   == {adm}               \* bypass everything
Diagrams == {d1, d2}            \* ownership-enabled resources
Children == {c1}                \* derive permission from a parent diagram
\* parent is a VARIABLE (see below); initial mapping c1 |-> d1 set in Init

Resources    == Diagrams \cup Children
Readers      == Users \cup Admins \cup {ANON}
GranteeSpace == Users \cup {PUBLIC, PENDING}   \* who a grant may name
Levels       == {"view", "edit"}
LevelOrNone  == {"none", "view", "edit"}
GrantKeys    == Diagrams \X GranteeSpace

VARIABLES owner, grants, parent
vars == <<owner, grants, parent>>

GrantOf(d, g) == IF <<d, g>> \in DOMAIN grants THEN grants[<<d, g>>] ELSE "none"
IsAdmin(u)    == u \in Admins
DiagramOf(x)  == IF x \in Diagrams THEN x ELSE parent[x]

\* ---- permission predicates on a diagram ----
SeeDiagram(u, d) ==
    \/ IsAdmin(u)
    \/ u = owner[d]
    \/ GrantOf(d, u) \in {"view", "edit"}
    \/ GrantOf(d, PUBLIC) \in {"view", "edit"}

EditDiagram(u, d) ==
    \/ IsAdmin(u)
    \/ u = owner[d]
    \/ GrantOf(d, u) = "edit"
    \/ GrantOf(d, PUBLIC) = "edit"

\* ---- the resolver: children derive from their parent diagram ----
CanSee(u, x)  == SeeDiagram(u, DiagramOf(x))
CanEdit(u, x) == EditDiagram(u, DiagramOf(x))

\* delete: a diagram is owner/admin only; a child follows edit-of-parent
CanDelete(u, x) ==
    IF x \in Diagrams
    THEN IsAdmin(u) \/ u = owner[x]
    ELSE CanEdit(u, x)

\* ---- event recipients (who the emitter publishes to) ----
DiagramRecipients(d) ==
    {owner[d]}
    \cup {u \in Users : GrantOf(d, u) \in {"view", "edit"}}
    \cup (IF GrantOf(d, PUBLIC) \in {"view", "edit"} THEN {PUBLIC} ELSE {})

Recipients(x) == DiagramRecipients(DiagramOf(x))

\* a reader receives the event iff it landed in their own namespace (membership)
\* or in the public namespace (PUBLIC in the recipient set => published publicly)
Delivers(u, x) == (u \in Recipients(x)) \/ (PUBLIC \in Recipients(x))

\* ---- state machine ----
Init ==
    /\ owner \in [Diagrams -> Users]
    /\ grants = [k \in GrantKeys |-> "none"]
    /\ parent = (c1 :> d1)

SetGrant(d, g, l) ==
    /\ grants' = [grants EXCEPT ![<<d, g>>] = l]
    /\ UNCHANGED <<owner, parent>>

ClearGrant(d, g) ==
    /\ grants' = [grants EXCEPT ![<<d, g>>] = "none"]
    /\ UNCHANGED <<owner, parent>>

\* ownership transfer changes only the owner field; grants are preserved
Transfer(d, u) ==
    /\ owner' = [owner EXCEPT ![d] = u]
    /\ UNCHANGED <<grants, parent>>

\* A child is reparented to a different diagram. Modeled UNRESTRICTED (any child,
\* any target) to test whether derived child access can leak when the parent moves.
\* The agent forbids this on update (the parent reference is immutable), so this is
\* the permissive upper bound the implementation refines.
Reparent(c, dNew) ==
    /\ parent' = [parent EXCEPT ![c] = dNew]
    /\ UNCHANGED <<owner, grants>>

Next ==
    \/ \E d \in Diagrams, g \in GranteeSpace, l \in Levels : SetGrant(d, g, l)
    \/ \E d \in Diagrams, g \in GranteeSpace : ClearGrant(d, g)
    \/ \E d \in Diagrams, u \in Users : Transfer(d, u)
    \/ \E c \in Children, dNew \in Diagrams : Reparent(c, dNew)

Spec == Init /\ [][Next]_vars

\* ===================== invariants =====================

TypeOK ==
    /\ owner \in [Diagrams -> Users]
    /\ grants \in [GrantKeys -> LevelOrNone]
    /\ parent \in [Children -> Diagrams]

\* No over-delivery: anyone who receives an event for x can also read x.
\* Ties the recipient-scoped routing to the CRUD read predicate.
InvEventConfidentiality ==
    \A u \in Readers, x \in Resources : Delivers(u, x) => CanSee(u, x)

\* No under-delivery: every authorized non-admin reader receives the event.
\* (Admins read on demand; they are not on the event feed by design.)
InvEventCompleteness ==
    \A u \in Users, x \in Resources : (CanSee(u, x) /\ ~IsAdmin(u)) => Delivers(u, x)

\* Edit is at least as strong as view.
InvEditImpliesSee ==
    \A u \in Readers, x \in Resources : CanEdit(u, x) => CanSee(u, x)

\* Delete of a diagram is strictly owner/admin -- never via an edit/public grant.
InvDeleteOwnerOnly ==
    \A u \in Readers, d \in Diagrams : CanDelete(u, d) => (IsAdmin(u) \/ u = owner[d])

\* A child can never be seen/edited by someone who can't see/edit its parent.
InvChildNeedsParentSee ==
    \A u \in Readers, x \in Children : CanSee(u, x) => CanSee(u, parent[x])
InvChildNeedsParentEdit ==
    \A u \in Readers, x \in Children : CanEdit(u, x) => CanEdit(u, parent[x])

\* A view-only grantee (no other relationship, no public-edit) cannot edit a child.
\* This is the "view is a lie" property the design must defeat.
InvViewGranteeCannotEditChild ==
    \A u \in Users, x \in Children :
        ( u # owner[parent[x]]
          /\ ~IsAdmin(u)
          /\ GrantOf(parent[x], u) = "view"
          /\ GrantOf(parent[x], PUBLIC) # "edit" )
        => ~CanEdit(u, x)

\* Anonymous callers reach only public resources, and never edit except public-edit.
InvAnonConfinedSee ==
    \A x \in Resources : CanSee(ANON, x) => (GrantOf(DiagramOf(x), PUBLIC) \in {"view", "edit"})
InvAnonConfinedEdit ==
    \A x \in Resources : CanEdit(ANON, x) => (GrantOf(DiagramOf(x), PUBLIC) = "edit")

\* Completeness for anonymous viewers: a public resource's events reach anonymous.
InvAnonPublicReceives ==
    \A x \in Resources :
        (GrantOf(DiagramOf(x), PUBLIC) \in {"view", "edit"}) => Delivers(ANON, x)

\* A pending grant (grantee = PENDING) is inert: a user with no personal/public
\* grant and not owner/admin cannot see the diagram even if a PENDING grant exists.
InvPendingNoAccess ==
    \A u \in Readers, d \in Diagrams :
        ( ~IsAdmin(u) /\ u # owner[d]
          /\ GrantOf(d, u) = "none"
          /\ GrantOf(d, PUBLIC) = "none" )
        => ~CanSee(u, d)

\* Reserved sentinels are never principals that own resources.
InvReservedNotOwner ==
    \A d \in Diagrams : owner[d] \notin {PUBLIC, PENDING, ANON}

=============================================================================
