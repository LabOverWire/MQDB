-------------------------------- MODULE GrantLifecycle --------------------------------
\* Verifies the level-merge and revocation rules of the diagram-sharing design
\* (docs/design/diagram-sharing.md) for a single (diagram, grantee) cell where the
\* grantee is a non-owner, non-admin, with no public grant -- so access == the
\* personal grant level.
\*   - direct /share  = set-to-level (may DOWNGRADE an existing grant)
\*   - cascade share  = max-of-levels (never downgrades; honors the request)
\*   - /unshare       = removes all access
\* History variables (prevLvl, lastAction, lastReq) turn these transition rules into
\* exhaustively-checkable state invariants.

EXTENDS Integers

VARIABLES lvl, prevLvl, lastAction, lastReq
vars == <<lvl, prevLvl, lastAction, lastReq>>

Rank(l)    == CASE l = "none" -> 0 [] l = "view" -> 1 [] l = "edit" -> 2
Geq(a, b)  == Rank(a) >= Rank(b)
Max2(a, b) == IF Rank(a) >= Rank(b) THEN a ELSE b

CanSee  == lvl \in {"view", "edit"}
CanEdit == lvl = "edit"

Init ==
    /\ lvl = "none"                        \* a grant cell starts empty
    /\ prevLvl = "none"
    /\ lastAction = "init"
    /\ lastReq = "none"

DirectShare(L) ==
    /\ L \in {"view", "edit"}
    /\ lvl' = L
    /\ prevLvl' = lvl
    /\ lastAction' = "direct"
    /\ lastReq' = L

CascadeShare(L) ==
    /\ L \in {"view", "edit"}
    /\ lvl' = Max2(lvl, L)
    /\ prevLvl' = lvl
    /\ lastAction' = "cascade"
    /\ lastReq' = L

Revoke ==
    /\ lvl' = "none"
    /\ prevLvl' = lvl
    /\ lastAction' = "revoke"
    /\ lastReq' = "none"

Next ==
    \/ \E L \in {"view", "edit"} : DirectShare(L)
    \/ \E L \in {"view", "edit"} : CascadeShare(L)
    \/ Revoke

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ lvl \in {"none", "view", "edit"}
    /\ prevLvl \in {"none", "view", "edit"}
    /\ lastAction \in {"init", "direct", "cascade", "revoke"}
    /\ lastReq \in {"none", "view", "edit"}

\* cascade never lowers an existing grant
InvCascadeNeverDowngrades == (lastAction = "cascade") => Geq(lvl, prevLvl)
\* cascade still reaches at least the requested level
InvCascadeMeetsRequest    == (lastAction = "cascade") => Geq(lvl, lastReq)
\* cascade is exactly max(previous, requested)
InvCascadeIsMax           == (lastAction = "cascade") => (lvl = Max2(prevLvl, lastReq))
\* direct share sets exactly the requested level (demotion allowed)
InvDirectSetsExact        == (lastAction = "direct") => (lvl = lastReq)
\* revocation removes all access
InvRevokeRemovesSee       == (lastAction = "revoke") => ~CanSee
InvRevokeRemovesEdit      == (lastAction = "revoke") => ~CanEdit

=============================================================================
