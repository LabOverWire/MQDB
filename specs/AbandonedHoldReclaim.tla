---- MODULE AbandonedHoldReclaim ----
(***************************************************************************)
(* Sub-second on-disconnect reclaim of an abandoned exclusive hold         *)
(* (ticketing seat holds), app-side-janitor architecture.                  *)
(*                                                                          *)
(* One seat. A hold is held by a buyer via a keep-alive connection. When    *)
(* that connection drops, a janitor (after a grace delay, abstracted as     *)
(* "may happen any time after disconnect") reaps the hold so the seat is    *)
(* reclaimable. The danger is a FALSE RELEASE: reaping a holder who has in   *)
(* fact come back. Oversell (two live holds) is inherited-safe: reclaim is   *)
(* release-then-create through the existing unique fence (Reclaim fires      *)
(* only when the seat is free), so the novel property to check is           *)
(* NoFalseRelease.                                                          *)
(*                                                                          *)
(* The janitor's CAS is modeled by a token: Observe takes a fresh snapshot   *)
(* (reassertedSinceObs := FALSE) and a reassert write flips it TRUE, so the  *)
(* CAS "hold unchanged since I looked" holds iff ~reassertedSinceObs -- the  *)
(* finite, artifact-free stand-in for an unbounded version compare.          *)
(*                                                                          *)
(* Two knobs model the design decisions:                                    *)
(*   Guarded            = janitor reaps with a CAS (reap only if the hold     *)
(*                        was not re-asserted since it observed).            *)
(*   ReconnectReasserts = a returning holder re-asserts its hold (a write     *)
(*                        that would fail the janitor's CAS) on reconnect.   *)
(*                                                                          *)
(* Expected: Guarded /\ ReconnectReasserts = SAFE; dropping either one       *)
(* reintroduces false release -- BOTH the CAS fence and the                 *)
(* reassert-on-reconnect contract are necessary.                            *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS Guarded, ReconnectReasserts, Buyers

VARIABLES holder, present, obsPending, reassertedSinceObs, falseRelease

vars == <<holder, present, obsPending, reassertedSinceObs, falseRelease>>

None == "none"

TypeOK ==
    /\ holder \in (Buyers \cup {None})
    /\ present \in BOOLEAN
    /\ obsPending \in BOOLEAN
    /\ reassertedSinceObs \in BOOLEAN
    /\ falseRelease \in BOOLEAN

Init ==
    /\ holder \in Buyers
    /\ present = TRUE
    /\ obsPending = FALSE
    /\ reassertedSinceObs = FALSE
    /\ falseRelease = FALSE

\* The holder's keep-alive connection drops.
Disconnect ==
    /\ holder # None
    /\ present = TRUE
    /\ present' = FALSE
    /\ UNCHANGED <<holder, obsPending, reassertedSinceObs, falseRelease>>

\* The janitor reacts to the disconnect event (after its grace delay) and
\* snapshots the hold to CAS on later.
Observe ==
    /\ holder # None
    /\ present = FALSE
    /\ ~obsPending
    /\ obsPending' = TRUE
    /\ reassertedSinceObs' = FALSE
    /\ UNCHANGED <<holder, present, falseRelease>>

\* The holder comes back. Under the janitor contract it re-asserts its hold (a
\* write that would fail the janitor's pending CAS); without that contract it
\* only restores presence.
Reconnect ==
    /\ holder # None
    /\ present = FALSE
    /\ present' = TRUE
    /\ reassertedSinceObs' = IF ReconnectReasserts THEN TRUE ELSE reassertedSinceObs
    /\ UNCHANGED <<holder, obsPending, falseRelease>>

\* The janitor's grace expired: reap. Guarded => only if the hold was not
\* re-asserted since it observed (CAS). Reaping a present holder is a false
\* release.
ReapCAS ==
    /\ obsPending
    /\ LET reap == (holder # None) /\ (~Guarded \/ ~reassertedSinceObs)
       IN /\ holder' = IF reap THEN None ELSE holder
          /\ falseRelease' = IF reap /\ present THEN TRUE ELSE falseRelease
    /\ obsPending' = FALSE
    /\ reassertedSinceObs' = FALSE
    /\ UNCHANGED present

\* A new buyer reclaims the freed seat. The unique fence permits this only
\* when the seat is free, so at most one live hold ever exists.
Reclaim(b) ==
    /\ holder = None
    /\ holder' = b
    /\ present' = TRUE
    /\ obsPending' = FALSE
    /\ reassertedSinceObs' = FALSE
    /\ UNCHANGED falseRelease

Next ==
    \/ Disconnect
    \/ Observe
    \/ Reconnect
    \/ ReapCAS
    \/ \E b \in Buyers : Reclaim(b)

Spec == Init /\ [][Next]_vars

InvNoFalseRelease == ~falseRelease
====
