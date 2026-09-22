---------------------------- MODULE ClusterRedial ----------------------------
(***************************************************************************)
(* Design model for the redial half of gh #146 (configured-peers scope),   *)
(* baseline main @ 8945b67 (after #145 framing fix, #147 two-lane writer).  *)
(*                                                                          *)
(* WHAT IS MODELLED (see scratchpad/redial-design-facts.md for the code     *)
(* facts each abstraction is grounded in):                                  *)
(*   * peers map slot per DIRECTED pair a->b: Absent | Live(gen) | Broken   *)
(*     (gen). A dial opens ONE bidirectional stream, so Connect(a,b) sets    *)
(*     BOTH a's outbound slot[a][b] and the accept side slot[b][a], each     *)
(*     UNCONDITIONALLY replacing whatever was there (HashMap::insert         *)
(*     replaces, dropping the displaced connection).                        *)
(*   * removal is driven by SEND-side death (writer write_all error), which  *)
(*     emits an in-flight death notice carrying the connection's gen; it is  *)
(*     NOT triggered by receiver-exit and NOT by clean replacement.          *)
(*   * removal is a compare-and-remove keyed on the gen id (Guarded); the    *)
(*     id is monotonic (MonotonicId) or reusable (models quinn stable_id).   *)
(*   * redial: Connect fires when slot[a][b] is not Live, i.e. absent or     *)
(*     broken (the impl's mesh tick re-dials configured members the          *)
(*     heartbeat view reports as not alive, driven by liveness rather than   *)
(*     the transport peer map; connect_to_peer replaces any stale entry).    *)
(*                                                                          *)
(* The three configs map to gh #140's surviving design rules:               *)
(*   ClusterRedial.cfg  Guarded=TRUE  MonotonicId=TRUE  -> safety holds +    *)
(*                      broken links re-converge (liveness).                 *)
(*   *_unguarded.cfg    Guarded=FALSE                    -> InvNoLiveDrop     *)
(*                      violated (by-key removal deletes the newer entry).   *)
(*   *_reusable.cfg     Guarded=TRUE  MonotonicId=FALSE  -> InvNoLiveDrop     *)
(*                      violated (a stale notice matches a reused id).       *)
(*   *_noredial.cfg     Redial=FALSE  -> Converge violated (removal without  *)
(*                      re-dial leaves a slot permanently absent).           *)
(*                                                                          *)
(* PREMISE, NOT VERIFIED: that SEND-death (not receiver-exit) is the right   *)
(* removal trigger is BAKED IN (SendBreak is the only pd producer; there is  *)
(* no receiver-exit action). It is grounded in the code + the #140 audit,    *)
(* but this model assumes it rather than proving it. The guard / monotonic   *)
(* id / redial rules ARE demonstrated as checker output above.               *)
(* IMPLEMENTATION PROOF-OBLIGATION: the safety result holds only if the      *)
(* generation counter is node-local and in-memory, co-located with removal   *)
(* and the pending-notice queue, so a restart clears all three together. If  *)
(* the counter were persisted, derived from a wire value, or outlived by its *)
(* notices, ABA would reappear and this model could not see it.              *)
(*                                                                          *)
(* HONEST SCOPE / NOT MODELLED: node crash/restart incarnations (abstracted  *)
(* as SendBreak); address gossip / non-configured discovery (that stays      *)
(* #140); the shared-stream framing + two-lane priority (#143/#147); mesh    *)
(* single-hop completeness (#140 topology). Connect updates both directed    *)
(* slots (dialer insert + accepter insert) in ONE atomic transition,         *)
(* collapsing two cross-node async events -- defensible since the slots and  *)
(* InvNoLiveDrop are per-directed-pair, but it hides some dialer/accepter     *)
(* interleaving. Generations are bounded by MaxGen, so results are           *)
(* "verified for these constants", not proven for all sizes; liveness is     *)
(* verified up to MaxBreaks disruptions.                                     *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Nodes, MaxGen, MaxBreaks, Guarded, MonotonicId, Redial

ASSUME MaxGen \in Nat /\ MaxGen >= 2
ASSUME MaxBreaks \in Nat
ASSUME Guarded \in BOOLEAN /\ MonotonicId \in BOOLEAN /\ Redial \in BOOLEAN

VARIABLES slot, gctr, pd, droppedLive, breaksLeft

vars == <<slot, gctr, pd, droppedLive, breaksLeft>>

Pairs == {p \in Nodes \X Nodes : p[1] # p[2]}

Absent == [st |-> "absent", gen |-> 0]

Notices == [from : Nodes, to : Nodes, gen : 1..MaxGen]

TypeOK ==
    /\ slot \in [Nodes -> [Nodes -> [st : {"absent", "live", "broken"}, gen : 0..MaxGen]]]
    /\ gctr \in [Nodes -> [Nodes -> 0..MaxGen]]
    /\ pd \subseteq Notices
    /\ droppedLive \in [Nodes -> [Nodes -> BOOLEAN]]
    /\ breaksLeft \in 0..MaxBreaks

Init ==
    /\ slot = [a \in Nodes |-> [b \in Nodes |-> Absent]]
    /\ gctr = [a \in Nodes |-> [b \in Nodes |-> 0]]
    /\ pd = {}
    /\ droppedLive = [a \in Nodes |-> [b \in Nodes |-> FALSE]]
    /\ breaksLeft = MaxBreaks

NextGen(cur) == IF MonotonicId THEN cur + 1 ELSE (cur % MaxGen) + 1

\* a dials b: only when a's outbound slot to b is absent (redial driver dials
\* the unlinked). One bidirectional stream => both a's outbound and b's accept
\* side are (re)established with fresh generations, replacing any prior entry.
Connect(a, b) ==
    /\ a # b
    /\ slot[a][b].st # "live"        \* redial a link that is down (absent or broken)
    /\ (Redial \/ gctr[a][b] = 0)   \* with redial off, only the first (startup) dial fires
    /\ (MonotonicId => (gctr[a][b] < MaxGen /\ gctr[b][a] < MaxGen))
    /\ gctr' = [gctr EXCEPT ![a][b] = NextGen(gctr[a][b]),
                            ![b][a] = NextGen(gctr[b][a])]
    /\ slot' = [slot EXCEPT ![a][b] = [st |-> "live", gen |-> gctr'[a][b]],
                            ![b][a] = [st |-> "live", gen |-> gctr'[b][a]]]
    /\ UNCHANGED <<pd, droppedLive, breaksLeft>>

\* a's send to b fails (peer gone / network): the writer emits a death notice
\* carrying this connection's gen. Clean replacement is NOT a SendBreak.
SendBreak(a, b) ==
    /\ a # b
    /\ slot[a][b].st = "live"
    /\ breaksLeft > 0
    /\ breaksLeft' = breaksLeft - 1
    /\ slot' = [slot EXCEPT ![a][b].st = "broken"]
    /\ pd' = pd \cup {[from |-> a, to |-> b, gen |-> slot[a][b].gen]}
    /\ UNCHANGED <<gctr, droppedLive>>

\* a death notice is delivered. Guarded: compare-and-remove keyed on the gen id
\* (the real guard checks id equality, NOT a re-checkable "broken" flag). By-key:
\* remove whatever occupies the slot. droppedLive records removing a LIVE slot.
Remove(a, b, g) ==
    /\ [from |-> a, to |-> b, gen |-> g] \in pd
    /\ pd' = pd \ {[from |-> a, to |-> b, gen |-> g]}
    /\ LET cur == slot[a][b]
           doRemove == IF Guarded THEN (cur.st # "absent" /\ cur.gen = g)
                                   ELSE (cur.st # "absent")
       IN IF doRemove
          THEN /\ slot' = [slot EXCEPT ![a][b] = Absent]
               /\ droppedLive' = IF cur.st = "live"
                                 THEN [droppedLive EXCEPT ![a][b] = TRUE]
                                 ELSE droppedLive
          ELSE UNCHANGED <<slot, droppedLive>>
    /\ UNCHANGED <<gctr, breaksLeft>>

ConnectStep == \E a, b \in Nodes : Connect(a, b)
BreakStep   == \E a, b \in Nodes : SendBreak(a, b)
RemoveStep  == \E n \in Notices : Remove(n.from, n.to, n.gen)

Next == ConnectStep \/ BreakStep \/ RemoveStep

Fairness ==
    /\ \A a, b \in Nodes : WF_vars(Connect(a, b))
    /\ \A n \in Notices : WF_vars(Remove(n.from, n.to, n.gen))

Spec == Init /\ [][Next]_vars /\ Fairness

----------------------------------------------------------------------------
\* SAFETY: a live (healthy) connection is never removed.
InvNoLiveDrop == \A a \in Nodes : \A b \in Nodes : ~droppedLive[a][b]

\* LIVENESS: every directed pair eventually becomes live and stays live
\* (redial re-establishes broken links; a stale death notice never permanently
\* prevents reconnection). Verified up to MaxBreaks disruptions.
AllLinked == \A p \in Pairs : slot[p[1]][p[2]].st = "live"
Converge == <>[]AllLinked
=============================================================================
