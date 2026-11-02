----------------------- MODULE ClusterPromotedTransfer -----------------------
(***************************************************************************)
(* A REPLICA PROMOTED WHILE ITS SNAPSHOT IS IN FLIGHT (gh #272).            *)
(*                                                                          *)
(* ClusterEmptyPrimary treats a transfer as one step, so it cannot show a  *)
(* write acked after the export. Here the old primary (old) keeps acking   *)
(* writes after the export until it applies the swap. The new primary      *)
(* (new) refuses replicated writes while it waits, so those writes reach   *)
(* it only by catchup. A write is a counter value, so "holds every acked   *)
(* write" is newData >= acked.                                              *)
(*                                                                          *)
(* Design:                                                                 *)
(*  "none":   take the role on import, no catchup (aa1b513).                *)
(*  "after":  take the role on import, then ask the source for the rest.   *)
(*  "before": on import stay waiting and ask the source for what follows   *)
(*            the snapshot; take the role on its end marker when it covers *)
(*            nothing newer. A lost exchange is asked again. If the marker *)
(*            covers more than the source can send, take a new snapshot.   *)
(*            If the source dies, take the role with what arrived.         *)
(*                                                                          *)
(* Lag = FALSE: the old primary applies the swap in the same step, so it   *)
(* acks nothing once the map names the new primary. Lag = TRUE: it keeps   *)
(* acking until its own apply, the #153 and #318 window; the marker cannot *)
(* cover what is acked after it.                                            *)
(*                                                                          *)
(* ABSTRACTIONS: one partition, two nodes; catchup is one piece; only the  *)
(* case where the swap lands before the import (an import before the swap *)
(* makes the node a replica, which #284 already catches up). Held writes,  *)
(* epochs and other nodes are not modelled.                                *)
(*                                                                          *)
(* RESULTS (MaxWrites 2, MaxLost 1, MaxEvict 1, Crashable FALSE unless     *)
(* noted):                                                                  *)
(*  _none:     ServeCorrect fails: a write acked after the export is       *)
(*             missing when the node takes the role.                        *)
(*  _after:    ServeCorrect fails: same, until catchup lands.               *)
(*  _before:   ServeCorrect and EventuallyServing hold (113 states).        *)
(*  _before_crash (Crashable TRUE): EventuallyServing holds; ServeCorrect  *)
(*             is checked only while the source lives (287 states). A      *)
(*             write only on a dead source is the replication window.      *)
(*  _before_lag (Lag TRUE): ServeCorrect fails: the old primary acks a     *)
(*             write after the marker. Not closed here (#153, #318).        *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS Design, Lag, MaxWrites, MaxLost, MaxEvict, Crashable

VARIABLES acked, oldData, swapped, oldApplied, oldUp, snap, phase, newData,
          newRole, covers, lost, evicted, logFloor

vars == <<acked, oldData, swapped, oldApplied, oldUp, snap, phase, newData,
          newRole, covers, lost, evicted, logFloor>>

None == 99

TypeOK ==
    /\ acked \in 0..MaxWrites
    /\ phase \in {"requested", "exported", "asked", "marked", "done"}
    /\ newRole \in {"awaiting", "primary"}

Init ==
    /\ acked = 0
    /\ oldData = 0
    /\ swapped = FALSE
    /\ oldApplied = FALSE
    /\ oldUp = TRUE
    /\ snap = None
    /\ phase = "requested"
    /\ newData = 0
    /\ newRole = "awaiting"
    /\ covers = None
    /\ lost = 0
    /\ evicted = 0
    /\ logFloor = 0

Write ==
    /\ oldUp
    /\ ~oldApplied
    /\ acked < MaxWrites
    /\ acked' = acked + 1
    /\ oldData' = oldData + 1
    /\ UNCHANGED <<swapped, oldApplied, oldUp, snap, phase, newData, newRole, covers, lost, evicted, logFloor>>

Export ==
    /\ oldUp
    /\ phase = "requested"
    /\ phase' = "exported"
    /\ snap' = oldData
    /\ UNCHANGED <<acked, oldData, swapped, oldApplied, oldUp, newData, newRole, covers, lost, evicted, logFloor>>

Swap ==
    /\ ~swapped
    /\ swapped' = TRUE
    /\ oldApplied' = IF Lag THEN oldApplied ELSE TRUE
    /\ UNCHANGED <<acked, oldData, oldUp, snap, phase, newData, newRole, covers, lost, evicted, logFloor>>

OldApplies ==
    /\ swapped
    /\ ~oldApplied
    /\ oldUp
    /\ oldApplied' = TRUE
    /\ UNCHANGED <<acked, oldData, swapped, oldUp, snap, phase, newData, newRole, covers, lost, evicted, logFloor>>

ImportedPhase ==
    CASE Design = "before" /\ oldUp -> "asked"
      [] Design = "after" -> "asked"
      [] OTHER -> "done"

ImportedRole ==
    IF Design = "before" /\ oldUp THEN "awaiting" ELSE "primary"

Import ==
    /\ phase = "exported"
    /\ swapped
    /\ newData' = snap
    /\ phase' = ImportedPhase
    /\ newRole' = ImportedRole
    /\ UNCHANGED <<acked, oldData, swapped, oldApplied, oldUp, snap, covers, lost, evicted, logFloor>>

Answer ==
    /\ oldUp
    /\ phase = "asked"
    /\ covers' = oldData
    /\ newData' = IF newData >= logFloor THEN oldData ELSE newData
    /\ phase' = "marked"
    /\ UNCHANGED <<acked, oldData, swapped, oldApplied, oldUp, snap, newRole, lost, evicted, logFloor>>

Lose ==
    /\ phase = "marked"
    /\ lost < MaxLost
    /\ lost' = lost + 1
    /\ phase' = "asked"
    /\ covers' = None
    /\ newData' = newData
    /\ UNCHANGED <<acked, oldData, swapped, oldApplied, oldUp, snap, newRole, evicted, logFloor>>

Evict ==
    /\ evicted < MaxEvict
    /\ phase = "asked"
    /\ oldUp
    /\ logFloor' = oldData + 1
    /\ evicted' = evicted + 1
    /\ UNCHANGED <<acked, oldData, swapped, oldApplied, oldUp, snap, phase, newData, newRole, covers, lost>>

Finish ==
    /\ phase = "marked"
    /\ newRole' = IF newData >= covers THEN "primary" ELSE newRole
    /\ phase' = IF newData >= covers THEN "done" ELSE "requested"
    /\ logFloor' = 0
    /\ covers' = None
    /\ UNCHANGED <<acked, oldData, swapped, oldApplied, oldUp, snap, newData, lost, evicted>>

Crash ==
    /\ Crashable
    /\ oldUp
    /\ oldUp' = FALSE
    /\ UNCHANGED <<acked, oldData, swapped, oldApplied, snap, phase, newData, newRole, covers, lost, evicted, logFloor>>

SourceGone ==
    /\ ~oldUp
    /\ newRole = "awaiting"
    /\ phase # "done"
    /\ phase # "exported"
    /\ newRole' = "primary"
    /\ phase' = "done"
    /\ UNCHANGED <<acked, oldData, swapped, oldApplied, oldUp, snap, newData, covers, lost, evicted, logFloor>>

Next ==
    \/ Write
    \/ Export
    \/ Swap
    \/ OldApplies
    \/ Import
    \/ Answer
    \/ Lose
    \/ Evict
    \/ Finish
    \/ Crash
    \/ SourceGone

Spec ==
    /\ Init /\ [][Next]_vars
    /\ WF_vars(Export) /\ WF_vars(Swap) /\ WF_vars(OldApplies) /\ WF_vars(Import)
    /\ WF_vars(Answer) /\ WF_vars(Finish) /\ WF_vars(SourceGone)

ServeCorrect == (newRole = "primary" /\ oldUp) => newData >= acked

EventuallyServing == <>(newRole = "primary")
=============================================================================
