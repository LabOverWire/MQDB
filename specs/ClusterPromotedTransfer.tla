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
(*            nothing newer. A lost exchange is asked again until 30 s     *)
(*            after the import. If the marker covers more than the source  *)
(*            can send, take a new snapshot. If the source dies, take the  *)
(*            role with what arrived.                                      *)
(*                                                                          *)
(* TakeWhenOverdue = TRUE adds the 30 s step: with the source alive and the *)
(* exchange unanswered, the node takes the role with what it has, and a    *)
(* write the source acked after the export is missing there. The code does *)
(* this; main also loses this write. FALSE leaves the step out, so the     *)
(* _before cfgs assume the source answers within 30 s.                     *)
(*                                                                          *)
(* Lag = FALSE: the old primary applies the swap in the same step, so it   *)
(* acks nothing once the map names the new primary. Lag = TRUE: it keeps   *)
(* acking until its own apply, the #153 and #318 window; the marker cannot *)
(* cover what is acked after it.                                            *)
(*                                                                          *)
(* StaleOrigin puts an older-epoch entry in the source's log above its     *)
(* sequence (staleSeq), or a sequence with no entry of this lineage:        *)
(*  "import": the source re-imported from a node that never had its last   *)
(*            writes, so its sequence moved back under its own log.        *)
(*  "reset":  handle_write set its sequence to 0 on a newer epoch; no import.*)
(*  "jump":   the source imports a newer lineage after the export, so a    *)
(*            sequence the taker asks for holds only the old entry.        *)
(* The catchup cap (newest_held) hides the entry until the sequence reaches *)
(* it. Then the taker applies it and skips the real write at that sequence *)
(* (skipped), or, with "jump", applies it as if it were the real one.       *)
(* Guards: ClearOnImport empties the log on an import. TruncateOnAppend     *)
(* drops entries at or above an appended sequence. NewestFirst makes the   *)
(* taker apply the newest epoch of a sequence first, for a source without  *)
(* the other two.                                                           *)
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
(*  _before_overdue (TakeWhenOverdue TRUE): ServeCorrect fails: the role   *)
(*             is taken without the write acked after the export. Not      *)
(*             closed here.                                                *)
(*  _stale_import_none (no guard): ServeCorrect fails: the write acked    *)
(*             after the export is skipped.                                 *)
(*  _stale_import_clear (ClearOnImport): holds (113 states).                *)
(*  _stale_reset_clear (ClearOnImport): ServeCorrect fails: no import ran.  *)
(*  _stale_reset_truncate (TruncateOnAppend): holds (181 states).           *)
(*  _stale_jump_truncate (TruncateOnAppend, NewestFirst): ServeCorrect      *)
(*             fails: nothing is appended at that sequence.                 *)
(*  _stale_main_source (NewestFirst only): holds for "import" (317 states). *)
(*  _stale_fix_import, _stale_fix_reset, _stale_fix_jump (all guards): hold *)
(*             (113, 181, 205 states).                                      *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS Design, Lag, MaxWrites, MaxLost, MaxEvict, Crashable, TakeWhenOverdue,
          StaleOrigin, ClearOnImport, TruncateOnAppend, NewestFirst

VARIABLES acked, oldData, swapped, oldApplied, oldUp, snap, phase, newData,
          newRole, covers, lost, evicted, logFloor, staleSeq, skipped, jumped

vars == <<acked, oldData, swapped, oldApplied, oldUp, snap, phase, newData,
          newRole, covers, lost, evicted, logFloor, staleSeq, skipped, jumped>>

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
    /\ staleSeq = None
    /\ skipped = FALSE
    /\ jumped = FALSE

Write ==
    /\ oldUp
    /\ ~oldApplied
    /\ acked < MaxWrites
    /\ acked' = acked + 1
    /\ oldData' = oldData + 1
    /\ staleSeq' = IF TruncateOnAppend /\ staleSeq # None /\ oldData + 1 >= staleSeq
                    THEN None ELSE staleSeq
    /\ UNCHANGED <<swapped, oldApplied, oldUp, snap, phase, newData, newRole, covers, lost, evicted, logFloor, skipped, jumped>>

SequenceMovesBack ==
    /\ StaleOrigin \in {"import", "reset"}
    /\ oldUp
    /\ phase = "requested"
    /\ staleSeq = None
    /\ staleSeq' = IF StaleOrigin = "import" /\ ClearOnImport THEN None ELSE oldData + 1
    /\ UNCHANGED <<acked, oldData, swapped, oldApplied, oldUp, snap, phase, newData, newRole, covers, lost, evicted, logFloor, skipped, jumped>>

SourceImportsAfterExport ==
    /\ StaleOrigin = "jump"
    /\ oldUp
    /\ ~jumped
    /\ ~oldApplied
    /\ acked < MaxWrites
    /\ phase \in {"exported", "asked"}
    /\ acked' = acked + 1
    /\ oldData' = oldData + 1
    /\ jumped' = TRUE
    /\ staleSeq' = IF ClearOnImport THEN None ELSE oldData + 1
    /\ logFloor' = IF ClearOnImport THEN oldData + 2 ELSE logFloor
    /\ UNCHANGED <<swapped, oldApplied, oldUp, snap, phase, newData, newRole, covers, lost, evicted, skipped>>

Export ==
    /\ oldUp
    /\ phase = "requested"
    /\ phase' = "exported"
    /\ snap' = oldData
    /\ UNCHANGED <<acked, oldData, swapped, oldApplied, oldUp, newData, newRole, covers, lost, evicted, logFloor, staleSeq, skipped, jumped>>

Swap ==
    /\ ~swapped
    /\ swapped' = TRUE
    /\ oldApplied' = IF Lag THEN oldApplied ELSE TRUE
    /\ UNCHANGED <<acked, oldData, oldUp, snap, phase, newData, newRole, covers, lost, evicted, logFloor, staleSeq, skipped, jumped>>

OldApplies ==
    /\ swapped
    /\ ~oldApplied
    /\ oldUp
    /\ oldApplied' = TRUE
    /\ UNCHANGED <<acked, oldData, swapped, oldUp, snap, phase, newData, newRole, covers, lost, evicted, logFloor, staleSeq, skipped, jumped>>

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
    /\ UNCHANGED <<acked, oldData, swapped, oldApplied, oldUp, snap, covers, lost, evicted, logFloor, staleSeq, skipped, jumped>>

StaleVisible ==
    /\ staleSeq # None
    /\ newData >= logFloor
    /\ staleSeq > newData
    /\ staleSeq <= oldData

Answer ==
    /\ oldUp
    /\ phase = "asked"
    /\ covers' = oldData
    /\ newData' = IF newData >= logFloor THEN oldData ELSE newData
    /\ phase' = "marked"
    /\ skipped' = (skipped \/ (StaleVisible /\ (jumped \/ ~NewestFirst)))
    /\ UNCHANGED <<acked, oldData, swapped, oldApplied, oldUp, snap, newRole, lost, evicted, logFloor, staleSeq, jumped>>

Lose ==
    /\ phase = "marked"
    /\ lost < MaxLost
    /\ lost' = lost + 1
    /\ phase' = "asked"
    /\ covers' = None
    /\ newData' = newData
    /\ UNCHANGED <<acked, oldData, swapped, oldApplied, oldUp, snap, newRole, evicted, logFloor, staleSeq, skipped, jumped>>

Evict ==
    /\ evicted < MaxEvict
    /\ phase = "asked"
    /\ oldUp
    /\ logFloor' = oldData + 1
    /\ evicted' = evicted + 1
    /\ UNCHANGED <<acked, oldData, swapped, oldApplied, oldUp, snap, phase, newData, newRole, covers, lost, staleSeq, skipped, jumped>>

Finish ==
    /\ phase = "marked"
    /\ newRole' = IF newData >= covers THEN "primary" ELSE newRole
    /\ phase' = IF newData >= covers THEN "done" ELSE "requested"
    /\ logFloor' = 0
    /\ covers' = None
    /\ UNCHANGED <<acked, oldData, swapped, oldApplied, oldUp, snap, newData, lost, evicted, staleSeq, skipped, jumped>>

Crash ==
    /\ Crashable
    /\ oldUp
    /\ oldUp' = FALSE
    /\ UNCHANGED <<acked, oldData, swapped, oldApplied, snap, phase, newData, newRole, covers, lost, evicted, logFloor, staleSeq, skipped, jumped>>

SourceGone ==
    /\ ~oldUp
    /\ newRole = "awaiting"
    /\ phase # "done"
    /\ phase # "exported"
    /\ newRole' = "primary"
    /\ phase' = "done"
    /\ UNCHANGED <<acked, oldData, swapped, oldApplied, oldUp, snap, newData, covers, lost, evicted, logFloor, staleSeq, skipped, jumped>>

Overdue ==
    /\ TakeWhenOverdue
    /\ oldUp
    /\ newRole = "awaiting"
    /\ phase = "asked"
    /\ newRole' = "primary"
    /\ phase' = "done"
    /\ UNCHANGED <<acked, oldData, swapped, oldApplied, oldUp, snap, newData, covers, lost, evicted, logFloor, staleSeq, skipped, jumped>>

Next ==
    \/ Write
    \/ SequenceMovesBack
    \/ SourceImportsAfterExport
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
    \/ Overdue

Spec ==
    /\ Init /\ [][Next]_vars
    /\ WF_vars(Export) /\ WF_vars(Swap) /\ WF_vars(OldApplies) /\ WF_vars(Import)
    /\ WF_vars(Answer) /\ WF_vars(Finish) /\ WF_vars(SourceGone)

ServeCorrect == (newRole = "primary" /\ oldUp) => (newData >= acked /\ ~skipped)

EventuallyServing == <>(newRole = "primary")
=============================================================================
