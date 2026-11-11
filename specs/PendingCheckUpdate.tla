---- MODULE PendingCheckUpdate ----
\* One cluster record and a few clients. An update may wait on a remote FK or
\* unique check; the controller lock is dropped while it waits, so other
\* updates, deletes and creates of the same id can commit in between.
\*
\* Rebase = FALSE models main: the update builds the merged record before the
\* wait and writes it as-is when the check returns (a missing record fails
\* the write with NotFound).
\* Rebase = TRUE models the fix: on completion the update re-reads the record
\* and refuses if it is gone (404), if it carried an expected version and the
\* record version moved (412), or if any field it sets no longer holds the
\* value it read before the wait (409); otherwise it writes its own fields
\* onto the current record with version current + 1.
\*
\* Omitted: replication, failover and primary moves (all writes here happen
\* on one primary under one lock); the check itself (it passes or fails
\* nondeterministically); unique reservations and their release; vault
\* encryption; creates and deletes that themselves wait on checks; schema
\* validation. Each client issues exactly one operation. Versions restart at
\* 1 when a record is recreated, so the version invariants are stated per
\* incarnation (between deletes). The 409 on a changed field is not needed
\* for these invariants (a later ack may overwrite an earlier one); it is
\* there because the unique claim release, which this spec omits, frees the
\* value the update read before the wait. A record deleted and re-created
\* during the wait is not distinguished from the one the update read: if
\* the new record holds the same values in the fields the update sets, the
\* update applies onto it, and an expected version can match the new
\* incarnation's version.
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Clients, Rebase

Fields == {"u", "o"}
Values == {0, 1, 2}
Initial == [f \in Fields |-> 0]

UpdateOps == [kind : {"update"}, field : Fields, val : {1, 2},
              pending : BOOLEAN, expect : BOOLEAN]
DeleteOps == {[kind |-> "delete"]}
CreateOps == [kind : {"create"}, vals : [Fields -> Values]]
Ops == UpdateOps \cup DeleteOps \cup CreateOps

VARIABLES exists, ver, val, pc, op, base, bver, merged, mver, log

vars == <<exists, ver, val, pc, op, base, bver, merged, mver, log>>

NoOp == [kind |-> "none"]

Init ==
    /\ exists = TRUE
    /\ ver = 1
    /\ val = Initial
    /\ pc = [c \in Clients |-> "idle"]
    /\ op = [c \in Clients |-> NoOp]
    /\ base = [c \in Clients |-> Initial]
    /\ bver = [c \in Clients |-> 0]
    /\ merged = [c \in Clients |-> Initial]
    /\ mver = [c \in Clients |-> 0]
    /\ log = <<>>

Ack(entry) == log' = Append(log, entry)

Finish(c) == pc' = [pc EXCEPT ![c] = "done"]

Unchanged_waiting == UNCHANGED <<base, bver, merged, mver>>

StartDelete(c, o) ==
    /\ o.kind = "delete"
    /\ op' = [op EXCEPT ![c] = o]
    /\ Finish(c)
    /\ Unchanged_waiting
    /\ IF exists
       THEN /\ exists' = FALSE
            /\ Ack([kind |-> "delete", client |-> c, ver |-> ver, writes |-> <<>>])
            /\ UNCHANGED <<ver, val>>
       ELSE UNCHANGED <<exists, ver, val, log>>

StartCreate(c, o) ==
    /\ o.kind = "create"
    /\ op' = [op EXCEPT ![c] = o]
    /\ Finish(c)
    /\ Unchanged_waiting
    /\ IF ~exists
       THEN /\ exists' = TRUE
            /\ ver' = 1
            /\ val' = o.vals
            /\ Ack([kind |-> "create", client |-> c, ver |-> 1,
                    writes |-> o.vals])
       ELSE UNCHANGED <<exists, ver, val, log>>

Writes(o) == [f \in {o.field} |-> o.val]

StartUpdate(c, o) ==
    /\ o.kind = "update"
    /\ op' = [op EXCEPT ![c] = o]
    /\ UNCHANGED exists
    /\ IF ~exists
       THEN /\ Finish(c)
            /\ UNCHANGED <<ver, val, log>>
            /\ Unchanged_waiting
       ELSE IF o.pending
       THEN /\ pc' = [pc EXCEPT ![c] = "waiting"]
            /\ base' = [base EXCEPT ![c] = val]
            /\ bver' = [bver EXCEPT ![c] = ver]
            /\ merged' = [merged EXCEPT ![c] = [val EXCEPT ![o.field] = o.val]]
            /\ mver' = [mver EXCEPT ![c] = ver + 1]
            /\ UNCHANGED <<ver, val, log>>
       ELSE /\ Finish(c)
            /\ Unchanged_waiting
            /\ val' = [val EXCEPT ![o.field] = o.val]
            /\ ver' = ver + 1
            /\ Ack([kind |-> "update", client |-> c, ver |-> ver + 1, prev |-> ver,
                    writes |-> Writes(o), expected |-> IF o.expect THEN ver ELSE 0])

Start(c) ==
    /\ pc[c] = "idle"
    /\ \E o \in Ops :
        \/ StartDelete(c, o)
        \/ StartCreate(c, o)
        \/ StartUpdate(c, o)

CheckFails(c) ==
    /\ pc[c] = "waiting"
    /\ Finish(c)
    /\ UNCHANGED <<exists, ver, val, op, base, bver, merged, mver, log>>

Expected(c) == IF op[c].expect THEN bver[c] ELSE 0

CompleteMain(c) ==
    /\ IF exists
       THEN /\ val' = merged[c]
            /\ ver' = mver[c]
            /\ Ack([kind |-> "update", client |-> c, ver |-> mver[c], prev |-> ver,
                    writes |-> Writes(op[c]), expected |-> Expected(c)])
       ELSE UNCHANGED <<ver, val, log>>

Refused(c) ==
    \/ ~exists
    \/ op[c].expect /\ ver # bver[c]
    \/ val[op[c].field] # base[c][op[c].field]

CompleteRebase(c) ==
    /\ IF Refused(c)
       THEN UNCHANGED <<ver, val, log>>
       ELSE /\ val' = [val EXCEPT ![op[c].field] = op[c].val]
            /\ ver' = ver + 1
            /\ Ack([kind |-> "update", client |-> c, ver |-> ver + 1, prev |-> ver,
                    writes |-> Writes(op[c]), expected |-> Expected(c)])

Complete(c) ==
    /\ pc[c] = "waiting"
    /\ Finish(c)
    /\ UNCHANGED <<exists, op, base, bver, merged, mver>>
    /\ IF Rebase THEN CompleteRebase(c) ELSE CompleteMain(c)

Next == \E c \in Clients : Start(c) \/ CheckFails(c) \/ Complete(c)

Spec == Init /\ [][Next]_vars

DeleteBetween(i, j) == \E k \in (i + 1)..(j - 1) : log[k].kind = "delete"

Overwritten(i, f) ==
    \E j \in (i + 1)..Len(log) :
        log[j].kind = "delete" \/ f \in DOMAIN log[j].writes

InvNoLostAck ==
    \A i \in 1..Len(log) :
        \A f \in DOMAIN log[i].writes :
            ~Overwritten(i, f) => exists /\ val[f] = log[i].writes[f]

Versioned(i) == log[i].kind \in {"update", "create"}

InvVersionsIncrease ==
    \A i, j \in 1..Len(log) :
        (i < j /\ Versioned(i) /\ Versioned(j) /\ log[j].kind = "update"
            /\ ~DeleteBetween(i, j))
            => log[i].ver < log[j].ver

InvNoDuplicateVersion ==
    \A i, j \in 1..Len(log) :
        (i < j /\ Versioned(i) /\ Versioned(j) /\ ~DeleteBetween(i, j))
            => log[i].ver # log[j].ver

InvExpectedHonoured ==
    \A i \in 1..Len(log) :
        (log[i].kind = "update" /\ log[i].expected > 0)
            => log[i].prev = log[i].expected

====
