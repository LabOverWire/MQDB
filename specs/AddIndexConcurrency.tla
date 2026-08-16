---- MODULE AddIndexConcurrency ----
EXTENDS Naturals, FiniteSets

CONSTANT Serialize   \* TRUE = admin mutex held across the whole add_index (proposed fix)
                     \* FALSE = current PR #124 code (def critical section only)

Procs  == {"A", "B"}
Fields == {"fa", "fb"}
FieldOf == [p \in Procs |-> IF p = "A" THEN "fa" ELSE "fb"]

VARIABLES
    mem,    \* IndexManager in-memory definition for the entity (set of fields)
    disk,   \* persisted definition on disk (meta/index/<entity>, a single replaced key)
    mutex,  \* proposed admin mutex: "free" or the holding proc
    pc,     \* per-proc program counter
    snap,   \* per-proc captured previous definition (definition_snapshot)
    mrg     \* per-proc computed merged definition

vars == <<mem, disk, mutex, pc, snap, mrg>>

Init ==
    /\ mem   = {}
    /\ disk  = {}
    /\ mutex = "free"
    /\ pc    = [p \in Procs |-> "start"]
    /\ snap  = [p \in Procs |-> {}]
    /\ mrg   = [p \in Procs |-> {}]

\* Lines 45-50: the index_manager write-lock critical section. No await occurs
\* between acquiring the write lock and dropping it, so it is atomic and
\* mutually exclusive (TLA interleaving already serialises atomic actions).
\* In Serialize mode the whole op is additionally guarded by the admin mutex.
DoCS(p) ==
    /\ pc[p] = "start"
    /\ Serialize => mutex = "free"
    /\ snap' = [snap EXCEPT ![p] = mem]
    /\ mrg'  = [mrg  EXCEPT ![p] = mem \cup {FieldOf[p]}]
    /\ mem'  = mem \cup {FieldOf[p]}
    /\ mutex' = IF Serialize THEN p ELSE mutex
    /\ pc'   = [pc EXCEPT ![p] = "added"]
    /\ UNCHANGED disk

\* Lines 89-93: backfill finished, persist the captured merged definition.
\* persist_index overwrites the single definition key, so disk := mrg[p].
Succeed(p) ==
    /\ pc[p] = "added"
    /\ Serialize => mutex = p
    /\ disk' = mrg[p]
    /\ mutex' = IF Serialize THEN "free" ELSE mutex
    /\ pc'   = [pc EXCEPT ![p] = "done"]
    /\ UNCHANGED <<mem, snap, mrg>>

\* Lines 52-55: backfill or persist failed before the def was durably written;
\* restore_definition replaces the in-memory def with the captured snapshot.
Fail(p) ==
    /\ pc[p] = "added"
    /\ Serialize => mutex = p
    /\ mem'  = snap[p]
    /\ mutex' = IF Serialize THEN "free" ELSE mutex
    /\ pc'   = [pc EXCEPT ![p] = "done"]
    /\ UNCHANGED <<disk, snap, mrg>>

Next == \E p \in Procs : DoCS(p) \/ Succeed(p) \/ Fail(p)

Spec == Init /\ [][Next]_vars

AllDone == \A p \in Procs : pc[p] = "done"

\* Findings 1 & 2: once both add_index calls have finished, the in-memory
\* registry and the on-disk definition must agree. A violation is exactly the
\* silently-diverged state the review flagged.
Consistent == AllDone => (mem = disk)

====
