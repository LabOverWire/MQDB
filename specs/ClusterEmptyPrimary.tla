------------------------- MODULE ClusterEmptyPrimary -------------------------
(***************************************************************************)
(* A PRIMARY THAT RESTARTS WITHOUT ITS DATA (gh #272).                     *)
(*                                                                          *)
(* One partition, three nodes, primary plus one replica. The Raft log is   *)
(* a sequence of assignments (hist); a node applies the newest one on its  *)
(* own step (applied). A crash loses the node's roles and its local map;   *)
(* a wiping crash also loses its disk. Failure detection (Detect) fires    *)
(* only while a node is down, so a fast restart can beat it. Detection     *)
(* promotes the replica, or any live node when no replica is alive         *)
(* (select_new_primary). AddReplica is add_missing_replicas; Swap is the   *)
(* rebalancer moving the primary to the replica.                           *)
(*                                                                          *)
(* VERIFIED FACTS MODELLED (Design = "current"):                            *)
(*  - apply_raft_partition_map calls become_primary whenever the map names *)
(*    the node primary, whatever its role and whatever its store holds     *)
(*  - a replica without the Primary or Replica role requests a snapshot    *)
(*    from the primary; request_snapshot is a no-op while a request for    *)
(*    the partition is recorded, and nothing clears that record except an  *)
(*    import, so a request lost with its source is never sent again        *)
(*  - a snapshot import merges into the store; it makes the node primary   *)
(*    if its map names it, and replica otherwise                           *)
(*  - any node answers a snapshot request with what its store holds        *)
(*                                                                          *)
(* Design = "fix": a node takes the primary role only if it already holds  *)
(* the partition (Primary or Replica role), or if it has not held the      *)
(* partition since boot and its disk records that it held the primary role *)
(* at this very assignment (marker). Otherwise it waits for a snapshot from *)
(* the replica and becomes primary on import. A waiting node asks again    *)
(* when its assignment changes or its request is lost (Retry stands for    *)
(* the retry timeout and for a failed transfer).                           *)
(*                                                                          *)
(* A node keeps records in memory (mem) and on disk. A write reaches both; *)
(* a snapshot import reaches memory only (import_partition does not        *)
(* persist); a crash reloads memory from disk. So the marker is written     *)
(* only for a partition the node did not fill by an import in this run.    *)
(*                                                                          *)
(* ABSTRACTIONS: a snapshot transfer is one step (export and import), so  *)
(* writes cannot fall between them; a write reaches the replica named by  *)
(* the newest assignment if that replica has the Replica role, standing in *)
(* for sequence-gap catch-up. One failure at a time (RF=2): a node crashes *)
(* only while every other node is up and every assigned node holds every  *)
(* acked write, and the primary only while a replica has the Replica role.*)
(* With Lag = FALSE the rebalancer moves only a primary that holds the     *)
(* Primary role, and it steps down in the same step. Lag = TRUE lets the   *)
(* old primary (or a node still waiting to become primary) act on its old *)
(* map until its own ApplyMap.                                              *)
(*                                                                          *)
(* Marker is the persisted held-primary epoch (`_held_primary`, written by  *)
(* become_primary). Stores written before the marker existed (no marker,  *)
(* records on disk) are trusted as before; that upgrade path is not       *)
(* modelled.                                                               *)
(*                                                                          *)
(* RESULTS (3 nodes, MaxWrites 2, MaxCrashes 2, MaxWipes 1, MaxSwaps 1):    *)
(*  _current:        ServeCorrect fails in 3 steps: the primary crashes    *)
(*                   with a wipe, restarts before Detect, takes the role.  *)
(*  _current_live:   liveness holds (the bug is a safety one).             *)
(*  _fix:            all invariants and both properties hold.             *)
(*  _fix_noretry:    EventuallyServing fails: a request lost with its     *)
(*                   source is never sent again.                            *)
(*  _fix_lag, _current_lag: ServeCorrect fails without any crash: the old  *)
(*                   primary of a rebalance keeps serving on its old map   *)
(*                   after the new primary took over. Not caused or fixed  *)
(*                   by this change.                                        *)
(*  _markimport:     Design "markimport" also writes the marker after an   *)
(*                   import; ServeCorrect fails: a wiped node fills from a *)
(*                   snapshot, restarts without a wipe, and trusts a disk  *)
(*                   that never held the imported records.                 *)
(*  _recovered:      Design "recovered" trusts any data on disk instead of *)
(*                   the marker; ServeCorrect fails: a replica that missed *)
(*                   writes while down is promoted after its restart.      *)
(*                                                                          *)
(* ServeCorrect: a live node in the primary role holds every acked write.  *)
(* NoAckedLoss: every acked write is on some disk or some live memory.    *)
(* EventuallyServing: once failures stop, the assigned primary serves.     *)
(* EventuallyReplicated: once failures stop, the assigned replica has the  *)
(* Replica role.                                                            *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Design, MaxWrites, MaxCrashes, MaxWipes, MaxSwaps, Lag, RetryLost

Nodes == {1, 2, 3}
Keys == {1}
None == 0
Roles == {"none", "primary", "replica", "awaiting"}

VARIABLES up, mem, disk, role, applied, requested, src, live, held, imported,
          marker, hist, dead, truth, writes, crashes, wipes, swaps

vars == <<up, mem, disk, role, applied, requested, src, live, held, imported,
          marker, hist, dead, truth, writes, crashes, wipes, swaps>>

Empty == [k \in Keys |-> 0]
Latest == hist[Len(hist)]
LocalMap(n) == hist[applied[n]]
HasAll(n) == \A k \in Keys : mem[n][k] = truth[k]
Failed(n) == ~up[n] \/ (n \in {Latest.p, Latest.r} /\ ~HasAll(n))

TypeOK ==
    /\ up \in [Nodes -> BOOLEAN]
    /\ mem \in [Nodes -> [Keys -> 0..MaxWrites + 1]]
    /\ disk \in [Nodes -> [Keys -> 0..MaxWrites + 1]]
    /\ role \in [Nodes -> Roles]
    /\ applied \in [Nodes -> 0..(1 + MaxCrashes * 2 + MaxSwaps + 4)]
    /\ requested \in [Nodes -> BOOLEAN]
    /\ src \in [Nodes -> Nodes \cup {None}]
    /\ live \in [Nodes -> BOOLEAN]
    /\ dead \subseteq Nodes

Init ==
    /\ up = [n \in Nodes |-> TRUE]
    /\ disk = [n \in Nodes |-> IF n \in {1, 2} THEN [k \in Keys |-> 1] ELSE Empty]
    /\ mem = disk
    /\ role = [n \in Nodes |-> CASE n = 1 -> "primary" [] n = 2 -> "replica" [] OTHER -> "none"]
    /\ applied = [n \in Nodes |-> 1]
    /\ requested = [n \in Nodes |-> FALSE]
    /\ src = [n \in Nodes |-> None]
    /\ live = [n \in Nodes |-> FALSE]
    /\ held = [n \in Nodes |-> n \in {1, 2}]
    /\ imported = [n \in Nodes |-> FALSE]
    /\ marker = [n \in Nodes |-> IF n = 1 THEN 1 ELSE 0]
    /\ hist = <<[p |-> 1, r |-> 2]>>
    /\ dead = {}
    /\ truth = [k \in Keys |-> 1]
    /\ writes = 1
    /\ crashes = 0
    /\ wipes = 0
    /\ swaps = 0

Ask(n, from) ==
    /\ requested' = [requested EXCEPT ![n] = TRUE]
    /\ src' = [src EXCEPT ![n] = from]
    /\ live' = [live EXCEPT ![n] = up[from]]

KeepRequest == UNCHANGED <<requested, src, live>>

AwaitFrom(n, from) ==
    /\ role' = [role EXCEPT ![n] = "awaiting"]
    /\ IF Design # "current" \/ ~requested[n] THEN Ask(n, from) ELSE KeepRequest

TakeRole(n, r) ==
    /\ role' = [role EXCEPT ![n] = r]
    /\ IF Design # "current" THEN
           /\ requested' = [requested EXCEPT ![n] = FALSE]
           /\ live' = [live EXCEPT ![n] = FALSE]
           /\ src' = [src EXCEPT ![n] = None]
       ELSE KeepRequest

HeldData(n) == role[n] \in {"primary", "replica"}
TrustDisk(n) ==
    /\ ~held[n]
    /\ IF Design = "recovered"
       THEN \E k \in Keys : disk[n][k] # 0
       ELSE marker[n] = Len(hist)

ApplyAsPrimary(n, m) ==
    IF Design = "current" \/ HeldData(n) \/ TrustDisk(n) \/ m.r = None
    THEN TakeRole(n, "primary")
    ELSE AwaitFrom(n, m.r)

ApplyAsReplica(n, m) ==
    IF HeldData(n) THEN TakeRole(n, "replica") ELSE AwaitFrom(n, m.p)

ApplyMap(n) ==
    /\ up[n]
    /\ applied[n] # Len(hist)
    /\ LET m == Latest IN
       /\ CASE m.p = n -> ApplyAsPrimary(n, m)
            [] m.r = n -> ApplyAsReplica(n, m)
            [] OTHER -> TakeRole(n, "none")
    /\ applied' = [applied EXCEPT ![n] = Len(hist)]
    /\ held' = [held EXCEPT ![n] = held[n] \/ role'[n] \in {"primary", "replica"}]
    /\ marker' = [marker EXCEPT ![n] =
                      IF role'[n] = "primary" /\ ~imported[n] THEN Len(hist) ELSE marker[n]]
    /\ UNCHANGED <<up, mem, disk, imported, hist, dead, truth, writes, crashes, wipes, swaps>>

ImportedRole(n) ==
    LET m == LocalMap(n) IN
    CASE m.p = n -> "primary"
      [] m.r = n -> "replica"
      [] OTHER -> IF Design # "current" THEN "none" ELSE "replica"

ServeSnapshot(n) ==
    /\ up[n]
    /\ role[n] = "awaiting"
    /\ live[n]
    /\ up[src[n]]
    /\ mem' = [mem EXCEPT ![n] = [k \in Keys |->
                   IF mem[src[n]][k] # 0 THEN mem[src[n]][k] ELSE mem[n][k]]]
    /\ role' = [role EXCEPT ![n] = ImportedRole(n)]
    /\ requested' = [requested EXCEPT ![n] = FALSE]
    /\ live' = [live EXCEPT ![n] = FALSE]
    /\ src' = [src EXCEPT ![n] = None]
    /\ held' = [held EXCEPT ![n] = held[n] \/ ImportedRole(n) \in {"primary", "replica"}]
    /\ imported' = [imported EXCEPT ![n] = Design \in {"fix", "recovered"}]
    /\ marker' = [marker EXCEPT ![n] =
                      IF ImportedRole(n) = "primary" /\ Design \in {"current", "markimport"}
                      THEN applied[n] ELSE marker[n]]
    /\ UNCHANGED <<up, disk, applied, hist, dead, truth, writes, crashes, wipes, swaps>>

Retry(n) ==
    /\ Design # "current"
    /\ RetryLost
    /\ up[n]
    /\ role[n] = "awaiting"
    /\ ~live[n]
    /\ LET m == LocalMap(n)
           from == IF m.p = n THEN m.r ELSE m.p
       IN /\ from \in Nodes
          /\ from # n
          /\ up[from]
          /\ Ask(n, from)
    /\ UNCHANGED <<up, mem, disk, role, applied, held, imported, marker, hist, dead, truth, writes, crashes, wipes, swaps>>

Write(k) ==
    /\ writes < MaxWrites + 1
    /\ \E p \in Nodes :
        /\ up[p]
        /\ role[p] = "primary"
        /\ LET v == writes + 1
               r == Latest.r
               gets(n) == n = p \/ (n = r /\ n # p /\ up[n] /\ role[n] = "replica")
           IN /\ mem' = [n \in Nodes |-> IF gets(n) THEN [mem[n] EXCEPT ![k] = v] ELSE mem[n]]
              /\ disk' = [n \in Nodes |-> IF gets(n) THEN [disk[n] EXCEPT ![k] = v] ELSE disk[n]]
              /\ truth' = [truth EXCEPT ![k] = v]
              /\ writes' = v
    /\ UNCHANGED <<up, role, applied, requested, src, live, held, imported, marker, hist, dead, crashes, wipes, swaps>>

Crash(n, wipe) ==
    /\ up[n]
    /\ crashes < MaxCrashes
    /\ wipe => wipes < MaxWipes
    /\ \A m \in Nodes \ {n} : ~Failed(m)
    /\ n = Latest.p => (Latest.r # None /\ role[Latest.r] = "replica")
    /\ up' = [up EXCEPT ![n] = FALSE]
    /\ disk' = IF wipe THEN [disk EXCEPT ![n] = Empty] ELSE disk
    /\ mem' = [mem EXCEPT ![n] = IF wipe THEN Empty ELSE disk[n]]
    /\ imported' = [imported EXCEPT ![n] = FALSE]
    /\ marker' = IF wipe THEN [marker EXCEPT ![n] = 0] ELSE marker
    /\ role' = [role EXCEPT ![n] = "none"]
    /\ applied' = [applied EXCEPT ![n] = 0]
    /\ requested' = [requested EXCEPT ![n] = FALSE]
    /\ src' = [src EXCEPT ![n] = None]
    /\ live' = [m \in Nodes |-> IF m = n \/ src[m] = n THEN FALSE ELSE live[m]]
    /\ held' = [held EXCEPT ![n] = FALSE]
    /\ crashes' = crashes + 1
    /\ wipes' = IF wipe THEN wipes + 1 ELSE wipes
    /\ UNCHANGED <<hist, dead, truth, writes, swaps>>

Restart(n) ==
    /\ ~up[n]
    /\ up' = [up EXCEPT ![n] = TRUE]
    /\ dead' = dead \ {n}
    /\ UNCHANGED <<mem, disk, marker, role, applied, requested, src, live, held, imported, hist, truth, writes, crashes, wipes, swaps>>

Detect(n) ==
    /\ ~up[n]
    /\ n \notin dead
    /\ dead' = dead \cup {n}
    /\ LET m == Latest
           alive == Nodes \ (dead \cup {n})
       IN CASE m.p = n ->
                 IF m.r # None /\ m.r \in alive
                 THEN hist' = Append(hist, [p |-> m.r, r |-> None])
                 ELSE \E q \in alive : hist' = Append(hist, [p |-> q, r |-> None])
            [] m.r = n -> hist' = Append(hist, [p |-> m.p, r |-> None])
            [] OTHER -> UNCHANGED hist
    /\ UNCHANGED <<up, mem, disk, role, applied, requested, src, live, held, imported, marker, truth, writes, crashes, wipes, swaps>>

AddReplica(n) ==
    /\ up[n]
    /\ n \notin dead
    /\ Latest.r = None
    /\ Latest.p # n
    /\ hist' = Append(hist, [p |-> Latest.p, r |-> n])
    /\ UNCHANGED <<up, mem, disk, role, applied, requested, src, live, held, imported, marker, dead, truth, writes, crashes, wipes, swaps>>

Swap ==
    /\ swaps < MaxSwaps
    /\ LET m == Latest IN
       /\ m.r # None
       /\ up[m.p] /\ up[m.r]
       /\ m.p \notin dead /\ m.r \notin dead
       /\ hist' = Append(hist, [p |-> m.r, r |-> m.p])
       /\ IF Lag
          THEN UNCHANGED <<role, applied>>
          ELSE /\ role[m.p] = "primary"
               /\ applied[m.p] = Len(hist)
               /\ role' = [role EXCEPT ![m.p] = "replica"]
               /\ applied' = [applied EXCEPT ![m.p] = Len(hist) + 1]
    /\ swaps' = swaps + 1
    /\ UNCHANGED <<up, mem, disk, requested, src, live, held, imported, marker, dead, truth, writes, crashes, wipes>>

Next ==
    \/ \E n \in Nodes : ApplyMap(n) \/ ServeSnapshot(n) \/ Retry(n) \/ Restart(n) \/ Detect(n) \/ AddReplica(n)
    \/ \E n \in Nodes, wipe \in BOOLEAN : Crash(n, wipe)
    \/ \E k \in Keys : Write(k)
    \/ Swap

Fairness ==
    /\ \A n \in Nodes : WF_vars(ApplyMap(n))
    /\ \A n \in Nodes : WF_vars(ServeSnapshot(n))
    /\ \A n \in Nodes : WF_vars(Retry(n))
    /\ \A n \in Nodes : WF_vars(Restart(n))
    /\ \A n \in Nodes : WF_vars(Detect(n))
    /\ \A n \in Nodes : WF_vars(AddReplica(n))

Spec == Init /\ [][Next]_vars /\ Fairness

ServeCorrect == \A n \in Nodes : (up[n] /\ role[n] = "primary") => HasAll(n)

NoAckedLoss ==
    \A k \in Keys : truth[k] # 0 =>
        \E n \in Nodes : disk[n][k] = truth[k] \/ (up[n] /\ mem[n][k] = truth[k])

Serving ==
    /\ up[Latest.p]
    /\ applied[Latest.p] = Len(hist)
    /\ role[Latest.p] = "primary"

Replicated == Latest.r # None => (up[Latest.r] /\ role[Latest.r] = "replica")

EventuallyServing == <>[]Serving
EventuallyReplicated == <>[]Replicated
=============================================================================
