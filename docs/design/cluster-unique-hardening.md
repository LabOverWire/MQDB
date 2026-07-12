# Cluster Unique-Constraint Hardening — Implementation Plan

Status: draft for review. Pairs with the TLA models in `specs/ClusterUniqueV2.tla`
(faithful current behaviour, reproduces oversell) and `specs/ClusterUniqueFix.tla`
(the verified target: safety + liveness).

## 0. Implementation status & resume point (2026-07)

Two branches, nothing pushed yet:
- `unique-guard-cas` — the **agent-mode** fix (separate, complete): `4cc7900` makes
  `Database::create`/`update` atomic via an id-free `uniq/entity/field/value` CAS guard key +
  `expect_absent` storage precondition (see `specs/UniqueGuard.tla`). Ready to PR on its own.
  Also carries all the cluster TLA specs (`e104649`, `7816e9a`).
- `cluster-unique-phase1` (branched off the above) — **cluster Phase 1**, in progress.

**Done + tested on `cluster-unique-phase1`:**
- Step 1 (`638eecf`): cross-node reserve/commit/release now persist + replicate (were in-memory-only).
- Steps 2–3 (`fdd1a17`): unconditional `StorageBackend::sync()` fsync on durability-critical unique
  writes (even `flush()` was a no-op under `PeriodicMs`); commit/release response dispatch arms.
- Reconciler model (`27afb19`, `specs/ClusterUniqueReconciler.tla`): reclaim is a TOCTOU — safe only
  with a claim-conditional record write (Phase 2) or deadline-gated reclaim (Phase 1). See §5.
- Reconciler store ops (`b094227`): `UniqueStore::reassert` + `StoreManager::reassert_replicated`
  (record-driven repair, 5 unit tests). `ReassertResult`: Established/Repaired/AlreadyCommitted/
  Conflict(oversell)/Pending. **Keyed on record_id, not request_id** — the original create's
  reservation has request_id=uuid, so a plain reserve+commit can't repair a lost commit; that's why
  `reassert` exists.
- Review pass (`c5bc8cc`): fixed the one regression it surfaced (cross-node reserve test now expects
  the replication Write). All core/agent/cluster suites green; clippy clean.
- Reconcile wiring (this session, all five remaining items — the record-driven reconciler, no
  cross-partition query needed):
  1. `UniqueReassertRequest` wire message (BeBytes, msg type 86) + `ClusterMessage` variant +
     dispatch arm + fire-and-forget `handle_unique_reassert_request` on the value-primary calling
     `reassert_replicated` + `write_or_forward` + `sync_unique_write` (mirrors `UniqueCommitRequest`).
     Round-trip test in `protocol/tests.rs`.
  2. `NodeController::reconcile_unique_claims` (`unique.rs`): iterates this node's durable records
     for entities with unique constraints, reconciles only records whose *data* partition this node
     is primary of (so each record is reconciled once); per `(field,value)` routes a reassert
     (local `reassert_replicated` when value-primary is self, else the message). `Conflict` logged
     as detected oversell.
  3. Runs on a dedicated `unique_reconcile_interval` = **10s** (< the 30s reservation TTL) in the
     cluster-agent event loop — this cadence is the Phase 1 safety condition (a lost commit is
     re-committed before `cleanup_expired` can reclaim it).
  4. `unique_cleanup_expired` moved out of the hourly `handle_session_cleanup` into the same 10s
     `handle_unique_reconcile` handler, run **after** `reconcile_unique_claims` so repair always
     precedes reclaim in a single tick. `cleanup_expired` still only reclaims *uncommitted*
     reservations (`unique_store.rs`), so it never touches a committed/re-committed claim.
  5. Four integration tests in `tests/cluster_integration_test.rs`: reassert re-establishes a lost
     reservation from the record; reassert repairs a lost commit; abandoned (no record) survives
     reconcile then is reclaimed by TTL; cross-node `UniqueReassertRequest` establishes on the
     value-primary. All cluster suites green (615 tests); clippy clean.

**Phase 1 is complete.** After Phase 1: **Phase 2** (epoch-fenced quorum reserve, closes dual-primary
— §3/§4, verified in `specs/ClusterUniqueQuorum.tla`) and the E2E cluster tests + comprehensive test
strategy.

## 1. Problem (verified)

The cluster unique-constraint reservation is a fragile per-node in-memory lock,
decoupled from the durable+replicated record. It oversells (two committed records
share one unique value) through four mechanisms, all reproduced or code-confirmed:

- **A — durability/TTL loss.** Reservations are not fsync'd (`config.rs:20` `PeriodicMs(10)`),
  async-replicated with no quorum (`session_ops.rs:133-203`), and **cross-node reserves/commits
  are in-memory-only on the owner** (`unique.rs:299,329,349`, not persisted/replicated). TTL
  cleanup is hourly and memory-only (`mod.rs:26`, `event_loop.rs:476`). A reservation vanishes
  while the record persists → re-reserve → oversell.
- **A′ — promotion gap.** A promoted replica lacks the old primary's unacked reservations.
- **B — dual-primary.** Heartbeat-timeout failover with no lease (`transport.rs:493-499`): a
  network-partitioned old primary keeps granting reserves (handler does no ownership/epoch check,
  `unique.rs:277-316`) while a replica is promoted. Same window in planned rebalance handoff.
- **commit-loss.** Commit is fire-and-forget, its response has no dispatch arm
  (`mod.rs:1149-1170` catch-all), no retry, no rollback, no reconciler.

## 2. Target (from `ClusterUniqueFix.tla`)

The unique claim must be **both**:
- **single-authoritative / consensus-fenced** — a partitioned minority or stale primary cannot
  create a divergent claim (proven necessary: the unfenced per-node model oversells);
- **durable** — survives crash/failover/handoff (proven necessary: the fenced-but-not-durable
  variant oversells on crash).

Plus: TTL becomes GC-of-abandoned-claims only (never reclaims an active/committed claim);
commit is reliable; the existing **fail-closed on no-primary** behaviour is preserved
(`unique.rs:111-113`). Liveness (`Resolves`) holds: no value wedges.

## 3. The key decision — where fencing comes from (no per-create Raft)

Constraint: **do not put per-unique-create load on the metadata Raft group.** It exists for rare
membership/rebalance events; routing every unique claim through it (rejected Option A below) would
turn a metadata-consensus group into a data-path bottleneck. The metadata Raft is used only for
what it already does — electing one primary per partition and assigning a monotonic `epoch`.

**Rejected — Option A: route claims through the metadata Raft log.** Correct, but couples
per-create latency/throughput to the metadata group and total-orders all claims cluster-wide.

**Rejected — Option B (naive): quorum-ack the per-partition replication as-is.** The unique
partition's replica set is **RF=2**; a 2-of-2 quorum is not fault-tolerant and a 1-of-2 quorum
cannot fence a partition. Insufficient by itself.

**Recommended — Option C: epoch-fenced, quorum-durable primary-backup.** Fencing comes from the
*already-Raft-elected* single primary plus an epoch token; durability comes from quorum
replication — neither adds per-create Raft traffic.

- **Single serializer (unchanged):** the value-partition primary is elected by metadata Raft
  (`replication.rs:321-407`) and is the sole point that runs the reserve CAS. One primary per
  epoch ⇒ its local order is authoritative; no multi-writer consensus per claim.
- **Quorum-durable reserve/commit:** the primary replicates the reservation to a **majority** of
  the unique-partition replica set and returns success only after majority ack (reuse the existing
  `replicate_write` + `QuorumTracker`, `replication_ops.rs:213-262`, instead of the async
  fire-and-forget `replicate_write_async_with_extra`). A minority-partitioned old primary cannot
  reach a majority ⇒ its reserve **fails** (fenced), and the promoted primary is in the acking
  majority so it already holds the reservation (closes A′).
- **Epoch fence:** every reserve/commit carries the primary's epoch; replicas durably reject writes
  with an epoch below the highest they have seen (today unique writes hardcode `Epoch::ZERO`,
  `unique_ops.rs:32,61,93` — this must carry the real partition epoch, `map.rs:17`).
- **Epoch sealing at promotion (the subtle part):** on failover the new primary must install its
  epoch at a **majority** of replicas (which then reject the old epoch) *before serving reserves*.
  This one-time, per-failover quorum round — not per create — closes the window where the old
  primary could still gather a quorum at the stale epoch. Without it, epoch fencing has its own
  propagation race (see §5).
- **Replica set for unique partitions must be ≥3** for a meaningful majority (fencing + availability).
  Either raise RF for unique partitions, or replicate the unique keyspace to a fixed quorum group
  (e.g. the Raft members) decoupled from data RF. At RF=2 the scheme degrades to fail-closed under
  partition — safe but less available.

Option C satisfies the verified target — *single-authoritative* (Raft-elected primary + epoch
fence: a minority/stale primary cannot win) and *durable* (majority quorum survives minority
failure/failover) — while leaving the metadata Raft load unchanged. It is Option B done correctly.
The cost is subtlety: it re-implements a slice of consensus (epochs + sealing + quorum) on the
replication path, so it **must be TLA-modelled explicitly** before coding (§8).

## 4. Phasing

Deliberately incremental — each phase is independently shippable and testable.

### Phase 1 — durability, reliable commit, reconciler (closes A, A′, commit-loss)

Does **not** require consensus reserve; closes the common (single-partition failure / restart /
timeout) classes and adds oversell *detection*. Leaves dual-primary (B) narrowed but not eliminated.

1. **Persist + replicate every reserve/commit/release**, including the cross-node path. Make the
   remote handlers (`unique.rs:277-356`) go through the replicated variants
   (`unique_ops.rs:13-105`), not the in-memory-only ones. This alone removes "in-memory-only
   remote reservation" (the worst durability gap).
2. **fsync the unique keyspace** (or use `Immediate` for `DB_UNIQUE`), so a crash doesn't lose an
   acknowledged reserve (`config.rs:20`, `fjall_backend.rs:34-42`).
3. **Reliable commit.** Add dispatch arms for `UniqueCommitResponse`/`UniqueReleaseResponse`
   (`mod.rs:1149-1170`), track acks, retry on timeout, and surface failure instead of dropping it
   (`unique.rs:217-223,391-421`).
4. **Reconciler** (new, leader-driven, periodic): for each uncommitted claim past a deadline, and
   for each committed record lacking a committed claim, reconcile against the **durable record**
   (the source of truth):
   - uncommitted claim + record exists → promote claim to committed (repair a lost commit);
   - uncommitted claim + no record → release (reclaim an abandoned claim);
   - **two committed records for one value → emit a hard error/metric** (detected oversell; cannot
     be auto-resolved — see §5).
   This makes the reconciler the backstop that guarantees `record ⇒ committed-claim` and provides
   the abandonment liveness the model's `Expire` abstracts.
5. **Abandonment-based TTL.** Replace the hourly blind cleanup with the reconciler's
   record-existence check; a claim is only reclaimed when its record is absent past the deadline.

### Phase 2 — epoch-fenced quorum reserve (Option C, closes B)

6. **Carry the real partition epoch** on every unique reserve/commit/release write (replace the
   hardcoded `Epoch::ZERO`, `unique_ops.rs:32,61,93`); replicas durably reject stale-epoch writes.
7. **Quorum-durable reserve/commit:** switch the unique write path from
   `replicate_write_async_with_extra` to the quorum-tracked `replicate_write`
   (`replication_ops.rs:213-262`); return success only on majority ack. Retire the in-memory-only
   remote handlers.
8. **Epoch sealing at promotion:** `become_primary` (`mod.rs:503-509`) must install the new epoch
   at a majority of the unique-partition replicas and refuse to serve reserves until sealed.
9. **Ownership check on handlers:** `handle_unique_reserve_request` (`unique.rs:277-316`) rejects if
   the node is not the current-epoch primary. Not-primary / no-quorum → reserve fails → create
   rejected (preserves fail-closed).
10. **Replica set ≥3 for unique partitions** (raise RF or use a fixed quorum group).

Rationale for splitting: Phase 1 removes the failure modes that need no fencing and is safe to ship
first; Phase 2 adds the epoch/quorum/sealing machinery. Under network partition, only Phase 2
prevents B — Phase 1 can *detect* a dual-commit but not prevent it, because both records are durable
and committed (§5).

### Phase 2 implementation plan (fixed quorum group) — decided 2026-07

Chosen replica-set model (step 10): **fixed quorum group** — the unique keyspace is replicated to a
fixed ≥3 group decoupled from data RF, not by raising RF per unique partition. Grounded against
`specs/ClusterUniqueQuorum.tla`; the TLA→code mapping is:

| TLA | Meaning | Code |
|---|---|---|
| `Nodes` | the fixed quorum group | new `unique_quorum_group()` — see membership decision below |
| `Majorities` | `2·|Q| > |group|` | new `unique_majority()` helper |
| `promised[n]` | highest epoch replica accepts | **exists**: `ReplicaState.epoch` fence in `handle_write` (`replication.rs:105`) already rejects `write.epoch < epoch` and adopts higher |
| `actingEpoch[n]` | epoch node believes it's primary at | `ReplicaState.epoch` when role=Primary (from partition map) |
| `accepted[n]` | reservation the replica holds | that node's `DB_UNIQUE` store content |
| `sealedAt[n]` | epoch node has sealed | **new** `ReplicaState.sealed_epoch` |
| `leaderView[n]` | owner the sealed primary learned | the reservation present in the primary's store after the seal-merge |
| `Seal(n,Q)` | read-majority + epoch-promise before serving | **new** seal round on `become_primary` |
| `Reserve/Commit(n,r,Q)` | quorum-accept at epoch, gated on sealed | quorum-durable unique write |

**Load-bearing prerequisite (why order matters):** a seal's read-quorum can only *learn* an existing
reservation, and an epoch-promise can only *land*, if reserve/commit already replicate to the quorum
group. So the unique partitions must be replicated to **every group member** (each holds
`ReplicaState` + `DB_UNIQUE` for them) before sealing is meaningful. That reorders the steps vs §4:

**Status:** P2.a + P2.b + P2.c-1 **DONE**.
- P2.a/b (sequence-free group replication): `registered_nodes()` on `HeartbeatManager` +
  `unique_quorum_group()`; `UniqueReplicate` wire message (now `{ request_id, write }`, type 87);
  `send_unique_replicate` fan-out on the value-primary (epoch-stamped from the partition
  `ReplicaState`) + epoch-fenced `handle_unique_replicate` receive path gated on per-partition
  `unique_promised`.
- P2.c-1 (majority-durable reserve, **primary MQTT create path** = `json_ops` + `routing.rs`):
  `UniqueReplicateAck` (type 88); per-reserve `UniqueQuorumTracker` (acks/needed/deadline/responder)
  in `pending_unique_quorum`; `replicate_unique_reserve` returns a `oneshot::Receiver<bool>` that
  resolves `true` on majority (self counts as one) or `false` on the deadline sweep
  (`sweep_unique_quorum`, wired into the tick). `start_unique_constraint_check` collects the receivers
  into `phase1.pending_quorum`; the `json_ops` create/update paths route **any** reserve (mixed *or*
  all-local) through the async completion (`|| !pending_quorum.is_empty()`); `routing.rs`'
  `combine_quorum` folds a quorum failure into the existing reserve-failure path → release + reject
  (fail-closed). Members ack in `handle_unique_replicate` when `request_id != 0`.
- Tests: 3 `node_controller` unit tests (majority-ack resolves, timeout fails closed, single-node
  immediate) + prior P2.b tests. Full suite green (498+74+49), clippy clean.

**P2.c-2 (remote-primary reserve) DONE:** `UniqueQuorumTracker.completion` is now an enum
(`Local(oneshot)` vs `RemoteReserve { from, response_request_id }`). `handle_unique_reserve_request`
on a fresh `Reserved` calls `replicate_unique_reserve_remote` and **returns without responding**; the
tracker sends the deferred `UniqueReserveResponse` (Reserved on majority, Error on timeout via the
async `sweep_unique_quorum`). The coordinator's existing `await_unique_reserves` therefore becomes
majority-gated transparently. `record_unique_quorum_ack`/`sweep_unique_quorum` are now async and route
through `fire_unique_quorum_completion`. Integration test `unique_constraint_cross_node_message_flow`
drives the full flow (reserve → group-replicate → ack → deferred response). Both the local and remote
primary reserve paths on the MQTT create path are now majority-durable.

**P2.c gaps at this snapshot — both since RESOLVED (see "Follow-ups — all resolved" below):**
- **Secondary create path** (`db_ops` `reserve_unique_local` + `spawn_unique_completion` Path 2): was
  P2.b fire-and-forget. Now `reserve_unique_local` threads `pending_quorum` into the `db_ops` create
  phase1 and `spawn_unique_completion` awaits `await_unique_quorum` before acquiring the lock — gated
  and majority-awaited on both create paths.
- **Quorum-timeout status:** a quorum-timeout no longer reuses the 409 message. The
  `ReserveFailure { Conflict(409), NotDurable(503) }` enum returns a retryable 503 for a not-yet-durable
  reserve.

**P2.d-1 (seal epoch-promise round) DONE:** `UniqueSealRequest`/`UniqueSealResponse` (types 89/94).
`become_primary` queues a per-partition seal (`pending_seal_queue`); the event-loop tick drains it
(`drain_pending_seals`) — kept off `become_primary` so that stays sync. `initiate_unique_seal`
promises this node's epoch for the partition and sends the seal to the group; a member accepts iff
`epoch >= promised` (then promises it), else rejects (fence). The primary marks
`unique_sealed[partition] = epoch` once a **majority** accepts (self counts); `sweep_unique_seals`
retries timed-out seals (liveness). A rejected response means a member already promised a higher
epoch → this node is superseded → abandon. **Effect:** combined with P2.c majority reserves, the
epoch promise at a majority already **fences the old primary** — its stale-epoch group writes are
rejected, so its reserves can't reach a majority and fail closed. Tests: 3 `node_controller` unit
(single-node seal, majority seal, seal-request fencing) + 2 protocol round-trips + 1 integration
(`unique_seal_fences_superseded_primary_reserve`). Full suite green (503+75+49), clippy clean.
Non-breaking — no serve-gate yet.

**P2.e (serve-gate) + monotonic guard DONE — Phase 2 core complete, closes B:**
- **Serve-gate:** both reserve paths (`start_unique_constraint_check` local branch,
  `handle_unique_reserve_request` remote) refuse unless `unique_partition_sealed(part)` — i.e.
  `unique_sealed[part] == acting_epoch`. A superseded primary is sealed only at its stale epoch, so
  it fails closed. `become_primary` seals a single-node group synchronously (`seal_or_queue_unique`,
  majority 1); multi-node queues, and `tick`/`send_tick_output` now emit the seal requests
  (`collect_pending_seals` → `TickOutput.seal_requests`) so the handshake completes over normal
  ticks (agent path uses the async `drain_pending_seals`; both share `prepare_unique_seal`).
- **Monotonic guard:** `UniqueStore::apply_replicated` rejects any Insert/Update that would reassign
  a **committed** value to a *different* record (`AlreadyCommitted`). A committed claim is final; only
  its own record's release frees it. This closes the missed-**committed**-reservation overwrite.

  ⚠ **TLA-verified correction (`specs/ClusterUniqueMonotonic.tla`):** the monotonic guard is **NOT** a
  sufficient substitute for the seal-reservation-transfer (`leaderView` / d-2), contrary to the
  initial claim. Model-checking the implemented design (seal promises the epoch but does *not* learn
  reservations; primary serves from its local CAS; members apply under epoch-fence + monotonic guard)
  **violates NoOversell** in 9 steps: a new primary that **missed an UNCOMMITTED reservation** for V
  (it wasn't in that reserve's quorum, and the seal transfers nothing) grants V to a different record;
  the member holding the uncommitted claim permits the overwrite (guard is committed-only); both
  records get written. The verified model that holds NoOversell (`ClusterUniqueQuorum.tla`, 3076
  states) has `leaderView` — the seal reading + learning existing reservations. **So d-2 is required.**
- **d-2 (seal-reservation-learning) — IMPLEMENTED (closes the residual).** An accepting member returns
  its reservations for the partition in `UniqueSealResponse.reservations`
  (`UniqueStore::export_for_partition`); the promoting primary merges them
  (`UniqueStore::merge_for_seal`: learn an absent claim, upgrade uncommitted→committed, never
  downgrade a committed claim nor change a committed owner) in `handle_unique_seal_response` **before**
  the seal completes. Because the original reserve was majority-durable and the seal reads a majority,
  the two quorums intersect, so the promoting primary always learns a reservation it missed — exactly
  the verified `leaderView` step. Its local CAS then rejects a conflicting reserve. Tests:
  `merge_for_seal_learns_missing_and_never_downgrades`, `seal_response_teaches_primary_a_missed_reservation`.
  With this, the implementation matches `ClusterUniqueQuorum.tla`; **dual-primary B is fully closed.**
- Tests: `unsealed_partition_fails_reserve_closed` (gate), `apply_replicated_rejects_reassigning_committed_value`
  + `apply_replicated_allows_recommit_by_same_record` (guard); `unique_constraint_cross_node_message_flow`
  updated to complete the seal handshake before reserving. Full suite green (506+75+49), clippy clean.

**The three P2 mechanisms close B *except for one TLA-identified residual*:** (1) seal fences the old
primary (epoch-promise at a majority ⇒ its stale-epoch group writes are rejected ⇒ its reserves fail
closed); (2) the serve-gate stops a new primary serving before it has sealed; (3) **d-2
seal-reservation-learning** — the seal teaches a promoting primary any reservation it missed, so its
CAS rejects a conflicting reserve (the `leaderView` step; the monotonic guard remains as a
member-level backstop for committed values). All four are implemented; the implementation now matches
the verified `ClusterUniqueQuorum.tla`. **Both create paths are gated:** the primary MQTT path
(`json_ops`/`routing.rs`) and the forwarded cluster path (`db_ops` `reserve_unique_local`, gated on
`unique_partition_sealed`).

**Follow-ups — all resolved:**
- The forwarded `db_ops` create path is **gated and majority-awaited**: `reserve_unique_local` threads
  `pending_quorum` into the `db_ops` create phase1, and `spawn_unique_completion` awaits both
  `pending_remote` and `pending_quorum` *before* acquiring the lock (no deadlock).
- **FK-then-unique continuations DONE.** `complete_pending_fk_work` no longer reserves inline under the
  lock; it runs `start_unique_constraint_check` and returns `FkThenUniqueOutcome::NeedUnique(phase1)`,
  and `spawn_fk_completion` drops the lock then drives it via `spawn_unique_completion` — so the
  majority-await runs outside the lock (a deadlock inline; confirmed by a verification pass). This
  unified the two divergent create paths and also removed a latent up-to-5s stall on the FK path's
  inline remote-reserve await. Regression test: `fk_then_unique_defers_reserve_to_async_completion`.
- **409→503 DONE.** A `ReserveFailure { Conflict(409), NotDurable(503) }` enum is threaded through
  every reserve failure path (sync gate, forwarded path, and the async `await_unique_reserves` /
  `combine_quorum` completion): a real conflict is a permanent 409, while unsealed / no-primary /
  quorum-timeout is a transient 503 the client should retry. Counter-test:
  `unsealed_partition_reserve_is_retryable_not_a_conflict`.
- **TLA re-check DONE** (caught the monotonic-guard gap → drove the d-2 seal-learning fix above).
- **NoOversell integration test DONE.** `no_oversell_when_new_primary_missed_a_reservation` reproduces
  the TLA counterexample in the sim cluster: node0 reserves a value for A but the replication reaches
  only node2; node1 is promoted (epoch 2) and seals; the seal must *learn* A; node1's reserve for a
  different record B must then Conflict — exactly one record holds the value. Verified as a real
  regression guard: it **fails** if the d-2 seal-learning merge is removed. (A full-stack `mqdb dev`
  E2E needs an Enterprise license unavailable in this environment; the sim harness + unit tests + TLA
  cover the mechanics license-free.)

## Open follow-ups (post-review, 2026-07-11)

Verified by a 3-agent review quorum; none is a happy-path correctness regression. Fixed here:
concurrent reserve/quorum await, and the remote-reserve timeout reservation leak.

- **Remote-reserve timeout leaked a reservation — FIXED.** `handle_unique_reserve_request` deferred a
  remote-coordinated reserve via `replicate_unique_reserve_remote`; on quorum timeout it sent `Error`
  but never released the local reservation it held, wedging the value until its 30s TTL. The
  `RemoteReserve` completion now carries the claim identity and `fire_unique_quorum_completion`
  releases it on failure. Counter-test: `remote_reserve_quorum_timeout_releases_local_reservation`.
- **Sequential reserve/quorum awaits — FIXED.** `combine_quorum` (`routing.rs`) and
  `spawn_unique_completion` (`event_loop.rs`) awaited `await_unique_reserves` then `await_unique_quorum`
  sequentially (each ~5s → ~10s worst-case create latency); neither short-circuited the quorum wait.
  Both now `tokio::join!` the two rounds (max of the deadlines, ~5s). No behavior change — both results
  were already awaited unconditionally.
- **Reconciler full-scan cost — OPEN (perf follow-up).** `reconcile_unique_claims` calls
  `db_data.list(entity)` (which clones every record of the entity, unbounded) and JSON-deserializes
  each owned record every 10s, and `handle_unique_reconcile` holds the controller **write** lock across
  the entire scan and its awaited reassert sends — blocking all DB ops on the node for the duration.
  Cost is O(total records), independent of how many claims actually need repair (normally zero). Fix:
  scope to owned partitions via `list_for_partition` before cloning, iterate without materializing the
  full Vec, drop/reacquire the lock in chunks, and/or track a dirty-set instead of full scans.
- **Dynamic reconfiguration — OPEN (known limitation, out of the verified model).** `unique_quorum_group()`
  reads `HeartbeatManager::registered_nodes()`, which **grows at runtime**: `receive_heartbeat` inserts a
  previously-unknown sender, so the group is not actually fixed and does not strictly equal the Raft
  `cluster_members` set assumed above. If the group grows between a majority-durable reserve and a
  post-failover seal, the reserve-write-majority and seal-read-majority can be **disjoint**, the new
  primary never learns the reservation, and case B oversell reappears. The TLA models use a fixed
  `Nodes` constant, so this is unmodeled. Fix (future): source the quorum group from the Raft
  `cluster_members` set and gate membership changes through joint-consensus / epoch reconfiguration so no
  reserve-majority and seal-majority can be disjoint; at minimum stop `receive_heartbeat` from silently
  expanding the quorum basis.

- **P2.a — Quorum group + majority helpers.** `unique_quorum_group() -> Vec<NodeId>` and
  `unique_majority(n)`. Land together with their first consumer (P2.b) to avoid dead code.
- **P2.b — Replicate the `DB_UNIQUE` keyspace to the group.** **Structural fact (verified,
  `mqdb-core/src/partition/functions.rs:31`):** `unique_partition(entity,field,value)` hashes into
  the *same* 256-partition space as `data_partition` — unique reservations are the `DB_UNIQUE`
  keyspace *within* the ordinary partitions, there is **no** separate set of "unique partitions." So
  step 10 is not a per-partition RF bump; it is a **per-keyspace replication group**: `DB_UNIQUE`
  writes for partition P must fan out to the fixed group (all members hold `DB_UNIQUE` for P), while
  `DB_DATA`/other writes for P keep data RF=2. Consequences: (a) the replication target for a write
  becomes keyspace-dependent (`entity == DB_UNIQUE` → group; else → partition replicas); (b) a group
  member must accept a `DB_UNIQUE` write for P even when it is not a data replica of P.

  **Mechanism (refined — sequence-free, matches the model exactly).** `ClusterUniqueQuorum.tla` has
  **no sequence numbers**: `accepted[m]` is just the latest reservation a member holds and
  `promised[m]` is a pure epoch fence. So the group does **not** need the data path's
  sequence/catchup/snapshot machinery. `DB_UNIQUE` replicates to the group as **idempotent,
  epoch-fenced, order-independent** writes: each group member keeps a per-partition
  `promised_epoch`, accepts a `DB_UNIQUE` write iff `write.epoch >= promised_epoch` (then bumps it),
  and applies it last-writer-wins by epoch (commit/release are monotonic transitions on a key). A
  member that misses a message is repaired by the **Phase-1 reconciler** (record-driven reassert) and
  by the seal's majority read — no catchup/snapshot subsystem, no per-keyspace sequence. This is
  strictly simpler than a second replication group and is what the verified model actually specifies.
  New receive path (a `DB_UNIQUE`-keyspace apply that does the epoch-fence, distinct from
  `ReplicaState::handle_write`'s sequence logic). Still async, non-gating in P2.b.
- **P2.c — Quorum-durable reserve/commit.** Await a **majority** of group-member acks for the
  reserve/commit before the create flow returns `Reserved`/`Committed` (a dedicated ack tracker over
  the group; the sequence-free writes make this a simple per-request majority count, not the
  sequence-keyed `QuorumTracker`). Fail-closed on no-majority.
- **P2.d — Seal at promotion.** New `UniqueSealRequest`/`UniqueSealResponse` (BeBytes). On
  `become_primary` for a unique partition (`mod.rs:503-509`): send seal to the group, collect a
  **majority** of responses (each carries the replica's promised-epoch ack + its held reservations
  for the partition), merge learned reservations into the local store, set `sealed_epoch =
  actingEpoch`. `FenceOk`: a member refuses to promise if it already promised a higher epoch.
- **P2.e — Gate serving (flips fencing on).** Reserve/commit handlers (`unique.rs:277-352`) fail
  closed unless `sealed_epoch == actingEpoch` **and** this node is the current-epoch group primary
  (step 9). Ship P2.d + P2.e together so gating never precedes a working seal (else all unique ops
  fail closed). `leaderView ≠ NULL` (an existing reservation was learned) ⇒ reserve returns
  `Conflict` — this is exactly what closes A′/B.

Each of P2.b, P2.c, P2.{d+e} is independently shippable and testable; the model is the oracle for the
multi-node tests (concurrent same-value create, primary-kill during reserve/commit, induced
dual-primary partition). **Membership source:** use the **registered cluster membership** tracked by
`HeartbeatManager.nodes` (keyed by node id) plus `local_node` — more stable than the fluctuating alive
set, reachable from `NodeController` today with no new cross-task plumbing. Add a `HeartbeatManager`
accessor for the full registered set + a `NodeController::unique_quorum_group()` wrapper;
`unique_majority()` = `⌊|group|/2⌋ + 1`. **Caveat (not fully stable):** `registered_nodes()` is not a
fixed set — `receive_heartbeat` inserts a previously-unknown sender, so the group can grow at runtime
and does not strictly equal the metadata-Raft `cluster_members` set. That is safe under fixed
membership (what the TLA model verifies) but admits a disjoint-majority oversell if the group grows
across a reserve→failover-seal window — see "Open follow-ups → Dynamic reconfiguration" above. To close
it, source the group from Raft `cluster_members` and gate reconfiguration through consensus.

## 5. The hard part — abandonment vs record existence, and why B needs prevention

The reserve→record-write→commit sequence has a window: coordinator dies after reserve, maybe after
the record write, before commit. Safety requires: **never reclaim a claim whose record exists**
(else another claim oversells); liveness requires: **eventually reclaim a claim whose record does
not exist** (else the value wedges). The only sound discriminator is the durable record itself, so
reclamation must query the data partition (a different partition, `functions.rs:9,31`). Hence the
reconciler is not optional — it is the mechanism that ties the fragile claim to the durable record.

Dual-primary (B) cannot be repaired after the fact: two committed records for one value are both
durable and both acknowledged to clients. Deleting one is destructive and unsafe. Therefore B must
be **prevented** at reserve time by consensus — which is why Phase 2 (consensus reserve) is
required for full correctness, and why quorum-over-RF=2 (Option B) is insufficient.

**Modelled finding (`specs/ClusterUniqueReconciler.tla`): the reconciler's reclaim is itself a
TOCTOU.** If it reclaims an uncommitted, record-less claim while the coordinator is *about to* write
the record, a late unconditional write oversells (verified: `ClaimConditionalWrite=FALSE` → oversell
in 5 steps). The reclaim is safe **iff** the record write is fenced against a reclaimed claim.
Two ways to fence it:
- **Phase 2 (hard):** the record write is claim-conditional — it verifies the claim still holds
  (CAS) before persisting. Under this, even an aggressive reconciler holds `NoOversell` (verified,
  56 states).
- **Phase 1 (deadline discipline):** the reconciler reclaims **only** reservations expired past a
  safety margin (reuse the existing `expires_at`), and the create flow must abort — never write the
  record — once its reservation could have expired. This is safe under the operational assumption
  that a coordinator stalled past the margin aborts; the residual adversarial-stall window is closed
  by the Phase 2 claim-conditional write. The Phase 1 reconciler is therefore **deadline-gated, not
  aggressive.**

## 6. Component change map

| Component | File | Change |
|---|---|---|
| Reserve/commit/release ops | `store_manager/unique_ops.rs` | Phase 1: always persist+replicate. Phase 2: apply via Raft. |
| Remote handlers | `node_controller/unique.rs:277-356` | Persist+replicate (P1); ownership/epoch check then retire (P2). |
| Create continuation | `node_controller/mod.rs:1319-1392` | Reliable commit (P1); propose-to-Raft reserve/commit (P2). |
| Message dispatch | `node_controller/mod.rs:1149-1170` | Add commit/release response arms (P1). |
| Unique store | `db/unique_store.rs` | Reconciler hooks (P1); become Raft state machine (P2). |
| Raft commands/apply | `raft/coordinator/replication.rs` | New `Unique*` commands + apply (P2). |
| Reconciler | new (`store_manager` or `cluster_agent`) | Periodic leader-driven reconcile vs records (P1). |
| TTL cleanup | `cluster_agent/event_loop.rs:461-493` | Replace hourly blind cleanup with reconciler (P1). |
| Durability | `cluster_agent/config.rs:20` | fsync/Immediate for `DB_UNIQUE` (P1). |

## 7. Migration & compatibility

- On upgrade, existing on-disk `DB_UNIQUE` reservations are stale/incomplete. Rebuild committed
  claims from the durable records via the reconciler on first boot (scan records with unique
  fields, assert committed claims). Version the reservation record format (`UniqueReservation`
  already carries `version` = 1, `unique_store.rs:13`).
- Phase 2 changes the reservation authority from the per-partition store to the Raft state; provide
  a one-time import from the rebuilt store into the Raft log.

## 8. Testing (the model is the oracle)

- Multi-node `mqdb dev` cluster tests driving: concurrent same-value creates; primary kill during
  reserve/commit; induced network partition (dual-primary); rebalance during in-flight creates.
  Assert exactly one committed record per value (the `NoOversell` invariant).
- Reconciler unit tests for each branch (repair / reclaim / detect).
- **Option C is modelled and verified** in `specs/ClusterUniqueQuorum.tla` (3 nodes, majority
  quorums, per-node promised-epoch + accepted-reservation, promotion `Seal` = read-quorum +
  epoch-promise, dual-primary via concurrent `NewEpoch`). Necessity matrix (MaxEpoch=2):
  `Sealing ∧ Fence ∧ RequireMajority` → **NoOversell holds** (3076 states); dropping **any one**
  reintroduces oversell — no-seal (promoted primary misses the existing reservation), no-fence
  (superseded primary accepts at a stale epoch), no-majority (non-intersecting quorums). All three
  pillars are independently load-bearing and jointly sufficient. Option C thus equals the verified
  target (`ClusterUniqueFix`) with no per-create Raft.
- **Reconciler modelled** in `specs/ClusterUniqueReconciler.tla`: an aggressive reconciler holds
  `NoOversell` iff the record write is claim-conditional (`ClaimConditionalWrite=TRUE` → 56 states
  ok; `FALSE` → oversell). Drives the Phase 1 deadline-gated reclaim vs Phase 2 claim-conditional
  write decision (see §5).
- (Optional) liveness under eventual stability (progress requires a period without primary churn —
  the standard consensus caveat).
- Extend the fix model to make the reconciler's record-existence check explicit rather than the
  `Expire`/`Abandon` abstraction.

## 9. Risks & open questions

- **Option C re-implements a slice of consensus** (epochs + sealing + quorum) on the replication
  path. This is the main risk: the sealing race (§5) and epoch monotonicity are exactly where such
  schemes go subtly wrong. Mitigation: the mandatory TLA model (§8) with a "no-seal" negative
  switch before any code; no per-create Raft is the deliberate trade for this added subtlety.
- **Replica set size**: unique partitions need ≥3 members for a fault-tolerant, partition-fencing
  majority. Decide: raise RF for unique partitions, or replicate the unique keyspace to a fixed
  quorum group decoupled from data RF. At RF=2 the scheme is safe but fail-closed under partition.
- **Latency**: reserve/commit gain a majority-ack round-trip on the value-partition (not a
  cluster-wide consensus). Quantify; it only affects unique-constrained creates.
- **Reconciler cross-partition queries**: cost of checking record existence on the data partition;
  batch and rate-limit.
- **Metadata Raft unchanged**: confirm the existing group already spans enough nodes to elect
  primaries reliably — Option C leans on its epoch/election exactly as today, adding no new load.
- **Scope**: Phase 1 is a meaningful, low-risk improvement on its own; decide whether Phase 2 is
  committed up front or gated on Phase 1 results and measured partition-failure risk.
