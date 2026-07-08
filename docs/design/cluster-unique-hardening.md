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
