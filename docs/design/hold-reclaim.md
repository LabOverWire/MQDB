# Reclaiming silently-abandoned exclusive holds

## Problem

An application (e.g. ticketing seat holds) builds an *exclusive hold* on mqdb: a row
with a `unique(seatId)` constraint, one hold per seat. Two buyers racing to claim the
same seat is already solved — the unique guard rejects one with a hard 409 (see
`docs/design/cluster-unique-hardening.md`). The unsolved case is **silent
abandonment**: a holder crashes / closes the tab / loses the network and never returns.
No status change is ever written, so the hold lingers and the seat is stuck forever.

This is a **liveness** problem, orthogonal to the **safety** (no-oversell) problem the
unique-hardening work solved. Detecting "the owner is gone" is an *absence*, and an
absence produces no write for the unique machinery to arbitrate. Only two things can
observe it: **time** (a lease/TTL) or **connection state** (presence). The feature adds
the missing detection axis and hands the actual seat hand-off back to the existing
unique fence.

## Design (app-side janitor)

mqdb provides three primitives; the application runs a trusted (admin-authenticated)
*janitor* service. The janitor subscribes to presence, holds the `client_id → hold`
binding, applies a short grace (cancelled if the holder returns), then releases the
abandoned hold with a fenced (CAS) delete. Reclaimers then race a fresh unique-guarded
create — exactly one wins.

### The load-bearing invariant (TLA-proven)

Modeled in `specs/AbandonedHoldReclaim.tla` (+ `.cfg`, `_noreassert.cfg`, `_nocas.cfg`).
The reap is correct **iff both** hold:

1. **Terminal per-attempt CAS.** The janitor's release is conditioned on the hold not
   having changed since it observed (a compare on `_version`), and this check is
   *terminal* — a mismatch is a non-retryable error, not something the write loop
   retries past.
2. **Reassert-on-reconnect.** A returning holder performs a **write** (re-asserting the
   hold: bump `_version`, refresh `_bound_client_id` / a lease timestamp) within the
   grace window. That write is what makes the janitor's CAS fail.

The model shows: with both, `InvNoFalseRelease` holds over the full state space;
dropping either one false-releases a live holder. **Presence connect-event cancellation
is only a latency optimization — the correctness fence is the reassert-write + CAS.**

Oversell stays inherited-safe: reclaim is release-then-guarded-create through the
existing unique fence, and guard-release is in the same storage batch as the row remove,
so there is never a two-live-holds window.

## The three mqdb primitives

### A. Correct TTL backstop `[P0 — real bug, ship first]`

Presence is best-effort; the TTL sweep is the mandatory backstop for every missed
disconnect and every hard crash. Today it is broken in two (agent) / four (cluster) ways.

- **Agent** (`crates/mqdb-agent/src/database/background.rs`, `ttl_cleanup_task`): the
  sweep does a bare `batch.remove(key)` missing both `release_unique_guards`
  (`crud.rs:381`) — so an expired unique hold leaks its guard and the seat is
  **permanently unsellable** — and `expect_value` (`crud.rs:374`) — so a hold renewed
  between the scan and the commit is deleted on the stale snapshot (**data loss**). Fix:
  add both, using the *scanned bytes* for `expect_value` (vault byte-exactness), and
  switch to per-entity batches (the precondition is all-or-nothing).
- **Cluster** (`cluster_agent/event_loop.rs::handle_ttl_cleanup` →
  `cluster/db/data_store.rs::cleanup_expired_ttl`): worse — a raw in-memory
  `entities.remove` with **no ChangeEvent, no replication, no guard release, no FK
  cascade**. The missing ChangeEvent means the change-feed reclaim signal never fires in
  cluster mode. Fix: rewrite to take a write lock, gate on `is_primary_for_partition`,
  and route each expired row through the real delete path (`db_delete_prepare` +
  `db_commit` + `release_unique_for_deleted_record` + `publish_and_deliver_change_event`)
  with a version precondition.
- Optional (defer): lazy read-time expiry (`read()` returns not-found for expired) plus
  create-path opportunistic reap (evict an expired holder on unique-collision) so a seat
  unblocks before the sweep. A short sweep interval covers the common case without this.

### B. Client CAS (`expected_version`, terminal, both paths)

- Shared: add `expected_version: Option<u64>` to `Request::Update`/`Request::Delete`
  (`mqdb-core/src/transport.rs`) and carve it out of the payload in `build_request`
  (else it merges as a data field). Add a non-`Conflict` error variant
  (`PreconditionFailed`) + code.
- **The retry-loop trap:** the existing update/delete loops retry on `Conflict`,
  re-read, and write anyway — a naive pre-loop CAS is silently defeated. The check must
  live *inside* the per-attempt body, compared against the fresh read, and return the
  non-retryable error.
- Agent: `crates/mqdb-agent/src/database/crud.rs` (`try_update_once`/`try_delete_once`).
- Cluster: `crates/mqdb-cluster/src/cluster/node_controller/db_ops.rs`
  (`handle_json_update_local` right after `db_get`, **before** the unique-reserve round
  that drops the lock; delete before `db_delete_prepare`). Reuse the existing
  `__mqdb_fk_expected` terminal-mismatch precedent.
- Parity: payload parsing is shared (mqdb-core); enforcement is duplicated across the two
  paths and must be added to both.

### C. Presence feed (both modes)

- Cluster (`cluster/event_handler/broker_events.rs`): emit connect + disconnect events,
  fanned out the **LWT way** (`forward_publish_to_remotes` + topic-index targets) —
  `queue_local_publish` and change events are local-only and are the wrong template. The
  publish target must not be swallowed by the DB-op handler (carve `_presence` out of the
  `handle_db_publish` interception, or publish outside `$DB`), and a shared
  topic-protection rule (`$DB/_presence/# ReadOnly`, mirroring `$DB/+/events/#` in
  `mqdb-agent/src/topic_rules.rs`) lets a non-admin janitor subscribe. Keep the `mqdb-`
  internal-client skip.
- Agent: wire a `BrokerEventHandler` (none exists today) + a `spawn_presence_task`
  publishing locally.
- Payload: `{client_id, user_id, event, unexpected, ts}`. Treat as a **hint** — dropped
  under load, flap-reordered; `ts` authoritative.

## Phasing

| PR | Scope | Notes |
|----|-------|-------|
| 1a | TTL backstop fix — **agent** | `background.rs`: guard release + `expect_value` on scanned bytes + per-entity batches. Standalone data-loss + seat-lockout bug. |
| 1b | TTL backstop fix — **cluster** | Rewire `handle_ttl_cleanup` through the replicated delete path (primary-gated, guard release, change event, version precondition) — a larger change than 1a, split out to isolate risk. |
| 2  | Client CAS — both paths + new error | Add "retry-loop-doesn't-defeat-CAS" counter-tests. |
| 3  | Presence feed — both modes + topic rule | Gated on decisions 1–2 below. |
| app | Janitor + reassert-on-reconnect contract + short keepalive | Out of mqdb (application). |

The TTL fix is split into 1a (agent) and 1b (cluster) because the cluster sweep does a
raw in-memory `entities.remove` with no replication — a correct fix must route each
expired row through the replicated delete path (primary-gated), which is a substantially
larger change than the agent's batch tweak.

## Open decisions (recommendations; confirm before PR 3)

1. **Per-connection vs per-user binding.** Per-connection false-reclaims a user who still
   has another live connection. **Rec: per-user** (`_bound_user_id` + user-level presence
   aggregation).
2. **mqtt-lib `user_id`.** ✅ RESOLVED — shipped in mqtt5 `0.38.4` (pin `mqtt5 = "0.38.4"`,
   replacing the git dep). Both `ClientConnectEvent` and `ClientDisconnectEvent` now carry
   `pub user_id: Option<Arc<str>>` (mirrors `ClientPublishEvent::user_id`; the authenticated
   username, `None` for anonymous). Read as `event.user_id.as_deref()`. `ClientDisconnectEvent`
   also has `client_id: Arc<str>` + `unexpected: bool`, so the presence payload
   `{client_id, user_id, event, unexpected, ts}` is fully available (generate `ts`). This
   unblocks the per-user binding in decision 1. The pin is a workspace dependency bump done
   as its own step (it swaps a git dep for the published crate and needs its own build/test
   verification), landing with or before PR 3.
3. **Crash-detection latency.** Sub-second reclaim holds only for **graceful** disconnects;
   a hard crash is detected at the MQTT keepalive timeout. **Rec: short keepalive on hold
   connections + document** the crash-path bound.

## Verification

- **TLA (done):** `specs/AbandonedHoldReclaim.tla` — safe with CAS + reassert;
  false-releases without either.
- **Tests per phase:** TTL guard-release + version-precondition (both modes); CAS terminal
  + not-defeated-by-retry + agent/cluster parity; presence emit + cross-node delivery +
  internal-client skip.
- **Live E2E:** `mqdb dev` cluster — presence cross-node, CAS reject, end-to-end TTL
  reclaim.

## Why this is different from the double-sale work

The unique-hardening program proved a **safety** invariant (`NoOversell`: at most one
committed unique claim per value, across failover / reconfig / correlated loss). It is
triggered entirely by writes. A silently-vanished holder writes nothing, so that
machinery never fires — and the very durability that makes a claim survive failover is
what makes a dead claim stick. This feature adds the missing **liveness/detection** axis
(presence + TTL) and a **fenced release** (CAS), then routes the seat hand-off back
through the unique fence it already trusts. It reuses the double-sale guarantee; it does
not replace or weaken it.
