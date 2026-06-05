# Changelog

All notable changes to this project will be documented in this file.

Each entry lists the date and the crate versions that were released.

## 2026-06-05 — mqdb-vault 0.1.2, mqdb-cli 0.8.11

### Fixed

- Vault passphrase change (`admin_change`) could corrupt a user's records if the migration's finalize write failed after the records were already re-encrypted. `admin_change` rotates in three steps: install the new salt/check plus `vault_old_check`/`vault_old_salt` resume markers (`migration_start`), re-encrypt every owned record from the old key to the new key (`batch_vault_re_encrypt`), then clear the markers (`migration_done`). If `migration_done` failed after the batch succeeded, the identity was left `migration_status: pending, mode: re_encrypt` with records already under the new passphrase. On the next unlock, `resume_pending_migration` derived `old_crypto` from the supplied (now **new**) passphrase plus the old salt — a key that decrypts nothing — and unconditionally re-ran `batch_vault_re_encrypt`, double-encrypting the already-rotated records (the wrong-key decrypt is a graceful no-op, then the re-encrypt wraps the existing ciphertext again). The records became permanently unrecoverable. The `re_encrypt` resume now verifies the supplied passphrase against `vault_old_check` (with the old salt) before re-running the batch: it only re-encrypts when the old passphrase genuinely matches, otherwise it recognises the half-finalized state, skips the destructive batch, and clears the markers. The `decrypt` resume path (`admin_disable`) was already safe because `decrypt_record` is a no-op on plaintext. Verified end-to-end against the release binary: a reproduced half-finalized rotation now decrypts cleanly to plaintext where the previous build returned ciphertext. (#66)

## 2026-05-30 — mqdb-agent 0.8.9, mqdb-cli 0.8.10

### Fixed

- `Database::list` self-heals stale secondary-index entries instead of only logging `index pointed to non-existent entity: …`. When `list_from_index_ids` reads an index entry whose data row is missing (`Error::NotFound`), it now scans `idx/{entity}/` for every key ending in `/{id}` and removes them in a single batch. The purge is race-safe via a pre-scan re-check (`storage.get(&data_key).is_none()`): if the row was re-created before the heal runs, the helper returns `Ok(0)` and the next `list` call sees the live row. The atomic row+index delete shipped in 0.8.8 (#84 / 95c138c) prevents new stale entries on the agent delete path; this heals pre-existing entries and any future entries that might be left by paths outside `mqdb-agent` (e.g. cluster snapshot/replication). The NotFound log level moves from `warn!` to `info!`/`debug!` depending on whether a heal actually ran, so a quiet system stays quiet. (#89, follow-up perf optimisation tracked in #90.)

## 2026-05-29 — mqdb-core 0.7.3, mqdb-agent 0.8.8, mqdb-cli 0.8.9

### Fixed

- Optimistic-lock (CAS) conflicts on `Database::update`/`delete` could stall single-process embedded callers and, on the persistent backend, silently drop writes. `update` reads a record, merges fields across several `.await` points (schema, constraint, index locks), then commits under a compare-and-swap on the originally-read bytes; two concurrent writers to the same key both read version N, the first commits, and the second's swap failed with `Error::Conflict`. `update` is now a thin wrapper over `try_update_once` that retries on `Error::Conflict` (bounded at 32 attempts), re-reading the latest committed value and re-applying the partial-field merge each time, so concurrent same-process updates converge to a field-level last-writer-wins instead of erroring. Retry is skipped when `update` carries precomputed vault-constraint plaintext (`update_constraint_data`), which was derived upstream against a now-stale read and cannot be re-merged here; those still surface `Error::Conflict` for the caller to retry. The typed `Error::Conflict` variant is unchanged and remains the backstop after attempts are exhausted.
- `Database::delete` left stale secondary-index entries under concurrent load (the `index pointed to non-existent entity` warning). `delete` had no compare-and-swap, so a delete that read an older version committed unconditionally; if a concurrent `update` had already changed an indexed field and committed first, the delete removed only the old index key plus the data row and stranded the new index key. `delete` now CAS-guards the primary entity and retries, re-reading the fresh record so it removes the current index keys. (Cascade/set-null child rows are not individually CAS-guarded; the reported case was the primary entity.)
- `mqdb-core` fjall backend: the batch `commit` checked CAS preconditions against a snapshot and then applied the writes in a separate batch — a check-then-act that was not atomic. Under concurrency two commits could both pass their precondition check and both apply, producing a silent lost update with no `Error::Conflict` and, via the same race, stale index entries. The in-memory backend was already correct (its data write lock spans check and apply); the encrypted backend delegates the CAS to its inner backend. The fjall `commit` now serializes the precondition check and apply under a per-backend lock (the fsync stays outside the lock, so durability cost is unchanged). A before/after benchmark (agent mode, `--durability none`, concurrency=1, 20k ops/run) showed no write-throughput regression: insert ~5.5k ops/s and update ~6.9k ops/s in both builds, within run-to-run noise. New `mqdb-agent` concurrency tests cover update convergence and delete/index consistency.

## 2026-05-28 — mqdb-agent 0.8.7, mqdb-cli 0.8.8

### Added

- Recipient-scoped event confidentiality (diagram sharing phase 3, #78), behind a new opt-in `--scoped-events` flag (env `MQDB_SCOPED_EVENTS`, default off). Until now every authenticated subscriber to `$DB/{entity}/events/#` received all change events, including for records they cannot read. With the flag on, change events for ownership-enabled and derived (child) entities are published once per recipient to `$DB/u/{recipient}/events/{entity}/{id}`, where recipients are the resource owner plus its share grantees (child events resolve recipients through the parent diagram). Recipients are resolved at write time and travel with the change event, so cascade deletes still reach the owner and grantees after the parent record and its shares are removed. Global entities (no ownership/derivation) keep the single broadcast publish, unchanged. The broker enforces that a user may only subscribe to their own `$DB/u/{me}/events/#` namespace (admins may read any; only the internal event service may publish there); the legacy `$DB/+/events/#` topic is unaffected because owned-entity events are no longer published to it. Clients must subscribe to `$DB/u/{me}/events/#` instead of `$DB/{entity}/events/#` — a breaking change gated behind the flag. Authorization core verified in TLA+ (`specs/DiagramSharing.tla`: `InvEventConfidentiality` and `InvEventCompleteness` hold across 52,488 states, including reparented child states). Agent mode only; cluster parity tracked in #75; public/anonymous event topics tracked with phase 4 (#79).

## 2026-05-28 — mqdb-core 0.7.2, mqdb-agent 0.8.6, mqdb-cli 0.8.7

### Added

- Child-entity access derivation (diagram sharing phase 2, #77). A new opt-in `--ownership-derive` flag (env `MQDB_OWNERSHIP_DERIVE`) maps a child entity to its parent: `child=fk_field>parent_entity` pairs, comma-separated (e.g. `nodes=diagramId>diagrams,edges=diagramId>diagrams`). A derived child inherits access from the referenced parent record — read requires `view` on the parent, and create/update/delete require `edit`. The parent reference is immutable on update, so an editor cannot reparent a child into a diagram they cannot edit. Absent a mapping a child entity stays unrestricted (prior behavior), so the change is opt-in per deployment. This closes the pre-existing hole where any authenticated user could read or edit a child record (e.g. a diagram's nodes/edges) just by knowing its id. New `mqdb-core` API: `OwnershipConfig::with_derivations`/`derivation` and the `OwnershipDecision::Derive` variant. Agent mode only; cluster parity tracked in #75. Verified in TLA+: `specs/DiagramSharing.tla` now models a mutable parent with an unrestricted reparent action, and all 13 invariants still hold across reparented states (52,488 states), confirming derived access cannot leak when a child moves between parents.

## 2026-05-28 — mqdb-cli 0.8.6

### Fixed

- `mqdb agent start` (and `cluster start`) panicked at startup on every invocation in debug builds: `thread 'main' panicked ... Long option names must be unique ... '--' is in use by both 'passwd_data' and 'acl_data'`. Seventeen optional inline-content args (`passwd_data`, `acl_data`, `scram_data`, the `*_data`/`*_key`/`*_secret` variants in both agent and cluster) were declared `#[arg(long = "")]`, which clap registers as the empty long option `--`; the duplicates tripped clap's `debug_assert`, crashing every debug-build command before parsing (release builds skip the assert, so the flags were silently mis-defined and never parsed). Each now has a distinct hidden long name (`--passwd-data`, `--acl-data`, `--scram-data`, …), so debug builds run and the inline-content flags parse correctly while staying hidden from `--help`. Added a `Cli::command().debug_assert()` regression test so a future empty/duplicate long name fails CI instead of only crashing debug binaries.
- `mqdb agent start --memory-backend` no longer requires `--db`. The flag is now `required_unless_present = "memory_backend"`; in memory mode without a path it defaults to a temp directory, which the in-memory backend never touches. Callers no longer need to pass a dummy `--db`.

## 2026-05-28 — mqdb-core 0.7.1, mqdb-agent 0.8.5, mqdb-vault 0.1.1, mqdb-wasm 0.3.3, mqdb-cli 0.8.5

### Added

- Resource sharing for agent mode. An owner can grant another user `view` or `edit` on any ownership-enabled entity (the motivating case is diagrams) through four new resource-scoped MQTT topics: `$DB/{entity}/{id}/share` (`{"grantee","permission":"view|edit","cascade":true}`), `$DB/{entity}/{id}/unshare` (`{"grantee","cascade":true}`), `$DB/{entity}/{id}/shares` (owner/admin lists a resource's grants), and `$DB/{entity}/shared` (caller lists resources shared with them). Grants are stored in a server-managed `_shares` entity that is not reachable through generic CRUD — direct `$DB/_shares/...` returns 403. Read now requires `view` and update requires `edit`; delete stays owner-only, and deleting a resource clears its grants so a record reusing the same id cannot inherit them. `share`/`unshare` cascade by default over self-referencing diagram→diagram references (bounded at 256, cycle-safe; a direct share sets the level while a cascade only raises an existing grant). In OAuth deployments the grantee email is resolved to its canonical id via `_identity_links` (resolve-existing only — sharing with an unregistered email returns 404; password/SCRAM use the username verbatim). The authorization core is verified in TLA+ (`specs/DiagramSharing.tla`, `CascadeClosure.tla`, `GrantLifecycle.tla`).
- New `mqdb-core` public API backing the feature: `AccessLevel` (`View`/`Edit`, ordered), `SHARES_ENTITY`, and the `Share`/`Unshare`/`Shares`/`Shared` variants on `Request` and `DbOp`. Cluster parity is tracked in #75; the embedded `mqdb-wasm` build rejects these operations with "sharing is not supported in embedded mode".

## 2026-05-24 — mqdb-agent 0.8.4

### Removed

- Dead `jwt: String` field on `Session`, `NewSession`, and `SessionRef`. After 0.8.3 wired the JTI directly into the session, no code path read the stored JWT anymore: `handle_logout` uses `session.jti`, `destroy_others_by_canonical_id` returns JTIs, and `handle_ticket` mints fresh ticket JWTs from session claims rather than the stored one. The session-time JWT was never returned to the client either — callback, register, and login all set just the session-id cookie plus a user-info JSON body. Dropped the field and removed `mint_callback_jwt` entirely; its three callers now generate a JTI inline via `JtiRevocationStore::generate_jti()`. `handle_login` no longer needs to construct a `ProviderIdentity` or destructure `email_verified`. No behavior change; the affected types are crate-private.

## 2026-05-23 — mqdb-agent 0.8.3

### Fixed

- MQTT password change and reset (`$DB/_auth/password/change`, `$DB/_auth/password/reset/submit`) updated `_credentials` and returned success without invalidating any HTTP sessions for the same user, leaving every cookie-backed session live until its 24h TTL — the HTTP-only fix in 0.8.2 (#68) only closed the HTTP scope of #37. `SessionStore`, `JtiRevocationStore`, and the existing HTTP-session-invalidation step now reach both MQTT handlers via Option-wrapped `Arc` references threaded through `MqdbAgent` → `MessageContext` → `AdminContext` (Option because the cluster-agent code path does not run an HTTP server). After the MQTT credential write succeeds, `invalidate_http_sessions` calls `destroy_others_by_canonical_id(canonical_id, None)` (no live caller session at MQTT request time) and revokes the returned JTIs via the new `JtiRevocationStore::revoke_many`.
- Cleanup along the way: `Session`/`NewSession`/`SessionRef` now carry the `jti` directly (captured when the session's JWT is minted), so `destroy_others_by_canonical_id` returns JTIs rather than JWTs and `handle_logout` no longer decodes its own session's JWT to find the JTI. `mint_callback_jwt` returns `(jwt, jti)` so all 3 callers (callback, register, login) pass the JTI through to `SessionStore::create`; the dev-login session keeps an empty JTI and is filtered out of revocation results. `verify_jwt_ignore_expiry` is no longer called from the password-change/logout paths (only the refresh path still needs it). `HttpServerConfig` now owns the `Arc<SessionStore>` and `Arc<JtiRevocationStore>` so the same instances back both the HTTP server and the MQTT handler task — closing the architectural gap noted as the reason for partial scope in #68.
- Code-review nit from #68 addressed: `JtiRevocationStore::revoke` now `warn!`s when the `MAX_REVOKED_JTIS` cap is hit and the JTI is dropped (was silent).
- Test coverage: 2 new unit tests in `session_store.rs` (`destroy_others_skips_empty_jti_sessions`, `revoke_many_revokes_all_jtis`); the existing 3 destroy-others tests rewritten around JTIs instead of JWTs to match the new return type.

## 2026-05-23 — mqdb-agent 0.8.2

### Fixed

- Password changes left every other HTTP session for the same user valid until the 24h TTL expired (`SessionStore` only exposed `destroy(session_id)`, so `POST /auth/password/change` updated `_credentials` and returned 200 with the attacker's session still live). Added `SessionStore::destroy_others_by_canonical_id(canonical_id, keep_session_id)` returning the JWTs of removed sessions so the caller can revoke their JTIs. `handle_password_change` now keeps the caller's session and destroys the rest; `handle_password_reset_submit` destroys all sessions for the target canonical_id (the user has no live session at reset time). Each destroyed session's JWT is decoded with `verify_jwt_ignore_expiry` and its `jti` added to `JtiRevocationStore`, matching the pattern already used by `handle_logout` so reissued MQTT tickets bound to those JWTs are rejected. The MQTT password-change path (`$DB/_auth/password/change`) intentionally still does not invalidate HTTP sessions — `AdminContext` has no reference to `SessionStore`; tracked as follow-up to issue #37.
- Test coverage: 3 unit tests for `destroy_others_by_canonical_id` covering keep-current-session, destroy-all (`None` keep), and unknown-user (empty result).
- Observability for security-sensitive silent failures along the new path: `destroy_others_by_canonical_id` now `warn!`s on a poisoned `SessionStore` lock (was a silent empty return); `revoke_jwt_jtis` `warn!`s when a destroyed session's JWT fails to decode or is missing the `jti` claim (was a silent skip); `handle_password_reset_submit` now `error!`s and returns 500 when the verified challenge record is missing `canonical_id` (was `.unwrap_or("")` → silent no-op on `destroy_others_by_canonical_id`).

## 2026-05-22 — mqdb-agent 0.8.1, mqdb-cluster 0.3.6, mqdb-cli 0.7.7

### Fixed

- `http-api` feature flag was effectively a lie at every layer: declared in `mqdb-agent`'s `Cargo.toml` with `default = ["http-api"]`, but three pieces of `MqdbAgent` referenced `crate::http::*` unconditionally — the `http_config` field, the `with_http_config` builder, and the two call sites of `spawn_http_task` in `run` / `start`. `cargo check -p mqdb-agent --no-default-features` failed with five compile errors (`E0433` on the missing `crate::http`, `E0609` on the gated `identity_crypto` field, and an `E0282` inference failure cascading from those). Even after gating those, the flag was still meaningless at the CLI level because `mqdb-cli`, `mqdb-cluster`, and `mqdb-vault` all depended on `mqdb-agent` without `default-features = false` — so the HTTP server was always compiled in regardless of what `mqdb-cli --features` said. Fixed by: (1) gating the three `mqdb-agent` items behind `#[cfg(feature = "http-api")]`; (2) extracting `RateLimiter` out of `mqdb_agent::http::rate_limiter` to a top-level `mqdb_agent::rate_limiter` module so `mqdb-vault` no longer needs `http-api`; (3) flipping `mqdb-cli`, `mqdb-cluster`, and `mqdb-vault` to `default-features = false` on their `mqdb-agent` dep; (4) feature-gating the HTTP wiring in `mqdb-cluster` (`http_config` fields, `with_http_config`, `spawn_http_task`) and `mqdb-cli` (`build_http_config`, `build_identity_crypto`, both `--http-bind` call sites). `cargo build --bin mqdb --no-default-features` now actually drops the HTTP server: `jsonwebtoken` symbols go from present to zero, and `hyper`-related code drops ~60% (residual `hyper`/`argon2` come from `mqtt5`, not our HTTP server). `--http-bind` without `http-api` logs a warning and is otherwise ignored. Added a `cargo make clippy-no-default` task wired into the default `clippy`/`ci` chain so this regression cannot reach `main` silently again.

### Docs

- Corrected stale feature-flag names in user-facing docs. `README.md`, `docs/distributed-design.md`, and `docs/testing/01-setup.md` all referenced `--features agent-only` and a `native` feature flag that don't exist on the `mqdb` CLI or `mqdb-agent`. (A `native` feature does exist in `mqdb-core/Cargo.toml`, but it gates `tokio` for non-native targets and is unrelated to the agent/cluster split the README described.) The actual `mqdb` CLI features are `cluster` (default), `http-api` (default), `opentelemetry`, and `dev-insecure`; agent-only builds use `--no-default-features` (optionally adding `--features http-api`). Added a feature-flag reference table in `README.md` under "CLI Tool → Installation" documenting all four flags and their defaults — the `http-api` flag was previously undocumented anywhere outside its `Cargo.toml` declaration.

## 2026-05-19 — mqdb-cluster 0.3.5

### Removed

- `StoreManager::clear_partition` (the 15-line aggregator at `partition_io.rs:206`). It had **no callers anywhere in the codebase** — production, tests, or otherwise — and aggregated per-partition wipes across stores that included the broadcast catalog (schemas, constraints). With schemas and constraints now treated as broadcast state on the export side (every partition snapshot ships the full catalog, per the 0.3.2 / 0.3.4 work), the aggregator was wrong-by-construction: a partition wipe must not drop catalog entries that every node is supposed to hold.
- `SchemaStore::clear_partition` and its `clear_partition_removes_only_target` unit test. The method was referenced only by its own test; nothing in the migration, rebalance, or snapshot paths called it.
- `ConstraintStore::clear_partition` and its `clear_partition_removes_only_target` unit test. Same dead-code-with-self-test status as above.
- Per-partition `clear_partition` on `DbDataStore`, `IndexStore`, `UniqueStore`, `FkValidationStore`, and the MQTT-side stores (sessions, retained, topics, wildcards, inflight, offsets, idempotency, qos2) is intentionally preserved — those stores are genuinely partition-scoped.

### Changed

- Added struct-level doc comments on `SchemaStore` and `ConstraintStore` documenting the broadcast-catalog invariant: every node holds the full catalog, every partition snapshot ships all entries, and there is intentionally no per-partition clearing API. Makes the invariant explicit at the point where a future contributor would otherwise consider adding a clearing method.

### Notes

- Follow-up to the broadcast-catalog model established by PR #61 (the export-side counterpart). 83 deletions across `partition_io.rs`, `schema_store.rs`, and `constraint_store.rs`; 12 lines of documentation added.

## 2026-05-06 — mqdb-cli 0.7.6

### Fixed

- `--timeout` did not apply during the MQTT CONNECT handshake. `connect_client` in `crates/mqdb-cli/src/common.rs` only wrapped the request/response wait in `tokio::time::timeout`; the `MqttClient::connect[_with_options]` calls themselves had no timeout, so any command (`mqdb list`, `read`, `create`, `update`, `delete`, etc.) would hang indefinitely against a TCP listener that accepts the connection but never sends CONNACK (silent broker, half-open NAT, firewall drop after SYN-ACK).
- Extracted a shared `connect_with_timeout(client, client_id, conn)` helper in `common.rs` that wraps both `MqttClient::connect[_with_options]` calls in `tokio::time::timeout(Duration::from_secs(conn.timeout), …)` and surfaces `connect to {broker} timed out after {N}s` on expiry. The helper also honors `conn.insecure` for self-signed TLS — previously only the bench paths set this, the CRUD path silently skipped it.
- Routed every CLI bench/dev_bench connect through the new helper to close the same bug class for `mqdb bench db` (sync + async + cascade + unique + changefeed), `mqdb bench pubsub`, `mqdb dev bench` (db/pubsub/sub-pub), and the broker-readiness probes in both `bench/common.rs::wait_for_broker_ready` and `dev_bench/helpers.rs::wait_for_broker_ready`. Removed two now-redundant local `connect_client` helpers in `db_cascade.rs` and `db_changefeed.rs`. The `pubsub.rs` paths use custom `ConnectOptions` (clean-start, custom keep-alive) so their connect calls are wrapped inline with the same timeout pattern rather than going through the helper.
- Regression test `test_cli_connect_timeout_against_silent_listener` in `crates/mqdb-cli/tests/cli_test.rs` spawns a TCP listener that accepts the connection without speaking MQTT and asserts `mqdb list ... --timeout 2` exits within 5 seconds with a "timed out" error. Verified to fail on main (pre-fix exits at ~6s with "Connection reset by peer") and pass with the fix in place.

## 2026-05-06 — mqdb-cluster 0.3.4

### Fixed

- **Partition snapshot import did not populate `FkReverseIndex`.** This was the "Known gap" called out in the 0.3.2 entry. After a rebalance-driven replica promotion, the new primary held the imported `db_data` records and FK constraints but its in-memory reverse-index cache (`(target_entity, target_id, source_entity, source_field) → {source_id, …}`) was empty for those records. `start_fk_reverse_lookup` and `handle_fk_reverse_lookup_request` would return empty for any record sitting on a newly-imported partition, causing ON DELETE CASCADE to miss children that the new primary owned and ON DELETE RESTRICT to silently allow deletes that should have been blocked.
- `StoreManager::import_partition` now calls a new private `rebuild_fk_indexes_after_import` step at the end of the import. It iterates every registered FK constraint and calls the existing `rebuild_fk_index_for_constraint` helper, which walks `db_data.list(source_entity)` (now populated with the just-imported records) and seeds the reverse index. Mirrors the existing pattern at `apply.rs:215` where constraint Insert via Raft replication triggers the same rebuild.
- Test coverage: 12 new tests (466 → 478 in the cluster lib). Direct `FkReverseIndex` unit tests in `data_store.rs` (insert/lookup/remove, idempotent inserts, removing unknown source ids, field-scoped keys), `update_fk_reverse_index` and `rebuild_fk_index_for_constraint` unit tests in `constraint_ops.rs` (Insert/Update/Delete paths, no-op when no constraints, malformed JSON, non-FK constraint), and a regression test `import_partition_rebuilds_fk_reverse_index` in `partition_io.rs` that confirmed by fail-on-disable / pass-on-restore that the rebuild call is what makes the assertion pass.
- E2E in `examples/cluster-rebalance-stores/run.sh` now creates 20 extra child comments (2 per parent) spread across all 10 parents and adds a cascade-via-node-4 observation: deletes every parent through node 4 after rebalance, then prints how many of the eligible children were cascade-removed. Surfaced as an observation rather than a hard assertion because cascade outcomes through any specific node depend on whether that node has the FK constraint locally, which is governed by schema/constraint replication topology (separate concern; see below).

### Discovered while running the new E2E (separate follow-up)

- **Constraints don't reach all nodes uniformly.** Across runs of the new E2E, only the leader (node 1) consistently held both the unique and FK constraints locally; nodes 2/3 sometimes had a subset, and a freshly-joined node 4 had none. Because constraints route through `schema_partition(entity)`, any node that doesn't own that partition reaches the constraint only via forwarding — not in its local `db_constraints` store. The FkReverseIndex rebuild this PR adds is correct in its scope (it rebuilds for whatever constraints the importing node has locally), but a fully-correct cascade through every node requires constraints to be cluster-wide broadcast state. Tracked as future work alongside the schema replication topology issue first noted in the 0.3.2 CHANGELOG entry.

## 2026-05-03 — mqdb-cluster 0.3.3

### Fixed

- Unified `import_*` error handling across all six DB stores. Previously `IndexStore::import_entries` propagated `SerializationError` on a malformed entry while the other five (`DbDataStore`, `SchemaStore`, `ConstraintStore`, `UniqueStore`, `FkValidationStore`) silently skipped — a corrupt snapshot stream would either truncate or abort depending on which store hit the bad bytes first. All six now propagate `SerializationError` on deserialize failure.
- Fixed `IndexStore::import_entries` counter accuracy. It previously incremented `imported` even when `add_entry` returned `AlreadyExists`, inflating the count on idempotent snapshot replay. Now only successful inserts are counted; `AlreadyExists` is treated as benign replay and silently swallowed.

## 2026-04-25 — mqdb-cluster 0.3.2

### Fixed

- **Partition snapshot was missing five DB stores.** Following PR #50 (which added `db_data` to the snapshot), `SchemaStore`, `IndexStore`, `UniqueStore`, `FkValidationStore`, and `ConstraintStore` still had **no `export_for_partition` method at all**. A cluster carrying schemas, indexes, unique reservations, in-flight FK validations, or constraints would silently lose them on a rebalance-driven replica promotion — the new primary would have empty metadata and reject queries that depended on them.
- Added `export_for_partition` / `import_*` / `clear_partition` to all five stores. Each filters by the partition function appropriate to that store: `schema_partition(entity)` for schemas and constraints, `index_partition(entity, field, value)` for index entries, `unique_partition(entity, field, value)` for unique reservations, and `data_partition(target_entity, target_id)` for FK validation requests. Wired all five into `StoreManager::export_partition` / `import_partition` / `clear_partition`.
- Added `ClusterConstraint::partition()` helper returning `schema_partition(entity_str())`, since constraints are entity-scoped and live on the same partition as their entity's schema.
- Per-store roundtrip + `clear_partition` tests, plus an integrated test in `partition_io.rs` populating all six DB stores with entries spanning multiple partitions and asserting that exporting one partition correctly partition-filters every store on the destination.

### Known gaps not addressed here

- `FkReverseIndex` (cluster-wide cache of child-to-parent FK references) is **not** included in partition snapshots. It's not partition-scoped — should be either rebuilt during import via the FK constraint discovery path or treated as a separate broadcast entity. Tracked as future work. _Closed in 0.3.4 via the rebuild-during-import approach._

## 2026-04-25 — mqdb-cli 0.7.5

### Fixed

- Release workflow updated for the `LICENSE` file rename (introduced by the 0.7.0/0.8.0 license split). The npm publish step in `.github/workflows/release.yml` was still running `cp LICENSE pkg/` and declaring `"license": "AGPL-3.0-only"` in `pkg/package.json` — both incorrect after `LICENSE` was split into `LICENSE-APACHE` + `LICENSE-AGPL` and `mqdb-wasm` was relicensed to Apache-2.0. The v0.7.4 tag's workflow run failed at the `Copy LICENSE` step before the npm publish could happen, so `mqdb-wasm 0.3.2` was not published. Bumping `mqdb-cli` to 0.7.5 lets us cut a fresh `v0.7.5` tag whose workflow run will use the fixed steps and complete the npm publish.

## 2026-04-24 — mqdb-core 0.7.0, mqdb-agent 0.8.0, mqdb-wasm 0.3.2, mqdb-vault 0.1.0, mqdb-cluster 0.3.1, mqdb-cli 0.7.4

### Added

- New `mqdb-vault` crate (AGPL-3.0-only) housing all vault transparent-encryption logic behind a `VaultBackend` trait defined in `mqdb-agent`. The CLI instantiates `VaultBackendImpl` after the license check; unlicensed agents bind `NoopVaultBackend` and every `$DB/_vault/*` admin topic returns `vault not available`.
- `DbAccess` trait in `mqdb-agent::db_helpers` so vault admin methods work in both agent (direct `Database`) and cluster (MQTT-mediated `MqttDbAccess`) modes without duplicating logic.
- `.githooks/pre-commit` running `cargo make format-check` + `cargo make clippy` on every commit. Activation is per-clone: `git config core.hooksPath .githooks` (documented in `CONTRIBUTING.md`).
- Four unit tests in `mqdb-core::license` covering `check_runtime_expiry` past/future/boundary semantics and the `is_expired` wrapper.
- 11 integration tests in `crates/mqdb-vault/tests/backend_integration.rs` covering NoopBackend `Unavailable` responses, admin passphrase/identity error paths, encrypt-on-write / decrypt-on-read roundtrip with ciphertext-at-rest assertion, admin_change rotation, and admin_disable clearing vault state.
- `export_import_roundtrip_preserves_db_data` test in `mqdb-cluster::store_manager::partition_io` guarding against future regressions in partition snapshot export.

### Changed

- **License split.** `mqdb-core`, `mqdb-agent`, and `mqdb-wasm` are now **Apache-2.0** so downstream consumers can embed them without copyleft obligations. `mqdb-vault`, `mqdb-cluster`, and `mqdb-cli` remain **AGPL-3.0-only** (Pro/Enterprise features + the network-service binary). Dep graph is unidirectional: Apache crates never depend on AGPL crates.
- `LICENSE` renamed to `LICENSE-AGPL`; new `LICENSE-APACHE` added; `NOTICE` rewritten to describe the split; `README.md` licensing section updated; `deny.toml` AGPL exceptions narrowed to the three AGPL crates.
- `VaultBackend` uses native `Pin<Box<dyn Future>>` futures (no `async_trait` crate), matching the existing `AuthProvider` pattern in `topic_protection.rs`.
- `MqdbAgent` no longer owns `vault_key_store`, `vault_min_passphrase_length`, or `vault_unlock_limiter` — those are backend-internal now.
- Six MQTT and six HTTP vault admin handlers in `mqdb-agent` collapsed into thin dispatchers that delegate to the backend (~1000 lines deleted, net shrinkage).
- `handle_message` gates `vault_backend.encrypt_request` and `decrypt_response` on the sync `is_eligible` check so the NoopBackend path costs zero `Box::pin` allocations.
- Cluster HTTP vault admin now works end-to-end via `MqttDbAccess`. Cluster MQTT vault admin remains intentionally unsupported (returns "vault admin not supported in cluster mode" as before).
- `examples/vault-mqtt-admin/run.sh` and `examples/vault-cluster/run.sh` now require `MQDB_LICENSE_FILE` and pass `--license` to the agent/cluster.

### Fixed

- **Partition snapshot export was missing user data.** `StoreManager::export_partition` in `mqdb-cluster` was exporting 9 of 15 entity store types and omitting `db_data`. Not visible in normal operation (primaries write locally and replicate via the Raft log) but on rebalance-driven replica promotion the new primary returned `entity not found` for pre-rebalance records. Fix adds `db_data` to the export array, the `import_partition` match arm, and `clear_partition`.
- Cluster E2E probe loop in `examples/vault-cluster/run.sh` now reads `.data.id` from `mqdb create` output (had been reading `.id` since the script landed, silently leaving 20 ghost records per run and skewing count-based test assertions by 20).
- Cluster vault E2E node 4 `--peers` expanded to all three predecessors so QUIC connections are established to every prior node. Peer auto-discovery via Raft/gossip is tracked as future work.
- Regressions caught in self-review of the vault extraction: `$DB/_vault/change` and `POST /vault/change` parse `old_passphrase` (not `current_passphrase`); MQTT `VaultError::RateLimited` returns `ErrorCode::RateLimited` (429); `identity not found` returns `NotFound` (404); MQTT `InvalidPassphrase` returns `ErrorCode::Forbidden` (matching pre-refactor behavior).

### Removed

- `mqdb-agent::vault_ops` module (560 lines). Replaced by `mqdb-vault::backend::VaultBackendImpl` behind the `VaultBackend` trait.
- `mqdb-core::vault_keys` module. Moved to `mqdb-vault::key_store`.
- `mqdb-agent::vault_transform` module. Moved to `mqdb-vault::transform`.
- `mqdb-agent::http::vault_crypto`. Moved to `mqdb-vault::crypto`.

### Breaking changes

- `mqdb-core` 0.6.0 → **0.7.0**: `vault_keys` module removed from public API; consumers should depend on `mqdb-vault` directly (AGPL) or rely on the `VaultBackend` trait.
- `mqdb-agent` 0.7.2 → **0.8.0**: `vault_ops` module removed; `MqdbAgent` lost vault-specific public fields; six vault admin handlers re-shaped as thin dispatchers over `VaultBackend`. Downstream callers that previously imported vault types from `mqdb-agent` must now import the trait from `mqdb-agent::vault_backend` and the implementation from `mqdb-vault` (under AGPL).

## 2026-04-22 — mqdb-core 0.6.0, mqdb-wasm 0.3.1, mqdb-agent 0.7.2, mqdb-cli 0.7.3

### Added

- New `mqdb-core::query` module — shared helpers for filter matching (`matches_all_filters`), sorting (`sort_results`, `compare_json_values`), range-bound collection, index-key construction, and id extraction — used by both `mqdb-agent` and `mqdb-wasm`
- `FilterOp::from_js_op`, `FilterOp::is_equality`, `FilterOp::is_range`, `Filter::matches_document` helpers
- `WASM_INDEX_PREFIX` constant in `mqdb-core::keys` for the WASM backend's `index/` key prefix
- All 12 WASM example pages redesigned: per-page hero, "How it works" section with Mermaid flow diagrams, numbered "Try it" tour, inline pass/fail test summary panel next to every Run All Tests button
- Schema and constraints pages now use form builders with dropdowns for type / operator / on-delete action instead of free-text JSON

### Fixed

- `mqdb-wasm` `listConstraints` returned `[{}, {}]` in JS because `serde_wasm_bindgen::to_value(&Vec<serde_json::Value>)` loses `Value::Object` map contents — now routes through `serde_json::to_string` + `JSON.parse` (same workaround used by `listRelationships`). Two new regression tests cover this and the parallel fix in `getSchema` for non-primitive defaults.

### Changed

- `mqdb-agent` and `mqdb-wasm` now delegate filter / sort / range-bound / index-key logic to `mqdb-core::query` instead of maintaining parallel copies (~300 duplicated lines removed)
- Deleted `mqdb-wasm/src/encoding.rs` — superseded by `mqdb-core::keys::encode_value_for_index`

## 2026-04-20 — mqdb-wasm 0.3.0

### Added

- Persistent metadata reload: `openPersistent` and `openEncrypted` now restore schemas, constraints, indexes, relationships, and ID counters from IndexedDB on reopen
- Async admin variants: `addSchemaAsync`, `addUniqueConstraintAsync`, `addNotNullAsync`, `addForeignKeyAsync`, `addIndexAsync`, `addRelationshipAsync` — persist definitions to IndexedDB
- Index backfill on `addIndex` / `addIndexAsync` — existing records are indexed immediately
- camelCase JavaScript API: all multi-word methods use camelCase (`addSchema`, `readSync`, etc.) and structs drop the `Wasm` prefix (`Database`, `Cursor`)
- `in` filter operator for set-membership queries
- `$DB/_admin/index/*/add` and `$DB/_catalog` admin operations via `execute()`
- Atomic batch writes for CRUD operations (data + index entries written in a single transaction)
- CAS precondition on update and delete via `expect_value` in batch writes

### Fixed

- SetNull cascade now bumps `_version`, updates index entries, and dispatches update events
- Unique constraint validation uses index lookup (O(1)) instead of full entity scan (O(N))
- `serde_wasm_bindgen` Map/Object mismatch: `to_value` produces JS `Map` for struct options, causing `#[serde(default)]` to silently drop filter/sort/pagination fields — fixed by routing through `JSON.parse` for struct deserialization targets

## 2026-04-15 — mqdb-core 0.5.2, mqdb-agent 0.7.1

### Added

- Index definitions persist to `meta/index/{entity}` and reload on agent startup — indexes survive restarts without re-issuing `index add`

## 2026-04-10 — mqdb-core 0.5.1, mqdb-agent 0.7.0, mqdb-cli 0.7.2

### Added

- `MqdbAgent::start()` method that returns a `JoinHandle` and a `watch::Receiver<bool>` readiness signal, firing only after both the TCP accept loop and the internal `$DB/#` handler are ready
- Handler readiness oneshot in `spawn_handler_task` — signals after the `$DB/#` subscribe succeeds

### Fixed

- Replace hardcoded 500ms sleep in CLI tests with deterministic `start()` + `ready_rx` readiness signal
- Replace `wait_for_port` + `wait_for_ready` polling in admin tests with `start()` + `ready_rx`
- Replace static port counters with OS-assigned ephemeral ports in all test suites (agent, cli, cluster) to eliminate cross-binary port collisions
- Ensure database directory tree exists before fjall open to prevent EBADF on `FROM scratch` Docker images
- Direct tracing subscriber output to stderr in CLI to prevent log lines from corrupting JSON stdout

## 2026-04-10 — mqdb-core 0.5.1, mqdb-agent 0.6.1

### Security

- Cap password length at 256 bytes to prevent Argon2id CPU exhaustion
- Add rate limiters to vault enable/change MQTT handlers and OAuth token refresh endpoint
- Validate entity names (alphanumeric, `_`, `-`, max 128 chars) and record IDs (reject `+`, `#`, `/`, max 512 bytes) in topic parsers
- Reject JSON payloads over 4 MiB before parsing
- Normalize challenge error messages to prevent internal status leakage
- Replace bare SHA256 with HMAC-SHA256 for email hash fallback

## 2026-04-05 — mqdb-core 0.5.0, mqdb-agent 0.6.0, mqdb-cli 0.7.0

### Added

- Password reset endpoints: `POST /auth/password/reset/start` and `POST /auth/password/reset/submit` (HTTP, unauthenticated) for "forgot password" flow
- Password reset MQTT topics: `$DB/_auth/password/reset/start` and `$DB/_auth/password/reset/submit` for authenticated users
- Challenge `purpose` field to distinguish password reset from email verification challenges
- Purpose guard in `handle_verify_submit` to reject password reset challenges
- `--no-rate-limit` now disables all HTTP rate limiters (login, register, verify, password change, password reset)
- `AdminRequired` topic protection now falls through to ACL for non-admin users, enabling operator-provisioned service accounts

### Security

- Promote `$DB/_verify/#` to `AdminRequired` topic protection tier to prevent leakage of verification codes and receipt spoofing

## 2026-04-04 — mqdb-core 0.4.0, mqdb-agent 0.5.0, mqdb-cluster 0.3.0, mqdb-cli 0.6.0

### Added

- Password change endpoint: `POST /auth/password/change` (HTTP) and `$DB/_auth/password/change` (MQTT) for email-auth users with verified email
- `$DB/_auth/` topic namespace for self-service auth operations, exempt from topic protection
- MQTT 5.0 `correlation_data` echoing in all DB and admin response handlers, enabling `mqttv5 --wait-response` and standard request-response clients
- Dedicated `password_change_rate_limiter` (HTTP) and reuse of `vault_unlock_limiter` (MQTT) for brute-force protection

### Changed

- Cluster mode returns explicit error for `$DB/_auth/` topics (agent-only)

## 2026-04-03 — mqdb-core 0.3.0, mqdb-agent 0.4.0, mqdb-cluster 0.2.0, mqdb-cli 0.5.0

### Added

- MQTT vault admin operations: `$DB/_vault/{enable,unlock,lock,disable,change,status}` — self-service vault management over MQTT 5.0 request-response, no HTTP session required
- Shared `vault_ops` module extracting transport-agnostic vault batch operations from HTTP handlers
- Direct-DB vault operations (`_db` variants) for the MQTT handler path, avoiding deadlock from nested MQTT round-trips in the sequential message handler loop
- `ErrorCode::RateLimited` (429) for vault unlock brute-force protection over MQTT
- Topic protection exemptions for `$DB/_vault/*` and `$DB/_verify/*` (non-admin users can access these)
- `--vault-min-passphrase-length` flag (env: `MQDB_VAULT_MIN_PASSPHRASE_LENGTH`, default 0) to enforce minimum passphrase length on vault enable and change operations

### Changed

- Vault HTTP handlers refactored to thin wrappers over shared `vault_ops` functions
- Cluster mode returns explicit error for vault admin topics (vault requires agent mode)

## 2026-04-02 — mqdb-agent, mqdb-cli

### Added

- Email/password registration and login via `--email-auth` flag (`POST /auth/register`, `POST /auth/login`)
- Provider-agnostic email verification protocol over MQTT (`POST /auth/verify/start`, `POST /auth/verify/submit`, `GET /auth/verify/status`)
- MQTT verifier contract: challenges published to `$DB/_verify/challenges/{method}`, receipts on `$DB/_verify/receipts/{challenge_id}`
- Background receipt handler for delivery/failure/attestation status updates
- Periodic cleanup of expired verification challenges
- Argon2id password hashing with configurable parameters
- Rate limiting on register, login, and verification endpoints
- `MQDB_EMAIL_AUTH` environment variable

### Changed

- OAuth client secret no longer required when `--email-auth` is used without OAuth providers

## 2026-03-30 — mqdb-cli

### Added

- Environment variable support for all `agent start` and `cluster start` CLI flags (`MQDB_BIND`, `MQDB_DB`, `MQDB_DURABILITY`, `MQDB_NODE_ID`, etc.)
- Inline content environment variables for file-path flags: `MQDB_PASSWD`, `MQDB_ACL`, `MQDB_SCRAM`, `MQDB_JWT_KEY`, `MQDB_PASSPHRASE`, `MQDB_LICENSE`, `MQDB_QUIC_CERT`, `MQDB_QUIC_KEY`, `MQDB_QUIC_CA`, `MQDB_OAUTH_CLIENT_SECRET`, `MQDB_IDENTITY_KEY`, `MQDB_FEDERATED_JWT_CONFIG`, `MQDB_CERT_AUTH`
- Precedence: CLI flags > inline env vars (`MQDB_*`) > file-path env vars (`MQDB_*_FILE`)

## 2026-03-28 — mqdb-core, mqdb-agent, mqdb-cli

### Added

- OpenTelemetry tracing with OTLP export for agent mode (`--otlp-endpoint`, `--otel-service-name`, `--otel-sampling-ratio`)
- `#[instrument]` spans on all DB operations (create, read, update, delete, list) and schema/constraint changes
- `Request::operation_label()` method on mqdb-core transport types
- W3C traceparent correlation from incoming MQTT messages through to DB operation spans

### Changed

- Switched to independent per-crate versioning (workspace version removed)
- Deferred tracing subscriber initialization for `agent start` to avoid conflict with mqtt-lib's OTLP subscriber
- Extracted `AgentStartFields` struct from `AgentAction::Start` enum variant (clippy large_enum_variant fix)

## 2026-03-23 — initial release

### Added

- MQTT 5.0 broker with embedded JSON document database
- Standalone agent mode with full CRUD operations (create, read, update, delete, list)
- Distributed clustering with 256-partition sharding and RF=2 replication
- QUIC transport for inter-node communication with mTLS
- Raft consensus for cluster coordination
- Cross-node pub/sub routing with topic index and wildcard subscriptions
- Schema validation (JSON Schema draft 2020-12)
- Unique constraints with distributed 2-phase protocol
- Cascade delete with cross-ownership protection
- Owner-aware access control per entity
- ACL/RBAC for MQTT topic authorization
- Password authentication (plaintext file and SCRAM-SHA-256)
- JWT authentication (HS256, HS384, RS256, RS384, ES256, ES384)
- Vault integration for transparent field-level encryption (AES-256-GCM)
- Session migration and cleanup on node departure
- QoS 0/1/2 with state replication across cluster
- Last Will and Testament support
- Retained messages with TTL-based dedup
- WebSocket transport
- TLS support (TCP and WebSocket)
- WASM client library (mqdb-wasm) for browser-based applications
- CLI with CRUD commands, benchmarking, cluster management, and dev tooling
- Change event subscriptions (`$DB/{entity}/events/#`)
- Filter operators: `=`, `<>`, `>`, `<`, `>=`, `<=`, `~` (glob), `?` (null), `!?` (not null)
- Field projection on read and list operations
- Async pipelined benchmarking mode with QoS 1 backpressure
