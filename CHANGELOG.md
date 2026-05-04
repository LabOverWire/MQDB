# Changelog

All notable changes to this project will be documented in this file.

Each entry lists the date and the crate versions that were released.

## 2026-05-03 — mqdb-cluster 0.3.4

### Fixed

- **Partition snapshot import did not populate `FkReverseIndex`.** This was the "Known gap" called out in the 0.3.2 entry. After a rebalance-driven replica promotion, the new primary held the imported `db_data` records and FK constraints but its in-memory reverse-index cache (`(target_entity, target_id, source_entity, source_field) → {source_id, …}`) was empty for those records. `start_fk_reverse_lookup` and `handle_fk_reverse_lookup_request` would return empty for any record sitting on a newly-imported partition, causing ON DELETE CASCADE to miss children that the new primary owned and ON DELETE RESTRICT to silently allow deletes that should have been blocked.
- `StoreManager::import_partition` now calls a new private `rebuild_fk_indexes_after_import` step at the end of the import. It iterates every registered FK constraint and calls the existing `rebuild_fk_index_for_constraint` helper, which walks `db_data.list(source_entity)` (now populated with the just-imported records) and seeds the reverse index. Mirrors the existing pattern at `apply.rs:215` where constraint Insert via Raft replication triggers the same rebuild.
- Test coverage: 12 new tests (466 → 478 in the cluster lib). Direct `FkReverseIndex` unit tests in `data_store.rs` (insert/lookup/remove, idempotent inserts, removing unknown source ids, field-scoped keys), `update_fk_reverse_index` and `rebuild_fk_index_for_constraint` unit tests in `constraint_ops.rs` (Insert/Update/Delete paths, no-op when no constraints, malformed JSON, non-FK constraint), and a regression test `import_partition_rebuilds_fk_reverse_index` in `partition_io.rs` that confirmed by fail-on-disable / pass-on-restore that the rebuild call is what makes the assertion pass.
- E2E in `examples/cluster-rebalance-stores/run.sh` now creates 20 extra child comments spread across all 10 parents and ends with a hard assertion: deletes every parent through node 4 after rebalance, then verifies every child comment is gone. Without the snapshot fix, children whose data partition rebalanced to node 4 survive the cascade.

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

- `FkReverseIndex` (cluster-wide cache of child-to-parent FK references) is **not** included in partition snapshots. It's not partition-scoped — should be either rebuilt during import via the FK constraint discovery path or treated as a separate broadcast entity. Tracked as future work.

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
