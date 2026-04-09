# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.7.0] - 2026-04-05

Affected crates: mqdb-core (0.5.0), mqdb-agent (0.6.0), mqdb-cli (0.7.0).

### Added

- Password reset endpoints: `POST /auth/password/reset/start` and `POST /auth/password/reset/submit` (HTTP, unauthenticated) for "forgot password" flow
- Password reset MQTT topics: `$DB/_auth/password/reset/start` and `$DB/_auth/password/reset/submit` for authenticated users
- Challenge `purpose` field to distinguish password reset from email verification challenges
- Purpose guard in `handle_verify_submit` to reject password reset challenges
- `--no-rate-limit` now disables all HTTP rate limiters (login, register, verify, password change, password reset)

### Security

- Promote `$DB/_verify/#` to `AdminRequired` topic protection tier to prevent leakage of verification codes and receipt spoofing

## [0.6.0] - 2026-04-04

Affected crates: mqdb-core (0.4.0), mqdb-agent (0.5.0), mqdb-cluster (0.3.0), mqdb-cli (0.6.0).

### Added

- Password change endpoint: `POST /auth/password/change` (HTTP) and `$DB/_auth/password/change` (MQTT) for email-auth users with verified email
- `$DB/_auth/` topic namespace for self-service auth operations, exempt from topic protection
- MQTT 5.0 `correlation_data` echoing in all DB and admin response handlers, enabling `mqttv5 --wait-response` and standard request-response clients
- Dedicated `password_change_rate_limiter` (HTTP) and reuse of `vault_unlock_limiter` (MQTT) for brute-force protection

### Changed

- Cluster mode returns explicit error for `$DB/_auth/` topics (agent-only)

## [0.5.0] - 2026-04-03

Affected crates: mqdb-core (0.3.0), mqdb-agent (0.4.0), mqdb-cluster (0.2.0), mqdb-cli (0.5.0).

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

## [0.4.0] - 2026-04-02

Affected crates: mqdb-agent, mqdb-cli.

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

## [0.3.0] - 2026-03-30

Affected crates: mqdb-cli.

### Added

- Environment variable support for all `agent start` and `cluster start` CLI flags (`MQDB_BIND`, `MQDB_DB`, `MQDB_DURABILITY`, `MQDB_NODE_ID`, etc.)
- Inline content environment variables for file-path flags: `MQDB_PASSWD`, `MQDB_ACL`, `MQDB_SCRAM`, `MQDB_JWT_KEY`, `MQDB_PASSPHRASE`, `MQDB_LICENSE`, `MQDB_QUIC_CERT`, `MQDB_QUIC_KEY`, `MQDB_QUIC_CA`, `MQDB_OAUTH_CLIENT_SECRET`, `MQDB_IDENTITY_KEY`, `MQDB_FEDERATED_JWT_CONFIG`, `MQDB_CERT_AUTH`
- Precedence: CLI flags > inline env vars (`MQDB_*`) > file-path env vars (`MQDB_*_FILE`)

## [0.2.0] - 2026-03-28

Affected crates: mqdb-core, mqdb-agent, mqdb-cli.

### Added

- OpenTelemetry tracing with OTLP export for agent mode (`--otlp-endpoint`, `--otel-service-name`, `--otel-sampling-ratio`)
- `#[instrument]` spans on all DB operations (create, read, update, delete, list) and schema/constraint changes
- `Request::operation_label()` method on mqdb-core transport types
- W3C traceparent correlation from incoming MQTT messages through to DB operation spans

### Changed

- Switched to independent per-crate versioning (workspace version removed)
- Deferred tracing subscriber initialization for `agent start` to avoid conflict with mqtt-lib's OTLP subscriber
- Extracted `AgentStartFields` struct from `AgentAction::Start` enum variant (clippy large_enum_variant fix)

## [0.1.0] - 2026-03-23

Initial open-source release.

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
