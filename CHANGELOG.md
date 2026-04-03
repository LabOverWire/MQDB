# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

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
