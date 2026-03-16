# OAuth & Vault — Future Work

Architectural issues identified during PR #13 review that require deeper design work.

## Batch Vault Operations Are Not Atomic

`batch_vault_operation` in `handlers.rs` iterates over all user entities and encrypts/decrypts records one by one. If the process crashes mid-batch, some records are encrypted and some are plaintext. The `vault_check` token on the identity record indicates the vault *should* be enabled, but the data is in a mixed state.

**Impact**: Data corruption after crash during vault enable/disable/change.
**Mitigation**: The write fence prevents concurrent modifications during batch, but doesn't help with crash recovery.
**Potential fix**: Store a `vault_migration_status` field on the identity (e.g., `pending`, `complete`). On next unlock, if status is `pending`, re-run the batch. Alternatively, use a WAL-style approach where the batch operation is logged before execution.

## vault_pre_update TOCTOU in Agent Handlers

`crates/mqdb-agent/src/agent/handlers.rs` reads the entity record, checks if vault encryption applies, encrypts fields, then publishes the update. Between the read and the publish, the vault state could change (enable/disable). The write fence only protects the batch operation itself, not individual CRUD operations happening concurrently.

**Impact**: Low — the window is very small and requires exact timing. A record could be double-encrypted or left plaintext during a vault state transition.
**Potential fix**: Include a vault generation counter in the entity metadata. On write, check that the generation matches. If not, re-encrypt with the current key.

## Identity Race Condition Needs Unique Constraint

`resolve_or_create_identity` in `handlers.rs` does read-then-create for `_identities`. Two concurrent OAuth callbacks for the same email could both find no existing identity and both create one, resulting in duplicate canonical IDs for the same user.

**Impact**: Medium — user ends up with two identities instead of one, linked to different providers. Can cause confusion but not data loss.
**Potential fix**: Add a unique constraint on the `email` field (or `email_hash` when `IdentityCrypto` is active) of `_identities`. The existing MQDB unique constraint mechanism (`$DB/_admin/constraint/{entity}/add`) supports this. Requires the identity creation flow to handle constraint violation errors and retry with the existing record.

## Schema and Constraint Initialization

The internal entities (`_identity_links`, `_identities`, `_oauth_tokens`) don't have schemas or constraints set up automatically when the broker starts. This means:
- No unique constraints on `email` to prevent duplicates
- No schema validation on required fields
- Relies entirely on application-level code to maintain data integrity

**Impact**: Low — the application code is the only writer, so data integrity is maintained in practice.
**Potential fix**: Add an initialization step in the HTTP server startup that creates schemas and constraints for internal entities via the MQTT admin API. This would provide defense-in-depth.
