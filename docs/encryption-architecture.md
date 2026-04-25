# MQDB Encryption Architecture

> **Scope:** this document covers **server-side** encryption — Vault and Identity — as implemented in `mqdb-vault` and `mqdb-agent`. The client-side `mqdb-wasm` crate has its own, **separate** encryption feature exposed as `Database.openEncrypted(name, passphrase)`. It shares crypto primitives (AES-256-GCM, PBKDF2-SHA256, 600k iterations) with Vault but is a different system with a different threat model: it encrypts the whole client-side IndexedDB at the storage layer to keep plaintext off the user's device. It is **not** a port of Vault, has no lock/unlock state machine, no per-entity opt-in, and no operator-side admin endpoints. See `crates/mqdb-wasm/README.md` ("Client-Side Encryption vs Server-Side Vault") for the comparison and `crates/mqdb-wasm/src/crypto.rs` for the implementation.

MQDB implements two independent encryption-at-rest systems on the server: **Vault** (user-controlled) and **Identity** (server-managed). Both use AES-256-GCM authenticated encryption. This document describes the verified architecture as implemented in code.

---

## 1. Vault Encryption (User-Controlled)

### Purpose

Encrypts user-owned entity records with a user-provided passphrase. Keys exist only in process memory and are lost on restart. This protects against stolen storage media, exposed database dumps, and unauthorized disk-level access.

### Key Derivation

| Parameter | Value | Source |
|-----------|-------|--------|
| Algorithm | PBKDF2-HMAC-SHA256 | `vault_crypto.rs:29` |
| Iterations | 600,000 | `vault_crypto.rs:16` |
| Salt | 32 bytes, random, per-user | `vault_crypto.rs:14` |
| Output | 256-bit AES-256-GCM key | `vault_crypto.rs:13,42` |

The derived key is stored in a `VaultKeyStore` as `Zeroizing<Vec<u8>>` — memory is overwritten with zeros on drop. No key material is ever written to disk.

### Passphrase Verification

A check token is created by encrypting the constant `b"mqdb-vault-check-v1"` with the derived key (`vault_crypto.rs:17,87-88`). On unlock, the system re-derives the key and attempts to decrypt the stored check token. If the plaintext matches, the passphrase is correct. No plaintext passphrase or hash is stored.

Unlock attempts are rate-limited to 5 per user per minute (`agent.rs:330`, `rate_limiter.rs:9`).

### Field-Level Encryption

Vault encryption operates at the JSON field level, recursively encrypting every leaf value in a record.

**Per-field encryption:**

1. Generate a random 12-byte nonce (`vault_crypto.rs:209-212`)
2. For non-string values (numbers, booleans, null), prepend `\x01` before encryption (`vault_crypto.rs:146-151`). On decryption, the prefix signals that the plaintext should be parsed back to its original JSON type (`vault_crypto.rs:176-180`)
3. Construct AAD as `"{entity}:{record_id}"` (`vault_crypto.rs:216-217`). This binds ciphertext to a specific record, preventing copy-paste attacks between records
4. Encrypt with AES-256-GCM, producing ciphertext + 16-byte authentication tag
5. Store as `base64(nonce || ciphertext || tag)` (`vault_crypto.rs:218-229`)

**Recursive traversal** (`vault_transform.rs:59-89`):

- String leaf values: encrypted
- Number/bool/null leaf values: encrypted with `\x01` type prefix
- Objects: recurse into each field
- Arrays: recurse into each element

**Skipped fields** (`vault_transform.rs:14-25`):

- `id` (top-level): needed for routing
- Owner field (top-level): needed for access control
- Fields starting with `_` (any depth): system metadata

**Example:**

```
Input:  {"id": "rec-1", "name": "Alice", "profile": {"city": "Paris"}, "age": 30}
Output: {"id": "rec-1", "name": "base64(...)", "profile": {"city": "base64(...)"}, "age": "base64(...)"}
```

### Update Operations

Updates follow a read-modify-write pattern (`handlers.rs:247-315`):

1. Read the existing encrypted record from the database
2. Decrypt all fields
3. Merge the update delta into the decrypted record
4. Re-encrypt the merged result
5. Write the fully encrypted record back

This is necessary because individual encrypted fields cannot be updated in isolation — the AAD binds each field's ciphertext to the record.

### Vault Lifecycle

```
Disabled ──enable──→ Enabled+Unlocked
  ↑                      ↓       ↑
  └─────disable─────────┘    lock ↓ unlock
                           Enabled+Locked
```

| Operation | What happens | Cost |
|-----------|-------------|------|
| **Enable** | Derive key from passphrase. Batch-encrypt all user's owned records. Store salt and check token on identity record. Hold key in memory. | O(n) records |
| **Lock** | Remove key from `VaultKeyStore`. Data stays encrypted. | O(1), instant |
| **Unlock** | Re-derive key from passphrase. Verify against check token. Store key in memory. | O(1), unless crash recovery needed |
| **Disable** | Verify passphrase. Batch-decrypt all records to plaintext. Clear salt, check token, and key. | O(n) records |
| **Change passphrase** | Verify old passphrase. Generate new salt, derive new key. Batch re-encrypt all records (decrypt with old key, encrypt with new). Persist old salt during migration for crash recovery. | O(n) records |

### Crash Recovery (Saga Pattern)

Batch operations (enable, disable, change) can be interrupted by crashes. The saga pattern ensures convergence to a correct state:

1. **Before batch**: persist `vault_migration_status: "pending"` and `vault_migration_mode` ("encrypt", "decrypt", or "re_encrypt") on the identity record (`handlers.rs:1457-1458, 1687-1689, 1831-1835`)
2. **Execute batch idempotently**: re-encrypting an already-encrypted field produces valid (different) ciphertext; skipping a value that fails decryption leaves it unchanged
3. **After batch**: set `vault_migration_status: "complete"`
4. **On next unlock**: if `migration_status == "pending"`, resume the recorded migration mode (`handlers.rs:2002-2075`)

For passphrase changes, the old salt is persisted as `vault_old_salt` so the old key can be re-derived during recovery.

### Write Fences

Batch operations must not interleave with concurrent MQTT operations on the same user's data. A per-user `RwLock` fence prevents this (`vault_keys.rs:58-83`):

- Batch operations acquire an **exclusive write fence** (`acquire_fence`)
- MQTT handlers acquire a **shared read fence** (`read_fence`) before processing vault-eligible requests (`handlers.rs:102-104`)
- While a batch holds the write fence, all MQTT operations for that user block until the batch completes

This guarantees clients never observe a mixed state (some fields encrypted, others plaintext).

---

## 2. Identity Encryption (Server-Managed)

### Purpose

Encrypts OAuth infrastructure records (`_identities`, `_identity_links`, `_oauth_tokens`) transparently. Always active when OAuth is configured (`agent.rs:118-126,334-382`). Users never interact with this system directly.

### Key Hierarchy

```
First startup
    │
    ├─ Generate 256-bit identity key (SystemRandom)     identity_crypto.rs:54-59
    ├─ Generate 32-byte random salt                      identity_crypto.rs:61-63
    ├─ Derive wrapping key:
    │    PBKDF2-HMAC-SHA256(
    │      password = KEY_DERIVATION_INFO,               identity_crypto.rs:20
    │      salt = random 32 bytes,
    │      iterations = 600,000
    │    )                                               identity_crypto.rs:223-237
    ├─ Wrap identity key with AES-256-GCM                identity_crypto.rs:239-259
    └─ Store wrapped key + salt to .identity_key file    agent.rs:371-377

Subsequent startups
    │
    ├─ Load .identity_key file
    ├─ Re-derive wrapping key from stored salt
    ├─ Unwrap identity key
    └─ Derive purpose-specific keys via HKDF-SHA256:     identity_crypto.rs:99-129
         ├─ info="aes-encryption"  → encryption key
         └─ info="hmac-blind-index" → HMAC key
```

The wrapping key password is a hardcoded constant (`b"mqdb-identity-encryption-seed-v1"`). This means an attacker with both the `.identity_key` file and the binary can derive the wrapping key. The threat model assumes the binary itself is trusted; a future improvement could derive from an operator-provided secret.

### Field Encryption

Same AES-256-GCM scheme as vault encryption, with one difference:

- **AAD** = entity name only (`identity_crypto.rs:141,171`), not `entity:record_id`. This allows the same key to encrypt fields across all records in an entity without needing to know the record ID at query time.

### Blind Indexes

Identity encryption supports exact-match queries on encrypted fields via blind indexes:

```
blind_index(entity, value) = hex(HMAC-SHA256(hmac_key, "{entity}:{value}"))
```
(`identity_crypto.rs:183-187`)

The blind index is deterministic — the same input always produces the same output — so it can be stored alongside the encrypted field and used for equality lookups without decryption. Range queries and prefix matching are not supported.

---

## 3. Cluster Mode Behavior

In cluster mode, vault keys remain on the node where the user authenticated. Encrypted data traverses the cluster; keys never do.

### Architecture

```
Node A (user authenticated)              Node B (partition primary)
  │                                           │
  ├─ Encrypt fields locally                   │
  ├─ Forward encrypted payload ──────────────→│ Store encrypted data
  │                                           │
  ├─ Read request ───────────────────────────→│ Return encrypted data
  │← Encrypted response ─────────────────────┤
  ├─ Decrypt locally with user's key          │
  └─ Return plaintext to client               │
```

**Verification**: The `ClusterMessage` enum and `JsonDbRequest` struct contain no fields for vault keys or cryptographic material (`protocol/mod.rs`, `db_messages.rs:7-16`). Keys are resolved locally from `VaultKeyStore` only (`db_handler/mod.rs:166-175`).

### Operation Details

**Create** (`json_ops.rs:297-313`): Encrypt fields on authenticating node → forward encrypted payload → partition primary stores encrypted data.

**Read** (`db_ops.rs:484-516`): Partition primary returns encrypted data → authenticating node calls `vault_decrypt_forwarded_response()` with locally-held key.

**Update** (`json_ops.rs:153-164`): Authenticating node encrypts the update delta → forwards to partition primary → primary merges encrypted delta with stored encrypted data.

**List** (`query.rs:69-252`): When vault encryption is active, the authenticating node sends an **empty filter payload** to remote partitions (`query.rs:95-97`) — this prevents filter content from being disclosed. All partitions return their full (encrypted) record sets. The authenticating node:
1. Decrypts all records locally (`query.rs:191-216`)
2. Applies filter predicates on plaintext (`query.rs:218-227`)
3. Applies sorting (`query.rs:229-236`)
4. Applies projection (`query.rs:239-240`)

This means list operations on vault-encrypted entities require a full scan — server-side filtering is impossible when data is encrypted.

---

## 4. Threat Model

### Protected Against

- Stolen storage media (disk, backup tapes)
- Exposed database dumps
- Unauthorized storage-level access (operators reading files directly)
- Other users on the same server reading encrypted records
- Cross-record ciphertext transplant (AAD binding)

### Not Protected Against

- Compromised running server process (plaintext visible in memory during processing)
- Malicious server operator (can modify binary, inspect process memory)
- Network eavesdropping (use TLS/QUIC for transport security)
- Weak passphrases

### Passphrase Strength Reference

With 600,000 PBKDF2 iterations (~5 guesses/second/core):

| Passphrase strength | Single core | 1,000 GPU cores |
|---------------------|------------|-----------------|
| 4-digit PIN | 33 minutes | 2 seconds |
| 2 random words (diceware) | 187 years | 67 days |
| 4 random words (diceware) | ~10^9 years | ~10^6 years |

---

## 5. Key Source Files

| Component | File |
|-----------|------|
| Vault crypto primitives | `crates/mqdb-agent/src/http/vault_crypto.rs` |
| Field encrypt/decrypt helpers | `crates/mqdb-agent/src/vault_transform.rs` |
| Identity crypto | `crates/mqdb-agent/src/http/identity_crypto.rs` |
| Key store (memory + fences) | `crates/mqdb-core/src/vault_keys.rs` |
| Agent MQTT handler integration | `crates/mqdb-agent/src/agent/handlers.rs` |
| Agent HTTP vault endpoints | `crates/mqdb-agent/src/http/handlers.rs` |
| Cluster field encryption | `crates/mqdb-cluster/src/cluster/db_handler/json_ops.rs` |
| Cluster response decryption | `crates/mqdb-cluster/src/cluster/node_controller/db_ops.rs` |
| Cluster list gather+decrypt | `crates/mqdb-cluster/src/cluster/node_controller/query.rs` |
| Cluster vault key resolution | `crates/mqdb-cluster/src/cluster/db_handler/mod.rs` |
