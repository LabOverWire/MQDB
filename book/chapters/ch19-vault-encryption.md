# Chapter 19: Vault Encryption and Data Protection

Chapter 18 covered how MQDB authenticates users and authorizes their actions — who can connect, which topics they can publish to, and which records they own. But authentication and authorization answer the question of *access*: who is allowed to interact with the data. They say nothing about *exposure*: what happens when someone gains access to the storage layer itself — a stolen disk, a backup tape, a database file left on a decommissioned server.

This chapter covers MQDB's two encryption-at-rest systems: the *vault*, which gives users control over their own data's encryption with a passphrase only they know, and *identity encryption*, which protects OAuth credentials with a server-managed key. Both use AES-256-GCM authenticated encryption, but they serve different threat models and operate at different layers of the system.

> **Note**: The vault system is under active development. This chapter describes the functionality that exists in the codebase as of writing. Sections marked with *(planned)* describe known gaps and future work.

## 19.1 Two Threat Models

Authentication prevents unauthorized *access*. Encryption prevents unauthorized *reading*. These are complementary protections, not alternatives.

Consider a multi-tenant MQDB deployment where users authenticate via OAuth and store data in entities they own. Without encryption at rest, anyone with access to the storage files — a system administrator, a backup operator, an attacker who exfiltrates a disk image — can read every record in plaintext. Authentication is irrelevant; the data is just bytes on disk.

MQDB addresses this with two separate encryption systems, each targeting a different class of data:

| System | Protects | Key Source | User Control | Always On |
|--------|----------|------------|-------------|-----------|
| Vault | User-owned entity records | User passphrase | Enable/disable/lock/unlock | No |
| Identity Crypto | OAuth identity records | Server-generated key | None | Yes |

The vault encrypts entity data that users create through the MQTT API — notes, documents, sensor readings, anything stored in entities with ownership enabled. The user provides a passphrase; the server derives an encryption key; records are encrypted field by field. The user can lock the vault (removing the key from memory, rendering reads opaque) and unlock it later by re-entering the passphrase.

Identity encryption protects the OAuth infrastructure itself — email addresses, provider tokens, identity links. These records are created and managed by the server during the OAuth flow, not by the user directly. The encryption key is generated randomly and stored wrapped alongside the data. The user never sees or manages this key.

The separation matters because the threat models differ. Vault encryption protects user data from server operators (the user controls the passphrase). Identity encryption protects OAuth credentials from storage-level exposure (the server controls the key but does not expose it through any API). Neither system provides end-to-end encryption — the server sees plaintext during processing. Both protect data at rest.

## 19.2 Vault Crypto Primitives

The vault's cryptographic foundation is AES-256-GCM with PBKDF2 key derivation. A user provides a passphrase; MQDB derives a 256-bit key; that key encrypts and decrypts individual field values within JSON records.

**Key derivation** uses PBKDF2-HMAC-SHA256 with 600,000 iterations and a 32-byte random salt generated per user. The iteration count is deliberately high — at 600K rounds, a single derivation takes roughly 200ms on modern hardware, making brute-force attacks against stolen ciphertexts computationally expensive. The salt ensures that two users with the same passphrase produce different keys.

**Encryption** operates at the field level, not the record level. Given a JSON record like `{"id": "rec-1", "name": "Alice", "email": "alice@example.com", "age": 30}`, the vault encrypts each string field independently. Non-string values (numbers, booleans, null) pass through unchanged — AES-256-GCM operates on byte sequences, and encrypting a number would require serializing it to bytes, encrypting, base64-encoding, and storing as a string, losing type information in the process.

Each encrypted value is formatted as `base64(nonce || ciphertext || tag)`:

| Component | Size | Purpose |
|-----------|------|---------|
| Nonce | 12 bytes | Unique per encryption, prevents replay |
| Ciphertext | Variable | Encrypted field value |
| Tag | 16 bytes | Authentication tag, prevents tampering |

The nonce is generated randomly for each field encryption using the system's cryptographic random number generator. Reusing a nonce with the same key would be catastrophic — it would allow an attacker to recover plaintext by XORing two ciphertexts. Random 12-byte nonces provide a collision probability of approximately 2^-48 after a billion encryptions, which is acceptable for a database with per-user keys.

**Additional authenticated data** (AAD) binds each ciphertext to its record. The AAD string is `"{entity}:{record_id}"` — for example, `"notes:rec-1"`. This means a ciphertext encrypted for record `rec-1` cannot be copied to record `rec-2` and decrypted successfully. The authentication tag verification will fail because the AAD does not match. This prevents a class of attacks where an adversary with write access (but not the encryption key) swaps ciphertext between records to confuse users.

**Skip fields** control which fields are encrypted. Two categories are always excluded: fields starting with `_` (system-managed metadata like `_created_at`, `_version`) and explicitly listed fields (typically `id` and the owner field). These fields must remain in plaintext for the database to function — you cannot route a query to the correct partition if the `id` is encrypted.

**Passphrase verification** uses a check token: a known constant (`b"mqdb-vault-check-v1"`) encrypted with the derived key. When a user attempts to unlock the vault, the server derives the key from the provided passphrase and the stored salt, then tries to decrypt the check token. If decryption succeeds and the plaintext matches the expected constant, the passphrase is correct. If it fails, the passphrase is wrong. This avoids storing the passphrase or its hash — only the encrypted check token and salt are persisted.

## 19.3 The Vault Lifecycle

The vault has five operations, each represented as an HTTP endpoint. The lifecycle forms a state machine with two axes: *enabled* (whether encryption is configured) and *unlocked* (whether the key is in memory).

```
                   ┌─────────────────────────────────────┐
                   │                                     │
                   ▼                                     │
 ┌──────────┐  enable  ┌──────────────────┐  disable  ┌─┴────────────────┐
 │ Disabled │ ───────► │ Enabled+Unlocked │ ────────► │     Disabled     │
 └──────────┘          └──────────────────┘           └──────────────────┘
                          │           ▲
                    lock  │           │  unlock
                          ▼           │
                       ┌──────────────┴──┐
                       │ Enabled+Locked  │
                       └─────────────────┘
```

**Enable** (`POST /vault/enable`): The user provides a passphrase. The server generates a random 32-byte salt, derives the AES-256-GCM key using PBKDF2, creates a check token, then encrypts every record the user owns across all entities. The salt, check token, and vault-enabled flag are stored on the user's identity record. The derived key is stored in the in-memory key store. After enable, the vault is both enabled and unlocked.

The batch encryption is the expensive part. For a user who owns 500 records across three entities, enable reads each record, encrypts its string fields, and writes it back as an update. This is why enable acquires an exclusive fence (described in Section 19.4) — concurrent reads during batch encryption would see a mix of encrypted and plaintext records.

**Lock** (`POST /vault/lock`): Removes the key from the in-memory key store. No data is modified — the records remain encrypted at rest. Subsequent MQTT reads return encrypted ciphertext because no key is available for decryption. Lock is instantaneous.

**Unlock** (`POST /vault/unlock`): The user provides the passphrase. The server decodes the stored salt, derives the key, and verifies it against the check token. If verification succeeds, the key is stored in the in-memory key store. Reads return plaintext again. Unlock is rate-limited to 5 attempts per user to prevent brute-force passphrase guessing.

**Disable** (`POST /vault/disable`): The reverse of enable. The user provides the passphrase for confirmation. The server verifies the passphrase, then batch-decrypts every owned record, writing plaintext back to storage. The salt, check token, and vault-enabled flag are cleared from the identity record. The key is removed from the in-memory key store.

**Change passphrase** (`POST /vault/change`): Rotates the encryption key without a plaintext window. The user provides both the old and new passphrases. The server derives the old key, verifies it, generates a new salt, derives the new key, then re-encrypts every owned record: decrypt with the old key, encrypt with the new key, write back. The identity record is updated with the new salt and check token.

Change is the most expensive operation — it requires reading, decrypting, re-encrypting, and writing every owned record. For a user with many records, this can take several seconds. The exclusive fence prevents concurrent MQTT operations during the re-encryption.

## 19.4 In-Memory Key Management

The `VaultKeyStore` holds derived encryption keys in memory, indexed by canonical user ID. Three design decisions define its behavior.

**Keys are volatile.** The key store is a `HashMap` in process memory. When the server restarts, all keys are lost. Users must unlock their vaults again after every restart. This is deliberate — a key that survives restart must be persisted somewhere, and any persistence mechanism creates an attack surface. Volatile keys mean that stealing the database files without access to the running process yields only ciphertext.

**Keys are zeroized.** Every key is wrapped in `Zeroizing<Vec<u8>>` from the `zeroize` crate. When the wrapper is dropped (on lock, disable, or process exit), the memory is overwritten with zeros before deallocation. This prevents the key from lingering in freed memory where a memory dump or swap file analysis could recover it.

**Batch operations use write fences.** The key store maintains a second map of `tokio::RwLock` instances, one per user. When a batch operation begins (enable, disable, or change passphrase), it acquires the write lock:

```
acquire_fence("user-abc")  →  OwnedRwLockWriteGuard
```

While the fence is held, any MQTT operation for that user calls `read_fence("user-abc")`, which blocks until the batch completes. This prevents a race where:

1. Batch encryption is processing record 50 of 100
2. An MQTT read for record 75 arrives
3. Record 75 is still plaintext (batch hasn't reached it yet)
4. The MQTT handler looks for a vault key, finds one (batch stored it), and tries to decrypt
5. Decryption fails because the field is plaintext, not ciphertext

The read fence serializes MQTT operations behind the batch, ensuring they see a consistent view: either all records encrypted (after batch) or all plaintext (before batch). If no fence exists for a user (no batch in progress), `read_fence` returns immediately — it is a no-op for the common case.

## 19.5 Transparent Encryption in the MQTT Data Path

The vault's value lies in transparency: MQTT clients send and receive plaintext, unaware that encryption exists. The server intercepts requests and responses in the agent handler, applying encryption and decryption based on the presence of a vault key in the store.

**Eligibility check.** Not all MQTT operations go through vault processing. A record is vault-eligible only if its entity is not internal (does not start with `_`) and has ownership configured. Internal entities like `_sessions` and `_mqtt_subs` are infrastructure — encrypting them would break the broker. Entities without ownership have no way to associate a record with a user's key.

**Create path.** When a client publishes to `$DB/notes/create` with payload `{"name": "Alice", "email": "alice@example.com"}`, the handler:

1. Extracts the sender's user ID from the `x-mqtt-sender` MQTT user property
2. Checks if the entity `notes` is vault-eligible
3. Waits on the read fence (blocks if a batch operation is in progress)
4. Retrieves the user's vault key from the key store
5. Generates a record ID if the payload lacks one
6. Encrypts all eligible string fields (skipping `id` and the owner field)
7. Passes the encrypted payload to the database

The database stores `{"id": "rec-1", "name": "base64(...)", "email": "base64(...)"}`. The plaintext never touches storage.

**Read and list paths.** On response, the handler checks for a vault key and decrypts each string field. If decryption fails (the field was not encrypted, or was encrypted with a different key), the field is left unchanged. This graceful fallback handles mixed state during transitions and records that predate vault enablement.

For list operations, the handler iterates the response array and decrypts each record independently, extracting the `id` from each record for the AAD.

**The update problem.** Updates are the complex case. MQTT updates are partial — a client might send `{"email": "new@example.com"}` to update only the email field. But vault encryption uses the full record's `id` as part of the AAD, and the existing encrypted fields must remain valid. A naive approach of encrypting only the delta fields and sending them as an update would leave the existing encrypted fields untouched but create an inconsistent state if the delta overlaps with encrypted fields.

The solution reads the full record, decrypts it, applies the delta as a JSON merge, re-encrypts the entire record, and sends the merged result as the update:

1. Read existing record from database (encrypted)
2. Decrypt all fields locally
3. Merge delta into decrypted record
4. Remove system fields (`id`, owner field) to prevent client overwrite
5. Re-encrypt the merged record
6. Send as a full update

This means every vault-encrypted update is a read-modify-write cycle, even if the client only changed one field. The additional read is the cost of field-level encryption with partial updates.

**Delete path.** Deletes require no vault processing — there is no data in the request or response to encrypt or decrypt.

## 19.6 Identity Encryption

Identity encryption is a separate system from the vault. Where the vault protects user-created entity data with a user-provided passphrase, identity encryption protects OAuth infrastructure records with a server-generated key. The user never interacts with identity encryption; it is always on when OAuth is configured.

**What it protects.** Three internal entities store OAuth data:
- `_identities`: canonical user records with email, name, provider information
- `_identity_links`: mappings from provider-specific IDs (e.g., `google:12345`) to canonical IDs, with email and name
- `_oauth_tokens`: refresh tokens and access tokens

These records contain personally identifiable information (email addresses, names) and sensitive credentials (OAuth tokens). Storing them in plaintext means a database breach exposes every user's email and active tokens.

**Key architecture.** Identity encryption uses a three-layer key hierarchy:

1. A 32-byte *identity key* is generated randomly at first startup
2. A *wrapping key* is derived from a fixed salt using PBKDF2 (600K iterations), using a constant info string as the "passphrase"
3. The identity key is encrypted (wrapped) with the wrapping key and stored in the database alongside the salt

On subsequent startups, the server reads the salt and wrapped key from storage, derives the wrapping key, and unwraps the identity key. The wrapping layer means the identity key never appears in plaintext in storage — only the wrapped form is persisted.

From the identity key, two purpose-specific keys are derived using HKDF-SHA256:

| Derived Key | HKDF Info | Purpose |
|------------|-----------|---------|
| Encryption key | `aes-encryption` | AES-256-GCM for field encryption |
| HMAC key | `hmac-blind-index` | HMAC-SHA256 for searchable blind indexes |

The encryption key works identically to the vault's field-level encryption — AES-256-GCM with random nonces and entity-scoped AAD. The difference is that the AAD uses only the entity name (not `entity:id`), because identity records need to be searchable by encrypted fields.

**Blind indexing.** The HMAC key enables searching encrypted fields without decrypting them. When the server stores an identity link with email `user@example.com`, it computes:

```
blind_index("_identity_links", "user@example.com")
    = HMAC-SHA256(hmac_key, "_identity_links:user@example.com")
    = "a3f2b7c9..."  (hex-encoded)
```

This deterministic hash is stored alongside the encrypted email. When a user authenticates via OAuth and the server needs to find their identity link by email, it computes the blind index for the email and searches for that hash value instead of the encrypted email.

The blind index reveals nothing about the plaintext (it is a keyed hash, not reversible without the HMAC key) but allows equality lookups. It cannot support range queries, prefix searches, or pattern matching — only exact equality. For the OAuth use case (looking up a user by their exact email from their OAuth provider), this is sufficient.

## 19.7 What Went Wrong

### Batch operations are not atomic

The most significant known issue is crash recovery for batch vault operations. When a user enables the vault, the server iterates through every owned record, encrypts it, and writes it back. If the process crashes at record 50 of 100, fifty records are encrypted and fifty are plaintext. The identity record shows `vault_enabled: true` and has a valid check token.

On the next unlock, the server derives the key and stores it in the key store. MQTT reads attempt to decrypt all fields. The fifty plaintext records fail decryption gracefully (the handler leaves them unchanged), so the user sees a mix: some records decrypted correctly, others showing plaintext that was never encrypted.

This is a data consistency problem, not a data loss problem — no records are destroyed, but the vault's promise ("all your data is encrypted") is violated. The read path's graceful fallback (leaving fields unchanged when decryption fails) masks the inconsistency rather than surfacing it.

*(Planned)* A `vault_migration_status` field on the identity record would track whether a batch completed successfully. On unlock, if the status is `pending`, the server would re-run the batch operation to encrypt the remaining plaintext records.

### The update TOCTOU window

The vault pre-update path reads a record, decrypts it, merges the delta, re-encrypts, and writes. Between the read and the write, the vault state could change (a concurrent disable removes the key). The write fence protects batch operations, but individual CRUD operations are not fenced — they only call `read_fence`, which is a no-op when no batch is running.

The window is extremely narrow: it requires a vault disable (which acquires the exclusive fence) to interleave exactly between a single MQTT update's read and write phases. In practice, the fence serialization makes this nearly impossible — the batch would block subsequent MQTT operations via the read fence. But the theoretical possibility exists.

*(Planned)* A vault generation counter on entity metadata would allow the write path to detect that the vault state changed between read and write, and retry with the current state.

### Identity records lack schema constraints

The `_identities` entity has no unique constraint on the email field. Two concurrent OAuth callbacks for the same email could both create an identity record, producing duplicate canonical IDs. The identity creation code does a read-then-create, but no reservation prevents the race. The existing unique constraint protocol (Chapter 15) could enforce uniqueness, but the internal entity schemas are not initialized at startup.

*(Planned)* An initialization step at HTTP server startup would create schemas and unique constraints for internal entities, providing defense-in-depth beyond application-level guards.

## Lessons

**Server-side encryption is not end-to-end encryption.** The vault encrypts data at rest, but the server processes plaintext during every request. A compromised server (or a malicious operator with access to the running process) can read all data for users whose vaults are unlocked. The threat model is stolen disks and backup tapes, not compromised servers. Users who need protection from the server operator need client-side encryption, which is a fundamentally different architecture.

**Volatile keys are a feature.** The decision to lose all vault keys on restart initially seems like a limitation. But it creates a strong security property: the window of exposure is bounded by the process lifetime. An attacker who gains disk access to a stopped server finds only ciphertext. An attacker who gains access to a running server can only access data for currently-unlocked vaults. The restart boundary is a natural key rotation event — users re-authenticate and re-unlock, confirming they still possess the passphrase.

**Field-level encryption preserves structure at the cost of metadata leakage.** Encrypting individual fields within a JSON record preserves the record's structure: the key names, the number of fields, and the types of non-string values remain visible. An observer can see that a record has five fields, three of which are encrypted strings, one number, and one boolean — without knowing the plaintext. Record-level encryption (encrypting the entire JSON blob as a single ciphertext) would hide this metadata but would prevent the database from operating on any field without decryption. The tradeoff is acceptable for MQDB's use case: the database needs to read `id` and owner fields for routing and access control, which requires at least partial structural visibility.

**Separate keys for separate concerns.** The vault and identity encryption use independent key hierarchies even though both use AES-256-GCM. A vault compromise (user passphrase leaked) does not expose identity records. An identity key compromise (server storage breached) does not expose vault-encrypted data. This independence means each system's failure mode is contained. The cost is implementation complexity — two crypto modules, two key stores, two sets of encrypt/decrypt paths. But the alternative (a single key for all encryption) would mean that any key compromise exposes everything.

## What Comes Next

*(This chapter will be expanded as the vault implementation matures. The planned additions include: crash recovery for batch operations, vault generation counters for TOCTOU prevention, automatic schema initialization for internal entities, and cluster-mode vault key synchronization.)*

Chapter 20 covers operating MQDB in production — deployment modes, cluster sizing, monitoring, backup, and the CLI tools that tie the system together.
