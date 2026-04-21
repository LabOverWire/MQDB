# 5. Protocol-Level Field Encryption

A unified broker-database architecture enables a security capability that is structurally impossible in conventional separated systems: field-level encryption at the protocol boundary, requiring no application middleware and no external key management.

## 5.1 The Architectural Argument

In a conventional IoT data stack — MQTT broker, application bridge, database — encryption of stored data must happen at the application layer. The broker delivers plaintext messages to a bridge process, which must encrypt sensitive fields before writing them to the database and decrypt them when reading back. This bridge must manage encryption keys, handle key rotation, and maintain the mapping between plaintext and ciphertext representations. The broker itself never participates in encryption; it is a pass-through.

In MQDB's agent mode, the broker *is* the database. When a client publishes to `$DB/medical/patient-42/update`, the message enters the MQTT protocol layer and reaches the topic handler, which is also the database engine. Field-level encryption can occur at this boundary — after MQTT processing but before storage — without any intermediate process. The client publishes a standard MQTT message; the database stores encrypted fields; no application code sits between them.

Because the protocol endpoint and the storage engine are the same process, encryption can be inserted at the exact boundary where data transitions from "in flight" (MQTT message) to "at rest" (database record). In a separated architecture, this boundary spans a network hop and requires a dedicated encryption service.

## 5.2 Implementation

MQDB implements this capability as the Vault, exposed through MQTT topics that follow the same topic-as-interface pattern as CRUD operations.

**Key derivation.** Each user derives an encryption key from a passphrase using PBKDF2-HMAC-SHA256 with 600,000 iterations and a 256-bit random salt. The server never stores the passphrase — only the derived key material is held in memory while the vault is unlocked. This provides a zero-knowledge property: a database dump reveals only ciphertext and salts, not keys or passphrases.

**Encryption.** Individual record fields are encrypted with AES-256-GCM using 96-bit random nonces. Each encryption operation binds the ciphertext to its record through additional authenticated data (AAD) constructed as `{entity}:{id}` — for example, `users:user-42`. This binding prevents ciphertext from being transplanted between records: decrypting a field from `users:user-42` with the AAD of `users:user-99` fails authentication.

**Recursive field processing.** JSON documents are encrypted field-by-field, recursively descending into nested objects and arrays. String values are encrypted directly. Non-string values (numbers, booleans, null) are prefixed with a type marker before encryption so that the original JSON type can be restored on decryption. Two categories of fields are never encrypted: system fields (any key beginning with `_`, such as `_created_at` or `_version`) and ownership fields (the field designated as the record owner identifier, if ownership is configured for the entity). The `id` field is also excluded, as it serves as part of the AAD.

**Vault lifecycle.** The vault is managed through six operations — enable, unlock, lock, change passphrase, disable, and status query — exposed through both MQTT topics (`$DB/_vault/enable`, etc.) and an HTTP REST API (`POST /vault/enable`, etc.). The MQTT interface follows the same request-response pattern as CRUD operations. The HTTP interface provides an alternative access path for administrative tooling. Both paths converge on the same encryption engine; the dual-transport design reflects the system's role as both an MQTT broker and an HTTP-accessible service.

## 5.3 Scope and Limitations

The vault is available in agent mode only. Cluster mode explicitly rejects vault operations with a "vault admin not supported in cluster mode" error. Extending field encryption to a distributed deployment requires a key distribution protocol — synchronizing per-user encryption keys across nodes while maintaining the zero-knowledge property — that is not yet implemented.

The architectural argument for protocol-level encryption holds strongest in single-node deployments, where the protocol boundary and the storage boundary are the same process boundary. In cluster mode, nodes coordinate over a network, and the architecture resembles a separated system with respect to the trust boundary between nodes.

For the separated-architecture comparison: even in single-node deployments, a standalone MQTT broker cannot provide field-level encryption of database records because it has no database. The encryption capability exists precisely because the broker and database are unified.
