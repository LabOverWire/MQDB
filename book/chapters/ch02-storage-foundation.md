# Chapter 2: The Storage Foundation

Before distributing anything, build the single-node database. The components introduced in this chapter — key encoding, pluggable backends, the schema and constraint system, secondary indexes, reactive subscriptions, and the transactional outbox — reappear in Part II when the system goes distributed. The agent-mode design is the foundation that cluster mode extends.

## 2.1 A Flat Key Space

The entire database lives in a single sorted key-value store. Every piece of data — user records, index entries, schemas, subscriptions, pending events — shares one keyspace. The only structure comes from prefixes: byte-string conventions that partition the flat space into logical namespaces.

Nine prefixes create the full namespace:

| Prefix | Purpose | Example |
|--------|---------|---------|
| `data/` | Entity records | `data/users/abc-123` |
| `idx/` | Secondary indexes, unique constraint checks | `idx/users/email/alice@example.com/abc-123` |
| `meta/` | Schemas, constraints, counters | `meta/schema/users`, `meta/seq:users` |
| `sub/` | Subscriptions | `sub/sub-001` |
| `_outbox/` | Pending change events | `_outbox/op-uuid` |
| `dedup/` | Deduplication markers | `dedup/corr-id` |
| `_dead_letter/` | Failed outbox entries | `_dead_letter/op-uuid` |
| `_crypto/` | Encryption metadata | `_crypto/salt`, `_crypto/check` |

The first six are visible to application logic. The last two are infrastructure: `_dead_letter/` holds outbox entries that exhausted their retries, and `_crypto/` stores the encryption salt and verification marker for the encrypted backend.

Why a flat keyspace instead of separate column families or tables? Because the atomicity boundary is a single batch commit across all prefixes. When a create operation writes a data key, its index entries, and its outbox event, all three land in one batch. If the database used separate stores, cross-store atomicity would require a distributed transaction protocol — exactly the complexity that Chapter 1 argued against.

### Lexicographic Ordering

The key encoding is designed so that lexicographic byte ordering produces meaningful results. A prefix scan on `data/users/` returns all users. A prefix scan on `idx/users/email/alice@example.com/` returns all user IDs with that email. A range scan between `meta/constraint/fk/posts/` and `meta/constraint/fk/posts/\xff` returns all foreign key constraints on the `posts` entity. Every query the database supports reduces to either a point lookup or a prefix/range scan over sorted bytes.

This works because the key encoding function for data keys follows a rigid structure:

```rust
pub fn encode_data_key(entity: &str, id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(DATA_PREFIX.len() + 1 + entity.len() + 1 + id.len());
    key.extend_from_slice(DATA_PREFIX);   // "data"
    key.push(SEPARATOR);                  // b'/'
    key.extend_from_slice(entity.as_bytes());
    key.push(SEPARATOR);
    key.extend_from_slice(id.as_bytes());
    key
}
```

The separator is a single byte (`b'/'`), chosen because it never appears in entity names or IDs under the validation rules. This gives clean prefix boundaries: scanning `data/users/` will never accidentally pick up `data/users_archive/` because `_` sorts after `/` in ASCII.

### The Numeric Encoding Trick

Secondary indexes must preserve sort order for range queries. Strings sort correctly by default in lexicographic order, but numbers do not: the string `"9"` sorts after `"42"` because `'9' > '4'` in ASCII. The encoding function solves this by zero-padding integers to 20 digits:

```rust
serde_json::Value::Number(n) => {
    if let Some(i) = n.as_i64() {
        Ok(format!("{i:020}").into_bytes())  // 42 → "00000000000000000042"
    } else if let Some(f) = n.as_f64() {
        Ok(format!("{f:020.6}").into_bytes()) // 3.14 → "000000000003.140000"
    }
}
```

Now `"00000000000000000009"` sorts before `"00000000000000000042"`, and a range scan over an age index returns records in numeric order. Twenty digits accommodates values up to `u64::MAX` (about 1.8 × 10¹⁹). Floats use 20 characters with 6 decimal places, which preserves order for non-negative values. The encoding does not handle negative numbers — a deliberate simplification for the JSON document model, where negative index values are uncommon.

### CRC32 Integrity

Every serialized entity carries a CRC32 checksum prepended as 4 bytes in big-endian order. On write, `encode_with_checksum` computes the CRC32 of the JSON payload and prepends it:

```rust
pub fn encode_with_checksum(data: &[u8]) -> Vec<u8> {
    let checksum = compute_checksum(data);
    let mut result = Vec::with_capacity(4 + data.len());
    result.extend_from_slice(&checksum.to_be_bytes());
    result.extend_from_slice(data);
    result
}
```

On read, `decode_and_verify` extracts the stored checksum, recomputes it over the payload, and returns a `ChecksumMismatch` error if they differ. The error propagates as `Error::Corruption { entity, id }` — the database knows exactly which record is damaged.

This catches bitflips in storage, truncated writes, and corruption from misaligned reads. CRC32 is not cryptographic — it won't detect intentional tampering — but it is fast enough to run on every read without measurable overhead. The test suite verifies that flipping any single bit in the payload triggers detection.

### ID Extraction

The `id` field lives in the storage key, not in the JSON blob. When `Entity::from_json` receives input data, it extracts the `id` field and removes it from the JSON object. When `Entity::to_json` returns data to a caller, it re-inserts `id` into the JSON. This separation serves two purposes: it eliminates redundant storage (the ID is already encoded in the key), and it ensures the ID is always consistent with the key — no possibility of the JSON `id` field disagreeing with the storage key.

## 2.2 Pluggable Storage Backends

The storage layer is defined by a trait, not a concrete type. `StorageBackend` declares seven operations:

```rust
pub trait StorageBackend: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn insert(&self, key: &[u8], value: &[u8]) -> Result<()>;
    fn remove(&self, key: &[u8]) -> Result<()>;
    fn prefix_scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
    fn range_scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
    fn batch(&self) -> Box<dyn BatchOperations>;
    fn flush(&self) -> Result<()>;
}
```

Three implementations exist:

**Fjall** is the production backend. It wraps an LSM-tree engine (the `fjall` crate), using a single keyspace named `"main"`. Three durability modes control fsync behavior: `Immediate` flushes every write with `PersistMode::SyncAll`, `PeriodicMs(n)` relies on the engine's background flush (used in cluster mode for throughput), and `None` skips persistence entirely. Prefix scans and range scans operate on a snapshot (`self.db.snapshot()`), providing a consistent view even while concurrent writes land — the iterator sees the database as it was at snapshot creation time.

**Memory** is a `BTreeMap<Vec<u8>, Vec<u8>>` behind an `RwLock`. It provides the same sort order as Fjall — `BTreeMap` iterates in sorted key order, just like an LSM-tree. Prefix scans use `range(prefix..)` with a `take_while` predicate. This backend runs in tests and in WASM where filesystem access is unavailable.

**Encrypted** is a decorator that wraps any other backend. It uses AES-256-GCM for per-value encryption with a key derived from a passphrase via PBKDF2 (600,000 iterations of HMAC-SHA256). Each value gets a fresh 12-byte random nonce. The storage key itself is used as Associated Authenticated Data (AAD) for the GCM cipher — meaning a value encrypted under key `data/users/42` cannot be decrypted if someone moves it to key `data/users/99`. On first open, the backend generates a random 32-byte salt, stores it at `_crypto/salt`, encrypts a known plaintext (`"mqdb"`) and stores it at `_crypto/check`. On subsequent opens, it re-derives the key and attempts to decrypt the check value — a wrong passphrase fails immediately rather than silently producing garbage on later reads.

### The Batch Contract

The `BatchOperations` trait is where atomicity lives:

```rust
pub trait BatchOperations: Send {
    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>);
    fn remove(&mut self, key: Vec<u8>);
    fn expect_value(&mut self, key: Vec<u8>, expected_value: Vec<u8>);
    fn commit(self: Box<Self>) -> Result<()>;
}
```

`insert` and `remove` queue operations. `expect_value` registers a precondition: at commit time, the current value for this key must match the expected value exactly, or the entire batch is rejected with a `Conflict` error. This is optimistic concurrency control — no locks are held during the batch construction phase. The check and write happen atomically at `commit()`.

In the Fjall backend, `commit()` first takes a snapshot and checks all preconditions against it, then applies all operations in a single fjall batch. In the memory backend, `commit()` acquires the write lock, checks preconditions, and applies operations while holding the lock — the write lock serializes all batch commits.

The encrypted backend has a subtlety: `expect_value` receives a plaintext expected value, but the underlying backend stores ciphertext. During `commit()`, the encrypted batch reads the current ciphertext, decrypts it, compares with the expected plaintext, and if matched, re-encrypts and passes the ciphertext expectation to the inner batch. Two layers of precondition checking, but the caller only sees plaintext.

Why trait-based abstraction? Because the same database code — CRUD operations, constraint validation, index maintenance — runs identically regardless of whether data is on disk (Fjall), in memory (tests), or encrypted. The caller never knows which backend is active. This pays off immediately in testing (memory backend, no filesystem setup) and later in WASM deployment (memory backend, no native dependencies).

## 2.3 The Database API

The `Database` struct ties the components together. It holds 13 `Arc`-wrapped fields:

```rust
pub struct Database {
    storage: Arc<Storage>,
    registry: Arc<SubscriptionRegistry>,
    dispatcher: Arc<EventDispatcher>,
    outbox: Arc<Outbox>,
    index_manager: Arc<RwLock<IndexManager>>,
    relationship_registry: Arc<RwLock<RelationshipRegistry>>,
    schema_registry: Arc<RwLock<SchemaRegistry>>,
    constraint_manager: Arc<RwLock<ConstraintManager>>,
    consumer_groups: Arc<RwLock<HashMap<String, ConsumerGroup>>>,
    id_gen_lock: Arc<Mutex<()>>,
    config: Arc<DatabaseConfig>,
    shutdown_tx: watch::Sender<bool>,
    background_handles: Arc<std::sync::Mutex<Vec<JoinHandle<()>>>>,
}
```

`Database::open` is the entry point. It opens storage (encrypted or plain, depending on config), loads schemas and constraints from `meta/*` keys, loads subscriptions from `sub/*` keys, and spawns up to three background tasks: the outbox processor (retries failed event dispatches), TTL cleanup (scans for expired records), and consumer timeout cleanup (evicts stale members from consumer groups). The `shutdown_tx` watch channel coordinates shutdown — sending `true` triggers all background tasks to exit their loops.

### The Create Path

Walking through `Database::create` reveals the atomicity model:

1. **Apply schema defaults.** If a schema exists for this entity, insert any missing fields that have default values defined.
2. **Generate or accept ID.** If the input JSON contains an `id` field, use it. Otherwise, generate a sequential ID by reading the counter at `meta/seq:{entity}`, incrementing it, and writing it back. The `id_gen_lock` mutex serializes ID generation to prevent duplicates.
3. **Handle TTL.** If `ttl_secs` is present, compute `_expires_at = now + ttl_secs` and remove the `ttl_secs` field. The TTL cleanup task will delete the record after expiration.
4. **Inject version.** Set `_version = 1`. Every update increments this field, enabling optimistic concurrency on updates via `expect_value`.
5. **Validate schema.** If a schema exists, verify all required fields are present and all provided values match their declared types.
6. **Create BatchWriter.** This is the atomicity boundary. Everything from here to `commit()` is all-or-nothing.
7. **Validate constraints.** Check unique constraints (no duplicate values), foreign key constraints (referenced entity exists), and not-null constraints (required fields are non-null). These checks happen against the current storage state.
8. **Insert data key.** Serialize the entity (with CRC32 checksum) and add the insert to the batch.
9. **Update indexes.** For each indexed field, add an index entry to the batch.
10. **Enqueue outbox entry.** Serialize the change event and add it to the batch under `_outbox/{uuid}`.
11. **Commit.** One call to `batch.commit()` persists data, indexes, and outbox entry atomically.
12. **Dispatch event.** Push the change event to subscribers through the event dispatcher.
13. **Mark delivered.** Delete the outbox entry. If dispatch fails or the process crashes, the outbox processor will retry on the next sweep.

Steps 7 through 11 share one `BatchWriter`. That single `commit()` is the atomicity boundary. Either the record, its indexes, and its outbox entry all exist, or none of them do. There is no window where data exists without its corresponding event, and no window where an event exists without its corresponding data.

### The Update Path

The update path follows the same structure but adds an `expect_value` precondition. After reading the existing record, the update merges new fields into the existing data (a shallow merge — top-level keys are replaced, nested objects are not deep-merged), increments `_version`, validates the schema, and builds a batch. Before inserting the new value, it calls `batch.expect_value(key, existing_data)` — the raw bytes of the record as they were read. At `commit()` time, if the stored bytes differ (because another concurrent update landed in between), the entire batch is rejected with a `Conflict` error. The caller can retry with a fresh read.

This is optimistic concurrency control at the storage layer. No locks are held during the read-modify-write cycle. The window between read and commit is a race window, but the `expect_value` precondition makes the race detectable rather than destructive. In practice, conflicts are rare because MQDB's primary workloads — IoT telemetry, event streams — are insert-heavy rather than update-heavy.

### The Delete Path

Delete is the most complex operation because it triggers constraint cascades. The sequence: read the existing record, check constraints (which may return a `Vec<DeleteOperation>`), build a batch containing the primary delete plus all cascade and set-null operations, enqueue outbox events for every affected record, and commit. The delete path demonstrates the full power of the single-batch model: a cascade that touches 20 records across 3 entities still commits in one atomic operation. If any part fails — a constraint blocks it, serialization fails, the batch commit encounters a conflict — nothing changes.

### Background Tasks

Three tasks run in the background after `Database::open`:

**The outbox processor** wakes on a configurable interval (default 5 seconds), scans `_outbox/*`, and re-dispatches pending entries. This is the crash recovery mechanism described in Section 2.7.

**The TTL cleanup task** wakes periodically (default 60 seconds), scans all `data/*` keys, deserializes each record, checks the `_expires_at` field against the current time, and deletes expired records. Expired records are removed in a single batch with their index entries and outbox events — the same atomic pattern as a normal delete, just triggered by the clock instead of a client request.

**The consumer timeout task** wakes at half the consumer timeout interval, iterates through all consumer groups, and evicts members whose last heartbeat exceeds the timeout. Evicted members' partitions are redistributed to surviving members via the rebalance algorithm.

All three tasks are coordinated by a `watch::channel<bool>`. On shutdown, the `Database` sends `true` on the channel, and each task exits its `tokio::select!` loop.

## 2.4 Schema and Constraints

Schemas are opt-in. An entity without a schema accepts any JSON object — the database operates as a schemaless document store. When a schema exists, it enforces structure without restricting it: fields declared in the schema must conform to their types, but additional fields are allowed. This is an open schema model.

Six types are supported: String, Number, Boolean, Array, Object, and Null. Each field definition carries a name, a type, a required flag, and an optional default value. Defaults are applied before validation — a field with a default value is never "missing" by the time validation runs.

Schemas are persisted under `meta/schema/{entity}` as JSON and loaded into memory at startup. The `SchemaRegistry` holds an in-memory `HashMap<String, Schema>` and delegates validation: if a schema exists for the entity, validate; otherwise, pass through.

### Three Constraint Types

**Unique constraints** enforce that no two records share the same value for a given field (or combination of fields for composite constraints). Validation works by prefix-scanning the index for the field value: `idx/{entity}/{field}/{value}/`. If any key exists in that prefix range with a different ID, the constraint is violated. This means unique constraints depend on secondary indexes being maintained — the index must exist for the constraint check to work.

**Not-null constraints** verify that a field exists in the JSON object and its value is not JSON `null`. The check is a simple field lookup on the entity data.

**Foreign key constraints** verify referential integrity. On create or update, if the source field contains a non-null value, the constraint checks that the referenced target entity exists by performing a direct key lookup (`data/{target_entity}/{target_id}`). If the key is missing, the operation is rejected.

### Cascade Delete

The interesting complexity is in delete. When a record is deleted, the constraint manager must find all records that reference it via foreign key and apply the configured policy. Three policies exist:

**Restrict** blocks the delete if any referencing records exist. The check scans all records of the source entity (a prefix scan on `data/{source_entity}/`), deserializes each one, and checks whether its FK field points to the record being deleted.

**Cascade** deletes all referencing records recursively. `validate_delete` returns a `Vec<DeleteOperation>` containing every cascade and set-null operation needed. The implementation uses depth-first search with a visited set to detect cycles — if entity A references entity B which references entity A, the cycle is broken by the visited check.

**SetNull** nullifies the FK field in referencing records instead of deleting them. The field is set to JSON `null` and the record's `_version` is incremented.

The caller builds a single `BatchWriter` containing the primary delete, all cascade deletes, all set-null updates, and all corresponding index removals and outbox events. One `commit()` — the entire cascade tree is atomic. Either the parent and all its cascaded children are deleted, or nothing changes.

```rust
let delete_ops = constraint_manager.validate_delete(&existing_entity, &self.storage)?;
let mut batch = self.storage.batch();
batch.remove(key.clone());
// ... process each DeleteOperation into batch inserts/removes ...
batch.commit()?;
```

Constraints are persisted under `meta/constraint/{type}/{entity}/{name}` and loaded at startup. This means constraint definitions survive restarts — they are part of the database state, not ephemeral configuration.

This entire constraint system is agent-mode only — local key lookups, local prefix scans, one batch commit. Chapter 15 covers what happens when foreign key validation must cross partition boundaries in cluster mode: the local key lookup becomes an async cross-node request, the lock must be dropped and reacquired, and the consistency window between check and write opens wider.

## 2.5 Secondary Indexes

Index entries are keys with empty values. The key encodes everything:

```
idx/{entity}/{field}/{encoded_value}/{id} → []
```

A prefix scan on `idx/users/email/alice@example.com/` returns all user IDs with that email address. The ID is extracted from the last segment of the key. The value is empty — zero bytes — because the information is entirely in the key structure.

Why empty values? Index entries are write-heavy. Every create adds index entries, every update removes old entries and adds new ones, every delete removes entries. If index entries stored a copy of the record data, every data write would mean writing the data twice — once to the data key, once to each index entry. Empty values mean index maintenance is cheap: just key insertions and deletions. The actual record is always fetched from the data key when needed.

### Atomic Index Maintenance

The `IndexManager` provides two operations: `update_indexes` (for creates and updates) and `remove_indexes` (for deletes). Both operate on a `BatchWriter`:

```rust
fn update_indexes(&self, batch: &mut BatchWriter, entity: &Entity, old_entity: Option<&Entity>) {
    if let Some(old) = old_entity {
        Self::remove_index_entries(batch, old, fields);
    }
    Self::add_index_entries(batch, entity, fields);
}
```

On update, old index entries are removed and new ones are added in the same batch. The `commit()` that persists the data also persists the index changes. There is no window where the data says one thing and the index says another.

The `lookup_by_field` method performs the reverse operation: given a field name and value, find all matching entity IDs. It prefix-scans `idx/{entity}/{field}/{value}/` and extracts the ID from each key's last segment. The list operation uses this for index-accelerated filtering: if the first filter is an equality check and an index exists for that field, the query fetches candidate IDs from the index rather than scanning every record.

Arrays and objects cannot be indexed — `encode_value_for_index` returns an error for these types. Only scalar values (strings, numbers, booleans, null) produce index entries. This keeps the index structure flat and the lookup semantics unambiguous.

Index entries serve three consumers, not one. The `IndexManager` creates them for field-value lookups. The `ConstraintManager` scans them to enforce unique constraints — `validate_unique` prefix-scans `idx/{entity}/{field}/{value}/` and rejects the write if any key exists with a different ID. And the query path in `list_with_filters` uses them for index-accelerated equality filtering, fetching candidate IDs from the index rather than scanning every record in the entity. The same empty-value keys serve all three purposes because the information is in the key structure, not the value.

## 2.6 Reactive Subscriptions

Every write produces a `ChangeEvent`:

```rust
pub struct ChangeEvent {
    pub sequence: u64,           // Global monotonic counter
    pub entity: String,          // "users", "orders", etc.
    pub id: String,              // Record identifier
    pub operation: Operation,    // Create, Update, or Delete
    pub data: Option<Value>,     // Full record data (None for deletes)
    pub sender: Option<String>,  // Who initiated the write
    pub client_id: Option<String>,
    pub scope: Option<(String, String)>,
}
```

The `sequence` field comes from a global `AtomicU64`, incremented with `SeqCst` ordering on every event creation. This provides a total ordering within a single process — every event has a unique, monotonically increasing sequence number. In cluster mode, sequence numbers are scoped to partitions (Chapter 5), but in agent mode the global counter suffices.

### Pattern Matching

Subscriptions use MQTT-style wildcard patterns. `+` matches exactly one path segment. `#` matches zero or more remaining segments. The matching function splits both pattern and path on `/` separators and walks the segments:

- `users/+` matches `users/123` but not `users/123/profile`
- `users/#` matches `users/123` and `users/123/profile` and `users`
- `+/123` matches `users/123` and `orders/123`

Each `Subscription` optionally filters on entity name before pattern matching — a subscription for entity `"users"` with pattern `users/+` won't accidentally match a `posts/users/123` event.

### Three Dispatch Modes

The `EventDispatcher` categorizes matching subscriptions by their mode:

**Broadcast** is the default. Every matching subscription receives every event. The dispatcher iterates through all matching broadcast subscriptions and sends the event to each listener's broadcast channel. This is the mode used by MQTT clients subscribing to `$DB/users/events/#` — every subscriber sees every user change.

**LoadBalanced** distributes events across members of a share group using round-robin. An `AtomicUsize` counter per group tracks the current position. Each event goes to exactly one member of the group — `subs[counter % subs.len()]`. This is the shared subscription model: multiple workers processing events with each event handled by exactly one worker.

**Ordered** routes events by partition. The event's partition is computed as `hash(entity:id) % num_partitions`, using `DefaultHasher` for deterministic assignment. The `ConsumerGroup` tracks which consumer owns which partition. All events for the same `entity:id` combination always land on the same partition and therefore the same consumer. This guarantees ordering: a create followed by an update followed by a delete for `users/42` will always arrive at the same consumer in that order.

### Consumer Groups

Consumer groups implement partition assignment. When a consumer joins, `ConsumerGroup::add_member` triggers a rebalance. The algorithm is simple: sort consumer IDs lexicographically, then assign partitions round-robin. With 8 partitions and 2 consumers (`"a"` and `"b"`), consumer `"a"` gets partitions [0, 2, 4, 6] and consumer `"b"` gets [1, 3, 5, 7]. The lexicographic sort ensures deterministic assignment — two group instances with the same members produce identical assignments regardless of join order.

Stale member detection runs as a background task. Each member has a `last_heartbeat` timestamp updated on activity. If the elapsed time exceeds `consumer_timeout_ms` (default 30 seconds), the member is removed and the group rebalances. The surviving members absorb the evicted member's partitions.

The partition count for consumer groups defaults to 8 — configurable via `SharedSubscriptionConfig`. This is independent of the 256 partitions used for data distribution in cluster mode. These are logical partitions for event routing, not storage partitions.

### Event Topics

Each `ChangeEvent` generates a topic path that determines which MQTT subscribers receive it. The topic structure depends on context:

Without scoping: `$DB/{entity}/events/{id}` — or, when partitioned shared subscriptions are active, `$DB/{entity}/events/p{partition}/{id}`. The partition number in the topic path enables MQTT-level shared subscription routing: subscribers to `$DB/users/events/p3/#` only receive events for records that hash to partition 3.

With scoping: `$DB/{scope_entity}/{scope_value}/{entity}/events/{event_type}`. Scoping allows hierarchical event routing — an order event scoped to a tenant produces a topic like `$DB/tenants/acme/orders/events/created`, so a subscriber listening to `$DB/tenants/acme/#` receives all events across all entities within that tenant.

The `event_type` suffix distinguishes operation types: `created`, `updated`, `deleted`. A subscriber can filter for only creations by subscribing to `$DB/users/events/created` rather than `$DB/users/events/#`.

### Subscription Persistence

Subscriptions are persisted under `sub/{id}` as serialized JSON. The `SubscriptionRegistry` loads them from storage at startup and maintains an in-memory `HashMap` for fast matching. On every event dispatch, the registry scans all subscriptions and returns those that match — a linear scan through the subscription list. For workloads with thousands of subscriptions, this becomes the chapter's performance story: the current implementation trades simplicity for throughput. A trie-based index (similar to what the cluster mode uses for MQTT topic subscriptions in Chapter 8) would accelerate matching, but the agent-mode subscription count is typically small enough that linear scanning is acceptable.

## 2.7 The Outbox Pattern

Chapter 1 described the two-system problem: data and events served by separate systems, with the engineering challenge of keeping them synchronized. The outbox pattern was presented as the best available mitigation. MQDB goes one step further: the outbox is not a separate table polled by a relay. It is entries in the same key-value store, committed in the same batch as the data.

The `Outbox` struct manages entries under the `_outbox/` prefix. `enqueue_event` adds a `StoredOutboxEntry` to a `BatchWriter`:

```rust
pub fn enqueue_event(&self, batch: &mut BatchWriter, operation_id: &str, event: &ChangeEvent) {
    let key = format!("_outbox/{operation_id}");
    let stored = StoredOutboxEntry {
        events: vec![event.clone()],
        retry_count: 0,
        created_at: now_unix_secs(),
        dispatched_count: 0,
    };
    batch.insert(key.into_bytes(), serde_json::to_vec(&stored).unwrap_or_default());
}
```

The batch is not committed here — the caller does that, after also inserting data, indexes, and constraint entries. One `commit()` persists everything.

### The Happy Path

The normal flow: `batch.commit()` → `dispatcher.dispatch(event)` → `outbox.mark_delivered(operation_id)`. The `mark_delivered` call simply removes the `_outbox/{operation_id}` key. The entry existed for a few microseconds between commit and dispatch.

### The Crash Path

If the process crashes between `batch.commit()` and `mark_delivered`, the outbox entry survives in storage. On restart, the `OutboxProcessor` background task periodically scans `_outbox/*` and re-dispatches pending entries. For each entry, it skips already-dispatched events using the `dispatched_count` field (supporting partial recovery for multi-event entries like cascade deletes), dispatches remaining events, and either marks the entry delivered or increments its retry count.

After `max_retries` (default 10), entries are moved to `_dead_letter/` — same structure, different prefix. Dead letter entries can be inspected and manually replayed or removed. They never disappear silently.

### Multi-Event Entries

A cascade delete produces multiple events from one operation: the primary delete plus all cascaded deletes and set-null updates. The `enqueue_events` method stores a `Vec<ChangeEvent>` in a single outbox entry. If dispatch fails partway through (say, 2 of 5 events dispatched), `update_dispatched_count` records the progress. On retry, the processor skips the already-dispatched events and resumes from where it left off.

### Why This Eliminates the Two-System Problem

The traditional outbox pattern requires a separate relay process that polls the outbox table and publishes events to a message broker. MQDB's outbox publishes events to the same system that stored them — because the event dispatcher is part of the database, not an external broker. The "relay" is a simple scan of `_outbox/*` keys, dispatching directly to in-process subscribers. No network hop, no external broker, no polling lag.

The guarantee is the same as the traditional outbox: data and event share one transaction boundary. But the implementation is simpler because there is only one system.

### What Went Wrong: The Durability-Consistency Coupling

The outbox design contains a subtle coupling that only became apparent during cluster mode development. In agent mode with `Immediate` durability, every `batch.commit()` forces an fsync. The outbox entry hits disk before the process continues. If the process crashes after commit but before dispatch, the entry survives.

In cluster mode with `PeriodicMs(10)`, fsync happens every 10 milliseconds. A crash within that window loses both the data write and the outbox entry — which is correct, because they share the same batch. But during development, an early version of the cluster outbox wrote the data to an in-memory store and the outbox entry to Fjall separately. A crash could lose the in-memory data while the outbox entry survived on disk, causing the processor to replay events for records that no longer existed. The fix was to ensure that the in-memory store and the Fjall-backed outbox entry share the same write path, so their durability fates are always coupled. This is a classic mistake that lead to many hours of TLA+ modelling.

The lesson: the outbox guarantee is not just "data and event are in the same batch." It is "data and event have the same durability fate." If one can survive a crash that the other cannot, the guarantee is broken regardless of batch atomicity.

## 2.8 What Cluster Mode Changes

In a network distributed system, things change slightly. Every component in this chapter has a cluster-mode counterpart. The single-node design is not thrown away — it is the foundation that cluster mode extends.

**Storage** moves from Fjall on disk to in-memory `HashMap` per partition, with Fjall backing for crash recovery. Each partition maintains its own data store, index store, and constraint store. The `StorageBackend` trait is not used directly in cluster mode — instead, the `StoreManager` holds `HashMap`-based stores that provide the same operations (get, insert, remove, prefix scan) but scoped to a single partition.

**Schemas and constraints** become binary `BeBytes` structs replicated across nodes via the `ReplicationWrite` pipeline. When a schema is created in cluster mode, it is serialized into a compact binary format and replicated to all nodes as a partitioned write. The `SchemaRegistry` and `ConstraintManager` still exist, but they are populated from replicated data rather than loaded from local `meta/*` keys.

**Indexes** become hex-encoded entries in an in-memory `BTreeMap`. The encoding changes from raw bytes to hex strings to accommodate the in-memory storage format, but the lookup semantics — prefix scan on `{entity}/{field}/{value}/` — remain identical.

**Foreign key validation** becomes async cross-node requests. In agent mode, validating a foreign key is a synchronous local key lookup: `storage.get(&target_key)`. In cluster mode, the target entity may live on a different node's partition. The constraint check sends a request over the cluster transport, drops the lock, awaits the response, reacquires the lock, and completes the operation. Cascade side effects are partitioned into local and remote operations via a `CascadeSideEffect` enum with four variants: `LocalDelete`, `LocalSetNull`, `RemoteDelete`, and `RemoteSetNull`. Local effects execute immediately in the same batch. Remote effects are fired over the cluster transport to the partition that owns the target entity. Chapter 15 covers this lock-drop/reacquire pattern and the consistency implications of the gap between check and write.

**The outbox** becomes the `ClusterOutbox`, following the same pattern: data and event share one batch commit, with a background processor for crash recovery. The guarantee is preserved — data and its corresponding event have the same durability fate. Cluster mode adds a second outbox prefix, `_cascade/`, for remote FK side effects. When a delete triggers cascade or set-null operations on entities owned by other partitions, the remote operations are recorded as a `CascadeOutboxPayload` in the same atomic batch as the primary delete. The coordinating node sends the cascade requests and waits for acknowledgments from each target partition. The `_cascade/` entry is marked delivered only after all acks arrive. If the node crashes before acknowledgments complete, the startup recovery procedure scans both `_outbox/` and `_cascade/` prefixes and replays any pending operations.

The agent-mode design's value is that it makes every invariant explicit and testable in a single-process environment. Constraint validation, index maintenance, the outbox guarantee — all of these are correct and complete on a single node. Cluster mode adds routing and coordination on top, but the per-partition logic is the same logic that runs in agent mode. The foundation holds.

### The Configuration Surface

The `DatabaseConfig` struct controls every tunable in the system: durability mode (Immediate, PeriodicMs, or None), event channel capacity (default 1,000), maximum list results (default 10,000), maximum subscriptions (default 1,000), TTL cleanup interval, cursor and sort buffer sizes, outbox retry parameters, shared subscription partition count, and an optional encryption passphrase. The builder-pattern API lets each parameter be overridden independently:

```rust
let config = DatabaseConfig::new("./data/mydb")
    .with_durability(DurabilityMode::PeriodicMs(10))
    .with_event_capacity(5000)
    .with_passphrase("secret".into());
```

A `without_background_tasks()` method disables all background tasks — used in tests to avoid flaky timing-dependent assertions, and in cluster mode where the cluster's own event loop handles the equivalent work.

These configuration knobs are the translation layer between the design decisions in this chapter and the operational requirements of deployment. The defaults are chosen for agent mode: `Immediate` durability (safety first), 60-second TTL cleanup (minimal background I/O), 10 outbox retries before dead-lettering (enough to survive transient failures without infinite loops). Cluster mode overrides most of these: `PeriodicMs(10)` for throughput, background tasks disabled (the cluster event loop handles outbox and cleanup), and in-memory stores instead of Fjall for partition data.
