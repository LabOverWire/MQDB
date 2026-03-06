# Chapter 2: The Storage Foundation

Before distributing anything, one needs to build the single-node database. The components introduced in this chapter — key encoding, pluggable backends, the schema and constraint system, secondary indexes, reactive subscriptions, and the transactional outbox — reappear in Part II when the system goes distributed. The agent-mode design is the foundation that cluster mode extends.

## 2.1 A Flat Key Space

The entire database lives in a single sorted key-value store. Every piece of data — user records, index entries, schemas, subscriptions, pending events — shares one keyspace. The only structure comes from prefixes: byte-string conventions that partition the flat space into logical namespaces.

Eight prefixes create the full namespace:

| Prefix          | Purpose                                     | Example                                     |
| --------------- | ------------------------------------------- | ------------------------------------------- |
| `data/`         | Entity records                              | `data/users/abc-123`                        |
| `idx/`          | Secondary indexes, unique constraint checks | `idx/users/email/alice@example.com/abc-123` |
| `meta/`         | Schemas, constraints, counters              | `meta/schema/users`, `meta/seq:users`       |
| `sub/`          | Subscriptions                               | `sub/sub-001`                               |
| `_outbox/`      | Pending change events                       | `_outbox/op-uuid`                           |
| `dedup/`        | Deduplication markers                       | `dedup/corr-id`                             |
| `_dead_letter/` | Failed outbox entries                       | `_dead_letter/op-uuid`                      |
| `_crypto/`      | Encryption metadata                         | `_crypto/salt`, `_crypto/check`             |

The first six are visible to application logic. The last two are infrastructure: `_dead_letter/` holds outbox entries that exhausted their retries, and `_crypto/` stores the encryption salt and verification marker for the encrypted backend.

Why a flat keyspace instead of separate column families or tables? Because the atomicity boundary is a single batch commit across all prefixes. When a create operation writes a data key, its index entries, and its outbox event, all three land in one batch. If the database used separate stores, cross-store atomicity would require a distributed transaction protocol — exactly the complexity that Chapter 1 argued against.

### Lexicographic Ordering

The key encoding is designed so that lexicographic byte ordering produces meaningful results. A prefix scan on `data/users/` returns all users. A prefix scan on `idx/users/email/alice@example.com/` returns all user IDs with that email. A range scan between `idx/users/age/{encoded_30}` and `idx/users/age/{encoded_65}` returns all user IDs with ages in that range — provided the numeric encoding preserves sort order in byte representation, which is the subject of the next subsection. Every query the database supports reduces to either a point lookup, a prefix scan, or a range scan over sorted bytes.

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

### Encoding Numbers in Keys

The data keys shown above contain only strings — entity names and IDs — which sort correctly in byte order by default. But the prefix table listed another namespace: `idx/`, for secondary indexes. Section 2.5 covers indexes in full, but the key structure matters here because an index key embeds a field's _value_ directly into the key:

```
idx/{entity}/{field}/{encoded_value}/{id}
```

A query like "find all users with age greater than 30" becomes a range scan starting at `idx/users/age/{encoded_30}/`. For this to work, the encoded value must sort in the same order as the original number. String values need no special treatment — `"alice"` sorts before `"bob"` in both human and byte order. Numbers are the problem.

If the database stored the number 9 as the ASCII string `"9"` and 42 as `"42"`, then `"9"` would sort _after_ `"42"` because `'9' > '4'` in ASCII. Negative numbers are worse — `-1` would sort after `-5` in text because `'1' > '5'` in the second character. Any range query over numeric fields would return garbage.

The encoding solves both problems by working directly on the binary representation instead of text. For integers, it converts to big-endian bytes and flips the sign bit:

```rust
fn encode_i64_sortable(val: i64) -> [u8; 8] {
    let mut out = val.to_be_bytes();
    out[0] ^= 0x80;
    out
}
```

Two's complement big-endian already sorts positive integers correctly — `1` produces smaller bytes than `1000`. The problem is the sign bit: in two's complement, negative numbers have the high bit set, so they sort _after_ all positive numbers in raw byte order. Flipping the sign bit with XOR 0x80 inverts this relationship: negatives now sort before positives in byte order. The result is that `i64::MIN` encodes to `0x00_00_00_00_00_00_00_00` and `i64::MAX` to `0xFF_FF_FF_FF_FF_FF_FF_FF`, with every value in between landing at the correct position for byte comparison.

Floats use the same sign-bit idea on IEEE 754 bits, but negative floats need all bits flipped because IEEE 754 reverses the magnitude ordering for negative values — a more negative float has a larger binary representation:

```rust
fn encode_f64_sortable(val: f64) -> [u8; 8] {
    let mut out = val.to_bits().to_be_bytes();
    if val.is_sign_negative() {
        for b in &mut out { *b ^= 0xFF; }
    } else {
        out[0] ^= 0x80;
    }
    out
}
```

The full range from `f64::NEG_INFINITY` through zero to `f64::INFINITY` sorts correctly in byte order. Both encodings produce exactly 8 bytes per number — fixed-width, compact, and comparison-friendly. A single `memcmp` determines ordering without parsing.

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

On read, the corresponding decode function extracts the stored checksum, recomputes it over the payload, and returns an error if they differ. The error identifies exactly which record is damaged — the entity and ID are included in the error context.

This catches bitflips in storage, truncated writes, and corruption from misaligned reads. CRC32 is not cryptographic — it won't detect intentional tampering — but it is fast enough to run on every read without measurable overhead. The test suite verifies that flipping any single bit in the payload triggers detection.

### ID Extraction

The `id` field lives in the storage key, not in the JSON blob. When an entity is constructed from input JSON, the `id` field is extracted and removed from the object. When the entity is serialized back to JSON, `id` is re-inserted. This separation serves two purposes: it eliminates redundant storage (the ID is already encoded in the key), and it ensures the ID is always consistent with the key — no possibility of the JSON `id` field disagreeing with the storage key.

## 2.2 Pluggable Storage Backends

The storage layer is defined by a trait, not a concrete type. The storage trait declares seven operations:

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

**Fjall** is the production backend. It wraps an LSM-tree engine using a single keyspace. Three durability modes control fsync behavior: Immediate flushes every write synchronously, Periodic relies on the engine's background flush at a configurable interval (used in cluster mode for throughput), and None skips persistence entirely. Prefix scans and range scans operate on a snapshot, providing a consistent view even while concurrent writes land — the iterator sees the database as it was at snapshot creation time.

**Memory** is a sorted map behind a read-write lock. It provides the same sort order as Fjall — the map iterates in sorted key order, just like an LSM-tree. Prefix scans use range iteration with a predicate. This backend runs in tests and in WASM where filesystem access is unavailable.

**Encrypted** is a decorator that wraps any other backend. It uses AES-256-GCM for per-value encryption with a key derived from a passphrase via PBKDF2 (600,000 iterations of HMAC-SHA256). Each value gets a fresh 12-byte random nonce. The storage key itself is used as Associated Authenticated Data (AAD) for the GCM cipher — meaning a value encrypted under key `data/users/42` cannot be decrypted if someone moves it to key `data/users/99`. On first open, the backend generates a random 32-byte salt, stores it at `_crypto/salt`, encrypts a known plaintext (`"mqdb"`) and stores it at `_crypto/check`. On subsequent opens, it re-derives the key and attempts to decrypt the check value — a wrong passphrase fails immediately rather than silently producing garbage on later reads.

### The Batch Contract

The batch operations trait is where atomicity lives:

```rust
pub trait BatchOperations: Send {
    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>);
    fn remove(&mut self, key: Vec<u8>);
    fn expect_value(&mut self, key: Vec<u8>, expected_value: Vec<u8>);
    fn commit(self: Box<Self>) -> Result<()>;
}
```

The insert and remove methods queue operations. The expect-value method registers a precondition: at commit time, the current value for this key must match the expected value exactly, or the entire batch is rejected with a conflict error. This is optimistic concurrency control — no locks are held during the batch construction phase. The check and write happen atomically at commit time.

In the Fjall backend, commit first takes a snapshot and checks all preconditions against it, then applies all operations in a single batch. In the memory backend, commit acquires the write lock, checks preconditions, and applies operations while holding the lock — the write lock serializes all batch commits.

The encrypted backend has a subtlety: the expect-value call receives a plaintext expected value, but the underlying backend stores ciphertext. During commit, the encrypted batch reads the current ciphertext, decrypts it, compares with the expected plaintext, and if matched, re-encrypts and passes the ciphertext expectation to the inner batch. Two layers of precondition checking, but the caller only sees plaintext.

Why trait-based abstraction? Because the same database code — CRUD operations, constraint validation, index maintenance — runs identically regardless of whether data is on disk (Fjall), in memory (tests), or encrypted. The caller never knows which backend is active. This pays off immediately in testing (memory backend, no filesystem setup) and later in WASM deployment (memory backend, no native dependencies).

## 2.3 The Database API

The database struct ties the components together. It holds 13 shared-ownership fields:

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

The open method is the entry point. It opens storage (encrypted or plain, depending on config), loads schemas and constraints from metadata keys, loads subscriptions from storage, and spawns up to three background tasks: the outbox processor (retries failed event dispatches), TTL cleanup (scans for expired records), and consumer timeout cleanup (evicts stale members from consumer groups). A watch channel coordinates shutdown — sending a signal triggers all background tasks to exit their loops.

### The Create Path

Walking through the create path reveals the atomicity model:

1. **Apply schema defaults.** If a schema exists for this entity, insert any missing fields that have default values defined.
2. **Generate or accept ID.** If the input JSON contains an `id` field, use it. Otherwise, generate a sequential ID by reading a per-entity counter from metadata, incrementing it, and writing it back. A mutex serializes ID generation to prevent duplicates.
3. **Handle TTL.** If a TTL is present, compute an expiration timestamp and remove the TTL field. The TTL cleanup task will delete the record after expiration.
4. **Inject version.** Set the version counter to 1. Every update increments this field, enabling optimistic concurrency on updates via the expect-value precondition.
5. **Validate schema.** If a schema exists, verify all required fields are present and all provided values match their declared types.
6. **Create a batch.** This is the atomicity boundary. Everything from here to commit is all-or-nothing.
7. **Validate constraints.** Check unique constraints (no duplicate values), foreign key constraints (referenced entity exists), and not-null constraints (required fields are non-null). These checks happen against the current storage state.
8. **Insert data key.** Serialize the entity (with CRC32 checksum) and add the insert to the batch.
9. **Update indexes.** For each indexed field, add an index entry to the batch.
10. **Enqueue outbox entry.** Serialize the change event and add it to the batch under the outbox prefix.
11. **Commit.** One call persists data, indexes, and outbox entry atomically.
12. **Dispatch event.** Push the change event to subscribers through the event dispatcher.
13. **Mark delivered.** Delete the outbox entry. If dispatch fails or the process crashes, the outbox processor will retry on the next sweep.

Steps 7 through 11 share one batch. That single commit is the atomicity boundary. Either the record, its indexes, and its outbox entry all exist, or none of them do. There is no window where data exists without its corresponding event, and no window where an event exists without its corresponding data.

### The Update Path

The update path follows the same structure but adds a precondition. After reading the existing record, the update merges new fields into the existing data (a shallow merge — top-level keys are replaced, nested objects are not deep-merged), increments the version, validates the schema, and builds a batch. Before inserting the new value, it registers an expect-value precondition — the raw bytes of the record as they were read. At commit time, if the stored bytes differ (because another concurrent update landed in between), the entire batch is rejected with a conflict error. The caller can retry with a fresh read.

This is optimistic concurrency control at the storage layer. No locks are held during the read-modify-write cycle. The window between read and commit is a race window, but the precondition makes the race detectable rather than destructive. In practice, conflicts are rare because MQDB's primary workloads — IoT telemetry, event streams — are insert-heavy rather than update-heavy.

### The Delete Path

Delete is the most complex operation because it triggers constraint cascades. The sequence: read the existing record, check constraints (which may return a list of cascade and set-null operations), build a batch containing the primary delete plus all side effects, enqueue outbox events for every affected record, and commit. The delete path demonstrates the full power of the single-batch model: a cascade that touches 20 records across 3 entities still commits in one atomic operation. If any part fails — a constraint blocks it, serialization fails, the batch commit encounters a conflict — nothing changes.

### Background Tasks

Three tasks run in the background after the database opens:

**The outbox processor** wakes on a configurable interval (default 5 seconds), scans `_outbox/*`, and re-dispatches pending entries. This is the crash recovery mechanism described in Section 2.7.

**The TTL cleanup task** wakes periodically (default 60 seconds), scans all `data/*` keys, deserializes each record, checks the `_expires_at` field against the current time, and deletes expired records. Expired records are removed in a single batch with their index entries and outbox events — the same atomic pattern as a normal delete, just triggered by the clock instead of a client request.

**The consumer timeout task** wakes at half the consumer timeout interval, iterates through all consumer groups, and evicts members whose last heartbeat exceeds the timeout. Evicted members' partitions are redistributed to surviving members via the rebalance algorithm.

All three tasks are coordinated by a watch channel. On shutdown, the database signals the channel, and each task exits its select loop.

## 2.4 Schema and Constraints

Schemas are opt-in. An entity without a schema accepts any JSON object — the database operates as a schemaless document store. When a schema exists, it enforces structure without restricting it: fields declared in the schema must conform to their types, but additional fields are allowed. This is an open schema model.

Six types are supported: String, Number, Boolean, Array, Object, and Null. Each field definition carries a name, a type, a required flag, and an optional default value. Defaults are applied before validation — a field with a default value is never "missing" by the time validation runs.

Schemas are persisted in the metadata keyspace as JSON and loaded into memory at startup. The schema registry holds an in-memory map and delegates validation: if a schema exists for the entity, validate; otherwise, pass through.

### Three Constraint Types

**Unique constraints** enforce that no two records share the same value for a given field (or combination of fields for composite constraints). Validation works by prefix-scanning the index for the field value: `idx/{entity}/{field}/{value}/`. If any key exists in that prefix range with a different ID, the constraint is violated. This means unique constraints depend on secondary indexes being maintained — the index must exist for the constraint check to work.

**Not-null constraints** verify that a field exists in the JSON object and its value is not JSON `null`. The check is a simple field lookup on the entity data.

**Foreign key constraints** verify referential integrity. On create or update, if the source field contains a non-null value, the constraint checks that the referenced target entity exists by performing a direct key lookup (`data/{target_entity}/{target_id}`). If the key is missing, the operation is rejected.

### Cascade Delete

The interesting complexity is in delete. When a record is deleted, the constraint manager must find all records that reference it via foreign key and apply the configured policy. Three policies exist:

**Restrict** blocks the delete if any referencing records exist. The check scans all records of the source entity, deserializes each one, and checks whether its FK field points to the record being deleted.

**Cascade** deletes all referencing records recursively. The constraint validation returns a list of every cascade and set-null operation needed. The implementation uses depth-first search with a visited set to detect cycles — if entity A references entity B which references entity A, the cycle is broken by the visited check.

**SetNull** nullifies the FK field in referencing records instead of deleting them. The field is set to JSON `null` and the record's version is incremented.

The caller builds a single batch containing the primary delete, all cascade deletes, all set-null updates, and all corresponding index removals and outbox events. One commit — the entire cascade tree is atomic. Either the parent and all its cascaded children are deleted, or nothing changes.

```rust
let delete_ops = constraint_manager.validate_delete(&existing_entity, &self.storage)?;
let mut batch = self.storage.batch();
batch.remove(key.clone());
// ... process each DeleteOperation into batch inserts/removes ...
batch.commit()?;
```

Constraints are persisted in the metadata keyspace and loaded at startup. This means constraint definitions survive restarts — they are part of the database state, not ephemeral configuration.

This entire constraint system is agent-mode only — local key lookups, local prefix scans, one batch commit. Chapter 15 covers what happens when foreign key validation must cross partition boundaries in cluster mode: the local key lookup becomes an async cross-node request, the lock must be dropped and reacquired, and the consistency window between check and write opens wider.

## 2.5 Secondary Indexes

Index entries are keys with empty values. The key encodes everything:

```
idx/{entity}/{field}/{encoded_value}/{id} → []
```

A prefix scan on `idx/users/email/alice@example.com/` returns all user IDs with that email address. The ID is extracted from the last segment of the key. The value is empty — zero bytes — because the information is entirely in the key structure.

Why empty values? Index entries are write-heavy. Every create adds index entries, every update removes old entries and adds new ones, every delete removes entries. If index entries stored a copy of the record data, every data write would mean writing the data twice — once to the data key, once to each index entry. Empty values mean index maintenance is cheap: just key insertions and deletions. The actual record is always fetched from the data key when needed.

### Atomic Index Maintenance

The index manager provides two operations: update (for creates and updates) and remove (for deletes). Both operate on the same batch as the data write:

```rust
fn update_indexes(&self, batch: &mut BatchWriter, entity: &Entity, old_entity: Option<&Entity>) {
    if let Some(old) = old_entity {
        Self::remove_index_entries(batch, old, fields);
    }
    Self::add_index_entries(batch, entity, fields);
}
```

On update, old index entries are removed and new ones are added in the same batch. The commit that persists the data also persists the index changes. There is no window where the data says one thing and the index says another.

Two lookup modes perform the reverse operation — finding entity IDs from index entries:

**Equality lookup** handles exact matches: given a field name and value, it prefix-scans the index for that value and extracts the ID from each key's last segment. A query filtering `email = "alice@example.com"` hits the index directly instead of scanning every record.

**Range lookup** handles inequalities: given a field name with optional lower and upper bounds, it range-scans the index between the encoded bound values. A query filtering `age > 30` scans from the encoded representation of 30 to the end of the age index. Each bound carries an inclusive flag — `>=` includes the boundary value, `>` excludes it. This is where the sort-preserving numeric encoding pays off: because the encoding of 30 sorts before the encoding of 31 in byte order, the storage engine's range scan returns exactly the right records without any post-filtering.

The query planner tries three strategies in order: first, an indexed equality filter; second, indexed range filters (combining multiple range conditions on the same field into a single scan with both lower and upper bounds); third, a full table scan as fallback. The first two produce a candidate ID set that the remaining non-indexed filters narrow down.

Arrays and objects cannot be indexed — only scalar values (strings, numbers, booleans, null) produce index entries. This keeps the index structure flat and the lookup semantics unambiguous.

Index entries serve three consumers, not one. The index manager uses them for equality and range lookups. The constraint manager scans them to enforce unique constraints — prefix-scanning for a field value and rejecting the write if any key exists with a different ID. And the query planner routes through them whenever a filter targets an indexed field. The same empty-value keys serve all three purposes because the information is in the key structure, not the value.

## 2.6 Reactive Subscriptions

Every write produces a `ChangeEvent`:

```rust
pub struct ChangeEvent {
    pub sequence: u64,              // Global monotonic counter
    pub entity: String,             // "users", "orders", etc.
    pub id: String,                 // Record identifier
    pub operation: Operation,       // Create, Update, or Delete
    pub data: Option<Value>,        // Full record data, including for deletes
    pub operation_id: Option<String>, // Tracks multi-event operations (e.g., cascades)
    pub sender: Option<String>,     // Who initiated the write
    pub client_id: Option<String>,
    pub scope: Option<(String, String)>,
}
```

The sequence field comes from a global atomic counter incremented on every event creation. Every event has a unique, monotonically increasing sequence number. In cluster mode, sequence numbers are scoped to partitions (Chapter 5), but in agent mode the global counter suffices.

### Pattern Matching

Subscriptions use MQTT-style wildcard patterns. `+` matches exactly one path segment. `#` matches zero or more remaining segments. The matching function splits both pattern and path on `/` separators and walks the segments:

- `users/+` matches `users/123` but not `users/123/profile`
- `users/#` matches `users/123` and `users/123/profile` and `users`
- `+/123` matches `users/123` and `orders/123`

Each `Subscription` optionally filters on entity name before pattern matching — a subscription for entity `"users"` with pattern `users/+` won't accidentally match a `posts/users/123` event.

### Three Dispatch Modes

The event dispatcher categorizes matching subscriptions by their mode:

**Broadcast** is the default. Every matching subscription receives every event. The dispatcher iterates through all matching broadcast subscriptions and sends the event to each listener's channel. This is the mode used by MQTT clients subscribing to `$DB/users/events/#` — every subscriber sees every user change.

**LoadBalanced** distributes events across members of a share group using round-robin. An atomic counter per group tracks the current position. Each event goes to exactly one member of the group. This is the shared subscription model: multiple workers processing events with each event handled by exactly one worker.

**Ordered** routes events by partition. The event's partition is computed by hashing the entity and ID combination, modulo the partition count. A consumer group tracks which consumer owns which partition. All events for the same entity-and-ID combination always land on the same partition and therefore the same consumer. This guarantees ordering: a create followed by an update followed by a delete for `users/42` will always arrive at the same consumer in that order.

### Consumer Groups

Consumer groups implement partition assignment. When a consumer joins, the group triggers a rebalance. The algorithm is simple: sort consumer IDs lexicographically, then assign partitions round-robin. With 8 partitions and 2 consumers (`"a"` and `"b"`), consumer `"a"` gets partitions [0, 2, 4, 6] and consumer `"b"` gets [1, 3, 5, 7]. The lexicographic sort ensures deterministic assignment — two group instances with the same members produce identical assignments regardless of join order.

Stale member detection runs as a background task. Each member has a `last_heartbeat` timestamp updated on activity. If the elapsed time exceeds `consumer_timeout_ms` (default 30 seconds), the member is removed and the group rebalances. The surviving members absorb the evicted member's partitions.

The partition count for consumer groups defaults to 8 — configurable at startup. This is independent of the 256 partitions used for data distribution in cluster mode. These are logical partitions for event routing, not storage partitions.

### Event Topics

Each `ChangeEvent` generates a topic path that determines which MQTT subscribers receive it. The topic structure depends on context:

Without scoping: `$DB/{entity}/events/{id}` — or, when partitioned shared subscriptions are active, `$DB/{entity}/events/p{partition}/{id}`. The partition number in the topic path enables MQTT-level shared subscription routing: subscribers to `$DB/users/events/p3/#` only receive events for records that hash to partition 3.

With scoping: `$DB/{scope_entity}/{scope_value}/{entity}/events/{event_type}`. Scoping allows hierarchical event routing — an order event scoped to a tenant produces a topic like `$DB/tenants/acme/orders/events/created`, so a subscriber listening to `$DB/tenants/acme/#` receives all events across all entities within that tenant.

The `event_type` suffix distinguishes operation types: `created`, `updated`, `deleted`. A subscriber can filter for only creations by subscribing to `$DB/users/events/created` rather than `$DB/users/events/#`.

### Subscription Persistence

Subscriptions are persisted in storage as serialized JSON. The subscription registry loads them at startup and maintains an in-memory map for fast matching. On every event dispatch, the registry scans all subscriptions and returns those that match — a linear scan through the subscription list. For workloads with thousands of subscriptions, this becomes the chapter's performance story: the current implementation trades throughput for simplicity. A trie-based index (similar to what the cluster mode uses for MQTT topic subscriptions in Chapter 8) would accelerate matching, but the agent-mode subscription count is typically small enough that linear scanning is acceptable.

## 2.7 The Outbox Pattern

Chapter 1 described the two-system problem: data and events served by separate systems, with the engineering challenge of keeping them synchronized. The outbox pattern was presented as the best available mitigation. MQDB goes one step further: the outbox is not a separate table polled by a relay. It is entries in the same key-value store, committed in the same batch as the data.

The outbox manages entries under a dedicated key prefix. Enqueueing an event adds a stored entry to the batch:

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

The normal flow: commit the batch, dispatch the event to subscribers, then mark the outbox entry as delivered. Marking delivered simply removes the outbox key. The entry existed for a few microseconds between commit and dispatch.

### The Crash Path

If the process crashes between commit and delivery, the outbox entry survives in storage. On restart, the outbox processor background task periodically scans the outbox prefix and re-dispatches pending entries. For each entry, it skips already-dispatched events using a dispatched count field (supporting partial recovery for multi-event entries like cascade deletes), dispatches remaining events, and either marks the entry delivered or increments its retry count.

After exhausting retries (default 10), entries are moved to the dead letter prefix — same structure, different location. Dead letter entries can be inspected and manually replayed or removed. They never disappear silently.

### Multi-Event Entries

A cascade delete produces multiple events from one operation: the primary delete plus all cascaded deletes and set-null updates. A single outbox entry stores the entire batch of change events. If dispatch fails partway through (say, 2 of 5 events dispatched), the dispatched count records the progress. On retry, the processor skips the already-dispatched events and resumes from where it left off.

### Why This Eliminates the Two-System Problem

The traditional outbox pattern requires a separate relay process that polls the outbox table and publishes events to a message broker. MQDB's outbox publishes events to the same system that stored them — because the event dispatcher is part of the database, not an external broker. The "relay" is a simple scan of the outbox keyspace, dispatching directly to in-process subscribers. No network hop, no external broker, no polling lag.

The guarantee is the same as the traditional outbox: data and event share one transaction boundary. But the implementation is simpler because there is only one system.

### What Went Wrong: The Durability-Consistency Coupling

The outbox design contains a subtle coupling that only became apparent during cluster mode development. In agent mode with immediate durability, every batch commit forces an fsync. The outbox entry hits disk before the process continues. If the process crashes after commit but before dispatch, the entry survives.

Cluster mode uses periodic flushing (every 10 milliseconds), where fsync happens on a timer. A crash within that window loses both the data write and the outbox entry — they share the same batch, so either both survive or neither does. But during development, an early version of the cluster outbox wrote the data to an in-memory store and the outbox entry to Fjall separately. A crash could lose the in-memory data while the outbox entry survived on disk, causing the processor to replay events for records that no longer existed. The fix was to ensure that the in-memory store and the Fjall-backed outbox entry share the same write path, so their durability fates are always coupled. This is a classic mistake that led to many hours of formal modelling before the root cause became clear.

The lesson: the outbox guarantee is not just "data and event are in the same batch." It is "data and event have the same durability fate." If one can survive a crash that the other cannot, the guarantee is broken regardless of batch atomicity.

## 2.8 What Cluster Mode Changes

In a distributed system, most components in this chapter change substantially. The single-node design is not thrown away — it is the foundation that cluster mode extends.

**Storage** moves from the LSM-tree on disk to in-memory hash maps per partition, with the LSM-tree backing for crash recovery. Each partition maintains its own data store, index store, and constraint store. The storage trait is not used directly in cluster mode — instead, a store manager holds per-partition maps that provide the same operations (get, insert, remove, prefix scan) but scoped to a single partition.

**Schemas and constraints** become binary structs replicated across nodes via the replication pipeline. When a schema is created in cluster mode, it is serialized into a compact binary format and replicated to all nodes as a partitioned write. The schema registry and constraint manager still exist, but they are populated from replicated data rather than loaded from local metadata keys.

**Indexes** become hex-encoded entries in an in-memory sorted map. The encoding changes from raw bytes to hex strings because the in-memory map uses string keys (for JSON serialization during replication), and raw bytes are not valid UTF-8. The lookup semantics — prefix scan by entity, field, and value — remain identical.

**Foreign key validation** becomes async cross-node requests. In agent mode, validating a foreign key is a synchronous local key lookup. In cluster mode, the target entity may live on a different node's partition. The constraint check sends a request over the cluster transport, drops the lock, awaits the response, reacquires the lock, and completes the operation. Cascade side effects are partitioned into local and remote operations — local deletes and set-nulls execute immediately in the same batch, while remote effects are fired over the cluster transport to the partition that owns the target entity. Chapter 15 covers this lock-drop/reacquire pattern and the consistency implications of the gap between check and write.

**The outbox** becomes the cluster outbox, following the same pattern: data and event share one batch commit, with a background processor for crash recovery. The guarantee is preserved — data and its corresponding event have the same durability fate. Cluster mode adds a second outbox prefix for remote FK side effects. When a delete triggers cascade or set-null operations on entities owned by other partitions, the remote operations are recorded in the same atomic batch as the primary delete. The coordinating node sends the cascade requests and waits for acknowledgments from each target partition. The cascade entry is marked delivered only after all acknowledgments arrive. If the node crashes before acknowledgments complete, the startup recovery procedure scans both outbox prefixes and replays any pending operations.

The agent-mode design's value is that it makes every invariant explicit and testable in a single-process environment. Constraint validation, index maintenance, the outbox guarantee — all of these are correct and complete on a single node. Cluster mode adds routing and coordination on top, but the per-partition logic is the same logic that runs in agent mode. The foundation holds.

### The Configuration Surface

The database configuration controls every tunable in the system: durability mode (immediate, periodic, or none), event channel capacity (default 1,000), maximum list results (default 10,000), maximum subscriptions (default 1,000), TTL cleanup interval, cursor and sort buffer sizes, outbox retry parameters, shared subscription partition count, and an optional encryption passphrase. The builder-pattern API lets each parameter be overridden independently:

```rust
let config = DatabaseConfig::new("./data/mydb")
    .with_durability(DurabilityMode::PeriodicMs(10))
    .with_event_capacity(5000)
    .with_passphrase("secret".into());
```

A configuration option disables all background tasks — used in tests to avoid flaky timing-dependent assertions, and in cluster mode where the cluster's own event loop handles the equivalent work.

The defaults are chosen for agent mode: immediate durability (safety first), 60-second TTL cleanup (minimal background I/O), 10 outbox retries before dead-lettering (enough to survive transient failures without infinite loops). Cluster mode overrides most of these: periodic flushing for throughput, background tasks disabled (the cluster event loop handles outbox and cleanup), and in-memory stores instead of the LSM-tree for partition data.
