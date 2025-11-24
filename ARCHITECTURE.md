# MQDB Architecture

## 1. Overview & Design Philosophy

### What is MQDB?

MQDB is a reactive embedded database built in Rust that combines:
- **Reactive subscriptions** with MQTT-style pub/sub patterns
- **Schema-less JSON** entities with optional schema validation
- **ACID transactions** with optimistic locking
- **LSM-based storage** built on Fjall engine

### Core Principles

1. **Reactive-first**: All mutations trigger subscription events, enabling real-time UIs and event-driven architectures
2. **Embedded efficiency**: Low-latency local access with minimal dependencies
3. **Data integrity**: ACID guarantees with comprehensive constraint system (schemas, unique, foreign keys)
4. **Extensibility**: Designed for future MQTT broker integration

### Design Philosophy

> **Correct, then fast. Simple, then featureful.**

MQDB prioritizes correctness and simplicity over raw performance and feature count. The architecture reflects this through explicit error handling, clear component boundaries, and incremental feature additions.

---

## 2. High-Level Architecture

### Architecture Layers (Bottom to Top)

```
┌─────────────────────────────────────────────────┐
│        Application / MQTT Integration           │
├─────────────────────────────────────────────────┤
│         Database API (database.rs)              │
│  CRUD │ Queries │ Subscriptions │ Constraints   │
├─────────────────────────────────────────────────┤
│          Reactive Event System                  │
│  EventDispatcher │ SubscriptionRegistry         │
├─────────────────────────────────────────────────┤
│         Domain Logic Layer                      │
│  Schema │ Constraints │ Indexes │ Relations     │
├─────────────────────────────────────────────────┤
│         Entity & Key Management                 │
│  Entity (JSON ↔ Bytes) │ Keys (Encoding)        │
├─────────────────────────────────────────────────┤
│         Storage Abstraction                     │
│  BatchWriter │ Transactions │ Durability        │
├─────────────────────────────────────────────────┤
│              Fjall LSM Engine                   │
│  Partitions │ Transactions │ Persistence        │
└─────────────────────────────────────────────────┘
```

### Component Interaction

- **Database API** coordinates all operations, managing transactions and event dispatch
- **Reactive System** broadcasts changes to interested subscribers
- **Domain Logic** enforces business rules (constraints, schemas, indexes)
- **Entity Layer** handles JSON serialization and key encoding
- **Storage Layer** provides transactional guarantees and persistence
- **Fjall Engine** manages physical storage (LSM tree, compaction, WAL)

---

## 3. Core Components

### 3.1 Storage Layer (`storage.rs`)

**Responsibility:** Persistence and transaction management

**Key Features:**
- Wraps Fjall `TransactionalKeyspace` for ACID transactions
- `BatchWriter` for atomic multi-key operations
- Optimistic locking via preconditions
- Configurable durability modes (`Immediate`, `Periodic`, `None`)

**Transaction Model:**
```rust
// Optimistic locking pattern
let mut batch = storage.batch();
batch.expect_value(key, old_value);  // Precondition check
batch.insert(key, new_value);
batch.commit()?;  // Fails if precondition not met
```

**Why Fjall?**
- Rust-native LSM implementation (no C bindings)
- Built-in transactional support
- Memory-safe with zero-cost abstractions
- Efficient for write-heavy workloads
- Active maintenance and good performance

---

### 3.2 Key Encoding Scheme (`keys.rs`)

**Namespace Design:**
```
data/{entity}/{id}                          → Entity data
idx/{entity}/{field}/{value}/{id}           → Secondary indexes
sub/{subscription_id}                       → Persistent subscriptions
dedup/{correlation_id}                      → Idempotency cache
meta/{key}                                  → Sequences, metadata
meta/schema/{entity}                        → Schema definitions
meta/constraint/{type}/{entity}/{name}      → Constraint definitions
fkref/{target_entity}/{target_id}/...       → FK reverse lookups (reserved)
```

**Design Decisions:**

1. **Prefix-based organization**: Enables efficient prefix scans for entity listing and index lookups
2. **Hierarchical structure**: Supports MQTT-style wildcard pattern matching
3. **Human-readable**: UTF-8 paths aid debugging (can inspect with hex editor)
4. **Value encoding for indexes**:
   - Numbers: Zero-padded to 20 digits for lexicographic sorting
   - Strings: Direct UTF-8 bytes
   - Booleans: `"true"` or `"false"`
   - Null: `"null"`

**Example Keys:**
```
data/users/123                              → User entity with ID 123
idx/users/email/alice@example.com/123       → Email index entry
meta/schema/users                           → Users schema definition
meta/constraint/fk/posts/posts_author_id_users_fk
```

---

### 3.3 Entity Layer (`entity.rs`)

**Data Model:**
- JSON-based schema-less storage
- ID field separated from data object (not stored in JSON)
- Checksum-protected serialization (CRC32)

**Entity Lifecycle:**
```
User JSON → Entity → Serialize → Checksum+Bytes → Storage
                                                       ↓
User JSON ← Entity ← Deserialize ← Verify ← Read from Storage
```

**Serialization Format:**
```
[4 bytes: CRC32 checksum][N bytes: JSON data]
```

**Corruption Detection:**
- All entities stored with CRC32 checksum prefix
- Verification on every read operation
- Explicit `Error::Corruption` for data integrity issues
- Early detection prevents cascading failures

**ID Handling:**
- IDs stored in key path, not in JSON data
- Automatically added when converting to user-facing JSON
- Reduces redundancy and storage size

---

### 3.4 Index System (`index.rs`)

**Purpose:** Accelerate filtered queries beyond full table scans

**Index Structure:**
```
idx/{entity}/{field}/{value}/{id}

Example:
idx/users/status/active/123
idx/users/status/active/456
idx/users/email/alice@example.com/123
```

**Index Lifecycle:**
- **Create**: Insert index entries for indexed fields
- **Update**: Remove old index entries, insert new ones
- **Delete**: Remove all index entries for entity

**Query Optimization:**
- Equality filters (`FilterOp::Eq`) use indexes when available
- Falls back to full scan for non-indexed fields
- Index maintenance happens within same transaction as data updates

**Current Limitations:**
- Only single-field indexes (no composite indexes yet)
- Only first filter in query uses index
- Other filters applied in-memory after index lookup
- Complex queries may require full scan

**Trade-offs:**
- **Pro**: Simple implementation, covers common cases
- **Pro**: Automatic maintenance (always consistent with data)
- **Con**: Storage overhead (duplicate data in indexes)
- **Con**: Write amplification (update data + all indexes)

---

### 3.5 Constraint System (`constraint.rs`, `schema.rs`)

#### Schema Validation

**Purpose:** Optional type safety for JSON entities

**Features:**
- Type checking: `String`, `Number`, `Boolean`, `Array`, `Object`, `Null`
- Required fields enforcement
- Default values (auto-applied on create)

**Example:**
```rust
let schema = Schema::new("users")
    .add_field(FieldDefinition::new("name", FieldType::String).required())
    .add_field(FieldDefinition::new("age", FieldType::Number))
    .add_field(FieldDefinition::new("status", FieldType::String)
        .default(json!("active")));
```

#### Constraint Types

**1. Unique Constraints**
- Prevent duplicate values across entities
- Single-field: `email` must be unique
- Composite: `(user_id, slug)` pair must be unique
- Implementation: Validated via index prefix scans
- Automatically creates indexes for performance

**2. Not-Null Constraints**
- Field-level enforcement of required values
- Rejects both missing fields and explicit `null`
- Applied on create and update operations

**3. Foreign Key Constraints**
- Reference integrity between entities
- Validates referenced entity exists
- Three deletion policies:
  - `CASCADE`: Recursively delete referencing entities
  - `RESTRICT`: Prevent deletion if references exist
  - `SET_NULL`: Set FK field to null when referenced entity deleted

**Validation Timing:**

All constraint validation happens **within the transaction**:
```
Start Transaction
    ↓
Apply Schema Defaults
    ↓
Validate Schema (types, required fields)
    ↓
Validate Constraints (unique, FK, not-null)
    ↓
Insert/Update Data
    ↓
Update Indexes
    ↓
Commit (atomic)
```

**Cascade Delete Implementation:**

```
Delete user/123
    ↓
Find all FK constraints targeting users
    ↓
For each CASCADE constraint:
    Find referencing entities (posts where author_id=123)
    Recursively collect cascade deletions (comments on those posts)
    ↓
For each RESTRICT constraint:
    Validate no references exist (fail if found)
    ↓
For each SET_NULL constraint:
    Update referencing entities (set author_id=null)
    ↓
Execute all operations in single batch (atomic)
    ↓
Dispatch delete events for all affected entities
```

**Recursive Cascade Handling:**
- Uses depth-first search with visited tracking
- Prevents infinite loops on circular references
- Example: User → Posts → Comments (2 levels deep)
- All deletions happen in single atomic transaction

---

### 3.6 Reactive System (`subscription.rs`, `dispatcher.rs`, `events.rs`)

**Purpose:** Real-time change notifications for event-driven architectures

#### Event Flow

```
Mutation (create/update/delete)
    ↓
BatchWriter.commit() succeeds
    ↓
ChangeEvent created
    ↓
EventDispatcher.dispatch()
    ↓
SubscriptionRegistry.find_matching()
    ↓
Filter by pattern match
    ↓
Broadcast to matching subscribers
```

#### Subscription Matching

**Pattern Format:** `{entity}/{id}`

**MQTT-Style Wildcards:**
- `+`: Matches single level (one segment)
- `#`: Matches multiple levels (zero or more segments)

**Examples:**
```
Pattern          Matches                     Doesn't Match
--------         -------                     -------------
users/+          users/123                   users/123/profile
users/#          users/123                   posts/456
                 users/123/profile
+/123            users/123                   users/456
                 posts/123
users/123        users/123                   users/124
```

**Use Cases:**
- `users/#` - All user changes
- `users/+` - All users, but not nested paths
- `+/123` - Any entity with ID 123
- `users/123/profile` - Specific nested resource

#### Subscription Persistence

**Storage:** `sub/{subscription_id}` → `Subscription{pattern, entity_filter}`

**Lifecycle:**
1. **Create**: Store in `sub/` namespace, add to in-memory registry
2. **Startup**: Load all subscriptions from storage into registry
3. **Delete**: Remove from both storage and registry

**Benefits:**
- Subscriptions survive database restarts
- No need to re-subscribe after crash
- Simplifies application logic

#### Event Delivery

**Broadcast Channel:**
- Bounded capacity (configurable, default 1000)
- Single writer (dispatcher), multiple readers (subscribers)
- Lagging receivers don't block writers (events dropped with warning)

**Per-Subscription Channels:**
- Isolated channels for individual subscriptions
- Each has own buffer and backpressure handling
- Allows different consumers to process at different rates

**Guarantees:**
- Events dispatched in order of commit
- No guarantee of delivery (best-effort)
- Subscribers responsible for handling missed events

---

## 4. Data Flow Diagrams

### 4.1 Create Operation

```
db.create("users", json!({...}))
    ↓
Generate ID (atomic sequence counter)
    ↓
Apply schema defaults (if schema exists)
    ↓
Validate schema (types, required fields)
    ↓
Create Entity object
    ↓
┌─────────────────────────────┐
│   Start BatchWriter         │
│                             │
│   Validate NOT NULL         │
│   Validate Unique (index)   │
│   Validate Foreign Keys     │
│                             │
│   Insert data/{entity}/{id} │
│   Insert index entries      │
│                             │
│   Commit (atomic)           │
└─────────────────────────────┘
    ↓
Dispatch ChangeEvent::Create
    ↓
Return entity with ID to caller
```

### 4.2 Update Operation

```
db.update("users", "123", json!({...}))
    ↓
Read existing entity from storage
    ↓
Merge updates with existing data
    ↓
Validate schema (if exists)
    ↓
┌─────────────────────────────────┐
│   Start BatchWriter             │
│                                 │
│   Add optimistic lock           │
│   expect_value(key, old_data)   │
│                                 │
│   Validate constraints          │
│                                 │
│   Insert updated data           │
│   Remove old index entries      │
│   Insert new index entries      │
│                                 │
│   Commit (atomic)               │
│   (fails if old_data changed)   │
└─────────────────────────────────┘
    ↓
Dispatch ChangeEvent::Update
    ↓
Return updated entity
```

### 4.3 Delete Operation with Cascade

```
db.delete("users", "123")
    ↓
Read entity to delete
    ↓
Find FK constraints referencing this entity
    ↓
Collect cascade operations (recursive DFS)
    ├─ CASCADE: posts where author_id=123
    │   └─ CASCADE: comments where post_id in posts
    ├─ RESTRICT: verify no references
    └─ SET_NULL: update categories.owner_id=null
    ↓
┌──────────────────────────────────┐
│   Start BatchWriter              │
│                                  │
│   Delete primary entity          │
│   Remove primary indexes         │
│                                  │
│   For each CASCADE:              │
│     Delete entity                │
│     Remove indexes               │
│                                  │
│   For each SET_NULL:             │
│     Update entity                │
│     Update indexes               │
│                                  │
│   Commit (atomic)                │
└──────────────────────────────────┘
    ↓
Dispatch delete events:
    - Primary entity
    - All cascaded entities
    ↓
Return success
```

### 4.4 Query with Index

```
db.list("users", filters=[{field: "status", op: Eq, value: "active"}], ...)
    ↓
Check if first filter is Eq (index-eligible)
    ↓
    ├─ YES: Index Lookup Path
    │   ↓
    │   Encode value for index key
    │   value = "active" → "active"
    │   ↓
    │   Prefix scan: idx/users/status/active/*
    │   ↓
    │   Extract IDs from index keys
    │   [123, 456, 789]
    │   ↓
    │   Load entities by ID
    │   Read data/users/123, data/users/456, ...
    │   ↓
    │   Apply remaining filters in-memory
    │
    └─ NO: Full Scan Path
        ↓
        Prefix scan: data/users/*
        ↓
        Deserialize all entities
        ↓
        Apply all filters in-memory
        ↓
    ↓
Sort results (if sort requested)
    ↓
Apply pagination (offset, limit)
    ↓
Load relationships (if includes requested)
    ↓
Return Vec<Value>
```

---

## 5. Transaction Model

### ACID Guarantees

**Atomicity:**
- All operations within `BatchWriter` commit atomically
- Constraint validation + data updates + index updates = single transaction
- Failure at any point rolls back entire batch
- Example: Create with 3 indexes either inserts all 4 keys or none

**Consistency:**
- Schema validation before any writes
- Constraint validation within transaction boundary
- Optimistic locking prevents concurrent modification conflicts
- Indexes always consistent with data (updated in same transaction)

**Isolation:**
- Read transactions see snapshot at transaction start (Fjall MVCC)
- Write transactions serialized (Fjall single-writer)
- Optimistic locking detects concurrent modifications
- Level: Serializable (strongest isolation)

**Durability:**
- Configurable via `DatabaseConfig`:
  - `Immediate`: fsync before commit returns (safest)
  - `Periodic(ms)`: background flush at intervals (balanced)
  - `None`: no fsync (fastest, testing only)
- Immediate mode guarantees persistence before acknowledgment
- Periodic mode has recovery window equal to flush interval

### Optimistic Locking

**Purpose:** Detect concurrent modifications during updates

**Mechanism:**
```rust
// Thread 1: Read-Modify-Write
let old_data = storage.get(key)?;           // Read
let new_data = apply_updates(old_data);     // Modify

let mut batch = storage.batch();
batch.expect_value(key, old_data);          // Expect unchanged
batch.insert(key, new_data);                // Write
batch.commit()?;                            // Commit or conflict

// Thread 2 modifies same key between read and commit
// → Thread 1's commit fails with Error::Conflict
```

**Error Handling:**
- `Error::Conflict` if value changed since read
- Application can retry with fresh read
- No deadlocks (no locks held)
- No lost updates (conflicts detected)

**When Used:**
- All `update()` operations
- Internally for atomic counter increments
- Ensures "last write wins" doesn't silently discard changes

**Trade-off:**
- Better concurrency than pessimistic locks
- No deadlock risk
- Simpler implementation
- But: requires retry logic for conflicts

---

## 6. Design Decisions & Trade-offs

### 6.1 Why JSON Entities?

**Pros:**
- Schema flexibility: Add fields without migrations
- Rich nested structures: Arrays, objects naturally supported
- Human-readable: Easy debugging and inspection
- Web API integration: Direct serialization to/from HTTP
- Developer productivity: Familiar format

**Cons:**
- Storage overhead: JSON is verbose compared to binary formats
- Parse cost: Deserialization on every read
- No compile-time safety: Typos caught at runtime
- Larger on disk: ~2-3x vs. binary formats

**Mitigation:**
- Optional schema validation for type safety
- Checksum verification for corruption detection
- Efficient serde_json for parsing performance

**Alternatives Considered:**
- Protocol Buffers: Requires schema definition, less flexible
- MessagePack: Binary, but still requires parsing
- Custom binary: Complex, hard to debug

**Decision:** JSON's flexibility and debuggability outweigh performance costs for embedded use cases.

---

### 6.2 Why Reactive Subscriptions?

**Use Cases:**
- Real-time UIs (live updates without polling)
- Event-driven architectures (decouple producers/consumers)
- MQTT integration (bridge database changes to IoT devices)
- Cache invalidation (automatic freshness)

**Benefits:**
- **Single source of truth**: Database becomes event log
- **Automatic propagation**: No manual cache invalidation
- **Decoupled components**: Observers don't know about each other
- **MQTT compatibility**: Patterns directly map to MQTT topics

**Trade-offs:**
- **Write latency**: Event dispatch adds ~0.1ms overhead
- **Memory**: Broadcast channel buffers events
- **Complexity**: Need to handle lagging subscribers
- **Event loss**: Bounded channels drop events if consumers slow

**Alternatives Considered:**
- Polling: Simple but wasteful, high latency
- WAL tailing: Complex, requires parsing write-ahead log
- Triggers: SQL-style, but requires query language

**Decision:** MQTT-style subscriptions align with target use case (reactive applications, future MQTT broker integration).

---

### 6.3 Why Optimistic Locking?

**Alternative:** Pessimistic locks (acquire lock before read)

**Pros of Optimistic:**
- Higher concurrency for read-heavy workloads
- No deadlock risk (no locks held)
- Simpler implementation (no lock manager)
- Fits LSM storage model (no in-place updates)
- Lower latency for non-conflicting operations

**Cons of Optimistic:**
- Write conflicts require retry logic
- Not suitable for high-contention scenarios
- Application complexity (handle conflicts)

**When Optimistic Works:**
- Read-heavy workloads (most database usage)
- Low contention (different entities modified concurrently)
- Short transactions (less chance of conflict)

**When Pessimistic Better:**
- High contention (same entity modified frequently)
- Long transactions
- Complex conflict resolution

**Decision:** Optimistic locking matches embedded database usage patterns (mostly independent entity updates).

---

### 6.4 Index Design Trade-offs

**Current Design:**
- Single-field indexes only
- Only first filter uses index
- Others applied in-memory

**Rationale:**
- **Simplicity**: Easy to implement and maintain
- **Coverage**: Handles 80% of use cases (single-field lookups)
- **Storage**: Composite indexes cause key explosion
- **Performance**: Good enough for embedded scale

**Limitations:**
```rust
// Only "status" filter uses index, "age" scanned in-memory
filters = [
    Filter::new("status", Eq, "active"),
    Filter::new("age", Gt, 18),
]
```

**Future Improvements:**
- Composite indexes: Index on `(status, age)` pair
- Query planner: Choose best index among multiple
- Covering indexes: Include non-key fields to avoid data lookup

**Trade-off:** Current simple design is maintainable and sufficient for target workloads.

---

## 7. Supporting Systems

### 7.1 TTL Expiration

**Purpose:** Automatic cleanup of time-limited data (sessions, caches)

**Mechanism:**
1. Background task runs at configurable interval (default: disabled)
2. Scans all entities with `_expires_at` field
3. Deletes expired entities (current time > expiration)
4. Dispatches delete events for each expired entity

**Usage:**
```rust
let session = json!({
    "user_id": "123",
    "token": "abc...",
    "ttl_secs": 3600  // Expires in 1 hour
});

db.create("sessions", session).await?;
```

**Transformation:**
- Input: `ttl_secs` field (relative seconds)
- Stored: `_expires_at` field (absolute Unix timestamp)
- `ttl_secs` removed from stored entity

**Index Optimization (Future):**
- Current: Full scan of all entities
- Future: Index on `_expires_at` for efficient lookup

---

### 7.2 Idempotency (`dedup.rs`)

**Purpose:** Prevent duplicate operations from retries or network issues

**Mechanism:**
```rust
// Client provides correlation ID
let result = db.create_with_correlation(
    "users",
    data,
    "unique-request-id-123"
).await?;

// Retry with same correlation ID
// → Returns cached response, doesn't create duplicate
```

**Storage:** `dedup/{correlation_id}` → `{response, timestamp}`

**Lifecycle:**
1. Check cache for correlation ID
2. If found: Return cached response (idempotent)
3. If not found: Execute operation, cache response
4. TTL-based expiration (configurable)

**Use Cases:**
- Retry safety in distributed systems
- At-least-once delivery guarantees
- Network failure recovery

---

### 7.3 Relationship Loading

**Purpose:** Load related entities (SQL join-like functionality)

**Setup:**
```rust
db.add_relationship("posts", "author", "users").await;
```

**Usage:**
```rust
// Single entity with relationship
let post = db.read("posts", "1", vec!["author"]).await?;
// post.author contains full user object

// List with relationships
let posts = db.list("posts", filters, sort, page, vec!["author"]).await?;
// Each post has author nested
```

**Implementation:**
1. Look up `{field}_id` in entity (e.g., `author_id`)
2. Read referenced entity from target table
3. Nest full entity under `field` name
4. Max depth: 3 levels (prevents infinite loops on circular refs)

**Limitations:**
- Not performant for large lists (N+1 query problem)
- No batch loading (future optimization)
- Circular reference protection (max depth)

---

### 7.4 Backup & Restore

**Physical Backup (via Database API):**
```rust
db.backup("path/to/backup")?;
```
- Flushes memtables to disk
- Copies entire database directory
- Fast, preserves SST files

**Backup via MQTT Agent:**

The MqdbAgent provides server-side managed backups:
```
Topic: $DB/_admin/backup
Payload: {"name": "daily_backup"}
```
- Backups stored in `{db_path}/backups/{name}/`
- Names must be alphanumeric, underscore, or hyphen only
- Validates names to prevent path traversal

**Listing Backups:**
```
Topic: $DB/_admin/backup/list
Response: ["backup1", "backup2", ...]
```

**Restore:**
Restore requires agent restart due to open file handles. The CLI provides guidance:
```bash
mqdb restore --name daily_backup
# Error: restore requires agent restart
```

For restore, stop the agent and copy backup directory over the database directory.

---

### 7.5 Cursor API

**Purpose:** Memory-efficient iteration over large result sets

**Usage:**
```rust
let mut cursor = db.cursor("users", filters).await?;

while let Some(batch) = cursor.next_batch(100).await? {
    for entity in batch {
        process(entity);
    }
}
```

**Benefits:**
- Bounded memory usage (configurable buffer size)
- Lazy loading (fetch on demand)
- Allows processing datasets larger than RAM

**Implementation:**
- Fjall iterator over key range
- Deserialize entities on-demand
- Buffer management for batch fetching

---

## 8. Extension Points

### 8.1 MQTT Agent (MqdbAgent)

**Current Implementation:**

The `MqdbAgent` component provides an embedded MQTT broker that exposes database operations:

```rust
let db = Database::open("./data/mydb").await?;
let agent = MqdbAgent::new(db)
    .with_bind_address("0.0.0.0:1884".parse()?)
    .with_password_file("passwd.txt".into())
    .with_acl_file("acl.txt".into())
    .with_backup_dir("./backups".into());

agent.run().await?;
```

**Architecture:**
```
MQTT Client
    ↓
MqttBroker (embedded)
    ↓
Internal Handler (subscribed to $DB/#)
    ↓
parse_db_topic() / parse_admin_topic()
    ↓
Database API / Admin Handlers
    ↓
Response via response_topic (MQTT 5.0)
```

**CRUD Topics:**
- `$DB/{entity}/create` - Create entity
- `$DB/{entity}/{id}` - Read entity
- `$DB/{entity}/{id}/update` - Update entity
- `$DB/{entity}/{id}/delete` - Delete entity
- `$DB/{entity}/list` - List with filters
- `$DB/{entity}/events/#` - Change notifications

**Admin Topics:**
- `$DB/_admin/schema/{entity}/set` - Set schema
- `$DB/_admin/schema/{entity}/get` - Get schema
- `$DB/_admin/constraint/{entity}/add` - Add constraint
- `$DB/_admin/constraint/{entity}/list` - List constraints
- `$DB/_admin/backup` - Create backup
- `$DB/_admin/backup/list` - List backups
- `$DB/_admin/restore` - Restore (requires restart)

**Request/Response Pattern:**

Clients use MQTT 5.0 `response_topic` property:
```
Publish to: $DB/users/create
Properties: response_topic = "client/responses/uuid"
Payload: {"name": "Alice"}

Response on: client/responses/uuid
Payload: {"ok": true, "data": {"id": "1", "name": "Alice"}}
```

**Authentication & Authorization:**

- Password file format: `username:bcrypt_hash`
- ACL file format: `user <name> topic <pattern> permission <read|write|readwrite|deny>`
- Admin operations restricted via `$DB/_admin/#` pattern

---

### 8.2 Custom Storage Backends

**Current Abstraction:**
- `Storage` struct wraps Fjall
- Could be trait for pluggable backends

**Potential Backends:**
- RocksDB: More mature LSM implementation
- LMDB: B-tree based, different performance profile
- FoundationDB: Distributed, ACID across nodes
- SQLite: SQL compatibility, widespread adoption

**Requirements:**
- Transactional batch writes
- Prefix iteration (for scans)
- MVCC or equivalent isolation

---

### 8.3 Query Language

**Current:** Programmatic filters
```rust
let filters = vec![
    Filter::new("status", FilterOp::Eq, json!("active")),
    Filter::new("age", FilterOp::Gt, json!(18)),
];
```

**Future:** SQL-like or reactive queries
```rust
db.query("SELECT * FROM users WHERE status = 'active' AND age > 18").await?;

// Or reactive queries
db.subscribe_query("users WHERE status = 'active'").await?;
// → Notified when results change
```

---

## 9. Performance Characteristics

### Benchmark Results (M-series Mac, Release Mode)

| Operation | Throughput | p50 Latency | p95 Latency | p99 Latency |
|-----------|------------|-------------|-------------|-------------|
| Writes    | 168k/s     | 0.01ms      | 0.01ms      | 0.01ms      |
| Reads     | 558k/s     | 0.00ms      | 0.00ms      | 0.01ms      |
| Updates   | 191k/s     | 0.00ms      | 0.01ms      | 0.01ms      |
| List/Scan | 91/s       | 10.96ms     | -           | -           |

### Bottlenecks

**Write Performance:**
- Durability mode (fsync): Immediate mode ~10x slower than None
- Index maintenance: Each index adds ~5% overhead
- Event dispatch: ~0.1ms added latency
- Constraint validation: Unique checks require index scan

**Read Performance:**
- Deserialization: JSON parsing dominates
- Checksum verification: CRC32 computation
- Storage lookup: Usually cached in memtable/block cache

**Scan Performance:**
- Full entity deserialization for all results
- In-memory filtering for non-indexed fields
- Sorting requires loading all results into memory

### Optimization Opportunities

1. **Lazy Deserialization:**
   - Only parse JSON when accessed
   - Store raw bytes, deserialize on-demand

2. **Projection Pushdown:**
   - Return subset of fields
   - Reduces parse time and network transfer

3. **Batch Foreign Key Validation:**
   - Current: Individual lookups for each FK
   - Optimized: Single scan for all FKs

4. **Composite Indexes:**
   - Support multi-field indexes
   - Eliminate in-memory filtering for complex queries

5. **TTL Index:**
   - Index on `_expires_at` field
   - Avoid full scan for expiration cleanup

---

## 10. Error Handling Strategy

### Error Types

```rust
pub enum Error {
    Storage(String),              // Fjall errors (I/O, corruption)
    NotFound { entity, id },      // Entity doesn't exist
    Conflict,                     // Optimistic lock failure
    Corruption(String),           // Checksum mismatch
    SchemaViolation { entity, field, reason },
    UniqueViolation { entity, field, value },
    ForeignKeyViolation { entity, field, target_entity, target_id },
    ForeignKeyRestrict { entity, id, referencing_entity },
    NotNullViolation { entity, field },
    Validation(String),           // General validation errors
    InvalidKey(String),           // Key decoding errors
    InvalidForeignKey,            // FK value not a string
}
```

### Error Philosophy

1. **Explicit over Generic:**
   - Specific error types (not just `String`)
   - Contextual information (entity, field, value)
   - Enables proper error handling by callers

2. **Recoverable vs. Fatal:**
   - Recoverable: `NotFound`, `Conflict`, `UniqueViolation` (retry possible)
   - Fatal: `Storage`, `Corruption` (database issue)

3. **Error Contexts:**
   - Include entity name, field name, values
   - Aids debugging and user feedback
   - Example: "unique constraint violation: users.email value 'alice@example.com' already exists"

4. **Propagation:**
   - Use `?` operator for propagation
   - Convert Fjall errors to `Error::Storage`
   - Convert serde errors to `Error::Storage` or `Error::Validation`

---

## 11. Configuration Options

### DatabaseConfig

```rust
pub struct DatabaseConfig {
    pub path: PathBuf,                          // Database directory
    pub durability: DurabilityMode,             // Flush strategy
    pub event_channel_capacity: usize,          // Broadcast buffer (default: 1000)
    pub max_list_results: usize,                // Prevent huge result sets (default: 10000)
    pub max_subscriptions: usize,               // Limit active subs (default: 1000)
    pub ttl_cleanup_interval_secs: Option<u64>, // Background task (default: None)
    pub max_cursor_buffer: usize,               // Cursor memory limit (default: 1000)
    pub max_sort_buffer: usize,                 // Max entities for in-memory sort (default: 10000)
}
```

### Durability Modes

```rust
pub enum DurabilityMode {
    Immediate,        // fsync before commit returns (safest, slowest)
    Periodic(u64),    // fsync every N ms (balanced)
    None,             // no fsync (fastest, data loss risk)
}
```

### Tuning Guidance

**High Throughput (testing):**
```rust
DatabaseConfig::new("db")
    .with_durability(DurabilityMode::None)
    .with_event_channel_capacity(10000)
```

**Low Latency (production):**
```rust
DatabaseConfig::new("db")
    .with_durability(DurabilityMode::Immediate)
```

**Balanced (typical production):**
```rust
DatabaseConfig::new("db")
    .with_durability(DurabilityMode::Periodic(100))  // 100ms flush
    .with_ttl_cleanup_interval(Some(60))             // Clean every 60s
```

**Memory-Constrained:**
```rust
DatabaseConfig::new("db")
    .with_max_cursor_buffer(100)
    .with_max_sort_buffer(1000)
    .with_event_channel_capacity(100)
```

---

## 12. Testing Strategy

### Test Coverage: 93 Tests

**Breakdown:**
- 26 unit tests (component isolation)
- 67 integration tests (full workflows)
  - 25 constraint tests
  - 17 CRUD + features
  - 13 crash recovery
  - 6 transaction conflicts
  - 4 concurrency
  - 2 durability

### Test Categories

**1. Unit Tests:**
- `entity.rs`: Serialization, JSON conversion
- `keys.rs`: Key encoding/decoding
- `schema.rs`: Validation, defaults
- `constraint.rs`: Constraint creation
- `checksum.rs`: Corruption detection
- `index.rs`: Index definition

**2. Integration Tests:**
- `integration_test.rs`: CRUD, subscriptions, indexes, relationships, TTL
- `constraint_test.rs`: All constraint types and combinations
- `crash_recovery_test.rs`: Restart scenarios, data integrity
- `transaction_test.rs`: ACID guarantees, conflicts
- `concurrency_test.rs`: Parallel writes, race conditions
- `durability_test.rs`: Fsync behavior, crash simulation

**3. Constraint Tests (25 tests):**
- NOT NULL: 4 tests (create/update violations, success)
- Unique: 6 tests (single, composite, concurrent, update)
- Foreign Key: 8 tests (valid, invalid, null, CASCADE, RESTRICT, SET_NULL, multilevel)
- Schema: 3 tests (type validation, required, defaults)
- Persistence: 4 tests (survive restart)
- Combined: 1 test (all constraints together)

### Test Patterns

**Setup/Teardown:**
```rust
let tmp = TempDir::new()?;
let db = Database::open(tmp.path()).await?;
// Test code
// TempDir cleanup automatic
```

**Concurrency Testing:**
```rust
let db1 = Arc::new(db);
let db2 = db1.clone();

let handle1 = tokio::spawn(async move { db1.create(...).await });
let handle2 = tokio::spawn(async move { db2.create(...).await });

let (result1, result2) = tokio::join!(handle1, handle2);
```

**Crash Simulation:**
```rust
// Write data
db.create(...).await?;
drop(db);  // Close database

// Reopen
let db = Database::open(path).await?;
// Verify data survived
```

---

## 13. Operational Considerations

### Monitoring

**Key Metrics to Track:**
- Write throughput (ops/sec)
- Read latency percentiles (p50, p95, p99)
- Index size growth over time
- Active subscription count
- Event channel saturation (dropped events)
- Disk I/O for compaction
- Background task execution time (TTL cleanup)

**Instrumentation Points:**
```rust
// Add tracing spans
#[instrument]
pub async fn create(&self, entity_name: String, data: Value) -> Result<Value> {
    // Automatic timing and logging
}
```

### Debugging

**Tools:**
- Key encoding is human-readable (UTF-8 paths)
- Entities stored as JSON (inspect with hex editor + JSON formatter)
- Checksums detect corruption early (fail-fast)
- Tracing integration for structured logging

**Debug Workflow:**
1. Enable tracing: `RUST_LOG=mqdb=debug`
2. Inspect storage: `ls -R data/`
3. Examine keys: Raw bytes are readable UTF-8
4. Verify checksums: CRC32 mismatch = corruption
5. Check subscriptions: `sub/` namespace lists active patterns

### Migration & Schema Evolution

**Adding Fields:**
- No migration needed (schema-less)
- Old entities missing new field get default (if schema exists)
- Application handles backward compatibility

**Renaming Fields:**
```rust
// Application-level mapping
let user = db.read("users", id).await?;
let email = user.get("email")
    .or_else(|| user.get("email_address"));  // Old field name
```

**Type Changes:**
```rust
// Validate + migrate
for user in db.list("users", ...).await? {
    if user["age"].is_string() {
        let age = user["age"].as_str().unwrap().parse::<i64>()?;
        db.update("users", id, json!({"age": age})).await?;
    }
}
```

**Adding Constraints:**
```rust
// Validate existing data first
let violations = find_unique_violations(&db, "users", "email").await?;
if !violations.is_empty() {
    return Err("Cannot add unique constraint: duplicates exist");
}

// Then add constraint
db.add_unique_constraint("users", vec!["email"]).await?;
```

### Backup Strategy

**Production Recommendation:**
1. Physical backups for fast recovery (daily)
2. Logical backups for long-term archival (weekly)
3. Test restore procedures regularly
4. Store backups off-site (disaster recovery)

**Automated Backup:**
```rust
// Cron job or scheduled task
let timestamp = Utc::now().format("%Y%m%d_%H%M%S");
db.backup_physical(format!("backups/physical_{}", timestamp)).await?;
db.backup_logical(format!("backups/logical_{}.jsonl", timestamp)).await?;
```

---

## 14. Conclusion

### MQDB is Optimized For:

- **Real-time applications** needing change observation (reactive UIs, dashboards)
- **Embedded systems** with local persistence needs
- **Event-driven architectures** where database changes trigger workflows
- **Future MQTT integration** for IoT and pub/sub systems

### Key Strengths:

1. **ACID Transactions:** Full transactional guarantees with comprehensive constraint system
2. **Reactive Subscriptions:** MQTT-style patterns for real-time change notifications
3. **Schema Flexibility:** JSON entities with optional type validation
4. **Simple Deployment:** Single binary, embedded (no separate server process)
5. **Rust Safety:** Memory-safe, no data races, strong type system

### Design Philosophy:

> **Correct, then fast. Simple, then featureful.**

MQDB prioritizes:
- Correctness (ACID, constraints, checksums)
- Simplicity (clear components, explicit errors)
- Then performance (good enough for embedded scale)
- Then features (incremental additions)

### When to Use MQDB:

✅ **Good fit:**
- Local-first applications
- Real-time dashboards
- Event sourcing patterns
- Embedded devices
- Rapid prototyping (schema-less)

❌ **Not ideal for:**
- Massive scale (billions of entities)
- Complex analytical queries (no query planner)
- Distributed systems (no replication)
- SQL compatibility required

### Contributing:

The architecture is designed for extensibility. Key extension points:
- Custom storage backends (trait abstraction)
- MQTT integration layer (dispatcher hooks)
- Query language (filter system)
- Additional constraint types (framework in place)

See [CONTRIBUTING.md](CONTRIBUTING.md) for development guidelines.
