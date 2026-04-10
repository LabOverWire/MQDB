# MQDB Architecture

## 1. Overview & Design Philosophy

### What is MQDB?

MQDB is a message-oriented reactive database built in Rust that combines:
- **Native MQTT integration** with embedded broker and topic-based API
- **Distributed clustering** with 256-partition sharding, Raft consensus, and automatic rebalancing
- **Point-to-point delivery** with consumer groups and partition-based routing
- **Reactive subscriptions** with MQTT-style pub/sub patterns
- **Schema-less JSON** entities with optional schema validation
- **Per-entity atomicity** with optimistic locking and outbox pattern
- **Pluggable storage** with Fjall (native) and Memory (WASM) backends

### Core Principles

1. **Message-oriented**: Database operations exposed via MQTT topics, enabling seamless IoT integration
2. **Reactive-first**: All mutations trigger subscription events with configurable delivery modes
3. **Embedded efficiency**: Low-latency local access, runs in browsers via WASM
4. **Data integrity**: Per-entity atomicity with comprehensive constraint system
5. **Reliability**: Outbox pattern ensures atomic event persistence with retry and dead letter queue

### Design Philosophy

> **Correct, then fast. Simple, then featureful.**

MQDB prioritizes correctness and simplicity over raw performance and feature count. The architecture reflects this through explicit error handling, clear component boundaries, and incremental feature additions.

---

## 2. High-Level Architecture

### Architecture Layers (Bottom to Top)

**Single-Node (Agent Mode):**
```
┌─────────────────────────────────────────────────┐
│           MQTT Clients / Web Browsers           │
├─────────────────────────────────────────────────┤
│         MQTT Agent (MqdbAgent)                  │
│  Embedded Broker │ Auth │ ACL │ Topic Handlers  │
├─────────────────────────────────────────────────┤
│     Database API (mqdb-agent/database/)         │
│  CRUD │ Queries │ Subscriptions │ Constraints   │
├─────────────────────────────────────────────────┤
│          Reactive Event System                  │
│  EventDispatcher │ ConsumerGroups │ Outbox      │
├─────────────────────────────────────────────────┤
│         Domain Logic Layer                      │
│  Schema │ Constraints │ Indexes │ Relations     │
├─────────────────────────────────────────────────┤
│         Entity & Key Management                 │
│  Entity (JSON ↔ Bytes) │ Keys (Encoding)        │
├─────────────────────────────────────────────────┤
│         Storage Abstraction (StorageBackend)    │
│  BatchWriter │ Transactions │ Durability        │
├─────────────────────────────────────────────────┤
│    Fjall (Native) │ Memory (WASM/Testing)       │
│  LSM Persistence  │  In-Memory HashMap          │
└─────────────────────────────────────────────────┘
```

**Multi-Node (Cluster Mode):**
```
┌─────────────────────────────────────────────────┐
│           MQTT Clients / Web Browsers           │
├─────────────────────────────────────────────────┤
│       ClusteredAgent (cluster_agent/mod.rs)     │
│  Embedded Broker │ Auth │ ACL │ Event Handler   │
├─────────────────────────────────────────────────┤
│    NodeController (mqdb-cluster/node_controller)│
│  Partition Map │ Write Routing │ Replication    │
├─────────────────────────────────────────────────┤
│      Raft Consensus    │    Store Manager       │
│  Leader Election │     │  17 Typed Stores       │
│  Log Replication │     │  Sessions, Topics, DB  │
├─────────────────────────────────────────────────┤
│     Cluster Transport (mqdb-cluster/transport)  │
│  MQTT Bridges  or  Direct QUIC Transport        │
├─────────────────────────────────────────────────┤
│         Storage Abstraction (StorageBackend)    │
│  BatchWriter │ Transactions │ Durability        │
├─────────────────────────────────────────────────┤
│               Fjall (Native)                    │
│  Raft Log │ Stores │ LSM Persistence            │
└─────────────────────────────────────────────────┘
```

### Component Interaction

- **MQTT Agent** exposes database via MQTT topics with authentication and ACL
- **Database API** coordinates all operations, managing transactions and event dispatch
- **Reactive System** routes events based on subscription mode (Broadcast/LoadBalanced/Ordered)
- **Consumer Groups** manage partition assignment and rebalancing for point-to-point delivery
- **Outbox** ensures atomic event persistence with retry and dead letter queue
- **Domain Logic** enforces business rules (constraints, schemas, indexes)
- **Entity Layer** handles JSON serialization and key encoding
- **Storage Backend** provides pluggable persistence (Fjall for native, Memory for WASM)

---

## 3. Core Components

### 3.1 Storage Layer (`crates/mqdb-core/src/storage/`)

**Responsibility:** Persistence and transaction management with pluggable backends

**Architecture (Synchronous API):**
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

pub trait BatchOperations: Send {
    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>);
    fn remove(&mut self, key: Vec<u8>);
    fn expect_value(&mut self, key: Vec<u8>, expected_value: Vec<u8>);
    fn commit(self: Box<Self>) -> Result<()>;
}
```

**Asynchronous API:**
For asynchronous contexts, `AsyncStorageBackend` and `AsyncBatchOperations` traits are provided, mirroring the synchronous API but returning `impl Future`.

**Available Backends:**

| Backend | Use Case | Persistence | Platform | Internal Implementation |
|---------|----------|-------------|----------|-------------------------|
| `FjallBackend` | Production | LSM-based disk | Native | `fjall::Database` with `Keyspace` |
| `MemoryBackend` | Testing/WASM | In-memory HashMap | All | `std::collections::BTreeMap` |

**Key Features:**
- `BatchOperations` for atomic multi-key operations
- Optimistic locking via preconditions
- Configurable durability modes (`Immediate`, `Periodic`, `None`)

**Transaction Model:**
```rust
let mut batch = storage.batch();
batch.expect_value(key, old_value);  // Precondition check
batch.insert(key, new_value);
batch.commit()?;  // Fails if precondition not met
```

**Why Fjall (Native)?**
- Rust-native LSM implementation (no C bindings)
- Built-in transactional support
- Memory-safe with zero-cost abstractions
- Efficient for write-heavy workloads

**Why Memory Backend (WASM)?**
- No file system dependencies
- Runs in browsers via WebAssembly
- Fast for testing (no I/O overhead)
- Same API as persistent backend

---

### 3.2 Key Encoding Scheme (`crates/mqdb-core/src/keys.rs`)

**Namespace Design:**
```
data/{entity}/{id}                          → Entity data
idx/{entity}/{field}/{value}/{id}           → Secondary indexes
sub/{subscription_id}                       → Persistent subscriptions
dedup/{correlation_id}                      → Idempotency cache
meta/{key}                                  → Sequences, metadata
meta/schema/{entity}                        → Schema definitions
meta/constraint/{type}/{entity}/{name}      → Constraint definitions
_outbox/{operation_id}                      → Pending events for delivery
_dead_letter/{operation_id}                 → Failed events after max retries
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
meta/constraint/fk/posts/posts_author_id_fk → Foreign key constraint
```

---

### 3.3 Entity Layer (`crates/mqdb-core/src/entity.rs`)

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

### 3.4 Index System (`crates/mqdb-core/src/index.rs`)

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

### 3.5 Constraint System (`crates/mqdb-core/src/constraint.rs`, `crates/mqdb-core/src/schema.rs`)

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

**Validation Timing (Agent Mode):**

In agent mode, all constraint validation happens within a single batch commit:
```
Start BatchWriter
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

In cluster mode, constraint enforcement is best-effort with cross-partition coordination. Unique constraints use a two-phase reservation protocol (reserve with TTL, then commit or release). Foreign key constraints use a one-phase existence check (create/update) and scatter-gather reverse lookup (delete). Both have a lock-drop/reacquire gap between check and write — see Section 5.

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
Execute all operations in single batch (atomic in agent mode)
    ↓
Dispatch delete events for all affected entities
```

**Recursive Cascade Handling:**
- Uses depth-first search with visited tracking
- Prevents infinite loops on circular references
- Example: User → Posts → Comments (2 levels deep)
- Agent mode: all deletions happen in a single atomic batch commit
- Cluster mode: cascade operations are per-entity with cross-partition scatter-gather; not atomic across entities

---

### 3.6 Reactive System (`crates/mqdb-core/src/subscription.rs`, `crates/mqdb-agent/src/dispatcher.rs`, `crates/mqdb-core/src/events.rs`, `crates/mqdb-agent/src/consumer_group.rs`)

**Purpose:** Real-time change notifications with configurable delivery modes

#### Subscription Modes

| Mode | Behavior | Use Case |
|------|----------|----------|
| **Broadcast** | All subscribers receive all events | Notifications, caches |
| **LoadBalanced** | Round-robin across group members | Worker pools |
| **Ordered** | Partition-based (same entity:id → same consumer) | Event sourcing |

#### Event Flow

```
Mutation (create/update/delete)
    ↓
BatchWriter.commit() succeeds
    ↓
ChangeEvent created (includes entity:id hash for partitioning)
    ↓
EventDispatcher.dispatch()
    ↓
┌─────────────────────────────────────────────┐
│  Route by subscription mode:                │
│                                             │
│  Broadcast → All matching subscribers       │
│  LoadBalanced → Round-robin in group        │
│  Ordered → Partition owner in group         │
└─────────────────────────────────────────────┘
    ↓
Deliver to selected subscribers
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
- No guarantee of delivery (best-effort for Broadcast)
- Subscribers responsible for handling missed events

#### Consumer Groups (`crates/mqdb-agent/src/consumer_group.rs`)

**Purpose:** Partition-based message routing for point-to-point delivery

**Partition Assignment:**
```
8 partitions, 3 consumers:
  Consumer A: [0, 1, 2]
  Consumer B: [3, 4, 5]
  Consumer C: [6, 7]

Event routing (Ordered mode):
  hash("orders:123") % 8 = 3 → Consumer B
  hash("orders:456") % 8 = 7 → Consumer C
```

**Rebalancing:**
- Triggered on member join/leave
- Deterministic assignment (sorted member IDs)
- All members in group get same view

**Heartbeat Mechanism:**
```rust
db.heartbeat(&subscription_id).await?;
```
- Consumers send periodic heartbeats
- Stale consumers removed after timeout (default: 30s)
- Removal triggers partition rebalance

**Mode Validation:**
- Groups cannot mix LoadBalanced and Ordered modes
- First subscriber sets the mode
- Subsequent subscribers must match or get rejected

#### Outbox Pattern (`crates/mqdb-core/src/outbox.rs`)

**Purpose:** Atomic event persistence with guaranteed delivery

**Flow:**
```
1. Write entity + event to outbox (atomic)
2. OutboxProcessor polls pending events
3. Dispatch to subscribers
4. Mark delivered (remove from outbox)
5. On failure: increment retry count
6. After max retries: move to dead letter queue
```

**Configuration:**
```rust
OutboxConfig {
    max_retries: 3,
    retry_interval_ms: 1000,
    dead_letter_enabled: true,
}
```

**Benefits:**
- Events never lost for local writes (persisted before commit returns)
- Automatic retry with backoff
- Dead letter queue for investigation
- Survives crashes and restarts

#### Cluster-Mode Outbox (`crates/mqdb-cluster/src/cluster/store_manager/outbox.rs`)

In cluster mode, the `ClusterOutbox` provides the same durability guarantee for change events on local partitions. The outbox entry is written atomically with the data in a single fjall batch via `PartitionStorage::write_with_outbox()`. On startup, `recover_pending_outbox()` scans for undelivered entries and replays them.

Writes forwarded to remote primaries publish change events fire-and-forget on the originating node — the outbox covers local partition writes only (~1/N of writes in an N-node cluster).

---

## 4. Data Flow Diagrams

### 4.1 Create Operation

```
db.create("users", json!({...}))
    ↓
Extract client-provided ID from payload, or generate one (atomic sequence counter)
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

## 5. Consistency Model

| Property | Agent Mode | Cluster Mode |
|----------|-----------|-------------|
| Write atomicity | Per-entity batch commit (fjall) | Per-entity write lock serialization |
| Replication | N/A (standalone) | Eventual consistency (async, epoch-based failover) |
| Local durability | `Immediate` fsync (default) | `PeriodicMs(10)` (default, configurable) |
| Constraint enforcement | Atomic with data (same batch) | Best-effort (lock-drop gap, TTL self-heal) |
| Change events | Outbox (atomic with data) | Outbox for local partitions, fire-and-forget for forwarded |
| Isolation | Optimistic concurrency (`_version` + batch preconditions) | Per-entity write lock |

### Per-Entity Atomicity (Agent Mode)

In agent mode, each create, update, or delete commits data, indexes, constraint state, and the outbox entry in a single `BatchWriter.commit()` via fjall. This provides per-entity atomicity: a create with 3 indexes either writes all 4 keys or none. There is no multi-entity transaction — each operation is independent.

### Cluster Mode

In cluster mode, each data write is persisted to fjall via `PartitionStorage` before updating in-memory stores. Writes are serialized per-entity via the `NodeController` write lock. Async replication propagates writes to replica nodes with sequence numbers, gap detection, and epoch-based failover.

Change events are persisted atomically with data via a transactional outbox (`ClusterOutbox`). On the happy path, the event is published immediately and the outbox entry removed. On crash, startup recovery scans pending outbox entries and replays them. The outbox covers writes to local partitions (~1/3 of writes in a 3-node cluster). Writes forwarded to remote primaries publish change events fire-and-forget on the originating node.

### Constraint Enforcement

- Schema validation runs before writes
- Unique constraints use a two-phase reservation protocol (reserve with TTL, commit or release)
- Foreign key constraints use a one-phase existence check (create/update) and scatter-gather reverse lookup (delete)
- Optimistic locking via `_version` field detects concurrent modifications

### Durability

Configurable via `stores_durability`:
- `Immediate`: fsync before commit returns (agent mode default)
- `Periodic(ms)`: background flush at intervals (cluster mode default: 10ms)
- `None`: no explicit flush (testing only)

The Raft log always uses `Immediate` durability. Agent mode defaults to `Immediate` for maximum safety. Cluster mode defaults to `PeriodicMs(10)` — the outbox pattern guarantees consistency between data and change events regardless of fsync timing, since both share the same batch. The 10ms window trades a small durability risk on hard crash (power loss) for significantly higher write throughput. Note that replication is asynchronous: a primary failure can lose writes that were acknowledged but not yet replicated to the replica, regardless of the local fsync mode.

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

### 7.2 Idempotency (`crates/mqdb-agent/src/dedup.rs`)

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

// Blocking: runs the agent until shutdown
agent.run().await?;

// Non-blocking alternative: returns a handle and a readiness signal
let (handle, mut ready_rx) = agent.start().await?;
ready_rx.changed().await?; // wait until broker + handler are ready
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
- `$DB/_admin/consumer-groups` - List consumer groups
- `$DB/_admin/consumer-groups/{name}` - Show consumer group details

**Subscription Management Topics:**
- `$DB/_sub/subscribe` - Create subscription (supports shared subscriptions)
- `$DB/_sub/{id}/heartbeat` - Send heartbeat for shared subscription
- `$DB/_sub/{id}/unsubscribe` - Remove subscription

**Subscribe Payload:**
```json
{
  "pattern": "orders/#",
  "entity": "orders",
  "group": "order-processors",
  "mode": "ordered"
}
```

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

### 8.2 Storage Backends

**Current Implementation:**

The `StorageBackend` trait enables pluggable persistence:

```rust
pub trait StorageBackend: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn insert(&self, key: &[u8], value: &[u8]) -> Result<()>;
    fn remove(&self, key: &[u8]) -> Result<()>;
    fn prefix_scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
    fn batch(&self) -> Box<dyn BatchWriter>;
    fn flush(&self) -> Result<()>;
}
```

**Available Backends:**

| Backend | File | Use Case |
|---------|------|----------|
| `FjallBackend` | `crates/mqdb-core/src/storage/fjall_backend.rs` | Native production (LSM persistence) |
| `MemoryBackend` | `crates/mqdb-core/src/storage/memory_backend.rs` | WASM/testing (in-memory) |

**Adding Custom Backends:**

Implement `StorageBackend` trait. Potential additions:
- RocksDB: More mature LSM implementation
- LMDB: B-tree based, different performance profile
- IndexedDB: Browser-native persistence for WASM
- SQLite: SQL compatibility layer

**Requirements:**
- Transactional batch writes with optimistic locking
- Prefix iteration (for scans)
- Thread-safe (Send + Sync)

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

## 9. Cluster Integration

The single-node architecture described in this document forms the foundation for MQDB's distributed cluster mode. This section describes how the components integrate.

### 9.1 Deployment Modes

MQDB supports two deployment modes:

| Mode | Entry Point | Use Case |
|------|-------------|----------|
| **Agent** | `MqdbAgent` (`crates/mqdb-agent/src/agent/mod.rs`) | Single-node standalone broker |
| **Cluster** | `ClusteredAgent` (`crates/mqdb-cluster/src/cluster_agent/mod.rs`) | Multi-node distributed system |

Both modes share the same database API, but cluster mode adds:
- Raft consensus for partition management
- MQTT bridges or direct QUIC for inter-node transport
- 256 fixed partitions with replication factor 2
- Automatic rebalancing and failover

### 9.2 Storage Layer in Cluster Mode

In cluster mode, the `StorageBackend` trait is used by multiple subsystems:

**Raft Storage** (`crates/mqdb-cluster/src/cluster/raft/storage.rs`):
- Persists Raft log entries and metadata
- Uses `DurabilityMode::Immediate` for consensus safety
- Path: `{db_path}/raft/`

**Cluster Stores** (`crates/mqdb-cluster/src/cluster/store_manager.rs`):
- Manages 17 distinct stores for MQTT and database state
- Uses configurable durability (default: `PeriodicMs(10)`)
- Path: `{db_path}/stores/`
- Includes: sessions, subscriptions, retained messages, QoS state, database data

### 9.3 Entity Types

Cluster mode introduces two classes of entities:

**Partitioned Entities** (hash-based distribution):
- Sessions: `hash(client_id) % 256`
- Database records: `hash(entity/id) % 256`
- Retained messages: `hash(topic) % 256`

**Broadcast Entities** (replicated to all nodes):
- `_topic_index`: Topic → subscriber mappings
- `_wildcards`: Wildcard subscription patterns
- `_client_loc`: Client → connected node mappings
- `_db_schema`: Database schema definitions
- `_db_constraint`: Constraint definitions

Broadcast entities enable local pub/sub routing without cross-node queries.

### 9.4 Transport Options

Cluster nodes communicate via two transport implementations:

| Transport | Class | Characteristics |
|-----------|-------|-----------------|
| **MQTT Bridges** | `MqttTransport` | Uses broker infrastructure, bridge overhead (DEPRECATED) |
| **Direct QUIC** | `QuicDirectTransport` | Bypasses broker, 1.2-2.5x faster |

Both implement the `ClusterTransport` trait and support:
- Point-to-point messaging to specific nodes
- Broadcast to all cluster members
- Partition-based routing to primary owners

### 9.5 Replication Model

MQDB uses a two-tier replication model:

**Control Plane (Raft Consensus)**:
- Strong consistency for partition map changes
- Leader election and membership management
- Log replication with quorum acknowledgment

**Data Plane (Async Replication)**:
- Primary-replica model (RF=2)
- Primary applies locally, forwards to replica
- Best-effort replication (no per-write quorum)
- Optimized for throughput over strict consistency

### 9.6 API Compatibility

Database operations work identically in both modes:

**Agent Mode**:
```
$DB/{entity}/create → MqdbAgent → Database API → Local storage
```

**Cluster Mode**:
```
$DB/{entity}/create → ClusteredAgent → NodeController → Partition primary → Replicas
```

The `$DB/` topic prefix is handled by both modes, ensuring client applications work transparently.

For detailed cluster architecture, see `DISTRIBUTED_DESIGN.md`.

---

## 10. Performance Characteristics (Single-Node)

### Benchmark Results (Single-Node Agent Mode, M-series Mac, Release Mode)

| Operation | Throughput | p50 Latency | p95 Latency | p99 Latency |
|-----------|------------|-------------|-------------|-------------|
| Writes    | 168k/s     | 0.01ms      | 0.01ms      | 0.01ms      |
| Reads     | 558k/s     | 0.00ms      | 0.00ms      | 0.01ms      |
| Updates   | 191k/s     | 0.00ms      | 0.01ms      | 0.01ms      |
| List/Scan | 91/s       | 10.96ms     | -           | -           |

Note: These benchmarks are for single-node agent mode. For cluster mode performance characteristics, see `DISTRIBUTED_DESIGN.md` section A6.

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

2. **Projection Pushdown:** (implemented)
   - Clients specify `--projection field1,field2` to return a subset of fields
   - Applied after filtering/sorting in both agent and cluster modes
   - In scatter-gather, projection is applied at the aggregation step to preserve filter fields

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

## 11. Error Handling Strategy

### Error Types

```rust
pub enum Error {
    Storage(fjall::Error),        // Fjall errors (native only)
    StorageGeneric(String),       // Generic storage errors
    NotFound { entity, id },      // Entity doesn't exist
    Serialization(serde_json::Error), // JSON parse/serialize errors
    InvalidKey(String),           // Key decoding errors
    Validation(String),           // General validation errors
    ConstraintViolation(String),  // Generic constraint errors
    Internal(String),             // Internal errors
    SystemTime(String),           // System time errors
    Corruption { entity, id },    // Checksum mismatch
    BackupFailed(String),         // Backup/restore failures
    Conflict(String),             // Optimistic lock failure
    SchemaViolation { entity, field, reason },
    UniqueViolation { entity, field, value },
    ForeignKeyViolation { entity, field, target_entity, target_id },
    ForeignKeyRestrict { entity, id, referencing_entity },
    NotNullViolation { entity, field },
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

## 12. Configuration Options

### DatabaseConfig

```rust
pub struct DatabaseConfig {
    pub path: PathBuf,                          // Database directory
    pub durability: DurabilityMode,             // Flush strategy (default: Immediate)
    pub event_channel_capacity: usize,          // Broadcast buffer (default: 1000)
    pub max_list_results: Option<usize>,        // Prevent huge result sets (default: Some(10000))
    pub max_subscriptions: Option<usize>,       // Limit active subs (default: Some(1000))
    pub ttl_cleanup_interval_secs: Option<u64>, // Background task (default: Some(60))
    pub max_cursor_buffer: usize,               // Cursor memory limit (default: 100)
    pub max_sort_buffer: usize,                 // Max entities for in-memory sort (default: 10000)
    pub outbox: OutboxConfig,                   // Outbox settings (see below)
    pub shared_subscription: SharedSubscriptionConfig, // Shared subscription settings
    pub spawn_background_tasks: bool,           // Enable background tasks (default: true)
}

pub struct OutboxConfig {
    pub enabled: bool,                          // Enable outbox pattern (default: true)
    pub retry_interval_ms: u64,                 // Retry interval (default: 5000)
    pub max_retries: u32,                       // Max retries before dead letter (default: 10)
    pub batch_size: usize,                      // Batch processing size (default: 100)
}

pub struct SharedSubscriptionConfig {
    pub num_partitions: u8,                     // Partitions for shared subs (default: 8)
    pub consumer_timeout_ms: u64,               // Consumer heartbeat timeout (default: 30000)
}
```

### Durability Modes

```rust
pub enum DurabilityMode {
    Immediate,        // fsync before commit returns (safest, slowest)
    PeriodicMs(u64),  // fsync every N ms (balanced)
    None,             // no fsync (fastest, data loss risk)
}
```

### Tuning Guidance

**High Throughput (testing):**
```rust
DatabaseConfig::new("db")
    .with_durability(DurabilityMode::None)       // Skip fsync
    .with_event_capacity(10000)                  // Larger event buffer
    .without_background_tasks()                  // No cleanup overhead
```

**Low Latency (production):**
```rust
DatabaseConfig::new("db")
    .with_durability(DurabilityMode::Immediate)  // fsync every commit
```

**Balanced (typical production):**
```rust
DatabaseConfig::new("db")
    .with_durability(DurabilityMode::PeriodicMs(100))  // 100ms flush
    .with_ttl_cleanup_interval(Some(60))               // Clean every 60s
```

**Memory-Constrained:**
```rust
DatabaseConfig::new("db")
    .with_max_cursor_buffer(100)
    .with_max_sort_buffer(1000)
    .with_event_channel_capacity(100)
```

---

## 13. Testing Strategy

### Test Coverage: 683+ Tests

**Major Test Suites:**
- 472 unit tests (library components including cluster modules)
- 68 protocol/encoding tests
- 49 subscription/topic tests
- Integration tests:
  - 25 constraint tests
  - 17 CRUD + features
  - 13 crash recovery
  - 7 point-to-point delivery
  - 6 transaction tests
  - 4 concurrency tests
  - 4 cluster integration tests

### Test Categories

**1. Unit Tests:**
- `crates/mqdb-core/src/entity.rs`: Serialization, JSON conversion
- `crates/mqdb-core/src/keys.rs`: Key encoding/decoding
- `crates/mqdb-core/src/schema.rs`: Validation, defaults
- `crates/mqdb-core/src/constraint.rs`: Constraint creation
- `crates/mqdb-core/src/checksum.rs`: Corruption detection
- `crates/mqdb-core/src/index.rs`: Index definition

**2. Integration Tests:**
- `integration_test.rs`: CRUD, subscriptions, indexes, relationships, TTL
- `constraint_test.rs`: All constraint types and combinations
- `crash_recovery_test.rs`: Restart scenarios, data integrity
- `atomicity_test.rs`: Per-entity atomicity, concurrent writes
- `concurrency_test.rs`: Parallel writes, race conditions
- `durability_test.rs`: Fsync behavior, crash simulation
- `point_to_point_test.rs`: Shared subscriptions, consumer groups, heartbeat

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

## 14. Operational Considerations

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

## 15. Security: Topic Protection

### Overview

Topic protection provides defense-in-depth security by enforcing access controls at the broker level, independent of ACL configuration. Even with a permissive ACL like `$DB/#`, internal topics remain protected.

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     MQTT Client Request                          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              TopicProtectionAuthProvider                         │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  1. Check if internal client (mqdb-* prefix)            │   │
│  │     → Yes: Bypass protection, delegate to inner         │   │
│  │  2. Check against PROTECTED_TOPICS rules                │   │
│  │     → BlockAll: Return denied                           │   │
│  │     → ReadOnly: Deny publish, allow subscribe           │   │
│  │     → AdminRequired: Check admin_users set              │   │
│  │  3. Check internal entity access ($DB/_*)               │   │
│  │  4. Delegate to inner AuthProvider (ACL)                │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              CompositeAuthProvider (ACL)                         │
│              - Password/SCRAM/JWT authentication                 │
│              - ACL rule evaluation                               │
└─────────────────────────────────────────────────────────────────┘
```

### Protection Rules

Located in `crates/mqdb-agent/src/topic_rules.rs`:

| Pattern | Tier | Purpose |
|---------|------|---------|
| `_mqdb/#` | BlockAll | Cluster heartbeat, coordination |
| `$DB/_idx/#` | BlockAll | Secondary index data |
| `$DB/_unique/#` | BlockAll | Unique constraint enforcement |
| `$DB/_fk/#` | BlockAll | Foreign key validation |
| `$DB/_query/#` | BlockAll | Query execution internals |
| `$DB/p+/#` | BlockAll | Partition-specific topics (p0, p1, ..., p63) |
| `$SYS/#` | ReadOnly | Broker statistics, monitoring |
| `$DB/_admin/#` | AdminRequired | Schema, constraints, backup operations |
| `$DB/_oauth_tokens/#` | AdminRequired | OAuth token storage |

### Pattern Matching

The `matches_pattern` function supports:
- `#` - Multi-level wildcard (matches any number of levels)
- `+` - Single-level wildcard (matches exactly one level)
- `p+` - Prefix wildcard (matches `p` followed by digits: `p0`, `p63`)

Example: `$DB/p+/#` matches `$DB/p0/users/create` but NOT `$DB/posts/create`

### Admin User Management

Admin users are configured via:
1. CLI: `--admin-users alice,bob`
2. Config: `auth_config.admin_users: HashSet<String>`

Admin status is checked by matching the MQTT username against the admin_users set.

### Internal Service Exemption

To perform essential background tasks (like managing cluster state), each MQDB node requires privileged access to its own broker. This is achieved through a dynamic, self-registering internal service account, ensuring security without requiring shared static credentials across a cluster.

#### Identity Creation at Startup

On every startup, each MQDB node generates a new, unique identity for its internal client:

1.  **Username:** A random, unpredictable username is created with the format `mqdb-internal-<UUID>`.
2.  **Password:** A random, unpredictable password is also generated.

This process occurs within the `configure_broker_auth` function in `crates/mqdb-agent/src/auth_config.rs`.

#### Automatic Registration

A key part of this design is automatic registration. After generating the new credentials, the node immediately makes them valid for its own broker. For example, if using a password file, the node automatically appends the new `username:hashed_password` pair to the file. This ensures the internal client can always authenticate with its own broker instance.

#### Authorization Bypass

The `TopicProtectionAuthProvider` is configured with the dynamically generated `service_username`. When a client connects, the provider performs the following check:

1.  It compares the authenticated `user_id` of the connecting client against the stored `service_username`.
2.  If they match exactly, the client is identified as the internal service client.
3.  The provider then **bypasses all topic protection rules** (`BlockAll`, `AdminRequired`, etc.) and delegates the authorization decision to the next provider in the chain (e.g., the ACL provider).

This mechanism is secure because the credentials are node-local, random, and ephemeral. An external client cannot guess the credentials, and simply using a client ID with an `mqdb-` prefix provides no special access, as the check is performed on the authenticated username.

#### Cluster Interaction

In a cluster, each node's internal client only ever connects to its own local broker instance to perform privileged operations. The results of these operations (e.g., a state change) are then propagated to other nodes via the underlying cluster transport (secured by mTLS when --quic-ca is provided), not by having one node's internal client connect to another node.

### Source Files

| File | Purpose |
|------|---------|
| `crates/mqdb-agent/src/topic_rules.rs` | Protection rules, pattern matching, tier definitions |
| `crates/mqdb-agent/src/topic_protection.rs` | AuthProvider wrapper implementation |
| `crates/mqdb-agent/src/auth_config.rs` | Admin user configuration |
| `crates/mqdb-agent/src/agent/mod.rs` | Wraps broker auth with protection (agent mode) |
| `crates/mqdb-cluster/src/cluster_agent/mod.rs` | Wraps broker auth with protection (cluster mode) |

### Block Reasons

When topic protection blocks access, internal logging captures:

| Reason | Description |
|--------|-------------|
| `InternalTopicBlocked` | Tier BlockAll - completely blocked |
| `ReadOnlyTopic` | Tier ReadOnly - publish attempt blocked |
| `AdminRequired` | Tier AdminRequired - non-admin user |
| `InternalEntityAccess` | Entity starting with `_` accessed by non-admin |

### Integration with ACL

Topic protection runs **before** ACL evaluation:

1. **Topic Protection** (hardcoded, cannot override):
   - Blocks internal topics regardless of ACL rules
   - Enforces read-only on `$SYS/#`
   - Requires admin for sensitive topics

2. **ACL Layer** (user-configured):
   - Evaluates user rules, role rules, assignments
   - Applies `readwrite`, `read`, `write`, `deny` permissions

This layered approach ensures that even if an ACL grants broad access like `user * topic # permission readwrite`, internal MQDB topics remain protected.

---

## 16. MQTT User Properties: Identity & Echo Suppression

### Broker-Injected Properties (Anti-Spoof)

The broker (mqtt-lib) automatically injects user properties on every PUBLISH packet. It strips any client-supplied values first, then injects the real ones — clients cannot spoof these.

| Property | Value | Purpose |
|----------|-------|---------|
| `x-mqtt-sender` | Authenticated username (absent in anonymous mode) | Ownership enforcement, access control |
| `x-mqtt-client-id` | MQTT `client_id` from CONNECT | Client identification passed to DB layer |

Both are set in `mqtt-lib/crates/mqtt5/src/broker/client_handler/publish.rs` via `Properties::inject_sender()` and `Properties::inject_client_id()`.

### Application-Level Property

| Property | Value | Purpose |
|----------|-------|---------|
| `x-origin-client-id` | The `client_id` that triggered the DB mutation | Echo suppression in change-event subscribers |

Set by MQDB's event publisher when emitting change events:
- Agent mode: `crates/mqdb-agent/src/agent/tasks.rs`
- Cluster mode: `crates/mqdb-cluster/src/cluster/db_handler/json_ops.rs`

### Why Both `x-mqtt-client-id` and `x-origin-client-id` Exist

Change events flow through an intermediary — the internal event publisher client. The broker injects the event publisher's own identity on that re-publish, not the originating client's:

```
1. Client "browser-abc" → PUBLISH $DB/users/create
   Broker injects: x-mqtt-sender = "alice@gmail.com"
                   x-mqtt-client-id = "browser-abc"

2. agent/handlers.rs extracts both properties,
   passes to execute_with_sender(sender, client_id)

3. Database creates record, emits ChangeEvent {
       sender: Some("alice@gmail.com"),
       client_id: Some("browser-abc"),
   }

4. Internal event publisher → PUBLISH $DB/users/events/create/{id}
   MQDB sets:       x-origin-client-id = "browser-abc"  (from ChangeEvent)
   Broker injects:  x-mqtt-client-id = "mqdb-internal-<uuid>"  (publisher's own)
                    x-mqtt-sender = "mqdb-internal-<uuid>"     (publisher's own)
```

Subscribers receiving the change event see:

| Property | Value | Useful for echo suppression? |
|----------|-------|------------------------------|
| `x-mqtt-client-id` | `mqdb-internal-<uuid>` | No — identifies the event publisher |
| `x-mqtt-sender` | `mqdb-internal-<uuid>` | No — identifies the event publisher |
| `x-origin-client-id` | `browser-abc` | **Yes** — identifies the originating client |

For regular pub/sub (non-DB topics), `x-mqtt-client-id` directly identifies the publisher and no `x-origin-client-id` is present.

### Property Usage by Message Type

| Message type | `x-mqtt-sender` | `x-mqtt-client-id` | `x-origin-client-id` |
|--------------|-----------------|---------------------|-----------------------|
| DB request (`$DB/.../create`) | Ownership checks | Stored in `ChangeEvent.client_id` | Not present |
| Regular pub/sub | Available to subscribers | Available to subscribers | Not present |
| Change event (`$DB/.../events/#`) | Internal publisher (ignore) | Internal publisher (ignore) | **Original client** |

### Echo Suppression in Subscribers

A browser client subscribed to `$DB/users/events/#` will receive its own mutations back as change events. To suppress these echoes, the subscriber compares `x-origin-client-id` against its own MQTT `client_id`:

```
if event.userProperties["x-origin-client-id"] === myClientId:
    skip (echo of own operation)
else:
    apply (change from another client)
```

### Source Files

| File | Role |
|------|------|
| `mqtt-lib/.../properties/accessors.rs` | `inject_sender()`, `inject_client_id()` |
| `mqtt-lib/.../client_handler/publish.rs` | Calls inject on every client PUBLISH |
| `crates/mqdb-agent/src/agent/handlers.rs:59-71` | Reads `x-mqtt-sender` and `x-mqtt-client-id` for DB operations |
| `crates/mqdb-agent/src/agent/tasks.rs:148-152` | Adds `x-origin-client-id` to change events (agent mode) |
| `crates/mqdb-cluster/src/cluster/db_handler/json_ops.rs:1009-1012` | Adds `x-origin-client-id` to change events (cluster mode) |
| `crates/mqdb-core/src/events.rs:17-31` | `ChangeEvent` struct carries `sender` and `client_id` through the pipeline |

---

## 17. Conclusion

### MQDB is Optimized For:

- **Real-time applications** needing change observation (reactive UIs, dashboards)
- **IoT systems** with native MQTT integration and topic-based API
- **Event-driven architectures** with point-to-point delivery and consumer groups
- **Cross-platform deployment** (native servers, WASM browsers)
- **Message processing pipelines** with ordered delivery guarantees

### Key Strengths:

1. **Native MQTT Integration:** Embedded broker with topic-based database API
2. **Distributed Clustering:** 256-partition sharding with Raft consensus and automatic rebalancing
3. **Point-to-Point Delivery:** Consumer groups with LoadBalanced and Ordered modes
4. **Per-Entity Atomicity:** Data, indexes, and outbox committed in single batch
5. **Reactive Subscriptions:** MQTT-style patterns for real-time change notifications
6. **Platform Flexibility:** Runs native (Fjall) or in browsers (WASM/Memory)
7. **Rust Safety:** Memory-safe, no data races, strong type system

### Design Philosophy:

> **Correct, then fast. Simple, then featureful.**

MQDB prioritizes:
- Correctness (per-entity atomicity, constraints, checksums)
- Simplicity (clear components, explicit errors)
- Then performance (good enough for embedded scale)
- Then features (incremental additions)

### When to Use MQDB:

✅ **Good fit:**
- IoT applications with MQTT devices
- Real-time dashboards and UIs
- Event sourcing with ordered delivery
- Message processing pipelines
- Browser-based applications (WASM)
- Rapid prototyping (schema-less)
- Distributed MQTT broker with embedded database (cluster mode)

❌ **Not ideal for:**
- Massive scale (billions of entities)
- Complex analytical queries (no query planner)
- SQL compatibility required

### Contributing:

The architecture is designed for extensibility. Key extension points:
- Custom storage backends (implement `StorageBackend` trait from `crates/mqdb-core/src/storage/backend.rs`)
- Cluster transport (implement `ClusterTransport` trait from `crates/mqdb-cluster/src/cluster/transport.rs`)
- MQTT integration layer (dispatcher hooks in `crates/mqdb-agent/src/agent/mod.rs`)
- Query language (filter system in `crates/mqdb-agent/src/database/mod.rs`)
- Additional constraint types (framework in place via `crates/mqdb-cluster/src/cluster/db/constraint_store.rs`)

See [CONTRIBUTING.md](../CONTRIBUTING.md) for development guidelines.

---

## Related Documentation

- **DISTRIBUTED_DESIGN.md** - Detailed cluster architecture and design decisions
- **IMPLEMENTATION_PLAN.md** - Milestone plan (all M1-M10 complete)
- **COMPLETE_MATRIX_DOC.md** - Benchmark specification
- **README.md** - User-facing documentation with examples
