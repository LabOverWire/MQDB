# MQDB Distributed Architecture Design

> **Living Document** - Updated iteratively with code verification.
> Every claim has file:line references. When in doubt, verify against code.
> Last verified: January 2026 (all milestones M1-M10 complete)

## Overview

MQDB is a distributed MQTT broker with embedded database capabilities. The cluster uses MQTT bridges for inter-node communication and Raft consensus for partition management.

**Design Foundations** (specified before implementation):
- **BeBytes crate** for all protocol encoding/decoding (custom, modifiable as needed)
- **QUIC preferred** for inter-node transport (UDP port mirrors TCP port 1883)
- **MQTT bridges** as the cluster communication substrate
- **64 fixed partitions** (never changes)
- **Replication Factor = 2** (primary + one replica)

---

# ARCHITECTURE

## A1: Unified MQTT-DB Architecture

MQDB presents a unique challenge: it must partition and replicate two fundamentally different data domains—MQTT broker state and database records—while maintaining consistency between them.

### A1.1 The Core Insight

Both MQTT and database operations reduce to the same primitive: **keyed writes to partitioned stores**. A subscription is a write keyed by client_id. A database record is a write keyed by entity+id. Both can be serialized, partitioned, replicated, and queried using identical machinery.

### A1.2 Universal Write Abstraction

Every mutation in MQDB—whether from an MQTT client subscribing or a database insert—flows through a single abstraction: `ReplicationWrite`. This structure carries:

- **What changed**: Entity type, identifier, serialized data
- **How it changed**: Insert, Update, or Delete operation
- **Where it lives**: Partition ID (0-63) for routing
- **When it changed**: Epoch and sequence for ordering

The entity type string (e.g., `_sessions`, `_db_data`, `_topic_index`) determines which store processes the write on each node.

### A1.3 Two Classes of Entities

**Partitioned Entities** follow standard hash-based distribution:
- Sessions, subscriptions, QoS state → partitioned by `hash(client_id) % 64`
- Retained messages → partitioned by `hash(topic) % 64`
- Database records → partitioned by `hash(entity/id) % 64`

**Broadcast Entities** must exist completely on every node:
- TopicIndex (`_topic_index`) → maps topics to subscribers
- WildcardStore (`_wildcards`) → holds wildcard subscription patterns
- ClientLocationStore (`_client_loc`) → maps client_id to connected node

Broadcast entities exist because publish routing requires local lookups. When a message arrives on Node A, it must immediately determine which clients (potentially on Node B) subscribe to that topic—without cross-node queries.

---

## A2: Event Flow Architecture

### A2.1 MQTT Events as Storage Operations

The `ClusterEventHandler` intercepts 7 MQTT broker events and translates them into storage operations:

| Event | Storage Operation |
|-------|-------------------|
| `on_client_connect` | Create/update session, store will topic |
| `on_client_disconnect` | Update session, publish LWT if needed |
| `on_client_subscribe` | Add subscription, update topic index, query retained |
| `on_client_unsubscribe` | Remove subscription, update topic index |
| `on_client_publish` | Route to subscribers, handle QoS state |
| `on_retained_set` | Store/clear retained message |
| `on_message_delivered` | Acknowledge QoS 1/2 delivery |

Each handler follows the same pattern: read current state, compute new state, generate `ReplicationWrite`, route to primary.

### A2.2 The write_or_forward Decision

When a write is generated, the node must decide its role:

1. **Am I the primary?** Check `partition_map.primary(partition) == my_node_id`
2. **If primary**: Apply locally, replicate to replicas
3. **If not primary**: Forward `WriteRequest` to the partition's primary node
4. **Special case**: Broadcast entities apply locally first, then follow normal routing

This creates a two-phase pattern for non-primary nodes: the write eventually reaches the primary, which applies and replicates it, and the write returns to the originating node as a replicated write.

### A2.3 DB Operations as Events

Database operations enter through the `$DB/` MQTT topic namespace. The `DbRequestHandler` parses these publishes and calls controller methods (`db_create`, `db_update`, `db_delete`) that follow the same `ReplicationWrite → write_or_forward` pattern as MQTT operations.

**Two API Levels:**

| API | Topic Pattern | Payload | Use Case |
|-----|---------------|---------|----------|
| Low-level | `$DB/p{partition}/{entity}/{op}` | Binary BeBytes | Direct partition access, max performance |
| High-level | `$DB/{entity}/{op}` | JSON | Auto-routing, CLI-friendly |

The high-level JSON API (`db_topic.rs:174-218`) automatically selects a local partition for creates and routes operations to the appropriate partition primary. This enables standard CLI commands (`mqdb create/read/update/delete/list`) to work transparently in cluster mode.

**Key Files**: `event_handler.rs:43-668`, `node_controller.rs:904-942`, `db_handler.rs`, `db_topic.rs`

### A2.4 Write Amplification: Subscription Example

A single MQTT subscription demonstrates how broadcast entities create write amplification. When a client subscribes to a topic, the system generates **65 to 129 ReplicationWrites** depending on whether the pattern contains wildcards.

**Step 1: Subscription Record** (`event_handler.rs:293-297`)

The subscription itself is stored in `_mqtt_subs`, partitioned by client_id:
```
add_subscription_replicated(client_id, topic, qos)
→ 1 ReplicationWrite to partition hash(client_id) % 64
```

**Step 2: TopicIndex Broadcast** (`event_handler.rs:334-342`)

Every node needs the complete topic→subscriber mapping for publish routing. The TopicIndex is a broadcast entity, so it writes to all 64 partitions:
```
subscribe_topic_replicated(topic, client_id, partition, qos)
→ 64 ReplicationWrites, one per partition (store_manager.rs:868-880)
```

**Step 3: Wildcard Broadcast** (`event_handler.rs:299-317`, only if pattern contains `+` or `#`)

Wildcard patterns also need cluster-wide visibility:
```
subscribe_wildcard_replicated() + WildcardBroadcast
→ 64 ReplicationWrites to _wildcards entity
```

**Total writes per subscription:**

| Pattern Type | Writes Generated | Breakdown |
|--------------|------------------|-----------|
| Exact topic (e.g., `sensor/temp`) | 65 | 1 subscription + 64 topic index |
| Wildcard topic (e.g., `sensor/+/temp`) | 129 | 1 subscription + 64 topic index + 64 wildcards |

This amplification is fundamental to the design—without broadcast entities, publish routing would require cross-node queries for every message, destroying throughput.

---

## A3: Store Abstraction Layer

### A3.1 StoreManager: The Unified Coordinator

The `StoreManager` coordinates 15 distinct stores behind a common interface:

**MQTT Stores (10)**:
- `sessions` - Client session lifecycle
- `subscriptions` - Topic subscriptions per client
- `retained` - Retained messages by topic
- `topics` - TopicIndex (topic → subscribers)
- `wildcards` - Wildcard subscription trie
- `wildcard_pending` - Pending wildcard operations
- `qos2` - QoS 2 exactly-once state machines
- `inflight` - QoS 1 messages awaiting acknowledgment
- `offsets` - Consumer group offsets
- `idempotency` - Request deduplication tokens

**Database Stores (5)**:
- `db_data` - Entity records
- `db_schema` - Entity schemas with constraints
- `db_index` - Secondary indexes
- `db_unique` - Unique constraint reservations
- `db_fk` - Foreign key validation

### A3.2 The apply_write() Dispatcher

When a `ReplicationWrite` arrives (either from local operations or replication), the central `apply_write()` method dispatches based on entity type string:

```
match write.entity:
    "_sessions"     → sessions.apply_replicated()
    "_mqtt_subs"    → subscriptions.apply_replicated()
    "_topic_index"  → topics.apply_replicated()
    "_db_data"      → db_data.apply_replicated()
    ...
```

Each store implements idempotent `apply_replicated()` methods that handle Insert/Update/Delete operations uniformly.

### A3.3 Dual-Mode Methods

Stores expose paired methods for generating replicated writes:

- `create_session_replicated()` → Returns `(SessionData, ReplicationWrite)`
- `db_upsert_replicated()` → Returns `(DbEntity, ReplicationWrite)`
- `set_retained_replicated()` → Returns `(RetainedMessage, ReplicationWrite)`

The caller uses the entity immediately and routes the write for cluster-wide replication.

**Key Files**: `store_manager.rs:68-128`, `entity.rs`

---

## A4: Replication Pipeline

### A4.1 Two-Tier Replication Model

MQDB separates cluster coordination from data replication:

**Control Plane (Raft Consensus)** — Strong consistency for cluster metadata:
- Partition assignments (which node owns partition 0-63)
- Cluster membership (add/remove nodes)
- Leader election

**Data Plane (Async Replication)** — High throughput for actual data:
- Primary receives write, assigns sequence, applies locally, broadcasts to replicas
- Replicas validate sequence ordering, apply in-order, send acknowledgments
- Write returns to caller immediately after sending to replicas (doesn't wait for acks)
- Acks are processed asynchronously for logging/observability only

This separation is intentional. Running every data write through Raft consensus would require 2+ round trips per write—unacceptable for MQTT message throughput. Instead, Raft ensures all nodes agree on WHO owns each partition, then data flows directly primary→replica without consensus overhead.

The tradeoff: potential data loss if primary fails before replication completes. See Part 8 for full Raft details.

**Key Files**: `raft/coordinator.rs` (control plane), `replication.rs` (data plane)

### A4.2 Durability Tradeoff

The async replication model creates a durability gap:

1. Primary applies write locally, sends to replicas, returns success to client
2. Primary crashes before replicas receive/apply the write
3. Replica becomes new primary — **write is lost**

The client believes the write succeeded, but the data is gone. This is a fundamental distributed systems tradeoff:

| Approach | Examples | Latency | Durability |
|----------|----------|---------|------------|
| Synchronous (wait for quorum) | etcd, Kafka `acks=all` | High (2+ RTT) | Strong |
| Semi-synchronous (wait for 1 replica) | MySQL semi-sync | Medium | Good |
| Asynchronous (current MQDB) | Redis, MongoDB `w:1` | Low | Weak |

**Why MQDB chose async**: MQTT broker throughput. Waiting for quorum on every publish would add 2+ network round trips per message—unacceptable for high-frequency sensor data or real-time messaging.

**The infrastructure for sync exists**: `QuorumTracker::with_completion()` returns a oneshot receiver that resolves when quorum is reached (`node_controller.rs:843`). The `create_session_quorum()` method demonstrates the pattern but is currently unused.

**See A7 for planned improvements** to add configurable durability levels.

### A4.3 Sequence Ordering Algorithm

Each partition maintains sequence state on the replica (`replication.rs:84-123`):

- **`self.sequence`**: The highest sequence number successfully applied (starts at 0)
- **`expected`**: The next sequence we need = `self.sequence + 1`
- **`pending_writes`**: Buffer for out-of-order writes we can't apply yet

When a write arrives with sequence `seq`, the replica decides:

| Condition | Meaning | Action | Response |
|-----------|---------|--------|----------|
| `seq < expected` | Already applied (duplicate) | Ignore, already have it | `Ack::Ok` |
| `seq == expected` | Exactly what we need | Apply, advance sequence, drain pending | `Ack::Ok` |
| `seq > expected` (gap ≤ 1000) | Missing some writes | Buffer this write, wait for missing | `Ack::SequenceGap(expected)` |
| `seq > expected` (gap > 1000) | Too far behind | Don't buffer (would consume too much memory) | `Ack::SequenceGap(expected)` |

**Example**: Replica has `sequence=5` (applied writes 1-5), so `expected=6`:

1. Receive `seq=4` → Already have it, respond OK (idempotent)
2. Receive `seq=8` → Missing 6,7. Buffer write 8 in `pending_writes`, respond `SequenceGap(6)`
3. Receive `seq=6` → Apply it, `sequence=6`. Check pending: 7 missing, stop. Respond OK
4. Receive `seq=7` → Apply it, `sequence=7`. Check pending: 8 exists! Apply 8, `sequence=8`. Respond OK

**Drain pending** (`replication.rs:125-134`): After applying a write, check if the next sequence is buffered. Keep applying consecutive buffered writes until a gap is found.

### A4.4 Catchup Mechanism

When a replica detects a sequence gap:

1. Replica sends `SequenceGap` acknowledgment with expected sequence
2. Primary retrieves missing writes from write_log (bounded buffer, 10K entries)
3. Primary sends `CatchupResponse` with missing writes
4. Replica processes writes in sequence order

If write_log has evicted the missing writes, the catchup response will be empty and the replica remains behind. Snapshot transfer is only triggered during partition migration (when a new primary takes over), not as an automatic fallback for catchup failures.

### A4.5 Epoch-Based Consistency

Epoch numbers prevent stale writes during rebalancing:

- Each partition assignment carries an epoch
- Writes carry the epoch of the assigning primary
- Replicas reject writes with epochs older than their current assignment
- Epoch increments on every partition reassignment

**Key Files**: `replication.rs:84-175`, `node_controller.rs:482-713`, `quorum.rs`

---

## A5: Query Coordination

### A5.1 Scatter-Gather Pattern

Distributed queries fan out to multiple partitions and aggregate results:

1. **Coordinator** generates `QueryRequest` for each target partition
2. **Partition owners** execute local queries, return `QueryResponse`
3. **Coordinator** collects responses, detects completion or timeout
4. **Merge phase** deduplicates by ID, applies filters, then sorts results
5. **Result** includes aggregated data plus pagination cursor

The merge phase ensures consistent ordering regardless of which node receives the query or the order in which partition responses arrive.

### A5.2 Partition Pruning

Single-ID queries can skip scatter-gather:

- Query for specific `entity/id` → hash to single partition
- Filter with `id=VALUE` → extract value, hash to single partition
- Otherwise → query all 64 partitions

### A5.3 Pagination via ScatterCursor

Multi-partition pagination uses a composite cursor:

- `PartitionCursor`: partition ID, sequence, last_key
- `ScatterCursor`: aggregates cursors for all queried partitions

Each partition maintains independent pagination state, allowing efficient resumption.

### A5.4 Retained Message Queries

Wildcard subscriptions require all-partition queries:

- Exact topic: `hash(topic) % 64` → single partition
- Wildcard pattern (`+`, `#`): query all 64 partitions, filter locally

**Key Files**: `query_coordinator.rs:1-470`, `cursor.rs:1-140`, `protocol.rs:661-909` (QueryRequest/Response)

---

## A6: Transport Layer Design

### A6.1 Why MQTT Bridges?

MQDB uses MQTT as the cluster communication substrate rather than custom RPC:

- **Reuses existing infrastructure**: The embedded MQTT broker already handles messaging
- **Topic-based routing**: Natural fit for partition-specific traffic
- **Built-in reliability**: QoS 1/2 provide delivery guarantees
- **Debuggability**: Standard MQTT tools work on cluster traffic
- **No new dependencies**: No Zookeeper, etcd, or custom discovery

### A6.1.1 Async Transport (RPITIT)

The `ClusterTransport` trait uses native Rust async traits (RPITIT - Return Position Impl Trait in Trait):

```rust
pub trait ClusterTransport: Send + Sync + Debug {
    fn send(&self, to: NodeId, message: ClusterMessage)
        -> impl Future<Output = Result<(), TransportError>> + Send + '_;

    fn broadcast(&self, message: ClusterMessage)
        -> impl Future<Output = Result<(), TransportError>> + Send + '_;

    fn recv(&self) -> Option<InboundMessage>;  // Sync polling
}
```

All callers properly `await` transport operations. No fire-and-forget spawning—this was critical for fixing packet corruption issues (see 11.7).

### A6.2 Topic Namespaces

Five topic namespaces structure MQDB communication:

| Prefix | Purpose | QoS | Bridged |
|--------|---------|-----|---------|
| `_mqdb/cluster/` | Control messages, heartbeats, Raft | 1 | Yes |
| `_mqdb/repl/` | Partition replication | 1 | Yes |
| `_mqdb/forward/` | Message forwarding to subscribers | 1 | Yes |
| `$DB/` | Database operations | 2 | Yes |
| `$SYS/` | Admin operations (rebalance, status) | 1 | No (local) |

**Detailed topic patterns** (`mqtt_transport.rs:107-163`):

| Pattern | Purpose |
|---------|---------|
| `_mqdb/cluster/nodes/{node_id}` | Unicast control message to specific node |
| `_mqdb/cluster/broadcast` | Broadcast control message to all nodes |
| `_mqdb/cluster/heartbeat/{node_id}` | Node heartbeat from specific node |
| `_mqdb/repl/p{partition}/seq{sequence}` | Replication write for partition at sequence |
| `_mqdb/forward/{partition}` | Forward publish to clients on partition's node |

**$DB/ topic patterns** - Two modes exist:

*Client API patterns* (`agent.rs:90-105`) — Used by CLI and external clients:

| Pattern | Purpose |
|---------|---------|
| `$DB/{entity}/create` | Create record (auto-partitioned) |
| `$DB/{entity}/{id}` | Read record |
| `$DB/{entity}/{id}/update` | Update record |
| `$DB/{entity}/{id}/delete` | Delete record |
| `$DB/{entity}/list` | List records |
| `$DB/{entity}/events/#` | Subscribe to entity changes |

*Internal partitioned patterns* (`db_topic.rs`) — Used for cross-node routing:

| Pattern | Purpose |
|---------|---------|
| `$DB/p{partition}/{entity}/create` | Create on specific partition |
| `$DB/p{partition}/{entity}/{id}` | Read from specific partition |
| `$DB/p{partition}/{entity}/{id}/update` | Update on specific partition |
| `$DB/p{partition}/{entity}/{id}/delete` | Delete on specific partition |
| `$DB/_idx/p{partition}/update` | Secondary index update |
| `$DB/_unique/p{partition}/{op}` | Unique constraint (reserve/commit/release) |
| `$DB/_fk/p{partition}/validate` | Foreign key validation |
| `$DB/_query/{query_id}/request` | Distributed query scatter |
| `$DB/_query/{query_id}/response` | Distributed query gather |

*Subscription patterns* (`agent.rs:49-63`):

| Pattern | Purpose |
|---------|---------|
| `$DB/_sub/subscribe` | Register change subscription |
| `$DB/_sub/{sub_id}/heartbeat` | Keep subscription alive |
| `$DB/_sub/{sub_id}/unsubscribe` | Remove subscription |

*Admin patterns* (`bin/mqdb.rs`):

| Pattern | Purpose |
|---------|---------|
| `$DB/_admin/schema/{entity}/set` | Define entity schema |
| `$DB/_admin/schema/{entity}/get` | Get entity schema |
| `$DB/_admin/constraint/{entity}/add` | Add constraint |
| `$DB/_admin/constraint/{entity}/list` | List constraints |
| `$DB/_admin/backup` | Trigger backup |
| `$DB/_admin/backup/list` | List backups |
| `$DB/_admin/restore` | Restore from backup |
| `$DB/_admin/consumer-groups` | List consumer groups |
| `$DB/_admin/consumer-groups/{name}` | Get consumer group |
| `$DB/_resp/{client_id}` | Response routing |

### A6.3 QUIC as Preferred Transport

When available, MQTT bridges use QUIC instead of TCP:

- **StreamStrategy::DataPerTopic**: Each topic gets its own stream (no head-of-line blocking)
- **Flow headers**: Congestion awareness
- **Datagrams**: Unreliable delivery option for heartbeats
- **Automatic fallback**: TCP if QUIC unavailable

### A6.4 Bridge Topology

Each node creates N-1 outbound bridges (one per peer). Messages flow bidirectionally across each bridge. Bridge configuration:

- `clean_start = false`: Preserve session across reconnects
- Topics: `_mqdb/cluster/#`, `_mqdb/forward/#`, `_mqdb/repl/#`, `$DB/#`

**QUIC stream caching** (`quic_stream_manager.rs`): With `DataPerTopic` strategy, streams are cached per topic for reuse. When cache reaches 100 streams (hardcoded default), LRU eviction closes the oldest stream. This is a **performance optimization**, not a limit—unlimited topics are supported, they just share cached streams via eviction.

**Key Files**: `transport.rs:83-113`, `mqtt_transport.rs:19-21`, `cluster_agent.rs:172-201`

### A6.5 Admin Request Handlers

The cluster agent (`cluster_agent.rs`) handles admin requests via `$SYS/mqdb/cluster/#`:

| Topic | Handler | Response |
|-------|---------|----------|
| `$SYS/mqdb/cluster/status` | `build_status_response()` | Node info, Raft state, partition assignments |
| `$SYS/mqdb/cluster/rebalance` | `handle_rebalance_request()` | Triggers partition rebalancing (leader only) |

**Request/Response Pattern**:
1. CLI publishes to admin topic with `response_topic` in message properties
2. Cluster agent extracts `response_topic` from `msg.properties.response_topic`
3. Handler builds JSON response
4. Agent publishes response to the specified `response_topic`

**Status Response** includes:
- `node_id`, `node_name`, `is_raft_leader`, `raft_term`
- `alive_nodes` - list of other alive node IDs
- `partitions` - all 64 partitions with primary, replicas, epoch

**CLI Commands**:
```bash
mqdb cluster status --broker 127.0.0.1:1883
mqdb cluster rebalance --broker 127.0.0.1:1883
```

---

## A7: Future Improvements

### A7.1 Configurable Durability Levels

**Problem**: Current async replication can lose writes if primary fails before replication completes.

**Proposed Solution**: Per-operation durability settings:

| Level | Behavior | Use Case |
|-------|----------|----------|
| `Async` | Current behavior, return immediately | High-frequency sensor data |
| `OneReplica` | Wait for 1 replica ack | Important messages |
| `Quorum` | Wait for majority acks | Critical data, DB operations |

**Implementation Path**:
1. `QuorumTracker::with_completion()` already returns a oneshot receiver
2. Add durability parameter to `replicate_write()` variants
3. For MQTT: map to QoS levels (QoS 0 → Async, QoS 1 → OneReplica, QoS 2 → Quorum)
4. For DB: add durability field to `$DB/` request protocol

**Alternative Approaches Considered**:

- **Local WAL with fsync**: Sync to local disk before returning, async replicate. On failure, new primary recovers from old primary's disk. Adds disk I/O latency but simpler than distributed quorum.

- **Chain replication**: Primary → Replica1 → Replica2, client acks from tail. Guarantees all replicas have data before ack. Higher latency but stronger guarantee.

### A7.2 Automatic Catchup Fallback to Snapshot

**Problem**: When write_log has evicted missing writes, catchup fails silently and replica remains permanently behind.

**Proposed Solution**: Trigger snapshot transfer when catchup detects evicted writes, not just during partition migration.

### A7.3 Read-Your-Writes Consistency

**Problem**: After a write, reading from a replica may return stale data.

**Proposed Solution**: Return sequence number with write ack, allow reads to specify minimum sequence requirement.

---

# IMPLEMENTATION REFERENCE

## Part 1: Core Types & Constants

### 1.1 Fundamental Types

| Type | Definition | Range | File:Line |
|------|------------|-------|-----------|
| `NUM_PARTITIONS` | `64` (u16 const) | Fixed, never changes | `partition.rs:3` |
| `NodeId` | Newtype(u16) | 1-65535, 0=INVALID | `node.rs:4-72` |
| `PartitionId` | Newtype(u16) | 0-63 | `partition.rs:5-94` |
| `Epoch` | Newtype(u64) | 0-MAX, saturating add | `epoch.rs:4-95` |

**NodeId** (`src/cluster/node.rs:4-72`):
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, BeBytes)]
pub struct NodeId(u16);

impl NodeId {
    pub const INVALID: u16 = 0;

    pub fn validated(id: u16) -> Option<Self> {
        if id == Self::INVALID { None } else { Some(Self(id)) }
    }
}
```

**PartitionId** (`src/cluster/partition.rs:5-94`):
```rust
pub const NUM_PARTITIONS: u16 = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PartitionId(u16);

impl PartitionId {
    pub fn new(id: u16) -> Option<Self> {
        if id < NUM_PARTITIONS { Some(Self(id)) } else { None }
    }

    pub fn all() -> impl Iterator<Item = Self> {
        (0..NUM_PARTITIONS).map(Self)
    }
}
```

### 1.2 Entity Constants

All cluster-managed data types (`src/cluster/entity.rs`):

| Constant | Value | Purpose |
|----------|-------|---------|
| `SESSIONS` | `"_sessions"` | Client session state |
| `QOS2` | `"_mqtt_qos2"` | QoS 2 exactly-once state |
| `INFLIGHT` | `"_mqtt_inflight"` | QoS 1 pending messages |
| `SUBSCRIPTIONS` | `"_mqtt_subs"` | Per-client subscriptions |
| `RETAINED` | `"_mqtt_retained"` | Retained messages |
| `TOPIC_INDEX` | `"_topic_index"` | Topic → subscriber mapping (BROADCAST) |
| `WILDCARDS` | `"_wildcards"` | Wildcard subscription trie (BROADCAST) |
| `CLIENT_LOCATIONS` | `"_client_loc"` | Client → connected node mapping (BROADCAST) |
| `OFFSETS` | `"_offsets"` | Consumer offsets |
| `IDEMPOTENCY` | `"_idemp"` | Idempotency tokens |
| `DB_DATA` | `"_db_data"` | Database records |
| `DB_SCHEMA` | `"_db_schema"` | Database schemas |
| `DB_INDEX` | `"_db_idx"` | Database indexes |
| `DB_UNIQUE` | `"_db_unique"` | Database unique constraints |
| `DB_FK` | `"_db_fk"` | Database foreign keys |
| `DB_CONSTRAINT` | `"_db_constraint"` | Constraint definitions (BROADCAST) |

**BROADCAST entities** (`TOPIC_INDEX`, `WILDCARDS`, `CLIENT_LOCATIONS`) must exist on ALL nodes for publish routing.

### 1.3 Timing Constants

| Constant | Value | File |
|----------|-------|------|
| `SUBSCRIPTION_RECONCILIATION_INTERVAL_MS` | 300,000 (5 min) | `subscription_cache.rs:7` |
| `WILDCARD_RECONCILIATION_INTERVAL_MS` | 60,000 (1 min) | `wildcard_pending.rs:6` |
| `CATCHUP_REQUEST_INTERVAL_MS` | 5,000 | `replication.rs:24` |
| `OFFSET_STALE_TTL_MS` | 604,800,000 (7 days) | `offset_store.rs:86` |
| `DEFAULT_QUERY_TIMEOUT_MS` | 10,000 | `query_coordinator.rs:7` |
| `DEFAULT_REBALANCE_TIMEOUT_MS` | 60,000 | `rebalance_coordinator.rs:5` |
| `TTL_MS` (idempotency) | 86,400,000 (24 hours) | `idempotency_store.rs:7` |
| `DEFAULT_CHUNK_SIZE` (snapshot) | 65,536 (64 KB) | `snapshot.rs:290` |

### 1.4 Partitioning Scheme

- **64 fixed partitions** (0-63) - count NEVER changes
- **Replication Factor (RF) = 2** - primary + 1 replica
- Partition ID = `crc32fast::hash(key.as_bytes()) % 64`
- Partition map managed by Raft consensus

### 1.5 Node Roles Per Partition

Each node can be:
- **Primary** for some partitions (handles writes, replicates to replicas)
- **Replica** for other partitions (receives replicated writes, failover target)
- **None** for partitions it doesn't own

### 1.6 Inter-Node Communication

- **MQTT Bridges** connect nodes (not direct TCP sockets)
- Bridges subscribe to cluster topics on remote nodes
- All cluster messages flow through MQTT publish/subscribe
- Topic prefixes (`src/cluster/mqtt_transport.rs:19-21`):
  ```rust
  const CLUSTER_TOPIC_PREFIX: &str = "_mqdb/cluster";
  const REPLICATION_TOPIC_PREFIX: &str = "_mqdb/repl";
  const FORWARD_TOPIC_PREFIX: &str = "_mqdb/forward";
  ```

### 1.7 Transport Protocol

QUIC preferred for multi-node clusters:
- Better congestion control
- Stream multiplexing (`StreamStrategy::DataPerTopic`)
- Built-in encryption
- TCP fallback for testing/development

---

## Part 2: Message Protocol

### 2.1 Wire Format

All cluster messages follow this format (`src/cluster/mqtt_transport.rs:318-389`):

```
[sender_node_id: 2 bytes BE][message_type: 1 byte][serialized_data: variable]
```

### 2.2 Message Type Codes

| Code | Type | Purpose |
|------|------|---------|
| 0 | `Heartbeat` | Node liveness and partition claims |
| 1 | `DetailedHeartbeat` | Reserved (not currently used) |
| 2 | `DeathNotice` | Node declared dead |
| 3 | `DrainNotification` | Node draining for shutdown |
| 10 | `Write` | ReplicationWrite to replicas |
| 11 | `Ack` | ReplicationAck from replica |
| 12 | `CatchupRequest` | Request missed writes |
| 13 | `CatchupResponse` | Missed writes response |
| 15 | `WriteRequest` | Forward write to primary |
| 20 | `RequestVote` | Raft election request |
| 21 | `RequestVoteResponse` | Raft election response |
| 22 | `AppendEntries` | Raft log replication |
| 23 | `AppendEntriesResponse` | Raft log replication ack |
| 30 | `ForwardedPublish` | Client publish to remote subscriber |
| 40 | `SnapshotRequest` | Request partition snapshot |
| 41 | `SnapshotChunk` | Snapshot data chunk |
| 42 | `SnapshotComplete` | Snapshot transfer complete |
| 50 | `QueryRequest` | Cross-node query |
| 51 | `QueryResponse` | Query results |
| 52 | `BatchReadRequest` | Batch read request |
| 53 | `BatchReadResponse` | Batch read response |
| 54 | `JsonDbRequest` | JSON database operation request |
| 55 | `JsonDbResponse` | JSON database operation response |
| 60 | `WildcardBroadcast` | Wildcard subscription broadcast |
| 70 | `PartitionUpdate` | Partition assignment change |
| 80 | `UniqueReserveRequest` | Reserve unique constraint value |
| 81 | `UniqueReserveResponse` | Unique reservation response |
| 82 | `UniqueCommitRequest` | Commit reserved unique value |
| 83 | `UniqueCommitResponse` | Unique commit response |
| 84 | `UniqueReleaseRequest` | Release reserved unique value |
| 85 | `UniqueReleaseResponse` | Unique release response |

### 2.3 Heartbeat Message (27 bytes)

`src/cluster/protocol.rs` - BeBytes serialization:

```rust
struct Heartbeat {
    version: u8,           // 1 byte, = 1
    node_id: u16,          // 2 bytes, sender BE
    timestamp_ms: u64,     // 8 bytes, current time BE
    primary_bitmap: u64,   // 8 bytes, bit N = primary for partition N
    replica_bitmap: u64,   // 8 bytes, bit N = replica for partition N
}
// Total: 1 + 2 + 8 + 8 + 8 = 27 bytes
```

### 2.4 ReplicationWrite Message (variable)

`src/cluster/protocol.rs:152-208`:

```
version: u8
partition: u16 BE
operation: u8 (0=Insert, 1=Update, 2=Delete)
epoch: u32 BE
sequence: u64 BE
entity_len: u8
id_len: u8
data_len: u32 BE
entity: [entity_len bytes]
id: [id_len bytes]
data: [data_len bytes]
```

### 2.5 ForwardedPublish Message (variable)

`src/cluster/protocol.rs:515-658`:

```rust
struct ForwardedPublish {
    origin_node: NodeId,         // Node that received original PUBLISH
    topic: String,
    qos: u8,
    retain: bool,
    payload: Vec<u8>,
    targets: Vec<ForwardTarget>, // Clients on destination node
    timestamp_ms: u64,           // For deduplication (avoids dropping repeated messages)
}

struct ForwardTarget {
    client_id: String,
    qos: u8,
}
```

Wire format (VERSION 2):
```
version: u8 (= 2)
origin_node: u16 BE
timestamp_ms: u64 BE
topic_len: u16 BE
topic: [topic_len bytes UTF-8]
qos: u8
retain: u8 (0/1)
payload_len: u32 BE
payload: [payload_len bytes]
target_count: u8
[for each target]:
  client_id_len: u8
  client_id: [client_id_len bytes UTF-8]
  target_qos: u8
```

### 2.6 Raft Messages

**RequestVoteRequest** (26 bytes, `src/cluster/raft/rpc.rs`):
```
term: u64           // 8 bytes
candidate_id: u16   // 2 bytes
last_log_index: u64 // 8 bytes
last_log_term: u64  // 8 bytes
// Total: 8 + 2 + 8 + 8 = 26 bytes
```

**RequestVoteResponse** (9 bytes):
```rust
struct RequestVoteResponse {
    term: u64,
    vote_granted: u8,  // 1=yes, 0=no
}
```

**AppendEntriesRequest** (variable):
```
Header (38 bytes):
  term: u64           // 8 bytes
  leader_id: u16      // 2 bytes
  prev_log_index: u64 // 8 bytes
  prev_log_term: u64  // 8 bytes
  leader_commit: u64  // 8 bytes
  entry_count: u32    // 4 bytes
  // Total: 8 + 2 + 8 + 8 + 8 + 4 = 38 bytes

Per entry:
  entry_len: u32
  entry_bytes: [LogEntry BeBytes]
```

**AppendEntriesResponse** (17 bytes):
```rust
struct AppendEntriesResponse {
    term: u64,
    success: u8,       // 1=yes, 0=no
    match_index: u64,
}
```

### 2.7 Snapshot Messages

**SnapshotRequest** (5 bytes, `src/cluster/snapshot.rs:29-57`):
```rust
struct SnapshotRequest {
    version: u8,      // = 1
    partition: u16,   // BE
    requester_id: u16, // BE
}
```

**SnapshotChunk** (23+ bytes, `src/cluster/snapshot.rs:59-140`):
```rust
struct SnapshotChunk {
    partition: PartitionId,
    chunk_index: u32,
    total_chunks: u32,
    sequence_at_snapshot: u64,
    data: Vec<u8>,           // Up to 64KB per chunk
}
```

**SnapshotComplete** (12 bytes, `src/cluster/snapshot.rs:142-202`):
```rust
struct SnapshotComplete {
    version: u8,
    partition: u16,
    status: u8,         // 0=Ok, 1=Failed, 2=NoData
    final_sequence: u64,
}
```

### 2.8 Cluster Topics

| Topic Pattern | Purpose | QoS |
|--------------|---------|-----|
| `_mqdb/cluster/nodes/{node_id}` | Unicast to specific node | 1 |
| `_mqdb/cluster/broadcast` | Broadcast to all nodes | 1 |
| `_mqdb/cluster/heartbeat/{node_id}` | Node heartbeats | 1 |
| `_mqdb/repl/{partition}/{sequence}` | Partition replication | 1 |
| `_mqdb/forward/{partition}` | Client message forwarding | 1 |
| `$DB/#` | Database operations | 2 |

---

## Part 3: Heartbeat Protocol

### 3.1 Heartbeat Structure

```rust
struct Heartbeat {
    version: u8,           // Protocol version
    node_id: u16,          // Sender node ID
    timestamp_ms: u64,     // Sender timestamp
    primary_bitmap: u64,   // Bit N = this node is primary for partition N
    replica_bitmap: u64,   // Bit N = this node is replica for partition N
}
```

### 3.2 Heartbeat Interval

- Send interval: 1000ms (1 second)
- Suspect threshold: 7500ms (timeout / 2, node marked suspected)
- Death threshold: 15000ms (node marked dead)

**Note**: Suspect threshold is computed dynamically as `heartbeat_timeout_ms / 2`.

### 3.3 Heartbeat Purposes

1. **Liveness Detection** - Detect node failures
2. **Partition Map Discovery** - Nodes learn primaries from heartbeats
3. **Consistency Verification** - Detect partition map discrepancies

### 3.4 Node Status State Machine

```
Unknown → (receive heartbeat) → Alive
Alive → (miss heartbeat) → Suspected
Suspected → (receive heartbeat) → Alive
Suspected → (timeout) → Dead
Dead → (receive heartbeat) → Alive
```

**Important**: Nodes in `Unknown` status are NOT timed out. Only timeout after receiving at least one heartbeat.

---

## Part 4: Partition Map Management

### 4.1 Raft-Based Updates

For nodes in the Raft cluster:
- Receive partition map updates via Raft log replication
- Authoritative source of partition assignments
- Updates triggered by:
  - Node join
  - Node failure
  - Rebalancing

### 4.2 Heartbeat-Based Discovery

For nodes joining the cluster (not yet in Raft):
- Learn partition primaries from heartbeats
- When heartbeat claims primary for partition and local map has no primary → trust heartbeat
- Enables routing writes before fully joining Raft cluster

### 4.3 Partition Assignment Structure

```rust
struct PartitionAssignment {
    primary: Option<NodeId>,
    replicas: Vec<NodeId>,  // Up to RF-1 replicas
    epoch: Epoch,           // Monotonic version number
}
```

---

## Part 5: Write Path

### 5.1 Write Classification

**Local Write** (this node is partition primary):
1. Apply write to local store
2. Replicate to replica nodes asynchronously
3. Return success immediately

**Remote Write** (another node is partition primary):
1. Forward write request to primary
2. Primary applies and replicates
3. Await response (or timeout)

### 5.2 Broadcast Entities

Special entities that must exist on ALL nodes (not just partition owners):

| Entity | Purpose |
|--------|---------|
| `_topic_index` | Maps topics to subscriber client IDs |
| `_wildcards` | Wildcard subscription trie |
| `_client_loc` | Maps client_id to connected node |

**Broadcast Write Flow**:
1. Apply locally FIRST (regardless of partition ownership)
2. Then forward to partition primary for official replication
3. Ensures every node has complete index for publish routing

### 5.3 write_or_forward() Logic

```rust
pub fn write_or_forward(&mut self, write: ReplicationWrite) {
    let partition = write.partition;

    // Broadcast entities - apply locally first
    let is_broadcast = write.entity == "_topic_index"
        || write.entity == "_wildcards"
        || write.entity == "_client_loc";
    if is_broadcast {
        self.stores.apply_write(&write);
    }

    // Route to partition owner
    if self.is_local_partition(partition) {
        // We're primary - replicate to replicas
        let replicas = self.partition_map.replicas(partition).to_vec();
        self.replicate_write_async(write, &replicas);
    } else if let Some(primary) = self.partition_map.primary(partition)
        .or_else(|| self.heartbeat.partition_map().primary(partition))
    {
        // Forward to primary (check heartbeat map as fallback)
        self.transport.send(primary, ClusterMessage::WriteRequest(write));
    } else {
        // No primary known - log warning
        tracing::warn!(?partition, "no primary found for partition");
    }
}
```

---

## Part 6: Entity Types

### 6.1 Broadcast Entities (Full Replication)

Must exist completely on every node:

| Entity | Key | Purpose |
|--------|-----|---------|
| `_topic_index` | topic string | Topic → subscribers mapping |
| `_wildcards` | pattern | Wildcard subscription trie |
| `_client_loc` | client_id | Client → connected node mapping |

### 6.2 Partitioned Entities

Follow normal partition assignment:

| Entity | Key | Purpose |
|--------|-----|---------|
| `_sessions` | client_id | Client session state |
| `_mqtt_subs` | client_id | Client subscription list |
| `_retained` | topic | Retained messages |
| `_qos_state` | message_id | QoS 1/2 delivery state |
| `_lwt` | client_id | Last Will and Testament |
| User entities | user-defined | Database records |

---

## Part 7: Cross-Node Pub/Sub

### 7.1 The Problem

Subscriber on Node B needs to receive messages published on Node A.

### 7.2 Solution: TopicIndex Broadcast

**Subscribe Flow** (Node B):
1. Client subscribes to `sensor/temp` on Node B
2. Node B creates TopicIndex entry: `sensor/temp → [client_id]`
3. TopicIndex is broadcast entity → applied locally on Node B
4. TopicIndex write replicated to all partitions
5. All nodes (including Node A) receive the TopicIndex entry

**Publish Flow** (Node A):
1. Client publishes to `sensor/temp` on Node A
2. Node A looks up `sensor/temp` in local TopicIndex
3. Finds subscriber client_id with home node = Node B
4. Node A forwards publish to Node B via `_mqdb/forward/{partition}`
5. Node B delivers to subscriber

### 7.3 Why TopicIndex Must Be Broadcast

If TopicIndex was partitioned:
- Node A might not have entries for topics whose partition is owned by Node B
- Node A couldn't route publishes to Node B's subscribers
- Cross-node pub/sub would fail

---

## Part 8: Raft Consensus

### 8.1 Raft Scope

Used for:
- Partition map management
- Node membership changes
- Leader election

NOT used for:
- Individual write replication (async, not consensus)
- Heartbeats (separate protocol)

### 8.2 Raft Commands

```rust
enum RaftCommand {
    AddNode { node_id: NodeId, address: String },
    RemoveNode { node_id: NodeId },
    UpdatePartition(PartitionUpdate),
}

struct PartitionUpdate {
    partition: u8,
    primary: u16,
    replica1: u16,
    replica2: u16,
    epoch: u32,
}
```

### 8.3 Single-Node Bootstrap

When first node starts:
1. Becomes Raft leader (single-node cluster)
2. Proposes UpdatePartition for all 64 partitions
3. Assigns itself as primary for all partitions
4. Waits for other nodes to join

---

## Part 9: Failure Handling

### 9.1 Node Death Detection

```rust
pub fn check_timeouts(&mut self, now: u64) -> Vec<NodeId> {
    let mut dead_nodes = Vec::new();

    for (&node_id, state) in &mut self.nodes {
        // Skip nodes we've never heard from
        if state.status == NodeStatus::Dead || state.status == NodeStatus::Unknown {
            continue;
        }

        let elapsed = now.saturating_sub(state.last_heartbeat);

        if elapsed > self.config.heartbeat_timeout_ms {
            state.status = NodeStatus::Dead;
            dead_nodes.push(NodeId::validated(node_id).unwrap());
        }
    }

    dead_nodes
}
```

### 9.2 Partition Failover

1. Primary dies (detected via heartbeat timeout)
2. Raft leader receives death notification
3. Raft leader proposes partition reassignment:
   - Replica becomes new primary
   - Another node becomes new replica (if available)
4. All nodes apply new partition map via Raft

---

## Part 10: Configuration

### 10.1 Single Node Start

```bash
mqdb cluster start \
    --node-id 1 \
    --bind 127.0.0.1:1883 \
    --db /var/lib/mqdb/node1
```

### 10.2 Join Existing Cluster

```bash
mqdb cluster start \
    --node-id 2 \
    --bind 127.0.0.1:1884 \
    --db /var/lib/mqdb/node2 \
    --peers "1@127.0.0.1:1883"
```

### 10.3 Bridge Configuration

```rust
BridgeConfig::new(format!("bridge-to-node-{}", peer_id), remote_addr)
    .add_topic("_mqdb/cluster/#", BridgeDirection::Both, QoS::AtLeastOnce)
    .add_topic("_mqdb/forward/#", BridgeDirection::Both, QoS::AtLeastOnce)
    .add_topic("_mqdb/repl/#", BridgeDirection::Both, QoS::AtLeastOnce)
    .add_topic("$DB/#", BridgeDirection::Both, QoS::ExactlyOnce)
```

---

## Part 11: Known Issues

### 11.1 Raft Leader Flapping (Single-Node)

**Status**: FIXED (Commit 8d554d6)

**Symptom**: Single-node Raft kept cycling between candidate and follower every ~500ms.

**Root Cause**: When starting an election, single-node clusters would send `RequestVote` to peers (none) and wait for responses. With no peers to respond, the node would timeout and start another election, never becoming leader.

**Fix Applied**: Added immediate quorum check in `start_election()` (`node.rs`). Single-node clusters now check `has_quorum()` right after voting for themselves and immediately become leader if quorum is satisfied (1 vote for 1-node cluster).

### 11.2 Partition Map Not Syncing to Joining Nodes

**Status**: FIXED (Session 46)

**Symptom**: Node 2 shows "no primary found for partition" for all partitions. New nodes kept starting elections (term storm up to term 200+).

**Root Cause**: `handle_node_alive()` added nodes to `cluster_members` but did NOT call `self.node.add_peer(node)`. Without being Raft peers, the leader never sent `AppendEntries` to new nodes.

**Fix Applied**: Added `self.node.add_peer(node)` in `raft/coordinator.rs:handle_node_alive()`.

### 11.3 First Heartbeat Before Partition Init

**Status**: FIXED

**Symptom**: First heartbeat had empty primary_bitmap.

**Root Cause**: Heartbeat was sent before Raft applied partition assignments.

**Fix Applied**: Added `has_any_assignment()` check in `HeartbeatManager::should_send()` (`heartbeat.rs`). Heartbeats are now only sent after the partition map has at least one partition assigned to the local node.

### 11.4 Immediate Node Death Detection

**Status**: FIXED

**Symptom**: Node 2 immediately detects Node 1 as "dead" on startup.

**Cause**: Timeout check applied to nodes with `last_heartbeat = 0`.

**Fix Applied**: Skip `Unknown` status nodes in timeout check (`heartbeat.rs`).

### 11.5 Multi-Node Raft Election Flapping (Batch Flush)

**Status**: FIXED (Session 47)

**Symptom**: 2-node and 3-node clusters experienced repeated death/revival cycles. Nodes marked each other as dead every 15-30 seconds despite being healthy.

**Root Cause**: Synchronous disk I/O in Raft storage. When a follower received `AppendEntries` with 64 partition updates, each log entry was persisted with a separate `flush()` call. 64 flushes took 20+ seconds, blocking the async event loop and causing heartbeat timeouts.

**Code path**:
1. `RaftNode::handle_append_entries()` loops over entries calling `persist_log_entry()`
2. `persist_log_entry()` calls `RaftStorage::append_log_entry()`
3. `append_log_entry()` does `backend.insert()` + `backend.flush()` per entry
4. 64 entries × 1 flush each = event loop blocked for 20+ seconds

**Fix Applied**:
1. Added `RaftStorage::append_log_entries_batch()` that uses batch writes with single flush (`storage.rs:76-88`)
2. Added `RaftNode::persist_log_entries()` helper (`node.rs:161-165`)
3. Changed `handle_append_entries()` to call batch method instead of loop (`node.rs:360`)

**Result**: Partition updates now process in <1ms instead of 20+ seconds. Both 2-node and 3-node clusters stable with zero death events.

### 11.6 Pub/Sub from Follower Nodes Unreliable

**Status**: FIXED (Session 46)

**Symptom**: Publishing from follower nodes to subscribers on other nodes was unreliable.

**Root Cause**: New nodes joining the cluster weren't added as Raft peers, so they never received partition map updates.

**Fix Applied**: Added `self.node.add_peer(node)` in `raft/coordinator.rs:handle_node_alive()`.

### 11.7 Full Mesh Topology with Multiple Peers

**Status**: FIXED (Session 48)

**Symptom**: When starting a node with multiple `--peers` (e.g., `--peers "1@...,2@..."`), connections failed with "Invalid packet type: 0" errors.

**Root Cause**: Fire-and-forget `tokio::spawn()` in transport overwhelmed the MQTT client when establishing multiple peer connections concurrently, causing packet boundary corruption.

**Fix Applied**: Full async refactor of ClusterTransport using RPITIT (Return Position Impl Trait in Trait). All transport operations now properly await instead of spawning fire-and-forget tasks.

### 11.8 3-Node Cross-Node Pub/Sub Routing Failures

**Status**: FIXED

**Symptom**: Messages published on Node 1 weren't reaching subscribers on Node 3 (indirect routing).

**Root Cause**: TopicIndex only stored partition, not the actual node where client is connected.

**Fix Applied**: Added ClientLocationStore (`_client_loc` entity) as a broadcast entity that tracks client_id → connected_node mapping.

### 11.9 Pub/Sub Tests Fail on Run 2+

**Status**: FIXED

**Symptom**: Cross-node pub/sub worked on first run but failed on subsequent runs with same client IDs.

**Root Cause**: ForwardedPublish deduplication used (origin, topic, payload) fingerprint, causing legitimate repeated messages to be dropped.

**Fix Applied**: Added `timestamp_ms` field to ForwardedPublish (VERSION 2) for proper deduplication.

### 11.10 Auto-Rebalance Race Condition

**Status**: FIXED

**Symptom**: When Node 3 joined a cluster, it sometimes received zero partitions even though auto-rebalancing should assign ~21 partitions (64/3).

**Root Cause**: Race condition in `handle_node_alive()`. When a new node joined, `trigger_rebalance()` was called which proposed multiple `AssignPartition` commands via Raft. However, `compute_balanced_assignments()` was called based on pre-rebalance state, and if multiple nodes joined quickly, the partition counts became inconsistent.

**Fix Applied**: Added `pending_partition_proposals` counter in `RaftCoordinator` to track in-flight partition assignments. `trigger_rebalance()` now waits for pending proposals to be applied before computing new assignments.

---

## Part 12: Session Management

### 12.1 SessionData Structure

`src/cluster/session.rs:7-32`:

```rust
#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct SessionData {
    pub version: u8,
    pub client_id_len: u8,
    pub client_id: Vec<u8>,
    pub session_partition: u16,
    pub clean_session: u8,
    pub connected: u8,               // 1=connected, 0=disconnected
    pub connected_node: u16,         // CRITICAL: NodeId where client is connected
    pub last_seen: u64,
    pub session_expiry_interval: u32,
    pub mqtt_sub_version: u64,
    pub has_will: u8,
    pub lwt_published: u8,
    pub lwt_token_present: u8,
    pub lwt_token: [u8; 16],
    pub will_qos: u8,
    pub will_retain: u8,
    pub will_topic_len: u16,
    pub will_topic: Vec<u8>,
    pub will_payload_len: u32,
    pub will_payload: Vec<u8>,
}
```

**Key Field**: `connected_node: u16` - determines which node receives forwarded publishes.

### 12.2 Session Partition Mapping

`src/cluster/session.rs:177-191`:

```rust
pub fn session_partition(client_id: &str) -> PartitionId {
    let hash = crc32fast::hash(client_id.as_bytes());
    let partition_num = (hash % u32::from(NUM_PARTITIONS)) as u16;
    PartitionId::new(partition_num).unwrap()
}
```

All session state is stored on the partition derived from `client_id`.

### 12.3 Session Creation Flow

`src/cluster/event_handler.rs:43-118`:

1. `on_client_connect()` triggered by MQTT broker
2. If `clean_start=true`: Clear old session and subscriptions
3. Call `stores.create_session_replicated(client_id)`
4. Session created with `connected=1`, `connected_node=this_node`
5. Store will topic if provided
6. ReplicationWrite sent to partition primary

### 12.4 Session Replication

`src/cluster/store_manager.rs:621-640`:

```rust
pub fn create_session_replicated(
    &self,
    client_id: &str,
) -> Result<(SessionData, ReplicationWrite), SessionError> {
    let (session, data) = self.sessions.create_session_with_data(client_id)?;
    let partition = session_partition(client_id);
    let write = ReplicationWrite::new(
        partition,
        Operation::Insert,
        Epoch::ZERO,
        0,
        entity::SESSIONS.to_string(),
        client_id.to_string(),
        data,
    );
    Ok((session, write))
}
```

### 12.5 Session Migration (Reconnection to Different Node)

When client reconnects to Node B (was on Node A):

1. Old disconnect sets `connected=false` on Node A
2. New connect creates/reuses session with `connected=true`, `connected_node=NodeB`
3. Session replicated to partition primary
4. Future publishes route to Node B based on `connected_node` field

---

## Part 13: Publish Routing Flow

### 13.1 Entry Point

`src/cluster/event_handler.rs:451-583`:

```rust
fn on_client_publish(&self, event: ClientPublishEvent) {
    // Filter internal clients/topics
    if event.client_id.starts_with("mqdb-forward-") { return; }
    if event.topic.starts_with("_mqdb/") { return; }
    // ... routing logic
}
```

### 13.2 TopicIndex Lookup

`src/cluster/topic_index.rs:190-198`:

```rust
pub fn get_subscribers(&self, topic: &str) -> Vec<SubscriberLocation> {
    self.entries
        .read()
        .unwrap()
        .get(topic)
        .map(|e| e.subscribers.clone())
        .unwrap_or_default()
}
```

**SubscriberLocation** contains:
- `client_id: Vec<u8>`
- `client_partition: u16` - partition where client session lives
- `qos: u8`

### 13.3 Local vs Remote Decision

`src/cluster/event_handler.rs:508-534`:

```rust
for target in route.targets {
    // ClientLocationStore is checked FIRST (preferred source)
    // Falls back to session if not found
    let connected_node = ctrl
        .stores()
        .client_locations
        .get(&target.client_id)
        .or_else(|| {
            ctrl.stores()
                .sessions
                .get(&target.client_id)
                .filter(|s| s.connected == 1)
                .and_then(|s| NodeId::validated(s.connected_node))
        });

    if let Some(target_node) = connected_node
        && target_node != node_id
    {
        // Remote subscriber - add to forward list
        remote_nodes.entry(target_node).or_default()
            .push(ForwardTarget::new(target.client_id, target.qos));
    }
}
```

**Note**: `ClientLocationStore` is the primary lookup source for client→node mapping. Session `connected_node` is the fallback.

### 13.4 ForwardedPublish Sending

`src/cluster/event_handler.rs:547-562`:

```rust
for (target_node, targets) in remote_nodes {
    let fwd = ForwardedPublish::new(
        node_id,      // origin
        topic.clone(),
        qos,
        retain,
        payload.clone(),
        targets,      // Vec<ForwardTarget>
    );
    transport.send_async(target_node, ClusterMessage::ForwardedPublish(fwd)).await;
}
```

### 13.5 Remote Node Receives ForwardedPublish

`src/cluster/node_controller.rs:1217-1251`:

```rust
fn handle_forwarded_publish(&mut self, from: NodeId, fwd: &ForwardedPublish) {
    // Deduplication check (LRU cache, 1000 capacity)
    let fingerprint = Self::forward_fingerprint(fwd);
    if self.forward_dedup.contains(&fingerprint) {
        return; // Discard duplicate
    }
    self.forward_dedup.insert(fingerprint);

    // Deliver via forward_client (prevents re-routing)
    self.transport.queue_local_publish(fwd.topic.clone(), fwd.payload.clone(), fwd.qos);
}
```

**Deduplication fingerprint** = hash of (origin_node, timestamp_ms, topic, payload)

### 13.6 Final Delivery

`src/cluster/mqtt_transport.rs:588-599`:

Uses `forward_client` (client ID: `mqdb-forward-{node_id}`) to publish locally.
This client ID is filtered in `on_client_publish()` to prevent infinite forwarding.

---

## Part 14: Wildcard Subscriptions

### 14.1 TopicTrie Structure

`src/cluster/topic_trie.rs:42-60`:

```rust
struct TrieNode {
    children: HashMap<String, TrieNode>,  // Exact segment matches
    single_wildcard: Option<Box<TrieNode>>, // + wildcard branch
    multi_wildcard: Option<Box<TrieNode>>,  // # wildcard branch
    subscribers: Vec<WildcardSubscriber>,
}

pub struct TopicTrie {
    root: TrieNode,
    pattern_count: usize,
}
```

### 14.2 Wildcard Pattern Rules

`src/cluster/topic_trie.rs:257-275`:

- `+` matches exactly one topic level
- `#` matches zero or more levels (must be last segment)
- System topics (`$...`) never matched by wildcards
- Invalid: `sensors/#/temp` (# not at end)

### 14.3 Matching Algorithm

`src/cluster/topic_trie.rs:153-177`:

```rust
fn match_recursive(node: &TrieNode, levels: &[&str], depth: usize, matches: &mut Vec) {
    // Multi-wildcard (#) matches all remaining levels
    if let Some(ref multi) = node.multi_wildcard {
        matches.extend(multi.subscribers.iter());
    }

    if depth == levels.len() {
        matches.extend(node.subscribers.iter()); // Exact match
        return;
    }

    let level = levels[depth];

    // Exact segment match
    if let Some(child) = node.children.get(level) {
        Self::match_recursive(child, levels, depth + 1, matches);
    }

    // Single wildcard (+) matches one level
    if let Some(ref single) = node.single_wildcard {
        Self::match_recursive(single, levels, depth + 1, matches);
    }
}
```

### 14.4 WildcardBroadcast Message

Message type code: **60**

`src/cluster/protocol.rs:1128-1231`:

```rust
pub struct WildcardBroadcast {
    version: u8,
    operation: u8,        // 0=Subscribe, 1=Unsubscribe
    pattern_len: u16,
    pattern: Vec<u8>,
    client_id_len: u8,
    client_id: Vec<u8>,
    client_partition: u16,
    qos: u8,
    subscription_type: u8,
}
```

### 14.5 Broadcast Flow

On wildcard subscribe (`src/cluster/event_handler.rs:299-323`):

1. Store locally in WildcardStore
2. Create `WildcardBroadcast::subscribe(...)` message
3. `transport.broadcast(msg)` to ALL nodes
4. Each node applies to local trie

---

## Part 15: QoS State Management

### 15.1 QoS 1 Inflight Store

`src/cluster/inflight_store.rs:7-23`:

```rust
pub struct InflightMessage {
    pub version: u8,            // Serialization version
    pub packet_id: u16,
    pub qos: u8,
    pub attempts: u8,           // Retry count
    pub last_attempt: u64,      // Timestamp ms
    pub client_id: Vec<u8>,
    pub topic: Vec<u8>,
    pub payload: Vec<u8>,
}
```

Key format: `_mqtt_inflight/p{partition}/clients/{client_id}/pending/{packet_id}`

### 15.2 QoS 2 State Machine

`src/cluster/qos2_store.rs:8-20`:

```
Inbound:  PubrecSent(1) → PubrelReceived(2) → PubcompSent(3)
Outbound: PublishSent(1) → PubrelSent(2)
```

```rust
pub struct Qos2State {
    pub version: u8,   // Serialization version
    direction: u8,     // 0=Inbound, 1=Outbound
    state: u8,         // Phase number
    packet_id: u16,
    client_id: Vec<u8>,
    topic: Vec<u8>,
    payload: Vec<u8>,
    created_at: u64,
}
```

### 15.3 Retry Mechanism

`src/cluster/inflight_store.rs:77-82`:

```rust
// Exponential backoff: base * 2^attempts
// Base: 1000ms, Max: 300,000ms (5 minutes)
pub fn backoff_ms(&self) -> u64 {
    let base: u64 = 1000;
    let exp = self.attempts.min(10) as u32;
    (base * 2u64.pow(exp)).min(300_000)
}
```

Default `max_attempts`: 10

### 15.4 ACK Handling

`src/cluster/event_handler.rs:626-668`:

- QoS 1 PUBACK: `acknowledge_inflight_replicated(client_id, packet_id)`
- QoS 2 PUBCOMP: `complete_qos2_replicated(client_id, packet_id)`

Both create ReplicationWrite with `Operation::Delete` to remove state.

---

## Part 16: Retained Messages

### 16.1 RetainedMessage Structure

`src/cluster/retained_store.rs:36-47`:

```rust
pub struct RetainedMessage {
    pub version: u8,
    pub topic_len: u16,
    pub topic: Vec<u8>,
    pub qos: u8,
    pub timestamp_ms: u64,
    pub payload_len: u32,
    pub payload: Vec<u8>,
}
```

### 16.2 Retain on Publish

`src/cluster/store_manager.rs:827-853`:

- Create RetainedMessage
- Empty payload = DELETE operation (clears retained)
- Non-empty payload = INSERT operation
- Partition = `topic_partition(topic)`
- ReplicationWrite sent to partition primary

### 16.3 Deliver on Subscribe

`src/cluster/event_handler.rs:286-291`:

1. Check local store: `query_local_retained_exact(topic)`
2. If not local: `start_async_retained_query(topic)` to partition primary
3. Receive retained messages via QueryResponse
4. Deliver via `queue_local_publish_retained()` with retain=true

### 16.4 Clear Retained

Empty payload publish with retain=true:
- `set_with_data()` detects empty payload
- Removes from HashMap
- ReplicationWrite with `Operation::Delete`

---

## Part 17: Snapshot Transfer

### 17.1 Trigger

`src/cluster/node_controller.rs:1412-1428`:

Snapshots triggered during **partition migration**:
- New primary requests snapshot from old primary
- Called in `start_partition_migration()`

### 17.2 Request/Response Flow

```
1. New Primary → SnapshotRequest → Old Primary
2. Old Primary → [SnapshotChunk × N] → New Primary
3. Old Primary → SnapshotComplete → New Primary
```

### 17.3 Chunk Size

`src/cluster/snapshot.rs:290`:

```rust
pub const DEFAULT_CHUNK_SIZE: usize = 64 * 1024; // 64 KB
```

### 17.4 State Rebuild

`src/cluster/node_controller.rs:1301-1371`:

1. SnapshotBuilder collects chunks (can arrive out-of-order)
2. Once all chunks received, `assemble()` concatenates in order
3. `stores.import_partition(data)` imports into 9 store types
4. SnapshotComplete sent with final sequence number

### 17.5 Stores Imported

- sessions, qos2, subscriptions, retained, topics
- wildcards, inflight, offsets, idempotency

---

## Part 18: Cluster Startup Sequence

### 18.1 CLI Entry

`src/bin/mqdb.rs:371-395`:

```bash
mqdb cluster start --node-id 1 --bind 127.0.0.1:1883 --db /tmp/mqdb --peers "2@127.0.0.1:1884"
```

Peer format: `{node_id}@{address}:{port}` (comma-separated for multiple)

### 18.2 ClusteredAgent Initialization

`src/cluster_agent.rs:131-170`:

```
1. Validate NodeId (1-65535)
2. Create MqttTransport
3. Create NodeController
4. Open Raft storage backend (FjallBackend)
5. Create RaftCoordinator with persisted state
6. Create shutdown channel
7. Wrap in Arc<RwLock<>>
```

### 18.3 Run Sequence

`src/cluster_agent.rs:206-530`:

```
1. Create ClusterEventHandler
2. Configure BrokerConfig with bridges
3. Spawn MQTT broker task
4. Wait for broker ready signal
5. Transport connects to local broker (127.0.0.1:{port})
   - Subscribes to: _mqdb/cluster/*, _mqdb/repl/*, _mqdb/forward/*
6. Register peers with Controller and Raft
7. Start main event loop:
   - 100ms: Controller tick, Raft tick, heartbeats
   - 60s: Wildcard reconciliation
   - 300s: Subscription reconciliation
   - 3600s: Cleanup expired sessions/idempotency
```

### 18.4 Bridge Configuration

`src/cluster_agent.rs:172-201`:

```rust
BridgeConfig::new(&bridge_name, &peer.address)
    .add_topic("_mqdb/cluster/#", BridgeDirection::Both, QoS::AtLeastOnce)
    .add_topic("_mqdb/forward/#", BridgeDirection::Both, QoS::AtLeastOnce)
    .add_topic("_mqdb/repl/#", BridgeDirection::Both, QoS::AtLeastOnce)
    .add_topic("$DB/#", BridgeDirection::Both, QoS::ExactlyOnce)
```

Bridge settings:
- `clean_start = false` (preserve session across reconnects)
- QUIC: `StreamStrategy::DataPerTopic`, 256 max streams, datagram support
- `fallback_tcp = true` for testing

### 18.5 Raft Leader Partition Init

`src/cluster_agent.rs:396-421`:

When Raft leader and partitions not initialized:
1. Calculate primary for each partition: `partition % node_count`
2. Calculate replica: `(primary_idx + 1) % node_count`
3. Propose `RaftCommand::update_partition()` for all 64 partitions

---

## DOCUMENTED ELSEWHERE

The following are implemented but documented in other files:

- **LWT (Last Will Testament)**: Implemented in M9, see `IMPLEMENTATION_PLAN.md`
- **Subscription cache reconciliation**: `SubscriptionCache::reconcile()` runs every 5 minutes
- **Database operations ($DB/#)**: See README.md "MQTT Topic Structure" section
- **Backup and restore**: See README.md "Admin Operations" section

## FUTURE DOCUMENTATION

The following sections may need documentation:

- Performance tuning and benchmarks
- Security model and authentication
- Monitoring and observability
- Operational runbooks
- Capacity planning guidelines
- Network topology considerations
- Split-brain prevention details
- Graceful shutdown procedures
- Rolling upgrade process
- Multi-datacenter deployment

---

## Appendix A: Key Source Files

| File | Purpose |
|------|---------|
| `src/cluster/node_controller.rs` | Main cluster controller, message handling |
| `src/cluster/heartbeat.rs` | Heartbeat management, node status |
| `src/cluster/partition_map.rs` | Partition assignments |
| `src/cluster/mqtt_transport.rs` | MQTT-based cluster transport |
| `src/cluster/raft/coordinator.rs` | Raft consensus coordinator |
| `src/cluster/raft/node.rs` | Raft node state machine |
| `src/cluster/raft/state.rs` | Raft state and commands |
| `src/cluster/topic_index.rs` | Topic → subscriber mapping |
| `src/cluster/topic_trie.rs` | Wildcard subscription trie |
| `src/cluster/wildcard_store.rs` | Wildcard store wrapper |
| `src/cluster/client_location.rs` | Client → connected node mapping |
| `src/cluster/event_handler.rs` | MQTT event hooks for cluster |
| `src/cluster/session.rs` | SessionData structure and store |
| `src/cluster/store_manager.rs` | All entity stores coordination |
| `src/cluster/protocol.rs` | Message definitions (BeBytes) |
| `src/cluster/snapshot.rs` | Snapshot transfer protocol |
| `src/cluster/inflight_store.rs` | QoS 1 inflight tracking |
| `src/cluster/qos2_store.rs` | QoS 2 state machine |
| `src/cluster/retained_store.rs` | Retained message store |
| `src/cluster/entity.rs` | Entity name constants |
| `src/cluster/node.rs` | NodeId type |
| `src/cluster/partition.rs` | PartitionId type, NUM_PARTITIONS |
| `src/cluster/epoch.rs` | Epoch type |
| `src/cluster_agent.rs` | Cluster node startup |
| `src/bin/mqdb.rs` | CLI entry point |

---

## Appendix B: Document Maintenance

This document should be updated with every significant cluster change.
All claims must have file:line references for verification.

**Verification checklist before each section:**
1. Read the actual source file
2. Confirm struct fields match
3. Confirm function signatures match
4. Confirm constants match
5. Update line numbers if code changed
