# MQDB Distributed Implementation Plan

> **WARNING: INCOMPLETE RECONSTRUCTION**
> This document was reconstructed from conversation context after data loss.

## Milestone Overview

| Milestone | Description | Status |
|-----------|-------------|--------|
| M1 | Core Cluster Primitives | Done |
| M2 | Replication Pipeline | Done |
| M3 | Transport Layer (MQTT/QUIC) | Done |
| M4 | Raft Consensus | Done |
| M5 | DB Integration | Done |
| M6 | Topic Index & Subscription Routing | Done |
| M7 | Wildcard Subscriptions | Done |
| M8 | QoS State Replication | Not Started |
| M9 | Last Will & Testament | Not Started |
| M10 | Session Migration & Cleanup | Not Started |

---

## M1: Core Cluster Primitives (DONE)

### Components
- `NodeId` - Node identifier (u16)
- `PartitionId` - Partition identifier (0-63)
- `Epoch` - Monotonic version for partition assignments
- `PartitionMap` - Mapping of partitions to primaries/replicas
- `PartitionAssignment` - Per-partition ownership info

### Files
- `src/cluster/mod.rs`
- `src/cluster/partition_map.rs`

---

## M2: Replication Pipeline (DONE)

### Components
- `ReplicationWrite` - Write operation to replicate
- `ReplicationAck` - Acknowledgment from replica
- `StoreManager` - Manages all replicated stores
- Write operation types: Insert, Update, Delete

### Files
- `src/cluster/protocol.rs`
- `src/cluster/store_manager.rs`

---

## M3: Transport Layer (DONE)

### Components
- `ClusterTransport` trait - Abstract transport interface
- `MqttTransport` - MQTT-based implementation
- Message serialization via BeBytes
- Cluster topics for different message types

### Files
- `src/cluster/transport.rs`
- `src/cluster/mqtt_transport.rs`

---

## M4: Raft Consensus (DONE)

### Components
- `RaftCoordinator` - Manages Raft state machine
- `RaftCommand` - Commands for cluster changes
- Leader election
- Log replication
- Partition map updates via Raft

### Files
- `src/cluster/raft/mod.rs`
- `src/cluster/raft/coordinator.rs`

---

## M5: DB Integration (DONE)

### Components
- DB entity CRUD operations
- Schema, Index, Unique, FK stores
- Request/response via MQTT 5.0
- DB partitioning by entity key

### Files
- `src/cluster/db.rs`
- `src/cluster/db_handler.rs`

---

## M6: Topic Index & Subscription Routing (DONE)

### Goal
Enable cross-node pub/sub - subscriber on node B receives messages published on node A.

### Requirements
1. TopicIndex must exist on ALL nodes (broadcast entity)
2. Every node needs complete topic→subscriber mapping
3. Publish routing uses local TopicIndex lookup

### Implementation Tasks

#### 6.1 TopicIndex Broadcast (DONE)
- [x] TopicIndex and Wildcards are broadcast entities
- [x] Apply locally BEFORE forwarding to partition primary
- [x] Idempotent apply (safe for duplicate writes)

#### 6.2 Heartbeat Timeout Fix (DONE)
- [x] Skip `Unknown` status nodes in timeout check
- [x] Only timeout nodes after receiving at least one heartbeat

#### 6.3 Partition Map Sync via Raft (DONE)
- [x] Raft coordinator adds new nodes as peers in `handle_node_alive()`
- [x] Partition updates replicated via Raft `AppendEntries`
- [x] Node controller applies PartitionUpdate commands from Raft

#### 6.4 Cross-Node Message Routing (DONE)
- [x] Publish routing looks up subscribers via TopicIndex
- [x] Session's `connected_node` field tracks which node client is on
- [x] ForwardedPublish messages sent to remote nodes for delivery

### Verification
Cross-node pub/sub tested and working:
```bash
# Subscriber on Node 2 (port 1884)
mosquitto_sub -p 1884 -t "crosstest/#" -i subscriber123

# Publisher on Node 1 (port 1883)
mosquitto_pub -p 1883 -t "crosstest/hello" -m "message" -i publisher456

# Result: Subscriber receives message ✓
```

### Key Fix: Raft Peer Discovery
The critical fix was in `src/cluster/raft/coordinator.rs:handle_node_alive()`:
```rust
let is_new_member = !self.cluster_members.contains(&node);
if is_new_member {
    self.cluster_members.push(node);
    self.node.add_peer(node);  // THIS WAS MISSING
    tracing::info!(?node, "added new node as Raft peer");
}
```

Without `add_peer()`, the Raft leader never sent `AppendEntries` to new nodes, so they never received partition updates and kept trying to start elections.

---

## M7: Wildcard Subscriptions (DONE)

### Goal
Support wildcard patterns (+, #) across cluster.

### Requirements
1. Wildcards store must be broadcast entity ✅
2. Publish routing must check both exact and wildcard matches ✅
3. TopicTrie must be consistent across all nodes ✅

### Implementation

#### 7.1 TopicTrie Data Structure (DONE)
- [x] Efficient trie for pattern matching (`+` single-level, `#` multi-level)
- [x] Pattern validation (# at end, no mixed wildcards)
- [x] System topic protection ($SYS/* blocked)
- [x] File: `src/cluster/topic_trie.rs`

#### 7.2 WildcardStore Broadcast (DONE)
- [x] WildcardStore wraps TopicTrie with serialization
- [x] WildcardBroadcast message for cluster propagation
- [x] Subscribe/unsubscribe broadcast to all nodes
- [x] Files: `src/cluster/wildcard_store.rs`, `src/cluster/protocol.rs`

#### 7.3 Publish Routing Integration (DONE)
- [x] PublishRouter.route_with_wildcards() combines exact + wildcard matches
- [x] Deduplication by client_id with max QoS
- [x] Cross-node forwarding based on client partition
- [x] File: `src/cluster/publish_router.rs`

#### 7.4 Event Handler Integration (DONE)
- [x] on_client_subscribe() broadcasts wildcard subscriptions
- [x] on_client_publish() queries both TopicIndex and WildcardStore
- [x] File: `src/cluster/event_handler.rs`

### Tests
- `pubsub_wildcard_routing` - single-node routing logic
- `wildcard_deduplication_max_qos` - same client on multiple patterns
- `wildcard_broadcast_replication` - 2-node broadcast propagation
- `cross_node_wildcard_publish_routing` - end-to-end cross-node routing
- `test_wildcard_plus_subscription` - MQTT protocol + wildcard
- `test_wildcard_hash_subscription` - MQTT protocol # wildcard

### Known Limitations
- Retained messages for wildcard subscriptions handled locally only (no cross-node query)
- Failed wildcard broadcasts tracked but not automatically retried

---

## M8: QoS State Replication (NOT STARTED)

### Goal
Replicate QoS 1/2 delivery state for session continuity.

### Requirements
1. Pending publishes tracked per partition
2. PUBACK/PUBREC/PUBREL/PUBCOMP state replicated
3. Retry on primary, failover to replica

---

## M9: Last Will & Testament (NOT STARTED)

### Goal
Deliver LWT messages when client disconnects unexpectedly.

### Requirements
1. LWT stored on client's session partition
2. On disconnect, trigger LWT publish
3. LWT publish routed through normal pub/sub path

---

## M10: Session Migration & Cleanup (NOT STARTED)

### Goal
Handle session migration on node failure and cleanup expired sessions.

### Requirements
1. Session state on failed node's partitions migrated to new primary
2. Expired sessions cleaned up cluster-wide
3. Subscription cache reconciliation after failover

---

## Testing Strategy

### Unit Tests
- Each component tested in isolation
- Mock transport for cluster tests

### Integration Tests
- Multi-node cluster in single process
- Cross-node pub/sub verification
- Node failure simulation

### Manual Tests
- Real multi-process cluster
- Network partition simulation
- Performance benchmarks

---

## Dependencies

### mqtt-lib Fixes Needed

1. **PUBACK Timeout Fix** (was committed as `ac885a6`, may be lost)
   - Bridge forwarding must be non-blocking
   - Don't await PUBACK when forwarding to remote broker
   - File: `mqtt5/src/broker/bridge/connection.rs`

```rust
// In handle_remote_message():
// Change from:
let _ = local_client.publish_qos(...).await;  // BLOCKS!

// To:
let client = local_client.clone();
tokio::spawn(async move {
    let _ = client.publish_qos(...).await;
});
```
