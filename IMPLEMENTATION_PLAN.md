# MQDB Distributed Implementation Plan

> **Status: ALL MILESTONES COMPLETE (M1-M11)**
> Last verified: March 2026

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
| M8 | QoS State Replication | Done |
| M9 | Last Will & Testament | Done |
| M10 | Session Migration & Cleanup | Done |
| M11 | Vault Transparent Encryption | Done |

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

## M8: QoS State Replication (DONE)

### Goal
Replicate QoS 1/2 delivery state for session continuity.

### Requirements
1. Pending publishes tracked per partition ✅
2. PUBACK/PUBREC/PUBREL/PUBCOMP state replicated ✅
3. Retry on primary, failover to replica ✅

### Implementation

#### 8.1 Qos2Store Replication (DONE)
- [x] `start_inbound_replicated()` - creates state and returns ReplicationWrite
- [x] `start_outbound_replicated()` - creates state and returns ReplicationWrite
- [x] `advance_replicated()` - advances state and returns ReplicationWrite
- [x] `complete_replicated()` - completes and returns Delete ReplicationWrite
- [x] `clear_client_with_data()` - clears all client state, returns data for replication
- [x] `apply_replicated()` - applies Insert/Update/Delete from replica
- [x] File: `src/cluster/qos2_store.rs`

#### 8.2 InflightStore Replication (DONE)
- [x] `add_replicated()` - adds inflight message and returns ReplicationWrite
- [x] `acknowledge_replicated()` - acknowledges and returns Delete ReplicationWrite
- [x] `clear_client_with_data()` - clears all client state, returns data for replication
- [x] `apply_replicated()` - applies Insert/Delete from replica
- [x] File: `src/cluster/inflight_store.rs`

#### 8.3 StoreManager Integration (DONE)
- [x] `start_qos2_inbound_replicated()`, `start_qos2_outbound_replicated()`
- [x] `advance_qos2_replicated()`, `complete_qos2_replicated()`
- [x] `clear_qos2_client_replicated()` - generates Delete writes for all client QoS2 state
- [x] `add_inflight_replicated()`, `acknowledge_inflight_replicated()`
- [x] `clear_inflight_client_replicated()` - generates Delete writes for all client inflight
- [x] File: `src/cluster/store_manager.rs`

#### 8.4 Disconnect Cleanup (DONE)
- [x] `on_client_disconnect()` uses replicated cleanup methods
- [x] Delete operations forwarded to partition primary for replication
- [x] File: `src/cluster/event_handler.rs`

### Tests
- `qos2_state_survives_primary_failover` - QoS2 state restored on new primary
- `qos2_cleanup_replicates_delete_to_replica` - disconnect cleanup replicated

### Known Limitations
- No TTL/timeout for orphan QoS state (client never reconnects)
- Retry timing not replicated (local timer only)

---

## M9: Last Will & Testament (DONE)

### Goal
Deliver LWT messages when client disconnects unexpectedly.

### Requirements
1. LWT stored on client's session partition ✅
2. On disconnect, trigger LWT publish ✅
3. LWT publish routed through normal pub/sub path ✅

### Implementation

#### 9.1 SessionData LWT Fields (DONE)
- [x] `has_will`, `will_qos`, `will_retain`, `will_topic`, `will_payload`
- [x] `lwt_published` flag to prevent duplicate delivery
- [x] `lwt_token_present`, `lwt_token` for idempotent publishing
- [x] File: `src/cluster/session.rs`

#### 9.2 LwtPublisher API (DONE)
- [x] `prepare_lwt(client_id)` - sets token, returns `LwtPrepared` with topic/payload/qos/retain
- [x] `complete_lwt(client_id, token)` - validates token, marks `lwt_published = true`
- [x] `recover_pending_lwts()` - finds sessions with pending LWT (token set but not published)
- [x] Token-based idempotency prevents duplicate LWT delivery on retry
- [x] File: `src/cluster/lwt.rs`

#### 9.3 Event Handler Integration (DONE)
- [x] `on_client_disconnect()` calls `prepare_lwt()` for unexpected disconnects
- [x] LWT routed via TopicIndex + WildcardStore lookup
- [x] Cross-node delivery via ForwardedPublish messages
- [x] `complete_lwt()` called after routing completes
- [x] File: `src/cluster/event_handler.rs`

#### 9.4 Startup Recovery (DONE)
- [x] `recover_pending_lwts()` called during cluster agent startup
- [x] Pending LWTs from previous crash are re-routed and marked complete
- [x] File: `src/cluster_agent.rs`

### Tests
- `lwt_triggered_on_death` - single-node LWT prepare/complete flow
- `lwt_published_exactly_once` - idempotency with token validation
- `cross_node_lwt_routing` - LWT forwarded to subscriber on remote node
- `mqtt_lwt_death_detection` - end-to-end LWT delivery via MQTT protocol

### Known Limitations
- LWT cleanup on clean disconnect not tested

---

## M10: Session Migration & Cleanup (DONE)

### Goal
Handle session migration on node failure and cleanup expired sessions.

### Requirements
1. Session state on failed node's partitions migrated to new primary
2. Expired sessions cleaned up cluster-wide
3. Subscription cache reconciliation after failover

### Implementation

**Session cleanup on expiry** (`cluster_agent.rs`):
- `cleanup_expired_sessions()` returns expired sessions
- `clear_expired_session_subscriptions()` cleans up subscriptions for each expired session
- Removes entries from TopicIndex, WildcardStore, SubscriptionCache
- Clears QoS2 and Inflight state for expired clients
- Replicates all cleanup operations

**Subscription reconciliation on partition takeover**:
- Track `became_primary` flag during partition map updates
- Trigger immediate `reconcile()` when becoming primary for any partition
- Ensures subscription cache matches actual topic subscriptions

**Session disconnection on node death**:
- Added `sessions_on_node(node_id)` method to SessionStore
- Dead node detection marks sessions as disconnected
- Updates replicated to ensure consistent session state

### Testing
- `session_expiry_cleans_subscriptions` test in `cluster_integration_test.rs`

---

## M11: Vault Transparent Encryption (DONE)

### Goal
Per-user transparent encryption at rest for owned entities, in both agent and cluster modes.

### Requirements
1. Users derive AES-256-GCM key from passphrase via PBKDF2 (600k iterations)
2. Vault enable encrypts all existing owned records; disable decrypts them
3. Unlocked vault transparently encrypts on write and decrypts on read
4. Locked vault returns raw ciphertext (proving data encrypted at rest)
5. Works across cluster nodes — scatter-gather list decrypts results from all partitions
6. Change passphrase re-encrypts all records atomically
7. All string leaf values encrypted at any JSON depth (nested objects, arrays); `_`-prefixed keys skipped at all depths; `id` and ownership field skipped at top level only; non-string types (number, bool, null) stored as-is

### Implementation

**HTTP API** (`src/http/server.rs`, `src/http/handlers.rs`):
- Six endpoints: `/vault/enable`, `/vault/unlock`, `/vault/lock`, `/vault/change`, `/vault/disable`, `/vault/status`
- Cookie-based session authentication required
- Rate-limited unlock attempts

**Cryptography** (`src/http/vault_crypto.rs`):
- `VaultCrypto` struct wraps AES-256-GCM key derived via PBKDF2-HMAC-SHA256
- Per-record nonce derived from entity + record ID (deterministic, no nonce storage)
- Recursive field-level encryption: each string leaf value encrypted independently at any JSON depth

**Transform layer** (`src/vault_transform.rs`):
- `vault_encrypt_fields` / `vault_decrypt_fields` for record-level operations
- `is_vault_eligible` gates encryption to owned, non-system entities
- `build_vault_skip_fields` excludes `id` and ownership field from encryption

**Key storage** (`src/vault_keys.rs`):
- `VaultKeyStore` maps canonical_id → `VaultCrypto` (in-memory only)
- Keys never persisted — user must unlock after each server restart

**Cluster integration** (`src/http/handlers.rs`):
- `batch_vault_operation` iterates all owned records via `update_entity` MQTT round-trips
- Scatter-list results decrypted after aggregation from all partition owners
- Constraint validation operates on plaintext (decrypt before validate, re-encrypt after)

### Files
- `src/http/vault_crypto.rs`
- `src/http/handlers.rs`
- `src/vault_transform.rs`
- `src/vault_keys.rs`
- `examples/vault-mqtt/` (single-node demo)
- `examples/vault-cluster/` (multi-node E2E test, 70 tests)

### Testing
- Unit tests in `vault_crypto.rs` and `vault_transform.rs`
- E2E script `examples/vault-cluster/run.sh` (70 tests covering enable, CRUD, lock/unlock, cross-node, rebalance, change passphrase, disable)

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
