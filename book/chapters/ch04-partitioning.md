# Chapter 4: Partitioning

Part II begins here. Everything built in Part I — the storage engine, the protocol layer, the reactive subscriptions — now has to work across multiple nodes. The first problem is dividing the data.

A single node holds all 256 partitions, all records, all subscriptions. Cluster mode distributes those partitions across nodes. The question is how: which node owns which data, how does every node agree on the answer, and what happens when a node arrives or disappears.

## 4.1 Fixed Partitions vs. Consistent Hashing

The literature offers two main approaches to distributing data across nodes.

**Consistent hashing** maps both keys and nodes onto a hash ring. Each key is assigned to the nearest node clockwise on the ring. When a node joins or leaves, only the keys adjacent to it on the ring move. The advantage is incremental rebalancing — most keys stay put. The disadvantage is uneven distribution: with few nodes, the arcs between them vary widely in size, so some nodes carry much more data than others. Virtual nodes solve this by placing each physical node at multiple points on the ring. Dynamo, Cassandra, and Riak use variations of this approach.

**Fixed partitions** divide the keyspace into a predetermined number of buckets. Every key hashes to a bucket. Buckets are assigned to nodes. When a node joins, some buckets move to it. When a node leaves, its buckets move to survivors. The partition count never changes — what changes is the mapping from partition to node.

MQDB uses 256 fixed partitions.

The choice comes down to what stays constant. In consistent hashing, the ring is constant but the partition boundaries shift as nodes come and go. In fixed partitioning, the boundaries are constant but the assignments shift. Fixed boundaries mean every component in the system — the hash function, the heartbeat protocol, the replication pipeline, the Raft log — can be built around a known, constant number of partitions. The partition map is a fixed-size array, not a dynamically-sized ring. The heartbeat carries a 256-bit bitmap, not a variable-length list of token ranges. The rebalancer moves whole partitions between nodes; it never splits or merges them. This simplicity has a ceiling. With 256 partitions and a replication factor of 2, each partition requires two distinct nodes: one primary and one replica. That yields 512 role assignments. With 3 nodes, each handles roughly 85 primary partitions and 85 replica partitions — a comfortable distribution.

For the target deployment — clusters of 3 to 16 nodes — 256 partitions provide granularity fine enough for balanced distribution and coarse enough for bounded control plane overhead. Every heartbeat, every Raft proposal, every rebalancing calculation is O(256): constant regardless of the data size. Dynamic partition splitting (CockroachDB's ranges, DynamoDB's automatic splitting) would remove the ceiling, but the partition map, heartbeat protocol, and rebalancing logic would all grow with the data, not just the cluster size. That complexity buys scaling beyond hundreds of nodes, which is outside MQDB's design target.

The 256 number itself is a balance between granularity and overhead. Too few partitions (say, 16) and a 3-node cluster cannot distribute them evenly — one node always has one more than the others, but the percentage imbalance is large. Too many partitions (say, 4096) and the heartbeat bitmap grows to 512 bytes, the Raft log carries more partition updates on bootstrap, and the rebalancer has more work to do on every topology change. 256 fits in 32 bytes as a bitmap, bootstraps in a single Raft batch, and distributes evenly enough across clusters of 3 to 16 nodes.

## 4.2 The Partition Function

Every key in the system maps to a partition through CRC32:

```
partition = crc32(key_bytes) % 256
```

The key varies by entity type. For database records, the key is the entity name and record ID joined by a slash: `users/abc-123` hashes to a partition. For client sessions, the key is the client ID. For retained messages, the key is the topic string. For subscriptions, the key is the client ID (so all of a client's subscriptions live on the same partition as the client's session).

```rust
pub fn from_key(key: &str) -> Self {
    let hash = crc32fast::hash(key.as_bytes());
    Self(u16::try_from(hash % u32::from(NUM_PARTITIONS)).unwrap_or(0))
}

pub fn from_entity_id(entity: &str, id: &str) -> Self {
    let key = format!("{entity}/{id}");
    Self::from_key(&key)
}
```

CRC32 is not a cryptographic hash. It is not even the best non-cryptographic hash for distribution uniformity — murmur3 and xxhash both score better on avalanche tests. But CRC32 has three properties that matter here.

First, it is fast. The `crc32fast` crate uses hardware CRC32C instructions on x86 and ARM when available, falling back to a software implementation. On modern hardware, CRC32 of a short string completes in single-digit nanoseconds. Since every write, read, and forward decision starts with a partition lookup, the hash function is in the hot path.

Second, it is deterministic across platforms. CRC32 is a well-defined algorithm with a single correct output for any input. There are no platform-specific variations, no seed values to synchronize, no endianness concerns in the hash itself. Every node in the cluster computes the same partition for the same key without coordination.

Third, it is sufficient. The distribution needs to be uniform across 256 buckets, not across 2^32 possible values. Even a mediocre hash function distributes well when reduced modulo a small number. CRC32's known weakness — poor avalanche behavior for inputs that differ by only a few bits — does not matter when the inputs are entity names and UUIDs. Two records in the same entity with different UUIDs differ by many bytes, not by one bit. Empirically, CRC32 distributes MQDB's real workloads (UUID-keyed records, MQTT client IDs, topic strings) evenly across 256 partitions.

### Co-Location by Key Choice

The choice of hash key determines which data lands together. A client's session, subscriptions, QoS tracking, and inflight messages all hash by client ID, so they all land on the same partition. This is intentional: when a client disconnects, the disconnect handler needs to access the session, mark subscriptions for cleanup, and resolve pending QoS exchanges. If these lived on different partitions (potentially on different nodes), a single disconnect would require cross-partition coordination. By hashing all client data on client ID, a disconnect is a local operation on the partition primary.

This echoes the storage engine's batch semantics from Chapter 2. In agent mode, a single batch writes the session update and subscription cleanup atomically. In cluster mode, the same writes go to the same partition primary, so they can still be batched.

Database records use a different key: `entity/id`. This means records from the same entity are distributed across partitions (different IDs hash to different partitions), which is the desired behavior for balanced load. If all records in an entity hashed to the same partition, a popular entity would create a hot partition. The entity name is included in the key so that records from different entities with the same ID hash to different partitions — `users/abc` and `orders/abc` land on different nodes.

## 4.3 The Partition Map

The partition map is the source of truth for who owns what. It is a fixed-size array of 256 entries, one per partition. Each entry records three things:

```rust
pub struct PartitionAssignment {
    pub primary: Option<NodeId>,
    pub replicas: Vec<NodeId>,
    pub epoch: Epoch,
}
```

The **primary** is the node that accepts writes for this partition. All creates, updates, and deletes for keys hashing to this partition must go through the primary. The primary is optional because during initial bootstrap, before the first Raft proposal, no node owns any partition.

The **replicas** are the nodes that hold copies of this partition's data. With the default replication factor of 2, each partition has one replica. The replica receives writes from the primary asynchronously (Chapter 5 covers the replication pipeline). If the primary fails, the replica can be promoted.

The **epoch** is a monotonically increasing counter that increments every time the partition's assignment changes. When a partition moves from Node 1 to Node 2 during rebalancing, the epoch increments. Writes carry the epoch they were generated under. A replica that receives a write with an epoch older than its current assignment rejects it — the write came from an old primary that has not yet learned about the reassignment. Chapter 5 discusses this in detail.

The partition map is stored as a flat array:

```rust
pub struct PartitionMap {
    version: u64,
    assignments: [PartitionAssignment; 256],
}
```

The version increments on every assignment change, providing a single number that nodes can compare to detect whether their maps are in sync. The fixed-size array means the map never allocates — it is created once at startup and updated in place as Raft proposals are applied.

### Heartbeat Bitmaps

Every node periodically sends a heartbeat to every other node. The heartbeat contains the node's view of which partitions it owns. Rather than sending the full partition map (which would be hundreds of bytes of structured data), the heartbeat encodes partition ownership as two bitmaps: one for primaries, one for replicas. Each bitmap is 256 bits — four 64-bit words, 32 bytes total.

```rust
pub struct Heartbeat {
    version: u8,
    node_id: u16,
    timestamp_ms: u64,
    primary_bitmap: [u64; 4],
    replica_bitmap: [u64; 4],
}
```

Setting and checking a bit:

```rust
pub fn set_primary(&mut self, partition: PartitionId) {
    let idx = partition.get() as usize / 64;
    let bit = partition.get() as usize % 64;
    self.primary_bitmap[idx] |= 1u64 << bit;
}

pub fn is_primary(&self, partition: PartitionId) -> bool {
    let idx = partition.get() as usize / 64;
    let bit = partition.get() as usize % 64;
    (self.primary_bitmap[idx] >> bit) & 1 == 1
}
```

The heartbeat serves two purposes. First, it is a liveness signal — if a node stops sending heartbeats, other nodes declare it dead after a timeout. Second, it is a partition map synchronization mechanism. When a node receives a heartbeat, it can compare the sender's bitmaps against its own partition map to detect disagreements. If Node 2's heartbeat claims it is primary for partition 47, but Node 1's partition map says Node 1 is primary for partition 47, there is a conflict that Raft needs to resolve.

The bitmap representation is compact and efficient. A heartbeat message is 1 (version) + 2 (node ID) + 8 (timestamp) + 32 (primary bitmap) + 32 (replica bitmap) = 75 bytes. At the default 1-second heartbeat interval in a 16-node cluster, each node sends 15 heartbeats per second — roughly 1 KB/s of heartbeat traffic. This is negligible compared to replication traffic.

The fixed partition count makes this representation possible. With a variable partition count, heartbeats would need a variable-length structure — a list of partition IDs, or a dynamically-sized bitmap.

## 4.4 The Write-or-Forward Decision

The core routing logic in the cluster is a three-way branch that every write passes through.

When a mutation is generated — a database record created, a session updated, a subscription added — the node determines the partition from the key, then checks its own partition map:

1. **Am I the primary?** Check the local partition map. If yes: apply the write locally, assign a sequence number, send it to replicas.
2. **Am I not the primary, but I know who is?** Look up the primary in the partition map. Forward the write to that node as a write request over the cluster transport.
3. **Is there no primary at all?** The partition is unassigned — the cluster is bootstrapping or in a degraded state. Log a warning. The write is lost.

The second case is the common path for non-primary nodes. A client connects to Node 2 and creates a record. The record's key hashes to partition 47. Node 2's partition map says Node 1 is primary for partition 47. Node 2 forwards the write to Node 1. Node 1 applies it, replicates it to Node 3 (the replica), and the write is complete. The client receives a response through the MQTT response topic, unaware that the write was forwarded.

This forwarding is transparent to the MQTT protocol layer. The topic hierarchy, the request-response pattern, the user properties described in Chapter 3 — none of these change in cluster mode. What changes is the routing layer underneath: the same MQTT publish that executed locally in agent mode now passes through a forwarding step if it arrives at the wrong node.

The forwarding decision happens on every write. Its cost is a partition map lookup — an array index operation, O(1) — followed by a comparison against the local node ID. This is the reason the partition map is a fixed-size array rather than a hash map or tree.

### Reads Follow a Similar Pattern

Read operations (get, list) also check partition ownership. For a single-record read, the node checks whether it is the primary or a replica for the partition. If it is either, it serves the read locally — replicas can serve reads because MQDB uses eventual consistency, and a slightly stale read is acceptable. If the node has no role for the partition, it forwards the request to the primary.

List operations are more complex because they must aggregate results across all partitions. A list of all users requires scanning every partition's data store. In cluster mode, this becomes a scatter-gather operation: the coordinating node sends a list request to every node, each node scans its primary partitions and returns matching records, and the coordinator merges the results. Chapter 8 covers this in detail.

## 4.5 Two Classes of Entities

Not all data follows the partition-and-forward pattern. MQDB divides its entities into two classes based on a fundamental question: does this data need to be available for local lookup on every node?

### Partitioned Entities

Most entities are partitioned. A record hashes to a partition, the partition has a primary, and writes go to that primary. This is the standard model described in the previous sections.

Partitioned entities include:
- **Sessions** — client connection state, partitioned by client ID
- **Subscriptions** — topic subscriptions per client, partitioned by client ID
- **Retained messages** — messages stored for future subscribers, partitioned by topic
- **QoS state** — exactly-once delivery tracking, partitioned by client ID
- **Inflight messages** — pending QoS 1 acknowledgments, partitioned by client ID
- **Database records** — all user-created entity data, partitioned by entity name and record ID

For these entities, the partition primary is the only node that needs the data at write time. Other nodes may hold replicas for durability, but they never need to query the data as part of handling an unrelated request.

### Broadcast Entities

Some entities must exist completely on every node. These are the broadcast entities, and they exist because of a specific requirement: **publish routing must be local**.

When a message is published on Node 1, the broker must immediately determine which clients subscribe to that topic. Some of those clients may be connected to Node 2 or Node 3. The broker cannot send a network request to every other node asking "do any of your clients subscribe to this topic?" for every incoming publish — that would add a network round trip to every message, destroying the latency characteristics that make MQTT useful for real-time applications.

The solution is to keep a complete copy of the subscriber map on every node. When a client on Node 2 subscribes to `sensors/temperature`, every node learns about it. When a message is published to `sensors/temperature` on Node 1, Node 1 looks up the subscribers in its local copy of the topic index, discovers that a client on Node 2 is subscribed, and forwards the message to Node 2. The lookup is local. The forwarding is a single network hop to a known destination.

Broadcast entities include:
- **Topic index** — maps exact topics to the set of subscribing clients and their connected nodes
- **Wildcard store** — maps wildcard patterns (e.g., `sensors/+/temperature`) to subscribers
- **Client locations** — maps client IDs to the node they are currently connected to
- **Database schema** — entity schema definitions (needed for validation on every node)
- **Constraint definitions** — constraint metadata (needed for enforcement on every node)

The first three are essential for message routing. The topic index and wildcard store are read on every publish to determine where to forward the message. Client locations are read when the topic index says a subscriber is on another node — the location tells the publisher which node to send to. Without local copies, every publish would require cross-node queries before the message could be delivered.

Schema and constraint definitions are broadcast for a different reason: validation. When a database write arrives at any node, that node must validate the record against the entity's schema and check constraints before forwarding to the partition primary. If schemas lived only on one partition, every validation would require a network request.

### How Broadcast Entities Propagate

Broadcast entities use a different write path than partitioned entities. When a broadcast entity is modified, the originating node applies the change locally first, then sends it to all other alive nodes in the cluster. This is the reverse of the partitioned write path, where the write goes to the primary first and the primary replicates to replicas.

The "apply locally first" order matters for latency. When a client subscribes on Node 1, Node 1 updates its local topic index immediately. If a message is published on Node 1 one millisecond later, Node 1 already knows about the subscription. It does not need to wait for the broadcast to reach other nodes and come back. The local-first approach means the subscribing node always has the most up-to-date view, and other nodes converge within a heartbeat interval.

The propagation mechanism varies by entity. The topic index uses a dedicated broadcast message — a single cluster message sent to all nodes, with no persistence writes. The wildcard store also uses a dedicated broadcast message for real-time propagation, but additionally persists 256 local writes for restart recovery (section 4.6 explains why). Client locations, schemas, and constraints use the standard replication write mechanism but are tagged as broadcast, so the write-or-forward logic sends them to all alive nodes instead of just the partition primary and its replicas.

## 4.6 Write Amplification

Broadcast entities trade write cost for read locality. The trade is not always cheap.

### Exact Topic Subscriptions

When a client subscribes to an exact topic like `sensors/temperature`, two things happen:

1. The subscription record is stored as a partitioned write, hashed by client ID. One replication write to the subscription's partition primary.
2. The topic index is updated via a single cluster broadcast message. One message sent to all other nodes.

Total cost: 1 replication write + 1 broadcast message. This is lightweight. The broadcast message is small (topic name, client ID, partition, QoS) and is handled in-memory on each receiving node.

### Wildcard Subscriptions

When a client subscribes to a wildcard pattern like `sensors/+/temperature`, the cost is higher:

1. The subscription record is stored as a partitioned write, same as exact topics.
2. The wildcard store is updated locally with 256 persistence writes — one for each partition — so that the wildcard trie can be rebuilt from storage on restart.
3. A single wildcard broadcast message is sent to all other nodes.

Total cost: 1 replication write + 256 local persistence writes + 1 broadcast message.

The 256 local writes deserve explanation. The wildcard trie is an in-memory data structure that matches published topics against wildcard patterns. On a clean startup, the trie must be rebuilt from durable storage. The wildcard store persists each wildcard entry across all 256 partitions in the local storage engine, so that on restart, a full scan of any partition's storage will find the wildcard entries and reconstruct the trie. This is a deliberate choice: the wildcard trie is too important for publish routing to depend on cluster communication during startup. A node that restarts must be able to rebuild its subscriber map from local storage alone, before it reconnects to the cluster.

The 256 writes are local disk writes, not network operations. They go to the node's own storage engine in a batch. On modern SSDs, 256 small writes in a batch complete in under a millisecond. The cost is real but bounded.

### Why This Is Fundamental

The write amplification is not a bug to be optimized away. It is a direct consequence of the design requirement that publish routing be local. The only question is the propagation mechanism — and MQDB chose broadcast messages over per-partition replication writes specifically to minimize the amplification.

An earlier design considered sending 256 individual replication writes through the Raft log for each broadcast entity update. This was correct but expensive: the Raft log grew quickly, and each write required consensus. The current design bypasses Raft for broadcast propagation entirely. The broadcast messages are sent directly over the cluster transport, and each node applies them to its local stores without consensus. This means broadcast updates are eventually consistent — a node might briefly have a stale subscriber map — but the convergence time is bounded by the heartbeat interval, and the Raft log stays compact.

For workloads with relatively stable subscriptions — subscribe once, receive many messages — the write amplification is amortized over the lifetime of the subscription. A sensor that subscribes to a command topic on startup and stays subscribed for weeks generates 256 local writes once. The thousands of messages it receives afterward are all routed with local lookups, zero network round trips. The one-time write cost buys ongoing read efficiency.

For workloads with rapidly churning subscriptions — clients that subscribe and unsubscribe frequently — the amplification can become noticeable, especially for wildcard patterns. This is an inherent tension in the design: local read performance requires global write propagation. MQDB optimizes for the common case (stable subscriptions) and accepts the cost for the uncommon case (rapid churn).

## 4.7 What Went Wrong

### The Single-Node Bootstrap Race

When the first node starts a new cluster, it must bootstrap itself: elect itself as Raft leader, propose 256 partition assignments (all pointing to itself), and apply them. Only then can it accept writes.

The initial implementation had a timing gap. The node started its MQTT accept loop before the bootstrap completed. Clients connected, sent database requests, and the node had no partition map — every partition had no primary. The writes were silently dropped.

The fix was to tie bootstrap to leader election. When a node wins a Raft election and discovers it is the only node in the cluster, it triggers the rebalancer immediately, which proposes all 256 partition assignments in a single Raft batch. The partition map is populated before the first write arrives, because the accept loop waits for the health check to report readiness, and readiness requires all 256 partitions to have an assigned primary and a Raft leader to exist.

This is a general pattern in distributed systems: the bootstrap sequence must complete before the system advertises availability. A cluster that accepts connections before it has finished initializing will drop or misroute requests during the gap. The fix is always the same — gate availability on initialization.

### The Batch Flush Catastrophe

When a follower node joins an existing cluster, the Raft leader sends it all current log entries, including 256 partition assignments. The follower applies each entry by updating its partition map and persisting the change.

The initial implementation persisted each partition update with a separate disk flush. 256 sequential flushes on a typical SSD take 20+ seconds. During those 20+ seconds, the node's event loop was blocked. It could not process heartbeats. Other nodes timed out waiting for heartbeats and declared the new node dead. The death detection triggered rebalancing, which tried to move partitions away from the "dead" node, which generated more Raft proposals, which the node could not process because it was still flushing the original 256 updates.

The fix was trivial: batch all Raft-applied writes into a single flush. Processing time dropped from 20+ seconds to under a millisecond. A single disk operation replaced 256.

The lesson is that I/O patterns matter more than algorithms. The partitioning algorithm was correct. The rebalancing algorithm was correct. The Raft implementation was correct. But 256 sequential disk flushes, each waiting for the drive to confirm the write, turned a sub-millisecond operation into a 20-second one. The difference between batching and not batching was the difference between a working cluster and a cluster that could not add nodes.

This is Chapter 6's territory in detail, but the root cause belongs here: the partition count (256) directly determined the number of flushes. A system with 16 partitions would have flushed 16 times — still slow, but perhaps not enough to trigger death detection. A system with 4096 partitions would have flushed 4096 times and been completely unusable. The fixed partition count made the problem predictable and the fix straightforward. With dynamic partition counts, the batch size would vary, and the performance cliff would appear at different cluster sizes.

### The Heartbeat Partition Map Race

Nodes learn about partition assignments from two sources: Raft log entries and heartbeats from other nodes. In the early implementation, these two sources could disagree.

A new node joins the cluster. The Raft leader proposes partition assignments for the new node. The Raft entry is committed but has not yet been applied on all nodes. Meanwhile, the new node starts sending heartbeats with its primary bitmap set for the partitions it believes it owns (because it applied the Raft entry locally). Other nodes receive the heartbeat and update their view of the partition map — but the Raft entry has not arrived yet, so the heartbeat and the Raft state disagree.

The fix was to make the partition map authoritative: the local partition map (populated from Raft) is the primary source, and the heartbeat partition map is a secondary source used only as a fallback. The write-or-forward decision checks the local map first. If the local map has no primary for a partition but the heartbeat map does, the heartbeat value is used as a best-effort fallback. This covers the convergence window without undermining Raft's authority over the partition map.

## 4.8 Rebalancing

Rebalancing is the process of redistributing partitions when the cluster topology changes — a node joins, a node leaves, or an operator triggers manual redistribution.

### Initial Assignment

When the first node bootstraps a cluster, it assigns all 256 partitions to itself. Every partition has the same primary (the sole node) and no replicas (there is only one node). This is a degenerate but correct state: the single node can serve all reads and writes without forwarding.

When a second node joins, the rebalancer runs. Since partitions are already initialized, it uses incremental rebalancing rather than computing from scratch. The incremental algorithm identifies that Node 1 holds all 256 primaries while Node 2 holds none, and redistributes half the primaries to Node 2. It also adds Node 2 as a replica for Node 1's remaining partitions, and Node 1 as a replica for Node 2's new partitions. The result is 128 primaries and 128 replicas per node.

### Incremental Rebalancing

When a third node joins an existing two-node cluster, the rebalancer does not recompute from scratch. It computes incremental changes in three phases:

**Phase 1: Redistribute primaries.** The ideal distribution is 256/3 ≈ 85 primaries per node. A node donates a partition if it holds more than ideal + 1 primaries, or if it holds more than ideal and the target node holds fewer than ideal. The donated partition's old primary becomes a replica.

**Phase 2: Add missing replicas.** After primary redistribution, some partitions may have fewer replicas than the replication factor requires. The rebalancer assigns replicas to nodes that do not yet hold the partition, preferring the least-loaded node.

**Phase 3: Redistribute replicas.** If replica counts are heavily skewed — some nodes holding far more replicas than others — the rebalancer moves replicas from overloaded nodes to underweight ones.

Each phase produces a list of partition reassignments. Each reassignment carries the old and new primary, the old and new replicas, and a new epoch. The reassignments are proposed to Raft as a batch. Once committed, every node applies them to its local partition map.

### Node Removal

When a node is declared dead (heartbeat timeout) or explicitly removed, the rebalancer computes removal assignments. For each partition where the removed node was primary, the replica is promoted to primary. For each partition where the removed node was a replica, a new replica is selected from the remaining nodes, preferring the least-loaded one. Every affected partition gets a new epoch.

The promotion of replicas to primaries is instant — no data transfer is needed, because the replica already has a copy of the partition's data. The data may be slightly behind the failed primary (async replication means some recent writes may not have reached the replica), but the partition is available immediately. The gap between the replica's last received sequence and the primary's last written sequence represents the data loss window, which Chapter 5 quantifies.

## What Comes Next

The partition map tells every node which data it owns. But ownership without data is useless — when a partition moves to a new node, the data must follow. Chapter 5 covers the replication pipeline: how writes propagate from primary to replica, how sequence numbers detect gaps, and how the catchup mechanism recovers from divergence.
