# Chapter 5: Replication

The partition map from Chapter 4 tells every node which data it owns. But ownership is an empty promise without data behind it. When a node becomes the primary for partition 42, it needs the actual records that belong to partition 42. When a replica exists for fault tolerance, it needs to receive every write the primary applies. This chapter covers the replication pipeline: the wire format that carries writes between nodes, the sequence numbering that detects gaps, the catchup mechanism that recovers from them, and the epoch system that prevents stale writes during rebalancing.

## 5.1 The Universal Write Abstraction

Every mutation in MQDB — creating a session, storing a retained message, inserting a database record, updating a subscription — becomes a `ReplicationWrite` before it leaves the node that produced it. This structure is the unit of replication:

```rust
pub struct ReplicationWrite {
    pub partition: PartitionId,
    pub operation: Operation,    // Insert, Update, Delete
    pub epoch: Epoch,
    pub sequence: u64,
    pub entity: String,
    pub id: String,
    pub data: Vec<u8>,
}
```

The partition tells the receiver which partition's sequence counter this write belongs to. The epoch prevents stale writes (covered in Section 5.6). The sequence gives the write its position in the partition's total order. The entity identifies which subsystem produced the write — `_sessions`, `_mqtt_subs`, `_db_data`, and others. The id names the specific record. The data carries the serialized payload.

This maps to a 22-byte fixed wire header:

| Bytes | Field | Type |
|-------|-------|------|
| 1 | version | u8 |
| 2 | partition | u16 |
| 1 | operation | u8 |
| 4 | epoch | u32 |
| 8 | sequence | u64 |
| 1 | entity length | u8 |
| 1 | id length | u8 |
| 4 | data length | u32 |

After the header come the entity string, id string, and data payload, with their lengths already encoded in the header. No delimiters, no escaping, no JSON parsing on the receiving end. A receiver reads 22 bytes, extracts the three lengths, and reads exactly that many more bytes.

Entity length is a single byte, capping entity names at 255 characters — more than enough for the internal names like `_mqtt_subs` or `_db_data`.

The key property of this design is that the replication layer does not understand what it replicates. A `ReplicationWrite` carrying a session creation is structurally identical to one carrying a database insert. The entity string is opaque to the transport and sequencing logic. Only the store manager on the receiving end inspects it.

## 5.2 Two-Tier Replication Model

MQDB separates replication into two tiers that serve different purposes and make different guarantees.

**The control plane** uses Raft consensus for partition assignments. When a node joins the cluster, when a partition moves from one node to another, or when a node is removed, these changes flow through Raft. The Raft log carries four command types: `UpdatePartition` (change a partition's primary or replica assignment), `AddNode`, `RemoveNode`, and `Noop` (used for leader election). A partition assignment only takes effect after the Raft leader commits it and a majority of nodes acknowledge.

**The data plane** uses asynchronous replication for actual writes. When a primary applies a write, it sends the `ReplicationWrite` to each replica and moves on. It does not wait for replicas to acknowledge before telling the caller the write succeeded. The primary has applied the write to its own storage and will serve it to readers immediately.

The alternative — running every write through Raft — would require a majority of nodes to acknowledge every session creation, every subscription update, every database insert. For a system that also serves as an MQTT broker handling thousands of publishes per second, the latency cost is unacceptable. A publish that arrives, gets routed to subscribers, and triggers writes to session state, inflight tracking, and QoS bookkeeping would need to wait for Raft rounds on each of those writes.

The tradeoff is straightforward: the partition map is small (256 assignments), changes rarely (only during rebalancing or node failure), and must be globally consistent. It uses Raft. The data is large, changes constantly, and can tolerate brief inconsistency between primary and replicas. It uses async replication.

Chapter 6 covers Raft in detail. The rest of this chapter focuses on the data plane.

## 5.3 The Durability Tradeoff

Async replication means the primary does not wait for replicas. This creates a window where a write exists only on the primary. If the primary crashes during that window, the write is lost. The replica becomes the new primary through Raft consensus, but it never received the write.

How large is this window? It depends on the time between the primary applying the write locally and the replica receiving and applying it. In practice, on a local network, this is measured in single-digit milliseconds. But there is no upper bound — network congestion, or replica overload can stretch it.

MQDB actually has two replication functions that reflect different tradeoffs within this async model.

The first, `replicate_write`, creates a `QuorumTracker` that monitors acknowledgments from replicas. The caller sends the write to the replicas, registers the tracker, and can wait for a specified number of ACKs before considering the write durable. This is used for session creation, where the broker needs to know the session exists on at least one replica before proceeding with the client connection.

The second, `replicate_write_async`, applies the write locally, appends it to the write log, sends it to all replicas, and returns immediately. No tracker, no waiting for ACKs. This is the path for the vast majority of writes: database CRUD, subscription changes, retained messages, inflight state, QoS2 tracking, and all broker state mutations that flow through the `write_or_forward` path.

Both functions share the same core sequence: advance the partition's sequence counter, build the `ReplicationWrite` with the new sequence number, append to the write log, apply to local storage, send to replicas. The difference is only whether the caller waits for confirmation.

How does this compare to other systems?

**etcd** runs every write through Raft. A write is not acknowledged until a majority of nodes have persisted it. This gives strong durability guarantees but limits throughput to what the Raft leader can coordinate — typically tens of thousands of writes per second.

**MySQL semi-synchronous replication** waits for at least one replica to acknowledge receiving the write (not applying it) before acknowledging to the client. This is a middle ground: the write survives single-node failure, but the primary still waits for one network round trip.

**Redis replication** is fully asynchronous by default. The primary applies and acknowledges the write without waiting for replicas. MQDB's `replicate_write_async` path operates identically.

For an MQTT broker that must handle high-frequency publishes with low latency, the Redis-style approach is the right default. The writes that need stronger guarantees (session creation) use the quorum path. Everything else prioritizes throughput.

## 5.4 Sequence Ordering on Replicas

Every partition maintains a monotonically increasing sequence counter. When the primary applies a write, it advances the counter and stamps the write with the new value. Sequence 1, then 2, then 3. The replica tracks its own `sequence` field: the highest sequence number it has applied in order.

When a write arrives at a replica, four things can happen.

**Duplicate.** The write's sequence is less than or equal to the replica's current sequence. This write has already been applied, possibly from a catchup response or a retransmission. The replica acknowledges it as OK and ignores it. This makes replication idempotent — applying the same write twice has no effect.

**Expected.** The write's sequence equals `current + 1`. This is the normal case. The replica applies the write, advances its sequence counter, and then checks whether any buffered writes (from case three) can now be applied. If the replica had buffered sequences 5 and 6 while waiting for 4, receiving 4 triggers applying 4, 5, and 6 in a single pass.

**Small gap.** The write's sequence is greater than `current + 1` but the gap is at most 1,000. This means some writes arrived out of order. The replica buffers the write in a `BTreeMap` keyed by sequence number and responds with a `SequenceGap` acknowledgment that includes the expected sequence number. The `BTreeMap` keeps buffered writes sorted, so when the missing write finally arrives and triggers the "expected" case, the `apply_pending` loop drains all contiguous buffered writes.

**Large gap.** The write's sequence is more than 1,000 ahead of the expected sequence. Something went seriously wrong — the replica missed too many writes to buffer them. It responds with a `SequenceGap` acknowledgment, and the gap triggers the catchup mechanism (Section 5.5). The 1,000-write threshold bounds memory usage: each buffered write is a full `ReplicationWrite` including its data payload, so buffering thousands of them would consume significant memory.

## 5.5 Catchup Mechanism

When a replica falls behind — either because it was offline, slow, or experienced network issues — it needs to recover the writes it missed. MQDB uses a two-tier recovery: write log catchup for small gaps, and full snapshots for large ones.

### The Write Log

Every primary maintains a per-partition write log: a bounded FIFO queue that stores the most recent 10,000 `ReplicationWrite` entries. Each entry records the sequence number and the full write payload. When the primary applies a write and replicates it, it also appends it to the write log.

```rust
struct BoundedLog {
    entries: VecDeque<WriteLogEntry>,
    base_sequence: u64,
}
```

The `base_sequence` tracks the sequence number of the most recently evicted entry. When the log exceeds 10,000 entries, the oldest entry is popped from the front and `base_sequence` advances. This tells the catchup handler whether a given range is still available: if the requested `from_sequence` is greater than or equal to the oldest entry in the log, catchup can proceed. If not, the data has been evicted.

Why 10,000? It bounds memory per partition. With 256 partitions, worst case is 2.56 million write log entries across the node. Each entry stores a full `ReplicationWrite`, so the actual memory depends on payload sizes, but the entry count is fixed. For a system processing thousands of writes per second, 10,000 entries covers roughly a few seconds of writes — enough to handle brief network hiccups and message reordering, but not extended outages.

### Catchup Flow

When the primary receives a `SequenceGap` acknowledgment from a replica, it initiates catchup. The replica's ACK includes the sequence number it expected, so the primary knows exactly which writes the replica is missing.

1. The primary queries the write log for the range from the replica's expected sequence to the current sequence.
2. If the write log can serve the range (the oldest entry covers the requested start), it sends a `CatchupResponse` containing all the missing writes.
3. If the write log has evicted the requested entries, it sends an empty `CatchupResponse`.

Catchup requests are throttled to one every 5 seconds per partition. Without throttling, a replica with a persistent gap would flood the primary with requests on every incoming write.

When the replica receives the catchup response, it sorts the writes by sequence and feeds them one at a time through its `handle_write` logic — the same four cases from Section 5.4. This reuses the existing ordering and deduplication logic rather than adding a separate code path.

### Snapshot Fallback

If the catchup response is empty — meaning the replica fell behind by more than 10,000 writes and the log has already evicted what it needs — the replica needs the full partition state. It transitions to `AwaitingSnapshot` role and sends a `SnapshotRequest` to the primary.

The same fallback triggers if the catchup response's lowest sequence is more than 1,000 ahead of the replica's current position. Even with the missing writes available, such a large gap means the replica is too far behind for incremental catchup to be practical.

The primary responds by serializing the entire partition's state into a byte stream and sending it in 64 KB chunks. Each `SnapshotChunk` carries the partition ID, the chunk index, the total number of chunks, the sequence number at the time the snapshot was taken, and the chunk data.

On the receiving end, a `SnapshotBuilder` collects chunks (which may arrive out of order) into a pre-allocated vector. When all chunks have arrived, it assembles them into a contiguous byte stream. The replica deserializes the snapshot, replaces its local state for that partition, and sets its sequence counter to the snapshot's sequence. From that point, normal replication continues.

While awaiting a snapshot, the replica rejects all normal writes for that partition with a `NotReplica` acknowledgment. This prevents applying writes against a state that is about to be replaced entirely.

### Recovery Summary

The recovery escalation works in stages:

1. **Out-of-order write arrives** → buffer it, send `SequenceGap` ACK
2. **Missing write arrives later** → apply it, drain buffered writes (normal replication resumes)
3. **Gap persists, catchup requested** → primary sends missing writes from log
4. **Write log cannot serve the gap** → snapshot transfer, full state replacement

Each stage is more expensive than the previous one. The design minimizes how often the expensive stages trigger: the write log covers brief gaps, and its 10,000-entry size means snapshots only happen during extended outages or when a new node joins the cluster with no data.

## 5.6 Epoch-Based Consistency

Sequence numbers alone are not enough. Consider what happens during rebalancing: partition 42 moves from Node A (primary) to Node B (new primary). Node A has been assigning sequences 1, 2, 3, ... to its writes. Node B starts fresh as primary with sequence 1. If a replica receives sequence 1 from both Node A and Node B, which one wins?

Epochs solve this. Every partition assignment carries an epoch — a monotonically increasing counter that advances each time the partition's primary changes through Raft. When Node B becomes the new primary for partition 42, the assignment carries a higher epoch than Node A's last assignment.

The replica checks epochs before sequences:

```rust
if write.epoch < self.epoch {
    return ReplicationAck::stale_epoch(self.partition, self.epoch, self.node_id);
}

if write.epoch > self.epoch {
    self.epoch = write.epoch;
    self.sequence = 0;
    self.pending_writes.clear();
}
```

A write from a stale epoch is rejected outright. The replica tells the sender what its current epoch is — this lets the old primary discover that it has been superseded.

A write from a newer epoch resets the replica's state: the sequence counter goes back to 0, and any buffered pending writes from the old epoch are discarded. The new primary starts its own sequence from 1, and the replica follows its ordering from scratch.

This means that during a partition migration, replicas do not need explicit "you have a new primary" notifications. The first write from the new primary carries the new epoch, and the replica adapts automatically. The old primary's in-flight writes are simply rejected, and any quorum trackers waiting for ACKs from that partition will see `StaleEpoch` responses and fail.

The quorum tracker treats a `StaleEpoch` response as an immediate failure — if a replica reports a newer epoch, the primary's writes for that partition are no longer valid regardless of how many other replicas acknowledge them.

The acknowledgment protocol carries four statuses:

| Status | Meaning |
|--------|---------|
| Ok | Write applied successfully |
| StaleEpoch | Sender's epoch is behind the replica's |
| NotReplica | This node is not a replica for this partition |
| SequenceGap | Write is out of order; includes the expected sequence |

Each status drives a different response from the sender. `Ok` counts toward quorum. `StaleEpoch` fails the quorum immediately. `NotReplica` counts as a failed node. `SequenceGap` triggers catchup.

## 5.7 The Store Abstraction Layer

When a `ReplicationWrite` arrives at a node — whether from the replication pipeline, a catchup response, or a snapshot — something must turn the opaque bytes into meaningful state changes. That something is the store manager.

The store manager holds references to every in-memory store the node maintains. It dispatches writes by matching the entity string:

```rust
pub fn apply_to_memory(&self, write: &ReplicationWrite) -> Result<(), StoreApplyError> {
    match write.entity.as_str() {
        "_sessions"       => self.apply_session(write),
        "_mqtt_qos2"      => self.apply_qos2(write),
        "_mqtt_subs"      => self.apply_subscription(write),
        "_mqtt_retained"  => self.apply_retained(write),
        "_topic_index"    => self.apply_topic_index(write),
        "_wildcards"      => self.apply_wildcard(write),
        "_mqtt_inflight"  => self.apply_inflight(write),
        "_offsets"        => self.apply_offset(write),
        "_idemp"          => self.apply_idempotency(write),
        "_db_data"        => self.apply_db_data(write),
        "_db_schema"      => self.apply_db_schema(write),
        "_db_idx"         => self.apply_db_index(write),
        "_db_unique"      => self.apply_db_unique(write),
        "_db_fk"          => self.apply_db_fk(write),
        "_db_constraint"  => self.apply_db_constraint(write),
        "_client_loc"     => self.apply_client_location(write),
        _                 => Err(StoreApplyError::UnknownEntity),
    }
}
```

Sixteen entity types. Nine handle MQTT broker state (sessions, QoS2 tracking, subscriptions, retained messages, inflight state, topic index, wildcards, consumer offsets, idempotency records). Six handle the embedded database (data records, schemas, indexes, unique constraints, foreign keys, constraint definitions). One handles cluster routing (client locations).

Each `apply_*` method calls `apply_replicated` on the corresponding store, passing the operation (insert, update, delete), the record ID, and the data payload. The store deserializes the data and applies the mutation to its in-memory structure. This is the same interface whether the write originated locally or arrived from a remote node — the store does not know or care.

### Dual Persistence

Writes go through two stages: disk persistence and memory application. The `apply_write` method calls both:

```rust
pub fn apply_write(&self, write: &ReplicationWrite) -> Result<(), StoreApplyError> {
    if let Some(storage) = &self.storage {
        storage.write(write)
            .map_err(|_| StoreApplyError::PersistenceError)?;
    }

    self.apply_to_memory(write)
}
```

Disk first, then memory. If the disk write fails, the memory is not updated, which prevents the node from serving data it cannot recover after a restart. The storage layer (covered in Chapter 2) handles serialization and durability.

When multiple writes must be persisted atomically — during catchup, for example, where a batch of writes arrives together — the store manager provides `persist_writes_batch`, which writes all entries in a single storage batch operation. This avoids the overhead of individual disk syncs for each write in the batch.

### The Dual-Return Pattern

The store methods that produce replication writes follow a consistent pattern: they return both the domain object the caller needs and the `ReplicationWrite` that the replication layer needs. For example, `create_session_replicated` returns `(SessionData, ReplicationWrite)`. The store builds both from the same operation — it creates the session, serializes it, and packages the serialized bytes into a `ReplicationWrite` with placeholder epoch (0) and sequence (0). The caller uses the `SessionData` to proceed with the client connection. The replication layer takes the `ReplicationWrite`, stamps it with the real epoch and sequence from the partition state, and sends it to replicas.

This means the store does not know about partitions, epochs, or sequences. It produces the raw write payload. The replication layer adds the routing and ordering metadata. The separation keeps stores testable without a full cluster: unit tests can call `create_session_replicated` and inspect the returned `ReplicationWrite` without any transport.

### Idempotent Replay

Every `apply_replicated` implementation must handle being called with the same data twice. During catchup, a replica may receive writes it has partially applied. During snapshot restoration, the store's entire state is replaced, and subsequent writes that were already included in the snapshot will arrive again through normal replication.

The stores handle this by treating the operation and ID as a natural key. Inserting a record that already exists with the same ID overwrites it. Deleting a record that does not exist is a no-op. This means the replication pipeline does not need to track which writes each store has seen — it simply replays writes, and the stores converge to the correct state regardless of ordering or duplication.

## What Went Wrong: The Catchup Flooding Bug

The initial catchup implementation had no "give up" state. When a replica fell behind, it would buffer out-of-order writes up to the 1,000-entry gap limit, then reject further writes with `SequenceGap` acknowledgments. Each `SequenceGap` triggered a catchup request to the primary. If the write log had already evicted the missing entries, the primary would respond with an empty `CatchupResponse`. The replica would receive the empty response, still have a gap, and the cycle would repeat.

Meanwhile, normal writes kept arriving. Each one was rejected with another `SequenceGap` ack, which triggered another catchup request. The replica was generating two messages for every incoming write — the rejection ack and the catchup request — while making zero progress.

The fix was the `AwaitingSnapshot` role. When a catchup response is empty but the replica still has a gap, or when the catchup data is more than 1,000 sequences ahead of the replica's position, the replica transitions from `Replica` to `AwaitingSnapshot`. In this state, it rejects all normal writes with `NotReplica` instead of `SequenceGap`. `NotReplica` does not trigger catchup requests — the old primary simply marks that replica as failed in its quorum tracker and moves on.

The replica then requests a full snapshot from the primary. Snapshot requests are tracked in a `HashSet<PartitionId>` so the replica does not re-request a snapshot that is already in flight. Once the snapshot arrives and is applied, the replica transitions back to `Replica` with its sequence counter set to the snapshot's sequence, and normal replication resumes.

The distinction between `SequenceGap` and `NotReplica` as rejection reasons is what breaks the loop. Both reject the write. But `SequenceGap` says "I'm trying to keep up, please help me catch up," while `NotReplica` says "I'm not participating in replication for this partition right now." The primary responds to the first with catchup data. It ignores the second.

## What Comes Next

The replication pipeline moves data from primary to replica, but it depends on the partition map to know who is primary and who is replica. Chapter 6 covers the Raft consensus protocol that manages that partition map: how leaders are elected, how partition assignments are proposed and committed, and the bugs that surfaced when the theoretical protocol met a real implementation.
