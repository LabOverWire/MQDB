# 4. System Architecture

MQDB is implemented as a single Rust binary that functions simultaneously as an MQTT 5.0 broker and a distributed document database. This section describes the single-node and cluster architectures, the partitioning scheme, inter-node transport, and distributed constraint enforcement.

## 4.1 Single-Node Architecture

In agent mode, MQDB runs as a single process containing two subsystems:

**MQTT broker.** A full MQTT 5.0 broker supporting TCP, TLS, QUIC, and WebSocket transports. The broker handles connection management, session persistence, subscription matching, QoS message flow, and retained messages. Three authentication methods are supported: password files, SCRAM-SHA-256, and JSON Web Tokens.

**Embedded database.** An LSM-tree key-value store (fjall) provides persistent storage for database records, secondary indexes, constraint metadata, and schema definitions. Records are stored as JSON documents keyed by `{entity}/{id}`.

The two subsystems are connected by a topic handler. When a client publishes to a topic matching the `$DB/` prefix, the broker intercepts the message and routes it to the database engine via the topic parser described in Section 3.1. The topic parser returns a structured operation (entity, record ID, operation verb), and the database engine executes the corresponding CRUD operation. The result is published to the client's response topic as described in Section 3.2.1.

Topics that do not match the `$DB/` prefix are handled as standard MQTT pub/sub — the broker routes them to matching subscribers without database involvement. This means MQDB serves as a drop-in replacement for a standalone MQTT broker, with the database capabilities as an addition rather than a replacement.

Authorization operates at two levels. An ACL/RBAC system controls MQTT topic access (who can publish or subscribe to which topics). A separate ownership system controls database record access (who can read, update, or delete which records). These two systems are independent: a user with MQTT publish permission on `$DB/users/create` can create records, but the ownership system may restrict which records they can subsequently read or modify.

## 4.2 Cluster Architecture

In cluster mode, MQDB runs multiple nodes, each containing the same broker and database subsystems, organized into a two-tier architecture: a Raft-based control plane and an asynchronous data plane.

### 4.2.1 Control Plane: Raft

The control plane uses the Raft consensus protocol [raft] for two responsibilities:

1. **Partition assignment.** Which node serves as primary and which as replica for each of the 256 partitions. The Raft log records `UpdatePartition` commands that assign partitions to nodes.
2. **Cluster membership.** Nodes join and leave the cluster via `AddNode` and `RemoveNode` commands committed through Raft.

Raft is *not* used for per-write consensus. Each database write is applied locally by the primary and asynchronously replicated to the replica. This is a deliberate design choice that trades strong consistency for throughput and latency, placing MQDB in the same consistency class as Redis replication and MongoDB with write concern `w:1`.

The Raft implementation uses randomized election timeouts of 3–5 seconds, a 500ms heartbeat interval, and a 10-second startup grace period for initial cluster formation. Log compaction retains the 1000 most recent entries and triggers after every applied entry.

### 4.2.2 Data Plane: Asynchronous Replication

When a client publishes a write operation to a node that is primary for the target partition, the following sequence occurs:

1. The primary assigns a monotonically increasing sequence number from a per-partition counter.
2. The write is applied to the local database.
3. The write is broadcast to the replica for that partition.
4. The primary returns success to the client.

The primary does not wait for the replica to acknowledge the write. This is a send-and-forget model: the write is considered durable after local application. If the primary fails before the replica receives the write, the write is lost.

Replicas validate the sequence numbers of incoming writes. Writes are applied in sequence order. If a write arrives out of order, the replica buffers it, provided the gap does not exceed 1000 entries. Writes that arrive with a gap larger than 1000 trigger a full state synchronization from the primary.

This design provides per-partition causal ordering: within a single partition, operations are observed in the order assigned by the primary. Cross-partition ordering is not guaranteed. Cluster-wide linearizability is not provided.

The replication factor is 2: one primary and one replica per partition. Infrastructure for quorum-based write acknowledgment exists (a `QuorumTracker` component that can await replica confirmation before returning success to the client) but is not activated in the current implementation.

## 4.3 Partition Routing

MQDB uses 256 fixed partitions. Records are assigned to partitions by computing CRC32 over the key `{entity}/{id}` and taking the result modulo 256:

```
partition(E, K) = CRC32(E + "/" + K) mod 256
```

When a client publishes to the high-level API (e.g., `$DB/users/user-42/update`), the receiving node computes the target partition from the topic segments, determines the primary node for that partition from the partition map, and forwards the operation if necessary. The client does not need to know which node owns which partition — routing is transparent.

Secondary indexes and unique constraint entries use separate hash functions with distinct key prefixes (`idx:{entity}:{field}:` and `unique:{entity}:{field}:`) to distribute index entries independently of data records. This means a record and its index entries may reside on different partitions — and therefore on different nodes — requiring cross-node coordination for index updates and constraint checks.

Schema definitions use a fourth hash function keyed on `schema:{entity}`, ensuring all schema metadata for an entity resides on a single partition regardless of how many records exist.

## 4.4 Transport

Inter-node communication uses QUIC with mutual TLS authentication. All cluster traffic — heartbeats, Raft messages, replication writes, constraint protocol messages, and subscription broadcasts — travels over a single QUIC connection between each pair of nodes.

The transport supports three mesh topologies:

- **Partial mesh (default):** Each node connects to lower-numbered nodes. Node 1 has zero outbound connections; node N connects to node 1. This minimizes connection count while ensuring reachability.
- **Upper mesh:** Each node connects to higher-numbered nodes. Inverts the partial mesh direction.
- **Full mesh:** Every node connects to every other node. Maximizes routing directness at the cost of N(N-1)/2 connections.

Heartbeats contain partition ownership bitmaps (256 bits for primaries, 256 bits for replicas), enabling each node to maintain a current partition map without querying the Raft leader.

## 4.5 Broadcast Entities

Not all data can be partitioned. Three categories of metadata must be available on every node for correct message routing:

**TopicIndex.** Maps each topic subscription to the node(s) hosting the subscribing client(s). When a client on node A publishes to a topic that has subscribers on node B, the TopicIndex on node A directs the message to node B. Each entry records the subscriber's client ID, the partition owning the subscriber's session, and the subscription QoS.

**WildcardStore.** Tracks wildcard subscription patterns (subscriptions containing `+` or `#`) across the cluster. Wildcard matching requires checking all patterns against each published topic — this data must be on every node to avoid forwarding every message to a central matcher.

**ClientLocations.** Maps client IDs to the node where each client is currently connected. Used for routing messages to specific clients (e.g., response topic delivery in the request-response pattern).

These broadcast entities follow a common replication pattern: apply the change locally first, then replicate to all alive nodes. When a client subscribes to a wildcard pattern on node A, node A updates its local WildcardStore and broadcasts the subscription to all other nodes. The broadcast is fire-and-forget — there is no quorum requirement — but heartbeat-based reconciliation ensures eventual convergence.

## 4.6 Distributed Constraint Enforcement

MQDB supports unique constraints on entity fields (e.g., "the `email` field in `users` must be unique across all records"). In a distributed system, enforcing uniqueness requires coordination across nodes because the constraint index entry and the data record may reside on different partitions.

MQDB uses a three-phase protocol:

**Phase 1: Reserve.** Before inserting or updating a record with a uniquely constrained field, the writing node sends a reserve request to the node that is primary for the unique index partition (determined by hashing `unique:{entity}:{field}:{value}`). If the value is not already reserved or committed, the primary marks it as reserved with a 30-second TTL and an idempotency key.

**Phase 2: Await.** If multiple fields have unique constraints, reserve requests are sent in parallel to potentially different nodes. The writing node awaits all responses with a 5-second timeout. If any reserve is denied (conflict) or times out, the writing node releases all successful reserves and returns an error to the client.

**Phase 3: Commit or Release.** If all reserves succeed, the writing node applies the data write and sends commit messages (fire-and-forget) to all reserve holders. If the write fails for any reason, release messages are sent instead. The 30-second TTL on reserves provides automatic cleanup if commit/release messages are lost.

The reserve-commit protocol addresses the topic space: reserve, commit, and release messages are published to partition-addressed topics (`$DB/_unique/p{N}/reserve`, `$DB/_unique/p{N}/commit`, `$DB/_unique/p{N}/release`). This means constraint enforcement uses the same topic-based routing as data operations — the constraint protocol is itself an application of the topic-to-database mapping.

**Atomicity boundaries.** In agent mode, the data write, index updates, and constraint entries are committed in a single atomic batch by the storage engine — there is no window of inconsistency. In cluster mode, the data write is atomic within its partition, but constraint commits to remote nodes are fire-and-forget. If a commit message is lost (due to a network partition or node failure), the data write has already succeeded, and the constraint reservation remains in its reserved state until the 30-second TTL expires. During this window, a concurrent insert of the same unique value would be incorrectly rejected. The system trades strict distributed atomicity for availability and simplicity — a design choice consistent with the async replication model described in Section 4.2.2.

Secondary indexes are maintained as a side effect of data writes. When a record is inserted or updated, index entries are computed and forwarded to the partition owning each index entry. Like unique constraints, index partitions are determined by a hash function with a distinct key prefix, ensuring independent distribution.
