# Building a Distributed Reactive Database: Design and Implementation with MQDB

## Working Title Options

1. **Building a Distributed Reactive Database** — Design Decisions, Tradeoffs, and Implementation
2. **MQTT Meets Database** — Building a Distributed System from Protocol to Production
3. **Distributed Systems from Scratch** — Designing and Implementing a Reactive Database in Rust

## Positioning

**Comparable titles:**
- *Designing Data-Intensive Applications* (Kleppmann) — theory-heavy, no implementation
- *Database Internals* (Petrov) — storage engines and distributed systems internals
- *Programming Rust* (Blandy/Orendorff) — language-focused, not system design

**Gap this book fills:** A concrete, implementation-driven distributed systems book. Readers follow the design and construction of a real system — MQDB — from empty directory to clustered deployment. Every chapter pairs a distributed systems concept with its actual implementation, including the bugs found and design decisions revised along the way.

**Unique angle:** MQDB unifies two traditionally separate systems (message broker + database) into one. This creates design challenges that don't appear in pure databases or pure message brokers — broadcast entities, cross-node pub/sub routing, dual replication tiers, and shared transport layers. The book treats these as teaching moments for general distributed systems principles.

## Target Audience

Senior backend engineers and system designers who:
- Have built services but not distributed infrastructure
- Want to understand what happens inside systems like Kafka, CockroachDB, or etcd
- Are interested in Rust for systems programming (but the concepts transcend language)
- Have read DDIA and want the implementation counterpart

**Prerequisites:** Familiarity with at least one systems language (Rust, Go, C++), basic networking (TCP/UDP), and general database concepts (CRUD, indexes, transactions).

## Estimated Length

~400-500 pages across 20 chapters in 5 parts. Each chapter ~20-25 pages.

---

## Part I: The Design Thesis (Chapters 1-3, ~70 pages)

### Chapter 1: Why Unify Messaging and Storage?

The opening chapter frames the problem: modern architectures use separate message brokers and databases, creating operational complexity, consistency gaps, and data synchronization challenges. MQDB's thesis is that both reduce to the same primitive — keyed writes to partitioned stores.

- **1.1 The Two-System Problem** — Services today use a database for state and a message broker for events. Change Data Capture, outbox patterns, and dual-write problems emerge from this split.
- **1.2 The Core Insight** — A subscription is a keyed write. A database record is a keyed write. Both can be serialized, partitioned, replicated, and queried using identical machinery.
- **1.3 MQTT as the Unifying Protocol** — Why MQTT 5.0 (request/response, user properties, shared subscriptions) provides a natural API for both messaging and database operations. The `$DB/` topic namespace.
- **1.4 What We're Building** — Architecture overview: embedded MQTT broker + reactive database + distributed cluster. The three deployment modes (library, standalone agent, cluster).
- **1.5 Tradeoffs Acknowledged Up Front** — What this design sacrifices: SQL compatibility, strong multi-key transactions across partitions, sub-millisecond tail latencies under replication. Honest framing sets the tone for the book.

### Chapter 2: The Storage Foundation

Before distributing anything, build the single-node database. This chapter covers the storage engine, key encoding, and the reactive subscription system.

- **2.1 Key Encoding Scheme** — The `data/`, `idx/`, `sub/`, `meta/` prefix scheme. Why lexicographic ordering matters for range scans. How entity isolation works via key prefixes.
- **2.2 Pluggable Storage Backends** — The `StorageBackend` trait. Fjall (LSM-tree) for production, in-memory for WASM, async for network storage. Why trait-based abstraction pays off when you need to run in a browser.
- **2.3 Schema and Constraint System** — Type validation, unique constraints (single and composite), foreign keys with cascade policies, not-null enforcement. Making invalid states unrepresentable at the storage layer.
- **2.4 Secondary Indexes** — Index encoding (`idx/{entity}/{field}/{value}/{id}`), atomic index maintenance during writes, consistency between data and index entries.
- **2.5 Reactive Subscriptions** — The event dispatcher: broadcast, load-balanced, and ordered modes. Wildcard matching with `+` and `#`. How every write produces an observable event. The subscription registry and consumer groups.
- **2.6 The Outbox Pattern** — Atomic event persistence with retry, exponential backoff, and dead letter queue. Why at-least-once delivery matters and how deduplication via correlation IDs achieves effective exactly-once.

### Chapter 3: MQTT 5.0 as a Database Protocol

A deep dive into MQTT 5.0 features and how they map to database operations. This chapter serves readers who know databases but not MQTT, and vice versa.

- **3.1 MQTT 5.0 Essentials** — Publish/subscribe, QoS levels (0/1/2), retained messages, clean start, session expiry. Just enough protocol for the book's needs.
- **3.2 Request/Response Pattern** — How MQTT 5.0's response topics enable synchronous-style database queries over an asynchronous protocol. The `$DB/{entity}/create` → response topic flow.
- **3.3 Topic-Based API Design** — Client API topics (`$DB/{entity}/{op}`) vs. internal partitioned topics (`$DB/p{partition}/{entity}/{op}`). How the high-level API auto-routes to the correct partition.
- **3.4 User Properties as Metadata** — How MQTT 5.0 user properties carry database metadata: `x-origin-client-id`, `x-mqtt-sender`, correlation data. The bridge between protocol headers and application semantics.
- **3.5 QoS as a Durability Knob** — QoS 0 for fire-and-forget telemetry, QoS 1 for at-least-once database writes, QoS 2 for exactly-once operations. How QoS level maps to the database's consistency guarantees.

---

## Part II: Distributing the System (Chapters 4-9, ~150 pages)

### Chapter 4: Partitioning

The first step toward distribution: splitting data across nodes. This chapter covers partitioning strategy, the partition map, and the fundamental write routing decision.

- **4.1 Fixed Partitions vs. Consistent Hashing** — Why MQDB uses 256 fixed partitions instead of consistent hashing or virtual nodes. The simplicity argument: partition count never changes, rebalancing moves whole partitions between nodes.
- **4.2 The Partition Function** — `crc32fast::hash(key.as_bytes()) % 256`. Why CRC32 over murmur3 or xxhash. Distribution uniformity analysis across real workloads.
- **4.3 Partition Map** — The `PartitionAssignment` structure: primary, replicas, epoch. How every node maintains a local copy. Why epochs prevent stale writes during rebalancing.
- **4.4 The write_or_forward Decision** — The core routing logic: "Am I the primary? Apply locally. Otherwise, forward to primary." Three lines of code that define the entire write path.
- **4.5 Two Classes of Entities** — Partitioned entities (sessions, DB records) vs. broadcast entities (TopicIndex, WildcardStore, ClientLocations). Why publish routing requires local lookups on every node.
- **4.6 Write Amplification** — A single MQTT subscription generates 257 `ReplicationWrite` operations (1 subscription + 256 TopicIndex broadcasts). Why this amplification is fundamental, not a bug.

### Chapter 5: Replication

Data on one node is data at risk. This chapter covers the async replication pipeline, sequence ordering, and the durability tradeoffs.

- **5.1 The Universal Write Abstraction** — `ReplicationWrite`: entity type, identifier, serialized data, partition ID, epoch, sequence. Every mutation — MQTT or database — flows through this single structure.
- **5.2 Two-Tier Replication Model** — Control plane (Raft, strong consistency) for partition assignments. Data plane (async replication) for actual writes. Why running every write through Raft consensus is unacceptable for MQTT throughput.
- **5.3 The Durability Tradeoff** — Async replication means potential data loss if the primary fails before replication completes. Comparison with synchronous (etcd), semi-synchronous (MySQL), and asynchronous (Redis) approaches. Why MQDB chose async and what it means for operators.
- **5.4 Sequence Ordering on Replicas** — The four cases: duplicate (ignore), expected (apply + drain pending), small gap (buffer), large gap (reject). How the `pending_writes` buffer handles out-of-order delivery without unbounded memory.
- **5.5 Catchup Mechanism** — When a replica detects a sequence gap: `SequenceGap` acknowledgment, write log retrieval (bounded 10K entries), `CatchupResponse`. What happens when the write log has evicted the missing entries.
- **5.6 Epoch-Based Consistency** — How epochs prevent stale writes during rebalancing. Replicas reject writes with epochs older than their current assignment.
- **5.7 The Store Abstraction Layer** — `StoreManager` coordinates 17 stores. The `apply_write()` dispatcher routes by entity type string. Dual-mode methods return both entity and `ReplicationWrite`. Idempotent `apply_replicated()` for safe replay.

### Chapter 6: Consensus with Raft

Raft manages the control plane: who owns which partition. This chapter covers the Raft implementation, including the bugs found and fixed during development.

- **6.1 What Raft Manages (and What It Doesn't)** — Partition assignments, cluster membership, leader election. NOT individual write replication, NOT heartbeats. The scope boundary is a design decision, not an implementation shortcut.
- **6.2 Raft Basics for Implementation** — Leader election, log replication, safety guarantees. Covered at implementation depth, not textbook depth. Focus on the decisions that diverge from the Raft paper.
- **6.3 Log Entry Types** — `RaftCommand`: `AddNode`, `RemoveNode`, `UpdatePartition`. The log carries partition map changes, not data writes.
- **6.4 Single-Node Bootstrap** — First node starts, becomes leader, proposes 256 partition assignments to itself. The bootstrap problem: how does a cluster begin?
- **6.5 The Batch Flush Bug** — (Issue 11.5) When a follower received 256 partition updates, each was persisted with a separate `flush()`. 256 flushes took 20+ seconds, blocking the event loop, causing heartbeat timeouts, triggering false death detection. Fix: batch writes with single flush. Processing time dropped from 20+ seconds to <1ms. A case study in how I/O patterns matter more than algorithms.
- **6.6 The Missing add_peer Bug** — (Issue 11.2) New nodes were added to `cluster_members` but not as Raft peers. Without `add_peer()`, the leader never sent `AppendEntries` to new nodes. They never received partition assignments. One missing function call broke the entire cluster. A case study in protocol completeness.
- **6.7 Log Compaction** — Raft log grows unbounded without compaction. Compaction after applied entries, O(1) lookups for applied state.

### Chapter 7: Transport Layer Evolution

The most instructive chapter in the book: how the transport layer evolved from MQTT bridges to direct QUIC, driven by performance data. A case study in measuring, understanding, and redesigning.

- **7.1 MQTT Bridges: The Initial Design** — Reusing the embedded MQTT broker for cluster communication. Topic-based routing, built-in QoS, zero new dependencies, standard debugging tools. Why this was a reasonable starting point.
- **7.2 Bridge Topology Options** — Partial mesh, upper mesh, full mesh. Bridge direction semantics (Out, In, Both). Why full mesh with Both direction causes message amplification loops. The loop prevention mechanism (detecting bridge clients by ID pattern).
- **7.3 The Bridge Overhead Problem** — Performance data: 0 peers = 154K msg/s pubsub, 2 peers = 5-7K msg/s. Each bridge connection shares the broker event loop with client connections. Bridge sessions compete with real traffic.
- **7.4 The ClusterTransport Trait** — Designing an abstraction that supports multiple transports. RPITIT (Return Position Impl Trait in Trait) for native async. `send()`, `broadcast()`, `recv()`. How the trait enabled swapping transports without changing any caller.
- **7.5 Direct QUIC Transport** — Bypassing the MQTT broker entirely. QUIC server + client connections per peer. Length-prefixed framing. The same binary protocol, different delivery mechanism.
- **7.6 mTLS for Cluster Security** — Dual-role certificates (serverAuth + clientAuth EKU). `WebPkiClientVerifier` for mutual authentication. Why cluster certificates should use a dedicated CA.
- **7.7 Performance Results** — QUIC achieves uniform ~8,500 ops/s across ALL nodes regardless of peer count. MQTT varied 10-18x between nodes; QUIC varies 1.03-1.12x. Full mesh becomes viable. The data that justified the redesign.
- **7.8 The Fire-and-Forget Bug** — (Issue 11.7) `tokio::spawn()` for transport operations overwhelmed the MQTT client when establishing multiple connections, causing packet boundary corruption. Fix: proper `await` on all transport operations. Async correctness matters even when "it usually works."

### Chapter 8: Cross-Node Pub/Sub Routing

The defining challenge of unifying a message broker with a distributed database: routing published messages to subscribers across nodes without cross-node queries on every publish.

- **8.1 The Routing Problem** — Subscriber on Node B needs messages published on Node A. Node A must know about Node B's subscribers without querying Node B for every publish.
- **8.2 TopicIndex as Broadcast Entity** — Every node maintains a complete topic-to-subscriber mapping. Subscribe on any node → broadcast to all nodes. Publish on any node → local lookup, forward to target nodes.
- **8.3 ClientLocationStore** — Maps client_id to connected node. Required for determining WHERE to forward a publish. Also a broadcast entity.
- **8.4 Wildcard Routing** — The TopicTrie data structure for `+` and `#` pattern matching. WildcardStore as a separate broadcast entity. `PublishRouter.route_with_wildcards()`.
- **8.5 ForwardedPublish Protocol** — Origin node, topic, QoS, retain flag, payload, target clients, timestamp. Version 2 wire format. The deduplication problem and the timestamp fix (Issue 11.9).
- **8.6 The Missing Broadcast Bug** — (Issue 11.11) Only wildcard subscriptions were broadcast. Individual topic subscriptions only wrote to local TopicIndex. Added `TopicSubscriptionBroadcast` (message type 61). A case study in incomplete symmetry.

### Chapter 9: Query Coordination

Queries that span multiple partitions require coordination. This chapter covers the scatter-gather pattern, partition pruning, and paginating across a distributed dataset.

- **9.1 Scatter-Gather Pattern** — Generate `QueryRequest` per target partition, collect `QueryResponse` from owners, merge results. How to detect completion vs. timeout.
- **9.2 Partition Pruning** — Single-ID queries hash to one partition, skipping the scatter phase. Filter-based pruning when `id=VALUE` is present.
- **9.3 Merge and Sort** — Deduplication by ID, filter application, cross-partition sorting. Why sorting must happen at the coordinator, not at each partition.
- **9.4 Pagination with ScatterCursor** — `PartitionCursor` per partition (partition ID, sequence, last_key). `ScatterCursor` aggregates all partition cursors. Each partition maintains independent pagination state for efficient resumption.
- **9.5 Retained Message Queries** — Exact topic hashes to one partition — a point lookup. Wildcard patterns are the hard case: the broker must find all retained messages whose topics match the wildcard, but those messages are scattered across partitions by topic hash. The MQTT 5.0 spec says the server SHOULD deliver retained messages matching a subscription's topic filter, including wildcards. Most distributed MQTT implementations — including AWS IoT Core — simply don't. They skip wildcard retained delivery entirely because the distributed query is hard to get right.
- **9.6 Three Layers of Retained Dedup** — MQDB implements wildcard retained delivery via scatter-gather, but the naive approach (one query per partition) produced 170x message duplication in a 3-node cluster. Three dedup layers were needed: (1) per-node queries instead of per-partition (a `HashSet<NodeId>` reduces 170 queries to 2 remote requests), (2) same-topic dedup (skip topics already delivered from an earlier response), (3) local-store filtering (exclude topics that exist in the local retained store, preventing double delivery from primary/replica overlap). A case study in how "query all partitions" is never as simple as it sounds — the naive fan-out works correctly for unique-per-partition data like DB records, but retained messages have replication overlap that requires explicit deduplication at every layer.

---

## Part III: Cluster Lifecycle (Chapters 10-14, ~100 pages)

### Chapter 10: Failure Detection and Recovery

How the cluster detects and responds to node failures.

- **10.1 Heartbeat Protocol** — 75-byte heartbeat message carrying partition bitmaps. 1-second interval, 7.5-second suspect threshold, 15-second death threshold. The `Unknown → Alive → Suspected → Dead` state machine.
- **10.2 Why Bitmaps in Heartbeats** — Heartbeats carry primary and replica bitmaps (256 bits each). Nodes learn the partition map from heartbeats, enabling routing before Raft membership completes.
- **10.3 The False Death Problem** — (Issue 11.4) Nodes with `last_heartbeat = 0` were immediately declared dead. Fix: skip `Unknown` status nodes in timeout checks. False positives in failure detection cause cascading instability.
- **10.4 Partition Failover** — Primary dies → Raft leader receives death notification → replica becomes new primary → new replica assigned if available. Epoch increments on every reassignment.
- **10.5 Snapshot Transfer** — `SnapshotRequest`, `SnapshotChunk` (64KB chunks), `SnapshotComplete`. When a new primary takes over, it may need state it doesn't have. Chunked transfer with sequence tracking.

### Chapter 11: Rebalancing

Adding and removing nodes requires redistributing partitions. This chapter covers the rebalancer, the race conditions encountered, and the solutions.

- **11.1 The Rebalancing Problem** — 3 nodes should each own ~85 partitions. When Node 3 joins a 2-node cluster, ~85 partitions must migrate. When Node 2 dies, its partitions must be reassigned.
- **11.2 Balanced Assignment Algorithm** — `compute_balanced_assignments()`: target count per node, round-robin assignment, respecting replication factor constraints (primary and replica on different nodes).
- **11.3 The Rebalance Race Condition** — (Issue 11.10) When a new node joined, multiple Raft proposals were submitted based on pre-rebalance state. If proposals interleaved with applications, partition counts became inconsistent. Fix: `pending_partition_proposals` counter that gates new proposals until in-flight ones are applied.
- **11.4 Partition Migration** — Moving a partition from Node A to Node B: snapshot transfer, sequence catchup, epoch bump, atomic switchover.
- **11.5 Single-Node Bootstrap Edge Case** — (Issue 11.17) First node assigns all 256 partitions to itself. But rebalancing only triggers on `handle_node_alive()`, which doesn't fire for the first node. Fix: `trigger_rebalance_internal()` on leader election.

### Chapter 12: Session Management

MQTT sessions are stateful: subscriptions, QoS state, inflight messages. This chapter covers how sessions work in a distributed context.

- **12.1 Session Lifecycle** — Connect, subscribe, publish, disconnect. Session data: subscriptions, QoS 1/2 state machines, inflight messages, will topic.
- **12.2 Session Partitioning** — Sessions partitioned by `hash(client_id) % 256`. Clean start vs. session resumption across nodes.
- **12.3 QoS State Replication** — QoS 1 inflight messages, QoS 2 exactly-once state machines. Both replicated as partitioned entities.
- **12.4 Last Will and Testament** — Token-based idempotency for LWT delivery. Startup recovery of pending LWTs. Why idempotency matters when the node that should deliver the LWT might also have failed.
- **12.5 Session Migration and Cleanup** — Expired session cleanup, subscription reconciliation on partition takeover, node-death session disconnection.

### Chapter 13: The Message Processor Pipeline

How messages flow from network to storage, and why separating concerns improves latency.

- **13.1 The Pipeline Architecture** — Transport → MessageProcessor → Main Queue / Raft Task. Why processing messages in the transport thread blocks I/O.
- **13.2 Message Classification** — Heartbeats → heartbeat manager. Raft → dedicated Raft task. ForwardedPublish → dedup then main queue. Everything else → main queue directly.
- **13.3 Deduplication** — ForwardedPublish fingerprinting: hash of (origin_node, timestamp_ms, topic, payload). LRU cache with 1000 entries.
- **13.4 The Dedicated Executor** — Isolated Tokio runtime for CPU-intensive tasks. 2-8 worker threads. Why Raft consensus and replication need predictable latency separate from client-facing operations.
- **13.5 Processing Batches** — `ProcessingBatch`: heartbeat updates, dead nodes, outgoing heartbeat, deduplicated publishes. Batching amortizes overhead across multiple messages.

### Chapter 14: The Wire Protocol

Binary protocol design for distributed system communication.

- **14.1 Design Principles** — Fixed-size headers, length-prefixed variable data, big-endian integers, no padding. Why binary over JSON/protobuf for cluster-internal traffic.
- **14.2 BeBytes: Custom Serialization** — The `BeBytes` derive macro for zero-copy serialization. Why a custom crate instead of serde/bincode/prost.
- **14.3 Message Type Registry** — 30+ message types from heartbeats (type 0) to unique constraint operations (types 80-85). Extensibility without versioning hell.
- **14.4 Heartbeat: A Protocol Case Study** — 75 bytes carrying 256-bit bitmaps for both primary and replica assignments. Compact enough for 1-second intervals, rich enough for partition discovery.
- **14.5 Version Fields** — Every message starts with a version byte. Forward compatibility: unknown versions are logged and skipped, not rejected.

---

## Part IV: Advanced Patterns (Chapters 15-18, ~80 pages)

### Chapter 15: Constraints in a Distributed System

Enforcing uniqueness and referential integrity across partitions requires coordination. This chapter covers the two-phase reservation protocol for unique constraints and the scatter-gather protocol for foreign keys.

- **15.1 The Problem** — Two clients on different nodes simultaneously create records with the same unique field value. Without coordination, both succeed, violating the constraint.
- **15.2 Two-Phase Unique Protocol** — Phase 1: Reserve the unique value on the partition that owns it (hash of field value). Phase 2: If reservation succeeds, commit the write; otherwise, release and fail.
- **15.3 Lock Drop/Reacquire Pattern** — When holding `Arc<RwLock>` and needing to await a remote response that arrives via the same event loop: send request while locked, drop lock, await response, reacquire lock, complete operation. Why this pattern appears repeatedly in async distributed code.
- **15.4 Composite Unique Constraints** — Multiple fields concatenated into a single reservation key. How composite keys interact with the partition function.
- **15.5 Foreign Key Existence Checks** — On create/update, verify the referenced entity exists on its partition primary. One-phase, read-only — simpler than unique constraints because no reservation is needed. When a create requires both unique and FK checks, the FK check runs first and the unique reservation is chained after.
- **15.6 The In-Memory FK Reverse Index** — `FkReverseIndex`: a `HashMap<(target_entity, target_id, source_entity, source_field), HashSet<source_id>>` maintained on every data write. Replaces O(n) full-table scans with O(1) lookups. Rebuilt on constraint creation from existing data. Why the index lives in-memory (partition data is already in-memory in cluster mode) and uses `Mutex` rather than `RwLock`.
- **15.7 FK Reverse Lookups on Delete** — Scatter-gather across nodes (skipping nodes with zero primaries) to find referencing records via the reverse index. Three actions: Restrict (block delete), Cascade (delete children), SetNull (null FK field). Short-circuit on first Restrict hit. Depth limit (`MAX_CASCADE_DEPTH = 16`) and work-item limit (`MAX_CASCADE_WORK_ITEMS = 16,000`) prevent unbounded recursion.
- **15.8 Cascade Execution and Acknowledgment** — `CascadeSideEffect` partitions effects into local (same batch) and remote (transport request). Remote cascades use request/response with `cascade_ack` — the coordinating node waits for acknowledgments before marking the `_cascade/` outbox entry delivered. The `__mqdb_fk_expected` CAS guard on set-null operations: field must still hold the expected value, preventing overwrites of concurrent updates. A 30-second retry loop replays unacknowledged remote operations.
- **15.9 Consistency Tradeoffs** — TLA+ model proved phantom reads possible during the lock-drop gap (TOCTOU). Concurrent create + delete of an FK target can produce orphan references. The set-null CAS guard is not ABA-safe — a field changed A→B→A would match — but requires three concurrent operations in a precise interleaving. Why these tradeoffs are acceptable: catches the vast majority of violations, and the alternative (holding locks across network round-trips) causes deadlocks.

### Chapter 16: Consumer Groups and Event Routing

Distributing work across multiple consumers with ordering guarantees.

- **16.1 Three Dispatch Modes** — Broadcast (all receive), LoadBalanced (round-robin), Ordered (partition-sticky). When to use each.
- **16.2 Consumer Group Rebalancing** — Heartbeat-based stale detection. Partition reassignment when consumers join or leave.
- **16.3 Ordered Delivery** — Partition-sticky assignment ensures all events for the same key go to the same consumer. How this interacts with the 256-partition scheme.
- **16.4 Offset Management** — Consumer offsets as partitioned entities. 7-day stale TTL. Resume from last processed position.

### Chapter 17: Distributed Unique Constraints and the Two-Phase Protocol

*Merged into Chapter 15 — this slot is available for:*

### Chapter 17: Performance Analysis and Benchmarking

How to measure, understand, and improve distributed system performance.

- **17.1 Benchmarking Methodology** — Why naive benchmarks produce misleading results. Database state accumulation (56x list degradation from empty to 80K records). Straggler responses after async benchmarks. Isolation requirements.
- **17.2 Sync vs. Async Benchmarks** — Sync: one operation at a time, measures latency. Async: pipelined, measures throughput at saturation. QoS 0 vs QoS 1 backpressure (QoS 0 causes connection resets at ~5000 ops/s; QoS 1 achieves ~12000 ops/s via natural PUBACK backpressure).
- **17.3 The Full Benchmark Matrix** — 4 configurations (agent + 3 topologies) x 3 nodes x 6 operations x 3 replicates = 60 benchmark runs. Why triplicates with mean/stddev matter.
- **17.4 Reading Performance Data** — Agent mode: 6,222 insert / 9,362 get / 154,913 pubsub. Cluster with QUIC: uniform ~8,500 insert across all nodes. How to interpret the numbers and what they mean for capacity planning.
- **17.5 The Bridge Overhead Discovery** — How benchmark data (not theory) revealed that MQTT bridges degraded performance 8-30x on follower nodes. The data that motivated QUIC transport. Lesson: measure first, optimize second.

### Chapter 18: Access Control, Ownership, and Scopes

Who can do what with which data — from connection-time authentication through topic-level authorization to row-level ownership enforcement and event-scoped multi-tenancy.

- **18.1 The Authentication Stack** — Five mechanisms, one connection. Password files (bcrypt hashing), SCRAM-SHA-256 (challenge-response, no plaintext on wire), JWT (HS256/RS256/ES256 with issuer/audience validation), federated JWT (multiple JWKS providers), and certificate-based auth (mTLS client identity). The `CompositeAuthProvider` pattern: enhanced-auth provider wrapping a password fallback for the internal service account. Rate limiting (5 attempts, 60s window, 300s lockout) as a layer, not a bolt-on.
- **18.2 Topic Protection** — Three tiers: `BlockAll` (cluster-internal topics like `_mqdb/#`, `$DB/_unique/#`, `$DB/_fk/#`), `ReadOnly` (event streams `$DB/+/events/#`, system info `$SYS/#`), `AdminRequired` (`$DB/_admin/#`). The `TopicProtectionAuthProvider` decorator that wraps every other auth provider. Entity-level access: `_`-prefixed entities blocked for non-admin users. The `$DB/_health` carve-out.
- **18.3 ACL and Role-Based Access** — The ACL file format: user rules, role definitions, user-role assignments. Four permission levels: read (subscribe), write (publish), readwrite, deny. The `AclManager` evaluation chain. Runtime management via MQTT admin API (`$DB/_admin/acl/...`, `$DB/_admin/user/...`). CLI tooling: `mqdb acl add`, `mqdb acl role-add`, `mqdb acl assign`, `mqdb acl check`.
- **18.4 Per-Entity Ownership** — Row-level access control via `OwnershipConfig`. The `execute_with_sender` gate: read/update/delete check `record[owner_field] == sender`, list auto-injects `owner_field = sender` filter. Ownership transfer prevention (owner field stripped from update payloads). Admin bypass. The `x-mqtt-sender` user property as the identity bridge between MQTT and the database layer.
- **18.5 Scopes and Event Routing** — `ScopeConfig` as an event namespace mechanism. How `--event-scope orders=tenantId` changes event topics from `$DB/orders/events/{id}` to `$DB/tenants/{tenantValue}/orders/events/created`. Hierarchical subscription: `$DB/tenants/acme/#` captures all entity events within a tenant. What scopes are NOT: not data isolation, not query filtering, not a security boundary — purely event routing topology.
- **18.6 The Internal Service Account** — `mqdb-internal-{uuid}` with random password, auto-injected at startup. Bypasses topic protection entirely. Why cluster-internal traffic needs unrestricted access to `BlockAll` topics. The `is_internal_service` check and its security implications.
- **18.7 What Went Wrong: Anonymous Mode and the Missing Service Account** — The bug where `--anonymous` mode without the `dev-insecure` feature didn't exist, and with it, topic protection blocked internal clients (no service account) and admin topics (no admin users). The fix: always create the service account, add `all_users_admin` flag. A case study in how security layers interact — each layer was correct in isolation, but their composition broke the system.
- **18.8 Error Sanitization** — Internal error details never leak to clients. Sanitized MQTT reason strings. Why `"permission denied"` is the right error message even when the real cause is more specific.

---

## Part V: Operating and Extending (Chapters 19-20, ~50 pages)

### Chapter 19: Operating MQDB

From development to production deployment.

- **19.1 Deployment Modes** — Library (embedded in Rust application), standalone agent (single-node), cluster (multi-node). When to use each.
- **19.2 Cluster Sizing** — Partition count is fixed at 256. Replication factor is 2. How many nodes for your workload? Why 3-node clusters are the sweet spot for most use cases.
- **19.3 Monitoring** — Health endpoint (`$DB/_health`), cluster status (`$SYS/mqdb/cluster/status`), partition distribution verification. What to alert on.
- **19.4 Backup and Restore** — The backup/restore protocol via `$DB/_admin/backup`. Consistent snapshots, point-in-time recovery considerations.
- **19.5 Capacity Planning** — Using benchmark data for capacity planning. Agent mode throughput vs. cluster mode throughput. Network bandwidth requirements for replication.

### Chapter 20: The WASM Frontier

Running a database in the browser and at the edge.

- **20.1 Why WASM?** — Edge computing, offline-first applications, reducing server round trips. The same database API in the browser as on the server.
- **20.2 The Memory Backend** — In-memory storage for WASM targets. Same interface as Fjall, different performance characteristics.
- **20.3 Feature Flags and Conditional Compilation** — `native` vs. `wasm` feature flags. How `#[cfg(feature = "...")]` enables single-codebase multi-target.
- **20.4 Async Runtime Differences** — `tokio` on native, `wasm-bindgen-futures` on WASM. The `runtime.rs` abstraction layer.

---

## Appendices (~30 pages)

### Appendix A: The Complete Wire Protocol Reference

All 30+ message types with exact byte layouts, organized by function (heartbeat, Raft, replication, query, unique constraints, snapshots).

### Appendix B: Entity Type Reference

All 16 entity types with their partitioning strategy (broadcast vs. partitioned), key format, and store implementation.

### Appendix C: Configuration Reference

All CLI flags, environment variables, and timing constants with their defaults and recommended production values.

### Appendix D: The Bug Diary

Chronological account of every significant bug encountered during development (Issues 11.1-11.20), organized as: symptom, investigation, root cause, fix, lesson learned. This is teaching material — each bug illustrates a distributed systems principle.

---

## Chapter Dependency Graph

```
Ch1 (Thesis) ──→ Ch2 (Storage) ──→ Ch3 (MQTT Protocol)
                       │
                       ▼
                  Ch4 (Partitioning) ──→ Ch5 (Replication) ──→ Ch6 (Raft)
                       │                      │
                       ▼                      ▼
                  Ch8 (Pub/Sub)          Ch7 (Transport)
                       │
                       ▼
                  Ch9 (Queries)
                       │
                       ▼
              Ch10 (Failures) ──→ Ch11 (Rebalancing) ──→ Ch12 (Sessions)
                                                              │
                                                              ▼
                                                        Ch13 (Pipeline)
                                                              │
                                                              ▼
                                                        Ch14 (Wire Protocol)
                       │
                       ▼
              Ch15 (Unique) ──→ Ch16 (Consumer Groups)
                       │
                       ▼
              Ch17 (Perf) ──→ Ch18 (Security) ──→ Ch19 (Operations) ──→ Ch20 (WASM)
```

Readers can skip Parts IV-V on first reading. Parts I-III form the core narrative.

---

## Sample Chapter Suggestion

**Chapter 7 (Transport Layer Evolution)** is the strongest candidate for a sample chapter:
- Self-contained narrative arc (design → problem → measurement → redesign → validation)
- Demonstrates the book's approach: real data driving real decisions
- Includes code, architecture diagrams, performance tables, and bug stories
- Accessible to readers who haven't read prior chapters
- Shows what distinguishes this book from theory-only distributed systems texts

---

## Writing Approach

Each chapter follows a consistent structure:
1. **The Problem** — What challenge does this component solve?
2. **Design Options** — What approaches were considered? (with tradeoff tables)
3. **The Implementation** — How it actually works, with code excerpts and data structure diagrams
4. **What Went Wrong** — Bugs found during development (from the 20-issue bug diary)
5. **Lessons Learned** — General principles extracted from this specific experience

Code excerpts are Rust but explained at the concept level. Readers familiar with any systems language should follow the logic. Rust-specific features (ownership, lifetimes, async traits) are explained when first introduced.
