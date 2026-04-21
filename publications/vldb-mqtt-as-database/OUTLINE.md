# MQDB: MQTT Topic Semantics as the Foundation for a Distributed Database

## Target venue
VLDB Journal (The International Journal on Very Large Data Bases)

## Core thesis
MQTT's topic hierarchy and subscription semantics provide a sufficient abstraction to implement a distributed database — including CRUD operations, partition routing, data replication, change notification, and cluster coordination — within a single unified system, eliminating the architectural separation between messaging broker and database that characterizes existing IoT data infrastructure.

## Precise consistency claim
MQDB provides per-partition causal ordering with async replication (RF=2). Raft consensus governs the control plane (partition assignment, cluster membership) but not per-write durability. This places MQDB in the same consistency class as Redis replication and MongoDB w:1. Infrastructure for quorum-based durability exists (QuorumTracker) but is not yet active.

---

## 1. Introduction (~2 pages)

### The two-system problem
- IoT architectures universally separate messaging (MQTT broker) from storage (database)
- Concrete examples: TBMQ requires Kafka + Redis + PostgreSQL; edge deployments need Mosquitto + SQLite + Redis
- This separation creates: consistency gaps between broker and DB state, deployment complexity, doubled failure modes, duplicated authentication/authorization
- Each system maintains its own notion of topics/tables, sessions/connections, and ordering guarantees

### The convergence observation
- Kafka's evolution: log → KRaft (KIP-595) — messaging system grows an internal database for metadata
- EMQX 6.0: broker adds RocksDB + Raft for message durability
- Harper: database exposes MQTT topic interface to tables
- Direction of convergence: the boundary between messaging and storage is artificial

### MQDB's claim
- MQTT's topic hierarchy is not merely a message routing mechanism — it is a data addressing scheme
- `$DB/{entity}/{id}/{operation}` directly encodes entity, record identity, and operation in the topic path
- MQTT 5.0's request-response pattern turns pub/sub into synchronous CRUD
- MQTT subscriptions provide built-in change data capture (CDC)
- A single system built on this insight replaces broker + database + cache with one binary
- This paper formalizes the mapping, describes the architecture, and evaluates whether the unified approach is competitive

### Contributions
1. A formal mapping from MQTT topic semantics to database operations, showing that the topic hierarchy and subscription model are sufficient for CRUD, partition routing, and change notification
2. An architecture that uses this mapping to build a distributed database with 256-partition sharding, Raft-based control plane, and async replication — all within a single process
3. An evaluation comparing the unified system against conventional separated architectures on throughput, latency, and operational complexity

---

## 2. Background and Related Work (~3 pages)

### 2.1 MQTT protocol essentials
- Topic hierarchy and segment semantics (levels, wildcards: + and #)
- QoS levels: 0 (at most once), 1 (at least once), 2 (exactly once)
- Retain flag: broker stores last message per topic
- MQTT 5.0 additions: request-response pattern (correlation data, response topic), user properties, subscription identifiers
- Shared subscriptions for load balancing

### 2.2 Existing MQTT-database convergence (Tier 1 systems)
- **Harper**: topic namespace = table, retain = upsert, eventual consistency, no Raft, QoS 0/1 only. Validates the intuition but no formal analysis, weaker consistency guarantees, no academic publication
- **EMQX Durable Storage**: RocksDB + Raft added to broker. Topic-Timestamp-Value storage model. Persistence subordinate to messaging — no CRUD, no query semantics
- **FairCom MQ**: MQTT broker with built-in JSON/SQL DB. Unification is operational (store-and-forward reliability), not architectural
- **Machbase Neo**: Time-series DB with MQTT ingestion. Topic as table address, but MQTT is a convenience layer

### 2.3 Academic work on MQTT distribution
- Amico et al. (Computer Networks, 2023): distributed MQTT brokers with per-topic routing
- TBMQ (Journal of Big Data, 2025): Kafka-backed MQTT at scale — exemplifies the two-system problem
- Neither addresses database semantics

### 2.4 IoT databases with MQTT interfaces
- **Apache IoTDB** (VLDB 2020, SIGMOD 2023): tree-structured namespace, TsFile format, 10M values/sec. Built-in MQTT service maps topic to timeseries. Critical difference: MQTT is bolted on as ingestion protocol; internal model is time-series tree. Sets the evaluation bar for this paper
- Edge computing approaches (SQLite + Redis + Mosquitto): typical duct-tape integration

### 2.5 Foundational systems
- Raft (USENIX ATC 2014): consensus for MQDB's control plane
- Kafka and KRaft (KIP-595): convergence from messaging toward database semantics
- CockroachDB (SIGMOD 2020): template for distributed database systems papers

### 2.6 Gap statement
No academic work formalizes the argument that MQTT's topic hierarchy and subscription semantics are sufficient for a distributed database. Existing systems either add persistence to brokers, bolt MQTT onto databases, or unify pragmatically without formal treatment.

---

## 3. The Topic-to-Database Mapping (~3 pages)

*This is the paper's primary intellectual contribution.*

### 3.1 Topic hierarchy as data addressing

The MQTT topic `$DB/{entity}/{id}/{operation}` provides four semantic levels:

| Topic segment | Database concept | Example |
|---|---|---|
| `$DB/` | Database namespace | Fixed prefix |
| `{entity}` | Table / collection | `users`, `sensors` |
| `{id}` | Primary key | `user-42`, `sensor-7` |
| `{operation}` | CRUD verb | `create`, `update`, `delete` |

Formal mapping (precise definitions):
- **Entity** ↔ MQTT topic level 2: a named collection
- **Record identity** ↔ MQTT topic level 3: a unique key within entity
- **Operation** ↔ MQTT topic level 4: one of {create, update, delete}
- **Read** ↔ MQTT topic levels 2+3 only: `$DB/{entity}/{id}` (no operation suffix)
- **List** ↔ MQTT topic level 2 + keyword: `$DB/{entity}/list`
- **Payload** ↔ MQTT message body: JSON document (the record data)

### 3.2 Two MQTT mechanisms for database interaction

MQDB uses two distinct MQTT mechanisms for database interaction, not one:

**Mechanism 1: Request-response for synchronous CRUD (MQTT 5.0)**

| MQTT publish target | Database operation |
|---|---|
| `$DB/users/create` (+ response topic) | INSERT — server assigns ID, returns record |
| `$DB/users/user-42` (+ response topic) | SELECT by ID — returns record data |
| `$DB/users/user-42/update` (+ response topic) | UPDATE — modifies record, returns updated data |
| `$DB/users/user-42/delete` (+ response topic) | DELETE — removes record, returns confirmation |
| `$DB/users/list` (+ response topic) | LIST — returns filtered/sorted collection |

Client publishes to the operation topic with a response topic and correlation data. Broker processes the operation and publishes the result back on the response topic with matching correlation data. This transforms an async pub/sub protocol into a synchronous database client.

**Mechanism 2: Subscriptions as change data capture (CDC)**

| MQTT subscription | Database equivalent |
|---|---|
| `$DB/users/events/#` | CDC stream for entity `users` |
| `$DB/users/events/user-42` | CDC stream for specific record |
| `$DB/+/events/#` | Cross-entity CDC (all entities) |

Subscriptions deliver **future change events only** — they do not return current state or snapshots. Each change event contains: sequence number, entity, record ID, operation type (created/updated/deleted), and the full record data.

This separation is fundamental: request-response is the query interface; subscriptions are the streaming interface. Together they cover the full spectrum of database interaction patterns.

### 3.3 QoS: a non-mapping and future opportunity

An obvious question: do MQTT QoS levels map to database durability guarantees? In principle, QoS 0 (at most once) could mean fire-and-forget writes, QoS 1 (at least once) could mean acknowledged writes, and QoS 2 (exactly once) could mean transactional writes.

**In the current implementation, this mapping does not exist.** QoS affects only the MQTT transport layer — the guarantee that the message reaches the broker. Once a message reaches the topic handler, the database operation proceeds identically regardless of input QoS. Responses are always published at QoS 1.

This is an honest non-mapping that we acknowledge rather than hide. The infrastructure for QoS-to-durability mapping exists (QuorumTracker for quorum-based write confirmation) but is not yet activated. We discuss this as a concrete future extension in §7.4.

### 3.4 Request-response as synchronous operations
- MQTT 5.0 correlation data + response topic enables synchronous CRUD
- Client publishes to `$DB/users/create` with response topic `reply/client-1`
- Broker processes operation, publishes result to `reply/client-1` with matching correlation data
- This transforms an async pub/sub protocol into a request-response database client
- **Known limitation**: In cluster mode, correlation data is preserved through the internal wire protocol but dropped at the final MQTT publish step (a bug, not a design choice)

### 3.5 Internal partition-addressed API
- Topics of the form `$DB/p{N}/{entity}/{id}/{operation}` bypass automatic routing
- Directly addresses partition 0–255
- **Internal only**: blocked from external clients via topic protection rules (`$DB/p+/#` = BlockAll)
- Used for: replication, unique constraint two-phase protocol, foreign key validation, index updates
- Demonstrates that partition addressing is also expressed in topic semantics, even though external clients use only the auto-routed high-level API

### 3.6 Limitations of the mapping
- No multi-record transactions (no BEGIN/COMMIT in MQTT)
- No joins across entities (each topic addresses one entity)
- No SQL — query expressiveness limited to filter/sort/project on single entities
- QoS does not currently map to database durability levels
- Subscriptions provide CDC but not point-in-time snapshots

---

## 4. System Architecture (~4 pages)

### 4.1 Single-node architecture (Agent mode)
- Single binary: MQTT 5.0 broker + embedded key-value store (fjall/LSM)
- Topic handler intercepts `$DB/` publishes, routes to DB engine
- Non-DB topics handled as standard MQTT pub/sub
- Authentication: password, SCRAM-SHA-256, JWT
- Authorization: ACL/RBAC for topic access + per-entity ownership for DB records

### 4.2 Cluster architecture
- Two-tier design: Raft control plane + async data plane

**Control plane (Raft)**:
- Partition assignment: which node owns which of the 256 partitions (primary + replica)
- Cluster membership: node join/leave (AddNode, RemoveNode commands)
- Leader election
- NOT used for per-write consensus (deliberate trade-off)
- Log compaction: retains 1000 entries, compacts after every applied entry
- Election timeout: 3–5s randomized, heartbeat 500ms, startup grace 10s

**Data plane (async replication)**:
- Primary receives write → assigns monotonic sequence number → applies locally → broadcasts to replicas → returns success to client
- Two write paths: (1) `replicate_write()` with QuorumTracker — used only for session creation; (2) `replicate_write_async()` — fire-and-forget, used for all data writes
- Replicas validate sequence ordering, buffer out-of-order writes (gap ≤ 1000), apply in-order
- Per-partition causal ordering guaranteed; cluster-wide linearizability NOT guaranteed
- Replication factor 2 (one primary + one replica per partition)

### 4.3 Partition routing
- CRC32 hash of `{entity}/{id}` modulo 256 → deterministic partition assignment
- High-level API (`$DB/{entity}/{id}`) auto-routes to owning node
- Internal API (`$DB/p{N}/{entity}/...`) addresses partitions directly
- Separate hash functions for data, secondary indexes, and unique constraints

### 4.4 Transport
- Inter-node communication via QUIC (mTLS)
- Binary protocol for heartbeats, Raft messages, replication writes
- Topology options: partial mesh, upper mesh, full mesh

### 4.5 Broadcast entities
- Some data must exist on ALL nodes (not partitioned):
  - TopicIndex: maps subscriptions to nodes for cross-node pub/sub routing
  - Wildcards: wildcard subscription state
  - ClientLocations: which client is connected to which node
- Apply locally first, then forward to all alive nodes
- Required for correct cross-node message routing

### 4.6 Constraint enforcement in distributed context
- Unique constraints use a three-phase protocol: reserve → await responses → commit/release
- Topics: `$DB/_unique/p{N}/{reserve|commit|release}`
- Reserves have 30s TTL for automatic cleanup; 5s timeout on remote reserve responses
- Lock drop/reacquire pattern for async operations while holding write locks
- Secondary indexes partitioned separately from data (same CRC32 scheme, different key format)

---

## 5. Vault: Protocol-Level Field Encryption (~1 page)

*Include because this capability demonstrates an architectural advantage of unification that is impossible in separated broker+database systems. Scoped to single-node deployments.*

### 5.1 The architectural argument
- In a conventional stack (MQTT broker + external database), encryption must happen at the application layer — between the broker output and the database input
- In MQDB's agent mode, the broker IS the database, so field-level encryption occurs at the topic handler level — below the protocol boundary
- A client publishing to `$DB/medical/123/update` never sees plaintext if the vault is locked
- No application middleware required; no key management outside the single binary

### 5.2 Implementation sketch
- Per-user AES-256-GCM field-level encryption (not per-database — each user derives their own key)
- PBKDF2-HMAC-SHA256 key derivation (600,000 iterations) from user passphrase
- Zero-knowledge: server never stores passphrase, only derived key in memory when unlocked
- Record-level additional authenticated data (AAD): `{entity}:{id}` binds ciphertext to specific record
- Vault operations exposed as MQTT topics: `$DB/_vault/{enable|unlock|lock|status|change|disable}`
- Transparent to CRUD clients — encryption/decryption happens at the broker/DB boundary
- Recursive: nested JSON objects and arrays are encrypted field-by-field
- System fields (prefix `_`) and ownership fields are never encrypted

### 5.3 Scope and limitations
- **Agent mode only**: Cluster mode explicitly rejects vault operations. Extending vault to distributed deployments requires key distribution protocol not yet implemented.
- The architectural argument (encryption at protocol boundary) holds strongest for single-node deployments where the protocol boundary and storage boundary are identical
- In cluster mode, MQDB's architecture is closer to a separated system (nodes coordinate over network), weakening this particular argument
- Separated architectures cannot achieve protocol-level encryption without custom middleware between broker and database — this remains true for single-node comparison

---

## 6. Evaluation (~4 pages)

### 6.1 Experimental setup
- Hardware description
- Software versions (MQDB version, comparison systems)
- Network configuration (localhost for single-machine, LAN for multi-node)

### 6.2 Microbenchmarks: CRUD throughput and latency

**Workloads** (aligned with YCSB where applicable):
- **Insert throughput**: Sequential inserts, 64B–4KB payload sizes
- **Point read latency**: Random reads by ID (YCSB Workload B analog)
- **Update throughput**: Random updates to existing records (YCSB Workload A analog)
- **Scan/list performance**: Full entity list with varying filter complexity
- **Delete throughput**: Sequential deletes
- **Mixed workload**: 50% read / 50% update (YCSB Workload A)

**Comparison baselines**:
1. Mosquitto + PostgreSQL (via application bridge)
2. Mosquitto + Redis (pub/sub + cache pattern)
3. MQDB agent mode (single-node, unified)
4. MQDB cluster mode (3-node, 256 partitions)

**Metrics**: ops/sec (throughput), p50/p95/p99 latency, CPU utilization, memory footprint

### 6.3 Pub/sub throughput
- Message throughput (msg/sec) at QoS 0 and QoS 1
- Cross-node pub/sub routing overhead (publisher on node 1, subscriber on node 2)
- Comparison against standalone Mosquitto and EMQX

### 6.4 Cluster scalability
- 1-node vs 3-node throughput for partitioned operations
- Partition routing overhead (high-level API auto-routing vs internal direct partition)
- Replication overhead: single-node vs RF=2
- Node failure and recovery: time to detect failure, repartition, resume serving

### 6.5 Operational complexity comparison
- Deployment: lines of configuration, number of processes, number of network connections
- Failure modes: enumerate what can fail in MQDB (1 process) vs Mosquitto+PostgreSQL (2+ processes, network between them, schema sync)
- Authentication surface: MQDB (one auth config covers broker + DB) vs separated (auth in broker AND database)

### 6.6 Comparison with IoTDB benchmarks
- Where directly comparable to IoTDB's published numbers (VLDB 2020), provide comparison
- Where workloads differ (time-series vs general CRUD), explain why direct comparison is not meaningful and what analog metrics are used instead

---

## 7. Discussion (~2 pages)

### 7.1 When this architecture is appropriate
- IoT data management where devices already speak MQTT
- Edge/fog computing where operational simplicity matters (single binary)
- Applications requiring both real-time pub/sub and persistent CRUD on the same data
- NOT appropriate for: heavy analytical queries, multi-table joins, SQL-dependent applications, workloads requiring strong linearizability

### 7.2 Consistency trade-offs
- Async replication means writes can be lost on primary failure before replica applies them
- Per-partition causal ordering vs cluster-wide linearizability
- QuorumTracker infrastructure exists for future durability levels but is not yet active
- Honest comparison: CockroachDB/Spanner provide linearizability; MQDB does not. Different design points for different workloads

### 7.3 The convergence thesis
- Kafka → KRaft: messaging grows internal database
- EMQX → Durable Storage: broker grows persistence
- MQDB: database grows from messaging primitives (opposite direction)
- These systems are converging. MQDB's contribution is showing the mapping explicitly and building from the messaging layer up, rather than bolting storage on

### 7.4 Limitations and future work
- No multi-record transactions
- No cross-entity joins
- Query expressiveness limited to single-entity filter/sort/project
- **QoS-to-durability mapping**: Infrastructure exists (QuorumTracker) to map QoS 1 → primary-ack, QoS 2 → quorum-ack. Activation would extend the topic-to-database mapping to include durability semantics
- Vault extension to cluster mode (requires distributed key management)
- Correlation data restoration in cluster mode (known bug)
- Reactive aggregates and materialized views (possible via MQTT subscriptions, not implemented)
- Subscription snapshots: initial state delivery on subscribe (current: CDC-only)

---

## 8. Conclusion (~0.5 pages)

Restate: MQTT topic semantics are sufficient for a distributed database. The mapping is formal and complete for CRUD, partition routing, change notification, and constraint enforcement. The unified architecture eliminates the two-system problem, reduces operational complexity, and achieves competitive performance. The vault demonstrates capabilities enabled by unification that are impossible in separated architectures. This is the first formal treatment of this architectural approach.

---

## Figures and diagrams needed

1. **Architecture comparison**: Side-by-side of conventional (broker + DB + cache + bridges) vs MQDB (single binary). Boxes-and-arrows.
2. **Topic-to-database mapping table**: The formal mapping from Section 3.1, as a prominent figure
3. **Two mechanisms diagram**: Request-response (synchronous CRUD) vs subscriptions (CDC) — how both use MQTT but serve different database interaction patterns
4. **Partition routing flow**: How a publish to `$DB/users/42/update` routes through topic parser → CRC32 hash → partition map → owning node → local DB apply → replication
5. **Two-tier architecture**: Control plane (Raft) vs data plane (async replication) diagram
6. **Replication sequence diagram**: Primary apply → broadcast → replica sequence validation → in-order apply
7. **Benchmark results**: Throughput bar charts, latency CDF plots, scalability line graphs

## Estimated length
~20 pages (VLDB Journal format), including figures and references

---

## Verification checklist

Each claim in the paper must be verified against code before inclusion:

- [x] §3.1: Topic parsing in `db_topic.rs` — exact topic formats (VERIFIED: 16 DbTopicOperation variants, two-tier API)
- [x] §3.2: Subscription patterns — CDC only, not queries (CORRECTED: subscriptions deliver future events only, no snapshots)
- [x] §3.3: QoS handling — QoS ignored for DB ops (CORRECTED: both QoS 0 and 1 produce identical DB behavior, responses hardcoded QoS 1)
- [x] §3.4: Request-response — correlation data handling (VERIFIED: works in agent mode; BUG in cluster mode — correlation data dropped at queue_local_publish)
- [x] §3.5: Low-level partition API — internal only (CORRECTED: BlockAll for external clients, used for replication/constraints/indexes)
- [x] §4.2: Raft control plane — partition assignments AND cluster membership (CORRECTED: 4 command types: UpdatePartition, AddNode, RemoveNode, Noop)
- [x] §4.2: Async replication — two write paths (VERIFIED: replicate_write with QuorumTracker for sessions only; replicate_write_async fire-and-forget for data)
- [x] §4.3: CRC32 partition hash — deterministic (VERIFIED: separate hash functions for data/indexes/unique constraints)
- [x] §4.5: Broadcast entities — TopicIndex, Wildcards, ClientLocations (VERIFIED: 24 entity constants, apply-locally-first pattern)
- [x] §4.6: Unique constraint two-phase protocol (VERIFIED: reserve → commit/release via $DB/_unique/p{N}/...)
- [x] §5.2: Vault — per-user AES-256-GCM, agent-mode only (CORRECTED: per-user not per-database; cluster rejects vault ops)
- [ ] §6: All benchmark numbers (to be generated)
