# Chapter 1: Why Unify Messaging and Storage?

Every distributed application needs two things: a place to store state and a way to propagate changes. In practice, this means a database and a message broker. The database holds truth. The broker moves it around.

These two systems have evolved independently for decades. Relational databases perfected transactions and consistency. Message brokers — from IBM MQ to RabbitMQ to Apache Kafka — perfected asynchronous delivery and decoupling. The division of labor seems natural: one system stores, the other streams.

But the seam between them is where the hardest problems live.

This book is about what happens when you refuse to accept that seam — when you build a system that treats storage and messaging as a single concern. It follows the design and implementation of MQDB, a distributed reactive database that speaks MQTT natively. Every design decision, every bug, every tradeoff is drawn from a real system running real workloads. The distributed systems principles, however, are universal.

## 1.1 The Two-System Problem

Consider a straightforward scenario. In order for an e-commerce service to process an order, it needs to do the following:

1. Write the order to the database.
2. Publish an "order created" event so that the inventory service, the billing service, and the notification service can react.

Simple enough. Trying to make these operations atomic becomes a huge cooperative challenge.

### Dual Writes

The naive approach writes to both systems independently:

```
begin_transaction();
  database.insert(order);
  broker.publish("orders.created", order);
commit();
```

Since the database and the broker are separate systems with separate failure modes, this fails. If the database write succeeds but the broker publish fails, the order exists without anyone knowing about it. If the broker publish succeeds but the database write rolls back, downstream services react to an order that never existed.

You cannot wrap two independent systems in a single atomic transaction without a distributed transaction protocol like XA — adding coordinator overhead, lock contention across systems, and partial-failure recovery complexity.

### Change Data Capture

One popular mitigation is Change Data Capture (CDC). It works by letting the application write to the database only. A separate process — Debezium tailing the PostgreSQL WAL, or Maxwell reading the MySQL binlog — picks up committed changes and publishes them to the broker.

CDC eliminates dual writes. The database is the single source of truth, and events are derived from committed state. But CDC introduces its own challenges:

**Latency.** Events arrive after the transaction commits, after the WAL flushes, after the CDC connector polls. In high-throughput scenarios, the lag between a write and its corresponding event can stretch from milliseconds to seconds.

**Schema coupling.** The CDC connector reads the database's physical representation — column types, table structures, internal row formats. Changes to the database schema can break the connector silently.

**Operational complexity.** CDC adds a new stateful component to your infrastructure. The connector must track its position in the log, handle schema changes, manage backfill for new consumers, and recover from failures. Debezium alone requires Kafka Connect, Zookeeper (or KRaft), and careful configuration of snapshot modes.

**Semantic gap.** The database sees rows and columns. The application thinks in domain events. CDC bridges them, but the translation is lossy. A row update doesn't carry the intent behind the change. Was this a price correction, a status transition, or a bulk migration? The downstream consumer must infer meaning from data.

### The Outbox Pattern

The outbox pattern improves on CDC by making events explicit. The application writes both the business data and the event into the *same* database transaction:

```
begin_transaction();
  database.insert(order);
  database.insert(outbox_event("orders.created", order));
commit();
```

A separate relay process reads the outbox table and publishes events to the broker. Because the event and the data share a transaction, they are guaranteed to be consistent. The event is a first-class artifact, not derived from physical row changes.

This is a significant improvement. But the outbox pattern still requires:

- A polling or tailing relay process (more infrastructure)
- Idempotent consumers (the relay may publish the same event twice during failure recovery)
- Careful management of the outbox table (cleanup, partitioning, index maintenance)
- Two systems: the database and the broker remain separate

The outbox pattern doesn't eliminate the two-system problem. It manages it.

### What All Three Approaches Share

Dual writes, CDC, and the outbox pattern all accept the same premise: storage and messaging are separate concerns served by separate systems, and the engineering challenge is to synchronize them.

But what if the premise is wrong? What if a single system could store data *and* stream changes, atomically, with no relay, no connector, no synchronization gap?

This is the question MQDB asks.

## 1.2 The Core Insight

At the storage layer, a database write and a message broker subscription are structurally identical operations. Both are keyed writes to partitioned stores.

When a database inserts a record, it writes a value keyed by entity and identifier:

```
key:   data/users/user-42
value: {"name": "Alice", "email": "alice@example.com"}
```

When an MQTT client subscribes to a topic, the broker writes a value keyed by client identifier:

```
key:   _mqtt_subs/client-7
value: {"topic": "sensor/temp", "qos": 1}
```

When a session connects, the broker writes session state keyed by client identifier:

```
key:   _sessions/client-7
value: {"connected_node": 1, "clean_start": false}
```

Strip away the domain-specific semantics, and every operation reduces to the same primitive:

> Given a key, write (or read, or delete) a value.

This is the core insight. If both database records and broker state are keyed writes, then the machinery for managing them — partitioning, replication, consistency, querying — can be unified. One storage engine. One replication pipeline. One partition map. One consensus protocol.

### ReplicationWrite: The Universal Mutation

MQDB makes this insight concrete through a single data structure called `ReplicationWrite`. Every mutation in the system — whether it originates from a database INSERT or an MQTT SUBSCRIBE — flows through this structure:

```rust
pub struct ReplicationWrite {
    pub partition: PartitionId,     // Where it lives (0-255)
    pub operation: Operation,       // Insert, Update, or Delete
    pub epoch: Epoch,               // Version of the partition assignment
    pub sequence: u64,              // Ordering within the partition
    pub entity: String,             // What kind of thing it is
    pub id: String,                 // Which specific thing
    pub data: Vec<u8>,              // The serialized content
}
```

The `entity` field is a plain string that distinguishes between different types of data. Database records use entity names like `"users"` or `"orders"`. MQTT state uses internal names like `"_sessions"`, `"_mqtt_subs"`, or `"_topic_index"`. The replication pipeline doesn't care about the distinction. It routes writes by partition, orders them by sequence, and delivers them to replicas.

This unification has a profound consequence: when the database writes a record, the MQTT broker already knows about it — because the write flowed through the same event system that manages subscriptions. There is no separate CDC connector, no outbox table, no relay process. The write *is* the event.

### Two Classes of Entities

Not all data behaves the same way. MQDB distinguishes between two classes:

**Partitioned entities** follow standard hash-based distribution. A database record with key `users/user-42` hashes to partition `hash("users/user-42") % 256`. Only the node that owns that partition stores the primary copy. Sessions, subscriptions, QoS state, retained messages, and user-defined database records are all partitioned entities.

**Broadcast entities** must exist on every node. When a message arrives on Node A, the node must immediately determine which clients — potentially connected to Node B or Node C — subscribe to that topic. Cross-node queries on every publish would destroy throughput. So MQDB replicates certain entities to all nodes: the topic-to-subscriber index, the wildcard subscription trie, and the client-to-node location map.

This distinction is fundamental. Partitioned entities scale horizontally — add more nodes, and each node handles fewer partitions. Broadcast entities trade write amplification for read locality. The cost varies by subscription type: an exact topic subscription generates 1 `ReplicationWrite` plus a single cluster broadcast message, while a wildcard subscription generates 1 `ReplicationWrite` plus 256 local persistence writes (to rebuild the wildcard trie on restart) plus a cluster broadcast message. In both cases, publish routing requires zero network round trips because every node maintains a complete subscriber map.

Chapter 4 covers partitioning in detail. Chapter 8 explains why broadcast entities are necessary for cross-node pub/sub routing.

## 1.3 MQTT as the Unifying Protocol

The decision to use MQTT as the database protocol may seem unusual. MQTT is an IoT messaging protocol — designed for constrained devices, sensor networks, and telemetry. Why use it as a database interface?

### What MQTT 5.0 Provides

MQTT 5.0 is not the lightweight protocol people remember from early IoT deployments. Version 5.0 added features that make it surprisingly suitable as a general-purpose application protocol:

**Request/Response.** MQTT 5.0 introduced response topics and correlation data, enabling synchronous-style queries over an asynchronous protocol. A client subscribes to a response topic, then publishes a database query to `$DB/users/list` with that response topic attached. The server processes the query and publishes the result back to the response topic. From the client's perspective, this behaves like a synchronous API call.

**Shared Subscriptions.** Multiple clients can share a subscription, with the broker distributing messages among them. This enables load-balanced event processing without an external coordination layer.

**User Properties.** Arbitrary key-value metadata can be attached to any message. This enables headers for correlation IDs, content types, authentication tokens, and operation metadata.

**Session Expiry.** Persistent sessions with configurable lifetimes enable durable subscriptions that survive client disconnections and reconnections.

**Topic Aliases.** Compact representations for frequently-used topics, reducing wire overhead for high-frequency operations.

### The $DB/ Namespace

MQDB maps database operations to MQTT topics under a reserved `$DB/` prefix:

| Topic Pattern | Operation |
|---|---|
| `$DB/{entity}/create` | Create a record |
| `$DB/{entity}/{id}` | Read a record |
| `$DB/{entity}/{id}/update` | Update a record |
| `$DB/{entity}/{id}/delete` | Delete a record |
| `$DB/{entity}/list` | List records with filters |
| `$DB/{entity}/events/#` | Subscribe to changes |

A client creating a user record publishes JSON to `$DB/users/create` with a response topic. The server creates the record, assigns an ID, and publishes the result (or an error) back to the response topic. The request and response flow through the same protocol channel that carries MQTT messages.

This design means that any MQTT client — in any language, on any platform — can perform database operations without a custom driver. The `mosquitto_pub` and `mosquitto_sub` command-line tools work. MQTT client libraries for Python, JavaScript, Go, Java, C, and every other language work. The database API is the messaging API.

### Why Not REST, gRPC, or a Custom Protocol?

Every protocol choice is a tradeoff. Here is what MQTT provides that alternatives don't, and what it sacrifices:

**Versus REST/HTTP:** MQTT maintains persistent connections. A client connects once and can issue many operations without the overhead of TCP handshake, TLS negotiation, and HTTP framing per request. MQTT's binary framing is more compact than HTTP headers. And the subscription model — where the server pushes changes to the client — is native to MQTT but requires workarounds (WebSockets, Server-Sent Events, long polling) in HTTP.

**Versus gRPC:** gRPC provides strong typing through Protocol Buffers and efficient binary encoding. But gRPC requires code generation, language-specific stubs, and HTTP/2 framing. MQTT clients exist in every language, including embedded C for microcontrollers. MQDB's target includes constrained devices that can run an MQTT client but not a gRPC runtime.

**Versus a custom protocol:** A purpose-built protocol could optimize for the exact needs of a distributed database. But it would require building and maintaining client libraries for every target language and platform. By using MQTT, MQDB inherits a mature ecosystem of clients, debugging tools, and operational knowledge.

**What MQTT sacrifices:** Type safety at the protocol level (payloads are opaque byte arrays, not typed schemas), SQL-style query expressiveness (filter operators are limited compared to a full query language), and developer familiarity (most backend engineers know SQL; fewer know MQTT topic patterns).

These sacrifices are real. MQDB compensates through schema validation at the application layer (Chapter 2), a filter system with ten operators (Chapter 9), and comprehensive CLI tooling that abstracts the MQTT protocol from day-to-day use.

## 1.4 What We're Building

MQDB is three things layered on top of each other:

### Layer 1: Reactive Database

At its core, MQDB is a key-value database with a document model. Records are JSON objects stored in named entities (collections). The database supports schemas with type validation, unique constraints (single and composite), foreign keys with cascade/restrict/set-null policies, not-null constraints, secondary indexes, and ten filter operators for queries.

What makes it reactive is that every write — create, update, delete — produces an observable event. Clients subscribe to entity changes using wildcard patterns (`users/#` catches all user changes, `+/123` catches changes to ID 123 across all entities). Subscriptions can be broadcast (all consumers receive all events), load-balanced (round-robin across a consumer group), or ordered (partition-sticky routing so events for the same key always reach the same consumer).

The reactive core also includes an outbox with configurable retry logic and a dead letter queue. This ensures that events derived from writes survive crashes without a separate relay process — because events *are* writes.

### Layer 2: MQTT Broker

The database is embedded inside a full MQTT 5.0 broker. Standard MQTT clients connect, subscribe to topics, publish messages, and use QoS 0/1/2 delivery guarantees. The broker supports TLS, QUIC transport, password-based authentication (Argon2), SCRAM-SHA-256 challenge-response, JWT tokens (symmetric and asymmetric), and federated JWT with multiple issuers.

The broker and the database share a single storage layer. MQTT session state (subscriptions, QoS inflight messages, retained messages) and database records live in the same key-value store, managed by the same replication pipeline. This sharing is what eliminates the two-system problem — there is no seam between the broker and the database because they are the same system.

### Layer 3: Distributed Cluster

For workloads that exceed a single node, MQDB clusters distribute data across multiple nodes using 256 fixed partitions with a replication factor of 2. Raft consensus manages partition assignments — which node is the primary for each partition, and which node holds the replica. Data replication is asynchronous for throughput, while Raft provides strong consistency for the cluster control plane.

Inter-node communication uses direct QUIC streams with mTLS authentication. Each node exposes two endpoints: one for MQTT client connections and one for cluster traffic at a configurable offset (default +100, so a node bound to port 1883 listens for cluster traffic on 1983). Cross-node pub/sub routing works transparently — a subscriber on Node B receives messages published on Node A without either client being aware of the cluster topology.

### Three Deployment Modes

These three layers map to three deployment modes:

**Library.** Import `mqdb` as a Rust crate and use the `Database` API directly. No network, no broker, no cluster. The database runs in-process with pluggable storage backends: an LSM-tree (Fjall) for production, an in-memory backend for WASM, and an async backend for network-attached storage. This mode suits embedded applications, WASM deployments, and unit testing.

```rust
let db = Database::open("./data/mydb").await?;
let user = json!({"name": "Alice", "email": "alice@example.com"});
let created = db.create("users".into(), user).await?;
```

**Standalone Agent.** A single-node process running the MQTT broker with the embedded database. Clients connect via MQTT and perform database operations through the `$DB/` topic namespace. Authentication, ACLs, and topic protection are available. This mode suits single-server deployments where MQTT integration is needed but distribution is not.

```bash
mqdb agent start --db ./data/mydb --bind 0.0.0.0:1883 --passwd passwd.txt
```

**Cluster.** Multiple nodes forming a distributed cluster. Each node runs the full broker and database stack. Raft coordinates partition ownership, QUIC transports cluster traffic, and the replication pipeline ensures data availability across node failures.

```bash
mqdb cluster start --node-id 1 --db /data/node1 --bind 0.0.0.0:1883 \
    --quic-cert server.pem --quic-key server.key --quic-ca ca.pem
```

The book focuses primarily on the cluster mode, because that is where the distributed systems challenges live. But the layered architecture means that every chapter about the database internals (storage, schemas, indexes, constraints) applies equally to all three modes.

## 1.5 Tradeoffs Acknowledged Up Front

Honesty about tradeoffs is the difference between engineering and marketing. MQDB makes deliberate sacrifices, and you should understand them before reading further.

### No SQL (Yet)

MQDB's current query interface is a set of filter operators applied to JSON documents. There is no SELECT, no JOIN, no GROUP BY, no subqueries. Filters support equality, comparison, glob patterns, null checks, and set membership — sufficient for most application queries but nowhere near the expressiveness of SQL.

The core engine was built without a query language because the primary workloads — IoT telemetry, reactive dashboards, event-driven microservices — tend to be read-by-key and subscribe-to-changes, not ad-hoc analytical queries. A filter API over MQTT was sufficient to ship.

But the architecture does not preclude SQL. The storage layer already supports secondary indexes, and the filter system already performs predicate evaluation over JSON documents. A SQL glue layer — parsing SQL into the existing filter and index primitives — is a natural next step. The unified storage model that merges messaging and database operations can just as well serve a query language on top.

### Eventual Consistency

MQDB replicates data asynchronously with sequence-based ordering and gap detection. When a client writes to the primary, the primary applies the write locally, sends it to replicas, and acknowledges the client — without waiting for replicas to confirm. Replicas apply writes in sequence order and request catchup for any gaps. This is textbook eventual consistency.

For MQDB, eventual consistency is not a compromise — it is the natural fit for a reactive data model. Clients do not query and poll for results. They subscribe to `$DB/{entity}/events/#` and receive change events as they propagate. The data arrives when it arrives. There is no "read-after-write" consistency problem because the interaction model is subscriptions, not reads. MQTT's QoS guarantees (at-least-once for QoS 1, exactly-once for QoS 2) handle delivery reliability at the protocol level, and events arrive in sequence order within each partition.

The write model is per-entity atomicity on the primary: each single-entity operation is serialized, with data and its outbox entry persisted in the same storage batch. In agent mode, the default is `Immediate` fsync — writes are durable before the client is acknowledged. In cluster mode, the default is `PeriodicMs(10)` — the storage engine flushes every 10ms, trading a small durability window on hard crash for higher write throughput. The outbox guarantees that data and change events share the same durability fate: if a crash loses the last 10ms of unfsynced writes, the corresponding outbox entries are also lost, so there are no orphaned events. A primary failure can also lose writes that were acknowledged but not yet replicated, which is an acceptable tradeoff for a system where the dominant workloads are sensor telemetry, reactive dashboards, and event-driven microservices.

For users who need stronger durability guarantees — acknowledged writes surviving node failure — the infrastructure for synchronous replication exists in the codebase (a `QuorumTracker` that returns a oneshot receiver for quorum confirmation). Sync replication is a planned opt-in upgrade, not the default.

Chapter 5 covers the replication pipeline, sequence ordering, and gap catchup in detail.

### Fixed Partition Count

MQDB uses 256 fixed partitions. This number never changes. There is no online repartitioning, no partition splitting, no dynamic scaling of the partition count.

256 was chosen as a balance between granularity and overhead. With 3 nodes, each node handles ~85 primary partitions — enough for balanced distribution. With 16 nodes, each handles 16. With a replication factor of 2, each partition requires two distinct nodes (one primary, one replica), yielding 512 total role assignments. Beyond ~256 nodes, some nodes would have no primary partitions; beyond ~512, some nodes would have no role at all.

The fixed count is a deliberate tradeoff between horizontal scaling and synchronization overhead. Every heartbeat carries a 256-bit bitmap of partition assignments. Raft consensus tracks a fixed-size partition map. Rebalancing moves whole partitions between nodes. All of this is O(256) — constant, regardless of data size. Dynamic partition splitting (as in CockroachDB's ranges or DynamoDB's automatic splitting) would let MQDB scale beyond 256 nodes, but the partition map, heartbeat protocol, and rebalancing logic would all grow with the data, not just the cluster size. For the target deployment — 3 to 16 node clusters — 256 fixed partitions provide fine-grained distribution with bounded control plane overhead.

### No Cross-Partition Transactions

MQDB provides per-entity atomicity within a single partition. In agent mode, each create, update, or delete writes data, indexes, and the outbox entry in a single batch commit to the LSM-tree. In cluster mode, each partition's data is persisted to fjall before updating in-memory stores, with the change event outbox entry written atomically in the same batch. On crash recovery, pending outbox entries are scanned and replayed. The outbox guarantees consistency between data and change events regardless of fsync timing, since both share the same fjall batch. In neither mode is there a multi-entity transaction, a multi-partition transaction protocol, two-phase commit, or distributed MVCC.

Constraint enforcement crosses partition boundaries but is not transactional. Unique constraints use a two-phase reservation protocol (reserve with TTL, then commit or release). Foreign key constraints use a one-phase existence check on create/update and a scatter-gather reverse lookup on delete. Both protocols have a window between the check and the write where concurrent operations can create inconsistencies — the lock-drop/reacquire gap discussed in Chapter 15.

This is the same tradeoff made by many distributed databases. Google Spanner provides cross-partition transactions through synchronized clocks. CockroachDB uses a distributed transaction protocol. Both pay for it in latency and complexity. MQDB currently prioritizes throughput over cross-partition consistency. But the constraint enforcement infrastructure — two-phase reservation for unique constraints, scatter-gather for foreign key checks — already demonstrates cross-partition coordination with well-defined consistency windows. A formal distributed transaction protocol is a possible future addition, built on the same inter-partition messaging primitives.

### Write Amplification for Broadcast Entities

Broadcast entities trade write cost for read locality. The cost depends on the subscription type: an exact topic subscription generates 1 `ReplicationWrite` plus a lightweight cluster broadcast message. A wildcard subscription generates 1 `ReplicationWrite` plus 256 local persistence writes (to rebuild the wildcard trie on restart) plus a cluster broadcast message. In both cases, every node maintains a complete subscriber map so that publish routing requires zero network round trips.

For workloads with relatively stable subscriptions (subscribe once, receive many messages), this cost is amortized. For workloads with rapidly churning wildcard subscriptions, the local persistence writes can become a bottleneck.

## What Comes Next

The rest of this book follows the construction of MQDB from the storage layer up through the distributed cluster. Each chapter pairs a distributed systems concept with its implementation, including the bugs we found and the designs we revised.

Part I continues with the storage foundation (Chapter 2) and the MQTT protocol mapping (Chapter 3).

Part II covers distribution: partitioning, replication, Raft consensus, the transport layer evolution from MQTT bridges to QUIC, cross-node pub/sub routing, and distributed query coordination.

Part III addresses the cluster lifecycle: failure detection, rebalancing, session management, the message processing pipeline, and the wire protocol.

Part IV covers advanced patterns: distributed unique constraints, consumer groups, performance benchmarking methodology, and security architecture.

Part V discusses operations and the WASM deployment target.

Every chapter follows the same structure: the problem, the design options considered, the implementation chosen, what went wrong during development, and the lessons extracted. The bugs are not embarrassments to be hidden. They are the most instructive part of the story. A race condition in partition rebalancing, a missing function call that prevented Raft log replication, a batch-flush bottleneck that caused false death detection — each reveals a principle that applies far beyond MQDB.

The code is in Rust, but the concepts are language-agnostic. If you have built services in any systems language, you will follow the logic. Rust-specific features (ownership, lifetimes, async traits) are explained when they first appear.

Let's start with what lives at the bottom of the stack: the storage engine.
