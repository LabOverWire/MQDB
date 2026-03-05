# Chapter 1: Why Unify Messaging and Storage?

Every distributed application needs two things: a place to store state and a way to propagate changes. In practice, this means a database and a message broker. The database holds truth. The broker moves it around.

These two systems have evolved independently for decades. Relational databases perfected transactions and consistency. Message brokers — from IBM MQ to RabbitMQ to Apache Kafka — perfected asynchronous delivery and decoupling. The division of labor seems natural: one system stores, the other streams.

But the seam between them is where the hardest problems live.

This book follows the design and implementation of MQDB, a distributed reactive database that speaks MQTT natively and treats storage and messaging as a single concern. Every decision, bug, and tradeoff comes from building it. It shows the gap between concept and implementation, and the process of bridging them.

## 1.1 The Two-System Problem

Consider a straightforward scenario. In order for an e-commerce service to process an order, it needs to do the following:

1. Write the order to the database.
2. Publish an "order created" event so that the inventory service, the billing service, and the notification service can react.

Simple enough, but trying to make these operations atomic becomes a huge cooperative challenge.

### Dual Writes

The naive approach writes to both systems independently:

```
begin_transaction();
  database.insert(order);
  broker.publish("orders.created", order);
commit();
```

Since the database and the broker are separate systems with separate failure modes, this fails. If the database write succeeds but the broker publish fails, the order exists without anyone knowing about it. If the broker publish succeeds but the database write rolls back, downstream services react to an order that never existed.

You cannot wrap these two independent systems in a single atomic transaction without a distributed transaction protocol, which adds coordinator overhead, lock contention across systems, and partial-failure recovery complexity.

### Change Data Capture

One popular mitigation is Change Data Capture (CDC). It works by letting the application write to the database only. A separate process — Debezium tailing the PostgreSQL WAL, or Maxwell reading the MySQL binlog — picks up committed changes and publishes them to the broker.

CDC eliminates dual writes. The database is the single source of truth, and events are derived from committed state. But CDC introduces its own challenges:

**Latency.** Events arrive after the transaction commits, after the WAL flushes, after the CDC connector polls. In high-throughput scenarios, the lag between a write and its corresponding event can stretch from milliseconds to seconds.

**Schema coupling.** The CDC connector reads the database's physical representation — column types, table structures, internal row formats. Changes to the database schema can break the connector silently.

**Operational complexity.** CDC adds a new stateful component to your infrastructure. The connector must track its position in the log, handle schema changes, manage backfill for new consumers, and recover from failures. Debezium alone requires Kafka Connect, Zookeeper (or KRaft), and careful configuration of snapshot modes.

**Semantic gap.** The database sees rows and columns. The application thinks in domain events. CDC bridges them, but the translation is lossy. A row update doesn't carry the intent behind the change. Was this a price correction, a status transition, or a bulk migration? The downstream consumer must infer meaning from data.

### The Outbox Pattern

The outbox pattern improves on CDC by making events explicit. The application writes both the business data and the event into the _same_ database transaction:

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

But what if the premise is wrong? What if a single system could store data _and_ stream changes, atomically, with no relay, no connector, no synchronization gap?

This is the question MQDB tries to answer.

## 1.2 The Core Insight

At the storage layer, a database write and a message broker subscription are structurally identical operations. Both are keyed writes to partitioned stores. They differ in lifetime — a subscription persists across sessions while a record may be ephemeral — and in access patterns. But the storage mechanics are the same: serialize a value, compute a partition, write to a key.

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

> Given a key, write a value. Updates overwrite existing keys. Deletes write tombstones or remove keys. Reads are the only operation that does not modify storage. Everything else is a write.

This is the core insight. If both database records and broker state are keyed writes, then the machinery for managing them — partitioning, replication, consistency, querying — can be unified.

### ReplicationWrite: The Universal Mutation

MQDB makes this concrete through a single data structure called `ReplicationWrite`. Every mutation in the system — whether it originates from a database INSERT or an MQTT SUBSCRIBE — flows through this structure:

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

This unification means that when the database writes a record, the MQTT broker already knows about it — because the write flowed through the same event system that manages subscriptions. There is no separate CDC connector, no outbox table, no relay process. The write _is_ the event.

### Two Classes of Entities

Not all data behaves the same way. MQDB distinguishes between two classes:

**Partitioned entities** follow standard hash-based distribution. A database record with key `users/user-42` hashes to partition `hash("users/user-42") % 256`. Only the node that owns that partition stores the primary copy. Sessions, subscriptions, QoS state, retained messages, and user-defined database records are all partitioned entities.

**Broadcast entities** must exist on every node. When a message arrives on Node A, the node must immediately determine which clients — potentially connected to Node B or Node C — subscribe to that topic. Cross-node queries on every publish destroys throughput (believe me, we tried). So MQDB replicates certain entities to all nodes: the topic-to-subscriber index, the wildcard subscription trie, the client-to-node location map, and schema and constraint definitions.

Chapter 4 covers partitioning in detail. Chapter 8 explains why broadcast entities are necessary for cross-node pub/sub routing.

## 1.3 MQTT as the Unifying Protocol

The decision to use MQTT as the transport protocol may seem unusual for a database. MQTT is an IoT messaging protocol — designed for constrained devices, sensor networks, and telemetry. Why use it as a database interface?

### What MQTT 5.0 Provides

MQTT 5.0 is not the lightweight protocol people remember from early IoT deployments. Version 5.0 added features that make it surprisingly suitable as a general-purpose application protocol:

**Request/Response.** MQTT 5.0 introduced response topics and correlation data, enabling synchronous-style queries over an asynchronous protocol. A client subscribes to a response topic, then publishes a database query to `$DB/users/list` with that response topic attached. The server processes the query and publishes the result back to the response topic. From the client's perspective, this behaves like a synchronous API call.

**Shared Subscriptions.** Multiple clients can share a subscription, with the broker distributing messages among them. This enables load-balanced event processing without an external coordination layer.

**User Properties.** Arbitrary key-value metadata can be attached to any message. This enables headers for correlation IDs, content types, authentication tokens, and operation metadata.

**Session Expiry.** Persistent sessions with configurable lifetimes enable durable subscriptions that survive client disconnections and reconnections.

**Topic Aliases.** Compact representations for frequently-used topics, reducing wire overhead for high-frequency operations.

### The $DB/ Namespace

MQDB maps database operations to MQTT topics under a reserved `$DB/` prefix:

| Topic Pattern              | Operation                 |
| -------------------------- | ------------------------- |
| `$DB/{entity}/create`      | Create a record           |
| `$DB/{entity}/{id}`        | Read a record             |
| `$DB/{entity}/{id}/update` | Update a record           |
| `$DB/{entity}/{id}/delete` | Delete a record           |
| `$DB/{entity}/list`        | List records with filters |
| `$DB/{entity}/events/#`    | Subscribe to changes      |

A client creating a user record publishes JSON to `$DB/users/create` with a response topic. The server creates the record, assigns an ID, and publishes the result (or an error) back to the response topic. This design means that any MQTT client — in any language, on any platform — can perform database operations without a custom driver. The `mosquitto_pub` and `mosquitto_sub` command-line tools work. MQTT client libraries for Python, JavaScript, Go, Java, C, and every other language work. The database API is the messaging API.

### Why Not REST, gRPC, or a Custom Protocol?

Every protocol choice is a tradeoff. Here is what MQTT provides that alternatives don't, and what it sacrifices:

**Versus REST/HTTP:** MQTT maintains persistent connections. A client connects once and can issue many operations without the overhead of TCP handshake, TLS negotiation, and HTTP framing per request. MQTT's binary framing is more compact than HTTP headers. And the subscription model — where the server pushes changes to the client — is native to MQTT but requires workarounds (WebSockets, Server-Sent Events, long polling) in HTTP.

**Versus gRPC:** gRPC provides strong typing through Protocol Buffers and efficient binary encoding. But gRPC requires code generation, language-specific stubs, and HTTP/2 framing. MQTT clients exist in every language, including embedded C for microcontrollers. MQDB's target includes constrained devices that can run an MQTT client but not a gRPC runtime.

**Versus a custom protocol:** A purpose-built protocol could optimize for the exact needs of a distributed database. But it would require building and maintaining client libraries for every target language and platform. By using MQTT, MQDB inherits a mature ecosystem of clients, debugging tools, and operational knowledge.

**What MQTT sacrifices:** Type safety at the protocol level (payloads are opaque byte arrays, not typed schemas), SQL-style query expressiveness requires a dedicated translation layer, and developer familiarity (most backend engineers know SQL; fewer know MQTT topic patterns).

MQDB compensates through schema validation at the application layer (Chapter 2), a filter system (Chapter 9), and comprehensive CLI tooling that abstracts the MQTT protocol from day-to-day use.

## 1.4 What We're Building

MQDB is three things layered on top of each other:

### Layer 1: Reactive Database

At its core, MQDB is a key-value database with a document model. Records are JSON objects stored in named entities (collections). The database supports schemas with type validation, unique constraints (single and composite), foreign keys with cascade/restrict/set-null policies, not-null constraints, secondary indexes, and filter operators for queries.

What makes it reactive is that every write — create, update, delete — produces an observable event. Clients subscribe to entity changes using wildcard patterns (`users/#` catches all user changes, `+/123` catches changes to ID 123 across all entities). Subscriptions can be broadcast (all consumers receive all events), load-balanced (round-robin across a consumer group), or ordered (partition-sticky routing so events for the same key always reach the same consumer).

The reactive core also includes an outbox with configurable retry logic and a dead letter queue. This ensures that events derived from writes survive crashes without a separate relay process — because events _are_ writes.

### Layer 2: MQTT Broker

The database is embedded inside a full MQTT 5.0 broker. For this, a purpose-built MQTT broker was used. In order to truly embed transactions within the MQTT pipeline, one must intercept MQTT events as they are processed by the broker. The advantage is that the protocol is the still MQTT, so standard MQTT clients connect, subscribe to topics, publish messages, and use QoS 0/1/2 delivery guarantees. The broker supports TLS, QUIC transport, password-based authentication (Argon2), SCRAM-SHA-256 challenge-response, JWT tokens (symmetric and asymmetric), and federated JWT with multiple issuers. These are necessary if the premise is to flatten the stack into a single system.

The broker and the database share a single storage layer. MQTT session state (subscriptions, QoS inflight messages, retained messages) and database records live in the same key-value store, managed by the same replication pipeline. This sharing is what eliminates the two-system problem.

### Layer 3: Distributed Cluster

For workloads that exceed a single node, MQDB clusters distribute data across multiple nodes using 256 fixed partitions with a replication factor of 2. Raft consensus manages partition assignments — which node is the primary for each partition, and which node holds the replica. Data replication is asynchronous for throughput, while Raft provides strong consistency for the cluster control plane.

Inter-node communication uses direct QUIC streams with mTLS authentication. Each node exposes two endpoints: one for MQTT client connections and one for cluster traffic at a configurable offset (default +100, so a node bound to port 1883 listens for cluster traffic on 1983). Cross-node pub/sub routing works transparently — a subscriber on Node B receives messages published on Node A without either client being aware of the cluster topology.

### Three Deployment Modes

These three layers map to three deployment modes:

**Library.** Import the crate and use the database API directly. No network, no broker, no cluster. The database runs in-process with pluggable storage backends: an LSM-tree (Fjall) for production, an in-memory backend for tests and WASM, and an encrypted backend for at-rest encryption. This mode suits embedded applications, WASM deployments, and unit testing.

```rust
let db = Database::open("./data/mydb").await?;
let user = json!({"name": "Alice", "email": "alice@example.com"});
let created = db.create("users".into(), user, None, None, &ScopeConfig::default()).await?;
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

## 1.5 The Flattening Movement

MQDB is not the first system to ask whether the layers between data and user are all necessary. A growing number of projects share the premise that the traditional stack — CDN, load balancer, reverse proxy, API gateway, application server, ORM, cache, message queue — exists because of historical contingency, not fundamental necessity. Each layer patches a problem created by the commitment to HTTP and request/response. What if you started from a different foundation?

### The Reactive Database Pioneers

**RethinkDB** was the first open-source database designed specifically to push data to applications in real time. Launched in 2012, it introduced changefeeds — any query could become a live subscription with `.changes()`, streaming diffs as they happened. The technical vision was sound. The company shut down in October 2016 because "correct real-time" was a hard sell when the market rewarded operational familiarity. The CNCF purchased the source code and donated it to the Linux Foundation under the Apache 2.0 license. The post-mortem, written by co-founder Slava Akhmechet, is worth reading: RethinkDB optimized for elegance and correctness when the market valued ease of adoption.

**Meteor** (2012–2016) took the idea further by unifying the reactive pipeline end to end. Its Distributed Data Protocol (DDP) used publish/subscribe over WebSockets, with a local MiniMongo replica on the client that stayed in sync automatically. The developer experience was genuinely magical — applications "just worked" in real time. Meteor faded as the React ecosystem matured and its MongoDB dependency became a constraint, but it proved that a protocol-native reactive system could eliminate entire categories of glue code.

Both RethinkDB and Meteor demonstrated demand for reactive data systems. Both also showed the fragility of adding reactivity on top of a storage engine that was not designed for it — RethinkDB's changefeeds were a layer above its B-tree storage, and Meteor's DDP depended on MongoDB's oplog, which was built for replication, not application-level subscriptions.

### The Local-First Wave

A more recent wave inverts the architecture entirely: instead of making the server push data to the client, make the client hold its own data and sync in the background.

**CouchDB and PouchDB** are the earliest implementation of this pattern. CouchDB's multi-master replication treats every node — server, phone, browser — as a peer. The Couch Replication Protocol runs over HTTP, and PouchDB implements it in JavaScript. The architecture is genuinely flat: all reads and writes go to the local database, and the only data exchange between client and server happens through replication. The limitation is that CouchDB never unified messaging with storage — it is a database that can sync, not a messaging system that can persist.

**ElectricSQL** comes from researchers who co-invented CRDTs (Marc Shapiro and Nuno Preguiça are on the team). It syncs a subset of Postgres to a client-side database — originally SQLite-WASM, now PGlite (a WASM build of Postgres itself). The cloud becomes a sync conduit rather than the source of truth. ElectricSQL flattens by extending Postgres; MQDB is more radical in that the data model is pub/sub-native from the start.

**Zero** (from Rocicorp, the Replicache team) distributes the backend all the way to the UI's main thread. Client-side queries run against a local cache and return results in the next frame. Mutations flow back to Postgres through a sync server. The developer experience is that of an embedded database, but Postgres remains the source of truth.

**Ditto** is the closest spiritual cousin to MQDB in the physical world. Devices form ad-hoc mesh networks over Bluetooth, peer-to-peer WiFi, and LAN, using CRDTs for conflict resolution. It is cloud-optional — a "Big Peer" server component exists but is not required for device-to-device sync. Ditto has real production deployments (Chick-fil-A's POS systems, airline operations for Delta and Japan Airlines) and raised $82M in Series B funding in 2025. The difference from MQDB: Ditto is transport-agnostic targeting mobile and edge; MQDB is MQTT-native targeting a unified protocol from sensor to browser to server cluster.

All of these converge on one idea: the UI should be a reactive function of local state, and network sync should be an implementation detail.

### The Server-Side Collapse

**Phoenix LiveView** attacks the problem from the opposite direction. Instead of moving the database closer to the client, it moves the UI closer to the server. LiveView renders HTML on the server and pushes DOM diffs to the browser over a persistent WebSocket connection. The result is dramatic layer elimination: no REST API, no GraphQL, no client-side state management, no JavaScript build tooling. The entire application collapses into one language (Elixir) on one side. The approach works because the BEAM VM handles millions of lightweight processes, making a stateful WebSocket per user viable. LiveView has spawned a genre of server-rendered reactive frameworks: Hotwire (Ruby), Laravel Livewire (PHP), Blazor Server (.NET).

The tradeoff is that server-rendered UIs have no offline story, latency-dependent UX, and no path to IoT or edge devices. LiveView proves the appetite for stack flattening is enormous, but it flattens in only one direction.

### The Multi-Model Newcomers

**SurrealDB** is a Rust-based multi-model database — document, graph, relational, vector search, time-series — in a single binary with built-in authentication and row-level permissions. Its `LIVE SELECT` statement creates persistent subscriptions that push changes over WebSocket, and it runs in WASM. SurrealDB flattens by absorbing multiple database paradigms into one system, but it speaks its own RPC protocol over WebSocket/HTTP, meaning every client needs a SurrealDB SDK.

**Convex** (open-sourced in 2024) takes the position that queries should be TypeScript code running inside the database. Like React components reacting to state changes, Convex queries re-execute when their underlying data changes, with transactional consistency across the entire reactive pipeline. Convex supports web, Node.js, and React Native, but has no IoT or embedded story and no protocol-level integration.

### Where This Leaves MQDB

The landscape reveals a pattern. Every project picks a subset of the problem:

| Project          | What it flattens             | What it misses                                 |
| ---------------- | ---------------------------- | ---------------------------------------------- |
| RethinkDB        | Database-to-application push | Protocol, offline, IoT                         |
| CouchDB/PouchDB  | Sync, offline-first          | Messaging, real-time push                      |
| Meteor/DDP       | Full reactive stack          | Offline, IoT, performance at scale             |
| ElectricSQL      | Postgres-to-client sync      | Messaging, IoT, protocol ubiquity              |
| Zero             | Backend-to-UI-thread         | IoT, embedded, messaging                       |
| Ditto            | Device-to-device mesh        | Web, server clusters, protocol standardization |
| Phoenix LiveView | Frontend/backend boundary    | Offline, IoT, client-side state                |
| SurrealDB        | Multi-model + live queries   | Protocol ubiquity, messaging                   |
| Convex           | Backend + reactive queries   | IoT, embedded, protocol                        |

MQDB's position in this landscape comes from a single bet: MQTT as the unifying protocol. Every other project either invents a custom protocol (SurrealDB's RPC, Meteor's DDP, Ditto's mesh protocol) or bolts sync onto an existing database protocol (ElectricSQL and Zero extending Postgres). MQTT is already spoken by billions of devices, has mature client libraries in every language including embedded C, and its topic-based publish/subscribe maps naturally to both database subscriptions and message routing.

This bet has four consequences. First, any MQTT client is already a database client — no custom SDK required. Second, the topic hierarchy (`users/{userId}/#`) maps directly to access control without a separate authorization layer. Third, the protocol works on microcontrollers, in browsers via WebSocket, and on server clusters. Fourth, existing IoT infrastructure — sensors, gateways, edge devices already speaking MQTT — gains database capabilities without a protocol migration.

ElectricSQL and Zero flatten the web stack. Ditto flattens the mobile and edge stack. MQDB flattens the IoT, web, and edge stack with a single protocol.

## 1.6 Tradeoffs Acknowledged Up Front

Honesty about tradeoffs is the difference between engineering and marketing. MQDB makes deliberate sacrifices, and you should understand them before reading further.

### No SQL (Yet)

The core engine was built without a query language because the primary workloads — IoT telemetry, reactive dashboards, event-driven microservices — tend to be read-by-key and subscribe-to-changes, not ad-hoc analytical queries. A filter API over MQTT provides a simple and efficient way to filter and transform data.

But the architecture does not preclude SQL. The storage layer already supports secondary indexes, and the filter system already performs predicate evaluation over JSON documents. A SQL glue layer — parsing SQL into the existing filter and index primitives — is a natural next step, and maybe the contents of the next book. The unified storage model that merges messaging and database operations can just as well serve a query language on top.

### Eventual Consistency

MQDB replicates data asynchronously. The primary acknowledges the client without waiting for replicas to confirm. This is eventual consistency, and it fits a reactive data model: clients subscribe to change events rather than polling, so the data arrives when it arrives. Chapter 4 explains why replica staleness is less of a concern than it sounds, and Chapter 5 covers the replication pipeline in detail.

For users who need stronger durability guarantees — acknowledged writes surviving node failure — the infrastructure for synchronous replication exists in the codebase (a quorum tracker that returns a notification channel for quorum confirmation). Sync replication is a planned opt-in upgrade, not the default.

Chapter 5 covers the replication pipeline, sequence ordering, and gap catchup in detail.

### Fixed Partition Count

MQDB uses 256 fixed partitions. This number never changes. There is no partition splitting, and no dynamic scaling of the partition count.

256 was chosen as a balance between granularity and overhead. With 3 nodes, each node handles ~85 primary partitions — enough for balanced distribution. With 16 nodes, each handles 16. With a replication factor of 2, each partition requires two distinct nodes (one primary, one replica), yielding 512 total role assignments. Beyond ~256 nodes, some nodes would have no primary partitions; beyond ~512, some nodes would have no role at all.

The fixed count is a deliberate tradeoff between horizontal scaling and synchronization overhead. Every heartbeat carries a 256-bit bitmap of partition assignments. Raft consensus tracks a fixed-size partition map. Rebalancing moves whole partitions between nodes. All of this is O(256) — constant, regardless of data size. Dynamic partition splitting (as in CockroachDB's ranges or DynamoDB's automatic splitting) would let MQDB scale beyond 256 nodes, but the partition map, heartbeat protocol, and rebalancing logic would all grow with the data, not just the cluster size. For the target deployment — 3 to 16 node clusters — 256 fixed partitions provide fine-grained distribution with bounded control plane overhead.

### No Cross-Partition Transactions

MQDB provides per-entity atomicity within a single partition. In agent mode, each create, update, or delete writes data, indexes, and the outbox entry in a single batch commit to the LSM-tree. In cluster mode, each partition's data is persisted to fjall before updating in-memory stores, with the change event outbox entry written atomically in the same batch. On crash recovery, pending outbox entries are scanned and replayed. The outbox guarantees consistency between data and change events regardless of fsync timing, since both share the same fjall batch.

Constraint enforcement (unique checks, foreign key validation) crosses partition boundaries but is not transactional — there is a window between the check and the write where concurrent operations can create inconsistencies. Chapter 15 covers the protocols and their consistency gaps.

This is the same tradeoff made by many distributed databases. Google Spanner provides cross-partition transactions through synchronized clocks. CockroachDB uses a distributed transaction protocol. Both pay for it in latency and complexity. MQDB currently prioritizes throughput over cross-partition consistency. But the constraint enforcement infrastructure — two-phase reservation for unique constraints, scatter-gather for foreign key checks — already demonstrates cross-partition coordination with well-defined consistency windows. A formal distributed transaction protocol is a possible future addition, built on the same inter-partition messaging primitives.

### Write Amplification for Broadcast Entities

Broadcast entities trade write cost for read locality. The cost depends on the subscription type: an exact topic subscription generates 1 `ReplicationWrite` plus a lightweight cluster broadcast message. A wildcard subscription generates one replication write plus 256 local persistence writes plus a cluster broadcast message. In both cases, every node maintains a complete subscriber map so that publish routing requires zero network round trips.

Chapter 4 quantifies this cost and explains when it matters.

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
