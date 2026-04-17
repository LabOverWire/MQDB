# 2. Background and Related Work

## 2.1 MQTT Protocol Essentials

MQTT (Message Queuing Telemetry Transport) [mqtt5] is a publish/subscribe messaging protocol designed for constrained devices and unreliable networks. Version 5.0, ratified as an OASIS standard in 2019, introduced several features that are material to this paper's thesis. We summarize the protocol features that MQDB relies on.

**Topic hierarchy.** Messages are published to hierarchical topics delimited by `/` (e.g., `sensors/building-A/temperature`). Each segment carries semantic meaning, and the depth is unbounded. Subscribers specify topic filters that may include two wildcard characters: `+` matches exactly one segment, and `#` matches any number of trailing segments. The topic hierarchy is the protocol's primary addressing mechanism — it determines which subscribers receive which messages.

**Quality of Service.** MQTT defines three delivery guarantees: QoS 0 (at most once — fire and forget), QoS 1 (at least once — acknowledged delivery), and QoS 2 (exactly once — four-step handshake). These guarantees govern message delivery between client and broker, not end-to-end between publisher and subscriber.

**Retained messages.** A message published with the retain flag is stored by the broker and delivered to any future subscriber on that topic. This provides a "last known value" semantic per topic.

**Request-response (MQTT 5.0).** A publisher may include a *response topic* and *correlation data* in its message properties. The recipient publishes its reply to the response topic with matching correlation data, enabling synchronous request-response patterns over the asynchronous pub/sub substrate. This feature is critical for MQDB's CRUD interface.

**Shared subscriptions.** Multiple subscribers to `$share/group/topic` receive messages in a round-robin fashion rather than each receiving a copy. This enables consumer-group semantics similar to Kafka.

## 2.2 Messaging-Storage Convergence in Practice

Several systems have moved toward unifying MQTT messaging with data storage. We categorize them by the direction of their approach: messaging systems adding persistence, or databases adding MQTT interfaces.

### 2.2.1 Brokers Adding Persistence

**EMQX Durable Storage.** EMQX 6.0 [emqx-ds] adds persistent message storage using RocksDB with Raft-based replication. Messages are stored as (topic, timestamp, value) triples, and the system supports replay from stored streams. The storage layer is subordinate to the messaging function: it provides durable message delivery, not database query semantics. There is no CRUD interface, no record identity beyond topic-timestamp pairs, and no query language. The design explicitly frames persistence as a reliability feature for messaging rather than a database capability.

**FairCom MQ.** FairCom MQ embeds a JSON and SQL database within a C++ MQTT broker supporting versions 3.1, 3.1.1, and 5.0. The database provides store-and-forward reliability — messages are persisted for delivery guarantees — and supports SQL queries against stored messages. The unification is operational (the database serves the broker's reliability needs) rather than architectural (the topic hierarchy does not define the data model).

### 2.2.2 Databases Adding MQTT Interfaces

**Harper.** Harper (formerly HarperDB) [harper] is the system closest to MQDB's thesis. It maps database tables to MQTT topic namespaces: publishing to a topic with the retain flag performs an upsert, and subscribing to a topic pattern receives change notifications. The system uses LMDB for storage, provides eventual consistency without a consensus protocol, and supports QoS 0 and 1. Harper validates the intuition that MQTT topics can serve as a database interface, but it does not provide a formal analysis of the mapping, does not implement distributed consensus, and has no published academic evaluation. To our knowledge, no peer-reviewed paper describes Harper's architecture.

**Machbase Neo.** Machbase Neo is a time-series database with an MQTT ingestion interface. Topics of the form `db/append/TAG` insert time-series data points. MQTT serves as a convenience protocol for data ingestion rather than the foundational data addressing mechanism — the internal model is a columnar time-series store, not a topic hierarchy.

### 2.2.3 Comparison

Table 3 summarizes the distinguishing characteristics of these systems relative to MQDB.

**Table 3.** Comparison of MQTT-database convergence approaches.

| System | Direction | CRUD interface | Consensus | Topic = address | Academic evaluation |
|---|---|---|---|---|---|
| EMQX DS | Broker → storage | No | Raft | No | No |
| FairCom MQ | Broker + DB | SQL (not topic-based) | No | No | No |
| Harper | DB → MQTT | Topic-based | No | Yes | No |
| Machbase Neo | DB → MQTT | Ingestion only | No | Partial | No |
| **MQDB** | **Unified** | **Topic-based** | **Raft** | **Yes** | **This paper** |

The critical differentiator is that MQDB is the only system where the MQTT topic hierarchy both defines the data addressing scheme and provides the distribution mechanism (partition routing, replication, constraint enforcement), backed by a formal consensus protocol.

## 2.3 Academic Work on MQTT Distribution

Longo and Redondi [longo23] present the design and implementation of a distributed MQTT broker with per-topic routing and in-band signaling for broker coordination. Their work addresses broker scalability but does not consider database semantics — the broker routes messages but does not store, query, or index them.

Shvaika et al. [tbmq] describe TBMQ, a distributed MQTT broker that uses Apache Kafka for internal message dispatch between broker nodes, Redis for session state caching, and PostgreSQL for persistent storage. The system achieves high throughput (reported scalability to 1M messages/second) but is a canonical example of the two-system problem: four separate systems (broker, Kafka, Redis, PostgreSQL) must be deployed and kept consistent.

Neither paper addresses the question of whether MQTT's topic semantics are sufficient for database operations.

## 2.4 IoT Databases with MQTT Interfaces

Apache IoTDB [iotdb-vldb20, iotdb-sigmod23] is a native time-series database with a tree-structured device namespace and a custom storage format (TsFile) optimized for time-series ingestion. It reports ingestion rates exceeding 10 million values per second. IoTDB includes an MQTT service that maps topic paths to time-series identifiers, with a configurable `PayloadFormatter` for parsing message payloads into data points.

The critical architectural difference between IoTDB and MQDB is the role of MQTT. In IoTDB, MQTT is an optional ingestion protocol — one of several ways to insert data, alongside JDBC, REST, and native clients. The internal data model is a time-series tree, not a topic hierarchy. In MQDB, the topic hierarchy *is* the data model: CRUD operations, partition routing, change notifications, and constraint enforcement are all expressed in topic semantics.

IoTDB's systems paper structure (VLDB 2020, SIGMOD 2023) illustrates the expected level of architectural description and experimental rigor for an MQTT-adjacent data system. We do not attempt a direct performance comparison with IoTDB because the workloads differ fundamentally: IoTDB targets time-series ingestion of primitive values into a tree-structured namespace, while MQDB targets general-purpose CRUD on JSON documents with field-level indexing and constraint enforcement. The two systems occupy different points in the IoT data stack and are not interchangeable.

## 2.5 Foundational Work

**Publish/subscribe systems.** The canonical survey of pub/sub systems by Eugster et al. [pubsub-survey] classifies approaches by their subscription model (topic-based, content-based, type-based) and decoupling properties (space, time, synchronization). MQDB operates in the topic-based model and leverages all three forms of decoupling. Vargas et al. [vargas05] explored integrating databases with publish/subscribe systems, proposing event-driven information sharing across database boundaries — a precursor to the convergence we observe and formalize.

**Consensus.** Raft [raft] provides the consensus protocol for MQDB's control plane (partition assignment and cluster membership). We use Raft for metadata consensus only; per-write replication is asynchronous, placing MQDB in a different consistency class than systems like CockroachDB [cockroachdb] that use consensus for every write.

**Benchmarking.** We adopt the Yahoo Cloud Serving Benchmark (YCSB) [ycsb] methodology for our CRUD evaluation, adapting its standard workloads (read-heavy, update-heavy, mixed) to the MQTT request-response interface.

## 2.6 Gap Statement

No academic work formalizes the argument that MQTT's topic hierarchy and subscription semantics are sufficient to implement a distributed database. A search of VLDB, SIGMOD, SOSP, and OSDI proceedings from 2020 to 2026 yields no papers with "MQTT" in the title. The Apache IoTDB papers are the closest academic treatment, but they address time-series storage with MQTT as an optional interface, not MQTT as a foundational data model.

Existing convergence systems (Harper, EMQX DS, FairCom MQ) unify messaging and storage pragmatically but without formal analysis of the mapping's sufficiency, its limitations, or its performance implications. This paper provides that analysis.
