# 1. Introduction

Internet of Things (IoT) applications that collect, process, and act on device data face an architectural choice that is rarely questioned: the messaging layer and the storage layer are separate systems. An MQTT broker handles device communication — publish/subscribe routing, session management, quality-of-service guarantees — while a database handles data persistence, queries, and consistency. Between the two sits application middleware: code that subscribes to broker topics, transforms payloads, writes to the database, and manages the inevitable synchronization failures between the systems that were supposed to be working together.

This paper argues that the separation is unnecessary. We show that MQTT's topic hierarchy provides a sufficient data addressing scheme for database operations, and that a single system built on this observation can serve as both messaging broker and database simultaneously.

## 1.1 The Two-System Problem

The prevailing IoT data architecture separates messaging from storage into independently operated systems. This separation is so deeply assumed that it is rarely examined, yet it creates a family of engineering problems that practitioners solve repeatedly in every deployment.

**Consistency gaps.** When a device publishes a sensor reading, the broker delivers the message to subscribers, and a bridge application writes the reading to the database. If the database write fails after the broker has delivered the message, subscribers have seen data that the database has not recorded. If the bridge application crashes between receiving the message and writing it, the data is lost — the broker has acknowledged delivery, but no persistent record exists. Maintaining consistency between broker state and database state requires exactly-once processing semantics that neither system provides in isolation.

**Deployment complexity.** ThingsBoard MQTT (TBMQ) [tbmq], a production MQTT broker, requires Apache Kafka for internal message dispatch, Redis for session caching, and PostgreSQL for persistent storage — four separate systems to deploy, configure, monitor, and upgrade. A recent edge computing study [edge-mqtt-db] describes a Siemens Industrial Edge deployment requiring Mosquitto, SQLite, and Redis within a single device. Each additional system introduces its own failure modes, configuration surface, and operational burden.

**Duplicated concerns.** Both the broker and the database maintain authentication, authorization, connection management, and ordering guarantees — but with independent implementations that do not share state. A user authenticated to the broker is not automatically authorized in the database. A topic-level access control rule in the broker has no counterpart in the database's row-level security. Maintaining coherent security policies across the two systems requires manual synchronization.

**The fundamental issue** is not that these problems are unsolvable — they are solved, repeatedly, by every team that deploys an MQTT-based IoT system. The issue is that they arise from an architectural decision that may not be necessary.

## 1.2 The Convergence Observation

Several systems have independently moved toward unifying messaging and storage, approaching from opposite directions:

**From messaging toward storage.** Apache Kafka began as a distributed log for message processing [kafka] but has progressively absorbed database-like responsibilities. KRaft (KIP-500) [kraft] replaced ZooKeeper with an internal Raft-based metadata store, eliminating an external dependency by building a database into the messaging system. EMQX, a widely deployed MQTT broker, added durable storage in version 6.0 using RocksDB and Raft-based replication [emqx-ds] — persistent, replicated message storage within the broker process.

**From storage toward messaging.** Harper (formerly HarperDB) [harper] exposes database tables as MQTT topic namespaces: publishing to a topic creates or updates a record, and subscribing to a topic receives change notifications. The system uses MQTT as both the data ingestion and query interface, though without formal analysis of the mapping or consensus-based replication.

**The direction of convergence.** These systems are approaching a common point from opposite sides. Kafka grows an internal database. EMQX adds persistent storage. Harper maps topics to tables. The boundary between messaging and storage, once assumed fundamental, is revealing itself as an artifact of historical system design rather than a necessary architectural constraint.

## 1.3 MQDB and This Paper's Thesis

MQDB makes this convergence explicit by starting from a single observation: **MQTT's topic hierarchy is not merely a message routing mechanism — it is a data addressing scheme.**

The topic `$DB/users/user-42/update` encodes four pieces of information: a database namespace (`$DB`), an entity name (`users`), a record identifier (`user-42`), and an operation (`update`). This encoding is not a convention imposed on the topic space — it is a direct consequence of MQTT's hierarchical topic structure, which naturally accommodates the kind/identity/verb pattern that database operations require.

Two additional MQTT features complete the mapping. First, MQTT 5.0's request-response pattern [mqtt5] — correlation data and response topics — transforms the publish/subscribe protocol into a synchronous CRUD interface: a client publishes a database operation and receives the result on a dedicated response topic. Second, MQTT subscriptions with wildcards provide built-in change data capture: subscribing to `$DB/users/events/#` delivers all future creates, updates, and deletes for the `users` entity, with no additional infrastructure.

A single system built on this mapping replaces the broker, database, and application bridge with one binary. Authentication is configured once, not twice. Access control covers both messaging and data operations. We make one distinction precise: the claim is not that MQTT provides transactional semantics — `PUBACK` acknowledges receipt of a publish, not commit of a database operation — but that the protocol is sufficient to *implement* them broker-side without modification or extension. Atomicity is supplied by the storage engine (a single batch commit per request covering record, indexes, and constraint reservations); the broker publishes the Request/Response reply only after that commit returns. A standard MQTT 5.0 client therefore observes atomic CRUD by using the Request/Response pattern alone, with no client-side transaction logic. Section 6.1 describes this mechanism in detail, and §6.2–§6.4 measure its cost against conventional separated baselines.

## 1.4 Contributions

This paper makes three contributions:

1. **A formal topic-to-database mapping** (Section 3). We define how MQTT topic hierarchy, request-response semantics, and subscription patterns map to CRUD operations, partition routing, and change data capture. We show that this mapping is sufficient for single-entity database operations and identify precisely where it falls short (multi-record transactions, cross-entity joins, SQL expressiveness).

2. **A distributed architecture built on this mapping** (Section 4). We describe how MQDB implements 256-partition sharding with CRC32-based routing, a Raft control plane for cluster membership and partition assignment, asynchronous replication with per-partition causal ordering, and distributed constraint enforcement — all within a single process per node. We characterize the system's consistency guarantees and compare them to Redis replication and MongoDB with `w:1`.

3. **An evaluation comparing the unified architecture against conventional separated systems** (Section 6). We measure CRUD throughput and latency, pub/sub message rates, and operational complexity, comparing MQDB against Mosquitto + PostgreSQL, Mosquitto + Redis, and REST + PostgreSQL baselines.

The remainder of the paper is organized as follows. Section 2 surveys related work in MQTT broker architecture, IoT databases, and messaging-storage convergence. Section 3 formalizes the topic-to-database mapping. Section 4 describes the system architecture. Section 5 discusses protocol-level field encryption as an architectural consequence of unification. Section 6 presents the evaluation. Section 7 discusses applicability, consistency trade-offs, and limitations. Section 8 concludes.
