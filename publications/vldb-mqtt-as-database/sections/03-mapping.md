# 3. The Topic-to-Database Mapping

The central claim of this paper is that MQTT's topic hierarchy provides a sufficient addressing scheme for database operations. In this section, we formalize this mapping and show how two distinct MQTT mechanisms — request-response and subscriptions — cover the full spectrum of database interaction patterns: synchronous CRUD and asynchronous change notification.

## 3.1 Topic Hierarchy as Data Addressing

MQTT organizes messages into a hierarchical topic space delimited by forward slashes. Each segment carries independent semantic meaning, and the protocol's subscription model allows filtering at any level using single-level (`+`) and multi-level (`#`) wildcards [mqtt5]. MQDB reserves the prefix `$DB/` as a database namespace and interprets subsequent topic segments as database addressing components.

**Definition 1 (Topic-to-Record Mapping).** An MQTT topic of the form `$DB/E/K/O`, where `E` is an entity name, `K` is a record identifier, and `O` is an operation verb, maps to a database operation on record `K` in collection `E`. Formally:

- **Entity** `E` ↔ topic level 2: a named collection (analogous to a table or document collection).
- **Record identity** `K` ↔ topic level 3: a unique key within `E`.
- **Operation** `O` ↔ topic level 4: one of {`create`, `update`, `delete`}.
- **Payload** ↔ MQTT message body: a JSON document containing the record data.

Two operations use shorter topic paths. A read operation is expressed as `$DB/E/K` (two levels, no operation suffix) — the absence of an operation verb signals retrieval. A list operation is expressed as `$DB/E/list`, where `list` is a reserved keyword rather than a record identifier.

Table 1 shows the complete mapping from MQTT topics to database operations as implemented in MQDB.

**Table 1.** Topic-to-operation mapping.

| MQTT topic | Operation | Payload semantics |
|---|---|---|
| `$DB/E/create` | INSERT | JSON document; server assigns ID |
| `$DB/E/K` | SELECT | Empty or projection spec |
| `$DB/E/K/update` | UPDATE | JSON fields to modify |
| `$DB/E/K/delete` | DELETE | Empty |
| `$DB/E/list` | LIST | Filter, sort, and pagination spec |

This mapping is total for CRUD operations: every topic matching the pattern `$DB/E/...` resolves unambiguously to exactly one database operation. Topics that do not match are passed through as standard MQTT pub/sub messages, preserving full backward compatibility with MQTT clients that do not use the database features.

The mapping constrains the key space as a consequence of embedding keys in MQTT topics. Entity names are restricted to alphanumeric characters, underscores, and hyphens (maximum 128 characters). Record identifiers must not contain MQTT wildcard characters (`+`, `#`) or the path separator (`/`), as these have reserved meaning in the topic hierarchy (maximum 512 characters). Payloads are limited to 4 MiB. Entity names beginning with an underscore are reserved for internal operations.

These constraints partition the topic space into three non-overlapping regions: database operations (`$DB/E/...` where `E` does not begin with `_`), internal operations (`$DB/_*/...`), and standard MQTT messaging (everything else).

## 3.2 Two Mechanisms for Database Interaction

Database clients require two interaction patterns: synchronous operations (query, get a response) and asynchronous notifications (subscribe, receive updates). MQDB maps these to two distinct MQTT mechanisms rather than overloading one.

### 3.2.1 Request-Response for Synchronous CRUD

MQTT 5.0 introduced request-response semantics [mqtt5, §4.10]: a client publishes a message with two additional properties — a *response topic* and *correlation data*. The receiver processes the request and publishes the result to the response topic, echoing the correlation data so the client can match responses to requests.

MQDB uses this mechanism for all CRUD operations. A client wishing to create a record publishes a JSON document to `$DB/users/create` with response topic `reply/client-1` and an opaque correlation identifier. The broker's topic handler intercepts the publish, parses the topic (Definition 1), executes the database operation against the embedded storage engine, and publishes the result — including the server-assigned record ID — to `reply/client-1` with matching correlation data.

This transforms the publish/subscribe protocol into a request-response database interface without any protocol extension. The client experience is indistinguishable from a synchronous database call: send request, receive response. Standard MQTT client libraries suffice; no custom driver is required.

The sequence for each CRUD operation:

1. **Create**: Client publishes to `$DB/E/create`. Server generates a UUID, stores the record, and responds with the complete record including the assigned `id` field.
2. **Read**: Client publishes to `$DB/E/K`. Server retrieves the record and responds with its current state. The payload may include a projection specification to limit returned fields.
3. **Update**: Client publishes to `$DB/E/K/update`. Server merges the provided fields into the existing record and responds with the updated state.
4. **Delete**: Client publishes to `$DB/E/K/delete`. Server removes the record and responds with a confirmation.
5. **List**: Client publishes to `$DB/E/list`. The payload contains filter predicates, sort directives, and pagination parameters. Server responds with the matching records.

All responses include a status field and the operation result. Error conditions (record not found, validation failure, constraint violation) are returned as structured error responses on the same response topic — the client always receives exactly one response per request.

### 3.2.2 Subscriptions as Change Data Capture

The second database interaction pattern — receiving notifications when data changes — maps directly to MQTT subscriptions. When a record is created, updated, or deleted, the system publishes a change event to the topic `$DB/E/events/K`, where `E` is the entity and `K` is the record identifier. The event payload is a JSON document containing:

```json
{
  "sequence": 42,
  "entity": "users",
  "id": "user-42",
  "operation": "created",
  "data": {"name": "Alice", "email": "alice@example.com"}
}
```

The `sequence` field provides a monotonically increasing counter for event ordering. The `operation` field takes one of three values: `created`, `updated`, or `deleted`. The `data` field contains the full record state after the operation (or the deleted record's last state).

MQTT's wildcard subscriptions provide natural granularity control over the change stream:

**Table 2.** Subscription patterns and their CDC equivalents.

| MQTT subscription | CDC equivalent |
|---|---|
| `$DB/users/events/user-42` | Changes to a specific record |
| `$DB/users/events/#` | All changes in entity `users` |
| `$DB/+/events/#` | All changes across all entities |

This is a direct consequence of MQTT's topic hierarchy: the wildcard `+` matches any single segment (any entity), and `#` matches any trailing segments (any record within an entity). No custom subscription protocol is needed — the broker's standard subscription matching delivers exactly the right events to each subscriber.

An important property of this mapping: subscriptions deliver *future* events only. They do not provide a snapshot of current state. A client that subscribes to `$DB/users/events/#` will receive all subsequent creates, updates, and deletes, but will not receive the current contents of the `users` collection. This is consistent with MQTT's fundamental model — subscriptions observe message flow, not stored state — and makes the CDC semantics unambiguous. Clients requiring current state use the request-response mechanism (a `list` or `read` operation) and then subscribe for subsequent changes.

### 3.2.3 The Separation Is Fundamental

The distinction between request-response (synchronous CRUD) and subscriptions (asynchronous CDC) is not an implementation convenience — it reflects a genuine architectural boundary. Request-response operations are routed through the topic parser to the database engine and return a result. Subscriptions are processed by the broker's standard subscription mechanism and deliver events as they occur. The two mechanisms share the topic namespace but operate through different code paths, and a client may use either or both.

This separation mirrors the distinction between queries and change feeds in systems like CockroachDB's changefeeds [cockroachdb] and Kafka's consumer model [kafka]. MQDB achieves both within a single protocol, eliminating the need for a separate streaming interface.

## 3.3 QoS: A Non-Mapping

MQTT defines three Quality of Service levels: QoS 0 (at most once), QoS 1 (at least once), and QoS 2 (exactly once) [mqtt5, §4.3]. An obvious question is whether these levels map to database durability guarantees — QoS 0 for fire-and-forget writes, QoS 1 for acknowledged writes, QoS 2 for transactional writes.

In the current implementation, this mapping does not exist. QoS governs only the MQTT transport layer: the guarantee that a published message reaches the broker. Once a message reaches the topic handler, the database operation proceeds identically regardless of the input QoS level. Responses are always published at QoS 1.

We report this non-mapping explicitly rather than constructing a post-hoc justification. The semantic gap between MQTT QoS and database durability is real but bridgeable: the system includes infrastructure for quorum-based write acknowledgment (a `QuorumTracker` that can await replica confirmation before returning success) that is not yet activated. Connecting QoS levels to this infrastructure — QoS 0 for primary-only acknowledgment, QoS 1 for single-replica confirmation, QoS 2 for full-quorum durability — is a concrete future extension that we discuss in Section 7.4.

## 3.4 Internal Partition-Addressed Topics

In addition to the high-level API described above, MQDB defines a partition-addressed topic format: `$DB/p{N}/E/K/O`, where `N` ∈ [0, 255] identifies a specific partition. This format bypasses the automatic routing that maps records to partitions via consistent hashing (described in Section 4.3) and instead addresses a partition directly.

This internal API serves four purposes within the cluster:

1. **Replication**: The primary for a partition forwards writes to replicas using partition-addressed topics, ensuring the write arrives at the correct partition on the receiving node.
2. **Unique constraint protocol**: The three-phase reserve-await-commit protocol (Section 4.6) routes constraint checks to the partition owning the unique index entry.
3. **Foreign key validation**: Cross-entity reference checks are routed to the partition owning the referenced record.
4. **Secondary index updates**: Index entries are partitioned independently from data records, and updates are routed to the index entry's partition.

External clients cannot access partition-addressed topics. The topic protection layer blocks all topics matching `$DB/p+/#` for non-internal connections. The partition-addressed API is thus an internal implementation detail that demonstrates a useful property: MQTT's topic hierarchy can express partition addressing as naturally as it expresses entity-record addressing. The same topic parser handles both, differing only in the presence of a partition prefix.

## 3.5 Limitations of the Mapping

The topic-to-database mapping is sufficient for single-entity CRUD operations, partition routing, constraint enforcement, and change notification. It is not sufficient for:

- **Multi-record transactions.** MQTT has no concept of BEGIN/COMMIT. Each publish is an independent operation. Individual operations are atomic within a single node (the storage engine commits data writes, index updates, and constraint entries in a single batch), but in cluster mode, cross-partition operations — such as unique constraint commits on remote nodes — are best-effort with TTL-based cleanup (Section 4.6). Achieving multi-record atomicity would require a transaction protocol layered on top of MQTT topics.
- **Cross-entity joins.** Each topic addresses exactly one entity. A join across `users` and `orders` cannot be expressed as a single topic operation.
- **SQL expressiveness.** The list operation supports filter predicates, sort directives, and field projection, but the query language is limited to single-entity operations with equality, comparison, and glob pattern matching. Aggregation, grouping, and subqueries are not supported.
- **Point-in-time snapshots.** Subscriptions provide CDC but not time-travel queries or consistent snapshots across multiple records.

These limitations are inherent to the topic-based addressing model: a topic encodes a single entity, a single record, and a single operation. Extending the mapping to multi-entity operations would require either multi-topic coordination (complex) or a query language embedded in payloads (departing from the topic-as-address principle). We consider these constraints acceptable for MQDB's target workloads — IoT data management, edge computing, and applications requiring both real-time pub/sub and persistent CRUD on the same data — and discuss potential extensions in Section 7.4.
