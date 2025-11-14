# MQTT-Integrated Reactive Database — Architecture & MVP Specification

## Overview

This document defines the conceptual and architectural foundations for a **reactive, subscription-aware embedded database** implemented in Rust. The database will use **Fjall** as its primary storage backend, taking architectural inspiration from **BadgerDB (by Dgraph)** for ideas around LSM-tree organization, memory management, and efficient key-value compaction. The ultimate goal is to support **reactive updates** that can efficiently notify MQTT subscribers about changes in subscribed topics or keys.

---

## 1. Core Design Principles

1. **Reactive Subscriptions** — Built-in change observation and dependency tracking. Any mutation should be able to trigger subscription events through a unified notification layer.
2. **Embedded Efficiency** — Optimized for low-latency local access and persistent storage using Fjall’s append-only LSM-like model.
3. **Separation of Concerns** — Storage, indexing, and reactive propagation layers are modular and independently testable.
4. **MQTT Compatibility** — Designed to be extended with an MQTT integration layer (later phase) for direct reactive messaging based on data changes.
5. **Persistence and Consistency** — Atomic write guarantees via Fjall’s transactional API; consistency enforced through WAL-style writes.

---

## 2. High-Level Architecture

### 2.1. Layers

1. **Storage Layer (Fjall)**

   - Key-Value persistence.
   - Leverages Fjall’s LSM-based design for write amplification control and efficient sequential I/O.
   - Inspired by Badger’s value log separation: small keys and metadata in LSM, larger payloads stored separately (Fjall provides support for value separation internally).

2. **Reactive Core**

   - Maintains an internal **subscription registry** mapping key prefixes → subscriber lists.
   - Tracks live read dependencies for queries so that clients can subscribe to “live queries.”
   - Propagates change events via an event dispatcher (simple async channel or reactive stream hub).

3. **Transaction Coordinator**

   - Wraps Fjall’s atomic batch operations.
   - Generates change events for all modified keys.
   - Maintains operation logs for debugging and backpressure control.

4. **Indexing and Query Abstraction**

   - Secondary indexes built as additional Fjall keyspaces.
   - Reactive views can subscribe to index changes.
   - Basic query types: point lookup, prefix scan, reactive scan.

5. **Notification Engine (Future MQTT Integration)**

   - Layer responsible for mapping DB change events into MQTT publish messages.
   - Each subscribed MQTT topic maps to one or more database key patterns.

---

## 3. Data Model

- **Keys:** Byte sequences, semantically grouped by prefix.
- **Values:** Arbitrary byte payloads with optional metadata (version, timestamp, TTL).
- **Namespaces:** Each namespace maps to an independent Fjall database or keyspace.
- **Metadata Index:** Maintains change sequence numbers and dependency tracking for reactive diff propagation.

---

## 4. Reactive Event Flow

1. A write operation (`put`, `delete`, `batch`) executes in Fjall.
2. Transaction coordinator collects changed keys and emits an event.
3. The reactive core looks up registered subscriptions affected by these keys.
4. Subscribers are notified through the in-process event bus (future: MQTT publisher).

---

## 5. Comparison with BadgerDB Inspiration

| Aspect       | BadgerDB                       | Fjall (Chosen Backend)                | Integration Implication                         |
| ------------ | ------------------------------ | ------------------------------------- | ----------------------------------------------- |
| LSM Engine   | Custom Go implementation       | Rust-native LSM variant               | Same design philosophy, less GC overhead        |
| Value Log    | Separate vlog for large values | Built-in support for value separation | Directly reusable for reactive data payloads    |
| Transactions | Managed via write batches      | Built-in transactional interface      | Simplifies reactive commit tracking             |
| Caching      | Manual block cache             | Internal page caching                 | Integrate cache invalidation with reactive diff |

Badger’s architecture inspires:

- Efficient LSM compaction and value log separation.
- Background GC for stale entries.
- Predictable write path design for reactive consistency.

Fjall’s Rust-native approach ensures:

- Memory safety and zero-cost abstractions.
- Simplified integration with async channels and Tokio runtime.

---

## 6. MVP Deliverables

1. **Core Reactive Database Library**

   - CRUD operations via Fjall.
   - Event emission on change.
   - Subscription registry and in-process notification.

2. **Transactional Wrapper**

   - Multi-key atomic updates.
   - Event coalescing per transaction.

3. **Reactive Subscription Engine**

   - Prefix-based subscriptions.
   - Async notification API.

4. **Integration Scaffolding**

   - Well-defined API for MQTT layer to attach to.
   - Abstract `Notifier` trait for MQTT bridge or other transports.

---

## 7. Future Considerations

- **Replication and WAL Streaming** — Similar to Badger’s value log streaming for replication.
- **TTL and Compaction Hooks** — TTL-based cleanup triggering reactive notifications.
- **Reactive Query Language** — Allow subscription to expressions rather than key ranges.
- **Persistence Metrics and Debugging** — Event latency tracing integrated into the runtime.

---

## 8. Next Steps

- Define the **Reactive Core interface** (key subscription, event delivery, registry persistence).
- Specify Fjall **keyspace organization** (main data, indexes, metadata, TTL tracking).
- Establish **integration contract** for MQTT mapping.
- Outline test harness for validating reactive behavior under concurrent updates.

---

## MQTT-Database Translation Specification

### 1. Message Format

All MQTT payloads are JSON objects with the following structure:

```json
{
  "op": "create|read|update|delete|list",
  "entity": "string",
  "data": {
    /* operation-specific */
  },
  "reply_to": "string",
  "correlation_id": "string"
}
```

**Fields:**

- `op`: Operation type. Required.
- `entity`: Table/entity name. Required.
- `data`: Operation payload. Structure depends on `op`.
- `reply_to`: MQTT topic for response delivery. Required.
- `correlation_id`: Opaque string for correlating request/response. Optional; if omitted, generated by broker.

---

### 2. Topic Routing

**Request topics** follow the pattern: `db/{entity}/{op}`

Examples:

- `db/users/create`
- `db/posts/read`
- `db/comments/update`
- `db/products/list`

**Response topics** are client-specified via `reply_to` field. Convention: `db/responses/{correlation_id}` or similar.

---

### 3. Operations

#### 3.1 CREATE

**Topic:** `db/{entity}/create`

**Request payload:**

```json
{
  "op": "create",
  "entity": "users",
  "data": {
    /* complete entity fields */
  },
  "reply_to": "...",
  "correlation_id": "req-1"
}
```

**Response (success):**

```json
{
  "status": "ok",
  "correlation_id": "req-1",
  "result": {
    /* created entity with generated IDs */
  }
}
```

**Response (error):**

```json
{
  "status": "error",
  "correlation_id": "req-1",
  "error": {
    "code": "validation_error|constraint_violation|internal_error",
    "message": "string"
  }
}
```

---

#### 3.2 READ

**Topic:** `db/{entity}/read`

**Request payload:**

```json
{
  "op": "read",
  "entity": "users",
  "data": {
    "id": 123
  },
  "reply_to": "...",
  "correlation_id": "req-2"
}
```

**Semantics:** Retrieve single entity by primary key (or composite key fields in `data.id` or `data.keys`).

**Response (success):**

```json
{
  "status": "ok",
  "correlation_id": "req-2",
  "result": {
    /* entity record */
  }
}
```

**Response (not found):**

```json
{
  "status": "not_found",
  "correlation_id": "req-2"
}
```

---

#### 3.3 UPDATE

**Topic:** `db/{entity}/update`

**Request payload:**

```json
{
  "op": "update",
  "entity": "users",
  "data": {
    "id": 123,
    "fields": {
      "name": "new name",
      "email": "new@example.com"
    }
  },
  "reply_to": "...",
  "correlation_id": "req-3"
}
```

**Semantics:** Update entity identified by `id` with fields in `fields` object. Unspecified fields unchanged.

**Response (success):**

```json
{
  "status": "ok",
  "correlation_id": "req-3",
  "result": {
    /* updated entity */
  }
}
```

---

#### 3.4 DELETE

**Topic:** `db/{entity}/delete`

**Request payload:**

```json
{
  "op": "delete",
  "entity": "users",
  "data": {
    "id": 123
  },
  "reply_to": "...",
  "correlation_id": "req-4"
}
```

**Response (success):**

```json
{
  "status": "ok",
  "correlation_id": "req-4"
}
```

**Response (not found):**

```json
{
  "status": "not_found",
  "correlation_id": "req-4"
}
```

---

#### 3.5 LIST

**Topic:** `db/{entity}/list`

**Request payload:**

```json
{
  "op": "list",
  "entity": "users",
  "data": {
    "filters": [
      { "field": "status", "op": "eq", "value": "active" },
      { "field": "created_at", "op": "gte", "value": "2025-01-01" }
    ],
    "sort": [{ "field": "created_at", "direction": "desc" }],
    "pagination": {
      "limit": 50,
      "offset": 0
    }
  },
  "reply_to": "...",
  "correlation_id": "req-5"
}
```

**Filter operators:** `eq`, `neq`, `lt`, `lte`, `gt`, `gte`, `in`, `like`, `is_null`, `is_not_null`

**Sort direction:** `asc`, `desc`

**Response (success):**

```json
{
  "status": "ok",
  "correlation_id": "req-5",
  "result": {
    "items": [
      /* array of entities */
    ],
    "total": 1523,
    "limit": 50,
    "offset": 0
  }
}
```

---

### 4. Relationships

**Nested entity access** via dot notation in field names:

```json
{
  "op": "read",
  "entity": "posts",
  "data": {
    "id": 1,
    "include": ["author", "comments"]
  }
}
```

Response includes related entities:

```json
{
  "status": "ok",
  "result": {
    "id": 1,
    "title": "...",
    "author": {
      /* user object */
    },
    "comments": [
      /* comment objects */
    ]
  }
}
```

---

### 5. Error Handling

**Error codes:**

- `validation_error`: Invalid input (missing required field, type mismatch)
- `constraint_violation`: DB constraint violated (unique, foreign key, etc.)
- `not_found`: Entity doesn't exist
- `unauthorized`: Client lacks permission (if auth is implemented)
- `internal_error`: Broker/DB error

**All error responses include:**

```json
{
  "status": "error",
  "correlation_id": "...",
  "error": {
    "code": "string",
    "message": "string",
    "details": {
      /* optional debug info */
    }
  }
}
```

---

### 6. Atomicity & Transactions

- Single operations are atomic (CRUD on one entity).
- No multi-entity transactions in this version.
- Idempotency: `correlation_id` can be used for deduplication on retry.

---
