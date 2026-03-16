# Queue Feature — Ideation Document

This document captures the design thinking for MQDB's queue feature. It supersedes the earlier QUEUE_DESIGN.md where decisions conflict. Use this as the reference during planning and implementation.

## The Core Idea

Queue items are regular MQDB entity records. The queue system is a thin state machine layer on top of existing CRUD, partitioning, replication, and shared subscriptions. No new storage engine, no custom protocol — just MQTT publishes intercepted early in the broker pipeline.

## What Already Exists

The following MQDB primitives are reused without modification:

| Primitive | Role in Queue |
|-----------|--------------|
| Entity CRUD | Queue items are records. Create = produce, delete = post-ACK cleanup |
| Partitioning (256 fixed, hash on `entity/id`) | Queue items distribute across partitions automatically |
| Write forwarding | Producers connect to any node; writes route to partition primary |
| Async replication (RF=2) | Queue items replicate to replicas for durability |
| Change events (QoS 1, durable outbox) | Optional notification for monitoring subscribers (separate from queue delivery) |
| Shared subscriptions (LoadBalanced / Ordered) | Consumer group distribution for change event subscribers |
| Consumer groups with partition assignment | Ordered mode assigns partitions to consumers deterministically |
| Session disconnect detection | Broker knows immediately when a client disconnects |
| `become_primary` / `step_down` lifecycle | Hooks for initializing/tearing down per-partition queue state |
| `PublishAction` interceptor (mqtt-lib) | Early interception of ACK/NACK/extend publishes before routing |

## PublishAction — The Key Enabler

The mqtt-lib `on_client_publish` event handler fires after authorization but before subscription matching and storage. It returns one of:

- **`Continue`** — normal pipeline, no interference (non-queue topics)
- **`Handled`** — suppress the publish, complete QoS handshake (PUBACK sent to client). The message never reaches routing or storage. Used for ACK, NACK, and extend operations.
- **`Transform(packet)`** — replace the publish with a modified packet, then route normally. Used to inject queue metadata into produce operations.

The handler runs on the client's publish codepath. For `Handled` operations (ACK/NACK/extend), the work is an in-memory state update — nanosecond-scale. No disk I/O, no routing, no cross-node traffic.

## Queue Item Lifecycle

```
    produce (create)
         │
         ▼
   ┌──────────┐
   │ pending  │◄──────── nack / visibility timeout / disconnect
   │ (queue)  │
   └────┬─────┘
        │ consumer available
        ▼
   ┌──────────┐
   │ in-flight│──── extend ──► reset visibility timer
   │          │
   └──┬───┬──┘
      │   │
     ack  nack/timeout
      │   │
      ▼   │    delivery_count > max?
   [delete]│         │
           │         ▼
           │   [dead letter]
           │   (move to {entity}_dlq)
           │
           └──► back to pending queue
```

### States

- **pending** — item is in the ordered pending queue, waiting for a consumer. Persisted as a record with `_queue_state: "pending"`.
- **in-flight** — item has been delivered to a consumer. Tracked in an in-memory map on the primary: `item_id → (consumer_id, delivered_at, timeout_deadline, delivery_count)`. The record's `_queue_state` is updated to `"in_flight"`.
- **deleted** — consumer ACKed. Record is deleted via normal CRUD. Deletion replicates to replicas.
- **dead-lettered** — delivery count exceeded the configured maximum. Item is deleted from the queue entity and created in `{entity}_dlq` (a regular entity). Both operations replicate normally.

### Operations via MQTT Topics

| Operation | Topic | PublishAction | What Happens |
|-----------|-------|---------------|-------------|
| Produce | `$DB/{entity}/create` | `Transform` | Inject `_queue_state: "pending"`, `_delivery_count: 0`. Record enters normal CRUD pipeline. Primary's delivery loop picks it up. |
| ACK | `$DB/_queue/{entity}/{id}/ack` | `Handled` | In-memory: remove from in-flight map, cancel visibility timer. Then delete the record (internal CRUD, replicates normally). |
| NACK | `$DB/_queue/{entity}/{id}/nack` | `Handled` | In-memory: remove from in-flight map, increment `_delivery_count`, update record state to `"pending"`, requeue in pending queue. If `delivery_count > max_deliveries`, dead-letter instead. |
| Extend | `$DB/_queue/{entity}/{id}/extend` | `Handled` | In-memory: reset `timeout_deadline` on the in-flight entry. Optionally accept a duration in the payload. |

Non-queue publishes hit the `on_client_publish` handler, see a non-queue topic, and return `Continue` immediately — zero overhead on normal traffic.

## Delivery Mechanism

Queue delivery is a dedicated loop on each primary node, separate from the change event system. Change events still fire for non-queue subscribers who want notifications (dashboards, audit logs), but queue consumers receive items through the delivery loop.

### Why Not Change Events?

Change events are push-on-create — they fire once when a record is created and cannot be replayed, reordered, or held back. Queue delivery needs:

- Ordering control (FIFO, LIFO, priority) — change events only deliver in creation order
- Redelivery after NACK or timeout — change events have no replay mechanism
- Backpressure per consumer (prefetch limits) — change events broadcast immediately
- Hold-back until a consumer is available — change events fire whether or not anyone is listening

The delivery loop provides all of these. Change events and queue delivery are independent — a queue-enabled entity emits change events AND delivers through the queue loop. They serve different subscribers with different semantics.

### Delivery Loop Behavior

Each primary node runs a delivery loop per queue-enabled entity (for partitions it owns):

1. Check: is there a pending item AND an available consumer (connected, under prefetch limit)?
2. If yes: pop the next item from the pending queue (ordered by policy), publish it to the consumer via internal publish, mark in-flight, start visibility timer.
3. If no consumer: items accumulate in the pending queue until one connects.
4. If no items: loop idles until a new item is produced or a NACKed item returns to pending.

The internal publish to the consumer uses the consumer's subscription topic, delivered as a normal MQTT message at QoS 1. From the consumer's perspective, they subscribed to a topic and receive messages — standard MQTT, no custom protocol.

## Ordering Policies

The pending queue is an ordered data structure. The ordering comparator is configurable per queue entity:

| Policy | Order | NACKed Items Return To | Use Case |
|--------|-------|----------------------|----------|
| FIFO | Oldest first (`_created_at` ascending) | Front of queue (retry before new work) | Task processing, job queues |
| LIFO | Newest first (`_created_at` descending) | Front of queue | Priority on freshness, cache warming |
| Priority | By user-defined field (ascending or descending) | Position determined by priority value | SLA-differentiated workloads |

FIFO within a partition is the default. Cross-partition ordering is best-effort — items on different partitions may be delivered in any relative order, similar to Kafka's per-partition ordering guarantee.

## Partitioning and Cluster Behavior

### How Items Distribute

Queue items are records keyed by `entity/id`. The ID hashes to one of 256 partitions. Each partition has a primary node. The queue delivery loop on each primary serves only the partitions it owns.

This means the queue is inherently partitioned. No single node sees all items. Each primary delivers its share.

### Producer Path

A producer can connect to any node. If the item's partition primary is a different node, the write is forwarded automatically (existing write forwarding). The primary stores the record, the delivery loop picks it up. No new machinery needed.

### Consumer Path — Local Consumption Only

Consumers receive items only from the node they're connected to. The primary delivers items from its partitions to its local consumers. Replicas have the data (via async replication) but do not run the delivery loop — only the primary delivers.

This means consumers must be distributed across nodes for even consumption. If all consumers connect to Node 1 but most items land on Node 2's partitions, those items sit undelivered until a consumer connects to Node 2. This is the same constraint as Kafka's partition-consumer assignment. It is an operational concern, not an architectural limitation.

### Failover

When a primary node dies:

1. Raft detects the failure, assigns the dead node's partitions to surviving nodes.
2. The new primary receives partition data via snapshot transfer.
3. `become_primary()` fires. The queue system hooks into this lifecycle event.
4. The new primary scans the entity's records for the acquired partitions, filters by `_queue_state` (`"pending"` or `"in_flight"`), sorts by ordering policy, and rebuilds the pending queue.
5. All items that were in-flight on the dead node are treated as pending (the in-memory in-flight map died with the old primary). These items will be redelivered — safe under at-least-once semantics.
6. Consumers connected to the new primary start receiving items from the newly acquired partitions.

### Rebalancing

Same as failover. When partitions move between nodes:

- Old primary: `step_down()` fires. Queue drops its pending queue and in-flight map for those partitions.
- New primary: `become_primary()` fires. Queue rebuilds from records.
- Items that were in-flight on the old primary may be redelivered on the new primary. At-least-once.

### No Cross-Node Coordination for Queue State

The in-flight map, pending queue, visibility timers, and delivery loop are all in-memory on the primary. Nothing is replicated across nodes for queue operations. The only cross-node traffic is the normal record replication that already exists. ACK/NACK/extend are handled locally via `PublishAction::Handled` — they never leave the node.

This is possible because of local consumption: the consumer is connected to the same node that owns the partition and runs the delivery loop. ACKs arrive on the same node that tracks the in-flight state.

## Visibility Timeout

The visibility timeout is a safety net for crashed consumers, not a deadline for healthy ones. It should be generous (30s or 60s, configurable per queue entity).

When an item is delivered:
- A timer entry is created: `(item_id, deadline = now + visibility_timeout)`
- The timer runs on the primary node as part of the event loop (a new periodic branch, similar to TTL cleanup)

When the timer fires:
- If the item is still in-flight (no ACK/NACK received): treat as implicit NACK
- Increment `_delivery_count`, return to pending queue
- If `_delivery_count > max_deliveries`: dead-letter

### Extend

For long-running processing, the consumer publishes to `$DB/_queue/{entity}/{id}/extend`. This resets the timer deadline. The consumer can extend as many times as needed. The extend payload can optionally carry a requested duration in seconds.

### Disconnect as Implicit NACK

MQTT disconnect detection is faster than any visibility timeout. When a consumer disconnects (clean or unclean), the broker's session handler fires immediately. The queue system scans the in-flight map for all items owned by that consumer and returns them to the pending queue. No timer wait needed.

This covers the common crash case (consumer dies, OS kills the process, network partition) much faster than a visibility timeout. The timeout exists for the edge case where the consumer is connected but stuck (infinite loop, deadlock, resource exhaustion).

## In-Memory State

Per queue-enabled entity, per primary node:

### Pending Queue
- Ordered collection (BTreeMap, BinaryHeap, or Vec sorted by policy)
- Keyed by ordering criteria (timestamp, priority field)
- Rebuilt on `become_primary` from a scan of entity records with `_queue_state: "pending"`

### In-Flight Map
- `HashMap<String, InFlightEntry>` keyed by item ID
- Each entry: `{ consumer_id, delivered_at, timeout_deadline, delivery_count }`
- Secondary index: `HashMap<String, Vec<String>>` keyed by consumer ID (for fast disconnect handling)
- Rebuilt on `become_primary`: items with `_queue_state: "in_flight"` are treated as pending (consumer context is lost)

### Visibility Timer
- A sorted structure (BTreeMap by deadline, or a timer wheel) checked periodically
- Runs as a branch in the event loop (e.g., every 1 second)
- On fire: check if item is still in-flight, if so, implicit NACK

All state is lost on crash or failover. Rebuilt from records. This is safe because:
- Pending items are records with `_queue_state: "pending"` — they survive in storage
- In-flight items become pending on rebuild — at-least-once redelivery
- The only "lost" information is which consumer had which item — and that consumer is gone anyway (the node crashed)

## Dead Letter

When `_delivery_count` exceeds the configured `max_deliveries` for the queue entity:

1. Delete the item from the queue entity (normal CRUD delete, replicates)
2. Create a copy in `{entity}_dlq` with the original data plus `_dead_letter_reason`, `_original_entity`, `_delivery_count` (normal CRUD create, replicates)

The DLQ entity is a regular MQDB entity. Operators can list, inspect, and reprocess dead-lettered items using standard CRUD operations. No special tooling needed.

## Queue Configuration

Queue behavior is configured per entity via the existing schema/admin API:

```
Topic: $DB/_admin/queue/{entity}/configure
Payload: {
    "enabled": true,
    "ordering": "fifo",              // "fifo" | "lifo" | "priority"
    "priority_field": null,          // field name for priority ordering
    "visibility_timeout_s": 30,      // seconds before implicit NACK
    "max_deliveries": 5,             // attempts before dead-letter
    "max_queue_depth": 0,            // 0 = unlimited
    "disconnect_grace_s": 5          // seconds to wait before implicit NACK on disconnect
}
```

The `disconnect_grace_s` field handles the case where a consumer disconnects and reconnects quickly (e.g., network blip). Instead of immediately NACKing all in-flight items, the queue waits for the grace period. If the consumer reconnects within the window, its in-flight items are preserved. If not, they return to pending.

## Delivery Guarantees

**At-least-once.** An item may be delivered more than once in these scenarios:

- Consumer processes the item and crashes before publishing ACK
- Visibility timeout fires while consumer is still processing (timeout too short)
- Partition failover: in-flight items on the old primary are redelivered on the new primary
- Rebalancing: same as failover for moved partitions

Consumers must be idempotent or use the item ID for deduplication.

**No exactly-once.** MQTT QoS 2 (exactly-once) applies to the transport layer, not to application-level processing. Even with QoS 2 delivery, a consumer crash between receiving the message and completing processing creates a redelivery. True exactly-once requires application-level deduplication, which is outside the queue system's scope.

**Ordering is per-partition, not global.** Items on different partitions may be delivered in any relative order. Within a partition, the ordering policy (FIFO/LIFO/priority) is respected. This is sufficient for most workloads and is the same guarantee that Kafka provides.

## What Changes vs. the Previous QUEUE_DESIGN.md

| Aspect | Previous Design | This Design |
|--------|----------------|-------------|
| Delivery mechanism | Change events + shared subscriptions | Dedicated delivery loop (change events still fire separately for notification subscribers) |
| ACK mechanism | Two-tier: PUBACK-as-ACK (default) or explicit ACK topic | Explicit ACK topic only, via `PublishAction::Handled` |
| NACK | Implicit (timeout only) | Explicit NACK topic + timeout + disconnect detection |
| Extend/touch | Not designed | `$DB/_queue/{entity}/{id}/extend` via `PublishAction::Handled` |
| Ordering | FIFO only (sequence numbers) | Configurable: FIFO, LIFO, priority |
| Cluster integration | Phase 5 (deferred) | Built-in from day one — rides on partition ownership model |
| Implementation phases | 5 phases, incremental | TBD — the design is simpler and may need fewer phases |

The key shift is from piggybacking on change events (previous design) to owning the delivery loop (this design). This gives us ordering control, redelivery, and backpressure — capabilities that change events cannot provide. The `PublishAction` interceptor, which did not exist when the previous design was written, makes ACK/NACK/extend cheap enough to justify explicit acknowledgment as the only mode.

## Open Questions

1. **Prefetch limit.** Should consumers declare how many items they can handle concurrently? A prefetch of 1 means strict sequential processing. A prefetch of 10 means 10 items in-flight simultaneously. The delivery loop would track per-consumer in-flight count and stop delivering when the limit is reached. MQTT 5.0's `receive_maximum` property could serve as the prefetch value, or we could use a separate configuration.

2. **Rate limiting.** Should the delivery loop enforce a maximum delivery rate per consumer (token bucket)? The previous design included this. It adds complexity but prevents a fast producer from overwhelming a slow consumer even with prefetch limits.

3. **Queue depth limits.** When `max_queue_depth` is reached and a new item is produced, what happens? Reject the produce (return error to client)? Drop the oldest item? This needs a policy decision.

4. **Delivery topic format.** What topic does the consumer receive messages on? Options:
   - `$DB/_queue/{entity}/deliver/{id}` — item ID in topic, consumer extracts it for ACK
   - `$DB/{entity}/events/queued` — looks like a change event, item ID in payload
   - Custom topic per consumer — more complex but allows per-consumer routing

5. **Observability.** Should the queue expose metrics (pending count, in-flight count, delivery rate, dead-letter count) via a `$SYS` topic or the admin API? This is straightforward to add but needs a topic/format decision.

6. **Multi-entity queues.** Can a consumer subscribe to queue delivery from multiple entities? Or is each queue subscription entity-specific? Wildcard queue subscriptions (`$DB/_queue/+/deliver`) would require the delivery loop to span entities.
