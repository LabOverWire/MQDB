# Work Queue Design

## Overview

MQDB work queues provide durable, push-based message processing built on top of the existing entity storage system. Queues are entities with special consumption semantics: messages persist until consumed, and the broker distributes them across competing consumers.

## Motivation

MQTT shared subscriptions distribute messages across connected consumers but lack durability and crash recovery. If no consumer is connected, QoS 0 messages are lost. If a worker crashes after PUBACK but before processing, the message is gone. Work queues close both gaps: messages persist in storage until consumed, and consumers can opt into explicit acknowledgment for crash recovery.

## Core Constraint

**No MQTT protocol changes.** Any standard MQTT client can consume from a queue by subscribing to a topic. The simplest consumer just subscribes, receives messages, and auto-PUBACKs — no special client libraries, custom packets, or proprietary extensions. Advanced features (explicit ACK, NACK, rate control) are opt-in via standard MQTT PUBLISH operations.

## Design Decisions

### Two-tier acknowledgment

The queue supports two consumer modes:

**Default mode (PUBACK = ACK):** A simple MQTT client subscribes to the queue topic, receives messages, and its client library auto-PUBACKs. The broker treats PUBACK as application-level acknowledgment — the message is removed from the queue. This gives persistence, ordering, competing consumers, and dead-letter handling. Crash recovery relies on MQTT session redelivery: if the consumer crashes before PUBACK, the broker resends on reconnect (standard MQTT QoS 1 behavior).

**Explicit ACK mode (opt-in):** A consumer that wants crash recovery beyond PUBACK opts in by publishing to the config topic:

```
PUBLISH "$DB/_queue/jobs/config" → { "explicit_ack": true }
```

In this mode, PUBACK alone does not remove the message. The consumer must publish to `$DB/_queue/{name}/ack/{message_id}` after processing. Visibility timeout kicks in for crash recovery — if no ACK arrives within the timeout, the message returns to the queue for another consumer. This protects the window between PUBACK and "actually processed." Note that after PUBACK, the MQTT session considers the message delivered — on reconnect, the broker will not redeliver it via session mechanisms. Recovery relies on visibility timeout, not MQTT redelivery. The consumer application must be aware of this trade-off.

The consumer must publish the config before subscribing to the queue (or at least before expecting explicit ACK semantics). There is no auto-detection — the config topic is the single mechanism for mode selection.

**NACK availability:** NACK is only meaningful in explicit ACK mode. In default mode, PUBACK immediately removes the message — there is no reject-and-retry path. Consumers that need the ability to reject messages must use explicit ACK mode.

### Push-based delivery

Consumers subscribe to a queue topic. The broker pushes messages as they become available, respecting per-consumer concurrency and rate limits. This is natural for MQTT — pull-based would require polling, which is unidiomatic.

### Queues as entities

Queue messages are stored as regular database entities under the entity name `_queue_{name}`. This reuses all existing infrastructure: storage backends, replication, partitioning, ChangeEvents, scoping. No new storage subsystem needed.

### Competing consumers

Each message is delivered to exactly one consumer. Multiple subscribers on the same queue topic form an implicit competing consumer group. The broker distributes messages across consumers using round-robin, respecting each consumer's prefetch and rate limits.

### Authorization via existing systems

- **Roles** control who can produce and consume (topic-level pub/sub permissions)
- **Ownership** is simply not configured for queue entities — consumers can ACK messages they didn't produce
- **Scopes** apply at the queue level for tenant isolation
- **Admin** operations require AdminRequired access

No new authorization concepts needed. The admin sets up roles and topic permissions at broker configuration time.

## Architecture

### Message Lifecycle — Default Mode (PUBACK = ACK)

```
Producer                    Broker                      Consumer
   |                          |                            |
   |-- PUBLISH send --------->|                            |
   |                          |-- store (available)        |
   |                          |                            |
   |                          |-- PUBLISH (QoS 1) ------->|
   |                          |   (mark inflight)          |
   |                          |                            |
   |                          |<-------- PUBACK -----------|
   |                          |-- delete message           |
   |                          |                            |
```

Consumer crashes before PUBACK → standard MQTT session redelivery on reconnect.

### Message Lifecycle — Explicit ACK Mode

```
Producer                    Broker                      Consumer
   |                          |                            |
   |-- PUBLISH send --------->|                            |
   |                          |-- store (available)        |
   |                          |                            |
   |                          |-- PUBLISH (QoS 1) ------->|
   |                          |   (mark inflight,          |
   |                          |    start visibility timer) |
   |                          |                            |
   |                          |<-------- PUBACK -----------|
   |                          |   (transport only,         |
   |                          |    message stays inflight) |
   |                          |                            |
   |                          |        [processing...]     |
   |                          |                            |
   |                          |<-- PUBLISH ack/{id} -------|
   |                          |-- delete message           |
   |                          |                            |
```

Consumer crashes after PUBACK but before ACK → visibility timeout expires → message redelivered.

### Message States

**Default mode:**
```
available ──[deliver]──> inflight ──[PUBACK]──> deleted
     ^                      |
     └──[session redelivery]┘ (on reconnect, standard MQTT)
```

**Explicit ACK mode:**
```
available ──[deliver]──> inflight ──[explicit ACK]──> deleted
     ^                      |
     |                      |──[NACK]──────────────> available (delivery_count++)
     |                      |
     |──[visibility timeout]┘ (delivery_count++)
     |                      |
     |──[disconnect + grace]┘ (delivery_count++)
                            |
                            └──[max_deliveries]────> dead_letter
```

### Storage Layout

Queue configuration:
```
meta/queue/{name} → {
    visibility_timeout_s: u32,  // default: 30
    max_deliveries: u32,        // default: 5, 0 = unlimited
    max_messages: u64,          // default: 0 (unlimited), max queue depth
    scan_interval_s: u32,       // default: 5, visibility timeout check frequency
    dead_letter: bool           // default: true
}
```

Queue messages (regular entity storage):
```
data/_queue_{name}/{message_id} → {
    _seq: u64,
    _state: "available" | "inflight",
    _delivery_count: u32,
    _consumer_id: Option<String>,
    _visibility_deadline: Option<u64>,
    _enqueued_at: u64,
    ...producer payload fields
}
```

Dead-lettered messages:
```
data/_dead_letter/_queue_{name}/{message_id} → {
    ...original message fields,
    _dead_lettered_at: u64,
    _last_consumer_id: String,
    _reason: "max_deliveries_exceeded"
}
```

### Reserved Entity Prefixes

The entity create handler rejects entity names starting with `_queue_` or `_dead_letter_` through the normal `$DB/{entity}/create` path. These prefixes are reserved for queue infrastructure.

### MQTT Topic API

**Standard MQTT operations (any client):**

| Topic | Direction | QoS | Description |
|-------|-----------|-----|-------------|
| `$DB/_queue/{name}/send` | Producer → Broker | 0/1 (max 1) | Enqueue a message |
| `$DB/_queue/{name}/msg/+` | Broker → Consumer | 1 (granted) | Message delivery (subscribe) |

The delivery topic is `$DB/_queue/{name}/msg/{message_id}`. The `msg/` sublevel separates delivery from command topics (`send`, `config`, `ack`, `nack`), so a consumer subscribing to `$DB/_queue/{name}/msg/+` only receives deliveries. The message ID is embedded in the topic so any MQTT client can extract it. The payload is the producer's original data — no envelope wrapping.

The broker intercepts publishes to `send`, `config`, `ack/*`, and `nack/*` subtopics as queue commands. They are not distributed to consumers subscribed to the queue delivery topic.

**Advanced operations (pub+sub clients, all optional):**

| Topic | Direction | Description |
|-------|-----------|-------------|
| `$DB/_queue/{name}/ack/{id}` | Consumer → Broker | Explicit ACK (removes message) |
| `$DB/_queue/{name}/nack/{id}` | Consumer → Broker | Reject message, return to queue |
| `$DB/_queue/{name}/config` | Consumer → Broker | Set consumer config |

**Admin operations:**

| Topic | Direction | Description |
|-------|-----------|-------------|
| `$DB/_admin/queue/create` | Admin → Broker | Create queue with config |
| `$DB/_admin/queue/{name}/delete` | Admin → Broker | Delete queue and all messages |
| `$DB/_admin/queue/list` | Admin → Broker | List queues with stats |
| `$DB/_admin/queue/{name}/config` | Admin → Broker | Get/update queue config |

### Consumer Configuration

All configuration is via the config topic publish. No SUBSCRIBE user properties required.

```
PUBLISH "$DB/_queue/jobs/config" → {
    "explicit_ack": true,   // opt into explicit ACK mode
    "prefetch": 10,         // per-queue prefetch override
    "max_rate": 50          // max deliveries per second
}
```

A consumer that never publishes to the config topic gets sensible defaults:
- `explicit_ack`: false (PUBACK = ACK)
- `prefetch`: controlled by `receive_maximum` from CONNECT
- `max_rate`: none (no rate limit)

Config changes take effect on the next delivery cycle. Inflight messages are not affected. The broker returns a success/error response to the config publish using the standard MQTT 5.0 request-response pattern (same as `$DB/{entity}/create`).

### Message ID Generation

The broker generates message IDs (UUID). The producer does not control the ID. On successful enqueue, the broker returns the message ID in the MQTT 5.0 response (same pattern as `$DB/{entity}/create` returning the created entity). Producers that need to track messages use the response.

### Prefetch Control

Two levels of prefetch interact:

- **`receive_maximum`** (from MQTT 5.0 CONNECT): the global cap on unacknowledged QoS 1 messages across all subscriptions. Enforced by the MQTT protocol itself.
- **Per-queue prefetch** (from config topic): a finer-grained limit within that global cap. Useful when a consumer subscribes to multiple queues with different concurrency needs.

A consumer subscribed to 3 queues with `receive_maximum = 30` and per-queue `prefetch = 10` each gets the expected behavior — up to 10 inflight per queue, 30 total. A consumer with `receive_maximum = 5` and per-queue `prefetch = 10` is capped at 5 total across all queues, because the protocol-level cap takes precedence.

### Rate Control

Per-consumer delivery rate limiting using a token bucket:

- `max_rate`: maximum deliveries per second (set via config topic)
- Tokens refill at `max_rate` per second
- Each delivery consumes one token
- When no tokens available, delivery pauses until refill

Rate limiting only matters during backlog drain. During real-time operation (messages arrive one at a time), the natural arrival rate is the bottleneck.

**Delivery gate:** broker pushes next available message only when:
```
pending_pubacks_this_queue < queue_prefetch
AND total_pending_pubacks_all_subscriptions < receive_maximum
AND (max_rate is None OR tokens > 0)
```

The `receive_maximum` budget is shared across all subscriptions (queue and non-queue). The broker's existing flow control already enforces this — if the client has hit `receive_maximum`, the broker cannot send any QoS 1 message. The queue delivery loop checks the broker-level in-flight count rather than maintaining a separate counter.

### Visibility Timeout (Explicit ACK Mode Only)

Visibility timeout only applies to consumers in explicit ACK mode. For default mode consumers, PUBACK removes the message — there is nothing to time out.

When a message is delivered to an explicit-ACK consumer:
1. `_state` set to `inflight`
2. `_consumer_id` set to the consumer's client ID
3. `_visibility_deadline` set to `now_ms + visibility_timeout_s * 1000`

A background task runs at `scan_interval_s` frequency, scanning for inflight messages past their visibility deadline:
- Reset `_state` to `available`
- Clear `_consumer_id` and `_visibility_deadline`
- Increment `_delivery_count`
- If `_delivery_count > max_deliveries` → move to dead letter

Worst-case recovery latency: `visibility_timeout_s + scan_interval_s`. For latency-sensitive queues, reduce `scan_interval_s`.

### Consumer Disconnect (Explicit ACK Mode Only)

When an explicit-ACK consumer disconnects (detected via TCP close or keepalive timeout):

1. Broker starts a grace period (default: 5 seconds)
2. If consumer reconnects within grace period: no action, messages remain inflight
3. If grace period expires: release all inflight messages for that consumer back to available state, increment delivery counts

This reduces recovery latency from `visibility_timeout_s` (potentially 30-60s) to `grace_period_s` (5s). The visibility timeout remains as a safety net for edge cases where disconnect detection fails.

For default-mode consumers, disconnect handling is standard MQTT: unacknowledged QoS 1 messages are redelivered on session reconnect.

### Session Expiry and Clean Start

Standard MQTT discards pending QoS 1 messages when a session expires or a client reconnects with `clean_start = true`. For queue messages, this would mean silent message loss. Queue delivery diverges from standard session semantics:

- **Session expiry:** When a consumer's session expires, all inflight queue messages assigned to that consumer are returned to `available` state (with `_delivery_count++`), not discarded.
- **`clean_start = true`:** When a consumer reconnects with `clean_start`, the same release happens — inflight queue messages return to available.

This divergence is specific to the `$DB/_queue/` namespace. Non-queue QoS 1 messages follow standard MQTT session semantics. The implementation hooks into the session cleanup code path to check whether pending messages belong to queue deliveries and handles them differently.

### FIFO Ordering

Messages are delivered in `_seq` order (auto-incrementing sequence assigned at enqueue time). Strict FIFO is best-effort:

- **Redelivery:** A NACKed or timed-out message returns to available state. Because the original `_seq` is preserved, it naturally sorts ahead of newer messages and is delivered next. This can cause head-of-line blocking if one message repeatedly fails.
- **Prefetch > 1:** With multiple inflight messages, the consumer can process and acknowledge them in any order.
- **Multiple consumers:** Different consumers process at different speeds.

For use cases requiring strict ordering, set `prefetch = 1` and use a single consumer.

### Queue Depth Limits

Each queue has an optional `max_messages` limit. When the queue reaches this limit, the broker rejects new messages with an error response ("queue full"). This prevents unbounded storage growth.

A queue with `max_messages = 0` (the default) has no depth limit.

### MQTT QoS Interaction

**Queue delivery is QoS 1 only.** The broker grants QoS 1 in the SUBACK regardless of what the consumer requests:

- **QoS 0 requested → granted QoS 1.** Without PUBACK, the broker cannot distinguish "received" from "lost in transit," so it would have to delete messages immediately on send or risk double-delivery. Upgrading to QoS 1 prevents silent message loss.
- **QoS 2 requested → granted QoS 1.** QoS 2 provides exactly-once *delivery*, but work queues need exactly-once *processing* — a consumer can crash after receiving a message but before acting on it. The QoS 2 four-step handshake (PUBLISH → PUBREC → PUBREL → PUBCOMP) adds broker-side state tracking and a coordination problem (which step is the queue ACK point?) without covering the failure mode that actually matters. Explicit ACK mode already solves crash-after-delivery at the application level, making QoS 2 redundant complexity.

Both downgrades are permitted by the MQTT spec (MQTT-3.8.4-6 and MQTT-3.9.3). Every standard MQTT client library handles a granted QoS lower than requested transparently — the consumer code does not change.

**Default mode:**
- PUBACK = ACK. The message is removed from the queue when the consumer PUBACKs.

**Explicit ACK mode:**
- PUBACK is transport-only. The message remains inflight until an explicit ACK publish.

**Producer QoS** is also capped at QoS 1 for queue send topics. QoS 2 would provide exactly-once enqueue, but the design defers deduplication to future extensions and the added handshake complexity is not justified. QoS 0 enqueues are accepted — the message could be lost between producer and broker, but once stored, it is durable.

### ChangeEvents

Queue operations emit standard ChangeEvents:
- **Enqueue** → `ChangeEvent::create` on `_queue_{name}`
- **ACK/PUBACK (delete)** → `ChangeEvent::delete` on `_queue_{name}` (with full message data)
- **Dead letter** → `ChangeEvent::create` on `_dead_letter/_queue_{name}`
- **State transitions** (inflight ↔ available) are internal bookkeeping, no ChangeEvents

### Topic Protection Rules

| Pattern | Tier |
|---------|------|
| `$DB/_queue/+/send` | Requires pub permission (producers) |
| `$DB/_queue/+/msg/+` | Requires sub permission (consumers) |
| `$DB/_queue/+/ack/#` | Requires pub permission (explicit-ACK consumers) |
| `$DB/_queue/+/nack/#` | Requires pub permission (explicit-ACK consumers) |
| `$DB/_queue/+/config` | Requires pub permission (advanced consumers) |
| `$DB/_admin/queue/#` | AdminRequired |

### Wildcard Subscriptions

A consumer subscribing to `$DB/_queue/+/msg/+` becomes a competing consumer on all queues simultaneously. This is allowed — universal workers that process any queue are a valid use case. The broker tracks which queues the consumer is competing on and applies per-queue prefetch and rate limits independently.

Subscribing to `$DB/_queue/{name}/msg/#` also works but is unnecessary — message IDs are single-level, so `msg/+` and `msg/#` match the same set of delivery topics.

### Monitoring

Subscribing to the queue delivery topic makes the subscriber a competing consumer — there is no way to passively observe deliveries. For monitoring queue throughput and activity, use ChangeEvents: subscribe to `$DB/_queue_{name}/events/#` to observe enqueue (create) and consume (delete) events without participating in the consumer group.

### Scoping

Queues support the existing `ScopeConfig`. A queue scoped to `(org, acme-corp)` restricts access to consumers within that scope. Events route through the scope's event channel.

### Cluster Mode

Queue entities are partitioned like any other entity — the partition primary owns the queue. Produce and consume operations from any node are forwarded to the primary via existing message routing. No scatter-gather needed because a queue is a single ordered stream within its partition.

Consumer state (inflight tracking, rate limiting) lives on the partition primary. When a consumer connected to node 2 subscribes to a queue owned by node 1, delivery is routed through the existing cross-node pub/sub infrastructure.

## Implementation Phases

### Phase 1: Queue storage and admin API
- Queue config in meta keys
- Admin operations: create, delete, list, config
- Reserved `_queue_` and `_dead_letter_` entity prefixes
- Topic protection rules for queue topics
- Queue message entity with internal fields (`_seq`, `_state`, `_delivery_count`, etc.)

### Phase 2: Produce and push delivery
- Send topic handler — creates message entity with `_state: available`
- Consumer subscription detection for queue topics (`$DB/_queue/{name}/msg/+`)
- QoS cap — grant QoS 1 in SUBACK for queue delivery topics (cap both QoS 0 up and QoS 2 down); cap producer send at QoS 1
- Push delivery loop — select next available message by `_seq`, mark inflight, deliver at QoS 1
- Prefetch via `receive_maximum` — use broker-level in-flight count (shared across all subscriptions)
- PUBACK interception — maintain packet-ID-to-message-ID mapping per consumer; when PUBACK arrives for a queue delivery, trigger queue ACK logic (delete message in default mode)
- Session expiry/clean_start handling — release inflight queue messages back to available instead of discarding

### Phase 3: ACK/NACK and visibility timeout
- Config topic handler — set `explicit_ack`, `prefetch`, `max_rate` per consumer; return success/error response
- ACK handler — delete message, emit ChangeEvent
- NACK handler — reset to available, increment delivery count (explicit ACK mode only)
- Background visibility timeout scanner (configurable interval)
- Consumer disconnect detection with grace period (explicit ACK mode)
- Dead letter routing when `max_deliveries` exceeded
- Queue depth limit enforcement on send

### Phase 4: Rate control
- Token bucket per consumer
- Delivery gate combining prefetch and rate limit
- Config topic for runtime prefetch/rate changes
- Backlog drain at controlled rate

### Phase 5: Cluster integration
- Queue operations forwarded to partition primary
- Consumer state on primary node
- Cross-node delivery via existing transport

## Future Extensions (not in initial implementation)

- **Message priority** — priority field, higher priority delivered first
- **Delayed/scheduled delivery** — `_available_at` timestamp, message invisible until then
- **Message TTL** — per-message expiry using existing `_expires_at` entity TTL
- **Dead letter replay** — admin API to move dead-lettered messages back to the queue
- **Batch ACK** — acknowledge multiple messages in one operation
- **Queue-to-queue routing** — automatic forwarding between queues
- **Message deduplication** — producer-side dedup key with time window
