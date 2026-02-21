# Chapter 3: MQTT 5.0 as a Database Protocol

Chapter 1 introduced the thesis: a system that unifies messaging and storage eliminates the two-system problem. Chapter 2 built the storage foundation — key encoding, pluggable backends, the atomic batch that ties data, indexes, and outbox entries into a single commit. This chapter connects the two. It shows how MQTT 5.0 becomes the wire protocol for a database: how PUBLISH messages become database operations, how topics become a dispatch table, and how a broker that subscribes to its own topics creates a complete request-response cycle without a custom protocol.

This is the chapter for two audiences. If you come from databases, the subscription and topic model will be unfamiliar — Section 3.1 covers the essentials. If you come from MQTT, the idea of turning a pub/sub protocol into a synchronous-feeling database API may seem forced — Section 3.3 shows how MQTT 5.0's response topics make it natural.

This chapter closes Part I. After this, the reader has the three foundation layers: storage (Chapter 2), protocol mapping (this chapter), and the thesis that unifies them (Chapter 1). Part II begins distributing the system.

## 3.1 MQTT 5.0 Essentials

MQTT is a binary publish/subscribe protocol over TCP (or QUIC, or WebSocket). A client connects to a broker, authenticates, and then publishes messages to topics or subscribes to topics to receive messages. Topics are hierarchical strings separated by `/` — `sensors/temperature/room-1`, `orders/created`, `$DB/users/create`. Topics are not pre-declared. Any client can publish to any topic at any time.

Two wildcards exist for subscriptions. `+` matches exactly one segment: `sensors/+/room-1` matches `sensors/temperature/room-1` and `sensors/humidity/room-1` but not `sensors/temperature`. `#` matches zero or more remaining segments: `sensors/#` matches everything under `sensors/`, including `sensors` itself.

Three QoS levels govern delivery guarantees. QoS 0 (at most once) is fire-and-forget — the broker makes a best-effort delivery with no acknowledgment. QoS 1 (at least once) adds a PUBACK packet: the broker confirms receipt, and the client can retransmit if the acknowledgment is lost. QoS 2 (exactly once) uses a four-packet handshake (PUBLISH, PUBREC, PUBREL, PUBCOMP) to guarantee that neither side processes the message more than once.

Retained messages let the broker store the last message published to a topic and deliver it immediately to new subscribers. This turns MQTT into a last-known-good cache: a subscriber joining late gets the current state without waiting for the next publish.

Sessions are persistent. A client disconnects and reconnects; the broker holds its subscription state for a configurable duration. MQDB sets this to 1 hour (`SESSION_EXPIRY_SECS = 3600`). A client can also request a clean start, discarding all prior session state.

MQTT 5.0 — the version MQDB uses — added features that make the protocol viable as a database interface: response topics (enabling request/response patterns), user properties (arbitrary key-value metadata per message), shared subscriptions (load-balanced delivery across a consumer group), topic aliases (compact representations for frequently-used topics), and session expiry intervals.

The MQDB broker runs with these defaults:

| Setting | Value | Source |
|---------|-------|--------|
| `max_clients` | 10,000 | `broker_defaults.rs:4` |
| `max_packet_size` | 10 MB | `broker_defaults.rs:5` |
| `maximum_qos` | 2 | `broker.rs:36` |
| `topic_alias_maximum` | 100 | `broker.rs:41` |
| `retain_available` | true | `broker.rs:37` |
| `shared_subscription_available` | true | `broker.rs:40` |
| `session_expiry_interval` | 3,600 seconds | `broker_defaults.rs:6` |

These are not aspirational settings. The 10 MB packet size accommodates large JSON documents — list responses with thousands of records, backup payloads, bulk operations. The 10,000 client limit reflects the intended deployment: IoT gateways with hundreds of devices, not a million-connection CDN. Both values are tuneable at startup, but the defaults reflect the system's target workload.

That is the protocol. The question is: how does a database use it? The naive approach would be to build a separate service — a translation layer that receives MQTT messages, calls a database API, and publishes responses. This would work, but it would reintroduce the two-system problem from Chapter 1: the MQTT broker and the database would be separate processes with separate failure modes, requiring exactly the synchronization infrastructure that MQDB exists to eliminate.

MQDB takes a different path. The database embeds itself inside the broker.

## 3.2 The Self-Subscribing Broker

The most important architectural decision in MQDB's protocol layer is that the broker subscribes to its own topics.

An MQTT broker routes messages between clients. Client A publishes to topic X, and the broker delivers that message to all clients subscribed to topic X. In MQDB, the broker itself is one of those clients. Three internal MQTT clients connect to the broker on startup, each with a specific role:

**`mqdb-internal-handler`** subscribes to `$DB/#` — the entire database topic namespace. Every database request from any external client arrives in this subscription callback. The callback pushes messages into an `mpsc::channel<Message>(256)`, and a `tokio::select!` loop processes them sequentially.

**`mqdb-response-publisher`** publishes responses back to clients' response topics. When the handler finishes processing a database request, the response is sent through this client, not through the handler client.

**`mqdb-event-publisher`** subscribes to the `Database`'s internal `broadcast::channel` for `ChangeEvent`s. When a database write produces a change event — every create, update, and delete does — the event publisher serializes it and publishes it to the appropriate event topic at QoS 1.

Why three clients instead of one? The handler receives messages via a subscription callback that pushes into an `mpsc` channel. If the same client published responses inside that callback, the broker's internal publish path could deadlock — the handler is processing an incoming message (holding broker resources) while trying to publish an outgoing message (which requires acquiring those same resources). The separate response client breaks the cycle. The event publisher is separate for the same reason: it publishes to event topics that the handler is subscribed to via `$DB/#`, and mixing roles would create a feedback loop.

Why loopback TCP connections to its own broker? Because the internal clients authenticate through the full MQTT auth stack. The broker generates a service account on startup — `mqdb-internal-{uuid}` with a random UUID password — and registers it with the password provider. The internal clients connect using these credentials. The same `TopicProtectionAuthProvider` that blocks external clients from internal topics wraps the entire auth chain, but it recognizes the service account via `is_internal_service` and lets it bypass topic restrictions. One auth path, not two. This means the service account is tested by the same code that tests every other user — no special-case authentication logic that can silently diverge.

The startup sequence in `MqdbAgent::run` proceeds in strict order:

1. `build_broker_config()` constructs the `BrokerConfig` with all MQTT 5.0 settings.
2. `apply_transport_config()` adds optional QUIC and WebSocket listeners.
3. `apply_auth_providers()` chains auth providers and wraps the final result in `TopicProtectionAuthProvider`.
4. `spawn_handler_task()` — the handler client connects, subscribes to `$DB/#`, enters the message loop.
5. `spawn_event_task()` — the event publisher connects, enters the `ChangeEvent` broadcast loop.
6. `spawn_http_task()` — optional HTTP/OAuth gateway.
7. `broker.run()` — the MQTT accept loop starts.

The handler and event tasks sleep briefly before connecting (100ms and 200ms respectively) to ensure the broker's accept loop is ready. This is a practical sequencing mechanism — the broker must be accepting connections before internal clients try to connect.

The 256-slot `mpsc` channel provides backpressure. Under QoS 0, if the handler cannot keep up, `try_send` drops messages silently — the channel is full, the callback returns, and the message is lost. Under QoS 1, the broker's own flow control prevents this: the PUBACK mechanism means the sending client waits for acknowledgment before sending the next message, naturally throttling the rate to what the handler can process.

## 3.3 Request/Response: From PUBLISH to Database

MQTT is fundamentally asynchronous. A client publishes a message and has no built-in way to receive a reply. MQTT 5.0 fixes this with response topics — a property on the PUBLISH packet that tells the receiver where to send the answer. MQDB uses this mechanism to turn an asynchronous protocol into a synchronous-feeling database API.

The client side is in the CLI's `execute_request` function. Three steps:

```rust
let response_topic = format!("mqdb-cli/responses/{}", uuid::Uuid::new_v4());
client.subscribe(&response_topic, move |msg| { /* collect response */ }).await;
client.publish_with_options(topic, payload, PublishOptions {
    properties: PublishProperties {
        response_topic: Some(response_topic.clone()),
        ..Default::default()
    },
    ..Default::default()
}).await;
```

Subscribe to a unique response topic, publish the request with that topic attached, wait for the response with a configurable timeout. The UUID in the response topic ensures no collisions between concurrent clients — two clients issuing simultaneous queries get independent response channels.

On the server side, `handle_message` in `handlers.rs` processes each message through a dispatch pipeline:

1. **Filter events.** Line 31: `if topic.contains("/events") { return; }`. Skip messages from event topics — these are the broker's own output, not client requests. Section 3.7 explains why this filter is essential.
2. **Try admin dispatch.** `parse_admin_topic(topic)` checks if the topic matches an admin operation. If so, handle it and return.
3. **Try DB dispatch.** `parse_db_topic(topic)` extracts entity, operation, and optional ID. If no match, log a warning and return.
4. **Build request.** `build_request(op, payload)` converts the JSON payload into a typed `Request` enum variant — the same `Request` type that Chapter 2's `Database::create`, `Database::read`, and friends accept.
5. **Extract metadata.** Pull `x-mqtt-sender` and `x-mqtt-client-id` from user properties.
6. **Execute.** `db.execute_with_sender(request, sender, client_id, ownership, scope_config)` runs the database operation with the authenticated user's identity.
7. **Respond.** If the original message had a response topic, serialize the response and publish it via QoS 1.

If the client omits the response topic, the write still happens — the response is simply not sent. This is fire-and-forget mode: useful for write-only telemetry where the client does not care about the result.

The response envelope is a JSON object with a consistent structure:

```json
{"status": "ok", "data": {"id": "abc-123", "name": "Alice", "_version": 1}}
{"status": "error", "code": 404, "message": "not found: users"}
```

Error codes follow HTTP status semantics: 400 for bad requests (malformed JSON, missing fields), 403 for forbidden operations (ACL or ownership violations), 404 for missing entities, 409 for constraint conflicts (unique, foreign key, or optimistic concurrency violations), and 500 for internal errors.

The connection to Chapter 2 is direct. `build_request` in `protocol/mod.rs` converts the `DbOperation` descriptor and raw bytes into the same `Request` enum that `Database::create`, `Database::read`, `Database::update`, `Database::delete`, and `Database::list` accept. The protocol layer does not have its own execution path — it parses the MQTT message into the database API's native type and calls the database directly. The MQTT protocol adds transport, authentication, and response routing. The database API provides storage, constraints, and atomicity. The boundary between them is a function call.

This separation matters when the system goes distributed. In cluster mode, the handler receives a message from a remote node, parses it into a `Request`, and routes it to the partition that owns the entity — potentially on a different node. The parse-and-dispatch logic is identical. Only the routing changes.

## 3.4 The Topic API

The topic hierarchy is the API. There is no URL router, no method table, no schema registry consulted at dispatch time. The topic string is split on `/`, pattern matched, and the match determines the operation.

### CRUD Topics

`parse_db_topic` in `protocol/mod.rs` strips the `$DB/` prefix and matches on the remaining segments:

```rust
match parts.as_slice() {
    [entity, "create"]       => Create { entity }
    [entity, "list"]         => List { entity }
    [entity, id]             => Read { entity, id }
    [entity, id, "update"]   => Update { entity, id }
    [entity, id, "delete"]   => Delete { entity, id }
    _ => None
}
```

The disambiguation rule: a two-segment topic where the second segment is not `"create"` or `"list"` is a Read. `$DB/users/abc-123` is a read. `$DB/users/create` is a create. This means `create` and `list` are reserved — you cannot have a record with ID `"create"` or `"list"`. In practice this never matters, because MQDB generates sequential numeric IDs by default, and client-provided IDs are typically UUIDs. But it reveals a fundamental property of topic-based API design: the topic structure is the grammar, and the grammar has reserved words.

Compare this to REST, where HTTP methods provide disambiguation: `GET /users/create` is unambiguous because the method is GET, not POST. In a topic-based API, there is no method verb — the topic string encodes both the resource and the operation. The topic is the method. This is why CRUD operations need dedicated suffixes (`/update`, `/delete`) while Read does not — Read is the default interpretation for any topic that does not match a keyword.

The pattern matching also explains why `$DB/` with no further segments returns `None` — it is not a valid database operation. The minimum is two segments: entity and operation-or-id. Three segments handle Update and Delete. More than three returns `None`. The grammar is strict: five patterns, no fallbacks, no ambiguity.

Request payloads follow the operation:

- **Create:** The JSON body becomes the record data. An empty payload creates an empty record (valid — the database assigns an ID and injects `_version: 1`). If the JSON contains an `id` field, it is used as the record identifier; otherwise, one is generated.
- **Read:** The payload is optional metadata — `{"includes": ["posts"], "projection": ["name", "email"]}` to request related records and field projection.
- **Update:** The JSON body is merged into the existing record (shallow merge, top-level keys replaced).
- **Delete:** Payload is ignored.
- **List:** The payload carries the full query: `{"filters": [...], "sort": [...], "pagination": {...}, "includes": [...], "projection": [...]}`. A convenience shorthand accepts `limit` and `offset` at the top level alongside the nested `pagination` object.
- **Bad JSON:** Immediate 400 error. The handler responds with the error to the response topic (if one exists) and stops processing.

### Admin Topics

Three namespaces under `$DB/` handle non-CRUD operations:

**`$DB/_health`** is the health check endpoint — the only `_`-prefixed topic accessible without admin privileges. Returns broker readiness, mode (agent or cluster), and partition count.

**`$DB/_sub/`** handles subscription management for the database's reactive subscription system (distinct from MQTT subscriptions): `subscribe`, `{id}/heartbeat`, `{id}/unsubscribe`.

**`$DB/_admin/`** contains 21 operations organized into subnamespaces: `schema/{entity}/set` and `schema/{entity}/get` for schema management; `constraint/{entity}/add` and `constraint/{entity}/list` for constraints; `backup`, `backup/list`, `restore` for backup management; `consumer-groups` and `consumer-groups/{name}` for consumer group inspection; `users/add`, `users/delete`, `users/list` for runtime user management; and nine ACL operations under `acl/rules/`, `acl/roles/`, and `acl/assignments/` for access control.

### Event Topics

Published by the event publisher, subscribed by clients:

- **Agent mode:** `$DB/{entity}/events/{id}` — one topic per record, event type embedded in the payload.
- **Cluster mode:** `$DB/{entity}/events/p{partition}/{id}` — partition number in the topic path enables MQTT shared subscription routing.
- **Scoped:** `$DB/{scope_entity}/{scope_value}/{entity}/events/{event_type}` — hierarchical event routing for multi-tenant scenarios. A subscriber to `$DB/tenants/acme/#` receives all events across all entities within that tenant.

The `event_type` suffix (`created`, `updated`, `deleted`) is derived from the `Operation` enum on the `ChangeEvent`. The `event_topic` method on `ChangeEvent` generates the topic path dynamically based on operation type, partition count, and scope configuration. In agent mode without scoping (where `num_partitions` is 0), the topic is `$DB/{entity}/events/{id}`. In cluster mode, partition routing is embedded in the path. With scoping enabled, the scope entity and value become topic segments, enabling hierarchical wildcard subscriptions across a tenant's entities.

This layered topic structure means that MQTT's native wildcard system provides the filtering that a database would implement in a query engine. A subscriber to `$DB/users/events/#` receives all user changes. A subscriber to `$DB/+/events/p3/#` receives all changes for records hashing to partition 3, across all entities. A subscriber to `$DB/tenants/acme/#` receives everything scoped to the "acme" tenant. No server-side filter registration is needed — the MQTT broker's topic matching engine does the work.

### The Hidden Layer

Several topic namespaces exist that external clients never see — they serve cluster-internal communication:

- `$DB/p{N}/{entity}/{op}` — partition-addressed binary API for cross-node database operations.
- `$DB/_idx/p{N}/update` — secondary index replication.
- `$DB/_unique/p{N}/reserve|commit|release` — unique constraint two-phase protocol.
- `$DB/_fk/p{N}/validate` — foreign key existence checks.
- `$DB/_query/{id}/request|response` — distributed query scatter-gather.

These topics are invisible to external clients because `TopicProtectionAuthProvider` blocks access at the authentication layer. No client — not even admin users — can publish or subscribe to these topics. Only the internal service account can reach them.

### Topic Protection

The `TopicProtectionAuthProvider` wraps every other auth provider as a decorator. It intercepts `authorize_publish` and `authorize_subscribe` calls and applies three protection tiers defined in `topic_rules.rs`:

| Tier | Effect | Topics |
|------|--------|--------|
| BlockAll | No publish, no subscribe, even for admin | `_mqdb/#`, `$DB/_idx/#`, `$DB/_unique/#`, `$DB/_fk/#`, `$DB/_query/#`, `$DB/p+/#` |
| ReadOnly | Subscribe allowed, publish blocked | `$DB/+/events/#`, `$SYS/#` |
| AdminRequired | Publish and subscribe require admin role | `$DB/_admin/#`, `$DB/_oauth_tokens/#` |

A catch-all rule blocks non-admin users from any `$DB/` topic where the entity starts with `_` (like `$DB/_sessions/list`), except `$DB/_health`.

The internal service account bypasses all three tiers. Without this, the handler client — which subscribes to `$DB/#`, overlapping with BlockAll topics — would be rejected by its own broker's auth layer. The bypass is simple: `is_internal_service` compares the `user_id` on the message against the stored service account username. A match means unrestricted access. A non-match falls through to the tier checks.

The ReadOnly tier for event topics (`$DB/+/events/#`) deserves explanation. External clients can subscribe to events — that is the entire point of a reactive database. But they cannot publish to event topics. Events are produced exclusively by the event publisher. If an external client could publish to `$DB/users/events/created`, it could inject fake change events into the event stream, breaking the guarantee that events reflect actual database state.

The `$SYS/#` tier follows the same logic. `$SYS/` is the MQTT convention for broker system topics — uptime, connected clients, message rates. Clients should read these for monitoring but never write to them. The ReadOnly tier enforces this at the auth layer, not by convention.

The three-tier model creates a clear security boundary. Regular database operations (`$DB/users/create`, `$DB/orders/list`) pass through with no special checks beyond the underlying ACL. Event and system topics are read-only. Admin operations require explicit admin role membership. Internal cluster topics are invisible. Each tier maps to a real security requirement, not an arbitrary classification.

## 3.5 User Properties as Metadata

MQTT 5.0 user properties are arbitrary key-value string pairs attached to any packet. MQDB uses three across two directions.

### Inbound: On Client Requests

| Property | Purpose |
|----------|---------|
| `x-mqtt-sender` | Authenticated user identity |
| `x-mqtt-client-id` | MQTT client ID |

These are set by the broker when it relays the PUBLISH to the internal handler. The broker knows the authenticated user from the CONNECT packet and the client ID from the session. It attaches them as per-message metadata — user properties that travel with each individual message.

Why not use MQTT's built-in username field? Because the internal handler processes messages from multiple clients sequentially through a single subscription. The username is a connection-level property — it identifies who connected to the broker, not who sent a particular message through the internal pipeline. User properties are per-message, so each PUBLISH carries the identity of its originator regardless of how many clients share the handler's subscription channel.

The handler extracts these in `handlers.rs`:

```rust
let sender_uid = message.properties.user_properties.iter()
    .find(|(k, _)| k == "x-mqtt-sender")
    .map(|(_, v)| v.as_str());

let mqtt_client_id = message.properties.user_properties.iter()
    .find(|(k, _)| k == "x-mqtt-client-id")
    .map(|(_, v)| v.as_str());
```

Both are passed to `db.execute_with_sender`, where `sender_uid` enables ownership checks (if configured) and `mqtt_client_id` is attached to the resulting `ChangeEvent` for attribution.

### Outbound: On Change Events

| Property | Purpose |
|----------|---------|
| `x-origin-client-id` | Identifies which client caused the mutation |

Set by the event publisher in `tasks.rs` when publishing change events:

```rust
if let Some(ref cid) = client_id {
    options.properties.user_properties.push((
        "x-origin-client-id".to_string(),
        cid.clone(),
    ));
}
```

Subscribers use this to filter events they caused. A UI that updates a record does not want to react to its own update event by re-rendering — it already has the new state. The `x-origin-client-id` property lets the subscriber compare the event's originator against its own client ID and skip the echo.

### OpenTelemetry Propagation

When the `opentelemetry` feature is enabled, `handlers.rs` extracts W3C trace context — `traceparent` and `tracestate` — from user properties and attaches the parent span to the database operation. A trace starting in a Python MQTT client continues through the Rust database handler. The MQTT user properties serve as the propagation medium, replacing the HTTP headers that OpenTelemetry typically uses for context propagation. The database operation's span becomes a child of the caller's span, connecting the distributed trace across the protocol boundary.

## 3.6 QoS as a Durability Knob

MQTT's QoS levels map naturally to database durability requirements.

**QoS 0 (at most once)** is fire-and-forget. The broker delivers the message to the handler, but there is no acknowledgment. If the handler is busy, the message may be dropped (the 256-slot channel uses `try_send`). For write-only telemetry — sensor readings, metrics, logs — losing an occasional message is acceptable.

**QoS 1 (at least once)** is the default for all database operations. The broker sends a PUBACK after processing. The acknowledgment provides two things: confirmation that the write landed in the handler's channel, and natural flow control. The client waits for PUBACK before sending the next message, which prevents the client from outrunning the handler.

**QoS 2 (exactly once)** uses a four-packet exchange. In cluster mode, QoS 2 state — the PUBREC/PUBREL tracking data — is replicated across nodes so that a node failure during the exchange does not break the exactly-once guarantee.

All database responses are published at QoS 1 regardless of the request's QoS. The handler calls `client.publish_qos1(response_topic, payload)` unconditionally. This means the response is acknowledged by the broker before the handler moves on — the client is guaranteed to receive the response if it is connected and subscribed.

### Change-Only Delivery

An MQDB-specific broker optimization addresses a problem with retained messages and reconnecting subscribers. In standard MQTT, when a subscriber reconnects and its session is restored, it receives all retained messages matching its subscriptions. For a subscriber to `$DB/+/events/#`, this could mean thousands of last-known-good events flooding in at reconnection — the last event for every entity in the database.

The `ChangeOnlyDeliveryConfig` in the broker configuration targets three topic patterns:

```
$DB/+/events/#
$DB/+/+/events/#
$DB/+/+/+/events/#
```

When change-only delivery is enabled for these patterns, reconnecting subscribers only receive retained messages whose payload has changed since the subscriber last received them. New subscribers still get the initial retained message (the current state); reconnecting subscribers get only genuinely new events. This converts what would be a flood of redundant data into a minimal catch-up stream.

The three topic patterns cover different scoping depths: single-segment entity (`$DB/+/events/#`), two-segment scoped entity (`$DB/+/+/events/#`), and three-segment deeply-scoped entity (`$DB/+/+/+/events/#`). The `+` wildcards match any entity or scope value. These patterns mirror the event topic structures described in Section 3.4 — agent mode, cluster mode, and scoped mode.

Without change-only delivery, MQTT's retained message semantics create a tension with event-driven subscriptions. Retained messages are designed for "current state" use cases — a temperature sensor publishes a retained message, and new subscribers immediately know the current temperature. Event topics carry a different semantic: "something happened." Replaying thousands of "something happened" messages to a reconnecting subscriber does not help — the subscriber needs "what happened since I disconnected," not the entire event history. Change-only delivery bridges this gap by suppressing duplicate retained payloads while preserving new ones.

### The QoS Spectrum for Database Workloads

The choice of QoS level is a per-operation decision that depends on the workload.

Write-only sensor telemetry — temperature readings arriving every second from thousands of devices — naturally fits QoS 0. The occasional dropped reading is acceptable, and the reduced protocol overhead (no PUBACK, no retransmission) means lower latency and battery consumption on constrained devices.

Transactional database operations — create a user, update an order, delete a record — require QoS 1. The PUBACK confirms that the message reached the handler, the response topic carries the result, and the flow control prevents the client from overwhelming the broker. QoS 1 does not guarantee exactly-once processing at the database level (the handler may process a retransmitted message twice), but MQDB's optimistic concurrency control via `_version` catches duplicate updates — the second attempt fails with a conflict because the expected version has already been incremented.

QoS 2 is available for operations where duplicate processing is unacceptable even at the transport level — financial transactions, one-time token consumption. The four-packet exchange guarantees that the message is processed exactly once by the handler. In cluster mode, QoS 2 state is replicated (Chapter 12), so the guarantee survives node failures.

## 3.7 What Went Wrong

### The Feedback Loop

Line 31 of `handlers.rs`:

```rust
if topic.contains("/events") {
    return;
}
```

One line. Easy to miss. Essential.

The handler subscribes to `$DB/#`. The event publisher publishes to `$DB/users/events/created`. Both live in the same broker. Without the filter, every database write triggers a change event, the event is published to an event topic, the event topic matches `$DB/#`, the broker delivers it to the handler, and the handler tries to parse it as a database request.

The parse fails — `$DB/users/events/created` does not match any `parse_db_topic` or `parse_admin_topic` pattern — but only after JSON deserialization and pattern matching. Under write-heavy loads, the handler spends more time rejecting its own events than processing real requests.

The overlap between the handler's subscription (`$DB/#`) and the event publisher's output (`$DB/{entity}/events/...`) is intentional. The handler needs all `$DB/` topics to receive database requests. The event publisher needs `$DB/{entity}/events/` topics to deliver change events. The two topic spaces overlap, and no MQTT subscription filter can separate them — wildcards match patterns, not intent. So the filter must be explicit, in application code, at the top of the message handler.

This is a general lesson for self-subscribing architectures: when your subscriber wildcard overlaps with your publisher's topic space, every publish triggers a receive. The architecture creates a cycle. The application must break it.

An alternative design would use separate topic prefixes — `$DB_REQ/` for requests and `$DB_EVT/` for events — so the handler could subscribe to `$DB_REQ/#` without matching event topics. We chose the unified `$DB/` prefix because it makes the API surface coherent: all database-related topics live under one namespace, clients subscribe to `$DB/#` to see everything, and the topic hierarchy reads as a single tree. The cost is the explicit filter in the handler. The benefit is a simpler mental model for API users.

### The Backpressure Discovery

During async benchmark development, QoS 0 achieved approximately 5,000 operations per second before TCP connections started resetting. QoS 1 hit approximately 12,000 operations per second without issues.

The root cause is flow control. QoS 0 has none. The client publishes messages as fast as TCP accepts bytes into its send buffer. The broker receives them, deserializes JSON, executes database operations, and publishes responses — all slower than the raw TCP receive rate. The 256-slot `mpsc` channel fills up. `try_send` starts dropping messages. But the client does not know — QoS 0 provides no feedback. TCP buffers accumulate, backpressure propagates to the TCP layer, and eventually the broker's connection handling falls behind, triggering connection resets.

QoS 1's PUBACK acts as natural backpressure. The client sends a message, waits for the PUBACK, sends the next message. Even with pipelining (multiple in-flight messages), the client cannot outrun the broker by more than the in-flight window size. The PUBACK creates a closed-loop system where the client's send rate is bounded by the broker's processing rate.

The performance lesson is counterintuitive: the protocol's reliability mechanism matters more as a flow control mechanism than as a delivery guarantee. The same acknowledgment that prevents message loss on a 2G cellular link prevents buffer overflow on a localhost benchmark. QoS 0 "should be faster" because it has less protocol overhead — no PUBACK packet, no retransmission timer, no inflight tracking. In practice, the overhead of waiting for PUBACK is what keeps the system stable under load. QoS 1 is not just more reliable than QoS 0 — it is faster, because it does not crash.

The async benchmark mode (pipelined, not request-response) makes this especially visible. In pipelined mode, the client sends messages without waiting for individual responses, measuring how many operations the system can sustain per second. With QoS 0, the client has no signal to slow down. With QoS 1, the broker's PUBACK processing rate becomes the natural speed limit — the client can have multiple messages in flight, but the inflight window bounds the gap between send rate and processing rate. The benchmark documentation records this as an explicit warning: QoS 0 in async mode causes connection resets at approximately 5,000 operations per second due to lack of flow control.

This is not unique to MQTT. TCP uses acknowledgments for flow control. gRPC uses HTTP/2 flow control frames. Any protocol that removes acknowledgments — for performance, for simplicity, for any reason — must replace the flow control mechanism with something else, or accept that the sender will eventually overwhelm the receiver. MQTT QoS 0 removes acknowledgments and provides no replacement. The result is predictable in hindsight.

### The Service Account Bootstrapping Problem

The internal clients need credentials to authenticate with the broker. The broker generates these credentials — `mqdb-internal-{uuid}` with a random UUID password — during startup, before the accept loop begins. But the credential generation path depends on the authentication configuration. Three cases arise:

When a password file is configured, the service account is added to the password provider using the same `add_user` API that external user management uses. The account lives alongside human-created users in the same credential store. When SCRAM or JWT authentication is configured (enhanced auth methods), the service account bypasses the enhanced auth entirely — it authenticates via username/password through a `CompositeAuthProvider` that tries the primary auth (SCRAM/JWT) first and falls back to the password provider. When anonymous mode is enabled, the service account is still created — anonymous clients do not need credentials, but the internal clients do, because `TopicProtectionAuthProvider` must identify them to grant the bypass.

The initial implementation of anonymous mode exposed a gap. The service account was created, but `TopicProtectionAuthProvider` did not know about admin users in anonymous mode (there were none configured). Internal clients that needed to publish to admin topics (`$DB/_admin/`) or event topics (`$DB/+/events/#`) were blocked by their own topic protection. The fix added an `all_users_admin` flag: when anonymous mode is enabled and no explicit admin users are configured, all users are treated as admin. This is acceptable because anonymous mode is a development convenience, not a production security posture.

The broader lesson: when a system authenticates with itself, every authentication path in the system must accommodate self-referential connections. A new auth method (SCRAM) broke internal connectivity because the internal clients used password auth, not SCRAM. The `CompositeAuthProvider` — try primary, fall back to secondary — was the fix. Every future auth method addition must account for the internal service account, or the broker will lock itself out of its own database.

## What Comes Next

Part I is complete. The reader now has three layers: a flat key-value store with atomic batches (Chapter 2), MQTT 5.0 as the wire protocol with topic-based dispatch and request-response semantics (this chapter), and the thesis that unifies them — storage and messaging as a single concern (Chapter 1).

Part II begins distributing the system. Chapter 4 introduces partitioning: how 256 fixed partitions divide the keyspace, how hash-based routing determines which node owns which record, and how the partition map is maintained as the cluster evolves. Everything built in Part I — the storage engine, the protocol layer, the reactive subscriptions — must now work across multiple nodes. The challenges multiply accordingly.

The protocol mapping described in this chapter survives distribution largely intact. The topic hierarchy, the request-response pattern, the user properties, the topic protection tiers — all of these work identically in cluster mode. What changes is routing: a PUBLISH to `$DB/users/create` may arrive at a node that does not own the partition for that user. The node must forward the request to the partition owner, wait for the response, and relay it back. The protocol layer does not know or care about this forwarding — it sees the same MQTT message, the same topic, the same response topic. The distribution layer, introduced in the next four chapters, handles the rest.
