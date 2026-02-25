# Chapter 7: Transport Layer Evolution

The Raft messages described in Chapter 6 — `RequestVote`, `AppendEntries`, and their responses — must travel between nodes somehow. So must the replication writes from Chapter 5, the heartbeats, the forwarded publishes, and every other cluster message. This chapter covers the transport layer: how MQDB started with MQTT bridges, discovered they degraded performance 5-10x on nodes with bridges, and replaced them with direct QUIC connections.

The narrative follows a pattern that recurs throughout distributed systems work: start with the obvious design, measure it under realistic conditions, discover that the bottleneck is not where you expected, build an abstraction that isolates the problem, and swap the implementation. Each step is instructive on its own. Together they illustrate why performance work demands profiling data, not intuition.

## 7.1 MQTT Bridges: The Initial Design

The starting point was obvious: MQDB already has a full MQTT broker. Why not use it for cluster communication?

A bridge is an MQTT client connection from one node's broker to another's. Messages published on cluster topics (`_mqdb/cluster/#` for coordination, `_mqdb/repl/+/+` for replication, `_mqdb/forward/+` for forwarded publishes) are delivered to all nodes subscribed to those topics. Each node's transport creates two internal MQTT clients:

```rust
struct MqttTransport {
    node_id: NodeId,
    client: MqttClient,          // subscribes to cluster topics, receives inbound
    forward_client: MqttClient,  // publishes forwarded messages
}
```

Why two clients? The `client` subscribes to cluster topics and receives all inbound messages. If the same client also published forwarded messages, those messages would trigger its own subscription callbacks — a feedback loop. The `forward_client` publishes without subscribing, breaking the cycle.

The advantages of this approach were real. Zero new dependencies: the broker already handles connection management, message framing, and delivery guarantees. Topic-based routing: subscribe to `_mqdb/repl/+/+` for replication traffic, `_mqdb/cluster/heartbeat/+` for heartbeats. Built-in QoS: at-least-once for replication, at-most-once for heartbeats. Standard debugging tools: any MQTT client can subscribe to cluster topics and observe the traffic.

On startup, the transport subscribes to five topic patterns that partition cluster traffic by purpose:

| Topic Pattern | Purpose | QoS |
|---------------|---------|-----|
| `_mqdb/cluster/nodes/{node_id}` | Directed messages (Raft, queries) | At-least-once |
| `_mqdb/cluster/broadcast` | Broadcast messages (partition updates) | At-least-once |
| `_mqdb/cluster/heartbeat/+` | Heartbeats from all nodes | At-most-once |
| `_mqdb/repl/+/+` | Replication writes (`p{partition}/seq{n}`) | At-least-once |
| `_mqdb/forward/+` | Forwarded publishes (cross-node pub/sub) | At-least-once |

Each subscription registers a callback that deserializes the MQTT payload into a `ClusterMessage` and pushes it onto the shared inbox channel. The inbox is a bounded `flume` channel (capacity 16,384), the same channel type used by the QUIC transport. All five callbacks feed the same inbox, and the event loop drains it without knowing which subscription produced each message.

The QoS mapping uses MQTT's delivery guarantees intentionally. Heartbeats use QoS 0 (at-most-once) because they are periodic and self-healing — a missed heartbeat will be replaced by the next one. Raft votes and replication writes also use QoS 0 because the Raft protocol handles retransmission at the application level. Most other messages use QoS 1 (at-least-once), including replication topic publishes and forwarded publishes, because loss would require complex recovery. The MQTT broker handles the PUBACK handshake transparently — the transport code specifies the QoS level and the broker ensures delivery.

The transport code was thin. All it had to do was serialize a `ClusterMessage` into bytes, publish it to the right topic, and deserialize inbound messages from subscription callbacks. The broker handled everything else. This simplicity was the design's strength — and its weakness, as benchmarking would reveal.

## 7.2 Bridge Topology Options

When node A bridges to node B, A opens an MQTT client connection to B's broker. The direction of message flow depends on the bridge configuration:

- **Out**: local publishes are forwarded to the remote broker.
- **Both**: bidirectional — local publishes go to the remote, and the remote's publishes come back.

For a 3-node cluster, three topology options determine which nodes bridge to which:

| Topology | Peer Configuration | Bridges per Node (N1, N2, N3) |
|----------|-------------------|-------------------------------|
| `partial` (default) | Each node bridges to lower-numbered nodes | 0, 1, 2 |
| `upper` | Each node bridges to higher-numbered nodes | 2, 1, 0 |
| `full` | Each node bridges to all others | 2, 2, 2 |

The topology code computes peers for each node at startup:

```rust
let peers: Vec<String> = match topology_name {
    "full" => (1..=nodes)
        .filter(|&n| n != node_id)
        .map(|n| format!("{}@127.0.0.1:{}", n, 1882 + u16::from(n)))
        .collect(),
    "upper" => ((node_id + 1)..=nodes)
        .map(|n| format!("{}@127.0.0.1:{}", n, 1882 + u16::from(n)))
        .collect(),
    _ => (1..node_id)  // partial: to lower-numbered nodes
        .map(|n| format!("{}@127.0.0.1:{}", n, 1882 + u16::from(n)))
        .collect(),
};
```

Bridge direction interacts with topology in a non-obvious way. Asymmetric topologies (partial, upper) require `Both` direction because each bridge must carry traffic in both directions — node 2's single bridge to node 1 must forward 2→1 messages AND receive 1→2 messages. Full mesh topology uses `Out` direction because the symmetric connections form bidirectional pairs: node 1's out-bridge to node 2 plus node 2's out-bridge to node 1 covers both directions.

Full mesh with `Both` direction was tested. The result: channel overflow within seconds, cluster unresponsive. The problem is message amplification. Node 1 publishes a heartbeat. Its bridge forwards it to node 2. Node 2's bridge, configured for bidirectional forwarding, forwards it to node 3. Node 3's bridge forwards it back to node 1. The heartbeat circulates forever, multiplying with each cycle.

Bridge loop prevention addresses this for normal operation. Bridge clients use predictable IDs like `mqdb-node-1-to-node-2`. The broker's router detects this pattern and skips forwarding messages that originated from bridge clients to other bridges. Without this check, messages amplify exponentially even in asymmetric topologies:

1. Node 1 publishes heartbeat
2. Bridge delivers to Node 2 (as client `node-1-to-node-2`)
3. Without loop prevention, Node 2's router forwards to Node 3's bridge
4. Node 3's router forwards back to Node 1's bridge
5. Infinite loop

The loop prevention filter breaks this chain by recognizing bridge-origin messages and excluding them from further bridge forwarding. But this filter cannot save full mesh with `Both` direction — the symmetric bidirectional bridges create forwarding paths that the router's pattern matching cannot distinguish from legitimate new messages.

## 7.3 The Bridge Overhead Problem

Benchmarking revealed a severe performance pattern: throughput correlated inversely with bridge count, not with being the Raft leader, not with partition ownership, but with the number of MQTT bridge connections on the node.

Async insert throughput by bridge count (MQTT transport, 3-node cluster):

| Bridges | Insert ops/s | Get ops/s | Update ops/s |
|---------|-------------|-----------|-------------|
| 0 | 7,415–9,800 | 977–15,548 | 8,374–9,800 |
| 1 | 792–1,872 | 1,252–1,262 | 560–792 |
| 2 | 1,196–2,128 | 1,220–1,327 | 542–807 |

Each bridge reduces throughput by 5-10x. The node with zero bridges performs 6-8x better than nodes with two bridges. In partial topology, node 1 (0 bridges) achieves 9,084 insert ops/s while node 2 (1 bridge) manages 1,414. In full mesh (every node has 2 bridges), the entire cluster drops to a mean of 1,690 insert ops/s.

For updates, the penalty is even worse. Full mesh MQTT averages 569 update ops/s — the worst node manages only 542 ops/s. The update path involves a write plus replication acknowledgment, meaning cluster messages dominate even more than for simple inserts.

The initial hypothesis was RwLock contention. Bridges hold the shared state lock while processing messages; DB operations wait for the same lock. More bridges means more lock holders, more waiting, lower throughput. Intuitive and wrong.

Profiling with per-operation timing instrumentation produced this data:

| Node | Bridges | Throughput | Avg Lock Wait | Avg Handle Time |
|------|---------|-----------|---------------|-----------------|
| Node 1 | 2 | 1,353 ops/s | 0.23 µs | 55.68 µs |
| Node 2 | 1 | — | 1.24 µs | 57.17 µs |
| Node 3 | 0 | 4,222 ops/s | 1.82 µs | 54.26 µs |

Node 3, the fastest node, has the **highest** lock wait time at 1.82 µs. Node 1, the slowest, has the lowest at 0.23 µs. If lock contention were the bottleneck, the relationship would be reversed. The hypothesis is disproven.

The actual differentiator is cluster message volume:

| Node | Bridges | Cluster Messages Processed | DB Operations | Ratio |
|------|---------|---------------------------|---------------|-------|
| Node 1 | 2 | 42,685 | 23,076 | 1.85:1 |
| Node 3 | 0 | 14,233 | 148,051 | 0.096:1 |

Node 1 processes 3x more cluster messages and performs 6.4x fewer DB operations than Node 3. The CPU time explanation is straightforward: bridge callbacks invoke `route_message_local_only()`, which publishes to the local broker. The local broker's event loop, the cluster message processing loop, and client request handling all run on the same Tokio async runtime. Cluster traffic steals CPU cycles from database operations.

The message type distribution makes the imbalance concrete. Node 1 (the Raft leader with 2 bridges) processed 13,393 Write messages (replication to replicas), 23,094 Ack messages (acknowledgments from replicas), 3,618 heartbeats, and 1,216 Raft messages during the benchmark run. Node 3 (0 bridges) processed only 28 Write messages, 10,825 Acks, 1,465 heartbeats, and 1,426 Raft messages. The leader's bridge connections deliver a flood of acknowledgments and replication traffic that the bridge-free node never sees.

Queue depth measurements confirmed this diagnosis. Average queue depth across all nodes stayed between 1.0 and 1.3 messages, with maximums under 25. Messages were not backing up — they were being processed promptly. The problem was that processing them consumed time the runtime would otherwise spend on client DB requests.

The architecture at fault:

```
MQTT Bridge:  Node 1 → MQTT PUBLISH → Node 2 broker → event loop → inbox
                                        ↑ competes with client DB traffic
```

Every cluster message arrives as an MQTT publish, enters the broker's message router, gets dispatched through subscription callbacks, and only then reaches the cluster inbox. The broker's event loop processes both cluster and client traffic in the same thread pool. More bridges mean more cluster traffic flowing through the shared broker, leaving less CPU for the operations users actually care about.

Four alternative hypotheses were evaluated and rejected based on the profiling data:

1. **Split the lock** — rejected because lock wait times are sub-microsecond on all nodes
2. **Batch cluster messages** — rejected because processing time per message is similar across nodes
3. **Defer bridge processing** — rejected because queue depth is already low (no backlog)
4. **Use read locks for reads** — rejected because lock contention is not the issue

The data pointed to one root cause: cluster traffic and client traffic sharing the same runtime. The fix had to separate them. No optimization to lock granularity, message batching, or queue management could address this — the problem was architectural, not algorithmic.

## 7.4 The Transport Abstraction

Before building the replacement, we needed an interface that both transports could implement. The `ClusterTransport` trait defines what the rest of the cluster expects from any transport:

```rust
pub trait ClusterTransport: Send + Sync + Debug + Clone {
    fn local_node(&self) -> NodeId;

    fn send(&self, to: NodeId, message: ClusterMessage)
        -> impl Future<Output = Result<(), TransportError>> + Send;

    fn broadcast(&self, message: ClusterMessage)
        -> impl Future<Output = Result<(), TransportError>> + Send;

    fn recv(&self) -> Option<InboundMessage>;
    fn try_recv_timeout(&self, timeout_ms: u64) -> Option<InboundMessage>;
    fn requeue(&self, msg: InboundMessage);
    fn pending_count(&self) -> usize;

    fn queue_local_publish(&self, topic: String, payload: Vec<u8>, qos: u8)
        -> impl Future<Output = ()> + Send;
    // ... retained and property-enriched variants
}
```

Several design decisions are worth examining.

**Async send, synchronous receive.** `send()` and `broadcast()` are async — the caller awaits completion. `recv()` is synchronous and non-blocking — it returns `None` if no message is available. This asymmetry reflects the event loop's structure: it sends messages in response to specific events (replication writes, Raft votes) but polls for inbound messages on a regular cadence.

**Requeue for message reordering.** The `requeue()` method pushes a message back to the front of the inbox. The event loop sometimes receives a message it cannot process yet — a replication write for a partition that is mid-migration, or an AppendEntries for a term that hasn't been committed locally. Rather than dropping or spawning a retry, the loop requeues the message and processes it on the next iteration.

**Return Position Impl Trait in Trait (RPITIT).** The `send()` signature uses `-> impl Future<Output = ...> + Send` instead of `-> Box<dyn Future<Output = ...> + Send>`. This Rust feature (stabilized in 1.75) avoids heap allocation for every send operation. In a system processing thousands of cluster messages per second, eliminating the `Box` overhead per message matters.

The `ClusterMessage` enum carries all 35 message types that traverse the cluster: heartbeats, Raft protocol messages (`RequestVote`, `AppendEntries`, and their responses), replication writes and acknowledgments, forwarded publishes, snapshot transfer chunks, query requests and responses, partition updates, wildcard and topic subscription broadcasts, JSON DB requests, unique constraint protocol messages (reserve, commit, release with their responses), and foreign key check messages. Every inter-node message in MQDB is a variant of this enum, regardless of which transport carries it.

A `ClusterTransportKind` enum wraps both implementations:

```rust
pub enum ClusterTransportKind {
    Mqtt(MqttTransport),    // deprecated
    Quic(QuicDirectTransport),
}
```

This enum implements `ClusterTransport` by delegating to whichever variant is active. Every caller — the Raft coordinator, the replication pipeline, the heartbeat manager, the event loop — uses `ClusterTransport` methods. Swapping MQTT for QUIC required zero changes to any caller.

## 7.5 Direct QUIC Transport

The replacement bypasses the MQTT broker entirely:

```
Direct QUIC:  Node 1 → QUIC stream → Node 2 listener → inbox (bypasses broker)
```

Each node runs a QUIC server on a dedicated cluster port, offset 100 from the MQTT bind port (e.g., MQTT on 1883, cluster on 1983). The offset is configurable but defaults to 100. This second endpoint serves only cluster traffic — heartbeats, Raft messages, replication writes, forwarded publishes. Client MQTT connections never touch it.

The core data structure is a peer connection map:

```rust
struct PeerConnection {
    _connection: Connection,
    send_stream: tokio::sync::Mutex<SendStream>,
}

struct QuicDirectTransport {
    node_id: NodeId,
    endpoint: Arc<RwLock<Option<Endpoint>>>,
    peers: Arc<RwLock<HashMap<NodeId, PeerConnection>>>,
    inbox_tx: flume::Sender<InboundMessage>,
    inbox_rx: flume::Receiver<InboundMessage>,
    requeue_buffer: Arc<Mutex<VecDeque<InboundMessage>>>,
    message_notify: Arc<Notify>,
    // ...
}
```

Each peer gets a `PeerConnection` with its own `SendStream` protected by a `tokio::sync::Mutex`. The mutex serializes writes to a single peer's stream (preventing the packet corruption that caused the fire-and-forget bug in Section 7.8) without blocking writes to other peers. The `requeue_buffer` provides the front-of-queue reinsertion that the `ClusterTransport` trait requires.

The connection lifecycle has three phases:

**Binding.** `bind()` creates the QUIC endpoint and spawns an `acceptor_task()` that listens for incoming connections from peer nodes. When a peer connects, the acceptor reads a 2-byte node ID header from the incoming stream, registers the peer in the connection map, and spawns a `receiver_task()` for that connection.

**Connecting.** `connect_to_peer()` opens a bidirectional QUIC stream to a peer's cluster port. QUIC streams are multiplexed within a single connection, so multiple logical streams can coexist without head-of-line blocking — but MQDB uses one bidirectional stream per peer, which suffices for the message rates involved. The connecting node sends its own 2-byte node ID as a header, then stores the send half of the stream in the peer connection map. A `receiver_task()` is spawned for the receive half. The result is symmetric: whether node A initiated the connection to node B or vice versa, both nodes end up with a send stream and a receiver task for each peer.

**Messaging.** `send_to_peer()` serializes the message and writes it to the peer's send stream with length-prefixed framing:

```
[4 bytes: payload length, big-endian u32]
[2 bytes: sender node ID, big-endian u16]
[1 byte: message type]
[variable: binary-encoded payload]
```

The receiver task reads the 4-byte length prefix, allocates a buffer of that size, reads the payload, and parses it into an `InboundMessage` using the same `ClusterMessage::decode_from_wire()` method that the MQTT transport uses. The binary protocol is identical between both transports — only the delivery mechanism differs.

The receiver task pushes parsed messages into a bounded `flume` channel (capacity: 16,384 messages) and notifies the event loop via a `tokio::sync::Notify`. If the inbox is full, the message is dropped with a warning log that includes the message type — back-pressure rather than unbounded growth.

Key constants from the implementation:

| Constant | Value | Purpose |
|----------|-------|---------|
| `SEND_TIMEOUT_MS` | 5,000 ms | Per-write timeout for stream writes |
| `INBOX_CHANNEL_CAPACITY` | 16,384 | Bounded inbox size |
| `MAX_MESSAGE_SIZE` | 10 MB | Reject oversized messages on receive |

The 5-second send timeout prevents a slow or disconnected peer from blocking the sender indefinitely. The length prefix is written first, then the payload, each with an independent timeout. If either write times out, the send fails with a `TransportError::SendFailed` and the caller (typically the broadcast loop) logs a warning and continues to the next peer.

Broadcasting iterates all peers sequentially, sending to each. A failure to reach one peer does not abort the broadcast — the remaining peers still receive the message. This design trades strict all-or-nothing delivery for resilience: a temporarily unreachable node does not block heartbeats or Raft messages to healthy nodes.

## 7.6 mTLS for Cluster Security

Cluster nodes authenticate each other using mutual TLS. The QUIC protocol mandates TLS 1.3, so encryption is always present. The question is whether nodes verify each other's identity.

Each node has two QUIC endpoints that share the same certificate files but serve different purposes:

| Endpoint | Port | Purpose | Authentication |
|----------|------|---------|----------------|
| MQTT broker QUIC listener | `--bind` port (e.g., 1883) | MQTT clients over QUIC | MQTT-level auth (password, SCRAM, JWT) |
| Cluster transport | `--bind` + 100 (e.g., 1983) | Heartbeats, Raft, replication | mTLS (when `--quic-ca` provided) |

When `--quic-ca` is provided, mTLS is enabled on the cluster endpoint:

- **Server side**: a `WebPkiClientVerifier` built from the CA certificate requires every connecting node to present a client certificate signed by that CA. Connections without valid client certificates are rejected at the TLS handshake.
- **Client side**: the connecting node presents its own `--quic-cert`/`--quic-key` as client identity via `with_client_auth_cert()`. The same certificate that identifies the node as a server also serves as its client identity.

The same certificate serves both roles. It must carry both `serverAuth` and `clientAuth` Extended Key Usage (EKU) extensions. The project's `generate_test_certs.sh` script produces certificates with both EKUs for this dual-role usage.

When `--quic-ca` is omitted, the cluster transport falls back to one-way TLS: the server presents its certificate, the client verifies it, but the server does not verify the client. A warning is logged. This mode is acceptable for development (where all nodes run on localhost) but insufficient for production, where an unauthorized node could join the cluster and receive replicated data.

Production deployments should use a dedicated CA for cluster certificates, separate from any CA used for external client TLS. This prevents an external client with a valid TLS certificate from being mistakenly accepted as a cluster peer.

## 7.7 Performance Results

Async insert throughput by topology (3-node cluster):

| Topology | Transport | N1 | N2 | N3 | Mean | Variance |
|----------|-----------|------|------|------|------|----------|
| Partial | MQTT | 9,084 | 1,414 | 0* | 3,499 | — |
| Partial | QUIC | 8,472 | 8,457 | 8,531 | 8,487 | 1.009x |
| Upper | MQTT | 2,128 | 1,872 | 7,415 | 3,805 | — |
| Upper | QUIC | 8,664 | 8,525 | 8,480 | 8,556 | 1.022x |
| Full | MQTT | 2,116 | 1,196 | 1,757 | 1,690 | — |
| Full | QUIC | 8,579 | 8,621 | 8,354 | 8,518 | 1.032x |

*N3 Partial MQTT had connectivity issues

The data tells the story along two axes: absolute throughput and variance between nodes.

**MQTT variance between nodes** ranges from moderate to extreme. In partial topology, node 1 (0 bridges) achieves 9,084 ops/s while node 2 (1 bridge) manages 1,414 — a 6.4x gap. For async updates, the worst case is full mesh: the fastest node achieves 584 ops/s and the slowest 542. The MQTT update variance across all topologies spans from 542 to 9,800 ops/s — an 18x spread.

**QUIC variance between nodes** is negligible. The maximum spread is 1.032x (full mesh insert), meaning the fastest node in the cluster is only 3.2% faster than the slowest. Across all topologies and operation types, QUIC variance stays between 1.009x and 1.123x.

The per-operation improvements for bridged nodes are dramatic:

| Operation | MQTT Range (all nodes) | QUIC Mean | Improvement for Bridged Nodes |
|-----------|----------------------|-----------|-------------------------------|
| Async Insert | 0–9,084 ops/s | ~8,500 ops/s | 4-6x |
| Async Get | 977–15,548 ops/s | ~16,600 ops/s | 12-14x |
| Async Update | 542–9,800 ops/s | ~9,300 ops/s | 16x (full mesh) |

The topology comparison at the mean level:

| Topology | MQTT Mean | QUIC Mean | QUIC Advantage |
|----------|-----------|-----------|----------------|
| Partial | 3,499 | 8,487 | 2.4x |
| Upper | 3,805 | 8,556 | 2.2x |
| Full | 1,690 | 8,518 | 5.0x |

QUIC's advantage increases with mesh density because MQTT bridge overhead compounds. In partial topology (1.3 bridges per node on average), QUIC is 2.4x faster. In full mesh (2 bridges per node), QUIC is 5x faster. The pattern holds across operation types — for gets, QUIC achieves a uniform ~16,500 ops/s while MQTT bridged nodes drop to 1,200-1,300; for updates, QUIC maintains ~9,300 ops/s while MQTT full mesh collapses to 569.

The most consequential result is that topology choice becomes irrelevant with QUIC. Partial, upper, and full mesh all achieve essentially identical throughput (~8,500 insert ops/s mean). Full mesh is now viable — it was unusable with MQTT bridges because the bridge overhead triggered Raft election flapping (Issue 11.16). With two bridges consuming CPU on every node, heartbeat processing slowed enough to trigger election timeouts, causing the leader to cycle between nodes in a continuous loop. The cluster never stabilized long enough to serve client traffic reliably. With QUIC, the same full mesh topology achieves 8,518 insert ops/s — indistinguishable from partial mesh at 8,487.

The uniformity is the real achievement. In a distributed database, unpredictable per-node performance makes capacity planning impossible. If node 2 serves 1,414 ops/s while node 1 serves 9,084, the cluster's effective throughput is constrained by the slowest node that clients happen to connect to. With QUIC, every node in the cluster delivers the same throughput regardless of its position in the topology. A load balancer that distributes connections randomly will achieve aggregate throughput proportional to node count, which is the baseline expectation for horizontal scaling.

## 7.8 What Went Wrong: The Fire-and-Forget Bug

Issue 11.7. When starting a node with multiple `--peers`, connections failed with "Invalid packet type: 0" errors. Nodes configured with a single peer worked fine.

The original transport code used `tokio::spawn()` to fire-and-forget transport operations. When the node needed to connect to peers and begin sending messages, each operation was spawned as an independent task:

```rust
// Before: fire-and-forget
tokio::spawn(async move {
    transport.send(peer_id, message).await;
});
```

When establishing connections to multiple peers concurrently, these spawned tasks overwhelmed the underlying MQTT client. Multiple tasks wrote to the same TCP connection without synchronization, corrupting packet boundaries. The receiving end tried to parse a partial packet, read a zero byte where it expected a valid MQTT packet type, and disconnected with "Invalid packet type: 0".

The bug only manifested with multiple peers because a single peer connection serialized naturally — there was only one task writing to the connection at any given time. Two or more concurrent `tokio::spawn()` calls raced on the shared MQTT client's write path.

The fix was a full async refactor using Return Position Impl Trait in Trait (RPITIT). All transport operations — `send`, `broadcast`, `connect_to_peer` — became proper async methods that callers `await` directly:

```rust
// After: caller awaits directly
transport.send(peer_id, message).await?;
```

No more background spawning for send operations. The caller controls the execution, and sequential sends to different peers are naturally serialized. The RPITIT approach means these async methods return `impl Future` without heap allocation — no `Box<dyn Future>`, no trait object overhead. The ergonomics improved alongside the correctness fix.

The lesson generalizes beyond MQTT. Fire-and-forget `spawn()` for I/O operations on shared resources (connections, file handles, channels) creates implicit concurrency that the resource may not tolerate. An MQTT client, like a TCP connection, expects sequential writes. Spawning concurrent tasks that write to the same client violates that expectation. The RPITIT refactor made the sequencing explicit by keeping send operations in the caller's async context instead of detaching them into the runtime's task pool.

This bug also motivated the per-peer `tokio::sync::Mutex<SendStream>` in the QUIC transport design. Even though callers now `await` send operations sequentially, the mutex provides defense in depth — if two different parts of the system happen to send to the same peer concurrently (for example, a heartbeat broadcast overlapping with a directed Raft response), the mutex serializes the writes at the stream level. The cost is negligible: the mutex is uncontended in the common case, and the protection against stream corruption is worth the occasional nanosecond of synchronization overhead.

## Lessons

Three lessons emerge from this chapter.

**Measure before optimizing.** The lock contention hypothesis was plausible and would have led to significant refactoring — splitting the RwLock, introducing read-write lock granularity, or restructuring the data access patterns. Profiling data disproved it in one table. Sub-microsecond lock wait times on all nodes made the hypothesis untenable. Without profiling, weeks of optimization effort would have targeted the wrong bottleneck.

**Abstractions enable experiments.** The `ClusterTransport` trait was defined before the QUIC implementation existed. This meant the MQTT transport and the QUIC transport could be developed and benchmarked independently, with zero changes to the ~20 call sites that use the transport. Defining the interface first, even when only one implementation exists, creates the seam that makes the swap possible.

**Reuse is not always cheaper.** The MQTT bridge approach reused the existing broker, saving implementation effort. But the coupling between cluster traffic and client traffic created an architectural constraint that no amount of optimization could resolve without fundamentally changing the approach. The direct QUIC transport required more implementation work (TLS configuration, connection management, length-prefixed framing, receiver tasks) but eliminated the root cause. Sometimes the correct response to a performance problem is not to optimize the existing path but to bypass it entirely.

## What Comes Next

The cluster now has a transport layer that delivers messages between nodes with uniform performance regardless of topology. Chapter 8 puts this to work: when a client publishes a message on node 1 and the subscriber is connected to node 3, the publish must be routed across the cluster. The TopicIndex and ClientLocationStore broadcast entities that make cross-node pub/sub possible are the subject of Chapter 8.
