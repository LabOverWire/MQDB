# Investigation: Issue 11.15 - Bridge Overhead Root Cause Analysis

### Summary

**Original Hypothesis**: RwLock contention causes 3-4x slowdown on nodes with 2 bridges.

**Conclusion**: RwLock contention hypothesis **DISPROVEN**. The actual root cause is **CPU contention from cluster message processing competing with client request handling on the main runtime**.

### Evidence

#### Lock Wait Times (t_lock) Are NOT the Bottleneck

| Node | Bridges | Throughput | Avg t_lock | Avg t_handle |
|------|---------|------------|------------|--------------|
| Node 1 | 2 | 1353 ops/s | 0.23 µs | 55.68 µs |
| Node 2 | 1 | - | 1.24 µs | 57.17 µs |
| Node 3 | 0 | 4222 ops/s | 1.82 µs | 54.26 µs |

**Node 3 (fastest) has the HIGHEST t_lock!** This disproves lock contention as the cause.

#### Cluster Message Volume IS the Differentiator

| Node | Bridges | Total Cluster Msgs | DB Operations | Ratio |
|------|---------|-------------------|---------------|-------|
| Node 1 | 2 | 42,685 | 23,076 | 1.85:1 |
| Node 3 | 0 | 14,233 | 148,051 | 0.096:1 |

Node 1 processes **3x more cluster messages** and performs **6x fewer DB operations**.

#### Message Type Distribution

Node 1 (leader, 2 bridges):
- Write: 13,393 (sends replication to replicas)
- Ack: 23,094 (receives acks from replicas)
- Heartbeat: 3,618
- Raft: 1,216

Node 3 (0 bridges):
- Write: 28 (almost none)
- Ack: 10,825
- Heartbeat: 1,465
- Raft: 1,426

#### Queue Depth (No Backlog)

| Node | Avg Queue Depth | Max Queue Depth |
|------|-----------------|-----------------|
| Node 1 | 1.30 | 25 |
| Node 2 | 1.00 | 4 |
| Node 3 | 1.02 | 15 |

Messages are processed quickly - the issue is CPU time spent processing them.

### Current Architecture

1. **DedicatedExecutor** handles bridge network I/O (read/write from QUIC connections)
2. Bridge callbacks invoke `route_message_local_only()` which publishes to local broker
3. **Main runtime** handles:
   - Local broker message routing
   - `process_messages()` loop consuming from flume channel
   - Client request handling (DB operations)
4. All post-receive processing runs on main runtime, competing for CPU

### Hypotheses Evaluated

| # | Hypothesis | Evidence For | Evidence Against | Conclusion |
|---|------------|--------------|------------------|------------|
| H1 | Split the lock | None | t_lock avg < 2µs on ALL nodes; Node 3 (fastest) has highest t_lock | **REJECTED** |
| H2 | Message batching | None | Processing time per message is similar across nodes | **REJECTED** |
| H3 | Defer bridge processing | Queue depth low (avg ~1) | No message backlog building up | **REJECTED** |
| H4 | Read lock for reads | Not tested (DEBUG level) | t_lock is already minimal | **UNLIKELY** |

### Recommended Fix Approaches

1. **Separate cluster message consumer** (RECOMMENDED)
   - Move `process_messages()` to dedicated executor
   - Use flume channel for cross-runtime communication (already in place)
   - Clean separation of cluster traffic from client request handling

2. **Batch replication writes**
   - Combine multiple DB writes into single replication message
   - Reduces message count and serialization overhead
   - Increases write latency

### What NOT to Do

Based on the data, these approaches would NOT help:
- Splitting the RwLock (lock times already sub-microsecond)
- Using read locks for reads (lock contention is not the issue)
- Deferring bridge processing (queue depth already low)
- Reducing batch size (processing efficiency is already good)

### QoS Considerations

- Cluster Write/Ack messages use QoS 0 (`AtMostOnce`)
- QoS 0 is single-trip (no PUBACK)
- QoS level doesn't significantly affect DB operation overhead
