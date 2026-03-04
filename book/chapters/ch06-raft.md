# Chapter 6: Consensus with Raft

Chapter 5 introduced the two-tier replication model: Raft for the control plane, async replication for the data plane. The data plane handles the constant stream of writes — sessions, subscriptions, database records — and tolerates brief inconsistency between primary and replica. The control plane manages the partition map: which node is primary for which partition, which nodes hold replicas, and what happens when nodes join or leave. The control plane uses Raft because the partition map must be globally consistent. Every node must agree on who owns what. A disagreement about partition ownership means two nodes might both accept writes for the same partition, creating divergent state that cannot be reconciled automatically.

This chapter covers MQDB's Raft implementation: the state machine, leader election, log replication, single-node bootstrap, and the bugs that surfaced when the theoretical protocol met a real system.

## 6.1 What Raft Manages (and What It Doesn't)

The scope boundary is precise. Raft manages the **partition map** (256 partition assignments), **cluster membership** (which nodes are in the cluster), and **leader election** (which node coordinates proposals). It does not manage individual data writes, heartbeat protocol timing, or replication sequencing.

Four command types flow through the Raft log:

```rust
pub enum RaftCommand {
    UpdatePartition(PartitionUpdate),
    AddNode { node_id: u16 },
    RemoveNode { node_id: u16 },
    Noop,
}
```

`UpdatePartition` carries the primary, two replica slots, and an epoch for a single partition. `AddNode` and `RemoveNode` modify the cluster membership list. `Noop` serves a specific purpose: when a new leader is elected, it proposes a `Noop` entry to establish its term in the log and advance the commit index. Without it, the leader cannot determine which of the previous leader's entries have been committed.

The contrast with systems like etcd or consul is stark. Those systems route all writes through Raft — every key-value put, every lock acquisition, every lease renewal passes through consensus. The throughput ceiling is whatever the Raft leader can coordinate across a majority of nodes, typically tens of thousands of operations per second.

MQDB routes only partition map changes through Raft. In a stable cluster, these happen rarely: a few hundred proposals during bootstrap, a batch during rebalancing, nothing during normal operation. The constant stream of database writes and MQTT state changes bypasses Raft entirely, flowing through the async replication pipeline from Chapter 5. This separation is why MQDB can serve tens of thousands of MQTT publishes per second across nodes while maintaining a consistent partition map — the hot path never touches consensus.

## 6.2 The State Machine

The Raft state lives in `RaftState`, a struct with 13 fields:

```rust
pub struct RaftState {
    node_id: NodeId,
    current_term: u64,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry>,
    log_base_index: u64,
    commit_index: u64,
    last_applied: u64,
    role: RaftRole,
    leader_id: Option<NodeId>,
    next_index: Vec<(NodeId, u64)>,
    match_index: Vec<(NodeId, u64)>,
    votes_received: Vec<NodeId>,
    peers: Vec<NodeId>,
}
```

**`current_term`** and **`voted_for`** are the only two values that must survive a restart (along with the log). They are persisted to disk as a 10-byte record — 8 bytes for the term, 2 bytes for the voted-for node ID — stored at key `_raft/state`. Raft has its own dedicated Fjall LSM-tree instance, separate from the data stores. The node's database directory contains two subdirectories: `raft/` for Raft persistence and `stores/` for everything else (database records, sessions, indexes). The Raft instance is always configured with immediate durability — every write is fsynced — because losing a term or vote after a crash can violate Raft's safety guarantees. The data store's durability is configurable.

**`log`** is the in-memory Raft log. Each `LogEntry` carries an index (u64), a term (u64), a 2-byte length prefix, and the command bytes. A `Noop` entry is 19 bytes on the wire. A `PartitionUpdate` entry is 29 bytes — 18 bytes of log header plus 11 bytes of partition data (1 byte for the partition number, 2 bytes each for primary and two replica slots, 4 bytes for the epoch).

**`log_base_index`** enables log compaction. After old entries are trimmed from the front of the log (Section 6.8), `log_base_index` records where the trimmed log starts. A `log_position()` function translates absolute log indices to positions in the in-memory vector by subtracting the base offset. An index that has been compacted away returns `None`.

### The Command Pattern

`RaftNode` — the wrapper around `RaftState` — never sends network messages directly. Every method that might need to communicate returns a `Vec<RaftOutput>`:

```rust
pub enum RaftOutput {
    SendRequestVote { to: NodeId, request: RequestVoteRequest },
    SendAppendEntries { to: NodeId, request: AppendEntriesRequest },
    ApplyCommand(RaftCommand),
    BecameLeader,
    BecameFollower { leader: Option<NodeId> },
}
```

The Raft coordinator collects these outputs and executes them asynchronously — sending messages over the cluster transport, applying commands to the partition map, broadcasting partition updates to the cluster. This separation keeps the Raft state machine pure: no I/O, no async, no transport dependencies. The state machine can be tested with in-memory nodes that exchange outputs directly, without any networking.

## 6.3 Leader Election

Raft elections are tick-driven, not interrupt-driven. The Raft task calls `tick()` every 200 milliseconds, passing the current wall-clock time. The tick function checks whether enough time has elapsed to trigger an election or send heartbeats.

Three timing constants control the protocol:

| Constant             | Value       | Purpose                                                                        |
| -------------------- | ----------- | ------------------------------------------------------------------------------ |
| Election timeout     | 3000-5000ms | How long a follower waits before starting an election (randomized per attempt) |
| Heartbeat interval   | 500ms       | How often the leader sends `AppendEntries` to peers                            |
| Startup grace period | 10000ms     | How long a single node waits before self-electing                              |

The election timeout is randomized on each attempt: a linear congruential generator seeded from the node ID produces a value between 3000 and 5000 milliseconds. The wide range (2 seconds) and long minimum (3 seconds) reflect MQDB's operating environment. Unlike academic Raft implementations that use 150-300ms timeouts, MQDB runs over real networks with QUIC transport. A 3-second minimum means a brief network hiccup does not trigger an unnecessary election.

### Election Flow

When a follower's election timeout expires:

1. The node calls `become_candidate()`: increments its term, votes for itself, clears any previous votes.
2. It checks `has_quorum()`. With zero peers, quorum is 1 and the self-vote is sufficient — the node becomes leader immediately.
3. With peers, it sends `RequestVote` messages containing its term, node ID, and the index and term of its last log entry.

Vote granting follows the Raft safety rules. A node can grant a vote if three conditions are met. The first check rejects candidates from old terms. The second check enforces the "one vote per term" rule — a node can only vote for one candidate per term, but can re-vote for the same candidate (idempotent). The third check is the **log recentness** requirement: the candidate's log must be at least as up-to-date as the voter's. This prevents a node with a stale log from being elected leader and overwriting entries that other nodes have already committed. "Up-to-date" means the candidate's last entry has a higher term, or the same term with an equal or higher index.

When a candidate receives votes from a quorum, it calls `become_leader()`, proposes a `Noop` entry, and starts sending heartbeats to all peers.

### Step-Down on Higher Term

Any message — `RequestVote`, `AppendEntries`, or their responses — that carries a term higher than the node's current term forces the node to step down to follower. This is the universal consistency rule in Raft: a higher term always wins. If a leader discovers that another node has a higher term, it steps down immediately, even if it was actively coordinating proposals.

### The Startup Grace Period

A single-node cluster has no peers. Without the grace period, it would self-elect immediately on the first tick — before any other nodes have had a chance to connect. The 10-second startup grace period delays the first election, giving time for peers to join and register.

With peers, elections can start immediately — the randomized timeout provides sufficient coordination. Without peers, the node waits 10 seconds. This covers the common deployment sequence: start node 1, start node 2, start node 3. Node 1 waits for peers to appear. When node 2 connects and registers as a peer, node 1 can start an election immediately without waiting for the full grace period.

## 6.4 Log Replication

The leader sends `AppendEntries` to every peer on each heartbeat (every 500ms). For each peer, the leader looks up `next_index[peer]` to determine which entries to include. It also sends `prev_log_index` and `prev_log_term` — the index and term of the entry immediately before the new ones — so the follower can verify log consistency. When there are no new entries, the message is empty and serves as a pure heartbeat. The follower still processes it — resetting its election timeout and updating its commit index.

### Follower Processing

When a follower receives `AppendEntries`:

1. **Term check.** If the request's term is less than the follower's current term, reject.
2. **Step-down.** If the request's term is higher, or if the follower is not already a follower, transition to follower for this term and leader.
3. **Reset election timeout.** The leader is alive.
4. **Persist entries to disk** via `persist_log_entries()` — a batch write.
5. **Consistency check.** The request carries `prev_log_index` and `prev_log_term`. The follower verifies that its log entry at `prev_log_index` has the matching term. If not, the follower's log has diverged from the leader's, and the entries are rejected.
6. **Append entries.** The follower appends entries to its log. If an entry at a given index already exists with a different term, the follower truncates its log from that point and appends the leader's entries.
7. **Update commit index.** The follower advances its commit index to `min(leader_commit, last_log_index)`.
8. **Apply committed entries.** Any entries between `last_applied` and `commit_index` are applied.

The response carries the follower's term and, on success, its `match_index` — the highest log entry it now holds. The leader uses this to update its per-peer tracking.

### Commit Advancement

The leader does not commit an entry when it first proposes it. It commits only after a quorum of nodes has acknowledged it. The commit advancement function scans from `commit_index + 1` to the last log entry, counting how many peers have a `match_index` at or above each entry (plus one for the leader itself). If the count meets quorum, the entry is committed.

One subtlety matters here: the leader only commits entries from its own term. Entries from previous terms are committed indirectly — when a current-term entry after them is committed, they come along for the ride. This is a critical Raft safety property. Without it, a leader could commit an entry from a previous term that was only replicated to a minority before the old leader failed, violating the consensus guarantee. This is why `Noop` exists: the new leader proposes it immediately after election, creating a current-term entry that, once committed, also commits all prior entries.

### Follower Response Handling

When the leader receives an `AppendEntriesResponse`:

- **Success:** advance `next_index` and `match_index` for that peer, then call `try_advance_commit_index` to see if any new entries have reached quorum.
- **Failure with match_index > 0:** the follower has some data but the consistency check failed at the requested position. Update `next_index` to `match_index + 1` and retry with earlier entries on the next heartbeat.
- **Failure with match_index = 0:** the follower's log is empty or completely diverged. Reset `next_index` to 1 and send the entire log on the next heartbeat.

This backoff mechanism means a new node that joins with an empty log will receive the entire log history on its first successful `AppendEntries` exchange. In MQDB, that log contains partition assignments — within a few heartbeat rounds, the new node knows the complete partition map.

## 6.5 Single-Node Bootstrap

A cluster of one faces a bootstrapping problem: no partition assignments exist yet, and the rebalancer needs a leader to propose them through Raft, but there is no leader because no election has happened.

The bootstrap sequence, driven by `raft_task.rs`:

1. The single node starts. It has no peers. The startup grace period (10 seconds) prevents immediate self-election.
2. After 10 seconds with no peers appearing, `can_start_election` returns true. The node becomes a candidate with quorum size 1. Its self-vote satisfies quorum immediately. It becomes leader.
3. The Raft task detects that it is leader and `partitions_initialized` is false. It calls `initialize_partitions()`.
4. The function sorts the cluster members (just one node), then iterates all 256 partitions. For each partition, it computes the primary as `partition_num % node_count` and the replica as `(partition_num + 1) % node_count`. With one node, every partition gets the same primary (node 1) and no replicas (there is only one node, and primary and replica would be the same).
5. Each assignment is proposed as a `RaftCommand::UpdatePartition` through Raft. Every 8 proposals, the function calls `tick()` and yields to the async runtime. The yielding prevents 256 proposals from monopolizing the event loop. Each interleaved `tick()` advances commit indices and applies committed entries, so partitions become available incrementally rather than all at once after a long pause.

6. After all 256 proposals, the task sets `pending_partition_proposals` to 256 and marks `partitions_initialized = true`.

When a second node joins later, `handle_node_alive()` adds it as both a cluster member and a Raft peer, then triggers rebalancing. The rebalancer computes incremental reassignments — moving roughly half the partitions to the new node and establishing replication.

## 6.6 What Went Wrong: The Batch Flush Bug

When a follower node joined an existing cluster, the Raft leader sent it all current log entries — including 256 `UpdatePartition` entries from the initial bootstrap. The follower had to persist these entries before acknowledging them, because Raft requires durable storage of log entries before responding to `AppendEntries`.

The original implementation persisted entries one at a time. Each call to `append_log_entry` inserted a key-value pair into the storage backend and called `flush()`:

```rust
pub fn append_log_entry(&self, entry: &LogEntry) -> Result<()> {
    let key = log_entry_key(entry.index);
    let bytes = entry.to_be_bytes();
    self.backend.insert(&key, &bytes)?;
    self.backend.flush()
}
```

256 entries meant 256 `insert` calls and 256 `flush` calls. Each `flush` is a disk sync — it forces the storage engine to write all pending data to the underlying device and wait for confirmation that the data has reached persistent storage. 256 sequential syncs took over 20 seconds.

During those 20+ seconds, the follower's event loop was blocked. It could not process heartbeats from any node. Other nodes' heartbeat timeouts expired — the follower appeared dead. The heartbeat manager declared the node dead, which triggered Raft rebalancing to move partitions away from the "dead" node. But the "dead" node was still processing the very partition assignments that would have made it alive.

The fix was the batch persistence function:

```rust
pub fn append_log_entries_batch(&self, entries: &[LogEntry]) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }
    let mut batch = self.backend.batch();
    for entry in entries {
        let key = log_entry_key(entry.index);
        let bytes = entry.to_be_bytes();
        batch.insert(key, bytes);
    }
    batch.commit()?;
    self.backend.flush()
}
```

All entries go into a single batch. One `commit`, one `flush`. Processing time dropped from 20+ seconds to under a millisecond. The follower now receives 256 entries, persists them in one disk operation, acknowledges, and continues processing heartbeats normally.

The follower's `handle_append_entries` calls the batch version before updating the in-memory state machine — disk first, state machine second. If the batch write fails, the entries are not added to the in-memory log, maintaining consistency.

The Raft algorithm was correct. The replication protocol was correct. The heartbeat timing was generous at 3-5 seconds. But 256 sequential disk syncs turned a sub-millisecond operation into one that exceeded every timeout in the system. The number 256 — the fixed partition count from Chapter 4 — was also the multiplier that turned a single slow I/O call into a catastrophic cascade.

## 6.7 What Went Wrong: The Missing Peer Registration Bug

When a new node joined the cluster, it was added to the `cluster_members` list but not registered as a Raft peer. The cluster appeared functional to existing nodes, but the new node never received partition assignments.

The root cause was two separate membership concepts that had to stay synchronized. The `cluster_members` list tracks which nodes exist in the cluster — used by the rebalancer and heartbeat manager. The `peers` list in `RaftState` tracks which nodes the Raft leader should send `AppendEntries` to. Adding a node to the first without the second meant the rebalancer knew the node existed and proposed partition assignments for it, but the leader never sent those proposals to the node. The proposals were committed (the existing nodes formed a quorum), the partition map was updated on existing nodes, but the new node's partition map remained empty.

From the new node's perspective, it had joined the cluster, was receiving heartbeats, but had no partition assignments and could not serve any requests. From existing nodes' perspective, the partition map said the new node was primary for some partitions, but routing writes to it would fail because the new node did not know it was primary.

The fix was a single addition in `handle_node_alive`:

```rust
let is_new_member = !self.cluster_members.contains(&node);
if is_new_member {
    self.cluster_members.push(node);
    self.node.add_peer(node);
}
```

The `add_peer` call registers the new node in `RaftState`'s peer list. If the current node is the leader, it initializes `next_index` to `last_log_index + 1` and `match_index` to 0 for the new peer. This means the leader starts by trying to send the newest entries. If the peer's log is empty, the consistency check will fail, and the leader will back off `next_index` one entry at a time until it finds a common point — or reaches 1 and sends the entire log. A new node with an empty log receives the full log history within a few heartbeat rounds.

Two membership lists. One function call to keep them in sync. That was the entire fix. The bug was invisible in single-node testing (no peers to register), and in the two-node case it was masked because the initial node was the leader and had been registered during bootstrap. Only in the three-node case — where node 3 joins after nodes 1 and 2 have already established a cluster — did the missing peer registration manifest, because node 3 was never the leader and had never been through the bootstrap path.

## 6.8 Log Compaction

Without compaction, the Raft log grows without bound. Every partition assignment, every cluster membership change, every `Noop` from a leader election remains in memory forever. In a long-running cluster with occasional rebalancing events, the log stays small. But during bootstrap (256 entries) or a major rebalancing event, hundreds of entries accumulate that serve no purpose once applied.

After every `apply_committed()`, the node calls `compact_log` with a retention count of 1,000. The function computes a safe index — `min(last_applied, commit_index)` — ensuring that no entry is removed before it has been both committed and applied. It drains entries before the safe point (minus the retention buffer) from the front of the log and updates `log_base_index` to maintain the absolute-to-relative index translation.

MQDB does not use Raft snapshots. The partition map is always fully reconstructable from the current in-memory state — there is no need to serialize it to a separate snapshot file and coordinate snapshot transfers between nodes. A new node that joins the cluster receives the full log from the leader (or enough of it to reconstruct the current state), and the partition map is rebuilt by applying each `UpdatePartition` command in sequence. This works because the partition map is small (256 entries) and the command set is simple (overwrite an assignment). A system where the Raft log carried thousands of complex commands might need snapshots to avoid replaying a long history. MQDB compacts the log to at most 1,000 entries, and entries are only created by partition map changes and membership operations — never by data writes.

## 6.9 Persistence

Three things are persisted in the dedicated Raft Fjall instance:

**Term and vote** at key `_raft/state`. A 10-byte record: 8 bytes for `current_term` (u64, big-endian) and 2 bytes for `voted_for` (u16, big-endian, with 0 meaning "no vote"). This is persisted on every state transition — becoming a candidate, granting a vote, stepping down to follower. It must survive restarts because a node that votes for candidate A, crashes, and restarts must not vote for candidate B in the same term.

**Log entries** at keys `_raft/log/{index}`. Each key is the log prefix followed by the 8-byte big-endian index, which ensures lexicographic ordering matches index ordering. The value is the `BeBytes`-serialized `LogEntry`. On startup, a prefix scan of `_raft/log/` loads and sorts the entries by index to reconstruct the log.

The Raft store is tiny — roughly 260 keys at peak (1 state key plus ~256 log entries before compaction trims them). Fjall pre-allocates a 64 MB journal file on creation, which looks alarming on disk, but it is a sparse file: actual disk usage is under 100 KB. The in-memory compaction from Section 6.8 trims the `Vec<LogEntry>` in memory but does not delete the corresponding keys from Fjall. In practice this does not matter — the data volume is so small that the Fjall memtable never fills up, no SSTable flushes are triggered, and disk usage stays negligible. If a cluster ran long enough with enough rebalancing events to accumulate thousands of persisted log entries, Fjall's own leveled compaction would handle the LSM-tree maintenance automatically.

**Recovery** loads the persisted state and log, then reconstructs the `RaftState`. The node starts as a follower with no known leader and no peers. `commit_index` and `last_applied` start at 0, which means the node will re-apply all log entries as they are committed after it reconnects to the cluster. Since applying the same partition assignment twice has no effect, this replay is safe.

The peers list is empty because peer discovery happens through heartbeats, not through persisted state. When the node starts and begins receiving heartbeats from other nodes, `handle_node_alive` adds them as peers. This avoids persisting cluster topology information that might be stale after a long outage — the node learns the current topology from the running cluster rather than from its own potentially outdated records.

## What Comes Next

The Raft messages described in this chapter — `RequestVote`, `AppendEntries`, and their responses — must travel between nodes somehow. So must the replication writes from Chapter 5, the heartbeats, the forwarded publishes, and every other cluster message. Chapter 7 covers the transport layer: how MQDB started with MQTT bridges, discovered they degraded performance 8-30x on follower nodes, and replaced them with direct QUIC connections.
