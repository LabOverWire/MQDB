# Writing Diary

Tracking the incremental writing of *Building a Distributed Reactive Database*.

## Book Structure

| Part | Chapters | Pages (est.) | Status |
|------|----------|-------------|--------|
| I: The Design Thesis | 1-3 | ~70 | Complete |
| II: Distributing the System | 4-9 | ~150 | Complete |
| III: Cluster Lifecycle | 10-14 | ~100 | In progress |
| IV: Advanced Patterns | 15-18 | ~80 | Not started |
| V: Operating and Extending | 19-20 | ~50 | Not started |
| Appendices | A-D | ~30 | Not started |

## Chapter Progress

| Ch | Title | Status | Draft Date | Revision Date | Word Count |
|----|-------|--------|------------|---------------|------------|
| 1 | Why Unify Messaging and Storage? | First draft | 2026-02-16 | | 3,522 |
| 2 | The Storage Foundation | First draft | 2026-02-18 | 2026-02-19 | 5,806 |
| 3 | MQTT 5.0 as a Database Protocol | First draft | 2026-02-19 | | 5,379 |
| 4 | Partitioning | First draft | 2026-02-20 | 2026-02-21 | ~4,200 |
| 5 | Replication | First draft | 2026-02-21 | | 3,899 |
| 6 | Consensus with Raft | First draft | 2026-02-23 | | 4,213 |
| 7 | Transport Layer Evolution | First draft | 2026-02-25 | | 4,474 |
| 8 | Cross-Node Pub/Sub Routing | First draft | 2026-03-01 | | 4,351 |
| 9 | Query Coordination | First draft | 2026-03-01 | | 3,369 |
| 10 | Failure Detection and Recovery | First draft | 2026-03-01 | | 3,568 |
| 11 | Rebalancing | First draft | 2026-03-03 | | 5,048 |
| 12 | Session Management | Not started | | | |
| 13 | The Message Processor Pipeline | Not started | | | |
| 14 | The Wire Protocol | Not started | | | |
| 15 | Constraints in a Distributed System | Not started | | | |
| 16 | Consumer Groups and Event Routing | Not started | | | |
| 17 | Performance Analysis and Benchmarking | Not started | | | |
| 18 | Access Control, Ownership, and Scopes | Not started | | | |
| 19 | Vault Encryption and Data Protection | First draft (placeholder) | 2026-03-03 | 2026-03-03 | 4,068 |
| 20 | Operating MQDB | Not started | | | |
| 21 | The WASM Frontier | Not started | | | |
| Preface | Preface | First draft | 2026-03-03 | | 1,055 |
| A | Wire Protocol Reference | Not started | | | |
| B | Entity Type Reference | Not started | | | |
| C | Configuration Reference | Not started | | | |
| D | The Bug Diary | Not started | | | |

## Writing Sessions

### Session 1 — 2026-02-16

**Work done:**
- Created `book/OUTLINE.md` with full 20-chapter outline across 5 parts
- Defined target audience (system designers), positioning (implementation counterpart to DDIA), and estimated length (~400-500 pages)
- Established chapter structure template: Problem → Design Options → Implementation → What Went Wrong → Lessons Learned
- Identified Chapter 7 (Transport Layer Evolution) as strongest sample chapter candidate

**Decisions made:**
- O'Reilly proposal format
- System designers as primary audience (not practitioners or IoT devs)
- Code excerpts in Rust but explained at concept level for polyglot readers

**Open questions:**
- Final title selection (3 candidates in outline)
- How to handle code listings — inline snippets vs. separate downloadable repo
- Diagram strategy — ASCII art in manuscript, professional diagrams for production?

### Session 2 — 2026-02-16

**Work done:**
- Wrote Chapter 1 first draft (`ch01-why-unify.md`, 3,522 words)
- Sections: Two-System Problem, Core Insight, MQTT as Protocol, What We're Building, Tradeoffs
- Grounded in README.md, DISTRIBUTED_DESIGN.md (A1-A2), actual ReplicationWrite struct from protocol/replication.rs, key encoding from keys.rs

**Key user feedback (CRITICAL — applies to all future chapters):**
- The book describes what is in the code and how we got there
- Check docs often and refer to them — every claim must be traceable to code or docs
- Refer back to git commits to verify any missing gaps in the narrative
- This is not a theoretical book — it is a chronicle of building a real system

**Notes on Chapter 1:**
- 3,522 words, below the 5,000-6,000 target — acceptable for an introductory thesis chapter
- Used actual `ReplicationWrite` struct from `src/cluster/protocol/replication.rs`
- Used actual key encoding prefixes from `src/keys.rs`
- Referenced actual `$DB/` topic patterns from README.md
- Did NOT include commit history narrative — Ch 1 sets up the thesis, not the journey
- The "how we got there" narrative starts in earnest from Chapter 4 onward

### Session 3 — 2026-02-17

**Work done (code, not prose):**
- Implemented foreign key constraint enforcement across the cluster (Steps 1-8 of FK plan)
- New files: `src/cluster/node_controller/fk.rs`, `src/cluster/protocol/fk.rs`
- FK existence checks on create/update, reverse lookups on delete
- Cascade (delete children), SetNull (null FK field), Restrict (block delete) — all wired through both DB paths
- Lock-drop/reacquire pattern for async FK checks (same as unique constraints)
- 8 integration tests covering all FK operations
- Renamed Chapter 15 from "Unique Constraints" to "Constraints in a Distributed System" to cover both unique and FK
- Updated source material mapping for Ch 15 with FK source files

**Implications for book:**
- Chapter 15 now covers two constraint protocols: unique (2-phase reserve/commit) and FK (1-phase existence check + scatter-gather reverse lookup)
- FK consistency model is eventual — TLA+ proved phantom reads possible during lock-drop gap
- This is a rich "What Went Wrong" section: the tradeoff between correctness and deadlock-freedom

### Session 4 — 2026-02-17

**Work done (code, not prose):**
- Fixed two bugs found during live cluster FK testing:
  1. **Constraint key format mismatch** — `apply_replicated` stored constraints with key `_db_constraint/{entity}/{name}` but `exists()`/`remove()`/`get()` looked up using `{entity}:{name}`. Constraint removal was broken in cluster mode. Fixed by normalizing keys in `apply_replicated` to match the in-memory format.
  2. **Cross-partition cascade/set-null routing** — `apply_fk_side_effects` called `db_delete()` which only works for local partitions. Entities on remote partitions silently failed (NotFound ignored). Added `fire_and_forget_json_request()` and `fire_and_forget_set_null()` that route via `ClusterMessage::JsonDbRequest` to the correct partition primary.
- Added `replication_id_to_key()` conversion function for backward-compatible key parsing
- Added response_topic guards to skip sending responses for fire-and-forget cascade operations
- Added `apply_replicated_uses_normalized_key` unit test
- Full live cluster FK test suite passing across 3 nodes: RESTRICT, CASCADE, SET NULL, multilevel cascade (users→posts→comments), cross-node operations

**Implications for book:**
- Chapter 15 gains a strong "What Went Wrong" anecdote: the gap between unit tests passing (local partitions only) and cluster reality (entities spread across partitions). The cascade delete appeared to work in unit tests because all entities were local, but failed in production when the hash ring distributed entities across nodes.
- The fire-and-forget pattern for cascade side effects is architecturally interesting — the coordinating node doesn't wait for cascade confirmation, trading consistency for simplicity. Each individual delete is atomic; the chain is best-effort.
- The constraint key mismatch bug illustrates why having two key derivation functions for the same data is a code smell — the fix was to make `apply_replicated` derive keys from the constraint data itself rather than trusting the replication write id format.

### Session 5 — 2026-02-18

**Work done:**
- Wrote Chapter 2 first draft (`ch02-storage-foundation.md`, 5,163 words)
- Sections: Flat Key Space, Pluggable Backends, Database API, Schema/Constraints, Secondary Indexes, Reactive Subscriptions, Outbox Pattern, Cluster Mode Changes
- Read all 20 source files listed in the plan before writing
- Verified all 14 factual claims (constants, defaults, struct fields, algorithm details) against source code — zero inaccuracies
- Updated section 2.8 to reflect cascade outbox system (`_cascade/` prefix, `CascadeSideEffect` enum, startup recovery scanning both prefixes) — word count now 5,395

**Key decisions:**
- Expanded the prefix table from 6 to 9 entries to include `fkref/`, `_dead_letter/`, and `_crypto/` — these are real prefixes in the keyspace and omitting them would be inaccurate
- Included a "What Went Wrong" section on durability-consistency coupling in the outbox — the bug where in-memory data and Fjall-backed outbox entries had different durability fates
- Did NOT include code from `query.rs` (list/filter/sort/pagination) — these are query-layer concerns better covered in Chapter 9 (Query Coordination)
- Kept the cascade delete description focused on the agent-mode single-batch model, with a forward reference to Chapter 15 for the cluster-mode cross-partition version

**Notes on Chapter 2:**
- 5,395 words, within range of the 5,000-6,000 target
- All code snippets drawn from actual source files with exact function signatures
- The chapter follows the plan's section structure closely (2.1-2.8)
- Forward references to Chapters 5, 8, 9, and 15 are planted for continuity
- The "What Went Wrong" on outbox durability coupling is a real lesson from development — the coupling between batch atomicity and durability mode is non-obvious

### Session 6 — 2026-02-19

**Work done:**
- Wrote Chapter 3 first draft (`ch03-mqtt-protocol.md`, 5,379 words)
- Sections: MQTT 5.0 Essentials, Self-Subscribing Broker, Request/Response, Topic API, User Properties, QoS as Durability Knob, What Went Wrong
- Read all 8 source files before writing: `agent/broker.rs`, `agent/tasks.rs`, `agent/handlers.rs`, `agent/mod.rs`, `broker_defaults.rs`, `protocol/mod.rs`, `topic_rules.rs`, `topic_protection.rs`, `bin/mqdb/common.rs`, `events.rs`, `auth_config.rs`
- Verified broker config constants against `broker_defaults.rs` (3 values)
- Verified topic protection patterns against `topic_rules.rs` (10 rules, 3 tiers)
- Verified user property names against `handlers.rs` (x-mqtt-sender, x-mqtt-client-id) and `tasks.rs` (x-origin-client-id)
- Verified AdminOperation count: 25 total variants, 21 under `$DB/_admin/`, 3 under `$DB/_sub/`, 1 at `$DB/_health`
- Verified service account naming pattern: `mqdb-internal-{uuid}` from `auth_config.rs`
- Verified internal client names: `mqdb-internal-handler`, `mqdb-response-publisher`, `mqdb-event-publisher`
- Cross-referenced Ch1's `$DB/` topic table (Section 1.3) — consistent
- Cross-referenced Ch2's `Request` enum and response format — consistent

**Key decisions:**
- Three "What Went Wrong" anecdotes: feedback loop, backpressure discovery, service account bootstrapping
- Included the REST vs topic-based API design comparison (method verbs vs topic keywords) to explain disambiguation
- Included the alternative design discussion (separate `$DB_REQ/` and `$DB_EVT/` prefixes) to explain why the feedback loop was accepted
- Did NOT include full admin operation table — 21 operations are too many to enumerate individually, grouped by subnamespace instead
- Kept OpenTelemetry section brief — it is feature-gated and not core to the protocol mapping story

**Notes on Chapter 3:**
- 5,379 words, within range of the 5,000-6,000 target
- Chapter completes Part I — all three foundation chapters done
- Forward references to Chapters 4, 12, and 15 planted for continuity
- The QoS backpressure discovery is one of the strongest "What Went Wrong" anecdotes in the book — QoS 0 being slower than QoS 1 is genuinely counterintuitive
- The service account bootstrapping problem connects to the anonymous mode bug documented in CLAUDE.md's known issues

### Session 7 — 2026-02-19

**Work done:**
- Revised Chapter 2 encoding section: rewrote "The Numeric Encoding Trick" (lines 46-76) to match the new binary sign-bit-flipping encoding in `src/keys.rs`
- Old approach: zero-padded text encoding (`format!("{i:020}")`) — didn't handle negative numbers
- New approach: `encode_i64_sortable` (big-endian bytes, XOR sign bit 0x80) and `encode_f64_sortable` (IEEE 754 bits, negative floats XOR all 0xFF, positive floats XOR sign bit 0x80)
- Fixed prefix count: "Nine prefixes" → "Eight prefixes" — the `fkref/` prefix from Session 5's expansion never existed in the codebase
- Updated word count: 5,393 → 5,488

**Notes:**
- The binary encoding produces 8 fixed bytes per number vs variable-length text — more compact and `memcmp`-friendly
- All sort-order tests pass for the full range from `i64::MIN`/`f64::NEG_INFINITY` through zero to `i64::MAX`/`f64::INFINITY`

---

## Writing Process Notes

### Conventions

- **File naming:** `book/chapters/ch{NN}-{slug}.md` (e.g., `ch01-why-unify.md`)
- **Draft stages:** Not started → First draft → Revised → Final
- **Word count target:** ~5,000-6,000 words per chapter (~20-25 pages in print)

### Source Material Mapping

Each chapter draws from specific MQDB source files and documentation. This mapping helps ensure accuracy and completeness.

| Chapter | Primary Sources |
|---------|----------------|
| 1 | README.md, DISTRIBUTED_DESIGN.md (A1) |
| 2 | src/storage/, src/database/, src/schema.rs, src/index.rs, src/keys.rs, src/outbox.rs |
| 3 | src/agent/, src/transport.rs, README.md (MQTT API sections) |
| 4 | src/cluster/partition_map.rs, src/cluster/partition.rs, DISTRIBUTED_DESIGN.md (A1, Part 1, Part 5) |
| 5 | src/cluster/replication.rs, src/cluster/store_manager/, DISTRIBUTED_DESIGN.md (A4) |
| 6 | src/cluster/raft/, DISTRIBUTED_DESIGN.md (Part 8, Issues 11.2, 11.5) |
| 7 | src/cluster/quic_transport.rs, src/cluster/mqtt_transport.rs, src/cluster/transport.rs, DISTRIBUTED_DESIGN.md (A6) |
| 8 | src/cluster/topic_index.rs, src/cluster/topic_trie.rs, src/cluster/publish_router.rs, src/cluster/client_location.rs, DISTRIBUTED_DESIGN.md (Part 7) |
| 9 | src/cluster/query_coordinator.rs, src/cursor.rs, src/cluster/node_controller/retained.rs, src/cluster/retained_store.rs, src/cluster/event_handler/broker_events.rs (deliver_retained_messages), DISTRIBUTED_DESIGN.md (A5) |
| 10 | src/cluster/heartbeat.rs, src/cluster/snapshot.rs, DISTRIBUTED_DESIGN.md (Part 3, Part 9) |
| 11 | src/cluster/rebalancer.rs, src/cluster/migration.rs, DISTRIBUTED_DESIGN.md (Part 4, Issues 11.10, 11.17) |
| 12 | src/cluster/session.rs, src/cluster/inflight_store.rs, src/cluster/qos2_store.rs, DISTRIBUTED_DESIGN.md (M8-M10) |
| 13 | src/cluster/message_processor.rs, src/cluster/dedicated_executor.rs, DISTRIBUTED_DESIGN.md (A6.6, A6.7) |
| 14 | src/cluster/protocol/, DISTRIBUTED_DESIGN.md (Part 2) |
| 15 | src/cluster/node_controller/unique.rs, src/cluster/node_controller/fk.rs, src/cluster/node_controller/db_ops.rs (CascadeSideEffect), src/cluster/node_controller/pending.rs, src/cluster/db/constraint_store.rs, src/cluster/db/data_store.rs (FkReverseIndex), src/cluster/store_manager/constraint_ops.rs, src/cluster/store_manager/outbox.rs (CascadeOutboxPayload), src/cluster/protocol/fk.rs, DISTRIBUTED_DESIGN.md |
| 16 | src/consumer_group.rs, src/dispatcher.rs |
| 17 | COMPLETE_MATRIX_DOC.md, COMPLETE_MATRIX_RESULTS.md, DISTRIBUTED_DESIGN.md (A6.4, A6.5 benchmarks) |
| 18 | src/auth_config.rs, src/topic_protection.rs, src/topic_rules.rs, src/types.rs (OwnershipConfig, ScopeConfig), src/transport.rs (execute_with_sender), src/agent/broker.rs, src/agent/handlers.rs, src/bin/mqdb/commands/auth.rs, src/bin/mqdb/commands/acl.rs, src/http/oauth.rs |
| 19 | src/http/vault_crypto.rs, src/http/identity_crypto.rs, src/vault_keys.rs, src/http/handlers.rs (vault endpoints, batch ops), src/agent/handlers.rs (vault MQTT data path), src/http/session_store.rs, src/http/server.rs (vault routes), docs/OAUTH_VAULT_FUTURE_WORK.md |
| 20 | CLI_TESTING_GUIDE.md, src/bin/mqdb/ |
| 21 | src/storage/memory_backend.rs, Cargo.toml (wasm feature) |

### Learnings

*Updated as we write. Patterns, pitfalls, and insights from the writing process.*

- Chapter 1 (thesis/intro) naturally runs shorter than technical chapters — don't pad it
- Always read the actual source files before writing, not just docs — the code is ground truth
- The `ReplicationWrite` struct is the single most important concept to introduce early
- Git commit history is a primary source for "how we got there" — use `git log` to trace design evolution
- DISTRIBUTED_DESIGN.md can be outdated — always verify claims against actual code call sites, not just method definitions
- Dead code in store_manager (`subscribe_topic_replicated`, `schema_register_replicated`) was removed — these created 256-write fan-outs but were superseded by lightweight broadcast messages
- When a method exists but is never called, check git blame/log to find when the calling code changed
- Avoid tautologies — don't restate a definition as a use case (e.g., "suits scenarios where MQTT is unnecessary" for the no-MQTT mode). Use concrete examples instead.
- When writing about prefixes/namespaces, enumerate ALL real prefixes from the code, not just the "visible" ones — infrastructure prefixes like `_dead_letter/`, `_crypto/` are real parts of the keyspace
- Run a systematic verification pass after writing: list every factual claim (constant values, defaults, struct fields) and check each against source. This caught zero errors in Ch2 but the discipline prevents drift as the code evolves
- The "What Went Wrong" sections are most powerful when they describe a real bug from development — the outbox durability coupling bug came from the cluster mode transition, not from agent mode itself
- When writing about architecture with multiple interacting components (internal clients, auth providers, topic protection), the "why not the simpler approach?" question generates the most insight — explaining why separate topic prefixes were rejected is more instructive than just describing the chosen design
- Count enum variants and match arms carefully — discrepancies between the code and the prose are easy to introduce when summarizing (AdminOperation has 25 variants total, not "22" as the plan estimated)
- Always verify prefix/namespace counts against actual code constants, not session notes — Session 5 expanded to "9 prefixes" including `fkref/`, but `fkref/` was never implemented. The code has 8 prefixes.
- When multiple bugs share the same root cause pattern, combine them into a single narrative section — 11.9 and 11.14 are structurally identical (time-blind dedup) and telling them together strengthens the lesson. Telling them separately would be repetitive.
- Minimize code in the book when abstract representations (flow diagrams, field tables, trie ASCII art) can convey the same information — code is a testament to openness, not a tutorial. Use it sparingly and purposefully.
- Documentation (DISTRIBUTED_DESIGN.md) can describe the dedup cache as "LRU" when it's actually FIFO (HashSet+VecDeque). Always verify implementation details against the code, not just the docs.
- When a chapter covers multiple interacting components (three broadcast stores + router + dedup), organize by concern (subscribe flow, publish flow, protocol) rather than by file — readers follow the data flow, not the module structure.

### Session 8 — 2026-02-21

**Work done:**
- Wrote Chapter 5 first draft (`ch05-replication.md`, 3,899 words)
- Sections: Universal Write Abstraction, Two-Tier Replication Model, Durability Tradeoff, Sequence Ordering on Replicas, Catchup Mechanism, Epoch-Based Consistency, Store Abstraction Layer, What Went Wrong (catchup flooding bug)
- Read all primary source files before writing: `protocol/replication.rs` (wire format), `replication.rs` (ReplicaState, 4-case handle_write), `write_log.rs` (BoundedLog, 10K cap), `quorum.rs` (QuorumTracker, PendingWrites), `node_controller/replication_ops.rs` (replicate_write, write_or_forward_impl), `node_controller/session_ops.rs` (replicate_write_async), `node_controller/catchup.rs` (catchup request/response, snapshot trigger), `snapshot.rs` (SnapshotBuilder, SnapshotSender, 64KB chunks), `store_manager/apply.rs` (16 entity types), `entity.rs` (17 constants, 16 in dispatch), `raft/state.rs` (4 RaftCommand types), `protocol/types.rs` (Operation enum)
- Verified wire header is 22 bytes (version:1, partition:2, operation:1, epoch:4, sequence:8, entity_len:1, id_len:1, data_len:4)
- Verified two replication functions: `replicate_write` (quorum-tracked, session creation only) vs `replicate_write_async` (fire-and-forget, all other writes)
- Verified 16 entity types in dispatch (17 defined in entity.rs, but OAUTH_TOKENS is not in apply_to_memory)
- Verified AwaitingSnapshot role and its behavior (NotReplica ack, no catchup requests, snapshot request tracking)

**Key decisions:**
- 3,899 words, below 5,000-6,000 target — similar to Ch1 (3,522). Chapter is lean but complete; no padding added
- Included "What Went Wrong" section on the catchup flooding bug (AwaitingSnapshot fix) — consistent with other chapters
- Included the dual-return store pattern (returns domain object + ReplicationWrite) — shows the separation between store logic and replication routing
- Did NOT include detailed description of each entity type in the dispatch — the 16-entry match statement speaks for itself
- Did NOT pad the comparison with etcd/MySQL/Redis beyond the minimum needed to frame the tradeoff
- Forward references to Chapter 6 (Raft) at both the two-tier model section and the closing "What Comes Next"

### Session 9 — 2026-02-23

**Work done:**
- Wrote Chapter 6 first draft (`ch06-raft.md`, 4,213 words)
- Sections: What Raft Manages, State Machine, Leader Election, Log Replication, Single-Node Bootstrap, Batch Flush Bug, Missing Peer Bug, Log Compaction, Persistence
- Read all primary source files before writing: `raft/state.rs` (RaftState, RaftCommand, LogEntry, compact_log, try_advance_commit_index, quorum_size), `raft/node.rs` (RaftConfig defaults, tick, election, heartbeats, RaftOutput command pattern), `raft/storage.rs` (persistence keys, batch vs individual persist, RaftPersistentState 10 bytes), `raft/rpc.rs` (wire format for RequestVote/AppendEntries), `raft/coordinator/replication.rs` (apply_command, handle_node_alive with add_peer at line 123, handle_node_death), `raft_task.rs` (initialize_partitions with BATCH_SIZE=8, 200ms tick interval, startup grace period flow)
- Verified PartitionUpdate is 11 bytes (u8 + u16 + u16 + u16 + u32), not 13 as plan estimated
- Verified RaftPersistentState is 10 bytes (u64 + u16)
- Verified quorum_size formula: `peers.len().div_ceil(2) + 1`
- Verified startup grace period is 10000ms, election timeout 3000-5000ms, heartbeat 500ms
- Verified add_peer initializes next_index to last_log_index()+1 when called on leader
- Verified compact_log keeps 1000 entries, uses min(last_applied, commit_index) as safe index

**Key decisions:**
- 4,213 words, below 5,000-6,000 target but consistent with Ch1 (3,522) and Ch5 (3,899). Chapter covers well-understood territory (Raft) so focuses on implementation decisions and bugs, not textbook explanations.
- Two "What Went Wrong" sections: batch flush (Issue 11.5) and missing peer registration (Issue 11.2). Both have clear narrative arcs: symptom, root cause, fix, lesson.
- Included the RaftOutput command pattern as a design highlight — separating state machine from I/O is a pattern readers can apply broadly.
- Included `can_grant_vote` with full log recentness check — this is the subtlest part of the election and readers should see the real code.
- Included `try_advance_commit_index` with the current-term-only safety property — another Raft subtlety that matters in practice.
- Did NOT include detailed RPC wire format — already covered enough in the LogEntry and PartitionUpdate byte layouts.
- Did NOT include the coordinator's full process_outputs implementation — the RaftOutput enum description is sufficient.
- Forward reference to Chapter 7 (Transport) at the closing.
- Chapter 4 already introduced the batch flush bug from the partitioning angle; this chapter tells the same story from the Raft/storage angle with more implementation detail.

### Session 10 — 2026-02-25

**Work done:**
- Wrote Chapter 7 first draft (`ch07-transport.md`, 4,474 words)
- Sections: MQTT Bridges Initial Design, Bridge Topology Options, Bridge Overhead Problem, Transport Abstraction, Direct QUIC Transport, mTLS for Cluster Security, Performance Results, What Went Wrong (fire-and-forget bug), Lessons
- Read all primary source files before writing: `transport.rs` (ClusterTransport trait, ClusterMessage enum), `quic_transport.rs` (QuicDirectTransport, bind/connect/send, wire format, mTLS config), `mqtt_transport.rs` (MqttTransport, dual clients, topic subscriptions), `cluster_agent/transport.rs` (ClusterTransportKind wrapper), `cluster_agent/config.rs` (cluster_port_offset=100)
- Verified all benchmark data against `COMPLETE_MATRIX_RESULTS.md` (lines 157-248)
- Verified root cause analysis data against `COMPLETE_MATRIX_DOC.md` (lines 325-416)
- Verified bridge loop prevention against `DISTRIBUTED_DESIGN.md` (lines 477-485)
- Verified 35 ClusterMessage variants by counting `transport.rs:21-67`
- Verified constants: SEND_TIMEOUT_MS=5000, INBOX_CHANNEL_CAPACITY=16384, MAX_MESSAGE_SIZE=10MB, cluster_port_offset=100
- Verified 5 MQTT subscription topic patterns from `mqtt_transport.rs:142-221`

**Key decisions:**
- 4,474 words, below 5,000-6,000 target but consistent with Ch5 (3,899) and Ch6 (4,213). The chapter is data-driven — tables carry narrative weight that word count undercounts.
- Included full root cause analysis with profiling data — the lock contention hypothesis being disproven is the chapter's strongest narrative moment.
- Included message type distribution data (Write, Ack, Heartbeat counts) to make the CPU contention concrete.
- Included PeerConnection struct and QuicDirectTransport struct — readers need to see the data structure to understand the per-peer mutex design.
- Included a Lessons section (3 lessons: measure before optimizing, abstractions enable experiments, reuse is not always cheaper) — first chapter to have an explicit lessons section separate from "What Went Wrong".
- Did NOT include detailed `InboundMessage::parse_from_payload` implementation — the wire format diagram suffices.
- Did NOT include the `build_server_config` / `build_client_config_secure` implementations — the description of mTLS behavior is sufficient without showing the rustls API calls.
- Forward reference to Chapter 8 (Cross-Node Pub/Sub) at the closing.

### Session 11 — 2026-03-01

**Work done:**
- Wrote Chapter 8 first draft (`ch08-cross-node-pubsub.md`, 4,351 words)
- Sections: The Routing Problem, Three Broadcast Stores, The Subscribe Flow, Wildcard Routing, The Publish Routing Flow, The Forwarded Publish Protocol, What Went Wrong (3 sections: ClientLocationStore, time-blind dedup, missing broadcast), Lessons
- Read all primary source files before writing: `topic_index.rs` (TopicIndex HashMap, SubscriberLocation, TopicIndexEntry, subscribe/unsubscribe_with_data, apply_replicated), `topic_trie.rs` (TrieNode with literal/+/# edges, match_recursive algorithm, $-topic protection, WildcardSubscriber), `publish_router.rs` (PublishRouter, route_targets, route_with_wildcards, effective_qos, RoutingTarget, PublishRouteResult), `client_location.rs` (ClientLocationEntry with version 2 timestamp, ClientLocationStore HashMap<String,NodeId>), `wildcard_store.rs` (WildcardStore wrapping TopicTrie with RwLock, match_topic converting WildcardSubscriber→SubscriberLocation), `event_handler/routing.rs` (route_and_forward_publish, resolve_connected_node two-tier lookup, handle_topic_subscribe, handle_wildcard_subscribe, resp/ topic filtering), `event_handler/broker_events.rs` (on_client_subscribe, on_client_publish, on_client_connect/disconnect, internal client filtering), `protocol/broadcast.rs` (WildcardBroadcast type 60, TopicSubscriptionBroadcast type 61, both version 2 with timestamp_ms), `protocol/publish.rs` (ForwardedPublish version 2 wire format, ForwardTarget), `protocol/types.rs` (MessageType enum, ForwardedPublish=30), `transport.rs` (ClusterMessage variants for broadcast types 60/61), `message_processor.rs` (FORWARD_DEDUP_CAPACITY=1000, forward_fingerprint hashing origin+timestamp+topic+payload, HashSet+VecDeque FIFO eviction)
- Verified message type codes: ForwardedPublish=30, WildcardBroadcast=60, TopicSubscriptionBroadcast=61
- Verified both broadcast structs use version 2 with timestamp_ms field
- Verified dedup fingerprint hashes 4 fields: origin_node, timestamp_ms, topic, payload (NOT qos, retain, targets)
- Verified dedup cache is HashSet<u64> + VecDeque<u64> with FIFO eviction, capacity 1000 — not a true LRU
- Verified response topic detection: `topic.starts_with("resp/") || topic.contains("/resp/")`
- Verified internal client filter: `client_id.starts_with("mqdb-")` and forward client filter: `client_id.starts_with("mqdb-forward-")`
- Verified $-topic protection in trie: `match_topic` returns empty vec for topics starting with '$'
- Cross-referenced all four bugs (11.8, 11.9, 11.11, 11.14) against DISTRIBUTED_DESIGN.md and HISTORICAL_SESSIONS.md

**Key decisions:**
- 4,351 words, consistent with Ch5 (3,899), Ch6 (4,213), Ch7 (4,474). Chapter has substantial tables and diagrams that carry narrative weight beyond word count.
- Minimized code usage per user guidance — used flow diagrams, tables, and trie ASCII art instead of code blocks. Only one code block (the subscribe flow diagram) uses pseudo-structure rather than actual Rust.
- Three "What Went Wrong" sections: 11.8 (missing ClientLocationStore), 11.9+11.14 (combined as "time-blind dedup"), 11.11 (missing broadcast for exact topics). Combined 11.9 and 11.14 because they share the identical root cause and fix pattern.
- Included the trie data structure explanation with ASCII visualization — readers need to understand how wildcard matching avoids full table scans.
- Included the response topic (`resp/`) optimization — shows pragmatic engineering (not broadcasting ephemeral subscriptions).
- Included the subscription cost comparison table (exact vs wildcard) — quantifies the broadcast overhead difference.
- Did NOT include actual Rust code from the source files — used abstract representations (tables, diagrams, field lists) per user guidance.
- Did NOT include the WildcardEntry or TopicIndexEntry binary serialization details — covered sufficiently by the field tables.
- Did NOT include the snapshot export/import mechanisms — those belong in Chapter 10 (Failure Detection and Recovery).
- Forward reference to Chapter 9 (Query Coordination) at the closing.
- Chapter picks up directly from Chapter 7's closing paragraph about TopicIndex and ClientLocationStore.

**Verification notes:**
- The subscribe flow diagram accurately reflects the code paths in broker_events.rs and routing.rs
- The ForwardedPublish wire format table matches protocol/publish.rs:to_bytes() exactly
- The dedup description matches message_processor.rs (HashSet+VecDeque FIFO, not LRU as some docs incorrectly state)
- The three propagation mechanisms table matches DISTRIBUTED_DESIGN.md A1.3

### Session 12 — 2026-03-01

**Work done:**
- Wrote Chapter 9 first draft (`ch09-query-coordination.md`, 3,369 words)
- Sections: The Scatter-Gather Pattern (with Two Query Paths), Partition Pruning, Merge and Sort, Field Projection, Pagination, Retained Message Queries, Three Layers of Retained Dedup, What Went Wrong (naive fan-out), Lessons
- Read all primary source files before writing: `query_coordinator.rs` (QueryCoordinator with start_query, receive_response, check_timeouts, prune_partitions, build_result), `cursor.rs` (PartitionCursor with partition/sequence/last_key, ScatterCursor with Vec<PartitionCursor>, binary encoding), `protocol/query.rs` (QueryRequest type 50, QueryResponse type 51, BatchReadRequest type 52, BatchReadResponse type 53), `node_controller/query.rs` (handle_query_request, start_scatter_list_query, handle_scatter_list_response, sort_scatter_results, apply_list_projection), `node_controller/retained.rs` (start_async_retained_query, start_async_retained_wildcard_query with queried_nodes HashSet, sync_retained_to_broker with TTL cache), `db_handler/helpers.rs` (parse_projection, validate_projection_against_schema), `db_handler/json_ops.rs` (list handler with filter/projection), `database/query.rs` (project_fields, validate_list_fields, list_core with pagination), `event_handler/broker_events.rs:249` (wildcard retained query call site), `db/partition.rs` (data_partition and topic_partition hash functions), `store_manager/query.rs` (query_entity dispatching), `node_controller/mod.rs` (PendingScatterRequest struct, handle_query_response_received)
- Verified partition hash is `CRC32(entity + "/" + id) % 256`
- Verified binary vs JSON path split: QueryCoordinator for internal entities, PendingScatterRequest for JSON DB
- Verified merge pipeline order: dedup (HashSet<String> by ID) → filter → sort (multi-field, coordinator-side) → truncate → project
- Verified PartitionCursor binary encoding: 12 bytes + key length; ScatterCursor: 2 bytes (count) + sum of partition cursors
- Verified three retained dedup layers: per-node query dedup (HashSet<NodeId>), local store filtering, TTL-based write filtering (5-second cache)
- Discovered DISTRIBUTED_DESIGN.md line 339 incorrectly claims wildcard retained queries are NOT implemented — code at broker_events.rs:249 confirms they ARE implemented

**Key decisions:**
- 3,369 words, the shortest chapter so far (Ch1: 3,522, Ch5: 3,899). The chapter is table-heavy and diagram-driven; tables carry significant narrative weight that word count undercounts.
- Included the two query paths (binary vs JSON) as a subsection rather than a separate section — the split is an implementation detail, not a design decision worth its own heading.
- Included the partition pruning table showing query patterns and partition counts — makes the 256x optimization concrete.
- Included the merge pipeline in strict order (dedup → filter → sort → truncate → project) with rationale for each ordering constraint — this is the intellectual core of the chapter.
- Included pagination with both cursor levels (PartitionCursor and ScatterCursor) — keyset pagination across independent partitions is non-obvious.
- Three sections on retained message dedup (9.6, 9.7, 9.8) form a mini-narrative: the problem, the solution's three layers, and what went wrong. This mirrors the replication factor amplification lesson from Ch5.
- Did NOT include actual Rust code — used tables, flow diagrams, and field layouts per user guidance.
- Did NOT include the QueryRequest/QueryResponse wire format byte layouts — already covered enough by the message type numbers and field descriptions.
- Forward reference to Chapter 10 (Failure Detection) at the closing.

**Verification notes:**
- The partition hash formula matches `db/partition.rs` (CRC32, not xxHash or FNV)
- The merge pipeline matches `node_controller/query.rs:sort_scatter_results` and `apply_list_projection`
- The three dedup layers match `node_controller/retained.rs` (Layer 1: queried_nodes HashSet, Layer 2: local store check) and `event_handler/broker_events.rs` (Layer 3: synced_retained_topics TTL cache)
- The 170x duplication figure is referenced from DISTRIBUTED_DESIGN.md Issue 11.20

### Session 13 — 2026-03-01

**Work done:**
- Wrote Chapter 10 first draft (`ch10-failure-detection.md`, 3,568 words)
- Sections: Heartbeat Protocol, The Node State Machine, Why Bitmaps in Heartbeats, The False Death Problem, Partition Failover, Snapshot Transfer, The Recovery Timeline, What Went Wrong (initialization race), Lessons
- Read all primary source files before writing: `heartbeat.rs` (HeartbeatManager, NodeStatus enum, register_node, should_send with has_any_assignment guard, create_heartbeat, receive_heartbeat, check_timeouts with Unknown/Dead skip, handle_death_notice, update_partition_map_from_heartbeat), `protocol/heartbeat.rs` (Heartbeat struct: version u8 + node_id u16 + timestamp_ms u64 + primary_bitmap [u64;4] + replica_bitmap [u64;4], TOTAL_SIZE=75, VERSION=2, set_primary/is_primary bit operations), `transport.rs` (TransportConfig defaults: heartbeat_interval_ms=1000, heartbeat_timeout_ms=15000, ack_timeout_ms=500), `raft/coordinator/replication.rs` (handle_node_death with processed_dead_nodes dedup, reassign_partitions_for_dead_node iterating all 256 partitions, select_new_primary preferring alive replica then any alive node, handle_node_alive with pending_new_nodes queue), `snapshot.rs` (SnapshotRequest 5 bytes, SnapshotChunk 23-byte header + data with DEFAULT_CHUNK_SIZE=64KB, SnapshotComplete 12 bytes with Ok/Failed/NoData status, SnapshotBuilder with out-of-order assembly, SnapshotSender with chunked iteration), `node_controller/snapshot.rs` (handle_snapshot_request, handle_snapshot_chunk with builder assembly and store import, handle_snapshot_complete with migration phase advance, request_snapshot with dedup HashSet), `node_controller/mod.rs` (handle_node_death sending RaftEvent::NodeDead, dead_nodes_for_session_update queue, check_timeouts call in tick), `migration.rs` (MigrationPhase: Preparing→Overlapping→HandingOff→Complete, MigrationState, MigrationManager)
- Verified heartbeat wire format: 75 bytes = 1 + 2 + 8 + 32 + 32
- Verified suspect threshold is timeout/2 = 7500ms (computed inline, not configurable separately)
- Verified Unknown and Dead nodes skipped in check_timeouts (line 180)
- Verified select_new_primary: `replicas.iter().find(|r| alive_nodes.contains(r)).or_else(|| alive_nodes.first())`
- Verified SnapshotChunk header is 23 bytes: 1 + 2 + 4 + 4 + 8 + 4
- Cross-referenced Issues 11.3 (empty heartbeat) and 11.4 (false death) against DISTRIBUTED_DESIGN.md

**Key decisions:**
- 3,568 words, consistent with Ch1 (3,522), Ch5 (3,899), Ch9 (3,369). Chapter is diagram-heavy and table-driven.
- Combined Issues 11.3 and 11.4 into a single "What Went Wrong" section (initialization race) since they share the root cause: treating uninitialized state as valid state.
- Included a recovery timeline section (10.7) with concrete timestamps for both fast path (replica promotion, ~16s) and slow path (snapshot needed, ~20s). Makes the timeout cost tangible.
- Included the snapshot transfer protocol diagram and SnapshotChunk wire format table — readers need to see the chunked transfer mechanism.
- Included the bitmap-in-heartbeat rationale (10.3) as a separate section — the "why carry 64 extra bytes" question is non-obvious and the answer (closing the Raft replication window) is instructive.
- Did NOT include actual Rust code — used state machine diagrams, wire format tables, flow diagrams, and pseudocode per user guidance.
- Did NOT include the MigrationManager phases in detail — those belong in Chapter 11 (Rebalancing).
- Did NOT include the session cleanup triggered by node death — that belongs in Chapter 12 (Session Management).
- Forward reference to Chapter 11 (Rebalancing) at the closing.

**Verification notes:**
- The heartbeat wire format table matches `protocol/heartbeat.rs` exactly (TOTAL_SIZE=75)
- The state machine transitions match `check_timeouts` and `receive_heartbeat` in `heartbeat.rs`
- The timeout defaults match `TransportConfig::default()` in `transport.rs`
- The reassignment logic matches `reassign_partitions_for_dead_node` in `replication.rs`
- The snapshot transfer protocol matches `node_controller/snapshot.rs` handler methods
- The SnapshotComplete is actually 12 bytes (1+2+1+8), not 11 as noted in session summary — verified against BeBytes derive

### Session 14 — 2026-03-03

**Work done:**
- Wrote Chapter 11 first draft (`ch11-rebalancing.md`, 5,048 words)
- Sections: The Rebalancing Problem, Three Algorithms (Balanced/Incremental/Removal), Leader-Only Coordination (Three Queues, Tick Loop, Backpressure Gate, Epoch Bumps), Partition Migration (4-phase state machine, Graceful Shutdown), What Went Wrong (rebalance race condition, single-node bootstrap), Lessons
- Read all primary source files before writing: `rebalancer.rs` (compute_balanced_assignments round-robin, compute_incremental_assignments 3-phase, compute_removal_assignments with replica promotion, PartitionReassignment struct), `raft/coordinator/mod.rs` (RaftCoordinator struct with 3 pending sets + 3 processed sets + pending_partition_proposals counter, partitions_initialized), `raft/coordinator/replication.rs` (handle_node_alive with backpressure gate, handle_node_death, handle_drain_notification, trigger_rebalance_internal choosing balanced vs incremental, trigger_drain_rebalance using removal algorithm, reassign_partitions_for_dead_node per-partition loop), `raft/coordinator/election.rs` (tick() with just_became_leader path, process_pending_dead/draining/new_nodes, scan_partition_map_for_dead_primaries safety net, propose_partition_update), `migration.rs` (MigrationPhase enum: Preparing/Overlapping/HandingOff/Complete, MigrationState struct with 8 fields, MigrationManager with advance_phase rejecting invalid transitions, MigrationCheckpoint with BeBytes derive), `snapshot.rs` (SnapshotSender with DEFAULT_CHUNK_SIZE=64KB, SnapshotBuilder with indexed slots, SnapshotChunk 23-byte header), `quorum.rs` (QuorumTracker with stale_epoch_seen flag, StaleEpoch causes immediate Failed result), `replication.rs` (ReplicaRole::AwaitingSnapshot, handle_write epoch validation: stale→StaleEpoch, newer→reset sequence, AwaitingSnapshot→NotReplica), `node_controller/mod.rs` (set_draining broadcast, can_shutdown_safely 3 conditions, collect_awaiting_snapshot_requests retry logic with requested_snapshots dedup set)
- Verified round-robin: `primary_idx = partition_num % node_count`, `replica_idx = (primary_idx + r) % node_count`
- Verified incremental 3-phase: redistribute_primaries → add_missing_replicas → redistribute_replicas
- Verified backpressure: `pending_partition_proposals` set to `proposed_indices.len()`, decremented by `saturating_sub(1)` in `apply_command`
- Verified tick order: process_pending_dead_nodes → process_pending_draining_nodes → (partitions_initialized ? process_pending_new_nodes : trigger_rebalance_internal)
- Verified scan_partition_map_for_dead_primaries runs after process_pending_dead_nodes
- Verified can_shutdown_safely: `self.draining && self.pending.is_empty() && self.outgoing_snapshots.is_empty()`
- Verified AwaitingSnapshot returns NotReplica on handle_write (replication.rs:101-103)
- Verified QuorumTracker: stale_epoch_seen returns Failed immediately (quorum.rs:148-149)

**Key decisions:**
- 5,048 words, within the 5,000-6,000 target. First chapter to hit the target range squarely.
- Included a full numeric example for incremental rebalancing (2-node→3-node transition) walking through all three phases with concrete numbers. This is the chapter's most instructive content.
- Included the epoch invariant explanation (one epoch per replica at any time, monotonic transitions) connecting rebalancing to the replication protocol from Chapter 5.
- Included scan_partition_map_for_dead_primaries as a "belt-and-suspenders" detail — shows defensive engineering in the leader transition path.
- Two "What Went Wrong" sections: rebalance race condition (read-modify-write race on partition map) and single-node bootstrap (no peers means no NodeAlive trigger).
- Four lessons: serialize state-dependent computations, bootstrap is not the normal path, incremental over global, event ordering is policy.
- Did NOT include actual Rust code — used pseudocode, tables, Mermaid diagrams per user guidance.
- Did NOT include the MigrationCheckpoint binary format details — just described its purpose.
- Forward reference to Chapter 12 (Session Management) at the closing.

**Verification notes:**
- The balanced assignment pseudocode matches rebalancer.rs:35-52 exactly
- The incremental 3-phase breakdown matches the function call chain: redistribute_primaries → add_missing_replicas → redistribute_replicas
- The removal algorithm description matches compute_removal_assignments: promote replica or assign least-loaded, fill replicas from round-robin
- The tick loop flowchart matches election.rs:14-47 exactly
- The backpressure gate matches replication.rs:249 (set) and replication.rs:88 (decrement)
- The migration state machine matches migration.rs:183-188 (valid_transition pattern match)
- The AwaitingSnapshot rejection matches replication.rs:101 (role check in handle_write)
- The stale epoch fast-fail matches quorum.rs:148-149 (stale_epoch_seen returns Failed)

### Session 15 — 2026-03-03

**Work done:**
- Wrote Chapter 19 placeholder draft (`ch19-vault-encryption.md`, 3,514 words)
- Sections: Two Threat Models, Vault Crypto Primitives, Vault Lifecycle, In-Memory Key Management, Transparent MQTT Data Path, Identity Encryption, What Went Wrong, Lessons
- Updated OUTLINE.md: inserted Chapter 19 in Part IV, renumbered existing Ch19 (Operations) → Ch20, Ch20 (WASM) → Ch21, updated dependency graph and page estimate
- Read all primary source files before writing: `vault_crypto.rs` (VaultCrypto, PBKDF2, AES-256-GCM, field-level encrypt/decrypt), `identity_crypto.rs` (IdentityCrypto, HKDF dual keys, blind indexing, key wrapping), `vault_keys.rs` (VaultKeyStore, zeroized keys, write fences), `handlers.rs` (vault HTTP endpoints: enable/unlock/lock/disable/change/status, batch_vault_operation, batch_vault_re_encrypt), `agent/handlers.rs` (vault_transform_request, vault_pre_update, vault_decrypt_response, is_vault_eligible), `session_store.rs` (Session.vault_unlocked, set_vault_unlocked_by_canonical_id), `server.rs` (vault route definitions, vault_unlock_limiter), `docs/OAUTH_VAULT_FUTURE_WORK.md` (batch atomicity, TOCTOU, identity race condition, schema initialization)
- Verified PBKDF2 iterations: 600,000 (vault_crypto.rs:16 and identity_crypto.rs:18)
- Verified nonce: 12 bytes, salt: 32 bytes, key: 32 bytes, tag: 16 bytes
- Verified AAD format: `"{entity}:{id}"` for vault, `"{entity}"` for identity
- Verified check token plaintext: `b"mqdb-vault-check-v1"`
- Verified rate limiter: 5 attempts per user (server.rs:82)
- Verified vault eligibility: `!entity.starts_with('_') && ownership.entity_owner_fields.contains_key(entity)`
- Verified write fence blocks read_fence during batch operations
- Verified blind index format: `HMAC-SHA256(key, "{entity}:{value}")` hex-encoded

**Key decisions:**
- 3,514 words, below the 5,000-6,000 target. Appropriate for a placeholder chapter on an incomplete feature — the chapter documents what exists and marks what is planned.
- Placed as Chapter 19 in Part IV (Advanced Patterns), after Chapter 18 (Access Control). The vault depends on OAuth sessions and ownership, both covered in Ch18.
- Included both vault and identity encryption in a single chapter — they share crypto primitives (AES-256-GCM, PBKDF2) but serve different threat models. Splitting into two chapters would create a very short identity-only chapter.
- Three "What Went Wrong" sections: batch atomicity (crash recovery), TOCTOU (vault state change during update), identity race condition (missing unique constraint). All documented in `OAUTH_VAULT_FUTURE_WORK.md`.
- Marked planned work with *(planned)* tags throughout — matches user's request for placeholder that acknowledges incompleteness.
- Did NOT include code — used tables, state diagrams, and narrative descriptions.
- Forward reference to Chapter 20 (Operations) at the closing.

### Session 15b — 2026-03-03

**Work done:**
- Updated Chapter 19 to reflect three vault fixes pushed by user (commit 341e6d5)
- Rewrote "What Went Wrong" section: replaced three *(planned)* items with three bug narratives (re-encryption resume losing old key, fence-key ordering race, fire-and-forget constraint init) plus one remaining *(planned)* item (update TOCTOU)
- Added migration status tracking description to Section 19.3 (vault lifecycle)
- Added old salt persistence for re-encryption resume to Section 19.3 (change passphrase)
- Updated unlock description in Section 19.3 to reflect fence-before-key ordering
- Trimmed "What Comes Next" to reflect only remaining planned work
- Word count: 3,514 → 4,068

### Session 16 — 2026-03-03

**Work done:**
- Wrote Preface first draft (`preface.md`, 1,055 words)
- Five "surprise" features that fell out of the MQTT constraint: change events, ownership, vault encryption, distributed routing, access control
- Closing thesis: architectural constraints as force multipliers, 5:1 ratio of emergent to engineered features
- Updated OUTLINE.md with Preface section before Part I
- Target was 1,000-1,500 words; landed at 1,055

**Key decisions:**
- Placed as preface, not a chapter section — this is authorial perspective on the discovery process, not technical architecture
- Each "surprise" grounded in specific code paths verified by Explore agent: events.rs publish, agent/handlers.rs x-mqtt-sender extraction, vault_transform_request interception, db/partition.rs topic-to-hash, topic_rules.rs protection tiers
- No code in the preface — pure narrative. Code comes in the chapters.
- The "5:1 ratio" claim: 5 emergent features (events, ownership, vault, routing, ACL) vs 5 hard problems (unique constraints, Raft, migration, sessions, query coordination) — honest about what was hard

### Memories for Future Sessions

*Key context that should survive conversation compaction.*

- Book lives in `book/` directory at project root
- Outline is `book/OUTLINE.md`, this diary is `book/WRITING_DIARY.md`
- Chapters go in `book/chapters/ch{NN}-{slug}.md`
- Target: ~5,000-6,000 words per chapter
- Audience: system designers, not practitioners
- Each chapter follows: Problem → Design Options → Implementation → What Went Wrong → Lessons Learned
- Source material mapping above links each chapter to specific code files
- **CRITICAL**: The book describes what is in the code and how we got there — check docs and code often, refer to git commits to verify gaps
- Writing incrementally across sessions; diary tracks all progress
