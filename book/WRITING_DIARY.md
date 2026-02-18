# Writing Diary

Tracking the incremental writing of *Building a Distributed Reactive Database*.

## Book Structure

| Part | Chapters | Pages (est.) | Status |
|------|----------|-------------|--------|
| I: The Design Thesis | 1-3 | ~70 | In progress |
| II: Distributing the System | 4-9 | ~150 | Not started |
| III: Cluster Lifecycle | 10-14 | ~100 | Not started |
| IV: Advanced Patterns | 15-18 | ~80 | Not started |
| V: Operating and Extending | 19-20 | ~50 | Not started |
| Appendices | A-D | ~30 | Not started |

## Chapter Progress

| Ch | Title | Status | Draft Date | Revision Date | Word Count |
|----|-------|--------|------------|---------------|------------|
| 1 | Why Unify Messaging and Storage? | First draft | 2026-02-16 | | 3,522 |
| 2 | The Storage Foundation | Not started | | | |
| 3 | MQTT 5.0 as a Database Protocol | Not started | | | |
| 4 | Partitioning | Not started | | | |
| 5 | Replication | Not started | | | |
| 6 | Consensus with Raft | Not started | | | |
| 7 | Transport Layer Evolution | Not started | | | |
| 8 | Cross-Node Pub/Sub Routing | Not started | | | |
| 9 | Query Coordination | Not started | | | |
| 10 | Failure Detection and Recovery | Not started | | | |
| 11 | Rebalancing | Not started | | | |
| 12 | Session Management | Not started | | | |
| 13 | The Message Processor Pipeline | Not started | | | |
| 14 | The Wire Protocol | Not started | | | |
| 15 | Constraints in a Distributed System | Not started | | | |
| 16 | Consumer Groups and Event Routing | Not started | | | |
| 17 | Performance Analysis and Benchmarking | Not started | | | |
| 18 | Security Architecture | Not started | | | |
| 19 | Operating MQDB | Not started | | | |
| 20 | The WASM Frontier | Not started | | | |
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
| 3 | src/agent/, src/auth_config.rs, src/topic_protection.rs, README.md (auth sections) |
| 4 | src/cluster/partition_map.rs, src/cluster/partition.rs, DISTRIBUTED_DESIGN.md (A1, Part 1, Part 5) |
| 5 | src/cluster/replication.rs, src/cluster/store_manager/, DISTRIBUTED_DESIGN.md (A4) |
| 6 | src/cluster/raft/, DISTRIBUTED_DESIGN.md (Part 8, Issues 11.2, 11.5) |
| 7 | src/cluster/quic_transport.rs, src/cluster/mqtt_transport.rs, src/cluster/transport.rs, DISTRIBUTED_DESIGN.md (A6) |
| 8 | src/cluster/topic_index.rs, src/cluster/topic_trie.rs, src/cluster/publish_router.rs, src/cluster/client_location.rs, DISTRIBUTED_DESIGN.md (Part 7) |
| 9 | src/cluster/query_coordinator.rs, src/cursor.rs, DISTRIBUTED_DESIGN.md (A5) |
| 10 | src/cluster/heartbeat.rs, src/cluster/snapshot.rs, DISTRIBUTED_DESIGN.md (Part 3, Part 9) |
| 11 | src/cluster/rebalancer.rs, src/cluster/migration.rs, DISTRIBUTED_DESIGN.md (Part 4, Issues 11.10, 11.17) |
| 12 | src/cluster/session.rs, src/cluster/inflight_store.rs, src/cluster/qos2_store.rs, DISTRIBUTED_DESIGN.md (M8-M10) |
| 13 | src/cluster/message_processor.rs, src/cluster/dedicated_executor.rs, DISTRIBUTED_DESIGN.md (A6.6, A6.7) |
| 14 | src/cluster/protocol/, DISTRIBUTED_DESIGN.md (Part 2) |
| 15 | src/cluster/node_controller/unique.rs, src/cluster/node_controller/fk.rs, src/cluster/db/constraint_store.rs, src/cluster/protocol/fk.rs, DISTRIBUTED_DESIGN.md |
| 16 | src/consumer_group.rs, src/dispatcher.rs |
| 17 | COMPLETE_MATRIX_DOC.md, COMPLETE_MATRIX_RESULTS.md, DISTRIBUTED_DESIGN.md (A6.4, A6.5 benchmarks) |
| 18 | src/auth_config.rs, src/topic_protection.rs, src/topic_rules.rs, README.md (auth sections) |
| 19 | CLI_TESTING_GUIDE.md, src/bin/mqdb/ |
| 20 | src/storage/memory_backend.rs, Cargo.toml (wasm feature) |

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
