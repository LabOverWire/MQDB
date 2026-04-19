# Paper Manufacturing Diary

Tracking the writing process, decisions, verification status, and open questions.

---

## Session 1 — 2026-04-11: Paper conception and outline

### Decisions made
- **Target venue**: VLDB Journal (The VLDB Journal — The International Journal on Very Large Data Bases)
- **Core thesis**: MQTT's topic hierarchy and QoS semantics are sufficient to implement a full transactional distributed database, eliminating the architectural separation between messaging broker and database
- **Paper type**: Systems paper (architecture + evaluation), not theory paper
- **Structure model**: Follow CockroachDB SIGMOD 2020 template — architecture overview, data model, consensus layer, evaluation

### Key framing decisions
- Lead with topic-to-database mapping as core contribution
- Harper acknowledged as validation of the intuition, but no formal analysis or strong consistency
- IoTDB (VLDB 2020, SIGMOD 2023) as primary academic comparison point
- Kafka/KRaft as evidence of convergence from the opposite direction
- "Two-system problem" as rhetorical device (TBMQ needs Kafka + Redis + PostgreSQL; edge paper needs Mosquitto + SQLite + Redis)
- Vault included only if tied tightly to architectural argument (protocol-level encryption impossible in separated architectures)

### Architectural claims requiring code verification
- [ ] Topic hierarchy directly encodes CRUD operations, entity routing, and record addressing
- [ ] MQTT QoS levels map to database consistency guarantees
- [ ] Raft consensus messages travel through the same MQTT-based transport as user data
- [ ] Partition routing is determined by topic structure
- [ ] Field-level encryption enforced at topic handler level (vault claim)
- [ ] Single binary replaces broker + database + cache
- [ ] MQTT retain flag semantics and their relationship to record persistence
- [ ] Exact consistency model: what transactional guarantees does MQDB actually provide?
- [ ] Unique constraints and their two-phase protocol
- [ ] Cross-node pub/sub routing via topic index broadcast

### Open questions
1. What benchmark baselines to use? Minimum: Mosquitto + PostgreSQL. Candidate additions: EMQX + Redis, HiveMQ + TimescaleDB
2. Which standard benchmark suite? YCSB is the canonical choice for KV stores. TPC-IoT exists but may be too time-series focused. Custom workload likely needed for MQTT-native operations.
3. Page limit and format for VLDB Journal? (Need to check current submission guidelines)
4. Should we include the outbox pattern / consumer groups, or save for follow-up?
5. How to handle the "no QoS 2 for database ops" design choice — is this a limitation or a deliberate trade-off?

### Corrections from code verification
- **256 partitions, not 64** — `partition/types.rs` is unambiguous: `NUM_PARTITIONS: u16 = 256`
- **Raft is control plane only** — manages partition assignments and membership, NOT per-write consensus. Cannot claim "transactional" in ACID sense. Consistency model is async replication with per-partition causal ordering
- **Retain flag is MQTT-only** — not used for DB record upserts (unlike Harper). DB records persist through direct topic handler → DB engine path
- **QuorumTracker exists but is unused** — `QuorumTracker::with_completion()` returns a oneshot receiver, infrastructure for future durability levels. Currently all writes return after local primary apply + async broadcast
- **Thesis revised for precision**: "MQTT topic semantics are sufficient to implement a distributed database" — careful not to overclaim transactional guarantees. MQDB sits in the Redis/MongoDB-w:1 consistency class

### Outline completed
- See OUTLINE.md for full 8-section structure
- Estimated ~20 pages in VLDB Journal format
- 7 figures/diagrams identified
- 12 verification checkpoints against code

---

## Session 1 (cont.) — Code verification results

### VLDB Journal submission guidelines (RESOLVED)
- **Page limit**: 25 pages max, two-column LaTeX (Springer template with "twocolumn" option)
- **Scope**: Systems papers explicitly welcome — "submissions with a substantial theory component are welcome, but the VLDB Journal expects such submissions also to embody a systems component"
- **Impact factor**: ~3.8 (JCR 2024), Q1 journal
- **Submission**: Rolling (journal, not PVLDB conference track which has monthly deadlines)
- **Reproducibility**: Must provide reproducibility package with all experiments, data, artifacts
- **Resubmission**: 12-month embargo after rejection
- **Overlap**: Must contain ≥30% new content vs prior publications
- URL: https://link.springer.com/journal/778/submission-guidelines

### Verification results — corrections needed in outline

#### §3.1 Topic parsing — VERIFIED with detail
- Two-tier API confirmed: high-level (`$DB/{entity}/{id}`) and low-level (`$DB/p{N}/{entity}/{id}`)
- 16 enum variants in DbTopicOperation (not 5 — includes internal ops: IndexUpdate, UniqueReserve/Commit/Release, FkValidate, QueryRequest/Response, plus Json* variants)
- Topic parser at `crates/mqdb-cluster/src/cluster/db_topic.rs:6-24`
- Agent-mode parser at `crates/mqdb-core/src/protocol/mod.rs:210-247` (simpler, 5 ops only)

#### §3.4 Request-response — VERIFIED with BUG FOUND
- Agent mode: correlation data correctly extracted and restored in PublishProperties (handlers.rs:40-42)
- **BUG**: Cluster mode drops correlation data at final publish step. `queue_local_publish()` in routing.rs:172 does NOT pass correlation_data. Wire protocol (JsonDbResponse) serializes it, but routing layer drops it.
- Paper should note this as a known limitation or fix it before publication

#### §3.5 Low-level partition API — VERIFIED with ACCESS RESTRICTION
- Parser recognizes `$DB/p{N}/...` topics (db_topic.rs:75-117)
- **BLOCKED from external clients**: topic_rules.rs:41 → `$DB/p+/#` = ProtectionTier::BlockAll
- Internal use only: cluster replication, constraint checks, index updates
- Paper claim correct — "available to advanced clients" is WRONG, must say "internal only"

#### §4.2 Raft — NEEDS CORRECTION
- Raft handles 4 command types: UpdatePartition, AddNode, RemoveNode, Noop (state.rs:24-29)
- Paper said "partition assignments only" — must say "partition assignments AND cluster membership"
- Election timeout: 3-5s randomized, heartbeat 500ms, startup grace 10s
- Log compaction: keeps 1000 entries, compacts after every applied entry

#### §4.2 Replication — NEEDS PRECISION
- Two distinct write paths found:
  1. `replicate_write()` — creates QuorumTracker, sends to replicas, returns before acks
  2. `replicate_write_async()` — NO QuorumTracker, pure fire-and-forget
- Most writes use path 2 (async). Path 1 used for session creation only.
- `QuorumTracker::with_completion()` called ONLY for session creation, NOT general writes
- Sequence: simple per-partition counter in ReplicaState (`self.sequence += 1`)
- Gap threshold: `max_pending_gap: 1000` (replication.rs:40), confirmed hardcoded
- Primary blocks on send() but doesn't wait for acks — "send-and-forget"

#### §4.3 Partitioning — VERIFIED
- CRC32 of `{entity}/{id}` % 256 confirmed (partition/functions.rs)
- Separate hash functions for data, indexes, unique constraints (different key prefixes)
- Tests confirm determinism (partition/functions.rs:85-104)

#### §4.5 Broadcast entities — VERIFIED
- Constants at entity.rs: TOPIC_INDEX="_topic_index", WILDCARDS="_wildcards", CLIENT_LOCATIONS="_client_loc"
- 24 total entity constants defined (including sessions, QoS2, inflight, retained, etc.)
- Broadcast pattern confirmed: apply locally → write_or_forward to partition primary

#### §4.6 Unique constraints — VERIFIED
- Two-phase protocol confirmed: reserve → commit/release
- Topics: `$DB/_unique/p{N}/{reserve|commit|release}` (db_topic.rs:133-153)
- Lock drop/reacquire pattern used in routing.rs for async constraint checking

#### §5.2 Vault — MULTIPLE CORRECTIONS NEEDED
- **AES-256-GCM**: CONFIRMED (vault_crypto.rs:6, ring crate)
- **PBKDF2-HMAC-SHA256**: CONFIRMED, 600,000 iterations (vault_crypto.rs:16)
- **CORRECTION: Per-user encryption, NOT per-database** — each user has own passphrase-derived key stored in VaultKeyStore keyed by canonical_id
- **CORRECTION: Agent mode ONLY** — cluster mode explicitly rejects vault ops (cluster_agent/admin.rs:66-70: "vault admin not supported in cluster mode")
- Field-level encryption: CONFIRMED, recursive (nested objects/arrays encrypted)
- Record-level AAD: `{entity}:{id}` binds ciphertext to specific record
- Passphrase never stored: CONFIRMED, uses Zeroizing<Vec<u8>> for derived key
- System fields (prefix `_`) and ownership fields never encrypted

### Impact on outline

1. **§3.5 must be corrected**: partition API is internal-only, not available to external clients
2. **§4.2 must be corrected**: Raft covers membership + partitions, and two write paths exist
3. **§5 must be substantially revised**: vault is per-user (not per-database) and agent-mode only
   - This WEAKENS the vault section for the paper — agent-only means the "protocol-level encryption" argument only applies to single-node deployments
   - Consider whether to keep §5 or reduce to a paragraph in §7 Discussion
4. **Correlation data bug** in cluster mode — either fix before publication or note as limitation

### TODO
- [x] Retrieve full citations with DOIs for all papers in LITERATURE-DIARY.md — agent running
- [x] Decide: keep vault section (§5) or move to Discussion given agent-only limitation? → Keeping, scoped to single-node
- [ ] Fix or document correlation data bug in cluster mode
- [ ] Design and run benchmark experiments (§6)
- [x] Revise outline to incorporate all corrections — done Session 2
- [x] Wait for subscription/QoS agent to complete and incorporate findings — done

---

## Session 2 — 2026-04-11 (cont.): Subscription/QoS verification and outline revision

### §3.2 Subscriptions — MAJOR CORRECTION NEEDED

The subscription/QoS verification agent found that the outline's §3.2 table is **fundamentally wrong** in two rows:

| Outline claim | Actual behavior | Evidence |
|---|---|---|
| `$DB/users/user-42` = "Point read (GET by ID)" | **WRONG.** Subscribing receives ONLY future change events, NOT current value. `subscribe()` returns a subscription ID only. No snapshot retrieval. | `database/subscriptions.rs:12-28`, `dispatcher.rs:45-105` |
| `$DB/users/+` = "Scan / full table watch" | **WRONG.** Receives only future change events for any record in entity. Does NOT return list of current records. | `subscription.rs:47-62` pattern matching on `{entity}/{id}` |
| `$DB/users/events/#` = CDC | **CORRECT.** Events published to `$DB/{entity}/events/{id}` with operation type in payload. | `events.rs:93-116` |
| `$DB/users/list` = List with filters | **CORRECT** but this is a request-response operation (publish + response topic), NOT a subscription. | `protocol/mod.rs:210-247` |

**Root cause of confusion**: Subscriptions in MQDB are **change feeds only**, not queries. The read/list operations use MQTT 5.0 **request-response** (publish to topic, receive response on response topic), not subscriptions.

**Correct framing for §3.2**:
- MQTT subscriptions → Change Data Capture (CDC)
- MQTT request-response (MQTT 5.0) → Synchronous CRUD queries
- These are two distinct mechanisms, not one

### §3.3 QoS — MAJOR CORRECTION NEEDED

| Outline claim | Actual behavior | Evidence |
|---|---|---|
| QoS 0 = "fire-and-forget write" | **WRONG.** QoS is completely ignored for DB operations. Both QoS 0 and QoS 1 produce identical behavior. | `protocol/mod.rs:249-305` — parser does not examine QoS; `handlers.rs:226-257` — identical processing |
| QoS 1 = "acknowledged write" | **WRONG.** Responses always sent with hardcoded QoS 1 regardless of input QoS. | `handlers.rs:34-55` — `qos: QoS::AtLeastOnce` hardcoded |
| QoS 2 = "reserved" | Correct — unused for DB ops | — |

**Correct framing for §3.3**:
QoS in MQDB affects only the MQTT transport layer (message delivery guarantee from client to broker), not the database operation semantics. Once a message reaches the broker, the DB operation is processed identically regardless of QoS. This is an important **non-mapping**: QoS does NOT map to database durability levels, though it could in a future version (via QuorumTracker).

**Impact on thesis**: The original claim "QoS levels map to database consistency guarantees" must be removed from the contributions list. The thesis should focus on topic hierarchy + subscription semantics, not QoS. QoS remains a possible future extension but is NOT currently part of the mapping.

### Additional findings from subscription agent

1. **ChangeEvent payload structure** (`events.rs:16-31`):
   - Fields: sequence, entity, id, operation (Create/Update/Delete), data (Option<Value>), operation_id, sender, client_id, scope
   - Events published at QoS 1 always (`tasks.rs:152-226`)

2. **Change-only delivery optimization** (`broker.rs:52-59`):
   - Broker suppresses duplicate consecutive events with identical payloads on event topics
   - Topic patterns: `$DB/+/events/#`, `$DB/+/+/events/#`, `$DB/+/+/+/events/#`
   - MQTT 5.0 feature used for deduplication

3. **Event topic format**:
   - Agent mode: `$DB/{entity}/events/{id}`
   - Cluster mode (with partitions): `$DB/{entity}/events/p{N}/{id}`

### Outline revision completed

Major changes:
1. §3.2 rewritten: subscriptions are CDC only; CRUD uses request-response
2. §3.3 rewritten: QoS does NOT map to DB semantics (honest non-mapping)
3. §3.5 corrected: partition API is internal-only
4. §5 scoped to single-node deployments with honest limitation acknowledgment
5. Core thesis revised: removed QoS from contributions claim
6. Verification checklist updated with results

### Updated verification checklist

- [x] §3.1: Topic parsing — VERIFIED (two-tier API, 16 operations)
- [x] §3.2: Subscription patterns — VERIFIED AND CORRECTED (CDC only, not queries)
- [x] §3.3: QoS handling — VERIFIED AND CORRECTED (QoS ignored for DB ops)
- [x] §3.4: Request-response — VERIFIED (works in agent, bug in cluster)
- [x] §3.5: Low-level partition API — VERIFIED AND CORRECTED (internal only)
- [x] §4.2: Raft control plane — VERIFIED AND CORRECTED (membership + partitions)
- [x] §4.2: Async replication — VERIFIED (two write paths, most are fire-and-forget)
- [x] §4.3: CRC32 partition hash — VERIFIED (deterministic, separate hash functions)
- [x] §4.5: Broadcast entities — VERIFIED (TopicIndex, Wildcards, ClientLocations)
- [x] §4.6: Unique constraint protocol — VERIFIED (two-phase: reserve → commit/release)
- [x] §5.2: Vault — VERIFIED AND CORRECTED (per-user, agent-only)
- [ ] §6: All benchmark numbers (to be generated)

---

## Session 3 — 2026-04-11 (cont.): Full prose draft

### Sections completed
All 8 sections now have prose drafts in `sections/`:

| Section | File | Words (approx) | Status |
|---|---|---|---|
| §1 Introduction | `01-introduction.md` | ~1,800 | First draft |
| §2 Background | `02-background.md` | ~2,200 | First draft |
| §3 Mapping | `03-mapping.md` | ~2,800 | First draft |
| §4 Architecture | `04-architecture.md` | ~2,500 | First draft |
| §5 Vault | `05-vault.md` | ~900 | First draft |
| §6 Evaluation | `06-evaluation.md` | ~1,500 | Framework only (TODO: benchmark results) |
| §7 Discussion | `07-discussion.md` | ~1,500 | First draft |
| §8 Conclusion | `08-conclusion.md` | ~400 | First draft |

Total: ~13,600 words across 8 sections (excluding tables and TODOs).

### Vault verification (Session 3)
Verified against code before writing §5:
- PBKDF2 iterations: 600,000 (vault_crypto.rs:16)
- Salt: 256-bit (32 bytes) random (vault_crypto.rs:14)
- Nonce: 96-bit (12 bytes) random per field (vault_crypto.rs:12)
- AES-256-GCM tag: 128-bit (16 bytes) (vault_crypto.rs:15)
- AAD format: `"{entity}:{id}"` (vault_crypto.rs:216-217)
- Cluster rejection: admin.rs:66-70, ErrorCode::Forbidden, "vault admin not supported in cluster mode"
- Type marker: `\x01` prefix for non-string values before encryption (vault_crypto.rs:146-150)
- Skip conditions: `key.starts_with('_')` OR in skip_fields (vault_crypto.rs:109, 155)
- Skip fields built from: `["id"]` + ownership field if configured (vault_transform.rs:14-20)
- Encoding: Base64 standard (nonce || ciphertext || tag) (vault_crypto.rs:229)

### Remaining work
- [ ] Run benchmark experiments and fill in §6 results tables
- [ ] Fix or document correlation data bug in cluster mode
- [ ] Create figures/diagrams (7 identified in outline)
- [ ] Convert Markdown to LaTeX (Springer VLDB Journal template)
- [ ] Literature: TPC-IoT spec, edge computing surveys
- [ ] Review complete draft for internal consistency
- [ ] Prepare reproducibility package

---

## Session 4 — 2026-04-16: Benchmark data provenance audit and SOP adoption

### Trigger

While smoke-testing the new `mqdb agent --memory-backend` flag earlier today, 24
`mqdb_mem_*.json` files were produced on the local macOS workstation and
written into `bench/results-aws/phase1/`. The new rows were then added to
Tables 4, 5, and 6 in §6 and a storage-engine-cost decomposition paragraph
was drafted around them. Reviewing §6 before commit revealed the problem: the
directory name implies AWS c7g.xlarge provenance, but the files were produced
locally.

Follow-up inventory of the same directory found a further 84 files
(`mqdb_*`, `baseline_pg_*`, `baseline_redis_*`, `mosquitto_pubsub_*`,
`rest_pg_*`) with uniform `Apr 15 12:10` or `Apr 15 18:27` timestamps. A
sequential sweep of the full matrix cannot complete within that window. The
`publications/` tree is untracked in git, so there is no commit history to
audit; the Terraform stack in `infra/` has never been applied (pre-requisites
still unchecked in `infra/DIARY.md`). Provenance for all 108 files in
`results-aws/phase1/` is therefore indeterminate, and none of the numbers in
the current §6 tables can be treated as AWS data.

### Actions taken

1. **Quarantine.**
   - 24 known-local `mqdb_mem_*.json` → `bench/results-local/phase1/`.
   - 84 indeterminate-origin files → `bench/results-unverified/phase1/`.
   - `bench/results-aws/phase1/` now contains only `README.md`, `.gitkeep`,
     `.guard`. Every quarantined directory has a README explaining why its
     files may not be cited in §6.
2. **SOP.** `bench/AWS-PROCEDURE.md` is now the single authorised entry point
   to `results-aws/`. Any other path is forbidden.
3. **Guardrails.** `bench/scripts/guard_aws_dir.sh` is sourced by
   `run_phase1_agent.sh` and by every `extras/*/run.sh`; it refuses writes
   into `results-aws/` unless `MQDB_AWS_RUN_TOKEN` is set, `.guard` exists,
   and IMDSv2 is reachable. `bench/scripts/provenance.sh` produces the
   per-run provenance object and sets `environment=aws-ec2` only when IMDSv2
   confirms EC2. `bench/scripts/aws_run.sh` orchestrates the full flow.
4. **Paper §6 revert.**
   - Removed the MQDB (memory) column from Tables 4, 5, and 6.
   - Removed the storage-engine-cost decomposition paragraph.
   - Added a "Data provenance" paragraph to §6.1 naming
     `bench/results-aws/phase1/run-manifest.json` as the authoritative
     source; tables currently reflect numbers pending re-execution.
   - Marked removed content with `<!-- PENDING AWS RUN -->`.
5. **Ancillary.** Added `--memory-backend` to `CHANGELOG.md` Unreleased.
   Updated `MEMORY.md` with the NEVER-LOCAL rule. Cross-referenced
   `docs/internal/vldb-benchmarks.md` (Session 1) for the full action log
   and verification steps.

### §6 status after this session

- Tables 4, 5, 6 now show only four configurations: MQDB (fjall),
  Mosquitto + PG, Mosquitto + Redis, REST + PG.
- Numerical values in these tables originate from
  `bench/results-unverified/phase1/` and are provisional. They are retained
  in the draft so the narrative is readable, but they will be replaced with
  AWS-sourced numbers once `bench/AWS-PROCEDURE.md` produces them.
- The MQDB (memory) row, the fjall-vs-memory contrast, and the
  storage-engine-cost paragraph will all be restored only after a logged
  AWS run.

### Rule (NEVER LOCAL)

**Any number cited in the paper as AWS data must come from
`bench/results-aws/phase1/` and must be produced by the SOP in
`bench/AWS-PROCEDURE.md`.** Local workstation results are for development
only and live under `bench/results-local/` with a README that forbids
citation. This rule is restated in `MEMORY.md`, `AWS-PROCEDURE.md`,
`docs/internal/vldb-benchmarks.md`, and the three README files under
`bench/results-*/phase1/`.

### Open items

- [ ] Run `bench/scripts/aws_run.sh` end-to-end in `ca-west-1` (requires AWS
      credentials, cost sign-off). Tracked in
      `docs/internal/vldb-benchmarks.md` Session 2.
- [ ] Re-populate Tables 4–6 from the JSON emitted by the SOP run; replace
      the `<!-- PENDING AWS RUN -->` markers with real values and restore
      the MQDB (memory) column once both configurations have matching
      provenance.
- [ ] Phase 2 (cluster) benchmarks inherit the same SOP but require a
      three-instance Terraform change.

---

## Session 5 — 2026-04-16 (cont.): Fold extras into main sweep

### Trigger

Session 4 placed MQDB (memory) and REST+PG behind `bench/extras/*/run.sh` on
the grounds that they were "secondary" configurations. Re-reading §6.1 made
clear that both are core: REST+PG is one of the four comparison points and
MQDB (memory) is the storage-engine-cost control. Gating either behind an
extras directory breaks the single-sweep guarantee of
`bench/AWS-PROCEDURE.md` (one provenance block per run) and invites future
drift between the configurations.

### Actions taken

1. **Directory layout.**
   - `bench/extras/rest-pg/` → `bench/rest-pg/` (Rust source for
     `rest-pg-bench`). The standalone `run.sh` was redundant with
     `run_phase1_agent.sh` and was deleted.
   - `bench/extras/mqdb-memory/` removed entirely; its logic moved into
     `run_phase1_agent.sh`.
   - `bench/extras/` no longer exists.
2. **`run_phase1_agent.sh`.** Added `start_mqdb_memory()` which mirrors
   `start_mqdb` but passes `--memory-backend`. CRUD and pub/sub loops now
   produce `mqdb_mem_{op}_run{N}.json` and `mqdb_mem_pubsub_qos{Q}_run{N}.json`
   in the same pass. Summary tables list all five labels: `mqdb`,
   `baseline_pg`, `baseline_redis`, `rest_pg`, `mqdb_mem` (CRUD) and `mqdb`,
   `mosquitto`, `mqdb_mem` (pub/sub). `bash -n` + `--dry-run` both pass.
3. **`aws_run.sh`.** `execute_runs` collapsed to a single invocation of
   `run_phase1_agent.sh`. All five configurations now share the same
   instance, OS load, and provenance object.
4. **`bench/AWS-PROCEDURE.md`.** Updated to describe the single unified
   sweep. Expected file list enumerates all five CRUD configs and all three
   pub/sub configs. Added a "not allowed" rule forbidding subset runs whose
   output is mixed into `results-aws/`.
5. **READMEs.**
   - `results-aws/phase1/README.md`: expected-contents list now covers all
     108 JSON files plus `provenance.json` and `run-manifest.json`.
   - `results-local/phase1/README.md`: removed the dangling
     `bench/extras/mqdb-memory/run.sh` reference.
6. **Paper §6.** Restoring "Five configurations tested" to §6.1 and
   re-adding the MQDB (memory) column to Tables 4, 5, 6 with
   `<!-- PENDING AWS RUN -->` cells. The storage-symmetry paragraph is
   rewritten to forward-reference the pending AWS run rather than omit the
   contrast entirely.

### Rationale

Session 4 reverted the MQDB (memory) content because the numbers were
local-only and the SOP was not yet in place. With the SOP in place and the
extras folded in, the *configuration* is permanent (it ships with the
harness); only the *numbers* are pending. Documenting the five-way structure
up front means the AWS run will only need to fill cells, not reshape the
section.

### §6 status after this session

- §6.1 enumerates five configurations: MQDB (fjall), Mosquitto + PG,
  Mosquitto + Redis, REST + PG, MQDB (memory).
- Tables 4, 5, 6 carry the MQDB (memory) column with `PENDING AWS RUN`
  placeholders in every cell of that column.
- The other four columns still contain indeterminate-origin numbers pending
  the SOP run (unchanged from Session 4).

### Open items

Unchanged from Session 4: run `aws_run.sh` end-to-end, repopulate all
tables from the emitted JSON, Phase 2 cluster work.

---

## Session 6 — 2026-04-16 (cont.): Reviewer-driven §6 prose revision

### Context

Reviewer-style critique of §6 raised issues that do not require a new AWS run
to fix. The critique and remediation are tracked in detail in
`docs/internal/vldb-benchmarks.md` Session 3; this diary entry is the
paper-side summary.

### What changed in `sections/06-evaluation.md`

1. **New "Durability semantics" paragraph** in §6.1. Names MQDB's benchmark
   default as `--durability periodic --durability-ms 10`, establishes that
   only MQDB (fjall) writes durably, and documents the escape hatch
   (`--durability immediate`).
2. **Rewritten "Storage symmetry and the tmpfs decision" paragraph** in §6.1.
   Frames the tmpfs choice as an explicit methodological decision (isolate
   protocol/architecture cost from storage cost), not a passive setup choice.
3. **§6.2 Analysis opener rewrite**. Replaced "outperforms all three
   baselines" with "outperforms each of the three external baselines", and
   added an explicit deferral of the MQDB (memory) comparison.
4. **§6.3 Analysis rewrite**. Dropped the Rust-vs-C attribution for the QoS 1
   44% lead. Dropped the "acknowledgment as natural rate limiter" speculation
   for QoS 0 variance. Both results are now flagged rather than explained.
5. **Table 7 restructure**. Dropped the rhetorical "Failure modes" row.
   Replaced "Data consistency guarantee" with two narrower rows:
   "Broker ↔ DB atomicity" and "Durability window (default)".
6. **Table 7 concluding paragraph** rewritten to use the new row names and to
   cross-reference the `--durability immediate` escape hatch.

### Durability facts verified against MQDB source

Read directly from the code to avoid paper-claim drift:

- `crates/mqdb-core/src/config.rs` — `DurabilityMode::{Immediate, PeriodicMs,
  None}`.
- `crates/mqdb-core/src/storage/fjall_backend.rs:31-39` — `sync_if_needed()`
  calls `PersistMode::SyncAll` only under `Immediate`; `PeriodicMs` and `None`
  rely on fjall's background flushing.
- `crates/mqdb-cli/src/cli_types/agent.rs` — CLI defaults to
  `--durability periodic --durability-ms 10`.

### What the revision does NOT fix

Tracked open items needing either user sign-off or an AWS run:

- c7g.xlarge only (reviewer called it too small for a credible database paper).
- Concurrency=1 only (no sweep).
- Round-robin mixed workload, not Zipfian.
- No p99.9 or max latency (N=3 × 1,000 ops is too few samples).
- Thesis-evaluation mismatch: §6 measures a fast JSON store but doesn't
  exercise change feeds, unique-constraint contention, or cascade deletes
  — the primitives that motivate MQTT-as-database.

---

## Session 7 — 2026-04-16: Widen SOP + thesis workloads (Step 3)

Decided: add all three thesis workloads + widen sweep parameters.

### Harness

- `db_changefeed.rs`, `db_unique.rs`, `db_cascade.rs` — three new MQDB bench ops.
- Bridge event emission, PG unique/cascade DDL, Redis SETNX/scripted cascade.
- REST+PG `LISTEN/NOTIFY` long-poll, unique/cascade endpoints, three new bench ops.
- `run_phase1_agent.sh`: four new suite functions, OPERATIONS=10000, N=5,
  concurrency sweep {1,8,32,128}, unique K∈{4,16,64}, cascade K∈{10,100,1000}.
- `aws_run.sh`: two-instance-type loop, per-instance subdirs, top-level manifest.
- `extract_tables.py`: reads manifest, emits Markdown for all 8 tables.

### Paper

- §6.1: two instances, N=5, 10000 ops, concurrency sweep.
- §6.2: Tables 4a/4b (per instance) + 4c (mixed concurrency on 4xlarge).
- §6.3: Tables 5a/5b (per instance).
- §6.4 new: Architectural Workloads (6.4.1 changefeed Table 6, 6.4.2 unique
  Table 7, 6.4.3 cascade Table 8).
- §6.5 renamed from §6.4: Architectural Comparison (Table 9).
- §6.6 new: Threats to Validity.
- §7.3 + §8: forward-references to §6.4.
- 30 `_PENDING AWS RUN_` markers await Step 4.

### Verification

`cargo make dev` clean. All shell scripts parse. Guard refuses without token.

### Open items

- [x] Decide §6.4 scope.
- [x] Widen SOP.
- [x] Run `aws_run.sh` end-to-end in `ca-west-1` (Step 4).
- [x] Repopulate tables via `extract_tables.py`.
- [ ] Phase 2 cluster benchmarks.

---

## Session 8 — AWS Execution + Table Population (2026-04-17/18)

Step 4 of the benchmark plan: executed `aws_run.sh` on two instance types in
ca-west-1 and populated all §6 tables.

### AWS run details

- **Commit**: d35e22a
- **Instances**: c7g.xlarge (i-01baf82986d018e28) + c7g.4xlarge (i-0d4e925db49ce5c9e)
- **Region**: ca-west-1a
- **AMI**: ami-009b601d5ffeb769c (Ubuntu 24.04 ARM)
- **Kernel**: Linux 6.17.0-1010-aws
- **Duration**: c7g.xlarge ~5h (15:25–20:12 UTC), c7g.4xlarge ~6.5h (20:12–02:42 UTC)
- **Result files**: 437 per instance type, 874 total
- **Top-level manifest**: `bench/results-aws/phase1/run-manifest.json`

### Prior fixes (commits during Step 4 iterations)

Seven AWS run attempts before success. Fixes across attempts:
- `19e16bc` wait for cloud-init before installing packages
- `5e3f433` fix python NameError in provenance.sh
- `8eb073a` fix MQDB_ROOT path and cargo PATH for remote execution
- `c073f7c` add seq column to PG schema for REST server changefeed
- `d35e22a` add SSH keepalive to prevent connection timeout

### Data anomalies discovered

1. **Bridge changefeed events_received=0**: Mosquitto + PG and Mosquitto + Redis
   bridge emit_event() publishes at QoS 0; under sustained 500 writes/s the
   event stream is silently dropped. Same bridge code delivers cascade events
   (K=10–1000) successfully, confirming the failure is load-dependent.
2. **MQDB cascade events_received=0**: cascade delete commits parent + K
   children in one WriteBatch but only emits a parent-level change event, not
   per-child events. Known v0.7.2 limitation.
3. **PG insert anomaly on c7g.4xlarge**: 76 ops/s (vs 499 on c7g.xlarge). PG
   WAL sync fires at ~41ms intervals; on faster hardware a larger fraction of
   operations hit the sync barrier (p95 at 41ms vs p99 on c7g.xlarge).
4. **REST+PG unique successes > 1**: HTTP driver's concurrent requests bypass
   application-layer serialization; PG unique index violations not surfaced
   through HTTP error path.

### extract_tables.py fixes

- `cell_unique()` now checks `wall_secs` fallback (REST+PG uses `wall_secs`
  not `duration_secs`)
- `cell_changefeed_ms()` shows "—" when `events_received=0`
- `cell_cascade_ms()` shows "—" for propagation latency when `events_received=0`

### §6 updates

All PENDING markers replaced with AWS data. Analysis paragraphs written for
§6.2 (CRUD), §6.3 (pub/sub), §6.4.1 (changefeed), §6.4.2 (unique), §6.4.3
(cascade). Zero PENDING markers remain.

---

## Session 6 — 2026-04-18: List performance fix and re-run preparation

### Bug found: list_with_early_pagination used prefix_scan instead of prefix_scan_batch

Investigation in `docs/internal/list-performance-investigation/FINDINGS.md` revealed
that `list_with_early_pagination()` in `query.rs` called `prefix_scan()`, which
materializes ALL records into a `Vec`, even when the client requests `limit: 10`.
The `prefix_scan_batch()` method — which stops after a specified count — existed
but was not used for the paginated list path.

**Root cause**: O(N) full-table scan in storage layer, not response serialization
or MQTT backpressure (initial hypothesis was wrong for the bench scenario).

**Fix**: Replace `prefix_scan()` with `prefix_scan_batch()`. Local result:
throughput constant at ~2,000 ops/s regardless of dataset size (7.7x improvement
at 10K records on fjall).

### List operation semantics documented

Added "List operation semantics" paragraph to §6.1 describing what each system
actually does for a list operation:

| System | Records read from storage | Records returned | Response size |
|--------|--------------------------|------------------|---------------|
| MQDB (fjall/memory) | 10 (bounded scan) | 10 | ~7 KB |
| Mosquitto + PG | 10 (SQL LIMIT 10) | 10 | ~7 KB |
| Mosquitto + Redis | 10,000 (SMEMBERS) + 10 (MGET) | 10 | ~7 KB |
| REST + PG | 100 (SQL LIMIT 100, hardcoded) | 100 | ~70 KB |

Key asymmetries:
- Redis `SMEMBERS` is O(N) regardless of limit — explains its 68 ops/s
- REST+PG hardcodes LIMIT 100 (no client-side limit param) — 10x more work per request
- The old MQDB bug read all N records but returned only 10 — now fixed

### §6 list analysis rewritten

Removed incorrect claim: "The list workload is the one case where a relational
engine's native scan outperforms the LSM-based approach." That was a bug, not an
architectural limitation.

New analysis describes each system's access pattern. MQDB list numbers marked
PENDING until AWS re-run completes.

### Re-run infrastructure created

- `bench/run_phase1_list_rerun.sh` — targeted script running only list + mixed +
  mixed_concurrency_sweep for all 5 configurations
- `bench/scripts/aws_run.sh` — added `PHASE1_SCRIPT` env var support

Usage: `PHASE1_SCRIPT=run_phase1_list_rerun.sh bash bench/scripts/aws_run.sh`

### Files affected by re-run (per instance type)

MQDB-only changes (fix in query.rs), but all 5 configs re-run for provenance:
- 25 list files (5 configs × 5 runs)
- 25 mixed standalone files (5 configs × 5 runs)
- 80 mixed concurrency files (4 configs × 4 concurrency × 5 runs)

### AWS re-run completed (2026-04-18)

Re-run executed on both c7g.xlarge and c7g.4xlarge. Key results:

**List standalone (16x improvement on c7g.xlarge):**
| Instance | MQDB (fjall) old | MQDB (fjall) new | MQDB (mem) old | MQDB (mem) new |
|---|---|---|---|---|
| c7g.xlarge | 141 | 2,281 | 280 | 2,430 |
| c7g.4xlarge | 115 | 2,521 | 206 | 2,693 |

MQDB list now fastest across all configurations on both instances.

**Mixed concurrency sweep (c7g.4xlarge) — dramatically different profile:**
| c | MQDB (fjall) old | MQDB (fjall) new |
|---|---|---|
| 1 | 421 | 2,545 |
| 8 | 249 | 502 |
| 32 | 473 | 6,777 |
| 128 | 479 | 6,369 |

c=8 dip persists (single event loop contention) but c=32 and c=128 now show
strong throughput scaling. The old numbers were dominated by the O(N) list
scan; with that fixed, MQDB exceeds PG at c=32 and competes at c=128.

Baseline consistency: PG c7g.4xlarge list (1,738 ± 14) within 3% of original
(1,784 ± 10). PG c7g.xlarge list (967 ± 238) shows cold-start effect in first
run; noted in analysis.

### Paper updated

- Tables 4a, 4b list rows: new numbers for all 5 configurations
- Table 4c: all new numbers from c7g.4xlarge sweep
- List analysis: PENDING markers replaced with actual numbers
- Table 4c analysis: rewritten to describe the new concurrency profile
  (c=8 single-loop contention, c=32 amortization breakthrough, c=128 PG pool advantage)
- Data provenance note: updated to reference both SOP runs
