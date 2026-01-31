# MQDB Formal Verification Process

## Purpose

Use TLA+ models verified by `tlc-executor` to find real bugs in MQDB's distributed subsystems. The process has strict phases with gates between them to prevent the most common failure mode: building a model that encodes a bug hypothesis as a structural assumption, then "discovering" the planted bug.

## Tool

All models are checked with `tlc-executor` at `/Volumes/SanDisk 4TB/repos/tlc-executor`. Specs live in `/Volumes/SanDisk 4TB/repos/tlc-executor/test_cases/mqdb/`. The Rust implementation lives at `/Volumes/SanDisk 4TB/repos/MQDB/`.

Run models with:
```bash
cargo run -- test_cases/mqdb/<Spec>.tla --constant <K1>=<V1> --constant <K2>=<V2> --allow-deadlock
```

## What to Model

Model subsystems where concurrency or distribution makes exhaustive testing impractical:

- Outbox dispatch atomicity under crash/restart
- Consumer group rebalancing during concurrent join/leave/timeout
- Cluster failover: primary promotion, epoch fencing, stale write rejection
- Quorum logic under node failures
- Idempotency under concurrent retries
- Replication protocol correctness

## What NOT to Model

Skip anything that unit/integration tests cover well:

- Sequential CRUD operations
- Schema validation, constraint checking (deterministic, no concurrency)
- HTTP routing, serialization, auth (no interesting state)
- Configuration parsing

---

## Phase 1 — Scope Selection

**Goal:** Identify what to model by reading the Rust source. No TLA+ in this phase.

**Input:** A subsystem name (e.g., "consumer group rebalancing").

**Process:**

1. Read all Rust source files relevant to the subsystem.
2. Identify every state transition: what triggers it, what state it reads, what state it mutates.
3. For each transition, record: function name, file path, line number, what concurrent transitions could interleave with it.
4. Identify implicit invariants the code relies on — properties that are assumed true but not explicitly checked at every step.

**Output:** A grounding document containing:

- Table of state transitions with file:line citations
- Which transitions involve concurrency or distributed coordination
- List of implicit invariants with Rust code citations
- Recommended scope for the TLA+ model (which transitions to include, which to abstract away)

**Gate:** The grounding document must exist and be reviewed before proceeding. No TLA+ code is written in this phase.

---

## Phase 2 — Model Construction

**Goal:** Write a TLA+ spec that faithfully represents the state transitions from Phase 1. No bug hunting.

**Input:** The grounding document from Phase 1.

**Rules:**

1. Every action in the TLA+ spec MUST have a traceability comment citing the Rust source it models:
   ```tla
   \* src/consumer_group.rs:142 ConsumerGroup::join()
   JoinGroup(c) == ...
   ```
   This is the ONE context where comments in code are mandatory. Without traceability, the model has no anchor to reality.

2. The spec MUST include a `TypeOK` invariant constraining all variables to their expected domains. This is the structural sanity check.

3. The spec MUST pass `tlc-executor` with no invariant violations before proceeding:
   ```bash
   cargo run -- test_cases/mqdb/<Spec>.tla --constant <params> --allow-deadlock
   ```

4. If `TypeOK` is violated, the model is wrong. Fix the model, not the invariant.

5. Do NOT add "interesting" invariants yet. Only TypeOK and structural invariants that assert the model doesn't produce nonsense states.

6. Keep constants small for initial verification (2 consumers, 2 partitions, etc.). Expand later.

**Output:** A TLA+ spec that passes TypeOK. Committed to `test_cases/mqdb/`.

**Gate:** The spec must pass tlc-executor with zero violations before proceeding to Phase 3.

---

## Phase 3 — Invariant Formulation

**Goal:** Add safety properties and check them. Violations found here are potential real bugs.

**Input:** The verified spec from Phase 2 and the implicit invariants from Phase 1.

**Process:**

1. For each implicit invariant identified in Phase 1, write a TLA+ invariant. Each invariant must:
   - State what real-world guarantee it captures (e.g., "a committed write survives any single node failure")
   - Cite the Rust code that implements this guarantee
   - Be phrased as a positive property ("all committed writes are durable") not a negative hypothesis ("I think data loss can happen")

2. Run tlc-executor with all invariants enabled.

3. If an invariant is violated, follow this decision tree:

   ```
   Invariant violated
   ├── Check: Does every action in the trace correspond to real Rust code?
   │   ├── NO → Model is wrong. Fix the model. Do not claim a bug.
   │   └── YES → Check: Is the action's guard/precondition faithful to the Rust code?
   │       ├── NO → Model is wrong. Fix the model.
   │       └── YES → Potential real bug. Proceed to reporting.
   ```

4. To report a potential bug, provide:
   - The counterexample trace from tlc-executor
   - For each step in the trace: the corresponding Rust function and how it maps
   - A concrete scenario description: "If consumer C1 sends a heartbeat at time T, and C2 joins at time T+1, and then C1's heartbeat times out at T+2, then..."
   - The specific Rust code path that would need to change to prevent the issue

**Output:** Invariant results. Either "all invariants pass" (the system is correct for the modeled scope) or a bug report with full traceability.

**Gate:** Every violation must be traced back to Rust code before being called a bug.

---

## Phase 4 — Bug Variants (Optional)

**Goal:** Demonstrate that the model is sensitive enough to catch known-bad behaviors.

**Input:** The verified spec from Phase 3.

**Process:**

1. Create a copy of the spec in `test_cases/mqdb/bugs/`.
2. Deliberately weaken ONE action by removing a guard or dropping a step.
3. Name the weakening explicitly: `MQDBCluster_BugNoEpochFence.tla`.
4. The weakened spec MUST violate an invariant that the correct spec satisfies.

**Rules:**
- Each bug variant removes exactly one guard/step
- The removed guard must cite the Rust line that implements it
- If the bug variant still passes all invariants, either the invariant is too weak or the guard wasn't actually necessary — both are worth investigating

**Output:** Bug variant specs that demonstrate the model's discriminating power.

---

## Anti-Patterns to Avoid

**Model encodes the hypothesis.** If you believe "rebalancing can orphan a partition," do NOT build a model where rebalancing sometimes skips partitions. Build a faithful model and see if orphaning emerges from correct-looking actions.

**Vacuous invariants.** An invariant that's trivially true (e.g., checking a variable is non-negative when it's initialized to 0 and only incremented) wastes time. Invariants should be properties that COULD plausibly be violated.

**Over-abstraction.** If the model abstracts away the mechanism you're trying to verify, it can't find bugs in that mechanism. If you're checking epoch fencing, the model must represent epochs explicitly.

**Under-specification of failure.** If the real system can crash mid-operation, the model must include crash transitions. A model that only represents clean state transitions will miss crash-recovery bugs.

---

## Subsystem Catalog

Existing verified models (in `test_cases/mqdb/`):

| Spec | Subsystem | Key Invariants |
|------|-----------|----------------|
| MQDBCore | Event store CRUD | MonotonicSequence, EventNotLost |
| MQDBOutbox | Outbox dispatch | NoDoubleDelivery, DispatchingInOutbox |
| MQDBConsumerGroup | Consumer groups | NoGapsWhenMembersExist, AllAssignedAreMembers |
| MQDBIdempotency | Request dedup | ExactlyOnceSemantics |
| MQDBAsyncReplication | Primary-backup | AckedDataDurable |
| MQDBQuorum | Quorum consensus | QuorumImpliesSuccess |
| MQDBCluster | Multi-partition | SinglePrimary, NoStaleEpochWrite |
| MQDBConstraints | DB constraints | UniqueConstraint, FKIntegrity |
| MQDBDelivery | Event delivery | NoDoubleDelivery |

Additional specs in `specs/`:

| Spec | Subsystem | Focus |
|------|-----------|-------|
| RaftElection | Leader election | Basic Raft voting |
| RaftFlappingFix | Flapping leader | Stability fix |
| ReplicationAmplification | Write amplification | Replication cost |
| WriteRequestDedup | Write dedup | At-most-once writes |

When starting a new verification, check this catalog first. Build on existing models rather than starting from scratch.
