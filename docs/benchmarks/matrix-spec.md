# Complete Benchmark Matrix Specification

This document defines the complete benchmark matrix for measuring MQDB performance across four configurations: standalone Agent mode and three cluster topologies (Partial, Upper, and Full mesh). Every experiment is run three times (triplicates) so that mean throughput and standard deviation can be reported. Recorded results live in [matrix-results.md](matrix-results.md); a companion root-cause analysis of bridge overhead lives in [issue-11.15-bridge-overhead.md](issue-11.15-bridge-overhead.md).

---

## CRITICAL: Methodology Requirements

### Issue #1: Database State Accumulation (MUST READ)

Async benchmarks create persistent records that affect subsequent tests:

| Test Sequence | Records Created | Cumulative |
|--------------|-----------------|------------|
| DI (async insert 60s) | ~60,000 | 60,000 |
| DG (async get, seeds 10K) | +10,000 | 70,000 |
| DU (async update, seeds 10K) | +10,000 | 80,000 |
| DL (async list) | 0 (reads all) | 80,000 |

**Impact:** Async list slows from 3868 ops/s (empty DB) to 69 ops/s (80K records) - a **56x degradation**.

### Issue #2: Straggler Responses

After async benchmarks, unprocessed responses remain in flight:
- Published: 3046 operations
- Completed: 862 (28%)
- Timed out: 2184 (72%)

### Isolation Requirements

| Test Type | Isolation | Method |
|-----------|-----------|--------|
| PubSub (A, B) | Per phase | Messages don't persist |
| Sync DB (C) | Per operation | Each uses unique entity |
| Async Insert (DI) | **Fresh DB** | Restart with `--clean` |
| Async Get (DG) | **Fresh DB** | Restart with `--clean` + seed |
| Async Update (DU) | **Fresh DB** | Restart with `--clean` + seed |
| Async List (DL) | **EXCLUDED** | See below |
| Async Delete (DD) | **Fresh DB** | Restart with `--clean` + seed |

### Async List Exclusion

**Decision:** Exclude async list (DL) from the matrix.

**Reason:** List performance depends on database size, not broker capability. Results are not comparable across runs.

**Alternative:** Sync list (CL) provides reliable measurements.

### Cooldown Protocol

After EACH async benchmark:
1. Wait **10 seconds** for stragglers
2. Verify broker health via `$DB/_health`
3. Record timeout warnings if any

---

## Configurations

| Config | Command | Bridge Distribution |
|--------|---------|---------------------|
| Agent | `mqdb agent start --db /tmp/mqdb-bench-agent --bind 127.0.0.1:1883 --passwd passwd.txt --admin-users bench` | N/A |
| Partial | `mqdb dev start-cluster --nodes 3 --clean --topology partial` | N1: 0, N2: 1, N3: 1 |
| Upper | `mqdb dev start-cluster --nodes 3 --clean --topology upper` | N1: 2, N2: 1, N3: 0 |
| Full | `mqdb dev start-cluster --nodes 3 --clean --topology full` | N1: 2, N2: 2, N3: 2 |

## Replication Requirements

**All experiments must be run 3 times (triplicates)** to calculate:
- Mean throughput
- Standard deviation

## Part A: Same-Node PubSub

Publisher and subscriber on the same node. Measures local broker performance.

| ID | Config | Port | Bridges | Command |
|----|--------|------|---------|---------|
| A1 | Agent | 1883 | 0 | `mqdb bench pubsub --broker 127.0.0.1:1883 --duration 10` |
| A2 | Partial | 1883 | 0 | `mqdb bench pubsub --broker 127.0.0.1:1883 --duration 10` |
| A3 | Partial | 1884 | 1 | `mqdb bench pubsub --broker 127.0.0.1:1884 --duration 10` |
| A4 | Partial | 1885 | 1 | `mqdb bench pubsub --broker 127.0.0.1:1885 --duration 10` |
| A5 | Upper | 1883 | 2 | `mqdb bench pubsub --broker 127.0.0.1:1883 --duration 10` |
| A6 | Upper | 1884 | 1 | `mqdb bench pubsub --broker 127.0.0.1:1884 --duration 10` |
| A7 | Upper | 1885 | 0 | `mqdb bench pubsub --broker 127.0.0.1:1885 --duration 10` |
| A8 | Full | 1883 | 2 | `mqdb bench pubsub --broker 127.0.0.1:1883 --duration 10` |
| A9 | Full | 1884 | 2 | `mqdb bench pubsub --broker 127.0.0.1:1884 --duration 10` |
| A10 | Full | 1885 | 2 | `mqdb bench pubsub --broker 127.0.0.1:1885 --duration 10` |

**Total: 10 experiments × 3 runs = 30 runs**

## Part B: Cross-Node PubSub

Publisher on one node, subscriber on another. Measures bridge forwarding performance.

| ID | Config | Pub | Sub | Command |
|----|--------|-----|-----|---------|
| B1 | Partial | 1883 | 1884 | `mqdb bench pubsub --pub-broker 127.0.0.1:1883 --sub-broker 127.0.0.1:1884 --duration 10` |
| B2 | Partial | 1883 | 1885 | `mqdb bench pubsub --pub-broker 127.0.0.1:1883 --sub-broker 127.0.0.1:1885 --duration 10` |
| B3 | Partial | 1884 | 1883 | `mqdb bench pubsub --pub-broker 127.0.0.1:1884 --sub-broker 127.0.0.1:1883 --duration 10` |
| B4 | Partial | 1884 | 1885 | `mqdb bench pubsub --pub-broker 127.0.0.1:1884 --sub-broker 127.0.0.1:1885 --duration 10` |
| B5 | Partial | 1885 | 1883 | `mqdb bench pubsub --pub-broker 127.0.0.1:1885 --sub-broker 127.0.0.1:1883 --duration 10` |
| B6 | Partial | 1885 | 1884 | `mqdb bench pubsub --pub-broker 127.0.0.1:1885 --sub-broker 127.0.0.1:1884 --duration 10` |
| B7 | Upper | 1883 | 1884 | `mqdb bench pubsub --pub-broker 127.0.0.1:1883 --sub-broker 127.0.0.1:1884 --duration 10` |
| B8 | Upper | 1883 | 1885 | `mqdb bench pubsub --pub-broker 127.0.0.1:1883 --sub-broker 127.0.0.1:1885 --duration 10` |
| B9 | Upper | 1884 | 1883 | `mqdb bench pubsub --pub-broker 127.0.0.1:1884 --sub-broker 127.0.0.1:1883 --duration 10` |
| B10 | Upper | 1884 | 1885 | `mqdb bench pubsub --pub-broker 127.0.0.1:1884 --sub-broker 127.0.0.1:1885 --duration 10` |
| B11 | Upper | 1885 | 1883 | `mqdb bench pubsub --pub-broker 127.0.0.1:1885 --sub-broker 127.0.0.1:1883 --duration 10` |
| B12 | Upper | 1885 | 1884 | `mqdb bench pubsub --pub-broker 127.0.0.1:1885 --sub-broker 127.0.0.1:1884 --duration 10` |
| B13 | Full | 1883 | 1884 | `mqdb bench pubsub --pub-broker 127.0.0.1:1883 --sub-broker 127.0.0.1:1884 --duration 10` |
| B14 | Full | 1883 | 1885 | `mqdb bench pubsub --pub-broker 127.0.0.1:1883 --sub-broker 127.0.0.1:1885 --duration 10` |
| B15 | Full | 1884 | 1883 | `mqdb bench pubsub --pub-broker 127.0.0.1:1884 --sub-broker 127.0.0.1:1883 --duration 10` |
| B16 | Full | 1884 | 1885 | `mqdb bench pubsub --pub-broker 127.0.0.1:1884 --sub-broker 127.0.0.1:1885 --duration 10` |
| B17 | Full | 1885 | 1883 | `mqdb bench pubsub --pub-broker 127.0.0.1:1885 --sub-broker 127.0.0.1:1883 --duration 10` |
| B18 | Full | 1885 | 1884 | `mqdb bench pubsub --pub-broker 127.0.0.1:1885 --sub-broker 127.0.0.1:1884 --duration 10` |

**Total: 18 experiments × 3 runs = 54 runs**

## Part C: Sync DB Operations

Synchronous DB operations where publisher waits for response. Tests all CRUD operations.

### Operations
- **insert**: Create 1000 new records
- **get**: Read 1000 records (requires --seed 1000)
- **update**: Modify 1000 records (requires --seed 1000)
- **list**: List operations with 100 records (requires --seed 100)
- **delete**: Remove 1000 records (requires --seed 1000, --cleanup)

### Test Matrix

For each operation, test all configurations:

| ID | Config | Port | Bridges |
|----|--------|------|---------|
| C*-1 | Agent | 1883 | 0 |
| C*-2 | Partial | 1883 | 0 |
| C*-3 | Partial | 1884 | 1 |
| C*-4 | Partial | 1885 | 1 |
| C*-5 | Upper | 1883 | 2 |
| C*-6 | Upper | 1884 | 1 |
| C*-7 | Upper | 1885 | 0 |
| C*-8 | Full | 1883 | 2 |
| C*-9 | Full | 1884 | 2 |
| C*-10 | Full | 1885 | 2 |

### Commands by Operation

**Insert (CI-1 through CI-10):**
```bash
mqdb bench db --broker 127.0.0.1:$PORT --op insert --operations 1000 --no-latency
```

**Get (CG-1 through CG-10):**
```bash
mqdb bench db --broker 127.0.0.1:$PORT --op get --operations 1000 --seed 1000 --no-latency
```

**Update (CU-1 through CU-10):**
```bash
mqdb bench db --broker 127.0.0.1:$PORT --op update --operations 1000 --seed 1000 --no-latency
```

**List (CL-1 through CL-10):**
```bash
mqdb bench db --broker 127.0.0.1:$PORT --op list --operations 100 --seed 100 --no-latency
```

**Delete (CD-1 through CD-10):**
```bash
mqdb bench db --broker 127.0.0.1:$PORT --op delete --operations 1000 --seed 1000 --no-latency --cleanup
```

**Total: 5 operations × 10 configurations × 3 runs = 150 runs**

## Part D: Async DB Operations

Asynchronous/pipelined DB operations that measure sustained throughput until saturation. Uses QoS 1 for flow control.

### Commands by Operation

**Async Insert (DI-1 through DI-10):**
```bash
mqdb bench db --broker 127.0.0.1:$PORT --op insert --async --duration 60
```

**Async Get (DG-1 through DG-10):**
```bash
mqdb bench db --broker 127.0.0.1:$PORT --op get --async --duration 60 --seed 10000
```

**Async Update (DU-1 through DU-10):**
```bash
mqdb bench db --broker 127.0.0.1:$PORT --op update --async --duration 60 --seed 10000
```

**Async Delete (DD-1 through DD-10):**
```bash
mqdb bench db --broker 127.0.0.1:$PORT --op delete --async --duration 60 --seed 10000 --cleanup
```

**~~Async List (DL-1 through DL-10):~~ EXCLUDED**
- Reason: Performance depends on database size, not broker capability
- See "CRITICAL: Methodology Requirements" section

**Total: 4 operations × 10 configurations × 3 runs = 120 runs**

## Summary

| Part | Description | Experiments | Runs (×3) |
|------|-------------|-------------|-----------|
| A | Same-Node PubSub | 10 | 30 |
| B | Cross-Node PubSub | 18 | 54 |
| C | Sync DB Operations | 50 | 150 |
| D | Async DB Operations (**4 ops, no list**) | **40** | **120** |
| **Total** | | **118** | **354** |

## Execution Order (REVISED)

**Key requirements:**
1. Restart broker/cluster between async test categories
2. 10-second cooldown after each async benchmark
3. Skip async list (DL) entirely
4. Each run must start from clean database state

### Per-Configuration Sequence

For EACH configuration (Agent, Partial, Upper, Full):

```
PHASE A: PUBSUB (no isolation needed)
1. Start broker/cluster (fresh)
2. Wait 5s for initialization
3. Run same-node pubsub ×3
4. Run cross-node pubsub ×3 (cluster only)

PHASE B: SYNC DB (self-isolating via entity names)
5. Run CI (insert) ×3
6. Run CG (get) ×3
7. Run CU (update) ×3
8. Run CL (list) ×3
9. Run CD (delete) ×3

PHASE C: ASYNC DB (require fresh DB each)
For each of DI, DG, DU, DD:
  10. RESTART broker/cluster (--clean)
  11. Wait 5s for initialization
  12. Run benchmark ×3
  13. Wait 10s cooldown
14. Kill broker/cluster
```

### Phase 1: Agent Mode

```bash
# Start fresh
rm -rf /tmp/mqdb-bench-agent
mqdb agent start --db /tmp/mqdb-bench-agent --bind 127.0.0.1:1883 --passwd passwd.txt --admin-users bench &
sleep 5

# A: PubSub
A1 ×3

# B: Sync DB (entity-isolated)
CI-1 ×3, CG-1 ×3, CU-1 ×3, CL-1 ×3, CD-1 ×3

# C: Async DB (restart between each)
pkill -f "mqdb agent" && rm -rf /tmp/mqdb-bench-agent
mqdb agent start ... & sleep 5
DI-1 ×3
sleep 10  # cooldown

pkill -f "mqdb agent" && rm -rf /tmp/mqdb-bench-agent
mqdb agent start ... & sleep 5
DG-1 ×3
sleep 10

pkill -f "mqdb agent" && rm -rf /tmp/mqdb-bench-agent
mqdb agent start ... & sleep 5
DU-1 ×3
sleep 10

pkill -f "mqdb agent" && rm -rf /tmp/mqdb-bench-agent
mqdb agent start ... & sleep 5
DD-1 ×3
sleep 10

pkill -f "mqdb agent"
```

### Phases 2-4: Cluster Modes

Same pattern using:
```bash
mqdb dev start-cluster --nodes 3 --clean --topology {partial|upper|full}
mqdb dev kill
```

## Output Format

Results should be recorded with:
- Experiment ID (e.g., DI-1)
- Run number (1, 2, 3)
- Throughput (ops/s or msg/s)
- Duration
- Published count (async only)
- Successful count (async only)
- Timeout/errors if any
- Saturation point (async only)

Final analysis should include:
- Mean throughput per experiment
- Standard deviation per experiment
- Comparison by bridge count
- Comparison by topology

## Known Issues

1. **Async list excluded** - Performance depends on DB size (56x degradation with 80K records)
2. **Cross-node pubsub ~1000 msg/s limit** - QoS 0 saturation, expected behavior
3. **Full mesh B13, B14 lower throughput** - Some full-mesh routes show intermittent degradation (B13 mean 886, B14 mean 936 msg/s; single-run dips to 655 and 803). See "Notable Anomalies" in matrix-results.md

---

## Investigation: Issue 11.15 - Bridge Overhead Root Cause Analysis

Moved to [issue-11.15-bridge-overhead.md](issue-11.15-bridge-overhead.md).
