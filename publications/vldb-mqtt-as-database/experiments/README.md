# VLDB Paper — Experimental Setup v1

Benchmark experiments for §6 (Evaluation) of the MQDB VLDB paper.

## Paper requirements

The evaluation section (§6) defines six experiment groups:

| Group | Section | Experiments | Baseline systems |
|-------|---------|-------------|------------------|
| CRUD throughput & latency | §6.2 | insert, get, update, delete, list, mixed | MQDB Agent, MQDB Cluster, Mosquitto+PG, Mosquitto+Redis |
| Pub/sub throughput | §6.3 | QoS 0, QoS 1, cross-node | MQDB Agent, MQDB Cluster, standalone Mosquitto |
| Cluster scalability | §6.4 | scaling, routing overhead, replication overhead, mesh topologies | MQDB Agent vs MQDB Cluster (3-node) |
| Operational complexity | §6.5 | qualitative comparison table | All four baselines |
| Published benchmark comparison | §6.6 | discussion (IoTDB context) | N/A |

## Measurement methodology

- All experiments run in **triplicate** (3 runs minimum)
- Report **mean and standard deviation**
- Throughput: operations per second (ops/s)
- Latency: end-to-end P50, P95, P99 (milliseconds)
- Fresh MQDB agent restart with clean DB before every individual run (prevents spillover)
- `DELETE FROM records` between baseline runs
- Sequential mode (concurrency=1) for CRUD comparison — the defensible metric

## Experiment phases

### Phase 1: Sequential CRUD (§6.2)

Extends `../bench/run_comparison.sh`. Single-node MQDB Agent vs Mosquitto+PostgreSQL.

| Parameter | Value |
|-----------|-------|
| Operations | 10,000 |
| Concurrency | 1 (sequential) |
| Record size | 5 fields × 100 bytes ≈ 600 bytes |
| Triplicates | 3 |
| Latency tracking | enabled (P50/P95/P99) |
| Operations | insert, get, update, delete, list, mixed |

Status: insert/get/update/delete/list working locally (1,000 ops). Need to add mixed, enable latency, increase to 10,000 ops.

### Phase 2: Pub/Sub throughput (§6.3)

New script. Measures message throughput on non-DB topics to verify the database subsystem doesn't degrade broker performance.

| Parameter | Value |
|-----------|-------|
| Duration | 10 seconds |
| Payload | 64 bytes |
| Publishers | 1 |
| Subscribers | 1 |
| QoS levels | 0 and 1 |
| Triplicates | 3 |

Baseline: standalone Mosquitto (same Docker container as CRUD baseline).

### Phase 3: Cluster scalability (§6.4)

New script. Uses `mqdb dev start-cluster` for 3-node local cluster with QUIC transport.

Sub-experiments:
1. **Throughput scaling** — aggregate insert throughput across 1-3 nodes
2. **Routing overhead** — client on primary node vs non-primary node
3. **Replication overhead** — single-node agent vs 3-node cluster (RF=2)
4. **Mesh topologies** — partial, upper, full mesh

### Phase 4: Redis baseline

Requires building a Redis bridge (analogous to the PG bridge in `../bench/`). Adds Mosquitto+Redis as the fourth baseline for §6.2.

## Directory structure

```
experiments/
├── README.md           # this file
├── DIARY.md            # session log, decisions, results
├── scripts/            # benchmark orchestration scripts (future)
└── results/            # per-run JSON output organized by phase (future)
```

Benchmark harness code lives in `../bench/` (bridge binary, Docker Compose, schema).

## Tools

All CRUD benchmarks use `mqdb bench db`. All pub/sub benchmarks use `mqdb bench pubsub`. Both produce JSON output via `--format json`.

## Local baseline results (macOS, 1,000 ops, sequential)

Reference numbers from development machine. AWS results will replace these for the paper.

| Operation | MQDB Agent (ops/s) | Mosquitto+PG (ops/s) | Ratio |
|-----------|--------------------|-----------------------|-------|
| insert    | 3,757              | 686                   | 5.5×  |
| get       | 5,455              | 815                   | 6.7×  |
| update    | 4,090              | 674                   | 6.1×  |
| delete    | 4,422              | 616                   | 7.2×  |
| list      | 2,091              | 549                   | 3.8×  |
