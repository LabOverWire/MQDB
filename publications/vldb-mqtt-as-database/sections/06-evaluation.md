# 6. Evaluation

We evaluate MQDB along three dimensions: CRUD throughput and latency (does the unified architecture impose a performance penalty?), pub/sub message rates (does the database subsystem degrade messaging performance?), and operational complexity (does the single-binary architecture reduce deployment burden?). We compare against three baselines representing the conventional separated architecture, and we include a second MQDB configuration that swaps the persistent storage engine for an in-memory map so that MQDB is evaluated on the same storage footing as the in-memory baseline.

## 6.1 Experimental Setup

**Hardware.** We run every experiment on two AWS EC2 instance types: c7g.xlarge (4 vCPU ARM Graviton 3, 8 GB RAM) and c7g.4xlarge (16 vCPU ARM Graviton 3, 32 GB RAM), both backed by gp3 EBS. The smaller instance represents a compute-optimized edge or gateway deployment; the larger instance represents a mid-tier data-aggregation node on the same ISA. Reporting both sizes isolates scaling behaviour on a fixed CPU microarchitecture: any gap between the two reflects concurrency and memory-hierarchy effects rather than ISA differences.

**Data provenance.** The authoritative results for this section are the JSON files under `bench/results-aws/phase1/`, produced by the procedure documented in `bench/AWS-PROCEDURE.md`. Every result file embeds a `provenance` object recording the EC2 instance id, region, kernel, CPU model, MQDB git commit, and the UTC timestamp of the run. A top-level `bench/results-aws/phase1/run-manifest.json` names the run. Any number cited below that does not correspond to a file listed in that manifest is flagged as pending re-execution under the SOP. The tables in this section reflect the current best available data; rows marked `<!-- PENDING AWS RUN -->` are not yet verified by a logged AWS run.

**Software versions.** MQDB v0.7.2 (Rust, compiled with `--release` for aarch64-linux). Mosquitto 2.0 (Docker, `eclipse-mosquitto:2`) as the baseline MQTT broker. PostgreSQL 17 (Docker, `postgres:17`, tmpfs-backed) as the baseline relational database. Redis 7 (Docker, `redis:7-alpine`) as the baseline key-value store. The REST+PG server uses axum 0.8 (Rust) with tokio-postgres and the same deadpool-postgres connection pool as the MQTT bridge.

**Durability semantics.** The five configurations span a range of durability commitments, and we report them explicitly because the CRUD throughput comparison is otherwise ambiguous. MQDB (fjall) runs with the benchmark default `--durability periodic --durability-ms 10`: writes are acknowledged once committed to the fjall WAL in memory, with `fsync` coalesced on a 10ms timer — bounding crash-window loss to at most 10ms of acknowledged writes. This is the documented production default for MQDB agent deployments and matches the durability posture of OLTP configurations tuned for throughput (e.g. PostgreSQL with `synchronous_commit=off`). MQDB (memory) has no durability by design. The three external baselines carry no durability in this measurement either: PostgreSQL's data directory is on tmpfs (the WAL is in RAM and is lost on crash), Redis uses the default `redis:7-alpine` image configuration (no AOF, no RDB snapshot during the measurement window), and REST+PG shares the tmpfs PostgreSQL instance. Consequently, of the five configurations, only MQDB (fjall) writes durably; the baselines compete at in-memory speed. A stricter-durability run (`--durability immediate`, per-write fsync) can be reproduced from the SOP by flipping the flag; we report periodic 10ms because it is the documented production default.

**Atomicity primitive and what the client observes.** The CRUD operations exercised throughout §6 are atomic per request, and we state the mechanism explicitly because the paper's thesis rests on a distinction the MQTT protocol alone does not make. The MQTT 5.0 `PUBACK` frame acknowledges only that the broker has received and durably queued a publish; it is not a commit signal. A system that treated `PUBACK` as the commit signal — acknowledging the client and writing to storage asynchronously — would deliver at-least-once message delivery, not atomic CRUD. MQDB's CRUD path instead uses MQTT 5.0's Request/Response pattern: the client publishes to `$DB/{entity}/{op}` with the `Response Topic` property set, and the broker's reply on that Response Topic is the semantic success signal. The reply is published only after the storage-layer commit returns. Internally, every CRUD operation accumulates the record bytes, every touched secondary-index entry, every constraint-reservation entry, and the change-event outbox row into a single fjall `WriteBatch` and commits once (see `crates/mqdb-agent/src/database/crud.rs:87–113`). fjall's LSM `commit()` is atomic and crash-consistent at the storage layer; until it returns `Ok`, neither the Response Topic publish nor the change-event fan-out to subscribers fires. A standard MQTT 5.0 client — Paho, mqttx, `mosquitto_rr` — therefore observes atomic CRUD by using the Request/Response pattern alone: no client-side transaction logic, no broker-specific protocol extension, no compound request. The one remaining uncertainty — a Response Topic reply lost in transit after a successful commit — is the standard network-timeout reconciliation problem and is addressed by honoring client-supplied identifiers for idempotent retry. Every experiment in §6.2–§6.4 treats the Response Topic reply (or, for REST+PG, the HTTP response body) as the success signal; `PUBACK` is never used as a proxy for commit.

**Five configurations tested:**

1. **MQDB Agent (fjall)** — single process, embedded fjall LSM storage engine on gp3 EBS, password authentication. This is the realistic production configuration: writes are durable across process restart.
2. **Mosquitto + PostgreSQL** — Mosquitto broker, a Rust bridge application subscribing to database operation topics under `$DB/`, and PostgreSQL storing records as JSONB in a single `records(id TEXT, entity TEXT, data JSONB)` table with an entity index.
3. **Mosquitto + Redis** — Mosquitto broker, a Rust bridge application, and Redis storing records as JSON strings keyed by `record:{entity}:{id}` with per-entity index sets.
4. **REST + PostgreSQL** — an axum HTTP server exposing CRUD endpoints (`POST /db/{entity}`, `GET /db/{entity}/{id}`, etc.), backed by the same PostgreSQL instance and schema as configuration 2, with the same connection pool (deadpool-postgres, pool size 4). This configuration eliminates the MQTT protocol entirely, isolating the database access cost from the messaging overhead.
5. **MQDB Agent (memory)** — identical to configuration 1 but launched with `--memory-backend`, which swaps the fjall LSM engine for an in-memory `BTreeMap` implementing the same `StorageBackend` trait. Every other MQDB component (topic parser, request-response handler, authentication, vault, ownership, subscription engine) is unchanged. This configuration is storage-symmetric with the Redis and tmpfs-backed PostgreSQL baselines: no configuration in §6.2/§6.3 writes to disk during the measurement window. The fjall-vs-memory contrast bounds the cost MQDB pays for durability.

**Storage symmetry and the tmpfs decision.** We intentionally strip disk I/O from the three external baselines so that the comparison isolates *protocol and architecture* cost from *storage* cost. PostgreSQL on tmpfs and the default Redis image both run entirely in memory during the measurement window; MQDB (fjall), by contrast, writes to gp3 EBS — a network-attached block device, not local storage — and pays the cost of real durable writes. Configuration 5 closes the gap in the other direction: MQDB (memory) runs on an in-memory `BTreeMap` implementing the same `StorageBackend` trait, matching the baselines' in-memory footing. This is a deliberate methodological choice — if MQDB (fjall) still outperforms an in-memory baseline, the advantage cannot be explained by disk I/O that the baseline paid and MQDB avoided. The five configurations are produced by a single `bench/run_phase1_agent.sh` sweep on the same instance and share the same `provenance` object; only the MQDB storage backend differs between configurations 1 and 5.

**Baseline bridge architecture.** The bridge is a minimum viable implementation: it connects to the Mosquitto broker as an MQTT 5.0 client, subscribes to `$DB/+/create`, `$DB/+/+`, `$DB/+/+/update`, `$DB/+/+/delete`, and `$DB/+/list`, parses each incoming publish into a database operation, executes the operation against PostgreSQL (via `deadpool-postgres`, pool size 4) or Redis (via the `redis` crate), and publishes the result back on the MQTT 5.0 response topic. The bridge does not batch operations or cache results. PostgreSQL connections use a pool (size 4); Redis uses a single multiplexed connection. This represents the simplest correct implementation of the separated architecture — the baseline that every real deployment must at least match.

**Measurement methodology.** Each experimental cell is run five times (N=5). We report the mean and population standard deviation (σ, dividing by N=5). MQDB is restarted with a fresh database before every individual run (not just between operation types) to prevent fjall compaction state from contaminating subsequent measurements. For baseline systems, the database is cleared between runs via `DELETE FROM records` (PostgreSQL) or `FLUSHDB` (Redis). All MQTT-based configurations use the same benchmark tool (`mqdb bench db`), which publishes MQTT 5.0 request-response messages and measures round-trip time, ensuring identical client-side overhead. The REST+PG configuration uses a dedicated HTTP benchmark client that issues equivalent operations over HTTP and measures the same latency percentiles, generating identical JSON records with the same field count and size.

**Workload parameters.** CRUD records are five-field JSON documents with 100-byte string values per field (~600 bytes per record). Each single-operation CRUD benchmark (§6.2 Tables 4a and 4b) executes 10,000 operations at concurrency 1 (sequential); read, update, delete, and list benchmarks pre-populate 10,000 records via insert before measurement begins. The mixed-workload experiment (§6.2 Table 4c) additionally sweeps concurrency levels {1, 8, 32, 128} to measure behaviour under queueing pressure on the larger instance. Pub/sub benchmarks (§6.3) use 64-byte payloads with one publisher and one subscriber for 10 seconds. Sequential concurrency=1 is deliberate for the per-operation tables: it measures per-operation cost in isolation, avoiding confounds from connection pooling, thread scheduling, and in-process concurrency advantages that would differ across configurations. §6.4 introduces three architectural-workload experiments (change-feed delivery, unique-constraint contention, cascade delete) whose workload shapes are described in their respective subsections.

## 6.2 CRUD Throughput and Latency

We measure six workloads: the five individual CRUD operations (insert, get, update, delete, list) and a mixed workload that cycles through all five operations in round-robin order (20% each). Tables 4a and 4b report per-instance results for the single-operation workloads at concurrency 1. Table 4c reports the mixed workload under a concurrency sweep on the larger instance, which exposes queueing effects that the sequential setting cannot reveal.

Each cell in Tables 4a and 4b reports throughput (ops/s) on the first line and CRUD latency percentiles p50 / p95 / p99 (μs) on the second line, both as mean ± stddev over N=5 runs.

**Table 4a.** CRUD throughput and latency on c7g.xlarge. N=5, 10,000 operations per run.

| Operation | MQDB (fjall) | Mosquitto + PG | Mosquitto + Redis | REST + PG | MQDB (memory) |
|---|---|---|---|---|---|
| insert | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ |
| get | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ |
| update | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ |
| delete | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ |
| list | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ |

**Table 4b.** Same measurements on c7g.4xlarge. N=5, 10,000 operations per run.

| Operation | MQDB (fjall) | Mosquitto + PG | Mosquitto + Redis | REST + PG | MQDB (memory) |
|---|---|---|---|---|---|
| insert | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ |
| get | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ |
| update | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ |
| delete | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ |
| list | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ |

**Table 4c.** Mixed workload (20% insert / 20% get / 20% update / 20% delete / 20% list) on c7g.4xlarge under a concurrency sweep. 10,000 operations per run, N=5. Each cell shows throughput (ops/s) and p99 latency (μs). REST+PG is omitted because the current REST harness drives requests sequentially; its concurrency scaling is deferred to future work.

| Concurrency | MQDB (fjall) | Mosquitto + PG | Mosquitto + Redis | MQDB (memory) |
|---|---|---|---|---|
| 1 | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ |
| 8 | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ |
| 32 | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ |
| 128 | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ | _PENDING AWS RUN_<br>_PENDING AWS RUN_ |

**Analysis.** Three architectural claims are tested across Tables 4a–4c. First, the five configurations differ in storage footing: MQDB (fjall) writes durably to gp3 EBS, while the MQTT-based baselines and REST+PG run on tmpfs and the `redis:7-alpine` default (no AOF/RDB) — only MQDB (fjall) pays a durable-write cost during the measurement window. Second, the MQTT-based baselines incur one additional inter-process hop (client → Mosquitto → bridge → DB → bridge → Mosquitto → client) that MQDB avoids (client → in-process broker/DB → client); REST+PG elides the MQTT bridge but still pays a TCP round-trip to PostgreSQL. Third, the MQDB (memory) column isolates the cost of the persistent storage engine from every other architectural layer: any gap between MQDB (fjall) and MQDB (memory) is attributable to fjall alone, because every layer above the `StorageBackend` trait is identical between the two runs. Tables 4a/4b report the per-instance single-operation cost; Table 4c reports how the mixed workload scales under queueing on the larger instance. <!-- PENDING AWS ANALYSIS: per-baseline speedups, tail-latency ratios, storage-engine decomposition, and concurrency-scaling inflection points will be written once the SOP run has populated the tables. -->

## 6.3 Pub/Sub Throughput

We measure MQDB's pub/sub performance on a non-database topic (`bench/test`) to verify that the database subsystem does not degrade the broker's core messaging function. Because the topic lies outside `$DB/`, neither the CRUD dispatcher nor the storage engine is invoked during these measurements; the purpose of Tables 5a and 5b is to establish that MQDB's broker sits within a constant factor of a production-grade baseline (Mosquitto) on its core messaging function, and that the embedded storage engine imposes no indirect cost on non-database topics.

**Table 5a.** Pub/sub throughput (msg/s) on c7g.xlarge. One publisher, one subscriber, 64-byte payloads, 10-second duration. Mean ± stddev over N=5 runs.

| QoS | MQDB (fjall) | Mosquitto | MQDB (memory) |
|---|---|---|---|
| 0 | _PENDING AWS RUN_ | _PENDING AWS RUN_ | _PENDING AWS RUN_ |
| 1 | _PENDING AWS RUN_ | _PENDING AWS RUN_ | _PENDING AWS RUN_ |

**Table 5b.** Same measurement on c7g.4xlarge.

| QoS | MQDB (fjall) | Mosquitto | MQDB (memory) |
|---|---|---|---|
| 0 | _PENDING AWS RUN_ | _PENDING AWS RUN_ | _PENDING AWS RUN_ |
| 1 | _PENDING AWS RUN_ | _PENDING AWS RUN_ | _PENDING AWS RUN_ |

**Analysis.** The two QoS levels exercise different broker paths. QoS 0 is fire-and-forget: the broker parses the publish frame, matches it against the subscription table, and fans it out to subscribers with no acknowledgment. QoS 1 adds per-session inflight state, `PUBACK` emission, and at-least-once delivery semantics. A gap between MQDB (fjall) and MQDB (memory) on a non-database topic would indicate shared state between the broker and storage subsystems; matching numbers confirm the separation is complete. <!-- PENDING AWS ANALYSIS: the per-QoS speedup or deficit, per-instance scaling, and fjall-vs-memory delta will be written once the SOP run has populated the tables. -->

## 6.4 Architectural Workloads

Tables 4–5 measure raw CRUD and messaging throughput — the cost of running a single operation on an unloaded system. The paper's central claim, however, is not that MQDB is faster at CRUD: it is that MQTT's topic hierarchy and subscription semantics are sufficient to implement the primitives a distributed database needs. This section exercises three such primitives — change-feed delivery, unique-constraint contention, and cascade delete — each of which stresses a part of the architecture that the single-operation microbenchmarks cannot reach.

### 6.4.1 Change-Feed Delivery Latency

Every write emits a change event on `$DB/{entity}/events/{id}`. A subscriber to this wildcard topic receives a stream of creates, updates, and deletes; the question this workload answers is *how quickly* a subscriber sees a write after the writer's request has returned. The MQDB path is push-only: the outbox row is committed inside the same fjall `WriteBatch` as the record insert, and the dispatcher publishes the event immediately after commit. The Mosquitto + PG and Mosquitto + Redis baselines publish an event from the bridge after a successful insert. REST+PG has no native push channel — we expose it via a long-poll endpoint `GET /db/{entity}/since/{cursor}` backed by PostgreSQL `LISTEN/NOTIFY`, which wakes within milliseconds of a commit but adds an HTTP round-trip per delivered batch.

The workload drives 500 writes/s for 30 seconds on a single entity with one subscriber. Delivery latency is measured as `(event_received_ns − write_sent_ns)`, keyed by record id.

**Table 6.** Change-feed delivery latency (ms, end-to-end). Mean ± stddev over N=5 runs on c7g.4xlarge.

| Configuration | p50 | p95 | p99 |
|---|---|---|---|
| MQDB (fjall) push | _PENDING AWS RUN_ | _PENDING AWS RUN_ | _PENDING AWS RUN_ |
| MQDB (memory) push | _PENDING AWS RUN_ | _PENDING AWS RUN_ | _PENDING AWS RUN_ |
| Mosquitto + PG bridge-emitted | _PENDING AWS RUN_ | _PENDING AWS RUN_ | _PENDING AWS RUN_ |
| Mosquitto + Redis bridge-emitted | _PENDING AWS RUN_ | _PENDING AWS RUN_ | _PENDING AWS RUN_ |
| REST + PG long-poll (LISTEN/NOTIFY) | _PENDING AWS RUN_ | _PENDING AWS RUN_ | _PENDING AWS RUN_ |

**Analysis.** MQDB's change-feed is a first-class consequence of the architecture: the same broker that handled the write fans the event out to subscribers, and the outbox row is committed in the same storage-engine batch as the record it describes. The bridge-emitted variants require the bridge process to serialize the event and publish it back through Mosquitto, adding one inter-process hop per event and a structural risk of bridge-vs-DB drift if the bridge crashes between commit and publish. The REST+PG long-poll variant adds both an HTTP round-trip and the `LISTEN/NOTIFY` wake-up latency. <!-- PENDING AWS ANALYSIS: quantitative latency gaps and per-configuration tail behaviour will be written once the SOP run has populated the table. -->

### 6.4.2 Unique-Constraint Contention

MQDB enforces unique constraints via the two-phase reservation protocol described in §4.6: a writer first reserves the unique field value in a partition-local sidecar entity, then performs the record write inside the same `WriteBatch`. Concurrent writers racing for the same value all reach the reservation step, but only one's commit wins; the rest see the conflict and abort cleanly. PostgreSQL enforces uniqueness via a B-tree unique index; the baseline bridge surfaces the index-violation error to the client. Redis has no native unique constraint on JSON documents — a bridge can approximate it with a `SETNX` sentinel key per unique value, but this tests Redis's CAS primitive rather than an enforced unique index, so we omit Redis from the direct comparison and address the gap in §6.5.

The workload adds a unique constraint on one field of a test entity, then spawns K concurrent clients; each client attempts 100 inserts on the same unique-field value, so exactly one insert per K-attempt batch succeeds and the remaining (K × 100) − 1 attempts must be detected as conflicts.

**Table 7.** Unique-constraint contention. K concurrent clients × 100 inserts on the same unique field. Each cell shows success count / conflict-detection p95 latency (μs) / total wall time (ms). Mean over N=5 runs on c7g.4xlarge.

| K | MQDB (fjall) | Mosquitto + PG | REST + PG |
|---|---|---|---|
| 4 | _PENDING AWS RUN_ | _PENDING AWS RUN_ | _PENDING AWS RUN_ |
| 16 | _PENDING AWS RUN_ | _PENDING AWS RUN_ | _PENDING AWS RUN_ |
| 64 | _PENDING AWS RUN_ | _PENDING AWS RUN_ | _PENDING AWS RUN_ |

**Analysis.** MQDB's reservation entity is partitioned by the unique-field hash, so every concurrent writer targeting the same value contends on a single partition's log; the winner of the reservation step is the only one whose record write commits. PostgreSQL's unique B-tree index serializes conflicting inserts in the storage engine. The cost the MQDB approach pays, relative to an in-engine unique index, is an extra sidecar write per successful insert; the cost it avoids, relative to a naïve bridge-implemented check-then-write, is the TOCTOU race window that bridge-side checks open. <!-- PENDING AWS ANALYSIS: success-rate stability, conflict-path latency inflation under increasing K, and throughput asymptote will be written once the SOP run has populated the table. -->

### 6.4.3 Cascade Delete

A parent-child foreign key with `on_delete: cascade` means deleting the parent also deletes every child that references it. In MQDB, cascade is synchronous and bounded: the parent delete's `WriteBatch` walks the reverse-index for the FK, enqueues every child delete plus its change-event into the same batch, and commits once (§4.6; `MAX_CASCADE_DEPTH=16`). PostgreSQL's `ON DELETE CASCADE` is enforced by the storage engine. The Mosquitto + Redis bridge implements cascade in application code: after deleting the parent, it scans the child set and issues per-child deletes, each as a separate Redis command. The REST+PG path delegates to PostgreSQL's cascade.

The workload inserts one parent and K children, then deletes the parent. `RT` is the round-trip from the parent-delete request to the acknowledged deletion of the last child; `events p95` is the 95th-percentile time from the delete request until the subscriber observes every child-delete change event.

**Table 8.** Cascade delete. 1 parent + K children. `RT` (ms) is the parent-delete round trip; `events p95` (ms) is the p95 time to observe all K child-delete events on the change feed. Mean over N=5 runs on c7g.4xlarge. REST+PG has no push channel, so its events column is omitted.

| K | MQDB (fjall) RT | MQDB (fjall) events p95 | Mosquitto + PG RT | Mosquitto + PG events p95 | Mosquitto + Redis RT | Mosquitto + Redis events p95 | REST + PG RT |
|---|---|---|---|---|---|---|---|
| 10 | _PENDING AWS RUN_ | _PENDING AWS RUN_ | _PENDING AWS RUN_ | _PENDING AWS RUN_ | _PENDING AWS RUN_ | _PENDING AWS RUN_ | _PENDING AWS RUN_ |
| 100 | _PENDING AWS RUN_ | _PENDING AWS RUN_ | _PENDING AWS RUN_ | _PENDING AWS RUN_ | _PENDING AWS RUN_ | _PENDING AWS RUN_ | _PENDING AWS RUN_ |
| 1000 | _PENDING AWS RUN_ | _PENDING AWS RUN_ | _PENDING AWS RUN_ | _PENDING AWS RUN_ | _PENDING AWS RUN_ | _PENDING AWS RUN_ | _PENDING AWS RUN_ |

**Analysis.** Cascade delete stresses atomicity more than throughput: the question is whether the parent delete and all K child deletes commit together. MQDB's cascade path commits every child delete and its corresponding change-event in the same `WriteBatch` as the parent delete — either all (1 + K) records and all (1 + K) events are visible, or none are. PostgreSQL's `ON DELETE CASCADE` provides the same transactional guarantee at the storage engine layer, but the change-event publish still depends on the bridge surviving past the commit. The Mosquitto + Redis bridge is structurally weaker: it cannot atomically commit the parent delete, the child deletes, and the change-event publish, so a crash mid-cascade leaves the child set in a partially-deleted state. <!-- PENDING AWS ANALYSIS: per-K latency scaling, event fan-out p95 vs K, and the MQDB cascade-depth ceiling behaviour will be written once the SOP run has populated the table. -->

## 6.5 Architectural Comparison

Beyond the quantitative workloads in §6.2–§6.4, the unified architecture reduces deployment and reasoning complexity relative to the separated baselines. Table 9 captures the qualitative differences; §6.4 provides quantitative backing for the change-feed, unique-constraint, and cascade rows.

**Table 9.** Architectural comparison.

| Metric | MQDB (agent) | MQDB (cluster) | Mosquitto + PG | Mosquitto + Redis | REST + PG |
|---|---|---|---|---|---|
| Processes to deploy | 1 | N (one per node) | 3 (broker, bridge, DB) | 3 (broker, bridge, cache) | 2 (server, DB) |
| Configuration files | 1 (auth) | 1 (auth) + certs | 3+ (broker, bridge, DB) | 3+ (broker, bridge, cache) | 2 (server, DB) |
| Authentication configs | 1 | 1 | 2 (broker + DB) | 2 (broker + Redis) | 2 (server + DB) |
| Network hops per operation | 0 (in-process) | 1 (QUIC, for remote partitions) | 2 (broker↔bridge, bridge↔DB) | 2 (broker↔bridge, bridge↔cache) | 1 (server↔DB) |
| Broker ↔ DB atomicity | Same fjall batch | Same fjall batch (per partition) | Bridge-implemented | Bridge-implemented | N/A (no broker) |
| Unique-constraint enforcement | Two-phase reservation (§4.6) | Partition-local two-phase | DB-native unique index | Bridge `SETNX` (key-level only) | DB-native unique index |
| Cascade atomicity | Single WriteBatch (parent + children + events) | Partition-local WriteBatch | `ON DELETE CASCADE` (records only; events bridge-emitted) | Bridge-scripted (no atomicity across commands) | `ON DELETE CASCADE` (records only; no push channel) |
| Change-feed delivery | Push, outbox-in-batch | Push, outbox-in-batch | Push, bridge-emitted | Push, bridge-emitted | Long-poll (`LISTEN/NOTIFY`) |
| Durability window (default) | 10ms periodic fsync | 10ms periodic fsync | DB-configured | DB-configured | DB-configured |

The row that differs most sharply across configurations is **broker ↔ DB atomicity**. In the separated architecture, the consistency between broker delivery and database state depends entirely on the bridge implementation — whether it uses database transactions, implements retry logic with idempotency keys, and handles partial failures. Every deployment must solve this problem, and every solution is application-specific. In MQDB agent mode, the database operation, its indexes, its constraint reservations, and the change-event outbox row are committed in a single fjall `WriteBatch`; in cluster mode the same applies within a partition, with cross-partition side effects (remote constraint commits, secondary-index updates) eventually consistent via TTL-based reconciliation. The durability window row clarifies that this atomicity is WAL-level, with `fsync` deferred by up to 10ms under the default `--durability periodic` setting; operators who require per-write fsync can select `--durability immediate` at a throughput cost that we do not characterize in this paper.

## 6.6 Threats to Validity

We note four threats to the validity of the evaluation in §6.2–§6.5, each of which constrains the claims the tables in this section support.

**Instance and microarchitecture coverage.** Every measurement is taken on two AWS instance sizes (c7g.xlarge and c7g.4xlarge) on ARM Graviton 3. A third size and an x86 family (for example c7i) would add a data point about ISA and cache-hierarchy sensitivity; we deliberately kept the matrix to two sizes on a single ISA to bound the experimental cost of the SOP run, and we flag the resulting coverage gap.

**Network and deployment topology.** All runs are single-AZ, single-instance. Cross-AZ latency, network partitioning, and the cluster-mode replication path are not exercised in §6.2–§6.4. Cluster-mode performance is out of scope for this paper and is planned for a follow-up study with a multi-machine deployment — see §7.4.

**Sample size and tail resolution.** Each cell is the mean of N=5 runs of 10,000 operations, for a nominal 50,000 samples per cell on the single-operation workloads. This is sufficient for stable p50/p95/p99 estimation but not for p99.9 or maximum latency; tail-beyond-p99 is explicitly out of scope. The concurrency sweep (Table 4c) stops at 128 — higher concurrency would expose further queueing regimes on the larger instance but requires a different harness configuration, which we defer.

**Baseline implementation fidelity.** The Mosquitto + Redis bridge uses `SETNX` to approximate unique constraints (§6.4.2) and scripts cascade in application code (§6.4.3); the REST+PG change-feed uses `LISTEN/NOTIFY` long-polling because REST has no native push channel. These are the simplest correct mappings of each MQDB capability onto the respective stack, not optimized implementations. A production Redis deployment might use Redis Streams for change-feed; a production REST deployment might use Server-Sent Events or WebSockets. We chose the minimum-viable mapping to surface the architectural gap, not to tune the baselines. Where a native equivalent does not exist at all (Redis unique constraints), the column is omitted with an explanatory footnote rather than filled with a misleading number.

