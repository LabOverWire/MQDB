# Experiments Diary

Tracking experiment design decisions, execution, issues, and results.

---

## Session 1 — 2026-04-13: Benchmark harness and initial local results

### What was built

- Docker Compose for baseline infrastructure (PostgreSQL 17 + Mosquitto 2.0.x)
- PostgreSQL uses tmpfs (RAM-backed) for fair comparison with MQDB's in-memory fjall
- `run_comparison.sh` orchestrates sequential CRUD triplicates
- Fixed `mqdb bench db --async --format json` (was silently ignored, now produces JSON)

### Methodology decisions

1. **Sequential (concurrency=1) is the defensible comparison for the paper.** Concurrent mode unfairly amplifies MQDB's in-process advantage because the baseline bridge degrades under concurrent load due to its architecture (Mosquitto → bridge → PG), not due to inherent Mosquitto+PG limitations.

2. **Async ramp mode unsuitable for comparison.** The `--async` flag uses a ramp algorithm that starts at 500 ops/s and increases by 50% per 2s interval. It measures saturation point, not throughput. Average throughput over the run includes slow-start, making it appear slower than sequential mode. Useful for finding system limits but not for apples-to-apples comparison.

3. **Fresh agent restart per run is mandatory.** Spillover between runs (fjall compaction, entity metadata, cached state) causes variance. The first run after a clean start is always fastest. Subsequent runs on the same agent instance show degradation. Solution: `stop_mqdb` + `rm -rf /tmp/mqdb-bench-agent` + `start_mqdb` before every individual benchmark run.

### Issues encountered and resolved

| Issue | Root cause | Fix |
|-------|-----------|-----|
| Mosquitto Docker bind-mount fails | colima can't mount from paths with spaces | `Dockerfile.mosquitto` with COPY instead |
| PG role "postgresql" not found | deadpool-postgres misparses `postgresql://` scheme | Use `postgres://` scheme |
| Local PG port conflict | host PG on 5432 | Docker maps to 5433 |
| Bridge connection refused after compose up | colima port forwarding lag | `nc -z` wait loop |
| `--password` flag doesn't exist | CLI uses `--pass` | Fixed in script |
| Password file race condition | `rm -f` before agent reads it | Move `rm` after sleep |
| Bench output has non-JSON preamble | "Waiting for broker..." lines | `extract_json` python helper |
| Async bench ignores `--format json` | Missing code path in `db_async.rs` | Added `OutputFormat::Json` branch |
| Spillover between runs | fjall state from prior runs | Fresh agent restart per run |

### Local results (macOS, 1,000 ops, sequential, fresh agent per run)

| Operation | MQDB Run1 | MQDB Run2 | MQDB Run3 | Baseline Run1 | Baseline Run2 | Baseline Run3 |
|-----------|-----------|-----------|-----------|---------------|---------------|---------------|
| insert    | 3,733     | 3,756     | 3,783     | 676           | 694           | 689           |
| get       | 5,299     | 5,467     | 5,600     | 807           | 818           | 819           |
| update    | 4,049     | 4,102     | 4,120     | 663           | 680           | 678           |
| delete    | 4,362     | 4,434     | 4,471     | 610           | 618           | 619           |
| list      | 2,041     | 2,093     | 2,139     | 543           | 551           | 553           |

Variance is tight (< 5% within triplicates). MQDB wins all operations 3.8×–7.2×.

### What's missing for the paper

- Mixed workload (50% read / 50% update)
- Latency percentiles (P50/P95/P99) — currently using `--no-latency`
- Pub/sub throughput (§6.3)
- Cluster experiments (§6.4)
- Redis baseline (§6.2)
- Higher operation count (10,000) for tighter statistics
- AWS execution for reproducible hardware

### Next steps

1. Verify `--op mixed` works with `--format json`
2. Enable latency tracking (remove `--no-latency`)
3. Build Redis bridge for fourth baseline
4. Design pub/sub and cluster benchmark scripts
5. Provision AWS infrastructure (see `../infra/`)
6. Run full experiment suite on AWS
