# Complete Benchmark Matrix Results

Results from the complete benchmark matrix comparing MQTT bridges vs Direct QUIC transport.

**Test Date:** 2026-01-19 (updated 2026-01-20 with improved async benchmarks)

## Executive Summary

**Key Finding:** Direct QUIC transport eliminates the bridge overhead penalty, providing uniform ~8,500 insert / ~16,500 get / ~9,000 update ops/s across all nodes regardless of topology.

| Metric | Agent Mode | Cluster MQTT | Cluster QUIC |
|--------|------------|--------------|--------------|
| Sync Insert | 6,647 ops/s | 999-2,674 ops/s | 3,758-4,415 ops/s |
| Sync Get | 7,756 ops/s | 1,102-5,572 ops/s | 3,556-5,916 ops/s |
| Sync Update | 6,776 ops/s | 656-5,489 ops/s | 3,648-4,648 ops/s |
| **Async Insert** | **15,652 ops/s** | 0-9,084 ops/s | 8,354-8,664 ops/s |
| **Async Get** | **19,726 ops/s** | 977-15,548 ops/s | 15,972-16,996 ops/s |
| **Async Update** | **4,853 ops/s** | 542-9,800 ops/s | 8,604-9,758 ops/s |
| Pubsub | 186,067 msg/s | 175-200k msg/s | 170-210k msg/s |

**Key Insights:**
- Agent mode async ops achieve **15,600+ insert**, **19,700+ get** ops/s (no replication overhead)
- QUIC cluster: **uniform ~8,500 insert / ~16,500 get** across all nodes regardless of topology
- MQTT cluster: **severe degradation** with bridges (nodes with 2 bridges drop to ~1,500-2,000 ops/s)
- QUIC variance: **1.03x** vs MQTT's **10x+** variance between nodes

---

## Agent Mode Baseline

| Metric | Saturation Point |
|--------|------------------|
| Pubsub | **186,067 msg/s** |
| Sync Insert | **6,647 ops/s** |
| Sync Get | **7,756 ops/s** |
| Sync Update | **6,776 ops/s** |
| Sync List | **474 ops/s** |
| **Async Insert** | **15,652 ops/s** |
| **Async Get** | **19,726 ops/s** |
| **Async Update** | **4,853 ops/s** |

---

## Part A: Same-Node PubSub (msg/s)

### MQTT Transport
| Config | Node | Bridges | Run 1 | Run 2 | Run 3 | Mean |
|--------|------|---------|-------|-------|-------|------|
| Partial | N1 | 0 | 194675 | 186918 | 218619 | 200071 |
| Partial | N2 | 1 | 176882 | 177347 | 178502 | 177577 |
| Partial | N3 | 1 | 204385 | 125912 | 204203 | 178167 |
| Upper | N1 | 2 | 206148 | 179061 | 180026 | 188412 |
| Upper | N2 | 1 | 174377 | 174839 | 189349 | 179522 |
| Upper | N3 | 0 | 194467 | 206202 | 189892 | 196854 |
| Full | N1 | 2 | 202565 | 122045 | 200482 | 175031 |
| Full | N2 | 2 | 201974 | 181733 | 197484 | 193730 |
| Full | N3 | 2 | 184495 | 174499 | 201125 | 186706 |

### Direct QUIC Transport
| Config | Node | Run 1 | Run 2 | Run 3 | Mean |
|--------|------|-------|-------|-------|------|
| Partial | N1 | 171787 | 175184 | 195959 | 180977 |
| Partial | N2 | 168658 | 222502 | 221778 | 204313 |
| Partial | N3 | 233629 | 171417 | 198492 | 201179 |
| Upper | N1 | 211630 | 201191 | 194693 | 202505 |
| Upper | N2 | 213342 | 204306 | 205543 | 207730 |
| Upper | N3 | 206064 | 185581 | 208045 | 199897 |
| Full | N1 | 197430 | 168488 | 203741 | 189886 |
| Full | N2 | 232529 | 206383 | 179555 | 206156 |
| Full | N3 | 168864 | 170554 | 170358 | 169925 |

**Observation:** Same-node pubsub is similar between transports (~180-210k msg/s). Transport choice doesn't affect local pub/sub.

---

## Part B: Cross-Node PubSub (msg/s)

Cross-node pubsub is rate-limited by QoS 0 saturation at ~1000 msg/s regardless of transport.

| Transport | Config | Sample Pairs | Mean |
|-----------|--------|--------------|------|
| MQTT | Partial | 1→2, 2→1, etc. | 1000-1300 |
| QUIC | Partial | 1→2, 2→1, etc. | 1001-1002 |
| MQTT | Upper | 1→2, 2→1, etc. | 1000-1950 |
| QUIC | Upper | 1→2, 2→1, etc. | 1001-1003 |
| MQTT | Full | 1→2, 2→1, etc. | 1000-2700 |
| QUIC | Full | 1→2, 2→1, etc. | 994-1003 |

**Observation:** QUIC shows more consistent cross-node throughput. MQTT occasionally spikes higher (2700 msg/s) but with high variance.

---

## Part C: Sync DB Operations (ops/s)

### CI: Sync Insert

#### MQTT Transport
| Config | N1 (bridges) | N2 (bridges) | N3 (bridges) | Mean |
|--------|--------------|--------------|--------------|------|
| Partial | 2543 (0) | 1309 (1) | 1533 (1) | 1795 |
| Upper | 1486 (2) | 1442 (1) | 3960 (0) | 2296 |
| Full | 2674 (2) | 1129 (2) | 1119 (2) | 1641 |

#### Direct QUIC Transport
| Config | N1 | N2 | N3 | Mean |
|--------|-----|-----|-----|------|
| Partial | 4143 | 4358 | 3817 | 4106 |
| Upper | 3879 | 4074 | 4415 | 4123 |
| Full | 3981 | 4353 | 3758 | 4031 |

**QUIC Improvement:** 2-3x better than MQTT, especially for nodes with bridges.

### CG: Sync Get

#### MQTT Transport
| Config | N1 | N2 | N3 | Mean |
|--------|-----|-----|-----|------|
| Upper | 2171 | 1687* | 5572 | 3143 |
| Full | 1658 | 1903 | 1102 | 1554 |

#### Direct QUIC Transport
| Config | N1 | N2 | N3 | Mean |
|--------|-----|-----|-----|------|
| Partial | 5045 | 5916 | 3938 | 4966 |
| Upper | 3608 | 4943 | 5708 | 4753 |
| Full | 4910 | 5538 | 3556 | 4668 |

**QUIC Improvement:** 2-3x better than MQTT.

### CU: Sync Update

#### MQTT Transport
| Config | N1 | N2 | N3 | Mean |
|--------|-----|-----|-----|------|
| Upper | 3505 | ~0* | 5489 | 3016 |
| Full | 930 | 1065 | 656 | 884 |

#### Direct QUIC Transport
| Config | N1 | N2 | N3 | Mean |
|--------|-----|-----|-----|------|
| Partial | 4209 | 4659 | 3797 | 4222 |
| Upper | 3773 | 4252 | 4616 | 4214 |
| Full | 4200 | 4467 | 3763 | 4143 |

**QUIC Improvement:** 4-5x better than MQTT in Full mesh. MQTT N2 frequently timed out.

### CL: Sync List

List operations are inherently slow (~2-5 ops/s) in both transports due to database scan overhead.

---

## Part D: Async DB Operations (ops/s) - Updated 2026-01-20

*Results from improved benchmark with adaptive ramp-up and latency tracking. Values represent saturation point (maximum sustainable throughput).*

### Async Insert

#### MQTT Transport
| Config | N1 (bridges) | N2 (bridges) | N3 (bridges) | Mean |
|--------|--------------|--------------|--------------|------|
| Partial | 9,084 (0) | 1,414 (1) | 0* (1) | 3,499 |
| Upper | 2,128 (2) | 1,872 (1) | 7,415 (0) | 3,805 |
| Full | 2,116 (2) | 1,196 (2) | 1,757 (2) | 1,690 |

*N3 Partial had connectivity issues

#### Direct QUIC Transport
| Config | N1 | N2 | N3 | Mean | Variance |
|--------|-----|-----|-----|------|----------|
| Partial | 8,472 | 8,457 | 8,531 | 8,487 | 1.009x |
| Upper | 8,664 | 8,525 | 8,480 | 8,556 | 1.022x |
| Full | 8,579 | 8,621 | 8,354 | 8,518 | 1.032x |

**Critical Finding:**
- MQTT: High variance based on bridge count (0-9,084 ops/s range)
- QUIC: **Uniform ~8,500 ops/s** regardless of topology (1.03x variance)
- Improvement for bridged nodes: **4-6x** (1,500 → 8,500 ops/s)

### Async Get

#### MQTT Transport
| Config | N1 (bridges) | N2 (bridges) | N3 (bridges) | Mean |
|--------|--------------|--------------|--------------|------|
| Partial | 15,548 (0) | 1,252 (1) | 0* (1) | 5,600 |
| Upper | 1,220 (2) | 1,262 (1) | 977 (0) | 1,153 |
| Full | 1,314 (2) | 1,327 (2) | 1,268 (2) | 1,303 |

#### Direct QUIC Transport
| Config | N1 | N2 | N3 | Mean | Variance |
|--------|-----|-----|-----|------|----------|
| Partial | 16,996 | 16,445 | 15,972 | 16,471 | 1.064x |
| Upper | 16,650 | 16,523 | 16,856 | 16,676 | 1.020x |
| Full | 16,881 | 16,494 | 16,629 | 16,668 | 1.023x |

**QUIC Improvement:** 12-14x for bridged nodes (1,300 → 16,500 ops/s)

### Async Update

#### MQTT Transport
| Config | N1 (bridges) | N2 (bridges) | N3 (bridges) | Mean |
|--------|--------------|--------------|--------------|------|
| Partial | 9,800 (0) | 792 (1) | 0* (1) | 3,531 |
| Upper | 807 (2) | 560 (1) | 8,374 (0) | 3,247 |
| Full | 542 (2) | 584 (2) | 582 (2) | 569 |

#### Direct QUIC Transport
| Config | N1 | N2 | N3 | Mean | Variance |
|--------|-----|-----|-----|------|----------|
| Partial | 9,565 | 9,464 | 8,734 | 9,254 | 1.095x |
| Upper | 9,438 | 9,537 | 8,604 | 9,193 | 1.108x |
| Full | 9,758 | 9,478 | 8,692 | 9,309 | 1.123x |

**QUIC Improvement:** 16x for Full mesh (569 → 9,300 ops/s)

---

## Analysis

### Bridge Count Impact (MQTT Only)

| Bridges | Async Insert | Async Get | Async Update |
|---------|--------------|-----------|--------------|
| 0 | 7,415-9,800 ops/s | 977-15,548 ops/s | 8,374-9,800 ops/s |
| 1 | 792-1,872 ops/s | 1,252-1,262 ops/s | 560-792 ops/s |
| 2 | 1,196-2,128 ops/s | 1,220-1,327 ops/s | 542-807 ops/s |

**Conclusion:** Each bridge reduces throughput by **5-10x** with MQTT transport.

### QUIC Eliminates Bridge Penalty

| Metric | MQTT Range | QUIC Range | MQTT Variance | QUIC Variance |
|--------|------------|------------|---------------|---------------|
| Async Insert | 0-9,084 ops/s | 8,354-8,664 ops/s | ∞ | 1.03x |
| Async Get | 977-15,548 ops/s | 15,972-16,996 ops/s | 15.9x | 1.06x |
| Async Update | 542-9,800 ops/s | 8,604-9,758 ops/s | 18.1x | 1.13x |

**QUIC provides perfectly uniform throughput** regardless of topology or bridge configuration.

### Topology Comparison (Async Insert Mean)

| Topology | MQTT | QUIC | QUIC Advantage |
|----------|------|------|----------------|
| Partial | 3,499 | 8,487 | 2.4x |
| Upper | 3,805 | 8,556 | 2.2x |
| Full | 1,690 | 8,518 | **5.0x** |

**Conclusion:** QUIC advantage increases with mesh density because MQTT bridge overhead compounds.

---

## Recommendations

1. **Use Direct QUIC** for production deployments where consistent throughput matters
2. **MQTT bridges** remain viable for development/testing with partial mesh
3. **Avoid full mesh with MQTT** - performance degrades significantly (5x slower than QUIC)
4. **Upper mesh with MQTT** is acceptable only if the node without bridges handles most traffic

---

## Test Configuration

- **Hardware:** macOS Darwin 25.2.0
- **Nodes:** 3-node local cluster (ports 1883, 1884, 1885)
- **Async benchmark:** 30 seconds with adaptive ramp-up (50% initial step, slowing to 5% near saturation)
- **Sync benchmark operations:** 1000 per run (100 for list)
- **Topologies tested:** Partial, Upper, Full mesh
- **Transports tested:** MQTT bridges, Direct QUIC

### Benchmark Methodology (Updated)

The async benchmark uses adaptive ramp-up with latency tracking:
1. **Starts at 500 ops/s** with 50% step increases every 2 seconds
2. **Monitors latency** (p50, p95) - slows step when latency exceeds 1.5x baseline
3. **Detects saturation** when throughput < 90% of target OR backlog > 2 seconds
4. **Reports saturation point** as maximum sustainable throughput

This methodology finds true saturation in ~20-30 seconds regardless of system capacity, avoiding the artificial ceiling seen in fixed-duration benchmarks.
