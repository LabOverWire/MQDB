# Complete Benchmark Matrix Results

Results from the complete benchmark matrix comparing MQTT bridges vs Direct QUIC transport.

**Test Date:** 2026-01-19 (updated 2026-01-20 with improved async benchmarks, 2026-01-26 with triplicate topology comparison)

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

---

## Part E: Triplicate Topology Comparison (2026-01-26)

*Fresh triplicate benchmarks comparing Partial Mesh vs Full Mesh with Direct QUIC transport. Statistical analysis included.*

### Per-Node Statistics (mean ± stdev)

#### Insert (ops/s)
| Node | Partial Mesh | Full Mesh | Difference |
|------|--------------|-----------|------------|
| Node 1 | 3,965 ± 193 | 4,492 ± 752 | +13.3% |
| Node 2 | 4,367 ± 117 | 5,092 ± 959 | +16.6% |
| Node 3 | 4,029 ± 30 | 4,396 ± 851 | +9.1% |

#### Get (ops/s)
| Node | Partial Mesh | Full Mesh | Difference |
|------|--------------|-----------|------------|
| Node 1 | 4,864 ± 421 | 5,736 ± 845 | +17.9% |
| Node 2 | 5,655 ± 154 | 6,592 ± 1,138 | +16.6% |
| Node 3 | 3,576 ± 117 | 4,809 ± 1,856 | +34.5% |

#### Update (ops/s)
| Node | Partial Mesh | Full Mesh | Difference |
|------|--------------|-----------|------------|
| Node 1 | 4,400 ± 33 | 5,069 ± 1,120 | +15.2% |
| Node 2 | 4,775 ± 109 | 5,742 ± 1,317 | +20.2% |
| Node 3 | 3,849 ± 168 | 4,815 ± 1,423 | +25.1% |

#### Delete (ops/s)
| Node | Partial Mesh | Full Mesh | Difference |
|------|--------------|-----------|------------|
| Node 1 | 4,775 ± 61 | 5,593 ± 1,211 | +17.1% |
| Node 2 | 5,347 ± 97 | 6,509 ± 1,833 | +21.7% |
| Node 3 | 2,763 ± 92 | 4,190 ± 2,548 | +51.7% |

#### PubSub (msg/s)
| Node | Partial Mesh | Full Mesh | Difference |
|------|--------------|-----------|------------|
| Node 1 | 215,900 ± 17,594 | 228,242 ± 17,015 | +5.7% |
| Node 2 | 215,806 ± 19,071 | 205,669 ± 2,701 | -4.7% |
| Node 3 | 215,595 ± 15,397 | 205,540 ± 3,773 | -4.7% |

### Cluster Aggregate Statistics (sum of all 3 nodes)

| Operation | Partial Mesh | Full Mesh | Diff | Winner |
|-----------|--------------|-----------|------|--------|
| Insert | 12,361 ± 331 ops/s | 13,981 ± 2,550 ops/s | +13.1% | Full |
| Get | 14,095 ± 549 ops/s | 17,137 ± 3,836 ops/s | +21.6% | Full |
| Update | 13,024 ± 195 ops/s | 15,625 ± 3,849 ops/s | +20.0% | Full |
| List | 52 ± 1 ops/s | 67 ± 24 ops/s | +29.0% | Full |
| Delete | 12,884 ± 148 ops/s | 16,293 ± 5,591 ops/s | +26.5% | Full |
| PubSub | 647,301 ± 20,135 msg/s | 639,451 ± 20,884 msg/s | -1.2% | Partial |

**Summary:** Full mesh wins 5/6 operations, Partial wins PubSub marginally.

### Statistical Significance (t-test)

| Operation | t-statistic | Significant? |
|-----------|-------------|--------------|
| Insert | 1.09 | NO |
| Get | 1.36 | NO |
| Update | 1.17 | NO |
| List | 1.10 | NO |
| Delete | 1.06 | NO |
| PubSub | -0.47 | NO |

*Critical value: |t| > 2.78 for p < 0.05 with df=4*

**None of the differences are statistically significant** due to high variance in Full Mesh results.

### Raw Run Data

#### Partial Mesh (Cluster Totals)
| Run | Insert | Get | Update | List | Delete | PubSub |
|-----|--------|-----|--------|------|--------|--------|
| 1 | 11,983 | 13,506 | 12,842 | 52 | 12,772 | 627,640 |
| 2 | 12,498 | 14,593 | 13,000 | 51 | 12,829 | 646,383 |
| 3 | 12,601 | 14,185 | 13,229 | 52 | 13,052 | 667,879 |

#### Full Mesh (Cluster Totals)
| Run | Insert | Get | Update | List | Delete | PubSub |
|-----|--------|-----|--------|------|--------|--------|
| 1 | 12,490 | 15,137 | 13,466 | 53 | 13,006 | 650,676 |
| 2 | 16,925 | 21,560 | 20,069 | 94 | 22,748 | 652,322 |
| 3 | 12,528 | 14,714 | 13,340 | 53 | 13,124 | 615,355 |

### Analysis

**Key Observation:** Full Mesh Run 2 was an outlier with 35-75% higher throughput than other runs. This inflated Full Mesh's mean but also created high variance (stddev 2,500-5,500 vs Partial's 100-550), making statistical significance impossible to establish.

| Metric | Partial Mesh | Full Mesh |
|--------|--------------|-----------|
| Mean Performance | Lower | Higher (+13-27%) |
| Consistency (CV) | **Very Low (1-4%)** | High (18-43%) |
| Predictability | **Excellent** | Variable |

### Recommendation

**Use Partial Mesh for predictable performance.** While Full Mesh *can* achieve higher throughput (as seen in Run 2), results are inconsistent. Partial Mesh provides stable, reproducible performance with minimal variance between runs.

---

## Part F: Triplicate Async Topology Comparison (2026-01-26)

*Fresh triplicate async benchmarks comparing Partial Mesh vs Full Mesh with Direct QUIC transport. Async mode uses adaptive ramp-up to find saturation point.*

### Per-Node Statistics (mean ± stdev) - Saturation Point ops/s

#### Async Insert
| Node | Partial Mesh | Full Mesh | Difference |
|------|--------------|-----------|------------|
| Node 1 | 9,531 ± 27 | 9,570 ± 98 | +0.4% |
| Node 2 | 9,500 ± 303 | 9,672 ± 69 | +1.8% |
| Node 3 | 9,540 ± 79 | 9,666 ± 80 | +1.3% |

#### Async Get
| Node | Partial Mesh | Full Mesh | Difference |
|------|--------------|-----------|------------|
| Node 1 | 16,782 ± 71 | 16,740 ± 426 | -0.3% |
| Node 2 | 16,728 ± 515 | 17,043 ± 98 | +1.9% |
| Node 3 | 16,999 ± 190 | 16,726 ± 199 | -1.6% |

#### Async Update
| Node | Partial Mesh | Full Mesh | Difference |
|------|--------------|-----------|------------|
| Node 1 | 10,238 ± 257 | 10,677 ± 90 | +4.3% |
| Node 2 | 9,474 ± 671 | 10,611 ± 165 | +12.0% |
| Node 3 | 10,841 ± 260 | 10,685 ± 296 | -1.4% |

### Cluster Aggregate Statistics (sum of all 3 nodes)

| Operation | Partial Mesh | Full Mesh | Diff | Winner |
|-----------|--------------|-----------|------|--------|
| Insert | 28,571 ± 404 ops/s | 28,909 ± 208 ops/s | +1.2% | Full |
| Get | 50,510 ± 647 ops/s | 50,509 ± 617 ops/s | -0.0% | Partial |
| Update | 30,553 ± 293 ops/s | 31,973 ± 400 ops/s | +4.6% | Full |

**Summary:** Full mesh wins 2/3 operations (Insert, Update), Partial wins Get by negligible margin.

### Statistical Significance (t-test)

| Operation | t-statistic | Significant? |
|-----------|-------------|--------------|
| Insert | 1.29 | NO |
| Get | -0.00 | NO |
| Update | 4.96 | **YES** |

*Critical value: |t| > 2.78 for p < 0.05 with df=4*

**Only Async Update shows statistically significant difference** - Full Mesh is ~4.6% faster with p < 0.05.

### Raw Run Data

#### Partial Mesh (Cluster Totals)
| Run | Insert | Get | Update |
|-----|--------|-----|--------|
| 1 | 28,957 | 50,894 | 30,372 |
| 2 | 28,606 | 49,763 | 30,396 |
| 3 | 28,151 | 50,872 | 30,891 |

#### Full Mesh (Cluster Totals)
| Run | Insert | Get | Update |
|-----|--------|-----|--------|
| 1 | 28,792 | 50,606 | 32,189 |
| 2 | 28,785 | 49,849 | 31,511 |
| 3 | 29,149 | 51,072 | 32,218 |

### Variance Analysis (Coefficient of Variation)

| Operation | Partial CV | Full CV | More Consistent |
|-----------|------------|---------|-----------------|
| Insert | 1.4% | 0.7% | Full |
| Get | 1.3% | 1.2% | Full |
| Update | 1.0% | 1.3% | Partial |

### Analysis

**Key Observation:** Unlike the sync benchmarks where Full Mesh showed high variance due to an outlier run, async benchmarks show **both topologies are highly consistent** (CV 0.7-1.4%).

| Metric | Partial Mesh | Full Mesh |
|--------|--------------|-----------|
| Insert Performance | 28,571 ops/s | 28,909 ops/s (+1.2%) |
| Get Performance | 50,510 ops/s | 50,509 ops/s (equal) |
| Update Performance | 30,553 ops/s | 31,973 ops/s (+4.6%) |
| Consistency | Excellent | Excellent |

### Recommendation

**For async workloads, topology choice has minimal impact.** Both Partial and Full Mesh achieve essentially identical throughput (~29k insert, ~50k get, ~31k update ops/s). The only statistically significant difference is a 4.6% advantage for Full Mesh on updates, which may not be operationally meaningful.

**Choose based on operational preferences:**
- **Partial Mesh:** Fewer connections, simpler network topology
- **Full Mesh:** Marginally better update performance, fully redundant connectivity
