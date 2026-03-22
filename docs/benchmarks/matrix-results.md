# MQDB Benchmark Results - 2026-03-20

**Date:** 2026-03-20
**Transport:** Direct QUIC (mTLS)
**Methodology:** Triplicate runs per experiment, mean +/- stdev reported
**Platform:** macOS (Darwin 25.3.0), local loopback

---

## 1. Executive Summary

| Metric | Agent | Partial | Upper | Full |
|--------|-------|---------|-------|------|
| Sync Insert (ops/s) | 3978 | 2698-2916 | 2731-2994 | 2604-2903 |
| Sync Get (ops/s) | 6440 | 3143-4856 | 3171-4905 | 3092-4745 |
| Sync Update (ops/s) | 4929 | 3061-3284 | 3013-3328 | 2854-3062 |
| Sync List (ops/s) | 193 | 5-5 | 5-5 | 5-5 |
| Sync Delete (ops/s) | 5153 | 3328-3835 | 3259-3693 | 3010-3576 |
| **Async Insert (ops/s)** | **8172** | 4605-4852 | 4198-4835 | 4577-4692 |
| **Async Get (ops/s)** | **18073** | 15622-15759 | 14643-15059 | 15309-15780 |
| **Async Update (ops/s)** | **11143** | 6098-6227 | 3465-5921 | 5714-5923 |
| **Async Delete (ops/s)** | **938** | 935-937 | 933-939 | 938-941 |
| Same-Node PubSub (msg/s) | 145476 | 126286-129438 | 126380-133605 | 130972-132331 |

**Key Findings:**

- Agent mode delivers the highest throughput across all operations (no replication overhead)
- Agent async: 8172 insert, 18073 get, 11143 update ops/s
- Cluster async insert: 4528-4709 ops/s across topologies (vs 8172 agent)
- Cross-node pubsub saturates at ~1003 msg/s (QoS 0 design limit)
- Same-node pubsub: ~126-145k msg/s across all cluster configurations

**Failed experiments:**
- DD-4: runs [2, 3]

---

## 2. Part A: Same-Node PubSub

Publisher and subscriber on the same node. Measures local broker throughput (msg/s).

| ID | Config | Node | Port | Peers | Run 1 | Run 2 | Run 3 | Mean +/- Stdev |
|----|--------|------|------|---------|-------|-------|-------|----------------|
| A1 | Agent | N/A | 1883 | 0 | 153115 | 142408 | 140904 | 145476 +/- 6658 |
| A2 | Partial | N1 | 1883 | 0 | 127194 | 134549 | 126570 | 129438 +/- 4438 |
| A3 | Partial | N2 | 1884 | 1 | 126309 | 126277 | 126273 | 126286 +/- 20 |
| A4 | Partial | N3 | 1885 | 1 | 126587 | 134040 | 126536 | 129054 +/- 4318 |
| A5 | Upper | N1 | 1883 | 2 | 136193 | 128412 | 125794 | 130133 +/- 5409 |
| A6 | Upper | N2 | 1884 | 1 | 136872 | 134638 | 129303 | 133605 +/- 3889 |
| A7 | Upper | N3 | 1885 | 0 | 125772 | 126969 | 126399 | 126380 +/- 598 |
| A8 | Full | N1 | 1883 | 2 | 130763 | 131604 | 130550 | 130972 +/- 557 |
| A9 | Full | N2 | 1884 | 2 | 134268 | 127955 | 134769 | 132331 +/- 3798 |
| A10 | Full | N3 | 1885 | 2 | 131138 | 133716 | 128513 | 131122 +/- 2602 |

### Summary by Topology

| Topology | Min (msg/s) | Max (msg/s) | Avg (msg/s) |
|----------|-------------|-------------|-------------|
| Agent | 145476 | 145476 | 145476 |
| Partial | 126286 | 129438 | 128259 |
| Upper | 126380 | 133605 | 130039 |
| Full | 130972 | 132331 | 131475 |

---

## 3. Part B: Cross-Node PubSub

Publisher on one node, subscriber on another. Measures cross-node forwarding (msg/s).
Cross-node pubsub is rate-limited by QoS 0 saturation at ~1003 msg/s.

### Partial Mesh

| ID | Direction | Pub Port | Sub Port | Run 1 | Run 2 | Run 3 | Mean +/- Stdev |
|----|-----------|----------|----------|-------|-------|-------|----------------|
| B1 | N1->N2 | 1883 | 1884 | 1003 | 1003 | 1003 | 1003 +/- 0 |
| B2 | N1->N3 | 1883 | 1885 | 1004 | 1003 | 1004 | 1003 +/- 1 |
| B3 | N2->N1 | 1884 | 1883 | 1003 | 1004 | 1003 | 1003 +/- 1 |
| B4 | N2->N3 | 1884 | 1885 | 1003 | 1003 | 1003 | 1003 +/- 0 |
| B5 | N3->N1 | 1885 | 1883 | 1003 | 1003 | 1003 | 1003 +/- 0 |
| B6 | N3->N2 | 1885 | 1884 | 1003 | 1003 | 1003 | 1003 +/- 0 |

### Upper Mesh

| ID | Direction | Pub Port | Sub Port | Run 1 | Run 2 | Run 3 | Mean +/- Stdev |
|----|-----------|----------|----------|-------|-------|-------|----------------|
| B7 | N1->N2 | 1883 | 1884 | 1004 | 1002 | 1003 | 1003 +/- 1 |
| B8 | N1->N3 | 1883 | 1885 | 1003 | 1004 | 1003 | 1003 +/- 0 |
| B9 | N2->N1 | 1884 | 1883 | 1003 | 1003 | 1003 | 1003 +/- 0 |
| B10 | N2->N3 | 1884 | 1885 | 1003 | 1003 | 1003 | 1003 +/- 0 |
| B11 | N3->N1 | 1885 | 1883 | 1003 | 1003 | 1003 | 1003 +/- 0 |
| B12 | N3->N2 | 1885 | 1884 | 1003 | 1003 | 1003 | 1003 +/- 0 |

### Full Mesh

| ID | Direction | Pub Port | Sub Port | Run 1 | Run 2 | Run 3 | Mean +/- Stdev |
|----|-----------|----------|----------|-------|-------|-------|----------------|
| B13 | N1->N2 | 1883 | 1884 | 1003 | 1001 | 655 | 886 +/- 200 |
| B14 | N1->N3 | 1883 | 1885 | 803 | 1003 | 1003 | 936 +/- 115 |
| B15 | N2->N1 | 1884 | 1883 | 1003 | 1000 | 1003 | 1002 +/- 2 |
| B16 | N2->N3 | 1884 | 1885 | 1001 | 1002 | 1003 | 1002 +/- 1 |
| B17 | N3->N1 | 1885 | 1883 | 1004 | 1003 | 1003 | 1003 +/- 1 |
| B18 | N3->N2 | 1885 | 1884 | 1003 | 1003 | 1003 | 1003 +/- 0 |

### Cross-Node PubSub Summary

| Topology | Mean (msg/s) | Min (msg/s) | Max (msg/s) |
|----------|-------------|-------------|-------------|
| Partial | 1003 | 1003 | 1003 |
| Upper | 1003 | 1003 | 1003 |
| Full | 972 | 886 | 1003 |

---

## 4. Part C: Sync DB Operations

Synchronous request-response DB operations (ops/s). Each operation waits for broker acknowledgment.

### Sync Insert

_1000 records created_

| ID | Config | Node | Port | Peers | Run 1 | Run 2 | Run 3 | Mean +/- Stdev |
|----|--------|------|------|---------|-------|-------|-------|----------------|
| CI-1 | Agent | N/A | 1883 | 0 | 3571 | 4187 | 4176 | 3978 +/- 353 |
| CI-2 | Partial | N1 | 1883 | 0 | 2852 | 2889 | 2876 | 2872 +/- 19 |
| CI-3 | Partial | N2 | 1884 | 1 | 2971 | 2828 | 2948 | 2916 +/- 77 |
| CI-4 | Partial | N3 | 1885 | 1 | 2662 | 2694 | 2738 | 2698 +/- 38 |
| CI-5 | Upper | N1 | 1883 | 2 | 2885 | 2908 | 2931 | 2908 +/- 23 |
| CI-6 | Upper | N2 | 1884 | 1 | 2787 | 2673 | 2733 | 2731 +/- 57 |
| CI-7 | Upper | N3 | 1885 | 0 | 2976 | 2995 | 3010 | 2994 +/- 17 |
| CI-8 | Full | N1 | 1883 | 2 | 2724 | 2801 | 2758 | 2761 +/- 39 |
| CI-9 | Full | N2 | 1884 | 2 | 3016 | 2808 | 2883 | 2903 +/- 105 |
| CI-10 | Full | N3 | 1885 | 2 | 2630 | 2553 | 2628 | 2604 +/- 44 |

### Sync Get

_1000 records read (seeded 1000)_

| ID | Config | Node | Port | Peers | Run 1 | Run 2 | Run 3 | Mean +/- Stdev |
|----|--------|------|------|---------|-------|-------|-------|----------------|
| CG-1 | Agent | N/A | 1883 | 0 | 6476 | 6421 | 6424 | 6440 +/- 31 |
| CG-2 | Partial | N1 | 1883 | 0 | 4488 | 4391 | 4510 | 4463 +/- 64 |
| CG-3 | Partial | N2 | 1884 | 1 | 4831 | 4781 | 4955 | 4856 +/- 90 |
| CG-4 | Partial | N3 | 1885 | 1 | 3377 | 2805 | 3246 | 3143 +/- 300 |
| CG-5 | Upper | N1 | 1883 | 2 | 4345 | 4247 | 4481 | 4358 +/- 117 |
| CG-6 | Upper | N2 | 1884 | 1 | 3230 | 2802 | 3480 | 3171 +/- 343 |
| CG-7 | Upper | N3 | 1885 | 0 | 4912 | 4975 | 4827 | 4905 +/- 75 |
| CG-8 | Full | N1 | 1883 | 2 | 4060 | 4070 | 4270 | 4133 +/- 118 |
| CG-9 | Full | N2 | 1884 | 2 | 4722 | 4801 | 4714 | 4745 +/- 48 |
| CG-10 | Full | N3 | 1885 | 2 | 3166 | 2823 | 3286 | 3092 +/- 240 |

### Sync Update

_1000 records modified (seeded 1000)_

| ID | Config | Node | Port | Peers | Run 1 | Run 2 | Run 3 | Mean +/- Stdev |
|----|--------|------|------|---------|-------|-------|-------|----------------|
| CU-1 | Agent | N/A | 1883 | 0 | 4985 | 4858 | 4943 | 4929 +/- 65 |
| CU-2 | Partial | N1 | 1883 | 0 | 3047 | 3013 | 3123 | 3061 +/- 56 |
| CU-3 | Partial | N2 | 1884 | 1 | 3265 | 3507 | 3081 | 3284 +/- 214 |
| CU-4 | Partial | N3 | 1885 | 1 | 3139 | 3068 | 3052 | 3086 +/- 46 |
| CU-5 | Upper | N1 | 1883 | 2 | 3177 | 3147 | 3216 | 3180 +/- 35 |
| CU-6 | Upper | N2 | 1884 | 1 | 3035 | 2987 | 3018 | 3013 +/- 24 |
| CU-7 | Upper | N3 | 1885 | 0 | 3340 | 3101 | 3544 | 3328 +/- 222 |
| CU-8 | Full | N1 | 1883 | 2 | 3034 | 2988 | 3039 | 3020 +/- 28 |
| CU-9 | Full | N2 | 1884 | 2 | 3114 | 3219 | 2853 | 3062 +/- 188 |
| CU-10 | Full | N3 | 1885 | 2 | 2825 | 2841 | 2895 | 2854 +/- 37 |

### Sync List

_100 list operations (seeded 100)_

| ID | Config | Node | Port | Peers | Run 1 | Run 2 | Run 3 | Mean +/- Stdev |
|----|--------|------|------|---------|-------|-------|-------|----------------|
| CL-1 | Agent | N/A | 1883 | 0 | 212 | 165 | 203 | 193 +/- 25 |
| CL-2 | Partial | N1 | 1883 | 0 | 5 | 5 | 5 | 5 +/- 0 |
| CL-3 | Partial | N2 | 1884 | 1 | 5 | 5 | 5 | 5 +/- 0 |
| CL-4 | Partial | N3 | 1885 | 1 | 5 | 5 | 5 | 5 +/- 0 |
| CL-5 | Upper | N1 | 1883 | 2 | 5 | 5 | 5 | 5 +/- 0 |
| CL-6 | Upper | N2 | 1884 | 1 | 6 | 6 | 5 | 5 +/- 0 |
| CL-7 | Upper | N3 | 1885 | 0 | 5 | 5 | 6 | 5 +/- 0 |
| CL-8 | Full | N1 | 1883 | 2 | 5 | 5 | 5 | 5 +/- 0 |
| CL-9 | Full | N2 | 1884 | 2 | 5 | 5 | 5 | 5 +/- 0 |
| CL-10 | Full | N3 | 1885 | 2 | 5 | 5 | 5 | 5 +/- 0 |

### Sync Delete

_1000 records removed (seeded 1000)_

| ID | Config | Node | Port | Peers | Run 1 | Run 2 | Run 3 | Mean +/- Stdev |
|----|--------|------|------|---------|-------|-------|-------|----------------|
| CD-1 | Agent | N/A | 1883 | 0 | 5212 | 5155 | 5092 | 5153 +/- 60 |
| CD-2 | Partial | N1 | 1883 | 0 | 3426 | 3556 | 3283 | 3422 +/- 136 |
| CD-3 | Partial | N2 | 1884 | 1 | 3906 | 3795 | 3803 | 3835 +/- 62 |
| CD-4 | Partial | N3 | 1885 | 1 | 3387 | 3273 | 3324 | 3328 +/- 57 |
| CD-5 | Upper | N1 | 1883 | 2 | 3919 | 3232 | 3505 | 3552 +/- 346 |
| CD-6 | Upper | N2 | 1884 | 1 | 3209 | 3235 | 3334 | 3259 +/- 66 |
| CD-7 | Upper | N3 | 1885 | 0 | 3662 | 3723 | 3695 | 3693 +/- 31 |
| CD-8 | Full | N1 | 1883 | 2 | 3767 | 3333 | 3357 | 3486 +/- 244 |
| CD-9 | Full | N2 | 1884 | 2 | 3672 | 3588 | 3469 | 3576 +/- 102 |
| CD-10 | Full | N3 | 1885 | 2 | 2994 | 2961 | 3076 | 3010 +/- 59 |

### Sync Operations Summary by Topology

Mean throughput (ops/s) averaged across all nodes in each topology:

| Operation | Agent | Partial | Upper | Full |
|-----------|-------|---------|-------|------|
| Insert | 3978 | 2829 | 2878 | 2756 |
| Get | 6440 | 4154 | 4144 | 3990 |
| Update | 4929 | 3144 | 3174 | 2979 |
| List | 193 | 5 | 5 | 5 |
| Delete | 5153 | 3528 | 3502 | 3357 |

---

## 5. Part D: Async DB Operations

Pipelined async operations measuring sustained throughput until saturation (ops/s).
Uses QoS 1 for backpressure. Duration: 60s per run. Fresh DB per operation category.

### Async Insert

_60s pipelined inserts_

| ID | Config | Node | Port | Peers | Run 1 | Run 2 | Run 3 | Mean +/- Stdev |
|----|--------|------|------|---------|-------|-------|-------|----------------|
| DI-1 | Agent | N/A | 1883 | 0 | 8246 | 8214 | 8057 | 8172 +/- 101 |
| DI-2 | Partial | N1 | 1883 | 0 | 4257 | 4854 | 4704 | 4605 +/- 311 |
| DI-3 | Partial | N2 | 1884 | 1 | 4988 | 4413 | 4607 | 4669 +/- 293 |
| DI-4 | Partial | N3 | 1885 | 1 | 5000 | 4496 | 5059 | 4852 +/- 309 |
| DI-5 | Upper | N1 | 1883 | 2 | 4746 | 4849 | 4910 | 4835 +/- 83 |
| DI-6 | Upper | N2 | 1884 | 1 | 4513 | 4938 | 4205 | 4552 +/- 368 |
| DI-7 | Upper | N3 | 1885 | 0 | 4715 | 4366 | 3513 | 4198 +/- 618 |
| DI-8 | Full | N1 | 1883 | 2 | 4584 | 4728 | 4764 | 4692 +/- 95 |
| DI-9 | Full | N2 | 1884 | 2 | 4512 | 4961 | 4258 | 4577 +/- 356 |
| DI-10 | Full | N3 | 1885 | 2 | 4623 | 4966 | 4450 | 4680 +/- 263 |

### Async Get

_60s pipelined gets (seeded 10000)_

| ID | Config | Node | Port | Peers | Run 1 | Run 2 | Run 3 | Mean +/- Stdev |
|----|--------|------|------|---------|-------|-------|-------|----------------|
| DG-1 | Agent | N/A | 1883 | 0 | 18168 | 18138 | 17914 | 18073 +/- 139 |
| DG-2 | Partial | N1 | 1883 | 0 | 15709 | 15938 | 15532 | 15726 +/- 204 |
| DG-3 | Partial | N2 | 1884 | 1 | 15812 | 15123 | 15931 | 15622 +/- 436 |
| DG-4 | Partial | N3 | 1885 | 1 | 15902 | 15764 | 15612 | 15759 +/- 145 |
| DG-5 | Upper | N1 | 1883 | 2 | 14910 | 15124 | 15144 | 15059 +/- 130 |
| DG-6 | Upper | N2 | 1884 | 1 | 14790 | 14947 | 14845 | 14861 +/- 80 |
| DG-7 | Upper | N3 | 1885 | 0 | 14586 | 15144 | 14198 | 14643 +/- 476 |
| DG-8 | Full | N1 | 1883 | 2 | 15652 | 15317 | 15325 | 15431 +/- 191 |
| DG-9 | Full | N2 | 1884 | 2 | 15670 | 15777 | 15892 | 15780 +/- 111 |
| DG-10 | Full | N3 | 1885 | 2 | 15448 | 15196 | 15283 | 15309 +/- 128 |

### Async Update

_60s pipelined updates (seeded 10000)_

| ID | Config | Node | Port | Peers | Run 1 | Run 2 | Run 3 | Mean +/- Stdev |
|----|--------|------|------|---------|-------|-------|-------|----------------|
| DU-1 | Agent | N/A | 1883 | 0 | 10627 | 11324 | 11479 | 11143 +/- 454 |
| DU-2 | Partial | N1 | 1883 | 0 | 6468 | 6208 | 6005 | 6227 +/- 232 |
| DU-3 | Partial | N2 | 1884 | 1 | 6398 | 6186 | 5710 | 6098 +/- 352 |
| DU-4 | Partial | N3 | 1885 | 1 | 6335 | 6121 | 6224 | 6227 +/- 107 |
| DU-5 | Upper | N1 | 1883 | 2 | 5487 | 5586 | 6044 | 5706 +/- 297 |
| DU-6 | Upper | N2 | 1884 | 1 | 6099 | 5881 | 5782 | 5921 +/- 162 |
| DU-7 | Upper | N3 | 1885 | 0 | 5954 | 2851 | 1590 | 3465 +/- 2246 |
| DU-8 | Full | N1 | 1883 | 2 | 5741 | 5624 | 5777 | 5714 +/- 80 |
| DU-9 | Full | N2 | 1884 | 2 | 5808 | 5948 | 5906 | 5887 +/- 72 |
| DU-10 | Full | N3 | 1885 | 2 | 6012 | 5868 | 5890 | 5923 +/- 78 |

### Async Delete

_60s pipelined deletes (seeded 10000)_

| ID | Config | Node | Port | Peers | Run 1 | Run 2 | Run 3 | Mean +/- Stdev |
|----|--------|------|------|---------|-------|-------|-------|----------------|
| DD-1 | Agent | N/A | 1883 | 0 | 936 | 938 | 940 | 938 +/- 2 |
| DD-2 | Partial | N1 | 1883 | 0 | 938 | 930 | 938 | 935 +/- 5 |
| DD-3 | Partial | N2 | 1884 | 1 | 932 | 936 | 938 | 935 +/- 3 |
| DD-4 | Partial | N3 | 1885 | 1 | 937 | FAILED | FAILED | 937 +/- 0 *(runs [2, 3] failed)* |
| DD-5 | Upper | N1 | 1883 | 2 | 931 | 938 | 938 | 936 +/- 4 |
| DD-6 | Upper | N2 | 1884 | 1 | 940 | 939 | 938 | 939 +/- 1 |
| DD-7 | Upper | N3 | 1885 | 0 | 930 | 928 | 940 | 933 +/- 6 |
| DD-8 | Full | N1 | 1883 | 2 | 939 | 947 | 938 | 941 +/- 5 |
| DD-9 | Full | N2 | 1884 | 2 | 940 | 941 | 939 | 940 +/- 1 |
| DD-10 | Full | N3 | 1885 | 2 | 940 | 938 | 937 | 938 +/- 2 |

### Async Operations Summary by Topology

Mean throughput (ops/s) averaged across all nodes in each topology:

| Operation | Agent | Partial | Upper | Full |
|-----------|-------|---------|-------|------|
| Insert | 8172 | 4709 | 4528 | 4650 |
| Get | 18073 | 15703 | 14854 | 15507 |
| Update | 11143 | 6184 | 5030 | 5842 |
| Delete | 938 | 936 | 936 | 940 |

---

## 6. Analysis

### Topology Comparison

#### Sync DB Performance

Cluster overhead ratio compared to agent mode (lower = more overhead):

| Operation | Partial/Agent | Upper/Agent | Full/Agent |
|-----------|---------------|-------------|------------|
| Insert | 0.71x | 0.72x | 0.69x |
| Get | 0.64x | 0.64x | 0.62x |
| Update | 0.64x | 0.64x | 0.60x |
| List | 0.03x | 0.03x | 0.03x |
| Delete | 0.68x | 0.68x | 0.65x |

#### Async DB Performance

| Operation | Partial/Agent | Upper/Agent | Full/Agent |
|-----------|---------------|-------------|------------|
| Insert | 0.58x | 0.55x | 0.57x |
| Get | 0.87x | 0.82x | 0.86x |
| Update | 0.55x | 0.45x | 0.52x |
| Delete | 1.00x | 1.00x | 1.00x |

#### Node Variance Within Topologies

Coefficient of variation (stdev/mean) across nodes within each topology, for async insert:

| Topology | Node Means | CV |
|----------|------------|-----|
| Partial | 4605, 4669, 4852 | 2.72% |
| Upper | 4835, 4552, 4198 | 7.05% |
| Full | 4692, 4577, 4680 | 1.36% |

### Performance Characteristics

1. **Agent mode** delivers the highest throughput as expected (no replication overhead)
2. **Cluster sync operations** run at approximately 55-75% of agent mode throughput
3. **Cluster async operations** show more variation:
   - Get remains high (~85% of agent) since reads hit local storage
   - Insert/Update show replication cost (~55-60% of agent)
   - Delete is consistent at ~938 ops/s across all configurations
4. **Cross-node pubsub** is uniformly ~1003 msg/s (QoS 0 saturation limit)
5. **Same-node pubsub** is minimally affected by cluster membership (~126-145k vs ~145k agent)
6. **List operations** are the slowest category and show dramatic difference between agent (~193 ops/s) and cluster (~5 ops/s)

### Notable Anomalies

- **DD-4 (Partial N3 async delete):** Runs 2 and 3 failed
- **DU-7 (Upper N3 async update):** Mean 3465 ops/s, significantly lower than other nodes (possible resource contention)
- **B13 (Full N1->N2 pubsub):** Run 3 showed degraded throughput (655 msg/s)
- **B14 (Full N1->N3 pubsub):** Run 1 showed degraded throughput (803 msg/s)

---

## 7. Test Configuration

| Parameter | Value |
|-----------|-------|
| Date | 2026-03-20 |
| Platform | macOS (Darwin 25.3.0) |
| Network | Local loopback (127.0.0.1) |
| Transport | Direct QUIC with mTLS |
| Cluster Size | 3 nodes |
| Replication Factor | 2 (primary + replica) |
| Partitions | 256 |
| PubSub Duration | 10s per run |
| Sync Operations | 1000 ops (100 for list) |
| Async Duration | 60s per run |
| Async QoS | 1 (with PUBACK backpressure) |
| Replications | 3 runs per experiment (triplicates) |
| Isolation | Fresh DB per async operation category |

### Configurations Tested

| Config | Topology | Peers per Node |
|--------|----------|------------------|
| Agent | Standalone | N/A |
| Partial | N1: 0, N2: 1 (->N1), N3: 1 (->N1) | 0, 1, 1 |
| Upper | N1: 2 (->N2,N3), N2: 1 (->N3), N3: 0 | 2, 1, 0 |
| Full | All nodes connect to all others | 2, 2, 2 |

### Experiment Count

| Part | Experiments | Runs (incl. failed) |
|------|-------------|---------------------|
| A: Same-Node PubSub | 10 | 30 |
| B: Cross-Node PubSub | 18 | 54 |
| C: Sync DB | 50 | 150 |
| D: Async DB | 40 | 120 |
| **Total** | **118** | **354** |
