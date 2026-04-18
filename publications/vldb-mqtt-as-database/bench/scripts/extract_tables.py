#!/usr/bin/env python3
"""
extract_tables.py - populate Section 6 table cells from bench JSON results.

Reads a top-level run-manifest.json produced by aws_run.sh and prints Markdown
for Tables 4a, 4b, 4c, 5a, 5b, 6, 7 and 8 to stdout. Paste the output into
publications/vldb-mqtt-as-database/sections/06-evaluation.md to replace the
`_PENDING AWS RUN_` markers.

Expected layout:
    <results-root>/run-manifest.json
    <results-root>/<instance_type>/manifest.json
    <results-root>/<instance_type>/<label>_<op>[_<suffix>]_run<N>.json

Where <instance_type> is e.g. c7g.xlarge or c7g.4xlarge. The same bench result
schema is produced by every MQDB bench sub-op and by the REST+PG bench driver.

Usage:
    python3 extract_tables.py <path-to-run-manifest.json>
"""

from __future__ import annotations

import json
import math
import statistics
import sys
from pathlib import Path


CRUD_LABELS = ["mqdb", "baseline_pg", "baseline_redis", "rest_pg", "mqdb_mem"]
CRUD_HEADERS = [
    "MQDB (fjall)",
    "Mosquitto + PG",
    "Mosquitto + Redis",
    "REST + PG",
    "MQDB (memory)",
]

MIXED_LABELS = ["mqdb", "baseline_pg", "baseline_redis", "mqdb_mem"]
MIXED_HEADERS = [
    "MQDB (fjall)",
    "Mosquitto + PG",
    "Mosquitto + Redis",
    "MQDB (memory)",
]

PUBSUB_LABELS = ["mqdb", "mosquitto", "mqdb_mem"]
PUBSUB_HEADERS = ["MQDB (fjall)", "Mosquitto", "MQDB (memory)"]

CHANGEFEED_LABELS = ["mqdb", "mqdb_mem", "baseline_pg", "baseline_redis", "rest_pg"]
CHANGEFEED_HEADERS = [
    "MQDB (fjall) push",
    "MQDB (memory) push",
    "Mosquitto + PG bridge-emitted",
    "Mosquitto + Redis bridge-emitted",
    "REST + PG long-poll (LISTEN/NOTIFY)",
]

UNIQUE_LABELS = ["mqdb", "baseline_pg", "rest_pg"]
UNIQUE_HEADERS = ["MQDB (fjall)", "Mosquitto + PG", "REST + PG"]

CASCADE_LABELS_RT_AND_EVENTS = ["mqdb", "baseline_pg", "baseline_redis"]
CASCADE_LABELS_RT_ONLY = ["rest_pg"]

PENDING = "_PENDING AWS RUN_"


def load(p: Path):
    with p.open() as f:
        return json.load(f)


def mean_sd(values):
    if not values:
        return None, None
    if len(values) == 1:
        return float(values[0]), 0.0
    m = statistics.fmean(values)
    sd = math.sqrt(sum((v - m) ** 2 for v in values) / len(values))
    return m, sd


def collect(subdir: Path, pattern: str):
    return [load(p) for p in sorted(subdir.glob(pattern)) if p.is_file()]


def field(files, key):
    return [d[key] for d in files if key in d]


def cell_throughput_latency(files):
    if not files:
        return f"{PENDING}<br>{PENDING}"
    tm, ts = mean_sd(field(files, "throughput_ops_sec"))
    p50m, p50s = mean_sd(field(files, "latency_p50_us"))
    p95m, p95s = mean_sd(field(files, "latency_p95_us"))
    p99m, p99s = mean_sd(field(files, "latency_p99_us"))
    if tm is None or p50m is None or p95m is None or p99m is None:
        return f"{PENDING}<br>{PENDING}"
    return (
        f"{tm:,.0f} ± {ts:,.0f}<br>"
        f"{p50m:.0f}±{p50s:.0f} / {p95m:.0f}±{p95s:.0f} / {p99m:.0f}±{p99s:.0f}"
    )


def cell_mixed(files):
    if not files:
        return f"{PENDING}<br>{PENDING}"
    tm, ts = mean_sd(field(files, "throughput_ops_sec"))
    p99m, p99s = mean_sd(field(files, "latency_p99_us"))
    if tm is None or p99m is None:
        return f"{PENDING}<br>{PENDING}"
    return f"{tm:,.0f} ± {ts:,.0f}<br>{p99m:.0f} ± {p99s:.0f}"


def cell_pubsub(files):
    if not files:
        return PENDING
    tm, ts = mean_sd(field(files, "throughput_msg_sec"))
    if tm is None:
        return PENDING
    return f"{tm:,.0f} ± {ts:,.0f}"


def cell_changefeed_ms(files, key):
    if not files:
        return PENDING
    if all(d.get("events_received", 0) == 0 for d in files):
        return "—"
    m, s = mean_sd([v / 1000.0 for v in field(files, key)])
    if m is None:
        return PENDING
    return f"{m:.2f} ± {s:.2f}"


def cell_unique(files):
    if not files:
        return PENDING
    s_mean, _ = mean_sd(field(files, "successes_total"))
    c_p95_mean, _ = mean_sd(field(files, "latency_conflict_p95_us"))
    wall_ms_values = [
        d.get("duration_secs", d.get("wall_secs", 0)) * 1000.0
        for d in files
        if "duration_secs" in d or "wall_secs" in d
    ]
    wall_ms_mean, _ = mean_sd(wall_ms_values)
    if s_mean is None or c_p95_mean is None or wall_ms_mean is None:
        return PENDING
    return f"{s_mean:.0f} / {c_p95_mean:.0f} μs / {wall_ms_mean:.0f} ms"


def cell_cascade_ms(files, key):
    if not files:
        return PENDING
    if "propagation" in key and all(d.get("events_received", 0) == 0 for d in files):
        return "—"
    m, _ = mean_sd([v / 1000.0 for v in field(files, key)])
    if m is None:
        return PENDING
    return f"{m:.2f}"


def render_table_4(subdir: Path, caption: str) -> str:
    header = "| Operation | " + " | ".join(CRUD_HEADERS) + " |"
    divider = "|---" * (len(CRUD_HEADERS) + 1) + "|"
    lines = [f"**{caption}**", "", header, divider]
    for op in ("insert", "get", "update", "delete", "list"):
        cells = [op]
        for label in CRUD_LABELS:
            cells.append(cell_throughput_latency(
                collect(subdir, f"{label}_{op}_run*.json")
            ))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def render_table_4c(subdir: Path) -> str:
    header = "| Concurrency | " + " | ".join(MIXED_HEADERS) + " |"
    divider = "|---" * (len(MIXED_HEADERS) + 1) + "|"
    lines = ["**Table 4c.** Mixed workload concurrency sweep (c7g.4xlarge).", "", header, divider]
    for c in (1, 8, 32, 128):
        cells = [str(c)]
        for label in MIXED_LABELS:
            cells.append(cell_mixed(
                collect(subdir, f"{label}_mixed_c{c}_run*.json")
            ))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def render_table_5(subdir: Path, caption: str) -> str:
    header = "| QoS | " + " | ".join(PUBSUB_HEADERS) + " |"
    divider = "|---" * (len(PUBSUB_HEADERS) + 1) + "|"
    lines = [f"**{caption}**", "", header, divider]
    for qos in (0, 1):
        cells = [str(qos)]
        for label in PUBSUB_LABELS:
            cells.append(cell_pubsub(
                collect(subdir, f"{label}_pubsub_qos{qos}_run*.json")
            ))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def render_table_6(subdir: Path) -> str:
    header = "| Configuration | p50 | p95 | p99 |"
    divider = "|---|---|---|---|"
    lines = ["**Table 6.** Change-feed delivery latency (ms, c7g.4xlarge).", "", header, divider]
    for label, label_txt in zip(CHANGEFEED_LABELS, CHANGEFEED_HEADERS):
        files = collect(subdir, f"{label}_changefeed_run*.json")
        p50 = cell_changefeed_ms(files, "latency_p50_us")
        p95 = cell_changefeed_ms(files, "latency_p95_us")
        p99 = cell_changefeed_ms(files, "latency_p99_us")
        lines.append(f"| {label_txt} | {p50} | {p95} | {p99} |")
    return "\n".join(lines)


def render_table_7(subdir: Path) -> str:
    header = "| K | " + " | ".join(UNIQUE_HEADERS) + " |"
    divider = "|---" * (len(UNIQUE_HEADERS) + 1) + "|"
    lines = ["**Table 7.** Unique-constraint contention (c7g.4xlarge).", "", header, divider]
    for k in (4, 16, 64):
        cells = [str(k)]
        for label in UNIQUE_LABELS:
            cells.append(cell_unique(
                collect(subdir, f"{label}_unique_k{k}_run*.json")
            ))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def render_table_8(subdir: Path) -> str:
    header_parts = ["K"]
    for label_txt in ("MQDB (fjall)", "Mosquitto + PG", "Mosquitto + Redis"):
        header_parts.append(f"{label_txt} RT")
        header_parts.append(f"{label_txt} events p95")
    header_parts.append("REST + PG RT")
    header = "| " + " | ".join(header_parts) + " |"
    divider = "|---" * len(header_parts) + "|"
    lines = ["**Table 8.** Cascade delete (c7g.4xlarge).", "", header, divider]
    for k in (10, 100, 1000):
        cells = [str(k)]
        for label in CASCADE_LABELS_RT_AND_EVENTS:
            files = collect(subdir, f"{label}_cascade_k{k}_run*.json")
            cells.append(cell_cascade_ms(files, "delete_latency_p95_us"))
            cells.append(cell_cascade_ms(files, "propagation_latency_p95_us"))
        for label in CASCADE_LABELS_RT_ONLY:
            files = collect(subdir, f"{label}_cascade_k{k}_run*.json")
            cells.append(cell_cascade_ms(files, "delete_latency_p95_us"))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def main():
    if len(sys.argv) < 2:
        print("Usage: extract_tables.py <path-to-run-manifest.json>", file=sys.stderr)
        sys.exit(1)

    manifest_path = Path(sys.argv[1]).resolve()
    if not manifest_path.is_file():
        print(f"ERROR: manifest not found: {manifest_path}", file=sys.stderr)
        sys.exit(2)

    manifest = load(manifest_path)
    root = manifest_path.parent
    instance_subdirs = manifest.get("instance_subdirs", [])
    if not instance_subdirs:
        print("ERROR: instance_subdirs is empty in run-manifest.json", file=sys.stderr)
        sys.exit(3)

    subdirs = {name: (root / name) for name in instance_subdirs}
    xlarge = subdirs.get("c7g.xlarge")
    fourxlarge = subdirs.get("c7g.4xlarge")

    print(f"<!-- extract_tables.py from {manifest_path} -->")
    print(f"<!-- commit={manifest.get('mqdb_git_commit', '?')} "
          f"version={manifest.get('mqdb_version', '?')} "
          f"utc={manifest.get('utc_timestamp', '?')} -->")
    print()

    if xlarge and xlarge.is_dir():
        print(render_table_4(xlarge, "Table 4a. CRUD on c7g.xlarge."))
        print()
        print(render_table_5(xlarge, "Table 5a. Pub/sub on c7g.xlarge."))
        print()
    else:
        print("<!-- c7g.xlarge subdirectory missing from manifest; Tables 4a and 5a skipped -->")
        print()

    if fourxlarge and fourxlarge.is_dir():
        print(render_table_4(fourxlarge, "Table 4b. CRUD on c7g.4xlarge."))
        print()
        print(render_table_5(fourxlarge, "Table 5b. Pub/sub on c7g.4xlarge."))
        print()
        print(render_table_4c(fourxlarge))
        print()
        print(render_table_6(fourxlarge))
        print()
        print(render_table_7(fourxlarge))
        print()
        print(render_table_8(fourxlarge))
        print()
    else:
        print("<!-- c7g.4xlarge subdirectory missing from manifest; Tables 4b, 4c, 5b, 6, 7, 8 skipped -->")


if __name__ == "__main__":
    main()
