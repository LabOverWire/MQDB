# Benchmarking & Health Endpoint

[Back to index](README.md)

## 15. Benchmarking

The `mqdb bench` commands measure performance.

### Pub/Sub Benchmark

```bash
mqdb bench pubsub \
  --publishers 4 \
  --subscribers 4 \
  --duration 10 \
  --size 256 \
  --qos 1
```

**Options:**

| Option | Description | Default |
|--------|-------------|---------|
| `--publishers` | Number of publisher tasks | `1` |
| `--subscribers` | Number of subscriber tasks | `1` |
| `--duration` | Duration in seconds | `10` |
| `--size` | Payload size in bytes | `64` |
| `--qos` | MQTT QoS level (0, 1, or 2) | `0` |
| `--topic` | Topic base pattern | `bench/test` |
| `--topics` | Number of topics to spread load across | `1` |
| `--wildcard` | Use wildcard subscription (`topic/#`) | `false` |
| `--warmup` | Warmup duration in seconds | `1` |
| `--pub-broker` | Broker for publishers (for cross-node testing) | (same as `--broker`) |
| `--sub-broker` | Broker for subscribers (for cross-node testing) | (same as `--broker`) |
| `--format` | Output format: `json`, `table`, `csv` | `table` |

### Database Benchmark

```bash
mqdb bench db \
  --operations 1000 \
  --entity bench_entity \
  --op mixed \
  --concurrency 4 \
  --fields 5 \
  --field-size 100
```

**Options:**

| Option | Description | Default |
|--------|-------------|---------|
| `--operations` | Number of operations | `1000` |
| `--entity` | Entity name to use | `bench_entity` |
| `--op` | Operation type: `insert`, `get`, `update`, `delete`, `list`, `mixed` | `mixed` |
| `--concurrency` | Number of concurrent clients | `1` |
| `--fields` | Fields per record | `5` |
| `--field-size` | Size of each field value in bytes | `100` |
| `--warmup` | Warmup operations before measuring | (none) |
| `--cleanup` | Delete test entity after benchmark | `false` |
| `--seed` | Pre-populate N records (for get/update/delete/list) | `0` |
| `--no-latency` | Disable latency tracking for pure throughput | `false` |
| `--async` | Use pipelined mode (fire all ops, collect responses) | `false` |
| `--qos` | MQTT QoS level for async mode | `1` |
| `--duration` | Duration in seconds (async mode only, overrides `--operations`) | (none) |
| `--format` | Output format: `json`, `table`, `csv` | `table` |

> **QoS Warning:** Async mode defaults to QoS 1 because QoS 0 causes connection resets at
> ~5000 ops/s due to lack of flow control. QoS 1's PUBACK provides natural backpressure.

---

## 16. Health Endpoint Testing

The `$DB/_health` topic provides cluster and node health status.

### Query Health Status

```bash
mosquitto_rr -h 127.0.0.1 -p 1883 -t '$DB/_health' -e 'r' -m '{}' -W 5
```

**Expected response:**
```json
{
  "status": "ok",
  "data": {
    "ready": true,
    "mode": "cluster",
    "node_id": 1,
    "node_name": "node-1",
    "raft_leader": 1,
    "alive_nodes": [1, 2, 3],
    "partition_count": 256,
    "primary_partitions": 22,
    "replica_partitions": 21
  }
}
```

### Health Check After Node Failure

1. Start 3-node cluster
2. Subscribe to health on Node 2
3. Kill Node 1
4. Verify health shows updated alive_nodes

```bash
# After killing Node 1
mosquitto_rr -h 127.0.0.1 -p 1884 -t '$DB/_health' -e 'r' -m '{}' -W 5
```

**Expected:** `alive_nodes` should show `[2, 3]` and a new leader elected.
