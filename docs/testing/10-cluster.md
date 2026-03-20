# Cluster Mode Setup, Dev Commands & Multi-Node

[Back to index](README.md)

## 11. Cluster Mode

Cluster mode runs a distributed MQDB with Raft consensus for partition management.

> **Note:** Standard CRUD operations (`mqdb create/read/update/delete/list`) work in cluster mode.
> The cluster automatically routes operations to the appropriate partition.
> For direct partition access, use the low-level `mqdb db` commands (Section 12).
>
> **Authentication:** `mqdb dev start-cluster` auto-generates credentials `admin`/`admin` and
> sets `--admin-users admin` on all nodes. All CLI commands against a dev cluster require
> `--user admin --pass admin`. Manual `mqdb cluster start` without `--passwd` allows unauthenticated
> connections (only useful for quick tests with `dev-insecure` builds).

### Starting a Single-Node Cluster

```bash
mqdb cluster start --node-id 1 --bind 127.0.0.1:1883 --db /tmp/mqdb-node1 \
  --quic-cert test_certs/server.pem --quic-key test_certs/server.key --quic-ca test_certs/ca.pem
```

**Expected behavior:**
- Becomes Raft leader immediately (single-node quorum)
- Assigns all 256 partitions to itself

> **Tip:** For quick testing without TLS, use `--no-quic` to fall back to TCP bridges.

### Starting a Multi-Node Cluster (Manual)

Generate TLS certificates first: `./scripts/generate_test_certs.sh`

**Terminal 1 (Node 1):**
```bash
mqdb cluster start \
  --node-id 1 \
  --bind 127.0.0.1:1883 \
  --db /tmp/mqdb-node1 \
  --peers 2@127.0.0.1:1884,3@127.0.0.1:1885 \
  --quic-cert test_certs/server.pem --quic-key test_certs/server.key --quic-ca test_certs/ca.pem
```

**Terminal 2 (Node 2):**
```bash
mqdb cluster start \
  --node-id 2 \
  --bind 127.0.0.1:1884 \
  --db /tmp/mqdb-node2 \
  --peers 1@127.0.0.1:1883,3@127.0.0.1:1885 \
  --quic-cert test_certs/server.pem --quic-key test_certs/server.key --quic-ca test_certs/ca.pem
```

**Terminal 3 (Node 3):**
```bash
mqdb cluster start \
  --node-id 3 \
  --bind 127.0.0.1:1885 \
  --db /tmp/mqdb-node3 \
  --peers 1@127.0.0.1:1883,2@127.0.0.1:1884 \
  --quic-cert test_certs/server.pem --quic-key test_certs/server.key --quic-ca test_certs/ca.pem
```

**Expected behavior:**
- One node wins Raft election and becomes leader
- Leader assigns partitions across all nodes
- QUIC connections established between nodes

> **Preferred:** Use `mqdb dev start-cluster --nodes 3 --clean` instead of manual setup.
> It handles cert paths, password generation, and topology configuration automatically.

### Cluster with QUIC Transport (Recommended)

QUIC is the default and recommended transport for cluster communication. First, generate TLS certificates:
```bash
./scripts/generate_test_certs.sh
```

Then start nodes with QUIC:
```bash
mqdb cluster start \
  --node-id 1 \
  --bind 127.0.0.1:1883 \
  --db /tmp/mqdb-node1 \
  --peers 2@127.0.0.1:1884 \
  --quic-cert test_certs/server.pem \
  --quic-key test_certs/server.key \
  --quic-ca test_certs/ca.pem
```

### Cluster Options Reference

Cluster mode supports all agent authentication and OAuth options (see Section 1), plus:

| Option | Description | Default |
|--------|-------------|---------|
| `--node-id` | Unique node ID (1-65535) | (required) |
| `--node-name` | Human-readable node name | (none) |
| `--bind` | MQTT listener address | `0.0.0.0:1883` |
| `--db` | Database directory path | (required) |
| `--peers` | Peer nodes: `id@host:port` (comma-separated) | (none) |
| `--quic-cert` | TLS certificate for QUIC transport | (none) |
| `--quic-key` | TLS private key for QUIC transport | (none) |
| `--quic-ca` | CA certificate for QUIC peer verification | (none) |
| `--no-quic` | Disable QUIC (use TCP bridges only) | `false` |
| `--no-persist-stores` | Disable store persistence (data lost on restart) | `false` |
| `--durability` | `immediate`, `periodic`, or `none` | `periodic` |
| `--durability-ms` | Fsync interval in ms (periodic mode) | `10` |
| `--bridge-out` | Use outgoing-only bridge direction | `false` |
| `--cluster-port-offset` | Port offset for cluster listener | `100` |

### Testing Without Persistence

For quick tests where data doesn't need to survive restarts:

```bash
mqdb cluster start --node-id 1 --bind 127.0.0.1:1883 --db /tmp/mqdb-test \
  --quic-cert test_certs/server.pem --quic-key test_certs/server.key --quic-ca test_certs/ca.pem \
  --no-persist-stores
```

### Check Cluster Status

```bash
mqdb cluster status --broker 127.0.0.1:1883
```

**Expected output:**
```json
{
  "leader": 1,
  "nodes": [
    {"id": 1, "name": "node-1", "status": "alive"},
    {"id": 2, "name": "node-2", "status": "alive"},
    {"id": 3, "name": "node-3", "status": "alive"}
  ],
  "partitions": 256
}
```

### Trigger Rebalance

```bash
mqdb cluster rebalance --broker 127.0.0.1:1883
```

### Cluster CRUD Operations

Standard CRUD commands work in cluster mode with automatic partition routing:

```bash
# Start a cluster (auto-generates admin/admin credentials)
mqdb dev start-cluster --nodes 1 --clean
sleep 5

# Create - automatically routed to a partition
mqdb create users --data '{"name": "Alice", "email": "alice@example.com"}' \
  --broker 127.0.0.1:1883 --user admin --pass admin

# Read
mqdb read users <id> --broker 127.0.0.1:1883 --user admin --pass admin

# Update
mqdb update users <id> --data '{"name": "Alice Updated", "age": 30}' \
  --broker 127.0.0.1:1883 --user admin --pass admin

# List with filter
mqdb list users --broker 127.0.0.1:1883 --user admin --pass admin
mqdb list users --filter "name=Alice Updated" --broker 127.0.0.1:1883 --user admin --pass admin

# Delete
mqdb delete users <id> --broker 127.0.0.1:1883 --user admin --pass admin

# Cleanup
mqdb dev kill
```

> **Note:** Response format in cluster mode differs from agent mode. In cluster mode,
> `id` and `entity` are top-level fields alongside `data`:
> ```json
> {"data": {"_version": 1, "name": "Alice", ...}, "entity": "users", "id": "<hash-id>", "status": "ok"}
> ```

---

## 13. Multi-Node Cluster Testing

### Setup 3-Node Cluster

Use `mqdb dev start-cluster` for automated setup (recommended):

```bash
mqdb dev start-cluster --nodes 3 --clean
```

Or set up manually with QUIC:

```bash
./scripts/generate_test_certs.sh
rm -rf /tmp/mqdb-node{1,2,3}

# Terminal 1
mqdb cluster start --node-id 1 --bind 127.0.0.1:1883 --db /tmp/mqdb-node1 \
  --peers 2@127.0.0.1:1884,3@127.0.0.1:1885 \
  --quic-cert test_certs/server.pem --quic-key test_certs/server.key --quic-ca test_certs/ca.pem

# Terminal 2
mqdb cluster start --node-id 2 --bind 127.0.0.1:1884 --db /tmp/mqdb-node2 \
  --peers 1@127.0.0.1:1883,3@127.0.0.1:1885 \
  --quic-cert test_certs/server.pem --quic-key test_certs/server.key --quic-ca test_certs/ca.pem

# Terminal 3
mqdb cluster start --node-id 3 --bind 127.0.0.1:1885 --db /tmp/mqdb-node3 \
  --peers 1@127.0.0.1:1883,2@127.0.0.1:1884 \
  --quic-cert test_certs/server.pem --quic-key test_certs/server.key --quic-ca test_certs/ca.pem
```

### Verify Cluster Formation

Wait for Raft election and partition assignment:
```bash
# Check cluster status from any node
mqdb cluster status --broker 127.0.0.1:1883
mqdb cluster status --broker 127.0.0.1:1884
mqdb cluster status --broker 127.0.0.1:1885
```

### Test Data Routing

Create data via different nodes:
```bash
# Via node 1
mqdb db create -p 0 -e items -d '{"via": "node1"}' --broker 127.0.0.1:1883 --user admin --pass admin

# Via node 2
mqdb db create -p 0 -e items -d '{"via": "node2"}' --broker 127.0.0.1:1884 --user admin --pass admin

# Via node 3
mqdb db create -p 0 -e items -d '{"via": "node3"}' --broker 127.0.0.1:1885 --user admin --pass admin
```

### Cross-Partition JSON Create Forwarding

Verify that JSON creates on non-local partitions are forwarded to the correct primary node:

```bash
# Start 3-node cluster (auto-generates admin/admin credentials)
mqdb dev start-cluster --nodes 3 --clean
sleep 5

# Create entities on each node (with 256 partitions across 3 nodes,
# ~2/3 of creates will hit non-local partitions and require forwarding)
mqdb create testfw -d '{"name":"from-node1"}' --broker 127.0.0.1:1883 --user admin --pass admin
mqdb create testfw -d '{"name":"from-node2"}' --broker 127.0.0.1:1884 --user admin --pass admin
mqdb create testfw -d '{"name":"from-node3"}' --broker 127.0.0.1:1885 --user admin --pass admin

# Create more records to ensure forwarding paths are exercised
for i in $(seq 1 10); do
  mqdb create testfw -d "{\"seq\":$i}" --broker 127.0.0.1:1884 --user admin --pass admin
done

# Verify all records are visible from any node
mqdb list testfw --broker 127.0.0.1:1883 --user admin --pass admin
mqdb list testfw --broker 127.0.0.1:1885 --user admin --pass admin

# Counts should match across all nodes
mqdb dev kill
```

**Expected:** All creates succeed (no 503 errors), and listing from any node returns the full set of records.

### Leader Failover Test

1. Identify the current Raft leader from cluster status
2. Kill the leader node (Ctrl+C)
3. Observe remaining nodes elect new leader
4. Verify operations continue on surviving nodes

```bash
# After killing leader, check new status
mqdb cluster status --broker 127.0.0.1:1884
```

### Cleanup

```bash
mqdb dev kill
```

---

## 14. Development Commands

The `mqdb dev` commands help with local development and testing.

### List Running Processes

```bash
mqdb dev ps
```

Shows all running MQDB processes (agents and cluster nodes).

### Kill Processes

```bash
# Kill all MQDB processes
mqdb dev kill --all

# Kill specific node
mqdb dev kill --node 2

# Kill agent only
mqdb dev kill --agent
```

### Clean Test Data

```bash
# Clean default test directory
mqdb dev clean

# Clean custom directory
mqdb dev clean --db-prefix /tmp/my-test
```

### View Logs

```bash
# View logs from all nodes
mqdb dev logs

# Follow logs in real-time
mqdb dev logs --follow

# View specific node logs
mqdb dev logs --node 2

# Filter by pattern
mqdb dev logs --pattern "error|warn"

# Show last N lines
mqdb dev logs --last 100
```

### Quick Cluster Start

Start a multi-node cluster for testing:

```bash
# Start 3-node cluster (default, QUIC transport)
mqdb dev start-cluster

# Start with custom node count
mqdb dev start-cluster --nodes 5

# Clean existing data first
mqdb dev start-cluster --clean

# Choose topology: partial (default), upper, or full
mqdb dev start-cluster --topology partial
mqdb dev start-cluster --topology upper
mqdb dev start-cluster --topology full

# Without QUIC transport
mqdb dev start-cluster --no-quic
```

> **Note:** `dev start-cluster` auto-generates a password file at `{db_prefix}-passwd` (default
> `/tmp/mqdb-test-passwd`) with credentials `admin`/`admin`. It also sets `--admin-users admin`
> on all nodes. Use `--user admin --pass admin` when running CLI commands against the cluster.

**Topology options:**
- `partial` (default): each node connects to lower-numbered nodes
- `upper`: each node connects to higher-numbered nodes
- `full`: every node connects to every other node

### Start-Cluster Options

| Option | Description | Default |
|--------|-------------|---------|
| `--nodes` | Number of cluster nodes | `3` |
| `--clean` | Remove existing data before starting | `false` |
| `--quic-cert` | TLS certificate path | `test_certs/server.pem` |
| `--quic-key` | TLS private key path | `test_certs/server.key` |
| `--quic-ca` | CA certificate path | `test_certs/ca.pem` |
| `--no-quic` | Disable QUIC transport | `false` |
| `--db-prefix` | Database path prefix | `/tmp/mqdb-test` |
| `--bind-host` | Host to bind | `127.0.0.1` |
| `--topology` | Mesh topology: `partial`, `upper`, `full` | `partial` |
| `--bridge-out` | Use Out-only bridge direction | `false` |
| `--no-bridge-out` | Use Both bridge direction even for full topology | `false` |
| `--passwd` | Path to password file (auto-generated if omitted) | (auto) |
| `--ownership` | Ownership config: `entity=field` pairs | (none) |

### Run Built-in Tests

```bash
# Run all cross-node tests (excludes ownership - see below)
mqdb dev test --all

# Run specific test suites
mqdb dev test --pubsub              # Cross-node pub/sub matrix
mqdb dev test --db                  # Cross-node DB CRUD
mqdb dev test --constraints         # Constraint tests
mqdb dev test --wildcards           # Wildcard subscriptions
mqdb dev test --retained            # Retained messages
mqdb dev test --lwt                 # Last Will & Testament
mqdb dev test --ownership           # Ownership enforcement (self-contained, see Section 22)
mqdb dev test --stress-constraints  # Constraint stress tests

# Specify node count
mqdb dev test --all --nodes 5
```

> **Note:** `--ownership` is excluded from `--all` because it manages its own authenticated cluster.
> It kills any running cluster, starts a fresh one with password auth and `--ownership` config,
> runs ownership tests, then kills the cluster.

### Dev Bench (Benchmarking with Auto-Start)

`dev bench` auto-starts an agent, runs benchmarks, and saves results. Parent options
(`--output`, `--baseline`, `--db`) go before the subcommand:

```bash
# Pub/sub benchmark
mqdb dev bench pubsub

# Database benchmark
mqdb dev bench db --operations 10000 --concurrency 4 --op mixed

# Save results to file and compare against baseline
mqdb dev bench --output results.json --baseline baseline.json db
```

> **Note:** `dev bench` uses higher defaults than standalone `bench`:
> pub/sub defaults to 4 publishers/subscribers (vs 1), db defaults to 10000 operations
> with concurrency 4 (vs 1000 operations, concurrency 1).

### Dev Profile (Performance Profiling)

Profile the broker using `samply` or `flamegraph`. Parent options (`--tool`, `--duration`, `--output`) go before the subcommand:

```bash
# Profile pub/sub with samply (default tool, 30s)
mqdb dev profile --duration 30 pubsub

# Profile DB operations with flamegraph
mqdb dev profile --tool flamegraph db --operations 10000

# Save profile output
mqdb dev profile --output profile.svg pubsub
```

### Dev Baseline (Benchmark Comparison)

Save and compare benchmark baselines:

```bash
# Save a baseline
mqdb dev baseline save v1.0 pubsub
mqdb dev baseline save v1.0 db

# List saved baselines
mqdb dev baseline list

# Compare current performance against a baseline
mqdb dev baseline compare v1.0 pubsub
mqdb dev baseline compare v1.0 db
```
