# MQDB CLI Testing Guide

This guide provides end-to-end validation of MQDB CLI functionality through manual commands.

## Prerequisites

### Build the CLI

```bash
cd /path/to/mqdb
cargo build --release
```

Binary location: `target/release/mqdb`

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MQDB_BROKER` | MQTT broker URL | `mqtt://localhost:1883` |
| `MQDB_USER` | Authentication username | (none) |
| `MQDB_PASS` | Authentication password | (none) |

### Terminal Setup

Open three terminal windows:

```
Terminal 1: MQDB Agent (runs continuously)
Terminal 2: CLI commands (CRUD, admin operations)
Terminal 3: Subscriber/watcher (for reactive tests)
```

---

## 1. Starting the Agent

### Basic Start (Anonymous Mode)

**Terminal 1:**
```bash
mqdb agent start --db ./data/testdb --anonymous
```

**Expected output:**
```
(agent starts silently, listening on 127.0.0.1:1883)
```

> **Note:** The `--anonymous` flag allows connections without authentication.
> Without it, you must provide `--passwd` and `--acl` files.

### Check Agent Status

**Terminal 2:**
```bash
mqdb agent status
```

**Expected output:**
```
status: connected
broker: mqtt://localhost:1883
database: ./data/testdb
```

### Start with Authentication

**Terminal 1:**
```bash
mqdb agent start --db ./data/testdb --passwd ./passwd.txt --acl ./acl.txt
```

---

## 2. Basic CRUD Operations

### Create Entity

**Terminal 2:**
```bash
mqdb create users --data '{"name": "Alice", "email": "alice@example.com", "age": 30}'
```

**Expected output:**
```json
{
  "id": "generated-uuid",
  "name": "Alice",
  "email": "alice@example.com",
  "age": 30
}
```

### Read Entity

```bash
# Use the ID returned from create
mqdb read users <id>
```

**Expected output:**
```json
{
  "id": "<generated-uuid>",
  "name": "Alice",
  "email": "alice@example.com",
  "age": 30
}
```

### Update Entity

```bash
mqdb update users <id> --data '{"age": 25}'
```

**Expected output:**
```json
{
  "id": "<id>",
  "name": "Alice",
  "email": "alice@example.com",
  "age": 25
}
```

### Delete Entity

```bash
mqdb delete users <id>
```

**Expected output:**
```
deleted users/<id>
```

### Output Formats

**Table format (default):**
```bash
mqdb read users user-001 --format table
```

**JSON format:**
```bash
mqdb read users user-001 --format json
```

**CSV format:**
```bash
mqdb list users --format csv
```

---

## 3. List and Filtering

### Setup Test Data

```bash
mqdb create products --data '{"name": "Laptop", "price": 999, "category": "electronics", "stock": 50}'
mqdb create products --data '{"name": "Mouse", "price": 29, "category": "electronics", "stock": 200}'
mqdb create products --data '{"name": "Desk", "price": 299, "category": "furniture", "stock": 25}'
mqdb create products --data '{"name": "Chair", "price": 199, "category": "furniture", "stock": 0}'
mqdb create products --data '{"name": "Keyboard", "price": 79, "category": "electronics", "stock": 150}'
```

> **Note:** IDs are auto-generated (1, 2, 3...). Use the returned IDs in subsequent commands.

### Basic List

```bash
mqdb list products
```

### Equality Filter (=)

```bash
mqdb list products --filter 'category=electronics'
```

**Expected:** Returns Laptop, Mouse, Keyboard

### Not Equal Filter (<>)

```bash
mqdb list products --filter 'category<>electronics'
```

**Expected:** Returns Desk, Chair

> **Note:** Use `<>` (SQL-style) for not-equal. The `!=` syntax also works but requires
> shell escaping due to bash history expansion.

### Greater Than Filter (>)

```bash
mqdb list products --filter 'price>100'
```

**Expected:** Returns Laptop, Desk, Chair

### Less Than Filter (<)

```bash
mqdb list products --filter 'price<100'
```

**Expected:** Returns Mouse, Keyboard

### Greater Than or Equal (>=)

```bash
mqdb list products --filter 'price>=199'
```

**Expected:** Returns Laptop, Desk, Chair

### Less Than or Equal (<=)

```bash
mqdb list products --filter 'stock<=25'
```

**Expected:** Returns Desk, Chair

### Like/Pattern Filter (~)

```bash
mqdb list products --filter 'name~*board*'
```

**Expected:** Returns Keyboard

### Is Null Filter (?)

```bash
mqdb list products --filter 'description?'
```

**Expected:** Returns all products (none have description field)

### Is Not Null Filter (!?)

```bash
mqdb list products --filter 'price!?'
```

**Expected:** Returns all products (all have price field)

### Sorting

**Ascending:**
```bash
mqdb list products --sort price:asc
```

**Descending:**
```bash
mqdb list products --sort price:desc
```

**Multiple sort fields:**
```bash
mqdb list products --sort category:asc --sort price:desc
```

### Pagination

```bash
mqdb list products --limit 2
mqdb list products --limit 2 --offset 2
```

### Combined Filters

```bash
mqdb list products --filter 'category=electronics' --filter 'price<100' --sort price:asc
```

**Expected:** Returns Mouse, Keyboard sorted by price

---

## 4. Schema Management

### Create Schema File

Create `users_schema.json`:
```json
{
  "name": {"type": "string", "required": true},
  "email": {"type": "string", "required": true},
  "age": {"type": "number", "default": 0},
  "active": {"type": "boolean", "default": true}
}
```

### Set Schema

```bash
mqdb schema set users --file users_schema.json
```

**Expected output:**
```
schema set for users
```

### Get Schema

```bash
mqdb schema get users
```

**Expected output:**
```json
{
  "name": {"type": "string", "required": true},
  "email": {"type": "string", "required": true},
  "age": {"type": "number", "default": 0},
  "active": {"type": "boolean", "default": true}
}
```

### Schema Validation - Required Field

```bash
mqdb create users --data '{"email": "test@example.com"}'
```

**Expected error:**
```
error: missing required field: name
```

### Schema Validation - Type Check

```bash
mqdb create users --data '{"name": "Test", "email": "test@example.com", "age": "not-a-number"}'
```

**Expected error:**
```
error: field 'age' expected number, got string
```

### Schema Validation - Default Values

```bash
mqdb create users --data '{"name": "Test", "email": "test@example.com"}'
mqdb read users <id>
```

**Expected:** Entity has `age: 0` and `active: true` from defaults

---

## 5. Constraints

### Unique Constraint (Single Field)

```bash
mqdb constraint add users --unique email
mqdb create users --data '{"name": "User1", "email": "unique@test.com"}'
mqdb create users --data '{"name": "User2", "email": "unique@test.com"}'
```

**Expected error on second create:**
```
error: unique constraint violation on field 'email'
```

### Not-Null Constraint

```bash
mqdb constraint add users --not-null name
```

### List Constraints

```bash
mqdb constraint list users
```

**Expected output:**
```
users constraints:
  - unique(email)
  - not_null(name)
```

### Foreign Key Constraint

Setup:
```bash
mqdb create authors --data '{"name": "Jane Author"}'
# Note the returned ID (e.g., author-abc123)
mqdb constraint add posts --fk "author_id:authors:id:cascade"
```

The `--fk` format is `field:target_entity:target_field:action` where action is `cascade`, `restrict`, or `set_null`.

Test referential integrity:
```bash
mqdb create posts --data '{"title": "Post 1", "author_id": "invalid-author"}'
```

**Expected error:**
```
error: foreign key constraint violation: author_id references non-existent authors/invalid-author
```

Valid foreign key:
```bash
mqdb create posts --data '{"title": "Post 1", "author_id": "author-abc123"}'
```

### Cascade Delete

```bash
mqdb delete authors <author-id>
mqdb list posts
```

**Expected:** Posts with matching author_id are also deleted

---

## 6. Reactive Subscriptions

### Watch Command (Broadcast)

**Terminal 3:**
```bash
mqdb watch users
```

**Terminal 2:**
```bash
mqdb create users --data '{"name": "Watcher Test"}'
```

**Expected in Terminal 3:**
```json
{"type": "create", "entity": "users", "id": "...", "data": {"name": "Watcher Test"}}
```

### Subscribe with Consumer Group (Load-Balanced)

**Terminal 3 (Consumer A):**
```bash
mqdb subscribe users --group workers --mode load_balanced
```

**Open Terminal 4 (Consumer B):**
```bash
mqdb subscribe users --group workers --mode load_balanced
```

**Terminal 2 (create multiple entities):**
```bash
mqdb create users --data '{"name": "User 1"}'
mqdb create users --data '{"name": "User 2"}'
mqdb create users --data '{"name": "User 3"}'
mqdb create users --data '{"name": "User 4"}'
```

**Expected:** Events distributed between Terminal 3 and Terminal 4 (each receives ~2 events)

### Subscribe with Ordered Mode

**Terminal 3:**
```bash
mqdb subscribe users --group processors --mode ordered
```

**Expected:** All events for same entity ID go to same consumer

### Custom Heartbeat Interval

```bash
mqdb subscribe users --group workers --heartbeat-interval 5000
```

---

## 7. Consumer Groups

### List Consumer Groups

```bash
mqdb consumer-group list
```

**Expected output:**
```
Consumer Groups:
  workers (2 members, 4 partitions)
  processors (1 member, 4 partitions)
```

### Show Consumer Group Details

```bash
mqdb consumer-group show workers
```

**Expected output:**
```
Group: workers
Mode: LoadBalanced
Members: 2
Partitions: 4

Member Assignments:
  consumer-abc123: [0, 1]
  consumer-def456: [2, 3]
```

---

## 8. Backup and Restore

### Create Backup

```bash
mqdb backup create
```

**Expected output:**
```
backup created: backup_20241208_143022
```

### List Backups

```bash
mqdb backup list
```

**Expected output:**
```
Backups:
  backup_20241208_143022 (1.2 MB)
  backup_20241207_091500 (1.1 MB)
```

### Restore from Backup

**Note:** Restore requires stopping and restarting the agent.

**Terminal 1:** Stop agent (Ctrl+C)

**Terminal 2:**
```bash
mqdb restore --name backup_20241208_143022
```

**Terminal 1:** Restart agent
```bash
mqdb agent start --db ./data/testdb
```

---

## 9. Authentication

### Create Password File

Interactive mode (prompts for password):
```bash
mqdb passwd admin
mqdb passwd readonly
```

Batch mode (password via command line):
```bash
mqdb passwd admin --batch secret123
mqdb passwd readonly --batch viewer456
```

Output to stdout instead of file:
```bash
mqdb passwd admin --batch secret123 --stdout
```

Creates/updates `passwd.txt` with hashed credentials.

### Connect with Credentials

```bash
export MQDB_USER=admin
export MQDB_PASS=secret123
mqdb list users
```

Or inline:
```bash
mqdb list users --user admin --pass secret123
```

---

## 10. Error Handling

### Entity Not Found

```bash
mqdb read users nonexistent-id
```

**Expected error:**
```
error: entity not found: users/nonexistent-id
```

### Connection Refused

```bash
MQDB_BROKER=mqtt://localhost:9999 mqdb list users
```

**Expected error:**
```
error: connection refused: mqtt://localhost:9999
```

### Invalid Filter Syntax

```bash
mqdb list users --filter 'invalid'
```

**Expected error:**
```
error: invalid filter syntax: 'invalid'
```

### Constraint Violation

```bash
mqdb constraint add users --not-null email
mqdb create users --data '{"name": "No Email"}'
```

**Expected error:**
```
error: not_null constraint violation: field 'email' cannot be null
```

---

## Quick Test Checklist

Run through these commands to validate core functionality:

```bash
# 1. Start agent
mqdb agent start --db ./data/quicktest

# 2. CRUD operations
mqdb create users --data '{"name": "Test User", "email": "test@example.com"}'
# Note the returned ID (e.g., abc123)
mqdb read users <id>
mqdb update users <id> --data '{"age": 25}'
mqdb list users
mqdb delete users <id>

# 3. Filtering and sorting
mqdb create items --data '{"name": "A", "value": 10}'
mqdb create items --data '{"name": "B", "value": 20}'
mqdb create items --data '{"name": "C", "value": 5}'
mqdb list items --filter 'value>5' --sort value:desc

# 4. Schema validation
echo '{"name": {"type": "string", "required": true}}' > /tmp/schema.json
mqdb schema set validated --file /tmp/schema.json
mqdb create validated --data '{}'  # Should fail

# 5. Constraints
mqdb constraint add items --unique name
mqdb create items --data '{"name": "A", "value": 100}'  # Should fail (duplicate name)

# 6. Subscriptions (in separate terminal)
# Terminal 3: mqdb watch items
# Terminal 2: mqdb create items --data '{"name": "D", "value": 15}'

# 7. Consumer groups
mqdb consumer-group list

# 8. Backup
mqdb backup create
mqdb backup list

# 9. Cleanup
rm -rf ./data/quicktest
```

---

## Troubleshooting

### Agent Won't Start

1. Check if MQTT broker is running:
   ```bash
   nc -zv localhost 1883
   ```

2. Verify database path is writable:
   ```bash
   mkdir -p ./data/testdb && touch ./data/testdb/.test && rm ./data/testdb/.test
   ```

3. Check for port conflicts or existing agent:
   ```bash
   mqdb agent status
   ```

### Connection Timeout

1. Verify broker URL:
   ```bash
   echo $MQDB_BROKER
   ```

2. Test broker connectivity:
   ```bash
   mosquitto_pub -h localhost -p 1883 -t test -m "ping"
   ```

### Authentication Failures

1. Verify credentials are set:
   ```bash
   echo $MQDB_USER $MQDB_PASS
   ```

2. Check password file exists and is readable:
   ```bash
   cat passwd.txt
   ```

### Subscription Not Receiving Events

1. Verify agent is running:
   ```bash
   mqdb agent status
   ```

2. Check consumer group membership:
   ```bash
   mqdb consumer-group show <group-name>
   ```

3. Verify heartbeat is active (check agent logs)

### Foreign Key Errors

1. Verify referenced entity exists:
   ```bash
   mqdb read <referenced-entity> <referenced-id>
   ```

2. Check constraint configuration:
   ```bash
   mqdb constraint list <entity>
   ```

---

## 11. Cluster Mode

Cluster mode runs a distributed MQDB with Raft consensus for partition management.

> **Note:** Standard CRUD operations (`mqdb create/read/update/delete/list`) now work in cluster mode.
> The cluster automatically routes operations to the appropriate partition.
> For direct partition access, use the low-level `mqdb db` commands (Section 12).

### Starting a Single-Node Cluster

```bash
mqdb cluster start --node-id 1 --bind 127.0.0.1:1883 --db /tmp/mqdb-node1 --no-quic
```

**Expected output:**
```
cluster node started node_id=1 node_name=node-1 bind=127.0.0.1:1883 peers=[]
became Raft leader node=1
Raft leader initializing partition assignments
```

The node should:
- Become Raft leader immediately (single-node quorum)
- Assign all 64 partitions to itself

### Starting a Multi-Node Cluster

**Terminal 1 (Node 1):**
```bash
mqdb cluster start \
  --node-id 1 \
  --bind 127.0.0.1:1883 \
  --db /tmp/mqdb-node1 \
  --peers 2@127.0.0.1:1884,3@127.0.0.1:1885 \
  --no-quic
```

**Terminal 2 (Node 2):**
```bash
mqdb cluster start \
  --node-id 2 \
  --bind 127.0.0.1:1884 \
  --db /tmp/mqdb-node2 \
  --peers 1@127.0.0.1:1883,3@127.0.0.1:1885 \
  --no-quic
```

**Terminal 3 (Node 3):**
```bash
mqdb cluster start \
  --node-id 3 \
  --bind 127.0.0.1:1885 \
  --db /tmp/mqdb-node3 \
  --peers 1@127.0.0.1:1883,2@127.0.0.1:1884 \
  --no-quic
```

**Expected behavior:**
- One node wins Raft election and becomes leader
- Leader assigns partitions across all nodes
- Bridges establish connections between nodes

### Cluster with QUIC Transport

First, generate TLS certificates:
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
  --quic-key test_certs/server.key
```

### Cluster Options Reference

| Option | Description |
|--------|-------------|
| `--node-id` | Unique node ID (1-65535, required) |
| `--node-name` | Human-readable node name (optional) |
| `--bind` | MQTT listener address (default: 0.0.0.0:1883) |
| `--db` | Database directory path (required) |
| `--peers` | Peer nodes in format `id@host:port` (comma-separated) |
| `--passwd` | Path to password file |
| `--acl` | Path to ACL file |
| `--quic-cert` | TLS certificate for QUIC transport |
| `--quic-key` | TLS private key for QUIC transport |
| `--no-quic` | Disable QUIC (use TCP bridges only) |
| `--no-persist-stores` | Disable store persistence (data lost on restart) |

### Testing Without Persistence

For quick tests where data doesn't need to survive restarts:

```bash
mqdb cluster start --node-id 1 --bind 127.0.0.1:1883 --db /tmp/mqdb-test \
  --no-quic --no-persist-stores
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
  "partitions": 64
}
```

### Trigger Rebalance

```bash
mqdb cluster rebalance --broker 127.0.0.1:1883
```

### Cluster CRUD Operations

Standard CRUD commands work in cluster mode with automatic partition routing:

```bash
# Start a single-node cluster
mqdb cluster start --node-id 1 --bind 127.0.0.1:1883 --db /tmp/mqdb-cluster \
  --quic-cert test_certs/server.pem --quic-key test_certs/server.key &
sleep 3

# Create - automatically routed to a partition
mqdb create users --data '{"name": "Alice", "email": "alice@example.com"}' --broker 127.0.0.1:1883
# Returns: {"status":"ok","data":{"entity":"users","id":"<generated-id>","data":{...}}}

# Read
mqdb read users <id> --broker 127.0.0.1:1883

# Update
mqdb update users <id> --data '{"name": "Alice Updated", "age": 30}' --broker 127.0.0.1:1883

# List with filter
mqdb list users --broker 127.0.0.1:1883
mqdb list users --filter "name=Alice Updated" --broker 127.0.0.1:1883

# Delete
mqdb delete users <id> --broker 127.0.0.1:1883

# Cleanup
pkill -f "mqdb cluster"
```

---

## 12. Cluster DB Debug Commands

The `mqdb db` command provides low-level access to cluster-mode database operations using the binary BeBytes protocol.

### Create Entity

```bash
mqdb db create -p <partition> -e <entity> -d '<json-data>'
```

**Example:**
```bash
mqdb db create -p 0 -e users -d '{"name": "Alice", "email": "alice@example.com"}'
```

**Expected output:**
```
Created: users a1b2c3d4e5f6-0001 {"name": "Alice", "email": "alice@example.com"}
```

The ID is auto-generated based on the partition.

### Read Entity

```bash
mqdb db read -p <partition> -e <entity> -i <id>
```

**Example:**
```bash
mqdb db read -p 0 -e users -i a1b2c3d4e5f6-0001
```

**Expected output:**
```
users a1b2c3d4e5f6-0001 {"name": "Alice", "email": "alice@example.com"}
```

### Update Entity

```bash
mqdb db update -p <partition> -e <entity> -i <id> -d '<json-data>'
```

**Example:**
```bash
mqdb db update -p 0 -e users -i a1b2c3d4e5f6-0001 -d '{"name": "Alice Updated", "email": "alice@example.com"}'
```

**Expected output:**
```
Updated: users a1b2c3d4e5f6-0001 {"name": "Alice Updated", "email": "alice@example.com"}
```

### Delete Entity

```bash
mqdb db delete -p <partition> -e <entity> -i <id>
```

**Example:**
```bash
mqdb db delete -p 0 -e users -i a1b2c3d4e5f6-0001
```

**Expected output:**
```
Deleted: users/a1b2c3d4e5f6-0001
```

### Error Cases

**Not found:**
```bash
mqdb db read -p 0 -e users -i nonexistent
```
**Output:** `Not found`

**Already exists (create duplicate):**
```bash
mqdb db create -p 0 -e users -d '{"name": "Test"}'
# Note the ID from output, then try to create same partition again
# IDs are unique per create, so duplicates are rare
```

**Invalid partition:**
```bash
mqdb db read -p 99 -e users -i test-id
```
**Output:** `Error: InvalidPartition`

### Full CRUD Workflow Test

```bash
# Start a single-node cluster
mqdb cluster start --node-id 1 --bind 127.0.0.1:1883 --db /tmp/mqdb-test --no-quic &
sleep 3

# Create
mqdb db create -p 0 -e products -d '{"name": "Widget", "price": 99}'
# Note the ID from output (e.g., abc123-0001)

# Read
mqdb db read -p 0 -e products -i abc123-0001

# Update
mqdb db update -p 0 -e products -i abc123-0001 -d '{"name": "Widget Pro", "price": 149}'

# Verify update
mqdb db read -p 0 -e products -i abc123-0001

# Delete
mqdb db delete -p 0 -e products -i abc123-0001

# Verify deletion
mqdb db read -p 0 -e products -i abc123-0001
# Should output: Not found

# Cleanup
pkill -f "mqdb cluster"
rm -rf /tmp/mqdb-test
```

### Testing Across Partitions

Create entities on different partitions:

```bash
mqdb db create -p 0 -e users -d '{"name": "User P0"}'
mqdb db create -p 1 -e users -d '{"name": "User P1"}'
mqdb db create -p 31 -e users -d '{"name": "User P31"}'
mqdb db create -p 63 -e users -d '{"name": "User P63"}'
```

Each partition can be independently queried.

---

## 13. Multi-Node Cluster Testing

### Setup 3-Node Cluster

```bash
# Clean up any previous data
rm -rf /tmp/mqdb-node{1,2,3}

# Terminal 1
mqdb cluster start --node-id 1 --bind 127.0.0.1:1883 --db /tmp/mqdb-node1 \
  --peers 2@127.0.0.1:1884,3@127.0.0.1:1885 --no-quic

# Terminal 2
mqdb cluster start --node-id 2 --bind 127.0.0.1:1884 --db /tmp/mqdb-node2 \
  --peers 1@127.0.0.1:1883,3@127.0.0.1:1885 --no-quic

# Terminal 3
mqdb cluster start --node-id 3 --bind 127.0.0.1:1885 --db /tmp/mqdb-node3 \
  --peers 1@127.0.0.1:1883,2@127.0.0.1:1884 --no-quic
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
mqdb db create -p 0 -e items -d '{"via": "node1"}' --broker 127.0.0.1:1883

# Via node 2
mqdb db create -p 0 -e items -d '{"via": "node2"}' --broker 127.0.0.1:1884

# Via node 3
mqdb db create -p 0 -e items -d '{"via": "node3"}' --broker 127.0.0.1:1885
```

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
pkill -f "mqdb cluster"
rm -rf /tmp/mqdb-node{1,2,3}
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
# Start 3-node cluster (default)
mqdb dev start-cluster

# Start with custom node count
mqdb dev start-cluster --nodes 5

# Clean existing data first
mqdb dev start-cluster --clean

# Without QUIC transport
mqdb dev start-cluster --no-quic
```

### Run Built-in Tests

```bash
# Run pub/sub tests
mqdb dev test --pubsub

# Run database tests
mqdb dev test --db

# Specify node count
mqdb dev test --nodes 3
```

---

## 15. Benchmarking

The `mqdb bench` commands measure performance.

### Pub/Sub Benchmark

```bash
mqdb bench pubsub \
  --publishers 4 \
  --subscribers 4 \
  --messages 10000 \
  --size 256 \
  --qos 1 \
  --topic "bench/test"
```

**Options:**

| Option | Description | Default |
|--------|-------------|---------|
| `--publishers` | Number of publisher tasks | 1 |
| `--subscribers` | Number of subscriber tasks | 1 |
| `--messages` | Total messages to send | 10000 |
| `--rate` | Messages per second (0 = unlimited) | 0 |
| `--size` | Payload size in bytes | 64 |
| `--qos` | MQTT QoS level (0, 1, or 2) | 0 |
| `--topic` | Topic pattern | bench/test |
| `--warmup` | Warmup messages before measuring | (none) |

**Expected output:**
```
Pub/Sub Benchmark Results
─────────────────────────
Messages sent:     10000
Messages received: 10000
Duration:          2.34s
Throughput:        4273 msg/s
Latency (p50):     1.2ms
Latency (p99):     8.5ms
```

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
| `--operations` | Number of operations | 1000 |
| `--entity` | Entity name to use | bench_entity |
| `--op` | Operation type (insert, get, update, delete, list, mixed) | mixed |
| `--concurrency` | Concurrent clients | 1 |
| `--fields` | Fields per record | 5 |
| `--field-size` | Size of each field value | 100 |
| `--warmup` | Warmup operations | (none) |
| `--cleanup` | Delete test data after | false |

**Expected output:**
```
Database Benchmark Results
──────────────────────────
Operations:    1000
Duration:      1.56s
Throughput:    641 ops/s
Latency (p50): 1.4ms
Latency (p99): 12.3ms

By Operation:
  insert: 234 ops, 2.1ms avg
  get:    312 ops, 0.8ms avg
  update: 267 ops, 1.9ms avg
  delete: 187 ops, 1.2ms avg
```

---

## 16. Health Endpoint Testing

The `$DB/_health` topic provides cluster and node health status.

### Subscribe to Health Status

**Terminal 3:**
```bash
mosquitto_sub -h 127.0.0.1 -p 1883 -t '$DB/_health' -v
```

**Terminal 2:**
```bash
mosquitto_pub -h 127.0.0.1 -p 1883 -t '$DB/_health' -m ''
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
    "partition_count": 64,
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
mosquitto_pub -h 127.0.0.1 -p 1884 -t '$DB/_health' -m ''
```

**Expected:** `alive_nodes` should show `[2, 3]` and a new leader elected.

---

## 17. Cluster Resilience Testing

These tests verify data survives failures and replication works correctly.

### Test 1: Data Persistence on Restart

Verify data survives a full cluster restart.

```bash
# 1. Start cluster
mqdb dev start-cluster --nodes 3 --clean

# 2. Create test data
mqdb create users --data '{"name": "Persist Test", "value": 123}' --broker 127.0.0.1:1883
# Note the returned ID

# 3. Verify data exists
mqdb read users <id> --broker 127.0.0.1:1883

# 4. Stop all nodes
mqdb dev kill --all

# 5. Restart cluster (without --clean)
mqdb dev start-cluster --nodes 3

# 6. Verify data survived
mqdb read users <id> --broker 127.0.0.1:1883
```

**Expected:** Data should be readable after restart.

### Test 2: Replication Verification

Verify data exists on replica nodes.

```bash
# 1. Start cluster
mqdb dev start-cluster --nodes 3 --clean

# 2. Create data via Node 1
mqdb create test_repl --data '{"key": "replication_test"}' --broker 127.0.0.1:1883
# Note the returned ID

# 3. Read from Node 1
mqdb read test_repl <id> --broker 127.0.0.1:1883

# 4. Read from Node 2
mqdb read test_repl <id> --broker 127.0.0.1:1884

# 5. Read from Node 3
mqdb read test_repl <id> --broker 127.0.0.1:1885
```

**Expected:** All nodes should return the data (replicated across cluster).

### Test 3: Primary Failure with Replica Takeover

Verify replica becomes primary when original primary dies.

```bash
# 1. Start cluster
mqdb dev start-cluster --nodes 3 --clean

# 2. Create multiple entities across partitions
for i in {1..10}; do
  mqdb create failover_test --data "{\"index\": $i}" --broker 127.0.0.1:1883
done

# 3. Note the IDs and verify all readable
mqdb list failover_test --broker 127.0.0.1:1883

# 4. Kill Node 1
mqdb dev kill --node 1

# 5. Wait for failover (2-3 seconds)
sleep 3

# 6. Verify all data still accessible via Node 2
mqdb list failover_test --broker 127.0.0.1:1884

# 7. Verify count matches
```

**Expected:** All 10 entities should still be readable from surviving nodes.

### Test 4: Network Partition Simulation

```bash
# 1. Start 3-node cluster
mqdb dev start-cluster --nodes 3 --clean

# 2. Create data
mqdb create partition_test --data '{"before": "partition"}' --broker 127.0.0.1:1883

# 3. Simulate partition by killing Node 3
mqdb dev kill --node 3

# 4. Create more data (should succeed with 2 nodes)
mqdb create partition_test --data '{"during": "partition"}' --broker 127.0.0.1:1883

# 5. Restart Node 3
mqdb cluster start --node-id 3 --bind 127.0.0.1:1885 --db /tmp/mqdb-test-3 \
  --peers 1@127.0.0.1:1883 --no-quic &

# 6. Wait for rejoin
sleep 5

# 7. Verify Node 3 has all data
mqdb list partition_test --broker 127.0.0.1:1885
```

**Expected:** Node 3 should catch up and have all data after rejoining.

---

## 18. MQTT Protocol Cluster Testing

These tests verify MQTT protocol features work correctly across cluster nodes.

### Test 1: QoS 1 Cross-Node Delivery

```bash
# Terminal 1: Start cluster
mqdb dev start-cluster --nodes 2 --clean

# Terminal 2: Subscribe on Node 2 with QoS 1
mosquitto_sub -h 127.0.0.1 -p 1884 -t "qos1/test" -q 1 -v

# Terminal 3: Publish to Node 1 with QoS 1
mosquitto_pub -h 127.0.0.1 -p 1883 -t "qos1/test" -m "QoS1 message" -q 1
```

**Expected:** Subscriber on Node 2 receives the message exactly once.

### Test 2: QoS 2 Cross-Node Delivery

```bash
# Terminal 2: Subscribe on Node 2 with QoS 2
mosquitto_sub -h 127.0.0.1 -p 1884 -t "qos2/test" -q 2 -v -C 5

# Terminal 3: Publish 5 messages with QoS 2
for i in {1..5}; do
  mosquitto_pub -h 127.0.0.1 -p 1883 -t "qos2/test" -m "Message $i" -q 2
done
```

**Expected:** Exactly 5 messages received, no duplicates.

### Test 3: Retained Messages Across Nodes

```bash
# 1. Publish retained message to Node 1
mosquitto_pub -h 127.0.0.1 -p 1883 -t "retained/test" -m "Retained content" -r

# 2. New subscriber on Node 2 should receive it
mosquitto_sub -h 127.0.0.1 -p 1884 -t "retained/test" -v -C 1
```

**Expected:** New subscriber immediately receives the retained message.

### Test 4: Retained Message Survives Node Failure

```bash
# 1. Publish retained message
mosquitto_pub -h 127.0.0.1 -p 1883 -t "retained/failover" -m "Survives restart" -r

# 2. Kill Node 1
mqdb dev kill --node 1

# 3. Subscribe on Node 2
mosquitto_sub -h 127.0.0.1 -p 1884 -t "retained/failover" -v -C 1
```

**Expected:** Retained message still delivered from replica.

### Test 5: Wildcard Subscriptions Cross-Node

```bash
# Terminal 2: Subscribe with wildcard on Node 2
mosquitto_sub -h 127.0.0.1 -p 1884 -t "sensors/+/temperature" -v

# Terminal 3: Publish to matching topics from Node 1
mosquitto_pub -h 127.0.0.1 -p 1883 -t "sensors/room1/temperature" -m "22.5"
mosquitto_pub -h 127.0.0.1 -p 1883 -t "sensors/room2/temperature" -m "23.1"
mosquitto_pub -h 127.0.0.1 -p 1883 -t "sensors/room3/temperature" -m "21.8"
```

**Expected:** All 3 messages received by wildcard subscriber.

### Test 6: Multi-Level Wildcard (#)

```bash
# Terminal 2: Subscribe with # wildcard
mosquitto_sub -h 127.0.0.1 -p 1884 -t "events/#" -v

# Terminal 3: Publish to various sub-topics
mosquitto_pub -h 127.0.0.1 -p 1883 -t "events/user/login" -m "user1"
mosquitto_pub -h 127.0.0.1 -p 1883 -t "events/user/logout" -m "user2"
mosquitto_pub -h 127.0.0.1 -p 1883 -t "events/system/startup" -m "node1"
```

**Expected:** All 3 messages received.

### Test 7: Last Will Testament (LWT) Cross-Node

```bash
# Terminal 2: Subscribe to LWT topic on Node 2
mosquitto_sub -h 127.0.0.1 -p 1884 -t "clients/status" -v

# Terminal 3: Connect client with LWT to Node 1
mosquitto_sub -h 127.0.0.1 -p 1883 -t "dummy" \
  --will-topic "clients/status" \
  --will-payload "client123 disconnected" \
  --will-qos 1 \
  -i client123

# Terminal 4: Kill the client ungracefully
pkill -9 -f "mosquitto_sub.*client123"
```

**Expected:** Terminal 2 receives "client123 disconnected" after ungraceful disconnect.

### Test 8: Session Persistence Across Failover

```bash
# 1. Connect with persistent session and subscribe on Node 1
mosquitto_sub -h 127.0.0.1 -p 1883 -t "session/test" -q 1 \
  -i persistent-client --disable-clean-session &
SUB_PID=$!

# 2. Disconnect client
kill $SUB_PID

# 3. Publish while client is disconnected
mosquitto_pub -h 127.0.0.1 -p 1883 -t "session/test" -m "Queued message" -q 1

# 4. Kill Node 1
mqdb dev kill --node 1

# 5. Reconnect same client to Node 2
mosquitto_sub -h 127.0.0.1 -p 1884 -t "session/test" -q 1 \
  -i persistent-client --disable-clean-session -C 1
```

**Expected:** Client receives the queued message after reconnecting to different node.

### Test 9: Message Deduplication Under Load

```bash
# Terminal 2: Subscribe and count messages
mosquitto_sub -h 127.0.0.1 -p 1884 -t "dedup/test" -C 100 | wc -l

# Terminal 3: Rapidly publish 100 messages
for i in {1..100}; do
  mosquitto_pub -h 127.0.0.1 -p 1883 -t "dedup/test" -m "msg$i" &
done
wait
```

**Expected:** Exactly 100 messages received (no duplicates).

---

## 19. Constraints in Cluster Mode

Verify database constraints work across cluster nodes.

### Unique Constraint Across Nodes

```bash
# 1. Start cluster
mqdb dev start-cluster --nodes 2 --clean

# 2. Add unique constraint
mqdb constraint add cluster_users --unique email --broker 127.0.0.1:1883

# 3. Create entity on Node 1
mqdb create cluster_users --data '{"name": "Alice", "email": "alice@test.com"}' \
  --broker 127.0.0.1:1883

# 4. Try duplicate on Node 2 (should fail)
mqdb create cluster_users --data '{"name": "Bob", "email": "alice@test.com"}' \
  --broker 127.0.0.1:1884
```

**Expected:** Second create fails with unique constraint violation.

### Foreign Key Across Nodes

```bash
# 1. Create parent entity on Node 1
mqdb create authors --data '{"name": "Jane"}' --broker 127.0.0.1:1883
# Note the ID (e.g., author-123)

# 2. Add FK constraint
mqdb constraint add books --fk "author_id:authors:id:restrict" --broker 127.0.0.1:1883

# 3. Create child with valid FK on Node 2
mqdb create books --data '{"title": "Book 1", "author_id": "author-123"}' \
  --broker 127.0.0.1:1884

# 4. Try invalid FK on Node 2 (should fail)
mqdb create books --data '{"title": "Book 2", "author_id": "nonexistent"}' \
  --broker 127.0.0.1:1884
```

**Expected:** Valid FK succeeds, invalid FK fails with constraint violation.

---

## Complete Verification Checklist

Run through this checklist to verify MQDB works completely:

### Agent Mode
- [ ] Agent start/stop
- [ ] CRUD operations (create, read, update, delete, list)
- [ ] Filtering and sorting
- [ ] Schema validation
- [ ] Constraints (unique, not-null, foreign key)
- [ ] Watch/Subscribe
- [ ] Consumer groups
- [ ] Backup/Restore

### Cluster Mode
- [ ] Single-node cluster start
- [ ] Multi-node cluster formation
- [ ] Raft leader election
- [ ] Partition assignment (64 partitions distributed)
- [ ] Cross-node data routing
- [ ] Cluster status command
- [ ] Cluster CRUD (create, read, update, delete, list)
- [ ] Cluster list with filters

### Cluster Resilience
- [ ] Data persistence on restart
- [ ] Replication verification
- [ ] Primary failure with replica takeover
- [ ] Node rejoin and catch-up

### MQTT Protocol (Cluster)
- [ ] QoS 0 cross-node delivery
- [ ] QoS 1 cross-node delivery
- [ ] QoS 2 cross-node delivery (exactly-once)
- [ ] Retained messages across nodes
- [ ] Retained message survives node failure
- [ ] Single-level wildcard (+) cross-node
- [ ] Multi-level wildcard (#) cross-node
- [ ] LWT cross-node delivery
- [ ] Session persistence across failover
- [ ] No message duplication under load

### Constraints (Cluster)
- [ ] Unique constraint enforced across nodes
- [ ] Foreign key validated across nodes

### Monitoring
- [ ] Health endpoint returns correct status
- [ ] Health updates on node failure

### Performance
- [ ] Pub/sub benchmark runs successfully
- [ ] Database benchmark runs successfully
