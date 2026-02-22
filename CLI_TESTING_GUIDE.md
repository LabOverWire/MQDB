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
  "status": "ok",
  "entity": "users",
  "id": "18c09201f3ac5801-0039",
  "data": {
    "name": "Alice",
    "email": "alice@example.com",
    "age": 30
  }
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
  "status": "ok",
  "entity": "users",
  "id": "<id>",
  "data": {
    "name": "Alice",
    "email": "alice@example.com",
    "age": 30
  }
}
```

### Update Entity

```bash
mqdb update users <id> --data '{"age": 25}'
```

**Expected output:**
```json
{
  "status": "ok",
  "entity": "users",
  "id": "<id>",
  "data": {
    "name": "Alice",
    "email": "alice@example.com",
    "age": 25
  }
}
```

### Delete Entity

```bash
mqdb delete users <id>
```

**Expected output:**
```json
{
  "status": "ok",
  "entity": "users",
  "id": "<id>",
  "deleted": true
}
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

> **Note:** IDs are auto-generated if not provided. To use a client-provided ID, include `"id"` in the payload:
> `mqdb create products --data '{"id": "my-uuid", "name": "Monitor", "price": 499}'`
> If no `"id"` field is present, the server generates one (sequential in agent mode, hash-based in cluster mode).

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

### Unique Constraint on Update (Conflict)

```bash
mqdb update users <user1-id> --data '{"email": "unique@test.com"}'
```

**Expected error:**
```
error: unique constraint violation on field 'email'
```

The update is rejected because another entity already has that email value.

### Unique Constraint on Update (Non-Unique Field Change)

```bash
mqdb update users <user1-id> --data '{"name": "New Name"}'
```

**Expected:** Success. Changing a non-unique field does not trigger unique constraint checks.

### Unique Constraint on Update (Value Recycling)

```bash
# Change user1's email away from its original value
mqdb update users <user1-id> --data '{"email": "different@test.com"}'

# Now create a new user with the old email — should succeed
mqdb create users --data '{"name": "User3", "email": "unique@test.com"}'
```

**Expected:** Both operations succeed. The old unique value is released when the update changes it.

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

### Cascade Delete with ChangeEvents

**Terminal 1 (subscriber):**
```bash
mqdb watch posts
```

**Terminal 2:**
```bash
mqdb create authors --data '{"name": "Alice"}'
# Note the returned ID (e.g., 1)
mqdb constraint add posts --fk "author_id:authors:id:cascade"
mqdb create posts --data '{"title": "Post 1", "author_id": "1"}'
mqdb create posts --data '{"title": "Post 2", "author_id": "1"}'

mqdb delete authors 1
```

**Expected in Terminal 1:** Delete events for each cascaded post:
```
Delete posts/1
Delete posts/2
```

### Set Null

Setup:
```bash
mqdb create authors --data '{"name": "Alice"}'
# Note the returned ID (e.g., 1)
mqdb constraint add posts --fk "author_id:authors:id:set_null"
mqdb create posts --data '{"title": "Post 1", "author_id": "1"}'
mqdb create posts --data '{"title": "Post 2", "author_id": "1"}'
```

Test:
```bash
mqdb delete authors 1
mqdb list posts
```

**Expected:** Posts still exist with `author_id: null` and `_version: 2`

### Set Null with ChangeEvents

**Terminal 1 (subscriber):**
```bash
mqdb watch posts
```

**Terminal 2:**
```bash
mqdb create authors --data '{"name": "Bob"}'
# Note the returned ID (e.g., 1)
mqdb constraint add posts --fk "author_id:authors:id:set_null"
mqdb create posts --data '{"title": "Post 1", "author_id": "1"}'
mqdb create posts --data '{"title": "Post 2", "author_id": "1"}'

mqdb delete authors 1
```

**Expected in Terminal 1:** Update events for each set-null post showing `author_id: null`

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
- Assign all 256 partitions to itself

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
# Start a single-node cluster
mqdb cluster start --node-id 1 --bind 127.0.0.1:1883 --db /tmp/mqdb-cluster \
  --quic-cert test_certs/server.pem --quic-key test_certs/server.key &
sleep 3

# Create - automatically routed to a partition
mqdb create users --data '{"name": "Alice", "email": "alice@example.com"}' --broker 127.0.0.1:1883
# Returns: {"status":"ok","entity":"users","id":"<generated-id>","data":{...}}

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

### Cross-Partition JSON Create Forwarding

Verify that JSON creates on non-local partitions are forwarded to the correct primary node:

```bash
# Start 3-node cluster
mqdb dev start-cluster --nodes 3 --clean
sleep 5

# Create entities on each node (with 256 partitions across 3 nodes,
# ~2/3 of creates will hit non-local partitions and require forwarding)
mqdb create testfw -d '{"name":"from-node1"}' --broker 127.0.0.1:1883
mqdb create testfw -d '{"name":"from-node2"}' --broker 127.0.0.1:1884
mqdb create testfw -d '{"name":"from-node3"}' --broker 127.0.0.1:1885

# Create more records to ensure forwarding paths are exercised
for i in $(seq 1 10); do
  mqdb create testfw -d "{\"seq\":$i}" --broker 127.0.0.1:1884
done

# Verify all records are visible from any node
mqdb list testfw --broker 127.0.0.1:1883
mqdb list testfw --broker 127.0.0.1:1885

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
# Run all cross-node tests (excludes ownership - see below)
mqdb dev test --all

# Run specific test suites
mqdb dev test --pubsub      # Cross-node pub/sub matrix
mqdb dev test --db          # Cross-node DB CRUD
mqdb dev test --wildcards   # Wildcard subscriptions
mqdb dev test --retained    # Retained messages
mqdb dev test --lwt         # Last Will & Testament
mqdb dev test --ownership   # Ownership enforcement (self-contained, see Section 22)

# Specify node count
mqdb dev test --all --nodes 5
```

> **Note:** `--ownership` is excluded from `--all` because it manages its own authenticated cluster.
> It kills any running cluster, starts a fresh one with password auth and `--ownership` config,
> runs ownership tests, then kills the cluster.

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

### Unique Constraint on Update Across Nodes (Conflict)

```bash
# 1. Start cluster
mqdb dev start-cluster --nodes 2 --clean

# 2. Add unique constraint
mqdb constraint add cluster_users --unique email --broker 127.0.0.1:1883

# 3. Create two entities on different nodes
mqdb create cluster_users --data '{"name": "Alice", "email": "alice@test.com"}' \
  --broker 127.0.0.1:1883
# Note Alice's ID

mqdb create cluster_users --data '{"name": "Bob", "email": "bob@test.com"}' \
  --broker 127.0.0.1:1884
# Note Bob's ID

# 4. Update Bob's email to conflict with Alice (should fail)
mqdb update cluster_users <bob-id> --data '{"email": "alice@test.com"}' \
  --broker 127.0.0.1:1884
```

**Expected:** Update fails with 409 unique constraint violation.

### Unique Constraint on Update Across Nodes (Value Recycling)

```bash
# 1. Using same cluster and entities from above

# 2. Update Alice's email to a new value
mqdb update cluster_users <alice-id> --data '{"email": "alice-new@test.com"}' \
  --broker 127.0.0.1:1883

# 3. Now update Bob's email to Alice's OLD value (should succeed)
mqdb update cluster_users <bob-id> --data '{"email": "alice@test.com"}' \
  --broker 127.0.0.1:1884
```

**Expected:** Update succeeds — the old unique value was released when Alice changed her email.

### Foreign Key Across Nodes

```bash
# 1. Create parent entity on Node 1
mqdb create authors --data '{"name": "Jane"}' --broker 127.0.0.1:1883
# Note the ID (e.g., 1)

# 2. Add FK constraint (restrict)
mqdb constraint add books --fk "author_id:authors:id:restrict" --broker 127.0.0.1:1883

# 3. Create child with valid FK on Node 2
mqdb create books --data '{"title": "Book 1", "author_id": "1"}' \
  --broker 127.0.0.1:1884

# 4. Try invalid FK on Node 2 (should fail)
mqdb create books --data '{"title": "Book 2", "author_id": "nonexistent"}' \
  --broker 127.0.0.1:1884
```

**Expected:** Valid FK succeeds, invalid FK fails with constraint violation.

### Cascade Delete Across Nodes

```bash
# 1. Create parent on Node 1
mqdb create authors --data '{"name": "Alice"}' --broker 127.0.0.1:1883
# Note the ID (e.g., 1)

# 2. Add cascade FK
mqdb constraint add posts --fk "author_id:authors:id:cascade" --broker 127.0.0.1:1883

# 3. Create children on different nodes (some may land on different partitions)
mqdb create posts --data '{"title": "Post 1", "author_id": "1"}' --broker 127.0.0.1:1884
mqdb create posts --data '{"title": "Post 2", "author_id": "1"}' --broker 127.0.0.1:1885
mqdb create posts --data '{"title": "Post 3", "author_id": "1"}' --broker 127.0.0.1:1883

# 4. Delete parent
mqdb delete authors 1 --broker 127.0.0.1:1883

# 5. Verify all posts deleted
mqdb list posts --broker 127.0.0.1:1883
```

**Expected:** All posts are cascade deleted across all nodes.

### Set Null Across Nodes

```bash
# 1. Create parent on Node 1
mqdb create authors --data '{"name": "Bob"}' --broker 127.0.0.1:1883
# Note the ID (e.g., 1)

# 2. Add set_null FK
mqdb constraint add posts --fk "author_id:authors:id:set_null" --broker 127.0.0.1:1883

# 3. Create children
mqdb create posts --data '{"title": "Post 1", "author_id": "1"}' --broker 127.0.0.1:1884
mqdb create posts --data '{"title": "Post 2", "author_id": "1"}' --broker 127.0.0.1:1885

# 4. Delete parent
mqdb delete authors 1 --broker 127.0.0.1:1883

# 5. Verify posts still exist with null FK
mqdb list posts --broker 127.0.0.1:1883
```

**Expected:** Posts still exist with `author_id: null` and `_version: 2`.

---

## 20. Cluster Database Features

These tests verify database-specific features work correctly in cluster mode.

### Test 1: UPDATE Merges Fields (Not Replace)

Verify that UPDATE only modifies specified fields, preserving others.

```bash
# 1. Start cluster
mqdb dev start-cluster --nodes 3 --clean

# 2. Create entity with multiple fields
mqdb create products --data '{"name": "Widget", "price": 100, "stock": 50, "category": "electronics"}' \
  --broker 127.0.0.1:1883
# Note the returned ID (e.g., abc123-0042)

# 3. Update only price field
mqdb update products <id> --data '{"price": 150}' --broker 127.0.0.1:1883

# 4. Read and verify ALL fields preserved
mqdb read products <id> --broker 127.0.0.1:1883
```

**Expected:** Response shows `price: 150` AND all other fields (`name`, `stock`, `category`) preserved.

```json
{
  "status": "ok",
  "entity": "products",
  "id": "<id>",
  "data": {
    "name": "Widget",
    "price": 150,
    "stock": 50,
    "category": "electronics"
  }
}
```

### Test 2: UPDATE Adds New Fields

```bash
# 1. Add a new field to existing entity
mqdb update products <id> --data '{"discount": true}' --broker 127.0.0.1:1883

# 2. Read and verify new field added
mqdb read products <id> --broker 127.0.0.1:1883
```

**Expected:** Entity now has `discount: true` plus all original fields.

### Test 3: Schema Commands in Cluster Mode

Verify schema set/get works across cluster nodes.

```bash
# 1. Start cluster
mqdb dev start-cluster --nodes 3 --clean

# 2. Create schema file
echo '{"type": "object", "properties": {"title": {"type": "string"}}}' > /tmp/articles_schema.json

# 3. Set schema on Node 1
mqdb schema set articles --file /tmp/articles_schema.json --broker 127.0.0.1:1883

# 4. Get schema from Node 2 (verifies broadcast)
mqdb schema get articles --broker 127.0.0.1:1884

# 5. Get schema from Node 3 (verifies broadcast)
mqdb schema get articles --broker 127.0.0.1:1885
```

**Expected:** All nodes return the same schema:
```json
{
  "status": "ok",
  "data": {
    "entity": "articles",
    "schema": {
      "type": "object",
      "properties": {
        "title": {"type": "string"}
      }
    },
    "version": 1
  }
}
```

### Test 4: Schema Update Broadcasts

```bash
# 1. Update schema with new field
echo '{"type": "object", "properties": {"title": {"type": "string"}, "content": {"type": "string"}}}' \
  > /tmp/articles_schema2.json

# 2. Update schema on Node 1
mqdb schema set articles --file /tmp/articles_schema2.json --broker 127.0.0.1:1883

# 3. Verify update on Node 3
mqdb schema get articles --broker 127.0.0.1:1885
```

**Expected:** Node 3 shows updated schema with version 2.

### Test 5: Cross-Node Update Merge

Verify UPDATE merge works when connecting to different nodes.

```bash
# 1. Create on Node 1
mqdb create items --data '{"a": 1, "b": 2, "c": 3}' --broker 127.0.0.1:1883
# Note ID

# 2. Update via Node 2
mqdb update items <id> --data '{"b": 20}' --broker 127.0.0.1:1884

# 3. Read from Node 3
mqdb read items <id> --broker 127.0.0.1:1885
```

**Expected:** `{"a": 1, "b": 20, "c": 3}` - only `b` changed.

---

## Complete Verification Checklist

Run through this checklist to verify MQDB works completely:

### Agent Mode
- [ ] Agent start/stop
- [ ] CRUD operations (create, read, update, delete, list)
- [ ] Filtering and sorting
- [ ] Schema validation
- [ ] Constraints (unique, not-null, foreign key)
- [ ] Unique constraint enforced on updates (conflict returns 409)
- [ ] Unique constraint allows non-unique field updates
- [ ] Unique constraint old value released on update
- [ ] Cascade delete removes children
- [ ] Set-null nullifies FK field (children remain, version bumped)
- [ ] Cascade delete emits Delete ChangeEvents for children
- [ ] Set-null emits Update ChangeEvents for modified children
- [ ] Watch/Subscribe
- [ ] Consumer groups
- [ ] Backup/Restore

### Agent Mode — Index Range Queries
- [ ] Range filters (>, >=, <, <=) on indexed field return correct results
- [ ] Combined range (>= AND <=) on indexed field returns correct range
- [ ] Range on indexed + equality on non-indexed filters correctly
- [ ] Boundary precision (inclusive vs exclusive) correct

### Cluster Mode
- [ ] Single-node cluster start
- [ ] Multi-node cluster formation
- [ ] Raft leader election
- [ ] Partition assignment (256 partitions distributed)
- [ ] Cross-node data routing
- [ ] Cluster status command
- [ ] Cluster CRUD (create, read, update, delete, list)
- [ ] Cluster list with filters
- [ ] UPDATE merges fields (not replace)
- [ ] Schema set broadcasts to all nodes
- [ ] Schema get works on all nodes

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

### Cluster Mode — Index Range Queries
- [ ] Range query returns same results from all nodes
- [ ] Combined range correct from non-owner node
- [ ] Range + non-indexed filter works across nodes
- [ ] Updated records reflected in range queries
- [ ] Deleted records excluded from range queries

### Constraints (Cluster)
- [ ] Unique constraint enforced across nodes (create)
- [ ] Unique constraint enforced across nodes (update conflict returns 409)
- [ ] Unique constraint old value released on update (value recycling)
- [ ] Foreign key validated across nodes
- [ ] Cascade delete removes children across nodes
- [ ] Set-null nullifies FK field across nodes
- [ ] Cascade delete emits ChangeEvents for deleted children
- [ ] Set-null emits Update ChangeEvents for modified children

### Ownership Enforcement
- [ ] List filters by authenticated sender
- [ ] Non-owner update returns 403 Forbidden
- [ ] Non-owner delete returns 403 Forbidden
- [ ] Owner update succeeds
- [ ] Owner delete succeeds
- [ ] Internal (no sender) bypasses ownership
- [ ] Ownership works across cluster nodes

### Monitoring
- [ ] Health endpoint returns correct status
- [ ] Health updates on node failure

### Performance
- [ ] Pub/sub benchmark runs successfully
- [ ] Database benchmark runs successfully

## Authentication & Authorization Testing

### Password Auth (Agent)
```bash
# Generate password file
mqdb passwd admin -b admin123 -f /tmp/passwd.txt
mqdb passwd alice -b alice123 -f /tmp/passwd.txt

# Start agent with password auth
mqdb agent start --db /tmp/mqdb-auth --passwd /tmp/passwd.txt --acl examples/acl.txt

# Test authenticated connection
mosquitto_pub -h 127.0.0.1 -p 1883 -u admin -P admin123 -t test -m hello
mosquitto_sub -h 127.0.0.1 -p 1883 -u admin -P admin123 -t test -C 1

# Test rejected connection (bad password)
mosquitto_pub -h 127.0.0.1 -p 1883 -u admin -P wrong -t test -m hello
```
- [ ] Authenticated client connects successfully
- [ ] Bad password rejected
- [ ] Anonymous connection rejected (no --anonymous flag)

### Password Auth (Cluster)
```bash
mqdb passwd admin -b admin123 -f /tmp/passwd.txt
mqdb cluster start --node-id 1 --db /tmp/mqdb-c1 --bind 127.0.0.1:1883 \
    --passwd /tmp/passwd.txt --acl examples/acl.txt \
    --quic-cert test_certs/server.pem --quic-key test_certs/server.key
```
- [ ] Cluster node starts with password auth
- [ ] Internal service account connects automatically
- [ ] External clients authenticate correctly

### SCRAM-SHA-256 Auth
```bash
# Generate SCRAM credentials
mqdb scram admin -b admin123 -f /tmp/scram.txt
mqdb scram alice -b alice123 -f /tmp/scram.txt

# Start agent with SCRAM auth
mqdb agent start --db /tmp/mqdb-scram --scram-file /tmp/scram.txt

# Delete a SCRAM user
mqdb scram alice -D -f /tmp/scram.txt
```
- [ ] SCRAM credential file generated correctly
- [ ] Agent starts with SCRAM auth configured
- [ ] SCRAM user deletion works

### JWT Auth
```bash
# Generate a test HS256 secret
openssl rand -base64 32 > /tmp/jwt-secret.key

# Start agent with JWT auth
mqdb agent start --db /tmp/mqdb-jwt \
    --jwt-algorithm hs256 --jwt-key /tmp/jwt-secret.key \
    --jwt-issuer myapp --jwt-audience mqdb
```
- [ ] Agent starts with JWT auth configured
- [ ] JWT clock skew setting applied

### Rate Limiting
```bash
# Start with default rate limits (enabled with any auth)
mqdb agent start --db /tmp/mqdb-rl --passwd /tmp/passwd.txt

# Start with custom rate limits
mqdb agent start --db /tmp/mqdb-rl --passwd /tmp/passwd.txt \
    --rate-limit-max-attempts 3 --rate-limit-window-secs 30 --rate-limit-lockout-secs 120

# Start with rate limiting disabled
mqdb agent start --db /tmp/mqdb-rl --passwd /tmp/passwd.txt --no-rate-limit
```
- [ ] Rate limiting enabled by default with auth
- [ ] Custom rate limit values applied
- [ ] Rate limiting disabled with --no-rate-limit

### ACL Management CLI
```bash
# Add user rule
mqdb acl add admin '$DB/#' readwrite -f /tmp/acl.txt

# Add role
mqdb acl role-add editor '$DB/users/#' readwrite -f /tmp/acl.txt
mqdb acl role-add editor '$DB/orders/#' readwrite -f /tmp/acl.txt

# Assign role
mqdb acl assign alice editor -f /tmp/acl.txt

# List all rules
mqdb acl list -f /tmp/acl.txt

# List roles
mqdb acl role-list -f /tmp/acl.txt

# Check permission
mqdb acl check alice '$DB/users/create' pub -f /tmp/acl.txt

# List user roles
mqdb acl user-roles alice -f /tmp/acl.txt

# Remove role assignment
mqdb acl unassign alice editor -f /tmp/acl.txt

# Remove user rule
mqdb acl remove admin -f /tmp/acl.txt

# Remove role
mqdb acl role-remove editor -f /tmp/acl.txt
```
- [ ] ACL add/remove user rules
- [ ] ACL role-add/role-remove role rules
- [ ] ACL assign/unassign roles to users
- [ ] ACL list shows all rules with optional user filter
- [ ] ACL role-list shows roles with optional name filter
- [ ] ACL check verifies pub/sub permissions correctly
- [ ] ACL user-roles lists assigned roles

---

## 21. Topic Protection Testing

Topic protection enforces hardcoded security rules independent of ACL configuration.

### Prerequisites

```bash
# Generate password and ACL files
mqdb passwd admin -b admin123 -f /tmp/passwd.txt
mqdb passwd alice -b alice123 -f /tmp/passwd.txt

# Start agent with admin user configured
mqdb agent start --db /tmp/mqdb-topic-prot --bind 127.0.0.1:1883 \
    --passwd /tmp/passwd.txt --admin-users admin
```

### Tier 0: BlockAll Topics

These topics are completely blocked for all external clients:

```bash
# Internal cluster topic - should fail
mosquitto_pub -h 127.0.0.1 -p 1883 -u admin -P admin123 -t '_mqdb/test' -m 'payload'
# Expected: Publish fails (not authorized)

# Index topic - should fail
mosquitto_pub -h 127.0.0.1 -p 1883 -u admin -P admin123 -t '$DB/_idx/users/email' -m '{}'
# Expected: Publish fails

# Partition topic - should fail
mosquitto_pub -h 127.0.0.1 -p 1883 -u admin -P admin123 -t '$DB/p0/users/create' -m '{}'
# Expected: Publish fails

# Subscribe to index topics - should fail
mosquitto_sub -h 127.0.0.1 -p 1883 -u admin -P admin123 -t '$DB/_idx/#' -C 1 -W 2
# Expected: Subscribe fails or no messages received
```

**Verification checklist:**
- [ ] `_mqdb/#` blocked for all clients (including admin)
- [ ] `$DB/_idx/#` blocked for all clients
- [ ] `$DB/_unique/#` blocked for all clients
- [ ] `$DB/_fk/#` blocked for all clients
- [ ] `$DB/_query/#` blocked for all clients
- [ ] `$DB/p0/...` through `$DB/p63/...` blocked for all clients

### Tier 1: ReadOnly Topics

`$SYS/#` topics allow subscribe but not publish:

```bash
# Subscribe to $SYS - should succeed
mosquitto_sub -h 127.0.0.1 -p 1883 -u alice -P alice123 -t '$SYS/#' -C 1 -W 5 &
SUB_PID=$!
sleep 1

# Publish to $SYS - should fail
mosquitto_pub -h 127.0.0.1 -p 1883 -u alice -P alice123 -t '$SYS/test' -m 'payload'
# Expected: Publish fails

kill $SUB_PID 2>/dev/null
```

**Verification checklist:**
- [ ] Subscribe to `$SYS/#` succeeds
- [ ] Publish to `$SYS/...` fails (even for admin)

### Tier 2: AdminRequired Topics

Admin topics require authenticated admin user:

```bash
# Without admin role - should fail
mosquitto_pub -h 127.0.0.1 -p 1883 -u alice -P alice123 \
    -t '$DB/_admin/backup' -m '{}'
# Expected: Publish fails (admin required)

# With admin role - should succeed
mosquitto_rr -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/_admin/backup' -e 'r' -m '{"name":"test_backup"}' -W 5
# Expected: Backup created or appropriate response

# OAuth tokens without admin - should fail
mosquitto_pub -h 127.0.0.1 -p 1883 -u alice -P alice123 \
    -t '$DB/_oauth_tokens/abc123' -m '{}'
# Expected: Publish fails

# OAuth tokens with admin - should succeed
mosquitto_pub -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/_oauth_tokens/abc123' -m '{"token":"test"}'
# Expected: Publish succeeds (or appropriate DB response)
```

**Verification checklist:**
- [ ] `$DB/_admin/#` blocked for non-admin users
- [ ] `$DB/_admin/#` accessible for admin users
- [ ] `$DB/_oauth_tokens/#` blocked for non-admin users
- [ ] `$DB/_oauth_tokens/#` accessible for admin users

### Internal Entity Protection

Entities starting with `_` require admin access:

```bash
# List internal entity without admin - should fail
mosquitto_rr -h 127.0.0.1 -p 1883 -u alice -P alice123 \
    -t '$DB/_sessions/list' -e 'r' -m '{}' -W 3
# Expected: Access denied or empty response

# List internal entity with admin - should succeed
mosquitto_rr -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/_sessions/list' -e 'r' -m '{}' -W 3
# Expected: Success with session list (may be empty)

# Access _mqtt_subs without admin - should fail
mosquitto_rr -h 127.0.0.1 -p 1883 -u alice -P alice123 \
    -t '$DB/_mqtt_subs/list' -e 'r' -m '{}' -W 3
# Expected: Access denied
```

**Verification checklist:**
- [ ] `$DB/_sessions/*` blocked for non-admin
- [ ] `$DB/_mqtt_subs/*` blocked for non-admin
- [ ] `$DB/_topic_index/*` blocked for non-admin
- [ ] Internal entities accessible for admin users

### Health Endpoint Exception

`$DB/_health` is always accessible (no admin required):

```bash
# Health check without auth (if anonymous allowed)
mosquitto_rr -h 127.0.0.1 -p 1883 -t '$DB/_health' -e 'r' -m '{}' -W 3
# Expected: {"status":"ok","data":{"ready":true,...}}

# Health check with regular user
mosquitto_rr -h 127.0.0.1 -p 1883 -u alice -P alice123 \
    -t '$DB/_health' -e 'r' -m '{}' -W 3
# Expected: {"status":"ok","data":{"ready":true,...}}
```

**Verification checklist:**
- [ ] `$DB/_health` accessible without admin role
- [ ] `$DB/_health` returns ready status

### Regular Topics Unaffected

Non-protected topics follow normal ACL rules:

```bash
# Create entity - should succeed (if ACL allows)
mosquitto_rr -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/users/create' -e 'r' -m '{"name":"Alice"}'
# Expected: Success with created entity

# Pub/sub on custom topics
mosquitto_sub -h 127.0.0.1 -p 1883 -u alice -P alice123 -t 'sensors/#' -C 1 -W 3 &
sleep 1
mosquitto_pub -h 127.0.0.1 -p 1883 -u alice -P alice123 -t 'sensors/temp' -m '22.5'
# Expected: Message received by subscriber
```

**Verification checklist:**
- [ ] `$DB/users/*` accessible (per ACL)
- [ ] Custom topics like `sensors/#` work normally
- [ ] ACL rules still apply to non-protected topics

### Cluster Mode Topic Protection

Topic protection also applies in cluster mode:

```bash
# Start cluster with admin user
mqdb dev start-cluster --nodes 3 --clean
# Note: Admin users need to be configured in cluster start

# Test BlockAll topics in cluster
mosquitto_pub -h 127.0.0.1 -p 1883 -t '$DB/p0/test' -m 'payload'
# Expected: Fails on all nodes

mosquitto_pub -h 127.0.0.1 -p 1884 -t '_mqdb/cluster/test' -m 'payload'
# Expected: Fails on all nodes

# Cleanup
mqdb dev kill
```

**Verification checklist:**
- [ ] BlockAll topics blocked on all cluster nodes
- [ ] AdminRequired topics require admin on all nodes
- [ ] Internal entity protection works across cluster

### Complete Topic Protection Checklist

Run through this checklist to verify topic protection:

**BlockAll (Tier 0):**
- [ ] `_mqdb/#` - cluster internal
- [ ] `$DB/_idx/#` - secondary indexes
- [ ] `$DB/_unique/#` - unique constraints
- [ ] `$DB/_fk/#` - foreign keys
- [ ] `$DB/_query/#` - query internals
- [ ] `$DB/p+/#` - partition topics (p0-p63)

**ReadOnly (Tier 1):**
- [ ] `$SYS/#` - subscribe allowed, publish denied

**AdminRequired (Tier 2):**
- [ ] `$DB/_admin/#` - admin operations
- [ ] `$DB/_oauth_tokens/#` - OAuth tokens

**Internal Entities:**
- [ ] `_sessions` - requires admin
- [ ] `_mqtt_subs` - requires admin
- [ ] `_topic_index` - requires admin
- [ ] `_client_loc` - requires admin

**Exceptions:**
- [ ] `$DB/_health` - always accessible
- [ ] Internal clients (`mqdb-*`) bypass protection

**Integration:**
- [ ] Protection runs before ACL evaluation
- [ ] Permissive ACL cannot override protection

---

## 22. Ownership Enforcement Testing

Ownership enforcement restricts list, update, and delete operations based on the authenticated
user's identity. When `--ownership entity=field` is configured, the broker:

- **List:** Injects a mandatory filter `field = sender` so users only see their own records
- **Update/Delete:** Checks that `record.data.field == sender` before allowing the operation
- **Create/Read:** Not restricted (IDs are UUIDs, not guessable)
- **No sender (internal):** Bypasses all ownership checks (for replication, outbox, etc.)

### Prerequisites

Ownership requires **real authentication** (password or SCRAM). In anonymous mode, the broker
has no sender identity to enforce against. The `x-mqtt-sender` user property is only injected
when authentication succeeds.

### Automated Test

The `mqdb dev test --ownership` command is fully self-contained:

```bash
mqdb dev test --ownership --nodes 3
```

This command:
1. Creates a temporary password file with users `alice`, `bob`, and `admin`
2. Kills any running cluster
3. Starts a fresh 3-node cluster with `--passwd`, `--admin-users admin`, and `--ownership test_owned=userId`
4. Waits for the authenticated cluster to become ready
5. Runs 7 test cases (see below)
6. Kills the cluster and cleans up

### Test Cases

| # | Test | Expected |
|---|------|----------|
| 1 | Create entity as alice with `userId: "alice"` | Success, returns ID |
| 2 | List as alice | Sees 1 record (own) |
| 3 | List as bob | Sees 0 records (filtered out) |
| 4 | Update alice's record as bob | Forbidden error |
| 5 | Delete alice's record as bob | Forbidden error |
| 6 | Update alice's record as alice | Success (owner) |
| 7 | Delete alice's record as alice | Success (owner) |

### Manual Testing

To test ownership manually:

```bash
# 1. Create password file
mqdb passwd alice -b alice -f /tmp/ownership-passwd
mqdb passwd bob -b bob -f /tmp/ownership-passwd
mqdb passwd admin -b admin -f /tmp/ownership-passwd

# 2. Start cluster with auth + ownership
mqdb dev start-cluster --nodes 3 --clean \
    --passwd /tmp/ownership-passwd \
    --ownership diagrams=userId

# 3. Wait for cluster
sleep 10

# 4. Create a diagram as alice
mqdb create diagrams \
    -d '{"userId": "alice", "title": "Alice Diagram"}' \
    --broker 127.0.0.1:1883 --user alice --pass alice --format json
# Note the returned ID

# 5. List as alice (should see the diagram)
mqdb list diagrams --broker 127.0.0.1:1883 --user alice --pass alice

# 6. List as bob (should see nothing)
mqdb list diagrams --broker 127.0.0.1:1883 --user bob --pass bob

# 7. Update as bob (should fail with forbidden)
mqdb update diagrams <id> -d '{"title": "Stolen"}' \
    --broker 127.0.0.1:1883 --user bob --pass bob

# 8. Delete as bob (should fail with forbidden)
mqdb delete diagrams <id> \
    --broker 127.0.0.1:1883 --user bob --pass bob

# 9. Update as alice (should succeed)
mqdb update diagrams <id> -d '{"title": "Updated"}' \
    --broker 127.0.0.1:1883 --user alice --pass alice

# 10. Delete as alice (should succeed)
mqdb delete diagrams <id> \
    --broker 127.0.0.1:1883 --user alice --pass alice

# 11. Cleanup
mqdb dev kill
```

### Ownership Configuration

The `--ownership` flag accepts `entity=field` pairs:

```bash
# Single entity
--ownership diagrams=userId

# Multiple entities (comma-separated)
--ownership "diagrams=userId,notes=ownerId"
```

For agent mode:
```bash
mqdb agent start --db /tmp/mqdb-own --passwd passwd.txt \
    --ownership diagrams=userId
```

For cluster mode (via dev command):
```bash
mqdb dev start-cluster --nodes 3 --clean \
    --passwd passwd.txt --ownership diagrams=userId
```

### Verification Checklist

**List filtering:**
- [ ] Authenticated user only sees records where `ownerField == username`
- [ ] Different user sees empty list for same entity
- [ ] Unauthenticated (internal) operations see all records

**Write protection:**
- [ ] Update by non-owner returns 403 Forbidden
- [ ] Delete by non-owner returns 403 Forbidden
- [ ] Update by owner succeeds
- [ ] Delete by owner succeeds
- [ ] Internal operations (no sender) bypass ownership checks

**Cluster mode:**
- [ ] Ownership filters applied on local list path
- [ ] Ownership filters included in scatter-gather queries to remote nodes
- [ ] Write ownership checks work on partition primary nodes

**Edge cases:**
- [ ] Entity without ownership config allows all operations
- [ ] Record missing the owner field allows all operations (no crash)

---

## 23. Index-Based Range Queries

When a field has an index (`mqdb index add`), range filters (`>`, `>=`, `<`, `<=`) use the
index for lookup instead of scanning all records. This section verifies that indexed range
queries return correct results in agent mode, cluster mode, and across cluster nodes.

### Agent Mode

#### Setup

```bash
# Start agent
mqdb agent start --db /tmp/mqdb-range-test --anonymous

# Add index on the price field (requires dev-insecure build for --anonymous)
mqdb index add products --fields price --broker 127.0.0.1:1883

# Create test data
mqdb create products -d '{"name": "Mouse", "price": 25, "category": "electronics"}' --broker 127.0.0.1:1883
mqdb create products -d '{"name": "Keyboard", "price": 75, "category": "electronics"}' --broker 127.0.0.1:1883
mqdb create products -d '{"name": "Monitor", "price": 300, "category": "electronics"}' --broker 127.0.0.1:1883
mqdb create products -d '{"name": "Desk", "price": 200, "category": "furniture"}' --broker 127.0.0.1:1883
mqdb create products -d '{"name": "Chair", "price": 150, "category": "furniture"}' --broker 127.0.0.1:1883
```

#### Test 1: Greater Than (>) on Indexed Field

```bash
mqdb list products --filter 'price>100' --broker 127.0.0.1:1883
```

**Expected:** Returns Monitor (300), Desk (200), Chair (150). Does NOT return Mouse (25) or Keyboard (75).

#### Test 2: Greater Than or Equal (>=) on Indexed Field

```bash
mqdb list products --filter 'price>=150' --broker 127.0.0.1:1883
```

**Expected:** Returns Monitor (300), Desk (200), Chair (150). Does NOT return Mouse (25) or Keyboard (75).

#### Test 3: Less Than (<) on Indexed Field

```bash
mqdb list products --filter 'price<100' --broker 127.0.0.1:1883
```

**Expected:** Returns Mouse (25), Keyboard (75). Does NOT return others.

#### Test 4: Less Than or Equal (<=) on Indexed Field

```bash
mqdb list products --filter 'price<=75' --broker 127.0.0.1:1883
```

**Expected:** Returns Mouse (25), Keyboard (75).

#### Test 5: Combined Range (>= AND <=) on Indexed Field

```bash
mqdb list products --filter 'price>=75' --filter 'price<=200' --broker 127.0.0.1:1883
```

**Expected:** Returns Keyboard (75), Chair (150), Desk (200). Does NOT return Mouse (25) or Monitor (300).

#### Test 6: Combined Range (> AND <) Exclusive Bounds

```bash
mqdb list products --filter 'price>75' --filter 'price<300' --broker 127.0.0.1:1883
```

**Expected:** Returns Chair (150), Desk (200). Does NOT return Keyboard (75) or Monitor (300).

#### Test 7: Range on Indexed Field + Equality on Non-Indexed Field

```bash
mqdb list products --filter 'price>=100' --filter 'category=furniture' --broker 127.0.0.1:1883
```

**Expected:** Returns Desk (200), Chair (150). The index narrows candidates by price, then
the non-indexed `category` filter is applied in-memory.

#### Test 8: Range Returns Empty Set

```bash
mqdb list products --filter 'price>500' --broker 127.0.0.1:1883
```

**Expected:** Returns empty list (no products above 500).

#### Test 9: Boundary Precision

```bash
mqdb list products --filter 'price>=300' --broker 127.0.0.1:1883
```

**Expected:** Returns exactly Monitor (300). Verifies inclusive lower bound includes the exact value.

```bash
mqdb list products --filter 'price>300' --broker 127.0.0.1:1883
```

**Expected:** Returns empty list. Verifies exclusive lower bound excludes the exact value.

#### Cleanup

```bash
# Stop agent
pkill -f "mqdb agent"
rm -rf /tmp/mqdb-range-test
```

### Cluster Mode — Single Node

Note: `mqdb index add` is agent-mode only. In cluster mode, range filters work via full
table scan (correct results, no index optimization). All cluster commands require
`--user admin --pass admin` (credentials generated by `dev start-cluster`).

#### Setup

```bash
mqdb dev start-cluster --nodes 1 --clean
sleep 8

mqdb create products -d '{"name": "Mouse", "price": 25}' --broker 127.0.0.1:1883 --user admin --pass admin
mqdb create products -d '{"name": "Keyboard", "price": 75}' --broker 127.0.0.1:1883 --user admin --pass admin
mqdb create products -d '{"name": "Monitor", "price": 300}' --broker 127.0.0.1:1883 --user admin --pass admin
mqdb create products -d '{"name": "Desk", "price": 200}' --broker 127.0.0.1:1883 --user admin --pass admin
mqdb create products -d '{"name": "Chair", "price": 150}' --broker 127.0.0.1:1883 --user admin --pass admin
```

#### Test 10: Range Query in Single-Node Cluster

```bash
mqdb list products --filter 'price>=100' --broker 127.0.0.1:1883 --user admin --pass admin
```

**Expected:** Returns Monitor (300), Desk (200), Chair (150).

#### Test 11: Combined Range in Single-Node Cluster

```bash
mqdb list products --filter 'price>50' --filter 'price<250' --broker 127.0.0.1:1883 --user admin --pass admin
```

**Expected:** Returns Keyboard (75), Chair (150), Desk (200).

#### Cleanup

```bash
mqdb dev kill
```

### Cluster Mode — Multi-Node (Cross-Node Range Queries)

#### Setup

```bash
mqdb dev start-cluster --nodes 3 --clean
sleep 8

# Create records via different nodes so data is distributed across partitions
mqdb create products -d '{"name": "Mouse", "price": 25}' --broker 127.0.0.1:1883 --user admin --pass admin
mqdb create products -d '{"name": "Keyboard", "price": 75}' --broker 127.0.0.1:1884 --user admin --pass admin
mqdb create products -d '{"name": "Monitor", "price": 300}' --broker 127.0.0.1:1885 --user admin --pass admin
mqdb create products -d '{"name": "Desk", "price": 200}' --broker 127.0.0.1:1883 --user admin --pass admin
mqdb create products -d '{"name": "Chair", "price": 150}' --broker 127.0.0.1:1884 --user admin --pass admin
mqdb create products -d '{"name": "Lamp", "price": 50}' --broker 127.0.0.1:1885 --user admin --pass admin
mqdb create products -d '{"name": "Headphones", "price": 120}' --broker 127.0.0.1:1883 --user admin --pass admin
```

#### Test 12: Range Query from Each Node

Query from every node and verify the same results:

```bash
mqdb list products --filter 'price>100' --broker 127.0.0.1:1883 --user admin --pass admin
mqdb list products --filter 'price>100' --broker 127.0.0.1:1884 --user admin --pass admin
mqdb list products --filter 'price>100' --broker 127.0.0.1:1885 --user admin --pass admin
```

**Expected:** All three return the same 4 records: Headphones (120), Chair (150), Desk (200), Monitor (300). The order may vary but the set must match.

#### Test 13: Combined Range from Non-Owner Node

```bash
mqdb list products --filter 'price>=50' --filter 'price<=200' --broker 127.0.0.1:1884 --user admin --pass admin
```

**Expected:** Returns Lamp (50), Keyboard (75), Headphones (120), Chair (150), Desk (200). Five records regardless of which node is queried.

#### Test 14: Range + Non-Indexed Filter Across Nodes

```bash
# First add some category data
mqdb create items -d '{"name": "A", "score": 10, "tier": "gold"}' --broker 127.0.0.1:1883 --user admin --pass admin
mqdb create items -d '{"name": "B", "score": 20, "tier": "silver"}' --broker 127.0.0.1:1884 --user admin --pass admin
mqdb create items -d '{"name": "C", "score": 30, "tier": "gold"}' --broker 127.0.0.1:1885 --user admin --pass admin
mqdb create items -d '{"name": "D", "score": 40, "tier": "silver"}' --broker 127.0.0.1:1883 --user admin --pass admin
mqdb create items -d '{"name": "E", "score": 50, "tier": "gold"}' --broker 127.0.0.1:1884 --user admin --pass admin

mqdb list items --filter 'score>=20' --filter 'tier=gold' --broker 127.0.0.1:1885 --user admin --pass admin
```

**Expected:** Returns C (score 30, gold) and E (score 50, gold).

#### Test 15: Empty Range Across Nodes

```bash
mqdb list products --filter 'price>1000' --broker 127.0.0.1:1884 --user admin --pass admin
```

**Expected:** Returns empty list from any node.

#### Test 16: Consistency After Update

Verify range queries reflect updated values:

```bash
# Get the ID of the Mouse record
mqdb list products --filter 'name=Mouse' --broker 127.0.0.1:1883 --user admin --pass admin
# Note the ID

# Update Mouse's price from 25 to 250
mqdb update products <mouse-id> -d '{"price": 250}' --broker 127.0.0.1:1883 --user admin --pass admin

# Mouse should now appear in price>200 results
mqdb list products --filter 'price>200' --broker 127.0.0.1:1884 --user admin --pass admin
```

**Expected:** Returns Mouse (250) and Monitor (300).

#### Test 17: Consistency After Delete

```bash
# Delete the Desk record
mqdb list products --filter 'name=Desk' --broker 127.0.0.1:1883 --user admin --pass admin
# Note the ID

mqdb delete products <desk-id> --broker 127.0.0.1:1883 --user admin --pass admin

# Desk should no longer appear
mqdb list products --filter 'price>=200' --broker 127.0.0.1:1885 --user admin --pass admin
```

**Expected:** Returns Mouse (250, from Test 16 update) and Monitor (300). Desk no longer appears.

#### Cleanup

```bash
mqdb dev kill
```

### Verification Checklist

**Agent Mode:**
- [ ] `>` on indexed field returns correct subset
- [ ] `>=` on indexed field includes boundary value
- [ ] `<` on indexed field returns correct subset
- [ ] `<=` on indexed field includes boundary value
- [ ] Combined `>=` and `<=` returns correct range
- [ ] Combined `>` and `<` excludes boundaries
- [ ] Range on indexed + equality on non-indexed filters correctly
- [ ] Empty range returns empty list
- [ ] Boundary precision (inclusive vs exclusive)

**Single-Node Cluster:**
- [ ] Range query returns correct results
- [ ] Combined range returns correct results

**Multi-Node Cluster:**
- [ ] Same range query returns identical results from all nodes
- [ ] Combined range returns correct results from non-owner node
- [ ] Range + non-indexed filter works across nodes
- [ ] Empty range returns empty from all nodes
- [ ] Updated records reflected in subsequent range queries
- [ ] Deleted records excluded from subsequent range queries

## 24. Field Projection (Partial Responses)

Field projection allows clients to request only specific fields in read and list responses, reducing payload size.

### Agent Mode

#### Setup

```bash
mqdb passwd testuser -b testpass -f /tmp/mqdb-projection-passwd
mqdb agent start --db /tmp/mqdb-projection --bind 127.0.0.1:1883 --passwd /tmp/mqdb-projection-passwd
```

Create test data:

```bash
mqdb create users -d '{"name": "Alice", "email": "alice@example.com", "age": 30, "city": "NYC"}' --user testuser --pass testpass
mqdb create users -d '{"name": "Bob", "email": "bob@example.com", "age": 25, "city": "LA"}' --user testuser --pass testpass
mqdb create users -d '{"name": "Charlie", "email": "charlie@example.com", "age": 35, "city": "NYC"}' --user testuser --pass testpass
```

#### Test 1: Read with Projection

```bash
ID=$(mqdb list users --user testuser --pass testpass | python3 -c "import sys,json; print(json.load(sys.stdin)['data'][0]['id'])")
mqdb read users "$ID" --projection name,email --user testuser --pass testpass
```

Expected: response contains only `id`, `name`, and `email` fields. No `age` or `city`.

#### Test 2: Read Projection Always Includes ID

```bash
mqdb read users "$ID" --projection name --user testuser --pass testpass
```

Expected: response contains `id` and `name` only. `id` is always included even when not in projection list.

#### Test 3: List with Projection

```bash
mqdb list users --projection name,city --user testuser --pass testpass
```

Expected: each result contains only `id`, `name`, and `city`. No `email` or `age`.

#### Test 4: Projection with Filters Combined

```bash
mqdb list users --filter 'city=NYC' --projection name --user testuser --pass testpass
```

Expected: returns only NYC users, each with only `id` and `name` fields.

#### Test 5: Projection with Nonexistent Field (No Schema)

```bash
mqdb read users "$ID" --projection name,nonexistent --user testuser --pass testpass
```

Expected: returns `id` and `name`. The `nonexistent` field is silently omitted.

#### Test 6: Projection with Schema Validation

```bash
cat > /tmp/users_schema.json << 'EOF'
{"entity": "users_strict", "fields": {"name": {"name": "name", "field_type": "String", "required": false, "default": null}, "email": {"name": "email", "field_type": "String", "required": false, "default": null}}}
EOF
mqdb schema set users_strict -f /tmp/users_schema.json --user testuser --pass testpass
mqdb create users_strict -d '{"name": "Dave", "email": "dave@example.com"}' --user testuser --pass testpass
ID2=$(mqdb list users_strict --user testuser --pass testpass | python3 -c "import sys,json; print(json.load(sys.stdin)['data'][0]['id'])")
mqdb read users_strict "$ID2" --projection nonexistent --user testuser --pass testpass
```

Expected: error response indicating the projection field does not exist in the schema.

#### Cleanup

```bash
pkill -f "mqdb agent"
rm -rf /tmp/mqdb-projection /tmp/mqdb-projection-passwd
```

### Cluster Mode

`mqdb dev start-cluster` auto-generates credentials `admin`/`admin` in `/tmp/mqdb-test-passwd`.

#### Setup

```bash
mqdb dev start-cluster --nodes 3 --clean
sleep 5
```

Create test data:

```bash
mqdb create items -d '{"name": "widget", "price": 10, "category": "A"}' --user admin --pass admin --broker 127.0.0.1:1883
mqdb create items -d '{"name": "gadget", "price": 20, "category": "B"}' --user admin --pass admin --broker 127.0.0.1:1883
mqdb create items -d '{"name": "doohickey", "price": 30, "category": "A"}' --user admin --pass admin --broker 127.0.0.1:1883
```

#### Test 7: Read with Projection from Primary Node

```bash
ID=$(mqdb list items --user admin --pass admin --broker 127.0.0.1:1883 | python3 -c "import sys,json; print(json.load(sys.stdin)['data'][0]['id'])")
mqdb read items "$ID" --projection name --user admin --pass admin --broker 127.0.0.1:1883
mqdb read items "$ID" --projection name --user admin --pass admin --broker 127.0.0.1:1884
mqdb read items "$ID" --projection name --user admin --pass admin --broker 127.0.0.1:1885
```

Expected: the primary node for this partition returns only `id` and `name`. Non-primary nodes forward to the primary and also return projected data.

#### Test 8: List with Projection Across Nodes

```bash
mqdb list items --projection name,price --user admin --pass admin --broker 127.0.0.1:1883
mqdb list items --projection name,price --user admin --pass admin --broker 127.0.0.1:1884
mqdb list items --projection name,price --user admin --pass admin --broker 127.0.0.1:1885
```

Expected: all nodes return all 3 items with only `id`, `name`, and `price`. No `category` field.

#### Test 9: List with Projection and Filters Across Nodes

```bash
mqdb list items --filter 'category=A' --projection name --user admin --pass admin --broker 127.0.0.1:1883
mqdb list items --filter 'category=A' --projection name --user admin --pass admin --broker 127.0.0.1:1884
mqdb list items --filter 'category=A' --projection name --user admin --pass admin --broker 127.0.0.1:1885
```

Expected: all nodes return 2 items (widget, doohickey) with only `id` and `name`. Filters are applied before projection in the scatter-gather aggregation.

#### Cleanup

```bash
mqdb dev kill
```

### Verification Checklist

**Agent Mode:**
- [ ] Read with projection returns only selected fields + id
- [ ] List with projection returns only selected fields + id for all results
- [ ] Projection + filters work together
- [ ] Nonexistent projection field silently omitted (no schema)
- [ ] Schema-validated projection rejects unknown fields

**Cluster Mode:**
- [ ] Read projection works from primary node
- [ ] Read projection works when forwarded to primary
- [ ] List projection works with scatter-gather across all nodes
- [ ] List projection + filters work across nodes
