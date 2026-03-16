# MQDB CLI Testing Guide

This guide provides end-to-end validation of MQDB CLI functionality through manual commands.

## Prerequisites

### Build the CLI

```bash
cd /path/to/mqdb

# Agent-only (open-source edition — no cluster commands)
cargo build --release --bin mqdb --features agent-only

# Full build with clustering (commercial edition, default)
cargo build --release
```

Binary location: `target/release/mqdb`

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MQDB_BROKER` | MQTT broker address | `127.0.0.1:1883` |
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

### Basic Start (with Authentication)

**Terminal 1:**
```bash
mqdb passwd admin -b admin123 -f ./passwd.txt
mqdb agent start --db ./data/testdb --passwd ./passwd.txt
```

**Expected output:**
```
(agent starts, listening on 127.0.0.1:1883)
```

> **Note:** The `--anonymous` flag exists but requires the `dev-insecure` build feature
> (`cargo build --release --features dev-insecure`). For production and standard testing,
> always use `--passwd` for password auth or `--scram-file` for SCRAM-SHA-256 auth.

### Check Agent Status

**Terminal 2:**
```bash
mqdb agent status
```

**Expected output:**
```json
{"data":{"mode":"agent","ready":true,"status":"healthy"},"status":"ok"}
```

The `status` command connects to the broker via MQTT and queries the `$DB/_health` endpoint.

### Start with Full Authentication Stack

```bash
mqdb agent start --db ./data/testdb --passwd ./passwd.txt --acl ./acl.txt \
    --admin-users admin
```

### Agent Start Options Reference

**Core:**

| Option | Description | Default |
|--------|-------------|---------|
| `--db` | Database directory path | (required) |
| `--bind` | MQTT listener address | `127.0.0.1:1883` |
| `--durability` | `immediate`, `periodic`, or `none` | `periodic` |
| `--durability-ms` | Fsync interval in ms (periodic mode) | `10` |
| `--ownership` | Ownership config: `entity=field` pairs | (none) |
| `--event-scope` | Scope events by entity field | (none) |
| `--passphrase-file` | File-at-rest encryption passphrase | (none) |

**Authentication:**

| Option | Description | Default |
|--------|-------------|---------|
| `--passwd` | Path to password file | (none) |
| `--acl` | Path to ACL file | (none) |
| `--scram-file` | Path to SCRAM-SHA-256 credentials file | (none) |
| `--jwt-algorithm` | JWT algorithm: `hs256`, `rs256`, `es256` | (none) |
| `--jwt-key` | Path to JWT secret/key file | (none) |
| `--jwt-issuer` | JWT issuer claim | (none) |
| `--jwt-audience` | JWT audience claim | (none) |
| `--jwt-clock-skew` | JWT clock skew tolerance in seconds | `60` |
| `--federated-jwt-config` | Path to federated JWT config JSON (conflicts with `--jwt-algorithm`) | (none) |
| `--cert-auth-file` | Path to certificate auth file | (none) |
| `--admin-users` | Comma-separated list of admin usernames | (none) |
| `--no-rate-limit` | Disable authentication rate limiting | `false` |
| `--rate-limit-max-attempts` | Max failed auth attempts before lockout | `5` |
| `--rate-limit-window-secs` | Rate limit window in seconds | `60` |
| `--rate-limit-lockout-secs` | Lockout duration in seconds | `300` |

**Transport:**

| Option | Description | Default |
|--------|-------------|---------|
| `--quic-cert` | TLS certificate (PEM) for MQTT-over-TLS | (none) |
| `--quic-key` | TLS private key (PEM) for MQTT-over-TLS | (none) |
| `--ws-bind` | WebSocket bind address (e.g. `0.0.0.0:8080`) | (none) |

**OAuth/Identity:**

| Option | Description | Default |
|--------|-------------|---------|
| `--http-bind` | HTTP server for OAuth (e.g. `0.0.0.0:8081`) | (none) |
| `--oauth-client-secret` | Path to Google OAuth client secret | (none) |
| `--oauth-redirect-uri` | OAuth redirect URI | `http://localhost:{port}/oauth/callback` |
| `--oauth-frontend-redirect` | Browser redirect after OAuth completes | (none) |
| `--ticket-expiry-secs` | Ticket JWT expiry in seconds | `30` |
| `--cookie-secure` | Set Secure flag on session cookies (HTTPS) | `false` |
| `--cors-origin` | CORS allowed origin for auth endpoints | (none) |
| `--ticket-rate-limit` | Max ticket requests per minute per IP | `10` |
| `--trust-proxy` | Trust X-Forwarded-For header | `false` |
| `--identity-key-file` | 32-byte identity encryption key file | (auto-generated) |

---

## 2. Basic CRUD Operations

> **Note:** These examples assume the agent started in Section 1 with `--passwd`. All commands
> require `--user admin --pass admin123` (or set `MQDB_USER`/`MQDB_PASS` env vars).

### Create Entity

**Terminal 2:**
```bash
mqdb create users --data '{"name": "Alice", "email": "alice@example.com", "age": 30}' \
  --user admin --pass admin123
```

**Expected output (agent mode):**
```json
{
  "data": {
    "_version": 1,
    "age": 30,
    "email": "alice@example.com",
    "id": "1",
    "name": "Alice"
  },
  "status": "ok"
}
```

> **Note:** Response format differs between modes. In agent mode, `id` is inside `data`.
> In cluster mode, `id` and `entity` are top-level fields alongside `data`.

### Read Entity

```bash
# Use the ID returned from create
mqdb read users <id>
```

**Expected output:**
```json
{
  "data": {
    "_version": 1,
    "age": 30,
    "email": "alice@example.com",
    "id": "<id>",
    "name": "Alice"
  },
  "status": "ok"
}
```

### Update Entity

```bash
mqdb update users <id> --data '{"age": 25}'
```

**Expected output:**
```json
{
  "data": {
    "_version": 2,
    "age": 25,
    "email": "alice@example.com",
    "id": "<id>",
    "name": "Alice"
  },
  "status": "ok"
}
```

### Delete Entity

```bash
mqdb delete users <id>
```

**Expected output:**
```json
{
  "data": null,
  "status": "ok"
}
```

### Output Formats

**JSON format (default):**
```bash
mqdb read users user-001 --format json
```

**Table format:**
```bash
mqdb read users user-001 --format table
```

**CSV format:**
```bash
mqdb list users --format csv
```

All client commands support `--format json|table|csv`. Default is `json`.

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
> If no `"id"` field is present, the server generates a partition-prefixed hex ID (e.g., `"6fc263177a320176-0011"`).
>
> **Important:** In agent mode, `create` with an existing ID performs an upsert (overwrites the existing record).
> To prevent duplicates with client-provided IDs, add a unique constraint on the relevant field.

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

> **Note:** Use `<>` (SQL-style) for not-equal. Use single quotes around the filter expression
> to prevent shell interpretation of special characters.

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

**Multiple sort fields (comma-separated):**
```bash
mqdb list products --sort 'category:asc,price:desc'
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
```json
{"data": {"message": "schema set"}, "status": "ok"}
```

### Get Schema

```bash
mqdb schema get users
```

**Expected output:**
```json
{
  "data": {
    "entity": "users",
    "fields": {
      "name": {"field_type": "String", "name": "name", "required": true, "default": null},
      "email": {"field_type": "String", "name": "email", "required": true, "default": null},
      "age": {"field_type": "Number", "name": "age", "required": false, "default": 0},
      "active": {"field_type": "Boolean", "name": "active", "required": false, "default": true}
    },
    "version": 1
  },
  "status": "ok"
}
```

> **Note:** The schema file uses lowercase `"type"` (e.g., `"string"`), but the stored
> schema returns the internal format with `"field_type"` (e.g., `"String"`).

### Schema Validation - Required Field

```bash
mqdb create users --data '{"email": "test@example.com"}'
```

**Expected error:**
```
error: schema validation failed: name - required field is missing
```

### Schema Validation - Type Check

```bash
mqdb create users --data '{"name": "Test", "email": "test@example.com", "age": "not-a-number"}'
```

**Expected error:**
```
error: schema validation failed: age - expected type Number, got string
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
```json
{"code": 409, "message": "unique constraint violation on field 'email'", "status": "error"}
```

### Unique Constraint on Update (Conflict)

```bash
mqdb update users <user1-id> --data '{"email": "unique@test.com"}'
```

**Expected error:**
```json
{"code": 409, "message": "unique constraint violation on field 'email'", "status": "error"}
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
```json
{
  "data": [
    {"name": "users_email_unique", "type": "unique", "fields": ["email"]},
    {"name": "users_name_notnull", "type": "notnull", "field": "name"}
  ],
  "status": "ok"
}
```

### Foreign Key Constraint

Setup:
```bash
mqdb create authors --data '{"name": "Jane Author"}'
# Note the returned ID (e.g., author-abc123)
mqdb constraint add posts --fk "author_id:authors:id:cascade"
```

The `--fk` format is `field:target_entity:target_field[:action]` where action is `cascade`, `restrict`, or `set_null`. Default action is `restrict` if omitted.

Test referential integrity:
```bash
mqdb create posts --data '{"title": "Post 1", "author_id": "invalid-author"}'
```

**Expected error:**
```
error: foreign key violation: referenced record authors/invalid-author does not exist
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

## 5b. Index Management

Indexes optimize range filter queries (`>`, `>=`, `<`, `<=`) by avoiding full table scans.

### Add an Index

```bash
mqdb index add products --fields price
```

Multi-field indexes:
```bash
mqdb index add products --fields category price
```

### Verify Index Usage

After adding an index, range queries on indexed fields use the index for lookup:

```bash
mqdb list products --filter 'price>100'
```

Without an index, this scans all records. With an index, only matching records are retrieved.

> **Note:** `index add` is currently an agent-mode operation. In cluster mode, range filters
> work via full table scan (correct results, no index optimization).

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

The `subscribe` command takes a **topic pattern** (not an entity name). Use the `$DB/{entity}/events/#` pattern to receive change events.

**Terminal 3 (Consumer A):**
```bash
mqdb subscribe '$DB/users/events/#' --group workers --mode load-balanced
```

**Open Terminal 4 (Consumer B):**
```bash
mqdb subscribe '$DB/users/events/#' --group workers --mode load-balanced
```

**Terminal 2 (create multiple entities):**
```bash
mqdb create users --data '{"name": "User 1"}'
mqdb create users --data '{"name": "User 2"}'
mqdb create users --data '{"name": "User 3"}'
mqdb create users --data '{"name": "User 4"}'
```

**Expected:** Events distributed between Terminal 3 and Terminal 4 (each receives ~2 events)

> **Note:** `subscribe` takes a raw MQTT topic pattern, unlike `watch` which takes an entity name.
> Mode values: `broadcast` (default), `load-balanced`, `ordered`.

### Subscribe with Ordered Mode

**Terminal 3:**
```bash
mqdb subscribe '$DB/users/events/#' --group processors --mode ordered
```

**Expected:** All events for same entity ID go to same consumer

### Custom Heartbeat Interval

The `--heartbeat-interval` flag specifies seconds (default: 10):

```bash
mqdb subscribe '$DB/users/events/#' --group workers --heartbeat-interval 5
```

---

## 7. Consumer Groups

### List Consumer Groups

```bash
mqdb consumer-group list
```

**Expected output (JSON, default format):**
```json
{"data": [{"name": "workers", "members": 2}, {"name": "processors", "members": 1}], "status": "ok"}
```

Use `--format table` for human-readable output.

### Show Consumer Group Details

```bash
mqdb consumer-group show workers
```

**Expected output (JSON, default format):**
```json
{"data": {"name": "workers", "mode": "LoadBalanced", "members": [...]}, "status": "ok"}
```

---

## 8. Backup and Restore

### Create Backup

```bash
mqdb backup create
mqdb backup create --name my-snapshot
```

The `--name` flag is optional (default: `backup`).

### List Backups

```bash
mqdb backup list
```

### Restore from Backup

Restore sends a restore command to the running broker via MQTT. The agent handles the restore internally — no restart required.

```bash
mqdb restore --name backup_20241208_143022
```

All backup/restore commands connect to the broker (support `--broker`, `--user`, `--pass`, `--insecure` flags).

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
```json
{"code": 404, "message": "not found: users", "status": "error"}
```

### Connection Refused

```bash
MQDB_BROKER=127.0.0.1:9999 mqdb list users
```

**Expected error:**
```
error: connection refused: 127.0.0.1:9999
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

**Expected error (depends on whether schema is set):**
```json
{"code": 400, "message": "not_null constraint violation: field 'email' is null", "status": "error"}
```

> **Note:** If a schema with `"required": true` is set for this field, the schema validation
> fires first with: `schema validation failed: email - required field is missing`

---

## Quick Test Checklist

Run through these commands to validate core functionality.

> **Tip:** For quick development testing with the `dev-insecure` feature build, you can use
> `mqdb agent start --db /tmp/quicktest --anonymous` and omit `--user`/`--pass` from all commands.

```bash
# 0. Setup credentials
mqdb passwd admin -b admin -f /tmp/quicktest-passwd

# 1. Start agent
mqdb agent start --db /tmp/quicktest --passwd /tmp/quicktest-passwd --admin-users admin

# 2. CRUD operations (in a second terminal)
mqdb create users --data '{"name": "Test User", "email": "test@example.com"}' --user admin --pass admin
# Note the returned ID (e.g., 1)
mqdb read users <id> --user admin --pass admin
mqdb update users <id> --data '{"age": 25}' --user admin --pass admin
mqdb list users --user admin --pass admin
mqdb delete users <id> --user admin --pass admin

# 3. Filtering and sorting
mqdb create items --data '{"name": "A", "value": 10}' --user admin --pass admin
mqdb create items --data '{"name": "B", "value": 20}' --user admin --pass admin
mqdb create items --data '{"name": "C", "value": 5}' --user admin --pass admin
mqdb list items --filter 'value>5' --sort value:desc --user admin --pass admin

# 4. Schema validation
echo '{"name": {"type": "string", "required": true}}' > /tmp/schema.json
mqdb schema set validated --file /tmp/schema.json --user admin --pass admin
mqdb create validated --data '{}' --user admin --pass admin  # Should fail

# 5. Constraints
mqdb constraint add items --unique name --user admin --pass admin
mqdb create items --data '{"name": "A", "value": 100}' --user admin --pass admin  # Should fail (duplicate name)

# 6. Subscriptions (in separate terminal)
# Terminal 3: mqdb watch items --user admin --pass admin
# Terminal 2: mqdb create items --data '{"name": "D", "value": 15}' --user admin --pass admin

# 7. Consumer groups
mqdb consumer-group list --user admin --pass admin

# 8. Backup
mqdb backup create --user admin --pass admin
mqdb backup list --user admin --pass admin

# 9. Cleanup
mqdb dev kill --agent
rm -rf /tmp/quicktest /tmp/quicktest-passwd
```

---

## Troubleshooting

### Agent Won't Start

1. Check for port conflicts:
   ```bash
   lsof -i :1883
   ```

2. Verify database path is writable:
   ```bash
   mkdir -p ./data/testdb && touch ./data/testdb/.test && rm ./data/testdb/.test
   ```

3. Check for existing agent processes:
   ```bash
   mqdb dev ps
   ```

### Connection Timeout

1. Verify broker address:
   ```bash
   echo $MQDB_BROKER
   ```

2. Test broker connectivity:
   ```bash
   mqdb agent status --broker 127.0.0.1:1883
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

## 10b. TLS and Client Options

All client commands (`create`, `read`, `update`, `delete`, `list`, `watch`, `subscribe`,
`schema`, `constraint`, `index`, `backup`, `restore`, `consumer-group`, `agent status`,
`cluster status`, `cluster rebalance`, `bench`) support these common options:

| Option | Description |
|--------|-------------|
| `--broker` | Broker address (default: `127.0.0.1:1883`, env: `MQDB_BROKER`) |
| `--user` | Authentication username (env: `MQDB_USER`) |
| `--pass` | Authentication password (env: `MQDB_PASS`) |
| `--timeout` | Connection timeout in seconds (default: `30`) |
| `--insecure` | Skip TLS certificate verification (for self-signed certs) |

When connecting to a broker with TLS (via `--quic-cert`/`--quic-key` on the server), use
`--insecure` to accept self-signed certificates during testing:

```bash
mqdb list users --broker 127.0.0.1:1883 --insecure
```

---

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
# Start a single-node cluster (auto-generates admin/admin credentials)
mqdb dev start-cluster --nodes 1 --clean
sleep 5

# Create
mqdb db create -p 0 -e products -d '{"name": "Widget", "price": 99}' --user admin --pass admin
# Note the ID from output (e.g., abc123-0001)

# Read
mqdb db read -p 0 -e products -i abc123-0001 --user admin --pass admin

# Update
mqdb db update -p 0 -e products -i abc123-0001 -d '{"name": "Widget Pro", "price": 149}' --user admin --pass admin

# Verify update
mqdb db read -p 0 -e products -i abc123-0001 --user admin --pass admin

# Delete
mqdb db delete -p 0 -e products -i abc123-0001 --user admin --pass admin

# Verify deletion
mqdb db read -p 0 -e products -i abc123-0001 --user admin --pass admin
# Should output: Not found

# Cleanup
mqdb dev kill
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

---

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

---

## 17. Cluster Resilience Testing

These tests verify data survives failures and replication works correctly.

### Test 1: Data Persistence on Restart

Verify data survives a full cluster restart.

```bash
# 1. Start cluster
mqdb dev start-cluster --nodes 3 --clean

# 2. Create test data
mqdb create users --data '{"name": "Persist Test", "value": 123}' \
  --broker 127.0.0.1:1883 --user admin --pass admin
# Note the returned ID

# 3. Verify data exists
mqdb read users <id> --broker 127.0.0.1:1883 --user admin --pass admin

# 4. Stop all nodes
mqdb dev kill --all

# 5. Restart cluster (without --clean)
mqdb dev start-cluster --nodes 3

# 6. Verify data survived
mqdb read users <id> --broker 127.0.0.1:1883 --user admin --pass admin
```

**Expected:** Data should be readable after restart.

### Test 2: Replication Verification

Verify data exists on replica nodes.

```bash
# 1. Start cluster
mqdb dev start-cluster --nodes 3 --clean

# 2. Create data via Node 1
mqdb create test_repl --data '{"key": "replication_test"}' \
  --broker 127.0.0.1:1883 --user admin --pass admin
# Note the returned ID

# 3. Read from Node 1
mqdb read test_repl <id> --broker 127.0.0.1:1883 --user admin --pass admin

# 4. Read from Node 2
mqdb read test_repl <id> --broker 127.0.0.1:1884 --user admin --pass admin

# 5. Read from Node 3
mqdb read test_repl <id> --broker 127.0.0.1:1885 --user admin --pass admin
```

**Expected:** All nodes should return the data (replicated across cluster).

### Test 3: Primary Failure with Replica Takeover

Verify replica becomes primary when original primary dies.

```bash
# 1. Start cluster
mqdb dev start-cluster --nodes 3 --clean

# 2. Create multiple entities across partitions
for i in {1..10}; do
  mqdb create failover_test --data "{\"index\": $i}" \
    --broker 127.0.0.1:1883 --user admin --pass admin
done

# 3. Note the IDs and verify all readable
mqdb list failover_test --broker 127.0.0.1:1883 --user admin --pass admin

# 4. Kill Node 1
mqdb dev kill --node 1

# 5. Wait for failover (2-3 seconds)
sleep 3

# 6. Verify all data still accessible via Node 2
mqdb list failover_test --broker 127.0.0.1:1884 --user admin --pass admin

# 7. Verify count matches
```

**Expected:** All 10 entities should still be readable from surviving nodes.

### Test 4: Network Partition Simulation

```bash
# 1. Start 3-node cluster
mqdb dev start-cluster --nodes 3 --clean

# 2. Create data
mqdb create partition_test --data '{"before": "partition"}' \
  --broker 127.0.0.1:1883 --user admin --pass admin

# 3. Simulate partition by killing Node 3
mqdb dev kill --node 3

# 4. Create more data (should succeed with 2 nodes)
mqdb create partition_test --data '{"during": "partition"}' \
  --broker 127.0.0.1:1883 --user admin --pass admin

# 5. Restart Node 3
mqdb cluster start --node-id 3 --bind 127.0.0.1:1885 --db /tmp/mqdb-test-3 \
  --peers 1@127.0.0.1:1883 \
  --quic-cert test_certs/server.pem --quic-key test_certs/server.key --quic-ca test_certs/ca.pem \
  --passwd /tmp/mqdb-test-passwd --admin-users admin &

# 6. Wait for rejoin
sleep 5

# 7. Verify Node 3 has all data
mqdb list partition_test --broker 127.0.0.1:1885 --user admin --pass admin
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
mqdb constraint add cluster_users --unique email --broker 127.0.0.1:1883 --user admin --pass admin

# 3. Create entity on Node 1
mqdb create cluster_users --data '{"name": "Alice", "email": "alice@test.com"}' \
  --broker 127.0.0.1:1883 --user admin --pass admin

# 4. Try duplicate on Node 2 (should fail)
mqdb create cluster_users --data '{"name": "Bob", "email": "alice@test.com"}' \
  --broker 127.0.0.1:1884 --user admin --pass admin
```

**Expected:** Second create fails with unique constraint violation.

### Unique Constraint on Update Across Nodes (Conflict)

```bash
# 1. Start cluster
mqdb dev start-cluster --nodes 2 --clean

# 2. Add unique constraint
mqdb constraint add cluster_users --unique email --broker 127.0.0.1:1883 --user admin --pass admin

# 3. Create two entities on different nodes
mqdb create cluster_users --data '{"name": "Alice", "email": "alice@test.com"}' \
  --broker 127.0.0.1:1883 --user admin --pass admin
# Note Alice's ID

mqdb create cluster_users --data '{"name": "Bob", "email": "bob@test.com"}' \
  --broker 127.0.0.1:1884 --user admin --pass admin
# Note Bob's ID

# 4. Update Bob's email to conflict with Alice (should fail)
mqdb update cluster_users <bob-id> --data '{"email": "alice@test.com"}' \
  --broker 127.0.0.1:1884 --user admin --pass admin
```

**Expected:** Update fails with 409 unique constraint violation.

### Unique Constraint on Update Across Nodes (Value Recycling)

```bash
# 1. Using same cluster and entities from above

# 2. Update Alice's email to a new value
mqdb update cluster_users <alice-id> --data '{"email": "alice-new@test.com"}' \
  --broker 127.0.0.1:1883 --user admin --pass admin

# 3. Now update Bob's email to Alice's OLD value (should succeed)
mqdb update cluster_users <bob-id> --data '{"email": "alice@test.com"}' \
  --broker 127.0.0.1:1884 --user admin --pass admin
```

**Expected:** Update succeeds — the old unique value was released when Alice changed her email.

### Foreign Key Across Nodes

```bash
# 1. Create parent entity on Node 1
mqdb create authors --data '{"name": "Jane"}' --broker 127.0.0.1:1883 --user admin --pass admin
# Note the ID

# 2. Add FK constraint (restrict)
mqdb constraint add books --fk "author_id:authors:id:restrict" \
  --broker 127.0.0.1:1883 --user admin --pass admin

# 3. Create child with valid FK on Node 2
mqdb create books --data '{"title": "Book 1", "author_id": "<author-id>"}' \
  --broker 127.0.0.1:1884 --user admin --pass admin

# 4. Try invalid FK on Node 2 (should fail)
mqdb create books --data '{"title": "Book 2", "author_id": "nonexistent"}' \
  --broker 127.0.0.1:1884 --user admin --pass admin
```

**Expected:** Valid FK succeeds, invalid FK fails with constraint violation.

### Cascade Delete Across Nodes

```bash
# 1. Create parent on Node 1
mqdb create authors --data '{"name": "Alice"}' --broker 127.0.0.1:1883 --user admin --pass admin
# Note the ID

# 2. Add cascade FK
mqdb constraint add posts --fk "author_id:authors:id:cascade" \
  --broker 127.0.0.1:1883 --user admin --pass admin

# 3. Create children on different nodes
mqdb create posts --data '{"title": "Post 1", "author_id": "<author-id>"}' \
  --broker 127.0.0.1:1884 --user admin --pass admin
mqdb create posts --data '{"title": "Post 2", "author_id": "<author-id>"}' \
  --broker 127.0.0.1:1885 --user admin --pass admin
mqdb create posts --data '{"title": "Post 3", "author_id": "<author-id>"}' \
  --broker 127.0.0.1:1883 --user admin --pass admin

# 4. Delete parent
mqdb delete authors <author-id> --broker 127.0.0.1:1883 --user admin --pass admin

# 5. Verify all posts deleted
mqdb list posts --broker 127.0.0.1:1883 --user admin --pass admin
```

**Expected:** All posts are cascade deleted across all nodes.

### Set Null Across Nodes

```bash
# 1. Create parent on Node 1
mqdb create authors --data '{"name": "Bob"}' --broker 127.0.0.1:1883 --user admin --pass admin
# Note the ID

# 2. Add set_null FK
mqdb constraint add posts --fk "author_id:authors:id:set_null" \
  --broker 127.0.0.1:1883 --user admin --pass admin

# 3. Create children
mqdb create posts --data '{"title": "Post 1", "author_id": "<author-id>"}' \
  --broker 127.0.0.1:1884 --user admin --pass admin
mqdb create posts --data '{"title": "Post 2", "author_id": "<author-id>"}' \
  --broker 127.0.0.1:1885 --user admin --pass admin

# 4. Delete parent
mqdb delete authors <author-id> --broker 127.0.0.1:1883 --user admin --pass admin

# 5. Verify posts still exist with null FK
mqdb list posts --broker 127.0.0.1:1883 --user admin --pass admin
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
  --broker 127.0.0.1:1883 --user admin --pass admin

# 3. Update only price field
mqdb update products <id> --data '{"price": 150}' --broker 127.0.0.1:1883 --user admin --pass admin

# 4. Read and verify ALL fields preserved
mqdb read products <id> --broker 127.0.0.1:1883 --user admin --pass admin
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
mqdb update products <id> --data '{"discount": true}' --broker 127.0.0.1:1883 --user admin --pass admin

# 2. Read and verify new field added
mqdb read products <id> --broker 127.0.0.1:1883 --user admin --pass admin
```

**Expected:** Entity now has `discount: true` plus all original fields.

### Test 3: Schema Commands in Cluster Mode

Verify schema set/get works across cluster nodes.

```bash
# 1. Start cluster
mqdb dev start-cluster --nodes 3 --clean

# 2. Create schema file (uses flat field-map format)
echo '{"title": {"type": "string", "required": true}}' > /tmp/articles_schema.json

# 3. Set schema on Node 1
mqdb schema set articles --file /tmp/articles_schema.json --broker 127.0.0.1:1883 \
    --user admin --pass admin

# 4. Get schema from Node 2 (verifies broadcast)
mqdb schema get articles --broker 127.0.0.1:1884 --user admin --pass admin

# 5. Get schema from Node 3 (verifies broadcast)
mqdb schema get articles --broker 127.0.0.1:1885 --user admin --pass admin
```

**Expected:** All nodes return the same schema (stored format uses `field_type` not `type`):
```json
{
  "data": {
    "entity": "articles",
    "fields": {
      "title": {"field_type": "String", "name": "title", "required": true, "default": null}
    },
    "version": 1
  },
  "status": "ok"
}
```

### Test 4: Schema Update Broadcasts

```bash
# 1. Update schema with new field
echo '{"title": {"type": "string", "required": true}, "content": {"type": "string"}}' \
  > /tmp/articles_schema2.json

# 2. Update schema on Node 1
mqdb schema set articles --file /tmp/articles_schema2.json --broker 127.0.0.1:1883 \
    --user admin --pass admin

# 3. Verify update on Node 3
mqdb schema get articles --broker 127.0.0.1:1885 --user admin --pass admin
```

**Expected:** Node 3 shows updated schema with version 2.

### Test 5: Cross-Node Update Merge

Verify UPDATE merge works when connecting to different nodes.

```bash
# 1. Create on Node 1
mqdb create items --data '{"a": 1, "b": 2, "c": 3}' --broker 127.0.0.1:1883 --user admin --pass admin
# Note ID

# 2. Update via Node 2
mqdb update items <id> --data '{"b": 20}' --broker 127.0.0.1:1884 --user admin --pass admin

# 3. Read from Node 3
mqdb read items <id> --broker 127.0.0.1:1885 --user admin --pass admin
```

**Expected:** `{"a": 1, "b": 20, "c": 3}` - only `b` changed.

---

## Complete Verification Checklist

Run through this checklist to verify MQDB works completely:

### Agent Mode
- [ ] Agent start/stop (with `--passwd`)
- [ ] Agent status returns health JSON
- [ ] CRUD operations (create, read, update, delete, list)
- [ ] Filtering and sorting
- [ ] Output formats (`--format json`, `--format table`, `--format csv`)
- [ ] Schema validation
- [ ] Index management (`index add`)
- [ ] Constraints (unique, not-null, foreign key)
- [ ] Unique constraint enforced on updates (conflict returns 409)
- [ ] Unique constraint allows non-unique field updates
- [ ] Unique constraint old value released on update
- [ ] Cascade delete removes children
- [ ] Set-null nullifies FK field (children remain, version bumped)
- [ ] Cascade delete emits Delete ChangeEvents for children
- [ ] Set-null emits Update ChangeEvents for modified children
- [ ] Watch/Subscribe
- [ ] Subscribe with consumer group (load-balanced mode)
- [ ] Consumer groups (list, show)
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
- [ ] `mqdb bench pubsub` runs successfully
- [ ] `mqdb bench db` runs successfully
- [ ] `mqdb dev bench pubsub` (auto-start) runs successfully
- [ ] `mqdb dev bench db` (auto-start) runs successfully
- [ ] `mqdb dev profile pubsub` produces profiling output
- [ ] `mqdb dev baseline save/list/compare` works correctly

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
mqdb acl check alice '$DB/users/create' write -f /tmp/acl.txt

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
mqdb dev kill --agent
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
{"name": {"type": "string"}, "email": {"type": "string"}}
EOF
mqdb schema set users_strict -f /tmp/users_schema.json --user testuser --pass testpass
mqdb create users_strict -d '{"name": "Dave", "email": "dave@example.com"}' --user testuser --pass testpass
ID2=$(mqdb list users_strict --user testuser --pass testpass | python3 -c "import sys,json; print(json.load(sys.stdin)['data'][0]['id'])")
mqdb read users_strict "$ID2" --projection nonexistent --user testuser --pass testpass
```

Expected: error response indicating the projection field does not exist in the schema.

#### Cleanup

```bash
mqdb dev kill --agent
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

---

## 25. Vault Encryption Testing

### Prerequisites

Vault requires:
- `--ownership` flag for at least one entity
- `--http-bind` for the vault HTTP API
- Authentication (`--passwd` or `--scram-file` + `--jwt-algorithm` + `--jwt-key`)

### Agent Mode Setup

```bash
mqdb passwd vault-user -b vault-pass -f /tmp/vault-test/passwd.txt
openssl rand -base64 32 > /tmp/vault-test/jwt.key

mqdb agent start --db /tmp/vault-test/db --bind 127.0.0.1:1883 \
    --http-bind 127.0.0.1:3000 --ws-bind 127.0.0.1:8083 \
    --passwd /tmp/vault-test/passwd.txt --jwt-algorithm hs256 --jwt-key /tmp/vault-test/jwt.key \
    --ownership notes=userId --no-rate-limit
```

### Test 1: Login and Enable Vault

```bash
SESSION=$(curl -s -c - -X POST http://127.0.0.1:3000/dev-login \
    -H 'Content-Type: application/json' \
    -d '{"username":"vault-user","email":"test@example.com","name":"Tester"}' \
    | grep session | awk '{print $NF}')

curl -s -b "session=$SESSION" -X POST http://127.0.0.1:3000/vault/enable \
    -H 'Content-Type: application/json' \
    -d '{"passphrase":"my-secret"}'
```

Expected: `{"records_encrypted":0,"status":"enabled"}`

### Test 2: Create Record (Encrypted on Write)

```bash
mqdb create notes --data '{"userId":"vault-user","title":"Secret","body":"Confidential"}' \
    --user vault-user --pass vault-pass
```

Expected: response shows plaintext `title` and `body` (vault transparently decrypts for the owner).

### Test 3: Verify Encryption at Rest

Read the same record from a different user (or with vault locked):

```bash
curl -s -b "session=$SESSION" -X POST http://127.0.0.1:3000/vault/lock
```

Then read:

```bash
mqdb read notes <id> --user vault-user --pass vault-pass
```

Expected: `title` and `body` are base64 ciphertext (20+ characters), `id` and `userId` are plaintext.

### Test 4: Unlock and Read Plaintext

```bash
curl -s -b "session=$SESSION" -X POST http://127.0.0.1:3000/vault/unlock \
    -H 'Content-Type: application/json' \
    -d '{"passphrase":"my-secret"}'
```

Then read:

```bash
mqdb read notes <id> --user vault-user --pass vault-pass
```

Expected: `title` = "Secret", `body` = "Confidential" (plaintext restored).

### Test 5: Update with Vault Enabled

```bash
mqdb update notes <id> --data '{"body":"Updated body"}' --user vault-user --pass vault-pass
```

Expected: response shows plaintext for all fields. Lock vault and re-read to verify `body` is now different ciphertext.

### Test 6: List with Vault (Scatter-Gather Decryption)

Create multiple notes, then list:

```bash
mqdb list notes --user vault-user --pass vault-pass
```

Expected: all titles and bodies in plaintext. Lock vault and list again — all should be ciphertext.

### Test 7: Change Passphrase

```bash
curl -s -b "session=$SESSION" -X POST http://127.0.0.1:3000/vault/change \
    -H 'Content-Type: application/json' \
    -d '{"old_passphrase":"my-secret","new_passphrase":"new-secret"}'
```

Expected: `{"records_re_encrypted":N,"status":"changed"}` where N matches record count.

Lock, then verify old passphrase is rejected:

```bash
curl -s -b "session=$SESSION" -X POST http://127.0.0.1:3000/vault/lock
curl -s -o /dev/null -w '%{http_code}' -b "session=$SESSION" \
    -X POST http://127.0.0.1:3000/vault/unlock \
    -H 'Content-Type: application/json' \
    -d '{"passphrase":"my-secret"}'
```

Expected: HTTP 401.

### Test 8: Disable Vault

```bash
curl -s -b "session=$SESSION" -X POST http://127.0.0.1:3000/vault/unlock \
    -H 'Content-Type: application/json' \
    -d '{"passphrase":"new-secret"}'

curl -s -b "session=$SESSION" -X POST http://127.0.0.1:3000/vault/disable \
    -H 'Content-Type: application/json' \
    -d '{"passphrase":"new-secret"}'
```

Expected: `{"records_decrypted":N,"status":"disabled"}`. All records now stored as plaintext.

### Test 9: Vault Status

```bash
curl -s -b "session=$SESSION" http://127.0.0.1:3000/vault/status
```

Expected after disable: `{"unlocked":false,"vault_enabled":false}`.

### Cluster Mode Vault Testing

Use the automated E2E script:

```bash
examples/vault-cluster/run.sh
```

This runs 70 tests covering:
- Enable vault, CRUD with encryption/decryption on the primary HTTP node
- Observer (different user) always sees ciphertext
- Cross-node read/create/list via a second HTTP node
- Lock/unlock cycle with ciphertext/plaintext verification
- 4th node rebalance with vault re-enable
- Scatter-gather list decryption across 4 nodes
- Change passphrase and old-passphrase rejection
- Disable vault and verify all records decrypted

### Verification Checklist

**Agent Mode:**
- [ ] Vault enable encrypts existing records
- [ ] Create with vault returns plaintext to owner
- [ ] Locked vault read returns ciphertext
- [ ] Unlock restores plaintext reads
- [ ] Update encrypts delta fields
- [ ] List decrypts all records
- [ ] Non-string fields (number, bool, null) unchanged at all depths
- [ ] Nested object string values encrypted recursively
- [ ] Array string elements encrypted recursively
- [ ] `_`-prefixed keys skipped at all depths
- [ ] `id` and ownership field skipped at top level only (encrypted when nested)
- [ ] System fields (`_version`, etc.) never encrypted
- [ ] Change passphrase re-encrypts all records
- [ ] Old passphrase rejected after change (HTTP 401)
- [ ] Disable decrypts all records back to plaintext
- [ ] Vault status reflects current state

**Cluster Mode:**
- [ ] Cross-node vault decrypt works (read from non-primary node)
- [ ] Cross-node vault encrypt works (create from non-primary node)
- [ ] Scatter-gather list decrypts results from all partitions
- [ ] Observer without vault key sees ciphertext in cluster mode
- [ ] Vault survives node rebalance (new node joins, data accessible)
- [ ] All 70 E2E tests pass (`examples/vault-cluster/run.sh`)

---

## 26. OAuth/Identity HTTP Endpoints

The HTTP server provides OAuth 2.0 login, session management, and identity linking.
These endpoints require `--http-bind` and OAuth provider configuration.

### Prerequisites

```bash
mqdb passwd admin -b admin123 -f /tmp/oauth-test/passwd.txt
openssl rand -base64 32 > /tmp/oauth-test/jwt.key

mqdb agent start --db /tmp/oauth-test/db --bind 127.0.0.1:1883 \
    --http-bind 127.0.0.1:3000 \
    --passwd /tmp/oauth-test/passwd.txt \
    --jwt-algorithm hs256 --jwt-key /tmp/oauth-test/jwt.key \
    --oauth-client-secret /path/to/google-client-secret.json \
    --ownership notes=userId
```

### HTTP Health Check

```bash
curl -s http://127.0.0.1:3000/health
```

Expected: `{"oauth_enabled":true,"status":"ok"}`

### OAuth Authorize (Redirect)

```bash
curl -s -o /dev/null -w '%{http_code} %{redirect_url}' \
    'http://127.0.0.1:3000/oauth/authorize?provider=google'
```

Expected: HTTP 302 redirect to Google's OAuth consent page with PKCE `code_challenge`.

### OAuth Callback

The callback endpoint (`GET /oauth/callback?code=...&state=...`) is invoked by the OAuth
provider after user consent. It:
1. Exchanges the authorization code for tokens (with PKCE verifier)
2. Verifies the `id_token`
3. Creates or links an identity in `_identities`/`_identity_links`
4. Creates an HTTP session with a `Set-Cookie` header
5. Redirects to `--oauth-frontend-redirect` URI with base64-encoded user info

This endpoint cannot be tested with curl alone (requires a real OAuth provider flow).

### Token Refresh

```bash
curl -s -X POST http://127.0.0.1:3000/oauth/refresh \
    -H 'Content-Type: application/json' \
    -d '{"token":"<expired-jwt>"}'
```

Expected on success: `{"expires_in":3600,"token":"<new-jwt>"}`
Expected on invalid token: HTTP 401 `{"error":"invalid or tampered token"}`
Expected on revoked token: HTTP 401 `{"error":"token has been revoked"}`

### Session Status

```bash
curl -s -b "session=$SESSION" http://127.0.0.1:3000/auth/session
```

Expected when authenticated:
```json
{"authenticated":true,"user":{"canonical_id":"...","email":"...","name":"...","picture":null,"provider":"...","provider_sub":"..."}}
```

Expected when not authenticated: `{"authenticated":false}`

### Ticket Request (Short-lived JWT)

```bash
curl -s -b "session=$SESSION" -X POST http://127.0.0.1:3000/auth/ticket
```

Expected: `{"expires_in":30,"ticket":"<jwt>"}`

The ticket JWT has `"ticket": true` in claims and expires quickly (default 30s,
configurable via `--ticket-expiry-secs`).

Rate limited: `--ticket-rate-limit` requests per minute per IP (default: 10).

### Logout

```bash
curl -s -b "session=$SESSION" -X POST http://127.0.0.1:3000/auth/logout
```

Expected: `{"status":"logged_out"}` with `Set-Cookie` that clears the session cookie.
The session JWT's `jti` is also revoked.

### Identity Unlinking

```bash
curl -s -b "session=$SESSION" -X POST http://127.0.0.1:3000/auth/unlink \
    -H 'Content-Type: application/json' \
    -d '{"provider":"google","provider_sub":"12345"}'
```

Expected: `{"status":"unlinked"}`
Expected when only one provider linked: HTTP 400 `{"error":"cannot unlink last remaining provider"}`
Expected when link belongs to another user: HTTP 403

### Dev Login (dev-insecure feature only)

```bash
curl -s -c - -X POST http://127.0.0.1:3000/auth/dev-login \
    -H 'Content-Type: application/json' \
    -d '{"email":"test@example.com","name":"Test User"}'
```

Expected: `{"canonical_id":"...","email":"test@example.com","name":"Test User"}`
with `Set-Cookie: session=...` header.

### CORS Configuration

Use `--cors-origin http://localhost:8000` to enable credentialed CORS for:
`/auth/ticket`, `/auth/logout`, `/auth/session`, `/auth/unlink`, `/oauth/refresh`,
and all `/vault/*` endpoints.

### OAuth CLI Options Reference

| Option | Description |
|--------|-------------|
| `--http-bind` | HTTP server bind address (e.g. `0.0.0.0:3000`) |
| `--oauth-client-secret` | Path to OAuth client secret JSON file |
| `--oauth-redirect-uri` | Override OAuth callback URI |
| `--oauth-frontend-redirect` | URI to redirect browser after OAuth completes |
| `--ticket-expiry-secs` | Ticket JWT lifetime (default: 30) |
| `--cookie-secure` | Set `Secure` flag on session cookies (requires HTTPS) |
| `--cors-origin` | CORS allowed origin for auth endpoints |
| `--ticket-rate-limit` | Max ticket requests per minute per IP (default: 10) |
| `--trust-proxy` | Trust `X-Forwarded-For` for client IP (behind reverse proxy) |
| `--identity-key-file` | Path to 32-byte identity encryption key (auto-generated if omitted) |

### Verification Checklist

- [ ] `/health` returns `{"oauth_enabled":true,"status":"ok"}`
- [ ] `/oauth/authorize` redirects to provider consent page
- [ ] `/oauth/callback` creates identity and session
- [ ] `/auth/session` returns user info when session cookie present
- [ ] `/auth/session` returns `{"authenticated":false}` without cookie
- [ ] `/auth/ticket` returns short-lived JWT with `ticket: true`
- [ ] `/auth/ticket` rate limited after N requests per minute
- [ ] `/auth/logout` clears session and revokes JWT `jti`
- [ ] `/auth/unlink` removes provider link (prevents unlinking last provider)
- [ ] `/oauth/refresh` issues new JWT from expired token + stored refresh token
- [ ] CORS headers present when `--cors-origin` set

---

## 27. Runtime ACL/User Management via MQTT

Users and ACL rules can be managed at runtime via MQTT `$DB/_admin/` topics,
without restarting the broker or editing files. Requires authentication to be configured.

### Setup

```bash
mqdb passwd admin -b admin123 -f /tmp/rtacl/passwd.txt
mqdb agent start --db /tmp/rtacl/db --bind 127.0.0.1:1883 \
    --passwd /tmp/rtacl/passwd.txt --admin-users admin
```

### User Management

Add a user:
```bash
mosquitto_rr -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/_admin/users/add' -e 'resp/test' -W 5 \
    -m '{"username":"alice","password":"alice123"}'
```

Expected: `{"data":{"message":"user added"},"status":"ok"}`

List users:
```bash
mosquitto_rr -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/_admin/users/list' -e 'resp/test' -W 5 -m '{}'
```

Expected: `{"data":["admin","alice"],"status":"ok"}`

Delete a user:
```bash
mosquitto_rr -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/_admin/users/delete' -e 'resp/test' -W 5 \
    -m '{"username":"alice"}'
```

Expected: `{"data":{"message":"user deleted"},"status":"ok"}`

### ACL Rule Management

Add a rule:
```bash
mosquitto_rr -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/_admin/acl/rules/add' -e 'resp/test' -W 5 \
    -m '{"user":"alice","topic":"$DB/users/#","access":"readwrite"}'
```

Expected: `{"data":{"message":"rule added"},"status":"ok"}`

List rules:
```bash
mosquitto_rr -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/_admin/acl/rules/list' -e 'resp/test' -W 5 \
    -m '{"user":"alice"}'
```

Expected: `{"data":[{"access":"readwrite","topic":"$DB/users/#","user":"alice"}],"status":"ok"}`

Remove a rule:
```bash
mosquitto_rr -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/_admin/acl/rules/remove' -e 'resp/test' -W 5 \
    -m '{"user":"alice","topic":"$DB/users/#"}'
```

### ACL Role Management

Create a role with rules:
```bash
mosquitto_rr -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/_admin/acl/roles/add' -e 'resp/test' -W 5 \
    -m '{"role":"editor","rules":[{"topic":"$DB/posts/#","access":"readwrite"},{"topic":"$DB/comments/#","access":"read"}]}'
```

Expected: `{"data":{"message":"role added"},"status":"ok"}`

List roles:
```bash
mosquitto_rr -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/_admin/acl/roles/list' -e 'resp/test' -W 5 -m '{}'
```

Expected: `{"data":{"editor":[{"access":"readwrite","topic":"$DB/posts/#"},{"access":"read","topic":"$DB/comments/#"}]},"status":"ok"}`

Delete a role:
```bash
mosquitto_rr -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/_admin/acl/roles/delete' -e 'resp/test' -W 5 \
    -m '{"role":"editor"}'
```

### Role Assignment

Assign a role to a user:
```bash
mosquitto_rr -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/_admin/acl/assignments/assign' -e 'resp/test' -W 5 \
    -m '{"user":"alice","role":"editor"}'
```

List user's roles:
```bash
mosquitto_rr -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/_admin/acl/assignments/list' -e 'resp/test' -W 5 \
    -m '{"user":"alice"}'
```

Unassign a role:
```bash
mosquitto_rr -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/_admin/acl/assignments/unassign' -e 'resp/test' -W 5 \
    -m '{"user":"alice","role":"editor"}'
```

Access values: `read`, `write`, `readwrite`, `deny` (also accepts `subscribe`, `publish`, `rw`, `all`, `none`).

### Verification Checklist

- [ ] Add user at runtime and authenticate with new credentials
- [ ] Delete user at runtime and verify connection rejected
- [ ] List users returns all password-auth users
- [ ] Add ACL rule at runtime and verify access granted
- [ ] Remove ACL rule and verify access denied
- [ ] List rules with optional user filter
- [ ] Create role with inline rules
- [ ] Delete role
- [ ] Assign/unassign roles to users
- [ ] List user's role assignments
- [ ] All operations require admin user (non-admin gets rejected)
- [ ] Operations return error when auth not configured: `authentication not configured`

---

## 28. Additional Admin MQTT Endpoints

### Entity Catalog

```bash
mosquitto_rr -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/_admin/catalog' -e 'resp/test' -W 5 -m '{}'
```

Expected: JSON with `entities` array, each entry containing:
```json
{
  "name": "users",
  "record_count": 5,
  "schema": {"entity":"users","version":1,"schema":{...}},
  "constraints": [{"name":"email_unique","type":"unique","fields":["email"]}],
  "ownership": null,
  "scope": null
}
```

Plus `server` object with `mode`, `node_id` (cluster), or `vault_enabled` (agent).

### Constraint Removal

```bash
mosquitto_rr -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/_admin/constraint/users/remove' -e 'resp/test' -W 5 \
    -m '{"name":"email_unique"}'
```

Expected: `{"data":{"message":"constraint removed"},"status":"ok"}`
Expected when not found: `{"code":404,"message":"constraint not found","status":"error"}`

### Verification Checklist

- [ ] Catalog returns all entities with schemas, constraints, record counts
- [ ] Catalog includes ownership and scope info when configured
- [ ] Constraint removal by name works
- [ ] Constraint removal returns 404 for non-existent constraint

---

## 29. Advanced Agent/Cluster Options

### WebSocket Transport

```bash
mqdb agent start --db /tmp/ws-test --bind 127.0.0.1:1883 \
    --ws-bind 0.0.0.0:8083 --passwd /tmp/passwd.txt
```

WebSocket clients connect on the `--ws-bind` port using the MQTT-over-WebSocket protocol.
Test with a WebSocket MQTT client (e.g., MQTT.js in browser).

### Storage-Level Encryption (Passphrase File)

```bash
echo 'my-storage-passphrase' > /tmp/passphrase.txt
chmod 600 /tmp/passphrase.txt

mqdb agent start --db /tmp/encrypted-db --bind 127.0.0.1:1883 \
    --passphrase-file /tmp/passphrase.txt --passwd /tmp/passwd.txt
```

This encrypts the entire storage layer with AES-256-GCM. All data at rest is encrypted.
Different from Vault encryption (which encrypts individual fields per-user).

The same passphrase file must be provided on every restart. Wrong passphrase = unreadable data.

### Event Scope

```bash
mqdb agent start --db /tmp/scope-test --bind 127.0.0.1:1883 \
    --event-scope customers=customerId --passwd /tmp/passwd.txt --admin-users admin
```

With event scope configured, change events publish to scoped topic paths:
- Without scope: `$DB/orders/events/created`
- With scope `customers=customerId`: `$DB/customers/{customerId}/orders/events/created`

Test:
```bash
# Terminal 1: Subscribe to scoped events
mosquitto_sub -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/customers/cust-1/orders/events/#' -v

# Terminal 2: Create a scoped record
mqdb create orders --data '{"customerId":"cust-1","item":"Widget","qty":5}' \
    --user admin --pass admin123
```

Expected: Terminal 1 receives event on `$DB/customers/cust-1/orders/events/created`.

### Certificate Authentication

```bash
mqdb agent start --db /tmp/cert-auth --bind 127.0.0.1:1883 \
    --cert-auth-file /path/to/cert-auth.json \
    --quic-cert server.pem --quic-key server.key
```

Certificate authentication maps client TLS certificate fields (CN, SAN) to usernames.

### Federated JWT Authentication

```bash
cat > /tmp/federated-jwt.json << 'EOF'
[
  {
    "issuer": "https://accounts.google.com",
    "jwks_uri": "https://www.googleapis.com/oauth2/v3/certs",
    "audience": "my-app-id"
  },
  {
    "issuer": "https://login.microsoftonline.com/{tenant}/v2.0",
    "jwks_uri": "https://login.microsoftonline.com/{tenant}/discovery/v2.0/keys",
    "audience": "my-app-id"
  }
]
EOF

mqdb agent start --db /tmp/fed-jwt --bind 127.0.0.1:1883 \
    --federated-jwt-config /tmp/federated-jwt.json
```

Accepts JWTs from multiple identity providers. Mutually exclusive with `--jwt-algorithm`.

### Durability Modes

```bash
# Immediate: fsync every write (safest, slowest)
mqdb agent start --db /tmp/dur-test --durability immediate --passwd /tmp/passwd.txt

# Periodic: fsync every N ms (default: 10ms)
mqdb agent start --db /tmp/dur-test --durability periodic --durability-ms 50 --passwd /tmp/passwd.txt

# None: no fsync (fastest, data loss on crash)
mqdb agent start --db /tmp/dur-test --durability none --passwd /tmp/passwd.txt
```

### Verification Checklist

- [ ] WebSocket transport accepts connections on `--ws-bind` port
- [ ] Passphrase file encrypts storage (data unreadable without it)
- [ ] Passphrase file required on restart (wrong passphrase = failure)
- [ ] Event scope publishes to `$DB/{scope_entity}/{scope_id}/{entity}/events/{op}`
- [ ] Certificate auth maps TLS client certs to usernames
- [ ] Federated JWT accepts tokens from multiple issuers
- [ ] Durability modes: `immediate` fsyncs every write, `periodic` at interval, `none` skips

---

## 30. Authentication Flow Testing

### SCRAM-SHA-256 Connection Flow

```bash
# Setup
mqdb scram admin -b admin123 -f /tmp/scram-flow/scram.txt

mqdb agent start --db /tmp/scram-flow/db --bind 127.0.0.1:1883 \
    --scram-file /tmp/scram-flow/scram.txt --admin-users admin

# Test SCRAM authentication
mqdb list users --user admin --pass admin123
```

Expected: Successful connection using SCRAM-SHA-256 challenge-response.

```bash
# Test wrong password
mqdb list users --user admin --pass wrongpassword
```

Expected: Authentication failure.

### JWT Connection Flow

```bash
# Setup
openssl rand -base64 32 > /tmp/jwt-flow/secret.key

mqdb agent start --db /tmp/jwt-flow/db --bind 127.0.0.1:1883 \
    --jwt-algorithm hs256 --jwt-key /tmp/jwt-flow/secret.key \
    --jwt-issuer myapp --jwt-audience mqdb

# Generate a test JWT (requires a JWT tool like `jwt-cli` or python)
python3 -c "
import json, hmac, hashlib, base64, time
key = open('/tmp/jwt-flow/secret.key','rb').read().strip()
header = base64.urlsafe_b64encode(json.dumps({'alg':'HS256','typ':'JWT'}).encode()).rstrip(b'=')
payload = base64.urlsafe_b64encode(json.dumps({
    'sub':'testuser','iss':'myapp','aud':'mqdb',
    'exp':int(time.time())+3600,'iat':int(time.time())
}).encode()).rstrip(b'=')
msg = header + b'.' + payload
sig = base64.urlsafe_b64encode(hmac.new(base64.b64decode(key),msg,hashlib.sha256).digest()).rstrip(b'=')
print((msg + b'.' + sig).decode())
" > /tmp/jwt-flow/token.txt

# Connect with JWT (via mqttv5 CLI or compatible client)
# mqttv5 sub --auth-method jwt --jwt-token "$(cat /tmp/jwt-flow/token.txt)" -H 127.0.0.1 -p 1883 -t test
```

### Rate Limiting Lockout Behavior

```bash
mqdb passwd admin -b admin123 -f /tmp/ratelimit/passwd.txt

mqdb agent start --db /tmp/ratelimit/db --bind 127.0.0.1:1883 \
    --passwd /tmp/ratelimit/passwd.txt \
    --rate-limit-max-attempts 3 --rate-limit-window-secs 60 --rate-limit-lockout-secs 120

# Trigger lockout: 3 failed attempts
for i in 1 2 3; do
    mosquitto_pub -h 127.0.0.1 -p 1883 -u admin -P wrongpass -t test -m test 2>&1 || true
done

# 4th attempt should be locked out even with correct password
mosquitto_pub -h 127.0.0.1 -p 1883 -u admin -P admin123 -t test -m test
```

Expected: After 3 failed attempts within 60 seconds, the client IP is locked out for 120 seconds.
The 4th connection attempt (even with correct credentials) is rejected.

### Verification Checklist

- [ ] SCRAM-SHA-256 authenticates successfully with correct password
- [ ] SCRAM-SHA-256 rejects wrong password
- [ ] JWT auth accepts valid token with matching issuer/audience
- [ ] JWT auth rejects expired/invalid tokens
- [ ] Rate limiting locks out after N failed attempts
- [ ] Lockout applies to correct credentials too (IP-based)
- [ ] Lockout expires after configured duration

---

## 31. Output Format Examples

All CRUD and list commands support `--format json` (default), `--format table`, and `--format csv`.

### JSON Output (default)

```bash
mqdb list users --format json --user admin --pass admin123
```

```json
[
  {"age": 25, "email": "alice@example.com", "id": "abc123", "name": "Alice"},
  {"age": 30, "email": "bob@example.com", "id": "def456", "name": "Bob"}
]
```

### Table Output

```bash
mqdb list users --format table --user admin --pass admin123
```

```
id       | name  | email              | age
---------+-------+--------------------+----
abc123   | Alice | alice@example.com  | 25
def456   | Bob   | bob@example.com    | 30
```

### CSV Output

```bash
mqdb list users --format csv --user admin --pass admin123
```

```
id,name,email,age
abc123,Alice,alice@example.com,25
def456,Bob,bob@example.com,30
```

### Single Record Formats

```bash
mqdb read users abc123 --format table --user admin --pass admin123
```

Table and CSV formats work for single-record responses too (`read`, `create`, `update`).

### Verification Checklist

- [ ] `--format json` outputs valid JSON array for list, JSON object for single record
- [ ] `--format table` outputs aligned columns with header
- [ ] `--format csv` outputs CSV with header row
- [ ] All three formats work for: list, read, create, update, schema get, constraint list

---

## 32. Query Limits and Enforcement

MQDB enforces hard limits on query complexity: `MAX_FILTERS=16`, `MAX_SORT_FIELDS=4`, and `MAX_LIST_RESULTS=10,000`.

### Setup

```bash
mqdb agent start --db /tmp/mqdb-limits-test --bind 127.0.0.1:1883 --passwd /tmp/passwd.txt --admin-users admin
```

Create test entity:
```bash
mqdb create products --data '{"name":"Widget","price":10,"category":"tools"}' --user admin --pass admin123
```

### 17 Filters Returns Error

```bash
mqdb list products \
    --filter 'name=a' --filter 'name=b' --filter 'name=c' --filter 'name=d' \
    --filter 'name=e' --filter 'name=f' --filter 'name=g' --filter 'name=h' \
    --filter 'name=i' --filter 'name=j' --filter 'name=k' --filter 'name=l' \
    --filter 'name=m' --filter 'name=n' --filter 'name=o' --filter 'name=p' \
    --filter 'name=q' \
    --user admin --pass admin123
```

**Expected:** Error response with `"validation error: too many filters: 17 exceeds maximum of 16"`

### 5 Sort Fields Returns Error

```bash
mqdb list products --sort 'a:asc,b:asc,c:asc,d:asc,e:asc' --user admin --pass admin123
```

**Expected:** Error response with `"validation error: too many sort fields: 5 exceeds maximum of 4"`

### 16 Filters Succeeds (Boundary)

```bash
mqdb list products \
    --filter 'name=a' --filter 'name=b' --filter 'name=c' --filter 'name=d' \
    --filter 'name=e' --filter 'name=f' --filter 'name=g' --filter 'name=h' \
    --filter 'name=i' --filter 'name=j' --filter 'name=k' --filter 'name=l' \
    --filter 'name=m' --filter 'name=n' --filter 'name=o' --filter 'name=p' \
    --user admin --pass admin123
```

**Expected:** Empty result set `[]` (no records match all 16 filters), but no error — the query is accepted.

### 4 Sort Fields Succeeds (Boundary)

```bash
mqdb list products --sort 'name:asc,price:desc,category:asc,id:desc' --user admin --pass admin123
```

**Expected:** Products listed sorted by the 4-field sort key. No error.

### Large Result Set Truncation

> **Note:** This test requires creating >10,000 records. The broker silently truncates results to 10,000 items and logs the truncation server-side.

```bash
# Create 10,001 records using a loop
for i in $(seq 1 10001); do
    mqdb create items --data "{\"n\":$i}" --user admin --pass admin123 > /dev/null
done

# List all
mqdb list items --user admin --pass admin123 | python3 -c "import sys,json; print(len(json.load(sys.stdin)))"
```

**Expected:** Output is `10000` — the result is silently truncated.

### Cluster Mode

Repeat the 17-filter and 5-sort-field tests from a non-primary node (dev cluster uses credentials `admin`/`admin`):

```bash
mqdb dev start-cluster --nodes 3 --clean
sleep 10
mqdb create products --data '{"name":"Widget"}' --user admin --pass admin --broker 127.0.0.1:1884

mqdb list products \
    --filter 'name=a' --filter 'name=b' --filter 'name=c' --filter 'name=d' \
    --filter 'name=e' --filter 'name=f' --filter 'name=g' --filter 'name=h' \
    --filter 'name=i' --filter 'name=j' --filter 'name=k' --filter 'name=l' \
    --filter 'name=m' --filter 'name=n' --filter 'name=o' --filter 'name=p' \
    --filter 'name=q' \
    --user admin --pass admin --broker 127.0.0.1:1885
```

**Expected:** Error `"validation error: too many filters: 17 exceeds maximum of 16"` from any node (same format as agent mode).

### Verification Checklist

- [ ] 17 filters returns 400 error with correct message
- [ ] 5 sort fields returns 400 error with correct message
- [ ] 16 filters succeeds (boundary)
- [ ] 4 sort fields succeeds (boundary)
- [ ] >10,000 results silently truncated to 10,000
- [ ] Limit errors reproduced from non-primary cluster node

---

## 33. Sort Edge Cases

### Setup

```bash
mqdb agent start --db /tmp/mqdb-sort-test --bind 127.0.0.1:1883 --passwd /tmp/passwd.txt --admin-users admin
```

### Sort on Missing Field

Create records where some lack the sort field:

```bash
mqdb create items --data '{"name":"Alpha","priority":1}' --user admin --pass admin123
mqdb create items --data '{"name":"Beta","priority":3}' --user admin --pass admin123
mqdb create items --data '{"name":"Gamma"}' --user admin --pass admin123
mqdb create items --data '{"name":"Delta","priority":2}' --user admin --pass admin123
mqdb create items --data '{"name":"Epsilon"}' --user admin --pass admin123
```

```bash
mqdb list items --sort 'priority:asc' --user admin --pass admin123
```

**Expected:** Records missing `priority` sort to the FRONT (ascending). Order: Gamma, Epsilon (no priority), then Alpha (1), Delta (2), Beta (3).

```bash
mqdb list items --sort 'priority:desc' --user admin --pass admin123
```

**Expected:** Records missing `priority` sort to the BACK (descending). Order: Beta (3), Delta (2), Alpha (1), then Gamma, Epsilon (no priority).

### Sort on Mixed Types

Create records with a field that has both number and string values:

```bash
mqdb create mixed --data '{"name":"A","value":100}' --user admin --pass admin123
mqdb create mixed --data '{"name":"B","value":"hello"}' --user admin --pass admin123
mqdb create mixed --data '{"name":"C","value":5}' --user admin --pass admin123
```

```bash
mqdb list mixed --sort 'value:asc' --user admin --pass admin123
```

**Expected:** All records returned. Ordering depends on JSON value comparison (numbers before strings, or vice versa). Verify the output is deterministic.

### Multi-Field Sort

```bash
mqdb create employees --data '{"dept":"Engineering","name":"Charlie","salary":90}' --user admin --pass admin123
mqdb create employees --data '{"dept":"Engineering","name":"Alice","salary":120}' --user admin --pass admin123
mqdb create employees --data '{"dept":"Sales","name":"Bob","salary":80}' --user admin --pass admin123
mqdb create employees --data '{"dept":"Engineering","name":"Bob","salary":100}' --user admin --pass admin123
mqdb create employees --data '{"dept":"Sales","name":"Alice","salary":95}' --user admin --pass admin123
```

```bash
mqdb list employees --sort 'dept:asc,name:asc' --user admin --pass admin123
```

**Expected:** Primary sort by `dept`, secondary sort by `name` within each department:
1. Engineering, Alice
2. Engineering, Bob
3. Engineering, Charlie
4. Sales, Alice
5. Sales, Bob

### Sort with All Null Values

All records missing the sort field:

```bash
mqdb create nullsort --data '{"name":"A"}' --user admin --pass admin123
mqdb create nullsort --data '{"name":"B"}' --user admin --pass admin123
mqdb create nullsort --data '{"name":"C"}' --user admin --pass admin123
```

```bash
mqdb list nullsort --sort 'missing_field:asc' --user admin --pass admin123
```

**Expected:** All 3 records returned. Order is stable (all are equal for the sort key).

### Verification Checklist

- [ ] Records missing sort field sort to front (ascending)
- [ ] Records missing sort field sort to back (descending)
- [ ] Mixed-type sort returns all records deterministically
- [ ] Multi-field sort applies correct priority (primary then secondary)
- [ ] Sort on universally missing field returns all records

---

## 34. Filter Edge Cases

### Setup

```bash
mqdb agent start --db /tmp/mqdb-filter-test --bind 127.0.0.1:1883 --passwd /tmp/passwd.txt --admin-users admin
```

### Filter on Nonexistent Field Without Schema

No schema set — filter on a field no record has:

```bash
mqdb create products --data '{"name":"Widget","price":10}' --user admin --pass admin123
mqdb create products --data '{"name":"Gadget","price":25}' --user admin --pass admin123

mqdb list products --filter 'color=red' --user admin --pass admin123
```

**Expected:** Empty result set `[]` — no records have `color`, so none match. No error because there is no schema to validate against.

### Filter on Nonexistent Field With Schema

Set a schema, then filter on a field not in it:

```bash
echo '{"name":{"type":"string"},"price":{"type":"number"}}' > /tmp/widgets_schema.json
mqdb schema set widgets --file /tmp/widgets_schema.json --user admin --pass admin123
mqdb create widgets --data '{"name":"Sprocket","price":15}' --user admin --pass admin123

mqdb list widgets --filter 'color=red' --user admin --pass admin123
```

**Expected:** Error: `"schema validation failed: color - filter field does not exist in schema"`

### Sort on Nonexistent Field With Schema

```bash
mqdb list widgets --sort 'color:asc' --user admin --pass admin123
```

**Expected:** Error: `"schema validation failed: color - sort field does not exist in schema"`

### Projection on Nonexistent Field With Schema

```bash
mqdb list widgets --projection color --user admin --pass admin123
```

**Expected:** Error: `"schema validation failed: color - projection field does not exist in schema"`

### Multiple Filters on Same Field (Range Narrowing)

```bash
mqdb create prices --data '{"item":"A","price":30}' --user admin --pass admin123
mqdb create prices --data '{"item":"B","price":75}' --user admin --pass admin123
mqdb create prices --data '{"item":"C","price":150}' --user admin --pass admin123
mqdb create prices --data '{"item":"D","price":200}' --user admin --pass admin123

mqdb list prices --filter 'price>50' --filter 'price<200' --user admin --pass admin123
```

**Expected:** Only item B (75) and C (150) returned — both filters narrow the range.

### Contradictory Filters

```bash
mqdb list prices --filter 'price>100' --filter 'price<50' --user admin --pass admin123
```

**Expected:** Empty result set `[]` — no value can be both >100 and <50.

### Filter with Empty String

```bash
mqdb create tags --data '{"label":"important"}' --user admin --pass admin123
mqdb create tags --data '{"label":""}' --user admin --pass admin123
mqdb create tags --data '{"label":"urgent"}' --user admin --pass admin123

mqdb list tags --filter 'label=' --user admin --pass admin123
```

**Expected:** Only the record with `"label":""` is returned.

### Like Filter with No Wildcards

```bash
mqdb list tags --filter 'label~important' --user admin --pass admin123
```

**Expected:** Only the record with `"label":"important"` is returned — like filter without `*` wildcards matches the exact value.

### Null Filter Combined with Equality

> **Note:** The `?` (is null) filter matches records where the field exists with an explicit JSON `null` value. Records that simply omit the field are NOT matched — a missing field is treated as "doesn't match any filter", not as null.

```bash
mqdb create inventory --data '{"category":"electronics","description":"A phone"}' --user admin --pass admin123
mqdb create inventory --data '{"category":"electronics","description":null}' --user admin --pass admin123
mqdb create inventory --data '{"category":"furniture","description":"A table"}' --user admin --pass admin123
mqdb create inventory --data '{"category":"furniture","description":null}' --user admin --pass admin123

mqdb list inventory --filter 'category=electronics' --filter 'description?' --user admin --pass admin123
```

**Expected:** Only the electronics record with `"description": null` is returned — `?` matches explicit null values, not missing fields.

### Verification Checklist

- [ ] Filter on nonexistent field without schema returns empty set
- [ ] Filter on nonexistent field with schema returns 400 error
- [ ] Sort on nonexistent field with schema returns 400 error
- [ ] Projection on nonexistent field with schema returns 400 error
- [ ] Multiple filters on same field narrow results correctly
- [ ] Contradictory filters return empty set
- [ ] Filter with empty string matches empty-string records
- [ ] Like filter without wildcards matches exact value
- [ ] Null filter matches explicit null, not missing fields

---

## 35. MQTT-Only Query Features

These features are accessible only via MQTT payloads, not through CLI `--filter` syntax.

### Setup

```bash
mqdb agent start --db /tmp/mqdb-mqtt-query-test --bind 127.0.0.1:1883 --passwd /tmp/passwd.txt --admin-users admin
```

### In Operator

The `in` operator matches records where a field's value is in a given array. This is only available via direct MQTT payload.

```bash
mqdb create products --data '{"name":"Phone","category":"electronics"}' --user admin --pass admin123
mqdb create products --data '{"name":"Laptop","category":"electronics"}' --user admin --pass admin123
mqdb create products --data '{"name":"Desk","category":"furniture"}' --user admin --pass admin123
mqdb create products --data '{"name":"Shirt","category":"clothing"}' --user admin --pass admin123
mqdb create products --data '{"name":"Chair","category":"furniture"}' --user admin --pass admin123
```

```bash
mosquitto_rr -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/products/list' -e 'resp/test' -W 5 \
    -m '{"filters":[{"field":"category","op":"in","value":["electronics","furniture"]}]}'
```

**Expected:** 4 records returned (Phone, Laptop, Desk, Chair) — clothing is excluded.

### In Operator with Empty Array

```bash
mosquitto_rr -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/products/list' -e 'resp/test' -W 5 \
    -m '{"filters":[{"field":"category","op":"in","value":[]}]}'
```

**Expected:** Empty result set — no value is contained in an empty array.

### In Operator Combined with Other Filters

```bash
mosquitto_rr -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/products/list' -e 'resp/test' -W 5 \
    -m '{"filters":[{"field":"category","op":"in","value":["electronics","furniture"]},{"field":"name","op":"eq","value":"Desk"}]}'
```

**Expected:** Only the Desk record returned — it matches both the `in` filter (furniture) and the `eq` filter (name=Desk).

### Includes (Relationships)

Includes load related entities via foreign key relationships. Requires a foreign key constraint between entities.

```bash
# Create parent entity
mqdb create authors --data '{"id":"author-1","name":"Alice"}' --user admin --pass admin123
mqdb create authors --data '{"id":"author-2","name":"Bob"}' --user admin --pass admin123

# Create child entity with FK
mqdb create posts --data '{"title":"First Post","author_id":"author-1"}' --user admin --pass admin123
mqdb create posts --data '{"title":"Second Post","author_id":"author-2"}' --user admin --pass admin123
mqdb create posts --data '{"title":"Third Post","author_id":"author-1"}' --user admin --pass admin123

# Add foreign key constraint (creates the relationship)
mqdb constraint add posts --fk 'author_id:authors:id' --user admin --pass admin123

# List posts with included author
mosquitto_rr -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/posts/list' -e 'resp/test' -W 5 \
    -m '{"includes":["authors"]}'
```

**Expected:** Each post includes its related author data nested in the response.

### Verification Checklist

- [ ] `in` operator via MQTT payload matches records in value array
- [ ] `in` operator with empty array returns empty set
- [ ] `in` operator combined with `eq` filter narrows correctly
- [ ] Includes load related entities via FK relationships

---

## 36. Index Backfill and Interactions

> **Note:** Index management is only supported in agent mode. Cluster mode returns `"index management is only supported in agent mode"`.

### Setup

```bash
mqdb agent start --db /tmp/mqdb-index-test --bind 127.0.0.1:1883 --passwd /tmp/passwd.txt --admin-users admin
```

### Index on Existing Data (Backfill)

Create records FIRST, then add an index. The index backfills existing data.

```bash
mqdb create products --data '{"name":"Phone","price":999}' --user admin --pass admin123
mqdb create products --data '{"name":"Laptop","price":1500}' --user admin --pass admin123
mqdb create products --data '{"name":"Mouse","price":25}' --user admin --pass admin123
mqdb create products --data '{"name":"Keyboard","price":75}' --user admin --pass admin123
mqdb create products --data '{"name":"Monitor","price":400}' --user admin --pass admin123

# Add index AFTER data exists
mqdb index add products --fields price --user admin --pass admin123

# Query using the indexed field
mqdb list products --filter 'price>100' --sort 'price:asc' --user admin --pass admin123
```

**Expected:** Returns Monitor (400), Phone (999), Laptop (1500) — the index was backfilled over existing data, range query works correctly.

### Index Add Is Idempotent

```bash
mqdb index add products --fields price --user admin --pass admin123
mqdb index add products --fields price --user admin --pass admin123
```

**Expected:** Both calls succeed with no error. The second call overwrites the index with the same definition and re-indexes.

### Only First Equality Filter Uses Index

```bash
mqdb index add products --fields name --user admin --pass admin123

mqdb list products --filter 'name=Mouse' --filter 'price>10' --user admin --pass admin123
```

**Expected:** Returns the Mouse record (price 25 > 10). The index on `name` accelerates the first equality filter; the second filter (`price>10`) is applied as a post-filter.

### Range Filter Uses Index

```bash
mqdb index add products --fields price --user admin --pass admin123

mqdb list products --filter 'price>100' --user admin --pass admin123
```

**Expected:** Returns Monitor, Phone, Laptop — the index on `price` is used for the range query.

### Verification Checklist

- [ ] Index on existing data backfills correctly (range query works)
- [ ] Adding same index twice causes no error (idempotent)
- [ ] First equality filter uses index, subsequent filters post-filter
- [ ] Range filter uses index correctly

---

## 37. Cluster Scatter-Gather Query Behavior

In cluster mode, list queries scatter to all nodes holding partitions and gather results. These tests verify consistency across nodes.

### Setup

The dev cluster uses credentials `admin`/`admin` (auto-generated by `mqdb dev start-cluster`).

```bash
mqdb dev start-cluster --nodes 3 --clean
sleep 10
```

Create test data (from node 1):

```bash
for i in $(seq 1 15); do
    mqdb create items --data "{\"name\":\"item-$(printf '%02d' $i)\",\"priority\":$i}" \
        --user admin --pass admin --broker 127.0.0.1:1883
done
```

### Sort Consistency Across Nodes

Query the same sort from each node — all must return identical order:

```bash
mqdb list items --sort 'priority:asc' --user admin --pass admin --broker 127.0.0.1:1883 > /tmp/node1.json
mqdb list items --sort 'priority:asc' --user admin --pass admin --broker 127.0.0.1:1884 > /tmp/node2.json
mqdb list items --sort 'priority:asc' --user admin --pass admin --broker 127.0.0.1:1885 > /tmp/node3.json

diff /tmp/node1.json /tmp/node2.json && diff /tmp/node2.json /tmp/node3.json && echo "PASS: all nodes consistent"
```

**Expected:** All three outputs are identical. No diff output, "PASS" printed.

### Filter+Sort Across Nodes

Test filter+sort across nodes:

```bash
mqdb list items --filter 'priority>5' --sort 'priority:desc' \
    --user admin --pass admin --broker 127.0.0.1:1883 > /tmp/complex1.json
mqdb list items --filter 'priority>5' --sort 'priority:desc' \
    --user admin --pass admin --broker 127.0.0.1:1884 > /tmp/complex2.json
mqdb list items --filter 'priority>5' --sort 'priority:desc' \
    --user admin --pass admin --broker 127.0.0.1:1885 > /tmp/complex3.json

diff /tmp/complex1.json /tmp/complex2.json && diff /tmp/complex2.json /tmp/complex3.json && echo "PASS"
```

**Expected:** All three return the same 10 records (priority 6-15) in descending order. Identical across nodes.

### No Deduplication Issues

```bash
mqdb list items --user admin --pass admin --broker 127.0.0.1:1884 \
    | python3 -c "
import sys,json
d=json.load(sys.stdin)
ids=[r['id'] for r in d['data']]
print('PASS: no dupes' if len(ids)==len(set(ids)) else f'FAIL: {len(ids)-len(set(ids))} dupes')
"
```

**Expected:** "PASS: no dupes" — each record appears exactly once regardless of partition distribution.

### Pagination in Cluster Mode

```bash
mqdb list items --sort 'priority:asc' --limit 3 --user admin --pass admin --broker 127.0.0.1:1884
```

**Expected:** Returns exactly 3 records (priority 1, 2, 3).

```bash
mqdb list items --sort 'priority:asc' --limit 3 --offset 5 --user admin --pass admin --broker 127.0.0.1:1884
```

**Expected:** Returns 3 records starting from position 5 (priority 6, 7, 8).

### Verification Checklist

- [ ] Sort consistency across all 3 nodes (identical order)
- [ ] Filter+sort consistent across all nodes
- [ ] No duplicate records in list results
- [ ] `--limit` returns correct number of records in cluster mode
- [ ] `--limit` with `--offset` returns correct page in cluster mode

---

## 38. Agent vs Cluster Response Format

Agent mode and cluster mode produce identical JSON response formats for all CRUD operations, list queries, pagination, and validation errors. Both modes generate the same partition-prefixed hex IDs — the agent is effectively a single-node cluster owning all 256 partitions.

### Response Format

**Create/Read/Update** — `id` embedded in `data`:
```json
{"status": "ok", "data": {"id": "6fc263177a320176-0011", "name": "Alice", "_version": 1}}
```

**Delete** — `id` and `deleted` flag in `data`:
```json
{"status": "ok", "data": {"id": "6fc263177a320176-0011", "deleted": true}}
```

**List** — flat records with `id` embedded:
```json
{"status": "ok", "data": [{"id": "6fc263177a320176-0011", "name": "Alice", "_version": 1}]}
```

**Validation errors** — same prefix in both modes:
```json
{"status": "error", "code": 400, "message": "validation error: too many filters: 17 exceeds maximum of 16"}
```

### Verification Checklist

- [ ] Create response format identical
- [ ] List response format identical
- [ ] Delete response format identical
- [ ] ID format identical (partition-prefixed hex in both modes)
- [ ] Validation error messages identical
- [ ] Pagination works in both modes

---

## Complete Verification Checklist Additions

### Query Limits (Section 32)
- [ ] 17 filters returns 400 error
- [ ] 5 sort fields returns 400 error
- [ ] 16 filters succeeds (boundary)
- [ ] 4 sort fields succeeds (boundary)

### Sort Edge Cases (Section 33)
- [ ] Records missing sort field sort to front (ascending)
- [ ] Multi-field sort applies correct priority

### Filter Edge Cases (Section 34)
- [ ] Filter on nonexistent field without schema excludes records
- [ ] Filter on nonexistent field with schema returns 400
- [ ] Sort on nonexistent field with schema returns 400
- [ ] Projection on nonexistent field with schema returns 400
- [ ] Contradictory filters return empty set
- [ ] Multiple filters on same field narrows results

### MQTT-Only Features (Section 35)
- [ ] In operator via MQTT payload
- [ ] In operator with empty array returns empty set
- [ ] Includes/relationships via MQTT payload

### Index Interactions (Section 36, Agent Only)
- [ ] Index on existing data backfills correctly
- [ ] Index add is idempotent
- [ ] First Eq filter uses index, rest post-filter

### Cluster Scatter-Gather (Section 37)
- [ ] Sort consistency across all nodes
- [ ] Filter+sort consistent across all nodes
- [ ] No duplicate records in results
- [ ] Pagination works in cluster mode

### Agent vs Cluster Format (Section 38)
- [ ] Create response format identical
- [ ] List response format identical
- [ ] Delete response format identical
- [ ] ID format identical (partition-prefixed hex in both modes)
- [ ] Validation error messages identical
- [ ] Pagination works in both modes
