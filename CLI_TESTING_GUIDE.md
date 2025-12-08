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

### Basic Start

**Terminal 1:**
```bash
mqdb agent start --db ./data/testdb
```

**Expected output:**
```
agent started on mqtt://localhost:1883
database: ./data/testdb
```

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

### Create with Custom ID

```bash
mqdb create users --id user-001 --data '{"name": "Bob", "email": "bob@example.com"}'
```

### Read Entity

```bash
mqdb read users user-001
```

**Expected output:**
```json
{
  "id": "user-001",
  "name": "Bob",
  "email": "bob@example.com"
}
```

### Update Entity

```bash
mqdb update users user-001 --data '{"age": 25}'
```

**Expected output:**
```json
{
  "id": "user-001",
  "name": "Bob",
  "email": "bob@example.com",
  "age": 25
}
```

### Delete Entity

```bash
mqdb delete users user-001
```

**Expected output:**
```
deleted users/user-001
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
mqdb create products --id p1 --data '{"name": "Laptop", "price": 999, "category": "electronics", "stock": 50}'
mqdb create products --id p2 --data '{"name": "Mouse", "price": 29, "category": "electronics", "stock": 200}'
mqdb create products --id p3 --data '{"name": "Desk", "price": 299, "category": "furniture", "stock": 25}'
mqdb create products --id p4 --data '{"name": "Chair", "price": 199, "category": "furniture", "stock": 0}'
mqdb create products --id p5 --data '{"name": "Keyboard", "price": 79, "category": "electronics", "stock": 150}'
```

### Basic List

```bash
mqdb list products
```

### Equality Filter (=)

```bash
mqdb list products --filter 'category=electronics'
```

**Expected:** Returns Laptop, Mouse, Keyboard

### Not Equal Filter (!=)

```bash
mqdb list products --filter 'category!=electronics'
```

**Expected:** Returns Desk, Chair

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
mqdb constraint add users unique email
mqdb create users --data '{"name": "User1", "email": "unique@test.com"}'
mqdb create users --data '{"name": "User2", "email": "unique@test.com"}'
```

**Expected error on second create:**
```
error: unique constraint violation on field 'email'
```

### Unique Constraint (Composite)

```bash
mqdb constraint add orders unique customer_id,product_id
```

### Not-Null Constraint

```bash
mqdb constraint add users not_null name
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
mqdb create authors --id author-1 --data '{"name": "Jane Author"}'
mqdb constraint add posts foreign_key author_id authors cascade
```

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
mqdb create posts --data '{"title": "Post 1", "author_id": "author-1"}'
```

### Cascade Delete

```bash
mqdb delete authors author-1
mqdb list posts
```

**Expected:** Posts with author_id=author-1 are also deleted

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
mqdb restore backup_20241208_143022
```

**Terminal 1:** Restart agent
```bash
mqdb agent start --db ./data/testdb
```

---

## 9. Authentication

### Create Password File

```bash
mqdb passwd add admin --password secret123
mqdb passwd add readonly --password viewer456
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
mqdb --user admin --password secret123 list users
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
mqdb constraint add users not_null email
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
mqdb create users --id test-1 --data '{"name": "Test User", "email": "test@example.com"}'
mqdb read users test-1
mqdb update users test-1 --data '{"age": 25}'
mqdb list users
mqdb delete users test-1

# 3. Filtering and sorting
mqdb create items --id i1 --data '{"name": "A", "value": 10}'
mqdb create items --id i2 --data '{"name": "B", "value": 20}'
mqdb create items --id i3 --data '{"name": "C", "value": 5}'
mqdb list items --filter 'value>5' --sort value:desc

# 4. Schema validation
echo '{"name": {"type": "string", "required": true}}' > /tmp/schema.json
mqdb schema set validated --file /tmp/schema.json
mqdb create validated --data '{}'  # Should fail

# 5. Constraints
mqdb constraint add items unique name
mqdb create items --id i4 --data '{"name": "A", "value": 100}'  # Should fail

# 6. Subscriptions (in separate terminal)
# Terminal 3: mqdb watch items
# Terminal 2: mqdb create items --id i5 --data '{"name": "D", "value": 15}'

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
