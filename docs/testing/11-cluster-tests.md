# Cluster Resilience, MQTT Protocol, Constraints, DB Features & Scatter-Gather

[Back to index](README.md)

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
