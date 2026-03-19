# Authentication, ACL, Topic Protection & Ownership

[Back to index](README.md)

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
