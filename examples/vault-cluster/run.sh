#!/bin/bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
MQDB_BIN="$REPO_ROOT/target/release/mqdb"
TEST_DIR="/tmp/vault-cluster-e2e"
CERT_DIR="$REPO_ROOT/test_certs"

if [[ -z "${MQDB_LICENSE_FILE:-}" ]]; then
    echo "ERROR: cluster + vault require an Enterprise license." >&2
    echo "Set MQDB_LICENSE_FILE to a license key path and retry." >&2
    exit 1
fi
if [[ ! -f "$MQDB_LICENSE_FILE" ]]; then
    echo "ERROR: MQDB_LICENSE_FILE does not exist: $MQDB_LICENSE_FILE" >&2
    exit 1
fi
NODE_PIDS=()
PASS=0
FAIL=0
TOTAL=0

PORT1=18831
PORT2=18832
PORT3=18833
PORT4=18834
HTTP1=13101
HTTP2=13102

cleanup() {
    for pid in "${NODE_PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
    if [[ -n "${SAVE_LOGS:-}" ]]; then
        mkdir -p /tmp/vault-e2e-logs
        cp "$TEST_DIR"/*.log /tmp/vault-e2e-logs/ 2>/dev/null || true
    fi
    rm -rf "$TEST_DIR"
}
trap cleanup EXIT

json_field() {
    local json="$1" path="$2"
    python3 -c "
import sys, json
try:
    d = json.loads(sys.argv[1])
    keys = sys.argv[2].split('.')
    for k in keys:
        if isinstance(d, list):
            d = d[int(k)]
        elif isinstance(d, dict):
            d = d[k]
        else:
            print('__PARSE_ERROR__')
            sys.exit(0)
    if d is None:
        print('__NULL__')
    else:
        print(d)
except (json.JSONDecodeError, KeyError, IndexError, TypeError, ValueError):
    print('__PARSE_ERROR__')
" "$json" "$path" 2>/dev/null
}

assert_eq() {
    local label="$1" actual="$2" expected="$3"
    TOTAL=$((TOTAL + 1))
    if [[ "$actual" == "__PARSE_ERROR__" ]]; then
        echo "  FAIL: $label (JSON parse error)"
        FAIL=$((FAIL + 1))
        return
    fi
    if [[ "$actual" == "$expected" ]]; then
        echo "  PASS: $label"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $label"
        echo "    expected: $expected"
        echo "    actual:   $actual"
        FAIL=$((FAIL + 1))
    fi
}

assert_neq() {
    local label="$1" actual="$2" unexpected="$3"
    TOTAL=$((TOTAL + 1))
    if [[ "$actual" == "__PARSE_ERROR__" ]]; then
        echo "  FAIL: $label (JSON parse error)"
        FAIL=$((FAIL + 1))
        return
    fi
    if [[ -z "$actual" ]]; then
        echo "  FAIL: $label (empty value)"
        FAIL=$((FAIL + 1))
        return
    fi
    if [[ "$actual" != "$unexpected" ]]; then
        echo "  PASS: $label"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $label"
        echo "    should NOT equal: $unexpected"
        echo "    actual:           $actual"
        FAIL=$((FAIL + 1))
    fi
}

assert_min_length() {
    local label="$1" value="$2" min_len="$3"
    TOTAL=$((TOTAL + 1))
    if [[ "$value" == "__PARSE_ERROR__" ]]; then
        echo "  FAIL: $label (JSON parse error)"
        FAIL=$((FAIL + 1))
        return
    fi
    local actual_len=${#value}
    if [[ $actual_len -ge $min_len ]]; then
        echo "  PASS: $label (len=$actual_len >= $min_len)"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $label"
        echo "    expected length >= $min_len, got $actual_len"
        echo "    value: $value"
        FAIL=$((FAIL + 1))
    fi
}

echo "=== Vault Cluster E2E Test ==="
echo ""

echo "Step 0: Setup"
rm -rf "$TEST_DIR"
mkdir -p "$TEST_DIR"

CANONICAL_ID="vault-cluster-user"
MQTT_PASS="testpass"
OBSERVER_USER="observer"
OBSERVER_PASS="observerpass"
PASSPHRASE="cluster-vault-pass"

echo "  Building MQDB with dev-insecure..."
cd "$REPO_ROOT"
cargo build --release --features dev-insecure 2>&1 | tail -3

if [[ ! -f "$CERT_DIR/server.pem" ]]; then
    echo "  Generating test certs..."
    "$REPO_ROOT/scripts/generate_test_certs.sh"
fi

echo "  Creating password file..."
"$MQDB_BIN" passwd "$CANONICAL_ID" -b "$MQTT_PASS" -f "$TEST_DIR/passwd.txt"
"$MQDB_BIN" passwd "$OBSERVER_USER" -b "$OBSERVER_PASS" -f "$TEST_DIR/passwd.txt"

echo "  Generating JWT key..."
openssl rand -base64 32 > "$TEST_DIR/jwt.key"

echo "  Creating dummy OAuth secret..."
echo "dummy-oauth-secret" > "$TEST_DIR/oauth-secret.txt"

COMMON_AUTH_ARGS=(
    --passwd "$TEST_DIR/passwd.txt"
    --jwt-algorithm hs256 --jwt-key "$TEST_DIR/jwt.key"
    --jwt-issuer mqdb --jwt-audience vault-test
    --oauth-client-secret "$TEST_DIR/oauth-secret.txt"
    --ownership notes=userId
    --admin-users "$OBSERVER_USER"
    --no-rate-limit
    --cors-origin http://localhost:8080
    --license "$MQDB_LICENSE_FILE"
)

COMMON_QUIC_ARGS=(
    --quic-cert "$CERT_DIR/server.pem"
    --quic-key "$CERT_DIR/server.key"
    --quic-ca "$CERT_DIR/ca.pem"
)

echo "  Starting node 1 (port $PORT1, http $HTTP1)..."
RUST_LOG=mqdb=debug "$MQDB_BIN" cluster start \
    --node-id 1 --bind "127.0.0.1:$PORT1" \
    --db "$TEST_DIR/db1" \
    --http-bind "127.0.0.1:$HTTP1" \
    "${COMMON_AUTH_ARGS[@]}" \
    "${COMMON_QUIC_ARGS[@]}" \
    > "$TEST_DIR/node1.log" 2>&1 &
NODE_PIDS+=($!)

sleep 2

echo "  Starting node 2 (port $PORT2, http $HTTP2)..."
RUST_LOG=mqdb=debug "$MQDB_BIN" cluster start \
    --node-id 2 --bind "127.0.0.1:$PORT2" \
    --db "$TEST_DIR/db2" \
    --http-bind "127.0.0.1:$HTTP2" \
    --peers "1@127.0.0.1:$PORT1" \
    "${COMMON_AUTH_ARGS[@]}" \
    "${COMMON_QUIC_ARGS[@]}" \
    > "$TEST_DIR/node2.log" 2>&1 &
NODE_PIDS+=($!)

sleep 2

echo "  Starting node 3 (port $PORT3, no http)..."
RUST_LOG=mqdb=debug "$MQDB_BIN" cluster start \
    --node-id 3 --bind "127.0.0.1:$PORT3" \
    --db "$TEST_DIR/db3" \
    --peers "1@127.0.0.1:$PORT1" \
    "${COMMON_AUTH_ARGS[@]}" \
    "${COMMON_QUIC_ARGS[@]}" \
    > "$TEST_DIR/node3.log" 2>&1 &
NODE_PIDS+=($!)

echo "  Waiting for cluster readiness..."
for i in $(seq 1 30); do
    if curl -sf "http://127.0.0.1:$HTTP1/health" > /dev/null 2>&1; then
        echo "  Node 1 HTTP ready after ${i}s"
        break
    fi
    if [[ $i -eq 30 ]]; then
        echo "  FATAL: Node 1 HTTP failed to start. Logs:"
        tail -50 "$TEST_DIR/node1.log"
        exit 1
    fi
    sleep 1
done

echo "  Waiting for partition distribution..."
for i in $(seq 1 45); do
    STATUS=$("$MQDB_BIN" cluster status --broker "127.0.0.1:$PORT1" --user "$OBSERVER_USER" --pass "$OBSERVER_PASS" --timeout 5 2>/dev/null || echo "")
    NODE_COUNT=$(echo "$STATUS" | grep -c "primary" 2>/dev/null || echo "0")
    if [[ "$NODE_COUNT" -ge 3 ]]; then
        echo "  Partitions distributed across $NODE_COUNT nodes after ${i}s"
        break
    fi
    if [[ $i -eq 45 ]]; then
        echo "  WARNING: Only $NODE_COUNT nodes have partitions (expected 3)"
    fi
    sleep 1
done

sleep 5

echo "  Verifying cross-node CRUD works..."
for i in $(seq 1 20); do
    PROBE=$("$MQDB_BIN" create notes --broker "127.0.0.1:$PORT1" --user "$CANONICAL_ID" --pass "$MQTT_PASS" --timeout 5 \
        -d "{\"title\":\"probe\",\"userId\":\"$CANONICAL_ID\"}" 2>/dev/null)
    PROBE_ID=$(json_field "$PROBE" "id")
    if [[ -n "$PROBE_ID" && "$PROBE_ID" != "__PARSE_ERROR__" ]]; then
        "$MQDB_BIN" delete notes "$PROBE_ID" --broker "127.0.0.1:$PORT1" --user "$CANONICAL_ID" --pass "$MQTT_PASS" --timeout 5 > /dev/null 2>&1
        echo "  Cross-node CRUD verified after ${i}s"
        break
    fi
    if [[ $i -eq 20 ]]; then
        echo "  WARNING: Cross-node CRUD not verified"
    fi
    sleep 1
done

COOKIE_JAR1="$TEST_DIR/cookies1.txt"
COOKIE_JAR2="$TEST_DIR/cookies2.txt"

echo ""
echo "--- Test 1: dev-login on node 1 ---"
RESP=$(curl -s -c "$COOKIE_JAR1" -X POST "http://127.0.0.1:$HTTP1/auth/dev-login" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d "{\"email\":\"vaulttest@example.com\",\"name\":\"Vault Tester\",\"canonical_id\":\"$CANONICAL_ID\"}")
echo "  Response: $RESP"
assert_eq "canonical_id matches" "$(json_field "$RESP" "canonical_id")" "$CANONICAL_ID"

sleep 3

echo ""
echo "--- Test 2: vault enable on node 1 ---"
RESP=$(curl -s -b "$COOKIE_JAR1" -X POST "http://127.0.0.1:$HTTP1/vault/enable" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d "{\"passphrase\":\"$PASSPHRASE\"}")
echo "  Response: $RESP"
assert_eq "vault enabled" "$(json_field "$RESP" "status")" "enabled"

echo ""
echo "--- Test 3: create note via node 1 MQTT (vault encrypts) ---"
RESP=$("$MQDB_BIN" create notes --broker "127.0.0.1:$PORT1" --user "$CANONICAL_ID" --pass "$MQTT_PASS" \
    -d "{\"title\":\"Secret Note\",\"body\":\"Confidential body\",\"userId\":\"$CANONICAL_ID\"}" 2>/dev/null)
echo "  Response: $RESP"
assert_eq "create status ok" "$(json_field "$RESP" "status")" "ok"
assert_eq "create returns plaintext title" "$(json_field "$RESP" "data.title")" "Secret Note"

RECORD_ID=$(json_field "$RESP" "id")
if [[ -z "$RECORD_ID" || "$RECORD_ID" == "__PARSE_ERROR__" ]]; then
    RECORD_ID=$(json_field "$RESP" "data.id")
fi
if [[ -z "$RECORD_ID" || "$RECORD_ID" == "__PARSE_ERROR__" ]]; then
    echo "  FATAL: No record ID returned"
    tail -50 "$TEST_DIR/node1.log"
    exit 1
fi
echo "  Record ID: $RECORD_ID"

echo ""
echo "--- Test 4: observer reads via node 1 (no vault key - sees ciphertext) ---"
RESP=$("$MQDB_BIN" read notes "$RECORD_ID" --broker "127.0.0.1:$PORT1" --user "$OBSERVER_USER" --pass "$OBSERVER_PASS" --timeout 10 2>/dev/null)
echo "  Response: $RESP"
assert_eq "observer read ok" "$(json_field "$RESP" "status")" "ok"
OBSERVER_TITLE=$(json_field "$RESP" "data.title")
assert_neq "observer sees ciphertext title" "$OBSERVER_TITLE" "Secret Note"
assert_min_length "title is base64 ciphertext" "$OBSERVER_TITLE" 20

echo ""
echo "--- Test 5: read note via node 1 MQTT (vault decrypts) ---"
RESP=$("$MQDB_BIN" read notes "$RECORD_ID" --broker "127.0.0.1:$PORT1" --user "$CANONICAL_ID" --pass "$MQTT_PASS" --timeout 10 2>/dev/null)
echo "  Response: $RESP"
assert_eq "read status ok" "$(json_field "$RESP" "status")" "ok"
assert_eq "read returns plaintext title" "$(json_field "$RESP" "data.title")" "Secret Note"
assert_eq "read returns plaintext body" "$(json_field "$RESP" "data.body")" "Confidential body"

echo ""
echo "--- Test 6: create more notes to spread across partitions ---"
for i in $(seq 1 5); do
    "$MQDB_BIN" create notes --broker "127.0.0.1:$PORT1" --user "$CANONICAL_ID" --pass "$MQTT_PASS" --timeout 10 \
        -d "{\"title\":\"Note $i\",\"body\":\"Body $i\",\"userId\":\"$CANONICAL_ID\"}" > /dev/null 2>&1
done
sleep 1
echo "  Created 5 additional notes"

echo ""
echo "--- Test 7: list via node 1 MQTT (scatter-gather, vault decrypts all) ---"
RESP=$("$MQDB_BIN" list notes --broker "127.0.0.1:$PORT1" --user "$CANONICAL_ID" --pass "$MQTT_PASS" --timeout 10 2>/dev/null)
echo "  Response (truncated): $(echo "$RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(json.dumps({'status':d.get('status'),'count':len(d.get('data',[])),'titles':[i.get('data',{}).get('title','?') for i in d.get('data',[])]}))" 2>/dev/null)"
assert_eq "list status ok" "$(json_field "$RESP" "status")" "ok"
LIST_COUNT=$(python3 -c "
import sys, json
try:
    d = json.loads(sys.argv[1])
    print(len(d.get('data', [])))
except:
    print('0')
" "$RESP" 2>/dev/null)
assert_eq "list returns 6 items" "$LIST_COUNT" "6"
CIPHER_COUNT=$(python3 -c "
import sys, json
try:
    d = json.loads(sys.argv[1])
    count = sum(1 for item in d.get('data', [])
                if len(item.get('data', {}).get('title', '')) >= 40)
    print(count)
except:
    print('-1')
" "$RESP" 2>/dev/null)
assert_eq "no list items have ciphertext titles" "$CIPHER_COUNT" "0"

echo ""
echo "--- Test 8: update note via node 1 MQTT (vault encrypts delta) ---"
RESP=$("$MQDB_BIN" update notes "$RECORD_ID" --broker "127.0.0.1:$PORT1" --user "$CANONICAL_ID" --pass "$MQTT_PASS" \
    -d "{\"body\":\"Updated body\"}" 2>/dev/null)
echo "  Response: $RESP"
assert_eq "update status ok" "$(json_field "$RESP" "status")" "ok"
assert_eq "update returns plaintext body" "$(json_field "$RESP" "data.body")" "Updated body"
assert_eq "title preserved after update" "$(json_field "$RESP" "data.title")" "Secret Note"

echo ""
echo "--- Test 9: observer reads updated note (sees ciphertext) ---"
RESP=$("$MQDB_BIN" read notes "$RECORD_ID" --broker "127.0.0.1:$PORT1" --user "$OBSERVER_USER" --pass "$OBSERVER_PASS" --timeout 10 2>/dev/null)
echo "  Response: $RESP"
OBSERVER_BODY=$(json_field "$RESP" "data.body")
assert_neq "observer sees encrypted body" "$OBSERVER_BODY" "Updated body"
assert_min_length "body is base64 ciphertext" "$OBSERVER_BODY" 20

echo ""
echo "--- Test 10: lock vault on node 1 ---"
RESP=$(curl -s -b "$COOKIE_JAR1" -X POST "http://127.0.0.1:$HTTP1/vault/lock" \
    -H "Origin: http://localhost:8080")
echo "  Response: $RESP"
assert_eq "vault locked" "$(json_field "$RESP" "status")" "locked"

echo ""
echo "--- Test 11: read via node 1 with locked vault (ciphertext) ---"
RESP=$("$MQDB_BIN" read notes "$RECORD_ID" --broker "127.0.0.1:$PORT1" --user "$CANONICAL_ID" --pass "$MQTT_PASS" --timeout 10 2>/dev/null)
echo "  Response: $RESP"
LOCKED_TITLE=$(json_field "$RESP" "data.title")
assert_neq "locked read returns ciphertext title" "$LOCKED_TITLE" "Secret Note"
assert_min_length "locked title is base64" "$LOCKED_TITLE" 20

echo ""
echo "--- Test 12: unlock vault on node 1 ---"
RESP=$(curl -s -b "$COOKIE_JAR1" -X POST "http://127.0.0.1:$HTTP1/vault/unlock" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d "{\"passphrase\":\"$PASSPHRASE\"}")
echo "  Response: $RESP"
assert_eq "vault unlocked" "$(json_field "$RESP" "status")" "unlocked"

echo ""
echo "--- Test 13: read via node 1 after unlock (plaintext) ---"
RESP=$("$MQDB_BIN" read notes "$RECORD_ID" --broker "127.0.0.1:$PORT1" --user "$CANONICAL_ID" --pass "$MQTT_PASS" --timeout 10 2>/dev/null)
echo "  Response: $RESP"
assert_eq "unlocked read ok" "$(json_field "$RESP" "status")" "ok"
assert_eq "unlocked read plaintext title" "$(json_field "$RESP" "data.title")" "Secret Note"

echo ""
echo "--- Test 14: dev-login on node 2 ---"
for i in $(seq 1 15); do
    if curl -sf "http://127.0.0.1:$HTTP2/health" > /dev/null 2>&1; then
        echo "  Node 2 HTTP ready"
        break
    fi
    if [[ $i -eq 15 ]]; then
        echo "  WARNING: Node 2 HTTP may not be ready"
    fi
    sleep 1
done
RESP=$(curl -s -c "$COOKIE_JAR2" -X POST "http://127.0.0.1:$HTTP2/auth/dev-login" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d "{\"email\":\"vaulttest@example.com\",\"name\":\"Vault Tester\",\"canonical_id\":\"$CANONICAL_ID\"}")
echo "  Response: $RESP"
assert_eq "node 2 login ok" "$(json_field "$RESP" "canonical_id")" "$CANONICAL_ID"

sleep 3

echo ""
echo "--- Test 15: unlock vault on node 2 ---"
for attempt in $(seq 1 5); do
    RESP=$(curl -s -b "$COOKIE_JAR2" -X POST "http://127.0.0.1:$HTTP2/vault/unlock" \
        -H "Content-Type: application/json" \
        -H "Origin: http://localhost:8080" \
        -d "{\"passphrase\":\"$PASSPHRASE\"}")
    if [[ "$(json_field "$RESP" "status")" == "unlocked" ]]; then
        break
    fi
    echo "  Retry $attempt: $RESP"
    sleep 2
done
echo "  Response: $RESP"
assert_eq "node 2 vault unlocked" "$(json_field "$RESP" "status")" "unlocked"

echo ""
echo "--- Test 16: read via node 2 MQTT (cross-node vault decrypt) ---"
for attempt in $(seq 1 3); do
    RESP=$("$MQDB_BIN" read notes "$RECORD_ID" --broker "127.0.0.1:$PORT2" --user "$CANONICAL_ID" --pass "$MQTT_PASS" --timeout 10 2>/dev/null)
    if [[ "$(json_field "$RESP" "status")" == "ok" ]]; then
        break
    fi
    echo "  Retry $attempt: $(json_field "$RESP" "status")"
    sleep 2
done
echo "  Response: $RESP"
assert_eq "node 2 read ok" "$(json_field "$RESP" "status")" "ok"
assert_eq "node 2 read plaintext title" "$(json_field "$RESP" "data.title")" "Secret Note"

echo ""
echo "--- Test 17: create via node 2 MQTT (cross-node vault encrypt) ---"
RESP=$("$MQDB_BIN" create notes --broker "127.0.0.1:$PORT2" --user "$CANONICAL_ID" --pass "$MQTT_PASS" \
    -d "{\"title\":\"Node 2 Note\",\"body\":\"Created on node 2\",\"userId\":\"$CANONICAL_ID\"}" 2>/dev/null)
echo "  Response: $RESP"
assert_eq "node 2 create ok" "$(json_field "$RESP" "status")" "ok"
assert_eq "node 2 create plaintext title" "$(json_field "$RESP" "data.title")" "Node 2 Note"

NODE2_ID=$(json_field "$RESP" "id")
if [[ -z "$NODE2_ID" || "$NODE2_ID" == "__PARSE_ERROR__" ]]; then
    NODE2_ID=$(json_field "$RESP" "data.id")
fi
echo "  Node 2 record ID: $NODE2_ID"

sleep 2

echo ""
echo "--- Test 18: observer reads node-2-created note (ciphertext) ---"
RESP=$("$MQDB_BIN" read notes "$NODE2_ID" --broker "127.0.0.1:$PORT2" --user "$OBSERVER_USER" --pass "$OBSERVER_PASS" --timeout 10 2>/dev/null)
echo "  Response: $RESP"
OBSERVER_N2_TITLE=$(json_field "$RESP" "data.title")
assert_neq "node 2 note stored encrypted" "$OBSERVER_N2_TITLE" "Node 2 Note"
assert_min_length "node 2 ciphertext" "$OBSERVER_N2_TITLE" 20

echo ""
echo "--- Test 19: list via node 2 MQTT (scatter-gather from all nodes) ---"
RESP=$("$MQDB_BIN" list notes --broker "127.0.0.1:$PORT2" --user "$CANONICAL_ID" --pass "$MQTT_PASS" --timeout 10 2>/dev/null)
echo "  Response (truncated): $(echo "$RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(json.dumps({'status':d.get('status'),'count':len(d.get('data',[]))}))" 2>/dev/null)"
assert_eq "node 2 list ok" "$(json_field "$RESP" "status")" "ok"
LIST2_COUNT=$(python3 -c "
import sys, json
try:
    d = json.loads(sys.argv[1])
    print(len(d.get('data', [])))
except:
    print('0')
" "$RESP" 2>/dev/null)
assert_eq "node 2 list returns 7 items" "$LIST2_COUNT" "7"

echo ""
echo "--- Test 20: vault status endpoint ---"
RESP=$(curl -s -b "$COOKIE_JAR1" -X GET "http://127.0.0.1:$HTTP1/vault/status" \
    -H "Origin: http://localhost:8080")
echo "  Response: $RESP"
assert_eq "vault_enabled is true" "$(json_field "$RESP" "vault_enabled")" "True"
assert_eq "unlocked is true" "$(json_field "$RESP" "unlocked")" "True"

echo ""
echo "--- Test 21: wrong passphrase rejected ---"
RESP=$(curl -s -o /dev/null -w "%{http_code}" -b "$COOKIE_JAR1" -X POST "http://127.0.0.1:$HTTP1/vault/unlock" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d '{"passphrase":"wrong-passphrase"}')
echo "  HTTP status: $RESP"
TOTAL=$((TOTAL + 1))
if [[ "$RESP" == "401" ]]; then
    echo "  PASS: wrong passphrase returns 401"
    PASS=$((PASS + 1))
else
    echo "  FAIL: wrong passphrase returns 401"
    echo "    expected: 401"
    echo "    actual:   $RESP"
    FAIL=$((FAIL + 1))
fi

echo ""
echo "--- Test 22: delete with vault enabled ---"
RESP=$("$MQDB_BIN" delete notes "$NODE2_ID" --broker "127.0.0.1:$PORT1" --user "$CANONICAL_ID" --pass "$MQTT_PASS" --timeout 10 2>/dev/null)
echo "  Response: $RESP"
assert_eq "delete ok" "$(json_field "$RESP" "status")" "ok"

sleep 1

RESP=$("$MQDB_BIN" read notes "$NODE2_ID" --broker "127.0.0.1:$PORT1" --user "$CANONICAL_ID" --pass "$MQTT_PASS" --timeout 10 2>/dev/null)
echo "  Read deleted: $RESP"
assert_eq "deleted note returns 404" "$(json_field "$RESP" "code")" "404"

echo ""
echo "--- Test 23: vault change passphrase ---"
NEW_PASSPHRASE="new-cluster-vault-pass"
RESP=$(curl -s -b "$COOKIE_JAR1" -X POST "http://127.0.0.1:$HTTP1/vault/change" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d "{\"old_passphrase\":\"$PASSPHRASE\",\"new_passphrase\":\"$NEW_PASSPHRASE\"}")
echo "  Response: $RESP"
assert_eq "vault passphrase changed" "$(json_field "$RESP" "status")" "changed"

echo ""
echo "--- Test 24: old passphrase rejected after change ---"
RESP=$(curl -s -b "$COOKIE_JAR1" -X POST "http://127.0.0.1:$HTTP1/vault/lock" \
    -H "Origin: http://localhost:8080")
echo "  Locked: $RESP"

RESP=$(curl -s -o /dev/null -w "%{http_code}" -b "$COOKIE_JAR1" -X POST "http://127.0.0.1:$HTTP1/vault/unlock" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d "{\"passphrase\":\"$PASSPHRASE\"}")
echo "  HTTP status with old passphrase: $RESP"
TOTAL=$((TOTAL + 1))
if [[ "$RESP" == "401" ]]; then
    echo "  PASS: old passphrase rejected"
    PASS=$((PASS + 1))
else
    echo "  FAIL: old passphrase rejected"
    echo "    expected: 401"
    echo "    actual:   $RESP"
    FAIL=$((FAIL + 1))
fi

echo ""
echo "--- Test 25: new passphrase works ---"
RESP=$(curl -s -b "$COOKIE_JAR1" -X POST "http://127.0.0.1:$HTTP1/vault/unlock" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d "{\"passphrase\":\"$NEW_PASSPHRASE\"}")
echo "  Response: $RESP"
assert_eq "new passphrase unlocks" "$(json_field "$RESP" "status")" "unlocked"

echo ""
echo "--- Test 26: read after passphrase change ---"
RESP=$("$MQDB_BIN" read notes "$RECORD_ID" --broker "127.0.0.1:$PORT1" --user "$CANONICAL_ID" --pass "$MQTT_PASS" --timeout 10 2>/dev/null)
echo "  Response: $RESP"
assert_eq "read ok after change" "$(json_field "$RESP" "status")" "ok"
assert_eq "plaintext title after change" "$(json_field "$RESP" "data.title")" "Secret Note"

echo ""
echo "--- Test 27: vault disable ---"
RESP=$(curl -s -b "$COOKIE_JAR1" -X POST "http://127.0.0.1:$HTTP1/vault/disable" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d "{\"passphrase\":\"$NEW_PASSPHRASE\"}")
echo "  Response: $RESP"
assert_eq "vault disabled" "$(json_field "$RESP" "status")" "disabled"
DECRYPTED_COUNT=$(json_field "$RESP" "records_decrypted")
TOTAL=$((TOTAL + 1))
if [[ "$DECRYPTED_COUNT" -gt 0 ]] 2>/dev/null; then
    echo "  PASS: records_decrypted > 0 ($DECRYPTED_COUNT)"
    PASS=$((PASS + 1))
else
    echo "  FAIL: records_decrypted > 0"
    echo "    actual: $DECRYPTED_COUNT"
    FAIL=$((FAIL + 1))
fi

echo ""
echo "--- Test 28: read after disable (plaintext in storage) ---"
RESP=$("$MQDB_BIN" read notes "$RECORD_ID" --broker "127.0.0.1:$PORT1" --user "$CANONICAL_ID" --pass "$MQTT_PASS" --timeout 10 2>/dev/null)
echo "  Owner read: $RESP"
assert_eq "owner read ok after disable" "$(json_field "$RESP" "status")" "ok"
assert_eq "owner plaintext title after disable" "$(json_field "$RESP" "data.title")" "Secret Note"

sleep 1

RESP=$("$MQDB_BIN" read notes "$RECORD_ID" --broker "127.0.0.1:$PORT1" --user "$OBSERVER_USER" --pass "$OBSERVER_PASS" --timeout 10 2>/dev/null)
echo "  Observer read: $RESP"
assert_eq "observer read ok after disable" "$(json_field "$RESP" "status")" "ok"
assert_eq "observer plaintext title after disable" "$(json_field "$RESP" "data.title")" "Secret Note"

echo ""
echo "--- Test 29: vault status after disable ---"
RESP=$(curl -s -b "$COOKIE_JAR1" -X GET "http://127.0.0.1:$HTTP1/vault/status" \
    -H "Origin: http://localhost:8080")
echo "  Response: $RESP"
assert_eq "vault_enabled is false" "$(json_field "$RESP" "vault_enabled")" "False"

echo ""
echo "=========================================="
echo "=== Phase 2: Node 4 Rebalance Tests ==="
echo "=========================================="
echo ""

echo "--- Test 30: start node 4 and wait for rebalancing ---"
echo "  Starting node 4 (port $PORT4, no http)..."
RUST_LOG=mqdb=debug "$MQDB_BIN" cluster start \
    --node-id 4 --bind "127.0.0.1:$PORT4" \
    --db "$TEST_DIR/db4" \
    --peers "1@127.0.0.1:$PORT1" \
    "${COMMON_AUTH_ARGS[@]}" \
    "${COMMON_QUIC_ARGS[@]}" \
    > "$TEST_DIR/node4.log" 2>&1 &
NODE_PIDS+=($!)

echo "  Waiting for node 4 to get primaries (2-cycle rebalance)..."
NODE4_PRIMARIES=0
for i in $(seq 1 90); do
    STATUS=$("$MQDB_BIN" cluster status --broker "127.0.0.1:$PORT1" --user "$OBSERVER_USER" --pass "$OBSERVER_PASS" --timeout 5 2>/dev/null || echo "")
    NODE4_LINE=$(echo "$STATUS" | grep "Node 4:" 2>/dev/null || echo "")
    if [[ -n "$NODE4_LINE" ]]; then
        NODE4_PRIMARIES=$(echo "$NODE4_LINE" | grep -o '[0-9]* primary' | grep -o '[0-9]*')
    else
        NODE4_PRIMARIES=0
    fi
    if [[ "$NODE4_PRIMARIES" -ge 30 ]]; then
        echo "  Node 4 has $NODE4_PRIMARIES primaries after ${i}s"
        break
    fi
    if [[ $((i % 15)) -eq 0 ]]; then
        echo "  ... ${i}s elapsed, node 4 has $NODE4_PRIMARIES primaries"
    fi
    if [[ $i -eq 90 ]]; then
        echo "  WARNING: Node 4 only has $NODE4_PRIMARIES primaries after 90s"
        echo "  DEBUG: full status output:"
        echo "$STATUS" | head -20
    fi
    sleep 1
done

TOTAL=$((TOTAL + 1))
if [[ "$NODE4_PRIMARIES" -ge 30 ]]; then
    echo "  PASS: node 4 has primaries ($NODE4_PRIMARIES >= 30)"
    PASS=$((PASS + 1))
else
    echo "  FAIL: node 4 has primaries"
    echo "    expected: >= 30"
    echo "    actual:   $NODE4_PRIMARIES"
    FAIL=$((FAIL + 1))
fi

sleep 3

echo ""
echo "--- Test 31: read existing record via node 4 ---"
for attempt in $(seq 1 5); do
    RESP=$("$MQDB_BIN" read notes "$RECORD_ID" --broker "127.0.0.1:$PORT4" --user "$CANONICAL_ID" --pass "$MQTT_PASS" --timeout 10 2>/dev/null)
    if [[ "$(json_field "$RESP" "status")" == "ok" ]]; then
        break
    fi
    echo "  Retry $attempt: $(json_field "$RESP" "status")"
    sleep 2
done
echo "  Response: $RESP"
assert_eq "node 4 read ok" "$(json_field "$RESP" "status")" "ok"
assert_eq "node 4 read correct title" "$(json_field "$RESP" "data.title")" "Secret Note"

echo ""
echo "--- Test 32: list via node 4 (scatter-gather across 4 nodes) ---"
RESP=$("$MQDB_BIN" list notes --broker "127.0.0.1:$PORT4" --user "$CANONICAL_ID" --pass "$MQTT_PASS" --timeout 10 2>/dev/null)
echo "  Response (truncated): $(echo "$RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(json.dumps({'status':d.get('status'),'count':len(d.get('data',[]))}))" 2>/dev/null)"
assert_eq "node 4 list ok" "$(json_field "$RESP" "status")" "ok"
LIST4_COUNT=$(python3 -c "
import sys, json
try:
    d = json.loads(sys.argv[1])
    print(len(d.get('data', [])))
except:
    print('0')
" "$RESP" 2>/dev/null)
assert_eq "node 4 list returns 6 items" "$LIST4_COUNT" "6"

echo ""
echo "--- Test 33: create via node 4 ---"
RESP=$("$MQDB_BIN" create notes --broker "127.0.0.1:$PORT4" --user "$CANONICAL_ID" --pass "$MQTT_PASS" \
    -d "{\"title\":\"Node 4 Note\",\"body\":\"Created on node 4\",\"userId\":\"$CANONICAL_ID\"}" 2>/dev/null)
echo "  Response: $RESP"
assert_eq "node 4 create ok" "$(json_field "$RESP" "status")" "ok"
assert_eq "node 4 create title" "$(json_field "$RESP" "data.title")" "Node 4 Note"

NODE4_ID=$(json_field "$RESP" "id")
if [[ -z "$NODE4_ID" || "$NODE4_ID" == "__PARSE_ERROR__" ]]; then
    NODE4_ID=$(json_field "$RESP" "data.id")
fi
echo "  Node 4 record ID: $NODE4_ID"

sleep 1

echo ""
echo "--- Test 34: read node-4-created record from node 1 ---"
RESP=$("$MQDB_BIN" read notes "$NODE4_ID" --broker "127.0.0.1:$PORT1" --user "$CANONICAL_ID" --pass "$MQTT_PASS" --timeout 10 2>/dev/null)
echo "  Response: $RESP"
assert_eq "node 1 reads node 4 record" "$(json_field "$RESP" "status")" "ok"
assert_eq "node 1 reads node 4 title" "$(json_field "$RESP" "data.title")" "Node 4 Note"

echo ""
echo "--- Test 35: delete via node 4 ---"
RESP=$("$MQDB_BIN" delete notes "$NODE4_ID" --broker "127.0.0.1:$PORT4" --user "$CANONICAL_ID" --pass "$MQTT_PASS" --timeout 10 2>/dev/null)
echo "  Response: $RESP"
assert_eq "node 4 delete ok" "$(json_field "$RESP" "status")" "ok"

sleep 1

RESP=$("$MQDB_BIN" read notes "$NODE4_ID" --broker "127.0.0.1:$PORT1" --user "$CANONICAL_ID" --pass "$MQTT_PASS" --timeout 10 2>/dev/null)
echo "  Verify deleted: $RESP"
assert_eq "deleted via node 4 returns 404" "$(json_field "$RESP" "code")" "404"

echo ""
echo "--- Test 36: re-enable vault with rebalanced cluster ---"
RESP=$(curl -s -b "$COOKIE_JAR1" -X POST "http://127.0.0.1:$HTTP1/vault/enable" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d "{\"passphrase\":\"$NEW_PASSPHRASE\"}")
echo "  Response: $RESP"
assert_eq "vault re-enabled" "$(json_field "$RESP" "status")" "enabled"

sleep 2

echo ""
echo "--- Test 37: create encrypted note in rebalanced cluster ---"
RESP=$("$MQDB_BIN" create notes --broker "127.0.0.1:$PORT1" --user "$CANONICAL_ID" --pass "$MQTT_PASS" \
    -d "{\"title\":\"Rebalanced Secret\",\"body\":\"Encrypted after rebalance\",\"userId\":\"$CANONICAL_ID\"}" 2>/dev/null)
echo "  Response: $RESP"
assert_eq "encrypted create ok" "$(json_field "$RESP" "status")" "ok"
assert_eq "encrypted create plaintext title" "$(json_field "$RESP" "data.title")" "Rebalanced Secret"

REBAL_ID=$(json_field "$RESP" "id")
if [[ -z "$REBAL_ID" || "$REBAL_ID" == "__PARSE_ERROR__" ]]; then
    REBAL_ID=$(json_field "$RESP" "data.id")
fi

echo ""
echo "--- Test 38: list in rebalanced cluster (scatter-gather across 4 encrypted) ---"
RESP=$("$MQDB_BIN" list notes --broker "127.0.0.1:$PORT1" --user "$CANONICAL_ID" --pass "$MQTT_PASS" --timeout 10 2>/dev/null)
echo "  Response (truncated): $(echo "$RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(json.dumps({'status':d.get('status'),'count':len(d.get('data',[]))}))" 2>/dev/null)"
assert_eq "rebalanced list ok" "$(json_field "$RESP" "status")" "ok"
REBAL_LIST_COUNT=$(python3 -c "
import sys, json
try:
    d = json.loads(sys.argv[1])
    print(len(d.get('data', [])))
except:
    print('0')
" "$RESP" 2>/dev/null)
assert_eq "rebalanced list returns 7 items" "$REBAL_LIST_COUNT" "7"
CIPHER_IN_LIST=$(python3 -c "
import sys, json
try:
    d = json.loads(sys.argv[1])
    count = sum(1 for item in d.get('data', [])
                if len(item.get('data', {}).get('title', '')) >= 40)
    print(count)
except:
    print('-1')
" "$RESP" 2>/dev/null)
assert_eq "no ciphertext in rebalanced list" "$CIPHER_IN_LIST" "0"

echo ""
echo "--- Test 39: observer sees ciphertext in rebalanced cluster ---"
RESP=$("$MQDB_BIN" read notes "$REBAL_ID" --broker "127.0.0.1:$PORT1" --user "$OBSERVER_USER" --pass "$OBSERVER_PASS" --timeout 10 2>/dev/null)
echo "  Response: $RESP"
REBAL_OBSERVER_TITLE=$(json_field "$RESP" "data.title")
assert_neq "observer sees ciphertext in rebalanced cluster" "$REBAL_OBSERVER_TITLE" "Rebalanced Secret"
assert_min_length "rebalanced ciphertext" "$REBAL_OBSERVER_TITLE" 20

SAVE_LOGS=1

echo ""
echo "=== Results: $PASS/$TOTAL passed, $FAIL failed ==="

if [[ $FAIL -gt 0 ]]; then
    echo ""
    echo "Node 1 logs (last 30 lines):"
    tail -30 "$TEST_DIR/node1.log"
    echo ""
    echo "Node 2 logs (last 30 lines):"
    tail -30 "$TEST_DIR/node2.log"
    echo ""
    echo "Node 4 logs (last 30 lines):"
    tail -30 "$TEST_DIR/node4.log"
    exit 1
fi

echo "All tests passed!"
