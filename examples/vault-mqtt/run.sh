#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
MQDB_BIN="$REPO_ROOT/target/release/mqdb"
TEST_DIR="/tmp/vault-e2e-test"
AGENT_PID=""
PASS=0
FAIL=0
TOTAL=24

cleanup() {
    if [[ -n "$AGENT_PID" ]]; then
        kill "$AGENT_PID" 2>/dev/null || true
        wait "$AGENT_PID" 2>/dev/null || true
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

echo "=== Vault MQTT E2E Test ==="
echo ""

echo "Step 0: Setup"
rm -rf "$TEST_DIR"
mkdir -p "$TEST_DIR"

CANONICAL_ID="vault-test-user"
MQTT_PASS="testpass"
OBSERVER_USER="observer"
OBSERVER_PASS="observerpass"
PASSPHRASE="test-vault-pass"

echo "  Building MQDB with dev-insecure..."
cd "$REPO_ROOT"
cargo build --release --features dev-insecure 2>&1 | tail -3

echo "  Creating password file..."
"$MQDB_BIN" passwd "$CANONICAL_ID" -b "$MQTT_PASS" -f "$TEST_DIR/passwd.txt"
"$MQDB_BIN" passwd "$OBSERVER_USER" -b "$OBSERVER_PASS" -f "$TEST_DIR/passwd.txt"

echo "  Generating JWT key..."
openssl rand -base64 32 > "$TEST_DIR/jwt.key"

echo "  Creating dummy OAuth secret..."
echo "dummy-oauth-secret" > "$TEST_DIR/oauth-secret.txt"

echo "  Starting MQDB agent..."
RUST_LOG=mqdb=debug "$MQDB_BIN" agent start \
    --db "$TEST_DIR/db" \
    --bind 127.0.0.1:18830 \
    --ws-bind 127.0.0.1:18083 \
    --http-bind 127.0.0.1:13000 \
    --passwd "$TEST_DIR/passwd.txt" \
    --jwt-algorithm hs256 --jwt-key "$TEST_DIR/jwt.key" \
    --jwt-issuer mqdb --jwt-audience vault-test \
    --oauth-client-secret "$TEST_DIR/oauth-secret.txt" \
    --ownership notes=userId \
    --admin-users "$OBSERVER_USER" \
    --no-rate-limit \
    --cors-origin http://localhost:8080 \
    > "$TEST_DIR/agent.log" 2>&1 &
AGENT_PID=$!

echo "  Waiting for agent readiness..."
for i in $(seq 1 30); do
    if curl -sf http://127.0.0.1:13000/health > /dev/null 2>&1; then
        echo "  Agent ready after ${i}s"
        break
    fi
    if [[ $i -eq 30 ]]; then
        echo "  FATAL: Agent failed to start. Logs:"
        cat "$TEST_DIR/agent.log"
        exit 1
    fi
    sleep 1
done

sleep 1

COOKIE_JAR="$TEST_DIR/cookies.txt"

echo ""
echo "--- Test 1: dev-login ---"
RESP=$(curl -s -c "$COOKIE_JAR" -X POST http://127.0.0.1:13000/auth/dev-login \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d "{\"email\":\"test@example.com\",\"name\":\"Test User\",\"canonical_id\":\"$CANONICAL_ID\"}")
echo "  Response: $RESP"
assert_eq "canonical_id matches" "$(json_field "$RESP" "canonical_id")" "$CANONICAL_ID"

echo ""
echo "--- Test 2: vault enable ---"
RESP=$(curl -s -b "$COOKIE_JAR" -X POST http://127.0.0.1:13000/vault/enable \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d "{\"passphrase\":\"$PASSPHRASE\"}")
echo "  Response: $RESP"
assert_eq "vault enabled" "$(json_field "$RESP" "status")" "enabled"

echo ""
echo "--- Test 3: create note (vault unlocked - plaintext response) ---"
RESP=$("$MQDB_BIN" create notes --broker 127.0.0.1:18830 --user "$CANONICAL_ID" --pass "$MQTT_PASS" \
    -d "{\"title\":\"Secret\",\"body\":\"Top secret\",\"userId\":\"$CANONICAL_ID\"}" 2>/dev/null)
echo "  Response: $RESP"
assert_eq "response status ok" "$(json_field "$RESP" "status")" "ok"
assert_eq "create returns plaintext title" "$(json_field "$RESP" "data.title")" "Secret"

RECORD_ID=$(json_field "$RESP" "data.id")
if [[ -z "$RECORD_ID" || "$RECORD_ID" == "__PARSE_ERROR__" ]]; then
    echo "  FATAL: No record ID returned, cannot continue"
    echo "  Agent logs:"
    tail -50 "$TEST_DIR/agent.log"
    exit 1
fi
echo "  Record ID: $RECORD_ID"

echo ""
echo "--- Test 4: observer reads note (no vault key - proves at-rest encryption) ---"
RESP=$("$MQDB_BIN" read notes "$RECORD_ID" --broker 127.0.0.1:18830 --user "$OBSERVER_USER" --pass "$OBSERVER_PASS" 2>/dev/null)
echo "  Response: $RESP"
assert_eq "observer read status ok" "$(json_field "$RESP" "status")" "ok"
OBSERVER_TITLE=$(json_field "$RESP" "data.title")
assert_neq "observer sees ciphertext (not plaintext)" "$OBSERVER_TITLE" "Secret"
assert_min_length "observer title is base64 ciphertext" "$OBSERVER_TITLE" 20

echo ""
echo "--- Test 5: vault lock ---"
RESP=$(curl -s -b "$COOKIE_JAR" -X POST http://127.0.0.1:13000/vault/lock \
    -H "Origin: http://localhost:8080")
echo "  Response: $RESP"
assert_eq "vault locked" "$(json_field "$RESP" "status")" "locked"

echo ""
echo "--- Test 6: read note (vault locked - ciphertext) ---"
RESP=$("$MQDB_BIN" read notes "$RECORD_ID" --broker 127.0.0.1:18830 --user "$CANONICAL_ID" --pass "$MQTT_PASS" 2>/dev/null)
echo "  Response: $RESP"
assert_eq "locked read status ok" "$(json_field "$RESP" "status")" "ok"
LOCKED_TITLE=$(json_field "$RESP" "data.title")
assert_neq "locked title is not plaintext" "$LOCKED_TITLE" "Secret"
assert_min_length "locked title is base64 ciphertext" "$LOCKED_TITLE" 20

echo ""
echo "--- Test 7: vault unlock ---"
RESP=$(curl -s -b "$COOKIE_JAR" -X POST http://127.0.0.1:13000/vault/unlock \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d "{\"passphrase\":\"$PASSPHRASE\"}")
echo "  Response: $RESP"
assert_eq "vault unlocked" "$(json_field "$RESP" "status")" "unlocked"

echo ""
echo "--- Test 8: read note (vault unlocked - plaintext) ---"
RESP=$("$MQDB_BIN" read notes "$RECORD_ID" --broker 127.0.0.1:18830 --user "$CANONICAL_ID" --pass "$MQTT_PASS" 2>/dev/null)
echo "  Response: $RESP"
assert_eq "unlocked read status ok" "$(json_field "$RESP" "status")" "ok"
assert_eq "read returns plaintext title" "$(json_field "$RESP" "data.title")" "Secret"

echo ""
echo "--- Test 9: update note ---"
RESP=$("$MQDB_BIN" update notes "$RECORD_ID" --broker 127.0.0.1:18830 --user "$CANONICAL_ID" --pass "$MQTT_PASS" \
    -d "{\"body\":\"Updated secret\"}" 2>/dev/null)
echo "  Response: $RESP"
assert_eq "update status ok" "$(json_field "$RESP" "status")" "ok"
assert_eq "update returns plaintext body" "$(json_field "$RESP" "data.body")" "Updated secret"

echo ""
echo "--- Test 10: observer still sees ciphertext after update ---"
RESP=$("$MQDB_BIN" read notes "$RECORD_ID" --broker 127.0.0.1:18830 --user "$OBSERVER_USER" --pass "$OBSERVER_PASS" 2>/dev/null)
echo "  Response: $RESP"
assert_eq "observer post-update status ok" "$(json_field "$RESP" "status")" "ok"
OBSERVER_BODY=$(json_field "$RESP" "data.body")
assert_neq "observer sees encrypted body" "$OBSERVER_BODY" "Updated secret"
assert_min_length "observer body is base64 ciphertext" "$OBSERVER_BODY" 20

echo ""
echo "--- Test 11: list notes ---"
RESP=$("$MQDB_BIN" list notes --broker 127.0.0.1:18830 --user "$CANONICAL_ID" --pass "$MQTT_PASS" 2>/dev/null)
echo "  Response: $RESP"
assert_eq "list status ok" "$(json_field "$RESP" "status")" "ok"
assert_eq "list returns plaintext title" "$(json_field "$RESP" "data.0.title")" "Secret"

echo ""
echo "--- Test 12: wrong passphrase rejected ---"
RESP=$(curl -s -b "$COOKIE_JAR" -X POST http://127.0.0.1:13000/vault/unlock \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d "{\"passphrase\":\"wrong-passphrase\"}")
echo "  Response: $RESP"
assert_eq "wrong passphrase returns error" "$(json_field "$RESP" "error")" "incorrect passphrase"

echo ""
echo "--- Test 13: vault enable without session returns 401 ---"
RESP=$(curl -s -X POST http://127.0.0.1:13000/vault/enable \
    -H "Content-Type: application/json" \
    -d "{\"passphrase\":\"anything\"}")
echo "  Response: $RESP"
assert_eq "no-session vault enable rejected" "$(json_field "$RESP" "error")" "no session"

echo ""
echo "--- Test 14: vault unlock without session returns 401 ---"
RESP=$(curl -s -X POST http://127.0.0.1:13000/vault/unlock \
    -H "Content-Type: application/json" \
    -d "{\"passphrase\":\"anything\"}")
echo "  Response: $RESP"
assert_eq "no-session vault unlock rejected" "$(json_field "$RESP" "error")" "no session"

echo ""
echo "=== Results: $PASS/$TOTAL passed, $FAIL failed ==="

if [[ $FAIL -gt 0 ]]; then
    echo ""
    echo "Agent logs (last 50 lines):"
    tail -50 "$TEST_DIR/agent.log"
    exit 1
fi

echo "All tests passed!"
