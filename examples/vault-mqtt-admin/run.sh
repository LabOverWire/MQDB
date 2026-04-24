#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
MQDB_BIN="$REPO_ROOT/target/release/mqdb"
TEST_DIR="/tmp/vault-mqtt-admin-test"

if [[ -z "${MQDB_LICENSE_FILE:-}" ]]; then
    echo "ERROR: vault admin requires a Pro or Enterprise license." >&2
    echo "Set MQDB_LICENSE_FILE to a license key path and retry." >&2
    exit 1
fi
if [[ ! -f "$MQDB_LICENSE_FILE" ]]; then
    echo "ERROR: MQDB_LICENSE_FILE does not exist: $MQDB_LICENSE_FILE" >&2
    exit 1
fi
AGENT_PID=""
PASS=0
FAIL=0
TOTAL=28

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
    elif isinstance(d, bool):
        print('true' if d else 'false')
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

mqtt_vault() {
    local op="$1" payload="$2"
    mosquitto_rr -h 127.0.0.1 -p 18830 \
        -u "$CANONICAL_ID" -P "$MQTT_PASS" \
        -t "\$DB/_vault/$op" -e "vault-test/resp" \
        -m "$payload" -W 10 -N 2>/dev/null
}

echo "=== Vault MQTT Admin E2E Test ==="
echo ""
echo 'Tests the full vault lifecycle using MQTT $DB/_vault/* topics'
echo "(no HTTP vault API calls — only MQTT request-response)"
echo ""

echo "Step 0: Setup"
rm -rf "$TEST_DIR"
mkdir -p "$TEST_DIR"

CANONICAL_ID="vault-mqtt-user"
MQTT_PASS="testpass"
OBSERVER_USER="observer"
OBSERVER_PASS="observerpass"
PASSPHRASE="correct-horse-battery-staple"
NEW_PASSPHRASE="new-secret-passphrase-2026"
PORT=18830
HTTP_PORT=13000

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
    --bind 127.0.0.1:$PORT \
    --http-bind 127.0.0.1:$HTTP_PORT \
    --passwd "$TEST_DIR/passwd.txt" \
    --jwt-algorithm hs256 --jwt-key "$TEST_DIR/jwt.key" \
    --jwt-issuer mqdb --jwt-audience vault-test \
    --oauth-client-secret "$TEST_DIR/oauth-secret.txt" \
    --ownership notes=userId \
    --admin-users "$OBSERVER_USER" \
    --no-rate-limit \
    --vault-min-passphrase-length 8 \
    --license "$MQDB_LICENSE_FILE" \
    > "$TEST_DIR/agent.log" 2>&1 &
AGENT_PID=$!

echo "  Waiting for agent readiness..."
for i in $(seq 1 30); do
    if curl -sf http://127.0.0.1:$HTTP_PORT/health > /dev/null 2>&1; then
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
echo "Step 1: Create identity via dev-login (required for vault — identity must exist)"
RESP=$(curl -s -c "$COOKIE_JAR" -X POST http://127.0.0.1:$HTTP_PORT/auth/dev-login \
    -H "Content-Type: application/json" \
    -d "{\"email\":\"vault@example.com\",\"name\":\"Vault User\",\"canonical_id\":\"$CANONICAL_ID\"}")
assert_eq "identity created" "$(json_field "$RESP" "canonical_id")" "$CANONICAL_ID"

echo ""
echo "--- Test 1: vault status (disabled) ---"
RESP=$(mqtt_vault "status" "{}")
echo "  Response: $RESP"
assert_eq "status ok" "$(json_field "$RESP" "status")" "ok"
assert_eq "vault_enabled is false" "$(json_field "$RESP" "data.vault_enabled")" "false"
assert_eq "unlocked is false" "$(json_field "$RESP" "data.unlocked")" "false"

echo ""
echo "--- Test 2: vault enable with short passphrase rejected ---"
RESP=$(mqtt_vault "enable" '{"passphrase":"ab"}')
echo "  Response: $RESP"
assert_eq "short passphrase rejected" "$(json_field "$RESP" "status")" "error"
assert_eq "error message" "$(json_field "$RESP" "message")" "passphrase must be at least 8 characters"

echo ""
echo "--- Test 3: vault enable ---"
RESP=$(mqtt_vault "enable" "{\"passphrase\":\"$PASSPHRASE\"}")
echo "  Response: $RESP"
assert_eq "enable ok" "$(json_field "$RESP" "status")" "ok"
assert_eq "status enabled" "$(json_field "$RESP" "data.status")" "enabled"

echo ""
echo "--- Test 4: vault status (enabled + unlocked) ---"
RESP=$(mqtt_vault "status" "{}")
echo "  Response: $RESP"
assert_eq "vault_enabled is true" "$(json_field "$RESP" "data.vault_enabled")" "true"
assert_eq "unlocked is true" "$(json_field "$RESP" "data.unlocked")" "true"

echo ""
echo "--- Test 5: create note (vault unlocked — plaintext response) ---"
RESP=$("$MQDB_BIN" create notes --broker 127.0.0.1:$PORT --user "$CANONICAL_ID" --pass "$MQTT_PASS" \
    -d "{\"title\":\"Secret Note\",\"body\":\"Top secret content\",\"userId\":\"$CANONICAL_ID\"}" 2>/dev/null)
echo "  Response: $RESP"
assert_eq "create ok" "$(json_field "$RESP" "status")" "ok"
assert_eq "plaintext title" "$(json_field "$RESP" "data.title")" "Secret Note"

RECORD_ID=$(json_field "$RESP" "data.id")
if [[ -z "$RECORD_ID" || "$RECORD_ID" == "__PARSE_ERROR__" ]]; then
    echo "  FATAL: No record ID returned"
    tail -50 "$TEST_DIR/agent.log"
    exit 1
fi
echo "  Record ID: $RECORD_ID"

echo ""
echo "--- Test 6: vault lock (via MQTT) ---"
RESP=$(mqtt_vault "lock" "{}")
echo "  Response: $RESP"
assert_eq "lock ok" "$(json_field "$RESP" "status")" "ok"
assert_eq "status locked" "$(json_field "$RESP" "data.status")" "locked"

echo ""
echo "--- Test 7: read note (vault locked — ciphertext) ---"
RESP=$("$MQDB_BIN" read notes "$RECORD_ID" --broker 127.0.0.1:$PORT --user "$CANONICAL_ID" --pass "$MQTT_PASS" 2>/dev/null)
echo "  Response: $RESP"
LOCKED_TITLE=$(json_field "$RESP" "data.title")
assert_neq "locked title is ciphertext" "$LOCKED_TITLE" "Secret Note"
assert_min_length "locked title is base64" "$LOCKED_TITLE" 20

echo ""
echo "--- Test 8: vault unlock (via MQTT) ---"
RESP=$(mqtt_vault "unlock" "{\"passphrase\":\"$PASSPHRASE\"}")
echo "  Response: $RESP"
assert_eq "unlock ok" "$(json_field "$RESP" "status")" "ok"
assert_eq "status unlocked" "$(json_field "$RESP" "data.status")" "unlocked"

echo ""
echo "--- Test 9: read note (vault unlocked — plaintext restored) ---"
RESP=$("$MQDB_BIN" read notes "$RECORD_ID" --broker 127.0.0.1:$PORT --user "$CANONICAL_ID" --pass "$MQTT_PASS" 2>/dev/null)
echo "  Response: $RESP"
assert_eq "plaintext title restored" "$(json_field "$RESP" "data.title")" "Secret Note"

echo ""
echo "--- Test 10: vault change passphrase (via MQTT) ---"
RESP=$(mqtt_vault "change" "{\"old_passphrase\":\"$PASSPHRASE\",\"new_passphrase\":\"$NEW_PASSPHRASE\"}")
echo "  Response: $RESP"
assert_eq "change ok" "$(json_field "$RESP" "status")" "ok"
assert_eq "status changed" "$(json_field "$RESP" "data.status")" "changed"

echo ""
echo "--- Test 11: lock + old passphrase rejected ---"
mqtt_vault "lock" "{}" > /dev/null
RESP=$(mqtt_vault "unlock" "{\"passphrase\":\"$PASSPHRASE\"}")
echo "  Response: $RESP"
assert_eq "old passphrase rejected" "$(json_field "$RESP" "message")" "incorrect passphrase"

echo ""
echo "--- Test 12: new passphrase unlocks ---"
RESP=$(mqtt_vault "unlock" "{\"passphrase\":\"$NEW_PASSPHRASE\"}")
echo "  Response: $RESP"
assert_eq "new passphrase unlocks" "$(json_field "$RESP" "data.status")" "unlocked"

echo ""
echo "--- Test 13: read note after passphrase change (plaintext) ---"
RESP=$("$MQDB_BIN" read notes "$RECORD_ID" --broker 127.0.0.1:$PORT --user "$CANONICAL_ID" --pass "$MQTT_PASS" 2>/dev/null)
echo "  Response: $RESP"
assert_eq "title still decrypts" "$(json_field "$RESP" "data.title")" "Secret Note"

echo ""
echo "--- Test 14: vault disable (via MQTT) ---"
RESP=$(mqtt_vault "disable" "{\"passphrase\":\"$NEW_PASSPHRASE\"}")
echo "  Response: $RESP"
assert_eq "disable ok" "$(json_field "$RESP" "status")" "ok"
assert_eq "status disabled" "$(json_field "$RESP" "data.status")" "disabled"

echo ""
echo "--- Test 15: read note after disable (plaintext, no encryption) ---"
RESP=$("$MQDB_BIN" read notes "$RECORD_ID" --broker 127.0.0.1:$PORT --user "$CANONICAL_ID" --pass "$MQTT_PASS" 2>/dev/null)
echo "  Response: $RESP"
assert_eq "plaintext after disable" "$(json_field "$RESP" "data.title")" "Secret Note"

echo ""
echo "--- Test 16: vault status (disabled) ---"
RESP=$(mqtt_vault "status" "{}")
echo "  Response: $RESP"
assert_eq "vault_enabled is false after disable" "$(json_field "$RESP" "data.vault_enabled")" "false"

echo ""
echo "=== Results: $PASS/$TOTAL passed, $FAIL failed ==="

if [[ $FAIL -gt 0 ]]; then
    echo ""
    echo "Agent logs (last 50 lines):"
    tail -50 "$TEST_DIR/agent.log"
    exit 1
fi

echo "All tests passed!"
