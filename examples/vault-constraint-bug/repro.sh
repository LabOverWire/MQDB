#!/bin/bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
MQDB_BIN="$REPO_ROOT/target/release/mqdb"
TEST_DIR="/tmp/vault-constraint-repro"
AGENT_PID=""

PORT=19876
HTTP_PORT=19877
CANONICAL_ID="testuser"
MQTT_PASS="testpass"
PASSPHRASE="my-vault-passphrase"

cleanup() {
    if [[ -n "$AGENT_PID" ]]; then
        kill "$AGENT_PID" 2>/dev/null || true
        wait "$AGENT_PID" 2>/dev/null || true
    fi
    rm -rf "$TEST_DIR"
}
trap cleanup EXIT

json_field() {
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
" "$1" "$2" 2>/dev/null
}

echo "=== Vault + Unique Constraint Bug Reproduction ==="
echo ""

echo "Step 0: Setup"
cd "$REPO_ROOT"
echo "  Building MQDB with dev-insecure..."
cargo build --release --features dev-insecure 2>&1 | tail -3

rm -rf "$TEST_DIR"
mkdir -p "$TEST_DIR"

echo "  Creating password file..."
"$MQDB_BIN" passwd "$CANONICAL_ID" -b "$MQTT_PASS" -f "$TEST_DIR/passwd.txt"

echo "  Generating JWT key..."
openssl rand -base64 32 > "$TEST_DIR/jwt.key"

echo "  Creating OAuth secret..."
echo "dummy-oauth-secret" > "$TEST_DIR/oauth-secret.txt"

echo "  Starting agent..."
RUST_LOG=mqdb=debug "$MQDB_BIN" agent start \
    --db "$TEST_DIR/db" \
    --bind "127.0.0.1:$PORT" \
    --http-bind "127.0.0.1:$HTTP_PORT" \
    --passwd "$TEST_DIR/passwd.txt" \
    --jwt-algorithm hs256 --jwt-key "$TEST_DIR/jwt.key" \
    --jwt-issuer mqdb --jwt-audience test \
    --oauth-client-secret "$TEST_DIR/oauth-secret.txt" \
    --ownership "items=userId" \
    --admin-users "$CANONICAL_ID" \
    --no-rate-limit \
    --cors-origin http://localhost:8080 \
    > "$TEST_DIR/agent.log" 2>&1 &
AGENT_PID=$!

echo "  Waiting for agent readiness..."
for i in $(seq 1 30); do
    if curl -sf "http://127.0.0.1:$HTTP_PORT/health" > /dev/null 2>&1; then
        echo "  Agent ready after ${i}s"
        break
    fi
    if [[ $i -eq 30 ]]; then
        echo "  FATAL: Agent failed to start. Logs:"
        tail -30 "$TEST_DIR/agent.log"
        exit 1
    fi
    sleep 1
done

sleep 1
COOKIE_JAR="$TEST_DIR/cookies.txt"

echo ""
echo "=========================================="
echo "  Phase 1: WITHOUT vault (baseline)"
echo "=========================================="
echo ""

echo "  Adding unique constraint on 'email' field..."
RESP=$(mosquitto_rr -h 127.0.0.1 -p "$PORT" -u "$CANONICAL_ID" -P "$MQTT_PASS" \
    -t '$DB/_admin/constraint/items/add' -e 'resp/constraint' \
    -m '{"type":"unique","name":"unique_items_email","fields":["email"]}' -W 5 2>/dev/null)
echo "  Response: $RESP"

echo ""
echo "  Creating first item with email=alice@test.com..."
RESP=$("$MQDB_BIN" create items \
    --broker "127.0.0.1:$PORT" --user "$CANONICAL_ID" --pass "$MQTT_PASS" \
    -d '{"email":"alice@test.com","name":"Alice","userId":"testuser"}' 2>/dev/null)
echo "  Response: $RESP"
STATUS=$(json_field "$RESP" "status")
FIRST_ID=$(json_field "$RESP" "id")
if [[ "$FIRST_ID" == "__PARSE_ERROR__" ]]; then
    FIRST_ID=$(json_field "$RESP" "data.id")
fi
echo "  Status: $STATUS  ID: $FIRST_ID"

echo ""
echo "  Creating DUPLICATE with email=alice@test.com (expect 409 rejection)..."
RESP=$("$MQDB_BIN" create items \
    --broker "127.0.0.1:$PORT" --user "$CANONICAL_ID" --pass "$MQTT_PASS" \
    -d '{"email":"alice@test.com","name":"Alice Dup","userId":"testuser"}' 2>/dev/null)
echo "  Response: $RESP"
STATUS=$(json_field "$RESP" "status")
CODE=$(json_field "$RESP" "code")
if [[ "$STATUS" == "error" ]]; then
    echo "  CORRECT: unique constraint blocked the duplicate (code=$CODE)"
else
    echo "  WRONG: duplicate was accepted (status=$STATUS)"
fi

echo ""
echo "  Cleaning up..."
"$MQDB_BIN" delete items "$FIRST_ID" \
    --broker "127.0.0.1:$PORT" --user "$CANONICAL_ID" --pass "$MQTT_PASS" --timeout 10 2>/dev/null > /dev/null

echo ""
echo "=========================================="
echo "  Phase 2: WITH vault (bug test)"
echo "=========================================="
echo ""

echo "  Dev-login..."
RESP=$(curl -s -c "$COOKIE_JAR" -X POST "http://127.0.0.1:$HTTP_PORT/auth/dev-login" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d "{\"email\":\"test@example.com\",\"name\":\"Test User\",\"canonical_id\":\"$CANONICAL_ID\"}")
echo "  $RESP"

echo ""
echo "  Enabling vault..."
RESP=$(curl -s -b "$COOKIE_JAR" -X POST "http://127.0.0.1:$HTTP_PORT/vault/enable" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d "{\"passphrase\":\"$PASSPHRASE\"}")
echo "  $RESP"

sleep 1

echo ""
echo "  Creating first item with email=bob@test.com (vault encrypts it)..."
RESP=$("$MQDB_BIN" create items \
    --broker "127.0.0.1:$PORT" --user "$CANONICAL_ID" --pass "$MQTT_PASS" \
    -d '{"email":"bob@test.com","name":"Bob","userId":"testuser"}' 2>/dev/null)
echo "  Response: $RESP"
STATUS=$(json_field "$RESP" "status")
VAULT_ID1=$(json_field "$RESP" "id")
if [[ "$VAULT_ID1" == "__PARSE_ERROR__" ]]; then
    VAULT_ID1=$(json_field "$RESP" "data.id")
fi
echo "  Status: $STATUS  ID: $VAULT_ID1"

echo ""
echo "  Creating DUPLICATE with email=bob@test.com (expect 409 rejection)..."
RESP=$("$MQDB_BIN" create items \
    --broker "127.0.0.1:$PORT" --user "$CANONICAL_ID" --pass "$MQTT_PASS" \
    -d '{"email":"bob@test.com","name":"Bob Dup","userId":"testuser"}' 2>/dev/null)
echo "  Response: $RESP"
STATUS=$(json_field "$RESP" "status")
CODE=$(json_field "$RESP" "code")
VAULT_ID2=$(json_field "$RESP" "id")
if [[ "$VAULT_ID2" == "__PARSE_ERROR__" ]]; then
    VAULT_ID2=$(json_field "$RESP" "data.id")
fi

echo ""
echo "=========================================="
echo "  RESULT"
echo "=========================================="
echo ""
if [[ "$STATUS" == "error" ]]; then
    echo "  Unique constraint correctly rejected duplicate (code=$CODE)"
    echo "  BUG NOT REPRODUCED"
else
    echo "  BUG CONFIRMED: duplicate was accepted (status=$STATUS)"
    echo ""
    echo "  Record 1 ID: $VAULT_ID1"
    echo "  Record 2 ID: $VAULT_ID2"
    echo ""
    echo "  Both records have email=bob@test.com but vault encryption"
    echo "  produces different ciphertext for each, so the unique index"
    echo "  sees two different values and allows the duplicate."
    echo ""
    echo "  Listing all items to confirm:"
    LIST=$("$MQDB_BIN" list items \
        --broker "127.0.0.1:$PORT" --user "$CANONICAL_ID" --pass "$MQTT_PASS" 2>/dev/null)
    python3 -c "
import json, sys
d = json.loads(sys.argv[1])
items = d.get('data', [])
bobs = [i for i in items if i.get('data',{}).get('email') == 'bob@test.com']
print(f'  Total items: {len(items)}')
print(f'  Items with email=bob@test.com: {len(bobs)}')
for b in bobs:
    print(f'    - id={b.get(\"id\",\"?\")}, name={b.get(\"data\",{}).get(\"name\",\"?\")}')
" "$LIST" 2>/dev/null
fi

echo ""
echo "Done."
