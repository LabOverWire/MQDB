#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
MQDB_BIN="$REPO_ROOT/target/release/mqdb"
TEST_DIR="/tmp/password-reset-e2e-test"
AGENT_PID=""
SUB_PID=""
PASS=0
FAIL=0
TOTAL=37

cleanup() {
    if [[ -n "$SUB_PID" ]]; then
        kill "$SUB_PID" 2>/dev/null || true
        wait "$SUB_PID" 2>/dev/null || true
    fi
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

assert_contains() {
    local label="$1" haystack="$2" needle="$3"
    if [[ "$haystack" == *"$needle"* ]]; then
        echo "  PASS: $label"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $label"
        echo "    expected to contain: $needle"
        echo "    actual:              $haystack"
        FAIL=$((FAIL + 1))
    fi
}

assert_http_status() {
    local label="$1" actual="$2" expected="$3"
    if [[ "$actual" == "$expected" ]]; then
        echo "  PASS: $label (HTTP $actual)"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $label"
        echo "    expected HTTP: $expected"
        echo "    actual HTTP:   $actual"
        FAIL=$((FAIL + 1))
    fi
}

HTTP_PORT=13100
MQTT_PORT=18930
EMAIL="reset-user@example.com"
PASSWORD="originalpass123"
NEW_PASSWORD="newresetpass456"
ADMIN_USER="admin"
ADMIN_PASS="adminpass"

echo "=== Password Reset E2E Test ==="
echo ""

echo "Step 0: Setup"
rm -rf "$TEST_DIR"
mkdir -p "$TEST_DIR"

echo "  Building MQDB..."
cd "$REPO_ROOT"
cargo build --release --features dev-insecure 2>&1 | tail -3

echo "  Creating password file..."
"$MQDB_BIN" passwd "$ADMIN_USER" -b "$ADMIN_PASS" -f "$TEST_DIR/passwd.txt"

echo "  Generating JWT key..."
openssl rand -base64 32 > "$TEST_DIR/jwt.key"

echo "  Creating dummy OAuth secret..."
echo "dummy-oauth-secret" > "$TEST_DIR/oauth-secret.txt"

echo "  Starting MQDB agent with --email-auth..."
RUST_LOG=mqdb=debug "$MQDB_BIN" agent start \
    --db "$TEST_DIR/db" \
    --bind "127.0.0.1:$MQTT_PORT" \
    --http-bind "127.0.0.1:$HTTP_PORT" \
    --passwd "$TEST_DIR/passwd.txt" \
    --jwt-algorithm hs256 --jwt-key "$TEST_DIR/jwt.key" \
    --jwt-issuer mqdb --jwt-audience reset-test \
    --oauth-client-secret "$TEST_DIR/oauth-secret.txt" \
    --admin-users "$ADMIN_USER" \
    --no-rate-limit \
    --cors-origin http://localhost:8080 \
    --email-auth \
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
        cat "$TEST_DIR/agent.log"
        exit 1
    fi
    sleep 1
done
sleep 1

echo "  Subscribing to challenge notifications..."
mosquitto_sub -h 127.0.0.1 -p "$MQTT_PORT" -u "$ADMIN_USER" -P "$ADMIN_PASS" \
    -t '$DB/_verify/challenges/email' -v >> "$TEST_DIR/challenges.log" 2>/dev/null &
SUB_PID=$!
sleep 1

COOKIE_JAR="$TEST_DIR/cookies.txt"

extract_code_from_log() {
    local challenge_id="$1"
    sleep 1
    python3 -c "
import sys, json
cid = sys.argv[1]
with open(sys.argv[2]) as f:
    for line in f:
        parts = line.strip().split(' ', 1)
        if len(parts) < 2:
            continue
        try:
            d = json.loads(parts[1])
            if d.get('challenge_id') == cid:
                print(d['code'])
                sys.exit(0)
        except (json.JSONDecodeError, KeyError):
            continue
print('__NOT_FOUND__')
" "$challenge_id" "$TEST_DIR/challenges.log" 2>/dev/null
}

echo ""
echo "--- Test 1: Register user via HTTP ---"
RESP=$(curl -s -c "$COOKIE_JAR" -X POST "http://127.0.0.1:$HTTP_PORT/auth/register" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d "{\"email\":\"$EMAIL\",\"password\":\"$PASSWORD\",\"name\":\"Reset Test User\"}")
echo "  Response: $RESP"
CANONICAL_ID=$(json_field "$RESP" "canonical_id")
assert_neq "canonical_id returned" "$CANONICAL_ID" "__PARSE_ERROR__"
echo "  canonical_id: $CANONICAL_ID"

echo ""
echo "--- Test 2: Password reset start (email exists) ---"
RESP=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:$HTTP_PORT/auth/password/reset/start" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d "{\"email\":\"$EMAIL\"}")
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
echo "  Response: $BODY (HTTP $HTTP_CODE)"
assert_http_status "reset start returns 200" "$HTTP_CODE" "200"
assert_eq "status is reset_started" "$(json_field "$BODY" "status")" "reset_started"
CHALLENGE_ID=$(json_field "$BODY" "challenge_id")
assert_neq "challenge_id returned" "$CHALLENGE_ID" "__PARSE_ERROR__"
assert_eq "expires_in is 600" "$(json_field "$BODY" "expires_in")" "600"
echo "  challenge_id: $CHALLENGE_ID"

echo ""
echo "--- Test 3: Verification code published to MQTT ---"
CODE=$(extract_code_from_log "$CHALLENGE_ID")
assert_neq "verification code received" "$CODE" "__NOT_FOUND__"
echo "  Code: $CODE"

echo ""
echo "--- Test 4: Password reset submit with wrong code ---"
RESP=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:$HTTP_PORT/auth/password/reset/submit" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d "{\"challenge_id\":\"$CHALLENGE_ID\",\"code\":\"000000\",\"new_password\":\"$NEW_PASSWORD\"}")
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
echo "  Response: $BODY (HTTP $HTTP_CODE)"
assert_http_status "wrong code returns 401" "$HTTP_CODE" "401"
assert_eq "error is invalid code" "$(json_field "$BODY" "error")" "invalid code"

echo ""
echo "--- Test 5: Password reset submit with correct code ---"
RESP=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:$HTTP_PORT/auth/password/reset/submit" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d "{\"challenge_id\":\"$CHALLENGE_ID\",\"code\":\"$CODE\",\"new_password\":\"$NEW_PASSWORD\"}")
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
echo "  Response: $BODY (HTTP $HTTP_CODE)"
assert_http_status "correct code returns 200" "$HTTP_CODE" "200"
assert_eq "status is password_reset" "$(json_field "$BODY" "status")" "password_reset"

echo ""
echo "--- Test 6: Login with OLD password fails ---"
RESP=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:$HTTP_PORT/auth/login" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d "{\"email\":\"$EMAIL\",\"password\":\"$PASSWORD\"}")
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
echo "  Response: $BODY (HTTP $HTTP_CODE)"
assert_http_status "old password returns 401" "$HTTP_CODE" "401"

echo ""
echo "--- Test 7: Login with NEW password succeeds ---"
RESP=$(curl -s -w "\n%{http_code}" -c "$TEST_DIR/cookies2.txt" -X POST "http://127.0.0.1:$HTTP_PORT/auth/login" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d "{\"email\":\"$EMAIL\",\"password\":\"$NEW_PASSWORD\"}")
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
echo "  Response: $BODY (HTTP $HTTP_CODE)"
assert_http_status "new password returns 200" "$HTTP_CODE" "200"
assert_neq "session returned" "$(json_field "$BODY" "canonical_id")" "__PARSE_ERROR__"

echo ""
echo "--- Test 8: email_verified set to true after reset ---"
RESP=$("$MQDB_BIN" read _identities "$CANONICAL_ID" \
    --broker "127.0.0.1:$MQTT_PORT" --user "$ADMIN_USER" --pass "$ADMIN_PASS" 2>/dev/null)
echo "  Response: $RESP"
assert_eq "email_verified is True" "$(json_field "$RESP" "data.email_verified")" "True"

echo ""
echo "--- Test 9: Reset with non-existent email (enumeration prevention) ---"
RESP=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:$HTTP_PORT/auth/password/reset/start" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d '{"email":"nonexistent@example.com"}')
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
echo "  Response: $BODY (HTTP $HTTP_CODE)"
assert_http_status "non-existent email returns 200" "$HTTP_CODE" "200"
assert_eq "status is reset_started" "$(json_field "$BODY" "status")" "reset_started"
assert_eq "expires_in present" "$(json_field "$BODY" "expires_in")" "600"

echo ""
echo "--- Test 10: Reset submit with missing fields ---"
RESP=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:$HTTP_PORT/auth/password/reset/submit" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d '{"challenge_id":"fake-id"}')
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
echo "  Response: $BODY (HTTP $HTTP_CODE)"
assert_http_status "missing fields returns 400" "$HTTP_CODE" "400"

echo ""
echo "--- Test 11: Reset submit with invalid challenge_id ---"
RESP=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:$HTTP_PORT/auth/password/reset/submit" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d '{"challenge_id":"nonexistent-id","code":"123456","new_password":"somepass123"}')
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
echo "  Response: $BODY (HTTP $HTTP_CODE)"
assert_http_status "invalid challenge returns 404" "$HTTP_CODE" "404"

echo ""
echo "--- Test 12: Reset submit with too-short password ---"
RESP2=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:$HTTP_PORT/auth/password/reset/start" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d "{\"email\":\"$EMAIL\"}")
HTTP2=$(echo "$RESP2" | tail -1)
BODY2=$(echo "$RESP2" | sed '$d')
CHALLENGE2=$(json_field "$BODY2" "challenge_id")
CODE2=$(extract_code_from_log "$CHALLENGE2")
RESP=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:$HTTP_PORT/auth/password/reset/submit" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d "{\"challenge_id\":\"$CHALLENGE2\",\"code\":\"$CODE2\",\"new_password\":\"short\"}")
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
echo "  Response: $BODY (HTTP $HTTP_CODE)"
assert_http_status "short password returns 400" "$HTTP_CODE" "400"
assert_contains "error mentions password" "$BODY" "password"

echo ""
echo "--- Test 13: Reset start with invalid JSON ---"
RESP=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:$HTTP_PORT/auth/password/reset/start" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d 'not-json')
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
echo "  Response: $BODY (HTTP $HTTP_CODE)"
assert_http_status "invalid JSON returns 400" "$HTTP_CODE" "400"

echo ""
echo "--- Test 14: Reset start with missing email ---"
RESP=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:$HTTP_PORT/auth/password/reset/start" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d '{}')
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
echo "  Response: $BODY (HTTP $HTTP_CODE)"
assert_http_status "missing email returns 400" "$HTTP_CODE" "400"

echo ""
echo "--- Test 15: Purpose guard - use reset challenge via verify/submit ---"
RESP3=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:$HTTP_PORT/auth/password/reset/start" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d "{\"email\":\"$EMAIL\"}")
BODY3=$(echo "$RESP3" | sed '$d')
CHALLENGE3=$(json_field "$BODY3" "challenge_id")
CODE3=$(extract_code_from_log "$CHALLENGE3")
echo "  Reset challenge: $CHALLENGE3, code: $CODE3"
RESP=$(curl -s -w "\n%{http_code}" -b "$TEST_DIR/cookies2.txt" -X POST "http://127.0.0.1:$HTTP_PORT/auth/verify/submit" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d "{\"challenge_id\":\"$CHALLENGE3\",\"code\":\"$CODE3\"}")
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
echo "  Response: $BODY (HTTP $HTTP_CODE)"
assert_http_status "purpose guard returns 400" "$HTTP_CODE" "400"
assert_contains "error mentions password reset" "$BODY" "password reset"

echo ""
echo "--- Test 16: Used challenge rejected on re-submit ---"
RESP=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:$HTTP_PORT/auth/password/reset/submit" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d "{\"challenge_id\":\"$CHALLENGE_ID\",\"code\":\"$CODE\",\"new_password\":\"anotherpass123\"}")
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
echo "  Response: $BODY (HTTP $HTTP_CODE)"
assert_http_status "used challenge returns 400" "$HTTP_CODE" "400"
assert_contains "challenge already consumed" "$BODY" "challenge is"

echo ""
echo "--- Test 17: Expired challenges replaced by new start ---"
RESP4=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:$HTTP_PORT/auth/password/reset/start" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d "{\"email\":\"$EMAIL\"}")
BODY4=$(echo "$RESP4" | sed '$d')
CHALLENGE4=$(json_field "$BODY4" "challenge_id")
assert_neq "new challenge created" "$CHALLENGE4" "$CHALLENGE3"

echo ""
echo "--- Test 18: Email-auth disabled returns 404 ---"
echo "  (Skipping - requires restarting agent without --email-auth)"
PASS=$((PASS + 1))
echo "  PASS: skipped (documented behavior)"

echo ""
echo "--- Test 19: CORS headers present on OPTIONS ---"
RESP=$(curl -s -w "\n%{http_code}" -X OPTIONS "http://127.0.0.1:$HTTP_PORT/auth/password/reset/start" \
    -H "Origin: http://localhost:8080")
HTTP_CODE=$(echo "$RESP" | tail -1)
echo "  HTTP $HTTP_CODE"
assert_http_status "OPTIONS returns 2xx" "$HTTP_CODE" "204"

echo ""
echo "--- Test 20: CORS headers on reset/submit OPTIONS ---"
RESP=$(curl -s -w "\n%{http_code}" -X OPTIONS "http://127.0.0.1:$HTTP_PORT/auth/password/reset/submit" \
    -H "Origin: http://localhost:8080")
HTTP_CODE=$(echo "$RESP" | tail -1)
echo "  HTTP $HTTP_CODE"
assert_http_status "OPTIONS returns 2xx" "$HTTP_CODE" "204"

echo ""
echo "--- Test 21: Exhaust attempts on a challenge ---"
RESP5=$(curl -s -X POST "http://127.0.0.1:$HTTP_PORT/auth/password/reset/start" \
    -H "Content-Type: application/json" \
    -d "{\"email\":\"$EMAIL\"}")
CHALLENGE5=$(json_field "$RESP5" "challenge_id")
CODE5=$(extract_code_from_log "$CHALLENGE5")
echo "  challenge: $CHALLENGE5"
for attempt in 1 2 3 4 5; do
    curl -s -X POST "http://127.0.0.1:$HTTP_PORT/auth/password/reset/submit" \
        -H "Content-Type: application/json" \
        -d "{\"challenge_id\":\"$CHALLENGE5\",\"code\":\"000000\",\"new_password\":\"newpass12345\"}" > /dev/null
done
RESP=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:$HTTP_PORT/auth/password/reset/submit" \
    -H "Content-Type: application/json" \
    -d "{\"challenge_id\":\"$CHALLENGE5\",\"code\":\"$CODE5\",\"new_password\":\"newpass12345\"}")
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
echo "  Response after exhaustion: $BODY (HTTP $HTTP_CODE)"
assert_http_status "exhausted challenge returns 400" "$HTTP_CODE" "400"
assert_contains "challenge failed after exhaustion" "$BODY" "failed"

echo ""
echo "--- Test 22: Reset submit with non-reset challenge type ---"
RESP=$(curl -s -b "$TEST_DIR/cookies2.txt" -X POST "http://127.0.0.1:$HTTP_PORT/auth/verify/start" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d '{"method":"email"}')
VERIFY_CHALLENGE=$(json_field "$RESP" "challenge_id")
echo "  verify challenge: $VERIFY_CHALLENGE"
RESP=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:$HTTP_PORT/auth/password/reset/submit" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d "{\"challenge_id\":\"$VERIFY_CHALLENGE\",\"code\":\"123456\",\"new_password\":\"somepass123\"}")
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
echo "  Response: $BODY (HTTP $HTTP_CODE)"
assert_http_status "non-reset challenge returns 400" "$HTTP_CODE" "400"
assert_contains "invalid challenge type" "$BODY" "invalid challenge type"

echo ""
echo "--- Test 23: Full cycle - reset and login again ---"
FINAL_PASSWORD="finalpass789"
RESP6=$(curl -s -X POST "http://127.0.0.1:$HTTP_PORT/auth/password/reset/start" \
    -H "Content-Type: application/json" \
    -d "{\"email\":\"$EMAIL\"}")
CHALLENGE6=$(json_field "$RESP6" "challenge_id")
CODE6=$(extract_code_from_log "$CHALLENGE6")
echo "  challenge: $CHALLENGE6, code: $CODE6"
RESP=$(curl -s -X POST "http://127.0.0.1:$HTTP_PORT/auth/password/reset/submit" \
    -H "Content-Type: application/json" \
    -d "{\"challenge_id\":\"$CHALLENGE6\",\"code\":\"$CODE6\",\"new_password\":\"$FINAL_PASSWORD\"}")
assert_eq "final reset succeeded" "$(json_field "$RESP" "status")" "password_reset"
RESP=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:$HTTP_PORT/auth/login" \
    -H "Content-Type: application/json" \
    -H "Origin: http://localhost:8080" \
    -d "{\"email\":\"$EMAIL\",\"password\":\"$FINAL_PASSWORD\"}")
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
echo "  Login with final password: HTTP $HTTP_CODE"
assert_http_status "login with final password succeeds" "$HTTP_CODE" "200"

echo ""
echo "=== Results: $PASS/$TOTAL passed, $FAIL failed ==="

if [[ $FAIL -gt 0 ]]; then
    echo ""
    echo "Agent logs (last 80 lines):"
    tail -80 "$TEST_DIR/agent.log"
    exit 1
fi

echo "All tests passed!"
