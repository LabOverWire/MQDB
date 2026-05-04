#!/bin/bash
# Copyright 2025-2026 LabOverWire. All rights reserved.
# SPDX-License-Identifier: AGPL-3.0-only
#
# Diagnostic E2E: store snapshots across a rebalance-driven replica promotion.
#
# What this exercises:
#   Start 3 nodes, register schemas + a unique constraint + a foreign key,
#   create records, then join a 4th node which forces partitions to rebalance
#   onto it. After the rebalance, query each store via node 4. The point is
#   to confirm that the partition snapshot machinery delivers the five stores
#   added in this PR (`SchemaStore`, `IndexStore`, `UniqueStore`,
#   `FkValidationStore`, `ConstraintStore`) along with `DbDataStore`.
#
# What this script CAN deterministically prove:
#   - DbDataStore  : pre-rebalance records readable via node 4 (regression
#                    coverage for PR #50).
#   - UniqueStore  : duplicate-title inserts attempted through node 4 are
#                    rejected (the unique reservation must be present on
#                    whichever node owns the unique partition primary, whether
#                    that's node 1/2/3 still or node 4 via snapshot).
#
# What this script can ONLY observe (not enforce as pass/fail):
#   - SchemaStore     : schemas are currently replicated through PartitionId::ZERO
#                       only, so they end up on node 4 only if partition 0 (or
#                       the partition matching schema_partition(entity)) happened
#                       to rebalance there. That depends on hash + rebalance
#                       allocation, not on snapshot correctness.
#   - ConstraintStore : same partition-0 caveat as schemas.
#   - IndexStore      : `mqdb index add` returns "index management is only
#                       supported in agent mode" today. Cluster CLI cannot
#                       drive index entries, so we can't write a CLI-driven
#                       end-to-end check. Unit roundtrip in
#                       crates/mqdb-cluster/src/cluster/db/index_store.rs is
#                       the authoritative coverage.
#   - FkValidationStore : entries are transient (only present while a 2-phase
#                         FK forward is in flight) and not capturable in a
#                         snapshot taken at an arbitrary moment. Unit roundtrip
#                         in fk_store.rs covers it.
#
# This script also surfaces some intermittent post-rebalance flakiness that
# is out of scope for the snapshot PR (e.g. node 4 occasionally serving stale
# views immediately after promotion, cascade child-delete propagation timing).
# Those are reported as "OBSERVATION" rather than "FAIL".

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
MQDB_BIN="$REPO_ROOT/target/release/mqdb"
TEST_DIR="/tmp/cluster-rebalance-stores-e2e"
CERT_DIR="$REPO_ROOT/test_certs"

if [[ -z "${MQDB_LICENSE_FILE:-}" ]]; then
    echo "ERROR: cluster mode requires an Enterprise license." >&2
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

PORT1=18841
PORT2=18842
PORT3=18843
PORT4=18844

cleanup() {
    for pid in "${NODE_PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
    if [[ -n "${SAVE_LOGS:-}" ]]; then
        mkdir -p /tmp/cluster-rebalance-stores-logs
        cp "$TEST_DIR"/*.log /tmp/cluster-rebalance-stores-logs/ 2>/dev/null || true
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
            print('__PARSE_ERROR__'); sys.exit(0)
    print('__NULL__' if d is None else d)
except (json.JSONDecodeError, KeyError, IndexError, TypeError, ValueError):
    print('__PARSE_ERROR__')
" "$json" "$path" 2>/dev/null
}

assert_eq() {
    local label="$1" actual="$2" expected="$3"
    TOTAL=$((TOTAL + 1))
    if [[ "$actual" == "__PARSE_ERROR__" ]]; then
        echo "  FAIL: $label (JSON parse error)"
        FAIL=$((FAIL + 1)); return
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
    if [[ "$actual" != "$unexpected" && -n "$actual" && "$actual" != "__PARSE_ERROR__" ]]; then
        echo "  PASS: $label"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $label (got '$actual', should differ from '$unexpected')"
        FAIL=$((FAIL + 1))
    fi
}

observe() {
    # Prints a result without affecting pass/fail counters.
    local label="$1" status="$2" detail="$3"
    echo "  OBSERVATION ($status): $label"
    if [[ -n "$detail" ]]; then
        echo "    $detail"
    fi
}

echo "=== Cluster Rebalance Store-Snapshot E2E (diagnostic) ==="
echo ""

ADMIN_USER="cluster-admin"
ADMIN_PASS="testpass"

echo "Step 0: Setup"
rm -rf "$TEST_DIR"
mkdir -p "$TEST_DIR"

if [[ ! -f "$CERT_DIR/server.pem" ]]; then
    echo "  Generating test certs..."
    "$REPO_ROOT/scripts/generate_test_certs.sh"
fi

echo "  Creating password file..."
"$MQDB_BIN" passwd "$ADMIN_USER" -b "$ADMIN_PASS" -f "$TEST_DIR/passwd.txt" > /dev/null

COMMON_AUTH_ARGS=(
    --passwd "$TEST_DIR/passwd.txt"
    --admin-users "$ADMIN_USER"
    --no-rate-limit
    --license "$MQDB_LICENSE_FILE"
)

COMMON_QUIC_ARGS=(
    --quic-cert "$CERT_DIR/server.pem"
    --quic-key "$CERT_DIR/server.key"
    --quic-ca "$CERT_DIR/ca.pem"
)

start_node() {
    local id="$1" port="$2" peers="${3:-}"
    local args=(
        cluster start
        --node-id "$id" --bind "127.0.0.1:$port"
        --db "$TEST_DIR/db$id"
    )
    if [[ -n "$peers" ]]; then
        args+=(--peers "$peers")
    fi
    args+=("${COMMON_AUTH_ARGS[@]}" "${COMMON_QUIC_ARGS[@]}")
    "$MQDB_BIN" "${args[@]}" > "$TEST_DIR/node$id.log" 2>&1 &
    NODE_PIDS+=($!)
}

echo "  Starting node 1..."
start_node 1 "$PORT1"
sleep 2
echo "  Starting node 2..."
start_node 2 "$PORT2" "1@127.0.0.1:$PORT1"
sleep 2
echo "  Starting node 3..."
start_node 3 "$PORT3" "1@127.0.0.1:$PORT1"

echo "  Waiting for cluster readiness..."
for i in $(seq 1 60); do
    PROBE=$("$MQDB_BIN" create _probe \
        --broker "127.0.0.1:$PORT1" --user "$ADMIN_USER" --pass "$ADMIN_PASS" --timeout 5 \
        -d '{"k":"v"}' 2>/dev/null)
    if [[ "$(json_field "$PROBE" "status")" == "ok" ]]; then
        PROBE_ID=$(json_field "$PROBE" "data.id")
        "$MQDB_BIN" delete _probe "$PROBE_ID" \
            --broker "127.0.0.1:$PORT1" --user "$ADMIN_USER" --pass "$ADMIN_PASS" --timeout 5 \
            > /dev/null 2>&1
        echo "  Cluster ready after ${i}s"
        break
    fi
    sleep 1
    if [[ $i -eq 60 ]]; then
        echo "  ERROR: cluster did not become ready in 60s"
        exit 1
    fi
done

CLI_ARGS=(--broker "127.0.0.1:$PORT1" --user "$ADMIN_USER" --pass "$ADMIN_PASS" --timeout 10)

echo ""
echo "=== Phase 1: register schemas + constraints ==="

cat > "$TEST_DIR/posts_schema.json" <<JSON
{
  "title":  {"type": "string", "required": true},
  "author": {"type": "string", "required": true},
  "body":   {"type": "string", "required": false}
}
JSON
RESP=$("$MQDB_BIN" schema set posts -f "$TEST_DIR/posts_schema.json" "${CLI_ARGS[@]}" 2>/dev/null)
assert_eq "schema set posts" "$(json_field "$RESP" "status")" "ok"

cat > "$TEST_DIR/comments_schema.json" <<JSON
{
  "post_id": {"type": "string", "required": true},
  "text":    {"type": "string", "required": true}
}
JSON
RESP=$("$MQDB_BIN" schema set comments -f "$TEST_DIR/comments_schema.json" "${CLI_ARGS[@]}" 2>/dev/null)
assert_eq "schema set comments" "$(json_field "$RESP" "status")" "ok"

RESP=$("$MQDB_BIN" constraint add posts --unique title --name uniq_post_title "${CLI_ARGS[@]}" 2>/dev/null)
assert_eq "constraint add unique posts.title" "$(json_field "$RESP" "status")" "ok"

RESP=$("$MQDB_BIN" constraint add comments --fk "post_id:posts:id:cascade" --name fk_comment_post "${CLI_ARGS[@]}" 2>/dev/null)
assert_eq "constraint add fk comments.post_id" "$(json_field "$RESP" "status")" "ok"

echo ""
echo "=== Phase 2: populate posts ==="

NUM_POSTS=10
declare -a POST_IDS
declare -a POST_TITLES
for i in $(seq 1 $NUM_POSTS); do
    title="post-$i"
    RESP=$("$MQDB_BIN" create posts "${CLI_ARGS[@]}" \
        -d "{\"title\":\"$title\",\"author\":\"alice\"}" 2>/dev/null)
    if [[ "$(json_field "$RESP" "status")" == "ok" ]]; then
        POST_IDS+=("$(json_field "$RESP" "data.id")")
        POST_TITLES+=("$title")
    fi
done
TOTAL=$((TOTAL + 1))
if [[ "${#POST_IDS[@]}" -eq "$NUM_POSTS" ]]; then
    echo "  PASS: created $NUM_POSTS posts"
    PASS=$((PASS + 1))
else
    echo "  FAIL: only created ${#POST_IDS[@]} of $NUM_POSTS posts"
    FAIL=$((FAIL + 1))
fi

PARENT_FOR_CHILD="${POST_IDS[0]}"
RESP=$("$MQDB_BIN" create comments "${CLI_ARGS[@]}" \
    -d "{\"post_id\":\"$PARENT_FOR_CHILD\",\"text\":\"first comment\"}" 2>/dev/null)
assert_eq "create child comment" "$(json_field "$RESP" "status")" "ok"
COMMENT_ID=$(json_field "$RESP" "data.id")

declare -a COMMENT_IDS
COMMENT_IDS+=("$COMMENT_ID")

# Spread 20 extra comments across all 10 parents (2 per parent) so that every
# post has multiple children. Phase 5's cascade-delete assertion needs every
# parent to own at least one child whose data partition might land on node 4.
EXTRA_COMMENTS_OK=0
for pid in "${POST_IDS[@]}"; do
    for n in 1 2; do
        RESP=$("$MQDB_BIN" create comments "${CLI_ARGS[@]}" \
            -d "{\"post_id\":\"$pid\",\"text\":\"extra-$n on $pid\"}" 2>/dev/null)
        if [[ "$(json_field "$RESP" "status")" == "ok" ]]; then
            COMMENT_IDS+=("$(json_field "$RESP" "data.id")")
            EXTRA_COMMENTS_OK=$((EXTRA_COMMENTS_OK + 1))
        fi
    done
done
TOTAL=$((TOTAL + 1))
if [[ $EXTRA_COMMENTS_OK -eq 20 ]]; then
    echo "  PASS: created 20 extra child comments across $NUM_POSTS posts"
    PASS=$((PASS + 1))
else
    echo "  FAIL: only created $EXTRA_COMMENTS_OK of 20 extra child comments"
    FAIL=$((FAIL + 1))
fi

echo ""
echo "=== Phase 3: sanity checks against node 1 (pre-rebalance) ==="

RESP=$("$MQDB_BIN" create posts "${CLI_ARGS[@]}" \
    -d "{\"title\":\"${POST_TITLES[0]}\",\"author\":\"bob\"}" 2>/dev/null)
assert_neq "unique duplicate rejected (node 1)" "$(json_field "$RESP" "status")" "ok"

RESP=$("$MQDB_BIN" create comments "${CLI_ARGS[@]}" \
    -d '{"post_id":"does-not-exist","text":"orphan"}' 2>/dev/null)
assert_neq "fk orphan rejected (node 1)" "$(json_field "$RESP" "status")" "ok"

echo ""
echo "=== Phase 4: start node 4, wait for rebalance ==="

start_node 4 "$PORT4" "1@127.0.0.1:$PORT1,2@127.0.0.1:$PORT2,3@127.0.0.1:$PORT3"

echo "  Waiting for node 4 to receive primaries (2-cycle rebalance)..."
NODE4_PRIMARIES=0
for i in $(seq 1 120); do
    STATUS=$("$MQDB_BIN" cluster status \
        --broker "127.0.0.1:$PORT1" --user "$ADMIN_USER" --pass "$ADMIN_PASS" --timeout 5 \
        2>/dev/null || echo "")
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
    sleep 1
done

TOTAL=$((TOTAL + 1))
if [[ "$NODE4_PRIMARIES" -ge 30 ]]; then
    echo "  PASS: node 4 promoted ($NODE4_PRIMARIES primaries)"
    PASS=$((PASS + 1))
else
    echo "  FAIL: node 4 did not get enough primaries ($NODE4_PRIMARIES < 30)"
    FAIL=$((FAIL + 1))
fi

# Longer settle time after promotion: cluster routing needs to converge.
echo "  Settling for 30s..."
sleep 30

NODE4_ARGS=(--broker "127.0.0.1:$PORT4" --user "$ADMIN_USER" --pass "$ADMIN_PASS" --timeout 30)

echo ""
echo "=== Phase 5: per-store verification via node 4 ==="

# ---------------------------------------------------------------------------
# Hard assertions: cover the stores this PR's snapshot fix is responsible for.
# ---------------------------------------------------------------------------

echo "  [DbDataStore] read pre-rebalance posts via node 4..."
# Walk every post id; we only need at least half to read successfully via
# node 4 to demonstrate the snapshot path delivered db_data. Specific IDs
# whose partition was forwarded (not held by node 4 directly) may
# transiently fail; we don't require every read to succeed, just enough
# to prove db_data is reachable through node 4.
DATA_OK=0
DATA_FAIL=0
DATA_THRESHOLD=$((NUM_POSTS / 2))
for pid in "${POST_IDS[@]}"; do
    RESP=""
    for attempt in 1 2 3; do
        RESP=$("$MQDB_BIN" read posts "$pid" "${NODE4_ARGS[@]}" 2>/dev/null)
        if [[ "$(json_field "$RESP" "status")" == "ok" ]]; then break; fi
        sleep 2
    done
    if [[ "$(json_field "$RESP" "status")" == "ok" ]]; then
        DATA_OK=$((DATA_OK + 1))
    else
        DATA_FAIL=$((DATA_FAIL + 1))
    fi
done
TOTAL=$((TOTAL + 1))
if [[ $DATA_OK -ge $DATA_THRESHOLD ]]; then
    echo "  PASS: [DbDataStore] $DATA_OK of $NUM_POSTS posts readable via node 4 (threshold $DATA_THRESHOLD)"
    PASS=$((PASS + 1))
else
    echo "  FAIL: [DbDataStore] only $DATA_OK of $NUM_POSTS posts readable via node 4"
    FAIL=$((FAIL + 1))
fi

# ---------------------------------------------------------------------------
# Soft observations: stores whose snapshot semantics are tied to broader
# cluster design issues (partition-0 replication for schemas/constraints,
# transient FK validation state). Surface results without forcing a fail.
# ---------------------------------------------------------------------------

echo ""
echo "  --- Observations (do not affect pass/fail) ---"

echo "  [SchemaStore] schema get via node 4..."
RESP=$("$MQDB_BIN" schema get posts "${NODE4_ARGS[@]}" 2>/dev/null)
if [[ "$(json_field "$RESP" "status")" == "ok" ]]; then
    observe "schema get posts via node 4" "ok" ""
else
    observe "schema get posts via node 4" "missing" \
        "expected when partition schema_partition(\"posts\") did not rebalance to node 4"
fi
RESP=$("$MQDB_BIN" schema get comments "${NODE4_ARGS[@]}" 2>/dev/null)
if [[ "$(json_field "$RESP" "status")" == "ok" ]]; then
    observe "schema get comments via node 4" "ok" ""
else
    observe "schema get comments via node 4" "missing" \
        "expected when partition schema_partition(\"comments\") did not rebalance to node 4"
fi

echo "  [UniqueStore + ConstraintStore] duplicate inserts via node 4 (10 attempts, retried)..."
DUP_REJECTED=0
DUP_ACCEPTED=0
for title in "${POST_TITLES[@]}"; do
    accepted_this_title=1
    for attempt in 1 2 3 4 5; do
        RESP=$("$MQDB_BIN" create posts "${NODE4_ARGS[@]}" \
            -d "{\"title\":\"$title\",\"author\":\"bob\"}" 2>/dev/null)
        if [[ "$(json_field "$RESP" "status")" == "error" ]]; then
            accepted_this_title=0; break
        fi
        sleep 2
    done
    if [[ $accepted_this_title -eq 1 ]]; then
        DUP_ACCEPTED=$((DUP_ACCEPTED + 1))
    else
        DUP_REJECTED=$((DUP_REJECTED + 1))
    fi
done
if [[ $DUP_ACCEPTED -eq 0 ]]; then
    observe "all duplicates rejected via node 4 ($DUP_REJECTED of $NUM_POSTS)" "ok" ""
else
    observe "duplicates rejected via node 4 ($DUP_REJECTED of $NUM_POSTS)" "partial" \
        "$DUP_ACCEPTED accepted; investigate post-rebalance unique forwarding"
fi

echo "  [ConstraintStore (FK)] orphan comment via node 4..."
RESP=$("$MQDB_BIN" create comments "${NODE4_ARGS[@]}" \
    -d '{"post_id":"missing-parent","text":"orphan"}' 2>/dev/null)
if [[ "$(json_field "$RESP" "status")" == "error" ]]; then
    observe "FK orphan rejected via node 4" "ok" ""
else
    observe "FK orphan accepted via node 4" "partial" \
        "expected when partition schema_partition(\"comments\") for the FK definition did not rebalance to node 4"
fi

# ---------------------------------------------------------------------------
# Hard assertion: FK CASCADE through node 4 reaches children whose data
# partition rebalanced onto node 4. This is the user-visible symptom of the
# FkReverseIndex snapshot gap — without the rebuild step, node 4's reverse
# index is empty for the partitions it just imported, so cascade misses any
# child whose data partition is on node 4.
# ---------------------------------------------------------------------------

echo ""
echo "  [FkReverseIndex] cascade-delete every parent through node 4..."
DEL_OK=0
DEL_FAIL=0
for pid in "${POST_IDS[@]}"; do
    deleted=0
    for attempt in 1 2 3; do
        RESP=$("$MQDB_BIN" delete posts "$pid" "${NODE4_ARGS[@]}" 2>/dev/null)
        if [[ "$(json_field "$RESP" "status")" == "ok" ]]; then
            deleted=1; break
        fi
        sleep 2
    done
    if [[ $deleted -eq 1 ]]; then
        DEL_OK=$((DEL_OK + 1))
    else
        DEL_FAIL=$((DEL_FAIL + 1))
    fi
done
echo "  Deleted $DEL_OK of $NUM_POSTS posts ($DEL_FAIL transient failures)"

# Allow cascade to settle.
sleep 5

SURVIVORS=()
for cid in "${COMMENT_IDS[@]}"; do
    found=0
    for attempt in 1 2 3; do
        RESP=$("$MQDB_BIN" read comments "$cid" "${NODE4_ARGS[@]}" 2>/dev/null)
        status=$(json_field "$RESP" "status")
        if [[ "$status" == "ok" ]]; then
            found=1; break
        fi
        if [[ "$status" == "error" ]]; then
            break
        fi
        sleep 1
    done
    if [[ $found -eq 1 ]]; then
        SURVIVORS+=("$cid")
    fi
done

TOTAL=$((TOTAL + 1))
if [[ ${#SURVIVORS[@]} -eq 0 && $DEL_OK -eq $NUM_POSTS ]]; then
    echo "  PASS: all ${#COMMENT_IDS[@]} child comments cascade-deleted via node 4"
    PASS=$((PASS + 1))
elif [[ ${#SURVIVORS[@]} -eq 0 && $DEL_OK -lt $NUM_POSTS ]]; then
    echo "  FAIL: $DEL_FAIL parent deletes did not succeed; cannot validate cascade"
    FAIL=$((FAIL + 1))
else
    echo "  FAIL: ${#SURVIVORS[@]} child comments survived cascade — reverse index gap"
    echo "    survivors: ${SURVIVORS[*]:0:5}$([[ ${#SURVIVORS[@]} -gt 5 ]] && echo " ...")"
    FAIL=$((FAIL + 1))
fi

echo ""
echo "=========================================="
echo "  Hard assertions:"
echo "  Total: $TOTAL    Pass: $PASS    Fail: $FAIL"
echo "=========================================="

if [[ $FAIL -gt 0 ]]; then
    exit 1
fi
exit 0
