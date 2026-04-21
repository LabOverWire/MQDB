#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MQDB_ROOT="${MQDB_ROOT:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"
[ -f "$HOME/.cargo/env" ] && source "$HOME/.cargo/env"
MQDB_BIN="${MQDB_ROOT}/target/release/mqdb"
BRIDGE_BIN="${SCRIPT_DIR}/target/release/mqdb-baseline-bench"
REST_BIN="${SCRIPT_DIR}/rest-pg/target/release/rest-pg-bench"
PG_CONN="postgres://postgres@127.0.0.1:5433/mqdb_bench"
REDIS_URL="redis://127.0.0.1:6380"
RESULTS_DIR="${RESULTS_DIR:-${SCRIPT_DIR}/results/phase1}"
OPERATIONS="${OPERATIONS:-10000}"
TRIPLICATES="${TRIPLICATES:-5}"
PUBSUB_DURATION="${PUBSUB_DURATION:-10}"
CONCURRENCY_LEVELS="${CONCURRENCY_LEVELS:-1 8 32 128}"
CHANGEFEED_WRITE_RATE="${CHANGEFEED_WRITE_RATE:-500}"
CHANGEFEED_DURATION="${CHANGEFEED_DURATION:-30}"
UNIQUE_CONCURRENCY_LEVELS="${UNIQUE_CONCURRENCY_LEVELS:-4 16 64}"
UNIQUE_ATTEMPTS="${UNIQUE_ATTEMPTS:-100}"
CASCADE_CHILDREN_LEVELS="${CASCADE_CHILDREN_LEVELS:-10 100 1000}"
CASCADE_RUNS="${CASCADE_RUNS:-5}"
PROVENANCE_JSON="${PROVENANCE_JSON:-}"

source "${SCRIPT_DIR}/scripts/guard_aws_dir.sh"
if ! guard_aws_dir; then
    echo "run_phase1_agent.sh: guard_aws_dir refused the RESULTS_DIR" >&2
    exit 1
fi

merge_provenance() {
    local file="$1"
    [ -z "$PROVENANCE_JSON" ] && return 0
    [ ! -f "$PROVENANCE_JSON" ] && return 0
    [ ! -f "$file" ] && return 0
    python3 - "$file" "$PROVENANCE_JSON" <<'PYEOF'
import json, sys
out, prov = sys.argv[1], sys.argv[2]
with open(out) as f: data = json.load(f)
with open(prov) as f: provenance = json.load(f)
data["provenance"] = provenance
with open(out, "w") as f: json.dump(data, f, indent=2)
PYEOF
}
PG_BRIDGE_PID=""
REDIS_BRIDGE_PID=""
REST_SERVER_PID=""
MQDB_PID=""

cleanup() {
    echo "Cleaning up..."
    [ -n "$PG_BRIDGE_PID" ] && kill "$PG_BRIDGE_PID" 2>/dev/null || true
    [ -n "$REDIS_BRIDGE_PID" ] && kill "$REDIS_BRIDGE_PID" 2>/dev/null || true
    [ -n "$REST_SERVER_PID" ] && kill "$REST_SERVER_PID" 2>/dev/null || true
    [ -n "$MQDB_PID" ] && kill "$MQDB_PID" 2>/dev/null || true
    rm -rf /tmp/mqdb-bench-agent
    cd "$SCRIPT_DIR" && docker compose down -v 2>/dev/null || true
}

check_prereqs() {
    local missing=0
    if ! command -v docker &>/dev/null; then
        echo "Missing: docker"
        missing=1
    fi
    if [ ! -f "$MQDB_BIN" ]; then
        echo "Missing: $MQDB_BIN — building..."
        (cd "$MQDB_ROOT" && cargo build --release)
    fi
    if [ ! -f "$BRIDGE_BIN" ]; then
        echo "Missing: $BRIDGE_BIN — building..."
        (cd "$SCRIPT_DIR" && cargo build --release)
    fi
    if [ ! -f "$REST_BIN" ]; then
        echo "Missing: $REST_BIN — building..."
        (cd "${SCRIPT_DIR}/rest-pg" && cargo build --release)
    fi
    if [ "$missing" -eq 1 ]; then
        exit 1
    fi
}

start_services() {
    echo "Starting PostgreSQL, Redis, and Mosquitto instances via Docker Compose..."
    cd "$SCRIPT_DIR" && docker compose up -d --wait
    echo "Initializing PG schema..."
    docker compose exec -T postgres psql -U postgres -d mqdb_bench -q < "${SCRIPT_DIR}/schema.sql"
    echo "Waiting for host-side port forwarding..."
    local retries=30
    for i in $(seq 1 $retries); do
        if nc -z 127.0.0.1 5433 2>/dev/null \
            && nc -z 127.0.0.1 1884 2>/dev/null \
            && nc -z 127.0.0.1 1885 2>/dev/null \
            && nc -z 127.0.0.1 6380 2>/dev/null; then
            sleep 1
            echo "All services ready"
            return 0
        fi
        sleep 1
    done
    echo "ERROR: Services not reachable on host after ${retries}s"
    exit 1
}

start_pg_bridge() {
    "$BRIDGE_BIN" bridge --broker "127.0.0.1:1884" --pg "$PG_CONN" &
    PG_BRIDGE_PID=$!
    echo "PG bridge started (PID=$PG_BRIDGE_PID)"
    sleep 2
    if ! kill -0 "$PG_BRIDGE_PID" 2>/dev/null; then
        echo "ERROR: PG bridge process died."
        exit 1
    fi
}

start_redis_bridge() {
    "$BRIDGE_BIN" bridge-redis --broker "127.0.0.1:1885" --redis-url "$REDIS_URL" &
    REDIS_BRIDGE_PID=$!
    echo "Redis bridge started (PID=$REDIS_BRIDGE_PID)"
    sleep 2
    if ! kill -0 "$REDIS_BRIDGE_PID" 2>/dev/null; then
        echo "ERROR: Redis bridge process died."
        exit 1
    fi
}

start_rest_server() {
    "$REST_BIN" serve --bind "127.0.0.1:3000" --pg "$PG_CONN" &
    REST_SERVER_PID=$!
    echo "REST server started (PID=$REST_SERVER_PID)"
    sleep 2
    if ! kill -0 "$REST_SERVER_PID" 2>/dev/null; then
        echo "ERROR: REST server died."
        exit 1
    fi
}

stop_mqdb() {
    if [ -n "$MQDB_PID" ]; then
        kill "$MQDB_PID" 2>/dev/null || true
        wait "$MQDB_PID" 2>/dev/null || true
        MQDB_PID=""
    fi
    rm -rf /tmp/mqdb-bench-agent
}

start_mqdb() {
    stop_mqdb
    PASSWD_FILE=$(mktemp)
    "$MQDB_BIN" passwd bench -b bench -f "$PASSWD_FILE"

    "$MQDB_BIN" agent start \
        --db /tmp/mqdb-bench-agent \
        --bind 127.0.0.1:1883 \
        --passwd "$PASSWD_FILE" \
        --admin-users bench &
    MQDB_PID=$!
    sleep 3
    rm -f "$PASSWD_FILE"
    if ! kill -0 "$MQDB_PID" 2>/dev/null; then
        echo "ERROR: MQDB agent died."
        exit 1
    fi
}

start_mqdb_memory() {
    stop_mqdb
    PASSWD_FILE=$(mktemp)
    "$MQDB_BIN" passwd bench -b bench -f "$PASSWD_FILE"

    "$MQDB_BIN" agent start \
        --db /tmp/mqdb-bench-agent \
        --memory-backend \
        --bind 127.0.0.1:1883 \
        --passwd "$PASSWD_FILE" \
        --admin-users bench &
    MQDB_PID=$!
    sleep 3
    rm -f "$PASSWD_FILE"
    if ! kill -0 "$MQDB_PID" 2>/dev/null; then
        echo "ERROR: MQDB agent (memory) died."
        exit 1
    fi
}

pg_clear() {
    cd "$SCRIPT_DIR" && docker compose exec -T postgres psql -U postgres -d mqdb_bench -c "DELETE FROM records" -q
}

redis_clear() {
    cd "$SCRIPT_DIR" && docker compose exec -T redis redis-cli FLUSHDB > /dev/null
}

extract_json() {
    python3 -c "
import sys, json
text = sys.stdin.read()
start = text.index('{')
obj = json.loads(text[start:])
json.dump(obj, sys.stdout, indent=2)
"
}

run_bench() {
    local broker="$1"
    local op="$2"
    local extra_args="${3:-}"
    local raw
    raw=$("$MQDB_BIN" bench db \
        --broker "$broker" \
        --op "$op" \
        --operations "$OPERATIONS" \
        $extra_args \
        --format json 2>/dev/null)
    echo "$raw" | extract_json
}

run_rest_bench() {
    local op="$1"
    local seed="${2:-0}"
    "$REST_BIN" bench \
        --url "http://127.0.0.1:3000" \
        --op "$op" \
        --operations "$OPERATIONS" \
        --seed "$seed" \
        --cleanup 2>/dev/null
}

run_bench_changefeed() {
    local broker="$1"
    local extra_args="${2:-}"
    local raw
    raw=$("$MQDB_BIN" bench db \
        --broker "$broker" \
        --op changefeed \
        --write-rate "$CHANGEFEED_WRITE_RATE" \
        --duration "$CHANGEFEED_DURATION" \
        $extra_args \
        --format json 2>/dev/null)
    echo "$raw" | extract_json
}

run_rest_bench_changefeed() {
    local entity="$1"
    "$REST_BIN" bench \
        --url "http://127.0.0.1:3000" \
        --op changefeed \
        --entity "$entity" \
        --write-rate "$CHANGEFEED_WRITE_RATE" \
        --duration "$CHANGEFEED_DURATION" 2>/dev/null
}

run_bench_unique() {
    local broker="$1"
    local concurrency="$2"
    local extra_args="${3:-}"
    local raw
    raw=$("$MQDB_BIN" bench db \
        --broker "$broker" \
        --op unique \
        --concurrency "$concurrency" \
        --attempts-per-client "$UNIQUE_ATTEMPTS" \
        $extra_args \
        --format json 2>/dev/null)
    echo "$raw" | extract_json
}

run_rest_bench_unique() {
    local entity="$1"
    local concurrency="$2"
    "$REST_BIN" bench \
        --url "http://127.0.0.1:3000" \
        --op unique \
        --entity "$entity" \
        --concurrency "$concurrency" \
        --attempts-per-client "$UNIQUE_ATTEMPTS" 2>/dev/null
}

run_bench_cascade() {
    local broker="$1"
    local children="$2"
    local extra_args="${3:-}"
    local raw
    raw=$("$MQDB_BIN" bench db \
        --broker "$broker" \
        --op cascade \
        --children "$children" \
        --runs "$CASCADE_RUNS" \
        $extra_args \
        --format json 2>/dev/null)
    echo "$raw" | extract_json
}

run_rest_bench_cascade() {
    local entity="$1"
    local children="$2"
    "$REST_BIN" bench \
        --url "http://127.0.0.1:3000" \
        --op cascade \
        --entity "$entity" \
        --children "$children" \
        --runs "$CASCADE_RUNS" 2>/dev/null
}

run_bench_mixed_concurrent() {
    local broker="$1"
    local concurrency="$2"
    local extra_args="${3:-}"
    local raw
    raw=$("$MQDB_BIN" bench db \
        --broker "$broker" \
        --op mixed \
        --operations "$OPERATIONS" \
        --concurrency "$concurrency" \
        $extra_args \
        --format json 2>/dev/null)
    echo "$raw" | extract_json
}

run_pubsub() {
    local broker="$1"
    local qos="$2"
    local extra_args="${3:-}"
    local raw
    raw=$("$MQDB_BIN" bench pubsub \
        --broker "$broker" \
        --duration "$PUBSUB_DURATION" \
        --qos "$qos" \
        $extra_args \
        --format json 2>/dev/null)
    echo "$raw" | extract_json
}

print_crud_results() {
    local op="$1"

    printf "%-20s" ""
    for run in $(seq 1 "$TRIPLICATES"); do
        printf " %12s" "Run${run}"
    done
    echo ""

    for label in mqdb baseline_pg baseline_redis rest_pg mqdb_mem; do
        printf "%-20s" "$label"
        for run in $(seq 1 "$TRIPLICATES"); do
            local f="${RESULTS_DIR}/${label}_${op}_run${run}.json"
            if [ -f "$f" ]; then
                local tput
                tput=$(python3 -c "import json; d=json.load(open('$f')); print(f\"{d['throughput_ops_sec']:.0f}\")" 2>/dev/null || echo "err")
                printf " %10s" "${tput} ops/s"
            else
                printf " %12s" "N/A"
            fi
        done
        echo ""
    done
}

print_pubsub_results() {
    local qos="$1"

    printf "%-20s" ""
    for run in $(seq 1 "$TRIPLICATES"); do
        printf " %12s" "Run${run}"
    done
    echo ""

    for label in mqdb mosquitto mqdb_mem; do
        printf "%-20s" "$label"
        for run in $(seq 1 "$TRIPLICATES"); do
            local f="${RESULTS_DIR}/${label}_pubsub_qos${qos}_run${run}.json"
            if [ -f "$f" ]; then
                local tput
                tput=$(python3 -c "import json; d=json.load(open('$f')); print(f\"{d['throughput_msg_sec']:.0f}\")" 2>/dev/null || echo "err")
                printf " %10s" "${tput} msg/s"
            else
                printf " %12s" "N/A"
            fi
        done
        echo ""
    done
}

run_crud_triplicates() {
    local op="$1"
    local seed="${2:-0}"
    local extra_flags="${3:-}"

    echo ""
    echo "--- $op ($TRIPLICATES triplicates, $OPERATIONS ops, seed=$seed) ---"

    for run in $(seq 1 "$TRIPLICATES"); do
        start_mqdb
        echo "  [$run/$TRIPLICATES] MQDB $op..."
        run_bench "127.0.0.1:1883" "$op" "--user bench --pass bench --seed $seed --cleanup $extra_flags" \
            > "${RESULTS_DIR}/mqdb_${op}_run${run}.json"
        merge_provenance "${RESULTS_DIR}/mqdb_${op}_run${run}.json"

        pg_clear
        echo "  [$run/$TRIPLICATES] Baseline PG $op..."
        run_bench "127.0.0.1:1884" "$op" "--seed $seed --cleanup $extra_flags" \
            > "${RESULTS_DIR}/baseline_pg_${op}_run${run}.json"
        merge_provenance "${RESULTS_DIR}/baseline_pg_${op}_run${run}.json"

        pg_clear
        redis_clear
        echo "  [$run/$TRIPLICATES] Baseline Redis $op..."
        run_bench "127.0.0.1:1885" "$op" "--seed $seed --cleanup $extra_flags" \
            > "${RESULTS_DIR}/baseline_redis_${op}_run${run}.json"
        merge_provenance "${RESULTS_DIR}/baseline_redis_${op}_run${run}.json"

        pg_clear
        redis_clear
        echo "  [$run/$TRIPLICATES] REST+PG $op..."
        run_rest_bench "$op" "$seed" \
            > "${RESULTS_DIR}/rest_pg_${op}_run${run}.json"
        merge_provenance "${RESULTS_DIR}/rest_pg_${op}_run${run}.json"

        start_mqdb_memory
        echo "  [$run/$TRIPLICATES] MQDB-memory $op..."
        run_bench "127.0.0.1:1883" "$op" "--user bench --pass bench --seed $seed --cleanup $extra_flags" \
            > "${RESULTS_DIR}/mqdb_mem_${op}_run${run}.json"
        merge_provenance "${RESULTS_DIR}/mqdb_mem_${op}_run${run}.json"

        pg_clear
    done
}

run_pubsub_triplicates() {
    local qos="$1"

    echo ""
    echo "--- pubsub QoS $qos ($TRIPLICATES triplicates, ${PUBSUB_DURATION}s each) ---"

    for run in $(seq 1 "$TRIPLICATES"); do
        start_mqdb
        echo "  [$run/$TRIPLICATES] MQDB pubsub QoS $qos..."
        run_pubsub "127.0.0.1:1883" "$qos" "--user bench --pass bench" \
            > "${RESULTS_DIR}/mqdb_pubsub_qos${qos}_run${run}.json"
        merge_provenance "${RESULTS_DIR}/mqdb_pubsub_qos${qos}_run${run}.json"

        echo "  [$run/$TRIPLICATES] Mosquitto pubsub QoS $qos..."
        run_pubsub "127.0.0.1:1884" "$qos" \
            > "${RESULTS_DIR}/mosquitto_pubsub_qos${qos}_run${run}.json"
        merge_provenance "${RESULTS_DIR}/mosquitto_pubsub_qos${qos}_run${run}.json"

        start_mqdb_memory
        echo "  [$run/$TRIPLICATES] MQDB-memory pubsub QoS $qos..."
        run_pubsub "127.0.0.1:1883" "$qos" "--user bench --pass bench" \
            > "${RESULTS_DIR}/mqdb_mem_pubsub_qos${qos}_run${run}.json"
        merge_provenance "${RESULTS_DIR}/mqdb_mem_pubsub_qos${qos}_run${run}.json"
    done
}

run_changefeed_suite() {
    echo ""
    echo "--- changefeed ($TRIPLICATES runs, ${CHANGEFEED_WRITE_RATE} writes/s x ${CHANGEFEED_DURATION}s) ---"

    for run in $(seq 1 "$TRIPLICATES"); do
        start_mqdb
        echo "  [$run/$TRIPLICATES] MQDB changefeed..."
        run_bench_changefeed "127.0.0.1:1883" "--user bench --pass bench --entity cf_entity" \
            > "${RESULTS_DIR}/mqdb_changefeed_run${run}.json"
        merge_provenance "${RESULTS_DIR}/mqdb_changefeed_run${run}.json"

        pg_clear
        echo "  [$run/$TRIPLICATES] Baseline PG changefeed..."
        run_bench_changefeed "127.0.0.1:1884" "--entity cf_entity" \
            > "${RESULTS_DIR}/baseline_pg_changefeed_run${run}.json"
        merge_provenance "${RESULTS_DIR}/baseline_pg_changefeed_run${run}.json"

        pg_clear
        redis_clear
        echo "  [$run/$TRIPLICATES] Baseline Redis changefeed..."
        run_bench_changefeed "127.0.0.1:1885" "--entity cf_entity" \
            > "${RESULTS_DIR}/baseline_redis_changefeed_run${run}.json"
        merge_provenance "${RESULTS_DIR}/baseline_redis_changefeed_run${run}.json"

        pg_clear
        echo "  [$run/$TRIPLICATES] REST+PG changefeed..."
        run_rest_bench_changefeed "cf_entity" \
            > "${RESULTS_DIR}/rest_pg_changefeed_run${run}.json"
        merge_provenance "${RESULTS_DIR}/rest_pg_changefeed_run${run}.json"

        start_mqdb_memory
        echo "  [$run/$TRIPLICATES] MQDB-memory changefeed..."
        run_bench_changefeed "127.0.0.1:1883" "--user bench --pass bench --entity cf_entity" \
            > "${RESULTS_DIR}/mqdb_mem_changefeed_run${run}.json"
        merge_provenance "${RESULTS_DIR}/mqdb_mem_changefeed_run${run}.json"

        pg_clear
    done
}

run_unique_suite() {
    echo ""
    echo "--- unique-contention (${TRIPLICATES} runs, k ∈ {${UNIQUE_CONCURRENCY_LEVELS}}) ---"

    for k in $UNIQUE_CONCURRENCY_LEVELS; do
        for run in $(seq 1 "$TRIPLICATES"); do
            start_mqdb
            echo "  [k=$k run=$run/$TRIPLICATES] MQDB unique..."
            run_bench_unique "127.0.0.1:1883" "$k" "--user bench --pass bench --entity uniq_entity" \
                > "${RESULTS_DIR}/mqdb_unique_k${k}_run${run}.json"
            merge_provenance "${RESULTS_DIR}/mqdb_unique_k${k}_run${run}.json"

            pg_clear
            echo "  [k=$k run=$run/$TRIPLICATES] Baseline PG unique..."
            run_bench_unique "127.0.0.1:1884" "$k" "--entity uniq_entity" \
                > "${RESULTS_DIR}/baseline_pg_unique_k${k}_run${run}.json"
            merge_provenance "${RESULTS_DIR}/baseline_pg_unique_k${k}_run${run}.json"

            pg_clear
            redis_clear
            echo "  [k=$k run=$run/$TRIPLICATES] Baseline Redis unique..."
            run_bench_unique "127.0.0.1:1885" "$k" "--entity uniq_entity" \
                > "${RESULTS_DIR}/baseline_redis_unique_k${k}_run${run}.json"
            merge_provenance "${RESULTS_DIR}/baseline_redis_unique_k${k}_run${run}.json"

            pg_clear
            echo "  [k=$k run=$run/$TRIPLICATES] REST+PG unique..."
            run_rest_bench_unique "uniq_entity_${k}_${run}" "$k" \
                > "${RESULTS_DIR}/rest_pg_unique_k${k}_run${run}.json"
            merge_provenance "${RESULTS_DIR}/rest_pg_unique_k${k}_run${run}.json"

            start_mqdb_memory
            echo "  [k=$k run=$run/$TRIPLICATES] MQDB-memory unique..."
            run_bench_unique "127.0.0.1:1883" "$k" "--user bench --pass bench --entity uniq_entity" \
                > "${RESULTS_DIR}/mqdb_mem_unique_k${k}_run${run}.json"
            merge_provenance "${RESULTS_DIR}/mqdb_mem_unique_k${k}_run${run}.json"

            pg_clear
        done
    done
}

run_cascade_suite() {
    echo ""
    echo "--- cascade-delete (${TRIPLICATES} runs, K ∈ {${CASCADE_CHILDREN_LEVELS}}, ${CASCADE_RUNS} iterations each) ---"

    for k in $CASCADE_CHILDREN_LEVELS; do
        for run in $(seq 1 "$TRIPLICATES"); do
            start_mqdb
            echo "  [K=$k run=$run/$TRIPLICATES] MQDB cascade..."
            run_bench_cascade "127.0.0.1:1883" "$k" "--user bench --pass bench --entity casc_entity" \
                > "${RESULTS_DIR}/mqdb_cascade_k${k}_run${run}.json"
            merge_provenance "${RESULTS_DIR}/mqdb_cascade_k${k}_run${run}.json"

            pg_clear
            echo "  [K=$k run=$run/$TRIPLICATES] Baseline PG cascade..."
            run_bench_cascade "127.0.0.1:1884" "$k" "--entity casc_entity" \
                > "${RESULTS_DIR}/baseline_pg_cascade_k${k}_run${run}.json"
            merge_provenance "${RESULTS_DIR}/baseline_pg_cascade_k${k}_run${run}.json"

            pg_clear
            redis_clear
            echo "  [K=$k run=$run/$TRIPLICATES] Baseline Redis cascade..."
            run_bench_cascade "127.0.0.1:1885" "$k" "--entity casc_entity" \
                > "${RESULTS_DIR}/baseline_redis_cascade_k${k}_run${run}.json"
            merge_provenance "${RESULTS_DIR}/baseline_redis_cascade_k${k}_run${run}.json"

            pg_clear
            echo "  [K=$k run=$run/$TRIPLICATES] REST+PG cascade..."
            run_rest_bench_cascade "casc_entity_${k}_${run}" "$k" \
                > "${RESULTS_DIR}/rest_pg_cascade_k${k}_run${run}.json"
            merge_provenance "${RESULTS_DIR}/rest_pg_cascade_k${k}_run${run}.json"

            start_mqdb_memory
            echo "  [K=$k run=$run/$TRIPLICATES] MQDB-memory cascade..."
            run_bench_cascade "127.0.0.1:1883" "$k" "--user bench --pass bench --entity casc_entity" \
                > "${RESULTS_DIR}/mqdb_mem_cascade_k${k}_run${run}.json"
            merge_provenance "${RESULTS_DIR}/mqdb_mem_cascade_k${k}_run${run}.json"

            pg_clear
        done
    done
}

run_mixed_concurrency_sweep() {
    echo ""
    echo "--- mixed concurrency sweep (${TRIPLICATES} runs, c ∈ {${CONCURRENCY_LEVELS}}, ${OPERATIONS} ops) ---"

    for c in $CONCURRENCY_LEVELS; do
        for run in $(seq 1 "$TRIPLICATES"); do
            start_mqdb
            echo "  [c=$c run=$run/$TRIPLICATES] MQDB mixed..."
            run_bench_mixed_concurrent "127.0.0.1:1883" "$c" "--user bench --pass bench --seed $OPERATIONS --cleanup" \
                > "${RESULTS_DIR}/mqdb_mixed_c${c}_run${run}.json"
            merge_provenance "${RESULTS_DIR}/mqdb_mixed_c${c}_run${run}.json"

            pg_clear
            echo "  [c=$c run=$run/$TRIPLICATES] Baseline PG mixed..."
            run_bench_mixed_concurrent "127.0.0.1:1884" "$c" "--seed $OPERATIONS --cleanup" \
                > "${RESULTS_DIR}/baseline_pg_mixed_c${c}_run${run}.json"
            merge_provenance "${RESULTS_DIR}/baseline_pg_mixed_c${c}_run${run}.json"

            pg_clear
            redis_clear
            echo "  [c=$c run=$run/$TRIPLICATES] Baseline Redis mixed..."
            run_bench_mixed_concurrent "127.0.0.1:1885" "$c" "--seed $OPERATIONS --cleanup" \
                > "${RESULTS_DIR}/baseline_redis_mixed_c${c}_run${run}.json"
            merge_provenance "${RESULTS_DIR}/baseline_redis_mixed_c${c}_run${run}.json"

            start_mqdb_memory
            echo "  [c=$c run=$run/$TRIPLICATES] MQDB-memory mixed..."
            run_bench_mixed_concurrent "127.0.0.1:1883" "$c" "--user bench --pass bench --seed $OPERATIONS --cleanup" \
                > "${RESULTS_DIR}/mqdb_mem_mixed_c${c}_run${run}.json"
            merge_provenance "${RESULTS_DIR}/mqdb_mem_mixed_c${c}_run${run}.json"

            pg_clear
        done
    done
}

print_changefeed_results() {
    printf "%-20s" ""
    for run in $(seq 1 "$TRIPLICATES"); do
        printf " %14s" "Run${run}"
    done
    echo ""

    for label in mqdb baseline_pg baseline_redis rest_pg mqdb_mem; do
        printf "%-20s" "$label"
        for run in $(seq 1 "$TRIPLICATES"); do
            local f="${RESULTS_DIR}/${label}_changefeed_run${run}.json"
            if [ -f "$f" ]; then
                local p95
                p95=$(python3 -c "import json; d=json.load(open('$f')); print(d.get('latency_p95_us', 'N/A'))" 2>/dev/null || echo "err")
                printf " %10s" "${p95} µs p95"
            else
                printf " %14s" "N/A"
            fi
        done
        echo ""
    done
}

print_unique_results() {
    local k="$1"
    printf "%-20s" ""
    for run in $(seq 1 "$TRIPLICATES"); do
        printf " %14s" "Run${run}"
    done
    echo ""

    for label in mqdb baseline_pg baseline_redis rest_pg mqdb_mem; do
        printf "%-20s" "$label"
        for run in $(seq 1 "$TRIPLICATES"); do
            local f="${RESULTS_DIR}/${label}_unique_k${k}_run${run}.json"
            if [ -f "$f" ]; then
                local s c
                s=$(python3 -c "import json; d=json.load(open('$f')); print(d.get('successes_total', 0))" 2>/dev/null || echo "?")
                c=$(python3 -c "import json; d=json.load(open('$f')); print(d.get('conflicts_total', 0))" 2>/dev/null || echo "?")
                printf " %6s/%-6s" "$s" "$c"
            else
                printf " %14s" "N/A"
            fi
        done
        echo ""
    done
}

print_cascade_results() {
    local k="$1"
    printf "%-20s" ""
    for run in $(seq 1 "$TRIPLICATES"); do
        printf " %14s" "Run${run}"
    done
    echo ""

    for label in mqdb baseline_pg baseline_redis rest_pg mqdb_mem; do
        printf "%-20s" "$label"
        for run in $(seq 1 "$TRIPLICATES"); do
            local f="${RESULTS_DIR}/${label}_cascade_k${k}_run${run}.json"
            if [ -f "$f" ]; then
                local dp95 pp95
                dp95=$(python3 -c "import json; d=json.load(open('$f')); print(d.get('delete_latency_p95_us', 0))" 2>/dev/null || echo "?")
                pp95=$(python3 -c "import json; d=json.load(open('$f')); print(d.get('propagation_latency_p95_us', 0))" 2>/dev/null || echo "?")
                printf " %6s/%-6s" "$dp95" "$pp95"
            else
                printf " %14s" "N/A"
            fi
        done
        echo ""
    done
}

print_mixed_concurrency_results() {
    local c="$1"
    printf "%-20s" ""
    for run in $(seq 1 "$TRIPLICATES"); do
        printf " %12s" "Run${run}"
    done
    echo ""

    for label in mqdb baseline_pg baseline_redis mqdb_mem; do
        printf "%-20s" "$label"
        for run in $(seq 1 "$TRIPLICATES"); do
            local f="${RESULTS_DIR}/${label}_mixed_c${c}_run${run}.json"
            if [ -f "$f" ]; then
                local tput
                tput=$(python3 -c "import json; d=json.load(open('$f')); print(f\"{d['throughput_ops_sec']:.0f}\")" 2>/dev/null || echo "err")
                printf " %10s" "${tput} ops/s"
            else
                printf " %12s" "N/A"
            fi
        done
        echo ""
    done
}

main() {
    if [ "${1:-}" = "--dry-run" ]; then
        echo "run_phase1_agent.sh: --dry-run OK (RESULTS_DIR=$RESULTS_DIR)"
        exit 0
    fi

    check_prereqs
    trap cleanup EXIT

    mkdir -p "$RESULTS_DIR"

    start_services
    start_pg_bridge
    start_redis_bridge
    start_rest_server

    echo ""
    echo "============================================"
    echo "  PHASE 1: Agent-Mode Experiments           "
    echo "  MQDB (fjall) vs Mosquitto+PG vs           "
    echo "  Mosquitto+Redis vs REST+PG vs             "
    echo "  MQDB (memory)                             "
    echo "  $TRIPLICATES runs x $OPERATIONS ops       "
    echo "  concurrency: $CONCURRENCY_LEVELS          "
    echo "  changefeed: ${CHANGEFEED_WRITE_RATE}/s x ${CHANGEFEED_DURATION}s"
    echo "  unique k: $UNIQUE_CONCURRENCY_LEVELS      "
    echo "  cascade K: $CASCADE_CHILDREN_LEVELS       "
    echo "============================================"

    echo ""
    echo "=== §6.2 CRUD Throughput + Latency ==="

    run_crud_triplicates "insert" 0
    run_crud_triplicates "get"    "$OPERATIONS"
    run_crud_triplicates "update" "$OPERATIONS"
    run_crud_triplicates "delete" "$OPERATIONS"
    run_crud_triplicates "list"   "$OPERATIONS"
    run_crud_triplicates "mixed"  "$OPERATIONS"

    echo ""
    echo "=== §6.3 Pub/Sub Throughput ==="

    run_pubsub_triplicates 0
    run_pubsub_triplicates 1

    echo ""
    echo "=== §6.4 Architectural Workloads ==="

    run_changefeed_suite
    run_unique_suite
    run_cascade_suite

    echo ""
    echo "=== §6.2 Mixed Concurrency Sweep ==="

    run_mixed_concurrency_sweep

    echo ""
    echo "============================================"
    echo "            PHASE 1 RESULTS                 "
    echo "============================================"

    echo ""
    echo "--- CRUD ---"
    for op in insert get update delete list mixed; do
        echo ""
        echo "  $op:"
        print_crud_results "$op"
    done

    echo ""
    echo "--- Pub/Sub ---"
    for qos in 0 1; do
        echo ""
        echo "  QoS $qos:"
        print_pubsub_results "$qos"
    done

    echo ""
    echo "--- §6.4.1 Change-feed (delivery p95 µs) ---"
    print_changefeed_results

    echo ""
    echo "--- §6.4.2 Unique-contention (successes/conflicts) ---"
    for k in $UNIQUE_CONCURRENCY_LEVELS; do
        echo ""
        echo "  k=$k:"
        print_unique_results "$k"
    done

    echo ""
    echo "--- §6.4.3 Cascade-delete (delete-p95 µs / propagation-p95 µs) ---"
    for k in $CASCADE_CHILDREN_LEVELS; do
        echo ""
        echo "  K=$k:"
        print_cascade_results "$k"
    done

    echo ""
    echo "--- Mixed concurrency sweep ---"
    for c in $CONCURRENCY_LEVELS; do
        echo ""
        echo "  c=$c:"
        print_mixed_concurrency_results "$c"
    done

    echo ""
    echo "Raw results in: $RESULTS_DIR/"
}

main "$@"
