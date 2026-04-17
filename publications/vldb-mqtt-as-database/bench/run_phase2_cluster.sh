#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MQDB_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
MQDB_BIN="${MQDB_ROOT}/target/release/mqdb"
RESULTS_DIR="${SCRIPT_DIR}/results/phase2"
OPERATIONS="${OPERATIONS:-1000}"
TRIPLICATES="${TRIPLICATES:-3}"
PUBSUB_DURATION="${PUBSUB_DURATION:-10}"
LICENSE="${LICENSE:-}"

if [ -z "$LICENSE" ]; then
    echo "ERROR: LICENSE env var must point to the license key file"
    echo "Usage: LICENSE=/path/to/license.key $0"
    exit 1
fi

cleanup() {
    echo "Cleaning up..."
    "$MQDB_BIN" dev kill 2>/dev/null || true
    "$MQDB_BIN" dev clean 2>/dev/null || true
}

check_prereqs() {
    if [ ! -f "$MQDB_BIN" ]; then
        echo "Missing: $MQDB_BIN — building..."
        (cd "$MQDB_ROOT" && cargo build --release)
    fi
    if [ ! -f "$LICENSE" ]; then
        echo "ERROR: License file not found: $LICENSE"
        exit 1
    fi
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

start_cluster() {
    local topology="${1:-partial}"
    local nodes="${2:-3}"

    "$MQDB_BIN" dev kill 2>/dev/null || true
    "$MQDB_BIN" dev clean 2>/dev/null || true
    sleep 1

    "$MQDB_BIN" dev start-cluster \
        --nodes "$nodes" \
        --clean \
        --topology "$topology" \
        --license "$LICENSE"

    sleep 5

    echo "Verifying partition distribution..."
    mosquitto_rr -h 127.0.0.1 -p 1883 -t '$SYS/mqdb/cluster/status' -e 'r' -m '{}' -W 5 | python3 -c "
import sys,json
d=json.load(sys.stdin)
primaries={}
for p in d['data']['partitions']:
    n = p['primary']
    primaries[n] = primaries.get(n,0)+1
print('Primary distribution:', primaries)
" || echo "WARNING: Could not verify partition distribution"
}

stop_cluster() {
    "$MQDB_BIN" dev kill 2>/dev/null || true
    "$MQDB_BIN" dev clean 2>/dev/null || true
    sleep 1
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

print_results() {
    local prefix="$1"

    printf "%-20s" ""
    for run in $(seq 1 "$TRIPLICATES"); do
        printf " %12s" "Run${run}"
    done
    echo ""

    for f_pattern in "${RESULTS_DIR}/${prefix}"*_run1.json; do
        [ -f "$f_pattern" ] || continue
        local label
        label=$(basename "$f_pattern" | sed "s/_run1.json//")
        printf "%-20s" "$label"
        for run in $(seq 1 "$TRIPLICATES"); do
            local f="${RESULTS_DIR}/${label}_run${run}.json"
            if [ -f "$f" ]; then
                local tput
                tput=$(python3 -c "import json; d=json.load(open('$f')); k='throughput_ops_sec' if 'throughput_ops_sec' in d else 'messages_per_sec'; print(f'{d[k]:.0f}')" 2>/dev/null || echo "err")
                printf " %12s" "${tput}"
            else
                printf " %12s" "N/A"
            fi
        done
        echo ""
    done
}

NODE1="127.0.0.1:1883"
NODE2="127.0.0.1:1884"
NODE3="127.0.0.1:1885"

run_crud_on_node() {
    local node="$1"
    local node_label="$2"
    local op="$3"
    local seed="${4:-0}"
    local run="$5"

    run_bench "$node" "$op" "--seed $seed --cleanup" \
        > "${RESULTS_DIR}/cluster_${node_label}_${op}_run${run}.json"
}

main() {
    check_prereqs
    trap cleanup EXIT

    mkdir -p "$RESULTS_DIR"

    echo ""
    echo "============================================"
    echo "  PHASE 2: Cluster-Mode Experiments         "
    echo "  3-node MQDB cluster, 256 partitions, RF=2 "
    echo "  $TRIPLICATES triplicates x $OPERATIONS ops"
    echo "============================================"

    echo ""
    echo "=== §6.4.1 CRUD on Local vs Remote Primary ==="

    for run in $(seq 1 "$TRIPLICATES"); do
        start_cluster "partial" 3

        for op in insert get update delete list; do
            local seed=0
            [ "$op" != "insert" ] && seed="$OPERATIONS"

            echo "  [$run/$TRIPLICATES] node1 $op..."
            run_crud_on_node "$NODE1" "node1" "$op" "$seed" "$run"

            echo "  [$run/$TRIPLICATES] node2 $op..."
            run_crud_on_node "$NODE2" "node2" "$op" "$seed" "$run"

            echo "  [$run/$TRIPLICATES] node3 $op..."
            run_crud_on_node "$NODE3" "node3" "$op" "$seed" "$run"
        done

        stop_cluster
    done

    echo ""
    echo "=== §6.3 Cluster Pub/Sub (same-node + cross-node) ==="

    for run in $(seq 1 "$TRIPLICATES"); do
        start_cluster "partial" 3

        for qos in 0 1; do
            echo "  [$run/$TRIPLICATES] same-node pubsub QoS $qos..."
            run_pubsub "$NODE1" "$qos" \
                > "${RESULTS_DIR}/cluster_samenode_pubsub_qos${qos}_run${run}.json"

            echo "  [$run/$TRIPLICATES] cross-node pubsub QoS $qos..."
            run_pubsub "$NODE1" "$qos" "--pub-broker $NODE1 --sub-broker $NODE2" \
                > "${RESULTS_DIR}/cluster_crossnode_pubsub_qos${qos}_run${run}.json"
        done

        stop_cluster
    done

    echo ""
    echo "=== §6.4.3 Replication Overhead (1-node vs 3-node) ==="

    for run in $(seq 1 "$TRIPLICATES"); do
        start_cluster "partial" 1

        echo "  [$run/$TRIPLICATES] single-node insert..."
        run_bench "$NODE1" "insert" "--cleanup" \
            > "${RESULTS_DIR}/cluster_1node_insert_run${run}.json"

        stop_cluster
        start_cluster "partial" 3

        echo "  [$run/$TRIPLICATES] 3-node insert (node1)..."
        run_bench "$NODE1" "insert" "--cleanup" \
            > "${RESULTS_DIR}/cluster_3node_insert_run${run}.json"

        stop_cluster
    done

    echo ""
    echo "=== §6.4.4 Mesh Topology Comparison ==="

    for topology in partial upper full; do
        for run in $(seq 1 "$TRIPLICATES"); do
            start_cluster "$topology" 3

            echo "  [$run/$TRIPLICATES] $topology mesh insert (node1)..."
            run_bench "$NODE1" "insert" "--cleanup" \
                > "${RESULTS_DIR}/cluster_${topology}_insert_run${run}.json"

            stop_cluster
        done
    done

    echo ""
    echo "============================================"
    echo "            PHASE 2 RESULTS                 "
    echo "============================================"

    echo ""
    echo "--- CRUD per node ---"
    print_results "cluster_node"

    echo ""
    echo "--- Pub/Sub ---"
    print_results "cluster_samenode"
    print_results "cluster_crossnode"

    echo ""
    echo "--- Replication overhead ---"
    print_results "cluster_1node"
    print_results "cluster_3node"

    echo ""
    echo "--- Topology comparison ---"
    print_results "cluster_partial"
    print_results "cluster_upper"
    print_results "cluster_full"

    echo ""
    echo "Raw results in: $RESULTS_DIR/"
}

main "$@"
