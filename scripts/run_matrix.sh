#!/bin/bash
set -e

MQDB="./target/release/mqdb"
RESULTS_DIR="benchmark_results_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"

LICENSE_ARG=""
if [ -n "$MQDB_LICENSE" ]; then
    LICENSE_ARG="--license $MQDB_LICENSE"
fi

log() {
    echo "[$(date +%H:%M:%S)] $1" | tee -a "$RESULTS_DIR/log.txt"
}

run_pubsub() {
    local id=$1
    local broker=$2
    local pub_broker=$3
    local sub_broker=$4
    local run=$5

    if [ -z "$pub_broker" ]; then
        result=$($MQDB bench pubsub --broker "$broker" --duration 10 2>&1 | grep -E "Throughput" | awk '{print $3}')
    else
        result=$($MQDB bench pubsub --pub-broker "$pub_broker" --sub-broker "$sub_broker" --duration 10 2>&1 | grep -E "Throughput" | awk '{print $3}')
    fi
    echo "$id,$run,$result" >> "$RESULTS_DIR/pubsub.csv"
    log "$id run $run: $result msg/s"
}

run_sync_db() {
    local id=$1
    local broker=$2
    local op=$3
    local run=$4

    local ops=1000
    local seed=""
    [ "$op" = "list" ] && ops=100
    [ "$op" = "get" ] || [ "$op" = "update" ] && seed="--seed 1000"
    [ "$op" = "list" ] && seed="--seed 100"

    result=$($MQDB bench db --broker "$broker" --op "$op" --operations $ops --no-latency $seed 2>&1 | grep -E "Throughput" | awk '{print $3}')
    echo "$id,$run,$result" >> "$RESULTS_DIR/sync_db.csv"
    log "$id run $run: $result ops/s"
}

run_async_db() {
    local id=$1
    local broker=$2
    local op=$3
    local run=$4

    local seed=""
    [ "$op" = "get" ] || [ "$op" = "update" ] && seed="--seed 10000"

    result=$($MQDB bench db --broker "$broker" --op "$op" --async --duration 60 $seed 2>&1 | grep -E "Throughput" | awk '{print $3}')
    echo "$id,$run,$result" >> "$RESULTS_DIR/async_db.csv"
    log "$id run $run: $result ops/s"
}

echo "id,run,throughput" > "$RESULTS_DIR/pubsub.csv"
echo "id,run,throughput" > "$RESULTS_DIR/sync_db.csv"
echo "id,run,throughput" > "$RESULTS_DIR/async_db.csv"

case "$1" in
    agent)
        log "=== AGENT MODE ==="
        pkill -f "mqdb agent" 2>/dev/null || true
        rm -rf /tmp/mqdb-bench-agent
        $MQDB agent start --db /tmp/mqdb-bench-agent --bind 127.0.0.1:1883 --anonymous &
        sleep 5

        for run in 1 2 3; do
            run_pubsub "A1" "127.0.0.1:1883" "" "" $run
        done

        for op in insert get update list; do
            for run in 1 2 3; do
                run_sync_db "C${op:0:1}-1" "127.0.0.1:1883" "$op" $run
            done
        done

        for op in insert get update; do
            log "Restarting for async $op"
            pkill -f "mqdb agent" 2>/dev/null || true
            sleep 2
            rm -rf /tmp/mqdb-bench-agent
            $MQDB agent start --db /tmp/mqdb-bench-agent --bind 127.0.0.1:1883 --anonymous &
            sleep 5
            for run in 1 2 3; do
                run_async_db "D${op:0:1}-1" "127.0.0.1:1883" "$op" $run
            done
            sleep 10
        done

        pkill -f "mqdb agent" 2>/dev/null || true
        ;;

    partial|upper|full)
        topo=$1

        topo_upper=$(echo $topo | tr '[:lower:]' '[:upper:]')
        log "=== ${topo_upper} QUIC ==="
        $MQDB dev kill 2>/dev/null || true
        $MQDB dev start-cluster --nodes 3 --clean --topology $topo $LICENSE_ARG
        sleep 8

        for run in 1 2 3; do
            for port in 1883 1884 1885; do
                node=$((port - 1882))
                run_pubsub "A-${topo}-N${node}" "127.0.0.1:$port" "" "" $run
            done
        done

        for run in 1 2 3; do
            for pub in 1883 1884 1885; do
                for sub in 1883 1884 1885; do
                    [ "$pub" = "$sub" ] && continue
                    pub_node=$((pub - 1882))
                    sub_node=$((sub - 1882))
                    run_pubsub "B-${topo}-${pub_node}to${sub_node}" "" "127.0.0.1:$pub" "127.0.0.1:$sub" $run
                done
            done
        done

        for op in insert get update list; do
            for run in 1 2 3; do
                for port in 1883 1884 1885; do
                    node=$((port - 1882))
                    run_sync_db "C${op:0:1}-${topo}-N${node}" "127.0.0.1:$port" "$op" $run
                done
            done
        done

        for op in insert get update; do
            log "Restarting for async $op"
            $MQDB dev kill 2>/dev/null || true
            sleep 2
            $MQDB dev start-cluster --nodes 3 --clean --topology $topo $LICENSE_ARG
            sleep 8
            for run in 1 2 3; do
                for port in 1883 1884 1885; do
                    node=$((port - 1882))
                    run_async_db "D${op:0:1}-${topo}-N${node}" "127.0.0.1:$port" "$op" $run
                done
            done
            sleep 10
        done

        $MQDB dev kill 2>/dev/null || true
        ;;

    *)
        echo "Usage: $0 {agent|partial|upper|full}"
        exit 1
        ;;
esac

log "Results saved to $RESULTS_DIR/"
