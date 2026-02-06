#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
MQDB_BIN="$REPO_ROOT/target/release/mqdb"
DEMO_DIR="/tmp/mqdb-google-demo"
BROKER_PID=""
HTTP_PID=""

cleanup() {
    echo ""
    echo "Shutting down..."
    [[ -n "$BROKER_PID" ]] && kill "$BROKER_PID" 2>/dev/null || true
    [[ -n "$HTTP_PID" ]] && kill "$HTTP_PID" 2>/dev/null || true
    exit 0
}

trap cleanup SIGINT SIGTERM

print_banner() {
    echo "╔═══════════════════════════════════════════════════════════════╗"
    echo "║         MQDB Google OAuth Sign-Up/Sign-In Demo               ║"
    echo "╚═══════════════════════════════════════════════════════════════╝"
    echo ""
}

check_dependencies() {
    local missing=()
    command -v python3 &>/dev/null || missing+=("python3")
    command -v openssl &>/dev/null || missing+=("openssl")

    if [[ ${#missing[@]} -gt 0 ]]; then
        echo "Error: Missing dependencies: ${missing[*]}"
        exit 1
    fi
}

build_mqdb() {
    if [[ ! -f "$MQDB_BIN" ]]; then
        echo "Building MQDB (release)..."
        cd "$REPO_ROOT"
        cargo build --release
        echo "Build complete."
        echo ""
    fi
}

get_client_id() {
    mkdir -p "$DEMO_DIR"

    if [[ -n "$GOOGLE_CLIENT_ID" ]]; then
        echo "Using GOOGLE_CLIENT_ID from environment"
        return
    fi

    if [[ -f "$DEMO_DIR/client_id.txt" ]]; then
        GOOGLE_CLIENT_ID=$(cat "$DEMO_DIR/client_id.txt")
        echo "Using saved Google Client ID"
        return
    fi

    echo "┌─────────────────────────────────────────────────────────────────┐"
    echo "│ Google OAuth Client ID Required                                 │"
    echo "├─────────────────────────────────────────────────────────────────┤"
    echo "│ 1. Go to: https://console.cloud.google.com/apis/credentials     │"
    echo "│ 2. Create OAuth 2.0 Client ID (Web application)                 │"
    echo "│ 3. Add authorized redirect URI:                                  │"
    echo "│    http://localhost:8081/oauth/callback                          │"
    echo "│ 4. Copy the Client ID                                           │"
    echo "└─────────────────────────────────────────────────────────────────┘"
    echo ""
    read -p "Enter Google Client ID: " GOOGLE_CLIENT_ID

    if [[ -z "$GOOGLE_CLIENT_ID" ]]; then
        echo "Error: Client ID is required"
        exit 1
    fi

    echo "$GOOGLE_CLIENT_ID" > "$DEMO_DIR/client_id.txt"
    echo ""
}

get_client_secret() {
    if [[ -n "$GOOGLE_CLIENT_SECRET" ]]; then
        echo "$GOOGLE_CLIENT_SECRET" > "$DEMO_DIR/client_secret.txt"
        echo "Using GOOGLE_CLIENT_SECRET from environment"
        return
    fi

    if [[ -f "$DEMO_DIR/client_secret.txt" ]]; then
        echo "Using saved Google Client Secret"
        return
    fi

    echo "┌─────────────────────────────────────────────────────────────────┐"
    echo "│ Google OAuth Client Secret Required                             │"
    echo "├─────────────────────────────────────────────────────────────────┤"
    echo "│ Copy the client secret from Google Cloud Console                │"
    echo "└─────────────────────────────────────────────────────────────────┘"
    echo ""
    read -sp "Enter Google Client Secret: " GOOGLE_CLIENT_SECRET
    echo ""

    if [[ -z "$GOOGLE_CLIENT_SECRET" ]]; then
        echo "Error: Client Secret is required"
        exit 1
    fi

    echo "$GOOGLE_CLIENT_SECRET" > "$DEMO_DIR/client_secret.txt"
    echo ""
}

start_broker() {
    mkdir -p "$DEMO_DIR"

    if [[ ! -f "$DEMO_DIR/jwt_secret.key" ]]; then
        openssl rand -base64 32 > "$DEMO_DIR/jwt_secret.key"
    fi

    echo "Starting MQDB agent..."
    echo "  - MQTT: 0.0.0.0:1883"
    echo "  - WebSocket: 0.0.0.0:8080/mqtt"
    echo "  - HTTP/OAuth: 0.0.0.0:8081"
    echo ""

    RUST_LOG=mqdb=info,mqtt5=debug "$MQDB_BIN" agent start \
        --bind 0.0.0.0:1883 \
        --ws-bind 0.0.0.0:8080 \
        --http-bind 0.0.0.0:8081 \
        --db "$DEMO_DIR/db" \
        --jwt-algorithm hs256 \
        --jwt-key "$DEMO_DIR/jwt_secret.key" \
        --jwt-audience "$GOOGLE_CLIENT_ID" \
        --oauth-client-secret "$DEMO_DIR/client_secret.txt" \
        --oauth-frontend-redirect "http://localhost:8000" \
        --no-rate-limit \
        > "$DEMO_DIR/broker.log" 2>&1 &

    BROKER_PID=$!

    sleep 2

    if ! kill -0 "$BROKER_PID" 2>/dev/null; then
        echo "Error: Broker failed to start. Check $DEMO_DIR/broker.log"
        cat "$DEMO_DIR/broker.log"
        exit 1
    fi

    echo "Broker started (PID: $BROKER_PID)"
    echo ""
}

start_http_server() {
    cat > "$DEMO_DIR/config.json" << EOF
{
    "clientId": "$GOOGLE_CLIENT_ID",
    "brokerUrl": "ws://localhost:8080/mqtt",
    "oauthUrl": "http://localhost:8081/oauth/authorize",
    "refreshUrl": "http://localhost:8081/oauth/refresh"
}
EOF

    cd "$SCRIPT_DIR"
    CONFIG_PATH="$DEMO_DIR/config.json" python3 "$SCRIPT_DIR/server.py" 8000 &
    HTTP_PID=$!
    sleep 1
    echo "HTTP server started (PID: $HTTP_PID)"
    echo ""
}

print_instructions() {
    echo "╔═══════════════════════════════════════════════════════════════╗"
    echo "║  Ready! Open in browser:                                      ║"
    echo "║                                                               ║"
    echo "║    http://localhost:8000                                      ║"
    echo "║                                                               ║"
    echo "╠═══════════════════════════════════════════════════════════════╣"
    echo "║  Steps:                                                       ║"
    echo "║  1. Click 'Sign in with Google' (redirects to Google)         ║"
    echo "║  2. MQDB exchanges code and issues JWT                        ║"
    echo "║  3. Connect to MQTT with MQDB-issued JWT                      ║"
    echo "║  4. Edit/delete profile, view all users                       ║"
    echo "╠═══════════════════════════════════════════════════════════════╣"
    echo "║  Logs: tail -f $DEMO_DIR/broker.log"
    echo "║  Press Ctrl+C to stop                                         ║"
    echo "╚═══════════════════════════════════════════════════════════════╝"
    echo ""
}

main() {
    print_banner
    check_dependencies
    build_mqdb
    get_client_id
    get_client_secret
    start_broker
    start_http_server
    print_instructions

    while true; do
        sleep 1
        if ! kill -0 "$BROKER_PID" 2>/dev/null; then
            echo "Broker stopped unexpectedly. Check $DEMO_DIR/broker.log"
            cleanup
        fi
    done
}

main "$@"
