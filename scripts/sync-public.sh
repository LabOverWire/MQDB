#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
TEMPLATE_DIR="$SCRIPT_DIR/public-templates"
OUTPUT_DIR=""

usage() {
    echo "Usage: $0 --output <dir>"
    echo ""
    echo "Generate a clean agent-only repo from the private MQDB repo."
    echo "The output directory will be created (must not already exist)."
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --output) OUTPUT_DIR="$2"; shift 2 ;;
        *) usage ;;
    esac
done

if [[ -z "$OUTPUT_DIR" ]]; then
    usage
fi

if [[ -d "$OUTPUT_DIR" ]]; then
    echo "Error: output directory already exists: $OUTPUT_DIR"
    echo "Remove it first or choose a different path."
    exit 1
fi

echo "==> Copying repo tree to $OUTPUT_DIR"
rsync -a \
    --exclude '.git' \
    --exclude 'target' \
    --exclude '*.swp' \
    --exclude '.DS_Store' \
    "$REPO_DIR/" "$OUTPUT_DIR/"

echo "==> Deleting cluster-only source code"
rm -rf "$OUTPUT_DIR/src/cluster"
rm -rf "$OUTPUT_DIR/src/cluster_agent"
rm -f  "$OUTPUT_DIR/src/bin/mqdb/commands/cluster.rs"
rm -rf "$OUTPUT_DIR/src/bin/mqdb/commands/dev"
rm -rf "$OUTPUT_DIR/src/bin/mqdb/commands/dev_bench"
rm -f  "$OUTPUT_DIR/src/bin/mqdb/cli_types/dev.rs"

echo "==> Deleting cluster-only tests"
rm -f  "$OUTPUT_DIR/tests/cluster_test.rs"
rm -f  "$OUTPUT_DIR/tests/cluster_integration_test.rs"
rm -f  "$OUTPUT_DIR/tests/mqtt_cluster_test.rs"
rm -rf "$OUTPUT_DIR/tests/simulation"

echo "==> Deleting cluster-only examples"
rm -rf "$OUTPUT_DIR/examples/vault-cluster"

echo "==> Deleting cluster-only scripts"
rm -f  "$OUTPUT_DIR/scripts/generate_test_certs.sh"
rm -f  "$OUTPUT_DIR/scripts/run_matrix.sh"
rm -f  "$OUTPUT_DIR/scripts/sync-public.sh"
rm -rf "$OUTPUT_DIR/scripts/public-templates"

echo "==> Deleting cluster-only docs"
rm -f  "$OUTPUT_DIR/DISTRIBUTED_DESIGN.md"
rm -f  "$OUTPUT_DIR/COMPLETE_MATRIX_DOC.md"
rm -f  "$OUTPUT_DIR/COMPLETE_MATRIX_RESULTS.md"
rm -f  "$OUTPUT_DIR/IMPLEMENTATION_PLAN.md"
rm -f  "$OUTPUT_DIR/docs/HISTORICAL_OVERVIEW_OF_DISTRIBUTED_ARCH.md"
rm -f  "$OUTPUT_DIR/docs/RAFT_FLAPPING_BUG.md"
rm -f  "$OUTPUT_DIR/docs/RAFT_SPLIT_BRAIN_BUG.md"

echo "==> Deleting internal dev files"
rm -rf "$OUTPUT_DIR/.claude"
rm -f  "$OUTPUT_DIR/CLAUDE.md"

echo "==> Overlaying template files"
cp "$TEMPLATE_DIR/Cargo.toml"                              "$OUTPUT_DIR/Cargo.toml"
cp "$TEMPLATE_DIR/src/lib.rs"                              "$OUTPUT_DIR/src/lib.rs"
cp "$TEMPLATE_DIR/src/bin/mqdb/main.rs"                    "$OUTPUT_DIR/src/bin/mqdb/main.rs"
cp "$TEMPLATE_DIR/src/bin/mqdb/cli_types/base.rs"          "$OUTPUT_DIR/src/bin/mqdb/cli_types/base.rs"
cp "$TEMPLATE_DIR/src/bin/mqdb/cli_types/mod.rs"           "$OUTPUT_DIR/src/bin/mqdb/cli_types/mod.rs"
cp "$TEMPLATE_DIR/src/bin/mqdb/cli_types/agent.rs"         "$OUTPUT_DIR/src/bin/mqdb/cli_types/agent.rs"
cp "$TEMPLATE_DIR/src/bin/mqdb/cli_types/db.rs"            "$OUTPUT_DIR/src/bin/mqdb/cli_types/db.rs"
cp "$TEMPLATE_DIR/src/bin/mqdb/commands/mod.rs"             "$OUTPUT_DIR/src/bin/mqdb/commands/mod.rs"
cp "$TEMPLATE_DIR/src/bin/mqdb/commands/consumer.rs"        "$OUTPUT_DIR/src/bin/mqdb/commands/consumer.rs"
cp "$TEMPLATE_DIR/Makefile.toml"                            "$OUTPUT_DIR/Makefile.toml"
cp "$TEMPLATE_DIR/README.md"                                "$OUTPUT_DIR/README.md"

echo "==> Removing empty scripts directory if it exists"
rmdir "$OUTPUT_DIR/scripts" 2>/dev/null || true

echo ""
echo "Sync complete: $OUTPUT_DIR"
echo ""
echo "Verify with:"
echo "  cd $OUTPUT_DIR && cargo check && cargo test && cargo clippy -- -W clippy::pedantic"
