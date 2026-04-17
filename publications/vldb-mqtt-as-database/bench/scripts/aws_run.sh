#!/usr/bin/env bash
#
# aws_run.sh — orchestrate a full AWS Phase 1 benchmark run.
#
# This is the only supported path to populate bench/results-aws/phase1/.
# See bench/AWS-PROCEDURE.md for the human-readable SOP.
#
# Steps (per instance type in INSTANCE_TYPES):
#   0. Sanity-check the local tree and AWS credentials
#   1. terraform apply -var instance_type=<it>  (bench/../infra/)
#   2. rsync bench/ to the instance (excluding results-*)
#   3. Install Docker + build MQDB release binary on the instance
#   4. Run provenance.sh on the instance to produce provenance.json
#   5. Derive MQDB_AWS_RUN_TOKEN from the IMDSv2 identity doc hash
#   6. Run run_phase1_agent.sh on the instance (single five-config sweep)
#   7. rsync /tmp/phase1/ back into bench/results-aws/phase1/<it>/
#   8. Write per-instance manifest.json under that subdir
#   9. terraform destroy (even on error)
#
# After the loop:
#  10. Write top-level run-manifest.json indexing all per-instance subdirs

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PAPER_ROOT="$(cd "$BENCH_ROOT/.." && pwd)"
MQDB_ROOT="$(cd "$PAPER_ROOT/../.." && pwd)"
INFRA_DIR="$PAPER_ROOT/infra"
RESULTS_DIR="$BENCH_ROOT/results-aws/phase1"
SSH_USER="${SSH_USER:-ubuntu}"
SSH_OPTS="${SSH_OPTS:--o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/dev/null}"
AWS_PROFILE="${AWS_PROFILE:-laboverwire}"
INSTANCE_TYPES="${INSTANCE_TYPES:-c7g.xlarge c7g.4xlarge}"
TERRAFORM="${TERRAFORM:-terraform}"

INSTANCE_UP=0
INSTANCE_ID=""
PUBLIC_DNS=""
REGION=""
AMI_ID=""
RUN_TOKEN=""
MQDB_COMMIT=""

log() { printf '[aws_run] %s\n' "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

sanity_check() {
    [ -d "$INFRA_DIR" ] || die "infra directory missing: $INFRA_DIR"
    [ -f "$RESULTS_DIR/.guard" ] || die "missing .guard at $RESULTS_DIR"
    command -v "$TERRAFORM" >/dev/null || die "terraform not installed"
    command -v rsync >/dev/null        || die "rsync not installed"
    command -v ssh >/dev/null          || die "ssh not installed"
    command -v jq >/dev/null           || die "jq not installed"
    command -v aws >/dev/null          || die "aws CLI not installed"

    if [ -z "${SKIP_GIT_CHECK:-}" ]; then
        if [ -n "$(cd "$MQDB_ROOT" && git status --porcelain)" ]; then
            die "MQDB tree is dirty; commit or stash before running the SOP"
        fi
    fi
    MQDB_COMMIT=$(cd "$MQDB_ROOT" && git rev-parse HEAD)
    log "MQDB HEAD = $MQDB_COMMIT"
}

terraform_apply() {
    local instance_type="$1"
    log "terraform apply (instance_type=$instance_type)..."
    (cd "$INFRA_DIR" && "$TERRAFORM" init -input=false -no-color >/dev/null)
    (cd "$INFRA_DIR" && "$TERRAFORM" apply -auto-approve -no-color \
        -var "instance_type=$instance_type")
    INSTANCE_ID=$(cd "$INFRA_DIR" && "$TERRAFORM" output -raw instance_id)
    PUBLIC_DNS=$(cd "$INFRA_DIR" && "$TERRAFORM" output -raw public_dns)
    REGION=$(cd "$INFRA_DIR" && "$TERRAFORM" output -raw region 2>/dev/null || echo "ca-west-1")
    AMI_ID=$(cd "$INFRA_DIR" && "$TERRAFORM" output -raw ami_id 2>/dev/null || echo "unknown")
    INSTANCE_UP=1
    log "instance_id=$INSTANCE_ID public_dns=$PUBLIC_DNS"
}

terraform_destroy() {
    if [ "$INSTANCE_UP" -eq 0 ]; then
        return 0
    fi
    log "terraform destroy..."
    (cd "$INFRA_DIR" && "$TERRAFORM" destroy -auto-approve -no-color) || \
        log "terraform destroy failed; check AWS console"
    INSTANCE_UP=0
}

on_exit() {
    terraform_destroy
}

wait_for_ssh() {
    local retries=60
    for i in $(seq 1 $retries); do
        if ssh $SSH_OPTS "$SSH_USER@$PUBLIC_DNS" true 2>/dev/null; then
            log "SSH up after $i attempts"
            return 0
        fi
        sleep 5
    done
    die "SSH never came up on $PUBLIC_DNS"
}

rsync_bench() {
    log "rsync bench/ to instance..."
    rsync -av \
        --exclude 'results-aws' \
        --exclude 'results-local' \
        --exclude 'results-unverified' \
        --exclude 'target' \
        --exclude 'results' \
        -e "ssh $SSH_OPTS" \
        "$BENCH_ROOT/" "$SSH_USER@$PUBLIC_DNS:/home/$SSH_USER/bench/"
    rsync -av \
        --exclude 'target' \
        --exclude '.git' \
        -e "ssh $SSH_OPTS" \
        "$MQDB_ROOT/" "$SSH_USER@$PUBLIC_DNS:/home/$SSH_USER/mqdb/"
}

remote() {
    ssh $SSH_OPTS "$SSH_USER@$PUBLIC_DNS" "$@"
}

install_on_instance() {
    log "installing docker + rust on instance..."
    remote "sudo apt-get update -y && \
            sudo apt-get install -y docker.io docker-compose-v2 build-essential curl jq rsync python3 && \
            sudo systemctl enable --now docker && \
            sudo usermod -aG docker $SSH_USER"
    remote "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable"
    log "building mqdb release binary..."
    remote "cd /home/$SSH_USER/mqdb && \$HOME/.cargo/bin/cargo build --release -p mqdb-cli"
    log "building baseline bridge binary..."
    remote "cd /home/$SSH_USER/bench && \$HOME/.cargo/bin/cargo build --release"
    log "building rest-pg bench binary..."
    remote "cd /home/$SSH_USER/bench/rest-pg && \$HOME/.cargo/bin/cargo build --release"
}

run_provenance() {
    log "running provenance.sh on instance..."
    remote "mkdir -p /tmp/phase1 && \
            MQDB_SRC_ROOT=/home/$SSH_USER/mqdb \
            bash /home/$SSH_USER/bench/scripts/provenance.sh /tmp/phase1/provenance.json"
    remote "test -f /tmp/phase1/provenance.json" || die "provenance.json not produced"
    local env_value
    env_value=$(remote "python3 -c 'import json;print(json.load(open(\"/tmp/phase1/provenance.json\"))[\"environment\"])'")
    [ "$env_value" = "aws-ec2" ] || die "provenance says environment=$env_value, not aws-ec2"
}

derive_token() {
    log "deriving MQDB_AWS_RUN_TOKEN on instance..."
    RUN_TOKEN=$(remote "TOKEN=\$(curl -sS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 300' http://169.254.169.254/latest/api/token) && \
                        curl -sS -H \"X-aws-ec2-metadata-token: \$TOKEN\" http://169.254.169.254/latest/dynamic/instance-identity/document | sha256sum | awk '{print \$1}'")
    [ -n "$RUN_TOKEN" ] || die "failed to derive run token"
}

execute_runs() {
    log "running phase 1 on instance..."
    remote "remote_clean_phase1() { rm -rf /tmp/phase1 && mkdir -p /tmp/phase1; }; remote_clean_phase1"
    remote "MQDB_SRC_ROOT=/home/$SSH_USER/mqdb \
            bash /home/$SSH_USER/bench/scripts/provenance.sh /tmp/phase1/provenance.json"
    remote "export RESULTS_DIR=/tmp/phase1 && \
            export PROVENANCE_JSON=/tmp/phase1/provenance.json && \
            export MQDB_AWS_RUN_TOKEN=$RUN_TOKEN && \
            cd /home/$SSH_USER/bench && \
            bash run_phase1_agent.sh"
}

fetch_results() {
    local dest="$1"
    log "fetching results into $dest..."
    mkdir -p "$dest"
    rsync -av -e "ssh $SSH_OPTS" \
        "$SSH_USER@$PUBLIC_DNS:/tmp/phase1/" \
        "$dest/"
}

write_per_instance_manifest() {
    local dest="$1"
    local instance_type="$2"
    log "writing $dest/manifest.json..."
    local utc
    utc=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    local files
    files=$(cd "$dest" && ls *.json 2>/dev/null | jq -R . | jq -s .)
    local version
    version=$(awk -F'"' '/^version = "/ { print $2; exit }' "$MQDB_ROOT/Cargo.toml")
    jq -n \
        --arg utc "$utc" \
        --arg instance_id "$INSTANCE_ID" \
        --arg instance_type "$instance_type" \
        --arg region "$REGION" \
        --arg ami_id "$AMI_ID" \
        --arg commit "$MQDB_COMMIT" \
        --arg version "$version" \
        --argjson files "$files" \
        '{
            utc_timestamp: $utc,
            instance_id: $instance_id,
            instance_type: $instance_type,
            region: $region,
            ami_id: $ami_id,
            mqdb_git_commit: $commit,
            mqdb_version: $version,
            files: $files
        }' > "$dest/manifest.json"
}

write_top_manifest() {
    log "writing $RESULTS_DIR/run-manifest.json..."
    local utc
    utc=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    local subdirs="["
    local first=1
    for it in $INSTANCE_TYPES; do
        local m="$RESULTS_DIR/$it/manifest.json"
        if [ -f "$m" ]; then
            if [ "$first" -eq 1 ]; then
                first=0
            else
                subdirs="$subdirs,"
            fi
            subdirs="$subdirs\"$it\""
        fi
    done
    subdirs="$subdirs]"
    local version
    version=$(awk -F'"' '/^version = "/ { print $2; exit }' "$MQDB_ROOT/Cargo.toml")
    jq -n \
        --arg utc "$utc" \
        --arg commit "$MQDB_COMMIT" \
        --arg version "$version" \
        --argjson subdirs "$subdirs" \
        '{
            utc_timestamp: $utc,
            mqdb_git_commit: $commit,
            mqdb_version: $version,
            instance_subdirs: $subdirs
        }' > "$RESULTS_DIR/run-manifest.json"
    log "top-level manifest at $RESULTS_DIR/run-manifest.json"
}

run_for_instance_type() {
    local instance_type="$1"
    local subdir="$RESULTS_DIR/$instance_type"
    log "=== instance type: $instance_type ==="
    terraform_apply "$instance_type"
    wait_for_ssh
    rsync_bench
    install_on_instance
    run_provenance
    derive_token
    execute_runs
    fetch_results "$subdir"
    write_per_instance_manifest "$subdir" "$instance_type"
    terraform_destroy
}

main() {
    sanity_check
    trap on_exit EXIT
    for it in $INSTANCE_TYPES; do
        run_for_instance_type "$it"
    done
    write_top_manifest
    log "done. Log this run in docs/internal/vldb-benchmarks.md."
}

main "$@"
