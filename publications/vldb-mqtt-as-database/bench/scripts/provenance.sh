#!/usr/bin/env bash
#
# provenance.sh — emit the provenance JSON for a benchmark run.
#
# Runs on the target host (typically an EC2 instance inside aws_run.sh).
# Writes the JSON object to stdout; aws_run.sh redirects it to
# /tmp/phase1/provenance.json. If the host is not an EC2 instance (no
# IMDSv2), "environment" is set to "unknown" or "local" and the script exits
# with status 2 so the orchestrator can abort a supposedly-AWS run.

set -u

MQDB_SRC_ROOT="${MQDB_SRC_ROOT:-$(cd "$(dirname "$0")/../../../.." && pwd)}"
OUTFILE="${1:-/dev/stdout}"

imdsv2_token() {
    curl -sS -X PUT \
        -H "X-aws-ec2-metadata-token-ttl-seconds: 300" \
        --max-time 2 \
        http://169.254.169.254/latest/api/token 2>/dev/null || true
}

imdsv2_get() {
    local token="$1"
    local path="$2"
    curl -sS --max-time 2 \
        -H "X-aws-ec2-metadata-token: $token" \
        "http://169.254.169.254/latest/${path}" 2>/dev/null || true
}

utc_now() {
    date -u +"%Y-%m-%dT%H:%M:%SZ"
}

git_commit() {
    (cd "$MQDB_SRC_ROOT" && git rev-parse HEAD 2>/dev/null) || echo "unknown"
}

git_dirty() {
    (cd "$MQDB_SRC_ROOT" && [ -n "$(git status --porcelain 2>/dev/null)" ]) && \
        echo "true" || echo "false"
}

mqdb_version() {
    if [ -f "$MQDB_SRC_ROOT/Cargo.toml" ]; then
        awk -F'"' '/^version = "/ { print $2; exit }' \
            "$MQDB_SRC_ROOT/Cargo.toml" 2>/dev/null || echo "unknown"
    else
        echo "unknown"
    fi
}

kernel() {
    uname -sr 2>/dev/null || echo "unknown"
}

cpu_model() {
    if [ -r /proc/cpuinfo ]; then
        awk -F': ' '/^model name/ { print $2; exit } \
                    /^Processor/   { print $2; exit } \
                    /^CPU part/    { print "ARM part " $2; exit }' \
            /proc/cpuinfo 2>/dev/null || echo "unknown"
    else
        sysctl -n machdep.cpu.brand_string 2>/dev/null || echo "unknown"
    fi
}

host_fingerprint() {
    if command -v sha256sum >/dev/null 2>&1; then
        (hostname; kernel; cpu_model) | sha256sum | awk '{print $1}'
    elif command -v shasum >/dev/null 2>&1; then
        (hostname; kernel; cpu_model) | shasum -a 256 | awk '{print $1}'
    else
        echo "unknown"
    fi
}

json_escape() {
    python3 -c 'import json, sys; print(json.dumps(sys.stdin.read().strip()))'
}

TOKEN=$(imdsv2_token)
if [ -n "$TOKEN" ]; then
    ENVIRONMENT="aws-ec2"
    INSTANCE_ID=$(imdsv2_get "$TOKEN" meta-data/instance-id)
    INSTANCE_TYPE=$(imdsv2_get "$TOKEN" meta-data/instance-type)
    REGION=$(imdsv2_get "$TOKEN" meta-data/placement/region)
    AZ=$(imdsv2_get "$TOKEN" meta-data/placement/availability-zone)
    AMI_ID=$(imdsv2_get "$TOKEN" meta-data/ami-id)
    IMDS_OK=1
else
    ENVIRONMENT="unknown"
    INSTANCE_ID=""
    INSTANCE_TYPE=""
    REGION=""
    AZ=""
    AMI_ID=""
    IMDS_OK=0
fi

UTC=$(utc_now)
GIT_COMMIT=$(git_commit)
GIT_DIRTY=$(git_dirty)
VERSION=$(mqdb_version)
KERNEL=$(kernel)
CPU=$(cpu_model)
FP=$(host_fingerprint)

python3 - "$OUTFILE" <<PYEOF
import json, sys

outfile = sys.argv[1]
obj = {
    "utc_timestamp":          "$UTC",
    "environment":            "$ENVIRONMENT",
    "instance_id":            "$INSTANCE_ID",
    "instance_type":          "$INSTANCE_TYPE",
    "region":                 "$REGION",
    "availability_zone":      "$AZ",
    "ami_id":                 "$AMI_ID",
    "kernel":                 """$KERNEL""",
    "cpu_model":              """$CPU""",
    "mqdb_version":           "$VERSION",
    "mqdb_git_commit":        "$GIT_COMMIT",
    "mqdb_git_dirty":         ($GIT_DIRTY == "true") if "$GIT_DIRTY" in ("true","false") else None,
    "host_fingerprint_sha256":"$FP"
}
text = json.dumps(obj, indent=2)
if outfile == "/dev/stdout":
    sys.stdout.write(text + "\n")
else:
    with open(outfile, "w") as f:
        f.write(text + "\n")
PYEOF

if [ "$IMDS_OK" = "0" ]; then
    exit 2
fi
exit 0
