#!/usr/bin/env bash
#
# guard_aws_dir.sh — preflight for writes into results-aws/
#
# Every benchmark runner sources this script before creating its results
# directory. The script lets the run proceed when:
#   - RESULTS_DIR does not point inside results-aws/phase1/, OR
#   - RESULTS_DIR points inside results-aws/phase1/ AND
#       $MQDB_AWS_RUN_TOKEN is set AND
#       results-aws/phase1/.guard exists AND
#       IMDSv2 at 169.254.169.254 is reachable.
#
# It exits non-zero (and returns non-zero when sourced) otherwise. The aim
# is to make a local operator unable to accidentally populate results-aws/
# by pointing a runner at it.

set -u

guard_aws_dir() {
    local results_dir="${RESULTS_DIR:-}"
    if [ -z "$results_dir" ]; then
        echo "guard_aws_dir: RESULTS_DIR is unset" >&2
        return 1
    fi

    case "$results_dir" in
        */results-aws/*) ;;
        *)
            return 0
            ;;
    esac

    if [ -z "${MQDB_AWS_RUN_TOKEN:-}" ]; then
        echo "guard_aws_dir: refusing to write into '$results_dir'" >&2
        echo "  MQDB_AWS_RUN_TOKEN is not set." >&2
        echo "  results-aws/ may only be populated via bench/AWS-PROCEDURE.md." >&2
        return 1
    fi

    local guard_file
    guard_file="$(cd "$(dirname "$results_dir")" 2>/dev/null && pwd)/phase1/.guard"
    if [ ! -f "$guard_file" ]; then
        guard_file="$results_dir/.guard"
    fi
    if [ ! -f "$guard_file" ]; then
        echo "guard_aws_dir: sentinel .guard not found near '$results_dir'" >&2
        echo "  refusing to write; see bench/AWS-PROCEDURE.md" >&2
        return 1
    fi

    local token
    token=$(curl -sS -X PUT \
        -H "X-aws-ec2-metadata-token-ttl-seconds: 60" \
        --max-time 2 \
        http://169.254.169.254/latest/api/token 2>/dev/null || true)
    if [ -z "$token" ]; then
        echo "guard_aws_dir: IMDSv2 not reachable at 169.254.169.254" >&2
        echo "  refusing to write into results-aws/ from a non-EC2 host" >&2
        return 1
    fi

    return 0
}

if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    guard_aws_dir
    exit $?
fi
