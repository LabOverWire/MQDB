# AWS Benchmark Procedure (authoritative SOP)

This is the **only** supported way to populate
`bench/results-aws/phase1/`. Every file in that directory must be produced by
the steps below, embedded with provenance, and recorded in the session diary
at `docs/internal/vldb-benchmarks.md`.

Deviation from this SOP disqualifies the results from the paper. The rule is
**NEVER LOCAL** for any number cited in §6 as AWS data.

---

## 0. Pre-conditions

- AWS CLI profile `laboverwire`, region `ca-west-1` (Canada West — Calgary)
- Terraform ≥ 1.7
- `ssh-agent` loaded with the deploy key that matches
  `publications/vldb-mqtt-as-database/infra/variables.tf`
- `rsync`, `ssh`, `jq`, `curl` installed on the workstation
- `git status --porcelain` against the MQDB tree returns **empty**
  (no uncommitted changes). The git SHA of HEAD becomes part of the
  provenance bundle.
- Decide on the MQDB crate version and confirm it matches the version in
  `Cargo.toml`.

If any of the above is missing, stop here. Do not attempt the SOP with a
dirty tree — the recorded provenance would be misleading.

---

## 1. Provision (local workstation)

```bash
cd publications/vldb-mqtt-as-database/infra
terraform init
terraform apply -auto-approve
```

Capture the outputs:

- `instance_id`
- `public_dns`
- `region` (should equal `ca-west-1`)
- `ami_id`

These become the first entries in `bench/results-aws/phase1/run-manifest.json`
(created by `aws_run.sh`).

---

## 2. Bootstrap (orchestrated by `aws_run.sh`)

The script `bench/scripts/aws_run.sh` performs all of the following; do not
run these by hand unless the script is broken and the fix has been committed.

1. `rsync` of `bench/` to the instance, excluding `results-aws`,
   `results-local`, `results-unverified`, and `target/` so the local
   result quarantines never leak onto the host.
2. Installs Docker (Ubuntu 24.04 package set), pulls:
   - `postgres:17`
   - `redis:7-alpine`
   - `eclipse-mosquitto:2`
3. Checks out MQDB at the exact commit captured in §0 and builds the
   release binary on the instance (`cargo build --release`).
4. Runs `bench/scripts/provenance.sh` on the instance. The script writes
   `/tmp/phase1/provenance.json` containing:
   - `utc_timestamp`
   - `environment` = `aws-ec2` (only if IMDSv2 confirms EC2)
   - `instance_id`, `instance_type`, `region`, `availability_zone`, `ami_id`
   - `kernel`, `cpu_model`
   - `mqdb_version`, `mqdb_git_commit`, `mqdb_git_dirty`
   - `host_fingerprint_sha256`
   If IMDSv2 cannot be reached, the script sets `environment` to `local` or
   `unknown` and exits non-zero so `aws_run.sh` aborts.

---

## 3. Execute (on the instance)

`aws_run.sh` derives `MQDB_AWS_RUN_TOKEN` from the SHA-256 of the IMDSv2
identity document and exports it into the environment used by
`run_phase1_agent.sh`. Every runner script sources
`bench/scripts/guard_aws_dir.sh`, which verifies:

1. `$MQDB_AWS_RUN_TOKEN` is set.
2. The `.guard` file exists at `results-aws/phase1/.guard`.
3. IMDSv2 (`http://169.254.169.254/latest/api/token`) is reachable.

If any check fails, the runner aborts before the first write.

Environment for the run:

```bash
export RESULTS_DIR=/tmp/phase1
export PROVENANCE_JSON=/tmp/phase1/provenance.json
export MQDB_AWS_RUN_TOKEN=<derived>
```

Then:

```bash
bash bench/run_phase1_agent.sh
```

`run_phase1_agent.sh` now drives the full five-configuration sweep in a
single pass: MQDB (fjall), Mosquitto+PG, Mosquitto+Redis, REST+PG, and
MQDB (memory). All five configs share the same instance, OS load, and
provenance object, which is the only way the §6 tables compose correctly.
The per-operation files produced are:

- `mqdb_{op}_run{N}.json` — MQDB (fjall)
- `baseline_pg_{op}_run{N}.json` — Mosquitto + PostgreSQL bridge
- `baseline_redis_{op}_run{N}.json` — Mosquitto + Redis bridge
- `rest_pg_{op}_run{N}.json` — REST + PostgreSQL
- `mqdb_mem_{op}_run{N}.json` — MQDB (memory)

Pub/sub files follow the same convention:
`mqdb_pubsub_qos{Q}_run{N}.json`, `mosquitto_pubsub_qos{Q}_run{N}.json`,
`mqdb_mem_pubsub_qos{Q}_run{N}.json`.

Each result JSON is written with the provenance block merged in. The bench
binary reads `PROVENANCE_JSON` and copies the object under a top-level
`provenance` key in its output.

---

## 4. Fetch (local workstation)

`aws_run.sh` then pulls the results back:

```bash
rsync -av ec2-user@$PUBLIC_DNS:/tmp/phase1/ \
      bench/results-aws/phase1/
```

It also writes `bench/results-aws/phase1/run-manifest.json`:

```json
{
  "utc_timestamp": "2026-04-16T12:34:56Z",
  "instance_id": "i-0abc...",
  "instance_type": "c7g.xlarge",
  "region": "ca-west-1",
  "ami_id": "ami-...",
  "mqdb_git_commit": "d33b49e",
  "mqdb_version": "0.7.2",
  "files": [
    "mqdb_insert_run1.json",
    "..."
  ]
}
```

---

## 5. Teardown

```bash
cd publications/vldb-mqtt-as-database/infra
terraform destroy -auto-approve
```

The `aws_run.sh` script wraps this step so an aborted run still tears down.

---

## 6. Record

Append a session entry to:

1. `docs/internal/vldb-benchmarks.md` — date, commit, instance id, files
   produced, anomalies.
2. `publications/vldb-mqtt-as-database/infra/DIARY.md` — provisioning notes,
   costs, any AWS-side observations.
3. `publications/vldb-mqtt-as-database/PAPER-DIARY.md` — reference the run
   and call out any numbers that changed in §6.

The paper's §6 must be updated from the fetched JSON, not from any other
source. The `run-manifest.json` is the authoritative pointer.

---

## What is NOT allowed

- Writing into `bench/results-aws/` from a macOS or Linux workstation.
- Copying files from `results-local/` or `results-unverified/` into
  `results-aws/`.
- Running `run_phase1_agent.sh` with
  `RESULTS_DIR=bench/results-aws/phase1` on a workstation.
- Editing `bench/results-aws/phase1/*.json` by hand.
- Running any subset of the five configurations outside the unified
  `run_phase1_agent.sh` sweep and mixing the output into `results-aws/`.
  All five configs must come from the same pass so their provenance
  matches.
- Removing the `.guard` file to "just this once" slip results through.

The guard script exists because these mistakes have already happened once.
