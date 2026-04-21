# Infrastructure Diary

Tracking AWS provisioning, configuration decisions, costs, and issues.

---

## Session 1 — 2026-04-13: Initial planning

### Decisions

1. **Laboverwire AWS account.** All resources use the `laboverwire` AWS CLI profile (region `ca-west-1`, Canada West — Calgary). Fixed a typo in `~/.aws/config` (`retion` → `region`). Terraform configs must set `profile = "laboverwire"` in the AWS provider block.

2. **Single-instance approach.** All benchmarks run on one EC2 instance with localhost communication. This matches the paper methodology (§6.1: "All experiments use localhost communication to isolate software overhead from network latency"). Multi-instance experiments are out of scope for this paper.

3. **ARM (Graviton 3) as primary target.** `c7g.xlarge` (4 vCPU, 8 GB). Rationale: compute-optimized, good price-performance, aligns with IoT edge narrative. MQDB compiles for ARM without issues. If reviewers ask for x86 comparison, re-run on `c7i.xlarge`.

4. **Dedicated tenancy considered but likely overkill.** Standard instances with 3+ triplicates and stddev reporting should be sufficient for the paper. Dedicated adds ~2× cost. Decision: start with standard, switch to dedicated only if variance is unacceptable.

5. **Docker for baselines, native for MQDB.** PostgreSQL and Mosquitto run in Docker (matching the local setup). MQDB runs as a native binary. This is the realistic deployment model — MQDB replaces Docker-based infrastructure stacks.

6. **Terraform for IaC.** Simple setup: one instance, one security group, one key pair. No need for CloudFormation complexity or Pulumi dependencies.

### Pre-requisites before provisioning

- [ ] Verify MQDB cross-compiles to aarch64-unknown-linux-gnu (or build on-instance)
- [ ] Test `run_comparison.sh` works on Linux (Ubuntu 24.04)
- [ ] Add mixed workload to benchmark harness
- [ ] Build Redis bridge
- [ ] Write Terraform configs
- [ ] Estimate full experiment runtime to plan instance hours

### Open questions

1. Build on-instance vs cross-compile? On-instance is simpler (install Rust toolchain, cargo build). Cross-compile avoids the 5-10 min build time on each provision. Decision deferred — try on-instance first.
2. S3 for results or just scp? S3 is cleaner for automation but adds IAM complexity. scp is fine for manual runs. Start with scp.
3. Ubuntu 24.04 LTS or Amazon Linux 2023? Ubuntu has better Docker support and is more familiar. Go with Ubuntu.

---

## Session 2 — 2026-04-16: SOP and guardrails in place; stack still not applied

### Context

Provenance audit of `bench/results-aws/phase1/` (see
`../PAPER-DIARY.md` Session 4 and `../../../docs/internal/vldb-benchmarks.md`
Session 1) concluded that every file in that directory has indeterminate or
known-local provenance. The Terraform stack described in Session 1 has still
never been applied — all six pre-requisites below remain unchecked. The
paper §6 has been reverted to remove MQDB (memory) content, and the directory
is now empty of JSON files.

### What changed

1. `../bench/AWS-PROCEDURE.md` exists — the only authorised path to
   populate `../bench/results-aws/`.
2. `../bench/scripts/aws_run.sh` orchestrates
   `terraform apply → rsync → build on instance → provenance → execute →
   fetch → terraform destroy`. The script is trap-guarded so a `terraform
   destroy` runs even if execution fails partway.
3. `../bench/scripts/provenance.sh` collects IMDSv2 identity, instance id,
   region, AZ, AMI, kernel, CPU model, MQDB version, git commit, and host
   fingerprint. It sets `environment=aws-ec2` only when IMDSv2 confirms
   EC2; a local run gets `environment=unknown` and exit 2.
4. `../bench/scripts/guard_aws_dir.sh` enforces the directory rule. No
   script that writes into `results-aws/` can bypass it.
5. `../bench/results-aws/phase1/` now carries `.guard`, `.gitkeep`,
   `README.md` only.

### Pre-requisites (unchanged since Session 1)

- [ ] Verify MQDB cross-compiles to aarch64-unknown-linux-gnu (or build on-instance)
- [ ] Test `run_comparison.sh` works on Linux (Ubuntu 24.04)
- [ ] Add mixed workload to benchmark harness
- [ ] Build Redis bridge
- [ ] Write Terraform configs
- [ ] Estimate full experiment runtime to plan instance hours

The SOP assumes the Terraform configs exist and that the instance image
includes Docker, Rust, and the workloads referenced by the bench harness.
None of those assumptions are yet verified on AWS.

### Next session (infra)

- [ ] Write Terraform for one `c7g.xlarge` in `ca-west-1a`, security group
      for SSH from workstation IP only, IMDSv2 required, gp3 root volume.
- [ ] Bake an AMI or let `aws_run.sh install_on_instance` handle Docker +
      Rust bootstrap.
- [ ] First SOP run: low-cost smoke test (1 triplicate, 100 operations) to
      validate the end-to-end orchestration before the full paper run.
- [ ] After smoke test passes, run full matrix (3 triplicates × 6 ops × 4
      configs plus pub/sub) and fetch into `bench/results-aws/phase1/`.
- [ ] Log the session in `../../../docs/internal/vldb-benchmarks.md`
      Session 2 with instance id, timestamps, and file list.

---

## Session 3 — 2026-04-16: Two instance types, still unapplied

### Changes

- `variables.tf` now accepts `instance_type` (default `c7g.xlarge`).
- `main.tf` uses `var.instance_type` and outputs `instance_id`, `public_dns`,
  `region`, `ami_id`.
- `aws_run.sh` iterates over `INSTANCE_TYPES="c7g.xlarge c7g.4xlarge"`,
  calling `terraform apply -var instance_type=<it>` per iteration.
- Per-instance results go into `results-aws/phase1/<instance_type>/`.
- Top-level `run-manifest.json` indexes all per-instance subdirs.

### Status

The Terraform stack has still never been applied. Infrastructure changes
validated by reading the script and confirming the iteration logic. AWS
execution is Step 4, pending cost sign-off in a separate session.

### Open items

- [x] First `terraform apply` (even as a smoke test).
- [x] Full SOP run: 5 triplicates × 10000 ops × 2 instance types.
- [x] Verify on-instance build (MQDB + baseline bridge + rest-pg).

---

## Session 4 — 2026-04-18: Full SOP run completed

### What happened

`aws_run.sh` executed end-to-end for both instance types in `ca-west-1`.
Terraform applied successfully for each iteration. On-instance build (MQDB +
baseline bridge + rest-pg) completed without issues on both ARM Graviton 3
instances.

### Instance runs

| Instance type | Instance ID | Result files |
|---|---|---|
| c7g.xlarge | i-01baf82986d018e28 | 437 |
| c7g.4xlarge | i-0d4e925db49ce5c9e | 437 |

Both instances were destroyed via `terraform destroy` after results were
fetched. Total: 874 JSON files in `results-aws/phase1/`.

### Pre-requisites resolved

- [x] Verify MQDB builds on aarch64 (built on-instance, no cross-compile)
- [x] `run_phase1_agent.sh` works on Linux (Ubuntu 24.04 on EC2)
- [x] Mixed workload in benchmark harness (concurrency sweep {1,8,32,128})
- [x] Redis bridge built and tested
- [x] Terraform configs working (parameterized `instance_type`)
- [x] Full experiment completed within budget

### Open items

- [ ] Phase 2 (cluster) — requires multi-instance Terraform (3 nodes)
