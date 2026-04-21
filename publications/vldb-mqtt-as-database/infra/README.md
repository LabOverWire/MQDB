# VLDB Paper — AWS Infrastructure

Terraform configuration for provisioning AWS resources to run the MQDB benchmark experiments on standardized, reproducible hardware.

## AWS account

All resources are provisioned under the **laboverwire** AWS account.

- **AWS CLI profile**: `laboverwire`
- **Region**: `ca-west-1` (Canada West — Calgary)

All terraform and CLI commands must use `AWS_PROFILE=laboverwire` or `--profile laboverwire`.

## Design goals

1. **Reproducible** — same hardware for every run, no noisy neighbors
2. **Self-contained** — single instance, all components on localhost (matches paper methodology)
3. **Tear-down safe** — `terraform destroy` removes everything, no lingering costs
4. **Minimal** — only what's needed for the benchmarks, nothing more

## Architecture

All experiments run on a single EC2 instance with all components communicating over localhost. This isolates software overhead from network latency, matching the paper's methodology (§6.1).

```
┌─────────────────────────────────────────────┐
│              EC2 Instance                    │
│                                             │
│  ┌──────────┐  ┌───────────┐  ┌──────────┐ │
│  │   MQDB   │  │ Mosquitto │  │ PostgreSQL│ │
│  │  Agent   │  │  (Docker) │  │  (Docker) │ │
│  │ :1883    │  │  :1884    │  │  :5433    │ │
│  └──────────┘  └─────┬─────┘  └─────┬─────┘ │
│                      │              │        │
│                ┌─────┴──────────────┘        │
│                │  Bridge Process             │
│                └────────────────────────      │
│                                             │
│  ┌──────────────────────────────────────┐   │
│  │  MQDB 3-Node Cluster (for §6.4)     │   │
│  │  :1883  :1884  :1885 (QUIC mTLS)    │   │
│  └──────────────────────────────────────┘   │
└─────────────────────────────────────────────┘
```

## Instance selection

| Candidate | vCPUs | RAM | Architecture | Rationale |
|-----------|-------|-----|-------------|-----------|
| `c7g.xlarge` | 4 | 8 GB | ARM (Graviton 3) | Compute-optimized, matches IoT edge narrative |
| `c7i.xlarge` | 4 | 8 GB | x86 (Intel) | Alternative if x86 comparison desired |
| `c7g.2xlarge` | 8 | 16 GB | ARM (Graviton 3) | If 3-node cluster needs more headroom |

Default: `c7g.xlarge`. ARM builds of MQDB are supported (Rust cross-compiles cleanly).

## Resources provisioned

- EC2 instance (standard tenancy, triplicates + stddev for variance control)
- Security group (SSH only, configurable CIDR)
- Key pair for SSH access
- EBS gp3 volume (50 GB, OS + binaries)
- User data script: installs Docker, Rust toolchain, system dependencies

## Usage

```bash
cd infra/
export AWS_PROFILE=laboverwire

# Initialize and plan
terraform init
terraform plan

# Provision
terraform apply

# Wait for user_data to finish
ssh -i ~/.ssh/id_ed25519 ubuntu@$(terraform output -raw instance_ip) \
  "tail -f /var/log/cloud-init-output.log"

# Transfer code (repo is private, scp is simplest)
scp -r ~/repos/MQDB ubuntu@$(terraform output -raw instance_ip):~/MQDB

# Build on instance
ssh -i ~/.ssh/id_ed25519 ubuntu@$(terraform output -raw instance_ip)
cd MQDB && cargo build --release
cd publications/vldb-mqtt-as-database/bench && cargo build --release

# Run benchmarks
./run_comparison.sh

# Download results
scp -r ubuntu@$(terraform output -raw instance_ip):~/MQDB/publications/vldb-mqtt-as-database/bench/results ./results-aws/

# Tear down
terraform destroy
```

## Directory structure

```
infra/
├── README.md           # this file
├── DIARY.md            # provisioning log, cost tracking, issues
├── main.tf             # EC2 instance, security group, key pair
├── variables.tf        # configurable parameters
├── outputs.tf          # instance IP, instance ID, SSH command
├── user_data.sh        # instance bootstrap script
└── .gitignore          # terraform state and cache
```

## Cost estimate

| Resource | Hourly cost | Per benchmark run (est. 2h) |
|----------|------------|----------------------------|
| c7g.xlarge | ~$0.145/hr | ~$0.29 |
| EBS gp3 50GB | ~$0.005/hr | ~$0.01 |
| **Total** | | **~$0.30 per run** |

Full experiment suite (4 phases × multiple configs × triplicates) estimated at ~$5-10 total.
