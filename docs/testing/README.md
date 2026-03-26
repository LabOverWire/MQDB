# MQDB CLI Testing Guide

This guide provides end-to-end validation of MQDB CLI functionality through manual commands.

The guide is split into focused subdocuments for efficient reference:

| Document                                   | Description                                                                 |
| ------------------------------------------ | --------------------------------------------------------------------------- |
| [01-setup.md](01-setup.md)                 | Prerequisites, agent start, error handling, troubleshooting                 |
| [02-crud.md](02-crud.md)                   | Basic CRUD operations and output formats                                    |
| [03-queries.md](03-queries.md)             | Filtering, sorting, pagination, limits, edge cases, MQTT-only features      |
| [04-schema.md](04-schema.md)               | Schema management and constraints (unique, FK, not-null, cascade)           |
| [05-indexes.md](05-indexes.md)             | Index management, range queries, projection, backfill                       |
| [06-subscriptions.md](06-subscriptions.md) | Reactive subscriptions and consumer groups                                  |
| [07-auth.md](07-auth.md)                   | Password, SCRAM, JWT, ACL, topic protection, ownership                      |
| [08-vault.md](08-vault.md)                 | Vault encryption testing                                                    |
| [09-backup.md](09-backup.md)               | Backup and restore                                                          |
| [10-cluster.md](10-cluster.md)             | Cluster mode setup, dev commands, multi-node                                |
| [11-cluster-tests.md](11-cluster-tests.md) | Resilience, MQTT protocol, cluster constraints, DB features, scatter-gather |
| [12-benchmarks.md](12-benchmarks.md)       | Benchmarking and health endpoint                                            |
| [13-http-api.md](13-http-api.md)           | OAuth/identity, admin MQTT endpoints, advanced options                      |
| [14-checklists.md](14-checklists.md)       | Quick test, complete verification, and additions checklists                 |
| [15-license.md](15-license.md)             | License key verification and enforcement                                   |
