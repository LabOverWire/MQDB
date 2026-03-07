# Vault Cluster E2E Test

End-to-end test for vault encryption across a 3-node MQDB cluster. Verifies that per-user encryption works transparently across nodes, including cross-node reads, writes, scatter-gather list operations, passphrase changes, and vault disable.

## Prerequisites

- Rust toolchain (builds with `--features dev-insecure`)
- Python 3
- OpenSSL CLI

## Running

```bash
./examples/vault-cluster/run.sh
```

The script builds MQDB, generates test certificates, starts a 3-node cluster with QUIC transport, and runs 29 tests automatically. Exits 0 on success, 1 on any failure.

## Cluster Topology

| Node | MQTT Port | HTTP Port | Role |
|------|-----------|-----------|------|
| 1 | 18831 | 13101 | First node (Raft leader), vault owner's primary |
| 2 | 18832 | 13102 | Peer, cross-node vault operations |
| 3 | 18833 | — | Data node only (no HTTP) |

Nodes 2 and 3 join via `--peers 1@127.0.0.1:18831`. Vault operations on node 2 require cross-node forwarding when the partition primary is on a different node.

## Test Coverage

| Tests | What they prove |
|-------|----------------|
| 1-5 | Single-node vault: login, enable, create, observer sees ciphertext, owner sees plaintext |
| 6-9 | Multi-partition: create notes across partitions, scatter-gather list decrypts all, update encrypts |
| 10-13 | Lock/unlock: locked vault returns ciphertext, unlock restores plaintext |
| 14-19 | Cross-node: login on node 2, unlock vault, cross-node read/create/list all work transparently |
| 20-21 | Vault status endpoint, wrong passphrase rejection |
| 22 | Delete with vault enabled |
| 23-26 | Passphrase change: old rejected, new works, data still decrypts |
| 27-29 | Vault disable: records decrypted to plaintext, vault_enabled=false |

## Architecture

```
Node 1 (port 18831)          Node 2 (port 18832)          Node 3 (port 18833)
  HTTP :13101                  HTTP :13102
  MQTT :18831                  MQTT :18832                  MQTT :18833
  ├── vault key store          ├── vault key store
  ├── partitions [0..N]        ├── partitions [N..M]        ├── partitions [M..255]
  └── QUIC ←────────────────→  └── QUIC ←────────────────→  └── QUIC
```

When node 2 reads a record whose partition primary is node 1, the request is forwarded via QUIC. Node 1 decrypts using the vault key (forwarded during unlock) and returns plaintext.

## See Also

- `examples/vault-mqtt/` — single-node vault demo with browser frontend
