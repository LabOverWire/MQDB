# Vault MQTT Admin E2E Test

Tests the full vault lifecycle using MQTT `$DB/_vault/*` topics (no HTTP vault API calls). All vault operations use MQTT 5.0 request-response via `mosquitto_rr`.

## Prerequisites

- Rust toolchain (builds with `--features dev-insecure`)
- Python 3
- OpenSSL CLI
- `mosquitto_rr` (from Mosquitto clients package)

## Run

```bash
./examples/vault-mqtt-admin/run.sh
```

The script builds MQDB, starts an agent, and runs 15 tests (24 assertions) automatically. Exits 0 on success, 1 on any failure.

## What it tests

| Test | Operation | What it proves |
|------|-----------|----------------|
| 1 | `$DB/_vault/status` | Status returns disabled/locked for new identity |
| 2 | `$DB/_vault/enable` | Enable derives key and encrypts all owned records |
| 3 | `$DB/_vault/status` | Status returns enabled + unlocked after enable |
| 4 | CRUD create | Create returns plaintext when vault is unlocked |
| 5 | `$DB/_vault/lock` | Lock removes key from memory |
| 6 | CRUD read | Read returns ciphertext when vault is locked |
| 7 | `$DB/_vault/unlock` | Unlock restores decryption |
| 8 | CRUD read | Read returns plaintext after unlock |
| 9 | `$DB/_vault/change` | Change passphrase re-encrypts all records |
| 10 | `$DB/_vault/unlock` | Old passphrase rejected after change |
| 11 | `$DB/_vault/unlock` | New passphrase unlocks vault |
| 12 | CRUD read | Records decrypt correctly with new key |
| 13 | `$DB/_vault/disable` | Disable decrypts all records and removes vault |
| 14 | CRUD read | Records are plaintext after disable |
| 15 | `$DB/_vault/status` | Status returns disabled after disable |

## How MQTT vault works

```
mosquitto_rr (MQTT 5.0 client)
  │
  ├── publish to $DB/_vault/enable  ─── response_topic set automatically
  │     │
  │     ▼
  │   MQTT Broker
  │     │── x-mqtt-sender injected = authenticated username
  │     │── routed to vault admin handler
  │     │── handler reads identity, derives key, batch-encrypts
  │     └── response published to response_topic
  │
  └── receive response on response_topic
```

Identity is resolved from the `x-mqtt-sender` user property that the broker injects on every PUBLISH. The broker strips any client-supplied `x-mqtt-sender` to prevent spoofing.

## Ports

| Port | Service |
|------|---------|
| 18830 | MQTT (TCP) |
| 13000 | HTTP (dev-login only — identity creation) |
