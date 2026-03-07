# Vault Encryption E2E Demo

Demonstrates MQDB's per-user vault encryption on the MQTT data path. Data is encrypted at rest with a passphrase-derived key. When the vault is unlocked, MQTT reads/writes transparently decrypt/encrypt. When locked, raw ciphertext is returned.

## Prerequisites

- Rust toolchain (builds with `--features dev-insecure`)
- Python 3
- OpenSSL CLI

## Automated Test

Runs 14 tests (24 assertions) covering the full vault lifecycle, including a second user who proves data is ciphertext at rest:

```bash
./examples/vault-mqtt/run.sh
```

The script builds MQDB, starts an agent, and runs all tests automatically. Exits 0 on success, 1 on any failure.

## Interactive Browser Demo

### 1. Start the agent

```bash
mkdir -p /tmp/vault-demo
./target/release/mqdb passwd vault-test-user -b testpass -f /tmp/vault-demo/passwd.txt
openssl rand -base64 32 > /tmp/vault-demo/jwt.key
echo "dummy" > /tmp/vault-demo/oauth-secret.txt

RUST_LOG=mqdb=info ./target/release/mqdb agent start \
    --db /tmp/vault-demo/db \
    --bind 127.0.0.1:18830 \
    --ws-bind 127.0.0.1:18083 \
    --http-bind 127.0.0.1:13000 \
    --passwd /tmp/vault-demo/passwd.txt \
    --jwt-algorithm hs256 --jwt-key /tmp/vault-demo/jwt.key \
    --jwt-issuer mqdb --jwt-audience vault-test \
    --oauth-client-secret /tmp/vault-demo/oauth-secret.txt \
    --ownership notes=userId \
    --no-rate-limit \
    --cors-origin http://localhost:8080
```

### 2. Serve the frontend

In a second terminal:

```bash
cd examples/vault-mqtt
python3 -m http.server 8080
```

### 3. Open http://localhost:8080

Walk through the panels left to right:

1. **Dev Login** — creates an HTTP session (bypasses OAuth for testing)
2. **Vault Enable** — derives an AES-256-GCM key from the passphrase, encrypts all owned records
3. **MQTT Connect** — connects via WebSocket with username/password auth
4. **Create** a note — response shows plaintext (vault is unlocked)
5. **Lock** the vault, then **Read** — response shows base64 ciphertext
6. **Unlock** the vault, then **Read** — plaintext is back

## What the Automated Test Verifies

| Test | What it proves |
|------|----------------|
| 1 | Dev-login creates session with correct canonical_id |
| 2 | Vault enable succeeds with passphrase |
| 3 | Create returns plaintext when vault is unlocked |
| 4 | **Observer user (no vault key) sees ciphertext — proves at-rest encryption** |
| 5 | Vault lock removes key from memory |
| 6 | Owner read returns ciphertext when vault is locked |
| 7 | Vault unlock restores decryption |
| 8 | Owner read returns plaintext after unlock |
| 9 | Update works transparently with vault encryption |
| 10 | **Observer still sees ciphertext after update** |
| 11 | List returns plaintext for vault owner |
| 12 | Wrong passphrase is rejected |
| 13-14 | Vault operations without a session return 401 |

## Architecture

```
Browser (index.html)
  |
  |-- fetch() with credentials --> HTTP server (port 13000)
  |     |-- POST /auth/dev-login    (session + cookie)
  |     |-- POST /vault/enable      (derive key, encrypt records)
  |     |-- POST /vault/lock        (remove key from memory)
  |     |-- POST /vault/unlock      (re-derive key)
  |     |-- POST /vault/change      (change passphrase, re-encrypt)
  |     |-- POST /vault/disable     (decrypt all, remove vault)
  |     |-- GET  /vault/status      (check vault state)
  |
  |-- mqtt.js (MQTT v5 over WebSocket) --> MQTT broker (port 18083)
        |-- x-mqtt-sender injected by broker = authenticated username
        |-- Agent handler looks up vault key by sender
        |-- Encrypt before DB write, decrypt after DB read
        |-- No key in memory = raw ciphertext returned
```

## Ports

| Port | Service |
|------|---------|
| 18830 | MQTT (TCP) |
| 18083 | MQTT (WebSocket) |
| 13000 | HTTP (OAuth/vault API) |
| 8080 | Static file server (for index.html) |
