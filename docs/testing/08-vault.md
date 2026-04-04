# Vault Encryption Testing

[Back to index](README.md)

## 25. Vault Encryption Testing

### Prerequisites

Vault requires:
- A Pro or Enterprise license with the `vault` feature (`--license /path/to/license.key`)
- Build with `dev-insecure` feature for dev-login: `cargo build --release --features dev-insecure`
- `--ownership` flag for at least one entity
- `--http-bind` for the vault HTTP API
- `--oauth-client-secret` (required when `--http-bind` is set; can be a dummy file)
- `--jwt-audience` (required for OAuth client_id)
- Authentication (`--passwd` or `--scram-file` + `--jwt-algorithm` + `--jwt-key`)

> **Note:** The MQTT username must match the `canonical_id` returned by dev-login for
> ownership to work. Use the same string as MQTT username and dev-login canonical_id.

### Agent Mode Setup

```bash
mkdir -p /tmp/vault-test
mqdb passwd vault-user -b vault-pass -f /tmp/vault-test/passwd.txt
openssl rand -base64 32 > /tmp/vault-test/jwt.key
echo "dummy" > /tmp/vault-test/oauth-secret.txt

mqdb agent start --db /tmp/vault-test/db --bind 127.0.0.1:1883 \
    --http-bind 127.0.0.1:3000 \
    --passwd /tmp/vault-test/passwd.txt --jwt-algorithm hs256 --jwt-key /tmp/vault-test/jwt.key \
    --jwt-audience vault-test --oauth-client-secret /tmp/vault-test/oauth-secret.txt \
    --ownership notes=userId --admin-users vault-user --no-rate-limit \
    --license /path/to/license.key
```

### Test 1: Login and Enable Vault

```bash
curl -s -c /tmp/vault-test/cookies.txt -X POST http://127.0.0.1:3000/auth/dev-login \
    -H 'Content-Type: application/json' \
    -d '{"email":"test@example.com","name":"Vault Tester"}'

curl -s -b /tmp/vault-test/cookies.txt -X POST http://127.0.0.1:3000/vault/enable \
    -H 'Content-Type: application/json' \
    -d '{"passphrase":"my-secret"}'
```

Expected: `{"records_encrypted":0,"status":"enabled"}`

### Test 2: Create Record (Encrypted on Write)

```bash
CANONICAL_ID=$(curl -s -b /tmp/vault-test/cookies.txt http://127.0.0.1:3000/auth/session \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['user']['canonical_id'])")

mqdb create notes --data "{\"userId\":\"$CANONICAL_ID\",\"title\":\"Secret\",\"body\":\"Confidential\"}" \
    --user vault-user --pass vault-pass
```

Expected: response shows plaintext `title` and `body` (vault transparently decrypts for the owner).

### Test 3: Verify Encryption at Rest

Read the same record from a different user (or with vault locked):

```bash
curl -s -b /tmp/vault-test/cookies.txt -X POST http://127.0.0.1:3000/vault/lock
```

Then read:

```bash
mqdb read notes <id> --user vault-user --pass vault-pass
```

Expected: `title` and `body` are base64 ciphertext (20+ characters), `id` and `userId` are plaintext.

### Test 4: Unlock and Read Plaintext

```bash
curl -s -b /tmp/vault-test/cookies.txt -X POST http://127.0.0.1:3000/vault/unlock \
    -H 'Content-Type: application/json' \
    -d '{"passphrase":"my-secret"}'
```

Then read:

```bash
mqdb read notes <id> --user vault-user --pass vault-pass
```

Expected: `title` = "Secret", `body` = "Confidential" (plaintext restored).

### Test 5: Update with Vault Enabled

```bash
mqdb update notes <id> --data '{"body":"Updated body"}' --user vault-user --pass vault-pass
```

Expected: response shows plaintext for all fields. Lock vault and re-read to verify `body` is now different ciphertext.

### Test 6: List with Vault (Scatter-Gather Decryption)

Create multiple notes, then list:

```bash
mqdb list notes --user vault-user --pass vault-pass
```

Expected: all titles and bodies in plaintext. Lock vault and list again — all should be ciphertext.

### Test 7: Change Passphrase

```bash
curl -s -b /tmp/vault-test/cookies.txt -X POST http://127.0.0.1:3000/vault/change \
    -H 'Content-Type: application/json' \
    -d '{"old_passphrase":"my-secret","new_passphrase":"new-secret"}'
```

Expected: `{"records_re_encrypted":N,"status":"changed"}` where N matches record count.

Lock, then verify old passphrase is rejected:

```bash
curl -s -b /tmp/vault-test/cookies.txt -X POST http://127.0.0.1:3000/vault/lock
curl -s -o /dev/null -w '%{http_code}' -b /tmp/vault-test/cookies.txt \
    -X POST http://127.0.0.1:3000/vault/unlock \
    -H 'Content-Type: application/json' \
    -d '{"passphrase":"my-secret"}'
```

Expected: HTTP 401.

### Test 8: Disable Vault

```bash
curl -s -b /tmp/vault-test/cookies.txt -X POST http://127.0.0.1:3000/vault/unlock \
    -H 'Content-Type: application/json' \
    -d '{"passphrase":"new-secret"}'

curl -s -b /tmp/vault-test/cookies.txt -X POST http://127.0.0.1:3000/vault/disable \
    -H 'Content-Type: application/json' \
    -d '{"passphrase":"new-secret"}'
```

Expected: `{"records_decrypted":N,"status":"disabled"}`. All records now stored as plaintext.

### Test 9: Vault Status

```bash
curl -s -b /tmp/vault-test/cookies.txt http://127.0.0.1:3000/vault/status
```

Expected after disable: `{"unlocked":false,"vault_enabled":false}`.

### Test 10: MQTT Vault Admin API

Run the automated E2E script:

```bash
./examples/vault-mqtt-admin/run.sh
```

This runs 16 tests (28 assertions) covering the full vault lifecycle over MQTT `$DB/_vault/*` topics: status, enable, lock, unlock, change passphrase, disable, short passphrase rejection (`--vault-min-passphrase-length`), and CRUD during locked/unlocked states.

### Cluster Mode Vault Testing

Use the automated E2E script:

```bash
examples/vault-cluster/run.sh
```

This runs 70 tests covering:
- Enable vault, CRUD with encryption/decryption on the primary HTTP node
- Observer (different user) always sees ciphertext
- Cross-node read/create/list via a second HTTP node
- Lock/unlock cycle with ciphertext/plaintext verification
- 4th node rebalance with vault re-enable
- Scatter-gather list decryption across 4 nodes
- Change passphrase and old-passphrase rejection
- Disable vault and verify all records decrypted

### Verification Checklist

**Agent Mode:**
- [ ] Vault enable encrypts existing records
- [ ] Create with vault returns plaintext to owner
- [ ] Locked vault read returns ciphertext
- [ ] Unlock restores plaintext reads
- [ ] Update encrypts delta fields
- [ ] List decrypts all records
- [ ] Non-string fields (number, bool, null) unchanged at all depths
- [ ] Nested object string values encrypted recursively
- [ ] Array string elements encrypted recursively
- [ ] `_`-prefixed keys skipped at all depths
- [ ] `id` and ownership field skipped at top level only (encrypted when nested)
- [ ] System fields (`_version`, etc.) never encrypted
- [ ] Change passphrase re-encrypts all records
- [ ] Old passphrase rejected after change (HTTP 401)
- [ ] Disable decrypts all records back to plaintext
- [ ] Vault status reflects current state

**MQTT Vault Admin:**
- [ ] All 6 vault topics work over MQTT request-response (`$DB/_vault/*`)
- [ ] Short passphrase rejected when `--vault-min-passphrase-length` configured
- [ ] Wrong passphrase returns error code 401
- [ ] Rate limiting returns error code 429
- [ ] All 28 E2E assertions pass (`examples/vault-mqtt-admin/run.sh`)

**Cluster Mode:**
- [ ] Cross-node vault decrypt works (read from non-primary node)
- [ ] Cross-node vault encrypt works (create from non-primary node)
- [ ] Scatter-gather list decrypts results from all partitions
- [ ] Observer without vault key sees ciphertext in cluster mode
- [ ] Vault survives node rebalance (new node joins, data accessible)
- [ ] All 70 E2E tests pass (`examples/vault-cluster/run.sh`)
