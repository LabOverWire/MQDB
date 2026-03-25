# License Key Verification

[Back to index](README.md)

## Overview

MQDB uses ECDSA P-256 signed license tokens to gate commercial features (vault encryption, clustering). Tokens are generated externally and verified by the MQDB binary using an embedded public key.

**Feature gates:**

| Feature | Required tier | Required feature flag |
|---------|--------------|----------------------|
| Vault encryption | Pro or Enterprise | `vault` |
| Clustering | Enterprise | `cluster` |

Without a license, MQDB runs in free tier (MQTT broker + DB, no vault, no clustering).

---

## 39. Verify License File

```bash
mqdb license verify --license /path/to/license.key
```

**Expected output (valid license):**
```
Customer:   customer@example.com
Tier:       enterprise
Features:   vault, cluster
Trial:      false
Expires at: 1777049787 (unix)
Days left:  29
Status:     valid
```

**Expected output (expired license):**
```
License invalid: license expired for 'customer@example.com' — expired at 1700000000
```

**Expected output (tampered token):**
```
License invalid: invalid license signature
```

---

## 40. Agent Start with License

### Vault with Valid License (should succeed)

```bash
mqdb passwd admin -b admin -f /tmp/lic-test-passwd
mqdb agent start --db /tmp/lic-test-db --passwd /tmp/lic-test-passwd \
    --passphrase-file /tmp/vault-pass.txt --license /tmp/valid.key
```

**Expected:** Agent starts, logs license details (customer, tier, features, days remaining).

### Vault without License (should fail)

```bash
mqdb agent start --db /tmp/lic-test-db --passwd /tmp/lic-test-passwd \
    --passphrase-file /tmp/vault-pass.txt
```

**Expected error:**
```
Error: "vault encryption requires a Pro or Enterprise license (--license)"
```

### Invalid License (should warn and continue in free tier)

```bash
mqdb agent start --db /tmp/lic-test-db --passwd /tmp/lic-test-passwd \
    --license /tmp/tampered.key
```

**Expected:** Agent starts with warning: `license validation failed: ... — running in free tier`

### Invalid License + Vault (should fail)

```bash
mqdb agent start --db /tmp/lic-test-db --passwd /tmp/lic-test-passwd \
    --passphrase-file /tmp/vault-pass.txt --license /tmp/tampered.key
```

**Expected:** Falls back to free tier (invalid license), then fails enforcement:
```
Error: "vault encryption requires a Pro or Enterprise license (--license)"
```

---

## 41. Cluster Start with License

### Cluster with Valid Enterprise License (should succeed)

```bash
mqdb cluster start --node-id 1 --bind 127.0.0.1:1883 --db /tmp/lic-cluster \
    --quic-cert test_certs/server.pem --quic-key test_certs/server.key \
    --quic-ca test_certs/ca.pem --license /tmp/enterprise.key
```

**Expected:** Cluster node starts, logs license details.

### Cluster without License (should fail)

```bash
mqdb cluster start --node-id 1 --bind 127.0.0.1:1883 --db /tmp/lic-cluster \
    --quic-cert test_certs/server.pem --quic-key test_certs/server.key \
    --quic-ca test_certs/ca.pem
```

**Expected error:**
```
Error: "clustering requires an Enterprise license (--license)"
```

### Cluster with Invalid License (should hard fail)

Unlike agent mode (which warns and continues in free tier), cluster mode immediately
fails if the license is invalid:

```bash
mqdb cluster start --node-id 1 --bind 127.0.0.1:1883 --db /tmp/lic-cluster \
    --quic-cert test_certs/server.pem --quic-key test_certs/server.key \
    --quic-ca test_certs/ca.pem --license /tmp/tampered.key
```

**Expected error:**
```
Error: "license validation failed: invalid license signature"
```

### Cluster with Pro License (no cluster feature — should fail)

```bash
mqdb cluster start --node-id 1 --bind 127.0.0.1:1883 --db /tmp/lic-cluster \
    --quic-cert test_certs/server.pem --quic-key test_certs/server.key \
    --quic-ca test_certs/ca.pem --license /tmp/pro-vault-only.key
```

**Expected error:**
```
Error: "your license does not include clustering"
```

---

## 42. Token Validation Edge Cases

### Expired Token

```bash
mqdb license verify --license /tmp/expired.key
```

**Expected:** `License invalid: license expired for '...' — expired at ...`

### Tampered Signature

Modify the last character of a valid token and save to file:

```bash
mqdb license verify --license /tmp/tampered.key
```

**Expected:** Error containing `invalid signature` or `invalid signature encoding`

### Wrong Algorithm Header

A token with `alg: EdDSA` instead of `ES256`:

**Expected:** `License invalid: unsupported algorithm: EdDSA`

### Malformed Token (missing parts)

```bash
echo "not-a-token" > /tmp/bad.key
mqdb license verify --license /tmp/bad.key
```

**Expected:** `License invalid: invalid license format: expected header.payload.signature`

---

## Verification Checklist

### License Verification
- [ ] `mqdb license verify` shows valid license details
- [ ] Expired license rejected with expiry message
- [ ] Tampered token rejected with signature error
- [ ] Malformed token rejected with format error
- [ ] Wrong algorithm rejected

### Agent License Enforcement
- [ ] Agent + vault + valid license starts successfully
- [ ] Agent + vault without license fails with tier error
- [ ] Agent with invalid license warns and continues in free tier
- [ ] Agent with invalid license + vault fails enforcement

### Cluster License Enforcement
- [ ] Cluster + enterprise license starts successfully
- [ ] Cluster without license fails with tier error
- [ ] Cluster with invalid license hard fails (unlike agent which warns)
- [ ] Cluster with pro license (no cluster feature) fails
