# OAuth/Identity, Admin MQTT Endpoints & Advanced Options

[Back to index](README.md)

## 26. OAuth/Identity HTTP Endpoints

The HTTP server provides OAuth 2.0 login, session management, and identity linking.
These endpoints require `--http-bind` and OAuth provider configuration.

### Prerequisites

```bash
mqdb passwd admin -b admin123 -f /tmp/oauth-test/passwd.txt
openssl rand -base64 32 > /tmp/oauth-test/jwt.key

mqdb agent start --db /tmp/oauth-test/db --bind 127.0.0.1:1883 \
    --http-bind 127.0.0.1:3000 \
    --passwd /tmp/oauth-test/passwd.txt \
    --jwt-algorithm hs256 --jwt-key /tmp/oauth-test/jwt.key \
    --oauth-client-secret /path/to/google-client-secret.json \
    --ownership notes=userId
```

### HTTP Health Check

```bash
curl -s http://127.0.0.1:3000/health
```

Expected: `{"oauth_enabled":true,"status":"ok"}`

### OAuth Authorize (Redirect)

```bash
curl -s -o /dev/null -w '%{http_code} %{redirect_url}' \
    'http://127.0.0.1:3000/oauth/authorize?provider=google'
```

Expected: HTTP 302 redirect to Google's OAuth consent page with PKCE `code_challenge`.

### OAuth Callback

The callback endpoint (`GET /oauth/callback?code=...&state=...`) is invoked by the OAuth
provider after user consent. It:
1. Exchanges the authorization code for tokens (with PKCE verifier)
2. Verifies the `id_token`
3. Creates or links an identity in `_identities`/`_identity_links`
4. Creates an HTTP session with a `Set-Cookie` header
5. Redirects to `--oauth-frontend-redirect` URI with base64-encoded user info

This endpoint cannot be tested with curl alone (requires a real OAuth provider flow).

### Token Refresh

```bash
curl -s -X POST http://127.0.0.1:3000/oauth/refresh \
    -H 'Content-Type: application/json' \
    -d '{"token":"<expired-jwt>"}'
```

Expected on success: `{"expires_in":3600,"token":"<new-jwt>"}`
Expected on invalid token: HTTP 401 `{"error":"invalid or tampered token"}`
Expected on revoked token: HTTP 401 `{"error":"token has been revoked"}`

### Session Status

```bash
curl -s -b "session=$SESSION" http://127.0.0.1:3000/auth/session
```

Expected when authenticated:
```json
{"authenticated":true,"user":{"canonical_id":"...","email":"...","name":"...","picture":null,"provider":"...","provider_sub":"..."}}
```

Expected when not authenticated: `{"authenticated":false}`

### Ticket Request (Short-lived JWT)

```bash
curl -s -b "session=$SESSION" -X POST http://127.0.0.1:3000/auth/ticket
```

Expected: `{"expires_in":30,"ticket":"<jwt>"}`

The ticket JWT has `"ticket": true` in claims and expires quickly (default 30s,
configurable via `--ticket-expiry-secs`).

Rate limited: `--ticket-rate-limit` requests per minute per IP (default: 10).

### Logout

```bash
curl -s -b "session=$SESSION" -X POST http://127.0.0.1:3000/auth/logout
```

Expected: `{"status":"logged_out"}` with `Set-Cookie` that clears the session cookie.
The session JWT's `jti` is also revoked.

### Identity Unlinking

```bash
curl -s -b "session=$SESSION" -X POST http://127.0.0.1:3000/auth/unlink \
    -H 'Content-Type: application/json' \
    -d '{"provider":"google","provider_sub":"12345"}'
```

Expected: `{"status":"unlinked"}`
Expected when only one provider linked: HTTP 400 `{"error":"cannot unlink last remaining provider"}`
Expected when link belongs to another user: HTTP 403

### Dev Login (dev-insecure feature only)

```bash
curl -s -c - -X POST http://127.0.0.1:3000/auth/dev-login \
    -H 'Content-Type: application/json' \
    -d '{"email":"test@example.com","name":"Test User"}'
```

Expected: `{"canonical_id":"...","email":"test@example.com","name":"Test User"}`
with `Set-Cookie: session=...` header.

### CORS Configuration

Use `--cors-origin http://localhost:8000` to enable credentialed CORS for:
`/auth/ticket`, `/auth/logout`, `/auth/session`, `/auth/unlink`, `/oauth/refresh`,
and all `/vault/*` endpoints.

### OAuth CLI Options Reference

| Option | Description |
|--------|-------------|
| `--http-bind` | HTTP server bind address (e.g. `0.0.0.0:3000`) |
| `--oauth-client-secret` | Path to OAuth client secret JSON file |
| `--oauth-redirect-uri` | Override OAuth callback URI |
| `--oauth-frontend-redirect` | URI to redirect browser after OAuth completes |
| `--ticket-expiry-secs` | Ticket JWT lifetime (default: 30) |
| `--cookie-secure` | Set `Secure` flag on session cookies (requires HTTPS) |
| `--cors-origin` | CORS allowed origin for auth endpoints |
| `--ticket-rate-limit` | Max ticket requests per minute per IP (default: 10) |
| `--trust-proxy` | Trust `X-Forwarded-For` for client IP (behind reverse proxy) |
| `--identity-key-file` | Path to 32-byte identity encryption key (auto-generated if omitted) |

### Verification Checklist

- [ ] `/health` returns `{"oauth_enabled":true,"status":"ok"}`
- [ ] `/oauth/authorize` redirects to provider consent page
- [ ] `/oauth/callback` creates identity and session
- [ ] `/auth/session` returns user info when session cookie present
- [ ] `/auth/session` returns `{"authenticated":false}` without cookie
- [ ] `/auth/ticket` returns short-lived JWT with `ticket: true`
- [ ] `/auth/ticket` rate limited after N requests per minute
- [ ] `/auth/logout` clears session and revokes JWT `jti`
- [ ] `/auth/unlink` removes provider link (prevents unlinking last provider)
- [ ] `/oauth/refresh` issues new JWT from expired token + stored refresh token
- [ ] CORS headers present when `--cors-origin` set

---

## 27. Email Verification Protocol

The HTTP server provides email verification via a provider-agnostic MQTT-based protocol.
These endpoints require `--email-auth` and `--http-bind`.

### Prerequisites

```bash
mqdb passwd admin -b admin123 -f /tmp/verify-test/passwd.txt
openssl rand -base64 32 > /tmp/verify-test/jwt.key

mqdb agent start --db /tmp/verify-test/db --bind 127.0.0.1:1883 \
    --http-bind 127.0.0.1:3000 \
    --passwd /tmp/verify-test/passwd.txt --admin-users admin \
    --jwt-algorithm hs256 --jwt-key /tmp/verify-test/jwt.key \
    --email-auth
```

### Register a User

```bash
SESSION=$(curl -s -c - -X POST http://127.0.0.1:3000/auth/register \
    -H 'Content-Type: application/json' \
    -d '{"email":"user@example.com","password":"testpass123","name":"Test User"}' \
    | grep session | awk '{print $NF}')
```

### Check Verification Status (Pre-Verification)

```bash
curl -s -b "session=$SESSION" http://127.0.0.1:3000/auth/verify/status
```

Expected: `{"email_verified":false,"pending_challenge":null}`

### Start Verification Challenge

```bash
curl -s -b "session=$SESSION" -X POST http://127.0.0.1:3000/auth/verify/start \
    -H 'Content-Type: application/json' \
    -d '{"method":"email"}'
```

Expected:
```json
{"challenge_id":"<uuid>","expires_in":600,"method":"email","status":"challenge_created"}
```

Save the `challenge_id` for the next step.

### Observe Challenge Notification (Verifier Side)

In a separate terminal, subscribe as admin to receive the challenge notification:

```bash
mosquitto_sub -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/_verify/challenges/email' -v -C 1
```

Expected payload contains `challenge_id`, `target` (plaintext email), `code` (6-digit), and `expires_at`.
Note the `code` for the submit step.

### Submit Verification Code

```bash
curl -s -b "session=$SESSION" -X POST http://127.0.0.1:3000/auth/verify/submit \
    -H 'Content-Type: application/json' \
    -d '{"challenge_id":"<uuid>","code":"<6-digit-code>"}'
```

Expected on correct code: `{"status":"verified"}`
Expected on wrong code: HTTP 401 `{"attempts_remaining":4,"error":"invalid code"}`

### Check Verification Status (Post-Verification)

```bash
curl -s -b "session=$SESSION" http://127.0.0.1:3000/auth/verify/status
```

Expected: `{"email_verified":true,"pending_challenge":null}`

### Rate Limiting

```bash
for i in 1 2 3 4; do
    curl -s -b "session=$SESSION" -X POST http://127.0.0.1:3000/auth/verify/start \
        -H 'Content-Type: application/json' -d '{}'
done
```

Expected: Fourth request returns HTTP 429 `{"error":"too many verification requests, try again later"}`

### Wrong Code Attempts

Start a new challenge after the rate limit window resets, then submit wrong codes:

```bash
curl -s -b "session=$SESSION" -X POST http://127.0.0.1:3000/auth/verify/submit \
    -H 'Content-Type: application/json' \
    -d '{"challenge_id":"<uuid>","code":"000000"}'
```

Expected: HTTP 401 `{"attempts_remaining":4,"error":"invalid code"}`

After 5 wrong attempts: HTTP 400 `{"error":"too many attempts"}`

### Delivery Receipt (Verifier → MQDB)

```bash
mosquitto_pub -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/_verify/receipts/<challenge_id>' \
    -m '{"challenge_id":"<uuid>","status":"delivered"}'
```

Expected: Challenge status updates to `"delivered"` (visible via verify/status endpoint).

### Verification Checklist

- [ ] `POST /auth/verify/start` creates challenge and returns challenge_id
- [ ] Challenge notification published to `$DB/_verify/challenges/{method}`
- [ ] Notification contains plaintext target, code, and expiry
- [ ] `POST /auth/verify/submit` validates code (SHA-256 comparison)
- [ ] Correct code sets challenge status to `verified` and identity `email_verified=true`
- [ ] Wrong code decrements attempts remaining
- [ ] 5 wrong attempts fails the challenge
- [ ] Expired challenge returns error on submit
- [ ] `GET /auth/verify/status` shows `email_verified` and pending challenge info
- [ ] Rate limiting enforced (3 requests/min per canonical_id)
- [ ] Delivery receipt updates challenge to `delivered`
- [ ] Failure receipt updates challenge to `failed`
- [ ] Attestation receipt (attestation mode) sets `verified` + `email_verified=true`
- [ ] Periodic cleanup expires stale challenges

---

## 28. Additional Admin MQTT Endpoints

### Entity Catalog

```bash
mosquitto_rr -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/_admin/catalog' -e 'resp/test' -W 5 -m '{}'
```

Expected: JSON with `entities` array, each entry containing:
```json
{
  "name": "users",
  "record_count": 5,
  "schema": {"entity":"users","version":1,"schema":{...}},
  "constraints": [{"name":"email_unique","type":"unique","fields":["email"]}],
  "ownership": null,
  "scope": null
}
```

Plus `server` object with `mode`, `node_id` (cluster), or `vault_enabled` (agent).

### Constraint Removal

```bash
mosquitto_rr -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/_admin/constraint/users/remove' -e 'resp/test' -W 5 \
    -m '{"name":"email_unique"}'
```

Expected: `{"data":{"message":"constraint removed"},"status":"ok"}`
Expected when not found: `{"code":404,"message":"constraint not found","status":"error"}`

### Verification Checklist

- [ ] Catalog returns all entities with schemas, constraints, record counts
- [ ] Catalog includes ownership and scope info when configured
- [ ] Constraint removal by name works
- [ ] Constraint removal returns 404 for non-existent constraint

---

## 29. Advanced Agent/Cluster Options

### WebSocket Transport

```bash
mqdb agent start --db /tmp/ws-test --bind 127.0.0.1:1883 \
    --ws-bind 0.0.0.0:8083 --passwd /tmp/passwd.txt
```

WebSocket clients connect on the `--ws-bind` port using the MQTT-over-WebSocket protocol.
Test with a WebSocket MQTT client (e.g., MQTT.js in browser).

### Storage-Level Encryption (Passphrase File)

```bash
echo 'my-storage-passphrase' > /tmp/passphrase.txt
chmod 600 /tmp/passphrase.txt

mqdb agent start --db /tmp/encrypted-db --bind 127.0.0.1:1883 \
    --passphrase-file /tmp/passphrase.txt --passwd /tmp/passwd.txt
```

This encrypts the entire storage layer with AES-256-GCM. All data at rest is encrypted.
Different from Vault encryption (which encrypts individual fields per-user).

The same passphrase file must be provided on every restart. Wrong passphrase = unreadable data.

### Event Scope

```bash
mqdb agent start --db /tmp/scope-test --bind 127.0.0.1:1883 \
    --event-scope customers=customerId --passwd /tmp/passwd.txt --admin-users admin
```

With event scope configured, change events publish to scoped topic paths:
- Without scope: `$DB/orders/events/created`
- With scope `customers=customerId`: `$DB/customers/{customerId}/orders/events/created`

Test:
```bash
# Terminal 1: Subscribe to scoped events
mosquitto_sub -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/customers/cust-1/orders/events/#' -v

# Terminal 2: Create a scoped record
mqdb create orders --data '{"customerId":"cust-1","item":"Widget","qty":5}' \
    --user admin --pass admin123
```

Expected: Terminal 1 receives event on `$DB/customers/cust-1/orders/events/created`.

### Certificate Authentication

```bash
mqdb agent start --db /tmp/cert-auth --bind 127.0.0.1:1883 \
    --cert-auth-file /path/to/cert-auth.json \
    --quic-cert server.pem --quic-key server.key
```

Certificate authentication maps client TLS certificate fields (CN, SAN) to usernames.

### Federated JWT Authentication

```bash
cat > /tmp/federated-jwt.json << 'EOF'
[
  {
    "issuer": "https://accounts.google.com",
    "jwks_uri": "https://www.googleapis.com/oauth2/v3/certs",
    "audience": "my-app-id"
  },
  {
    "issuer": "https://login.microsoftonline.com/{tenant}/v2.0",
    "jwks_uri": "https://login.microsoftonline.com/{tenant}/discovery/v2.0/keys",
    "audience": "my-app-id"
  }
]
EOF

mqdb agent start --db /tmp/fed-jwt --bind 127.0.0.1:1883 \
    --federated-jwt-config /tmp/federated-jwt.json
```

Accepts JWTs from multiple identity providers. Mutually exclusive with `--jwt-algorithm`.

### Durability Modes

```bash
# Immediate: fsync every write (safest, slowest)
mqdb agent start --db /tmp/dur-test --durability immediate --passwd /tmp/passwd.txt

# Periodic: fsync every N ms (default: 10ms)
mqdb agent start --db /tmp/dur-test --durability periodic --durability-ms 50 --passwd /tmp/passwd.txt

# None: no fsync (fastest, data loss on crash)
mqdb agent start --db /tmp/dur-test --durability none --passwd /tmp/passwd.txt
```

### Verification Checklist

- [ ] WebSocket transport accepts connections on `--ws-bind` port
- [ ] Passphrase file encrypts storage (data unreadable without it)
- [ ] Passphrase file required on restart (wrong passphrase = failure)
- [ ] Event scope publishes to `$DB/{scope_entity}/{scope_id}/{entity}/events/{op}`
- [ ] Certificate auth maps TLS client certs to usernames
- [ ] Federated JWT accepts tokens from multiple issuers
- [ ] Durability modes: `immediate` fsyncs every write, `periodic` at interval, `none` skips
