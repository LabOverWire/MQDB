# Setup, Agent Start, Error Handling & Troubleshooting

[Back to index](README.md)

## Prerequisites

### Build the CLI

```bash
cd /path/to/mqdb

# Agent-only (open-source edition — no cluster commands)
cargo build --release --bin mqdb --features agent-only

# Full build with clustering (commercial edition, default)
cargo build --release
```

Binary location: `target/release/mqdb`

### Environment Variables

All CLI flags for `mqdb agent start` and `mqdb cluster start` can be set via `MQDB_*` environment variables. CLI flags take precedence when both are set. See the [README](../../README.md#environment-variables) for the full reference.

**Client commands** (used by `create`, `read`, `list`, etc.):

| Variable | Description | Default |
|----------|-------------|---------|
| `MQDB_BROKER` | MQTT broker address | `127.0.0.1:1883` |
| `MQDB_USER` | Authentication username | (none) |
| `MQDB_PASS` | Authentication password | (none) |

**Server-side** (used by `agent start` / `cluster start`):

| Variable | Description | Default |
|----------|-------------|---------|
| `MQDB_BIND` | MQTT listener address | `127.0.0.1:1883` |
| `MQDB_DB` | Database directory path | (required) |
| `MQDB_PASSWD_FILE` | Path to password file | (none) |
| `MQDB_PASSWD` | Password file content (inline) | (none) |
| `MQDB_ACL_FILE` | Path to ACL file | (none) |
| `MQDB_ACL` | ACL file content (inline) | (none) |
| `MQDB_ADMIN_USERS` | Comma-separated admin usernames | (none) |
| `MQDB_PASSPHRASE` | Encryption passphrase (inline) | (none) |
| `MQDB_LICENSE` | License token (inline) | (none) |

For file-path flags, each `MQDB_*_FILE` variable accepts a path and the corresponding `MQDB_*` (without `_FILE`) accepts inline content. Inline takes precedence.

### Terminal Setup

Open three terminal windows:

```
Terminal 1: MQDB Agent (runs continuously)
Terminal 2: CLI commands (CRUD, admin operations)
Terminal 3: Subscriber/watcher (for reactive tests)
```

---

## 1. Starting the Agent

### Basic Start (with Authentication)

**Terminal 1:**
```bash
mqdb passwd admin -b admin123 -f ./passwd.txt
mqdb agent start --db ./data/testdb --passwd ./passwd.txt
```

**Expected output:**
```
(agent starts, listening on 127.0.0.1:1883)
```

> **Note:** The `--anonymous` flag exists but requires the `dev-insecure` build feature
> (`cargo build --release --features dev-insecure`). For production and standard testing,
> always use `--passwd` for password auth or `--scram-file` for SCRAM-SHA-256 auth.

### Check Agent Status

**Terminal 2:**
```bash
mqdb agent status
```

**Expected output:**
```json
{"data":{"mode":"agent","ready":true,"status":"healthy"},"status":"ok"}
```

The `status` command connects to the broker via MQTT and queries the `$DB/_health` endpoint.

### Start with Full Authentication Stack

```bash
mqdb agent start --db ./data/testdb --passwd ./passwd.txt --acl ./acl.txt \
    --admin-users admin
```

### Agent Start Options Reference

All options can be set via `MQDB_*` environment variables. File-path options have both a `*_FILE` env var (path) and a plain env var (inline content).

**Core:**

| Option | Env Var | Description | Default |
|--------|---------|-------------|---------|
| `--db` | `MQDB_DB` | Database directory path | (required) |
| `--bind` | `MQDB_BIND` | MQTT listener address | `127.0.0.1:1883` |
| `--durability` | `MQDB_DURABILITY` | `immediate`, `periodic`, or `none` | `periodic` |
| `--durability-ms` | `MQDB_DURABILITY_MS` | Fsync interval in ms (periodic mode) | `10` |
| `--ownership` | `MQDB_OWNERSHIP` | Ownership config: `entity=field` pairs | (none) |
| `--event-scope` | `MQDB_EVENT_SCOPE` | Scope events by entity field | (none) |
| `--passphrase-file` | `MQDB_PASSPHRASE_FILE` / `MQDB_PASSPHRASE` | Encryption passphrase | (none) |
| `--license` | `MQDB_LICENSE_FILE` / `MQDB_LICENSE` | License key | (none) |

**Authentication:**

| Option | Env Var | Description | Default |
|--------|---------|-------------|---------|
| `--passwd` | `MQDB_PASSWD_FILE` / `MQDB_PASSWD` | Password file | (none) |
| `--acl` | `MQDB_ACL_FILE` / `MQDB_ACL` | ACL file | (none) |
| `--scram-file` | `MQDB_SCRAM_FILE` / `MQDB_SCRAM` | SCRAM-SHA-256 credentials | (none) |
| `--jwt-algorithm` | `MQDB_JWT_ALGORITHM` | JWT algorithm: `hs256`, `rs256`, `es256` | (none) |
| `--jwt-key` | `MQDB_JWT_KEY_FILE` / `MQDB_JWT_KEY` | JWT secret/key | (none) |
| `--jwt-issuer` | `MQDB_JWT_ISSUER` | JWT issuer claim | (none) |
| `--jwt-audience` | `MQDB_JWT_AUDIENCE` | JWT audience claim | (none) |
| `--jwt-clock-skew` | `MQDB_JWT_CLOCK_SKEW` | JWT clock skew tolerance in seconds | `60` |
| `--federated-jwt-config` | `MQDB_FEDERATED_JWT_CONFIG_FILE` / `MQDB_FEDERATED_JWT_CONFIG` | Federated JWT config JSON | (none) |
| `--cert-auth-file` | `MQDB_CERT_AUTH_FILE` / `MQDB_CERT_AUTH` | Certificate auth file | (none) |
| `--admin-users` | `MQDB_ADMIN_USERS` | Comma-separated admin usernames | (none) |
| `--no-rate-limit` | `MQDB_NO_RATE_LIMIT` | Disable auth rate limiting | `false` |
| `--rate-limit-max-attempts` | `MQDB_RATE_LIMIT_MAX_ATTEMPTS` | Max failed auth attempts | `5` |
| `--rate-limit-window-secs` | `MQDB_RATE_LIMIT_WINDOW_SECS` | Rate limit window (seconds) | `60` |
| `--rate-limit-lockout-secs` | `MQDB_RATE_LIMIT_LOCKOUT_SECS` | Lockout duration (seconds) | `300` |

**Observability (requires `--features opentelemetry` build):**

| Option | Env Var | Description | Default |
|--------|---------|-------------|---------|
| `--otlp-endpoint` | `MQDB_OTLP_ENDPOINT` | OTLP collector endpoint | (none) |
| `--otel-service-name` | `MQDB_OTEL_SERVICE_NAME` | Service name for traces | `mqdb` |
| `--otel-sampling-ratio` | `MQDB_OTEL_SAMPLING_RATIO` | Sampling ratio 0.0-1.0 | `0.1` |

**Transport:**

| Option | Env Var | Description | Default |
|--------|---------|-------------|---------|
| `--quic-cert` | `MQDB_QUIC_CERT_FILE` / `MQDB_QUIC_CERT` | TLS certificate (PEM) | (none) |
| `--quic-key` | `MQDB_QUIC_KEY_FILE` / `MQDB_QUIC_KEY` | TLS private key (PEM) | (none) |
| `--ws-bind` | `MQDB_WS_BIND` | WebSocket bind address | (none) |

**OAuth/Identity:**

| Option | Env Var | Description | Default |
|--------|---------|-------------|---------|
| `--http-bind` | `MQDB_HTTP_BIND` | HTTP server for OAuth | (none) |
| `--oauth-client-secret` | `MQDB_OAUTH_CLIENT_SECRET_FILE` / `MQDB_OAUTH_CLIENT_SECRET` | OAuth client secret | (none) |
| `--oauth-redirect-uri` | `MQDB_OAUTH_REDIRECT_URI` | OAuth redirect URI | auto |
| `--oauth-frontend-redirect` | `MQDB_OAUTH_FRONTEND_REDIRECT` | Browser redirect after OAuth | (none) |
| `--ticket-expiry-secs` | `MQDB_TICKET_EXPIRY_SECS` | Ticket JWT expiry (seconds) | `30` |
| `--cookie-secure` | `MQDB_COOKIE_SECURE` | Secure flag on session cookies | `false` |
| `--cors-origin` | `MQDB_CORS_ORIGIN` | CORS allowed origin | (none) |
| `--ticket-rate-limit` | `MQDB_TICKET_RATE_LIMIT` | Max ticket requests/min/IP | `10` |
| `--trust-proxy` | `MQDB_TRUST_PROXY` | Trust X-Forwarded-For header | `false` |
| `--identity-key-file` | `MQDB_IDENTITY_KEY_FILE` / `MQDB_IDENTITY_KEY` | Identity encryption key | (auto) |

---

## 10. Error Handling

### Entity Not Found

```bash
mqdb read users nonexistent-id
```

**Expected error:**
```json
{"code": 404, "message": "not found: users", "status": "error"}
```

### Connection Refused

```bash
MQDB_BROKER=127.0.0.1:9999 mqdb list users
```

**Expected error:**
```
Error: ConnectionError("TCP connect failed: IO error: Connection refused (os error 61)")
```

### Invalid Filter Syntax

```bash
mqdb list users --filter 'invalid'
```

**Expected:** Empty result set (filter string without operator is silently ignored by the CLI parser).
```json
{"data": [], "status": "ok"}
```

### Constraint Violation

```bash
mqdb constraint add users --not-null email
mqdb create users --data '{"name": "No Email"}'
```

**Expected error (depends on whether schema is set):**
```json
{"code": 409, "message": "not null violation: users.email", "status": "error"}
```

> **Note:** If a schema with `"required": true` is set for this field, the schema validation
> fires first with: `schema validation failed: email - required field is missing`

---

## Troubleshooting

### Agent Won't Start

1. Check for port conflicts:
   ```bash
   lsof -i :1883
   ```

2. Verify database path is writable:
   ```bash
   mkdir -p ./data/testdb && touch ./data/testdb/.test && rm ./data/testdb/.test
   ```

3. Check for existing agent processes:
   ```bash
   mqdb dev ps
   ```

### Connection Timeout

1. Verify broker address:
   ```bash
   echo $MQDB_BROKER
   ```

2. Test broker connectivity:
   ```bash
   mqdb agent status --broker 127.0.0.1:1883
   ```

### Authentication Failures

1. Verify credentials are set:
   ```bash
   echo $MQDB_USER $MQDB_PASS
   ```

2. Check password file exists and is readable:
   ```bash
   cat passwd.txt
   ```

### Subscription Not Receiving Events

1. Verify agent is running:
   ```bash
   mqdb agent status
   ```

2. Check consumer group membership:
   ```bash
   mqdb consumer-group show <group-name>
   ```

3. Verify heartbeat is active (check agent logs)

### Foreign Key Errors

1. Verify referenced entity exists:
   ```bash
   mqdb read <referenced-entity> <referenced-id>
   ```

2. Check constraint configuration:
   ```bash
   mqdb constraint list <entity>
   ```

---

## 10b. TLS and Client Options

All client commands (`create`, `read`, `update`, `delete`, `list`, `watch`, `subscribe`,
`schema`, `constraint`, `index`, `backup`, `restore`, `consumer-group`, `agent status`,
`cluster status`, `cluster rebalance`, `bench`) support these common options:

| Option | Description |
|--------|-------------|
| `--broker` | Broker address (default: `127.0.0.1:1883`, env: `MQDB_BROKER`) |
| `--user` | Authentication username (env: `MQDB_USER`) |
| `--pass` | Authentication password (env: `MQDB_PASS`) |
| `--timeout` | Connection timeout in seconds (default: `30`) |
| `--insecure` | Skip TLS certificate verification (for self-signed certs) |

When connecting to a broker with TLS (via `--quic-cert`/`--quic-key` on the server), use
`--insecure` to accept self-signed certificates during testing:

```bash
mqdb list users --broker 127.0.0.1:1883 --insecure
```
