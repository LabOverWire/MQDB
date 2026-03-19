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

| Variable | Description | Default |
|----------|-------------|---------|
| `MQDB_BROKER` | MQTT broker address | `127.0.0.1:1883` |
| `MQDB_USER` | Authentication username | (none) |
| `MQDB_PASS` | Authentication password | (none) |

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

**Core:**

| Option | Description | Default |
|--------|-------------|---------|
| `--db` | Database directory path | (required) |
| `--bind` | MQTT listener address | `127.0.0.1:1883` |
| `--durability` | `immediate`, `periodic`, or `none` | `periodic` |
| `--durability-ms` | Fsync interval in ms (periodic mode) | `10` |
| `--ownership` | Ownership config: `entity=field` pairs | (none) |
| `--event-scope` | Scope events by entity field | (none) |
| `--passphrase-file` | File-at-rest encryption passphrase | (none) |

**Authentication:**

| Option | Description | Default |
|--------|-------------|---------|
| `--passwd` | Path to password file | (none) |
| `--acl` | Path to ACL file | (none) |
| `--scram-file` | Path to SCRAM-SHA-256 credentials file | (none) |
| `--jwt-algorithm` | JWT algorithm: `hs256`, `rs256`, `es256` | (none) |
| `--jwt-key` | Path to JWT secret/key file | (none) |
| `--jwt-issuer` | JWT issuer claim | (none) |
| `--jwt-audience` | JWT audience claim | (none) |
| `--jwt-clock-skew` | JWT clock skew tolerance in seconds | `60` |
| `--federated-jwt-config` | Path to federated JWT config JSON (conflicts with `--jwt-algorithm`) | (none) |
| `--cert-auth-file` | Path to certificate auth file | (none) |
| `--admin-users` | Comma-separated list of admin usernames | (none) |
| `--no-rate-limit` | Disable authentication rate limiting | `false` |
| `--rate-limit-max-attempts` | Max failed auth attempts before lockout | `5` |
| `--rate-limit-window-secs` | Rate limit window in seconds | `60` |
| `--rate-limit-lockout-secs` | Lockout duration in seconds | `300` |

**Transport:**

| Option | Description | Default |
|--------|-------------|---------|
| `--quic-cert` | TLS certificate (PEM) for MQTT-over-TLS | (none) |
| `--quic-key` | TLS private key (PEM) for MQTT-over-TLS | (none) |
| `--ws-bind` | WebSocket bind address (e.g. `0.0.0.0:8080`) | (none) |

**OAuth/Identity:**

| Option | Description | Default |
|--------|-------------|---------|
| `--http-bind` | HTTP server for OAuth (e.g. `0.0.0.0:8081`) | (none) |
| `--oauth-client-secret` | Path to Google OAuth client secret | (none) |
| `--oauth-redirect-uri` | OAuth redirect URI | `http://localhost:{port}/oauth/callback` |
| `--oauth-frontend-redirect` | Browser redirect after OAuth completes | (none) |
| `--ticket-expiry-secs` | Ticket JWT expiry in seconds | `30` |
| `--cookie-secure` | Set Secure flag on session cookies (HTTPS) | `false` |
| `--cors-origin` | CORS allowed origin for auth endpoints | (none) |
| `--ticket-rate-limit` | Max ticket requests per minute per IP | `10` |
| `--trust-proxy` | Trust X-Forwarded-For header | `false` |
| `--identity-key-file` | 32-byte identity encryption key file | (auto-generated) |

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
