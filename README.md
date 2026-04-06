# MQDB - Message-Oriented Reactive Database

A high-performance reactive database with native MQTT integration, point-to-point delivery, and consumer groups. Built in Rust with support for both native and WASM targets.

MQDB is available in two editions:

- **Agent** (open-source): Standalone embedded MQTT broker with full database, authentication, vault encryption, and consumer groups.
- **Native** (commercial): Everything in Agent plus distributed clustering with Raft consensus, QUIC transport, partition rebalancing, and cross-node replication.

## Features

Per-entity atomicity with constraint enforcement and correlation ID dedup.

- Schemas with type validation, required fields, and default values
- Unique constraints (single and composite), not-null constraints
- Foreign keys with CASCADE, RESTRICT, and SET NULL delete policies
- Owner-aware cascade: cross-owned entities survive deletion with FK set to null

Secondary indexes accelerate equality lookups. Ten filter operators cover the rest.

- Multi-field sorting with offset/limit pagination
- `In`, `Like` (glob), `IsNull`, `IsNotNull` and six comparison operators
- Relationship loading with nested entity expansion

Reactive from the ground up — every write produces an observable event.

- Persistent wildcard subscriptions (`+`, `#`) that survive restarts
- Real-time event streaming over async channels
- Outbox pattern with retry, exponential backoff, and dead letter queue

Shared subscriptions distribute work across consumers.

- Broadcast (all receive), load-balanced (round-robin), and ordered (partition-sticky) modes
- Consumer group rebalancing with heartbeat-based stale detection

TTL-based expiration cleans up stale entities automatically. Pluggable storage backends — Fjall (LSM-tree) for native, in-memory for WASM, async for network storage options.

## Architecture

### Core Components

| Component | Role |
|-----------|------|
| Storage Layer | Pluggable backend (Fjall / Memory / Async) with atomic batch operations |
| Reactive Core | Subscription registry with prefix/wildcard matching and event dispatching |
| Entity Layer | JSON-to-KV translation with schema-less storage |
| Index Manager | Secondary indexes for efficient queries |
| Event Dispatcher | Mode-aware routing (Broadcast / LoadBalanced / Ordered) |
| Consumer Groups | Partition assignment with automatic rebalancing |
| Outbox | Atomic event persistence with retry and dead letter queue |
| Dedup Store | Correlation ID-based idempotency |

### Key Encoding Scheme

```
data/{entity}/{id}              → entity data
idx/{entity}/{field}/{value}/{id} → secondary index entries
sub/{subscription_id}           → subscription metadata
dedup/{correlation_id}          → cached responses for idempotency
meta/{key}                      → system metadata (sequences, etc.)
_outbox/{operation_id}          → pending events for delivery
_dead_letter/{operation_id}     → failed events after max retries
```

## Quick Start

```rust
use mqdb_agent::Database;
use mqdb_core::{Filter, FilterOp};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open("./data/mydb").await?;

    let user = json!({
        "name": "Alice",
        "email": "alice@example.com",
        "status": "active"
    });

    let created = db.create("users".into(), user).await?;
    println!("Created: {}", created);

    let id = created["id"].as_str().unwrap();
    let retrieved = db.read("users".into(), id.to_string(), vec![]).await?;
    println!("Retrieved: {}", retrieved);

    let updates = json!({"name": "Alice Smith"});
    let updated = db.update("users".into(), id.to_string(), updates).await?;
    println!("Updated: {}", updated);

    db.delete("users".into(), id.to_string()).await?;
    println!("Deleted");

    Ok(())
}
```

## Reactive Subscriptions

```rust
let mut receiver = db.event_receiver();

let sub_id = db.subscribe("users/#".into(), Some("users".into())).await?;

tokio::spawn(async move {
    while let Ok(event) = receiver.recv().await {
        println!("Change: {:?} on {}/{}", event.operation, event.entity, event.id);
        if let Some(data) = event.data {
            println!("Data: {}", data);
        }
    }
});

let user = json!({"name": "Bob", "email": "bob@example.com"});
db.create("users".into(), user).await?;
```

### Wildcard Patterns

| Pattern | Matches | Example |
|---------|---------|---------|
| `users/+` | Single level | `users/123` |
| `users/#` | Multiple levels | `users/123`, `users/123/profile` |
| `+/123` | Any entity with id `123` | `orders/123`, `users/123` |

## Point-to-Point Delivery

MQDB supports shared subscriptions for distributing events across multiple consumers.

### Subscription Modes

| Mode | Behavior |
|------|----------|
| **Broadcast** (default) | All subscribers receive all events |
| **LoadBalanced** | Round-robin distribution across consumers in a group |
| **Ordered** | Partition-based routing — same entity:id always goes to same consumer |

```rust
use mqdb_core::SubscriptionMode;

let result = db.subscribe_shared(
    "orders/#".into(),
    Some("orders".into()),
    "order-processors".into(),
    SubscriptionMode::Ordered,
).await?;

println!("Subscription ID: {}", result.id);
println!("Assigned partitions: {:?}", result.assigned_partitions);

db.heartbeat(&result.id).await?;

db.unsubscribe(&result.id).await?;
```

### Consumer Groups

Consumer groups automatically rebalance partitions when members join or leave:

```rust
let groups = db.list_consumer_groups().await;
for group in groups {
    println!("{}: {} members, {} partitions",
        group.name, group.member_count, group.total_partitions);
}

if let Some(details) = db.get_consumer_group("order-processors").await {
    for member in details.members {
        println!("  {}: partitions {:?}", member.id, member.partitions);
    }
}
```

## Secondary Indexes

```rust
db.add_index("users".into(), vec!["email".into(), "status".into()]).await;

let filter = Filter::new("status".into(), FilterOp::Eq, json!("active"));
let active_users = db.list("users".into(), vec![filter], vec![], None, vec![]).await?;

for user in active_users {
    println!("{}: {}", user["name"], user["email"]);
}
```

### Filter Operations

| Operator | Meaning | Notes |
|----------|---------|-------|
| `Eq` | Equal | |
| `Neq` | Not equal | |
| `Lt` / `Lte` | Less than (or equal) | |
| `Gt` / `Gte` | Greater than (or equal) | |
| `In` | Value in array | |
| `Like` | Glob pattern | Uses `*` wildcard |
| `IsNull` | Field is null | |
| `IsNotNull` | Field is not null | |

### Extended Filtering

```rust
use mqdb_core::{Filter, FilterOp};

let filter = Filter::new("email".into(), FilterOp::Like, json!("*@example.com"));
let results = db.list("users".into(), vec![filter], vec![], None, vec![]).await?;

let in_filter = Filter::new("status".into(), FilterOp::In, json!(["active", "pending"]));
let results = db.list("users".into(), vec![in_filter], vec![], None, vec![]).await?;
```

## Sorting and Pagination

```rust
use mqdb_core::{SortOrder, Pagination};

let sort = vec![
    SortOrder::desc("created_at".into()),
    SortOrder::asc("name".into())
];
let pagination = Pagination::new(10, 0);

let users = db.list("users".into(), vec![], sort, Some(pagination), vec![]).await?;
```

## Relationships

```rust
db.add_relationship("posts".into(), "author".into(), "users".into()).await;

let post = json!({
    "title": "Hello World",
    "author_id": "user123"
});
db.create("posts".into(), post).await?;

let post_with_author = db.read("posts".into(), "1".to_string(), vec!["author".into()]).await?;
println!("Author: {}", post_with_author["author"]["name"]);

let posts = db.list("posts".into(), vec![], vec![], None, vec!["author".into()]).await?;
```

## Constraints & Data Integrity

MQDB provides a comprehensive constraint system for maintaining data integrity.

### Schemas with Type Validation

```rust
use mqdb_core::schema::{Schema, FieldDefinition, FieldType};

let schema = Schema::new("users")
    .add_field(FieldDefinition::new("name", FieldType::String).required())
    .add_field(FieldDefinition::new("age", FieldType::Number))
    .add_field(FieldDefinition::new("status", FieldType::String).default(json!("active")));

db.add_schema(schema).await?;
```

### Unique Constraints

```rust
db.add_unique_constraint("users".into(), vec!["email".into()]).await?;

db.add_unique_constraint("posts".into(), vec!["user_id".into(), "slug".into()]).await?;
```

### Not-Null Constraints

```rust
db.add_not_null("users".into(), "email".into()).await?;
```

### Foreign Keys

```rust
use mqdb_core::OnDeleteAction;

db.add_foreign_key(
    "posts".into(),
    "author_id".into(),
    "users".into(),
    "id".into(),
    OnDeleteAction::Cascade,
).await?;
```

| Delete Policy | Behavior |
|---------------|----------|
| `OnDeleteAction::Cascade` | Automatically delete referencing entities |
| `OnDeleteAction::Restrict` | Prevent deletion if references exist |
| `OnDeleteAction::SetNull` | Set foreign key field to null |

When ownership is configured, cascade deletes are owner-aware. Entities owned by the
deleting user are deleted normally. Cross-owned entities are excluded from the cascade — their
FK field is set to null instead. If the FK field has a NotNull constraint, the entire delete
is blocked. Admin users bypass ownership and perform a full blind cascade.

### Constraint Examples

| Example | Demonstrates |
|---------|-------------|
| `constraints_01_schemas.rs` | Type validation and default values |
| `constraints_02_unique.rs` | Single and composite unique constraints |
| `constraints_03_not_null.rs` | Required field enforcement |
| `constraints_04_fk_cascade.rs` | Cascade deletion (multilevel) |
| `constraints_05_fk_restrict.rs` | Prevent deletion with references |
| `constraints_06_fk_set_null.rs` | Optional relationships |
| `constraints_07_combined.rs` | All constraints working together |

## TTL (Time-To-Live)

```rust
use mqdb_core::config::DatabaseConfig;

let config = DatabaseConfig::new("./data/mydb")
    .with_ttl_cleanup_interval(Some(60));
let db = Database::open_with_config(config).await?;

let session = json!({
    "user_id": "user123",
    "token": "abc123",
    "ttl_secs": 3600
});

db.create("sessions".into(), session).await?;
```

## MQTT Agent

MQDB includes an embedded MQTT broker that exposes database operations via MQTT topics.

### Starting the Agent

```rust
use mqdb_agent::{Database, MqdbAgent};

let db = Database::open("./data/mydb").await?;
let agent = MqdbAgent::new(db)
    .with_bind_address("0.0.0.0:1884".parse()?)
    .with_password_file("passwd.txt".into())
    .with_acl_file("acl.txt".into());

agent.run().await?;
```

### MQTT Topic Structure

#### CRUD Operations

| Topic | Action |
|-------|--------|
| `$DB/{entity}/create` | Create entity (include `"id"` in payload for client-provided ID) |
| `$DB/{entity}/{id}` | Read entity (payload: `{"projection": ["field1"]}` for partial response) |
| `$DB/{entity}/{id}/update` | Update entity |
| `$DB/{entity}/{id}/delete` | Delete entity |
| `$DB/{entity}/list` | List entities (payload: filters, sort, projection) |
| `$DB/{entity}/events/#` | Subscribe to change events |

#### Admin Operations

| Topic | Action |
|-------|--------|
| `$DB/_admin/schema/{entity}/set` | Set schema |
| `$DB/_admin/schema/{entity}/get` | Get schema |
| `$DB/_admin/constraint/{entity}/add` | Add constraint |
| `$DB/_admin/constraint/{entity}/list` | List constraints |
| `$DB/_admin/backup` | Create backup |
| `$DB/_admin/backup/list` | List backups |
| `$DB/_admin/restore` | Restore (requires restart) |
| `$DB/_admin/consumer-groups` | List consumer groups |
| `$DB/_admin/consumer-groups/{name}` | Show consumer group details |

#### Subscription Management

| Topic | Action |
|-------|--------|
| `$DB/_sub/subscribe` | Create subscription (supports shared subscriptions) |
| `$DB/_sub/{id}/heartbeat` | Send heartbeat for shared subscription |
| `$DB/_sub/{id}/unsubscribe` | Remove subscription |

### ACL Configuration

ACL rules control per-user topic access. MQDB supports direct user rules, RBAC roles, and role assignment.

```
# Direct user rules
user admin topic $DB/# permission readwrite

# Role-based access control (RBAC)
role editor topic $DB/users/# permission readwrite
role editor topic $DB/orders/# permission readwrite

role viewer topic $DB/+/list permission write
role viewer topic $DB/+/read permission write
role viewer topic $DB/# permission deny

# Assign roles to users
assign alice editor
assign bob viewer

# Wildcard rules (apply to all users)
user * topic $DB/+/events/# permission read
user * topic +/responses permission readwrite
```

Permission values: `readwrite`, `read` (subscribe only), `write` (publish only), `deny`.

### Topic Protection

MQDB enforces hardcoded protection on internal topics that cannot be overridden by ACL configuration. This prevents misconfigured ACLs from exposing internal machinery.

#### Protection Tiers

| Tier | Topics | Behavior |
|------|--------|----------|
| BlockAll | `_mqdb/#`, `$DB/_idx/#`, `$DB/_unique/#`, `$DB/_fk/#`, `$DB/_query/#`, `$DB/p+/#` | All access denied |
| ReadOnly | `$SYS/#` | Subscribe allowed, publish denied |
| AdminRequired | `$DB/_admin/#`, `$DB/_oauth_tokens/#`, `$DB/_identities/#`, `$DB/_identity_links/#` | Requires admin user |

Entities starting with `_` (e.g., `_sessions`, `_mqtt_subs`) require admin access. Exceptions: `$DB/_health`, `$DB/_vault/*`, `$DB/_verify/*`, and `$DB/_auth/*` are accessible to any authenticated user.

#### Admin User Configuration

```bash
mqdb agent start --db /path/to/db --admin-users alice,bob

mqdb cluster start --node-id 1 --db /path/to/db --admin-users alice,bob \
    --quic-cert server.pem --quic-key server.key
```

Admin users have access to `$DB/_admin/*` topics (schema, constraints, backup), `$DB/_oauth_tokens/*` topics, and internal entities (`_sessions`, `_mqtt_subs`, etc.).

#### Internal Service Bypass

Internal MQDB components authenticate using a dynamically generated, node-local service username (`mqdb-internal-<UUID>`) and password created at startup. This identity is unique to each node and is automatically registered with its local broker. Topic protection checks the authenticated user identity (not client ID) to grant these internal components unrestricted access to cluster topics. This mechanism is secure because the credentials are random, ephemeral, and node-local, preventing external clients from spoofing internal access.

#### Protection Flow

```
Client Request
     │
     ▼
┌─────────────────────────────────┐
│  Topic Protection Layer         │ ← Hardcoded blocks (cannot override)
└─────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────┐
│  ACL Layer                      │ ← User-configured permissions
└─────────────────────────────────┘
```

### Authentication

Production deployments should always configure an authentication method. Without one, the broker accepts anonymous connections. The `mqdb dev start-cluster` command handles this automatically by generating a password file with default credentials (`admin` / `admin`).

MQDB supports multiple authentication methods, configurable via agent/cluster start flags:

**Password file** (`--passwd`): Simple username:password file (Argon2 hashed via `mqdb passwd`).

**SCRAM-SHA-256** (`--scram-file`): Challenge-response authentication without transmitting passwords. Generate credentials with `mqdb scram`.

**JWT** (`--jwt-algorithm`, `--jwt-key`): Token-based authentication using the MQTT password field. Supports HS256, RS256, ES256 algorithms with optional issuer/audience validation.

**Federated JWT** (`--federated-jwt-config`): Multiple JWT issuers with per-issuer keys and validation rules, configured via a JSON file.

**Email/password** (`--email-auth`): Built-in email/password registration and login via the HTTP API. Requires `--http-bind`. Includes a provider-agnostic email verification protocol — MQDB publishes verification challenges over MQTT, and external verifier clients (email sender, SMS gateway, etc.) subscribe, deliver proof out-of-band, and report delivery receipts.

**Rate limiting** (enabled by default): Protects against brute-force attacks. Configurable via `--rate-limit-max-attempts`, `--rate-limit-window-secs`, `--rate-limit-lockout-secs`. Disable with `--no-rate-limit`.

```bash
# Agent with password + ACL auth
mqdb agent start --bind 0.0.0.0:1883 --db ./data/mydb --passwd passwd.txt --acl acl.txt

# Agent with SCRAM + ACL auth
mqdb agent start --bind 0.0.0.0:1883 --db ./data/mydb --scram-file scram.txt --acl acl.txt

# Agent with JWT auth
mqdb agent start --bind 0.0.0.0:1883 --db ./data/mydb \
    --jwt-algorithm hs256 --jwt-key secret.key --jwt-issuer myapp --acl acl.txt

# Agent with federated JWT (multiple issuers)
mqdb agent start --bind 0.0.0.0:1883 --db ./data/mydb \
    --federated-jwt-config jwt_providers.json --acl acl.txt

# Agent with email/password registration and verification
mqdb agent start --bind 0.0.0.0:1883 --db ./data/mydb \
    --http-bind 0.0.0.0:3000 --email-auth \
    --jwt-algorithm hs256 --jwt-key secret.key --passwd passwd.txt
```

### Auth HTTP API

When `--http-bind` is set, the following HTTP endpoints are available:

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/auth/register` | Register with email and password (requires `--email-auth`) |
| POST | `/auth/login` | Login with email and password (requires `--email-auth`) |
| POST | `/auth/ticket` | Exchange session cookie for a short-lived MQTT JWT |
| GET | `/auth/session` | Check current session status |
| POST | `/auth/logout` | Destroy the session |
| POST | `/auth/password/change` | Change password (requires `--email-auth`, verified email) |
| POST | `/auth/password/reset/start` | Start password reset (requires `--email-auth`, unauthenticated) |
| POST | `/auth/password/reset/submit` | Submit reset code + new password (requires `--email-auth`, unauthenticated) |
| POST | `/auth/verify/start` | Start email verification challenge (requires `--email-auth`) |
| POST | `/auth/verify/submit` | Submit verification code (requires `--email-auth`) |
| GET | `/auth/verify/status` | Check email verification state (requires `--email-auth`) |
| GET | `/oauth/authorize` | Start OAuth flow (requires OAuth provider config) |
| GET | `/oauth/callback` | OAuth callback |
| POST | `/oauth/refresh` | Refresh OAuth token |

All auth endpoints use cookie-based sessions. The `/auth/ticket` endpoint exchanges a valid session for a JWT that can be used to authenticate MQTT connections.

### Password Change & Reset MQTT API

Password change and reset are also available over MQTT 5.0 request-response for JWT-authenticated users:

| Topic | Payload | Description |
|-------|---------|-------------|
| `$DB/_auth/password/change` | `{"current_password": "...", "new_password": "..."}` | Change password (requires verified email) |
| `$DB/_auth/password/reset/start` | `{"email": "user@example.com"}` | Start password reset (sends verification code) |
| `$DB/_auth/password/reset/submit` | `{"challenge_id": "...", "code": "...", "new_password": "..."}` | Submit reset code + new password |

## Vault Encryption

Per-user transparent encryption at rest. Each user derives an AES-256-GCM key from a passphrase. When the vault is unlocked, MQTT reads and writes transparently decrypt and encrypt owned entity fields. When locked, raw ciphertext is returned. Users without the vault key always see ciphertext, proving data is encrypted at rest.

All JSON leaf values at any depth are encrypted, including strings, numbers, booleans, and nulls inside nested objects and arrays. Non-string types are serialized before encryption and restored to their original types on decryption. Keys starting with `_` (system metadata) are skipped at all depths; the ownership field and `id` are skipped at the top level only.

Vault encryption requires the `--ownership` flag on at least one entity.

### Vault HTTP API

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/vault/enable` | Derive key from passphrase, encrypt all owned records |
| POST | `/vault/unlock` | Re-derive key, resume transparent decryption |
| POST | `/vault/lock` | Remove key from memory (reads return ciphertext) |
| POST | `/vault/change` | Change passphrase (re-encrypts all records with new key) |
| POST | `/vault/disable` | Decrypt all records and remove vault key permanently |
| GET | `/vault/status` | Check vault state (`vault_enabled`, `unlocked`) |

All HTTP vault endpoints require an authenticated session (cookie-based). Enable/unlock/change/disable require a JSON body with `passphrase` (and `old_passphrase`/`new_passphrase` for change).

### Vault MQTT API

Vault operations are also available over MQTT 5.0 request-response, using the `response_topic` publish property. User identity is resolved from the `x-mqtt-sender` user property (set automatically for JWT-authenticated connections).

| Topic | Payload | Description |
|-------|---------|-------------|
| `$DB/_vault/enable` | `{"passphrase": "..."}` | Derive key, encrypt all owned records |
| `$DB/_vault/unlock` | `{"passphrase": "..."}` | Re-derive key, resume transparent decryption |
| `$DB/_vault/lock` | `{}` | Remove key from memory |
| `$DB/_vault/change` | `{"old_passphrase": "...", "new_passphrase": "..."}` | Re-encrypt all records with new key |
| `$DB/_vault/disable` | `{"passphrase": "..."}` | Decrypt all records, remove vault |
| `$DB/_vault/status` | `{}` | Check vault state |

The MQTT and HTTP vault paths share the same rate limiter for unlock attempts.

Vault works in both agent and cluster modes. In cluster mode, vault decrypt operations are forwarded to the node that owns the partition, with responses routed back transparently. MQTT vault admin topics are not supported in cluster mode.

### Example

```bash
mqdb agent start --db /tmp/vault-demo/db --bind 127.0.0.1:1883 \
    --http-bind 127.0.0.1:3000 --ws-bind 127.0.0.1:8083 \
    --passwd passwd.txt --jwt-algorithm hs256 --jwt-key jwt.key \
    --ownership notes=userId --no-rate-limit
```

See `examples/vault-mqtt/` for a single-node demo and `examples/vault-cluster/` for a multi-node E2E test.

## Observability

MQDB supports OpenTelemetry tracing via OTLP export. When enabled, every database operation (create, read, update, delete, list) and schema/constraint change produces a trace span with entity name, record ID, and operation type.

### Enabling OpenTelemetry

Build with the `opentelemetry` feature:

```bash
cargo build --release --bin mqdb --features opentelemetry
```

Start the agent with an OTLP collector endpoint:

```bash
mqdb agent start --db ./data/mydb --passwd passwd.txt \
    --otlp-endpoint http://localhost:4317 \
    --otel-service-name my-service \
    --otel-sampling-ratio 1.0
```

| Option | Description | Default |
|--------|-------------|---------|
| `--otlp-endpoint` | OTLP collector endpoint (enables tracing) | (none) |
| `--otel-service-name` | Service name in traces | `mqdb` |
| `--otel-sampling-ratio` | Sampling ratio 0.0-1.0 | `0.1` |

OTel is only active when `--otlp-endpoint` is provided. Without it, the agent behaves identically to a non-OTel build.

### Span Hierarchy

```
database_operation (entity, topic)
  └── execute_with_sender (op = create|read|update|delete|list)
        └── create / read / update / delete / list (entity, id)
```

The `database_operation` span links to the W3C `traceparent` from the incoming MQTT message, enabling end-to-end trace correlation from client through broker to database.

### Compatible Backends

Any OTLP-compatible collector works: Jaeger, Grafana Tempo, Datadog, Honeycomb, AWS X-Ray (via ADOT collector).

## Distributed Clustering (Native Edition)

> Clustering requires the `native` feature (commercial license). Agent-only builds (`--features agent-only`) do not include cluster code.

MQDB supports distributed clustering with automatic failover and partition rebalancing. The cluster distributes data across 256 fixed partitions with a configurable replication factor (RF=2 by default). Raft consensus manages cluster topology and partition ownership. All inter-node communication flows over QUIC streams with mTLS mutual authentication.

### Starting a Cluster

```bash
# Generate TLS certificates for QUIC transport (includes both serverAuth and clientAuth EKU)
./scripts/generate_test_certs.sh

# Node 1 (first node becomes Raft leader)
./target/release/mqdb cluster start --node-id 1 --bind 127.0.0.1:1883 \
    --db /tmp/mqdb-node1 \
    --quic-cert test_certs/server.pem --quic-key test_certs/server.key --quic-ca test_certs/ca.pem

# Node 2 (joins via peer)
./target/release/mqdb cluster start --node-id 2 --bind 127.0.0.1:1884 \
    --db /tmp/mqdb-node2 --peers "1@127.0.0.1:1883" \
    --quic-cert test_certs/server.pem --quic-key test_certs/server.key --quic-ca test_certs/ca.pem

# Node 3 (joins via peer)
./target/release/mqdb cluster start --node-id 3 --bind 127.0.0.1:1885 \
    --db /tmp/mqdb-node3 --peers "1@127.0.0.1:1883" \
    --quic-cert test_certs/server.pem --quic-key test_certs/server.key --quic-ca test_certs/ca.pem
```

### Cluster CLI Commands

```bash
# Start a cluster node
mqdb cluster start --node-id 1 --bind 0.0.0.0:1883 --db ./data/node1 \
    --quic-cert server.pem --quic-key server.key

# Check cluster status
mqdb cluster status

# Trigger partition rebalancing
mqdb cluster rebalance
```

### Cross-Node Pub/Sub

Subscriptions work transparently across nodes:

```bash
# Subscribe on Node 2
mosquitto_sub -h 127.0.0.1 -p 1884 -t "events/#" -i subscriber1

# Publish on Node 1 - subscriber on Node 2 receives it
mosquitto_pub -h 127.0.0.1 -p 1883 -t "events/test" -m "hello" -i publisher1
```

### Cluster Options

| Option | Description |
|--------|-------------|
| `--node-id` | Unique node ID (1-65535, required) |
| `--node-name` | Human-readable node name |
| `--bind` | MQTT listener address (default: 0.0.0.0:1883) |
| `--db` | Database directory path |
| `--peers` | Peer nodes to join (format: id@host:port) |
| `--quic-cert` | TLS certificate for QUIC transport (must have serverAuth + clientAuth EKU) |
| `--quic-key` | TLS private key for QUIC transport |
| `--quic-ca` | CA certificate for mTLS peer verification (required for mTLS) |
| `--no-quic` | Disable QUIC and cluster communication (standalone mode) |
| `--no-persist-stores` | Disable store persistence (data lost on restart) |

### Cluster Transport

MQDB uses QUIC streams for all inter-node cluster communication — direct node-to-node with multiplexed streams, built-in flow control, and mTLS mutual authentication.

Each node exposes two QUIC endpoints that share the same certificate:

| Endpoint | Port | Purpose |
|----------|------|---------|
| MQTT broker QUIC listener | `--bind` port (e.g., 1883) | MQTT clients connecting over QUIC |
| Cluster inter-node transport | `--bind` port + 100 (e.g., 1983) | Heartbeats, Raft consensus, data replication |

When `--quic-ca` is provided, the cluster transport enables mTLS: each node verifies the peer's certificate against the CA before accepting or initiating a connection. The same `--quic-cert`/`--quic-key` serves both server and client roles. Without `--quic-ca`, the cluster falls back to one-way TLS with a warning.

```bash
# Start a 3-node cluster with mTLS (QUIC transport is the default)
mqdb dev start-cluster --nodes 3 --clean
```

### Data Persistence

Cluster nodes persist all data to disk by default: sessions, topic subscriptions, retained messages, QoS 1/2 in-flight state, and all `$DB/#` entity data. Storage uses an LSM-tree backend at `{db-path}/stores/`. On startup, nodes automatically recover persisted data and rebuild routing indexes.

Use `--no-persist-stores` for testing or ephemeral deployments where data doesn't need to survive restarts.

### Cluster Mode Features

| Feature | Status | Notes |
|---------|--------|-------|
| CRUD operations | Full | create, read, update, delete, list |
| Cross-node pub/sub | Full | Automatic routing with topic index broadcast |
| TTL expiration | Full | 60-second cleanup interval |
| Schema validation | Full | Registration and enforcement across nodes |
| Authentication | Full | Password, SCRAM, JWT, federated JWT, rate limiting |
| Backups | Partial | Per-node only (no cluster-wide snapshot) |
| Unique constraints | Full | Distributed reserve-commit-release protocol across nodes |
| Foreign key constraints | Full | Distributed existence checks, owner-aware cascade/set-null with ack-wait, in-memory reverse index |
| NOT NULL constraints | — | Agent mode only; not yet available in cluster mode |
| Consumer Groups | Partial | Shared subscriptions work; group tracking is local |

## CLI Tool

The `mqdb` CLI provides command-line access to a running MQDB agent.

### Installation

```bash
# Agent-only (open-source edition)
cargo build --release --bin mqdb --features agent-only

# Full build with clustering (commercial edition, default)
cargo build --release --bin mqdb
```

### Environment Variables

Every CLI flag can be set via environment variable. This is the primary configuration method for Docker and ECS deployments. CLI flags take precedence over env vars when both are set.

**Client commands** (create, read, list, watch, etc.):

| Variable | Purpose | Default |
|----------|---------|---------|
| `MQDB_BROKER` | Broker address | `127.0.0.1:1883` |
| `MQDB_USER` | Username for authentication | — |
| `MQDB_PASS` | Password for authentication | — |

**Agent/cluster server** (`mqdb agent start`, `mqdb cluster start`):

| Variable | Purpose | Default |
|----------|---------|---------|
| `MQDB_BIND` | MQTT listener address | `127.0.0.1:1883` (agent), `0.0.0.0:1883` (cluster) |
| `MQDB_DB` | Database directory path | (required) |
| `MQDB_DURABILITY` | `immediate`, `periodic`, or `none` | `periodic` |
| `MQDB_DURABILITY_MS` | Fsync interval in ms | `10` |
| `MQDB_OWNERSHIP` | Ownership config (`entity=field` pairs) | — |
| `MQDB_EVENT_SCOPE` | Scope events by entity field | — |
| `MQDB_WS_BIND` | WebSocket bind address | — |

**Authentication:**

| Variable | Purpose | Default |
|----------|---------|---------|
| `MQDB_PASSWD_FILE` | Path to password file | — |
| `MQDB_PASSWD` | Password file content (inline) | — |
| `MQDB_ACL_FILE` | Path to ACL file | — |
| `MQDB_ACL` | ACL file content (inline) | — |
| `MQDB_SCRAM_FILE` | Path to SCRAM credentials file | — |
| `MQDB_SCRAM` | SCRAM file content (inline) | — |
| `MQDB_JWT_ALGORITHM` | JWT algorithm: `hs256`, `rs256`, `es256` | — |
| `MQDB_JWT_KEY_FILE` | Path to JWT key file | — |
| `MQDB_JWT_KEY` | JWT key content (inline) | — |
| `MQDB_JWT_ISSUER` | JWT issuer claim | — |
| `MQDB_JWT_AUDIENCE` | JWT audience claim | — |
| `MQDB_JWT_CLOCK_SKEW` | JWT clock skew tolerance (seconds) | `60` |
| `MQDB_FEDERATED_JWT_CONFIG_FILE` | Path to federated JWT config | — |
| `MQDB_FEDERATED_JWT_CONFIG` | Federated JWT config JSON (inline) | — |
| `MQDB_CERT_AUTH_FILE` | Path to certificate auth file | — |
| `MQDB_CERT_AUTH` | Certificate auth content (inline) | — |
| `MQDB_ADMIN_USERS` | Comma-separated admin usernames | — |
| `MQDB_NO_RATE_LIMIT` | Disable auth rate limiting | `false` |

**Encryption and licensing:**

| Variable | Purpose | Default |
|----------|---------|---------|
| `MQDB_PASSPHRASE_FILE` | Path to encryption passphrase file | — |
| `MQDB_PASSPHRASE` | Encryption passphrase (inline) | — |
| `MQDB_VAULT_MIN_PASSPHRASE_LENGTH` | Minimum passphrase length for vault enable/change (0 = no minimum) | `0` |
| `MQDB_LICENSE_FILE` | Path to license key file | — |
| `MQDB_LICENSE` | License token (inline) | — |

**TLS/QUIC transport:**

| Variable | Purpose | Default |
|----------|---------|---------|
| `MQDB_QUIC_CERT_FILE` | Path to TLS certificate (PEM) | — |
| `MQDB_QUIC_CERT` | TLS certificate content (inline) | — |
| `MQDB_QUIC_KEY_FILE` | Path to TLS private key (PEM) | — |
| `MQDB_QUIC_KEY` | TLS private key content (inline) | — |
| `MQDB_QUIC_CA_FILE` | Path to CA certificate (cluster only) | — |
| `MQDB_QUIC_CA` | CA certificate content (inline, cluster only) | — |

**OAuth/Identity:**

| Variable | Purpose | Default |
|----------|---------|---------|
| `MQDB_HTTP_BIND` | HTTP server for OAuth | — |
| `MQDB_OAUTH_CLIENT_SECRET_FILE` | Path to OAuth client secret file | — |
| `MQDB_OAUTH_CLIENT_SECRET` | OAuth client secret (inline) | — |
| `MQDB_OAUTH_REDIRECT_URI` | OAuth redirect URI | auto |
| `MQDB_OAUTH_FRONTEND_REDIRECT` | Browser redirect after OAuth | — |
| `MQDB_COOKIE_SECURE` | Secure flag on session cookies | `false` |
| `MQDB_CORS_ORIGIN` | CORS allowed origin | — |
| `MQDB_IDENTITY_KEY_FILE` | Path to identity encryption key | auto |
| `MQDB_IDENTITY_KEY` | Identity encryption key (inline) | auto |
| `MQDB_EMAIL_AUTH` | Enable email/password registration and login | `false` |

**Cluster-only:**

| Variable | Purpose | Default |
|----------|---------|---------|
| `MQDB_NODE_ID` | Unique node ID (1-65535) | (required) |
| `MQDB_NODE_NAME` | Human-readable node name | — |
| `MQDB_PEERS` | Peer nodes (`id@host:port,...`) | — |
| `MQDB_NO_QUIC` | Disable QUIC transport | `false` |
| `MQDB_BRIDGE_OUT` | Outgoing-only bridge direction | `false` |
| `MQDB_CLUSTER_PORT_OFFSET` | Cluster listener port offset | `100` |

**Observability:**

| Variable | Purpose | Default |
|----------|---------|---------|
| `MQDB_OTLP_ENDPOINT` | OTLP collector endpoint | — |
| `MQDB_OTEL_SERVICE_NAME` | Service name for traces | `mqdb` |
| `MQDB_OTEL_SAMPLING_RATIO` | Sampling ratio 0.0-1.0 | `0.1` |

**Inline content vs file paths:** For secrets and certificates, each `*_FILE` variable accepts a file path while the corresponding variable without `_FILE` accepts the raw content directly. The inline content variables are designed for container deployments where secrets are injected as environment variable values rather than mounted files. When both are set, the inline content takes precedence.

### Docker Deployment

All configuration is done via environment variables — no config files needed:

```bash
docker run -d \
  -e MQDB_DB=/data \
  -e MQDB_BIND=0.0.0.0:1883 \
  -e MQDB_PASSWD="admin:\$argon2id\$..." \
  -e MQDB_ADMIN_USERS=admin \
  -v mqdb-data:/data \
  -p 1883:1883 \
  mqdb:latest agent start
```

For secrets injected by ECS/Kubernetes, use the inline variables:

```bash
docker run -d \
  -e MQDB_DB=/data \
  -e MQDB_BIND=0.0.0.0:1883 \
  -e MQDB_JWT_ALGORITHM=hs256 \
  -e MQDB_JWT_KEY="$JWT_SECRET" \
  -e MQDB_PASSPHRASE="$VAULT_PASSPHRASE" \
  -e MQDB_LICENSE="$LICENSE_TOKEN" \
  -p 1883:1883 \
  mqdb:latest agent start
```

### Authentication in CLI Commands

When the broker requires authentication, every CLI command needs credentials. Pass them with `--user` and `--pass`, or set `MQDB_USER` and `MQDB_PASS` to avoid repeating them:

```bash
export MQDB_USER=admin
export MQDB_PASS=admin
```

The `mqdb dev start-cluster` command creates a password file with `admin` / `admin` automatically, so setting these two variables is enough for all development work. The command examples below omit credentials for readability.

### Commands

```bash
# Start agent with authentication
mqdb agent start --bind 0.0.0.0:1884 --db ./data/mydb --passwd passwd.txt --acl acl.txt

# CRUD operations
mqdb create users -d '{"name": "Alice", "email": "alice@example.com"}'
mqdb create users -d '{"id": "my-uuid", "name": "Bob", "email": "bob@example.com"}'
mqdb read users 1
mqdb read users 1 --projection name,email
mqdb update users 1 -d '{"name": "Alice Smith"}'
mqdb delete users 1
mqdb list users --filter "status=active" --sort "-created_at" --limit 10
mqdb list users --projection name,email

# Watch for changes
mqdb watch users

# Point-to-point subscriptions
mqdb subscribe "orders/#" --group workers --mode load-balanced
mqdb subscribe "orders/#" --group processors --mode ordered
mqdb subscribe "users/#"  # broadcast mode (default)

# Consumer group management
mqdb consumer-group list
mqdb consumer-group show workers

# Schema management
mqdb schema set users schema.json
mqdb schema get users

# Constraints
mqdb constraint add users --type unique --fields email
mqdb constraint add posts --type foreign_key --field author_id --target users --on-delete cascade
mqdb constraint list users

# Backup
mqdb backup create --name daily_backup
mqdb backup list
mqdb restore --name daily_backup

# Password management
mqdb passwd admin -b admin123 -f passwd.txt
mqdb passwd admin --delete -f passwd.txt

# SCRAM credential management
mqdb scram admin -b admin123 -f scram.txt
mqdb scram admin -b admin123 -f scram.txt -i 8192

# ACL management
mqdb acl add admin '$DB/#' readwrite -f acl.txt
mqdb acl remove admin -f acl.txt
mqdb acl role-add editor '$DB/users/#' readwrite -f acl.txt
mqdb acl role-remove editor -f acl.txt
mqdb acl assign alice editor -f acl.txt
mqdb acl unassign alice editor -f acl.txt
mqdb acl check alice '$DB/users/create' pub -f acl.txt
mqdb acl list -f acl.txt
mqdb acl user-roles alice -f acl.txt
```

### Filter Syntax

| Operator | Meaning |
|----------|---------|
| `field=value` | Equals |
| `field!=value` | Not equals |
| `field>value` / `field>=value` | Greater than (or equal) |
| `field<value` / `field<=value` | Less than (or equal) |
| `field~pattern` | Like (glob pattern) |
| `field?` | Is null |
| `field!?` | Is not null |

### Output Formats

```bash
mqdb list users --format json    # JSON output (default)
mqdb list users --format table   # Table format
mqdb list users --format csv     # CSV format
```

### Field Projection (Partial Responses)

Return only selected fields from `read` and `list` operations. The `id` field is always included.

```bash
mqdb read users abc123 --projection name,email
mqdb list users --projection name,email
mqdb list users --filter "city=NYC" --projection name
```

When a schema is defined, projection fields are validated against it. Without a schema, unknown fields are silently omitted.

Via MQTT, include `"projection"` in the request payload:

```json
{"projection": ["name", "email"]}
```

## Testing

```bash
# Run all tests (default features = native)
cargo test

# Run agent-only tests (no cluster tests)
cargo test --features agent-only

cargo test --test integration_test
```

## Examples

### Basic Usage

| Example | Demonstrates |
|---------|-------------|
| `basic_usage.rs` | Complete CRUD operations and subscriptions |
| `benchmark.rs` | Performance testing |

### Constraint Examples

| Example | Demonstrates |
|---------|-------------|
| `constraints_01_schemas.rs` | Type validation and default values |
| `constraints_02_unique.rs` | Single and composite unique constraints |
| `constraints_03_not_null.rs` | Required field enforcement |
| `constraints_04_fk_cascade.rs` | Cascade deletion (multilevel) |
| `constraints_05_fk_restrict.rs` | Prevent deletion with references |
| `constraints_06_fk_set_null.rs` | Optional relationships |
| `constraints_07_combined.rs` | All constraints working together |

### Real-World Application

The `parking_lot.rs` example implements a complete parking lot management system with 5+ entities, comprehensive constraints (CASCADE/RESTRICT/SET_NULL foreign keys), reactive events via MQTT bridge, bidirectional DB-MQTT integration, full entry/exit flows with IoT simulation, real-time status updates, and TTL-based reservation expiration.

```bash
cargo run --example basic_usage
cargo run --example benchmark --release
cargo run --example constraints_01_schemas
cargo run --example constraints_07_combined
cargo run --example parking_lot
```

## Future Enhancements

| Area | Goal |
|------|------|
| Consumer group tracking | Cluster-wide group membership (currently node-local) |
| Coordinated backup/restore | Consistent cluster-wide snapshots |
| Reactive query language | Subscribe to expressions, not just topics |
| Metrics export | Prometheus/OTLP metrics alongside traces |
| TTL optimization | Expiration index to avoid full scans |
| Horizontal scaling | Partition counts beyond the current 256 |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines. All contributions require signing our [Contributor License Agreement](CLA.md).

## License

MQDB is licensed under the [GNU Affero General Public License v3.0](LICENSE).

For commercial licensing inquiries, contact contact@laboverwire.ca.
