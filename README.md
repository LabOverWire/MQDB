# MQDB - Message-Oriented Reactive Database

A high-performance reactive database with native MQTT integration, point-to-point delivery, and consumer groups. Built in Rust with support for both native and WASM targets.

## Features

ACID-compliant database with atomic multi-key transactions and correlation ID dedup.

- Schemas with type validation, required fields, and default values
- Unique constraints (single and composite), not-null constraints
- Foreign keys with CASCADE, RESTRICT, and SET NULL delete policies

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
use mqdb::{Database, Filter, FilterOp};
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
use mqdb::SubscriptionMode;

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
use mqdb::{Filter, FilterOp};

let filter = Filter::new("email".into(), FilterOp::Like, json!("*@example.com"));
let results = db.list("users".into(), vec![filter], vec![], None, vec![]).await?;

let in_filter = Filter::new("status".into(), FilterOp::In, json!(["active", "pending"]));
let results = db.list("users".into(), vec![in_filter], vec![], None, vec![]).await?;
```

## Sorting and Pagination

```rust
use mqdb::{SortOrder, Pagination};

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

MQDB provides a comprehensive constraint system for maintaining data integrity with ACID guarantees.

### Schemas with Type Validation

```rust
use mqdb::{Schema, FieldDefinition, FieldType};

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
use mqdb::OnDeleteAction;

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
use mqdb::DatabaseConfig;

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
use mqdb::{Database, MqdbAgent};

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
| `$DB/{entity}/{id}` | Read entity |
| `$DB/{entity}/{id}/update` | Update entity |
| `$DB/{entity}/{id}/delete` | Delete entity |
| `$DB/{entity}/list` | List entities with filters |
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
| AdminRequired | `$DB/_admin/#`, `$DB/_oauth_tokens/#` | Requires admin user |

Entities starting with `_` (e.g., `_sessions`, `_mqtt_subs`) require admin access. Exception: `$DB/_health` is always accessible.

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
```

## Distributed Clustering

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
| FK / NOT NULL constraints | — | Not yet available in cluster mode |
| Consumer Groups | Partial | Shared subscriptions work; group tracking is local |

## CLI Tool

The `mqdb` CLI provides command-line access to a running MQDB agent.

### Installation

```bash
cargo build --release --bin mqdb
```

### Environment Variables

| Variable | Purpose | Default |
|----------|---------|---------|
| `MQDB_BROKER` | Broker address | `127.0.0.1:1883` |
| `MQDB_USER` | Username for authentication | — |
| `MQDB_PASS` | Password for authentication | — |

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
mqdb create users '{"name": "Alice", "email": "alice@example.com"}'
mqdb create users '{"id": "my-uuid", "name": "Bob", "email": "bob@example.com"}'
mqdb read users 1
mqdb update users 1 '{"name": "Alice Smith"}'
mqdb delete users 1
mqdb list users --filter "status=active" --sort "-created_at" --limit 10

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

## Testing

```bash
cargo test
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
| Metrics and tracing | Persistence-layer observability |
| TTL optimization | Expiration index to avoid full scans |
| Horizontal scaling | Partition counts beyond the current 256 |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines. All contributions require signing our [Contributor License Agreement](CLA.md).

## License

MQDB is licensed under the [GNU Affero General Public License v3.0](LICENSE).

For commercial licensing inquiries, contact contact@laboverwire.ca.
