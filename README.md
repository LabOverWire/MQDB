# MQDB - Message-Oriented Reactive Database

A high-performance reactive database with native MQTT integration, point-to-point delivery, and consumer groups. Built in Rust with support for both native and WASM targets.

## Features

- **Reactive Subscriptions**: Built-in change observation with MQTT-style wildcard patterns (`+`, `#`)
- **Point-to-Point Delivery**: Shared subscriptions with LoadBalanced and Ordered modes
- **Consumer Groups**: Partition-based message routing with automatic rebalancing
- **High Performance**: 168k+ writes/sec, 558k+ reads/sec with sub-millisecond latency
- **ACID Constraints**: Schemas, unique constraints, foreign keys with CASCADE/RESTRICT/SET NULL
- **Secondary Indexes**: Efficient equality and range indexes for fast queries
- **Extended Filter Operators**: In, Like (glob patterns), IsNull, IsNotNull
- **Sorting & Pagination**: Multi-field sorting with offset/limit pagination
- **Relationships**: Foreign key-style relationships with nested entity loading
- **TTL Expiration**: Automatic entity cleanup with reactive delete events
- **Atomic Transactions**: Multi-key atomic batches with ACID guarantees
- **Outbox Pattern**: Atomic event persistence with retry and dead letter queue
- **Persistent Subscriptions**: Subscriptions survive restarts
- **Event Streaming**: Real-time change notifications via async channels
- **Storage Abstraction**: Pluggable backends (Fjall for native, Memory for WASM)
- **WASM Support**: Run in browsers with in-memory storage
- **Idempotent Operations**: Correlation ID-based deduplication

## Architecture

### Core Components

1. **Storage Layer**: Pluggable backend (Fjall for native, Memory for WASM) with atomic batch operations
2. **Reactive Core**: Subscription registry with prefix/wildcard matching and event dispatching
3. **Entity Layer**: JSON ↔ KV translation with schema-less storage
4. **Index Manager**: Secondary indexes for efficient queries
5. **Event Dispatcher**: Mode-aware routing (Broadcast/LoadBalanced/Ordered)
6. **Consumer Groups**: Partition assignment with automatic rebalancing
7. **Outbox**: Atomic event persistence with retry and dead letter queue
8. **Dedup Store**: Correlation ID-based idempotency

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

- `users/+` - matches single level (e.g., `users/123`)
- `users/#` - matches multiple levels (e.g., `users/123`, `users/123/profile`)
- `+/123` - matches any entity with id `123`

## Point-to-Point Delivery

MQDB supports shared subscriptions for distributing events across multiple consumers.

### Subscription Modes

- **Broadcast** (default): All subscribers receive all events
- **LoadBalanced**: Round-robin distribution across consumers in a group
- **Ordered**: Partition-based routing ensuring same entity:id always goes to same consumer

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

### Supported Filter Operations

- `Eq` - equality
- `Neq` - not equal
- `Lt` - less than
- `Lte` - less than or equal
- `Gt` - greater than
- `Gte` - greater than or equal
- `In` - value in array
- `Like` - glob pattern matching with `*` wildcard
- `IsNull` - field is null
- `IsNotNull` - field is not null

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

**Delete Policies:**
- `OnDeleteAction::Cascade` - Automatically delete referencing entities
- `OnDeleteAction::Restrict` - Prevent deletion if references exist
- `OnDeleteAction::SetNull` - Set foreign key field to null

See constraint examples for detailed usage:
- `constraints_01_schemas.rs` - Type validation and default values
- `constraints_02_unique.rs` - Single and composite unique constraints
- `constraints_03_not_null.rs` - Required field enforcement
- `constraints_04_fk_cascade.rs` - Cascade deletion (multilevel)
- `constraints_05_fk_restrict.rs` - Prevent deletion with references
- `constraints_06_fk_set_null.rs` - Optional relationships
- `constraints_07_combined.rs` - All constraints working together

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

## Performance

Benchmark results (release mode on M-series Mac):

| Operation | Throughput | p50 Latency | p95 Latency | p99 Latency |
|-----------|------------|-------------|-------------|-------------|
| Writes    | 168k/s     | 0.01ms      | 0.01ms      | 0.01ms      |
| Reads     | 558k/s     | 0.00ms      | 0.00ms      | 0.01ms      |
| Updates   | 191k/s     | 0.00ms      | 0.01ms      | 0.01ms      |
| List/Scan | 91/s       | 10.96ms     | -           | -           |

Run benchmarks:
```bash
cargo run --example benchmark --release
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

**CRUD Operations:**
- `$DB/{entity}/create` - Create entity (include `"id"` in payload to use a client-provided ID)
- `$DB/{entity}/{id}` - Read entity
- `$DB/{entity}/{id}/update` - Update entity
- `$DB/{entity}/{id}/delete` - Delete entity
- `$DB/{entity}/list` - List entities with filters
- `$DB/{entity}/events/#` - Subscribe to change events

**Admin Operations:**
- `$DB/_admin/schema/{entity}/set` - Set schema
- `$DB/_admin/schema/{entity}/get` - Get schema
- `$DB/_admin/constraint/{entity}/add` - Add constraint
- `$DB/_admin/constraint/{entity}/list` - List constraints
- `$DB/_admin/backup` - Create backup
- `$DB/_admin/backup/list` - List backups
- `$DB/_admin/restore` - Restore (requires restart)
- `$DB/_admin/consumer-groups` - List consumer groups
- `$DB/_admin/consumer-groups/{name}` - Show consumer group details

**Subscription Management:**
- `$DB/_sub/subscribe` - Create subscription (supports shared subscriptions)
- `$DB/_sub/{id}/heartbeat` - Send heartbeat for shared subscription
- `$DB/_sub/{id}/unsubscribe` - Remove subscription

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

#### Internal Entity Protection

Entities starting with `_` (e.g., `_sessions`, `_mqtt_subs`) require admin access. Exception: `$DB/_health` is always accessible.

#### Admin User Configuration

```bash
# Start agent with admin users
mqdb agent start --db /path/to/db --admin-users alice,bob

# Start cluster with admin users
mqdb cluster start --node-id 1 --db /path/to/db --admin-users alice,bob \
    --quic-cert server.pem --quic-key server.key
```

Admin users can:
- Access `$DB/_admin/*` topics (schema, constraints, backup)
- Access `$DB/_oauth_tokens/*` topics
- Access internal entities (`_sessions`, `_mqtt_subs`, etc.)

#### Internal Service Bypass

Internal MQDB components authenticate using a randomly-generated service username (`mqdb-internal-<uuid>`) created at startup. This username is only known to the server itself. Topic protection checks the authenticated user identity (not client ID) to grant internal components unrestricted access to cluster topics.

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

MQDB supports multiple authentication methods, configurable via agent/cluster start flags:

**Password file** (`--passwd`): Simple username:password file (bcrypt hashed via `mqdb passwd`).

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

MQDB supports distributed clustering with automatic failover and partition rebalancing.

### Architecture

- **64 fixed partitions** with configurable replication factor (RF=2 default)
- **Raft consensus** for cluster topology and partition ownership
- **MQTT bridges** for inter-node communication
- **QUIC transport** for secure, efficient cluster traffic (recommended for production)

### Starting a Cluster

```bash
# Generate TLS certificates for QUIC transport
./scripts/generate_test_certs.sh

# Node 1 (first node becomes Raft leader)
./target/release/mqdb cluster start --node-id 1 --bind 127.0.0.1:1883 \
    --db /tmp/mqdb-node1 --quic-cert test_certs/server.pem --quic-key test_certs/server.key

# Node 2 (joins via peer)
./target/release/mqdb cluster start --node-id 2 --bind 127.0.0.1:1884 \
    --db /tmp/mqdb-node2 --peers "1@127.0.0.1:1883" \
    --quic-cert test_certs/server.pem --quic-key test_certs/server.key

# Node 3 (joins via peer)
./target/release/mqdb cluster start --node-id 3 --bind 127.0.0.1:1885 \
    --db /tmp/mqdb-node3 --peers "1@127.0.0.1:1883" \
    --quic-cert test_certs/server.pem --quic-key test_certs/server.key
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
| `--quic-cert` | TLS certificate for QUIC transport |
| `--quic-key` | TLS private key for QUIC transport |
| `--no-quic` | Disable QUIC and cluster communication (standalone mode) |
| `--no-persist-stores` | Disable store persistence (data lost on restart) |

### Cluster Transport

MQDB uses QUIC streams for all inter-node cluster communication. This provides:
- Direct node-to-node communication without broker overhead
- Multiplexed streams with built-in flow control
- TLS encryption for all cluster traffic

```bash
# Start a 3-node cluster (QUIC transport is the default)
mqdb dev start-cluster --nodes 3 --clean
```

**Performance (DB insert throughput):**

| Topology | Node 1 | Node 2 | Node 3 |
|----------|--------|--------|--------|
| Partial Mesh | 3,576 ops/s | 4,171 ops/s | 3,649 ops/s |
| Full Mesh | 3,817 ops/s | 3,971 ops/s | 3,477 ops/s |

> **Note:** MQTT bridge transport is deprecated and retained only for historical reference.
> All production deployments should use the default QUIC transport.

### Data Persistence

Cluster nodes persist all data to disk by default:

- **Sessions**: MQTT client session state
- **Subscriptions**: Topic subscriptions per client
- **Retained messages**: Messages with retain flag
- **QoS state**: In-flight QoS 1/2 message tracking
- **Database records**: All `$DB/#` entity data

Data is stored in `{db-path}/stores/` using an LSM-tree backend. On startup, nodes automatically recover persisted data and rebuild routing indexes.

Use `--no-persist-stores` for testing or ephemeral deployments where data doesn't need to survive restarts.

### Cluster Mode Features

**Fully Supported:**
- CRUD operations (create, read, update, delete, list)
- Cross-node pub/sub with automatic routing
- TTL expiration (60-second cleanup interval)
- Schema validation and registration
- Authentication (password file, SCRAM-SHA-256, JWT, federated JWT, rate limiting; auto-generated internal credentials for inter-node traffic)

**Limited Support:**
- **Backups**: Creates per-node backup only (not cluster-wide snapshot)
- **Constraints**: Unique constraints fully supported; foreign key and NOT NULL constraints not yet available in cluster mode
- **Consumer Groups**: Returns empty list (shared subscriptions work, but group tracking is local)

## CLI Tool

The `mqdb` CLI provides command-line access to a running MQDB agent.

### Installation

```bash
cargo build --release --bin mqdb
```

### Environment Variables

- `MQDB_BROKER` - Broker address (default: `127.0.0.1:1883`)
- `MQDB_USER` - Username for authentication
- `MQDB_PASS` - Password for authentication

### Commands

```bash
# Start agent (with optional auth)
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

- `field=value` - Equals
- `field!=value` - Not equals
- `field>value` - Greater than
- `field<value` - Less than
- `field>=value` - Greater than or equal
- `field<=value` - Less than or equal
- `field~pattern` - Like (glob pattern)
- `field?` - Is null
- `field!?` - Is not null

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
- `basic_usage.rs` - Complete CRUD operations and subscriptions
- `benchmark.rs` - Performance testing

### Constraint Examples
- `constraints_01_schemas.rs` - Type validation and default values
- `constraints_02_unique.rs` - Single and composite unique constraints
- `constraints_03_not_null.rs` - Required field enforcement
- `constraints_04_fk_cascade.rs` - Cascade deletion (multilevel)
- `constraints_05_fk_restrict.rs` - Prevent deletion with references
- `constraints_06_fk_set_null.rs` - Optional relationships
- `constraints_07_combined.rs` - All constraints working together

### Real-World Application
- `parking_lot.rs` - Complete parking lot management system demonstrating:
  - Complex schema with 5+ entities and comprehensive constraints
  - Foreign keys with CASCADE/RESTRICT/SET_NULL policies
  - Reactive event system with MQTT bridge pattern
  - Bidirectional DB ↔ MQTT integration
  - Complete entry/exit flows with IoT device simulation
  - Real-time status updates via subscriptions
  - TTL-based reservation expiration

Run examples:
```bash
cargo run --example basic_usage
cargo run --example benchmark --release
cargo run --example constraints_01_schemas
cargo run --example constraints_07_combined
cargo run --example parking_lot
```

## Design Inspiration

- **Fjall**: Rust-native LSM storage engine
- **BadgerDB**: Value log separation, efficient compaction
- **MQTT**: Wildcard subscription patterns for reactive updates

## Implementation Status

### Phase 1-3: Core Features ✓
- [x] Core reactive database with CRUD operations
- [x] Transactional wrapper with atomic batches
- [x] Reactive subscription engine with persistence
- [x] Secondary indexes (equality and range)
- [x] Event dispatcher with async notifications
- [x] Correlation ID-based deduplication
- [x] Comprehensive tests and benchmarks

### Phase 4: Advanced Features ✓
- [x] Extended filter operators (In, Like, IsNull, IsNotNull)
- [x] Sorting and pagination for list operations
- [x] Relationships and nested entity loading
- [x] TTL-based cleanup with reactive notifications

### Phase 5: ACID Constraint System ✓
- [x] Schema validation with type checking
- [x] Field-level constraints (required, default values)
- [x] Unique constraints (single and composite)
- [x] Not-null constraints
- [x] Foreign key constraints with CASCADE/RESTRICT/SET NULL
- [x] Recursive cascade deletion
- [x] Atomic constraint validation
- [x] Constraint persistence across restarts

### Phase 6: MQTT Integration ✓
- [x] Embedded MQTT broker (MqdbAgent)
- [x] Authentication with password file
- [x] SCRAM-SHA-256 challenge-response authentication
- [x] JWT authentication (HS256, RS256, ES256)
- [x] Federated JWT (multiple issuers)
- [x] Authentication rate limiting (brute-force protection)
- [x] ACL-based authorization with RBAC roles
- [x] ACL CLI management (add/remove/role-add/assign/check/list)
- [x] CRUD operations via MQTT topics
- [x] Admin topics for schema/constraint/backup
- [x] CLI tool with all operations
- [x] Request/response pattern with response_topic
- [x] Server-side backup management

### Phase 7: Point-to-Point & Reliability ✓
- [x] Shared subscriptions with consumer groups
- [x] LoadBalanced mode (round-robin distribution)
- [x] Ordered mode (partition-based routing)
- [x] Consumer group rebalancing on join/leave
- [x] Heartbeat mechanism for stale consumer detection
- [x] Mode validation (prevent mixing modes in same group)
- [x] Outbox pattern for atomic event persistence
- [x] Retry with exponential backoff
- [x] Dead letter queue for failed events
- [x] CLI commands for subscribe and consumer-group

### Phase 8: Platform Support ✓
- [x] Storage abstraction with pluggable backends
- [x] Fjall backend for native (LSM-based persistence)
- [x] Memory backend for WASM (in-memory storage)
- [x] Feature flags for conditional compilation
- [x] WASM crate with JavaScript bindings

### Phase 9-10: Distributed Clustering ✓
- [x] 64-partition data distribution with RF=2
- [x] Raft consensus for cluster topology
- [x] MQTT bridges for inter-node communication
- [x] Heartbeat-based node failure detection
- [x] Automatic partition reassignment on node death
- [x] Cross-node pub/sub message routing
- [x] Session migration and cleanup
- [x] QoS state replication
- [x] Last Will Testament cross-node delivery

### Test Coverage
- 436 tests passing (unit + integration + cluster)
- Clippy clean with pedantic warnings enabled
- Full constraint coverage including multilevel cascade
- Point-to-point delivery tests with partition verification
- Comprehensive cluster integration tests (failover, split-brain, rebalancing)

## Future Enhancements

- Cluster-wide constraints (unique across all nodes)
- Cluster-wide consumer group tracking
- Coordinated cluster backup/restore
- Reactive query language (subscribe to expressions)
- Persistence metrics and tracing
- Optimized TTL cleanup with expiration index
- Horizontal scaling beyond 64 partitions

## License

MIT
