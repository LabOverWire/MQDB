# MQDB - Reactive Embedded Database

A high-performance, reactive embedded database built in Rust with MQTT-ready event streaming.

## Features

- **Reactive Subscriptions**: Built-in change observation with MQTT-style wildcard patterns (`+`, `#`)
- **High Performance**: 168k+ writes/sec, 558k+ reads/sec with sub-millisecond latency
- **ACID Constraints**: Schemas, unique constraints, foreign keys with CASCADE/RESTRICT/SET NULL
- **Secondary Indexes**: Efficient equality and range indexes for fast queries
- **Extended Filter Operators**: In, Like (glob patterns), IsNull, IsNotNull
- **Sorting & Pagination**: Multi-field sorting with offset/limit pagination
- **Relationships**: Foreign key-style relationships with nested entity loading
- **TTL Expiration**: Automatic entity cleanup with reactive delete events
- **Atomic Transactions**: Multi-key atomic batches with ACID guarantees
- **Persistent Subscriptions**: Subscriptions survive restarts
- **Event Streaming**: Real-time change notifications via async channels
- **LSM-based Storage**: Built on Fjall for efficient disk I/O
- **Idempotent Operations**: Correlation ID-based deduplication

## Architecture

### Core Components

1. **Storage Layer (Fjall)**: LSM-based key-value persistence with atomic batch operations
2. **Reactive Core**: Subscription registry with prefix/wildcard matching and event dispatching
3. **Entity Layer**: JSON ↔ KV translation with schema-less storage
4. **Index Manager**: Secondary indexes for efficient queries
5. **Event Dispatcher**: Async notification system with bounded channels
6. **Dedup Store**: Correlation ID-based idempotency

### Key Encoding Scheme

```
data/{entity}/{id}              → entity data
idx/{entity}/{field}/{value}/{id} → secondary index entries
sub/{subscription_id}           → subscription metadata
dedup/{correlation_id}          → cached responses for idempotency
meta/{key}                      → system metadata (sequences, etc.)
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

Run examples:
```bash
cargo run --example basic_usage
cargo run --example benchmark --release
cargo run --example constraints_01_schemas
cargo run --example constraints_07_combined
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

### Test Coverage
- 93 tests passing (26 unit + 67 integration including 25 constraint tests)
- Clippy clean, no warnings
- Full constraint coverage including multilevel cascade

## Future Enhancements

- MQTT broker integration layer
- Replication and WAL streaming
- Reactive query language (subscribe to expressions)
- Persistence metrics and tracing
- Optimized TTL cleanup with expiration index

## License

MIT
