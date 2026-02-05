# MQDB WASM Examples

Interactive examples demonstrating MQDB's WebAssembly database capabilities.

## Running the Examples

```bash
# Build the WASM package
cd crates/mqdb-wasm
wasm-pack build --target web

# Serve the examples
cd examples
python3 -m http.server 8080

# Open http://localhost:8080/
```

## Examples Overview

### Basic CRUD (`basic-crud.html`)

Core database operations for managing records.

- **Create** - Insert records with auto-generated IDs
- **Read** - Fetch records by ID
- **Update** - Partial field updates (merge semantics)
- **Delete** - Remove records
- **List** - Retrieve all records for an entity

### Schema Validation (`schema.html`)

Define and enforce data structure rules.

- **Field Types** - string, number, boolean, array, object
- **Required Fields** - Validation fails if missing
- **Default Values** - Auto-applied on create
- **Type Checking** - Validates on create and update

```javascript
db.add_schema('users', {
    fields: [
        { name: 'email', type: 'string', required: true },
        { name: 'age', type: 'number' },
        { name: 'active', type: 'boolean', default: true }
    ]
});
```

### Constraints (`constraints.html`)

Data integrity enforcement.

- **Unique** - Prevent duplicate values across records
- **Not Null** - Require non-null values
- **Foreign Key** - Reference integrity between entities
  - `restrict` - Block delete if referenced
  - `cascade` - Delete referencing records
  - `set_null` - Set reference to null on delete

```javascript
db.add_unique_constraint('users', ['email']);
db.add_not_null('users', 'name');
db.add_foreign_key('posts', 'author_id', 'users', 'id', 'cascade');
```

### Filtering & Sorting (`filtering.html`)

Advanced query capabilities.

**Filter Operators:**
| Operator | Description |
|----------|-------------|
| `eq` | Equal |
| `ne`, `<>` | Not equal |
| `gt`, `>` | Greater than |
| `lt`, `<` | Less than |
| `gte`, `>=` | Greater or equal |
| `lte`, `<=` | Less or equal |
| `glob`, `~` | Wildcard pattern (`*`) |
| `null`, `?` | Is null |
| `not_null`, `!?` | Is not null |

```javascript
const results = await db.list('products', {
    filters: [
        { field: 'price', op: 'gt', value: 100 },
        { field: 'name', op: 'glob', value: '*phone*' }
    ],
    sort: [{ field: 'price', direction: 'desc' }],
    pagination: { limit: 10, offset: 0 },
    projection: ['id', 'name', 'price']
});
```

### Subscriptions (`subscriptions.html`)

Real-time change notifications.

**Basic Subscription:**
```javascript
const subId = db.subscribe('*', 'users', (event) => {
    console.log(event.operation, event.entity, event.id, event.data);
});

db.unsubscribe(subId);
```

**Shared Subscriptions:**
```javascript
// Broadcast - all subscribers receive every event
db.subscribe_shared('*', 'orders', 'processors', 'broadcast', callback);

// Load Balanced - round-robin distribution
db.subscribe_shared('*', 'orders', 'workers', 'load-balanced', callback);
```

**Heartbeat:**
```javascript
// Update subscription timestamp (for liveness tracking)
db.heartbeat(subId);

// Get subscription details including last_heartbeat
const info = db.get_subscription_info(subId);
```

**Consumer Groups:**
```javascript
// List all shared subscription groups
const groups = db.list_consumer_groups();
// Returns: [{ name, member_count, members: [...] }]

// Get specific group details
const group = db.get_consumer_group('workers');
```

### Relationships (`relationships.html`)

Entity relationships and eager loading.

**Define Relationships:**
```javascript
// posts.author_id references authors.id
db.add_relationship('posts', 'author', 'authors');

// List defined relationships
const rels = db.list_relationships('posts');
```

**Read with Includes (Eager Loading):**
```javascript
// Fetches post and embeds the related author object
const post = await db.read_with_includes('posts', '1', ['author']);
// Result: { id: '1', title: '...', author_id: '1', author: { id: '1', name: 'Alice' } }
```

**List with Includes (Batch Loading):**
```javascript
const posts = await db.list('posts', {
    includes: ['author']
});
// All posts have their author objects embedded
```

**Nested Includes:**
```javascript
// Supports dot notation for nested relationships (max depth 3)
const posts = await db.list('posts', {
    includes: ['author.company']
});
```

### Cursor (`cursor.html`)

Streaming iteration over results.

**Create Cursor:**
```javascript
const cursor = db.cursor('products', {
    filters: [{ field: 'price', op: 'gt', value: 50 }],
    sort: [{ field: 'price', direction: 'asc' }]
});
```

**Iterate:**
```javascript
// One at a time
while (cursor.has_more()) {
    const item = cursor.next();
    console.log(item);
}

// In batches
const batch = cursor.next_batch(10);  // Up to 10 items
```

**Cursor Methods:**
| Method | Description |
|--------|-------------|
| `next()` | Get next item (undefined when exhausted) |
| `next_batch(n)` | Get up to n items as array |
| `reset()` | Reset to beginning |
| `has_more()` | Check if more items available |
| `count()` | Remaining items in buffer |
| `position()` | Current iteration position |

## Architecture

The WASM database runs entirely in the browser with no server required:

```
┌─────────────────────────────────────────┐
│              Browser                     │
│  ┌───────────────────────────────────┐  │
│  │         JavaScript API            │  │
│  │  db.create() / db.read() / ...    │  │
│  └───────────────┬───────────────────┘  │
│                  │                       │
│  ┌───────────────▼───────────────────┐  │
│  │         WASM Module               │  │
│  │  ┌─────────────────────────────┐  │  │
│  │  │    WasmDatabase (Rust)      │  │  │
│  │  │  - Schema validation        │  │  │
│  │  │  - Constraint checking      │  │  │
│  │  │  - Subscription dispatch    │  │  │
│  │  │  - Relationship loading     │  │  │
│  │  └─────────────┬───────────────┘  │  │
│  │                │                   │  │
│  │  ┌─────────────▼───────────────┐  │  │
│  │  │    In-Memory Storage        │  │  │
│  │  │  (mqdb::storage::Storage)   │  │  │
│  │  └─────────────────────────────┘  │  │
│  └───────────────────────────────────┘  │
└─────────────────────────────────────────┘
```

## API Reference

### Database Operations (Async)

| Method | Description |
|--------|-------------|
| `new WasmDatabase()` | Create in-memory database instance |
| `WasmDatabase.open_persistent(name)` | Create IndexedDB-backed instance |
| `create(entity, data)` | Insert record |
| `read(entity, id)` | Get record by ID |
| `read_with_includes(entity, id, includes)` | Get record with related data |
| `update(entity, id, fields)` | Update record fields |
| `delete(entity, id)` | Remove record |
| `list(entity, options)` | Query records |
| `cursor(entity, options)` | Create streaming cursor |

### Database Operations (Sync — memory backend only)

Sync methods perform the same operations as their async counterparts but return
values directly instead of Promises. They only work on `WasmDatabase` instances
created with `new WasmDatabase()` (memory backend). Calling them on an
IndexedDB-backed instance throws an error.

Use `is_memory_backend()` to check at runtime.

| Method | Description |
|--------|-------------|
| `create_sync(entity, data)` | Insert record |
| `read_sync(entity, id)` | Get record by ID |
| `update_sync(entity, id, fields)` | Update record fields |
| `delete_sync(entity, id)` | Remove record |
| `list_sync(entity, options)` | Query records (no `includes` support) |
| `is_memory_backend()` | Returns `true` if using memory storage |

```javascript
const db = new WasmDatabase();
console.log(db.is_memory_backend()); // true

const user = db.create_sync("users", { name: "Alice" });
const found = db.read_sync("users", user.id);
const all = db.list_sync("users", {
    filters: [{ field: "name", op: "eq", value: "Alice" }]
});
db.update_sync("users", user.id, { name: "Bob" });
db.delete_sync("users", user.id);
```

### Schema & Constraints

| Method | Description |
|--------|-------------|
| `add_schema(entity, schema)` | Define entity schema |
| `get_schema(entity)` | Get entity schema |
| `add_unique_constraint(entity, fields)` | Add unique constraint |
| `add_not_null(entity, field)` | Add not-null constraint |
| `add_foreign_key(source, field, target, targetField, onDelete)` | Add foreign key |
| `add_index(entity, fields)` | Add index |
| `list_constraints(entity)` | List entity constraints |

### Relationships

| Method | Description |
|--------|-------------|
| `add_relationship(source, field, target)` | Define relationship |
| `list_relationships(entity)` | List entity relationships |

### Subscriptions

| Method | Description |
|--------|-------------|
| `subscribe(pattern, entity, callback)` | Subscribe to changes |
| `subscribe_shared(pattern, entity, group, mode, callback)` | Shared subscription |
| `unsubscribe(subId)` | Remove subscription |
| `heartbeat(subId)` | Update subscription timestamp |
| `get_subscription_info(subId)` | Get subscription details |
| `list_consumer_groups()` | List all consumer groups |
| `get_consumer_group(name)` | Get consumer group details |
