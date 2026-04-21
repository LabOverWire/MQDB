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
db.addSchema('users', {
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
db.addUniqueConstraint('users', ['email']);
db.addNotNull('users', 'name');
db.addForeignKey('posts', 'author_id', 'users', 'id', 'cascade');
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
| `in` | Value in array |
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

**Index-Accelerated Queries:**

When a single-field index exists and a filter matches that field, `list`, `count`, and `cursor` scan the index instead of doing a full table scan. This works for both equality (`eq`) and range (`gt`, `gte`, `lt`, `lte`) filters, including combined upper+lower bounds. Remaining filters are applied on the reduced candidate set.

```javascript
db.addIndex('products', ['category']);

// This query scans the index on category, then applies the price filter
const results = await db.list('products', {
    filters: [
        { field: 'category', op: 'eq', value: 'electronics' },
        { field: 'price', op: 'gt', value: 100 }
    ]
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
db.subscribeShared('*', 'orders', 'processors', 'broadcast', callback);

// Load Balanced - round-robin distribution
db.subscribeShared('*', 'orders', 'workers', 'load-balanced', callback);
```

**Heartbeat:**
```javascript
// Update subscription timestamp (for liveness tracking)
db.heartbeat(subId);

// Get subscription details including last_heartbeat
const info = db.getSubscriptionInfo(subId);
```

**Consumer Groups:**
```javascript
// List all shared subscription groups
const groups = db.listConsumerGroups();
// Returns: [{ name, member_count, members: [...] }]

// Get specific group details
const group = db.getConsumerGroup('workers');
```

### Relationships (`relationships.html`)

Entity relationships and eager loading.

**Define Relationships:**
```javascript
// posts.author_id references authors.id
db.addRelationship('posts', 'author', 'authors');

// List defined relationships
const rels = db.listRelationships('posts');
```

**Read with Includes (Eager Loading):**
```javascript
// Fetches post and embeds the related author object
const post = await db.readWithIncludes('posts', '1', ['author']);
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

### Persistent Storage (`persistent.html`)

IndexedDB-backed storage for data that survives page reloads.

```javascript
// Open a persistent database (IndexedDB)
const db = await Database.openPersistent('my-database');
console.log(db.isMemoryBackend()); // false

// CRUD works the same — data persists across page reloads
const user = await db.create('users', { name: 'Alice' });

// Switch between databases
const otherDb = await Database.openPersistent('other-database');
```

- **Toggle storage modes** — switch between in-memory and IndexedDB at runtime
- **Named databases** — multiple independent databases via different names
- **IndexedDB inspector** — view raw stored key-value pairs
- **Counter demo** — persistent counter that survives page reloads

### Count (`count.html`)

Count records without fetching full data.

```javascript
// Count all records
const total = await db.count('products');

// Count with filters (uses index when available)
const expensive = await db.count('products', {
    filters: [{ field: 'category', op: 'eq', value: 'electronics' }]
});

// Sync variant (memory backend only)
const syncCount = db.countSync('products', null);
```

### Encrypted Storage (`encrypted.html`)

AES-256-GCM encryption at rest using a passphrase-derived key.

```javascript
const db = await Database.openEncrypted('my-db', 'my-passphrase');

// All async admin methods persist to encrypted storage
await db.addSchemaAsync('users', { fields: [...] });
await db.addUniqueConstraintAsync('users', ['email']);
await db.addNotNullAsync('users', 'name');
await db.addForeignKeyAsync('posts', 'author_id', 'users', 'id', 'cascade');
await db.addIndexAsync('users', ['age']);
await db.addRelationshipAsync('posts', 'author', 'users');

// CRUD works transparently — data encrypted/decrypted automatically
const user = await db.create('users', { name: 'Alice', email: 'alice@example.com' });
const read = await db.read('users', user.id);
```

### Sync API (`sync-api.html`)

Synchronous operations for memory-only databases. No `await` needed.

```javascript
const db = new Database();
console.log(db.isMemoryBackend()); // true

const user = db.createSync('users', { name: 'Alice' });
const read = db.readSync('users', user.id);
db.updateSync('users', user.id, { name: 'Bob' });
db.deleteSync('users', user.id);

const all = db.listSync('users', null);
const count = db.countSync('users', { filters: [{ field: 'name', op: 'eq', value: 'Bob' }] });
```

### Execute (`execute.html`)

MQTT-style topic routing interface. Same topic patterns as the networked MQDB broker.

```javascript
// Health check
const health = await db.execute('$DB/_health', null);

// CRUD via topics
const created = await db.execute('$DB/products/create', { name: 'Widget', price: 9.99 });
const read = await db.execute(`$DB/products/${created.id}`, null);
await db.execute(`$DB/products/${created.id}/update`, { price: 12.99 });
await db.execute(`$DB/products/${created.id}/delete`, null);
const list = await db.execute('$DB/products/list', {});

// Admin operations
await db.execute('$DB/_admin/schema/orders/set', { fields: [...] });
const schema = await db.execute('$DB/_admin/schema/orders/get', null);
await db.execute('$DB/_admin/constraint/orders/add', { type: 'unique', fields: ['code'] });
const constraints = await db.execute('$DB/_admin/constraint/orders/list', null);
await db.execute('$DB/_admin/index/orders/add', { fields: ['total'] });
const catalog = await db.execute('$DB/_admin/catalog', null);
```

### Cursor (`cursor.html`)

Streaming iteration over results.

**Create Cursor:**
```javascript
const cursor = await db.cursor('products', {
    filters: [{ field: 'price', op: 'gt', value: 50 }],
    sort: [{ field: 'price', direction: 'asc' }]
});
```

**Cursor with Projection:**
```javascript
const cursor = await db.cursor('products', {
    filters: [{ field: 'category', op: 'eq', value: 'books' }],
    projection: ['name', 'price']
});
// Each item contains only id, name, and price
```

**Iterate:**
```javascript
// One at a time
while (cursor.hasMore()) {
    const item = cursor.nextItem();
    console.log(item);
}

// In batches
const batch = cursor.nextBatch(10);  // Up to 10 items
```

**Cursor Methods:**
| Method | Description |
|--------|-------------|
| `nextItem()` | Get next item (undefined when exhausted) |
| `nextBatch(n)` | Get up to n items as array |
| `reset()` | Reset to beginning |
| `hasMore()` | Check if more items available |
| `count()` | Remaining items in buffer |
| `position()` | Current iteration position |

## Architecture

The WASM database runs entirely in the browser with no server required:

```
┌────────────────────────────────────────────────────────┐
│                       Browser                           │
│  ┌──────────────────────────────────────────────────┐  │
│  │              JavaScript API                      │  │
│  │  db.create() / db.list() / db.execute() / ...    │  │
│  └────────────────────┬─────────────────────────────┘  │
│                       │                                 │
│  ┌────────────────────▼─────────────────────────────┐  │
│  │              WASM Module                         │  │
│  │  ┌───────────────────────────────────────────┐   │  │
│  │  │    Database (Rust)                        │   │  │
│  │  │  - Schema validation                      │   │  │
│  │  │  - Constraint checking (unique, FK, NN)   │   │  │
│  │  │  - Index acceleration (eq + range)        │   │  │
│  │  │  - Subscription dispatch                  │   │  │
│  │  │  - Relationship loading                   │   │  │
│  │  └──────────────────┬────────────────────────┘   │  │
│  │                     │                            │  │
│  │  ┌──────────────────▼────────────────────────┐   │  │
│  │  │    Storage Backend                        │   │  │
│  │  │  ┌────────────┬────────────┬───────────┐  │   │  │
│  │  │  │  Memory    │ IndexedDB  │ Encrypted │  │   │  │
│  │  │  │ (volatile) │(persistent)│(AES-GCM)  │  │   │  │
│  │  │  └────────────┴────────────┴───────────┘  │   │  │
│  │  └───────────────────────────────────────────┘   │  │
│  └──────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────┘
```

## API Reference

### Database Operations (Async)

| Method | Description |
|--------|-------------|
| `new Database()` | Create in-memory database instance |
| `Database.openPersistent(name)` | Create IndexedDB-backed instance |
| `Database.openEncrypted(name, passphrase)` | Create encrypted IndexedDB-backed instance |
| `create(entity, data)` | Insert record |
| `read(entity, id)` | Get record by ID |
| `readWithIncludes(entity, id, includes)` | Get record with related data |
| `update(entity, id, fields)` | Update record fields |
| `delete(entity, id)` | Remove record |
| `list(entity, options)` | Query records |
| `count(entity, options)` | Count records matching filters |
| `cursor(entity, options)` | Create streaming cursor |

### Database Operations (Sync — memory backend only)

Sync methods perform the same operations as their async counterparts but return
values directly instead of Promises. They only work on `Database` instances
created with `new Database()` (memory backend). Calling them on an
IndexedDB-backed instance throws an error.

Use `isMemoryBackend()` to check at runtime.

| Method | Description |
|--------|-------------|
| `createSync(entity, data)` | Insert record |
| `readSync(entity, id)` | Get record by ID |
| `updateSync(entity, id, fields)` | Update record fields |
| `deleteSync(entity, id)` | Remove record |
| `listSync(entity, options)` | Query records (no `includes` support) |
| `countSync(entity, options)` | Count records (no `includes` support) |
| `isMemoryBackend()` | Returns `true` if using memory storage |

```javascript
const db = new Database();
console.log(db.isMemoryBackend()); // true

const user = db.createSync("users", { name: "Alice" });
const found = db.readSync("users", user.id);
const all = db.listSync("users", {
    filters: [{ field: "name", op: "eq", value: "Alice" }]
});
db.updateSync("users", user.id, { name: "Bob" });
db.deleteSync("users", user.id);
```

### Schema & Constraints

| Method | Description |
|--------|-------------|
| `addSchema(entity, schema)` | Define entity schema (memory) |
| `addSchemaAsync(entity, schema)` | Define entity schema (persisted) |
| `getSchema(entity)` | Get entity schema |
| `addUniqueConstraint(entity, fields)` | Add unique constraint (memory) |
| `addUniqueConstraintAsync(entity, fields)` | Add unique constraint (persisted) |
| `addNotNull(entity, field)` | Add not-null constraint (memory) |
| `addNotNullAsync(entity, field)` | Add not-null constraint (persisted) |
| `addForeignKey(source, field, target, targetField, onDelete)` | Add foreign key (memory) |
| `addForeignKeyAsync(source, field, target, targetField, onDelete)` | Add foreign key (persisted) |
| `addIndex(entity, fields)` | Add index (memory) |
| `addIndexAsync(entity, fields)` | Add index (persisted) |
| `listConstraints(entity)` | List entity constraints |

### Relationships

| Method | Description |
|--------|-------------|
| `addRelationship(source, field, target)` | Define relationship (memory) |
| `addRelationshipAsync(source, field, target)` | Define relationship (persisted) |
| `listRelationships(entity)` | List entity relationships |

### Execute

| Method | Description |
|--------|-------------|
| `execute(topic, payload)` | Execute operation via MQTT topic pattern |

### Subscriptions

| Method | Description |
|--------|-------------|
| `subscribe(pattern, entity, callback)` | Subscribe to changes |
| `subscribeShared(pattern, entity, group, mode, callback)` | Shared subscription |
| `unsubscribe(subId)` | Remove subscription |
| `heartbeat(subId)` | Update subscription timestamp |
| `getSubscriptionInfo(subId)` | Get subscription details |
| `listConsumerGroups()` | List all consumer groups |
| `getConsumerGroup(name)` | Get consumer group details |
