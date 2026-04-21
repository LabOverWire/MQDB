# mqdb-wasm

WebAssembly bindings for mqdb, providing a reactive database for browser environments with optional IndexedDB persistence and encryption.

## Build

```bash
wasm-pack build --target web
```

## Usage

```javascript
import init, { WasmDatabase } from './pkg/mqdb_wasm.js';

await init();

// In-memory (data lost on page refresh)
const db = new WasmDatabase();

// Persistent (IndexedDB-backed, survives page reloads)
const db = await WasmDatabase.open_persistent("my-app");

// Encrypted persistent (AES-GCM + PBKDF2, passphrase-derived key)
const db = await WasmDatabase.open_encrypted("my-app", "user-passphrase");

// Define schema
db.add_schema("users", {
  fields: [
    { name: "name", type: "string", required: true },
    { name: "email", type: "string" }
  ]
});

// Async CRUD operations
const user = await db.create("users", { name: "Alice", email: "alice@example.com" });
const found = await db.read("users", user.id);
await db.update("users", user.id, { name: "Bob" });
const all = await db.list("users");
const count = await db.count("users", { filters: [{ field: "name", op: "eq", value: "Alice" }] });
await db.delete("users", user.id);

// Sync CRUD operations (memory backend only)
const user2 = db.create_sync("users", { name: "Carol", email: "carol@example.com" });
const found2 = db.read_sync("users", user2.id);
db.update_sync("users", user2.id, { name: "Dave" });
const all2 = db.list_sync("users");
const count2 = db.count_sync("users");
db.delete_sync("users", user2.id);

// Subscribe to changes
const subId = db.subscribe("*", null, (event) => {
  console.log(event.operation, event.entity, event.id, event.data);
});

db.unsubscribe(subId);

// MQTT-style topic routing
const result = await db.execute("$DB/users/create", { name: "Eve" });
const listed = await db.execute("$DB/users/list", {});
```

## Storage Backends

| Constructor | Backend | Persistence | Encryption | Sync methods |
|-------------|---------|-------------|------------|--------------|
| `new WasmDatabase()` | Memory | No | No | Yes |
| `WasmDatabase.open_persistent(name)` | IndexedDB | Yes | No | No |
| `WasmDatabase.open_encrypted(name, passphrase)` | IndexedDB | Yes | AES-GCM | No |

Persistent backends automatically reload schemas, constraints, indexes, relationships, and ID counters on open. Encrypted backends derive a 256-bit key from the passphrase via PBKDF2 (100k iterations) and encrypt each record independently with AES-GCM.

## API

### Async Methods

| Method | Description |
|--------|-------------|
| `create(entity, data)` | Create record, returns with generated id and `_version: 1` |
| `read(entity, id)` | Get record by id |
| `read_with_includes(entity, id, includes)` | Get record with related data eagerly loaded |
| `update(entity, id, fields)` | Partial update, bumps `_version`, returns merged record |
| `delete(entity, id)` | Delete record (triggers cascade/set_null/restrict) |
| `list(entity, options?)` | Query with filters, sort, pagination, projection, includes |
| `count(entity, options?)` | Count records matching filters |
| `cursor(entity, options?)` | Create streaming cursor over results |
| `execute(topic, payload)` | MQTT-style topic routing (see below) |

### Sync Methods (memory backend only)

| Method | Description |
|--------|-------------|
| `create_sync(entity, data)` | Create record synchronously |
| `read_sync(entity, id)` | Get record by id synchronously |
| `update_sync(entity, id, fields)` | Partial update synchronously |
| `delete_sync(entity, id)` | Delete record synchronously |
| `list_sync(entity, options?)` | Query records synchronously (no `includes` support) |
| `count_sync(entity, options?)` | Count records synchronously |
| `is_memory_backend()` | Returns `true` if using memory storage |

### Schema & Constraints

| Method | Description |
|--------|-------------|
| `add_schema(entity, schema)` | Define entity schema (sync, memory only) |
| `add_schema_async(entity, schema)` | Define entity schema (persisted) |
| `get_schema(entity)` | Get entity schema |
| `add_unique_constraint(entity, fields)` | Add unique constraint (sync) |
| `add_unique_constraint_async(entity, fields)` | Add unique constraint (persisted) |
| `add_not_null(entity, field)` | Add not-null constraint (sync) |
| `add_not_null_async(entity, field)` | Add not-null constraint (persisted) |
| `add_foreign_key(source, field, target, targetField, onDelete)` | Add foreign key (sync) |
| `add_foreign_key_async(source, field, target, targetField, onDelete)` | Add foreign key (persisted) |
| `add_index(entity, fields)` | Add index (sync, backfills existing records) |
| `add_index_async(entity, fields)` | Add index (persisted, backfills existing records) |
| `list_constraints(entity)` | List entity constraints |

### Relationships

| Method | Description |
|--------|-------------|
| `add_relationship(source, field, target)` | Define relationship (sync) |
| `add_relationship_async(source, field, target)` | Define relationship (persisted) |
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

### Filter Operators

| Operator | Description |
|----------|-------------|
| `eq` (default) | Equal |
| `ne`, `<>` | Not equal |
| `gt`, `>` | Greater than |
| `lt`, `<` | Less than |
| `gte`, `>=` | Greater or equal |
| `lte`, `<=` | Less or equal |
| `glob`, `~` | Wildcard pattern (`*`) |
| `in` | Value in array |
| `null`, `?` | Is null |
| `not_null`, `!?` | Is not null |

### Index Acceleration

Single-field indexes accelerate both equality and range filters:

- **Equality** (`eq`): `list`, `count`, and `cursor` scan the index directly instead of a full table scan
- **Range** (`gt`, `gte`, `lt`, `lte`): bounded range scan on the index, including combined upper+lower bounds

Remaining filters are applied as post-filters on the reduced candidate set.

```javascript
db.add_index('products', ['price']);

// Index range scan for price > 100, then post-filter on category
const results = await db.list('products', {
    filters: [
        { field: 'price', op: 'gt', value: 100 },
        { field: 'category', op: 'eq', value: 'electronics' }
    ]
});
```

### MQTT-Style Topic Routing

The `execute(topic, payload)` method routes operations using MQTT topic patterns, matching the server-side MQDB API:

```javascript
// CRUD
await db.execute("$DB/users/create", { name: "Alice" });
await db.execute("$DB/users/123", {});              // read
await db.execute("$DB/users/123/update", { name: "Bob" });
await db.execute("$DB/users/123/delete", {});
await db.execute("$DB/users/list", {});

// Admin
await db.execute("$DB/_admin/schema/users/set", { fields: [...] });
await db.execute("$DB/_admin/constraint/users/add", { type: "unique", fields: ["email"] });
await db.execute("$DB/_admin/index/users/add", { fields: ["name"] });
await db.execute("$DB/_health", {});
await db.execute("$DB/_catalog", {});
```

## Notes

- `new WasmDatabase()` creates an in-memory instance (data lost on page refresh)
- `WasmDatabase.open_persistent(name)` creates an IndexedDB-backed instance that persists schemas, constraints, indexes, relationships, and data across page reloads
- `WasmDatabase.open_encrypted(name, passphrase)` adds AES-GCM encryption on top of IndexedDB persistence; wrong passphrase is detected on open
- Sync methods (`*_sync`) only work with the memory backend; they throw on IndexedDB
- All records carry a `_version` field that increments on every update (used for CAS via `expect_value`)
- Field types: `string`, `number`, `boolean`, `array`, `object`
- Subscription patterns: `*` (all), `entity` (exact), `prefix/*` (prefix match)
- Foreign key `on_delete` actions: `restrict` (block), `cascade` (delete children), `set_null` (null out FK field, bumps `_version`, updates indexes, dispatches update events)
