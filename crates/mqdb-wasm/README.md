# mqdb-wasm

WebAssembly bindings for mqdb, providing a reactive database for browser environments with optional IndexedDB persistence and encryption.

## Build

```bash
wasm-pack build --target web
```

## Usage

```javascript
import init, { Database } from './pkg/mqdb_wasm.js';

await init();

// In-memory (data lost on page refresh)
const db = new Database();

// Persistent (IndexedDB-backed, survives page reloads)
const db = await Database.openPersistent("my-app");

// Encrypted persistent (AES-GCM + PBKDF2, passphrase-derived key)
const db = await Database.openEncrypted("my-app", "user-passphrase");

// Define schema
db.addSchema("users", {
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
const user2 = db.createSync("users", { name: "Carol", email: "carol@example.com" });
const found2 = db.readSync("users", user2.id);
db.updateSync("users", user2.id, { name: "Dave" });
const all2 = db.listSync("users");
const count2 = db.countSync("users");
db.deleteSync("users", user2.id);

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
| `new Database()` | Memory | No | No | Yes |
| `Database.openPersistent(name)` | IndexedDB | Yes | No | No |
| `Database.openEncrypted(name, passphrase)` | IndexedDB | Yes | AES-256-GCM | No |

Persistent backends automatically reload schemas, constraints, indexes, relationships, and ID counters on open. Encrypted backends derive a 256-bit key from the passphrase via PBKDF2-SHA256 (600,000 iterations, 32-byte salt) using the browser's `SubtleCrypto` API, and encrypt every value going through the storage layer with AES-256-GCM (12-byte nonce, AAD bound to the storage key).

## Client-Side Encryption vs Server-Side Vault

`Database.openEncrypted` is **not** the same feature as MQDB's server-side **Vault** (`mqdb-vault`). They share crypto primitives (AES-256-GCM, PBKDF2-SHA256, 600k iterations) but solve different problems with different operational models — do not assume one is a port of the other.

| | `openEncrypted` (this crate) | `mqdb-vault` (server-side) |
|---|---|---|
| **Where** | In-browser, via WebCrypto | Agent / cluster, via `ring` |
| **License** | Apache-2.0 | AGPL-3.0-only (Pro/Enterprise) |
| **Scope** | Whole database — every byte going through the storage backend (records, indexes, schemas, constraints, all metadata) | Selected JSON field values per entity, opt-in. Keys, IDs, owner field, and `_*` system fields stay plaintext |
| **Granularity** | All-or-nothing at `open` time | Per-entity opt-in, can be enabled/disabled at runtime |
| **Lock state machine** | None — passphrase entered once at `openEncrypted`, released only by closing the page | Full state machine: `enable`, `unlock`, `lock`, `disable`, `change` admin endpoints with rate-limited unlock |
| **Migration** | Not needed — every byte encrypted from the start | Two-way migration: `enable` encrypts existing plaintext records; `disable` decrypts them |
| **Indexes work on encrypted fields** | Yes — backend transparently decrypts index entries on lookup | No — encrypted field values become random base64 each write, so equality/range queries against encrypted fields fail |
| **AAD** | Storage key (e.g. `data/people/rec-1`) | `entity:id` (e.g. `people:rec-1`) |
| **Threat model** | Protects user data on the user's own device against other browser apps, IndexedDB inspection, device theft | Protects user data on a server operator's disk against the operator and their backups |
| **Who holds the passphrase** | The end user | The application owner / operator |

Use `openEncrypted` when you want a self-contained client-side database that never leaves plaintext at rest in the browser. Use `mqdb-vault` when you run a server and need selective field-level protection with operational lock/unlock controls.

## API

### Async Methods

| Method | Description |
|--------|-------------|
| `create(entity, data)` | Create record, returns with generated id and `_version: 1` |
| `read(entity, id)` | Get record by id |
| `readWithIncludes(entity, id, includes)` | Get record with related data eagerly loaded |
| `update(entity, id, fields)` | Partial update, bumps `_version`, returns merged record |
| `delete(entity, id)` | Delete record (triggers cascade/set_null/restrict) |
| `list(entity, options?)` | Query with filters, sort, pagination, projection, includes |
| `count(entity, options?)` | Count records matching filters |
| `cursor(entity, options?)` | Create streaming cursor over results |
| `execute(topic, payload)` | MQTT-style topic routing (see below) |

### Sync Methods (memory backend only)

| Method | Description |
|--------|-------------|
| `createSync(entity, data)` | Create record synchronously |
| `readSync(entity, id)` | Get record by id synchronously |
| `updateSync(entity, id, fields)` | Partial update synchronously |
| `deleteSync(entity, id)` | Delete record synchronously |
| `listSync(entity, options?)` | Query records synchronously (no `includes` support) |
| `countSync(entity, options?)` | Count records synchronously |
| `isMemoryBackend()` | Returns `true` if using memory storage |

### Schema & Constraints

| Method | Description |
|--------|-------------|
| `addSchema(entity, schema)` | Define entity schema (sync, memory only) |
| `addSchemaAsync(entity, schema)` | Define entity schema (persisted) |
| `getSchema(entity)` | Get entity schema |
| `addUniqueConstraint(entity, fields)` | Add unique constraint (sync) |
| `addUniqueConstraintAsync(entity, fields)` | Add unique constraint (persisted) |
| `addNotNull(entity, field)` | Add not-null constraint (sync) |
| `addNotNullAsync(entity, field)` | Add not-null constraint (persisted) |
| `addForeignKey(source, field, target, targetField, onDelete)` | Add foreign key (sync) |
| `addForeignKeyAsync(source, field, target, targetField, onDelete)` | Add foreign key (persisted) |
| `addIndex(entity, fields)` | Add index (sync, backfills existing records) |
| `addIndexAsync(entity, fields)` | Add index (persisted, backfills existing records) |
| `listConstraints(entity)` | List entity constraints |

### Relationships

| Method | Description |
|--------|-------------|
| `addRelationship(source, field, target)` | Define relationship (sync) |
| `addRelationshipAsync(source, field, target)` | Define relationship (persisted) |
| `listRelationships(entity)` | List entity relationships |

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
db.addIndex('products', ['price']);

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

- `new Database()` creates an in-memory instance (data lost on page refresh)
- `Database.openPersistent(name)` creates an IndexedDB-backed instance that persists schemas, constraints, indexes, relationships, and data across page reloads
- `Database.openEncrypted(name, passphrase)` adds AES-256-GCM encryption on top of IndexedDB persistence; wrong passphrase is detected on open. This is client-side whole-DB encryption — see "Client-Side Encryption vs Server-Side Vault" above for how this differs from `mqdb-vault`
- Sync methods (`*Sync`) only work with the memory backend; they throw on IndexedDB
- All records carry a `_version` field that increments on every update (used for CAS via `expect_value`)
- Field types: `string`, `number`, `boolean`, `array`, `object`
- Subscription patterns: `*` (all), `entity` (exact), `prefix/*` (prefix match)
- Foreign key `on_delete` actions: `restrict` (block), `cascade` (delete children), `set_null` (null out FK field, bumps `_version`, updates indexes, dispatches update events)
