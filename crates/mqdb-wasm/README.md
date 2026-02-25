# mqdb-wasm

WebAssembly bindings for mqdb, providing an in-memory database for browser environments.

## Build

```bash
wasm-pack build --target web
```

## Usage

```javascript
import init, { WasmDatabase } from './pkg/mqdb_wasm.js';

await init();

const db = new WasmDatabase();

// Define schema
db.add_schema("users", {
  fields: [
    { name: "name", type: "string", required: true },
    { name: "email", type: "string" }
  ]
});

// Async CRUD operations
const user = await db.create("users", { name: "Alice", email: "alice@example.com" });
const found = await db.get("users", user.id);
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
```

## API

### Async Methods

| Method | Description |
|--------|-------------|
| `create(entity, data)` | Create record, returns with generated id |
| `read(entity, id)` | Get record by id |
| `update(entity, id, fields)` | Partial update, returns merged record |
| `delete(entity, id)` | Delete record |
| `list(entity, options?)` | List all records for entity |
| `count(entity, options?)` | Count records matching filters |
| `cursor(entity, options?)` | Create streaming cursor |

### Sync Methods (memory backend only)

| Method | Description |
|--------|-------------|
| `create_sync(entity, data)` | Create record synchronously |
| `read_sync(entity, id)` | Get record by id synchronously |
| `update_sync(entity, id, fields)` | Partial update synchronously |
| `delete_sync(entity, id)` | Delete record synchronously |
| `list_sync(entity, options?)` | List records synchronously |
| `count_sync(entity, options?)` | Count records synchronously |
| `is_memory_backend()` | Returns `true` if using memory storage |

### Other

| Method | Description |
|--------|-------------|
| `subscribe(pattern, entity, callback)` | Subscribe to changes, returns subscription id |
| `unsubscribe(subId)` | Unsubscribe |
| `add_schema(entity, schema)` | Register schema with validation |
| `get_schema(entity)` | Get registered schema |

## Notes

- `new WasmDatabase()` creates an in-memory instance (data lost on page refresh)
- `WasmDatabase.open_persistent(name)` creates an IndexedDB-backed instance
- Sync methods (`*_sync`) only work with the memory backend; they throw on IndexedDB
- Field types: `string`, `number`, `boolean`, `array`, `object`
- Subscription patterns: `*` (all), `entity` (exact), `prefix/*` (prefix match)
- Indexes (`add_index`, `add_unique_constraint`) accelerate equality filters in `list`, `list_sync`, `count`, `count_sync`, and `cursor` — single-field indexes are used automatically when an `eq` filter matches
- `cursor` accepts an optional `projection` array to reduce memory footprint
