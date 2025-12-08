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

// CRUD operations
const user = await db.create("users", { name: "Alice", email: "alice@example.com" });
const found = await db.get("users", user.id);
await db.update("users", user.id, { name: "Bob" });
const all = await db.list("users");
await db.delete("users", user.id);

// Subscribe to changes
const subId = db.subscribe("*", null, (event) => {
  console.log(event.operation, event.entity, event.id, event.data);
});

db.unsubscribe(subId);
```

## API

| Method | Description |
|--------|-------------|
| `create(entity, data)` | Create record, returns with generated id |
| `get(entity, id)` | Get record by id |
| `update(entity, id, fields)` | Partial update, returns merged record |
| `delete(entity, id)` | Delete record |
| `list(entity)` | List all records for entity |
| `subscribe(pattern, entity, callback)` | Subscribe to changes, returns subscription id |
| `unsubscribe(subId)` | Unsubscribe |
| `add_schema(entity, schema)` | Register schema with validation |
| `get_schema(entity)` | Get registered schema |

## Notes

- Data is stored in memory only (lost on page refresh)
- Field types: `string`, `number`, `boolean`, `array`, `object`
- Subscription patterns: `*` (all), `entity` (exact), `prefix/*` (prefix match)
