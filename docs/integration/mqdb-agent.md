# MQDB Agent: Frontend Integration Tasks

## Status: ✅ COMPLETE

MQDB provides generic infrastructure. Application-specific sync logic is handled by the frontend.

---

## What MQDB Provides

| Feature | Status | Usage |
|---------|--------|-------|
| WebSocket listener | ✅ | `--ws-bind 0.0.0.0:8080` |
| MQTT over WebSocket | ✅ | Browser connects to `ws://host:8080/mqtt` |
| CRUD operations | ✅ | `$DB/create/{entity}`, `$DB/read/{entity}/{id}`, etc. |
| Real-time events | ✅ | `$DB/{entity}/events/#` for change notifications |
| Change-only delivery | ✅ | Duplicate payloads suppressed on `$DB/+/events/#` |
| Foreign keys | ✅ | CASCADE delete supported |

---

## Event Topics

The broker publishes a `ChangeEvent` JSON payload to MQTT on every write:

| Mode | Topic format | Example |
|------|-------------|---------|
| Agent (standalone) | `$DB/{entity}/events/{id}` | `$DB/nodes/events/node-123` |
| Cluster | `$DB/{entity}/events/p{partition}/{id}` | `$DB/nodes/events/p3/node-123` |

Subscribe with `$DB/{entity}/events/#` to receive all events for an entity regardless of mode.

### ChangeEvent payload

```json
{
  "sequence": 42,
  "entity": "nodes",
  "id": "node-123",
  "operation": "Create",
  "data": { "name": "Alice" },
  "operation_id": "uuid-here"
}
```

- `operation`: `"Create"`, `"Update"`, or `"Delete"`
- `data`: present on Create/Update, absent on Delete
- `operation_id`: UUID for echo suppression (frontend can match against its own writes)

---

## Change-Only Delivery

Enabled by default on `$DB/+/events/#`. When a subscriber has already received an event with identical payload, the broker suppresses the duplicate. This is a server-side complement to the frontend's `operation_id`-based echo suppression.

---

## Sync Protocol: Frontend Responsibility

The TLA+ verified sync protocol requires sequence tracking to prevent duplicate mutations during initial sync. This is **application logic** that the frontend handles:

1. **`diagrams.version` field** - Frontend manages this as regular data
2. **On child mutation** - Frontend increments `diagram.version`
3. **On initial fetch** - Frontend reads `diagram.version` with child entities
4. **On incoming mutations** - Frontend filters by version

MQDB has no concept of "diagrams" or parent-child versioning. It's just a database that stores and syncs data.

---

## Usage

### Starting the broker

```bash
mqdb agent start --db /path --bind 0.0.0.0:1883 --ws-bind 0.0.0.0:8080 --anonymous
```

### Browser connection

```javascript
const client = new MqttClient('ws://127.0.0.1:8080/mqtt');
```

### Watching for changes

```javascript
client.subscribe('$DB/nodes/events/#');
client.subscribe('$DB/edges/events/#');
```

---

## No MQDB Changes Needed

The original plan incorrectly added diagram-specific logic to MQDB core. This was reverted. MQDB remains a generic MQTT broker with embedded database - application semantics belong in the application layer.
