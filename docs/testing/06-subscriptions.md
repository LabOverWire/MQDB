# Reactive Subscriptions & Consumer Groups

[Back to index](README.md)

## 6. Reactive Subscriptions

### Watch Command (Broadcast)

**Terminal 3:**
```bash
mqdb watch users
```

**Terminal 2:**
```bash
mqdb create users --data '{"name": "Watcher Test"}'
```

**Expected in Terminal 3:**
```json
{"type": "create", "entity": "users", "id": "...", "data": {"name": "Watcher Test"}}
```

### Subscribe with Consumer Group (Load-Balanced)

The `subscribe` command takes a **topic pattern** (not an entity name). Use the `$DB/{entity}/events/#` pattern to receive change events.

**Terminal 3 (Consumer A):**
```bash
mqdb subscribe '$DB/users/events/#' --group workers --mode load-balanced
```

**Open Terminal 4 (Consumer B):**
```bash
mqdb subscribe '$DB/users/events/#' --group workers --mode load-balanced
```

**Terminal 2 (create multiple entities):**
```bash
mqdb create users --data '{"name": "User 1"}'
mqdb create users --data '{"name": "User 2"}'
mqdb create users --data '{"name": "User 3"}'
mqdb create users --data '{"name": "User 4"}'
```

**Expected:** Events distributed between Terminal 3 and Terminal 4 (each receives ~2 events)

> **Note:** `subscribe` takes a raw MQTT topic pattern, unlike `watch` which takes an entity name.
> Mode values: `broadcast` (default), `load-balanced`, `ordered`.

### Subscribe with Ordered Mode

**Terminal 3:**
```bash
mqdb subscribe '$DB/users/events/#' --group processors --mode ordered
```

**Expected:** All events for same entity ID go to same consumer

### Custom Heartbeat Interval

The `--heartbeat-interval` flag specifies seconds (default: 10):

```bash
mqdb subscribe '$DB/users/events/#' --group workers --heartbeat-interval 5
```

---

## 7. Consumer Groups

### List Consumer Groups

```bash
mqdb consumer-group list
```

**Expected output (JSON, default format):**
```json
{"data": [{"name": "workers", "members": 2}, {"name": "processors", "members": 1}], "status": "ok"}
```

Use `--format table` for human-readable output.

### Show Consumer Group Details

```bash
mqdb consumer-group show workers
```

**Expected output (JSON, default format):**
```json
{"data": {"name": "workers", "mode": "LoadBalanced", "members": [...]}, "status": "ok"}
```
