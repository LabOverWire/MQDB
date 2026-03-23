# Ownership Transfer

Transfer ownership of an entity from one user to another.

## Topic

```
$DB/{entity}/{id}/transfer
```

Payload:

```json
{"new_owner": "target_user_id"}
```

Response: the updated entity JSON (same format as update response).

## Authorization

- Current owner can transfer to any user
- Admin users can transfer any entity
- Non-owners get 403 Forbidden
- If entity has no ownership configured, transfer is a no-op error (400)

## Behavior

- Only changes the owner field value, no cascade or side effects
- Target must be a non-empty string
- Target must differ from current owner
- Emits a standard ChangeEvent (update) on `$DB/{entity}/events/{id}`

## Implementation

### 1. Protocol layer (`crates/mqdb-core/src/protocol/mod.rs`)

Add `Transfer` to `DbOp` enum (line 18). Add match arm in `parse_db_topic` (line 148):

```rust
[entity, id, "transfer"] => Some(DbOperation {
    entity: (*entity).to_string(),
    operation: DbOp::Transfer,
    id: Some((*id).to_string()),
}),
```

This arm must go **before** the `[entity, id]` catch-all (Read).

### 2. Transport layer (`crates/mqdb-core/src/transport.rs`)

Add variant to `Request`:

```rust
Transfer {
    entity: String,
    id: String,
    new_owner: String,
}
```

Update `build_request()` to parse `{"new_owner": "..."}` payload for `DbOp::Transfer`.

### 3. Agent database (`crates/mqdb-agent/src/database/crud.rs`)

Add `transfer_ownership()` method:

1. Read entity by id
2. Get owner field from `OwnershipConfig`
3. Verify current owner == sender (or sender is admin)
4. Update entity data, setting owner field to new_owner
5. Return updated entity

### 4. Agent handler (`crates/mqdb-agent/src/agent/handlers.rs`)

The existing `execute_with_sender` dispatch handles all `Request` variants. Add a match arm for `Request::Transfer` that calls `transfer_ownership()`.

### 5. Cluster handler (`crates/mqdb-cluster/src/cluster_agent/admin.rs`)

Same as agent — the cluster agent event loop dispatches DB requests through a similar path. Add handling for `Request::Transfer`.

### 6. Cluster DB ops (`crates/mqdb-cluster/src/cluster/node_controller/db_ops.rs`)

Transfer is a specialized update — it routes to the partition primary that owns the entity, reads the entity, validates ownership, and writes the updated owner field. Reuse the existing update path with ownership validation prepended.

## Tests

### Unit tests (`crates/mqdb-agent/tests/constraint_test.rs` or new file)

- Owner transfers → succeeds, owner field updated
- Non-owner transfers → 403 Forbidden
- Admin transfers any entity → succeeds
- Transfer to same owner → error (400)
- Transfer on entity without ownership → error (400)
- Transfer emits ChangeEvent

### E2E

```bash
mqdb agent start --db /tmp/mqdb-transfer-test --bind 127.0.0.1:1883 \
    --passwd /tmp/test-passwd.txt --ownership "tasks=userId" --admin-users admin

# bob creates a task
mqdb create tasks -d '{"userId":"bob","name":"T1"}' --user bob --pass ...

# bob transfers to alice
mosquitto_rr -h 127.0.0.1 -p 1883 -t '$DB/tasks/<id>/transfer' \
    -e 'resp/test' -m '{"new_owner":"alice"}' -u bob -P ... -W 5

# verify alice now owns it
mqdb read tasks <id> --user alice --pass ...

# non-owner charlie cannot transfer
mosquitto_rr ... -u charlie -P ... # expect 403

# admin can transfer anything
mosquitto_rr ... -u admin -P ... # expect success
```
