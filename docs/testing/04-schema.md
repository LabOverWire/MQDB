# Schema Management & Constraints

[Back to index](README.md)

## 4. Schema Management

### Create Schema File

Create `users_schema.json`:
```json
{
  "name": {"type": "string", "required": true},
  "email": {"type": "string", "required": true},
  "age": {"type": "number", "default": 0},
  "active": {"type": "boolean", "default": true}
}
```

### Set Schema

```bash
mqdb schema set users --file users_schema.json
```

**Expected output:**
```json
{"data": {"message": "schema set"}, "status": "ok"}
```

### Get Schema

```bash
mqdb schema get users
```

**Expected output:**
```json
{
  "data": {
    "entity": "users",
    "fields": {
      "name": {"field_type": "String", "name": "name", "required": true, "default": null},
      "email": {"field_type": "String", "name": "email", "required": true, "default": null},
      "age": {"field_type": "Number", "name": "age", "required": false, "default": 0},
      "active": {"field_type": "Boolean", "name": "active", "required": false, "default": true}
    },
    "version": 1
  },
  "status": "ok"
}
```

> **Note:** The schema file uses lowercase `"type"` (e.g., `"string"`), but the stored
> schema returns the internal format with `"field_type"` (e.g., `"String"`).

### Schema Validation - Required Field

```bash
mqdb create users --data '{"email": "test@example.com"}'
```

**Expected error:**
```
error: schema validation failed: name - required field is missing
```

### Schema Validation - Type Check

```bash
mqdb create users --data '{"name": "Test", "email": "test@example.com", "age": "not-a-number"}'
```

**Expected error:**
```
error: schema validation failed: age - expected type Number, got string
```

### Schema Validation - Default Values

```bash
mqdb create users --data '{"name": "Test", "email": "test@example.com"}'
mqdb read users <id>
```

**Expected:** Entity has `age: 0` and `active: true` from defaults

---

## 5. Constraints

### Unique Constraint (Single Field)

```bash
mqdb constraint add users --unique email
mqdb create users --data '{"name": "User1", "email": "unique@test.com"}'
mqdb create users --data '{"name": "User2", "email": "unique@test.com"}'
```

**Expected error on second create:**
```json
{"code": 409, "message": "unique constraint violation: users.email", "status": "error"}
```

### Unique Constraint on Update (Conflict)

```bash
mqdb update users <user1-id> --data '{"email": "unique@test.com"}'
```

**Expected error:**
```json
{"code": 409, "message": "unique constraint violation: users.email", "status": "error"}
```

The update is rejected because another entity already has that email value.

### Unique Constraint on Update (Non-Unique Field Change)

```bash
mqdb update users <user1-id> --data '{"name": "New Name"}'
```

**Expected:** Success. Changing a non-unique field does not trigger unique constraint checks.

### Unique Constraint on Update (Value Recycling)

```bash
# Change user1's email away from its original value
mqdb update users <user1-id> --data '{"email": "different@test.com"}'

# Now create a new user with the old email — should succeed
mqdb create users --data '{"name": "User3", "email": "unique@test.com"}'
```

**Expected:** Both operations succeed. The old unique value is released when the update changes it.

### Not-Null Constraint

```bash
mqdb constraint add users --not-null name
```

### List Constraints

```bash
mqdb constraint list users
```

**Expected output:**
```json
{
  "data": [
    {"name": "users_email_unique", "type": "unique", "fields": ["email"]},
    {"name": "users_name_notnull", "type": "notnull", "field": "name"}
  ],
  "status": "ok"
}
```

### Foreign Key Constraint

Setup:
```bash
mqdb create authors --data '{"name": "Jane Author"}'
# Note the returned ID (e.g., author-abc123)
mqdb constraint add posts --fk "author_id:authors:id:cascade"
```

The `--fk` format is `field:target_entity:target_field[:action]` where action is `cascade`, `restrict`, or `set_null`. Default action is `restrict` if omitted.

Test referential integrity:
```bash
mqdb create posts --data '{"title": "Post 1", "author_id": "invalid-author"}'
```

**Expected error:**
```json
{"code": 409, "message": "foreign key violation: posts.author_id", "status": "error"}
```

Valid foreign key:
```bash
mqdb create posts --data '{"title": "Post 1", "author_id": "author-abc123"}'
```

### Cascade Delete

```bash
mqdb delete authors <author-id>
mqdb list posts
```

**Expected:** Posts with matching author_id are also deleted

### Cascade Delete with ChangeEvents

**Terminal 1 (subscriber):**
```bash
mqdb watch posts
```

**Terminal 2:**
```bash
mqdb create authors --data '{"name": "Alice"}'
# Note the returned ID (e.g., 1)
mqdb constraint add posts --fk "author_id:authors:id:cascade"
mqdb create posts --data '{"title": "Post 1", "author_id": "1"}'
mqdb create posts --data '{"title": "Post 2", "author_id": "1"}'

mqdb delete authors 1
```

**Expected in Terminal 1:** Delete events for each cascaded post:
```
Delete posts/1
Delete posts/2
```

### Set Null

Setup:
```bash
mqdb create authors --data '{"name": "Alice"}'
# Note the returned ID (e.g., 1)
mqdb constraint add posts --fk "author_id:authors:id:set_null"
mqdb create posts --data '{"title": "Post 1", "author_id": "1"}'
mqdb create posts --data '{"title": "Post 2", "author_id": "1"}'
```

Test:
```bash
mqdb delete authors 1
mqdb list posts
```

**Expected:** Posts still exist with `author_id: null` and `_version: 2`

### Set Null with ChangeEvents

**Terminal 1 (subscriber):**
```bash
mqdb watch posts
```

**Terminal 2:**
```bash
mqdb create authors --data '{"name": "Bob"}'
# Note the returned ID (e.g., 1)
mqdb constraint add posts --fk "author_id:authors:id:set_null"
mqdb create posts --data '{"title": "Post 1", "author_id": "1"}'
mqdb create posts --data '{"title": "Post 2", "author_id": "1"}'

mqdb delete authors 1
```

**Expected in Terminal 1:** Update events for each set-null post showing `author_id: null`

---

## 6. Owner-Aware Cascade Delete

When ownership is configured, cascade deletes respect entity ownership. Cross-owned entities
(owned by a different user than the one performing the delete) are excluded from the delete set.
Their FK field is set to null instead — unless a NotNull constraint blocks the operation entirely.

These tests require authentication. All commands below assume the broker was started with
`--passwd` and `--ownership`.

### Setup

```bash
mqdb passwd alice -b alice -f /tmp/cascade-passwd
mqdb passwd bob -b bob -f /tmp/cascade-passwd
mqdb passwd admin -b admin -f /tmp/cascade-passwd

mqdb agent start --db /tmp/mqdb-cascade --bind 127.0.0.1:1883 \
    --passwd /tmp/cascade-passwd \
    --ownership "projects=userId,tasks=userId" \
    --admin-users admin
```

Create the FK constraint and seed data:

```bash
mqdb constraint add tasks --fk "projectId:projects:id:cascade" \
    --broker 127.0.0.1:1883 --user admin --pass admin

mqdb create projects --data '{"name": "P1", "userId": "alice"}' \
    --user alice --pass alice
# Note project ID → <pid>

mqdb create tasks --data '{"name": "Alice Task", "userId": "alice", "projectId": "<pid>"}' \
    --user alice --pass alice

mqdb create tasks --data '{"name": "Bob Task", "userId": "bob", "projectId": "<pid>"}' \
    --user bob --pass bob
# Note bob's task ID → <btid>
```

### Same-Owner Cascade

Alice deletes her project. Alice's task (same owner) is cascade deleted.

```bash
mqdb delete projects <pid> --user alice --pass alice
mqdb list tasks --user alice --pass alice
```

**Expected:** Alice's task is deleted. Bob's task survives with `projectId: null`.

### Cross-Owner Cascade with Null FK

```bash
mqdb read tasks <btid> --user bob --pass bob
```

**Expected:** Bob's task still exists. Its `projectId` field is `null` and `_version` is `2`.

### Cross-Owner Cascade Blocked by NotNull

Re-seed the data (fresh broker), then add a NotNull constraint on the FK field:

```bash
mqdb constraint add tasks --not-null projectId \
    --broker 127.0.0.1:1883 --user admin --pass admin
```

Now alice tries to delete her project while bob's task has a non-nullable FK:

```bash
mqdb delete projects <pid> --user alice --pass alice
```

**Expected error:**
```
cascade blocked: tasks/<btid> owned by 'bob' has non-nullable FK field 'projectId'
```

The delete is rejected entirely — no entities are modified.

### Admin Bypass (Blind Cascade)

Admin users bypass ownership filtering. The cascade deletes all referencing entities regardless
of who owns them.

```bash
mqdb delete projects <pid> --user admin --pass admin
mqdb list tasks --user admin --pass admin
```

**Expected:** All tasks are deleted (both alice's and bob's). Admin cascade is blind.

### Unowned Entity Always Cascades

If the referencing entity has no ownership configured, it is always included in the cascade
regardless of the sender. Start a broker with ownership only on projects (not tasks):

```bash
mqdb agent start --db /tmp/mqdb-cascade-unowned --bind 127.0.0.1:1883 \
    --passwd /tmp/cascade-passwd \
    --ownership "projects=userId"
```

```bash
mqdb constraint add tasks --fk "projectId:projects:id:cascade" \
    --user admin --pass admin
mqdb create projects --data '{"name": "P1", "userId": "alice"}' \
    --user alice --pass alice
mqdb create tasks --data '{"name": "T1", "projectId": "<pid>"}' \
    --user bob --pass bob

mqdb delete projects <pid> --user alice --pass alice
mqdb list tasks --user admin --pass admin
```

**Expected:** Task is deleted. Entities without ownership config are always cascade-eligible.

### Verification Checklist

- [ ] Same-owner cascade: entities owned by sender are deleted
- [ ] Cross-owner cascade: entities owned by other users survive with FK set to null
- [ ] Cross-owner + NotNull: delete is blocked with 409
- [ ] Admin cascade: blind (deletes all regardless of owner)
- [ ] Unowned entity: always included in cascade
- [ ] ChangeEvents: cross-owner survivors emit Update events (FK nulled), not Delete events
