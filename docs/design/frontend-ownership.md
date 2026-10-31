# Frontend Integration: Ownership Enforcement

## What Changed (vs the Original Hypothesis)

The original discussion proposed **user-scoped topic namespacing** (`$DB/u/{userId}/...`) with ACL enforcement at the topic level. That approach was abandoned. Instead, MQDB now implements **server-side ownership enforcement** at the database layer. This is transparent to the frontend — the topic structure does not change.

### What the Frontend Does NOT Need to Do

- No topic restructuring (no `$DB/u/{userId}/...` prefix)
- No ACL file management per user
- No user provisioning hooks
- No changes to event subscription topics, unless the server enables `--scoped-events` (see [Event Subscriptions](#event-subscriptions))

### What the Frontend Needs to Do

1. **Add a `userId` field to diagrams on create**
2. **Authenticate with real credentials** (not anonymous mode)
3. That's it. The server handles everything else.

---

## How It Works

When a user authenticates via MQTT CONNECT (password, SCRAM, JWT, or OAuth), the broker injects their identity as `x-mqtt-sender` (the authenticated user id; see [Authenticate with Real Credentials](#2-authenticate-with-real-credentials)) and `x-mqtt-client-id` (MQTT client ID) user properties on every PUBLISH they send. These are anti-spoof: the broker strips any client-supplied values before injecting the real ones. The database layer reads `x-mqtt-sender` for ownership enforcement and `x-mqtt-client-id` for echo suppression in change events (see `../architecture.md` Section 16 for the full property flow). The ownership layer uses `x-mqtt-sender` to:

- **List operations**: Automatically injects a mandatory filter `userId = {authenticated_user}`. Alice only sees her own diagrams. Bob only sees his.
- **Read/Update/Delete operations**: Reads the existing record and allows the request when `data.userId == sender`. Otherwise read is allowed with a `view` or `edit` share grant, update only with an `edit` grant, and delete never (agent: `transport_execute.rs` calls `crud.rs` `check_access` for read and update and the owner-only `check_ownership` for delete; cluster: `check_local_share_access` in `node_controller/sharing.rs` for reads and updates graded on the node that received them, `check_forwarded_ownership` in `node_controller/db_ops.rs` for forwarded requests, and `check_cluster_ownership` in `db_handler/json_ops.rs` for deletes). A user without the required access gets 403; a record that does not exist returns 404 in agent mode, while in cluster mode a non-admin can get 403 for it. Read is **not** unrestricted.
- **Missing/null owner**: A record whose owner field is absent or null is **protected** — `None == Some(sender)` is false, so a non-admin without a share grant gets 403 (an admin can share such a record, and the grantee then has the granted access; delete stays admin-only). The not-owned treatment holds uniformly: the direct Read/Update/Delete path and the FK cascade path both treat a null/missing owner as not-owned (cascade set-nulls it instead of deleting). See [distributed-design.md → Owner-Aware Cascade](../distributed-design.md).
- **Create operations**: No restriction on ownership-enabled entities; the frontend sets `userId` in the data payload. A child entity mapped with `--ownership-derive` (agent mode only) needs `edit` access to its parent to be created.
- **Admin users**: Bypass all ownership checks. Admins see all records and can read/update/delete anything.
- **Internal operations**: When `sender` is absent (replication, outbox, internal handlers), all checks are bypassed.

### MQTT Topic Structure (Unchanged)

```
$DB/diagrams/create              → Create a diagram
$DB/diagrams/list                → List diagrams (filtered by ownership)
$DB/diagrams/{id}                → Read a diagram
$DB/diagrams/{id}/update         → Update a diagram (ownership checked)
$DB/diagrams/{id}/delete         → Delete a diagram (ownership checked)
$DB/diagrams/events/#            → Subscribe to diagram changes (unchanged unless --scoped-events is on)
```

By default all other entities (nodes, edges, topics, schemas, variables, groups) work exactly as before, with no ownership enforcement. Only entities configured with `--ownership` get ownership checks, plus child entities mapped to a parent with `--ownership-derive` (agent mode only; see [diagram-sharing.md](diagram-sharing.md#child-entity-enforcement)).

---

## Frontend Changes Required

### 1. Add `userId` to Diagram Creation

When creating a diagram, include the authenticated user's ID in the data payload:

```typescript
// Before (no ownership)
const payload = { title: "My Diagram" };

// After (with ownership)
const payload = { title: "My Diagram", userId: authenticatedUserId };
```

The `userId` field must match the sender identity: the CONNECT username for password auth, the SCRAM username (the `n=` attribute of the SCRAM exchange) for SCRAM, the JWT `sub` claim for `--jwt-algorithm` auth (including OAuth, where `sub` is the user's canonical id from `_identities`, not their email), or `<issuer prefix>:<sub>` for `--federated-jwt-config` auth (the prefix defaults to the issuer's host).

### 2. Authenticate with Real Credentials

Anonymous mode does not support ownership enforcement because there is no sender identity. The frontend must authenticate with one of:

- **Password auth**: MQTT CONNECT with `username` and `password`
- **SCRAM-SHA-256**: MQTT CONNECT with SCRAM exchange
- **JWT**: MQTT v5 enhanced authentication with method `JWT` and the token as the authentication data (a token sent as the CONNECT password is rejected)
- **OAuth**: Via the HTTP/OAuth flow that issues JWT tokens

The sender identity for ownership checks is the CONNECT username for password auth, the SCRAM username for SCRAM, the token's `sub` claim for `--jwt-algorithm` auth (including OAuth tickets), and `<issuer prefix>:<sub>` for `--federated-jwt-config` auth (the prefix defaults to the issuer's host).

### 3. Handle 403 Forbidden Responses

When a user without access reads, updates or deletes a record, the server returns (the message is `permission denied` in cluster mode):

```json
{
  "status": "error",
  "code": 403,
  "message": "forbidden"
}
```

The frontend should handle this gracefully — e.g., show a "you don't have permission" message. This should be rare if the UI only shows the user's own diagrams (which it will, since list is filtered).

### 4. No Changes to Child Entities

Child entities (nodes, edges, topics, schemas, variables, groups) do NOT need a `userId` field. By default their isolation rests only on diagram ids being hard to find, not on any server check:

1. User lists diagrams → only sees their own (ownership filter); diagrams shared with them come from `$DB/diagrams/shared`
2. User fetches nodes for a diagram → fetches by `diagramId` (one they own or were granted)
3. Other users can't discover diagram IDs (UUIDs, not enumerable)

This means no changes to node/edge/topic creation or event subscriptions. Child records themselves are not access-controlled: any authenticated user who knows a child's id can read, edit or delete it, unless the server maps the child entity with `--ownership-derive`, which grades child access against the parent diagram. Child events stay on the shared topic unless the child is mapped with `--ownership-derive` and `--scoped-events` is also on; then they go only to the diagram's owner and grantees. Both flags are agent mode only.

---

## Server Configuration

The server is started with:

```bash
# Agent mode
mqdb agent start --db /path/to/db --bind 0.0.0.0:1883 \
    --passwd passwd.txt --admin-users admin \
    --ownership diagrams=userId

# Cluster mode
mqdb cluster start --node-id 1 --bind 0.0.0.0:1883 --db /path/to/db \
    --passwd passwd.txt --admin-users admin \
    --ownership diagrams=userId \
    --quic-cert server.pem --quic-key server.key
```

Key flags:
- `--ownership diagrams=userId` — enforces ownership on the `diagrams` entity using the `userId` field
- `--admin-users admin` — the `admin` user bypasses all ownership checks
- `--passwd passwd.txt` — password file for authentication (or use `--scram-file`, `--jwt-*`, OAuth)

Multiple entities can be configured: `--ownership "diagrams=userId,projects=ownerId"`

---

## Authentication Setup

### Creating Users

```bash
# Create password entries for users
mqdb passwd alice@gmail.com -b alice_password -f passwd.txt
mqdb passwd bob@gmail.com -b bob_password -f passwd.txt
mqdb passwd admin -b admin_password -f passwd.txt
```

### OAuth Flow (Production)

For Google OAuth:
1. Server starts with `--http-bind 0.0.0.0:8080 --oauth-client-secret <file> --jwt-algorithm hs256 --jwt-key <file of at least 32 bytes> --jwt-audience <Google client id>` (plus `--oauth-redirect-uri` if the callback is not `http://localhost:{port}/oauth/callback`). `--jwt-key` signs the tickets and is required; `--jwt-audience` supplies the Google client id.
2. Frontend sends the browser to `GET /oauth/authorize?provider=google`
3. `GET /oauth/callback` sets a session cookie, then `POST /auth/ticket` returns a short-lived JWT
4. Frontend connects with MQTT v5 enhanced authentication, method `JWT`, with that JWT as the authentication data
5. The `sub` claim (the canonical id, not the email) becomes the sender identity

---

## Admin Access

Admin users (specified via `--admin-users`) bypass all ownership restrictions:

| Operation | Regular User | Admin User |
|-----------|-------------|------------|
| List diagrams | Only own diagrams (diagrams shared with them come from `$DB/diagrams/shared`) | All diagrams |
| Read diagram | Own diagrams, or ones shared with them at `view` or `edit` (403 otherwise) | Any diagram |
| Create diagram | Allowed | Allowed |
| Update diagram | Own diagrams, or ones shared with them at `edit` (403 otherwise) | Any diagram |
| Delete diagram | Only own diagrams (403 otherwise), even with an `edit` grant | Any diagram |

---

## Event Subscriptions

Recipient-scoped events shipped. For ownership-enabled entities, change events are routed to per-recipient topics `$DB/u/{user}/events/{entity}/{id}` (see `database/sharing.rs` `event_recipients` and `agent/tasks.rs`), enabled via the `--scoped-events` flag (`MQDB_SCOPED_EVENTS`, agent mode only; cluster: #211). Each subscriber subscribes to its own `$DB/u/{user}/events/#` namespace, and the built-in topic protection (`topic_protection.rs`) refuses a non-admin's subscription to another user's namespace. With `--scoped-events` on, the legacy shared `$DB/{entity}/events/#` topic remains only for entities with neither an owner field nor an `--ownership-derive` mapping; `diagrams` events no longer go there.

---

## Testing

### Quick Verification

```bash
# Start server with ownership
mqdb agent start --db /tmp/test-ownership --bind 127.0.0.1:1883 \
    --passwd passwd.txt --admin-users admin \
    --ownership diagrams=userId

# Create as alice
mqdb create diagrams -d '{"userId":"alice@gmail.com","title":"Alice Diagram"}' \
    --broker 127.0.0.1:1883 --user alice@gmail.com --pass alice_password

# List as alice (should see 1)
mqdb list diagrams --broker 127.0.0.1:1883 --user alice@gmail.com --pass alice_password

# List as bob (should see 0)
mqdb list diagrams --broker 127.0.0.1:1883 --user bob@gmail.com --pass bob_password

# List as admin (should see 1)
mqdb list diagrams --broker 127.0.0.1:1883 --user admin --pass admin_password
```

### Automated E2E Test

```bash
# Runs 9 automated tests covering create, list, update, delete, admin bypass
mqdb dev test --ownership --nodes 3 --license /path/to/license.key
```

---

## Summary

| Concern | Resolution |
|---------|-----------|
| Topic restructuring | Not needed — server-side enforcement |
| ACL per user | Not needed for ownership — only if you want topic-level isolation |
| `userId` field | Add to `diagrams` entity on create |
| Authentication | Required — anonymous mode incompatible |
| Child entity isolation | No `userId` needed; enforced against the parent only with `--ownership-derive` (agent mode only) |
| Admin access | `--admin-users` flag, bypasses all ownership |
| Event filtering | Recipient-scoped events available via `--scoped-events` (`$DB/u/{user}/events/#`, agent mode only) |
| Sharing | Implemented in agent mode; partial in cluster mode — see [diagram-sharing.md](diagram-sharing.md) |
