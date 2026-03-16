# Frontend Integration: Ownership Enforcement

## What Changed (vs the Original Hypothesis)

The original discussion proposed **user-scoped topic namespacing** (`$DB/u/{userId}/...`) with ACL enforcement at the topic level. That approach was abandoned. Instead, MQDB now implements **server-side ownership enforcement** at the database layer. This is transparent to the frontend — the topic structure does not change.

### What the Frontend Does NOT Need to Do

- No topic restructuring (no `$DB/u/{userId}/...` prefix)
- No ACL file management per user
- No user provisioning hooks
- No changes to event subscription topics

### What the Frontend Needs to Do

1. **Add a `userId` field to diagrams on create**
2. **Authenticate with real credentials** (not anonymous mode)
3. That's it. The server handles everything else.

---

## How It Works

When a user authenticates via MQTT CONNECT (password, SCRAM, JWT, or OAuth), the broker injects their identity as `x-mqtt-sender` (username) and `x-mqtt-client-id` (MQTT client ID) user properties on every PUBLISH they send. These are anti-spoof: the broker strips any client-supplied values before injecting the real ones. The database layer reads `x-mqtt-sender` for ownership enforcement and `x-mqtt-client-id` for echo suppression in change events (see `ARCHITECTURE.md` Section 16 for the full property flow). The ownership layer uses `x-mqtt-sender` to:

- **List operations**: Automatically injects a mandatory filter `userId = {authenticated_user}`. Alice only sees her own diagrams. Bob only sees his.
- **Update/Delete operations**: Reads the existing record, checks `data.userId == sender`. If mismatch, returns a 403 Forbidden error.
- **Create operations**: No restriction. The frontend sets `userId` in the data payload.
- **Read operations**: No restriction. IDs are UUIDs, not guessable.
- **Admin users**: Bypass all ownership checks. Admins see all records and can update/delete anything.
- **Internal operations**: When `sender` is absent (replication, outbox, internal handlers), all checks are bypassed.

### MQTT Topic Structure (Unchanged)

```
$DB/diagrams/create              → Create a diagram
$DB/diagrams/list                → List diagrams (filtered by ownership)
$DB/diagrams/{id}                → Read a diagram
$DB/diagrams/{id}/update         → Update a diagram (ownership checked)
$DB/diagrams/{id}/delete         → Delete a diagram (ownership checked)
$DB/diagrams/events/#            → Subscribe to diagram changes (unchanged)
```

All other entities (nodes, edges, topics, schemas, variables, groups) continue to work exactly as before — no ownership enforcement on them. Only entities configured with `--ownership` get ownership checks.

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

The `userId` field must match the MQTT username the frontend authenticates with. This is the same value that comes from OAuth (email or sub claim).

### 2. Authenticate with Real Credentials

Anonymous mode does not support ownership enforcement because there is no sender identity. The frontend must authenticate with one of:

- **Password auth**: MQTT CONNECT with `username` and `password`
- **SCRAM-SHA-256**: MQTT CONNECT with SCRAM exchange
- **JWT**: MQTT CONNECT with JWT token as password
- **OAuth**: Via the HTTP/OAuth flow that issues JWT tokens

The username used in MQTT CONNECT becomes the sender identity for ownership checks.

### 3. Handle 403 Forbidden Responses

When a non-owner attempts to update or delete a record, the server returns:

```json
{
  "status": "error",
  "code": 403,
  "error": "user 'bob@gmail.com' does not own diagrams/abc-123"
}
```

The frontend should handle this gracefully — e.g., show a "you don't have permission" message. This should be rare if the UI only shows the user's own diagrams (which it will, since list is filtered).

### 4. No Changes to Child Entities

Child entities (nodes, edges, topics, schemas, variables, groups) do NOT need a `userId` field. They inherit isolation through their `diagramId` foreign key:

1. User lists diagrams → only sees their own (ownership filter)
2. User fetches nodes for a diagram → fetches by `diagramId` (which they own)
3. Other users can't discover diagram IDs (UUIDs, not enumerable)

This means no changes to node/edge/topic creation or event subscriptions.

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
1. Server starts with `--http-bind 0.0.0.0:8080 --google-client-id <ID> --google-client-secret <SECRET>`
2. Frontend redirects to `/auth/google`
3. OAuth callback issues JWT token
4. Frontend uses JWT token as MQTT password
5. The `sub` claim (or email) becomes the sender identity

---

## Admin Access

Admin users (specified via `--admin-users`) bypass all ownership restrictions:

| Operation | Regular User | Admin User |
|-----------|-------------|------------|
| List diagrams | Only own diagrams | All diagrams |
| Read diagram | Allowed (by UUID) | Allowed |
| Create diagram | Allowed | Allowed |
| Update diagram | Only own diagrams (403 otherwise) | Any diagram |
| Delete diagram | Only own diagrams (403 otherwise) | Any diagram |

---

## Event Subscriptions

Event subscriptions (`$DB/diagrams/events/#`) are NOT filtered by ownership. All authenticated users receive events for all diagrams. This is intentional:

- The frontend already filters locally based on which diagrams it loaded
- Events for diagrams the user doesn't own are harmless (they won't have local state for those IDs)
- Future: ACL rules can restrict event subscriptions if needed (`%u` substitution is available)

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
mqdb dev test --ownership --nodes 3
```

---

## Summary

| Concern | Resolution |
|---------|-----------|
| Topic restructuring | Not needed — server-side enforcement |
| ACL per user | Not needed for ownership — only if you want topic-level isolation |
| `userId` field | Add to `diagrams` entity on create |
| Authentication | Required — anonymous mode incompatible |
| Child entity isolation | Inherited through `diagramId` FK — no `userId` needed |
| Admin access | `--admin-users` flag, bypasses all ownership |
| Event filtering | Not server-filtered — frontend filters locally |
| Sharing (future) | Can be added later by making ownership check look at a `sharedWith` array |
