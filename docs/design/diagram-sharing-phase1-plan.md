# Diagram Sharing — Phase 1 Implementation Plan

> **STATUS: IMPLEMENTED (historical plan).** This phase shipped in agent mode (`_shares` entity, `AccessLevel`, share-aware `check_access`). Retained for historical reference.

Phase 1 of `diagram-sharing.md`: the **grants + access core**. Delivers a complete
agent-mode vertical slice — share a diagram view/edit with a named user, grantee
reads/updates, owner-only delete, revoke, transitive cascade — plus cluster parity
for everything it touches. No child enforcement (phase 2), no event changes
(phase 3), no public/anonymous (phase 4).

## In scope

- `_shares` system entity + indexes.
- `AccessLevel` and the `check_access` refactor (owner **or** named grant).
- `$DB/{entity}/{id}/share|unshare|shares` and `$DB/{entity}/shared` topics.
- Grantee resolution (email→canonical_id) + pending grants.
- Transitive cascade over diagram→diagram references.
- Delete stays owner-only; read/update become share-aware.
- Cluster parity, built in step-by-step (not deferred).

## Explicitly deferred

- Child-entity derivation → phase 2. (Phase 1 only gates the ownership-enabled
  entity itself; children stay as they are today.)
- Recipient-scoped event routing / confidentiality → phase 3. **Events keep
  broadcasting in phase 1.** A grantee added in phase 1 can already read the
  diagram, but live-update confidentiality is a phase-3 concern.
- Public / anonymous (`_public`, anon ticket) → phase 4.

---

## Corrections from code verification (read before building)

1. **Ownership transfer is design-only, not implemented** (`docs/design/ownership-transfer.md`
   has no corresponding code). The sharing endpoints therefore follow the
   *resource-scoped suffix* pattern that `update`/`delete` already use in
   `parse_db_topic`, not an existing transfer implementation.
2. **`find_canonical_id_by_email` is HTTP-only** (`http/handlers.rs:537`, private,
   depends on `ServerState`) and **cannot be called from the MQTT request path**.
   Resolving a grantee during a `$DB/.../share` request requires refactoring that
   lookup into an MQTT-reachable helper. This is a real work item, not a detail.
3. **System entities are schema/index-registered at startup via MQTT admin
   publishes**, mirroring `initialize_identity_constraints`
   (`http/server.rs:362`): publish `$DB/_admin/constraint/{e}/add` and
   `$DB/_admin/index/{e}/add`. `_shares` registers the same way.

---

## Data model: `_shares`

Entity constant `entity::SHARES = "_shares"`. Record:

```json
{
  "id": "<uuid>",
  "resource_entity": "diagrams",
  "resource_id": "<uuid>",
  "grantee_key":   "bob@gmail.com",   // the input identifier (email or username); always present
  "grantee":       "<canonical-id | username | null-while-pending>",  // what `sender` will equal
  "permission":    "view",            // "view" | "edit"
  "granted_by":    "<owner-identity>",
  "created_at":    "<rfc3339>"
}
```

Refinement vs. the design doc: it used `grantee_email`; I split into
**`grantee_key`** (the always-present input identifier — covers password usernames
too) and **`grantee`** (the resolved match-identity, null only while a pending
OAuth grant awaits the grantee's first sign-in). `can_see`/`can_edit` match on
`grantee == sender`; a null `grantee` matches nobody, so pending grants are inert.

Indexes / constraints (registered at startup):

- index `(grantee)` — the per-operation access lookup and `/shared` discovery.
- index `(resource_entity, resource_id)` — `/shares` listing and cleanup.
- index `(grantee_key)` — pending-grant resolution on sign-in.
- unique `(resource_entity, resource_id, grantee_key)` — one grant per
  (resource, identifier); re-share updates in place. `grantee_key` is always
  present, so no null-in-unique question. (Composite unique is supported — the
  constraint payload takes a `fields` array.)

`_shares` is **not** exposed via generic CRUD topics; it is read/written only by
the share handlers below. Confirm during build: the mechanism that blocks external
`$DB/_shares/...` CRUD (system/`_`-prefixed protection) — verify `_`-prefixed
entities are already gated, or add `_shares` to the protected set.

---

## Topic / protocol API

Resource-scoped, parsed by `parse_db_topic` (`protocol/mod.rs:210`). New `DbOp`
variants and the arm ordering (the 3-part arms and `[entity,"shared"]` **must
precede** the `[entity, id]` Read catch-all):

| Topic | `DbOp` | parse arm |
|-------|--------|-----------|
| `$DB/{e}/{id}/share` | `Share` | `[e, id, "share"]` |
| `$DB/{e}/{id}/unshare` | `Unshare` | `[e, id, "unshare"]` |
| `$DB/{e}/{id}/shares` | `Shares` | `[e, id, "shares"]` |
| `$DB/{e}/shared` | `Shared` | `[e, "shared"]` (beside `create`/`list`) |

`Request` variants (`transport.rs:14`) + `build_request` parsing
(`transport.rs:253`):

```
Share   { entity, id, grantee: String, permission: String, cascade: bool }
Unshare { entity, id, grantee: String, cascade: bool }
Shares  { entity, id }
Shared  { entity }
```

Dispatch in `execute_with_sender` (`transport_execute.rs:36`) — new match arms
calling the database methods below. (Note: the existing admin endpoints `_vault`,
`_auth`, `_admin` go through `parse_admin_topic`; these resource-scoped share ops
go through `parse_db_topic`, consistent with `update`/`delete`.)

---

## Access-check refactor

Add to `mqdb-core/src/types.rs` (beside `OwnershipConfig`):

```
pub enum AccessLevel { View, Edit }   // Edit >= View
```

Replace the two share-relevant call sites of `check_ownership`
(`crud.rs:410`) with a share-aware check:

```
check_access(entity, id, owner_field, sender, required: AccessLevel) -> Result<()>:
    if is_owner(entity, id, owner_field, sender): Ok
    else match share_level(entity, id, sender):   // _shares lookup by (grantee, resource)
        Some(l) if l >= required: Ok
        _: Err(Forbidden)
```

- `is_owner` = the existing exact-match logic extracted from `check_ownership`.
- `share_level(entity, id, sender)` = read `_shares` for
  `(resource_entity=entity, resource_id=id, grantee=sender)`; return its level.
- Admin still short-circuits earlier in `OwnershipConfig::evaluate` → `Allowed`.

Wire-up in `transport_execute.rs`:

- **Read** (`:72`) → `check_access(..., View)`.
- **Update** (`:90`) → `check_access(..., Edit)`; keep stripping the owner field.
- **Delete** (`:122`) → **unchanged** (keep `check_ownership` / owner-only, per
  Decision #1).
- **List** (`:156`) → unchanged owner filter; discovery is the separate `/shared`.

Phase-1 scope note: `check_access` here covers **owner + named grant only**. Child
derivation and the public/anon branches are added in later phases — design the
signature so they slot in without a rewrite.

---

## Grantee resolution + pending grants

1. **Refactor the email lookup to be MQTT-reachable.** Extract the
   `_identity_links` email→canonical_id query (`http/handlers.rs:537`) into a
   public helper in `db_helpers.rs` (MQTT-path reachable, takes the agent's
   MQTT client / db handle, not `ServerState`). The HTTP handler then calls the
   shared helper too — no logic duplication.
2. **Resolve on `/share`:**
   - identities enabled + email found → `grantee = canonical_id`.
   - identities enabled + email unknown → **pending**: store `grantee = null`,
     `grantee_key = email`.
   - password-only deployment (no `_identities`) → `grantee = grantee_key`
     (username verbatim).
3. **Pending sweep on sign-in.** In `resolve_or_create_identity`
   (`http/handlers.rs:396`), after a canonical_id is established for a verified
   email, query `_shares` by `grantee_key = email` with `grantee = null` and fill
   in `grantee = canonical_id`. Bounded by the number of pending grants per email.

This is the highest-uncertainty work item; it crosses the HTTP/MQTT module
boundary. It can be split: **1c-i** resolve-existing-only (unknown email → 400),
**1c-ii** pending + sweep, so the core lands before pending support.

---

## Cascade

In the share/unshare handlers, walk diagram→diagram references — the registered
relationships / FKs where `source_entity == target_entity == entity`
(`relationship.rs`, `constraint.rs`). Bounded, cycle-safe BFS (already verified in
`specs/CascadeClosure.tla`):

```
const MAX_CASCADE_DIAGRAMS = 256
visited = {}; queue = [root]
while queue and |visited| < MAX_CASCADE_DIAGRAMS:
    id = pop; if id in visited: continue; visited.add(id)
    for f in self_reference_fields(entity):
        r = record(entity,id)[f]; if r and r not in visited: push(r)
upsert a grant for each id in visited
```

Level rule (verified in `specs/GrantLifecycle.tla`): **direct share = set-to-level**
(may demote); **cascade = max-of-levels** (never downgrades an existing grant).
`unshare --cascade` removes across the same closure.

---

## Cluster parity (built incrementally, not deferred)

For each piece above, mirror it on the cluster path in the same step — the two DB
paths handle the same payloads in separate code (agent/cluster handler parity), and
deferring all cluster work is where parity bugs hide. Touchpoints:

- `cluster_agent/admin.rs` — dispatch the new `Request::Share|Unshare|Shares|Shared`.
- `cluster/node_controller/db_ops.rs` — route `_shares` reads/writes and the access
  check to the **resource's** partition primary (a grant is checked where the
  resource lives).
- `cluster/db_handler/` — apply `check_access` on the partitioned read/update path.

`_shares` is a partitioned entity; the access check reads it on the resource's
primary. Confirm the routing key during build.

---

## Ordered steps (each independently testable)

1. **1a — types + `_shares` registration.** `AccessLevel`; `entity::SHARES`;
   startup index/constraint registration; system-entity protection for `_shares`.
2. **1b — protocol/transport.** `DbOp` + `Request` variants; `parse_db_topic` arms
   (precedence!); `build_request`. Unit-test parsing in isolation.
3. **1c — grant store + access core.** `is_owner`, `share_level`, grant
   create/clear/list in `crud.rs`; `check_access`; wire Read/Update; delete
   unchanged. Agent-mode test: owner + manually-inserted grant → read/update gated.
4. **1d — share/unshare/shares/shared handlers** (agent) with grantee resolution
   (1c-i resolve-existing; 1c-ii pending + sweep).
5. **1e — cascade** in share/unshare.
6. **1f — cluster parity** for 1b–1e; `_shares` orphan cleanup on diagram delete.

Frontend hand-off: the client wrapper (`shareDiagram`, `unshareDiagram`,
`listDiagramShares`, `listSharedWithMe`) wraps these MQTT topics — broker-side
feature; the embedded-only WASM DB path does not participate.

---

## Tests (agent + 3-node cluster)

- view grant → grantee reads, update 403; edit grant → reads + updates; delete 403
  for both (owner-only).
- direct re-share view demotes an edit grant; cascade does not downgrade.
- cascade over A→B→A shares {A,B} once; respects `MAX_CASCADE_DIAGRAMS`.
- grantee resolution: email→canonical_id; unknown→pending; pending filled on first
  sign-in; password deployment uses username verbatim.
- `/shared` returns exactly the caller's grants; `/shares` lists a resource's grants
  (owner/admin only); non-owner cannot share.
- revoke removes access on next op.
- delete diagram removes its `_shares` rows.
- `_shares` is not reachable via generic `$DB/_shares/...` CRUD.

Extend `mqdb dev test` with a `--sharing` scenario for the agent + cluster matrix.

---

## Decisions / risks to confirm during build

- **System-entity CRUD protection** — verify `_`-prefixed entities are already
  blocked from external generic CRUD, or add `_shares` explicitly.
- **Composite-unique null semantics** — using `grantee_key` (always present) as the
  unique component avoids it; confirm the constraint engine accepts composite
  `fields`.
- **Email-lookup refactor** — the HTTP→shared-helper extraction is the main
  cross-cutting change; confirm `db_helpers.rs` has (or can take) the db/client
  handle needed from the MQTT path.
- **`_shares` partition routing key** in cluster mode — confirm the resource's
  primary is the right home for its grants.
