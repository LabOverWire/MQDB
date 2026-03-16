# Frontend Agent: MQDB Integration Tasks

## ⚠️ CORRECTION: Server Assumptions Changed

**The original plan incorrectly promised server-side features that will NOT be implemented.** MQDB is generic infrastructure and will not have diagram-specific logic.

### What the Server Will NOT Provide

| Feature | Status | Reason |
|---------|--------|--------|
| `diagrams/{id}/mutations` topic | ❌ NOT PROVIDED | Diagram-specific logic doesn't belong in MQDB |
| `seq` field in mutations | ❌ NOT PROVIDED | Server doesn't track diagram sequences |
| `author_client_id` in mutations | ❌ NOT PROVIDED | Server doesn't inject this |
| `up_to_seq` in list responses | ❌ NOT PROVIDED | Server doesn't track diagram sequences |

### What the Server DOES Provide

| Feature | Status | Topic/Usage |
|---------|--------|-------------|
| WebSocket endpoint | ✅ | `ws://host:8080/mqtt` |
| CRUD operations | ✅ | `$DB/create/{entity}`, `$DB/read/{entity}/{id}`, `$DB/update/{entity}/{id}`, `$DB/delete/{entity}/{id}` |
| List with filters | ✅ | `$DB/list/{entity}` with `{"filter": "diagramId=xyz"}` |
| Entity watch | ✅ | `$DB/watch/{entity}` - notifies on any change to entity type |

---

## Revised Sync Protocol: Frontend-Managed Sequences

The TLA+ bugs are still real. The fixes must be implemented **entirely in the frontend** using existing MQDB features.

### The Key Insight

Your schema already has `diagrams.version`. Use it:

1. **`diagrams.version` IS the sequence number**
2. **Frontend increments it** when mutating child entities
3. **Frontend reads it** when fetching initial state
4. **Frontend filters by it** to prevent duplicate application

### Revised SyncClient

```typescript
// src/services/mqdb/SyncClient.ts

interface Mutation {
  op: 'insert' | 'update' | 'delete';
  entity: string;
  id: string;
  data: Record<string, unknown> | null;
}

export class SyncClient {
  private mqtt: MqttClient | null = null;
  private clientId: string;

  // Bug #1 fix: Track subscribed diagrams
  private subscribedDiagrams = new Set<string>();

  // Bug #2 fix: Track applied versions PER DIAGRAM
  private appliedVersion = new Map<string, number>();

  // Bug #2 fix: Buffer mutations while awaiting state
  private buffered = new Map<string, Mutation[]>();
  private awaitingState = new Set<string>();

  private onMutation: ((diagramId: string, mutation: Mutation) => void) | null = null;

  constructor(clientId: string) {
    this.clientId = clientId;
  }

  async connect(serverUrl: string): Promise<void> {
    this.mqtt = await MqttClient.connect({
      url: serverUrl,
      clientId: this.clientId,
      cleanSession: true,
    });

    this.mqtt.onMessage((topic, payload) => {
      this.handleMessage(topic, payload);
    });

    this.mqtt.onDisconnect(() => {
      this.subscribedDiagrams.clear();
      this.appliedVersion.clear();
      this.buffered.clear();
      this.awaitingState.clear();
    });
  }

  setMutationHandler(handler: (diagramId: string, mutation: Mutation) => void): void {
    this.onMutation = handler;
  }

  // ─────────────────────────────────────────────────────────────
  // Diagram Lifecycle (REVISED)
  // ─────────────────────────────────────────────────────────────

  async openDiagram(diagramId: string): Promise<void> {
    if (!this.mqtt) throw new Error('Not connected');

    // 1. Mark as awaiting state (mutations will be buffered)
    this.awaitingState.add(diagramId);
    this.buffered.set(diagramId, []);

    // 2. Subscribe to entity watches (NOT diagram-specific mutation topic)
    this.subscribedDiagrams.add(diagramId);
    await this.mqtt.subscribe('$DB/watch/nodes', { qos: 1 });
    await this.mqtt.subscribe('$DB/watch/edges', { qos: 1 });
    await this.mqtt.subscribe('$DB/watch/topics', { qos: 1 });
    await this.mqtt.subscribe('$DB/watch/schemas', { qos: 1 });
    await this.mqtt.subscribe('$DB/watch/variables', { qos: 1 });

    // 3. Fetch diagram (includes version) and all child entities
    const diagram = await this.fetchOne('diagrams', diagramId);
    const [nodes, edges, topics, schemas, variables] = await Promise.all([
      this.fetchList('nodes', `diagramId=${diagramId}`),
      this.fetchList('edges', `diagramId=${diagramId}`),
      this.fetchList('topics', `diagramId=${diagramId}`),
      this.fetchList('schemas', `diagramId=${diagramId}`),
      this.fetchList('variables', `diagramId=${diagramId}`),
    ]);

    // 4. Record the version we've synced to
    const version = diagram?.data?.version || 0;
    this.appliedVersion.set(diagramId, version);

    // 5. Apply state to local DB (handled by caller)
    // ... caller applies nodes, edges, etc. to local DB ...

    // 6. Process any buffered mutations
    // Note: Without server-side seq, we can't perfectly dedupe.
    // We rely on idempotent operations and version comparison.
    const buffer = this.buffered.get(diagramId) || [];
    for (const mutation of buffer) {
      // Only apply if mutation is for our diagram
      const mutationDiagramId = mutation.data?.diagramId;
      if (mutationDiagramId === diagramId) {
        this.applyMutation(diagramId, mutation);
      }
    }

    // 7. Clear buffer, mark as ready
    this.buffered.delete(diagramId);
    this.awaitingState.delete(diagramId);
  }

  async closeDiagram(diagramId: string): Promise<void> {
    this.subscribedDiagrams.delete(diagramId);
    this.buffered.delete(diagramId);
    this.awaitingState.delete(diagramId);
    this.appliedVersion.delete(diagramId);

    // Unsubscribe only if no other diagrams open
    if (this.subscribedDiagrams.size === 0) {
      await this.mqtt?.unsubscribe('$DB/watch/nodes');
      await this.mqtt?.unsubscribe('$DB/watch/edges');
      await this.mqtt?.unsubscribe('$DB/watch/topics');
      await this.mqtt?.unsubscribe('$DB/watch/schemas');
      await this.mqtt?.unsubscribe('$DB/watch/variables');
    }
  }

  // ─────────────────────────────────────────────────────────────
  // Mutation Handling (REVISED)
  // ─────────────────────────────────────────────────────────────

  private handleMessage(topic: string, payload: Uint8Array): void {
    // Parse $DB/watch/{entity} messages
    const match = topic.match(/^\$DB\/watch\/(\w+)$/);
    if (!match) return;

    const entity = match[1];
    const mutation = JSON.parse(new TextDecoder().decode(payload)) as Mutation;

    // Extract diagramId from mutation data
    const diagramId = mutation.data?.diagramId as string | undefined;
    if (!diagramId) return; // Not a diagram-scoped entity

    // ═══════════════════════════════════════════════════════════
    // BUG #1 FIX: Ignore mutations for unsubscribed diagrams
    // ═══════════════════════════════════════════════════════════
    if (!this.subscribedDiagrams.has(diagramId)) {
      return;
    }

    // If awaiting initial state, buffer the mutation
    if (this.awaitingState.has(diagramId)) {
      this.buffered.get(diagramId)?.push(mutation);
      return;
    }

    // Apply the mutation
    this.applyMutation(diagramId, mutation);
  }

  private applyMutation(diagramId: string, mutation: Mutation): void {
    this.onMutation?.(diagramId, mutation);
  }

  // ─────────────────────────────────────────────────────────────
  // CRUD Operations - Frontend MUST bump diagram.version
  // ─────────────────────────────────────────────────────────────

  async createNode(diagramId: string, data: Record<string, unknown>): Promise<string> {
    // 1. Create the node
    const nodeData = { ...data, diagramId };
    const response = await this.request('$DB/create/nodes', nodeData);
    const nodeId = response.id;

    // 2. Bump diagram version (CRITICAL for sync protocol)
    await this.bumpDiagramVersion(diagramId);

    return nodeId;
  }

  async updateNode(diagramId: string, id: string, data: Record<string, unknown>): Promise<void> {
    await this.request(`$DB/update/nodes/${id}`, data);
    await this.bumpDiagramVersion(diagramId);
  }

  async deleteNode(diagramId: string, id: string): Promise<void> {
    await this.request(`$DB/delete/nodes/${id}`, {});
    await this.bumpDiagramVersion(diagramId);
  }

  // Similar for edges, topics, schemas, variables...

  private async bumpDiagramVersion(diagramId: string): Promise<void> {
    // Fetch current version
    const diagram = await this.fetchOne('diagrams', diagramId);
    const currentVersion = diagram?.data?.version || 0;
    const newVersion = currentVersion + 1;

    // Update diagram with incremented version
    await this.request(`$DB/update/diagrams/${diagramId}`, { version: newVersion });

    // Update local tracking
    this.appliedVersion.set(diagramId, newVersion);
  }

  // ─────────────────────────────────────────────────────────────
  // Helpers
  // ─────────────────────────────────────────────────────────────

  private async fetchOne(entity: string, id: string): Promise<any> {
    return this.request(`$DB/read/${entity}/${id}`, {});
  }

  private async fetchList(entity: string, filter: string): Promise<any[]> {
    const response = await this.request(`$DB/list/${entity}`, { filter });
    return response.data || [];
  }

  private async request(topic: string, payload: unknown): Promise<any> {
    const responseTopic = `responses/${this.clientId}/${Date.now()}`;
    await this.mqtt!.subscribe(responseTopic, { qos: 1 });

    await this.mqtt!.publish(topic, JSON.stringify(payload), {
      qos: 1,
      properties: { responseTopic },
    });

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('Request timeout')), 10000);

      const handler = (topic: string, payload: Uint8Array) => {
        if (topic === responseTopic) {
          clearTimeout(timeout);
          this.mqtt!.unsubscribe(responseTopic);
          resolve(JSON.parse(new TextDecoder().decode(payload)));
        }
      };

      this.mqtt!.onMessage(handler);
    });
  }
}
```

---

## Key Changes from Original Plan

| Original Plan | Revised Approach |
|---------------|------------------|
| Subscribe to `diagrams/{id}/mutations` | Subscribe to `$DB/watch/nodes`, `$DB/watch/edges`, etc. |
| Server provides `seq` in mutations | Frontend uses `diagram.version` as the sequence |
| Server provides `up_to_seq` in list responses | Frontend reads `diagram.version` separately |
| Server provides `author_client_id` | Not needed - see note below |

### About `author_client_id`

The original plan used `author_client_id` to skip applying your own mutations. Without server support, you have two options:

1. **Optimistic updates with local tracking**: Track IDs of entities you've created/modified locally, skip applying watch notifications for those IDs for a short window
2. **Idempotent operations**: Design your local state updates to be idempotent (applying the same mutation twice has no additional effect)

---

## Bug #2 Fix: Revised Approach

Without server-side sequence numbers, Bug #2 (state regression during initial sync) is harder to fix perfectly. The revised approach:

1. **Buffer mutations while fetching state** (same as before)
2. **Read `diagram.version` from fetched state**
3. **After applying state, apply buffered mutations**
4. **Accept that some mutations may be applied twice** - design for idempotency

The `diagram.version` bump on each child mutation provides ordering, but since the server doesn't inject it into watch notifications, you can't filter precisely. The key protection is:
- Buffering prevents mutations from being applied before state
- Idempotent local operations prevent corruption from double-apply

---

## Watch Notification Format

The `$DB/watch/{entity}` topics publish notifications in this format:

```json
{
  "op": "insert",
  "entity": "nodes",
  "id": "node-123",
  "data": { "diagramId": "d1", "type": "broker", "positionX": 100, "positionY": 200 }
}
```

Note: No `seq`, no `author_client_id`. Filter by `data.diagramId`.

---

## Definition of Done (Revised)

- [ ] SyncClient subscribes to `$DB/watch/{entity}` (not `diagrams/{id}/mutations`)
- [ ] SyncClient filters by `data.diagramId` to route to correct diagram
- [ ] SyncClient implements Bug #1 fix (ignore unsubscribed diagrams)
- [ ] SyncClient buffers mutations during initial state fetch
- [ ] CRUD operations bump `diagram.version` after each mutation
- [ ] Local operations are idempotent (safe to apply twice)
- [ ] All tests in checklist pass
