# Frontend Integration Reference

Purpose: how a browser frontend performs CRUD and consumes change events over MQDB's MQTT-over-WebSocket API. Audience: frontend developers building the diagramming client on top of MQDB.

## Status: current

MQDB is generic infrastructure. It stores and syncs data but has no diagram-specific logic. Sequence/version tracking for the sync protocol is the frontend's responsibility.

### What the Server Provides

| Feature | Topic / Usage |
|---------|---------------|
| WebSocket endpoint | `ws://host:8080/mqtt` |
| Create | `$DB/{entity}/create` |
| Read | `$DB/{entity}/{id}` |
| Update | `$DB/{entity}/{id}/update` |
| Delete | `$DB/{entity}/{id}/delete` |
| List with filters | `$DB/{entity}/list` with `{"filter": "diagramId=xyz"}` |
| Change events | `$DB/{entity}/events/#` — notifies on any change to that entity type |

The server does not provide diagram-specific mutation topics, `seq`/`up_to_seq` fields, or `author_client_id`. Echo suppression uses the `operation_id` present in each event.

---

## Change Event Format

The broker publishes a serialized `ChangeEvent` to `$DB/{entity}/events/{id}` on every write. Subscribe with `$DB/{entity}/events/#`.

```json
{
  "sequence": 42,
  "entity": "nodes",
  "id": "node-123",
  "operation": "Create",
  "data": { "diagramId": "d1", "type": "broker" }
}
```

- `operation`: `"Create"`, `"Update"`, or `"Delete"` (capitalized)
- `data`: present on Create/Update, absent on Delete
- Filter events by `data.diagramId` to route to the correct open diagram

---

## Sync Protocol: Frontend-Managed Versions

Your schema already has `diagrams.version`. Use it as the sequence number:

1. **`diagrams.version` IS the sequence** — frontend increments it on every child mutation.
2. **On initial open** — fetch the diagram (reads `version`) and its child entities, then record the version.
3. **Buffer events during fetch** — hold incoming events until initial state is applied to avoid state regression.
4. **Idempotent apply** — since the server does not inject a per-event sequence, design local applies to be safe if applied twice.

### Bug fixes this protocol addresses

- **Stale-diagram events**: ignore events whose `data.diagramId` is not currently subscribed.
- **State regression during sync**: buffer events until the fetched state is applied, then drain the buffer.

---

## SyncClient (concise)

```typescript
interface ChangeEvent {
  sequence: number;
  entity: string;
  id: string;
  operation: 'Create' | 'Update' | 'Delete';
  data?: Record<string, unknown>;
}

export class SyncClient {
  private mqtt: MqttClient | null = null;
  private subscribed = new Set<string>();
  private awaiting = new Set<string>();
  private buffered = new Map<string, ChangeEvent[]>();
  private appliedVersion = new Map<string, number>();
  private onEvent: ((diagramId: string, ev: ChangeEvent) => void) | null = null;

  constructor(private clientId: string) {}

  async connect(url: string): Promise<void> {
    this.mqtt = await MqttClient.connect({ url, clientId: this.clientId, cleanSession: true });
    this.mqtt.onMessage((topic, payload) => this.handleMessage(topic, payload));
  }

  setEventHandler(h: (diagramId: string, ev: ChangeEvent) => void): void {
    this.onEvent = h;
  }

  async openDiagram(diagramId: string): Promise<void> {
    if (!this.mqtt) throw new Error('Not connected');
    this.awaiting.add(diagramId);
    this.buffered.set(diagramId, []);
    this.subscribed.add(diagramId);

    for (const e of ['nodes', 'edges', 'topics', 'schemas', 'variables']) {
      await this.mqtt.subscribe(`$DB/${e}/events/#`, { qos: 1 });
    }

    const diagram = await this.request(`$DB/diagrams/${diagramId}`, {});
    this.appliedVersion.set(diagramId, diagram?.data?.version ?? 0);
    // caller applies fetched child entities to local state here

    for (const ev of this.buffered.get(diagramId) ?? []) {
      if (ev.data?.diagramId === diagramId) this.onEvent?.(diagramId, ev);
    }
    this.buffered.delete(diagramId);
    this.awaiting.delete(diagramId);
  }

  private handleMessage(topic: string, payload: Uint8Array): void {
    if (!/^\$DB\/\w+\/events\//.test(topic)) return;
    const ev = JSON.parse(new TextDecoder().decode(payload)) as ChangeEvent;
    const diagramId = ev.data?.diagramId as string | undefined;
    if (!diagramId || !this.subscribed.has(diagramId)) return;
    if (this.awaiting.has(diagramId)) {
      this.buffered.get(diagramId)?.push(ev);
      return;
    }
    this.onEvent?.(diagramId, ev);
  }

  async createNode(diagramId: string, data: Record<string, unknown>): Promise<string> {
    const res = await this.request('$DB/nodes/create', { ...data, diagramId });
    await this.bumpVersion(diagramId);
    return res.id;
  }

  async updateNode(diagramId: string, id: string, data: Record<string, unknown>): Promise<void> {
    await this.request(`$DB/nodes/${id}/update`, data);
    await this.bumpVersion(diagramId);
  }

  async deleteNode(diagramId: string, id: string): Promise<void> {
    await this.request(`$DB/nodes/${id}/delete`, {});
    await this.bumpVersion(diagramId);
  }

  private async bumpVersion(diagramId: string): Promise<void> {
    const next = (this.appliedVersion.get(diagramId) ?? 0) + 1;
    await this.request(`$DB/diagrams/${diagramId}/update`, { version: next });
    this.appliedVersion.set(diagramId, next);
  }

  private async request(topic: string, payload: unknown): Promise<any> {
    const responseTopic = `responses/${this.clientId}/${Date.now()}`;
    await this.mqtt!.subscribe(responseTopic, { qos: 1 });
    await this.mqtt!.publish(topic, JSON.stringify(payload), { qos: 1, properties: { responseTopic } });
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('Request timeout')), 10000);
      this.mqtt!.onMessage((t, p) => {
        if (t !== responseTopic) return;
        clearTimeout(timer);
        this.mqtt!.unsubscribe(responseTopic);
        resolve(JSON.parse(new TextDecoder().decode(p)));
      });
    });
  }
}
```

List with a filter uses `$DB/{entity}/list` and a `{ "filter": "diagramId=..." }` payload; the response `data` array holds the matching records.

---

## Definition of Done

- [ ] CRUD uses entity-first topics (`$DB/{entity}/create`, `$DB/{entity}/{id}/update`, ...)
- [ ] SyncClient subscribes to `$DB/{entity}/events/#` and routes by `data.diagramId`
- [ ] Events for unsubscribed diagrams are ignored
- [ ] Events are buffered during initial state fetch, then drained
- [ ] CRUD bumps `diagram.version` after each mutation
- [ ] Local applies are idempotent
