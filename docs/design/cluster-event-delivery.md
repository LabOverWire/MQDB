# Cluster change-event delivery

Status: design for gh #199 ("events can be lost").

## What goes wrong today

A change event is built on the partition primary, written to `_outbox/{uuid}` in the same batch as the record, handed to the send queues, and the outbox entry is removed right away (`NodeController::publish_and_deliver_change_event`). The outbox entry lives only on the primary; the replication write carries no event.

The model in `specs/ClusterEventDelivery.tla` (config `current_*`) finds three failures:

| Failure | How |
|---|---|
| Event lost | The primary crashes after the record reaches the replica but before the event leaves, or after the event is queued but before the QUIC writer flushes it. The promoted replica has no outbox entry. Without a crash: the local publish queue drops when full (`QuicDirectTransport::enqueue_local_publish`), a forward that fails only warns, and the receiving node drops when its inbox is full. |
| Out of order | On startup a node replays its whole outbox before it knows which partitions it holds (`recover_pending_outbox`), so an older event can arrive after the new primary's newer one. |
| Event for a discarded write | The same replay publishes events for writes the failover discarded. |

## Design

Every event is identified by the `(epoch, sequence)` of the record write it belongs to.

1. **The event rides with the write.** `ReplicationWrite` gets an optional trailer with the event (topic, payload, user properties, order key). The primary stores the event under `_evt/{partition}/{epoch}/{sequence}` in the same batch as the record. A replica stores it in the same batch as its copy of the record. A node that is not primary and forwards a `WriteRequest` attaches the event, and the primary takes it over.
2. **Only the primary sends, in order, and waits for acks.** Each pending event holds the nodes that still need it: the nodes with a matching subscription when the primary takes the event on. Per partition and target node, the primary sends the oldest event that target still needs as a new `ChangeEvent` message, and sends the next one only after that target's `ChangeEventAck`. An unacked event is resent after a timeout.
3. **The receiver filters on the original id.** Each node keeps, per partition, the highest `(epoch, sequence)` it has delivered. It delivers a `ChangeEvent` only if its id is higher, and acks either way. The local subscribers of the primary go through the same check. Delivery waits for space in the local queue instead of dropping.
4. **Pruning.** When the oldest events no longer need any target, the primary deletes them and sends `ChangeEventPrune` with the highest pruned id to its replicas. Pruning is cumulative, so a lost prune is covered by the next one.
5. **Role changes.** On `become_primary` a node loads the partition's pending events, works out their targets afresh, and starts sending from the oldest. Duplicates are filtered by the receivers. A node that stops being primary for a partition stops sending for it and keeps its pending events as a replica copy (a demoted primary becomes a replica without a new snapshot, so this copy is the only one it has). The new primary's prunes compare ids across epochs, so they remove those entries too, including any for writes the failover discarded. A node that restarts sends nothing until it is primary again. A node that is no longer a replica of a partition deletes its copy.
6. **Snapshots** carry the partition's pending events, so a replica built from a snapshot can take over delivery.

No re-stamping: the model shows that re-stamping inherited events with the new epoch makes subscribers see 1, 2, 1 after a failover. Filtering on the original id is enough. A promoted replica's pending events are a prefix of the old primary's history, so anything at or below a receiver's mark was already delivered to it.

The model checks this design in full with crashes, promotion, restarts, one or two lost events or acks, and an old primary that keeps running after the replica is promoted. Removing any of the parts above (event on the replica, the receiver filter, waiting for the ack, only the primary sending) fails a property.

## Mixed versions

- Nodes that deliver acked events send heartbeat version 4, with the same layout as version 3. Version 3 nodes read any version from 3 up the same way.
- Older nodes ignore the `ReplicationWrite` trailer: both the current decoder and the one before #208 read only the lengths they find. An older replica therefore keeps no pending events, as today.
- `ChangeEvent`, `ChangeEventAck` and `ChangeEventPrune` are new message types; older nodes drop unknown types. The primary sends `ChangeEvent` only to nodes that advertise the capability. To others it sends the existing `ForwardedPublish`, and the cursor moves on as soon as it is sent.
- A node forwarding a `WriteRequest` to a primary without the capability publishes the event itself, as today.
- Snapshot import skips sections it does not know.
- `_outbox/` entries written by older versions are still replayed once at startup, as today; the new code no longer writes them.

## Bounds

- At most 4096 pending events per partition. Past that, the oldest is dropped, with an error log naming the partition and the nodes that miss it. A node that heartbeats mark dead is removed from every pending event's targets.
- Resend after 500 ms without an ack. Prunes go out at most every 250 ms.
- The local queue is waited on for at most 1 s; a receiver that could not queue an event does not ack it, and the primary retries its own local subscribers the same way.
- The receiver's high-water marks are a fixed array of 256 entries.

## Not covered

- Writes an old primary makes after it has been cut off are lost by async replication, and their events may already have been delivered.
- A receiving node that restarts forgets its marks; its subscribers may see an event again.
- When nodes disagree about who is primary, a write can reach a node without the primary role, which drops it. That node publishes the event untracked, as before this change, so subscribers still see it.
- One event in flight per partition and target costs a round trip per event. Throughput is measured against main with watchers on every node and a burst of writes through every node.

## Tests

- Node-controller tests with the mock transport, for each failure in the table and each part of the design: the promoted replica delivers an event the dead primary never sent; a lost event or ack is resent and delivered once; a restarted node that is no longer primary sends nothing; an older target gets a `ForwardedPublish`; an older primary gets the forwarding node's own publish.
- E2E at 3 and 5 nodes against main: watchers on every node, writes through every node, the primary of the written partitions killed mid-run, and a count of events received per committed record. The same run with one node on main.
