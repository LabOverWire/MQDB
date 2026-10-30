// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::cluster::data_partition;
use crate::cluster::event_delivery::{PRUNE_EVERY_MS, RESEND_AFTER_MS};
use crate::cluster::message_processor::HeartbeatUpdate;
use crate::cluster::protocol::{ChangeEventDelivery, ChangeEventMark, ChangeEventRecord, EventId};
use crate::cluster::{PartitionAssignment, PartitionMap};
use mqdb_core::events::ChangeEvent;
use mqdb_core::storage::{MemoryBackend, StorageBackend};
use std::sync::atomic::Ordering;

const ENTITY: &str = "ev";
const ID: &str = "r1";

fn node(id: u16) -> NodeId {
    NodeId::validated(id).unwrap()
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap())
        .unwrap()
}

fn controller(id: u16) -> NodeController<MockTransport> {
    create_test_controller(node(id), MockTransport::new(node(id)))
}

fn controller_with_storage(
    id: u16,
    backend: &Arc<dyn StorageBackend>,
) -> NodeController<MockTransport> {
    let (tx_raft_messages, _rx_raft_messages) = flume::unbounded();
    let (tx_raft_events, _rx_raft_events) = flume::unbounded();
    NodeController::new_with_storage(
        node(id),
        MockTransport::new(node(id)),
        TransportConfig::default(),
        Some(Arc::clone(backend)),
        tx_raft_messages,
        tx_raft_events,
    )
}

fn heartbeat_from(ctrl: &mut NodeController<MockTransport>, from: NodeId, version: u8) {
    let mut bytes = Heartbeat::create(from, 1).to_be_bytes();
    bytes[0] = version;
    let (heartbeat, _) = Heartbeat::try_from_be_bytes(&bytes).unwrap();
    ctrl.apply_heartbeat_updates(&[HeartbeatUpdate {
        from,
        heartbeat,
        received_at: 1,
    }]);
}

fn upgraded(ctrl: &mut NodeController<MockTransport>, from: NodeId) {
    heartbeat_from(ctrl, from, Heartbeat::VERSION);
}

fn older(ctrl: &mut NodeController<MockTransport>, from: NodeId) {
    heartbeat_from(ctrl, from, 3);
}

fn watch(ctrl: &NodeController<MockTransport>, client: &str, on: NodeId) {
    ctrl.stores()
        .topics
        .subscribe(
            &format!("$DB/{ENTITY}/events/{ID}"),
            client,
            PartitionId::ZERO,
            1,
        )
        .unwrap();
    ctrl.stores().client_locations.set(client, on);
}

fn assign(
    ctrl: &mut NodeController<MockTransport>,
    primary: NodeId,
    replicas: &[NodeId],
) -> PartitionId {
    let partition = data_partition(ENTITY, ID);
    let mut map = PartitionMap::default();
    map.set(
        partition,
        PartitionAssignment {
            primary: Some(primary),
            replicas: replicas.to_vec(),
            epoch: Epoch::new(1),
        },
    );
    ctrl.update_partition_map(map);
    partition
}

fn primary_with(ctrl: &mut NodeController<MockTransport>, replicas: &[NodeId]) -> PartitionId {
    let partition = assign(ctrl, ctrl.node_id, replicas);
    ctrl.become_primary(partition, Epoch::new(1));
    partition
}

fn record_event(n: u64) -> ChangeEvent {
    ChangeEvent::update(
        ENTITY.to_string(),
        ID.to_string(),
        serde_json::json!({"id": ID, "n": n}),
    )
    .with_client_id(Some("editor-1".to_string()))
}

async fn commit(ctrl: &mut NodeController<MockTransport>, n: u64) -> ChangeEvent {
    let data = serde_json::json!({"id": ID, "n": n});
    let bytes = serde_json::to_vec(&data).unwrap();
    let write = if n == 1 {
        ctrl.stores
            .db_create_replicated(ENTITY, ID, &bytes, 0)
            .unwrap()
            .1
    } else {
        ctrl.stores
            .db_update_replicated(ENTITY, ID, &bytes, 0)
            .unwrap()
            .1
    };
    let event = record_event(n);
    ctrl.commit_with_change_event(write, event.clone()).await;
    event
}

fn change_events_sent(ctrl: &NodeController<MockTransport>) -> Vec<(u16, EventId)> {
    ctrl.transport
        .sent_messages()
        .into_iter()
        .filter_map(|(to, message)| match message {
            ClusterMessage::ChangeEvent(delivery) => Some((to.get(), delivery.id)),
            _ => None,
        })
        .collect()
}

fn acks_sent(ctrl: &NodeController<MockTransport>) -> Vec<(u16, EventId)> {
    ctrl.transport
        .sent_messages()
        .into_iter()
        .filter_map(|(to, message)| match message {
            ClusterMessage::ChangeEventAck(mark) => Some((to.get(), mark.id)),
            _ => None,
        })
        .collect()
}

fn prunes_sent(ctrl: &NodeController<MockTransport>) -> Vec<(u16, EventId)> {
    ctrl.transport
        .sent_messages()
        .into_iter()
        .filter_map(|(to, message)| match message {
            ClusterMessage::ChangeEventPrune(mark) => Some((to.get(), mark.id)),
            _ => None,
        })
        .collect()
}

fn replication_writes_sent(ctrl: &NodeController<MockTransport>) -> Vec<(NodeId, ClusterMessage)> {
    ctrl.transport
        .sent_messages()
        .into_iter()
        .filter(|(_, message)| {
            matches!(message, ClusterMessage::Write(w) | ClusterMessage::WriteRequest(w) if w.entity == crate::cluster::entity::DB_DATA)
        })
        .collect()
}

fn delivered_locally(ctrl: &NodeController<MockTransport>) -> Vec<RoutedPublish> {
    ctrl.transport
        .queued_publishes()
        .into_iter()
        .filter_map(|publish| match publish {
            QueuedPublish::Routed(routed) => Some(routed),
            QueuedPublish::Plain(..) => None,
        })
        .collect()
}

fn delivery(partition: PartitionId, epoch: u64, sequence: u64) -> ClusterMessage {
    ClusterMessage::ChangeEvent(ChangeEventDelivery {
        partition,
        id: EventId::new(epoch, sequence),
        record: ChangeEventRecord {
            topic: format!("$DB/{ENTITY}/events/{ID}"),
            payload: format!("{epoch}-{sequence}").into_bytes(),
            user_properties: Vec::new(),
            order_key: format!("{ENTITY}/{ID}"),
        },
    })
}

#[tokio::test]
async fn an_older_node_and_local_subscribers_get_the_event_with_its_origin() {
    let mut ctrl = controller(1);
    primary_with(&mut ctrl, &[]);
    watch(&ctrl, "remote-watcher", node(2));
    older(&mut ctrl, node(2));

    let event = commit(&mut ctrl, 1).await;

    let payload = serde_json::to_vec(&event).unwrap();
    let origin = vec![("x-origin-client-id".to_string(), "editor-1".to_string())];
    assert_eq!(
        delivered_locally(&ctrl),
        vec![RoutedPublish {
            topic: format!("$DB/{ENTITY}/events/{ID}"),
            payload: payload.clone(),
            qos: 1,
            user_properties: origin.clone(),
            order_key: Some(format!("{ENTITY}/{ID}")),
            share_groups: None,
        }]
    );
    let forwarded: Vec<_> = ctrl
        .transport
        .sent_messages()
        .into_iter()
        .filter_map(|(to, message)| match message {
            ClusterMessage::ForwardedPublish(fwd) => Some((to, fwd)),
            _ => None,
        })
        .collect();
    assert_eq!(forwarded.len(), 1);
    assert_eq!(forwarded[0].0, node(2));
    assert_eq!(forwarded[0].1.payload, payload);
    assert_eq!(forwarded[0].1.user_properties, origin);
    assert_eq!(forwarded[0].1.order_key.as_deref(), Some("ev/r1"));
    assert!(change_events_sent(&ctrl).is_empty());
}

#[tokio::test]
async fn an_upgraded_node_gets_the_next_event_only_after_acking_the_previous_one() {
    let mut ctrl = controller(1);
    let partition = primary_with(&mut ctrl, &[]);
    watch(&ctrl, "remote-watcher", node(2));
    upgraded(&mut ctrl, node(2));

    commit(&mut ctrl, 1).await;
    commit(&mut ctrl, 2).await;
    assert_eq!(change_events_sent(&ctrl), vec![(2, EventId::new(1, 1))]);

    ctrl.transport.inject_message(
        node(2),
        ClusterMessage::ChangeEventAck(ChangeEventMark::new(partition, EventId::new(1, 1))),
    );
    ctrl.process_messages().await;

    assert_eq!(
        change_events_sent(&ctrl),
        vec![(2, EventId::new(1, 1)), (2, EventId::new(1, 2))]
    );
    assert_eq!(delivered_locally(&ctrl).len(), 2);
}

#[tokio::test]
async fn an_unacked_event_is_resent() {
    let mut ctrl = controller(1);
    primary_with(&mut ctrl, &[]);
    watch(&ctrl, "remote-watcher", node(2));
    upgraded(&mut ctrl, node(2));

    commit(&mut ctrl, 1).await;
    ctrl.drive_change_events(now()).await;
    assert_eq!(change_events_sent(&ctrl).len(), 1);

    ctrl.drive_change_events(now() + RESEND_AFTER_MS).await;
    assert_eq!(
        change_events_sent(&ctrl),
        vec![(2, EventId::new(1, 1)), (2, EventId::new(1, 1))]
    );
}

#[tokio::test]
async fn a_promoted_replica_delivers_the_event_the_dead_primary_never_sent() {
    let mut primary = controller(1);
    let partition = primary_with(&mut primary, &[node(2)]);
    primary
        .transport
        .refuse_local_delivery
        .store(true, Ordering::SeqCst);
    commit(&mut primary, 1).await;
    let writes = replication_writes_sent(&primary);
    assert_eq!(writes.len(), 1);
    assert!(delivered_locally(&primary).is_empty());

    let mut replica = controller(2);
    assign(&mut replica, node(1), &[node(2)]);
    replica.become_replica(partition, Epoch::new(1), 0);
    for (_, message) in writes {
        replica.transport.inject_message(node(1), message);
    }
    replica.process_messages().await;
    assert!(change_events_sent(&replica).is_empty());
    assert!(delivered_locally(&replica).is_empty());

    watch(&replica, "remote-watcher", node(3));
    upgraded(&mut replica, node(3));
    replica.become_primary(partition, Epoch::new(2));
    replica.drive_change_events(now()).await;

    assert_eq!(change_events_sent(&replica), vec![(3, EventId::new(1, 1))]);
    assert_eq!(delivered_locally(&replica).len(), 1);
}

#[tokio::test]
async fn a_resent_event_is_delivered_once_and_acked_each_time() {
    let mut ctrl = controller(3);
    let partition = data_partition(ENTITY, ID);
    ctrl.transport
        .inject_message(node(1), delivery(partition, 1, 4));
    ctrl.transport
        .inject_message(node(1), delivery(partition, 1, 4));
    ctrl.process_messages().await;

    assert_eq!(delivered_locally(&ctrl).len(), 1);
    assert_eq!(
        acks_sent(&ctrl),
        vec![(1, EventId::new(1, 4)), (1, EventId::new(1, 4))]
    );
}

#[tokio::test]
async fn an_event_older_than_one_already_delivered_is_acked_but_not_delivered() {
    let mut ctrl = controller(3);
    let partition = data_partition(ENTITY, ID);
    ctrl.transport
        .inject_message(node(2), delivery(partition, 2, 1));
    ctrl.transport
        .inject_message(node(1), delivery(partition, 1, 9));
    ctrl.process_messages().await;

    let delivered = delivered_locally(&ctrl);
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].payload, b"2-1");
    assert_eq!(
        acks_sent(&ctrl),
        vec![(2, EventId::new(2, 1)), (1, EventId::new(1, 9))]
    );
}

#[tokio::test]
async fn a_receiver_that_cannot_queue_the_event_does_not_ack_it() {
    let mut ctrl = controller(3);
    let partition = data_partition(ENTITY, ID);
    ctrl.transport
        .refuse_local_delivery
        .store(true, Ordering::SeqCst);
    ctrl.transport
        .inject_message(node(1), delivery(partition, 1, 1));
    ctrl.process_messages().await;
    assert!(acks_sent(&ctrl).is_empty());

    ctrl.transport
        .refuse_local_delivery
        .store(false, Ordering::SeqCst);
    ctrl.transport
        .inject_message(node(1), delivery(partition, 1, 1));
    ctrl.process_messages().await;
    assert_eq!(delivered_locally(&ctrl).len(), 1);
    assert_eq!(acks_sent(&ctrl), vec![(1, EventId::new(1, 1))]);
}

#[tokio::test]
async fn the_local_subscribers_of_the_primary_get_an_event_it_could_not_queue_once_there_is_room() {
    let mut ctrl = controller(1);
    primary_with(&mut ctrl, &[]);
    ctrl.transport
        .refuse_local_delivery
        .store(true, Ordering::SeqCst);
    commit(&mut ctrl, 1).await;
    commit(&mut ctrl, 2).await;
    assert!(delivered_locally(&ctrl).is_empty());

    ctrl.transport
        .refuse_local_delivery
        .store(false, Ordering::SeqCst);
    ctrl.drive_change_events(now() + RESEND_AFTER_MS).await;

    let payloads: Vec<_> = delivered_locally(&ctrl)
        .into_iter()
        .map(|p| serde_json::from_slice::<ChangeEvent>(&p.payload).unwrap())
        .map(|e| e.data.unwrap()["n"].as_u64().unwrap())
        .collect();
    assert_eq!(payloads, vec![1, 2]);
}

#[tokio::test]
async fn a_restarted_node_sends_nothing_until_it_is_primary_again() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let mut before = controller_with_storage(1, &backend);
    let partition = primary_with(&mut before, &[]);
    watch(&before, "remote-watcher", node(2));
    upgraded(&mut before, node(2));
    commit(&mut before, 1).await;

    let mut after = controller_with_storage(1, &backend);
    assign(&mut after, node(2), &[node(1)]);
    watch(&after, "remote-watcher", node(2));
    upgraded(&mut after, node(2));
    after.load_stored_change_events();
    after.become_replica(partition, Epoch::new(2), 1);
    after.drive_change_events(now()).await;
    assert!(change_events_sent(&after).is_empty());
    assert!(delivered_locally(&after).is_empty());

    after.become_primary(partition, Epoch::new(3));
    after.drive_change_events(now()).await;
    assert_eq!(change_events_sent(&after), vec![(2, EventId::new(1, 1))]);
}

#[tokio::test]
async fn a_forwarding_node_hands_the_event_to_an_upgraded_primary() {
    let mut ctrl = controller(1);
    assign(&mut ctrl, node(2), &[]);
    upgraded(&mut ctrl, node(2));

    commit(&mut ctrl, 1).await;

    let writes = replication_writes_sent(&ctrl);
    assert_eq!(writes.len(), 1);
    let (to, ClusterMessage::WriteRequest(write)) = &writes[0] else {
        panic!("expected a write request, got {:?}", writes[0].1);
    };
    assert_eq!(*to, node(2));
    assert_eq!(
        write.event.as_ref().map(|e| e.topic.as_str()),
        Some("$DB/ev/events/r1")
    );
    assert!(delivered_locally(&ctrl).is_empty());
}

#[tokio::test]
async fn a_forwarding_node_publishes_the_event_itself_for_an_older_primary() {
    let mut ctrl = controller(1);
    assign(&mut ctrl, node(2), &[]);
    older(&mut ctrl, node(2));

    commit(&mut ctrl, 1).await;

    let writes = replication_writes_sent(&ctrl);
    assert_eq!(writes.len(), 1);
    let ClusterMessage::WriteRequest(write) = &writes[0].1 else {
        panic!("expected a write request, got {:?}", writes[0].1);
    };
    assert!(write.event.is_none());
    assert_eq!(delivered_locally(&ctrl).len(), 1);
}

#[tokio::test]
async fn a_forwarding_node_publishes_the_event_itself_when_the_primary_is_dead() {
    let mut ctrl = controller(1);
    assign(&mut ctrl, node(2), &[]);
    upgraded(&mut ctrl, node(2));
    ctrl.apply_dead_nodes(&[node(2)]);

    commit(&mut ctrl, 1).await;

    let writes = replication_writes_sent(&ctrl);
    let ClusterMessage::WriteRequest(write) = &writes[0].1 else {
        panic!("expected a write request, got {:?}", writes[0].1);
    };
    assert!(write.event.is_none());
    assert_eq!(delivered_locally(&ctrl).len(), 1);
}

#[tokio::test]
async fn a_forwarding_node_publishes_the_event_itself_when_the_hand_off_fails() {
    let mut ctrl = controller(1);
    assign(&mut ctrl, node(2), &[]);
    upgraded(&mut ctrl, node(2));
    ctrl.transport.refuse_sends.store(true, Ordering::SeqCst);

    commit(&mut ctrl, 1).await;

    assert_eq!(delivered_locally(&ctrl).len(), 1);
}

#[tokio::test]
async fn the_primary_takes_on_the_event_of_a_forwarded_write() {
    let mut forwarder = controller(1);
    let partition = assign(&mut forwarder, node(2), &[]);
    upgraded(&mut forwarder, node(2));
    commit(&mut forwarder, 1).await;
    let writes = replication_writes_sent(&forwarder);

    let mut primary = controller(2);
    assign(&mut primary, node(2), &[]);
    primary.become_primary(partition, Epoch::new(1));
    watch(&primary, "remote-watcher", node(3));
    upgraded(&mut primary, node(3));
    for (_, message) in writes {
        primary.transport.inject_message(node(1), message);
    }
    primary.process_messages().await;

    assert_eq!(change_events_sent(&primary), vec![(3, EventId::new(1, 1))]);
    assert_eq!(delivered_locally(&primary).len(), 1);
}

async fn forwarded_writes(primary: NodeId) -> (PartitionId, Vec<(NodeId, ClusterMessage)>) {
    let mut forwarder = controller(1);
    let partition = assign(&mut forwarder, primary, &[]);
    upgraded(&mut forwarder, primary);
    commit(&mut forwarder, 1).await;
    (partition, replication_writes_sent(&forwarder))
}

#[tokio::test]
async fn a_node_that_is_sent_a_write_it_is_not_primary_for_publishes_the_event() {
    let (partition, writes) = forwarded_writes(node(2)).await;

    let mut receiver = controller(2);
    assign(&mut receiver, node(1), &[node(2)]);
    receiver.become_replica(partition, Epoch::new(1), 0);
    for (_, message) in writes {
        receiver.transport.inject_message(node(1), message);
    }
    receiver.process_messages().await;

    assert_eq!(delivered_locally(&receiver).len(), 1);
    assert!(replication_writes_sent(&receiver).is_empty());
}

#[tokio::test]
async fn a_node_named_primary_without_the_primary_role_publishes_a_forwarded_event() {
    let (partition, writes) = forwarded_writes(node(2)).await;

    let mut receiver = controller(2);
    assign(&mut receiver, node(2), &[]);
    receiver.become_replica(partition, Epoch::new(1), 0);
    for (_, message) in writes {
        receiver.transport.inject_message(node(1), message);
    }
    receiver.process_messages().await;

    assert_eq!(delivered_locally(&receiver).len(), 1);
    assert!(change_events_sent(&receiver).is_empty());
}

#[tokio::test]
async fn a_node_named_primary_without_the_primary_role_publishes_its_own_event() {
    let mut ctrl = controller(1);
    let partition = assign(&mut ctrl, node(1), &[node(2)]);
    ctrl.become_replica(partition, Epoch::new(1), 0);

    commit(&mut ctrl, 1).await;

    assert_eq!(delivered_locally(&ctrl).len(), 1);
    assert!(replication_writes_sent(&ctrl).is_empty());
}

#[tokio::test]
async fn the_primary_prunes_its_replicas_once_every_target_acked() {
    let mut ctrl = controller(1);
    let partition = primary_with(&mut ctrl, &[node(2)]);
    upgraded(&mut ctrl, node(2));
    watch(&ctrl, "remote-watcher", node(3));
    upgraded(&mut ctrl, node(3));
    commit(&mut ctrl, 1).await;

    ctrl.drive_change_events(now()).await;
    assert!(prunes_sent(&ctrl).is_empty());

    ctrl.transport.inject_message(
        node(3),
        ClusterMessage::ChangeEventAck(ChangeEventMark::new(partition, EventId::new(1, 1))),
    );
    ctrl.process_messages().await;
    ctrl.drive_change_events(now() + PRUNE_EVERY_MS).await;
    assert_eq!(prunes_sent(&ctrl), vec![(2, EventId::new(1, 1))]);
}

#[tokio::test]
async fn a_prune_removes_the_replica_copy_from_memory_and_storage() {
    let mut primary = controller(1);
    let partition = primary_with(&mut primary, &[node(2)]);
    commit(&mut primary, 1).await;
    let writes = replication_writes_sent(&primary);

    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let mut replica = controller_with_storage(2, &backend);
    assign(&mut replica, node(1), &[node(2)]);
    replica.become_replica(partition, Epoch::new(1), 0);
    for (_, message) in writes {
        replica.transport.inject_message(node(1), message);
    }
    replica.process_messages().await;
    let stored = |ctrl: &NodeController<MockTransport>| {
        ctrl.stores()
            .cluster_outbox()
            .unwrap()
            .stored_change_events_for(partition)
            .unwrap()
            .len()
    };
    assert_eq!(replica.pending_change_events(partition), 1);
    assert_eq!(stored(&replica), 1);

    replica.transport.inject_message(
        node(1),
        ClusterMessage::ChangeEventPrune(ChangeEventMark::new(partition, EventId::new(1, 1))),
    );
    replica.process_messages().await;
    assert_eq!(replica.pending_change_events(partition), 0);
    assert_eq!(stored(&replica), 0);
}

#[tokio::test]
async fn a_failed_snapshot_import_keeps_the_replica_copy() {
    let mut primary = controller(1);
    let partition = primary_with(&mut primary, &[node(2)]);
    commit(&mut primary, 1).await;
    let writes = replication_writes_sent(&primary);

    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let mut replica = controller_with_storage(2, &backend);
    assign(&mut replica, node(1), &[node(2)]);
    replica.become_replica(partition, Epoch::new(1), 0);
    for (_, message) in writes {
        replica.transport.inject_message(node(1), message);
    }
    replica.process_messages().await;

    let section = crate::cluster::entity::CHANGE_EVENTS.as_bytes();
    let mut malformed = vec![1, u8::try_from(section.len()).unwrap()];
    malformed.extend_from_slice(section);
    malformed.extend_from_slice(&2u32.to_be_bytes());
    malformed.extend_from_slice(&[0, 0]);
    let chunk = crate::cluster::snapshot::SnapshotSender::new(partition, malformed, 0)
        .next_chunk()
        .unwrap();
    replica
        .transport
        .inject_message(node(1), ClusterMessage::SnapshotChunk(chunk));
    replica.process_messages().await;

    assert_eq!(replica.pending_change_events(partition), 1);
    let stored = replica
        .stores()
        .cluster_outbox()
        .unwrap()
        .stored_change_events_for(partition)
        .unwrap();
    assert_eq!(stored.len(), 1);
}

#[tokio::test]
async fn a_partition_snapshot_carries_the_pending_events() {
    let source_backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let mut primary = controller_with_storage(1, &source_backend);
    let partition = primary_with(&mut primary, &[]);
    watch(&primary, "remote-watcher", node(2));
    upgraded(&mut primary, node(2));
    commit(&mut primary, 1).await;
    let snapshot = primary.stores().export_partition(partition);

    let target_backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let target = controller_with_storage(3, &target_backend);
    target.stores().import_partition(&snapshot).unwrap();

    let events = target
        .stores()
        .cluster_outbox()
        .unwrap()
        .stored_change_events_for(partition)
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].id, EventId::new(1, 1));
    assert_eq!(events[0].record.order_key, "ev/r1");
}

fn primary_with_failing_store(
    id: u16,
) -> (
    NodeController<MockTransport>,
    Arc<crate::cluster::test_support::FaultyBackend>,
) {
    let backend = crate::cluster::test_support::FaultyBackend::shared();
    let storage: Arc<dyn StorageBackend> = backend.clone();
    (controller_with_storage(id, &storage), backend)
}

#[tokio::test]
async fn a_forwarded_write_the_primary_cannot_store_publishes_its_event_untracked() {
    let (partition, writes) = forwarded_writes(node(2)).await;
    let (mut primary, backend) = primary_with_failing_store(2);
    assign(&mut primary, node(2), &[]);
    primary.become_primary(partition, Epoch::new(1));
    backend.fail_writes(true);
    for (_, message) in writes {
        primary.transport.inject_message(node(1), message);
    }
    primary.process_messages().await;

    assert_eq!(delivered_locally(&primary).len(), 1);
    assert!(replication_writes_sent(&primary).is_empty());
}

#[tokio::test]
async fn a_write_with_an_event_the_primary_cannot_store_publishes_its_event_untracked() {
    let (mut primary, backend) = primary_with_failing_store(1);
    primary_with(&mut primary, &[node(2)]);
    backend.fail_writes(true);

    commit(&mut primary, 1).await;

    assert_eq!(delivered_locally(&primary).len(), 1);
    assert!(replication_writes_sent(&primary).is_empty());
    assert_eq!(backend.keys_containing(ID).unwrap(), 0);
}
