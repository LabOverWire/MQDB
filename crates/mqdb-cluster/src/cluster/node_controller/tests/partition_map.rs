// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::cluster::raft::PartitionUpdate;
use crate::cluster::{PartitionAssignment, PartitionMap};

fn node(id: u16) -> NodeId {
    NodeId::validated(id).unwrap()
}

fn controller(id: u16) -> NodeController<MockTransport> {
    create_test_controller(node(id), MockTransport::new(node(id)))
}

fn map_with(
    partition: PartitionId,
    primary: NodeId,
    replicas: &[NodeId],
    epoch: u64,
) -> PartitionMap {
    let mut map = PartitionMap::default();
    map.set(
        partition,
        PartitionAssignment::new(primary, replicas.to_vec(), Epoch::new(epoch)),
    );
    map
}

#[tokio::test]
async fn a_partition_update_copy_from_another_node_changes_nothing() {
    let partition = PartitionId::new(201).unwrap();
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(1), &[node(2)], 1))
        .await;
    assert_eq!(ctrl.role(partition), ReplicaRole::AwaitingSnapshot);

    ctrl.transport.inject_message(
        node(3),
        ClusterMessage::PartitionUpdate(PartitionUpdate {
            partition: 201,
            primary: 2,
            replica1: 1,
            replica2: 0,
            epoch: 1,
        }),
    );
    ctrl.process_messages().await;

    assert_eq!(ctrl.partition_map().primary(partition), Some(node(1)));
    assert_eq!(ctrl.role(partition), ReplicaRole::AwaitingSnapshot);
}

#[tokio::test]
async fn a_primary_left_out_of_the_raft_map_steps_down() {
    let partition = PartitionId::new(201).unwrap();
    let mut ctrl = controller(1);
    ctrl.become_primary(partition, Epoch::new(1));
    assert!(
        ctrl.apply_raft_partition_map(map_with(partition, node(1), &[node(2)], 1))
            .await
    );
    assert_eq!(ctrl.role(partition), ReplicaRole::Primary);

    let became_primary = ctrl
        .apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 2))
        .await;

    assert!(!became_primary);
    assert_eq!(ctrl.partition_map().primary(partition), Some(node(2)));
    assert_eq!(ctrl.role(partition), ReplicaRole::None);
}

#[tokio::test]
async fn a_replica_serves_reads_only_after_its_snapshot_arrives() {
    let partition = PartitionId::new(201).unwrap();
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(1), &[node(2)], 1))
        .await;
    assert_eq!(ctrl.role(partition), ReplicaRole::AwaitingSnapshot);
    assert!(!ctrl.can_serve_reads(partition));

    ctrl.become_replica(partition, Epoch::new(1), 0);

    assert!(ctrl.can_serve_reads(partition));
}

fn session_write(client_id: &str, epoch: u64) -> ReplicationWrite {
    let (_, mut write) = controller(9)
        .stores()
        .create_session_replicated(client_id)
        .unwrap();
    write.epoch = Epoch::new(epoch);
    write
}

fn snapshot_requests_to(ctrl: &NodeController<MockTransport>, to: NodeId) -> usize {
    ctrl.transport
        .sent_messages()
        .into_iter()
        .filter(|(target, message)| {
            *target == to && matches!(message, ClusterMessage::SnapshotRequest(_))
        })
        .count()
}

async fn serve_snapshot(
    source: &mut NodeController<MockTransport>,
    target: &mut NodeController<MockTransport>,
    partition: PartitionId,
) {
    source.transport.inject_message(
        target.node_id(),
        ClusterMessage::SnapshotRequest(SnapshotRequest::create(partition, target.node_id())),
    );
    source.process_messages().await;
    for (to, message) in source.transport.sent_messages() {
        if to == target.node_id() && matches!(message, ClusterMessage::SnapshotChunk(_)) {
            target.transport.inject_message(source.node_id(), message);
        }
    }
    target.process_messages().await;
}

fn controller_on(
    id: u16,
    backend: &Arc<crate::cluster::test_support::FaultyBackend>,
) -> NodeController<MockTransport> {
    let tx_raft_messages = flume::unbounded().0;
    let tx_raft_events = flume::unbounded().0;
    let storage: Arc<dyn mqdb_core::storage::StorageBackend> = backend.clone();
    NodeController::new_with_storage(
        node(id),
        MockTransport::new(node(id)),
        TransportConfig::default(),
        Some(storage),
        tx_raft_messages,
        tx_raft_events,
    )
}

#[tokio::test]
async fn a_node_without_the_data_waits_for_the_replica_before_taking_the_primary_role() {
    let partition = session_partition("c1");
    let mut replica = controller(3);
    replica.become_replica(partition, Epoch::new(1), 4);
    replica.stores().create_session_replicated("c1").unwrap();

    let mut wiped = controller(2);
    wiped
        .apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;

    assert_eq!(wiped.role(partition), ReplicaRole::AwaitingSnapshot);
    assert!(!wiped.can_serve_reads(partition));
    assert_eq!(snapshot_requests_to(&wiped, node(3)), 1);

    serve_snapshot(&mut replica, &mut wiped, partition).await;

    assert_eq!(wiped.role(partition), ReplicaRole::Primary);
    assert!(wiped.can_serve_reads(partition));
    assert!(wiped.stores().sessions.get("c1").is_some());
    assert_eq!(wiped.sequence(partition), Some(4));
}

#[tokio::test]
async fn a_restarted_primary_trusts_its_disk_only_for_the_assignment_it_held() {
    let partition = session_partition("c1");
    let backend = crate::cluster::test_support::FaultyBackend::shared();
    let mut before_crash = controller_on(2, &backend);
    before_crash
        .apply_raft_partition_map(map_with(partition, node(2), &[], 1))
        .await;
    assert_eq!(before_crash.role(partition), ReplicaRole::Primary);
    drop(before_crash);

    let mut same_assignment = controller_on(2, &backend);
    same_assignment
        .apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;
    assert_eq!(same_assignment.role(partition), ReplicaRole::Primary);

    let mut newer_assignment = controller_on(2, &backend);
    newer_assignment
        .apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 3))
        .await;
    assert_eq!(
        newer_assignment.role(partition),
        ReplicaRole::AwaitingSnapshot
    );
    assert_eq!(snapshot_requests_to(&newer_assignment, node(3)), 1);
}

#[tokio::test]
async fn a_replica_promoted_before_its_snapshot_lands_waits_for_it() {
    let partition = session_partition("c1");
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(1), &[node(2)], 1))
        .await;
    assert_eq!(ctrl.role(partition), ReplicaRole::AwaitingSnapshot);

    let became_primary = ctrl
        .apply_raft_partition_map(map_with(partition, node(2), &[node(1)], 2))
        .await;

    assert!(!became_primary);
    assert_eq!(ctrl.role(partition), ReplicaRole::AwaitingSnapshot);
    assert!(!ctrl.can_serve_reads(partition));
}

#[tokio::test]
async fn a_waiting_node_asks_the_new_source_when_the_assignment_changes() {
    let partition = session_partition("c1");
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;
    assert_eq!(snapshot_requests_to(&ctrl, node(3)), 1);
    ctrl.heartbeat.receive_heartbeat(
        node(3),
        &crate::cluster::protocol::Heartbeat::create(node(3), 0),
        0,
    );
    ctrl.apply_dead_nodes(&[node(3)]);
    assert_eq!(ctrl.node_status(node(3)), NodeStatus::Dead);

    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(4)], 2))
        .await;

    assert_eq!(ctrl.role(partition), ReplicaRole::AwaitingSnapshot);
    assert_eq!(snapshot_requests_to(&ctrl, node(4)), 1);
}

#[tokio::test]
async fn a_stalled_snapshot_request_is_sent_again() {
    let partition = session_partition("c1");
    let map = map_with(partition, node(2), &[node(3)], 1);
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map.clone()).await;
    assert_eq!(snapshot_requests_to(&ctrl, node(3)), 1);

    ctrl.apply_raft_partition_map(map.clone()).await;
    assert_eq!(snapshot_requests_to(&ctrl, node(3)), 1);

    let stalled_since = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_secs(11))
        .unwrap();
    if let Some(request) = ctrl.requested_snapshots.get_mut(partition) {
        request.last_activity = stalled_since;
    }
    ctrl.apply_raft_partition_map(map).await;

    assert_eq!(snapshot_requests_to(&ctrl, node(3)), 2);
}

#[tokio::test]
async fn a_snapshot_for_a_partition_no_longer_assigned_leaves_no_role() {
    let partition = session_partition("c1");
    let mut source = controller(1);
    source.become_primary(partition, Epoch::new(1));
    source.stores().create_session_replicated("c1").unwrap();
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(1), &[node(2)], 1))
        .await;
    ctrl.apply_raft_partition_map(map_with(partition, node(1), &[node(3)], 2))
        .await;

    serve_snapshot(&mut source, &mut ctrl, partition).await;

    assert_eq!(ctrl.role(partition), ReplicaRole::None);
    assert!(!ctrl.can_serve_reads(partition));
    assert!(ctrl.stores().sessions.get("c1").is_none());
}

#[tokio::test]
async fn a_forwarded_read_for_a_partition_not_held_is_refused() {
    let entity = "bigdocs";
    let id = "r2";
    let partition = crate::cluster::db::data_partition(entity, id);
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;

    ctrl.transport.inject_message(
        node(1),
        ClusterMessage::JsonDbRequest {
            partition,
            request: JsonDbRequest {
                request_id: 7,
                op: JsonDbOp::Read,
                entity: entity.to_string(),
                id: Some(id.to_string()),
                payload: b"{}".to_vec(),
                response_topic: "resp/r2".to_string(),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
        },
    );
    ctrl.process_messages().await;

    let payload = ctrl
        .transport
        .sent_messages()
        .into_iter()
        .find_map(|(to, message)| match message {
            ClusterMessage::JsonDbResponse(response) if to == node(1) => Some(response.payload),
            _ => None,
        })
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    assert_eq!(body["code"], 503);
}

#[tokio::test]
async fn a_snapshot_from_a_node_it_was_not_requested_from_is_ignored() {
    let partition = session_partition("c1");
    let mut stranger = controller(4);
    stranger.stores().create_session_replicated("c1").unwrap();
    let mut wiped = controller(2);
    wiped
        .apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;

    serve_snapshot(&mut stranger, &mut wiped, partition).await;

    assert_eq!(wiped.role(partition), ReplicaRole::AwaitingSnapshot);
    assert!(wiped.stores().sessions.get("c1").is_none());
}

#[tokio::test]
async fn a_primary_built_from_a_snapshot_does_not_trust_its_disk_after_a_restart() {
    let partition = session_partition("c1");
    let mut replica = controller(3);
    replica.become_replica(partition, Epoch::new(1), 4);
    replica.stores().create_session_replicated("c1").unwrap();
    let backend = crate::cluster::test_support::FaultyBackend::shared();
    let map = map_with(partition, node(2), &[node(3)], 1);

    let mut wiped = controller_on(2, &backend);
    wiped.apply_raft_partition_map(map.clone()).await;
    serve_snapshot(&mut replica, &mut wiped, partition).await;
    assert_eq!(wiped.role(partition), ReplicaRole::Primary);
    drop(wiped);

    let mut restarted = controller_on(2, &backend);
    restarted.stores().recover().unwrap();
    restarted.apply_raft_partition_map(map).await;

    assert_eq!(restarted.role(partition), ReplicaRole::AwaitingSnapshot);
    assert_eq!(snapshot_requests_to(&restarted, node(3)), 1);
}

#[tokio::test]
async fn a_wiped_node_that_persisted_replica_writes_is_not_treated_as_a_legacy_store() {
    let held = session_partition("c1");
    let backend = crate::cluster::test_support::FaultyBackend::shared();
    let mut first_boot = controller_on(2, &backend);
    first_boot
        .apply_raft_partition_map(map_with(held, node(2), &[node(3)], 1))
        .await;
    assert_eq!(first_boot.role(held), ReplicaRole::AwaitingSnapshot);
    let (_, write) = first_boot
        .stores()
        .db_create_replicated("bigdocs", "r1", b"{\"v\":1}", 1)
        .unwrap();
    first_boot.stores().apply_write(&write).unwrap();
    drop(first_boot);

    let mut restarted = controller_on(2, &backend);
    restarted.stores().recover().unwrap();
    assert!(restarted.stores().db_data.count() > 0);
    restarted
        .apply_raft_partition_map(map_with(held, node(2), &[node(3)], 1))
        .await;

    assert_eq!(restarted.role(held), ReplicaRole::AwaitingSnapshot);
}

#[tokio::test]
async fn a_forwarded_write_waits_while_the_primary_waits_for_its_snapshot() {
    let partition = session_partition("c1");
    let mut replica = controller(3);
    replica.become_replica(partition, Epoch::new(1), 0);
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;

    ctrl.transport.inject_message(
        node(1),
        ClusterMessage::WriteRequest(session_write("c1", 1)),
    );
    ctrl.process_messages().await;
    assert!(ctrl.stores().sessions.get("c1").is_none());

    serve_snapshot(&mut replica, &mut ctrl, partition).await;
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;

    assert_eq!(ctrl.role(partition), ReplicaRole::Primary);
    assert!(ctrl.stores().sessions.get("c1").is_some());
}

#[tokio::test]
async fn a_failed_snapshot_transfer_is_requested_again() {
    let partition = session_partition("c1");
    let map = map_with(partition, node(2), &[node(3)], 1);
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map.clone()).await;
    assert_eq!(snapshot_requests_to(&ctrl, node(3)), 1);

    for total_chunks in [2, 3] {
        ctrl.transport.inject_message(
            node(3),
            ClusterMessage::SnapshotChunk(crate::cluster::snapshot::SnapshotChunk::new(
                partition,
                0,
                total_chunks,
                0,
                Vec::new(),
            )),
        );
    }
    ctrl.process_messages().await;
    assert_eq!(ctrl.role(partition), ReplicaRole::AwaitingSnapshot);

    let stalled_since = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_secs(11))
        .unwrap();
    if let Some(request) = ctrl.requested_snapshots.get_mut(partition) {
        request.last_activity = stalled_since;
    }
    ctrl.apply_raft_partition_map(map).await;

    assert_eq!(snapshot_requests_to(&ctrl, node(3)), 2);
}

#[tokio::test]
async fn a_marked_primary_still_trusts_its_disk_after_a_stale_map_named_it_replica() {
    let partition = session_partition("c1");
    let backend = crate::cluster::test_support::FaultyBackend::shared();
    let mut before_crash = controller_on(2, &backend);
    before_crash
        .apply_raft_partition_map(map_with(partition, node(2), &[], 2))
        .await;
    drop(before_crash);

    let mut stale_source = controller(3);
    stale_source.become_primary(partition, Epoch::new(1));
    stale_source
        .stores()
        .create_session_replicated("c1")
        .unwrap();
    let mut restarted = controller_on(2, &backend);
    restarted
        .apply_raft_partition_map(map_with(partition, node(3), &[node(2)], 1))
        .await;
    assert_eq!(restarted.role(partition), ReplicaRole::AwaitingSnapshot);
    restarted
        .apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 2))
        .await;
    assert_eq!(restarted.role(partition), ReplicaRole::Primary);

    serve_snapshot(&mut stale_source, &mut restarted, partition).await;

    assert_eq!(restarted.role(partition), ReplicaRole::Primary);
    assert!(restarted.stores().sessions.get("c1").is_none());
}

#[tokio::test]
async fn a_store_from_before_the_marker_is_trusted_only_for_its_first_map() {
    let first = session_partition("c1");
    let later = session_partition("c2");
    let backend = crate::cluster::test_support::FaultyBackend::shared();
    let seed = controller_on(2, &backend);
    let (_, write) = seed
        .stores()
        .db_create_replicated("bigdocs", "r1", b"{\"v\":1}", 1)
        .unwrap();
    seed.stores().apply_write(&write).unwrap();
    drop(seed);

    let mut upgraded = controller_on(2, &backend);
    upgraded.stores().recover().unwrap();
    upgraded
        .apply_raft_partition_map(map_with(first, node(2), &[node(3)], 1))
        .await;
    assert_eq!(upgraded.role(first), ReplicaRole::Primary);

    let mut map = map_with(first, node(2), &[node(3)], 1);
    map.set(
        later,
        PartitionAssignment::new(node(2), vec![node(3)], Epoch::new(4)),
    );
    upgraded.apply_raft_partition_map(map).await;

    assert_eq!(upgraded.role(later), ReplicaRole::AwaitingSnapshot);
}

#[tokio::test]
async fn a_primary_built_from_an_empty_snapshot_trusts_its_disk_after_a_restart() {
    let partition = session_partition("c1");
    let mut replica = controller(3);
    replica.become_replica(partition, Epoch::new(1), 0);
    replica
        .stores()
        .db_schema
        .register("bigdocs", b"{\"fields\":[]}")
        .unwrap();
    let backend = crate::cluster::test_support::FaultyBackend::shared();
    let map = map_with(partition, node(2), &[node(3)], 1);

    let mut fresh = controller_on(2, &backend);
    fresh.apply_raft_partition_map(map.clone()).await;
    serve_snapshot(&mut replica, &mut fresh, partition).await;
    assert_eq!(fresh.role(partition), ReplicaRole::Primary);
    drop(fresh);

    let mut restarted = controller_on(2, &backend);
    restarted.stores().recover().unwrap();
    restarted.apply_raft_partition_map(map).await;

    assert_eq!(restarted.role(partition), ReplicaRole::Primary);
}

#[tokio::test]
async fn a_snapshot_arriving_after_the_partition_is_held_is_ignored() {
    let partition = crate::cluster::db::data_partition("bigdocs", "r1");
    let mut replica = controller(3);
    replica.become_replica(partition, Epoch::new(1), 0);
    replica
        .stores()
        .db_create_replicated("bigdocs", "r1", b"{\"v\":1}", 1)
        .unwrap();
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;
    serve_snapshot(&mut replica, &mut ctrl, partition).await;
    assert_eq!(ctrl.role(partition), ReplicaRole::Primary);
    ctrl.stores()
        .db_update_replicated("bigdocs", "r1", b"{\"v\":2}", 2)
        .unwrap();

    for (to, message) in replica.transport.sent_messages() {
        if to == node(2) && matches!(message, ClusterMessage::SnapshotChunk(_)) {
            ctrl.transport.inject_message(node(3), message);
        }
    }
    ctrl.process_messages().await;

    let stored = ctrl.stores().db_get("bigdocs", "r1").unwrap();
    let value: serde_json::Value = serde_json::from_slice(&stored.data).unwrap();
    assert_eq!(value["v"], 2);
}

#[tokio::test]
async fn a_local_write_waits_while_the_primary_waits_for_its_snapshot() {
    let partition = session_partition("c1");
    let mut replica = controller(3);
    replica.become_replica(partition, Epoch::new(1), 0);
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;

    ctrl.write_or_forward(session_write("c1", 1)).await;
    assert!(ctrl.stores().sessions.get("c1").is_none());

    serve_snapshot(&mut replica, &mut ctrl, partition).await;

    assert_eq!(ctrl.role(partition), ReplicaRole::Primary);
    assert!(ctrl.stores().sessions.get("c1").is_some());
}

#[tokio::test]
async fn held_writes_are_applied_as_soon_as_the_snapshot_lands() {
    let partition = session_partition("c1");
    let mut replica = controller(3);
    replica.become_replica(partition, Epoch::new(1), 0);
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;
    ctrl.transport.inject_message(
        node(1),
        ClusterMessage::WriteRequest(session_write("c1", 1)),
    );
    ctrl.process_messages().await;

    serve_snapshot(&mut replica, &mut ctrl, partition).await;

    assert!(ctrl.stores().sessions.get("c1").is_some());
}

#[tokio::test]
async fn a_waiting_primary_keeps_its_live_source_when_the_replica_moves() {
    let partition = session_partition("c1");
    let mut holder = controller(2);
    holder.become_replica(partition, Epoch::new(1), 3);
    holder.stores().create_session_replicated("c1").unwrap();
    let mut ctrl = controller(1);
    ctrl.heartbeat.receive_heartbeat(
        node(2),
        &crate::cluster::protocol::Heartbeat::create(node(2), 0),
        0,
    );
    ctrl.apply_raft_partition_map(map_with(partition, node(1), &[node(2)], 1))
        .await;
    assert_eq!(snapshot_requests_to(&ctrl, node(2)), 1);

    ctrl.apply_raft_partition_map(map_with(partition, node(1), &[node(3)], 2))
        .await;
    assert_eq!(snapshot_requests_to(&ctrl, node(3)), 0);

    serve_snapshot(&mut holder, &mut ctrl, partition).await;

    assert_eq!(ctrl.role(partition), ReplicaRole::Primary);
    assert!(ctrl.stores().sessions.get("c1").is_some());
}

#[tokio::test]
async fn a_held_write_is_forwarded_when_the_partition_moves_away_during_the_wait() {
    let partition = session_partition("c1");
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;
    ctrl.write_or_forward(session_write("c1", 0)).await;

    ctrl.apply_raft_partition_map(map_with(partition, node(3), &[node(2)], 2))
        .await;

    let forwarded = ctrl
        .transport
        .sent_messages()
        .into_iter()
        .filter(|(to, message)| {
            *to == node(3) && matches!(message, ClusterMessage::WriteRequest(_))
        })
        .count();
    assert_eq!(forwarded, 1);
}

#[tokio::test]
async fn a_held_write_times_out_while_the_primary_still_waits() {
    let partition = session_partition("c1");
    let mut replica = controller(3);
    replica.become_replica(partition, Epoch::new(1), 0);
    let map = map_with(partition, node(2), &[node(3)], 1);
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map.clone()).await;
    ctrl.transport.inject_message(
        node(1),
        ClusterMessage::WriteRequest(session_write("c1", 1)),
    );
    ctrl.process_messages().await;

    let expired = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_secs(31))
        .unwrap();
    for held in &mut ctrl.held_writes {
        held.since = expired;
    }
    ctrl.apply_raft_partition_map(map).await;
    serve_snapshot(&mut replica, &mut ctrl, partition).await;

    assert_eq!(ctrl.role(partition), ReplicaRole::Primary);
    assert!(ctrl.stores().sessions.get("c1").is_none());
}

#[tokio::test]
async fn new_ids_are_only_placed_on_partitions_held_in_the_primary_role() {
    let waiting = PartitionId::new(10).unwrap();
    let held = PartitionId::new(11).unwrap();
    let mut ctrl = controller(2);
    ctrl.become_primary(held, Epoch::new(1));
    let mut map = map_with(waiting, node(2), &[node(3)], 1);
    map.set(
        held,
        PartitionAssignment::new(node(2), vec![node(3)], Epoch::new(1)),
    );
    ctrl.apply_raft_partition_map(map).await;
    assert_eq!(ctrl.role(waiting), ReplicaRole::AwaitingSnapshot);

    for _ in 0..512 {
        assert_eq!(ctrl.pick_partition_for_create(), held);
    }
}

#[tokio::test]
async fn a_primary_built_from_a_topic_index_snapshot_takes_a_snapshot_after_a_restart() {
    let topic = "probe/topic/a";
    let partition = crate::cluster::topic_index::topic_partition(topic);
    let other = PartitionId::new((partition.get() + 1) % 256).unwrap();
    let mut replica = controller(3);
    replica.become_replica(partition, Epoch::new(1), 0);
    replica
        .stores()
        .topics
        .subscribe(topic, "remote-client", other, 1)
        .unwrap();
    let backend = crate::cluster::test_support::FaultyBackend::shared();
    let map = map_with(partition, node(2), &[node(3)], 1);

    let mut fresh = controller_on(2, &backend);
    fresh.apply_raft_partition_map(map.clone()).await;
    serve_snapshot(&mut replica, &mut fresh, partition).await;
    assert_eq!(fresh.role(partition), ReplicaRole::Primary);
    drop(fresh);

    let mut restarted = controller_on(2, &backend);
    restarted.stores().recover().unwrap();
    restarted.apply_raft_partition_map(map).await;

    assert_eq!(restarted.role(partition), ReplicaRole::AwaitingSnapshot);
}

#[tokio::test]
async fn taking_the_primary_role_by_import_is_reported_on_the_next_map_apply() {
    let partition = session_partition("c1");
    let mut replica = controller(3);
    replica.become_replica(partition, Epoch::new(1), 0);
    let map = map_with(partition, node(2), &[node(3)], 1);
    let mut ctrl = controller(2);
    assert!(!ctrl.apply_raft_partition_map(map.clone()).await);

    serve_snapshot(&mut replica, &mut ctrl, partition).await;

    assert!(ctrl.apply_raft_partition_map(map.clone()).await);
    assert!(!ctrl.apply_raft_partition_map(map).await);
}

#[tokio::test]
async fn taking_the_primary_role_by_import_is_reported_with_a_changed_map() {
    let partition = session_partition("c1");
    let other = PartitionId::new((partition.get() + 1) % 256).unwrap();
    let mut replica = controller(3);
    replica.become_replica(partition, Epoch::new(1), 0);
    let map = map_with(partition, node(2), &[node(3)], 1);
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map.clone()).await;
    serve_snapshot(&mut replica, &mut ctrl, partition).await;

    let mut changed = map;
    changed.set(
        other,
        PartitionAssignment::new(node(3), vec![node(1)], Epoch::new(1)),
    );

    assert!(ctrl.apply_raft_partition_map(changed).await);
}

#[tokio::test]
async fn a_stalled_request_keeps_a_live_source_that_left_the_assignment() {
    let partition = session_partition("c1");
    let mut ctrl = controller(1);
    ctrl.heartbeat.receive_heartbeat(
        node(2),
        &crate::cluster::protocol::Heartbeat::create(node(2), 0),
        0,
    );
    ctrl.apply_raft_partition_map(map_with(partition, node(1), &[node(2)], 1))
        .await;
    let moved = map_with(partition, node(1), &[node(3)], 2);
    ctrl.apply_raft_partition_map(moved.clone()).await;
    assert_eq!(snapshot_requests_to(&ctrl, node(3)), 0);

    let stalled_since = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_secs(11))
        .unwrap();
    if let Some(request) = ctrl.requested_snapshots.get_mut(partition) {
        request.last_activity = stalled_since;
    }
    ctrl.apply_raft_partition_map(moved).await;

    assert_eq!(snapshot_requests_to(&ctrl, node(2)), 2);
    assert_eq!(snapshot_requests_to(&ctrl, node(3)), 0);
}

#[tokio::test]
async fn a_write_held_for_another_node_is_not_sent_on_when_the_partition_moves_away() {
    let partition = session_partition("c1");
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;
    ctrl.transport.inject_message(
        node(1),
        ClusterMessage::WriteRequest(session_write("c1", 1)),
    );
    ctrl.process_messages().await;

    ctrl.apply_raft_partition_map(map_with(partition, node(3), &[node(2)], 2))
        .await;

    let forwarded = ctrl
        .transport
        .sent_messages()
        .into_iter()
        .filter(|(_, message)| matches!(message, ClusterMessage::WriteRequest(_)))
        .count();
    assert_eq!(forwarded, 0);
}

#[tokio::test]
async fn a_node_waiting_for_data_answers_a_snapshot_request_once_it_holds_the_partition() {
    let partition = session_partition("c1");
    let mut holder = controller(2);
    holder.become_replica(partition, Epoch::new(1), 0);
    holder.stores().create_session_replicated("c1").unwrap();
    let mut waiting = controller(1);
    waiting
        .apply_raft_partition_map(map_with(partition, node(1), &[node(2)], 1))
        .await;
    let mut new_replica = controller(3);
    new_replica
        .apply_raft_partition_map(map_with(partition, node(1), &[node(3)], 2))
        .await;
    assert_eq!(new_replica.role(partition), ReplicaRole::AwaitingSnapshot);

    serve_snapshot(&mut waiting, &mut new_replica, partition).await;
    assert_eq!(new_replica.role(partition), ReplicaRole::AwaitingSnapshot);

    serve_snapshot(&mut holder, &mut waiting, partition).await;
    assert_eq!(waiting.role(partition), ReplicaRole::Primary);
    for (to, message) in waiting.transport.sent_messages() {
        if to == node(3) && matches!(message, ClusterMessage::SnapshotChunk(_)) {
            new_replica.transport.inject_message(node(1), message);
        }
    }
    new_replica.process_messages().await;

    assert_eq!(new_replica.role(partition), ReplicaRole::Replica);
    assert!(new_replica.stores().sessions.get("c1").is_some());
}

#[tokio::test]
async fn two_empty_nodes_waiting_on_each_other_both_take_their_roles() {
    let partition = session_partition("c1");
    let map = map_with(partition, node(1), &[node(2)], 1);
    let mut primary = controller(1);
    let mut replica = controller(2);
    primary.apply_raft_partition_map(map.clone()).await;
    replica.apply_raft_partition_map(map).await;

    serve_snapshot(&mut replica, &mut primary, partition).await;
    serve_snapshot(&mut primary, &mut replica, partition).await;

    assert_eq!(primary.role(partition), ReplicaRole::Primary);
    assert_eq!(replica.role(partition), ReplicaRole::Replica);
}

async fn swap_a_waiting_primary_after_a_replica_move() -> (
    NodeController<MockTransport>,
    NodeController<MockTransport>,
    NodeController<MockTransport>,
    PartitionId,
) {
    let partition = session_partition("c1");
    let mut holder = controller(2);
    holder.become_replica(partition, Epoch::new(1), 0);
    holder.stores().create_session_replicated("c1").unwrap();
    let mut wiped = controller(1);
    let mut joiner = controller(3);
    wiped
        .apply_raft_partition_map(map_with(partition, node(1), &[node(2)], 1))
        .await;
    let moved = map_with(partition, node(1), &[node(3)], 2);
    wiped.apply_raft_partition_map(moved.clone()).await;
    joiner.apply_raft_partition_map(moved).await;
    wiped.transport.inject_message(
        node(3),
        ClusterMessage::SnapshotRequest(SnapshotRequest::create(partition, node(3))),
    );
    wiped.process_messages().await;
    let swapped = map_with(partition, node(3), &[node(1)], 3);
    wiped.apply_raft_partition_map(swapped.clone()).await;
    joiner.apply_raft_partition_map(swapped).await;
    (holder, wiped, joiner, partition)
}

#[tokio::test]
async fn a_waiting_primary_swapped_to_replica_keeps_its_live_source() {
    let (mut holder, mut wiped, mut joiner, partition) =
        swap_a_waiting_primary_after_a_replica_move().await;

    assert_eq!(snapshot_requests_to(&wiped, node(3)), 0);
    serve_snapshot(&mut joiner, &mut wiped, partition).await;
    assert_eq!(wiped.role(partition), ReplicaRole::AwaitingSnapshot);

    serve_snapshot(&mut holder, &mut wiped, partition).await;
    assert_eq!(wiped.role(partition), ReplicaRole::Replica);
    for (to, message) in wiped.transport.sent_messages() {
        if to == node(3) && matches!(message, ClusterMessage::SnapshotChunk(_)) {
            joiner.transport.inject_message(node(1), message);
        }
    }
    joiner.process_messages().await;

    assert_eq!(joiner.role(partition), ReplicaRole::Primary);
    assert!(joiner.stores().sessions.get("c1").is_some());
}

#[tokio::test]
async fn a_stalled_request_of_a_waiting_primary_swapped_to_replica_keeps_its_source() {
    let (_, mut wiped, _, partition) = swap_a_waiting_primary_after_a_replica_move().await;
    let stalled_since = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_secs(11))
        .unwrap();
    if let Some(request) = wiped.requested_snapshots.get_mut(partition) {
        request.last_activity = stalled_since;
    }

    wiped
        .apply_raft_partition_map(map_with(partition, node(3), &[node(1)], 3))
        .await;

    assert_eq!(snapshot_requests_to(&wiped, node(2)), 2);
    assert_eq!(snapshot_requests_to(&wiped, node(3)), 0);
}

async fn a_waiting_primary_removed_while_a_request_is_deferred() -> (
    NodeController<MockTransport>,
    NodeController<MockTransport>,
    NodeController<MockTransport>,
    PartitionId,
) {
    let partition = session_partition("c1");
    let mut holder = controller(2);
    holder.become_replica(partition, Epoch::new(1), 0);
    holder.stores().create_session_replicated("c1").unwrap();
    let mut wiped = controller(1);
    let mut joiner = controller(3);
    wiped
        .apply_raft_partition_map(map_with(partition, node(1), &[node(2)], 1))
        .await;
    let moved = map_with(partition, node(1), &[node(3)], 2);
    wiped.apply_raft_partition_map(moved.clone()).await;
    joiner.apply_raft_partition_map(moved).await;
    wiped.transport.inject_message(
        node(3),
        ClusterMessage::SnapshotRequest(SnapshotRequest::create(partition, node(3))),
    );
    wiped.process_messages().await;
    (holder, wiped, joiner, partition)
}

fn deliver_chunks(from: &NodeController<MockTransport>, to: &NodeController<MockTransport>) {
    for (target, message) in from.transport.sent_messages() {
        if target == to.node_id() && matches!(message, ClusterMessage::SnapshotChunk(_)) {
            to.transport.inject_message(from.node_id(), message);
        }
    }
}

#[tokio::test]
async fn a_waiting_node_that_leaves_the_assignment_does_not_serve_its_deferred_requests() {
    let (_, mut wiped, mut joiner, partition) =
        a_waiting_primary_removed_while_a_request_is_deferred().await;

    wiped
        .apply_raft_partition_map(map_with(partition, node(3), &[node(2)], 3))
        .await;
    deliver_chunks(&wiped, &joiner);
    joiner.process_messages().await;
    joiner
        .apply_raft_partition_map(map_with(partition, node(3), &[node(2)], 3))
        .await;

    assert_ne!(joiner.role(partition), ReplicaRole::Primary);
}

#[tokio::test]
async fn a_replica_promoted_while_its_source_waits_asks_the_new_replica() {
    let (mut holder, mut wiped, mut joiner, partition) =
        a_waiting_primary_removed_while_a_request_is_deferred().await;
    let removed = map_with(partition, node(3), &[node(2)], 3);

    joiner.apply_raft_partition_map(removed.clone()).await;
    wiped.apply_raft_partition_map(removed).await;
    deliver_chunks(&wiped, &joiner);
    joiner.process_messages().await;
    serve_snapshot(&mut holder, &mut joiner, partition).await;

    assert_eq!(joiner.role(partition), ReplicaRole::Primary);
    assert!(joiner.stores().sessions.get("c1").is_some());
}

fn chunks_to(ctrl: &NodeController<MockTransport>, to: NodeId) -> usize {
    ctrl.transport
        .sent_messages()
        .into_iter()
        .filter(|(target, message)| {
            *target == to && matches!(message, ClusterMessage::SnapshotChunk(_))
        })
        .count()
}

fn stall_request(ctrl: &mut NodeController<MockTransport>, partition: PartitionId) {
    let stalled_since = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_secs(11))
        .unwrap();
    if let Some(request) = ctrl.requested_snapshots.get_mut(partition) {
        request.last_activity = stalled_since;
    }
}

async fn waiting_with_a_deferred_request() -> (NodeController<MockTransport>, PartitionId) {
    let partition = session_partition("c1");
    let mut waiting = controller(1);
    waiting
        .apply_raft_partition_map(map_with(partition, node(1), &[node(2)], 1))
        .await;
    waiting.transport.inject_message(
        node(3),
        ClusterMessage::SnapshotRequest(SnapshotRequest::create(partition, node(3))),
    );
    waiting.process_messages().await;
    (waiting, partition)
}

#[tokio::test]
async fn a_deferred_request_stays_deferred_while_the_node_still_waits() {
    let (mut waiting, partition) = waiting_with_a_deferred_request().await;

    waiting
        .apply_raft_partition_map(map_with(partition, node(1), &[node(2)], 1))
        .await;

    assert_eq!(chunks_to(&waiting, node(3)), 0);
    assert_eq!(waiting.deferred_snapshot_requests.len(), 1);
}

#[tokio::test]
async fn a_repeated_request_is_deferred_once() {
    let (mut waiting, partition) = waiting_with_a_deferred_request().await;
    waiting.transport.inject_message(
        node(3),
        ClusterMessage::SnapshotRequest(SnapshotRequest::create(partition, node(3))),
    );
    waiting.process_messages().await;

    assert_eq!(waiting.deferred_snapshot_requests.len(), 1);
}

#[tokio::test]
async fn a_waiting_node_given_the_primary_role_by_the_map_serves_its_deferred_requests() {
    let (mut waiting, partition) = waiting_with_a_deferred_request().await;

    waiting
        .apply_raft_partition_map(map_with(partition, node(1), &[], 2))
        .await;

    assert_eq!(waiting.role(partition), ReplicaRole::Primary);
    assert_eq!(chunks_to(&waiting, node(3)), 1);
}

#[tokio::test]
async fn a_chunk_from_the_source_keeps_the_request_from_stalling() {
    let partition = session_partition("c1");
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;
    stall_request(&mut ctrl, partition);
    ctrl.transport.inject_message(
        node(3),
        ClusterMessage::SnapshotChunk(crate::cluster::snapshot::SnapshotChunk::new(
            partition,
            0,
            2,
            0,
            Vec::new(),
        )),
    );
    ctrl.process_messages().await;

    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;

    assert_eq!(snapshot_requests_to(&ctrl, node(3)), 1);
}

#[tokio::test]
async fn a_stalled_request_is_sent_again_when_another_partition_changes() {
    let partition = session_partition("c1");
    let other = PartitionId::new((partition.get() + 1) % 256).unwrap();
    let mut ctrl = controller(2);
    let map = map_with(partition, node(2), &[node(3)], 1);
    ctrl.apply_raft_partition_map(map.clone()).await;
    stall_request(&mut ctrl, partition);

    let mut changed = map;
    changed.set(
        other,
        PartitionAssignment::new(node(3), vec![node(1)], Epoch::new(1)),
    );
    ctrl.apply_raft_partition_map(changed).await;

    assert_eq!(snapshot_requests_to(&ctrl, node(3)), 2);
}

#[tokio::test]
async fn a_stalled_request_to_a_dead_source_asks_the_current_replica() {
    let partition = session_partition("c1");
    let mut ctrl = controller(1);
    ctrl.heartbeat.receive_heartbeat(
        node(2),
        &crate::cluster::protocol::Heartbeat::create(node(2), 0),
        0,
    );
    ctrl.apply_raft_partition_map(map_with(partition, node(1), &[node(2)], 1))
        .await;
    let moved = map_with(partition, node(1), &[node(3)], 2);
    ctrl.apply_raft_partition_map(moved.clone()).await;
    assert_eq!(snapshot_requests_to(&ctrl, node(3)), 0);
    ctrl.apply_dead_nodes(&[node(2)]);
    stall_request(&mut ctrl, partition);

    ctrl.apply_raft_partition_map(moved).await;

    assert_eq!(snapshot_requests_to(&ctrl, node(3)), 1);
}

async fn waiting_with_a_partial_snapshot() -> (NodeController<MockTransport>, PartitionId) {
    let partition = session_partition("c1");
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(3), &[node(2)], 1))
        .await;
    ctrl.transport.inject_message(
        node(3),
        ClusterMessage::SnapshotChunk(crate::cluster::snapshot::SnapshotChunk::new(
            partition,
            0,
            2,
            0,
            Vec::new(),
        )),
    );
    ctrl.process_messages().await;
    assert!(ctrl.pending_snapshots.contains_key(&partition));
    (ctrl, partition)
}

#[tokio::test]
async fn taking_a_role_directly_drops_the_snapshot_in_progress() {
    let (mut primary, partition) = waiting_with_a_partial_snapshot().await;
    primary.become_primary(partition, Epoch::new(2));
    let (mut replica, _) = waiting_with_a_partial_snapshot().await;
    replica.become_replica(partition, Epoch::new(2), 0);
    let (mut stepped_down, _) = waiting_with_a_partial_snapshot().await;
    stepped_down.step_down(partition);

    for ctrl in [&primary, &replica, &stepped_down] {
        assert!(!ctrl.pending_snapshots.contains_key(&partition));
        assert!(!ctrl.requested_snapshots.contains_key(partition));
    }
}

#[tokio::test]
async fn a_waiting_replica_swapped_to_primary_keeps_its_source_when_the_replica_moves() {
    let partition = session_partition("c1");
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(1), &[node(2)], 1))
        .await;
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(1)], 2))
        .await;

    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 3))
        .await;

    assert_eq!(ctrl.role(partition), ReplicaRole::AwaitingSnapshot);
    assert_eq!(snapshot_requests_to(&ctrl, node(1)), 1);
    assert_eq!(snapshot_requests_to(&ctrl, node(3)), 0);
}
