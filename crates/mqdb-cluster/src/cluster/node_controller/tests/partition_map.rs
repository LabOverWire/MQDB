// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::cluster::raft::PartitionUpdate;
use crate::cluster::snapshot::SnapshotRequest;
use crate::cluster::{PartitionAssignment, PartitionMap, PartitionMapExt};

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
    exchange_catchup(source, target).await;
}

async fn exchange_catchup(
    source: &mut NodeController<MockTransport>,
    target: &mut NodeController<MockTransport>,
) {
    for (to, message) in target.transport.sent_messages() {
        if to == source.node_id() && matches!(message, ClusterMessage::CatchupRequest(_)) {
            source.transport.inject_message(target.node_id(), message);
        }
    }
    source.process_messages().await;
    for (to, message) in source.transport.sent_messages() {
        if to == target.node_id() && matches!(message, ClusterMessage::CatchupResponse(_)) {
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
async fn a_restarted_primary_does_not_trust_a_marker_older_than_its_role() {
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

    ctrl.snapshot_request_stalled(partition);
    ctrl.drive_snapshots(crate::cluster::node_controller::snapshot::SNAPSHOT_STALL_TIMEOUT_MS + 1)
        .await;
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
                requested_owner: None,
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
async fn a_primary_built_from_a_snapshot_trusts_its_disk_after_a_restart() {
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

    assert_eq!(restarted.role(partition), ReplicaRole::Primary);
    assert_eq!(snapshot_requests_to(&restarted, node(3)), 0);
    assert!(restarted.stores().sessions.get("c1").is_some());
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
        .db_create_write("bigdocs", "r1", b"{\"v\":1}", 1)
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

async fn an_unmarked_store_keeps_the_role(record: ReplicationWrite) {
    let partition = record.partition;
    let backend = crate::cluster::test_support::FaultyBackend::shared();
    let first_version = controller_on(2, &backend);
    first_version.stores().apply_write(&record).unwrap();
    drop(first_version);

    let mut upgraded = controller_on(2, &backend);
    upgraded.stores().recover().unwrap();
    assert_eq!(upgraded.stores().db_data.count(), 0);
    upgraded
        .apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;

    assert_eq!(upgraded.role(partition), ReplicaRole::Primary);
    assert_eq!(snapshot_requests_to(&upgraded, node(3)), 0);
}

#[tokio::test]
async fn an_upgraded_store_holding_only_sessions_keeps_the_role_without_a_snapshot() {
    an_unmarked_store_keeps_the_role(session_write("c1", 1)).await;
}

#[tokio::test]
async fn an_upgraded_store_holding_only_retained_messages_keeps_the_role_without_a_snapshot() {
    let (_, record) = controller(9)
        .stores()
        .set_retained_replicated("topic/one", 1, b"payload", 1);
    an_unmarked_store_keeps_the_role(record).await;
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

    ctrl.snapshot_request_stalled(partition);
    ctrl.drive_snapshots(crate::cluster::node_controller::snapshot::SNAPSHOT_STALL_TIMEOUT_MS + 1)
        .await;
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
        .db_create_write("bigdocs", "r1", b"{\"v\":1}", 1)
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
    let (_, created) = replica
        .stores()
        .db_create_write("bigdocs", "r1", b"{\"v\":1}", 1)
        .unwrap();
    replica.stores().apply_write(&created).unwrap();
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;
    serve_snapshot(&mut replica, &mut ctrl, partition).await;
    assert_eq!(ctrl.role(partition), ReplicaRole::Primary);
    let (_, updated) = ctrl
        .stores()
        .db_update_write("bigdocs", "r1", b"{\"v\":2}", 2)
        .unwrap();
    ctrl.stores().apply_write(&updated).unwrap();

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
async fn a_primary_built_from_a_topic_index_snapshot_trusts_its_disk_after_a_restart() {
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

    assert_eq!(restarted.role(partition), ReplicaRole::Primary);
}

fn connect_subscriber(owner: &NodeController<MockTransport>, client_id: &str, filters: &[&str]) {
    let stores = owner.stores();
    stores.client_locations.claim(client_id, owner.node_id());
    for filter in filters {
        if filter.contains('+') || filter.contains('#') {
            stores
                .wildcards
                .subscribe_mqtt(filter, client_id, 1)
                .unwrap();
        } else {
            stores
                .topics
                .subscribe(filter, client_id, session_partition(client_id), 1)
                .unwrap();
        }
    }
}

async fn deliver_over_the_wire(
    from: &mut NodeController<MockTransport>,
    to: &mut NodeController<MockTransport>,
) {
    for (dest, message) in from.transport.take_sent() {
        if dest == to.node_id() {
            let mut wire = Vec::new();
            message.encode_payload(&mut wire);
            let decoded = ClusterMessage::decode_from_wire(message.message_type(), &wire)
                .expect("message decodes from the wire");
            to.transport.inject_message(from.node_id(), decoded);
        }
    }
    to.process_messages().await;
}

async fn hear_from_peer_after_restart(
    restarted: &mut NodeController<MockTransport>,
    peer: &mut NodeController<MockTransport>,
) {
    restarted.transport.inject_message(
        peer.node_id(),
        ClusterMessage::Heartbeat(Heartbeat::create(peer.node_id(), 0)),
    );
    restarted.process_messages().await;
    deliver_over_the_wire(restarted, peer).await;
    deliver_over_the_wire(peer, restarted).await;
}

fn routed_clients(ctrl: &NodeController<MockTransport>, topic: &str) -> Vec<String> {
    let wildcard_matches = ctrl.stores().wildcards.match_topic(topic);
    let mut clients: Vec<String> = crate::cluster::PublishRouter::new(&ctrl.stores().topics)
        .route_with_wildcards(topic, &wildcard_matches)
        .targets
        .into_iter()
        .map(|target| target.client_id)
        .collect();
    clients.sort();
    clients
}

fn client_outside(partitions: &[PartitionId], prefix: &str) -> String {
    (0..)
        .map(|n| format!("{prefix}-{n}"))
        .find(|client| !partitions.contains(&session_partition(client)))
        .unwrap()
}

#[tokio::test]
async fn a_restarted_primary_routes_its_publishes_to_subscribers_whose_sessions_it_does_not_hold() {
    let topic = "probe/topic/a";
    let partition = crate::cluster::topic_index::topic_partition(topic);
    let client = client_outside(&[partition], "remote-client");
    let mut owner = controller(1);
    connect_subscriber(&owner, &client, &[topic]);
    let mut replica = controller(3);
    replica.become_replica(partition, Epoch::new(1), 0);
    replica
        .stores()
        .topics
        .subscribe(topic, &client, session_partition(&client), 1)
        .unwrap();
    let backend = crate::cluster::test_support::FaultyBackend::shared();
    let map = map_with(partition, node(2), &[node(3)], 1);
    let mut fresh = controller_on(2, &backend);
    fresh.apply_raft_partition_map(map.clone()).await;
    serve_snapshot(&mut replica, &mut fresh, partition).await;
    assert_eq!(routed_clients(&fresh, topic), std::slice::from_ref(&client));
    drop(fresh);

    let mut restarted = controller_on(2, &backend);
    restarted.stores().recover().unwrap();
    restarted.apply_raft_partition_map(map).await;
    assert_eq!(restarted.role(partition), ReplicaRole::Primary);
    hear_from_peer_after_restart(&mut restarted, &mut owner).await;

    assert_eq!(
        routed_clients(&restarted, topic),
        std::slice::from_ref(&client)
    );
    assert_eq!(restarted.connected_node(&client), Some(node(1)));
}

#[tokio::test]
async fn a_restarted_node_relearns_exact_and_wildcard_subscribers_connected_to_a_peer() {
    let mut owner = controller(1);
    let client = client_outside(&[], "remote-client");
    connect_subscriber(&owner, &client, &["probe/exact", "probe/+/wild"]);
    owner
        .stores()
        .topics
        .subscribe(
            "probe/exact",
            "elsewhere",
            session_partition("elsewhere"),
            1,
        )
        .unwrap();
    owner.stores().client_locations.claim("elsewhere", node(3));

    let mut restarted = controller(2);
    hear_from_peer_after_restart(&mut restarted, &mut owner).await;

    assert_eq!(
        routed_clients(&restarted, "probe/exact"),
        std::slice::from_ref(&client)
    );
    assert_eq!(
        routed_clients(&restarted, "probe/x/wild"),
        std::slice::from_ref(&client)
    );
    assert_eq!(restarted.connected_node(&client), Some(node(1)));
    assert_eq!(restarted.connected_node("elsewhere"), None);
}

#[tokio::test]
async fn a_subscription_dropped_while_the_node_was_down_does_not_come_back_after_a_restart() {
    let client = client_outside(&[], "remote-client");
    let mut owner = controller(1);
    connect_subscriber(&owner, &client, &["probe/kept"]);
    let backend = crate::cluster::test_support::FaultyBackend::shared();
    let mut stale = crate::cluster::MqttSubscriptionSnapshot::create(&client);
    stale.add_subscription("probe/kept", 1);
    stale.add_subscription("probe/dropped", 1);
    stale.add_subscription("probe/+/dropped", 1);
    controller_on(2, &backend)
        .stores()
        .apply_write(&ReplicationWrite::new(
            session_partition(&client),
            Operation::Insert,
            Epoch::new(1),
            1,
            crate::cluster::entity::SUBSCRIPTIONS.to_string(),
            client.clone(),
            crate::cluster::SubscriptionCache::serialize(&stale),
        ))
        .unwrap();

    let mut restarted = controller_on(2, &backend);
    restarted.stores().recover().unwrap();
    assert_eq!(
        routed_clients(&restarted, "probe/dropped"),
        std::slice::from_ref(&client)
    );
    hear_from_peer_after_restart(&mut restarted, &mut owner).await;

    assert_eq!(
        routed_clients(&restarted, "probe/kept"),
        std::slice::from_ref(&client)
    );
    assert!(routed_clients(&restarted, "probe/dropped").is_empty());
    assert!(routed_clients(&restarted, "probe/x/dropped").is_empty());
}

fn resync_requests_sent(ctrl: &NodeController<MockTransport>, to: NodeId) -> usize {
    ctrl.transport
        .sent_messages()
        .iter()
        .filter(|(dest, message)| {
            *dest == to && matches!(message, ClusterMessage::SubscriptionResyncRequest(_))
        })
        .count()
}

#[tokio::test]
async fn a_peer_is_asked_for_its_subscribers_once_each_time_it_comes_back() {
    let mut ctrl = controller(2);
    let heartbeat = || ClusterMessage::Heartbeat(Heartbeat::create(node(1), 0));
    ctrl.transport.inject_message(node(1), heartbeat());
    ctrl.transport.inject_message(node(1), heartbeat());
    ctrl.process_messages().await;
    assert_eq!(resync_requests_sent(&ctrl, node(1)), 1);

    ctrl.apply_dead_nodes(&[node(1)]);
    ctrl.transport.inject_message(node(1), heartbeat());
    ctrl.process_messages().await;
    assert_eq!(resync_requests_sent(&ctrl, node(1)), 2);
}

#[tokio::test]
async fn a_peer_resync_leaves_a_client_connected_here_alone() {
    let client = client_outside(&[], "moved-client");
    let mut owner = controller(1);
    connect_subscriber(&owner, &client, &["probe/old"]);
    let mut restarted = controller(2);
    connect_subscriber(&restarted, &client, &["probe/new"]);

    hear_from_peer_after_restart(&mut restarted, &mut owner).await;

    assert_eq!(restarted.connected_node(&client), Some(node(2)));
    assert_eq!(
        routed_clients(&restarted, "probe/new"),
        std::slice::from_ref(&client)
    );
    assert!(routed_clients(&restarted, "probe/old").is_empty());
}

#[tokio::test]
async fn a_subscription_made_after_the_resync_reply_is_kept() {
    let client = client_outside(&[], "remote-client");
    let mut owner = controller(1);
    connect_subscriber(&owner, &client, &["probe/first"]);
    let mut restarted = controller(2);
    restarted.transport.inject_message(
        owner.node_id(),
        ClusterMessage::Heartbeat(Heartbeat::create(owner.node_id(), 0)),
    );
    restarted.process_messages().await;
    deliver_over_the_wire(&mut restarted, &mut owner).await;
    connect_subscriber(&owner, &client, &["probe/second"]);
    owner
        .transport
        .broadcast(ClusterMessage::TopicSubscriptionBroadcast(
            crate::cluster::TopicSubscriptionBroadcast::subscribe(
                "probe/second",
                &client,
                session_partition(&client),
                1,
            ),
        ))
        .await
        .unwrap();
    let sent: Vec<_> = owner
        .transport
        .take_sent()
        .into_iter()
        .map(|(_, message)| (node(2), message))
        .collect();
    owner.transport.outbox.lock().unwrap().extend(sent);
    deliver_over_the_wire(&mut owner, &mut restarted).await;

    assert_eq!(
        routed_clients(&restarted, "probe/first"),
        std::slice::from_ref(&client)
    );
    assert_eq!(
        routed_clients(&restarted, "probe/second"),
        std::slice::from_ref(&client)
    );
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

    ctrl.snapshot_request_stalled(partition);
    ctrl.drive_snapshots(crate::cluster::node_controller::snapshot::SNAPSHOT_STALL_TIMEOUT_MS + 1)
        .await;
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
    Box<NodeController<MockTransport>>,
    Box<NodeController<MockTransport>>,
    Box<NodeController<MockTransport>>,
    PartitionId,
) {
    let partition = session_partition("c1");
    let mut holder = Box::new(controller(2));
    holder.become_replica(partition, Epoch::new(1), 0);
    holder.stores().create_session_replicated("c1").unwrap();
    let mut wiped = Box::new(controller(1));
    let mut joiner = Box::new(controller(3));
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
    exchange_catchup(&mut wiped, &mut joiner).await;

    assert_eq!(joiner.role(partition), ReplicaRole::Primary);
    assert!(joiner.stores().sessions.get("c1").is_some());
}

#[tokio::test]
async fn a_stalled_request_of_a_waiting_primary_swapped_to_replica_keeps_its_source() {
    let (_, mut wiped, _, partition) = swap_a_waiting_primary_after_a_replica_move().await;
    wiped.snapshot_request_stalled(partition);
    wiped
        .drive_snapshots(crate::cluster::node_controller::snapshot::SNAPSHOT_STALL_TIMEOUT_MS + 1)
        .await;

    wiped
        .apply_raft_partition_map(map_with(partition, node(3), &[node(1)], 3))
        .await;

    assert_eq!(snapshot_requests_to(&wiped, node(2)), 2);
    assert_eq!(snapshot_requests_to(&wiped, node(3)), 0);
}

async fn a_waiting_primary_removed_while_a_request_is_deferred() -> (
    Box<NodeController<MockTransport>>,
    Box<NodeController<MockTransport>>,
    Box<NodeController<MockTransport>>,
    PartitionId,
) {
    let partition = session_partition("c1");
    let mut holder = Box::new(controller(2));
    holder.become_replica(partition, Epoch::new(1), 0);
    holder.stores().create_session_replicated("c1").unwrap();
    let mut wiped = Box::new(controller(1));
    let mut joiner = Box::new(controller(3));
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

    assert_eq!(chunks_to(&wiped, node(3)), 0);
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

async fn retry_after_stall(ctrl: &mut NodeController<MockTransport>, partition: PartitionId) {
    ctrl.snapshot_request_stalled(partition);
    ctrl.drive_snapshots(crate::cluster::node_controller::snapshot::SNAPSHOT_STALL_TIMEOUT_MS + 1)
        .await;
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
    waiting.heartbeat.receive_heartbeat(
        node(2),
        &crate::cluster::protocol::Heartbeat::create(node(2), 0),
        0,
    );
    waiting.apply_dead_nodes(&[node(2)]);

    waiting
        .apply_raft_partition_map(map_with(partition, node(1), &[], 2))
        .await;

    assert_eq!(waiting.role(partition), ReplicaRole::Primary);
    assert_eq!(chunks_to(&waiting, node(3)), 1);
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
    retry_after_stall(&mut ctrl, partition).await;

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
    assert!(ctrl.snapshot_in_progress(partition));
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
        assert!(!ctrl.snapshot_in_progress(partition));
        assert_eq!(ctrl.snapshot_asked_from(partition), None);
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

#[tokio::test]
async fn a_stalled_partial_transfer_is_requested_again() {
    let partition = session_partition("c1");
    let map = map_with(partition, node(2), &[node(3)], 1);
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map.clone()).await;
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
    assert!(ctrl.snapshot_in_progress(partition));
    retry_after_stall(&mut ctrl, partition).await;

    ctrl.apply_raft_partition_map(map).await;

    assert_eq!(snapshot_requests_to(&ctrl, node(3)), 2);
}

#[tokio::test]
async fn a_failed_import_is_requested_again() {
    let partition = session_partition("c1");
    let map = map_with(partition, node(2), &[node(3)], 1);
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map.clone()).await;
    ctrl.transport.inject_message(
        node(3),
        ClusterMessage::SnapshotChunk(crate::cluster::snapshot::SnapshotChunk::new(
            partition,
            0,
            1,
            0,
            vec![1, 2, 0xff, 0xfe, 0, 0, 0, 0],
        )),
    );
    ctrl.process_messages().await;
    assert_eq!(ctrl.role(partition), ReplicaRole::AwaitingSnapshot);
    retry_after_stall(&mut ctrl, partition).await;

    ctrl.apply_raft_partition_map(map).await;

    assert_eq!(snapshot_requests_to(&ctrl, node(3)), 2);
}

#[tokio::test]
async fn a_primary_built_from_a_constraint_only_snapshot_trusts_its_disk_after_a_restart() {
    let partition = session_partition("c1");
    let mut replica = controller(3);
    replica.become_replica(partition, Epoch::new(1), 0);
    replica
        .stores()
        .db_constraints
        .add(
            crate::cluster::db::ClusterConstraint::unique("bigdocs", "uniq_bigdocs", "email")
                .unwrap(),
        )
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

fn map_after_updates(partition: PartitionId, updates: &[(u16, u16, u32)]) -> PartitionMap {
    let mut map = PartitionMap::default();
    for &(primary, replica1, epoch) in updates {
        assert!(map.apply_update(&PartitionUpdate {
            partition: u8::try_from(partition.get()).unwrap(),
            primary,
            replica1,
            replica2: 0,
            epoch,
        }));
    }
    map
}

async fn primary_with_a_stored_record(
    backend: &Arc<crate::cluster::test_support::FaultyBackend>,
    partition: PartitionId,
    updates: &[(u16, u16, u32)],
) {
    let mut before = controller_on(2, backend);
    before
        .apply_raft_partition_map(map_after_updates(partition, updates))
        .await;
    assert_eq!(before.role(partition), ReplicaRole::Primary);
    let (_, write) = before
        .stores()
        .db_create_write("bigdocs", "r1", b"{\"v\":1}", 1)
        .unwrap();
    before.stores().apply_write(&write).unwrap();
}

#[tokio::test]
async fn a_primary_restarted_after_only_its_replica_changed_trusts_its_disk() {
    let partition = crate::cluster::db::data_partition("bigdocs", "r1");
    let backend = crate::cluster::test_support::FaultyBackend::shared();
    primary_with_a_stored_record(&backend, partition, &[(2, 0, 1)]).await;

    let mut restarted = controller_on(2, &backend);
    restarted.stores().recover().unwrap();
    restarted
        .apply_raft_partition_map(map_after_updates(partition, &[(2, 0, 1), (2, 3, 2)]))
        .await;

    assert_eq!(restarted.role(partition), ReplicaRole::Primary);
    assert_eq!(snapshot_requests_to(&restarted, node(3)), 0);
}

#[tokio::test]
async fn a_primary_restarted_after_another_node_held_the_role_takes_a_snapshot() {
    let partition = crate::cluster::db::data_partition("bigdocs", "r1");
    let backend = crate::cluster::test_support::FaultyBackend::shared();
    primary_with_a_stored_record(&backend, partition, &[(2, 0, 1)]).await;

    let mut restarted = controller_on(2, &backend);
    restarted.stores().recover().unwrap();
    restarted
        .apply_raft_partition_map(map_after_updates(
            partition,
            &[(2, 0, 1), (3, 0, 2), (3, 2, 3), (2, 3, 4)],
        ))
        .await;

    assert_eq!(restarted.role(partition), ReplicaRole::AwaitingSnapshot);
    assert_eq!(snapshot_requests_to(&restarted, node(3)), 1);
}

#[tokio::test]
async fn a_marker_from_a_newer_assignment_is_not_trusted_for_an_older_map() {
    let partition = crate::cluster::db::data_partition("bigdocs", "r1");
    let backend = crate::cluster::test_support::FaultyBackend::shared();
    primary_with_a_stored_record(&backend, partition, &[(2, 0, 1), (2, 0, 2)]).await;

    let mut restarted = controller_on(2, &backend);
    restarted.stores().recover().unwrap();
    restarted
        .apply_raft_partition_map(map_after_updates(partition, &[(2, 3, 1)]))
        .await;

    assert_eq!(restarted.role(partition), ReplicaRole::AwaitingSnapshot);
}

#[tokio::test]
async fn a_partition_still_waiting_for_its_data_is_not_counted_as_ready() {
    let partition = session_partition("c1");
    let mut replica = controller(3);
    replica.become_replica(partition, Epoch::new(1), 0);
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;
    assert_eq!(ctrl.partitions_ready(), 0);

    serve_snapshot(&mut replica, &mut ctrl, partition).await;

    assert_eq!(ctrl.role(partition), ReplicaRole::Primary);
    assert_eq!(ctrl.partitions_ready(), 1);
}

#[tokio::test]
async fn a_replica_still_waiting_for_its_data_does_not_block_readiness() {
    let partition = session_partition("c1");
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(1), &[node(2)], 1))
        .await;

    assert_eq!(ctrl.role(partition), ReplicaRole::AwaitingSnapshot);
    assert_eq!(ctrl.partitions_ready(), 1);
}

async fn retry_after_the_silent_source_grace(
    ctrl: &mut NodeController<MockTransport>,
    partition: PartitionId,
) {
    ctrl.snapshot_request_stalled(partition);
    ctrl.snapshot_waited_since(partition, 1);
    ctrl.drive_snapshots(90_002).await;
}

#[tokio::test]
async fn a_primary_waiting_on_a_node_never_heard_from_takes_the_role_after_the_grace() {
    let partition = session_partition("c1");
    let map = map_with(partition, node(2), &[node(3)], 1);
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map.clone()).await;
    assert_eq!(ctrl.role(partition), ReplicaRole::AwaitingSnapshot);
    ctrl.apply_raft_partition_map(map.clone()).await;
    assert_eq!(ctrl.role(partition), ReplicaRole::AwaitingSnapshot);

    retry_after_the_silent_source_grace(&mut ctrl, partition).await;

    assert!(ctrl.apply_raft_partition_map(map).await);
    assert_eq!(ctrl.role(partition), ReplicaRole::Primary);
}

#[tokio::test]
async fn a_primary_waiting_on_a_node_it_has_heard_from_keeps_waiting_past_the_grace() {
    let partition = session_partition("c1");
    let map = map_with(partition, node(2), &[node(3)], 1);
    let mut ctrl = controller(2);
    ctrl.heartbeat.receive_heartbeat(
        node(3),
        &crate::cluster::protocol::Heartbeat::create(node(3), 0),
        0,
    );
    ctrl.apply_raft_partition_map(map.clone()).await;

    retry_after_the_silent_source_grace(&mut ctrl, partition).await;
    ctrl.apply_raft_partition_map(map).await;

    assert_eq!(ctrl.role(partition), ReplicaRole::AwaitingSnapshot);
    assert_eq!(snapshot_requests_to(&ctrl, node(3)), 2);
}

#[tokio::test]
async fn a_replica_waiting_on_a_node_never_heard_from_stays_waiting() {
    let partition = session_partition("c1");
    let map = map_with(partition, node(1), &[node(2)], 1);
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map.clone()).await;

    retry_after_the_silent_source_grace(&mut ctrl, partition).await;
    ctrl.apply_raft_partition_map(map).await;

    assert_eq!(ctrl.role(partition), ReplicaRole::AwaitingSnapshot);
}

#[tokio::test]
async fn the_grace_survives_a_retry_of_the_request() {
    let partition = session_partition("c1");
    let map = map_with(partition, node(2), &[node(3)], 1);
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map.clone()).await;
    ctrl.drive_snapshots(1).await;
    let first_asked = ctrl.snapshot_first_asked(partition);
    assert_eq!(first_asked, Some(1));

    retry_after_stall(&mut ctrl, partition).await;
    ctrl.apply_raft_partition_map(map).await;

    assert_eq!(snapshot_requests_to(&ctrl, node(3)), 2);
    assert_eq!(ctrl.snapshot_first_asked(partition), first_asked);
}

#[tokio::test]
async fn a_deferred_request_from_a_node_that_died_is_forgotten() {
    let (mut waiting, _) = waiting_with_a_deferred_request().await;
    assert_eq!(waiting.deferred_snapshot_requests.len(), 1);
    waiting.heartbeat.receive_heartbeat(
        node(3),
        &crate::cluster::protocol::Heartbeat::create(node(3), 0),
        0,
    );

    waiting.apply_dead_nodes(&[node(3)]);

    assert!(waiting.deferred_snapshot_requests.is_empty());
}

#[tokio::test]
async fn a_failed_import_keeps_the_live_source_of_a_waiting_primary() {
    let partition = session_partition("c1");
    let mut ctrl = controller(2);
    ctrl.heartbeat.receive_heartbeat(
        node(3),
        &crate::cluster::protocol::Heartbeat::create(node(3), 0),
        0,
    );
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(4)], 2))
        .await;
    assert_eq!(ctrl.snapshot_asked_from(partition), Some(node(3)));
    ctrl.transport.inject_message(
        node(3),
        ClusterMessage::SnapshotChunk(crate::cluster::snapshot::SnapshotChunk::new(
            partition,
            0,
            1,
            0,
            vec![1, 2, 0xff, 0xfe, 0, 0, 0, 0],
        )),
    );
    ctrl.process_messages().await;
    assert_eq!(ctrl.role(partition), ReplicaRole::AwaitingSnapshot);

    retry_after_stall(&mut ctrl, partition).await;

    assert_eq!(snapshot_requests_to(&ctrl, node(3)), 2);
    assert_eq!(snapshot_requests_to(&ctrl, node(4)), 0);
}

#[tokio::test]
async fn the_grace_survives_a_new_epoch_with_the_same_source() {
    let partition = session_partition("c1");
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;
    ctrl.drive_snapshots(1).await;
    assert_eq!(ctrl.snapshot_first_asked(partition), Some(1));

    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 2))
        .await;

    assert_eq!(ctrl.snapshot_first_asked(partition), Some(1));
}

#[tokio::test]
async fn a_swapped_primary_keeps_its_source_across_repeated_stalls() {
    let (_, mut wiped, _, partition) = swap_a_waiting_primary_after_a_replica_move().await;

    retry_after_stall(&mut wiped, partition).await;
    retry_after_stall(&mut wiped, partition).await;

    assert_eq!(snapshot_requests_to(&wiped, node(2)), 3);
    assert_eq!(snapshot_requests_to(&wiped, node(3)), 0);
}

#[tokio::test]
async fn a_role_taken_while_driving_serves_deferred_requests_on_the_next_apply() {
    let (mut waiting, partition) = waiting_with_a_deferred_request().await;
    retry_after_the_silent_source_grace(&mut waiting, partition).await;
    assert_eq!(waiting.role(partition), ReplicaRole::Primary);
    assert_eq!(chunks_to(&waiting, node(3)), 0);

    waiting
        .apply_raft_partition_map(map_with(partition, node(1), &[node(2)], 1))
        .await;

    assert_eq!(chunks_to(&waiting, node(3)), 1);
}

#[tokio::test]
async fn a_local_write_with_an_outbox_is_refused_not_held_while_waiting() {
    let partition = crate::cluster::db::data_partition("bigdocs", "r1");
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;
    let (_, write) = ctrl
        .stores()
        .db_create_write("bigdocs", "r1", b"{\"v\":1}", 1)
        .unwrap();

    let outbox = [(b"k".to_vec(), b"v".to_vec())];
    let result = ctrl.write_or_forward_with_event(write, None, &outbox).await;

    assert!(result.is_err());
    assert!(ctrl.held_writes.is_empty());
}

#[tokio::test]
async fn a_held_local_write_keeps_its_change_event() {
    let partition = crate::cluster::db::data_partition("bigdocs", "r1");
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;
    let (_, write) = ctrl
        .stores()
        .db_create_write("bigdocs", "r1", b"{\"v\":1}", 1)
        .unwrap();
    let record = crate::cluster::protocol::ChangeEventRecord {
        topic: "t".to_string(),
        payload: Vec::new(),
        user_properties: Vec::new(),
        order_key: "r1".to_string(),
    };

    ctrl.write_or_forward_with_event(write, Some(record), &[])
        .await
        .unwrap();

    assert!(ctrl.held_writes[0].write.event.is_some());
}

#[tokio::test]
async fn a_deferred_requester_gets_the_writes_held_during_the_wait() {
    let partition = crate::cluster::db::data_partition("bigdocs", "r1");
    let mut holder = controller(3);
    holder.become_replica(partition, Epoch::new(1), 0);
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;
    let (_, write) = ctrl
        .stores()
        .db_create_write("bigdocs", "r1", b"{\"v\":1}", 1)
        .unwrap();
    ctrl.write_or_forward(write).await;
    ctrl.transport.inject_message(
        node(4),
        ClusterMessage::SnapshotRequest(SnapshotRequest::create(partition, node(4))),
    );
    ctrl.process_messages().await;

    serve_snapshot(&mut holder, &mut ctrl, partition).await;
    assert_eq!(ctrl.role(partition), ReplicaRole::Primary);

    let mut requester = controller(4);
    requester
        .apply_raft_partition_map(map_with(partition, node(2), &[node(4)], 2))
        .await;
    for (to, message) in ctrl.transport.sent_messages() {
        if to == node(4) && matches!(message, ClusterMessage::SnapshotChunk(_)) {
            requester.transport.inject_message(node(2), message);
        }
    }
    requester.process_messages().await;

    assert!(requester.stores().db_get("bigdocs", "r1").is_some());
}

#[tokio::test]
async fn a_primary_built_from_a_snapshot_with_only_change_events_trusts_its_disk_after_a_restart() {
    let partition = session_partition("c1");
    let source_backend = crate::cluster::test_support::FaultyBackend::shared();
    let mut replica = controller_on(3, &source_backend);
    replica.become_replica(partition, Epoch::new(1), 0);
    replica
        .stores()
        .cluster_outbox()
        .unwrap()
        .store_change_events(&[crate::cluster::store_manager::outbox::StoredChangeEvent {
            partition,
            id: crate::cluster::protocol::EventId::new(1, 1),
            record: crate::cluster::protocol::ChangeEventRecord {
                topic: "t".to_string(),
                payload: Vec::new(),
                user_properties: Vec::new(),
                order_key: "c1".to_string(),
            },
        }])
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

fn record_in(partition: PartitionId, prefix: &str) -> String {
    (0..)
        .map(|n| format!("{prefix}-{n}"))
        .find(|id| crate::cluster::data_partition("bigdocs", id) == partition)
        .unwrap()
}

#[tokio::test]
async fn a_write_acked_by_a_primary_built_from_a_snapshot_survives_its_restart() {
    let partition = crate::cluster::data_partition("bigdocs", "seed");
    let acked = record_in(partition, "acked");
    let mut replica = controller(3);
    replica.become_replica(partition, Epoch::new(1), 0);
    let (_, seed) = replica
        .stores()
        .db_create_write("bigdocs", "seed", b"{\"v\":1}", 1)
        .unwrap();
    replica.stores().apply_write(&seed).unwrap();
    let backend = crate::cluster::test_support::FaultyBackend::shared();
    let map = map_with(partition, node(2), &[node(3)], 1);

    let mut primary = controller_on(2, &backend);
    primary.apply_raft_partition_map(map.clone()).await;
    serve_snapshot(&mut replica, &mut primary, partition).await;
    assert_eq!(primary.role(partition), ReplicaRole::Primary);
    primary
        .db_create("bigdocs", &acked, b"{\"v\":2}", 2)
        .await
        .unwrap();
    primary.transport.take_sent();
    replica.transport.take_sent();
    assert!(replica.stores().db_get("bigdocs", &acked).is_none());
    drop(primary);

    let mut restarted = controller_on(2, &backend);
    restarted.stores().recover().unwrap();
    assert!(restarted.stores().db_get("bigdocs", &acked).is_some());
    restarted.apply_raft_partition_map(map).await;
    if restarted.role(partition) == ReplicaRole::AwaitingSnapshot {
        serve_snapshot(&mut replica, &mut restarted, partition).await;
    }

    assert_eq!(restarted.role(partition), ReplicaRole::Primary);
    assert!(
        restarted.stores().db_get("bigdocs", &acked).is_some(),
        "the write acked as primary was lost"
    );
}

fn location_write(client_id: &str, on: NodeId, operation: Operation) -> ClusterMessage {
    let entry = crate::cluster::ClientLocationEntry::create(client_id, on);
    ClusterMessage::WriteRequest(ReplicationWrite::new(
        session_partition(client_id),
        operation,
        Epoch::new(0),
        0,
        crate::cluster::entity::CLIENT_LOCATIONS.to_string(),
        crate::cluster::client_location_key(client_id),
        bebytes::BeBytes::to_be_bytes(&entry),
    ))
}

#[tokio::test]
async fn a_late_reply_for_a_client_that_moved_to_a_third_node_does_not_lose_the_new_node() {
    let client = client_outside(&[], "moving-client");
    let mut owner = controller(1);
    connect_subscriber(&owner, &client, &["probe/old"]);
    let mut restarted = controller(2);
    restarted.stores().client_locations.claim(&client, node(3));
    restarted
        .stores()
        .topics
        .subscribe("probe/new", &client, session_partition(&client), 1)
        .unwrap();

    hear_from_peer_after_restart(&mut restarted, &mut owner).await;

    assert!(restarted.connected_nodes(&client).contains(&node(3)));
    assert_eq!(
        routed_clients(&restarted, "probe/new"),
        std::slice::from_ref(&client)
    );

    restarted.transport.inject_message(
        owner.node_id(),
        location_write(&client, node(1), Operation::Delete),
    );
    restarted.process_messages().await;

    assert_eq!(restarted.connected_nodes(&client), [node(3)]);
}

#[tokio::test]
async fn a_peer_restarted_quickly_drops_the_clients_it_no_longer_has() {
    let mut peer = controller(1);
    let mut restarted = controller(2);
    restarted
        .stores()
        .client_locations
        .claim("gone-client", peer.node_id());
    connect_subscriber(&peer, "live-client", &["probe/live"]);

    hear_from_peer_after_restart(&mut restarted, &mut peer).await;

    assert_eq!(restarted.connected_nodes("gone-client"), []);
    assert_eq!(restarted.connected_nodes("live-client"), [node(1)]);
}

#[tokio::test]
async fn a_dead_node_no_longer_holds_its_clients() {
    let mut ctrl = controller(2);
    ctrl.stores().client_locations.claim("client-a", node(1));
    ctrl.stores().client_locations.claim("client-b", node(1));
    ctrl.stores().client_locations.claim("client-b", node(3));

    ctrl.transport
        .inject_message(node(3), ClusterMessage::DeathNotice { node_id: node(1) });
    ctrl.process_messages().await;

    assert_eq!(ctrl.connected_nodes("client-a"), []);
    assert_eq!(ctrl.connected_nodes("client-b"), [node(3)]);
}

#[tokio::test]
async fn a_publish_is_forwarded_to_every_node_holding_the_client() {
    let ctrl = controller(2);
    ctrl.stores().client_locations.claim("twice", node(3));
    ctrl.stores()
        .client_locations
        .claim_listed("twice", node(1));

    let mut forwarded: Vec<NodeId> = ctrl
        .remote_targets([crate::cluster::ForwardTarget::new("twice".to_string(), 1)])
        .into_keys()
        .collect();
    forwarded.sort_by_key(|node| node.get());

    assert_eq!(forwarded, [node(1), node(3)]);
}

#[tokio::test]
async fn a_client_connecting_to_the_peer_after_the_reply_is_located_on_the_restarted_node() {
    let mut owner = controller(1);
    let mut restarted = controller(2);
    *owner.transport.linked.lock().unwrap() = Some(vec![restarted.node_id()]);
    restarted.transport.inject_message(
        owner.node_id(),
        ClusterMessage::Heartbeat(Heartbeat::create(owner.node_id(), 0)),
    );
    restarted.process_messages().await;
    deliver_over_the_wire(&mut restarted, &mut owner).await;
    deliver_over_the_wire(&mut owner, &mut restarted).await;

    let client = client_outside(&[], "late-client");
    let entry = crate::cluster::ClientLocationEntry::create(&client, owner.node_id());
    owner
        .write_or_forward(ReplicationWrite::new(
            session_partition(&client),
            Operation::Insert,
            Epoch::new(0),
            0,
            crate::cluster::entity::CLIENT_LOCATIONS.to_string(),
            crate::cluster::client_location_key(&client),
            bebytes::BeBytes::to_be_bytes(&entry),
        ))
        .await;
    deliver_over_the_wire(&mut owner, &mut restarted).await;

    assert_eq!(restarted.connected_node(&client), Some(node(1)));
}

#[tokio::test]
async fn a_recovered_subscriber_that_no_peer_lists_is_kept() {
    let client = client_outside(&[], "gone-client");
    let mut owner = controller(1);
    let backend = crate::cluster::test_support::FaultyBackend::shared();
    let mut stale = crate::cluster::MqttSubscriptionSnapshot::create(&client);
    stale.add_subscription("probe/stale", 1);
    controller_on(2, &backend)
        .stores()
        .apply_write(&ReplicationWrite::new(
            session_partition(&client),
            Operation::Insert,
            Epoch::new(1),
            1,
            crate::cluster::entity::SUBSCRIPTIONS.to_string(),
            client.clone(),
            crate::cluster::SubscriptionCache::serialize(&stale),
        ))
        .unwrap();
    let mut restarted = controller_on(2, &backend);
    restarted.stores().recover().unwrap();

    hear_from_peer_after_restart(&mut restarted, &mut owner).await;

    assert_eq!(
        routed_clients(&restarted, "probe/stale"),
        std::slice::from_ref(&client)
    );
}

fn request_round(ctrl: &NodeController<MockTransport>, to: NodeId) -> u64 {
    ctrl.transport
        .sent_messages()
        .iter()
        .rev()
        .find_map(|(dest, message)| match message {
            ClusterMessage::SubscriptionResyncRequest(request) if *dest == to => {
                Some(request.round)
            }
            _ => None,
        })
        .unwrap()
}

const LATER_MS: u64 = 1_800_000_000_000;

#[tokio::test]
async fn a_lost_resync_reply_is_asked_for_again() {
    let client = client_outside(&[], "remote-client");
    let mut owner = controller(1);
    connect_subscriber(&owner, &client, &["probe/exact"]);
    let mut restarted = controller(2);
    restarted.transport.inject_message(
        owner.node_id(),
        ClusterMessage::Heartbeat(Heartbeat::create(owner.node_id(), 0)),
    );
    restarted.process_messages().await;
    deliver_over_the_wire(&mut restarted, &mut owner).await;
    owner.transport.take_sent();

    restarted.drive_subscription_resyncs(LATER_MS).await;
    assert_eq!(resync_requests_sent(&restarted, node(1)), 0);
    restarted
        .drive_subscription_resyncs(
            LATER_MS + crate::cluster::node_controller::subscription_resync::RESYNC_RETRY_MS,
        )
        .await;
    assert_eq!(resync_requests_sent(&restarted, node(1)), 1);

    let retried = request_round(&restarted, node(1));
    restarted.transport.take_sent();
    owner.transport.inject_message(
        restarted.node_id(),
        ClusterMessage::SubscriptionResyncRequest(crate::cluster::protocol::ResyncRequest {
            round: retried,
            ask_back: true,
            digest: crate::cluster::protocol::ListingDigest::default(),
        }),
    );
    owner.process_messages().await;
    deliver_over_the_wire(&mut owner, &mut restarted).await;

    assert_eq!(
        routed_clients(&restarted, "probe/exact"),
        std::slice::from_ref(&client)
    );
    assert_eq!(restarted.connected_node(&client), Some(node(1)));
    restarted
        .drive_subscription_resyncs(
            LATER_MS + 3 * crate::cluster::node_controller::subscription_resync::RESYNC_RETRY_MS,
        )
        .await;
    assert_eq!(resync_requests_sent(&restarted, node(1)), 0);
}

#[tokio::test]
async fn a_completed_resync_is_not_asked_for_again() {
    let mut owner = controller(1);
    let mut restarted = controller(2);
    hear_from_peer_after_restart(&mut restarted, &mut owner).await;
    restarted.transport.take_sent();

    restarted.drive_subscription_resyncs(LATER_MS).await;
    restarted
        .drive_subscription_resyncs(
            LATER_MS + crate::cluster::node_controller::subscription_resync::RESYNC_RETRY_MS,
        )
        .await;

    assert_eq!(resync_requests_sent(&restarted, node(1)), 0);
}

#[tokio::test]
async fn a_missing_resync_part_is_asked_for_again() {
    let mut restarted = controller(2);
    restarted.transport.inject_message(
        node(1),
        ClusterMessage::Heartbeat(Heartbeat::create(node(1), 0)),
    );
    restarted.process_messages().await;
    let round = request_round(&restarted, node(1));
    restarted.transport.take_sent();
    restarted.transport.inject_message(
        node(1),
        ClusterMessage::SubscriptionResync(crate::cluster::protocol::SubscriptionResync {
            round,
            part: 1,
            parts: 2,
            unchanged: false,
            continued: false,
            clients: Vec::new(),
        }),
    );
    restarted.process_messages().await;

    restarted.drive_subscription_resyncs(LATER_MS).await;

    assert_eq!(resync_requests_sent(&restarted, node(1)), 1);
}

#[tokio::test]
async fn a_dead_peer_is_not_asked_again() {
    let mut restarted = controller(2);
    restarted.transport.inject_message(
        node(1),
        ClusterMessage::Heartbeat(Heartbeat::create(node(1), 0)),
    );
    restarted.process_messages().await;
    restarted.transport.take_sent();
    restarted.apply_dead_nodes(&[node(1)]);

    restarted.drive_subscription_resyncs(LATER_MS).await;
    restarted
        .drive_subscription_resyncs(
            LATER_MS + crate::cluster::node_controller::subscription_resync::RESYNC_RETRY_MS,
        )
        .await;

    assert_eq!(resync_requests_sent(&restarted, node(1)), 0);
}

#[tokio::test]
async fn a_peer_that_never_saw_the_restart_learns_the_restarted_nodes_clients() {
    let client = client_outside(&[], "early-client");
    let mut peer = controller(1);
    let mut restarted = controller(2);
    connect_subscriber(&restarted, &client, &["probe/early"]);

    hear_from_peer_after_restart(&mut restarted, &mut peer).await;
    deliver_over_the_wire(&mut restarted, &mut peer).await;

    assert_eq!(peer.connected_node(&client), Some(node(2)));
    assert_eq!(
        routed_clients(&peer, "probe/early"),
        std::slice::from_ref(&client)
    );
}

#[tokio::test]
async fn a_resumed_client_announcement_adds_to_a_restarted_nodes_view() {
    let client = client_outside(&[], "resumed-client");
    let mut owner = controller(1);
    connect_subscriber(&owner, &client, &["probe/kept", "probe/+/wild"]);
    let mut restarted = controller(2);
    restarted
        .stores()
        .topics
        .subscribe("probe/stale", &client, session_partition(&client), 1)
        .unwrap();

    owner.announce_resumed_client(&client).await;
    let announced: Vec<_> = owner
        .transport
        .take_sent()
        .into_iter()
        .map(|(_, message)| (restarted.node_id(), message))
        .collect();
    owner.transport.outbox.lock().unwrap().extend(announced);
    deliver_over_the_wire(&mut owner, &mut restarted).await;

    assert_eq!(restarted.connected_node(&client), Some(node(1)));
    assert_eq!(
        routed_clients(&restarted, "probe/kept"),
        std::slice::from_ref(&client)
    );
    assert_eq!(
        routed_clients(&restarted, "probe/x/wild"),
        std::slice::from_ref(&client)
    );
    assert_eq!(
        routed_clients(&restarted, "probe/stale"),
        std::slice::from_ref(&client)
    );
}

#[tokio::test]
async fn a_silent_peer_is_asked_a_bounded_number_of_times() {
    let mut restarted = controller(2);
    restarted.transport.inject_message(
        node(1),
        ClusterMessage::Heartbeat(Heartbeat::create(node(1), 0)),
    );
    restarted.process_messages().await;
    let mut total = resync_requests_sent(&restarted, node(1));
    restarted.transport.take_sent();
    let step = crate::cluster::node_controller::subscription_resync::RESYNC_RETRY_MS;
    for tick in 0..=720u64 {
        restarted
            .drive_subscription_resyncs(LATER_MS + tick * step)
            .await;
        total += resync_requests_sent(&restarted, node(1));
        restarted.transport.take_sent();
    }

    assert_eq!(
        total,
        usize::try_from(crate::cluster::node_controller::subscription_resync::MAX_RESYNC_ATTEMPTS)
            .unwrap()
    );
}

#[tokio::test]
async fn a_reply_that_arrives_after_the_retry_still_completes_the_round() {
    let client = client_outside(&[], "remote-client");
    let mut owner = controller(1);
    connect_subscriber(&owner, &client, &["probe/exact"]);
    let mut restarted = controller(2);
    restarted.transport.inject_message(
        owner.node_id(),
        ClusterMessage::Heartbeat(Heartbeat::create(owner.node_id(), 0)),
    );
    restarted.process_messages().await;
    let step = crate::cluster::node_controller::subscription_resync::RESYNC_RETRY_MS;
    let mut now = LATER_MS;
    restarted.drive_subscription_resyncs(now).await;
    deliver_requests_only(&mut restarted, &mut owner).await;
    now += step;
    restarted.drive_subscription_resyncs(now).await;
    deliver_replies_only(&mut owner, &mut restarted).await;
    restarted.transport.take_sent();

    for _ in 0..8 {
        now += step;
        restarted.drive_subscription_resyncs(now).await;
    }

    assert_eq!(resync_requests_sent(&restarted, node(1)), 0);
    assert_eq!(
        routed_clients(&restarted, "probe/exact"),
        std::slice::from_ref(&client)
    );
}

#[tokio::test]
async fn a_failed_request_is_retried_after_the_backoff_not_every_tick() {
    use std::sync::atomic::Ordering;
    let mut restarted = controller(2);
    restarted
        .transport
        .refuse_sends
        .store(true, Ordering::SeqCst);
    restarted.transport.inject_message(
        node(1),
        ClusterMessage::Heartbeat(Heartbeat::create(node(1), 0)),
    );
    restarted.process_messages().await;
    restarted.drive_subscription_resyncs(LATER_MS).await;
    restarted
        .transport
        .refuse_sends
        .store(false, Ordering::SeqCst);

    let step = crate::cluster::node_controller::subscription_resync::RESYNC_RETRY_MS;
    for elapsed in (10..step).step_by(10) {
        restarted
            .drive_subscription_resyncs(LATER_MS + elapsed)
            .await;
    }
    assert_eq!(resync_requests_sent(&restarted, node(1)), 0);

    restarted.drive_subscription_resyncs(LATER_MS + step).await;
    assert_eq!(resync_requests_sent(&restarted, node(1)), 1);
}

#[tokio::test]
async fn a_peer_revived_through_the_processing_batch_is_asked_for_its_subscribers() {
    let mut restarted = controller(2);
    let processor = spawn_message_processor(node(2), PartitionMap::default());
    processor
        .inbox
        .send(InboundMessage {
            from: node(1),
            message: ClusterMessage::Heartbeat(Heartbeat::create(node(1), 1_000)),
            received_at: 1_000,
        })
        .unwrap();
    let batch = next_batch_with(&processor, 1_000, |batch| {
        !batch.heartbeat_updates.is_empty()
    })
    .await;

    restarted.apply_heartbeat_updates(&batch.heartbeat_updates);
    restarted.request_subscription_resyncs().await;

    assert_eq!(resync_requests_sent(&restarted, node(1)), 1);
}

async fn deliver_requests_only(
    from: &mut NodeController<MockTransport>,
    to: &mut NodeController<MockTransport>,
) {
    for (dest, message) in from.transport.take_sent() {
        if dest == to.node_id() && matches!(message, ClusterMessage::SubscriptionResyncRequest(_)) {
            to.transport.inject_message(from.node_id(), message);
        }
    }
    to.process_messages().await;
}

async fn deliver_replies_only(
    from: &mut NodeController<MockTransport>,
    to: &mut NodeController<MockTransport>,
) {
    let replies: Vec<_> = from
        .transport
        .take_sent()
        .into_iter()
        .filter(|(dest, message)| {
            *dest == to.node_id() && matches!(message, ClusterMessage::SubscriptionResync(_))
        })
        .collect();
    for (_, message) in replies {
        to.transport.inject_message(from.node_id(), message);
    }
    to.process_messages().await;
}

#[tokio::test]
async fn a_peer_declared_dead_by_notice_is_not_asked() {
    use std::sync::atomic::Ordering;
    let mut restarted = controller(2);
    restarted
        .transport
        .refuse_sends
        .store(true, Ordering::SeqCst);
    restarted.transport.inject_message(
        node(1),
        ClusterMessage::Heartbeat(Heartbeat::create(node(1), 0)),
    );
    restarted.process_messages().await;
    restarted
        .transport
        .inject_message(node(3), ClusterMessage::DeathNotice { node_id: node(1) });
    restarted.process_messages().await;
    restarted
        .transport
        .refuse_sends
        .store(false, Ordering::SeqCst);

    let step = crate::cluster::node_controller::subscription_resync::RESYNC_RETRY_MS;
    for tick in 0..20u64 {
        restarted
            .drive_subscription_resyncs(LATER_MS + tick * step)
            .await;
    }

    assert_eq!(resync_requests_sent(&restarted, node(1)), 0);
}
