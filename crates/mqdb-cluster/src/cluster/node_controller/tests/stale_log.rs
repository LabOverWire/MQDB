// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::cluster::data_partition;
use crate::cluster::protocol::{CatchupRequest, CatchupResponse, ReplicationWrite};
use crate::cluster::{PartitionAssignment, PartitionMap};

const ENTITY: &str = "snap";
const ID: &str = "big";

fn node(id: u16) -> NodeId {
    NodeId::validated(id).unwrap()
}

fn controller(id: u16) -> Box<NodeController<MockTransport>> {
    Box::new(create_test_controller(
        node(id),
        MockTransport::new(node(id)),
    ))
}

fn map_for(primary: NodeId, replicas: &[NodeId], epoch: u64) -> PartitionMap {
    let mut map = PartitionMap::default();
    map.set(
        data_partition(ENTITY, ID),
        PartitionAssignment::new(primary, replicas.to_vec(), Epoch::new(epoch)),
    );
    map
}

fn value(n: u64) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({"id": ID, "v": n})).unwrap()
}

fn counter(ctrl: &NodeController<MockTransport>) -> Option<u64> {
    let found = ctrl.db_get(ENTITY, ID)?;
    let json: serde_json::Value = serde_json::from_slice(&found.data).unwrap();
    json["v"].as_u64()
}

async fn deliver(from: &NodeController<MockTransport>, to: &mut NodeController<MockTransport>) {
    for (dest, message) in from.transport.take_sent() {
        if dest == to.node_id {
            to.transport.inject_message(from.node_id, message);
        }
    }
    to.process_messages().await;
}

fn heard_all(ctrl: &mut NodeController<MockTransport>) {
    for id in 1..=3 {
        ctrl.heartbeat.receive_heartbeat(
            node(id),
            &crate::cluster::protocol::Heartbeat::create(node(id), 0),
            0,
        );
    }
}

async fn apply_to_both(
    first: &mut NodeController<MockTransport>,
    second: &mut NodeController<MockTransport>,
    map: PartitionMap,
) {
    first.apply_raft_partition_map(map.clone()).await;
    second.apply_raft_partition_map(map).await;
}

async fn catchup_answer(
    ctrl: &mut NodeController<MockTransport>,
    partition: PartitionId,
    from_seq: u64,
) -> Vec<CatchupResponse> {
    ctrl.transport.take_sent();
    ctrl.transport.inject_message(
        node(9),
        ClusterMessage::CatchupRequest(CatchupRequest::create(
            partition,
            from_seq,
            u64::MAX,
            node(9),
        )),
    );
    ctrl.process_messages().await;
    ctrl.transport
        .take_sent()
        .into_iter()
        .filter_map(|(_, message)| match message {
            ClusterMessage::CatchupResponse(response) => Some(response),
            _ => None,
        })
        .collect()
}

fn epochs_of(responses: &[CatchupResponse]) -> Vec<(u64, u64)> {
    responses
        .iter()
        .flat_map(|response| response.writes.iter())
        .map(|write| (write.sequence, write.epoch.get()))
        .collect()
}

async fn source_reimports_and_a_taker_catches_up_after_a_later_write() -> u64 {
    let partition = data_partition(ENTITY, ID);
    let mut x = controller(1);
    let mut p = controller(2);
    let mut t = controller(3);
    heard_all(&mut x);
    heard_all(&mut p);
    heard_all(&mut t);

    x.become_primary(partition, Epoch::new(1));
    p.become_replica(partition, Epoch::new(1), 0);
    apply_to_both(&mut x, &mut p, map_for(node(1), &[node(2)], 1)).await;
    x.db_create(ENTITY, ID, &value(1), 100).await.unwrap();
    deliver(&x, &mut p).await;
    x.db_update(ENTITY, ID, &value(2), 200).await.unwrap();
    x.db_update(ENTITY, ID, &value(3), 300).await.unwrap();
    x.transport.take_sent();

    apply_to_both(&mut p, &mut x, map_for(node(2), &[node(1)], 2)).await;
    p.db_update(ENTITY, ID, &value(10), 400).await.unwrap();
    deliver(&p, &mut x).await;
    x.transport.take_sent();

    apply_to_both(&mut p, &mut x, map_for(node(2), &[node(4)], 3)).await;
    apply_to_both(&mut p, &mut x, map_for(node(2), &[node(1)], 4)).await;
    p.transport.take_sent();
    x.drive_snapshots(5).await;
    deliver(&x, &mut p).await;
    p.drive_snapshots(6).await;
    deliver(&p, &mut x).await;
    deliver(&x, &mut p).await;
    deliver(&p, &mut x).await;
    x.transport.take_sent();
    assert_eq!(counter(&x), Some(10));

    apply_to_both(&mut x, &mut p, map_for(node(1), &[node(2)], 5)).await;
    apply_to_both(&mut x, &mut t, map_for(node(1), &[node(3)], 6)).await;
    x.transport.take_sent();
    t.drive_snapshots(10).await;
    deliver(&t, &mut x).await;
    x.drive_snapshots(11).await;
    x.db_update(ENTITY, ID, &value(11), 500).await.unwrap();

    apply_to_both(&mut t, &mut x, map_for(node(3), &[node(1)], 7)).await;
    deliver(&x, &mut t).await;
    deliver(&t, &mut x).await;
    deliver(&x, &mut t).await;
    assert_eq!(t.role(partition), ReplicaRole::Primary);
    counter(&t).unwrap()
}

#[tokio::test]
async fn the_write_acked_after_the_export_replaces_an_older_log_entry_at_its_sequence() {
    let held = Box::pin(source_reimports_and_a_taker_catches_up_after_a_later_write()).await;
    assert_eq!(held, 11);
}

fn write_at(
    ctrl: &NodeController<MockTransport>,
    sequence: u64,
    epoch: u64,
    n: u64,
) -> ReplicationWrite {
    let (_, mut write) = ctrl
        .stores
        .db_update_write(ENTITY, ID, &value(n), 300)
        .unwrap();
    write.sequence = sequence;
    write.epoch = Epoch::new(epoch);
    write
}

#[tokio::test]
async fn a_sequence_reset_by_a_newer_epoch_does_not_leave_older_entries_to_be_sent() {
    let partition = data_partition(ENTITY, ID);
    let mut replica = controller(2);
    replica.become_replica(partition, Epoch::new(1), 0);
    replica
        .apply_raft_partition_map(map_for(node(1), &[node(2)], 1))
        .await;
    let (_, create) = replica
        .stores()
        .db_create_write(ENTITY, ID, &value(1), 100)
        .unwrap();
    let first = ReplicationWrite {
        sequence: 1,
        epoch: Epoch::new(1),
        ..create
    };
    replica
        .transport
        .inject_message(node(1), ClusterMessage::Write(first));
    replica.process_messages().await;
    for sequence in 2..=3 {
        let write = write_at(&replica, sequence, 1, sequence);
        replica
            .transport
            .inject_message(node(1), ClusterMessage::Write(write));
        replica.process_messages().await;
    }
    assert_eq!(replica.sequence(partition), Some(3));

    for sequence in 1..=2 {
        let write = write_at(&replica, sequence, 2, 10 + sequence);
        replica
            .transport
            .inject_message(node(3), ClusterMessage::Write(write));
        replica.process_messages().await;
    }
    assert_eq!(replica.sequence(partition), Some(2));

    let answer = catchup_answer(&mut replica, partition, 2).await;
    assert_eq!(epochs_of(&answer), vec![(2, 2)]);
}

#[tokio::test]
async fn a_catchup_for_a_sequence_only_reached_by_an_import_gets_no_older_entry() {
    let partition = data_partition(ENTITY, ID);
    let mut x = controller(1);
    let mut p = controller(2);
    heard_all(&mut x);
    heard_all(&mut p);
    x.become_primary(partition, Epoch::new(1));
    x.apply_raft_partition_map(map_for(node(1), &[node(3)], 1))
        .await;
    x.db_create(ENTITY, ID, &value(1), 100).await.unwrap();
    x.db_update(ENTITY, ID, &value(2), 200).await.unwrap();
    x.db_update(ENTITY, ID, &value(3), 300).await.unwrap();
    x.transport.take_sent();

    p.become_primary(partition, Epoch::new(2));
    p.apply_raft_partition_map(map_for(node(2), &[node(3)], 2))
        .await;
    p.db_create(ENTITY, ID, &value(10), 400).await.unwrap();
    p.db_update(ENTITY, ID, &value(11), 500).await.unwrap();
    p.db_update(ENTITY, ID, &value(12), 600).await.unwrap();
    p.transport.take_sent();

    x.apply_raft_partition_map(map_for(node(2), &[node(3)], 2))
        .await;
    apply_to_both(&mut p, &mut x, map_for(node(2), &[node(1)], 3)).await;
    x.drive_snapshots(5).await;
    deliver(&x, &mut p).await;
    p.drive_snapshots(6).await;
    deliver(&p, &mut x).await;
    assert_eq!(counter(&x), Some(12));
    assert_eq!(x.sequence(partition), Some(3));

    for asked_from in [1, 3] {
        let answer = catchup_answer(&mut x, partition, asked_from).await;
        assert_eq!(epochs_of(&answer), Vec::new());
        assert_eq!(answer.last().unwrap().covers_through, Some(3));
    }
}

#[tokio::test]
async fn a_replica_behind_a_primary_that_imported_recovers_by_snapshot() {
    let partition = data_partition(ENTITY, ID);
    let mut holder = controller(2);
    heard_all(&mut holder);
    holder.become_primary(partition, Epoch::new(1));
    holder
        .apply_raft_partition_map(map_for(node(2), &[node(4)], 1))
        .await;
    holder.db_create(ENTITY, ID, &value(1), 100).await.unwrap();
    holder.db_update(ENTITY, ID, &value(2), 200).await.unwrap();
    holder.db_update(ENTITY, ID, &value(3), 300).await.unwrap();
    holder.transport.take_sent();

    let mut imported = controller(1);
    heard_all(&mut imported);
    apply_to_both(&mut holder, &mut imported, map_for(node(2), &[node(1)], 2)).await;
    imported.drive_snapshots(5).await;
    deliver(&imported, &mut holder).await;
    holder.drive_snapshots(6).await;
    deliver(&holder, &mut imported).await;
    assert_eq!(imported.role(partition), ReplicaRole::Replica);
    assert_eq!(imported.sequence(partition), Some(3));
    imported.transport.take_sent();

    let mut behind = controller(3);
    heard_all(&mut behind);
    behind.become_replica(partition, Epoch::new(1), 0);
    let moved = map_for(node(1), &[node(3)], 3);
    behind.apply_raft_partition_map(moved.clone()).await;
    imported.apply_raft_partition_map(moved).await;
    assert_eq!(imported.role(partition), ReplicaRole::Primary);
    imported
        .db_update(ENTITY, ID, &value(4), 400)
        .await
        .unwrap();

    for now in 7..15 {
        deliver(&imported, &mut behind).await;
        deliver(&behind, &mut imported).await;
        imported.drive_snapshots(now).await;
        behind.drive_snapshots(now).await;
    }

    assert_eq!(behind.role(partition), ReplicaRole::Replica);
    assert_eq!(counter(&behind), Some(4));
    assert_eq!(behind.sequence(partition), Some(4));
}
