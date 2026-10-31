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
