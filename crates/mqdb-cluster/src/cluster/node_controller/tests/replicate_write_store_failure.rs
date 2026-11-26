// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::store_failure::controller_where;
use super::*;
use crate::cluster::session::session_partition;
use crate::cluster::test_support::FaultyBackend;

fn node(id: u16) -> NodeId {
    NodeId::validated(id).unwrap()
}

fn primary_with_replica(backend: &Arc<FaultyBackend>) -> NodeController<MockTransport> {
    controller_where(Some(backend), |_| node(1), &[node(2)])
}

fn session_writes_sent(ctrl: &NodeController<MockTransport>) -> Vec<String> {
    ctrl.transport
        .sent_messages()
        .into_iter()
        .filter_map(|(to, message)| match message {
            ClusterMessage::Write(write)
                if to == node(2) && write.entity == crate::cluster::entity::SESSIONS =>
            {
                Some(write.id)
            }
            _ => None,
        })
        .collect()
}

fn session_write(ctrl: &NodeController<MockTransport>, client_id: &str) -> ReplicationWrite {
    let (_, write) = ctrl.stores().create_session_replicated(client_id).unwrap();
    write
}

#[tokio::test]
async fn a_replicated_write_the_primary_cannot_store_is_refused_and_not_sent() {
    let backend = FaultyBackend::shared();
    let mut ctrl = primary_with_replica(&backend);
    let partition = session_partition("c1");
    let sequence_before = ctrl.sequence(partition);
    let write = session_write(&ctrl, "c1");
    backend.fail_writes(true);

    let result = ctrl.replicate_write(write, &[node(2)], 1).await;

    assert_eq!(result, Err(ReplicationError::StoreFailed));
    assert_eq!(session_writes_sent(&ctrl), Vec::<String>::new());
    assert_eq!(ctrl.write_log().entry_count(partition), 0);
    assert_eq!(ctrl.sequence(partition), sequence_before);
    assert_eq!(ctrl.pending_write_count(), 0);
}

#[tokio::test]
async fn a_replicated_write_the_primary_stores_is_logged_and_sent() {
    let backend = FaultyBackend::shared();
    let mut ctrl = primary_with_replica(&backend);
    let partition = session_partition("c1");
    let write = session_write(&ctrl, "c1");

    let sequence = ctrl.replicate_write(write, &[node(2)], 1).await.unwrap();

    assert_eq!(session_writes_sent(&ctrl), vec!["c1".to_string()]);
    assert_eq!(ctrl.write_log().entry_count(partition), 1);
    assert_eq!(ctrl.sequence(partition), Some(sequence));
    assert_eq!(ctrl.pending_write_count(), 1);
}

#[tokio::test]
async fn a_replicated_write_refused_for_pending_capacity_is_not_stored_logged_or_sent() {
    let backend = FaultyBackend::shared();
    let mut ctrl = primary_with_replica(&backend);
    let partition = session_partition("filler");
    let mut filled = 0;
    while ctrl.pending_write_count() < 1000 {
        let write = session_write(&ctrl, &format!("filler-{filled}"));
        let write = ReplicationWrite { partition, ..write };
        ctrl.replicate_write(write, &[node(2)], 1).await.unwrap();
        filled += 1;
    }
    let logged_before = ctrl.write_log().entry_count(partition);
    let sent_before = session_writes_sent(&ctrl).len();
    let sequence_before = ctrl.sequence(partition);
    let write = ReplicationWrite {
        partition,
        ..session_write(&ctrl, "c1")
    };
    assert!(ctrl.stores().sessions.remove("c1").is_some());

    let result = ctrl.replicate_write(write, &[node(2)], 1).await;

    assert_eq!(result, Err(ReplicationError::TooManyPending));
    assert_eq!(session_writes_sent(&ctrl).len(), sent_before);
    assert_eq!(ctrl.write_log().entry_count(partition), logged_before);
    assert_eq!(ctrl.sequence(partition), sequence_before);
    assert!(ctrl.stores().sessions.get("c1").is_none());
}

#[tokio::test]
async fn a_session_the_primary_cannot_store_is_refused_and_not_kept() {
    let backend = FaultyBackend::shared();
    let mut ctrl = primary_with_replica(&backend);
    let partition = session_partition("c1");
    backend.fail_writes(true);

    let refused = ctrl.create_session_replicated("c1").await;

    assert_eq!(refused.err(), Some(ReplicationError::StoreFailed));
    assert_eq!(session_writes_sent(&ctrl), Vec::<String>::new());
    assert_eq!(ctrl.write_log().entry_count(partition), 0);
    assert!(ctrl.stores().sessions.get("c1").is_none());

    backend.fail_writes(false);
    let created = ctrl.create_session_replicated("c1").await;

    assert_eq!(
        created.map(|session| session.client_id_str().to_string()),
        Ok("c1".to_string())
    );
    assert_eq!(session_writes_sent(&ctrl), vec!["c1".to_string()]);
    assert_eq!(ctrl.write_log().entry_count(partition), 1);
}

#[tokio::test]
async fn a_session_without_replicas_the_primary_cannot_store_is_refused_and_not_kept() {
    let backend = FaultyBackend::shared();
    let mut ctrl = controller_where(Some(&backend), |_| node(1), &[]);
    let partition = session_partition("c1");
    let sequence_before = ctrl.sequence(partition);
    backend.fail_writes(true);

    let result = ctrl.create_session_replicated("c1").await;

    assert_eq!(result.err(), Some(ReplicationError::StoreFailed));
    assert!(ctrl.stores().sessions.get("c1").is_none());
    assert_eq!(ctrl.write_log().entry_count(partition), 0);
    assert_eq!(ctrl.sequence(partition), sequence_before);
}

#[tokio::test]
async fn a_session_without_replicas_is_persisted_on_the_primary() {
    let backend = FaultyBackend::shared();
    let mut ctrl = controller_where(Some(&backend), |_| node(1), &[]);
    assert_eq!(backend.keys_containing("zzqq").unwrap(), 0);

    ctrl.create_session_replicated("zzqq").await.unwrap();

    assert_eq!(backend.keys_containing("zzqq").unwrap(), 1);
}

#[tokio::test]
async fn a_replicated_session_is_persisted_on_the_primary() {
    let backend = FaultyBackend::shared();
    let mut ctrl = primary_with_replica(&backend);
    assert_eq!(backend.keys_containing("zzqq").unwrap(), 0);

    ctrl.create_session_replicated("zzqq").await.unwrap();

    assert_eq!(backend.keys_containing("zzqq").unwrap(), 1);
}

#[tokio::test]
async fn a_quorum_session_the_primary_cannot_store_is_refused_and_not_sent() {
    let backend = FaultyBackend::shared();
    let mut ctrl = primary_with_replica(&backend);
    let partition = session_partition("c1");
    let sequence_before = ctrl.sequence(partition);
    backend.fail_writes(true);

    let result = ctrl.create_session_quorum("c1").await;

    assert_eq!(result.err(), Some(ReplicationError::StoreFailed));
    assert_eq!(session_writes_sent(&ctrl), Vec::<String>::new());
    assert_eq!(ctrl.write_log().entry_count(partition), 0);
    assert_eq!(ctrl.sequence(partition), sequence_before);
    assert_eq!(ctrl.pending_write_count(), 0);
    assert!(ctrl.stores().sessions.get("c1").is_none());
}

#[tokio::test]
async fn a_quorum_session_is_persisted_on_the_primary_and_sent() {
    let backend = FaultyBackend::shared();
    let mut ctrl = primary_with_replica(&backend);
    assert_eq!(backend.keys_containing("zzqq").unwrap(), 0);

    let created = ctrl.create_session_quorum("zzqq").await;

    assert!(created.is_ok());
    assert_eq!(session_writes_sent(&ctrl), vec!["zzqq".to_string()]);
    assert_eq!(backend.keys_containing("zzqq").unwrap(), 1);
}

#[tokio::test]
async fn a_quorum_session_refused_for_pending_capacity_leaves_no_trace() {
    let backend = FaultyBackend::shared();
    let mut ctrl = primary_with_replica(&backend);
    let partition = session_partition("filler");
    let mut filled = 0;
    while ctrl.pending_write_count() < 1000 {
        let write = session_write(&ctrl, &format!("filler-{filled}"));
        let write = ReplicationWrite { partition, ..write };
        ctrl.replicate_write(write, &[node(2)], 1).await.unwrap();
        filled += 1;
    }
    let zzqq_partition = session_partition("zzqq");
    let sequence_before = ctrl.sequence(zzqq_partition);
    let logged_before = ctrl.write_log().entry_count(zzqq_partition);
    let sent_before = session_writes_sent(&ctrl).len();

    let result = ctrl.create_session_quorum("zzqq").await;

    assert_eq!(result.err(), Some(ReplicationError::TooManyPending));
    assert_eq!(ctrl.sequence(zzqq_partition), sequence_before);
    assert_eq!(ctrl.write_log().entry_count(zzqq_partition), logged_before);
    assert_eq!(session_writes_sent(&ctrl).len(), sent_before);
    assert!(ctrl.stores().sessions.get("zzqq").is_none());
    assert_eq!(backend.keys_containing("zzqq").unwrap(), 0);
}

#[tokio::test]
async fn an_ack_for_one_partition_does_not_complete_another_partitions_quorum() {
    let backend = FaultyBackend::shared();
    let mut ctrl = primary_with_replica(&backend);
    let first = session_partition("a1");
    let other = (0..1000)
        .map(|i| format!("b{i}"))
        .find(|client| session_partition(client) != first)
        .unwrap();
    let second = session_partition(&other);
    let (_, mut first_done) = ctrl.create_session_quorum("a1").await.unwrap();
    let (_, mut second_done) = ctrl.create_session_quorum(&other).await.unwrap();
    let sequence = ctrl.sequence(second).unwrap();
    assert_eq!(ctrl.sequence(first), Some(sequence));

    ctrl.handle_ack(crate::cluster::protocol::ReplicationAck::ok(
        second,
        ctrl.epoch(second).unwrap(),
        sequence,
        node(2),
    ))
    .await;

    assert_eq!(
        (first_done.try_recv().ok(), second_done.try_recv().ok()),
        (None, Some(QuorumResult::Success))
    );
}

#[tokio::test]
async fn a_not_replica_ack_fails_the_quorum_session() {
    let backend = FaultyBackend::shared();
    let mut ctrl = primary_with_replica(&backend);
    let partition = session_partition("a1");
    let (_, mut done) = ctrl.create_session_quorum("a1").await.unwrap();

    ctrl.handle_ack(crate::cluster::protocol::ReplicationAck::not_replica(
        partition,
        node(2),
    ))
    .await;

    assert_eq!(
        (done.try_recv().ok(), ctrl.pending_write_count()),
        (Some(QuorumResult::Failed), 0)
    );
}

#[tokio::test]
async fn a_stale_epoch_ack_fails_the_quorum_session() {
    let backend = FaultyBackend::shared();
    let mut ctrl = primary_with_replica(&backend);
    let partition = session_partition("a1");
    let (_, mut done) = ctrl.create_session_quorum("a1").await.unwrap();

    ctrl.handle_ack(crate::cluster::protocol::ReplicationAck::stale_epoch(
        partition,
        Epoch::new(99),
        node(2),
    ))
    .await;

    assert_eq!(
        (done.try_recv().ok(), ctrl.pending_write_count()),
        (Some(QuorumResult::Failed), 0)
    );
}

#[tokio::test]
async fn a_replicated_write_without_replicas_holds_no_pending_slot() {
    let backend = FaultyBackend::shared();
    let mut ctrl = controller_where(Some(&backend), |_| node(1), &[]);
    let partition = session_partition("filler");
    let mut refused = None;
    for i in 0..1001 {
        let write = ReplicationWrite {
            partition,
            ..session_write(&ctrl, &format!("f-{i}"))
        };
        if let Err(error) = ctrl.replicate_write(write, &[], 0).await {
            refused = Some((i, error));
            break;
        }
    }

    assert_eq!((refused, ctrl.pending_write_count()), (None, 0));
}
