// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::db_ops::ForwardRequest;
use super::*;
use crate::cluster::NUM_PARTITIONS;
use crate::cluster::protocol::{Heartbeat, Operation};
use crate::cluster::quorum::QuorumResult;
use crate::cluster::session_partition;
use crate::cluster::test_support::{put_record, remove_record};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

fn create_test_controller(
    node_id: NodeId,
    transport: MockTransport,
) -> NodeController<MockTransport> {
    let tx_raft_messages = flume::unbounded().0;
    let tx_raft_events = flume::unbounded().0;
    NodeController::new(
        node_id,
        transport,
        TransportConfig::default(),
        tx_raft_messages,
        tx_raft_events,
    )
}

/// Establish the unique quorum group by placing `primary` + `replicas` in the (replicated)
/// partition map on partition 5 — the source `unique_fanout_group` now derives membership from.
fn set_unique_group(
    ctrl: &mut NodeController<MockTransport>,
    primary: NodeId,
    replicas: &[NodeId],
) {
    let mut map = crate::cluster::PartitionMap::default();
    map.set(
        PartitionId::new(5).unwrap(),
        crate::cluster::PartitionAssignment {
            primary: Some(primary),
            replicas: replicas.to_vec(),
            epoch: Epoch::new(1),
        },
    );
    ctrl.update_partition_map(map);
    // Found the voter set to the initial group on first setup (mirrors the leader founding voters =
    // membership); later membership changes do not re-found, so tests that manipulate the voter set
    // separately keep control of it. Without a voter set, reserves/seals fail closed.
    if ctrl.unique_voter_group().is_empty() {
        let group: std::collections::BTreeSet<NodeId> = std::iter::once(primary)
            .chain(replicas.iter().copied())
            .collect();
        ctrl.seed_unique_voters(group);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum QueuedPublish {
    Plain(String, Vec<u8>, u8),
    Routed(RoutedPublish),
}

#[derive(Debug, Clone)]
struct MockTransport {
    node_id: NodeId,
    inbox: Arc<Mutex<VecDeque<InboundMessage>>>,
    outbox: Arc<Mutex<Vec<(NodeId, ClusterMessage)>>>,
    linked: Arc<Mutex<Option<Vec<NodeId>>>>,
    queued_publishes: Arc<Mutex<Vec<QueuedPublish>>>,
    refuse_local_delivery: Arc<std::sync::atomic::AtomicBool>,
    refuse_sends: Arc<std::sync::atomic::AtomicBool>,
}

impl MockTransport {
    fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            inbox: Arc::new(Mutex::new(VecDeque::new())),
            outbox: Arc::new(Mutex::new(Vec::new())),
            linked: Arc::new(Mutex::new(None)),
            queued_publishes: Arc::new(Mutex::new(Vec::new())),
            refuse_local_delivery: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            refuse_sends: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    fn inject_message(&self, from: NodeId, message: ClusterMessage) {
        self.inbox.lock().unwrap().push_back(InboundMessage {
            from,
            message,
            received_at: 0,
        });
    }

    fn sent_messages(&self) -> Vec<(NodeId, ClusterMessage)> {
        self.outbox.lock().unwrap().clone()
    }

    fn queued_publishes(&self) -> Vec<QueuedPublish> {
        self.queued_publishes.lock().unwrap().clone()
    }
}

impl ClusterTransport for MockTransport {
    fn local_node(&self) -> NodeId {
        self.node_id
    }

    async fn send(
        &self,
        to: NodeId,
        message: ClusterMessage,
    ) -> Result<(), super::super::transport::TransportError> {
        if self.refuse_sends.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(super::super::transport::TransportError::NotConnected);
        }
        self.outbox.lock().unwrap().push((to, message));
        Ok(())
    }

    async fn broadcast(
        &self,
        message: ClusterMessage,
    ) -> Result<(), super::super::transport::TransportError> {
        self.outbox
            .lock()
            .unwrap()
            .push((NodeId::validated(0).unwrap_or(self.node_id), message));
        Ok(())
    }

    async fn send_to_partition_primary(
        &self,
        _: PartitionId,
        _: ClusterMessage,
    ) -> Result<(), super::super::transport::TransportError> {
        Ok(())
    }

    async fn direct_peers(&self) -> Option<Vec<NodeId>> {
        self.linked.lock().unwrap().clone()
    }

    fn recv(&self) -> Option<InboundMessage> {
        self.inbox.lock().unwrap().pop_front()
    }

    fn try_recv_timeout(&self, _: u64) -> Option<InboundMessage> {
        self.inbox.lock().unwrap().pop_front()
    }

    fn pending_count(&self) -> usize {
        self.inbox.lock().unwrap().len()
    }

    fn requeue(&self, msg: InboundMessage) {
        self.inbox.lock().unwrap().push_front(msg);
    }

    async fn queue_local_publish(&self, topic: String, payload: Vec<u8>, qos: u8) {
        self.queued_publishes
            .lock()
            .unwrap()
            .push(QueuedPublish::Plain(topic, payload, qos));
    }

    async fn queue_routed_publish(&self, publish: RoutedPublish) {
        self.queued_publishes
            .lock()
            .unwrap()
            .push(QueuedPublish::Routed(publish));
    }

    async fn deliver_routed_publish(&self, publish: RoutedPublish) -> bool {
        if self
            .refuse_local_delivery
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return false;
        }
        self.queue_routed_publish(publish).await;
        true
    }

    async fn queue_local_publish_retained(&self, _: String, _: Vec<u8>, _: u8) {}
    async fn queue_retained_refresh(&self, _: String) {}
}

fn forwarded_publish(origin: NodeId, topic: &str) -> ClusterMessage {
    ClusterMessage::ForwardedPublish(
        crate::cluster::protocol::ForwardedPublish::new(
            origin,
            topic.to_string(),
            1,
            false,
            b"reading".to_vec(),
            vec![crate::cluster::protocol::ForwardTarget::new(
                "subscriber".to_string(),
                1,
            )],
        )
        .with_user_properties(vec![(
            "x-origin-client-id".to_string(),
            "sensor-7".to_string(),
        )])
        .with_order_key(Some("plant/temp/sensor-7".to_string())),
    )
}

#[tokio::test]
async fn a_forwarded_publish_is_queued_for_delivery_not_republished_as_a_client() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let mut ctrl = create_test_controller(node1, MockTransport::new(node1));

    ctrl.transport
        .inject_message(node2, forwarded_publish(node2, "plant/temp"));
    ctrl.process_messages().await;

    assert_eq!(
        ctrl.transport.queued_publishes(),
        vec![QueuedPublish::Routed(RoutedPublish {
            topic: "plant/temp".to_string(),
            payload: b"reading".to_vec(),
            qos: 1,
            user_properties: vec![("x-origin-client-id".to_string(), "sensor-7".to_string())],
            order_key: Some("plant/temp/sensor-7".to_string()),
        })]
    );
}

#[tokio::test]
async fn a_forwarded_publish_from_the_message_processor_is_queued_for_delivery() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let mut ctrl = create_test_controller(node1, MockTransport::new(node1));

    ctrl.handle_filtered_message(InboundMessage {
        from: node2,
        message: forwarded_publish(node2, "plant/temp"),
        received_at: 0,
    })
    .await;

    assert_eq!(
        ctrl.transport.queued_publishes(),
        vec![QueuedPublish::Routed(RoutedPublish {
            topic: "plant/temp".to_string(),
            payload: b"reading".to_vec(),
            qos: 1,
            user_properties: vec![("x-origin-client-id".to_string(), "sensor-7".to_string())],
            order_key: Some("plant/temp/sensor-7".to_string()),
        })]
    );
}

#[test]
fn become_primary_and_replicate() {
    let node1 = NodeId::validated(1).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    let partition = PartitionId::ZERO;
    ctrl.become_primary(partition, Epoch::new(1));

    assert_eq!(ctrl.role(partition), ReplicaRole::Primary);
    assert_eq!(ctrl.sequence(partition), Some(0));
}

#[tokio::test]
async fn replicate_write_sends_to_replicas() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();

    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    let partition = PartitionId::ZERO;
    ctrl.become_primary(partition, Epoch::new(1));

    let write = ReplicationWrite::new(
        partition,
        Operation::Insert,
        Epoch::new(1),
        0,
        "test".to_string(),
        "1".to_string(),
        vec![1, 2, 3],
    );

    let seq = ctrl
        .replicate_write(write, &[node2, node3], 1)
        .await
        .unwrap();
    assert_eq!(seq, 1);

    let sent = ctrl.transport.sent_messages();
    assert_eq!(sent.len(), 2);
}

fn unique_reserve_write(
    ctrl: &NodeController<MockTransport>,
    entity: &str,
    field: &str,
    value: &[u8],
) -> ReplicationWrite {
    let params = crate::cluster::db::UniqueReserveParams {
        entity,
        field,
        value,
        record_id: "r1",
        request_id: "req-1",
        data_partition: PartitionId::new(5).unwrap(),
        ttl_ms: 30_000,
    };
    let (result, write) = ctrl.stores().unique_reserve_replicated(&params, 1000);
    assert_eq!(result, crate::cluster::db::ReserveResult::Reserved);
    write.expect("reserve should yield a replication write")
}

#[tokio::test]
async fn unique_reserve_resolves_on_majority_ack() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();
    let mut ctrl = create_test_controller(node1, MockTransport::new(node1));
    ctrl.register_peer(node2);
    ctrl.register_peer(node3);
    set_unique_group(&mut ctrl, node1, &[node2, node3]);
    ctrl.become_primary(PartitionId::new(5).unwrap(), Epoch::new(1));

    let write = unique_reserve_write(&ctrl, "users", "email", b"a@x.com");
    let rx = ctrl.replicate_unique_reserve(write).await;

    // group is {1,2,3}, majority 2, self already counts as one; one more ack completes it.
    ctrl.record_unique_quorum_ack(node2, 1).await;

    assert_eq!(
        rx.await,
        Ok(true),
        "reserve resolves once a majority holds it"
    );
}

#[tokio::test]
async fn unique_reserve_fails_closed_on_timeout() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();
    let mut ctrl = create_test_controller(node1, MockTransport::new(node1));
    ctrl.register_peer(node2);
    ctrl.register_peer(node3);
    set_unique_group(&mut ctrl, node1, &[node2, node3]);
    ctrl.become_primary(PartitionId::new(5).unwrap(), Epoch::new(1));

    let write = unique_reserve_write(&ctrl, "users", "email", b"a@x.com");
    let rx = ctrl.replicate_unique_reserve(write).await;

    // no acks arrive; the deadline sweep must fail the reserve closed.
    ctrl.sweep_unique_quorum(NodeController::<MockTransport>::current_time_ms() + 10_000)
        .await;

    assert_eq!(
        rx.await,
        Ok(false),
        "a reserve that never reaches a majority fails closed"
    );
}

#[tokio::test]
async fn remote_reserve_quorum_timeout_releases_local_reservation() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();
    let mut ctrl = create_test_controller(node1, MockTransport::new(node1));
    ctrl.register_peer(node2);
    ctrl.register_peer(node3);
    set_unique_group(&mut ctrl, node1, &[node2, node3]);
    ctrl.become_primary(PartitionId::new(5).unwrap(), Epoch::new(1));

    // A remote-coordinated reserve landed on the value-primary: it holds the reservation locally
    // and defers its response until the reservation is majority-durable.
    let write = unique_reserve_write(&ctrl, "users", "email", b"a@x.com");
    assert!(
        ctrl.stores()
            .unique_get("users", "email", b"a@x.com")
            .is_some(),
        "the value-primary holds the reservation after the local reserve"
    );
    ctrl.replicate_unique_reserve_remote(
        write,
        node2,
        42,
        UniqueClaimId {
            entity: "users".to_string(),
            field: "email".to_string(),
            value: b"a@x.com".to_vec(),
            idempotency_key: "req-1".to_string(),
        },
    )
    .await;

    // No group member acks; the deadline sweep fails the reserve closed. It must ALSO release the
    // local reservation, or a transient quorum timeout wedges the value until its 30s TTL.
    ctrl.sweep_unique_quorum(NodeController::<MockTransport>::current_time_ms() + 10_000)
        .await;

    assert!(
        ctrl.stores()
            .unique_get("users", "email", b"a@x.com")
            .is_none(),
        "a remote reserve that never reached a majority must release its local reservation"
    );

    let sent_error = ctrl.transport.sent_messages().iter().any(|(to, m)| {
        *to == node2
            && matches!(
                m,
                ClusterMessage::UniqueReserveResponse(r)
                    if r.request_id == 42
                        && matches!(r.status(), UniqueReserveStatus::Error)
            )
    });
    assert!(
        sent_error,
        "the coordinator must receive an Error response so it fails the create closed"
    );
}

#[tokio::test]
async fn reconcile_apply_skips_record_deleted_after_collection() {
    use crate::cluster::db::ClusterConstraint;

    let node1 = NodeId::validated(1).unwrap();
    let mut ctrl = create_test_controller(node1, MockTransport::new(node1));
    let mut map = crate::cluster::PartitionMap::default();
    for i in 0..NUM_PARTITIONS {
        let p = PartitionId::new(i).unwrap();
        ctrl.become_primary(p, Epoch::new(1));
        map.set(
            p,
            crate::cluster::PartitionAssignment {
                primary: Some(node1),
                replicas: vec![],
                epoch: Epoch::new(1),
            },
        );
    }
    ctrl.update_partition_map(map);
    ctrl.stores()
        .db_constraints
        .add(ClusterConstraint::unique("users", "uniq_email", "email"))
        .unwrap();
    put_record(
        &ctrl.stores().db_data,
        "users",
        "u1",
        br#"{"email":"a@x.com"}"#,
        1000,
    );

    let work = ctrl.collect_unique_reconcile_work();
    assert_eq!(
        work.len(),
        1,
        "one reassert work item for the durable record"
    );

    // The lock is released between collect and apply; the record is deleted in that window.
    remove_record(&ctrl.stores().db_data, "users", "u1");
    ctrl.apply_unique_reconcile_chunk(&work).await;

    let value = serde_json::to_vec(&serde_json::json!("a@x.com")).unwrap();
    assert!(
        ctrl.stores().unique_get("users", "email", &value).is_none(),
        "a reassert for a record deleted since collection must not resurrect a committed claim"
    );
}

#[tokio::test]
async fn reassert_replicated_rejects_stale_epoch_below_the_fence() {
    use crate::cluster::db::ReassertResult;

    let node1 = NodeId::validated(1).unwrap();
    let ctrl = create_test_controller(node1, MockTransport::new(node1));
    let p = PartitionId::new(5).unwrap();
    let value = serde_json::to_vec(&serde_json::json!("a@x.com")).unwrap();

    // The value site has seen data-partition epoch 3 (e.g. a release from the current primary).
    ctrl.stores().db_unique.fence_bump(p, 3);

    // A reassert stamped with epoch 2 (< 3) is from a superseded primary: dropped, no claim, no write.
    let (result, write) = ctrl
        .stores()
        .reassert_replicated("users", "email", &value, "u1", p, 2, 1000);
    assert_eq!(
        result,
        ReassertResult::Pending,
        "stale-epoch reassert is fenced out"
    );
    assert!(
        write.is_none(),
        "a fenced-out reassert emits no replication write"
    );
    assert!(
        ctrl.stores().unique_get("users", "email", &value).is_none(),
        "a stale reassert must not establish a committed claim"
    );

    // A current-epoch reassert (3 >= 3) passes the fence and establishes the claim.
    let (result, write) = ctrl
        .stores()
        .reassert_replicated("users", "email", &value, "u1", p, 3, 1000);
    assert_eq!(result, ReassertResult::Established);
    assert!(write.is_some());
    assert!(
        ctrl.stores().unique_get("users", "email", &value).is_some(),
        "a current-epoch reassert establishes the claim"
    );
}

#[tokio::test]
async fn project_reconcile_work_skips_key_deleted_after_gather() {
    use crate::cluster::db::ClusterConstraint;

    let node1 = NodeId::validated(1).unwrap();
    let mut ctrl = create_test_controller(node1, MockTransport::new(node1));
    let mut map = crate::cluster::PartitionMap::default();
    for i in 0..NUM_PARTITIONS {
        let p = PartitionId::new(i).unwrap();
        ctrl.become_primary(p, Epoch::new(1));
        map.set(
            p,
            crate::cluster::PartitionAssignment {
                primary: Some(node1),
                replicas: vec![],
                epoch: Epoch::new(1),
            },
        );
    }
    ctrl.update_partition_map(map);
    ctrl.stores()
        .db_constraints
        .add(ClusterConstraint::unique("users", "uniq_email", "email"))
        .unwrap();
    put_record(
        &ctrl.stores().db_data,
        "users",
        "u1",
        br#"{"email":"a@x.com"}"#,
        1000,
    );

    let keys = ctrl.collect_unique_reconcile_keys();
    assert_eq!(keys.len(), 1, "one owned durable record gathered");

    // The read lock is released between key-gather and the per-chunk parse; the record is deleted in
    // that window, so projecting the stale key must yield no work rather than parse a gone record.
    remove_record(&ctrl.stores().db_data, "users", "u1");
    let work = ctrl.project_unique_reconcile_work(&keys);
    assert!(
        work.is_empty(),
        "a key whose record was deleted after gather projects to no reassert work"
    );
}

#[tokio::test]
async fn unique_fanout_group_derives_from_map_not_heartbeat_growth() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();
    let mut ctrl = create_test_controller(node1, MockTransport::new(node1));

    // Membership comes from the replicated partition map: {node1, node2}.
    set_unique_group(&mut ctrl, node1, &[node2]);
    assert_eq!(ctrl.unique_fanout_group(), vec![node1, node2]);

    // A previously-unknown node appearing in the heartbeat-discovered set must NOT expand the quorum
    // basis — that local, divergent growth was the disjoint-majority oversell source.
    ctrl.register_peer(node3);
    assert_eq!(
        ctrl.unique_fanout_group(),
        vec![node1, node2],
        "the quorum group derives from the replicated map, not the heartbeat-discovered node set"
    );

    // node3 joins the group only once consensus (the map) places it there.
    set_unique_group(&mut ctrl, node1, &[node2, node3]);
    assert_eq!(ctrl.unique_fanout_group(), vec![node1, node2, node3]);
}

#[tokio::test]
async fn empty_voter_set_fails_closed_before_founding_in_multi_node_cluster() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let mut ctrl = create_test_controller(node1, MockTransport::new(node1));

    // A MULTI-node partition map exists but no voter set has been founded/adopted yet — set the map
    // directly so the founding in `set_unique_group` does not run. With an empty voter set in a
    // multi-node cluster the group is empty and the majority is unreachable, so reserves and seals
    // fail closed — it does NOT fall back to the full membership (which would let a restarted
    // promoting primary seal over a group larger than the established set and miss its holders).
    let mut map = crate::cluster::PartitionMap::default();
    map.set(
        PartitionId::new(5).unwrap(),
        crate::cluster::PartitionAssignment {
            primary: Some(node1),
            replicas: vec![node2],
            epoch: Epoch::new(1),
        },
    );
    ctrl.update_partition_map(map);

    assert!(ctrl.unique_voter_group().is_empty());
    assert_eq!(ctrl.unique_majority(), usize::MAX);
    assert!(!ctrl.is_unique_voter(node1));

    // A single-node cluster (this node is the only member) still self-serves — it is trivially its
    // own voter set, so there is no larger membership to miss a reservation on.
    let mut solo_ctrl = create_test_controller(node1, MockTransport::new(node1));
    let mut solo_map = crate::cluster::PartitionMap::default();
    solo_map.set(
        PartitionId::new(5).unwrap(),
        crate::cluster::PartitionAssignment {
            primary: Some(node1),
            replicas: vec![],
            epoch: Epoch::new(1),
        },
    );
    solo_ctrl.update_partition_map(solo_map);
    assert_eq!(solo_ctrl.unique_voter_group(), vec![node1]);
    assert_eq!(solo_ctrl.unique_majority(), 1);
}

#[tokio::test]
async fn unique_voter_group_lags_membership_when_seeded() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();
    let mut ctrl = create_test_controller(node1, MockTransport::new(node1));
    set_unique_group(&mut ctrl, node1, &[node2, node3]);

    // A lagged voter set excludes the newly-joined node3: the seal/majority denominator stays {1,2}
    // so node3 cannot dilute a majority away from an existing reservation's holders.
    ctrl.seed_unique_voters([node1, node2].into_iter().collect());
    assert_eq!(ctrl.unique_voter_group(), vec![node1, node2]);
    assert_eq!(ctrl.unique_majority(), 2);
    assert!(ctrl.is_unique_voter(node1));
    assert!(!ctrl.is_unique_voter(node3));
}

#[tokio::test]
async fn manage_unique_voters_founds_then_paces_growth() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();
    let mut ctrl = create_test_controller(node1, MockTransport::new(node1));
    set_unique_group(&mut ctrl, node1, &[node2]);

    // Founding takes the whole current membership at once.
    ctrl.manage_unique_voters(true, 1, 1_000);
    assert_eq!(ctrl.unique_voter_group(), vec![node1, node2]);

    // node3 joins the membership; within the change interval the voter set does NOT grow.
    set_unique_group(&mut ctrl, node1, &[node2, node3]);
    ctrl.manage_unique_voters(true, 1, 2_000);
    assert_eq!(ctrl.unique_voter_group(), vec![node1, node2]);

    // After the interval it promotes exactly one learner.
    ctrl.manage_unique_voters(
        true,
        1,
        1_000 + super::unique::UNIQUE_VOTER_CHANGE_INTERVAL_MS + 1,
    );
    assert_eq!(ctrl.unique_voter_group(), vec![node1, node2, node3]);
}

#[tokio::test]
async fn manage_unique_voters_removes_departed_voter() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();
    let mut ctrl = create_test_controller(node1, MockTransport::new(node1));
    set_unique_group(&mut ctrl, node1, &[node2, node3]);
    ctrl.manage_unique_voters(true, 1, 1_000);
    assert_eq!(ctrl.unique_voter_group(), vec![node1, node2, node3]);

    // node3 leaves the membership; after the interval it is dropped from the voter set.
    set_unique_group(&mut ctrl, node1, &[node2]);
    ctrl.manage_unique_voters(
        true,
        1,
        1_000 + super::unique::UNIQUE_VOTER_CHANGE_INTERVAL_MS + 1,
    );
    assert_eq!(ctrl.unique_voter_group(), vec![node1, node2]);
}

#[tokio::test]
async fn correlated_removal_does_not_collapse_voter_majority() {
    // Finding-2 regression guard. A correlated multi-node loss must NOT instantly shrink the voter
    // majority/seal denominator: removal is paced one node per interval (single-step safety, V6). If
    // the denominator collapsed by 2 at once, a seal could reach a majority that misses a
    // reservation's holder-majority and oversell (V6 unpaced). Departed voters stay counted until
    // paced removal, so a seal must reach a majority of the FULL set — which still intersects any
    // holder-majority — or fail closed.
    let n = |i: u16| NodeId::validated(i).unwrap();
    let mut ctrl = create_test_controller(n(1), MockTransport::new(n(1)));
    set_unique_group(&mut ctrl, n(1), &[n(2), n(3), n(4), n(5)]);
    ctrl.seed_unique_voters([n(1), n(2), n(3), n(4), n(5)].into_iter().collect());
    assert_eq!(ctrl.unique_majority(), 3);
    assert_eq!(
        ctrl.unique_voter_group(),
        vec![n(1), n(2), n(3), n(4), n(5)]
    );

    // Nodes 4 and 5 leave the partition map together (correlated failure), before the leader's paced
    // removal has run. The voter denominator must stay at 5 (majority 3), not collapse to {1,2,3}.
    set_unique_group(&mut ctrl, n(1), &[n(2), n(3)]);
    assert_eq!(
        ctrl.unique_majority(),
        3,
        "a correlated 2-node loss must not instantly collapse the voter majority"
    );
    assert_eq!(
        ctrl.unique_voter_group(),
        vec![n(1), n(2), n(3), n(4), n(5)],
        "departed voters remain in the denominator until paced removal (fail-closed, not oversell)"
    );

    // The leader then paces them out one node per interval, converging to the surviving membership.
    ctrl.manage_unique_voters(true, 1, super::unique::UNIQUE_VOTER_CHANGE_INTERVAL_MS + 1);
    assert_eq!(
        ctrl.unique_voter_group().len(),
        4,
        "exactly one departed voter is removed per interval"
    );
    ctrl.manage_unique_voters(
        true,
        1,
        2 * super::unique::UNIQUE_VOTER_CHANGE_INTERVAL_MS + 2,
    );
    assert_eq!(ctrl.unique_voter_group(), vec![n(1), n(2), n(3)]);
}

#[tokio::test]
async fn followers_do_not_manage_voters() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let mut ctrl = create_test_controller(node1, MockTransport::new(node1));
    set_unique_group(&mut ctrl, node1, &[node2]);

    // A non-leader never establishes a voter set of its own; it adopts the leader's via heartbeat.
    ctrl.manage_unique_voters(false, 1, 1_000);
    ctrl.seed_unique_voters([node1].into_iter().collect());
    ctrl.manage_unique_voters(false, 1, 2_000);
    assert_eq!(ctrl.unique_voter_group(), vec![node1]);
}

#[test]
fn heartbeat_round_trips_voter_set() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let mut hb = Heartbeat::create(node1, 1234);
    hb.set_voters(7, 3, &[node1, node2]);
    let bytes = hb.to_be_bytes();
    let (decoded, consumed) = Heartbeat::try_from_be_bytes(&bytes).unwrap();
    assert_eq!(consumed, bytes.len());
    assert!(decoded.carries_voters());
    assert_eq!(decoded.voter_term(), 7);
    assert_eq!(decoded.voter_seq(), 3);
    assert_eq!(decoded.voters(), vec![node1, node2]);
}

#[tokio::test]
async fn unique_reserve_completes_immediately_for_single_node_group() {
    let node1 = NodeId::validated(1).unwrap();
    let mut ctrl = create_test_controller(node1, MockTransport::new(node1));
    ctrl.become_primary(PartitionId::new(5).unwrap(), Epoch::new(1));

    let write = unique_reserve_write(&ctrl, "users", "email", b"a@x.com");
    let rx = ctrl.replicate_unique_reserve(write).await;

    assert_eq!(
        rx.await,
        Ok(true),
        "a single-node group is its own majority; reserve is immediately durable"
    );
}

#[tokio::test]
async fn fk_then_unique_defers_reserve_to_async_completion() {
    use crate::cluster::db::ClusterConstraint;
    use crate::cluster::node_controller::{FkCheckContinuation, FkThenUniqueOutcome};

    let node1 = NodeId::validated(1).unwrap();
    let mut ctrl = create_test_controller(node1, MockTransport::new(node1));
    // Single-node group: become_primary seals every partition synchronously.
    let mut map = crate::cluster::PartitionMap::default();
    for i in 0..NUM_PARTITIONS {
        let p = PartitionId::new(i).unwrap();
        ctrl.become_primary(p, Epoch::new(1));
        map.set(
            p,
            crate::cluster::PartitionAssignment {
                primary: Some(node1),
                replicas: vec![],
                epoch: Epoch::new(1),
            },
        );
    }
    ctrl.update_partition_map(map);
    ctrl.stores()
        .db_constraints
        .add(ClusterConstraint::unique("users", "uniq_email", "email"))
        .unwrap();

    let continuation = FkCheckContinuation::CreateFromNodeController {
        from: node1,
        entity: "users".to_string(),
        id: "u1".to_string(),
        data: serde_json::json!({ "id": "u1", "email": "a@x.com" }),
        partition: crate::cluster::db::data_partition("users", "u1"),
        request_id: "req-1".to_string(),
        sender: None,
        client_id: None,
        response_topic: "resp/t".to_string(),
        correlation_data: None,
    };

    let outcome = ctrl
        .complete_pending_fk_work(true, None, continuation)
        .await;

    // The reserve must be handed to the async completion (which awaits the quorum OUTSIDE the lock),
    // never performed inline here — doing so would deadlock on the ack path.
    assert!(
        matches!(outcome, FkThenUniqueOutcome::NeedUnique(_)),
        "an FK+unique create must defer its reserve to spawn_unique_completion"
    );
}

#[tokio::test]
async fn unsealed_partition_fails_reserve_closed() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();
    let mut ctrl = create_test_controller(node1, MockTransport::new(node1));
    ctrl.register_peer(node2);
    ctrl.register_peer(node3);
    set_unique_group(&mut ctrl, node1, &[node2, node3]);

    let entity = "users";
    ctrl.stores()
        .db_constraints
        .add(crate::cluster::db::ClusterConstraint::unique(
            entity,
            "uniq_email",
            "email",
        ))
        .unwrap();
    let value = b"a@x.com";
    let unique_part = crate::cluster::db::unique_partition(entity, "email", value);
    // Primary at epoch 1, but a multi-node group has not completed the seal handshake.
    ctrl.become_primary(unique_part, Epoch::new(1));
    assert!(
        !ctrl.unique_partition_sealed(unique_part),
        "a multi-node group is not sealed until the handshake completes"
    );

    let data = serde_json::json!({ "email": "a@x.com" });
    let result = ctrl
        .check_unique_constraints(
            entity,
            "u1",
            &data,
            PartitionId::new(5).unwrap(),
            "req-1",
            1000,
        )
        .await;

    assert!(
        result.is_err(),
        "reserve must fail closed while the value partition is unsealed"
    );
    assert!(
        ctrl.stores().unique_get(entity, "email", value).is_none(),
        "no reservation should be recorded for a fail-closed reserve"
    );
}

#[tokio::test]
async fn unique_seal_completes_for_single_node() {
    let node1 = NodeId::validated(1).unwrap();
    let mut ctrl = create_test_controller(node1, MockTransport::new(node1));
    let p = PartitionId::new(5).unwrap();

    ctrl.become_primary(p, Epoch::new(2));
    ctrl.drain_pending_seals().await;

    assert_eq!(
        ctrl.unique_sealed.get(&p.get()),
        Some(&Epoch::new(2)),
        "a single-node group is its own majority; the partition seals immediately"
    );
}

#[tokio::test]
async fn unique_seal_reaches_majority_on_response() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();
    let mut ctrl = create_test_controller(node1, MockTransport::new(node1));
    ctrl.register_peer(node2);
    ctrl.register_peer(node3);
    set_unique_group(&mut ctrl, node1, &[node2, node3]);
    let p = PartitionId::new(5).unwrap();

    ctrl.become_primary(p, Epoch::new(2));
    ctrl.drain_pending_seals().await;
    assert_eq!(
        ctrl.unique_sealed.get(&p.get()),
        None,
        "not sealed until a majority of the group has promised the epoch"
    );

    let resp = UniqueSealResponse::create(1, p, 2, true, Vec::new());
    ctrl.handle_unique_seal_response(node2, &resp);

    assert_eq!(
        ctrl.unique_sealed.get(&p.get()),
        Some(&Epoch::new(2)),
        "self + one accepting member is a majority of three"
    );
}

#[tokio::test]
async fn empty_voter_seal_re_queues_then_completes_after_founding() {
    // Proof that the empty-voter fail-closed seal RECOVERS (does not wedge): with no founded voter
    // set a multi-node primary must not seal; once the leader founds the set the re-queued seal must
    // complete. Guards against a poisoned `usize::MAX`-needed tracker leaking from the empty attempt.
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();
    let mut ctrl = create_test_controller(node1, MockTransport::new(node1));
    ctrl.register_peer(node2);
    ctrl.register_peer(node3);
    let p = PartitionId::new(5).unwrap();

    // Multi-node membership set directly so no voter set is founded (unlike `set_unique_group`).
    let mut map = crate::cluster::PartitionMap::default();
    map.set(
        p,
        crate::cluster::PartitionAssignment {
            primary: Some(node1),
            replicas: vec![node2, node3],
            epoch: Epoch::new(1),
        },
    );
    ctrl.update_partition_map(map);

    ctrl.become_primary(p, Epoch::new(2));
    ctrl.drain_pending_seals().await;
    assert_eq!(
        ctrl.unique_sealed.get(&p.get()),
        None,
        "fail closed: a multi-node primary must not seal with no established voter set"
    );
    assert!(
        !ctrl.unique_partition_sealed(p),
        "and it must not report itself sealed while failing closed"
    );

    // The leader founds the voter set; the re-queued seal must now proceed and reach a majority.
    ctrl.seed_unique_voters([node1, node2, node3].into_iter().collect());
    ctrl.drain_pending_seals().await;
    assert_eq!(
        ctrl.unique_sealed.get(&p.get()),
        None,
        "the re-queued seal now runs but still needs a majority of the founded set"
    );

    let resp = UniqueSealResponse::create(1, p, 2, true, Vec::new());
    ctrl.handle_unique_seal_response(node2, &resp);
    assert_eq!(
        ctrl.unique_sealed.get(&p.get()),
        Some(&Epoch::new(2)),
        "the re-queued seal recovers: self + one voter is a majority of three"
    );
}

#[tokio::test]
async fn seal_response_teaches_primary_a_missed_reservation() {
    use crate::cluster::db::{UniqueReservation, UniqueStore};

    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();
    let mut ctrl = create_test_controller(node1, MockTransport::new(node1));
    ctrl.register_peer(node2);
    ctrl.register_peer(node3);

    let value = b"a@x.com";
    let unique_part = crate::cluster::db::unique_partition("users", "email", value);
    // A committed claim for the value exists on the group, but this promoting primary missed it.
    let committed = UniqueReservation::create(
        "users",
        "email",
        value,
        "owner-a",
        "req-a",
        PartitionId::new(5).unwrap(),
        1000,
    )
    .with_committed();
    let member = UniqueStore::new(node2);
    member
        .apply_replicated(
            crate::cluster::protocol::Operation::Insert,
            &crate::cluster::db::unique_key(&committed),
            &UniqueStore::serialize(&committed),
        )
        .unwrap();
    let blob = member.export_for_partition(unique_part);

    assert!(
        ctrl.stores().unique_get("users", "email", value).is_none(),
        "primary starts out missing the reservation"
    );

    let resp = UniqueSealResponse::create(1, unique_part, 2, true, blob);
    ctrl.handle_unique_seal_response(node2, &resp);

    let learned = ctrl
        .stores()
        .unique_get("users", "email", value)
        .expect("the seal response must teach the primary the reservation it missed");
    assert!(learned.is_committed());
    assert_eq!(learned.record_id_str(), "owner-a");
}

#[tokio::test]
async fn unique_seal_request_fenced_by_higher_promise() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let mut ctrl = create_test_controller(node1, MockTransport::new(node1));
    let p = PartitionId::new(5).unwrap();

    ctrl.handle_unique_seal_request(node2, &UniqueSealRequest::create(1, p, 3))
        .await;
    ctrl.handle_unique_seal_request(node2, &UniqueSealRequest::create(2, p, 2))
        .await;

    let accepts: Vec<bool> = ctrl
        .transport
        .sent_messages()
        .iter()
        .filter_map(|(_, m)| match m {
            ClusterMessage::UniqueSealResponse(r) => Some(r.is_accepted()),
            _ => None,
        })
        .collect();
    assert_eq!(
        accepts,
        vec![true, false],
        "epoch 3 is promised, so the stale epoch-2 seal is fenced out"
    );
}

#[tokio::test]
async fn receive_heartbeat_marks_alive() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();

    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    ctrl.register_peer(node2);
    assert_eq!(ctrl.node_status(node2), NodeStatus::Unknown);

    let hb = Heartbeat::create(node2, 1000);
    ctrl.transport
        .inject_message(node2, ClusterMessage::Heartbeat(hb));
    ctrl.process_messages().await;

    assert_eq!(ctrl.node_status(node2), NodeStatus::Alive);
}

#[tokio::test]
async fn handle_write_as_replica() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();

    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    let partition = PartitionId::ZERO;
    ctrl.become_replica(partition, Epoch::new(1), 0);

    let write = ReplicationWrite::new(
        partition,
        Operation::Insert,
        Epoch::new(1),
        1,
        "test".to_string(),
        "1".to_string(),
        vec![],
    );

    ctrl.transport
        .inject_message(node2, ClusterMessage::Write(write));
    ctrl.process_messages().await;

    assert_eq!(ctrl.sequence(partition), Some(1));

    let sent = ctrl.transport.sent_messages();
    assert_eq!(sent.len(), 1);
    match &sent[0].1 {
        ClusterMessage::Ack(ack) => assert!(ack.is_ok()),
        _ => panic!("expected ack"),
    }
}

#[tokio::test]
async fn create_session_quorum_requires_primary() {
    let node1 = NodeId::validated(1).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    let result = ctrl.create_session_quorum("client1").await;
    assert!(matches!(result, Err(ReplicationError::NotPrimary)));
}

#[tokio::test]
async fn create_session_quorum_succeeds_as_primary() {
    let node1 = NodeId::validated(1).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    let partition = session_partition("client1");
    ctrl.become_primary(partition, Epoch::new(1));

    let (session, completion) = ctrl.create_session_quorum("client1").await.unwrap();
    assert_eq!(completion.await, Ok(QuorumResult::Success));
    assert_eq!(session.client_id_str(), "client1");
    assert!(ctrl.stores().sessions.get("client1").is_some());
}

#[tokio::test]
async fn replicate_write_async_sends_without_tracking() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();

    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    let partition = session_partition("tracked-client");
    ctrl.become_primary(partition, Epoch::new(1));

    let (_, write) = ctrl
        .stores()
        .create_session_replicated("tracked-client")
        .unwrap();

    let seq = ctrl
        .replicate_write_async(write, &[node2, node3], None)
        .await
        .unwrap();
    assert_eq!(seq, 1);

    let sent = ctrl.transport.sent_messages();
    assert_eq!(sent.len(), 2);
}

#[test]
fn subscribe_wildcard_broadcast_creates_64_writes() {
    let node1 = NodeId::validated(1).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    let partition = PartitionId::ZERO;
    let writes = ctrl
        .subscribe_wildcard_broadcast("sensors/+/temp", "client1", partition, 1)
        .unwrap();

    assert_eq!(writes.len(), NUM_PARTITIONS as usize);

    let mut partitions_covered: Vec<u16> = writes.iter().map(|w| w.partition.get()).collect();
    partitions_covered.sort_unstable();
    let expected: Vec<u16> = (0..NUM_PARTITIONS).collect();
    assert_eq!(partitions_covered, expected);
}

#[tokio::test]
async fn broadcast_wildcard_writes_sends_to_replicas() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();

    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    let partition = PartitionId::ZERO;
    let mut map = PartitionMap::default();
    map.set(
        partition,
        crate::cluster::PartitionAssignment {
            primary: Some(node1),
            replicas: vec![node2],
            epoch: Epoch::new(1),
        },
    );
    ctrl.update_partition_map(map);

    let writes = vec![ReplicationWrite::new(
        partition,
        Operation::Insert,
        Epoch::new(1),
        1,
        "test".to_string(),
        "1".to_string(),
        vec![],
    )];

    ctrl.broadcast_wildcard_writes(writes).await;

    let sent = ctrl.transport.sent_messages();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].0, node2);
}

#[tokio::test]
async fn create_session_quorum_signals_completion_on_acks() {
    use crate::cluster::protocol::ReplicationAck;
    use crate::cluster::transport::InboundMessage;

    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();

    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    let partition = session_partition("quorum-test-client");
    ctrl.become_primary(partition, Epoch::new(1));

    let mut map = PartitionMap::default();
    map.set(
        partition,
        crate::cluster::PartitionAssignment {
            primary: Some(node1),
            replicas: vec![node2],
            epoch: Epoch::new(1),
        },
    );
    ctrl.update_partition_map(map);

    let (session, mut rx) = ctrl
        .create_session_quorum("quorum-test-client")
        .await
        .unwrap();
    assert_eq!(session.client_id_str(), "quorum-test-client");

    assert!(rx.try_recv().is_err());

    let ack = ReplicationAck::ok(partition, Epoch::new(1), 1, node2);
    ctrl.handle_message(InboundMessage {
        from: node2,
        message: ClusterMessage::Ack(ack),
        received_at: 0,
    })
    .await;

    let result = rx.try_recv().unwrap();
    assert_eq!(result, QuorumResult::Success);
}

#[tokio::test]
async fn primary_has_local_data_after_replicate_write() {
    use crate::cluster::entity::SESSIONS;
    use bebytes::BeBytes;

    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();

    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    let partition = PartitionId::ZERO;
    ctrl.become_primary(partition, Epoch::new(1));

    let session_data = crate::cluster::session::SessionData::create("test-client", node1);
    let write = ReplicationWrite::new(
        partition,
        Operation::Insert,
        Epoch::new(1),
        0,
        SESSIONS.to_string(),
        "test-client".to_string(),
        session_data.to_be_bytes(),
    );

    let seq = ctrl.replicate_write(write, &[node2], 1).await.unwrap();
    assert_eq!(seq, 1);

    let session = ctrl.stores().sessions.get("test-client");
    assert!(
        session.is_some(),
        "Primary should have session locally after write"
    );
    assert_eq!(session.unwrap().client_id_str(), "test-client");

    assert!(
        ctrl.write_log().can_catchup(partition, 1),
        "Write log should contain the write for catchup"
    );
}

#[tokio::test]
async fn primary_has_local_data_after_replicate_write_async() {
    use crate::cluster::entity::SESSIONS;
    use bebytes::BeBytes;

    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();

    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    let partition = PartitionId::ZERO;
    ctrl.become_primary(partition, Epoch::new(1));

    let session_data = crate::cluster::session::SessionData::create("async-client", node1);
    let write = ReplicationWrite::new(
        partition,
        Operation::Insert,
        Epoch::new(1),
        0,
        SESSIONS.to_string(),
        "async-client".to_string(),
        session_data.to_be_bytes(),
    );

    let seq = ctrl
        .replicate_write_async(write, &[node2], None)
        .await
        .unwrap();
    assert_eq!(seq, 1);

    let session = ctrl.stores().sessions.get("async-client");
    assert!(
        session.is_some(),
        "Primary should have session locally after async write"
    );

    assert!(
        ctrl.write_log().can_catchup(partition, 1),
        "Write log should contain the async write for catchup"
    );
}

#[tokio::test]
async fn commit_offset_stores_locally_and_replicates() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    let consumer_id = "test-consumer";
    let consumer_partition = session_partition(consumer_id);
    ctrl.become_primary(consumer_partition, Epoch::new(1));
    ctrl.register_peer(node2);

    let mut map = PartitionMap::new();
    map.set(
        consumer_partition,
        crate::cluster::PartitionAssignment {
            primary: Some(node1),
            replicas: vec![node2],
            epoch: Epoch::new(1),
        },
    );
    ctrl.update_partition_map(map);

    let data_partition = PartitionId::new(5).unwrap();
    let offset = ctrl
        .commit_offset(consumer_id, data_partition, 12345, 1000)
        .await;

    assert_eq!(offset.sequence, 12345);
    assert_eq!(offset.timestamp, 1000);
    assert_eq!(offset.consumer_id_str(), consumer_id);

    let stored = ctrl.get_offset(consumer_id, data_partition);
    assert_eq!(stored, Some(12345));

    let sent = ctrl.transport.sent_messages();
    assert!(!sent.is_empty(), "Should have sent replication messages");
}

#[tokio::test]
async fn idempotency_check_and_commit() {
    use super::super::idempotency_store::IdempotencyCheck;

    let node1 = NodeId::validated(1).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    let partition = PartitionId::ZERO;
    ctrl.become_primary(partition, Epoch::new(1));

    let idem_key = "req-123";
    let entity = "users";
    let id = "user-1";
    let timestamp = 1000;

    let check1 = ctrl.check_idempotency(idem_key, partition, entity, id, timestamp);
    assert!(
        matches!(check1, Ok(IdempotencyCheck::Proceed)),
        "First check should return Proceed"
    );

    let check2 = ctrl.check_idempotency(idem_key, partition, entity, id, timestamp);
    assert!(
        check2.is_err(),
        "Second check while processing should error"
    );

    let response = b"success response";
    ctrl.commit_idempotency(partition, idem_key, response).await;

    let check3 = ctrl.check_idempotency(idem_key, partition, entity, id, timestamp + 100);
    match check3 {
        Ok(IdempotencyCheck::ReturnCached(cached)) => {
            assert_eq!(cached, response.to_vec());
        }
        other => panic!("Expected ReturnCached, got {other:?}"),
    }
}

#[test]
fn idempotency_rollback() {
    use super::super::idempotency_store::IdempotencyCheck;

    let node1 = NodeId::validated(1).unwrap();
    let transport = MockTransport::new(node1);
    let ctrl = create_test_controller(node1, transport);

    let partition = PartitionId::ZERO;

    let idem_key = "req-456";
    let entity = "orders";
    let id = "order-1";
    let timestamp = 2000;

    let check1 = ctrl.check_idempotency(idem_key, partition, entity, id, timestamp);
    assert!(matches!(check1, Ok(IdempotencyCheck::Proceed)));

    ctrl.rollback_idempotency(partition, idem_key);

    let check2 = ctrl.check_idempotency(idem_key, partition, entity, id, timestamp);
    assert!(
        matches!(check2, Ok(IdempotencyCheck::Proceed)),
        "After rollback, should be able to retry"
    );
}

#[tokio::test]
async fn db_create_stores_entity_and_replicates() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    let partition = crate::cluster::db::data_partition("users", "123");
    ctrl.become_primary(partition, Epoch::new(1));
    ctrl.register_peer(node2);

    let mut map = PartitionMap::new();
    map.set(
        partition,
        crate::cluster::PartitionAssignment {
            primary: Some(node1),
            replicas: vec![node2],
            epoch: Epoch::new(1),
        },
    );
    ctrl.update_partition_map(map);

    let result = ctrl
        .db_create("users", "123", b"{\"name\":\"Alice\"}", 1000)
        .await;
    assert!(result.is_ok());

    let entity = result.unwrap();
    assert_eq!(entity.entity_str(), "users");
    assert_eq!(entity.id_str(), "123");
    assert_eq!(entity.data, b"{\"name\":\"Alice\"}");

    let fetched = ctrl.db_get("users", "123");
    assert!(fetched.is_some());
    assert_eq!(fetched.unwrap().id_str(), "123");

    let sent = ctrl.transport.sent_messages();
    assert!(!sent.is_empty(), "Should replicate to peer");
}

#[tokio::test]
async fn db_create_fails_if_exists() {
    let node1 = NodeId::validated(1).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    let partition = crate::cluster::db::data_partition("users", "456");
    ctrl.become_primary(partition, Epoch::new(1));

    let result1 = ctrl.db_create("users", "456", b"{}", 1000).await;
    assert!(result1.is_ok());

    let result2 = ctrl.db_create("users", "456", b"{}", 2000).await;
    assert!(result2.is_err());
}

#[tokio::test]
async fn db_update_modifies_existing_entity() {
    let node1 = NodeId::validated(1).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    let partition = crate::cluster::db::data_partition("users", "789");
    ctrl.become_primary(partition, Epoch::new(1));

    ctrl.db_create("users", "789", b"{\"name\":\"Alice\"}", 1000)
        .await
        .unwrap();
    let updated = ctrl
        .db_update("users", "789", b"{\"name\":\"Bob\"}", 2000)
        .await
        .unwrap();

    assert_eq!(updated.data, b"{\"name\":\"Bob\"}");
    assert_eq!(updated.timestamp_ms, 2000);
}

#[tokio::test]
async fn db_delete_removes_entity() {
    let node1 = NodeId::validated(1).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    let partition = crate::cluster::db::data_partition("users", "del1");
    ctrl.become_primary(partition, Epoch::new(1));

    ctrl.db_create("users", "del1", b"{}", 1000).await.unwrap();
    assert!(ctrl.db_get("users", "del1").is_some());

    ctrl.db_delete("users", "del1").await.unwrap();
    assert!(ctrl.db_get("users", "del1").is_none());
}

#[tokio::test]
async fn db_list_returns_all_entities_of_type() {
    let node1 = NodeId::validated(1).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    for i in 0..3 {
        let id = format!("item{i}");
        let partition = crate::cluster::db::data_partition("items", &id);
        ctrl.become_primary(partition, Epoch::new(1));
    }

    ctrl.db_create("items", "item0", b"{}", 1000).await.unwrap();
    ctrl.db_create("items", "item1", b"{}", 1000).await.unwrap();
    ctrl.db_create("items", "item2", b"{}", 1000).await.unwrap();

    let items = ctrl.db_list("items");
    assert_eq!(items.len(), 3);
}

#[tokio::test]
async fn schema_register_broadcasts_to_alive_nodes() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();

    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    ctrl.heartbeat
        .receive_heartbeat(node2, &super::super::Heartbeat::create(node2, 0), 0);
    ctrl.heartbeat
        .receive_heartbeat(node3, &super::super::Heartbeat::create(node3, 0), 0);

    let schema = ctrl
        .schema_register("users", b"{\"fields\":[]}")
        .await
        .unwrap();
    assert_eq!(schema.entity_str(), "users");

    let sent = ctrl.transport.sent_messages();
    assert_eq!(sent.len(), 2);

    let targets: Vec<_> = sent.iter().map(|(n, _)| n.get()).collect();
    assert!(targets.contains(&2));
    assert!(targets.contains(&3));
}

#[tokio::test]
async fn schema_register_duplicate_fails() {
    let node1 = NodeId::validated(1).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    ctrl.schema_register("users", b"{}").await.unwrap();
    let result = ctrl.schema_register("users", b"{}").await;

    assert!(matches!(
        result,
        Err(super::super::db::SchemaStoreError::AlreadyExists)
    ));
}

#[tokio::test]
async fn schema_update_increments_version() {
    let node1 = NodeId::validated(1).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    ctrl.schema_register("users", b"{v1}").await.unwrap();
    let updated = ctrl.schema_update("users", b"{v2}").await.unwrap();

    assert_eq!(updated.schema_version, 2);
}

#[tokio::test]
async fn schema_get_and_list() {
    let node1 = NodeId::validated(1).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    ctrl.schema_register("users", b"{}").await.unwrap();
    ctrl.schema_register("orders", b"{}").await.unwrap();

    assert!(ctrl.schema_get("users").is_some());
    assert!(ctrl.schema_get("products").is_none());
    assert_eq!(ctrl.schema_list().len(), 2);
}

#[tokio::test]
async fn schema_is_valid_for_write_checks_active_state() {
    let node1 = NodeId::validated(1).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    assert!(!ctrl.schema_is_valid_for_write("users"));
    ctrl.schema_register("users", b"{}").await.unwrap();
    assert!(ctrl.schema_is_valid_for_write("users"));
}

fn setup_all_partitions_primary(ctrl: &mut NodeController<MockTransport>, node: NodeId) {
    let mut map = PartitionMap::new();
    for partition_id in 0..NUM_PARTITIONS {
        if let Some(p) = PartitionId::new(partition_id) {
            ctrl.become_primary(p, Epoch::new(1));
            map.set(
                p,
                crate::cluster::PartitionAssignment {
                    primary: Some(node),
                    replicas: vec![],
                    epoch: Epoch::new(1),
                },
            );
        }
    }
    ctrl.update_partition_map(map);
}

async fn setup_unique_entity_with_constraint(
    ctrl: &mut NodeController<MockTransport>,
    id: &str,
    data: &[u8],
    timestamp: u64,
) {
    let partition = crate::cluster::db::data_partition("users", id);
    ctrl.db_create("users", id, data, timestamp).await.unwrap();

    let request_id = uuid::Uuid::new_v4().to_string();
    let entity_data: serde_json::Value = serde_json::from_slice(data).unwrap();
    ctrl.check_unique_constraints("users", id, &entity_data, partition, &request_id, timestamp)
        .await
        .unwrap();
    ctrl.commit_unique_constraints("users", id, &entity_data, partition, &request_id, timestamp)
        .await;
}

#[tokio::test]
async fn db_update_enforces_unique_constraint() {
    let node1 = NodeId::validated(1).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    let constraint =
        crate::cluster::db::ClusterConstraint::unique("users", "email_unique", "email");
    ctrl.constraint_add(&constraint).await.unwrap();
    setup_all_partitions_primary(&mut ctrl, node1);

    setup_unique_entity_with_constraint(
        &mut ctrl,
        "user-a",
        br#"{"email":"alice@example.com","name":"Alice"}"#,
        1000,
    )
    .await;
    setup_unique_entity_with_constraint(
        &mut ctrl,
        "user-b",
        br#"{"email":"bob@example.com","name":"Bob"}"#,
        1001,
    )
    .await;

    let result = ctrl
        .handle_json_update_local_for_test("users", "user-b", br#"{"email":"alice@example.com"}"#)
        .await;
    let response: serde_json::Value = serde_json::from_slice(&result).unwrap();
    assert_eq!(response["status"], "error");
    assert_eq!(response["code"], 409);
}

#[tokio::test]
async fn db_update_allows_same_value() {
    let node1 = NodeId::validated(1).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    let constraint =
        crate::cluster::db::ClusterConstraint::unique("users", "email_unique", "email");
    ctrl.constraint_add(&constraint).await.unwrap();
    setup_all_partitions_primary(&mut ctrl, node1);

    setup_unique_entity_with_constraint(
        &mut ctrl,
        "user-1",
        br#"{"email":"alice@example.com","name":"Alice"}"#,
        1000,
    )
    .await;

    let result = ctrl
        .handle_json_update_local_for_test("users", "user-1", br#"{"name":"Alice B."}"#)
        .await;
    let response: serde_json::Value = serde_json::from_slice(&result).unwrap();
    assert_eq!(response["status"], "ok");
}

#[tokio::test]
async fn db_update_releases_old_unique_value() {
    let node1 = NodeId::validated(1).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    let constraint =
        crate::cluster::db::ClusterConstraint::unique("users", "email_unique", "email");
    ctrl.constraint_add(&constraint).await.unwrap();
    setup_all_partitions_primary(&mut ctrl, node1);

    setup_unique_entity_with_constraint(
        &mut ctrl,
        "user-a",
        br#"{"email":"alice@example.com","name":"Alice"}"#,
        1000,
    )
    .await;

    let result = ctrl
        .handle_json_update_local_for_test(
            "users",
            "user-a",
            br#"{"email":"newalice@example.com"}"#,
        )
        .await;
    let response: serde_json::Value = serde_json::from_slice(&result).unwrap();
    assert_eq!(response["status"], "ok", "update failed: {response}");

    let partition_b = crate::cluster::db::data_partition("users", "user-b");
    let request_id_b = uuid::Uuid::new_v4().to_string();
    let create_data_b: serde_json::Value =
        serde_json::from_slice(br#"{"email":"alice@example.com"}"#).unwrap();
    let check_result = ctrl
        .check_unique_constraints(
            "users",
            "user-b",
            &create_data_b,
            partition_b,
            &request_id_b,
            1002,
        )
        .await;
    assert!(
        check_result.is_ok(),
        "old unique value should be available after update changed it"
    );
}

fn find_two_ids_on_different_partitions(
    entity: &str,
) -> (String, PartitionId, String, PartitionId) {
    let first_id = format!("{entity}-0");
    let first_part = crate::cluster::db::data_partition(entity, &first_id);
    for i in 1..256u32 {
        let id = format!("{entity}-{i}");
        let p = crate::cluster::db::data_partition(entity, &id);
        if p != first_part {
            return (first_id, first_part, id, p);
        }
    }
    panic!("could not find two IDs on different partitions");
}

fn make_all_primary(ctrl: &mut NodeController<MockTransport>, node: NodeId) {
    let mut map = PartitionMap::new();
    for partition_id in 0..NUM_PARTITIONS {
        if let Some(p) = PartitionId::new(partition_id) {
            ctrl.become_primary(p, Epoch::new(1));
            map.set(
                p,
                crate::cluster::PartitionAssignment {
                    primary: Some(node),
                    replicas: vec![],
                    epoch: Epoch::new(1),
                },
            );
        }
    }
    ctrl.update_partition_map(map);
}

fn setup_all_primary_except(
    ctrl: &mut NodeController<MockTransport>,
    node: NodeId,
    excluded: PartitionId,
    excluded_primary: NodeId,
) {
    let mut map = PartitionMap::new();
    for partition_id in 0..NUM_PARTITIONS {
        if let Some(p) = PartitionId::new(partition_id) {
            if p == excluded {
                map.set(
                    p,
                    crate::cluster::PartitionAssignment {
                        primary: Some(excluded_primary),
                        replicas: vec![node],
                        epoch: Epoch::new(1),
                    },
                );
            } else {
                ctrl.become_primary(p, Epoch::new(1));
                map.set(
                    p,
                    crate::cluster::PartitionAssignment {
                        primary: Some(node),
                        replicas: vec![],
                        epoch: Epoch::new(1),
                    },
                );
            }
        }
    }
    ctrl.update_partition_map(map);
}

#[tokio::test]
async fn db_list_primary_only_excludes_replica_data() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    let (primary_id, _primary_part, replica_id, replica_part) =
        find_two_ids_on_different_partitions("items");

    put_record(&ctrl.stores.db_data, "items", &primary_id, b"{}", 1000);
    put_record(&ctrl.stores.db_data, "items", &replica_id, b"{}", 1000);

    assert_eq!(ctrl.db_list("items").len(), 2);

    setup_all_primary_except(&mut ctrl, node1, replica_part, node2);

    let primary_only = ctrl.db_list_primary_only("items");
    assert_eq!(primary_only.len(), 1);
    assert_eq!(primary_only[0].id_str(), primary_id);
}

#[tokio::test]
async fn fk_reverse_lookup_ignores_stale_replica_data() {
    use crate::cluster::db::{ClusterConstraint, OnDeleteAction};

    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    let (_, _, replica_post_id, replica_part) = find_two_ids_on_different_partitions("posts");

    setup_all_primary_except(&mut ctrl, node1, replica_part, node2);

    let fk = ClusterConstraint::foreign_key(
        "posts",
        "posts_author_fk",
        "author_id",
        "users",
        "id",
        OnDeleteAction::Restrict,
    );
    ctrl.constraint_add(&fk).await.unwrap();

    let user_data = serde_json::to_vec(&serde_json::json!({"name": "Alice"})).unwrap();
    ctrl.db_create("users", "u1", &user_data, 1000)
        .await
        .unwrap();

    let post_data = serde_json::json!({
        "title": "Stale Replica Post",
        "author_id": "u1",
        "id": replica_post_id
    });
    let post_bytes = serde_json::to_vec(&post_data).unwrap();
    put_record(
        &ctrl.stores.db_data,
        "posts",
        &replica_post_id,
        &post_bytes,
        1000,
    );

    assert_eq!(ctrl.db_list("posts").len(), 1);

    let (local_results, pending) = ctrl
        .start_fk_reverse_lookup("users", "u1", None)
        .await
        .unwrap();
    assert!(pending.is_empty());
    assert!(
        local_results.is_empty(),
        "stale replica data should not appear in FK reverse lookup"
    );
}

#[tokio::test]
async fn fk_reverse_lookup_finds_primary_partition_data() {
    use crate::cluster::db::{ClusterConstraint, OnDeleteAction};

    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    let (primary_post_id, _, _, replica_part) = find_two_ids_on_different_partitions("posts");

    setup_all_primary_except(&mut ctrl, node1, replica_part, node2);

    let fk = ClusterConstraint::foreign_key(
        "posts",
        "posts_author_fk",
        "author_id",
        "users",
        "id",
        OnDeleteAction::Cascade,
    );
    ctrl.constraint_add(&fk).await.unwrap();

    let user_data = serde_json::to_vec(&serde_json::json!({"name": "Alice"})).unwrap();
    ctrl.db_create("users", "u1", &user_data, 1000)
        .await
        .unwrap();

    let post_data = serde_json::json!({
        "title": "Primary Post",
        "author_id": "u1",
        "id": primary_post_id
    });
    let post_bytes = serde_json::to_vec(&post_data).unwrap();
    ctrl.db_create("posts", &primary_post_id, &post_bytes, 1000)
        .await
        .unwrap();

    let (local_results, pending) = ctrl
        .start_fk_reverse_lookup("users", "u1", None)
        .await
        .unwrap();
    assert!(pending.is_empty());
    assert_eq!(local_results.len(), 1);
    assert_eq!(local_results[0].referencing_ids.len(), 1);
    assert_eq!(local_results[0].referencing_ids[0], primary_post_id);
}

#[tokio::test]
async fn fk_reverse_lookup_restrict_blocks_only_on_primary_data() {
    use crate::cluster::db::{ClusterConstraint, OnDeleteAction};

    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    let (primary_post_id, _, replica_post_id, replica_part) =
        find_two_ids_on_different_partitions("posts");

    setup_all_primary_except(&mut ctrl, node1, replica_part, node2);

    let fk = ClusterConstraint::foreign_key(
        "posts",
        "posts_author_fk",
        "author_id",
        "users",
        "id",
        OnDeleteAction::Restrict,
    );
    ctrl.constraint_add(&fk).await.unwrap();

    let user_data = serde_json::to_vec(&serde_json::json!({"name": "Alice"})).unwrap();
    ctrl.db_create("users", "u1", &user_data, 1000)
        .await
        .unwrap();

    let post_bytes = |id: &str| {
        serde_json::to_vec(&serde_json::json!({
            "title": "Post",
            "author_id": "u1",
            "id": id
        }))
        .unwrap()
    };

    ctrl.db_create(
        "posts",
        &primary_post_id,
        &post_bytes(&primary_post_id),
        1000,
    )
    .await
    .unwrap();
    put_record(
        &ctrl.stores.db_data,
        "posts",
        &replica_post_id,
        &post_bytes(&replica_post_id),
        1000,
    );

    assert_eq!(ctrl.db_list("posts").len(), 2);

    let result = ctrl.start_fk_reverse_lookup("users", "u1", None).await;
    match result {
        Err(ref msg) => assert!(
            msg.contains("prevents deletion"),
            "error should mention delete prevention: {msg}"
        ),
        Ok(_) => panic!("RESTRICT should block when primary data has referencing records"),
    }
}

async fn setup_owned_posts_referencing_user(
    ctrl: &mut NodeController<MockTransport>,
    node: NodeId,
) {
    use crate::cluster::db::{ClusterConstraint, OnDeleteAction};

    make_all_primary(ctrl, node);
    ctrl.set_ownership(Arc::new(
        mqdb_core::types::OwnershipConfig::parse("posts=owner").unwrap(),
    ));

    let fk = ClusterConstraint::foreign_key(
        "posts",
        "posts_author_fk",
        "author_id",
        "users",
        "id",
        OnDeleteAction::Cascade,
    );
    ctrl.constraint_add(&fk).await.unwrap();

    let user = serde_json::to_vec(&serde_json::json!({"name": "Alice"})).unwrap();
    ctrl.db_create("users", "u1", &user, 1000).await.unwrap();

    let alice_post = serde_json::to_vec(&serde_json::json!({
        "author_id": "u1", "owner": "alice", "id": "post-alice"
    }))
    .unwrap();
    ctrl.db_create("posts", "post-alice", &alice_post, 1000)
        .await
        .unwrap();
    let bob_post = serde_json::to_vec(&serde_json::json!({
        "author_id": "u1", "owner": "bob", "id": "post-bob"
    }))
    .unwrap();
    ctrl.db_create("posts", "post-bob", &bob_post, 1000)
        .await
        .unwrap();
}

fn sent_fk_lookup_response(
    ctrl: &NodeController<MockTransport>,
    to: NodeId,
) -> crate::cluster::protocol::FkReverseLookupResponse {
    ctrl.transport
        .sent_messages()
        .into_iter()
        .find_map(|(dest, msg)| match msg {
            ClusterMessage::FkReverseLookupResponse(r) if dest == to => Some(r),
            _ => None,
        })
        .expect("FK reverse lookup response sent to requester")
}

#[tokio::test]
async fn handle_fk_reverse_lookup_partitions_cross_owned_by_sender() {
    use crate::cluster::protocol::FkReverseLookupRequest;

    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);
    setup_owned_posts_referencing_user(&mut ctrl, node1).await;

    let req = FkReverseLookupRequest::create(1, "posts", "author_id", "u1", "users", "alice");
    ctrl.handle_fk_reverse_lookup_request(node2, &req).await;

    let resp = sent_fk_lookup_response(&ctrl, node2);
    assert_eq!(
        resp.referencing_ids(),
        vec!["post-alice".to_string()],
        "alice's own post is owned and cascade-deletable"
    );
    assert_eq!(
        resp.cross_owned_ids(),
        vec!["post-bob".to_string()],
        "bob's post is cross-owned and must be set-null'd, not deleted"
    );
}

#[tokio::test]
async fn handle_fk_reverse_lookup_blind_sender_returns_all_owned() {
    use crate::cluster::protocol::FkReverseLookupRequest;

    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);
    setup_owned_posts_referencing_user(&mut ctrl, node1).await;

    let req = FkReverseLookupRequest::create(2, "posts", "author_id", "u1", "users", "");
    ctrl.handle_fk_reverse_lookup_request(node2, &req).await;

    let resp = sent_fk_lookup_response(&ctrl, node2);
    let mut owned = resp.referencing_ids();
    owned.sort();
    assert_eq!(
        owned,
        vec!["post-alice".to_string(), "post-bob".to_string()],
        "a blind (empty) sender treats every ref as owned"
    );
    assert!(resp.cross_owned_ids().is_empty());
}

#[tokio::test]
async fn cascade_set_nulls_cross_owned_ref_with_no_owned_siblings() {
    use super::db_ops::CascadeSideEffect;
    use super::fk::{FkReverseLookupResult, filter_and_extract_cascade};
    use crate::cluster::db::OnDeleteAction;

    let node1 = NodeId::validated(1).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);
    setup_owned_posts_referencing_user(&mut ctrl, node1).await;

    let reply = FkReverseLookupResult {
        constraint_name: "posts_author_fk".to_string(),
        source_entity: "posts".to_string(),
        source_field: "author_id".to_string(),
        on_delete: OnDeleteAction::Cascade,
        referencing_ids: Vec::new(),
        cross_owned_ids: vec!["post-bob".to_string()],
        target_id: "u1".to_string(),
    };

    let mut visited = std::collections::HashSet::new();
    visited.insert(("users".to_string(), "u1".to_string()));
    let (filtered, queue) = filter_and_extract_cascade(vec![reply], &mut visited);
    assert!(queue.is_empty());
    assert_eq!(
        filtered.len(),
        1,
        "a cross-owned-only cascade result must survive filtering"
    );

    let effects = ctrl.prepare_fk_side_effects(&filtered).unwrap();
    assert!(
        effects.iter().any(|e| matches!(
            e,
            CascadeSideEffect::LocalSetNull { entity, id, .. } if entity == "posts" && id == "post-bob"
        )),
        "post-bob is cross-owned and must be set-null'd, not silently dropped"
    );
    assert!(
        !effects.iter().any(|e| matches!(
            e,
            CascadeSideEffect::LocalDelete { id, .. } if id == "post-bob"
        )),
        "post-bob must never be deleted"
    );
}

#[tokio::test]
async fn cross_owned_set_null_blocked_on_typed_field() {
    use super::fk::{FkReverseLookupResult, filter_and_extract_cascade};
    use crate::cluster::db::OnDeleteAction;

    let node1 = NodeId::validated(1).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);
    setup_owned_posts_referencing_user(&mut ctrl, node1).await;

    let schema = mqdb_core::schema::Schema::new("posts").add_field(
        mqdb_core::schema::FieldDefinition::new("author_id", mqdb_core::schema::FieldType::String),
    );
    ctrl.schema_register("posts", &serde_json::to_vec(&schema).unwrap())
        .await
        .unwrap();

    let reply = FkReverseLookupResult {
        constraint_name: "posts_author_fk".to_string(),
        source_entity: "posts".to_string(),
        source_field: "author_id".to_string(),
        on_delete: OnDeleteAction::Cascade,
        referencing_ids: Vec::new(),
        cross_owned_ids: vec!["post-bob".to_string()],
        target_id: "u1".to_string(),
    };

    let mut visited = std::collections::HashSet::new();
    visited.insert(("users".to_string(), "u1".to_string()));
    let (filtered, queue) = filter_and_extract_cascade(vec![reply], &mut visited);
    assert!(queue.is_empty());

    assert!(
        ctrl.prepare_fk_side_effects(&filtered).is_err(),
        "cross-owned set-null on a typed field must be blocked"
    );
}

#[tokio::test]
async fn local_cascade_set_nulls_cross_owned_grandchild() {
    use super::db_ops::CascadeSideEffect;
    use crate::cluster::db::{ClusterConstraint, OnDeleteAction};

    let node1 = NodeId::validated(1).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);
    make_all_primary(&mut ctrl, node1);
    ctrl.set_ownership(Arc::new(
        mqdb_core::types::OwnershipConfig::parse("posts=owner,comments=owner").unwrap(),
    ));

    let fk_posts = ClusterConstraint::foreign_key(
        "posts",
        "posts_author_fk",
        "author_id",
        "users",
        "id",
        OnDeleteAction::Cascade,
    );
    ctrl.constraint_add(&fk_posts).await.unwrap();
    let fk_comments = ClusterConstraint::foreign_key(
        "comments",
        "comments_post_fk",
        "post_id",
        "posts",
        "id",
        OnDeleteAction::Cascade,
    );
    ctrl.constraint_add(&fk_comments).await.unwrap();

    let user = serde_json::to_vec(&serde_json::json!({"name": "Alice"})).unwrap();
    ctrl.db_create("users", "u1", &user, 1000).await.unwrap();
    let post = serde_json::to_vec(&serde_json::json!({
        "author_id": "u1", "owner": "alice", "id": "post-alice"
    }))
    .unwrap();
    ctrl.db_create("posts", "post-alice", &post, 1000)
        .await
        .unwrap();
    let owned_comment = serde_json::to_vec(&serde_json::json!({
        "post_id": "post-alice", "owner": "alice", "id": "comment-alice"
    }))
    .unwrap();
    ctrl.db_create("comments", "comment-alice", &owned_comment, 1000)
        .await
        .unwrap();
    let cross_comment = serde_json::to_vec(&serde_json::json!({
        "post_id": "post-alice", "owner": "carol", "id": "comment-carol"
    }))
    .unwrap();
    ctrl.db_create("comments", "comment-carol", &cross_comment, 1000)
        .await
        .unwrap();

    let (local_results, pending_remote) = ctrl
        .start_fk_reverse_lookup("users", "u1", Some("alice"))
        .await
        .unwrap();
    assert!(
        pending_remote.is_empty(),
        "single-node: collect_local_cascade path"
    );

    let all = ctrl
        .collect_local_cascade("users", "u1", local_results, Some("alice"))
        .unwrap();
    let effects = ctrl.prepare_fk_side_effects(&all).unwrap();
    let has = |pred: &dyn Fn(&CascadeSideEffect) -> bool| effects.iter().any(pred);

    assert!(
        has(&|e| matches!(
            e,
            CascadeSideEffect::LocalSetNull { entity, id, .. } if entity == "comments" && id == "comment-carol"
        )) && !has(&|e| matches!(
            e,
            CascadeSideEffect::LocalDelete { entity, id, .. } if entity == "comments" && id == "comment-carol"
        )),
        "carol's cross-owned grandchild must be set-null'd, not deleted, at cascade depth 2"
    );
    assert!(
        has(&|e| matches!(
            e,
            CascadeSideEffect::LocalDelete { entity, id, .. } if entity == "comments" && id == "comment-alice"
        )),
        "alice's own grandchild is still cascade-deleted"
    );
    assert!(
        has(&|e| matches!(
            e,
            CascadeSideEffect::LocalDelete { entity, id, .. } if entity == "posts" && id == "post-alice"
        )),
        "alice's own post is still cascade-deleted"
    );
}

async fn setup_post_with_owned_and_cross_owned_comments(
    ctrl: &mut NodeController<MockTransport>,
    node: NodeId,
) {
    use crate::cluster::db::{ClusterConstraint, OnDeleteAction};

    make_all_primary(ctrl, node);
    ctrl.set_ownership(Arc::new(
        mqdb_core::types::OwnershipConfig::parse("posts=owner,comments=owner").unwrap(),
    ));

    let fk_comments = ClusterConstraint::foreign_key(
        "comments",
        "comments_post_fk",
        "post_id",
        "posts",
        "id",
        OnDeleteAction::Cascade,
    );
    ctrl.constraint_add(&fk_comments).await.unwrap();

    let post = serde_json::to_vec(&serde_json::json!({
        "owner": "alice", "id": "post-alice"
    }))
    .unwrap();
    ctrl.db_create("posts", "post-alice", &post, 1000)
        .await
        .unwrap();
    let owned_comment = serde_json::to_vec(&serde_json::json!({
        "post_id": "post-alice", "owner": "alice", "id": "comment-alice"
    }))
    .unwrap();
    ctrl.db_create("comments", "comment-alice", &owned_comment, 1000)
        .await
        .unwrap();
    let cross_comment = serde_json::to_vec(&serde_json::json!({
        "post_id": "post-alice", "owner": "carol", "id": "comment-carol"
    }))
    .unwrap();
    ctrl.db_create("comments", "comment-carol", &cross_comment, 1000)
        .await
        .unwrap();
}

fn delete_post_request(sender: Option<&str>) -> crate::cluster::protocol::JsonDbRequest {
    use crate::cluster::protocol::{JsonDbOp, JsonDbRequest};
    JsonDbRequest {
        request_id: 1,
        op: JsonDbOp::Delete,
        entity: "posts".to_string(),
        id: Some("post-alice".to_string()),
        payload: Vec::new(),
        response_topic: String::new(),
        correlation_data: None,
        sender: sender.map(String::from),
        client_id: None,
    }
}

#[tokio::test]
async fn owner_aware_delete_request_set_nulls_cross_owned_comment() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);
    setup_post_with_owned_and_cross_owned_comments(&mut ctrl, node1).await;

    let partition = crate::cluster::db::data_partition("posts", "post-alice");
    let req = delete_post_request(Some("alice"));
    ctrl.handle_json_db_request(node2, partition, &req).await;

    assert!(
        ctrl.db_get("comments", "comment-carol").is_some(),
        "carol's cross-owned comment must survive an owner-aware delete"
    );
    assert!(
        ctrl.db_get("comments", "comment-alice").is_none(),
        "alice's own comment is cascade-deleted"
    );
}

#[tokio::test]
async fn cascade_delete_fan_out_propagates_sender_to_remote_primary() {
    use crate::cluster::JsonDbOp;

    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    let partition = crate::cluster::db::data_partition("comments", "comment-carol");
    let mut map = PartitionMap::new();
    map.set(
        partition,
        crate::cluster::PartitionAssignment {
            primary: Some(node2),
            replicas: vec![],
            epoch: Epoch::new(1),
        },
    );
    ctrl.update_partition_map(map);

    let ack = ctrl
        .send_cascade_request(
            partition,
            JsonDbOp::Delete,
            "comments",
            "comment-carol",
            &[],
            EventOrigin::new(Some("alice"), None),
        )
        .await;
    assert!(ack.is_some());

    let req = ctrl
        .transport
        .sent_messages()
        .into_iter()
        .find_map(|(_, msg)| match msg {
            ClusterMessage::JsonDbRequest { request, .. } => Some(request),
            _ => None,
        })
        .expect("cascade delete request sent to remote primary");
    assert_eq!(
        req.sender.as_deref(),
        Some("alice"),
        "cascade delete fan-out must carry the deleter identity so the remote re-cascade stays \
         owner-aware instead of blindly deleting cross-owned descendants"
    );
}

#[tokio::test]
async fn handle_fk_reverse_lookup_classifies_ownerless_ref_as_cross_owned() {
    use crate::cluster::protocol::FkReverseLookupRequest;

    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);
    setup_owned_posts_referencing_user(&mut ctrl, node1).await;

    let orphan_post = serde_json::to_vec(&serde_json::json!({
        "author_id": "u1", "id": "post-orphan"
    }))
    .unwrap();
    ctrl.db_create("posts", "post-orphan", &orphan_post, 1000)
        .await
        .unwrap();

    let req = FkReverseLookupRequest::create(1, "posts", "author_id", "u1", "users", "alice");
    ctrl.handle_fk_reverse_lookup_request(node2, &req).await;

    let resp = sent_fk_lookup_response(&ctrl, node2);
    assert_eq!(
        resp.referencing_ids(),
        vec!["post-alice".to_string()],
        "only alice's own post is owned/cascade-deletable by alice"
    );
    let mut cross_owned = resp.cross_owned_ids();
    cross_owned.sort();
    assert_eq!(
        cross_owned,
        vec!["post-bob".to_string(), "post-orphan".to_string()],
        "an ownerless post must be cross-owned (set-null'd) just like bob's, matching the \
         direct-delete path which forbids a non-admin from deleting an ownerless record"
    );
}

#[tokio::test]
async fn owner_aware_delete_request_set_nulls_ownerless_comment() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);
    setup_post_with_owned_and_cross_owned_comments(&mut ctrl, node1).await;

    let orphan_comment = serde_json::to_vec(&serde_json::json!({
        "post_id": "post-alice", "id": "comment-orphan"
    }))
    .unwrap();
    ctrl.db_create("comments", "comment-orphan", &orphan_comment, 1000)
        .await
        .unwrap();

    let partition = crate::cluster::db::data_partition("posts", "post-alice");
    let req = delete_post_request(Some("alice"));
    ctrl.handle_json_db_request(node2, partition, &req).await;

    assert!(
        ctrl.db_get("comments", "comment-orphan").is_some(),
        "an ownerless comment must survive an owner-aware cascade delete, not be hard-deleted"
    );
    assert!(
        ctrl.db_get("comments", "comment-alice").is_none(),
        "alice's own comment is still cascade-deleted"
    );
}

#[tokio::test]
async fn circular_fk_cascade_terminates() {
    use crate::cluster::db::{ClusterConstraint, OnDeleteAction};

    let node1 = NodeId::validated(1).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);
    make_all_primary(&mut ctrl, node1);

    let fk_a_to_b = ClusterConstraint::foreign_key(
        "alpha",
        "alpha_beta_fk",
        "beta_id",
        "beta",
        "id",
        OnDeleteAction::Cascade,
    );
    let fk_b_to_a = ClusterConstraint::foreign_key(
        "beta",
        "beta_alpha_fk",
        "alpha_id",
        "alpha",
        "id",
        OnDeleteAction::Cascade,
    );
    ctrl.constraint_add(&fk_a_to_b).await.unwrap();
    ctrl.constraint_add(&fk_b_to_a).await.unwrap();

    let a_data = serde_json::to_vec(&serde_json::json!({"beta_id": "b1"})).unwrap();
    let b_data = serde_json::to_vec(&serde_json::json!({"alpha_id": "a1"})).unwrap();
    ctrl.db_create("alpha", "a1", &a_data, 1000).await.unwrap();
    ctrl.db_create("beta", "b1", &b_data, 1001).await.unwrap();

    let (local, pending) = ctrl
        .start_fk_reverse_lookup("alpha", "a1", None)
        .await
        .unwrap();
    assert!(pending.is_empty());

    let result = ctrl.collect_local_cascade("alpha", "a1", local, None);
    assert!(
        result.is_ok(),
        "circular cascade should terminate via visited set"
    );

    let results = result.unwrap();
    let cascade_ids: Vec<_> = results.iter().flat_map(|r| &r.referencing_ids).collect();
    assert!(
        cascade_ids.contains(&&"b1".to_string()),
        "should find the referencing beta record"
    );
    assert!(
        cascade_ids.len() <= 2,
        "visited set should prevent infinite expansion: got {cascade_ids:?}"
    );
}

#[tokio::test]
async fn three_way_circular_fk_cascade_terminates() {
    use crate::cluster::db::{ClusterConstraint, OnDeleteAction};

    let node1 = NodeId::validated(1).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);
    make_all_primary(&mut ctrl, node1);

    let fk_a_b = ClusterConstraint::foreign_key(
        "aaa",
        "aaa_bbb_fk",
        "bbb_id",
        "bbb",
        "id",
        OnDeleteAction::Cascade,
    );
    let fk_b_c = ClusterConstraint::foreign_key(
        "bbb",
        "bbb_ccc_fk",
        "ccc_id",
        "ccc",
        "id",
        OnDeleteAction::Cascade,
    );
    let fk_c_a = ClusterConstraint::foreign_key(
        "ccc",
        "ccc_aaa_fk",
        "aaa_id",
        "aaa",
        "id",
        OnDeleteAction::Cascade,
    );
    ctrl.constraint_add(&fk_a_b).await.unwrap();
    ctrl.constraint_add(&fk_b_c).await.unwrap();
    ctrl.constraint_add(&fk_c_a).await.unwrap();

    let a_data = serde_json::to_vec(&serde_json::json!({"bbb_id": "b1"})).unwrap();
    let b_data = serde_json::to_vec(&serde_json::json!({"ccc_id": "c1"})).unwrap();
    let c_data = serde_json::to_vec(&serde_json::json!({"aaa_id": "a1"})).unwrap();
    ctrl.db_create("aaa", "a1", &a_data, 1000).await.unwrap();
    ctrl.db_create("bbb", "b1", &b_data, 1001).await.unwrap();
    ctrl.db_create("ccc", "c1", &c_data, 1002).await.unwrap();

    let (local, pending) = ctrl
        .start_fk_reverse_lookup("aaa", "a1", None)
        .await
        .unwrap();
    assert!(pending.is_empty());
    let result = ctrl.collect_local_cascade("aaa", "a1", local, None);
    assert!(
        result.is_ok(),
        "3-way circular cascade should terminate: {result:?}"
    );

    let results = result.unwrap();
    let all_ids: Vec<_> = results
        .iter()
        .map(|r| (&r.source_entity, &r.referencing_ids))
        .collect();
    assert!(
        !all_ids.is_empty(),
        "should find referencing records in the cycle"
    );
}

#[tokio::test]
async fn unlinked_nodes_names_only_members_missing_a_link() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();

    let transport = MockTransport::new(node1);
    *transport.linked.lock().unwrap() = Some(vec![node2]);
    let mut ctrl = create_test_controller(node1, transport);
    ctrl.seed_unique_voters([node1, node2, node3].into_iter().collect());

    assert_eq!(
        ctrl.unlinked_nodes().await,
        vec![node3],
        "node3 is a known member with no direct connection, node2 is linked, self is excluded"
    );
}

#[tokio::test]
async fn unlinked_nodes_is_empty_for_a_broker_mediated_transport() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();

    let mut ctrl = create_test_controller(node1, MockTransport::new(node1));
    ctrl.seed_unique_voters([node1, node2, node3].into_iter().collect());

    assert_eq!(
        ctrl.known_members(),
        vec![node2, node3],
        "voter gossip tells a node which members exist, excluding itself"
    );

    assert!(
        ctrl.unlinked_nodes().await.is_empty(),
        "a broker-mediated transport reports no direct peers, so nothing can be called unlinked"
    );
}

#[tokio::test]
async fn unlinked_nodes_is_empty_without_voter_gossip() {
    let node1 = NodeId::validated(1).unwrap();
    let ctrl = create_test_controller(node1, MockTransport::new(node1));

    assert!(ctrl.known_members().is_empty());
    assert!(ctrl.unlinked_nodes().await.is_empty());
}

#[tokio::test]
async fn forwarded_write_without_a_response_topic_keeps_no_vault_decrypt() {
    use crate::cluster::protocol::JsonDbOp;

    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let mut ctrl = create_test_controller(node1, MockTransport::new(node1));
    ctrl.set_ownership(Arc::new(
        mqdb_core::types::OwnershipConfig::parse("notes=owner").unwrap(),
    ));
    let keys = Arc::new(VaultKeyStore::new());
    keys.set("alice", zeroize::Zeroizing::new(vec![7u8; 32]));
    ctrl.set_vault_key_store(keys);

    let partition = crate::cluster::db::data_partition("notes", "n1");
    let mut map = PartitionMap::new();
    map.set(
        partition,
        crate::cluster::PartitionAssignment {
            primary: Some(node2),
            replicas: vec![],
            epoch: Epoch::new(1),
        },
    );
    ctrl.update_partition_map(map);

    let update = br#"{"n":6}"#;
    assert!(
        ctrl.forward_json_db_request(
            partition,
            &ForwardRequest {
                op: JsonDbOp::Update,
                entity: "notes",
                id: Some("n1"),
                payload: update,
                response_topic: "",
                correlation_data: None,
            },
            EventOrigin::new(Some("alice"), None),
        )
        .await
    );
    assert!(ctrl.pending_vault_decrypts.is_empty());

    assert!(
        ctrl.forward_json_db_request(
            partition,
            &ForwardRequest {
                op: JsonDbOp::Update,
                entity: "notes",
                id: Some("n1"),
                payload: update,
                response_topic: "resp/t",
                correlation_data: None,
            },
            EventOrigin::new(Some("alice"), None),
        )
        .await
    );
    assert_eq!(ctrl.pending_vault_decrypts.len(), 1);
}

fn three_node_map() -> PartitionMap {
    let mut map = PartitionMap::new();
    map.set(
        PartitionId::ZERO,
        crate::cluster::PartitionAssignment {
            primary: NodeId::validated(1),
            replicas: vec![NodeId::validated(2).unwrap(), NodeId::validated(3).unwrap()],
            epoch: Epoch::new(1),
        },
    );
    map
}

struct RunningProcessor {
    tick: flume::Sender<u64>,
    inbox: flume::Sender<InboundMessage>,
    batches: flume::Receiver<crate::cluster::ProcessingBatch>,
}

fn spawn_message_processor(node: NodeId, map: PartitionMap) -> RunningProcessor {
    let tx_raft_messages = flume::unbounded().0;
    let tx_raft_events = flume::unbounded().0;
    let tx_main_queue = flume::unbounded().0;
    let (inbox, rx_inbox) = flume::unbounded();
    let (tick, rx_tick) = flume::unbounded();
    let (tx_batch, batches) = flume::unbounded();
    let mut processor = crate::cluster::MessageProcessor::new(
        node,
        TransportConfig::default(),
        tx_raft_messages,
        tx_raft_events,
        tx_main_queue,
        rx_inbox,
        rx_tick,
    );
    processor.update_partition_map(map);
    tokio::spawn(processor.run(tx_batch));
    RunningProcessor {
        tick,
        inbox,
        batches,
    }
}

async fn next_batch_with(
    processor: &RunningProcessor,
    now: u64,
    wanted: impl Fn(&crate::cluster::ProcessingBatch) -> bool,
) -> crate::cluster::ProcessingBatch {
    for attempt in 0..50 {
        processor.tick.send(now + attempt).unwrap();
        let batch = processor.batches.recv_async().await.unwrap();
        if wanted(&batch) {
            return batch;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("processor never produced the expected batch");
}

#[tokio::test]
async fn followers_adopt_the_unique_voters_the_leader_founds() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();
    let mut leader = create_test_controller(node1, MockTransport::new(node1));
    let mut follower = create_test_controller(node2, MockTransport::new(node2));
    leader.update_partition_map(three_node_map());
    follower.update_partition_map(three_node_map());
    leader.manage_unique_voters(true, 3, 0);
    assert_eq!(leader.unique_voter_group(), [node1, node2, node3]);
    assert!(follower.unique_voter_group().is_empty());

    let leader_processor = spawn_message_processor(node1, three_node_map());
    let follower_processor = spawn_message_processor(node2, three_node_map());

    let outgoing = next_batch_with(&leader_processor, 1000, |batch| {
        batch.heartbeat_to_send.is_some()
    })
    .await
    .heartbeat_to_send
    .unwrap();
    leader.broadcast_heartbeat(outgoing).await;
    let broadcast = leader.transport.sent_messages();
    let [(_, message)] = broadcast.as_slice() else {
        panic!("expected one broadcast heartbeat, got {broadcast:?}");
    };
    let mut wire = Vec::new();
    message.encode_payload(&mut wire);
    let received = ClusterMessage::decode_from_wire(message.message_type(), &wire).unwrap();

    follower_processor
        .inbox
        .send(InboundMessage {
            from: node1,
            message: received,
            received_at: 1000,
        })
        .unwrap();
    let batch = next_batch_with(&follower_processor, 1000, |batch| {
        !batch.heartbeat_updates.is_empty()
    })
    .await;
    follower.apply_heartbeat_updates(&batch.heartbeat_updates);

    assert_eq!(follower.unique_voter_group(), [node1, node2, node3]);
    assert_eq!(follower.unique_majority(), 2);
}

struct RoutedEvent {
    topic: String,
    user_properties: Vec<(String, String)>,
    event: serde_json::Value,
}

fn routed_change_events(ctrl: &NodeController<MockTransport>) -> Vec<RoutedEvent> {
    ctrl.transport
        .queued_publishes()
        .into_iter()
        .filter_map(|publish| match publish {
            QueuedPublish::Routed(routed) => Some(RoutedEvent {
                topic: routed.topic,
                user_properties: routed.user_properties,
                event: serde_json::from_slice(&routed.payload).unwrap(),
            }),
            QueuedPublish::Plain(..) => None,
        })
        .collect()
}

fn assert_written_by_editor(events: &[RoutedEvent]) {
    for routed in events {
        let topic = &routed.topic;
        assert_eq!(
            routed.user_properties,
            [("x-origin-client-id".to_string(), "editor-1".to_string())],
            "{topic}"
        );
        assert_eq!(routed.event["sender"], "alice", "{topic}");
        assert_eq!(routed.event["client_id"], "editor-1", "{topic}");
    }
}

fn forwarded_request(
    op: crate::cluster::protocol::JsonDbOp,
    entity: &str,
    id: Option<&str>,
    payload: &serde_json::Value,
) -> crate::cluster::protocol::JsonDbRequest {
    let request = crate::cluster::protocol::JsonDbRequest {
        request_id: 1,
        op,
        entity: entity.to_string(),
        id: id.map(String::from),
        payload: serde_json::to_vec(payload).unwrap(),
        response_topic: "resp/t".to_string(),
        correlation_data: None,
        sender: Some("alice".to_string()),
        client_id: Some("editor-1".to_string()),
    };
    crate::cluster::protocol::JsonDbRequest::from_bytes(&request.to_bytes().unwrap()).unwrap()
}

#[tokio::test]
async fn forwarded_writes_publish_events_with_the_writers_origin_and_scope() {
    use crate::cluster::protocol::JsonDbOp;

    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let mut ctrl = create_test_controller(node1, MockTransport::new(node1));
    make_all_primary(&mut ctrl, node1);
    ctrl.set_scope_config(Arc::new(
        mqdb_core::types::ScopeConfig::parse("room=roomId").unwrap(),
    ));
    let partition = crate::cluster::db::data_partition("msg", "m1");

    for request in [
        forwarded_request(
            JsonDbOp::Create,
            "msg",
            None,
            &serde_json::json!({"id": "m1", "roomId": "r1", "n": 0}),
        ),
        forwarded_request(
            JsonDbOp::Update,
            "msg",
            Some("m1"),
            &serde_json::json!({"n": 1}),
        ),
        forwarded_request(JsonDbOp::Delete, "msg", Some("m1"), &serde_json::json!({})),
    ] {
        ctrl.handle_json_db_request(node2, partition, &request)
            .await;
    }

    let events = routed_change_events(&ctrl);
    let topics: Vec<&str> = events.iter().map(|routed| routed.topic.as_str()).collect();
    assert_eq!(
        topics,
        [
            "$DB/room/r1/msg/events/created",
            "$DB/room/r1/msg/events/updated",
            "$DB/room/r1/msg/events/deleted",
        ]
    );
    assert_written_by_editor(&events);
}

#[tokio::test]
async fn scope_records_with_a_generated_id_publish_on_their_own_scope() {
    use crate::cluster::db_handler::{DbRequestHandler, MqttRequestContext};
    use crate::cluster::protocol::JsonDbOp;

    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let mut ctrl = create_test_controller(node1, MockTransport::new(node1));
    make_all_primary(&mut ctrl, node1);
    ctrl.set_scope_config(Arc::new(
        mqdb_core::types::ScopeConfig::parse("room=roomId").unwrap(),
    ));

    let forwarded = forwarded_request(
        JsonDbOp::Create,
        "room",
        None,
        &serde_json::json!({"name": "forwarded"}),
    );
    ctrl.handle_json_db_request(node2, PartitionId::new(7).unwrap(), &forwarded)
        .await;
    DbRequestHandler::new(node1)
        .handle_publish(
            &mut ctrl,
            "$DB/room/create",
            br#"{"name":"local"}"#,
            &MqttRequestContext {
                response_topic: Some("resp/t"),
                correlation_data: None,
                sender: Some("alice"),
                client_id: Some("editor-1"),
            },
        )
        .await;

    let events = routed_change_events(&ctrl);
    let names: Vec<&str> = events
        .iter()
        .map(|routed| routed.event["data"]["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["forwarded", "local"]);
    for routed in &events {
        let id = routed.event["id"].as_str().unwrap();
        assert_eq!(routed.topic, format!("$DB/room/{id}/events/created"));
    }
    assert_written_by_editor(&events);
}

#[tokio::test]
async fn forwarded_writes_completed_after_a_unique_check_publish_their_events() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let mut ctrl = create_test_controller(node1, MockTransport::new(node1));
    make_all_primary(&mut ctrl, node1);
    let partition = crate::cluster::db::data_partition("users", "u1");
    let data = serde_json::json!({"id": "u1", "email": "a@x"});
    let merged_data = serde_json::json!({"id": "u1", "email": "b@x"});

    ctrl.complete_pending_unique_work(
        Vec::new(),
        Ok(Vec::new()),
        UniqueCheckContinuation::CreateFromNodeController {
            from: node2,
            entity: "users".to_string(),
            id: "u1".to_string(),
            data_bytes: serde_json::to_vec(&data).unwrap(),
            data,
            partition,
            request_id: "req-1".to_string(),
            now_ms: 1000,
            sender: Some("alice".to_string()),
            client_id: Some("editor-1".to_string()),
            response_topic: "resp/t".to_string(),
            correlation_data: None,
        },
    )
    .await;
    ctrl.complete_pending_unique_work(
        Vec::new(),
        Ok(Vec::new()),
        UniqueCheckContinuation::UpdateFromNodeController {
            from: node2,
            entity: "users".to_string(),
            id: "u1".to_string(),
            data_bytes: serde_json::to_vec(&merged_data).unwrap(),
            merged_data,
            partition,
            request_id: "req-2".to_string(),
            now_ms: 2000,
            new_diff: serde_json::json!({"email": "b@x"}),
            old_diff: serde_json::json!({"email": "a@x"}),
            sender: Some("alice".to_string()),
            client_id: Some("editor-1".to_string()),
            response_topic: "resp/t".to_string(),
            correlation_data: None,
        },
    )
    .await;

    let events = routed_change_events(&ctrl);
    let summary: Vec<(&str, &str)> = events
        .iter()
        .map(|routed| {
            (
                routed.topic.as_str(),
                routed.event["data"]["email"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        summary,
        [
            ("$DB/users/events/u1", "a@x"),
            ("$DB/users/events/u1", "b@x"),
        ]
    );
    assert_eq!(events[0].event["operation"], "Create");
    assert_eq!(events[1].event["operation"], "Update");
    assert_written_by_editor(&events);
}

#[tokio::test]
async fn forwarded_delete_cascade_events_carry_the_deleters_origin() {
    use crate::cluster::protocol::JsonDbOp;

    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let mut ctrl = create_test_controller(node1, MockTransport::new(node1));
    setup_post_with_owned_and_cross_owned_comments(&mut ctrl, node1).await;
    let partition = crate::cluster::db::data_partition("posts", "post-alice");

    let request = forwarded_request(
        JsonDbOp::Delete,
        "posts",
        Some("post-alice"),
        &serde_json::json!({}),
    );
    ctrl.handle_json_db_request(node2, partition, &request)
        .await;

    let events = routed_change_events(&ctrl);
    let mut changes: Vec<(String, String)> = events
        .iter()
        .map(|routed| {
            (
                routed.event["id"].as_str().unwrap().to_string(),
                routed.event["operation"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    changes.sort();
    assert_eq!(
        changes,
        [
            ("comment-alice".to_string(), "Delete".to_string()),
            ("comment-carol".to_string(), "Update".to_string()),
            ("post-alice".to_string(), "Delete".to_string()),
        ]
    );
    assert_written_by_editor(&events);
}

mod change_events;
mod forwarded_write;
mod partition_map;
mod store_failure;
