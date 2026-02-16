// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::cluster::NUM_PARTITIONS;
use crate::cluster::protocol::{Heartbeat, Operation};
use crate::cluster::quorum::QuorumResult;
use crate::cluster::session_partition;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

fn create_test_controller(
    node_id: NodeId,
    transport: MockTransport,
) -> NodeController<MockTransport> {
    let (tx_raft_messages, _rx_raft_messages) = flume::unbounded();
    let (tx_raft_events, _rx_raft_events) = flume::unbounded();
    NodeController::new(
        node_id,
        transport,
        TransportConfig::default(),
        tx_raft_messages,
        tx_raft_events,
    )
}

#[derive(Debug, Clone)]
struct MockTransport {
    node_id: NodeId,
    inbox: Arc<Mutex<VecDeque<InboundMessage>>>,
    outbox: Arc<Mutex<Vec<(NodeId, ClusterMessage)>>>,
}

impl MockTransport {
    fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            inbox: Arc::new(Mutex::new(VecDeque::new())),
            outbox: Arc::new(Mutex::new(Vec::new())),
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
        _partition: PartitionId,
        _message: ClusterMessage,
    ) -> Result<(), super::super::transport::TransportError> {
        Ok(())
    }

    fn recv(&self) -> Option<InboundMessage> {
        self.inbox.lock().unwrap().pop_front()
    }

    fn try_recv_timeout(&self, _timeout_ms: u64) -> Option<InboundMessage> {
        self.inbox.lock().unwrap().pop_front()
    }

    fn pending_count(&self) -> usize {
        self.inbox.lock().unwrap().len()
    }

    fn requeue(&self, msg: InboundMessage) {
        self.inbox.lock().unwrap().push_front(msg);
    }

    async fn queue_local_publish(&self, _topic: String, _payload: Vec<u8>, _qos: u8) {}

    async fn queue_local_publish_retained(&self, _topic: String, _payload: Vec<u8>, _qos: u8) {}
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

    let (session, _rx) = ctrl.create_session_quorum("client1").await.unwrap();
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
        .replicate_write_async(write, &[node2, node3])
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

    let seq = ctrl.replicate_write_async(write, &[node2]).await.unwrap();
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
async fn db_upsert_creates_or_updates() {
    let node1 = NodeId::validated(1).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    let partition = crate::cluster::db::data_partition("products", "p1");
    ctrl.become_primary(partition, Epoch::new(1));

    let entity1 = ctrl
        .db_upsert("products", "p1", b"{\"price\":100}", 1000)
        .await;
    assert_eq!(entity1.data, b"{\"price\":100}");

    let entity2 = ctrl
        .db_upsert("products", "p1", b"{\"price\":200}", 2000)
        .await;
    assert_eq!(entity2.data, b"{\"price\":200}");
    assert_eq!(entity2.timestamp_ms, 2000);
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
