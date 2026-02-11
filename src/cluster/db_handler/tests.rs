// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::super::db::data_partition;
use super::super::db_protocol::{DbReadRequest, DbResponse, DbStatus, DbWriteRequest};
use super::super::node_controller::NodeController;
use super::super::transport::{ClusterMessage, ClusterTransport, InboundMessage, TransportConfig};
use super::super::{Epoch, NUM_PARTITIONS, NodeId, PartitionId, PartitionMap};
use super::DbRequestHandler;
use crate::types::OwnershipConfig;
use bebytes::BeBytes;
use std::collections::{HashMap, VecDeque};
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

fn setup_controller_with_partition(partition: PartitionId) -> NodeController<MockTransport> {
    let node1 = NodeId::validated(1).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);
    ctrl.become_primary(partition, Epoch::new(1));

    let mut map = PartitionMap::default();
    map.set(
        partition,
        crate::cluster::PartitionAssignment {
            primary: Some(node1),
            replicas: vec![],
            epoch: Epoch::new(1),
        },
    );
    ctrl.update_partition_map(map);

    ctrl
}

fn setup_controller_all_partitions() -> NodeController<MockTransport> {
    let node1 = NodeId::validated(1).unwrap();
    let transport = MockTransport::new(node1);
    let mut ctrl = create_test_controller(node1, transport);

    let mut map = PartitionMap::default();
    for i in 0..NUM_PARTITIONS {
        let partition = PartitionId::new(i).unwrap();
        ctrl.become_primary(partition, Epoch::new(1));
        map.set(
            partition,
            crate::cluster::PartitionAssignment {
                primary: Some(node1),
                replicas: vec![],
                epoch: Epoch::new(1),
            },
        );
    }
    ctrl.update_partition_map(map);
    ctrl
}

fn ownership_config(entity: &str, field: &str) -> Arc<OwnershipConfig> {
    let mut fields = HashMap::new();
    fields.insert(entity.to_string(), field.to_string());
    Arc::new(OwnershipConfig::new(fields))
}

fn ownership_config_with_admins(
    entity: &str,
    field: &str,
    admins: &[&str],
) -> Arc<OwnershipConfig> {
    let mut fields = HashMap::new();
    fields.insert(entity.to_string(), field.to_string());
    let admin_set = admins.iter().map(|s| (*s).to_string()).collect();
    Arc::new(OwnershipConfig::new(fields).with_admin_users(admin_set))
}

fn parse_json_response(payload: &[u8]) -> serde_json::Value {
    serde_json::from_slice(payload).unwrap_or(serde_json::Value::Null)
}

#[tokio::test]
async fn handle_create_success() {
    let node1 = NodeId::validated(1).unwrap();
    let handler = DbRequestHandler::new(node1);

    let entity = "users";
    let data = b"test data";
    let partition = data_partition(entity, "test-id");

    let mut ctrl = setup_controller_with_partition(partition);

    let request = DbWriteRequest::create(data, 1000);
    let payload = request.to_be_bytes();

    let topic = format!("$DB/p{}/{}/create", partition.get(), entity);

    let response = handler
        .handle_publish(
            &mut ctrl,
            &topic,
            &payload,
            Some("$DB/_resp/client1"),
            Some(b"corr-123"),
            None,
            None,
        )
        .await;

    assert!(response.is_some());
    let resp = response.unwrap();
    assert_eq!(resp.topic, "$DB/_resp/client1");
    assert_eq!(resp.correlation_data, Some(b"corr-123".to_vec()));

    let (db_response, _) = DbResponse::try_from_be_bytes(&resp.payload).unwrap();
    assert_eq!(db_response.status(), DbStatus::Ok);
}

#[tokio::test]
async fn handle_read_not_found() {
    let node1 = NodeId::validated(1).unwrap();
    let handler = DbRequestHandler::new(node1);

    let entity = "users";
    let id = "nonexistent";
    let partition = data_partition(entity, id);

    let mut ctrl = setup_controller_with_partition(partition);

    let request = DbReadRequest::create();
    let payload = request.to_be_bytes();

    let topic = format!("$DB/p{}/{}/{}", partition.get(), entity, id);

    let response = handler
        .handle_publish(
            &mut ctrl,
            &topic,
            &payload,
            Some("$DB/_resp/client1"),
            None,
            None,
            None,
        )
        .await;

    assert!(response.is_some());
    let resp = response.unwrap();

    let (db_response, _) = DbResponse::try_from_be_bytes(&resp.payload).unwrap();
    assert_eq!(db_response.status(), DbStatus::NotFound);
}

#[tokio::test]
async fn handle_invalid_partition_returns_error() {
    let node1 = NodeId::validated(1).unwrap();
    let handler = DbRequestHandler::new(node1);

    let partition = PartitionId::ZERO;
    let mut ctrl = setup_controller_with_partition(partition);

    let request = DbWriteRequest::create(b"data", 1000);
    let payload = request.to_be_bytes();

    let topic = "$DB/p63/users/create";

    let response = handler
        .handle_publish(
            &mut ctrl,
            topic,
            &payload,
            Some("$DB/_resp/client1"),
            None,
            None,
            None,
        )
        .await;

    assert!(response.is_some());
    let resp = response.unwrap();

    let (db_response, _) = DbResponse::try_from_be_bytes(&resp.payload).unwrap();
    assert_eq!(db_response.status(), DbStatus::InvalidPartition);
}

#[tokio::test]
async fn no_response_without_response_topic() {
    let node1 = NodeId::validated(1).unwrap();
    let handler = DbRequestHandler::new(node1);

    let partition = PartitionId::ZERO;
    let mut ctrl = setup_controller_with_partition(partition);

    let request = DbWriteRequest::create(b"data", 1000);
    let payload = request.to_be_bytes();

    let topic = "$DB/p0/users/create";

    let response = handler
        .handle_publish(&mut ctrl, topic, &payload, None, None, None, None)
        .await;

    assert!(response.is_none());
}

#[tokio::test]
async fn parse_invalid_topic_returns_none() {
    let node1 = NodeId::validated(1).unwrap();
    let handler = DbRequestHandler::new(node1);

    let partition = PartitionId::ZERO;
    let mut ctrl = setup_controller_with_partition(partition);

    let response = handler
        .handle_publish(
            &mut ctrl,
            "not/a/db/topic",
            &[],
            Some("$DB/_resp/client1"),
            None,
            None,
            None,
        )
        .await;

    assert!(response.is_none());
}

#[tokio::test]
async fn json_update_forbidden_for_non_owner() {
    let node1 = NodeId::validated(1).unwrap();
    let ownership = ownership_config("diagrams", "userId");
    let handler = DbRequestHandler::new(node1).with_ownership(ownership);

    let mut ctrl = setup_controller_all_partitions();

    let entity = "diagrams";
    let id = "diag-1";
    let data = serde_json::json!({"userId": "alice", "title": "My Diagram"});
    let data_bytes = serde_json::to_vec(&data).unwrap();
    let now_ms = 1000_u64;
    ctrl.db_create(entity, id, &data_bytes, now_ms)
        .await
        .unwrap();

    let update_payload = serde_json::to_vec(&serde_json::json!({"title": "Stolen"})).unwrap();
    let topic = format!("$DB/{entity}/{id}/update");

    let response = handler
        .handle_publish(
            &mut ctrl,
            &topic,
            &update_payload,
            Some("$DB/_resp/client1"),
            None,
            Some("bob"),
            None,
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "error");
    assert_eq!(json["code"], 403);
    assert!(json["message"].as_str().unwrap().contains("bob"));
}

#[tokio::test]
async fn json_update_allowed_for_owner() {
    let node1 = NodeId::validated(1).unwrap();
    let ownership = ownership_config("diagrams", "userId");
    let handler = DbRequestHandler::new(node1).with_ownership(ownership);

    let mut ctrl = setup_controller_all_partitions();

    let entity = "diagrams";
    let id = "diag-2";
    let data = serde_json::json!({"userId": "alice", "title": "My Diagram"});
    let data_bytes = serde_json::to_vec(&data).unwrap();
    ctrl.db_create(entity, id, &data_bytes, 1000).await.unwrap();

    let update_payload = serde_json::to_vec(&serde_json::json!({"title": "Updated"})).unwrap();
    let topic = format!("$DB/{entity}/{id}/update");

    let response = handler
        .handle_publish(
            &mut ctrl,
            &topic,
            &update_payload,
            Some("$DB/_resp/client1"),
            None,
            Some("alice"),
            None,
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "ok");
}

#[tokio::test]
async fn json_delete_forbidden_for_non_owner() {
    let node1 = NodeId::validated(1).unwrap();
    let ownership = ownership_config("diagrams", "userId");
    let handler = DbRequestHandler::new(node1).with_ownership(ownership);

    let mut ctrl = setup_controller_all_partitions();

    let entity = "diagrams";
    let id = "diag-3";
    let data = serde_json::json!({"userId": "alice", "title": "Private"});
    let data_bytes = serde_json::to_vec(&data).unwrap();
    ctrl.db_create(entity, id, &data_bytes, 1000).await.unwrap();

    let topic = format!("$DB/{entity}/{id}/delete");

    let response = handler
        .handle_publish(
            &mut ctrl,
            &topic,
            &[],
            Some("$DB/_resp/client1"),
            None,
            Some("bob"),
            None,
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "error");
    assert_eq!(json["code"], 403);
}

#[tokio::test]
async fn json_delete_allowed_for_owner() {
    let node1 = NodeId::validated(1).unwrap();
    let ownership = ownership_config("diagrams", "userId");
    let handler = DbRequestHandler::new(node1).with_ownership(ownership);

    let mut ctrl = setup_controller_all_partitions();

    let entity = "diagrams";
    let id = "diag-4";
    let data = serde_json::json!({"userId": "alice", "title": "Deletable"});
    let data_bytes = serde_json::to_vec(&data).unwrap();
    ctrl.db_create(entity, id, &data_bytes, 1000).await.unwrap();

    let topic = format!("$DB/{entity}/{id}/delete");

    let response = handler
        .handle_publish(
            &mut ctrl,
            &topic,
            &[],
            Some("$DB/_resp/client1"),
            None,
            Some("alice"),
            None,
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "ok");
    assert_eq!(json["deleted"], true);
}

#[tokio::test]
async fn json_update_bypasses_ownership_when_no_sender() {
    let node1 = NodeId::validated(1).unwrap();
    let ownership = ownership_config("diagrams", "userId");
    let handler = DbRequestHandler::new(node1).with_ownership(ownership);

    let mut ctrl = setup_controller_all_partitions();

    let entity = "diagrams";
    let id = "diag-5";
    let data = serde_json::json!({"userId": "alice", "title": "Internal"});
    let data_bytes = serde_json::to_vec(&data).unwrap();
    ctrl.db_create(entity, id, &data_bytes, 1000).await.unwrap();

    let update_payload =
        serde_json::to_vec(&serde_json::json!({"title": "System Update"})).unwrap();
    let topic = format!("$DB/{entity}/{id}/update");

    let response = handler
        .handle_publish(
            &mut ctrl,
            &topic,
            &update_payload,
            Some("$DB/_resp/client1"),
            None,
            None,
            None,
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "ok");
}

#[tokio::test]
async fn json_list_filters_by_sender_ownership() {
    let node1 = NodeId::validated(1).unwrap();
    let ownership = ownership_config("diagrams", "userId");
    let handler = DbRequestHandler::new(node1).with_ownership(ownership);

    let mut ctrl = setup_controller_all_partitions();

    let entity = "diagrams";
    let alice_data = serde_json::json!({"userId": "alice", "title": "Alice's"});
    let bob_data = serde_json::json!({"userId": "bob", "title": "Bob's"});
    ctrl.db_create(
        entity,
        "d-alice",
        &serde_json::to_vec(&alice_data).unwrap(),
        1000,
    )
    .await
    .unwrap();
    ctrl.db_create(
        entity,
        "d-bob",
        &serde_json::to_vec(&bob_data).unwrap(),
        1001,
    )
    .await
    .unwrap();

    let topic = format!("$DB/{entity}/list");
    let payload = b"{}";

    let response = handler
        .handle_publish(
            &mut ctrl,
            &topic,
            payload,
            Some("$DB/_resp/client1"),
            None,
            Some("alice"),
            None,
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "ok");
    let items = json["data"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["data"]["userId"], "alice");
}

#[tokio::test]
async fn json_list_returns_all_when_no_sender() {
    let node1 = NodeId::validated(1).unwrap();
    let ownership = ownership_config("diagrams", "userId");
    let handler = DbRequestHandler::new(node1).with_ownership(ownership);

    let mut ctrl = setup_controller_all_partitions();

    let entity = "diagrams";
    let alice_data = serde_json::json!({"userId": "alice", "title": "Alice's"});
    let bob_data = serde_json::json!({"userId": "bob", "title": "Bob's"});
    ctrl.db_create(
        entity,
        "d-a2",
        &serde_json::to_vec(&alice_data).unwrap(),
        1000,
    )
    .await
    .unwrap();
    ctrl.db_create(
        entity,
        "d-b2",
        &serde_json::to_vec(&bob_data).unwrap(),
        1001,
    )
    .await
    .unwrap();

    let topic = format!("$DB/{entity}/list");

    let response = handler
        .handle_publish(
            &mut ctrl,
            &topic,
            b"{}",
            Some("$DB/_resp/client1"),
            None,
            None,
            None,
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "ok");
    let items = json["data"].as_array().unwrap();
    assert_eq!(items.len(), 2);
}

#[tokio::test]
async fn json_update_no_ownership_config_allows_any_sender() {
    let node1 = NodeId::validated(1).unwrap();
    let handler = DbRequestHandler::new(node1);

    let mut ctrl = setup_controller_all_partitions();

    let entity = "notes";
    let id = "n-1";
    let data = serde_json::json!({"userId": "alice", "text": "Secret"});
    let data_bytes = serde_json::to_vec(&data).unwrap();
    ctrl.db_create(entity, id, &data_bytes, 1000).await.unwrap();

    let update_payload = serde_json::to_vec(&serde_json::json!({"text": "Modified"})).unwrap();
    let topic = format!("$DB/{entity}/{id}/update");

    let response = handler
        .handle_publish(
            &mut ctrl,
            &topic,
            &update_payload,
            Some("$DB/_resp/client1"),
            None,
            Some("eve"),
            None,
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "ok");
}

#[tokio::test]
async fn admin_bypasses_ownership_on_update() {
    let node1 = NodeId::validated(1).unwrap();
    let ownership = ownership_config_with_admins("diagrams", "userId", &["admin"]);
    let handler = DbRequestHandler::new(node1).with_ownership(ownership);

    let mut ctrl = setup_controller_all_partitions();

    let data = serde_json::json!({"userId": "alice", "title": "Alice's"});
    ctrl.db_create(
        "diagrams",
        "d-adm1",
        &serde_json::to_vec(&data).unwrap(),
        1000,
    )
    .await
    .unwrap();

    let update_payload =
        serde_json::to_vec(&serde_json::json!({"title": "Admin Override"})).unwrap();
    let topic = "$DB/diagrams/d-adm1/update";

    let response = handler
        .handle_publish(
            &mut ctrl,
            topic,
            &update_payload,
            Some("$DB/_resp/c1"),
            None,
            Some("admin"),
            None,
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "ok");
    assert_eq!(json["data"]["title"], "Admin Override");
}

#[tokio::test]
async fn admin_bypasses_ownership_on_delete() {
    let node1 = NodeId::validated(1).unwrap();
    let ownership = ownership_config_with_admins("diagrams", "userId", &["admin"]);
    let handler = DbRequestHandler::new(node1).with_ownership(ownership);

    let mut ctrl = setup_controller_all_partitions();

    let data = serde_json::json!({"userId": "alice", "title": "Deletable"});
    ctrl.db_create(
        "diagrams",
        "d-adm2",
        &serde_json::to_vec(&data).unwrap(),
        1000,
    )
    .await
    .unwrap();

    let topic = "$DB/diagrams/d-adm2/delete";

    let response = handler
        .handle_publish(
            &mut ctrl,
            topic,
            &[],
            Some("$DB/_resp/c1"),
            None,
            Some("admin"),
            None,
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "ok");
    assert_eq!(json["deleted"], true);
}

#[tokio::test]
async fn admin_sees_all_records_on_list() {
    let node1 = NodeId::validated(1).unwrap();
    let ownership = ownership_config_with_admins("diagrams", "userId", &["admin"]);
    let handler = DbRequestHandler::new(node1).with_ownership(ownership);

    let mut ctrl = setup_controller_all_partitions();

    let alice = serde_json::json!({"userId": "alice", "title": "Alice's"});
    let bob = serde_json::json!({"userId": "bob", "title": "Bob's"});
    ctrl.db_create(
        "diagrams",
        "d-adm3",
        &serde_json::to_vec(&alice).unwrap(),
        1000,
    )
    .await
    .unwrap();
    ctrl.db_create(
        "diagrams",
        "d-adm4",
        &serde_json::to_vec(&bob).unwrap(),
        1001,
    )
    .await
    .unwrap();

    let topic = "$DB/diagrams/list";

    let response = handler
        .handle_publish(
            &mut ctrl,
            topic,
            b"{}",
            Some("$DB/_resp/c1"),
            None,
            Some("admin"),
            None,
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "ok");
    let items = json["data"].as_array().unwrap();
    assert_eq!(items.len(), 2);
}
