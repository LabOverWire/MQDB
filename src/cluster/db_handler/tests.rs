// Copyright 2025-2026 LabOverWire. All rights reserved.
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
    assert!(
        json["message"]
            .as_str()
            .unwrap()
            .contains("permission denied")
    );
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

#[tokio::test]
async fn json_create_rejects_wrong_type_with_schema() {
    let node1 = NodeId::validated(1).unwrap();
    let handler = DbRequestHandler::new(node1);

    let mut ctrl = setup_controller_all_partitions();

    let schema_json = serde_json::json!({
        "entity": "users",
        "fields": {
            "name": {"name": "name", "field_type": "String", "required": true, "default": null}
        }
    });
    let schema_bytes = serde_json::to_vec(&schema_json).unwrap();
    ctrl.stores_mut()
        .db_schema
        .register("users", &schema_bytes)
        .unwrap();

    let payload = serde_json::to_vec(&serde_json::json!({"name": 123})).unwrap();
    let topic = "$DB/users/create";

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

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "error");
    assert_eq!(json["code"], 400);
    assert!(
        json["message"].as_str().unwrap().contains("name"),
        "expected error to mention 'name', got: {}",
        json["message"]
    );
}

#[tokio::test]
async fn json_list_rejects_nonexistent_filter_field() {
    let node1 = NodeId::validated(1).unwrap();
    let handler = DbRequestHandler::new(node1);

    let mut ctrl = setup_controller_all_partitions();

    let schema_json = serde_json::json!({
        "entity": "users",
        "fields": {
            "name": {"name": "name", "field_type": "String", "required": false, "default": null}
        }
    });
    let schema_bytes = serde_json::to_vec(&schema_json).unwrap();
    ctrl.stores_mut()
        .db_schema
        .register("users", &schema_bytes)
        .unwrap();

    let data = serde_json::json!({"name": "Alice"});
    ctrl.db_create("users", "u-1", &serde_json::to_vec(&data).unwrap(), 1000)
        .await
        .unwrap();

    let list_payload = serde_json::to_vec(&serde_json::json!({
        "filters": [{"field": "nonexistent", "op": "eq", "value": "x"}]
    }))
    .unwrap();
    let topic = "$DB/users/list";

    let response = handler
        .handle_publish(
            &mut ctrl,
            topic,
            &list_payload,
            Some("$DB/_resp/client1"),
            None,
            None,
            None,
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "error");
    assert_eq!(json["code"], 400);
    assert!(
        json["message"].as_str().unwrap().contains("nonexistent"),
        "expected error to mention 'nonexistent', got: {}",
        json["message"]
    );
}

#[tokio::test]
async fn update_strips_ownership_field_for_non_admin() {
    let node1 = NodeId::validated(1).unwrap();
    let mut ownership_fields = std::collections::HashMap::new();
    ownership_fields.insert("items".to_string(), "userId".to_string());
    let ownership = OwnershipConfig::new(ownership_fields);
    let handler = DbRequestHandler::new(node1).with_ownership(Arc::new(ownership));

    let mut ctrl = setup_controller_all_partitions();

    let data = serde_json::json!({"name": "widget", "userId": "alice"});
    ctrl.db_create("items", "i-1", &serde_json::to_vec(&data).unwrap(), 1000)
        .await
        .unwrap();

    let update = serde_json::json!({"name": "gadget", "userId": "bob"});
    let topic = "$DB/items/i-1/update";
    let response = handler
        .handle_publish(
            &mut ctrl,
            topic,
            &serde_json::to_vec(&update).unwrap(),
            Some("$DB/_resp/client1"),
            None,
            Some("alice"),
            None,
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "ok");
    assert_eq!(json["data"]["userId"], "alice");
    assert_eq!(json["data"]["name"], "gadget");
}

#[tokio::test]
async fn read_forbidden_for_non_owner_cluster_path() {
    let node1 = NodeId::validated(1).unwrap();
    let mut ownership_fields = std::collections::HashMap::new();
    ownership_fields.insert("items".to_string(), "userId".to_string());
    let ownership = OwnershipConfig::new(ownership_fields);
    let handler = DbRequestHandler::new(node1).with_ownership(Arc::new(ownership));

    let mut ctrl = setup_controller_all_partitions();

    let data = serde_json::json!({"name": "widget", "userId": "alice"});
    ctrl.db_create("items", "i-1", &serde_json::to_vec(&data).unwrap(), 1000)
        .await
        .unwrap();

    let topic = "$DB/items/i-1";
    let response = handler
        .handle_publish(
            &mut ctrl,
            topic,
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
async fn fk_create_with_valid_reference() {
    use super::super::db::{ClusterConstraint, OnDeleteAction};

    let node1 = NodeId::validated(1).unwrap();
    let handler = DbRequestHandler::new(node1);
    let mut ctrl = setup_controller_all_partitions();

    let user_data = serde_json::to_vec(&serde_json::json!({"name": "Alice"})).unwrap();
    ctrl.db_create("users", "u1", &user_data, 1000)
        .await
        .unwrap();

    let fk = ClusterConstraint::foreign_key(
        "posts",
        "posts_author_fk",
        "author_id",
        "users",
        "id",
        OnDeleteAction::Restrict,
    );
    ctrl.constraint_add(&fk).await.unwrap();

    let payload = serde_json::to_vec(&serde_json::json!({
        "id": "p1",
        "title": "Hello",
        "author_id": "u1"
    }))
    .unwrap();

    let response = handler
        .handle_publish(
            &mut ctrl,
            "$DB/posts/create",
            &payload,
            Some("resp/t"),
            None,
            None,
            None,
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "ok");
    assert!(ctrl.db_get("posts", "p1").is_some());
}

#[tokio::test]
async fn fk_create_with_invalid_reference() {
    use super::super::db::{ClusterConstraint, OnDeleteAction};

    let node1 = NodeId::validated(1).unwrap();
    let handler = DbRequestHandler::new(node1);
    let mut ctrl = setup_controller_all_partitions();

    let fk = ClusterConstraint::foreign_key(
        "posts",
        "posts_author_fk",
        "author_id",
        "users",
        "id",
        OnDeleteAction::Restrict,
    );
    ctrl.constraint_add(&fk).await.unwrap();

    let payload = serde_json::to_vec(&serde_json::json!({
        "id": "p1",
        "title": "Hello",
        "author_id": "nonexistent"
    }))
    .unwrap();

    let response = handler
        .handle_publish(
            &mut ctrl,
            "$DB/posts/create",
            &payload,
            Some("resp/t"),
            None,
            None,
            None,
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "error");
    assert_eq!(json["code"], 409);
    assert!(ctrl.db_get("posts", "p1").is_none());
}

#[tokio::test]
async fn fk_update_to_invalid_reference() {
    use super::super::db::{ClusterConstraint, OnDeleteAction};

    let node1 = NodeId::validated(1).unwrap();
    let handler = DbRequestHandler::new(node1);
    let mut ctrl = setup_controller_all_partitions();

    let user_data = serde_json::to_vec(&serde_json::json!({"name": "Alice"})).unwrap();
    ctrl.db_create("users", "u1", &user_data, 1000)
        .await
        .unwrap();

    let post_data = serde_json::to_vec(&serde_json::json!({
        "title": "Hello",
        "author_id": "u1"
    }))
    .unwrap();
    ctrl.db_create("posts", "p1", &post_data, 1000)
        .await
        .unwrap();

    let fk = ClusterConstraint::foreign_key(
        "posts",
        "posts_author_fk",
        "author_id",
        "users",
        "id",
        OnDeleteAction::Restrict,
    );
    ctrl.constraint_add(&fk).await.unwrap();

    let update_payload =
        serde_json::to_vec(&serde_json::json!({"author_id": "nonexistent"})).unwrap();

    let response = handler
        .handle_publish(
            &mut ctrl,
            "$DB/posts/p1/update",
            &update_payload,
            Some("resp/t"),
            None,
            None,
            None,
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "error");
    assert_eq!(json["code"], 409);

    let stored = ctrl.db_get("posts", "p1").unwrap();
    let stored_data: serde_json::Value = serde_json::from_slice(&stored.data).unwrap();
    assert_eq!(stored_data["author_id"], "u1");
}

#[tokio::test]
async fn fk_delete_restrict_blocks() {
    use super::super::db::{ClusterConstraint, OnDeleteAction};

    let node1 = NodeId::validated(1).unwrap();
    let handler = DbRequestHandler::new(node1);
    let mut ctrl = setup_controller_all_partitions();

    let user_data = serde_json::to_vec(&serde_json::json!({"name": "Alice"})).unwrap();
    ctrl.db_create("users", "u1", &user_data, 1000)
        .await
        .unwrap();

    let post_data = serde_json::to_vec(&serde_json::json!({
        "title": "Hello",
        "author_id": "u1"
    }))
    .unwrap();
    ctrl.db_create("posts", "p1", &post_data, 1000)
        .await
        .unwrap();

    let fk = ClusterConstraint::foreign_key(
        "posts",
        "posts_author_fk",
        "author_id",
        "users",
        "id",
        OnDeleteAction::Restrict,
    );
    ctrl.constraint_add(&fk).await.unwrap();

    let response = handler
        .handle_publish(
            &mut ctrl,
            "$DB/users/u1/delete",
            &[],
            Some("resp/t"),
            None,
            None,
            None,
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "error");
    assert_eq!(json["code"], 409);
    assert!(ctrl.db_get("users", "u1").is_some());
}

#[tokio::test]
async fn fk_delete_cascade_removes_children() {
    use super::super::db::{ClusterConstraint, OnDeleteAction};

    let node1 = NodeId::validated(1).unwrap();
    let handler = DbRequestHandler::new(node1);
    let mut ctrl = setup_controller_all_partitions();

    let user_data = serde_json::to_vec(&serde_json::json!({"name": "Alice"})).unwrap();
    ctrl.db_create("users", "u1", &user_data, 1000)
        .await
        .unwrap();

    let post1 = serde_json::to_vec(&serde_json::json!({
        "title": "Post 1",
        "author_id": "u1"
    }))
    .unwrap();
    let post2 = serde_json::to_vec(&serde_json::json!({
        "title": "Post 2",
        "author_id": "u1"
    }))
    .unwrap();
    ctrl.db_create("posts", "p1", &post1, 1000).await.unwrap();
    ctrl.db_create("posts", "p2", &post2, 1000).await.unwrap();

    let fk = ClusterConstraint::foreign_key(
        "posts",
        "posts_author_fk",
        "author_id",
        "users",
        "id",
        OnDeleteAction::Cascade,
    );
    ctrl.constraint_add(&fk).await.unwrap();

    let response = handler
        .handle_publish(
            &mut ctrl,
            "$DB/users/u1/delete",
            &[],
            Some("resp/t"),
            None,
            None,
            None,
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "ok");
    assert_eq!(json["deleted"], true);

    assert!(ctrl.db_get("users", "u1").is_none());
    assert!(ctrl.db_get("posts", "p1").is_none());
    assert!(ctrl.db_get("posts", "p2").is_none());
}

#[tokio::test]
async fn fk_delete_set_null_nullifies_children() {
    use super::super::db::{ClusterConstraint, OnDeleteAction};

    let node1 = NodeId::validated(1).unwrap();
    let handler = DbRequestHandler::new(node1);
    let mut ctrl = setup_controller_all_partitions();

    let user_data = serde_json::to_vec(&serde_json::json!({"name": "Alice"})).unwrap();
    ctrl.db_create("users", "u1", &user_data, 1000)
        .await
        .unwrap();

    let post_data = serde_json::to_vec(&serde_json::json!({
        "title": "Hello",
        "author_id": "u1"
    }))
    .unwrap();
    ctrl.db_create("posts", "p1", &post_data, 1000)
        .await
        .unwrap();

    let fk = ClusterConstraint::foreign_key(
        "posts",
        "posts_author_fk",
        "author_id",
        "users",
        "id",
        OnDeleteAction::SetNull,
    );
    ctrl.constraint_add(&fk).await.unwrap();

    let response = handler
        .handle_publish(
            &mut ctrl,
            "$DB/users/u1/delete",
            &[],
            Some("resp/t"),
            None,
            None,
            None,
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "ok");
    assert_eq!(json["deleted"], true);

    assert!(ctrl.db_get("users", "u1").is_none());
    let post = ctrl.db_get("posts", "p1").unwrap();
    let post_json: serde_json::Value = serde_json::from_slice(&post.data).unwrap();
    assert_eq!(post_json["title"], "Hello");
    assert!(post_json["author_id"].is_null());
}

#[tokio::test]
async fn fk_create_with_null_fk_field_allowed() {
    use super::super::db::{ClusterConstraint, OnDeleteAction};

    let node1 = NodeId::validated(1).unwrap();
    let handler = DbRequestHandler::new(node1);
    let mut ctrl = setup_controller_all_partitions();

    let fk = ClusterConstraint::foreign_key(
        "posts",
        "posts_author_fk",
        "author_id",
        "users",
        "id",
        OnDeleteAction::Restrict,
    );
    ctrl.constraint_add(&fk).await.unwrap();

    let payload = serde_json::to_vec(&serde_json::json!({
        "id": "p1",
        "title": "No author",
        "author_id": null
    }))
    .unwrap();

    let response = handler
        .handle_publish(
            &mut ctrl,
            "$DB/posts/create",
            &payload,
            Some("resp/t"),
            None,
            None,
            None,
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "ok");
    assert!(ctrl.db_get("posts", "p1").is_some());
}

#[tokio::test]
async fn fk_delete_restrict_allows_when_no_references() {
    use super::super::db::{ClusterConstraint, OnDeleteAction};

    let node1 = NodeId::validated(1).unwrap();
    let handler = DbRequestHandler::new(node1);
    let mut ctrl = setup_controller_all_partitions();

    let user_data = serde_json::to_vec(&serde_json::json!({"name": "Alice"})).unwrap();
    ctrl.db_create("users", "u1", &user_data, 1000)
        .await
        .unwrap();

    let fk = ClusterConstraint::foreign_key(
        "posts",
        "posts_author_fk",
        "author_id",
        "users",
        "id",
        OnDeleteAction::Restrict,
    );
    ctrl.constraint_add(&fk).await.unwrap();

    let response = handler
        .handle_publish(
            &mut ctrl,
            "$DB/users/u1/delete",
            &[],
            Some("resp/t"),
            None,
            None,
            None,
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "ok");
    assert_eq!(json["deleted"], true);
    assert!(ctrl.db_get("users", "u1").is_none());
}

#[tokio::test]
async fn fk_delete_cascade_multilevel() {
    use super::super::db::{ClusterConstraint, OnDeleteAction};

    let node1 = NodeId::validated(1).unwrap();
    let handler = DbRequestHandler::new(node1);
    let mut ctrl = setup_controller_all_partitions();

    let user_data = serde_json::to_vec(&serde_json::json!({"name": "Alice"})).unwrap();
    ctrl.db_create("users", "u1", &user_data, 1000)
        .await
        .unwrap();

    let post = serde_json::to_vec(&serde_json::json!({
        "title": "Post 1",
        "author_id": "u1"
    }))
    .unwrap();
    ctrl.db_create("posts", "p1", &post, 1000).await.unwrap();

    let comment = serde_json::to_vec(&serde_json::json!({
        "body": "Great post",
        "post_id": "p1"
    }))
    .unwrap();
    ctrl.db_create("comments", "c1", &comment, 1000)
        .await
        .unwrap();

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

    let response = handler
        .handle_publish(
            &mut ctrl,
            "$DB/users/u1/delete",
            &[],
            Some("resp/t"),
            None,
            None,
            None,
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "ok");
    assert!(ctrl.db_get("users", "u1").is_none());
    assert!(ctrl.db_get("posts", "p1").is_none());
    assert!(ctrl.db_get("comments", "c1").is_none());
}

#[tokio::test]
async fn fk_delete_cascade_circular_no_infinite_loop() {
    use super::super::db::{ClusterConstraint, OnDeleteAction};

    let node1 = NodeId::validated(1).unwrap();
    let handler = DbRequestHandler::new(node1);
    let mut ctrl = setup_controller_all_partitions();

    let a1 = serde_json::to_vec(&serde_json::json!({"ref_b": "b1"})).unwrap();
    let b1 = serde_json::to_vec(&serde_json::json!({"ref_a": "a1"})).unwrap();
    ctrl.db_create("entity_a", "a1", &a1, 1000).await.unwrap();
    ctrl.db_create("entity_b", "b1", &b1, 1000).await.unwrap();

    let fk_a_to_b = ClusterConstraint::foreign_key(
        "entity_a",
        "a_refs_b",
        "ref_b",
        "entity_b",
        "id",
        OnDeleteAction::Cascade,
    );
    ctrl.constraint_add(&fk_a_to_b).await.unwrap();

    let fk_b_to_a = ClusterConstraint::foreign_key(
        "entity_b",
        "b_refs_a",
        "ref_a",
        "entity_a",
        "id",
        OnDeleteAction::Cascade,
    );
    ctrl.constraint_add(&fk_b_to_a).await.unwrap();

    let response = handler
        .handle_publish(
            &mut ctrl,
            "$DB/entity_a/a1/delete",
            &[],
            Some("resp/t"),
            None,
            None,
            None,
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "ok");
    assert!(ctrl.db_get("entity_a", "a1").is_none());
    assert!(ctrl.db_get("entity_b", "b1").is_none());
}

#[tokio::test]
async fn fk_delete_cascade_depth_limit_exceeded() {
    use super::super::db::{ClusterConstraint, OnDeleteAction};
    use super::super::node_controller::fk::MAX_CASCADE_DEPTH;

    let node1 = NodeId::validated(1).unwrap();
    let handler = DbRequestHandler::new(node1);
    let mut ctrl = setup_controller_all_partitions();

    let chain_len = MAX_CASCADE_DEPTH + 2;
    for i in 0..chain_len {
        let entity = format!("e{i}");
        let id = format!("r{i}");
        let data = if i == 0 {
            serde_json::json!({"name": "root"})
        } else {
            let prev_entity = format!("e{}", i - 1);
            serde_json::json!({"ref_parent": format!("r{}", i - 1), "parent_entity": prev_entity})
        };
        let bytes = serde_json::to_vec(&data).unwrap();
        ctrl.db_create(&entity, &id, &bytes, 1000).await.unwrap();
    }

    for i in 1..chain_len {
        let entity = format!("e{i}");
        let prev_entity = format!("e{}", i - 1);
        let fk = ClusterConstraint::foreign_key(
            &entity,
            &format!("fk_{entity}"),
            "ref_parent",
            &prev_entity,
            "id",
            OnDeleteAction::Cascade,
        );
        ctrl.constraint_add(&fk).await.unwrap();
    }

    let response = handler
        .handle_publish(
            &mut ctrl,
            "$DB/e0/r0/delete",
            &[],
            Some("resp/t"),
            None,
            None,
            None,
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "error");
    assert!(
        json["message"]
            .as_str()
            .unwrap()
            .contains("depth limit exceeded")
    );
}

#[tokio::test]
async fn fk_set_null_skips_when_fk_field_changed() {
    use super::super::db::{ClusterConstraint, OnDeleteAction};

    let node1 = NodeId::validated(1).unwrap();
    let handler = DbRequestHandler::new(node1);
    let mut ctrl = setup_controller_all_partitions();

    let user_a = serde_json::to_vec(&serde_json::json!({"name": "Alice"})).unwrap();
    let user_b = serde_json::to_vec(&serde_json::json!({"name": "Bob"})).unwrap();
    ctrl.db_create("users", "a", &user_a, 1000).await.unwrap();
    ctrl.db_create("users", "b", &user_b, 1000).await.unwrap();

    let post = serde_json::to_vec(&serde_json::json!({
        "title": "Hello",
        "author_id": "a"
    }))
    .unwrap();
    ctrl.db_create("posts", "p1", &post, 1000).await.unwrap();

    let fk = ClusterConstraint::foreign_key(
        "posts",
        "posts_author_fk",
        "author_id",
        "users",
        "id",
        OnDeleteAction::SetNull,
    );
    ctrl.constraint_add(&fk).await.unwrap();

    let update_payload = serde_json::to_vec(&serde_json::json!({"author_id": "b"})).unwrap();
    let response = handler
        .handle_publish(
            &mut ctrl,
            "$DB/posts/p1/update",
            &update_payload,
            Some("resp/t"),
            None,
            None,
            None,
        )
        .await;
    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "ok");

    let set_null_payload = serde_json::to_vec(&serde_json::json!({
        "author_id": null,
        "__mqdb_fk_expected": "author_id=a"
    }))
    .unwrap();
    let set_null_resp = ctrl
        .handle_json_update_local_for_test("posts", "p1", &set_null_payload)
        .await;
    let set_null_json: serde_json::Value = serde_json::from_slice(&set_null_resp).unwrap();
    assert_eq!(set_null_json["status"], "ok");

    let stored = ctrl.db_get("posts", "p1").unwrap();
    let stored_data: serde_json::Value = serde_json::from_slice(&stored.data).unwrap();
    assert_eq!(stored_data["author_id"], "b");
}

#[tokio::test]
async fn fk_create_with_absent_fk_field_allowed() {
    use super::super::db::{ClusterConstraint, OnDeleteAction};

    let node1 = NodeId::validated(1).unwrap();
    let handler = DbRequestHandler::new(node1);
    let mut ctrl = setup_controller_all_partitions();

    let fk = ClusterConstraint::foreign_key(
        "posts",
        "posts_author_fk",
        "author_id",
        "users",
        "id",
        OnDeleteAction::Restrict,
    );
    ctrl.constraint_add(&fk).await.unwrap();

    let payload = serde_json::to_vec(&serde_json::json!({
        "id": "p1",
        "title": "No author field at all"
    }))
    .unwrap();

    let response = handler
        .handle_publish(
            &mut ctrl,
            "$DB/posts/create",
            &payload,
            Some("resp/t"),
            None,
            None,
            None,
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "ok");
    assert!(ctrl.db_get("posts", "p1").is_some());
}

#[tokio::test]
async fn fk_create_with_multiple_constraints_validates_all() {
    use super::super::db::{ClusterConstraint, OnDeleteAction};

    let node1 = NodeId::validated(1).unwrap();
    let handler = DbRequestHandler::new(node1);
    let mut ctrl = setup_controller_all_partitions();

    let user_data = serde_json::to_vec(&serde_json::json!({"name": "Alice"})).unwrap();
    ctrl.db_create("users", "u1", &user_data, 1000)
        .await
        .unwrap();

    let cat_data = serde_json::to_vec(&serde_json::json!({"name": "Tech"})).unwrap();
    ctrl.db_create("categories", "c1", &cat_data, 1000)
        .await
        .unwrap();

    let fk_author = ClusterConstraint::foreign_key(
        "posts",
        "posts_author_fk",
        "author_id",
        "users",
        "id",
        OnDeleteAction::Restrict,
    );
    ctrl.constraint_add(&fk_author).await.unwrap();

    let fk_cat = ClusterConstraint::foreign_key(
        "posts",
        "posts_cat_fk",
        "category_id",
        "categories",
        "id",
        OnDeleteAction::Restrict,
    );
    ctrl.constraint_add(&fk_cat).await.unwrap();

    let valid = serde_json::to_vec(&serde_json::json!({
        "id": "p1",
        "title": "Hello",
        "author_id": "u1",
        "category_id": "c1"
    }))
    .unwrap();
    let response = handler
        .handle_publish(
            &mut ctrl,
            "$DB/posts/create",
            &valid,
            Some("resp/t"),
            None,
            None,
            None,
        )
        .await;
    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "ok");

    let bad_cat = serde_json::to_vec(&serde_json::json!({
        "id": "p2",
        "title": "Bad cat",
        "author_id": "u1",
        "category_id": "nonexistent"
    }))
    .unwrap();
    let response = handler
        .handle_publish(
            &mut ctrl,
            "$DB/posts/create",
            &bad_cat,
            Some("resp/t"),
            None,
            None,
            None,
        )
        .await;
    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "error");
    assert_eq!(json["code"], 409);

    let bad_author = serde_json::to_vec(&serde_json::json!({
        "id": "p3",
        "title": "Bad author",
        "author_id": "nonexistent",
        "category_id": "c1"
    }))
    .unwrap();
    let response = handler
        .handle_publish(
            &mut ctrl,
            "$DB/posts/create",
            &bad_author,
            Some("resp/t"),
            None,
            None,
            None,
        )
        .await;
    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "error");
    assert_eq!(json["code"], 409);
}

#[tokio::test]
async fn fk_delete_cascade_with_no_children_succeeds() {
    use super::super::db::{ClusterConstraint, OnDeleteAction};

    let node1 = NodeId::validated(1).unwrap();
    let handler = DbRequestHandler::new(node1);
    let mut ctrl = setup_controller_all_partitions();

    let user_data = serde_json::to_vec(&serde_json::json!({"name": "Alice"})).unwrap();
    ctrl.db_create("users", "u1", &user_data, 1000)
        .await
        .unwrap();

    let fk = ClusterConstraint::foreign_key(
        "posts",
        "posts_author_fk",
        "author_id",
        "users",
        "id",
        OnDeleteAction::Cascade,
    );
    ctrl.constraint_add(&fk).await.unwrap();

    let response = handler
        .handle_publish(
            &mut ctrl,
            "$DB/users/u1/delete",
            &[],
            Some("resp/t"),
            None,
            None,
            None,
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "ok");
    assert_eq!(json["deleted"], true);
    assert!(ctrl.db_get("users", "u1").is_none());
}
