// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::super::db::data_partition;
use super::super::db_protocol::{DbReadRequest, DbResponse, DbStatus, DbWriteRequest};
use super::super::node_controller::NodeController;
use super::super::transport::{ClusterMessage, ClusterTransport, InboundMessage, TransportConfig};
use super::super::{Epoch, NUM_PARTITIONS, NodeId, PartitionId, PartitionMap};
use super::DbRequestHandler;
use super::MqttRequestContext;
use bebytes::BeBytes;
use mqdb_core::types::OwnershipConfig;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

fn create_test_controller(
    node_id: NodeId,
    transport: MockTransport,
) -> NodeController<MockTransport> {
    let (tx_raft_messages, _) = flume::unbounded();
    let (tx_raft_events, _) = flume::unbounded();
    NodeController::new(
        node_id,
        transport,
        TransportConfig::default(),
        tx_raft_messages,
        tx_raft_events,
    )
}

type RetainedPublishes = Arc<Mutex<Vec<(String, Vec<u8>)>>>;
type RetainedRefreshes = Arc<Mutex<Vec<String>>>;
type SentMessages = Arc<Mutex<Vec<(NodeId, ClusterMessage)>>>;
type PresenceHarness = (
    crate::cluster::event_handler::ClusterEventHandler<MockTransport>,
    RetainedPublishes,
    SentMessages,
);

#[derive(Debug, Clone)]
struct MockTransport {
    node_id: NodeId,
    inbox: Arc<Mutex<VecDeque<InboundMessage>>>,
    outbox: Arc<Mutex<Vec<(NodeId, ClusterMessage)>>>,
    retained_publishes: RetainedPublishes,
    retained_refreshes: RetainedRefreshes,
}

impl MockTransport {
    fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            inbox: Arc::new(Mutex::new(VecDeque::new())),
            outbox: Arc::new(Mutex::new(Vec::new())),
            retained_publishes: Arc::new(Mutex::new(Vec::new())),
            retained_refreshes: Arc::new(Mutex::new(Vec::new())),
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
        _: PartitionId,
        _: ClusterMessage,
    ) -> Result<(), super::super::transport::TransportError> {
        Ok(())
    }

    async fn direct_peers(&self) -> Option<Vec<NodeId>> {
        None
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

    async fn queue_local_publish(&self, _: String, _: Vec<u8>, _: u8) {}

    async fn queue_local_publish_retained(&self, topic: String, payload: Vec<u8>, _: u8) {
        self.retained_publishes
            .lock()
            .unwrap()
            .push((topic, payload));
    }

    async fn queue_retained_refresh(&self, topic: String) {
        self.retained_refreshes.lock().unwrap().push(topic);
    }
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
            &MqttRequestContext {
                response_topic: Some("$DB/_resp/client1"),
                correlation_data: Some(b"corr-123"),
                sender: None,
                client_id: None,
            },
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
            &MqttRequestContext {
                response_topic: Some("$DB/_resp/client1"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
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
            &MqttRequestContext {
                response_topic: Some("$DB/_resp/client1"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
        )
        .await;

    assert!(response.is_some());
    let resp = response.unwrap();

    let (db_response, _) = DbResponse::try_from_be_bytes(&resp.payload).unwrap();
    assert_eq!(db_response.status(), DbStatus::InvalidPartition);
}

#[tokio::test]
async fn unsealed_partition_reserve_is_retryable_not_a_conflict() {
    use super::super::db::ClusterConstraint;

    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();
    let handler = DbRequestHandler::new(node1);
    let mut ctrl = create_test_controller(node1, MockTransport::new(node1));
    ctrl.register_peer(node2);
    ctrl.register_peer(node3);

    let mut map = PartitionMap::default();
    for i in 0..NUM_PARTITIONS {
        map.set(
            PartitionId::new(i).unwrap(),
            crate::cluster::PartitionAssignment {
                primary: Some(node1),
                replicas: vec![node2, node3],
                epoch: Epoch::new(1),
            },
        );
    }
    ctrl.update_partition_map(map);
    for i in 0..NUM_PARTITIONS {
        ctrl.become_primary(PartitionId::new(i).unwrap(), Epoch::new(1));
    }
    ctrl.constraint_add(&ClusterConstraint::unique("users", "uniq_email", "email").unwrap())
        .await
        .unwrap();

    let payload =
        serde_json::to_vec(&serde_json::json!({"id": "u1", "email": "free@x.com"})).unwrap();
    let response = handler
        .handle_publish(
            &mut ctrl,
            "$DB/users/create",
            &payload,
            &MqttRequestContext {
                response_topic: Some("resp/t"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "error");
    assert_eq!(
        json["code"], 503,
        "a reserve that fails only because the partition is unsealed is transient/retryable (503), \
         not a permanent unique-constraint violation (409)"
    );
    assert!(
        ctrl.db_get("users", "u1").is_none(),
        "no record should be created for a fail-closed reserve"
    );
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
        .handle_publish(
            &mut ctrl,
            topic,
            &payload,
            &MqttRequestContext {
                response_topic: None,
                correlation_data: None,
                sender: None,
                client_id: None,
            },
        )
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
            &MqttRequestContext {
                response_topic: Some("$DB/_resp/client1"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
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
            &MqttRequestContext {
                response_topic: Some("$DB/_resp/client1"),
                correlation_data: None,
                sender: Some("bob"),
                client_id: None,
            },
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
            &MqttRequestContext {
                response_topic: Some("$DB/_resp/client1"),
                correlation_data: None,
                sender: Some("alice"),
                client_id: None,
            },
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
            &MqttRequestContext {
                response_topic: Some("$DB/_resp/client1"),
                correlation_data: None,
                sender: Some("bob"),
                client_id: None,
            },
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
            &MqttRequestContext {
                response_topic: Some("$DB/_resp/client1"),
                correlation_data: None,
                sender: Some("alice"),
                client_id: None,
            },
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "ok");
    assert_eq!(json["data"]["deleted"], true);
}

#[tokio::test]
async fn delete_releases_unique_claim() {
    use super::super::db::ClusterConstraint;

    let node1 = NodeId::validated(1).unwrap();
    let handler = DbRequestHandler::new(node1);
    let mut ctrl = setup_controller_all_partitions();
    ctrl.constraint_add(&ClusterConstraint::unique("users", "uniq_email", "email").unwrap())
        .await
        .unwrap();

    let data_bytes = serde_json::to_vec(&serde_json::json!({"email": "a@x.com"})).unwrap();
    let rec = ctrl
        .db_create("users", "u1", &data_bytes, 1000)
        .await
        .unwrap();
    let value = serde_json::to_vec(&serde_json::json!("a@x.com")).unwrap();
    ctrl.stores()
        .db_unique
        .reassert("users", "email", &value, "u1", rec.partition(), 1000);
    assert!(
        ctrl.stores().unique_get("users", "email", &value).is_some(),
        "committed claim exists before delete"
    );

    let ctx = MqttRequestContext {
        response_topic: Some("resp/t"),
        correlation_data: None,
        sender: None,
        client_id: None,
    };
    let resp = handler
        .handle_publish(&mut ctrl, "$DB/users/u1/delete", &[], &ctx)
        .await
        .unwrap();
    assert_eq!(parse_json_response(&resp.payload)["data"]["deleted"], true);

    assert!(
        ctrl.stores().unique_get("users", "email", &value).is_none(),
        "deleting the record must release its committed unique claim, else the value is wedged \
         forever (TTL never reclaims a committed claim)"
    );
}

#[tokio::test]
async fn ttl_reap_releases_unique_claim_and_removes_row() {
    use super::super::db::ClusterConstraint;

    let mut ctrl = setup_controller_all_partitions();
    ctrl.constraint_add(&ClusterConstraint::unique("users", "uniq_email", "email").unwrap())
        .await
        .unwrap();

    let data_bytes =
        serde_json::to_vec(&serde_json::json!({"email": "a@x.com", "_expires_at": 100})).unwrap();
    let rec = ctrl
        .db_create("users", "u1", &data_bytes, 1000)
        .await
        .unwrap();
    let value = serde_json::to_vec(&serde_json::json!("a@x.com")).unwrap();
    ctrl.stores()
        .db_unique
        .reassert("users", "email", &value, "u1", rec.partition(), 1000);
    assert!(ctrl.stores().unique_get("users", "email", &value).is_some());

    let reaped = ctrl.reap_expired_ttl(1_000_000).await;
    assert_eq!(reaped, 1, "the expired row must be reaped");

    assert!(
        ctrl.db_get("users", "u1").is_none(),
        "the expired row must be removed"
    );
    assert!(
        ctrl.stores().unique_get("users", "email", &value).is_none(),
        "the TTL reap must release the committed unique claim so the value is reclaimable"
    );
}

#[tokio::test]
async fn ttl_reap_leaves_unexpired_row() {
    let mut ctrl = setup_controller_all_partitions();

    let data_bytes =
        serde_json::to_vec(&serde_json::json!({"v": 1, "_expires_at": 9_000_000_000u64})).unwrap();
    ctrl.db_create("widgets", "w1", &data_bytes, 1000)
        .await
        .unwrap();

    let reaped = ctrl.reap_expired_ttl(1000).await;
    assert_eq!(reaped, 0, "a not-yet-expired row must not be reaped");
    assert!(ctrl.db_get("widgets", "w1").is_some());
}

fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn stored_json(ctrl: &NodeController<MockTransport>, entity: &str, id: &str) -> serde_json::Value {
    let rec = ctrl.db_get(entity, id).expect("record stored");
    serde_json::from_slice(&rec.data).unwrap()
}

#[tokio::test]
async fn json_create_on_primary_turns_ttl_secs_into_expiry() {
    let node1 = NodeId::validated(1).unwrap();
    let handler = DbRequestHandler::new(node1);
    let mut ctrl = setup_controller_all_partitions();

    let before = unix_now_secs();
    let payload = serde_json::to_vec(&serde_json::json!({"id": "t1", "ttl_secs": 5})).unwrap();
    let response = handler
        .handle_publish(
            &mut ctrl,
            "$DB/ttldocs/create",
            &payload,
            &MqttRequestContext {
                response_topic: Some("$DB/_resp/client1"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
        )
        .await
        .unwrap();
    let after = unix_now_secs();

    let json = parse_json_response(&response.payload);
    assert_eq!(json["status"], "ok", "create failed: {json}");
    assert!(json["data"].get("ttl_secs").is_none(), "response: {json}");

    let stored = stored_json(&ctrl, "ttldocs", "t1");
    assert!(stored.get("ttl_secs").is_none(), "stored: {stored}");
    let expires_at = stored["_expires_at"]
        .as_u64()
        .unwrap_or_else(|| panic!("no numeric _expires_at in {stored}"));
    assert!(
        (before + 5..=after + 5).contains(&expires_at),
        "expires_at {expires_at} outside [{}, {}]",
        before + 5,
        after + 5
    );

    assert_eq!(ctrl.reap_expired_ttl(expires_at - 1).await, 0);
    assert_eq!(ctrl.reap_expired_ttl(expires_at + 1).await, 1);
    assert!(ctrl.db_get("ttldocs", "t1").is_none());
}

#[tokio::test]
async fn json_create_with_huge_ttl_secs_never_expires() {
    let node1 = NodeId::validated(1).unwrap();
    let handler = DbRequestHandler::new(node1);
    let mut ctrl = setup_controller_all_partitions();

    let payload =
        serde_json::to_vec(&serde_json::json!({"id": "t1", "ttl_secs": u64::MAX})).unwrap();
    let response = handler
        .handle_publish(
            &mut ctrl,
            "$DB/ttldocs/create",
            &payload,
            &MqttRequestContext {
                response_topic: Some("$DB/_resp/client1"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
        )
        .await
        .unwrap();
    let json = parse_json_response(&response.payload);
    assert_eq!(json["status"], "ok", "create failed: {json}");

    let stored = stored_json(&ctrl, "ttldocs", "t1");
    assert_eq!(
        stored["_expires_at"].as_u64(),
        Some(u64::MAX),
        "stored: {stored}"
    );
    assert_eq!(ctrl.reap_expired_ttl(unix_now_secs() + 1).await, 0);
    assert!(ctrl.db_get("ttldocs", "t1").is_some());
}

#[tokio::test]
async fn vault_json_create_forwarded_to_primary_keeps_ttl_expiry() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let ownership = ownership_config("notes", "userId");
    let key_store = Arc::new(mqdb_vault::VaultKeyStore::new());
    let derived = mqdb_vault::VaultCrypto::derive_with_raw_key(
        "passphrase",
        &mqdb_vault::VaultCrypto::generate_salt(),
    );
    key_store.set("alice", derived.1);
    let handler = DbRequestHandler::new(node1)
        .with_ownership(Arc::clone(&ownership))
        .with_vault_key_store(key_store);

    let partition = data_partition("notes", "n1");
    let assignment = crate::cluster::PartitionAssignment {
        primary: Some(node2),
        replicas: vec![],
        epoch: Epoch::new(1),
    };

    let outbox = Arc::new(Mutex::new(Vec::new()));
    let transport = MockTransport {
        node_id: node1,
        inbox: Arc::new(Mutex::new(VecDeque::new())),
        outbox: Arc::clone(&outbox),
        retained_publishes: Arc::new(Mutex::new(Vec::new())),
        retained_refreshes: Arc::new(Mutex::new(Vec::new())),
    };
    let mut receiver = create_test_controller(node1, transport);
    receiver.set_ownership(Arc::clone(&ownership));
    let mut map = PartitionMap::default();
    map.set(partition, assignment.clone());
    receiver.update_partition_map(map.clone());

    let payload = serde_json::to_vec(
        &serde_json::json!({"id": "n1", "userId": "alice", "body": "x", "ttl_secs": 5}),
    )
    .unwrap();
    let before = unix_now_secs();
    let response = handler
        .handle_publish(
            &mut receiver,
            "$DB/notes/create",
            &payload,
            &MqttRequestContext {
                response_topic: Some("$DB/_resp/alice"),
                correlation_data: None,
                sender: Some("alice"),
                client_id: Some("alice-client"),
            },
        )
        .await;
    assert!(response.is_none(), "a non-primary forwards the create");

    let request = outbox
        .lock()
        .unwrap()
        .iter()
        .find_map(|(to, m)| match m {
            ClusterMessage::JsonDbRequest { request, .. } if *to == node2 => Some(request.clone()),
            _ => None,
        })
        .expect("create forwarded to the primary");

    let mut primary = create_test_controller(node2, MockTransport::new(node2));
    primary.set_ownership(Arc::clone(&ownership));
    primary.become_primary(partition, Epoch::new(1));
    primary.update_partition_map(map);
    primary
        .handle_json_db_request(node1, partition, &request)
        .await;

    let after = unix_now_secs();

    let stored = stored_json(&primary, "notes", "n1");
    assert_ne!(
        stored["body"], "x",
        "vault must encrypt the forwarded body: {stored}"
    );
    assert!(stored.get("ttl_secs").is_none(), "stored: {stored}");
    let expires_at = stored["_expires_at"]
        .as_u64()
        .unwrap_or_else(|| panic!("no numeric _expires_at in {stored}"));
    assert!(
        (before + 5..=after + 5).contains(&expires_at),
        "expires_at {expires_at} outside [{}, {}]",
        before + 5,
        after + 5
    );
    assert_eq!(primary.reap_expired_ttl(expires_at - 1).await, 0);
    assert_eq!(primary.reap_expired_ttl(expires_at + 1).await, 1);
    assert!(primary.db_get("notes", "n1").is_none());
}

#[tokio::test]
async fn ttl_reap_pass_fills_its_budget_with_rows_it_owns() {
    let node1 = NodeId::validated(1).unwrap();
    let mut ctrl = create_test_controller(node1, MockTransport::new(node1));
    let half = NUM_PARTITIONS / 2;
    let mut map = PartitionMap::default();
    for i in 0..half {
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

    let expired = serde_json::to_vec(&serde_json::json!({"_expires_at": 100})).unwrap();
    let ids: Vec<String> = (0..6000).map(|i| format!("row-{i}")).collect();
    let owned = ids
        .iter()
        .filter(|id| data_partition("ttldocs", id).get() < half)
        .count();
    assert!(
        owned > 2048,
        "need more owned rows than two passes: {owned}"
    );
    for id in &ids {
        ctrl.stores()
            .db_create_replicated("ttldocs", id, &expired, 1000)
            .unwrap();
    }

    assert_eq!(ctrl.reap_expired_ttl(1_000).await, 1024);
    assert_eq!(ctrl.reap_expired_ttl(1_000).await, 1024);
}

#[tokio::test]
async fn ttl_reap_budget_skips_partitions_mapped_but_not_held_as_primary() {
    let node1 = NodeId::validated(1).unwrap();
    let mut ctrl = create_test_controller(node1, MockTransport::new(node1));
    let half = NUM_PARTITIONS / 2;
    let mut map = PartitionMap::default();
    for i in 0..NUM_PARTITIONS {
        let partition = PartitionId::new(i).unwrap();
        if i < half {
            map.set(
                partition,
                crate::cluster::PartitionAssignment {
                    primary: Some(node1),
                    replicas: vec![],
                    epoch: Epoch::new(1),
                },
            );
        } else {
            ctrl.become_primary(partition, Epoch::new(1));
        }
    }
    ctrl.update_partition_map(map);

    let expired = serde_json::to_vec(&serde_json::json!({"_expires_at": 100})).unwrap();
    let ids: Vec<String> = (0..6000).map(|i| format!("row-{i}")).collect();
    let held = ids
        .iter()
        .filter(|id| data_partition("ttldocs", id).get() >= half)
        .count();
    assert!(held > 2048, "need more held rows than two passes: {held}");
    for id in &ids {
        ctrl.stores()
            .db_create_replicated("ttldocs", id, &expired, 1000)
            .unwrap();
    }

    assert_eq!(ctrl.reap_expired_ttl(1_000).await, 1024);
    assert_eq!(ctrl.reap_expired_ttl(1_000).await, 1024);
}

#[tokio::test]
async fn cluster_update_delete_expected_version_cas() {
    let node1 = NodeId::validated(1).unwrap();
    let handler = DbRequestHandler::new(node1);
    let mut ctrl = setup_controller_all_partitions();

    let data = serde_json::to_vec(&serde_json::json!({"n": 0, "_version": 1})).unwrap();
    ctrl.db_create("docs", "d", &data, 1000).await.unwrap();

    let ctx = MqttRequestContext {
        response_topic: Some("resp/t"),
        correlation_data: None,
        sender: None,
        client_id: None,
    };

    let stale = serde_json::to_vec(&serde_json::json!({"n": 1, "_expected_version": 99})).unwrap();
    let r = handler
        .handle_publish(&mut ctrl, "$DB/docs/d/update", &stale, &ctx)
        .await
        .unwrap();
    assert_eq!(parse_json_response(&r.payload)["code"], 412);

    let ok = serde_json::to_vec(&serde_json::json!({"n": 1, "_expected_version": 1})).unwrap();
    let r = handler
        .handle_publish(&mut ctrl, "$DB/docs/d/update", &ok, &ctx)
        .await
        .unwrap();
    assert_eq!(parse_json_response(&r.payload)["status"], "ok");

    let r = handler
        .handle_publish(&mut ctrl, "$DB/docs/d/update", &ok, &ctx)
        .await
        .unwrap();
    assert_eq!(
        parse_json_response(&r.payload)["code"],
        412,
        "the version bumped, so the old expected version is now stale"
    );

    let bad_update =
        serde_json::to_vec(&serde_json::json!({"n": 2, "_expected_version": "2"})).unwrap();
    let r = handler
        .handle_publish(&mut ctrl, "$DB/docs/d/update", &bad_update, &ctx)
        .await
        .unwrap();
    assert_eq!(
        parse_json_response(&r.payload)["code"],
        400,
        "a malformed _expected_version must be rejected, not silently applied"
    );

    let bad_delete = serde_json::to_vec(&serde_json::json!({"_expected_version": "2"})).unwrap();
    let r = handler
        .handle_publish(&mut ctrl, "$DB/docs/d/delete", &bad_delete, &ctx)
        .await
        .unwrap();
    assert_eq!(
        parse_json_response(&r.payload)["code"],
        400,
        "a malformed _expected_version must not silently disable the delete CAS"
    );

    let dstale = serde_json::to_vec(&serde_json::json!({"_expected_version": 1})).unwrap();
    let r = handler
        .handle_publish(&mut ctrl, "$DB/docs/d/delete", &dstale, &ctx)
        .await
        .unwrap();
    assert_eq!(parse_json_response(&r.payload)["code"], 412);

    let dok = serde_json::to_vec(&serde_json::json!({"_expected_version": 2})).unwrap();
    let r = handler
        .handle_publish(&mut ctrl, "$DB/docs/d/delete", &dok, &ctx)
        .await
        .unwrap();
    assert_eq!(parse_json_response(&r.payload)["data"]["deleted"], true);
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
            &MqttRequestContext {
                response_topic: Some("$DB/_resp/client1"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
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
            &MqttRequestContext {
                response_topic: Some("$DB/_resp/client1"),
                correlation_data: None,
                sender: Some("alice"),
                client_id: None,
            },
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "ok");
    let items = json["data"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["userId"], "alice");
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
            &MqttRequestContext {
                response_topic: Some("$DB/_resp/client1"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
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
            &MqttRequestContext {
                response_topic: Some("$DB/_resp/client1"),
                correlation_data: None,
                sender: Some("eve"),
                client_id: None,
            },
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
            &MqttRequestContext {
                response_topic: Some("$DB/_resp/c1"),
                correlation_data: None,
                sender: Some("admin"),
                client_id: None,
            },
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
            &MqttRequestContext {
                response_topic: Some("$DB/_resp/c1"),
                correlation_data: None,
                sender: Some("admin"),
                client_id: None,
            },
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "ok");
    assert_eq!(json["data"]["deleted"], true);
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
            &MqttRequestContext {
                response_topic: Some("$DB/_resp/c1"),
                correlation_data: None,
                sender: Some("admin"),
                client_id: None,
            },
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
            &MqttRequestContext {
                response_topic: Some("$DB/_resp/client1"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
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
            &MqttRequestContext {
                response_topic: Some("$DB/_resp/client1"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
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
            &MqttRequestContext {
                response_topic: Some("$DB/_resp/client1"),
                correlation_data: None,
                sender: Some("alice"),
                client_id: None,
            },
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
            &MqttRequestContext {
                response_topic: Some("$DB/_resp/client1"),
                correlation_data: None,
                sender: Some("bob"),
                client_id: None,
            },
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
    )
    .unwrap();
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
            &MqttRequestContext {
                response_topic: Some("resp/t"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
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
    )
    .unwrap();
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
            &MqttRequestContext {
                response_topic: Some("resp/t"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
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
    )
    .unwrap();
    ctrl.constraint_add(&fk).await.unwrap();

    let update_payload =
        serde_json::to_vec(&serde_json::json!({"author_id": "nonexistent"})).unwrap();

    let response = handler
        .handle_publish(
            &mut ctrl,
            "$DB/posts/p1/update",
            &update_payload,
            &MqttRequestContext {
                response_topic: Some("resp/t"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
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
    )
    .unwrap();
    ctrl.constraint_add(&fk).await.unwrap();

    let response = handler
        .handle_publish(
            &mut ctrl,
            "$DB/users/u1/delete",
            &[],
            &MqttRequestContext {
                response_topic: Some("resp/t"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
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
    )
    .unwrap();
    ctrl.constraint_add(&fk).await.unwrap();

    let response = handler
        .handle_publish(
            &mut ctrl,
            "$DB/users/u1/delete",
            &[],
            &MqttRequestContext {
                response_topic: Some("resp/t"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "ok");
    assert_eq!(json["data"]["deleted"], true);

    assert!(ctrl.db_get("users", "u1").is_none());
    assert!(ctrl.db_get("posts", "p1").is_none());
    assert!(ctrl.db_get("posts", "p2").is_none());
}

#[tokio::test]
async fn cascade_delete_releases_child_unique_claim() {
    use super::super::db::{ClusterConstraint, OnDeleteAction};

    let node1 = NodeId::validated(1).unwrap();
    let handler = DbRequestHandler::new(node1);
    let mut ctrl = setup_controller_all_partitions();

    ctrl.db_create(
        "users",
        "u1",
        &serde_json::to_vec(&serde_json::json!({"name": "Alice"})).unwrap(),
        1000,
    )
    .await
    .unwrap();

    let post = serde_json::to_vec(
        &serde_json::json!({"title": "P1", "author_id": "u1", "slug": "post-1"}),
    )
    .unwrap();
    let rec = ctrl.db_create("posts", "p1", &post, 1000).await.unwrap();

    let slug = serde_json::to_vec(&serde_json::json!("post-1")).unwrap();
    ctrl.stores()
        .db_unique
        .reassert("posts", "slug", &slug, "p1", rec.partition(), 1000);
    assert!(ctrl.stores().unique_get("posts", "slug", &slug).is_some());

    ctrl.constraint_add(&ClusterConstraint::unique("posts", "posts_slug", "slug").unwrap())
        .await
        .unwrap();
    ctrl.constraint_add(
        &ClusterConstraint::foreign_key(
            "posts",
            "posts_author_fk",
            "author_id",
            "users",
            "id",
            OnDeleteAction::Cascade,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let resp = handler
        .handle_publish(
            &mut ctrl,
            "$DB/users/u1/delete",
            &[],
            &MqttRequestContext {
                response_topic: Some("resp/t"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(parse_json_response(&resp.payload)["data"]["deleted"], true);

    assert!(ctrl.db_get("posts", "p1").is_none(), "child cascaded away");
    assert!(
        ctrl.stores().unique_get("posts", "slug", &slug).is_none(),
        "cascade-deleting a child must release its committed unique claim, else its value is wedged"
    );
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
    )
    .unwrap();
    ctrl.constraint_add(&fk).await.unwrap();

    let response = handler
        .handle_publish(
            &mut ctrl,
            "$DB/users/u1/delete",
            &[],
            &MqttRequestContext {
                response_topic: Some("resp/t"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "ok");
    assert_eq!(json["data"]["deleted"], true);

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
    )
    .unwrap();
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
            &MqttRequestContext {
                response_topic: Some("resp/t"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
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
    )
    .unwrap();
    ctrl.constraint_add(&fk).await.unwrap();

    let response = handler
        .handle_publish(
            &mut ctrl,
            "$DB/users/u1/delete",
            &[],
            &MqttRequestContext {
                response_topic: Some("resp/t"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "ok");
    assert_eq!(json["data"]["deleted"], true);
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
    )
    .unwrap();
    ctrl.constraint_add(&fk_posts).await.unwrap();

    let fk_comments = ClusterConstraint::foreign_key(
        "comments",
        "comments_post_fk",
        "post_id",
        "posts",
        "id",
        OnDeleteAction::Cascade,
    )
    .unwrap();
    ctrl.constraint_add(&fk_comments).await.unwrap();

    let response = handler
        .handle_publish(
            &mut ctrl,
            "$DB/users/u1/delete",
            &[],
            &MqttRequestContext {
                response_topic: Some("resp/t"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
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
    )
    .unwrap();
    ctrl.constraint_add(&fk_a_to_b).await.unwrap();

    let fk_b_to_a = ClusterConstraint::foreign_key(
        "entity_b",
        "b_refs_a",
        "ref_a",
        "entity_a",
        "id",
        OnDeleteAction::Cascade,
    )
    .unwrap();
    ctrl.constraint_add(&fk_b_to_a).await.unwrap();

    let response = handler
        .handle_publish(
            &mut ctrl,
            "$DB/entity_a/a1/delete",
            &[],
            &MqttRequestContext {
                response_topic: Some("resp/t"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
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
        )
        .unwrap();
        ctrl.constraint_add(&fk).await.unwrap();
    }

    let response = handler
        .handle_publish(
            &mut ctrl,
            "$DB/e0/r0/delete",
            &[],
            &MqttRequestContext {
                response_topic: Some("resp/t"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
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
    )
    .unwrap();
    ctrl.constraint_add(&fk).await.unwrap();

    let update_payload = serde_json::to_vec(&serde_json::json!({"author_id": "b"})).unwrap();
    let response = handler
        .handle_publish(
            &mut ctrl,
            "$DB/posts/p1/update",
            &update_payload,
            &MqttRequestContext {
                response_topic: Some("resp/t"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
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
    )
    .unwrap();
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
            &MqttRequestContext {
                response_topic: Some("resp/t"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
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
    )
    .unwrap();
    ctrl.constraint_add(&fk_author).await.unwrap();

    let fk_cat = ClusterConstraint::foreign_key(
        "posts",
        "posts_cat_fk",
        "category_id",
        "categories",
        "id",
        OnDeleteAction::Restrict,
    )
    .unwrap();
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
            &MqttRequestContext {
                response_topic: Some("resp/t"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
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
            &MqttRequestContext {
                response_topic: Some("resp/t"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
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
            &MqttRequestContext {
                response_topic: Some("resp/t"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
        )
        .await;
    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "error");
    assert_eq!(json["code"], 409);
}

fn share_ctx(sender: &'static str) -> MqttRequestContext<'static> {
    MqttRequestContext {
        response_topic: Some("$DB/_resp/c"),
        correlation_data: None,
        sender: Some(sender),
        client_id: None,
    }
}

async fn share_setup() -> (
    DbRequestHandler,
    NodeController<MockTransport>,
    Arc<OwnershipConfig>,
) {
    let node1 = NodeId::validated(1).unwrap();
    let ownership = ownership_config("diagrams", "userId");
    let handler = DbRequestHandler::new(node1).with_ownership(Arc::clone(&ownership));
    let mut ctrl = setup_controller_all_partitions();
    ctrl.set_ownership(Arc::clone(&ownership));
    let data = serde_json::to_vec(&serde_json::json!({"userId": "alice", "title": "D"})).unwrap();
    ctrl.db_create("diagrams", "d1", &data, 1000).await.unwrap();
    (handler, ctrl, ownership)
}

fn grant_payload(grantee: &str, permission: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({"grantee": grantee, "permission": permission})).unwrap()
}

async fn resp_json(
    handler: &DbRequestHandler,
    ctrl: &mut NodeController<MockTransport>,
    topic: &str,
    payload: &[u8],
    sender: &'static str,
) -> serde_json::Value {
    let resp = handler
        .handle_publish(ctrl, topic, payload, &share_ctx(sender))
        .await
        .unwrap();
    parse_json_response(&resp.payload)
}

#[tokio::test]
async fn cluster_share_grants_view_then_edit_then_revoke() {
    let (handler, mut ctrl, _) = share_setup().await;

    assert_eq!(
        resp_json(&handler, &mut ctrl, "$DB/diagrams/d1", &[], "bob").await["code"],
        403
    );
    assert_eq!(
        resp_json(
            &handler,
            &mut ctrl,
            "$DB/diagrams/d1/share",
            &grant_payload("bob", "view"),
            "alice"
        )
        .await["status"],
        "ok"
    );
    assert_eq!(
        resp_json(&handler, &mut ctrl, "$DB/diagrams/d1", &[], "bob").await["status"],
        "ok"
    );
    let upd = serde_json::to_vec(&serde_json::json!({"title": "x"})).unwrap();
    assert_eq!(
        resp_json(&handler, &mut ctrl, "$DB/diagrams/d1/update", &upd, "bob").await["code"],
        403
    );

    resp_json(
        &handler,
        &mut ctrl,
        "$DB/diagrams/d1/share",
        &grant_payload("bob", "edit"),
        "alice",
    )
    .await;
    assert_eq!(
        resp_json(&handler, &mut ctrl, "$DB/diagrams/d1/update", &upd, "bob").await["status"],
        "ok"
    );

    let unshare = serde_json::to_vec(&serde_json::json!({"grantee": "bob"})).unwrap();
    resp_json(
        &handler,
        &mut ctrl,
        "$DB/diagrams/d1/unshare",
        &unshare,
        "alice",
    )
    .await;
    assert_eq!(
        resp_json(&handler, &mut ctrl, "$DB/diagrams/d1", &[], "bob").await["code"],
        403
    );
}

#[tokio::test]
async fn cluster_shares_listing_owner_only() {
    let (handler, mut ctrl, _) = share_setup().await;
    resp_json(
        &handler,
        &mut ctrl,
        "$DB/diagrams/d1/share",
        &grant_payload("bob", "view"),
        "alice",
    )
    .await;

    let listed = resp_json(&handler, &mut ctrl, "$DB/diagrams/d1/shares", &[], "alice").await;
    assert_eq!(listed["status"], "ok");
    assert_eq!(listed["data"].as_array().unwrap().len(), 1);
    assert_eq!(listed["data"][0]["grantee"], "bob");
    assert_eq!(
        resp_json(&handler, &mut ctrl, "$DB/diagrams/d1/shares", &[], "bob").await["code"],
        403
    );
}

#[tokio::test]
async fn cluster_non_owner_cannot_share() {
    let (handler, mut ctrl, _) = share_setup().await;
    assert_eq!(
        resp_json(
            &handler,
            &mut ctrl,
            "$DB/diagrams/d1/share",
            &grant_payload("bob", "view"),
            "carol"
        )
        .await["code"],
        403
    );
}

#[tokio::test]
async fn cluster_delete_clears_grants() {
    let (handler, mut ctrl, _) = share_setup().await;
    resp_json(
        &handler,
        &mut ctrl,
        "$DB/diagrams/d1/share",
        &grant_payload("bob", "view"),
        "alice",
    )
    .await;
    assert_eq!(ctrl.db_list("_shares").len(), 1);
    resp_json(&handler, &mut ctrl, "$DB/diagrams/d1/delete", &[], "alice").await;
    assert!(ctrl.db_list("_shares").is_empty(), "delete clears grants");
}

#[tokio::test]
async fn cluster_direct_shares_crud_forbidden() {
    let (handler, mut ctrl, _) = share_setup().await;
    for topic in [
        "$DB/_shares/create",
        "$DB/_shares/some-id",
        "$DB/_shares/list",
    ] {
        assert_eq!(
            resp_json(&handler, &mut ctrl, topic, b"{}", "alice").await["code"],
            403,
            "{topic} must be forbidden"
        );
    }
}

#[tokio::test]
async fn cluster_shared_hydrates_resources_for_grantee() {
    let (handler, mut ctrl, _) = share_setup().await;
    let d2 = serde_json::to_vec(&serde_json::json!({"userId": "alice", "title": "D2"})).unwrap();
    ctrl.db_create("diagrams", "d2", &d2, 1000).await.unwrap();

    resp_json(
        &handler,
        &mut ctrl,
        "$DB/diagrams/d1/share",
        &grant_payload("bob", "view"),
        "alice",
    )
    .await;
    resp_json(
        &handler,
        &mut ctrl,
        "$DB/diagrams/d2/share",
        &grant_payload("bob", "edit"),
        "alice",
    )
    .await;
    resp_json(
        &handler,
        &mut ctrl,
        "$DB/diagrams/d1/share",
        &grant_payload("carol", "view"),
        "alice",
    )
    .await;

    let shared = resp_json(&handler, &mut ctrl, "$DB/diagrams/shared", &[], "bob").await;
    assert_eq!(shared["status"], "ok");
    let rows = shared["data"].as_array().unwrap();
    assert_eq!(rows.len(), 2, "bob sees the 2 resources shared with him");
    let ids: Vec<&str> = rows.iter().filter_map(|r| r["id"].as_str()).collect();
    assert!(ids.contains(&"d1") && ids.contains(&"d2"));
    for row in rows {
        assert!(
            row["title"].is_string(),
            "shared returns hydrated resource records, not grant rows: {row}"
        );
        assert!(
            row["resource_id"].is_null(),
            "hydrated rows must not carry grant fields: {row}"
        );
        assert_eq!(row["userId"], "alice");
    }
}

#[tokio::test]
async fn cluster_shared_scatter_primary_hydrates_co_located_resource() {
    let (handler, mut ctrl, _) = share_setup().await;
    resp_json(
        &handler,
        &mut ctrl,
        "$DB/diagrams/d1/share",
        &grant_payload("bob", "view"),
        "alice",
    )
    .await;

    let filters = vec![
        mqdb_core::Filter::new(
            "resource_entity".to_string(),
            mqdb_core::FilterOp::Eq,
            serde_json::json!("diagrams"),
        ),
        mqdb_core::Filter::new(
            "grantee".to_string(),
            mqdb_core::FilterOp::Eq,
            serde_json::json!("bob"),
        ),
    ];
    let payload = serde_json::to_vec(&serde_json::json!({
        "filters": filters,
        "hydrate_entity": "diagrams",
    }))
    .unwrap();

    let out = ctrl.handle_json_list_local(mqdb_core::types::SHARES_ENTITY, &payload);
    let response: serde_json::Value = serde_json::from_slice(&out).unwrap();
    let items = response["data"].as_array().unwrap();
    assert_eq!(
        items.len(),
        1,
        "one grant for bob, hydrated to one resource"
    );
    assert_eq!(
        items[0]["id"], "d1",
        "hydrated item is keyed by resource id"
    );
    assert_eq!(items[0]["data"]["title"], "D");
    assert!(
        items[0]["data"]["resource_id"].is_null(),
        "primary returns the resource record, not the grant row: {}",
        items[0]
    );
}

#[tokio::test]
async fn cluster_share_and_read_forward_to_resource_primary() {
    use super::super::protocol::JsonDbOp;

    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let ownership = ownership_config("diagrams", "userId");
    let handler = DbRequestHandler::new(node1).with_ownership(Arc::clone(&ownership));

    let outbox = Arc::new(Mutex::new(Vec::new()));
    let transport = MockTransport {
        node_id: node1,
        inbox: Arc::new(Mutex::new(VecDeque::new())),
        outbox: Arc::clone(&outbox),
        retained_publishes: Arc::new(Mutex::new(Vec::new())),
        retained_refreshes: Arc::new(Mutex::new(Vec::new())),
    };
    let mut ctrl = create_test_controller(node1, transport);
    ctrl.set_ownership(Arc::clone(&ownership));

    let rp = data_partition("diagrams", "d1");
    let mut map = PartitionMap::default();
    map.set(
        rp,
        crate::cluster::PartitionAssignment {
            primary: Some(node2),
            replicas: vec![],
            epoch: Epoch::new(1),
        },
    );
    ctrl.update_partition_map(map);

    let read = handler
        .handle_publish(&mut ctrl, "$DB/diagrams/d1", &[], &share_ctx("bob"))
        .await;
    assert!(read.is_none(), "graded read forwards, no local response");
    let share = handler
        .handle_publish(
            &mut ctrl,
            "$DB/diagrams/d1/share",
            &grant_payload("bob", "view"),
            &share_ctx("alice"),
        )
        .await;
    assert!(share.is_none(), "share forwards, no local response");

    let sent = outbox.lock().unwrap();
    assert_eq!(sent.len(), 2, "both forwarded");
    let ops: Vec<JsonDbOp> = sent
        .iter()
        .filter_map(|(to, m)| {
            assert_eq!(*to, node2, "forwarded to the resource primary");
            match m {
                ClusterMessage::JsonDbRequest { request, .. } => Some(request.op),
                _ => None,
            }
        })
        .collect();
    assert!(ops.contains(&JsonDbOp::Read), "read was forwarded");
    assert!(ops.contains(&JsonDbOp::Share), "share was forwarded");
}

async fn forwarded_read_response(
    ctrl: &mut NodeController<MockTransport>,
    outbox: &Arc<Mutex<Vec<(NodeId, ClusterMessage)>>>,
    from: NodeId,
    partition: PartitionId,
    sender: &str,
) -> serde_json::Value {
    let req = super::super::protocol::JsonDbRequest {
        request_id: 1,
        op: super::super::protocol::JsonDbOp::Read,
        entity: "diagrams".to_string(),
        id: Some("d1".to_string()),
        payload: vec![],
        response_topic: "resp/t".to_string(),
        correlation_data: None,
        sender: Some(sender.to_string()),
        client_id: None,
    };
    ctrl.handle_json_db_request(from, partition, &req).await;
    let (_, msg) = outbox.lock().unwrap().pop().expect("a response was sent");
    match msg {
        ClusterMessage::JsonDbResponse(r) => parse_json_response(&r.payload),
        other => panic!("expected JsonDbResponse, got {other:?}"),
    }
}

#[tokio::test]
async fn cluster_forwarded_read_graded_against_grant_on_primary() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let ownership = ownership_config("diagrams", "userId");

    let outbox = Arc::new(Mutex::new(Vec::new()));
    let transport = MockTransport {
        node_id: node1,
        inbox: Arc::new(Mutex::new(VecDeque::new())),
        outbox: Arc::clone(&outbox),
        retained_publishes: Arc::new(Mutex::new(Vec::new())),
        retained_refreshes: Arc::new(Mutex::new(Vec::new())),
    };
    let mut ctrl = create_test_controller(node1, transport);
    ctrl.set_ownership(Arc::clone(&ownership));

    let rp = data_partition("diagrams", "d1");
    ctrl.become_primary(rp, Epoch::new(1));
    let mut map = PartitionMap::default();
    map.set(
        rp,
        crate::cluster::PartitionAssignment {
            primary: Some(node1),
            replicas: vec![],
            epoch: Epoch::new(1),
        },
    );
    ctrl.update_partition_map(map);

    let data = serde_json::to_vec(&serde_json::json!({"userId": "alice", "title": "D"})).unwrap();
    ctrl.db_create("diagrams", "d1", &data, 1000).await.unwrap();
    ctrl.handle_share_local(
        "diagrams",
        "d1",
        &grant_payload("bob", "view"),
        Some("alice"),
    )
    .await;

    let bob = forwarded_read_response(&mut ctrl, &outbox, node2, rp, "bob").await;
    assert_eq!(bob["status"], "ok", "grantee read graded ok on the primary");

    let carol = forwarded_read_response(&mut ctrl, &outbox, node2, rp, "carol").await;
    assert_eq!(carol["code"], 403, "non-grantee denied on the primary");
}

#[tokio::test]
async fn cluster_cascade_share_grants_closure() {
    use super::super::db::{ClusterConstraint, OnDeleteAction};

    let node1 = NodeId::validated(1).unwrap();
    let ownership = ownership_config("diagrams", "userId");
    let handler = DbRequestHandler::new(node1).with_ownership(Arc::clone(&ownership));
    let mut ctrl = setup_controller_all_partitions();
    ctrl.set_ownership(Arc::clone(&ownership));

    let fk = ClusterConstraint::foreign_key(
        "diagrams",
        "diagrams_parent_fk",
        "parent_id",
        "diagrams",
        "id",
        OnDeleteAction::Restrict,
    )
    .unwrap();
    ctrl.constraint_add(&fk).await.unwrap();

    for (id, parent) in [("d3", None), ("d2", Some("d3")), ("d1", Some("d2"))] {
        let mut rec = serde_json::json!({"userId": "alice", "title": id});
        if let Some(p) = parent {
            rec["parent_id"] = serde_json::json!(p);
        }
        ctrl.db_create("diagrams", id, &serde_json::to_vec(&rec).unwrap(), 1000)
            .await
            .unwrap();
    }

    let payload = serde_json::to_vec(
        &serde_json::json!({"grantee": "bob", "permission": "view", "cascade": true}),
    )
    .unwrap();
    let r = resp_json(
        &handler,
        &mut ctrl,
        "$DB/diagrams/d1/share",
        &payload,
        "alice",
    )
    .await;
    assert_eq!(r["status"], "ok");
    assert_eq!(r["data"]["resources_shared"], 3, "root + 2 referenced");

    for id in ["d1", "d2", "d3"] {
        assert_eq!(
            resp_json(
                &handler,
                &mut ctrl,
                &format!("$DB/diagrams/{id}"),
                &[],
                "bob"
            )
            .await["status"],
            "ok",
            "bob reads {id} via cascade"
        );
    }
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
    )
    .unwrap();
    ctrl.constraint_add(&fk).await.unwrap();

    let response = handler
        .handle_publish(
            &mut ctrl,
            "$DB/users/u1/delete",
            &[],
            &MqttRequestContext {
                response_topic: Some("resp/t"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
        )
        .await;

    let resp = response.unwrap();
    let json = parse_json_response(&resp.payload);
    assert_eq!(json["status"], "ok");
    assert_eq!(json["data"]["deleted"], true);
    assert!(ctrl.db_get("users", "u1").is_none());
}

#[cfg(feature = "http-api")]
async fn identity_share_setup() -> (
    DbRequestHandler,
    NodeController<MockTransport>,
    Arc<mqdb_agent::http::IdentityCrypto>,
) {
    let node1 = NodeId::validated(1).unwrap();
    let ownership = ownership_config("diagrams", "userId");
    let handler = DbRequestHandler::new(node1).with_ownership(Arc::clone(&ownership));
    let mut ctrl = setup_controller_all_partitions();
    ctrl.set_ownership(Arc::clone(&ownership));
    let crypto =
        Arc::new(mqdb_agent::http::IdentityCrypto::from_external_key(&[0x42u8; 32]).unwrap());
    ctrl.set_identity_crypto(Some(Arc::clone(&crypto)));
    let data = serde_json::to_vec(&serde_json::json!({"userId": "alice", "title": "D"})).unwrap();
    ctrl.db_create("diagrams", "d1", &data, 1000).await.unwrap();
    (handler, ctrl, crypto)
}

#[cfg(feature = "http-api")]
async fn seed_identity_link(
    ctrl: &mut NodeController<MockTransport>,
    crypto: &mqdb_agent::http::IdentityCrypto,
    email: &str,
    canonical: &str,
) {
    let email_hash = crypto.blind_index("_identity_links", email);
    let link = serde_json::to_vec(&serde_json::json!({
        "id": format!("google:{canonical}"),
        "canonical_id": canonical,
        "email_hash": email_hash,
    }))
    .unwrap();
    ctrl.db_create(
        "_identity_links",
        &format!("google:{canonical}"),
        &link,
        1000,
    )
    .await
    .unwrap();
}

#[cfg(feature = "http-api")]
#[tokio::test]
async fn cluster_identity_share_resolves_canonical_grantee() {
    let (handler, mut ctrl, crypto) = identity_share_setup().await;
    seed_identity_link(&mut ctrl, &crypto, "User@Example.com", "canon-1").await;

    let out = resp_json(
        &handler,
        &mut ctrl,
        "$DB/diagrams/d1/share",
        &grant_payload("User@Example.com", "view"),
        "alice",
    )
    .await;
    assert_eq!(out["data"]["status"], "shared");
    assert_eq!(out["data"]["grantee"], "canon-1");

    assert_eq!(
        resp_json(&handler, &mut ctrl, "$DB/diagrams/d1", &[], "canon-1").await["status"],
        "ok"
    );

    let shares = resp_json(&handler, &mut ctrl, "$DB/diagrams/d1/shares", &[], "alice").await;
    let grant = &shares["data"][0];
    assert_eq!(grant["grantee"], "canon-1");
    assert_eq!(
        grant["grantee_key"],
        crypto.blind_index(mqdb_core::types::SHARES_ENTITY, "user@example.com")
    );
    assert_eq!(grant["grantee_email"], "user@example.com");
}

#[cfg(feature = "http-api")]
#[tokio::test]
async fn cluster_identity_share_unresolved_is_pending() {
    let (handler, mut ctrl, _) = identity_share_setup().await;

    let out = resp_json(
        &handler,
        &mut ctrl,
        "$DB/diagrams/d1/share",
        &grant_payload("ghost@example.com", "view"),
        "alice",
    )
    .await;
    assert_eq!(out["data"]["status"], "pending");
    assert!(out["data"]["grantee"].is_null());
}

#[cfg(feature = "http-api")]
#[tokio::test]
async fn cluster_identity_unshare_by_email_clears_grant() {
    let (handler, mut ctrl, crypto) = identity_share_setup().await;
    seed_identity_link(&mut ctrl, &crypto, "user@example.com", "canon-1").await;
    resp_json(
        &handler,
        &mut ctrl,
        "$DB/diagrams/d1/share",
        &grant_payload("user@example.com", "view"),
        "alice",
    )
    .await;
    assert_eq!(
        resp_json(&handler, &mut ctrl, "$DB/diagrams/d1", &[], "canon-1").await["status"],
        "ok"
    );

    let unshare = serde_json::to_vec(&serde_json::json!({"grantee": "User@Example.com"})).unwrap();
    resp_json(
        &handler,
        &mut ctrl,
        "$DB/diagrams/d1/unshare",
        &unshare,
        "alice",
    )
    .await;
    assert_eq!(
        resp_json(&handler, &mut ctrl, "$DB/diagrams/d1", &[], "canon-1").await["code"],
        403
    );
}

#[cfg(feature = "http-api")]
fn suspended_resolution(
    ctrl: &mut NodeController<MockTransport>,
    crypto: &mqdb_agent::http::IdentityCrypto,
    request_id: u64,
    email: &str,
) -> String {
    use super::super::node_controller::{PendingScatterRequest, ShareResolveContinuation};

    let grantee_key = crypto.blind_index(mqdb_core::types::SHARES_ENTITY, email);
    let email_hash = crypto.blind_index("_identity_links", email);
    let cont = ShareResolveContinuation {
        email_hash: email_hash.clone(),
        entity: "diagrams".into(),
        id: "d1".into(),
        grantee_key: grantee_key.clone(),
        grantee_email: None,
        permission: "view".into(),
        cascade: false,
        granted_by: "alice".into(),
        response_topic: "$DB/_resp/c".into(),
        correlation_data: None,
        invalidated: false,
    };
    ctrl.pending_scatter_requests.insert(
        request_id,
        PendingScatterRequest {
            expected_count: 1,
            received: Vec::new(),
            client_response_topic: "$DB/_resp/c".into(),
            created_at_ms: 0,
            filters: Vec::new(),
            sorts: Vec::new(),
            projection: None,
            pagination: None,
            entity: "_identity_links".into(),
            vault_sender: None,
            continuation: Some(cont),
        },
    );
    grantee_key
}

#[cfg(feature = "http-api")]
fn resolved_link_item(crypto: &mqdb_agent::http::IdentityCrypto, email: &str) -> serde_json::Value {
    serde_json::json!({
        "id": "google:canon-1",
        "data": {
            "canonical_id": "canon-1",
            "email_hash": crypto.blind_index("_identity_links", email),
        }
    })
}

#[cfg(feature = "http-api")]
#[tokio::test]
async fn cluster_identity_unshare_invalidates_inflight_resolution() {
    let (handler, mut ctrl, crypto) = identity_share_setup().await;
    seed_identity_link(&mut ctrl, &crypto, "user@example.com", "canon-1").await;
    let grantee_key = suspended_resolution(&mut ctrl, &crypto, 42, "user@example.com");

    ctrl.invalidate_share_resolutions("diagrams", "d1", &grantee_key, Some("alice"));
    ctrl.handle_scatter_list_response(42, vec![resolved_link_item(&crypto, "user@example.com")])
        .await;

    assert_eq!(
        resp_json(&handler, &mut ctrl, "$DB/diagrams/d1", &[], "canon-1").await["code"],
        403
    );
}

#[cfg(feature = "http-api")]
#[tokio::test]
async fn cluster_identity_nonowner_cannot_invalidate_resolution() {
    let (handler, mut ctrl, crypto) = identity_share_setup().await;
    seed_identity_link(&mut ctrl, &crypto, "user@example.com", "canon-1").await;
    let grantee_key = suspended_resolution(&mut ctrl, &crypto, 44, "user@example.com");

    ctrl.invalidate_share_resolutions("diagrams", "d1", &grantee_key, Some("mallory"));
    ctrl.handle_scatter_list_response(44, vec![resolved_link_item(&crypto, "user@example.com")])
        .await;

    assert_eq!(
        resp_json(&handler, &mut ctrl, "$DB/diagrams/d1", &[], "canon-1").await["status"],
        "ok"
    );
}

#[cfg(feature = "http-api")]
#[tokio::test]
async fn cluster_identity_inflight_resolution_completes_when_not_unshared() {
    let (handler, mut ctrl, crypto) = identity_share_setup().await;
    seed_identity_link(&mut ctrl, &crypto, "user@example.com", "canon-1").await;
    suspended_resolution(&mut ctrl, &crypto, 43, "user@example.com");

    ctrl.handle_scatter_list_response(43, vec![resolved_link_item(&crypto, "user@example.com")])
        .await;

    assert_eq!(
        resp_json(&handler, &mut ctrl, "$DB/diagrams/d1", &[], "canon-1").await["status"],
        "ok"
    );
}

fn presence_handler_with_transport() -> PresenceHarness {
    let node1 = NodeId::validated(1).unwrap();
    let transport = MockTransport::new(node1);
    let retained = Arc::clone(&transport.retained_publishes);
    let outbox = Arc::clone(&transport.outbox);
    let ctrl = create_test_controller(node1, transport);
    let handler = crate::cluster::event_handler::ClusterEventHandler::new(
        node1,
        Arc::new(tokio::sync::RwLock::new(ctrl)),
    )
    .with_presence(true);
    (handler, retained, outbox)
}

fn cluster_connect_event(client_id: &str) -> mqtt5::broker::events::ClientConnectEvent {
    mqtt5::broker::events::ClientConnectEvent {
        client_id: client_id.into(),
        user_id: Some("alice".into()),
        clean_start: true,
        session_expiry_interval: 0,
        will_topic: None,
        will_payload: None,
        will_qos: None,
        will_retain: None,
    }
}

#[tokio::test]
async fn cluster_presence_publishes_locally_and_broadcasts() {
    use mqtt5::broker::events::BrokerEventHandler;

    let (handler, retained, outbox) = presence_handler_with_transport();
    handler
        .on_client_connect(cluster_connect_event("seat-holder-7"))
        .await;

    let published = retained.lock().unwrap().clone();
    assert_eq!(published.len(), 1, "presence must be delivered locally");
    assert_eq!(published[0].0, "$DB/_presence/seat-holder-7");
    let value: serde_json::Value = serde_json::from_slice(&published[0].1).unwrap();
    assert_eq!(value["event"], "connect");
    assert_eq!(value["user_id"], "alice");

    let broadcasts = outbox.lock().unwrap();
    let presence_broadcasts: Vec<_> = broadcasts
        .iter()
        .filter_map(|(_, msg)| match msg {
            ClusterMessage::PresenceBroadcast(b) => Some(b),
            _ => None,
        })
        .collect();
    assert_eq!(
        presence_broadcasts.len(),
        1,
        "cross-node delivery relies on a broadcast, not wildcard routing"
    );
    assert_eq!(
        presence_broadcasts[0].topic_str(),
        "$DB/_presence/seat-holder-7"
    );
}

#[tokio::test]
async fn cluster_presence_skips_internal_clients_and_takeover() {
    use mqtt5::broker::events::BrokerEventHandler;

    let (handler, retained, _) = presence_handler_with_transport();

    handler
        .on_client_connect(cluster_connect_event("mqdb-admin-1"))
        .await;
    assert!(
        retained.lock().unwrap().is_empty(),
        "internal clients must not appear in the presence feed"
    );

    handler
        .on_client_connect(cluster_connect_event("seat-holder-7"))
        .await;
    handler
        .on_client_connect(cluster_connect_event("seat-holder-7"))
        .await;
    retained.lock().unwrap().clear();

    handler
        .on_client_disconnect(mqtt5::broker::events::ClientDisconnectEvent {
            client_id: "seat-holder-7".into(),
            user_id: Some("alice".into()),
            reason: mqtt5::types::ReasonCode::Success,
            unexpected: true,
        })
        .await;
    assert!(
        retained.lock().unwrap().is_empty(),
        "a displaced connection must not report a live client as disconnected"
    );
}

#[tokio::test]
async fn received_presence_broadcast_is_published_locally() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let transport = MockTransport::new(node1);
    let retained = Arc::clone(&transport.retained_publishes);
    let mut ctrl = create_test_controller(node1, transport);
    ctrl.set_presence(true);

    let broadcast = crate::cluster::protocol::PresenceBroadcast::try_new(
        "$DB/_presence/remote-holder",
        br#"{"client_id":"remote-holder","event":"disconnect"}"#,
    )
    .expect("presence fits in a broadcast");
    ctrl.handle_presence_broadcast(node2, &broadcast).await;

    let published = retained.lock().unwrap().clone();
    assert_eq!(
        published.len(),
        1,
        "a node must locally publish presence it receives"
    );
    assert_eq!(published[0].0, "$DB/_presence/remote-holder");
}

#[tokio::test]
async fn received_presence_broadcast_is_ignored_when_presence_is_off() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let transport = MockTransport::new(node1);
    let retained = Arc::clone(&transport.retained_publishes);
    let ctrl = create_test_controller(node1, transport);

    let broadcast = crate::cluster::protocol::PresenceBroadcast::try_new(
        "$DB/_presence/remote-holder",
        br#"{"client_id":"remote-holder","event":"connect"}"#,
    )
    .expect("presence fits in a broadcast");
    ctrl.handle_presence_broadcast(node2, &broadcast).await;

    assert!(
        retained.lock().unwrap().is_empty(),
        "a node that did not opt into presence must not retain a peer's presence"
    );
}

type SessionHarness = (
    crate::cluster::event_handler::ClusterEventHandler<MockTransport>,
    Arc<tokio::sync::RwLock<NodeController<MockTransport>>>,
);

fn session_harness() -> SessionHarness {
    let node1 = NodeId::validated(1).unwrap();
    let ctrl = Arc::new(tokio::sync::RwLock::new(create_test_controller(
        node1,
        MockTransport::new(node1),
    )));
    let handler = crate::cluster::event_handler::ClusterEventHandler::new(node1, Arc::clone(&ctrl));
    (handler, ctrl)
}

fn session_connect(
    client_id: &str,
    clean_start: bool,
    session_expiry_interval: u32,
) -> mqtt5::broker::events::ClientConnectEvent {
    mqtt5::broker::events::ClientConnectEvent {
        session_expiry_interval,
        clean_start,
        ..cluster_connect_event(client_id)
    }
}

fn session_disconnect(client_id: &str) -> mqtt5::broker::events::ClientDisconnectEvent {
    mqtt5::broker::events::ClientDisconnectEvent {
        client_id: client_id.into(),
        user_id: Some("alice".into()),
        reason: mqtt5::types::ReasonCode::Success,
        unexpected: false,
    }
}

fn session_subscribe(client_id: &str, topic: &str) -> mqtt5::broker::events::ClientSubscribeEvent {
    mqtt5::broker::events::ClientSubscribeEvent {
        client_id: client_id.into(),
        subscriptions: vec![mqtt5::broker::events::SubscriptionInfo {
            topic_filter: topic.into(),
            qos: mqtt5::QoS::AtLeastOnce,
            result: mqtt5::broker::events::SubAckReasonCode::GrantedQoS1,
        }],
    }
}

#[tokio::test]
async fn reconnecting_to_the_same_node_restores_the_client_location() {
    use mqtt5::broker::events::BrokerEventHandler;

    let (handler, ctrl) = session_harness();
    handler
        .on_client_connect(session_connect("phone", false, 300))
        .await;
    handler
        .on_client_disconnect(session_disconnect("phone"))
        .await;
    handler
        .on_client_connect(session_connect("phone", false, 300))
        .await;

    let ctrl = ctrl.read().await;
    assert_eq!(
        ctrl.stores().client_locations.get("phone"),
        NodeId::validated(1),
        "a client that resumes its session on the same node must be routable from other nodes"
    );
    assert!(
        ctrl.stores()
            .sessions
            .get("phone")
            .is_some_and(|s| s.is_connected()),
        "the session record must say the client is connected again"
    );
}

#[tokio::test]
async fn disconnect_keeps_subscriptions_of_a_session_that_outlives_it() {
    use mqtt5::broker::events::BrokerEventHandler;

    let (handler, ctrl) = session_harness();
    handler
        .on_client_connect(session_connect("sensor", true, 300))
        .await;
    handler
        .on_client_subscribe(session_subscribe("sensor", "plant/temp"))
        .await;
    handler
        .on_client_disconnect(session_disconnect("sensor"))
        .await;

    assert_eq!(
        ctrl.read()
            .await
            .stores()
            .topics
            .get_client_topics("sensor")
            .len(),
        1,
        "a session with a nonzero expiry keeps its subscriptions after disconnect, whatever its clean start flag"
    );
}

#[tokio::test]
async fn disconnect_clears_subscriptions_of_a_session_that_ends_with_it() {
    use mqtt5::broker::events::BrokerEventHandler;

    let (handler, ctrl) = session_harness();
    handler
        .on_client_connect(session_connect("kiosk", false, 0))
        .await;
    handler
        .on_client_subscribe(session_subscribe("kiosk", "plant/temp"))
        .await;
    handler
        .on_client_disconnect(session_disconnect("kiosk"))
        .await;

    assert!(
        ctrl.read()
            .await
            .stores()
            .topics
            .get_client_topics("kiosk")
            .is_empty(),
        "a session with expiry 0 ends at disconnect, so its subscriptions must go"
    );
}

#[tokio::test]
async fn disconnect_of_a_taken_over_connection_keeps_the_new_one_routable() {
    use mqtt5::broker::events::BrokerEventHandler;

    let (handler, ctrl) = session_harness();
    handler
        .on_client_connect(session_connect("tablet", false, 0))
        .await;
    handler
        .on_client_subscribe(session_subscribe("tablet", "plant/temp"))
        .await;
    handler
        .on_client_connect(session_connect("tablet", false, 0))
        .await;
    handler
        .on_client_disconnect(session_disconnect("tablet"))
        .await;

    let ctrl = ctrl.read().await;
    assert_eq!(
        ctrl.stores().client_locations.get("tablet"),
        NodeId::validated(1),
        "the displaced connection's disconnect must not remove the live connection's location"
    );
    assert_eq!(
        ctrl.stores().topics.get_client_topics("tablet").len(),
        1,
        "the displaced connection's disconnect must not clear the live session's subscriptions"
    );
}

type RetainedSyncHarness = (
    Arc<mqtt5::broker::router::MessageRouter>,
    crate::cluster::LocalRetainedWriter,
    Arc<crate::cluster::RetainedStore>,
    RetainedRefreshes,
    Arc<tokio::sync::RwLock<NodeController<MockTransport>>>,
);

fn retained_sync_harness() -> RetainedSyncHarness {
    let node1 = NodeId::validated(1).unwrap();
    let transport = MockTransport::new(node1);
    let refreshes = Arc::clone(&transport.retained_refreshes);
    let ctrl = create_test_controller(node1, transport);
    let store = Arc::clone(&ctrl.stores().retained);
    let ctrl = Arc::new(tokio::sync::RwLock::new(ctrl));
    let echoes = Arc::new(crate::cluster::RetainedEchoes::new());
    let handler = crate::cluster::event_handler::ClusterEventHandler::new(node1, Arc::clone(&ctrl))
        .with_retained_echoes(Arc::clone(&echoes));
    let router =
        Arc::new(mqtt5::broker::router::MessageRouter::new().with_event_handler(Arc::new(handler)));
    let writer =
        crate::cluster::LocalRetainedWriter::new(Arc::clone(&router), Arc::clone(&store), echoes);
    (router, writer, store, refreshes, ctrl)
}

fn retained_write(
    topic: &str,
    operation: super::super::protocol::Operation,
    payload: &[u8],
) -> super::super::protocol::ReplicationWrite {
    let message = super::super::RetainedMessage::create(topic, 1, payload, 0);
    super::super::protocol::ReplicationWrite::new(
        super::super::topic_partition(topic),
        operation,
        Epoch::ZERO,
        0,
        super::super::entity::RETAINED.to_string(),
        topic.to_string(),
        message.to_be_bytes(),
    )
}

#[tokio::test]
async fn a_replicated_set_and_clear_written_to_the_broker_are_not_replicated_again() {
    let (router, writer, store, refreshes, _) = retained_sync_harness();

    store.set("plant/state", 1, b"on", 0);
    writer.refresh("plant/state").await;
    assert_eq!(
        router.get_retained_messages("plant/state").await.len(),
        1,
        "the refresh must write the stored value into the broker"
    );

    store.set("plant/state", 1, b"", 0);
    writer.refresh("plant/state").await;
    assert!(
        router.get_retained_messages("plant/state").await.is_empty(),
        "the refresh must clear the broker copy"
    );

    assert!(
        refreshes.lock().unwrap().is_empty(),
        "the broker's report of a sync must be recognised as its echo, not replicated as a new change"
    );
}

#[tokio::test]
async fn a_client_retained_change_is_replicated_and_refreshed() {
    let (router, _, store, refreshes, _) = retained_sync_harness();

    let publish = mqtt5::packet::publish::PublishPacket::new(
        "plant/state",
        b"off".to_vec(),
        mqtt5::QoS::AtLeastOnce,
    )
    .with_retain(true);
    router.route_message(&publish, None).await;

    assert_eq!(
        store.get("plant/state").map(|message| message.payload),
        Some(b"off".to_vec()),
        "a client change must reach the retained store"
    );
    assert_eq!(
        *refreshes.lock().unwrap(),
        vec!["plant/state".to_string()],
        "a client change must queue a refresh so the broker follows the store"
    );
}

#[tokio::test]
async fn every_replicated_retained_write_refreshes_the_broker() {
    use super::super::protocol::Operation;

    let (_, _, _, refreshes, ctrl) = retained_sync_harness();
    let ctrl = ctrl.read().await;

    ctrl.sync_retained_to_broker(&retained_write("plant/state", Operation::Insert, b"on"))
        .await;
    ctrl.sync_retained_to_broker(&retained_write("plant/state", Operation::Delete, b""))
        .await;
    ctrl.sync_retained_to_broker(&retained_write("plant/state", Operation::Insert, b"off"))
        .await;

    assert_eq!(
        *refreshes.lock().unwrap(),
        vec!["plant/state".to_string(); 3],
        "writes arriving in quick succession must each reach the broker"
    );
}

fn forwarded_publish_targets(outbox: &Mutex<Vec<(NodeId, ClusterMessage)>>) -> Vec<NodeId> {
    outbox
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, message)| matches!(message, ClusterMessage::ForwardedPublish(_)))
        .map(|(to, _)| *to)
        .collect()
}

#[tokio::test]
async fn a_forwarded_publish_reaches_local_subscribers_without_being_forwarded_again() {
    use mqtt5::broker::events::{BrokerEventHandler, ClientPublishEvent};
    use mqtt5::broker::router::MessageRouter;

    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let transport = MockTransport::new(node1);
    let outbox = Arc::clone(&transport.outbox);
    let ctrl = create_test_controller(node1, transport);
    ctrl.stores()
        .topics
        .subscribe("plant/temp", "remote-dashboard", PartitionId::ZERO, 1)
        .unwrap();
    ctrl.stores()
        .client_locations
        .set("remote-dashboard", node2);
    let ctrl = Arc::new(tokio::sync::RwLock::new(ctrl));
    let handler = Arc::new(crate::cluster::event_handler::ClusterEventHandler::new(
        node1,
        Arc::clone(&ctrl),
    ));
    let router = Arc::new(MessageRouter::new().with_event_handler(Arc::clone(&handler) as _));

    let mut receivers =
        crate::cluster::subscribe_test_client(&router, "local-dashboard", "plant/temp", 8).await;

    crate::cluster::RoutedDeliveryLanes::spawn(&router)
        .deliver(crate::cluster::LocalPublishRequest {
            topic: "plant/temp".to_string(),
            payload: b"21.5".to_vec(),
            qos: 1,
            kind: crate::cluster::LocalPublishKind::Routed,
            user_properties: Vec::new(),
            order_key: None,
            share_groups: None,
        })
        .await;

    let delivered = tokio::time::timeout(std::time::Duration::from_secs(1), receivers.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&delivered.publish.payload[..], b"21.5");
    assert!(
        forwarded_publish_targets(&outbox).is_empty(),
        "a forwarded publish must not be forwarded again to the other node's subscriber"
    );

    handler
        .on_client_publish(ClientPublishEvent {
            client_id: "mqdb-admin-1".into(),
            user_id: None,
            topic: "plant/temp".into(),
            payload: b"21.5".to_vec().into(),
            qos: mqtt5::QoS::AtLeastOnce,
            retain: false,
            packet_id: Some(1),
            response_topic: None,
            correlation_data: None,
        })
        .await;
    assert_eq!(
        forwarded_publish_targets(&outbox),
        vec![node2],
        "the same publish made by the admin client is forwarded to the other node"
    );
}

#[tokio::test]
async fn cli_clients_are_routable_across_nodes() {
    use mqtt5::broker::events::BrokerEventHandler;

    let (handler, ctrl) = session_harness();
    handler
        .on_client_connect(session_connect("mqdb-cli-watch", true, 0))
        .await;
    handler
        .on_client_subscribe(session_subscribe("mqdb-cli-watch", "$DB/wt/events/#"))
        .await;

    let ctrl = ctrl.read().await;
    assert_eq!(
        ctrl.stores().client_locations.get("mqdb-cli-watch"),
        NodeId::validated(1)
    );
    assert_eq!(
        ctrl.stores()
            .wildcards
            .get_client_patterns("mqdb-cli-watch"),
        vec![("$DB/wt/events/#".to_string(), 1)]
    );
}

#[tokio::test]
async fn internal_clients_are_not_tracked() {
    use mqtt5::broker::events::BrokerEventHandler;

    let (handler, ctrl) = session_harness();
    handler
        .on_client_connect(session_connect("mqdb-admin-1", true, 0))
        .await;
    handler
        .on_client_subscribe(session_subscribe("mqdb-admin-1", "$DB/wt/events/#"))
        .await;

    let ctrl = ctrl.read().await;
    assert_eq!(ctrl.stores().client_locations.get("mqdb-admin-1"), None);
    assert!(
        ctrl.stores()
            .wildcards
            .get_client_patterns("mqdb-admin-1")
            .is_empty()
    );
}

#[tokio::test]
async fn cluster_presence_skips_cli_clients() {
    use mqtt5::broker::events::BrokerEventHandler;

    let (handler, retained, _) = presence_handler_with_transport();
    handler
        .on_client_connect(cluster_connect_event("mqdb-cli-request"))
        .await;
    handler
        .on_client_disconnect(mqtt5::broker::events::ClientDisconnectEvent {
            client_id: "mqdb-cli-request".into(),
            user_id: Some("alice".into()),
            reason: mqtt5::types::ReasonCode::Success,
            unexpected: false,
        })
        .await;
    assert!(retained.lock().unwrap().is_empty());
}

fn without_response_topic() -> MqttRequestContext<'static> {
    MqttRequestContext {
        response_topic: None,
        correlation_data: None,
        sender: None,
        client_id: None,
    }
}

#[tokio::test]
async fn json_writes_without_a_response_topic_are_applied() {
    let handler = DbRequestHandler::new(NodeId::validated(1).unwrap());
    let mut ctrl = setup_controller_all_partitions();
    let data = serde_json::to_vec(&serde_json::json!({"n": 0})).unwrap();
    ctrl.db_create("nr", "r1", &data, 1000).await.unwrap();
    ctrl.db_create("nr", "r3", &data, 1000).await.unwrap();
    let ctx = without_response_topic();

    let update = handler
        .handle_publish(&mut ctrl, "$DB/nr/r1/update", br#"{"n":6}"#, &ctx)
        .await;
    let create = handler
        .handle_publish(&mut ctrl, "$DB/nr/create", br#"{"id":"r2","n":1}"#, &ctx)
        .await;
    let delete = handler
        .handle_publish(&mut ctrl, "$DB/nr/r3/delete", &[], &ctx)
        .await;

    assert!(update.is_none() && create.is_none() && delete.is_none());
    let field_n = |record: Option<crate::cluster::db::DbEntity>| {
        let record = record.expect("record exists");
        serde_json::from_slice::<serde_json::Value>(&record.data).unwrap()["n"].clone()
    };
    assert_eq!(field_n(ctrl.db_get("nr", "r1")), 6);
    assert_eq!(field_n(ctrl.db_get("nr", "r2")), 1);
    assert!(ctrl.db_get("nr", "r3").is_none());
}

#[tokio::test]
async fn json_write_without_a_response_topic_is_forwarded_to_the_primary() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let handler = DbRequestHandler::new(node1);
    let outbox = Arc::new(Mutex::new(Vec::new()));
    let transport = MockTransport {
        node_id: node1,
        inbox: Arc::new(Mutex::new(VecDeque::new())),
        outbox: Arc::clone(&outbox),
        retained_publishes: Arc::new(Mutex::new(Vec::new())),
        retained_refreshes: Arc::new(Mutex::new(Vec::new())),
    };
    let mut ctrl = create_test_controller(node1, transport);
    let mut map = PartitionMap::default();
    map.set(
        data_partition("nr", "r1"),
        crate::cluster::PartitionAssignment {
            primary: Some(node2),
            replicas: vec![],
            epoch: Epoch::new(1),
        },
    );
    ctrl.update_partition_map(map);

    let update = handler
        .handle_publish(
            &mut ctrl,
            "$DB/nr/r1/update",
            br#"{"n":6}"#,
            &without_response_topic(),
        )
        .await;

    assert!(update.is_none());
    let sent = outbox.lock().unwrap();
    let forwarded: Vec<(NodeId, super::super::protocol::JsonDbOp, String)> = sent
        .iter()
        .filter_map(|(to, message)| match message {
            ClusterMessage::JsonDbRequest { request, .. } => {
                Some((*to, request.op, request.response_topic.clone()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        forwarded,
        [(
            node2,
            super::super::protocol::JsonDbOp::Update,
            String::new()
        )]
    );
}

#[tokio::test]
async fn forwarded_write_without_a_response_topic_is_applied_without_a_reply() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let outbox = Arc::new(Mutex::new(Vec::new()));
    let transport = MockTransport {
        node_id: node1,
        inbox: Arc::new(Mutex::new(VecDeque::new())),
        outbox: Arc::clone(&outbox),
        retained_publishes: Arc::new(Mutex::new(Vec::new())),
        retained_refreshes: Arc::new(Mutex::new(Vec::new())),
    };
    let mut ctrl = create_test_controller(node1, transport);
    let partition = data_partition("nr", "r1");
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
    let data = serde_json::to_vec(&serde_json::json!({"n": 0})).unwrap();
    ctrl.db_create("nr", "r1", &data, 1000).await.unwrap();
    outbox.lock().unwrap().clear();

    let request = super::super::protocol::JsonDbRequest {
        request_id: 1,
        op: super::super::protocol::JsonDbOp::Update,
        entity: "nr".to_string(),
        id: Some("r1".to_string()),
        payload: br#"{"n":6}"#.to_vec(),
        response_topic: String::new(),
        correlation_data: None,
        sender: None,
        client_id: None,
    };
    ctrl.handle_json_db_request(node2, partition, &request)
        .await;

    let r1 = ctrl.db_get("nr", "r1").expect("r1 exists");
    let r1: serde_json::Value = serde_json::from_slice(&r1.data).unwrap();
    assert_eq!(r1["n"], 6);
    assert!(
        !outbox
            .lock()
            .unwrap()
            .iter()
            .any(|(_, message)| matches!(message, ClusterMessage::JsonDbResponse(_)))
    );
}

#[tokio::test]
async fn a_forwarded_create_on_a_node_that_lost_the_partition_is_refused() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();
    let outbox = Arc::new(Mutex::new(Vec::new()));
    let transport = MockTransport {
        node_id: node1,
        inbox: Arc::new(Mutex::new(VecDeque::new())),
        outbox: Arc::clone(&outbox),
        retained_publishes: Arc::new(Mutex::new(Vec::new())),
        retained_refreshes: Arc::new(Mutex::new(Vec::new())),
    };
    let mut ctrl = create_test_controller(node1, transport);
    let partition = data_partition("nr", "r1");
    let mut map = PartitionMap::default();
    map.set(
        partition,
        crate::cluster::PartitionAssignment {
            primary: Some(node3),
            replicas: vec![node1],
            epoch: Epoch::new(2),
        },
    );
    ctrl.update_partition_map(map);

    let request = super::super::protocol::JsonDbRequest {
        request_id: 1,
        op: super::super::protocol::JsonDbOp::Create,
        entity: "nr".to_string(),
        id: None,
        payload: br#"{"id":"r1","n":1}"#.to_vec(),
        response_topic: "resp/create".to_string(),
        correlation_data: None,
        sender: None,
        client_id: None,
    };
    ctrl.handle_json_db_request(node2, partition, &request)
        .await;

    assert!(ctrl.db_get("nr", "r1").is_none());
    let sent = outbox.lock().unwrap();
    assert!(
        !sent
            .iter()
            .any(|(_, message)| matches!(message, ClusterMessage::WriteRequest(_)))
    );
    let replies: Vec<(NodeId, serde_json::Value)> = sent
        .iter()
        .filter_map(|(to, message)| match message {
            ClusterMessage::JsonDbResponse(response) => {
                Some((*to, serde_json::from_slice(&response.payload).unwrap()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(replies.len(), 1);
    assert_eq!(replies[0].0, node2);
    assert_eq!(replies[0].1["status"], "error");
    assert_eq!(replies[0].1["code"], 503);
}

#[tokio::test]
async fn a_create_whose_primary_moves_during_its_unique_check_is_refused() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();
    let outbox = Arc::new(Mutex::new(Vec::new()));
    let transport = MockTransport {
        node_id: node1,
        inbox: Arc::new(Mutex::new(VecDeque::new())),
        outbox: Arc::clone(&outbox),
        retained_publishes: Arc::new(Mutex::new(Vec::new())),
        retained_refreshes: Arc::new(Mutex::new(Vec::new())),
    };
    let mut ctrl = create_test_controller(node1, transport);
    let partition = data_partition("nr", "r1");
    ctrl.become_primary(partition, Epoch::new(1));
    let mut map = PartitionMap::default();
    map.set(
        partition,
        crate::cluster::PartitionAssignment {
            primary: Some(node1),
            replicas: vec![node3],
            epoch: Epoch::new(1),
        },
    );
    ctrl.apply_raft_partition_map(map.clone()).await;
    map.set(
        partition,
        crate::cluster::PartitionAssignment {
            primary: Some(node3),
            replicas: vec![node1],
            epoch: Epoch::new(2),
        },
    );
    ctrl.apply_raft_partition_map(map).await;
    outbox.lock().unwrap().clear();

    let data = serde_json::json!({"id": "r1", "n": 1});
    ctrl.complete_pending_unique_work(
        Vec::new(),
        Ok(Vec::new()),
        super::super::node_controller::UniqueCheckContinuation::CreateFromNodeController {
            from: node2,
            entity: "nr".to_string(),
            id: "r1".to_string(),
            data_bytes: serde_json::to_vec(&data).unwrap(),
            data,
            partition,
            request_id: "req-1".to_string(),
            now_ms: 1000,
            sender: None,
            client_id: None,
            response_topic: "resp/create".to_string(),
            correlation_data: None,
        },
    )
    .await;

    assert!(ctrl.db_get("nr", "r1").is_none());
    let sent = outbox.lock().unwrap();
    assert!(
        !sent
            .iter()
            .any(|(_, message)| matches!(message, ClusterMessage::WriteRequest(_)))
    );
    let replies: Vec<serde_json::Value> = sent
        .iter()
        .filter_map(|(to, message)| match message {
            ClusterMessage::JsonDbResponse(response) if *to == node2 => {
                Some(serde_json::from_slice(&response.payload).unwrap())
            }
            _ => None,
        })
        .collect();
    assert_eq!(replies.len(), 1);
    assert_eq!(replies[0]["code"], 503);
}

fn lose_primary_role(
    ctrl: &mut NodeController<MockTransport>,
    lost: PartitionId,
    named_primary: NodeId,
) {
    let node1 = NodeId::validated(1).unwrap();
    ctrl.step_down(lost);
    let mut map = PartitionMap::default();
    for i in 0..NUM_PARTITIONS {
        let partition = PartitionId::new(i).unwrap();
        let primary = if partition == lost {
            named_primary
        } else {
            node1
        };
        map.set(
            partition,
            crate::cluster::PartitionAssignment {
                primary: Some(primary),
                replicas: if primary == node1 {
                    vec![]
                } else {
                    vec![node1]
                },
                epoch: Epoch::new(2),
            },
        );
    }
    ctrl.update_partition_map(map);
    ctrl.transport().outbox.lock().unwrap().clear();
}

fn sent_json_replies(ctrl: &NodeController<MockTransport>) -> Vec<serde_json::Value> {
    ctrl.transport()
        .outbox
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(_, message)| match message {
            ClusterMessage::JsonDbResponse(response) => {
                Some(serde_json::from_slice(&response.payload).unwrap())
            }
            _ => None,
        })
        .collect()
}

fn forwarded_request(
    op: super::super::protocol::JsonDbOp,
    entity: &str,
    id: Option<&str>,
    payload: &[u8],
    sender: Option<&str>,
) -> super::super::protocol::JsonDbRequest {
    super::super::protocol::JsonDbRequest {
        request_id: 1,
        op,
        entity: entity.to_string(),
        id: id.map(str::to_string),
        payload: payload.to_vec(),
        response_topic: "resp/forwarded".to_string(),
        correlation_data: None,
        sender: sender.map(str::to_string),
        client_id: None,
    }
}

fn grants_for(ctrl: &NodeController<MockTransport>, grantee: &str) -> usize {
    ctrl.db_list(mqdb_core::types::SHARES_ENTITY)
        .into_iter()
        .filter(|record| {
            serde_json::from_slice::<serde_json::Value>(&record.data)
                .is_ok_and(|value| value["grantee_key"] == grantee)
        })
        .count()
}

#[tokio::test]
async fn a_forwarded_unshare_on_a_node_that_lost_the_partition_is_refused() {
    let (handler, mut ctrl, _) = share_setup().await;
    resp_json(
        &handler,
        &mut ctrl,
        "$DB/diagrams/d1/share",
        &grant_payload("bob", "view"),
        "alice",
    )
    .await;
    let partition = data_partition("diagrams", "d1");
    lose_primary_role(&mut ctrl, partition, NodeId::validated(3).unwrap());

    let request = forwarded_request(
        super::super::protocol::JsonDbOp::Unshare,
        "diagrams",
        Some("d1"),
        br#"{"grantee":"bob"}"#,
        Some("alice"),
    );
    ctrl.handle_json_db_request(NodeId::validated(2).unwrap(), partition, &request)
        .await;

    let replies = sent_json_replies(&ctrl);
    assert_eq!(replies.len(), 1);
    assert_eq!(replies[0]["code"], 503);
    assert_eq!(grants_for(&ctrl, "bob"), 1);
}

#[tokio::test]
async fn a_forwarded_share_on_a_node_that_lost_the_partition_is_refused() {
    let (_, mut ctrl, _) = share_setup().await;
    let partition = data_partition("diagrams", "d1");
    lose_primary_role(&mut ctrl, partition, NodeId::validated(3).unwrap());

    let request = forwarded_request(
        super::super::protocol::JsonDbOp::Share,
        "diagrams",
        Some("d1"),
        &grant_payload("carol", "view"),
        Some("alice"),
    );
    ctrl.handle_json_db_request(NodeId::validated(2).unwrap(), partition, &request)
        .await;

    let replies = sent_json_replies(&ctrl);
    assert_eq!(replies.len(), 1);
    assert_eq!(replies[0]["code"], 503);
    assert_eq!(grants_for(&ctrl, "carol"), 0);
}

async fn author_with_post(ctrl: &mut NodeController<MockTransport>) {
    use super::super::db::{ClusterConstraint, OnDeleteAction};
    let user = serde_json::to_vec(&serde_json::json!({"name": "Alice"})).unwrap();
    ctrl.db_create("users", "u1", &user, 1000).await.unwrap();
    let post = serde_json::to_vec(&serde_json::json!({"title": "P1", "author_id": "u1"})).unwrap();
    ctrl.db_create("posts", "p1", &post, 1000).await.unwrap();
    let fk = ClusterConstraint::foreign_key(
        "posts",
        "posts_author_fk",
        "author_id",
        "users",
        "id",
        OnDeleteAction::Cascade,
    )
    .unwrap();
    ctrl.constraint_add(&fk).await.unwrap();
    assert_ne!(data_partition("users", "u1"), data_partition("posts", "p1"));
}

async fn delete_author(ctrl: &mut NodeController<MockTransport>) -> serde_json::Value {
    let handler = DbRequestHandler::new(NodeId::validated(1).unwrap());
    let response = handler
        .handle_publish(
            ctrl,
            "$DB/users/u1/delete",
            &[],
            &MqttRequestContext {
                response_topic: Some("resp/t"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
        )
        .await;
    parse_json_response(&response.unwrap().payload)
}

#[tokio::test]
async fn a_forwarded_delete_on_a_node_that_lost_the_parent_keeps_its_children() {
    let mut ctrl = setup_controller_all_partitions();
    author_with_post(&mut ctrl).await;
    let partition = data_partition("users", "u1");
    lose_primary_role(&mut ctrl, partition, NodeId::validated(3).unwrap());

    let request = forwarded_request(
        super::super::protocol::JsonDbOp::Delete,
        "users",
        Some("u1"),
        &[],
        None,
    );
    ctrl.handle_json_db_request(NodeId::validated(2).unwrap(), partition, &request)
        .await;

    let replies = sent_json_replies(&ctrl);
    assert_eq!(replies.len(), 1);
    assert_eq!(replies[0]["code"], 503);
    assert!(ctrl.db_get("users", "u1").is_some());
    assert!(ctrl.db_get("posts", "p1").is_some());
}

#[tokio::test]
async fn a_client_delete_without_the_parent_role_keeps_its_children() {
    let mut ctrl = setup_controller_all_partitions();
    author_with_post(&mut ctrl).await;
    lose_primary_role(
        &mut ctrl,
        data_partition("users", "u1"),
        NodeId::validated(1).unwrap(),
    );

    let json = delete_author(&mut ctrl).await;

    assert_eq!(json["code"], 503);
    assert!(ctrl.db_get("users", "u1").is_some());
    assert!(ctrl.db_get("posts", "p1").is_some());
}

fn memory_backend() -> Arc<dyn mqdb_core::storage::StorageBackend> {
    Arc::new(mqdb_core::storage::MemoryBackend::new())
}

fn controller_all_partitions_with_storage(
    backend: &Arc<dyn mqdb_core::storage::StorageBackend>,
) -> NodeController<MockTransport> {
    let node1 = NodeId::validated(1).unwrap();
    let (tx_raft_messages, _) = flume::unbounded();
    let (tx_raft_events, _) = flume::unbounded();
    let mut ctrl = NodeController::new_with_storage(
        node1,
        MockTransport::new(node1),
        TransportConfig::default(),
        Some(Arc::clone(backend)),
        tx_raft_messages,
        tx_raft_events,
    );
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

fn pending_cascades(
    ctrl: &NodeController<MockTransport>,
) -> Vec<crate::cluster::CascadePendingEntry> {
    ctrl.stores()
        .cluster_outbox()
        .unwrap()
        .pending_cascade_ops()
        .unwrap()
}

#[tokio::test]
async fn a_cascade_child_without_the_primary_role_stays_pending_until_it_can_be_deleted() {
    let mut ctrl = controller_all_partitions_with_storage(&memory_backend());
    author_with_post(&mut ctrl).await;
    let child_partition = data_partition("posts", "p1");
    lose_primary_role(&mut ctrl, child_partition, NodeId::validated(1).unwrap());

    let json = delete_author(&mut ctrl).await;
    tokio::task::yield_now().await;

    assert_eq!(json["status"], "ok");
    assert!(ctrl.db_get("users", "u1").is_none());
    assert!(ctrl.db_get("posts", "p1").is_some());
    let pending = pending_cascades(&ctrl);
    assert_eq!(pending.len(), 1);

    ctrl.become_primary(child_partition, Epoch::new(3));
    ctrl.replay_cascade_entries(&pending).await;

    assert!(ctrl.db_get("posts", "p1").is_none());
    assert!(pending_cascades(&ctrl).is_empty());
}

#[tokio::test]
async fn a_replayed_cascade_without_the_child_role_stays_pending() {
    let backend = memory_backend();
    let mut ctrl = controller_all_partitions_with_storage(&backend);
    author_with_post(&mut ctrl).await;
    let ops = [crate::cluster::CascadeRemoteOp::Delete {
        entity: "posts".to_string(),
        id: "p1".to_string(),
        sender: None,
    }];
    let (key, value) = crate::cluster::ClusterOutbox::build_cascade_entry("op-1", &ops);
    backend.insert(&key, &value).unwrap();
    let child_partition = data_partition("posts", "p1");
    lose_primary_role(&mut ctrl, child_partition, NodeId::validated(1).unwrap());

    let pending = pending_cascades(&ctrl);
    ctrl.replay_cascade_entries(&pending).await;
    tokio::task::yield_now().await;

    assert!(ctrl.db_get("posts", "p1").is_some());
    assert_eq!(pending_cascades(&ctrl).len(), 1);

    ctrl.become_primary(child_partition, Epoch::new(3));
    ctrl.replay_cascade_entries(&pending).await;

    assert!(ctrl.db_get("posts", "p1").is_none());
    assert!(pending_cascades(&ctrl).is_empty());
}

#[tokio::test]
async fn a_cascade_refused_by_its_receiver_is_not_acked() {
    let ctrl_node = NodeId::validated(1).unwrap();
    let mut ctrl = create_test_controller(ctrl_node, MockTransport::new(ctrl_node));
    let (tx, rx) = tokio::sync::oneshot::channel();
    ctrl.pending_constraints().insert_cascade_ack(42, tx);

    let response = super::super::protocol::JsonDbResponse::new(
        0,
        serde_json::to_vec(&serde_json::json!({
            "status": "error",
            "code": 503,
            "message": "this node is not the partition primary"
        }))
        .unwrap(),
        "_mqdb/cascade_ack/1/42".to_string(),
        None,
    );
    ctrl.handle_json_db_response(&response).await;

    assert!(!rx.await.unwrap());
}

#[tokio::test]
async fn a_forwarded_unique_create_on_a_node_that_lost_the_partition_is_refused_before_reserving() {
    use super::super::db::ClusterConstraint;
    let mut ctrl = setup_controller_all_partitions();
    ctrl.constraint_add(&ClusterConstraint::unique("users", "uniq_email", "email").unwrap())
        .await
        .unwrap();
    let partition = data_partition("users", "r9");
    lose_primary_role(&mut ctrl, partition, NodeId::validated(3).unwrap());

    let request = forwarded_request(
        super::super::protocol::JsonDbOp::Create,
        "users",
        None,
        br#"{"id":"r9","email":"x@y"}"#,
        None,
    );
    let pending = ctrl
        .handle_json_db_request(NodeId::validated(2).unwrap(), partition, &request)
        .await;

    assert!(pending.is_none());
    let replies = sent_json_replies(&ctrl);
    assert_eq!(replies.len(), 1);
    assert_eq!(replies[0]["code"], 503);
    assert_eq!(ctrl.transport().outbox.lock().unwrap().len(), 1);
}

async fn json_create(
    ctrl: &mut NodeController<MockTransport>,
    entity: &str,
    body: &serde_json::Value,
) -> serde_json::Value {
    let response = DbRequestHandler::new(NodeId::validated(1).unwrap())
        .handle_publish(
            ctrl,
            &format!("$DB/{entity}/create"),
            &serde_json::to_vec(body).unwrap(),
            &MqttRequestContext {
                response_topic: Some("resp/t"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
        )
        .await;
    match response {
        super::DbPublishResult::Response(response) => parse_json_response(&response.payload),
        super::DbPublishResult::NoResponse => serde_json::json!({"result": "no response"}),
        super::DbPublishResult::PendingUniqueCheck(_)
        | super::DbPublishResult::PendingFkCheck(_)
        | super::DbPublishResult::PendingFkDelete(_) => serde_json::json!({"result": "pending"}),
    }
}

#[tokio::test]
async fn json_create_refuses_an_id_longer_than_the_record_id_limit() {
    let mut ctrl = setup_controller_all_partitions();
    let limit = mqdb_core::protocol::MAX_RECORD_ID_LEN;

    for long_id in ["L".repeat(70_000), "m".repeat(limit + 1)] {
        let json = json_create(
            &mut ctrl,
            "items",
            &serde_json::json!({"id": long_id, "v": 1}),
        )
        .await;
        assert_eq!(json["status"], "error", "id of {} bytes", long_id.len());
        assert_eq!(json["code"], 400, "id of {} bytes", long_id.len());
        assert!(ctrl.db_get("items", &long_id).is_none());
    }

    let at_limit = "k".repeat(limit);
    let json = json_create(
        &mut ctrl,
        "items",
        &serde_json::json!({"id": at_limit, "v": 1}),
    )
    .await;
    assert_eq!(json["status"], "ok");
    let stored = ctrl
        .db_get("items", &at_limit)
        .expect("record at the limit is stored");
    let replicated = super::super::db::DbDataStore::deserialize(
        &super::super::db::DbDataStore::serialize(&stored),
    )
    .expect("record decodes");
    assert_eq!(replicated.id_str(), at_limit);
}

#[tokio::test]
async fn json_create_refuses_a_unique_value_too_large_to_store() {
    use super::super::db::ClusterConstraint;

    let mut ctrl = setup_controller_all_partitions();
    ctrl.constraint_add(&ClusterConstraint::unique("users", "uniq_code", "code").unwrap())
        .await
        .unwrap();

    let code = "c".repeat(40_000);
    let json = json_create(
        &mut ctrl,
        "users",
        &serde_json::json!({"id": "u1", "code": code}),
    )
    .await;
    assert_eq!(json["status"], "error");
    assert_eq!(json["code"], 400);
    assert!(ctrl.db_get("users", "u1").is_none());
    let value = serde_json::to_vec(&serde_json::json!(code)).unwrap();
    assert!(ctrl.stores().unique_get("users", "code", &value).is_none());

    let json = json_create(
        &mut ctrl,
        "users",
        &serde_json::json!({"id": "u2", "code": "short"}),
    )
    .await;
    assert_ne!(json["status"], "error", "{json}");
}

#[tokio::test]
async fn json_create_refuses_an_entity_name_longer_than_the_limit() {
    let mut ctrl = setup_controller_all_partitions();
    let limit = mqdb_core::protocol::MAX_ENTITY_NAME_LEN;

    for entity in ["e".repeat(65_500), "e".repeat(limit + 1)] {
        let json = json_create(&mut ctrl, &entity, &serde_json::json!({"id": "r1"})).await;
        assert_eq!(json["code"], 400, "entity of {} bytes", entity.len());
        assert!(ctrl.db_get(&entity, "r1").is_none());
    }

    let at_limit = "e".repeat(limit);
    let json = json_create(&mut ctrl, &at_limit, &serde_json::json!({"id": "r1"})).await;
    assert_eq!(json["status"], "ok");
}
