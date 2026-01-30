use super::super::db::data_partition;
use super::super::db_protocol::{DbReadRequest, DbResponse, DbStatus, DbWriteRequest};
use super::super::node_controller::NodeController;
use super::super::transport::{ClusterMessage, ClusterTransport, InboundMessage, TransportConfig};
use super::super::{Epoch, NodeId, PartitionId, PartitionMap};
use super::DbRequestHandler;
use bebytes::BeBytes;
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
        .handle_publish(&mut ctrl, &topic, &payload, Some("$DB/_resp/client1"), None)
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
        .handle_publish(&mut ctrl, topic, &payload, Some("$DB/_resp/client1"), None)
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
        .handle_publish(&mut ctrl, topic, &payload, None, None)
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
        )
        .await;

    assert!(response.is_none());
}
