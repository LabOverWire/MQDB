use super::db::data_partition;
use super::db_protocol::{
    DbReadRequest, DbResponse, DbStatus, DbWriteRequest, FkValidateRequest, FkValidateResponse,
    FkValidateStatus, IndexUpdateRequest, UniqueCommitRequest, UniqueReleaseRequest,
    UniqueReserveRequest, UniqueReserveResponse, UniqueReserveStatus,
};
use super::db_topic::{DbTopicOperation, ParsedDbTopic};
use super::node_controller::NodeController;
use super::transport::ClusterTransport;
use super::{NodeId, PartitionId};
use bebytes::BeBytes;

pub struct DbPublishResponse {
    pub topic: String,
    pub payload: Vec<u8>,
    pub correlation_data: Option<Vec<u8>>,
}

pub struct DbRequestHandler {
    node_id: NodeId,
}

#[allow(clippy::unused_self)]
impl DbRequestHandler {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self { node_id }
    }

    pub fn handle_publish<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        topic: &str,
        payload: &[u8],
        response_topic: Option<&str>,
        correlation_data: Option<&[u8]>,
    ) -> Option<DbPublishResponse> {
        let parsed = ParsedDbTopic::parse(topic)?;

        let response_payload = match parsed.operation {
            DbTopicOperation::Create { entity } => {
                self.handle_create(controller, parsed.partition?, &entity, payload)
            }
            DbTopicOperation::Read { entity, id } => {
                self.handle_read(controller, parsed.partition?, &entity, &id, payload)
            }
            DbTopicOperation::Update { entity, id } => {
                self.handle_update(controller, parsed.partition?, &entity, &id, payload)
            }
            DbTopicOperation::Delete { entity, id } => {
                self.handle_delete(controller, parsed.partition?, &entity, &id)
            }
            DbTopicOperation::IndexUpdate => {
                self.handle_index_update(controller, parsed.partition?, payload)
            }
            DbTopicOperation::UniqueReserve => {
                self.handle_unique_reserve(controller, parsed.partition?, payload)
            }
            DbTopicOperation::UniqueCommit => {
                self.handle_unique_commit(controller, parsed.partition?, payload)
            }
            DbTopicOperation::UniqueRelease => {
                self.handle_unique_release(controller, parsed.partition?, payload)
            }
            DbTopicOperation::FkValidate => {
                self.handle_fk_validate(controller, parsed.partition?, payload)
            }
            DbTopicOperation::QueryRequest { .. } | DbTopicOperation::QueryResponse { .. } => {
                return None;
            }
        };

        let resp_topic = response_topic?;

        Some(DbPublishResponse {
            topic: resp_topic.to_string(),
            payload: response_payload,
            correlation_data: correlation_data.map(<[u8]>::to_vec),
        })
    }

    fn handle_create<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        partition: PartitionId,
        entity: &str,
        payload: &[u8],
    ) -> Vec<u8> {
        let Ok((request, _)) = DbWriteRequest::try_from_be_bytes(payload) else {
            return DbResponse::error(DbStatus::InvalidRequest).to_be_bytes();
        };

        if !controller.is_local_partition(partition) {
            return DbResponse::error(DbStatus::InvalidPartition).to_be_bytes();
        }

        let id = self.generate_id_for_partition(entity, partition, &request.data);

        match controller.db_create(entity, &id, &request.data, request.timestamp_ms) {
            Ok(db_entity) => DbResponse::ok(&db_entity.to_be_bytes()).to_be_bytes(),
            Err(super::db::DbDataStoreError::AlreadyExists) => {
                DbResponse::error(DbStatus::AlreadyExists).to_be_bytes()
            }
            Err(_) => DbResponse::error(DbStatus::Error).to_be_bytes(),
        }
    }

    fn handle_read<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        partition: PartitionId,
        entity: &str,
        id: &str,
        payload: &[u8],
    ) -> Vec<u8> {
        let Ok((_, _)) = DbReadRequest::try_from_be_bytes(payload) else {
            return DbResponse::error(DbStatus::InvalidRequest).to_be_bytes();
        };

        let expected_partition = data_partition(entity, id);
        if expected_partition != partition {
            return DbResponse::error(DbStatus::InvalidPartition).to_be_bytes();
        }

        if !controller.is_local_partition(partition) {
            return DbResponse::error(DbStatus::InvalidPartition).to_be_bytes();
        }

        match controller.db_get(entity, id) {
            Some(db_entity) => DbResponse::ok(&db_entity.to_be_bytes()).to_be_bytes(),
            None => DbResponse::error(DbStatus::NotFound).to_be_bytes(),
        }
    }

    fn handle_update<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        partition: PartitionId,
        entity: &str,
        id: &str,
        payload: &[u8],
    ) -> Vec<u8> {
        let Ok((request, _)) = DbWriteRequest::try_from_be_bytes(payload) else {
            return DbResponse::error(DbStatus::InvalidRequest).to_be_bytes();
        };

        let expected_partition = data_partition(entity, id);
        if expected_partition != partition {
            return DbResponse::error(DbStatus::InvalidPartition).to_be_bytes();
        }

        if !controller.is_local_partition(partition) {
            return DbResponse::error(DbStatus::InvalidPartition).to_be_bytes();
        }

        match controller.db_update(entity, id, &request.data, request.timestamp_ms) {
            Ok(db_entity) => DbResponse::ok(&db_entity.to_be_bytes()).to_be_bytes(),
            Err(super::db::DbDataStoreError::NotFound) => {
                DbResponse::error(DbStatus::NotFound).to_be_bytes()
            }
            Err(_) => DbResponse::error(DbStatus::Error).to_be_bytes(),
        }
    }

    fn handle_delete<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        partition: PartitionId,
        entity: &str,
        id: &str,
    ) -> Vec<u8> {
        let expected_partition = data_partition(entity, id);
        if expected_partition != partition {
            return DbResponse::error(DbStatus::InvalidPartition).to_be_bytes();
        }

        if !controller.is_local_partition(partition) {
            return DbResponse::error(DbStatus::InvalidPartition).to_be_bytes();
        }

        match controller.db_delete(entity, id) {
            Ok(db_entity) => DbResponse::ok(&db_entity.to_be_bytes()).to_be_bytes(),
            Err(super::db::DbDataStoreError::NotFound) => {
                DbResponse::error(DbStatus::NotFound).to_be_bytes()
            }
            Err(_) => DbResponse::error(DbStatus::Error).to_be_bytes(),
        }
    }

    fn handle_index_update<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        partition: PartitionId,
        payload: &[u8],
    ) -> Vec<u8> {
        let Ok((request, _)) = IndexUpdateRequest::try_from_be_bytes(payload) else {
            return DbResponse::error(DbStatus::InvalidRequest).to_be_bytes();
        };

        if !controller.is_local_partition(partition) {
            return DbResponse::error(DbStatus::InvalidPartition).to_be_bytes();
        }

        let data_partition = PartitionId::new(request.data_partition)
            .unwrap_or_else(|| PartitionId::new(0).unwrap());

        let entry = super::db::IndexEntry::create(
            request.entity_str(),
            request.field_str(),
            &request.value,
            data_partition,
            request.record_id_str(),
        );

        match controller.stores_mut().db_index.add_entry(entry) {
            Ok(()) => DbResponse::ok(&[]).to_be_bytes(),
            Err(_) => DbResponse::error(DbStatus::Error).to_be_bytes(),
        }
    }

    fn handle_unique_reserve<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        partition: PartitionId,
        payload: &[u8],
    ) -> Vec<u8> {
        let Ok((request, _)) = UniqueReserveRequest::try_from_be_bytes(payload) else {
            return UniqueReserveResponse::create(UniqueReserveStatus::Error).to_be_bytes();
        };

        if !controller.is_local_partition(partition) {
            return UniqueReserveResponse::create(UniqueReserveStatus::Error).to_be_bytes();
        }

        let data_partition = PartitionId::new(request.data_partition)
            .unwrap_or_else(|| PartitionId::new(0).unwrap());

        let result = controller.stores_mut().db_unique.reserve(
            request.entity_str(),
            request.field_str(),
            &request.value,
            request.record_id_str(),
            request.request_id_str(),
            data_partition,
            u64::from(request.ttl_ms),
            request.now_ms,
        );

        let status = match result {
            super::db::ReserveResult::Reserved => UniqueReserveStatus::Reserved,
            super::db::ReserveResult::AlreadyReservedBySameRequest => {
                UniqueReserveStatus::AlreadyReservedBySameRequest
            }
            super::db::ReserveResult::Conflict => UniqueReserveStatus::Conflict,
        };

        UniqueReserveResponse::create(status).to_be_bytes()
    }

    fn handle_unique_commit<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        partition: PartitionId,
        payload: &[u8],
    ) -> Vec<u8> {
        let Ok((request, _)) = UniqueCommitRequest::try_from_be_bytes(payload) else {
            return UniqueReserveResponse::create(UniqueReserveStatus::Error).to_be_bytes();
        };

        if !controller.is_local_partition(partition) {
            return UniqueReserveResponse::create(UniqueReserveStatus::Error).to_be_bytes();
        }

        match controller.stores_mut().db_unique.commit(
            request.entity_str(),
            request.field_str(),
            &request.value,
            request.request_id_str(),
        ) {
            Ok(_) => UniqueReserveResponse::create(UniqueReserveStatus::Reserved).to_be_bytes(),
            Err(super::db::UniqueStoreError::NotFound) => {
                UniqueReserveResponse::create(UniqueReserveStatus::Error).to_be_bytes()
            }
            Err(_) => UniqueReserveResponse::create(UniqueReserveStatus::Error).to_be_bytes(),
        }
    }

    fn handle_unique_release<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        partition: PartitionId,
        payload: &[u8],
    ) -> Vec<u8> {
        let Ok((request, _)) = UniqueReleaseRequest::try_from_be_bytes(payload) else {
            return DbResponse::error(DbStatus::InvalidRequest).to_be_bytes();
        };

        if !controller.is_local_partition(partition) {
            return DbResponse::error(DbStatus::InvalidPartition).to_be_bytes();
        }

        let reservation = controller
            .stores()
            .db_unique
            .get_by_request_id(request.request_id_str());

        if let Some(res) = reservation {
            controller.stores_mut().db_unique.release(
                res.entity_str(),
                res.field_str(),
                &res.value,
                request.request_id_str(),
            );
            DbResponse::ok(&[]).to_be_bytes()
        } else {
            DbResponse::error(DbStatus::NotFound).to_be_bytes()
        }
    }

    fn handle_fk_validate<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        partition: PartitionId,
        payload: &[u8],
    ) -> Vec<u8> {
        let Ok((request, _)) = FkValidateRequest::try_from_be_bytes(payload) else {
            return FkValidateResponse::create(FkValidateStatus::Error, "").to_be_bytes();
        };

        if !controller.is_local_partition(partition) {
            return FkValidateResponse::create(FkValidateStatus::Error, request.request_id_str())
                .to_be_bytes();
        }

        let exists = controller.db_get(request.entity_str(), request.id_str()).is_some();

        let status = if exists {
            FkValidateStatus::Valid
        } else {
            FkValidateStatus::Invalid
        };

        FkValidateResponse::create(status, request.request_id_str()).to_be_bytes()
    }

    fn generate_id_for_partition(&self, entity: &str, partition: PartitionId, data: &[u8]) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        entity.hash(&mut hasher);
        data.hash(&mut hasher);
        self.node_id.get().hash(&mut hasher);
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
            .hash(&mut hasher);

        let base_id = hasher.finish();

        for suffix in 0..1000_u16 {
            let id = format!("{base_id:016x}-{suffix:04x}");
            if data_partition(entity, &id) == partition {
                return id;
            }
        }

        format!("{base_id:016x}-p{}", partition.get())
    }
}

impl std::fmt::Debug for DbRequestHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbRequestHandler")
            .field("node_id", &self.node_id)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::transport::{ClusterMessage, InboundMessage, TransportConfig};
    use crate::cluster::{Epoch, PartitionMap};
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    #[derive(Debug)]
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

        fn send(
            &self,
            to: NodeId,
            message: ClusterMessage,
        ) -> Result<(), super::super::transport::TransportError> {
            self.outbox.lock().unwrap().push((to, message));
            Ok(())
        }

        fn broadcast(
            &self,
            message: ClusterMessage,
        ) -> Result<(), super::super::transport::TransportError> {
            self.outbox
                .lock()
                .unwrap()
                .push((NodeId::validated(0).unwrap_or(self.node_id), message));
            Ok(())
        }

        fn send_to_partition_primary(
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
    }

    fn setup_controller_with_partition(partition: PartitionId) -> NodeController<MockTransport> {
        let node1 = NodeId::validated(1).unwrap();
        let transport = MockTransport::new(node1);
        let mut ctrl = NodeController::new(node1, transport, TransportConfig::default());
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

    #[test]
    fn handle_create_success() {
        let node1 = NodeId::validated(1).unwrap();
        let handler = DbRequestHandler::new(node1);

        let entity = "users";
        let data = b"test data";
        let partition = data_partition(entity, "test-id");

        let mut ctrl = setup_controller_with_partition(partition);

        let request = DbWriteRequest::create(data, 1000);
        let payload = request.to_be_bytes();

        let topic = format!("$DB/p{}/{}/create", partition.get(), entity);

        let response = handler.handle_publish(
            &mut ctrl,
            &topic,
            &payload,
            Some("$DB/_resp/client1"),
            Some(b"corr-123"),
        );

        assert!(response.is_some());
        let resp = response.unwrap();
        assert_eq!(resp.topic, "$DB/_resp/client1");
        assert_eq!(resp.correlation_data, Some(b"corr-123".to_vec()));

        let (db_response, _) = DbResponse::try_from_be_bytes(&resp.payload).unwrap();
        assert_eq!(db_response.status(), DbStatus::Ok);
    }

    #[test]
    fn handle_read_not_found() {
        let node1 = NodeId::validated(1).unwrap();
        let handler = DbRequestHandler::new(node1);

        let entity = "users";
        let id = "nonexistent";
        let partition = data_partition(entity, id);

        let mut ctrl = setup_controller_with_partition(partition);

        let request = DbReadRequest::create();
        let payload = request.to_be_bytes();

        let topic = format!("$DB/p{}/{}/{}", partition.get(), entity, id);

        let response = handler.handle_publish(
            &mut ctrl,
            &topic,
            &payload,
            Some("$DB/_resp/client1"),
            None,
        );

        assert!(response.is_some());
        let resp = response.unwrap();

        let (db_response, _) = DbResponse::try_from_be_bytes(&resp.payload).unwrap();
        assert_eq!(db_response.status(), DbStatus::NotFound);
    }

    #[test]
    fn handle_invalid_partition_returns_error() {
        let node1 = NodeId::validated(1).unwrap();
        let handler = DbRequestHandler::new(node1);

        let partition = PartitionId::new(0).unwrap();
        let mut ctrl = setup_controller_with_partition(partition);

        let request = DbWriteRequest::create(b"data", 1000);
        let payload = request.to_be_bytes();

        let topic = "$DB/p63/users/create";

        let response = handler.handle_publish(
            &mut ctrl,
            topic,
            &payload,
            Some("$DB/_resp/client1"),
            None,
        );

        assert!(response.is_some());
        let resp = response.unwrap();

        let (db_response, _) = DbResponse::try_from_be_bytes(&resp.payload).unwrap();
        assert_eq!(db_response.status(), DbStatus::InvalidPartition);
    }

    #[test]
    fn no_response_without_response_topic() {
        let node1 = NodeId::validated(1).unwrap();
        let handler = DbRequestHandler::new(node1);

        let partition = PartitionId::new(0).unwrap();
        let mut ctrl = setup_controller_with_partition(partition);

        let request = DbWriteRequest::create(b"data", 1000);
        let payload = request.to_be_bytes();

        let topic = "$DB/p0/users/create";

        let response = handler.handle_publish(&mut ctrl, topic, &payload, None, None);

        assert!(response.is_none());
    }

    #[test]
    fn parse_invalid_topic_returns_none() {
        let node1 = NodeId::validated(1).unwrap();
        let handler = DbRequestHandler::new(node1);

        let partition = PartitionId::new(0).unwrap();
        let mut ctrl = setup_controller_with_partition(partition);

        let response = handler.handle_publish(
            &mut ctrl,
            "not/a/db/topic",
            &[],
            Some("$DB/_resp/client1"),
            None,
        );

        assert!(response.is_none());
    }
}
