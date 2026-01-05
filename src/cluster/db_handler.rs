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
use serde_json::{json, Value};

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

    pub async fn handle_publish<T: ClusterTransport>(
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
                    .await
            }
            DbTopicOperation::Read { entity, id } => {
                self.handle_read(controller, parsed.partition?, &entity, &id, payload)
            }
            DbTopicOperation::Update { entity, id } => {
                self.handle_update(controller, parsed.partition?, &entity, &id, payload)
                    .await
            }
            DbTopicOperation::Delete { entity, id } => {
                self.handle_delete(controller, parsed.partition?, &entity, &id)
                    .await
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
            DbTopicOperation::JsonCreate { entity } => {
                self.handle_json_create(controller, &entity, payload).await
            }
            DbTopicOperation::JsonRead { entity, id } => {
                self.handle_json_read(controller, &entity, &id)
            }
            DbTopicOperation::JsonUpdate { entity, id } => {
                self.handle_json_update(controller, &entity, &id, payload)
                    .await
            }
            DbTopicOperation::JsonDelete { entity, id } => {
                self.handle_json_delete(controller, &entity, &id).await
            }
            DbTopicOperation::JsonList { entity } => {
                self.handle_json_list(controller, &entity, payload)
            }
        };

        let resp_topic = response_topic?;

        Some(DbPublishResponse {
            topic: resp_topic.to_string(),
            payload: response_payload,
            correlation_data: correlation_data.map(<[u8]>::to_vec),
        })
    }

    async fn handle_create<T: ClusterTransport>(
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

        match controller
            .db_create(entity, &id, &request.data, request.timestamp_ms)
            .await
        {
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

    async fn handle_update<T: ClusterTransport>(
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

        match controller
            .db_update(entity, id, &request.data, request.timestamp_ms)
            .await
        {
            Ok(db_entity) => DbResponse::ok(&db_entity.to_be_bytes()).to_be_bytes(),
            Err(super::db::DbDataStoreError::NotFound) => {
                DbResponse::error(DbStatus::NotFound).to_be_bytes()
            }
            Err(_) => DbResponse::error(DbStatus::Error).to_be_bytes(),
        }
    }

    async fn handle_delete<T: ClusterTransport>(
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

        match controller.db_delete(entity, id).await {
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

        let exists = controller
            .db_get(request.entity_str(), request.id_str())
            .is_some();

        let status = if exists {
            FkValidateStatus::Valid
        } else {
            FkValidateStatus::Invalid
        };

        FkValidateResponse::create(status, request.request_id_str()).to_be_bytes()
    }

    async fn handle_json_create<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        entity: &str,
        payload: &[u8],
    ) -> Vec<u8> {
        let data: Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(_) => return Self::json_error(400, "invalid JSON payload"),
        };

        let partition = controller.pick_partition_for_create();
        let id = self.generate_id_for_partition(entity, partition, payload);

        if !controller.is_local_partition(partition) {
            return Self::json_error(503, "partition not local, forwarding not yet implemented");
        }

        let data_bytes = serde_json::to_vec(&data).unwrap_or_default();
        let now_ms = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis()),
        )
        .unwrap_or(u64::MAX);

        match controller.db_create(entity, &id, &data_bytes, now_ms).await {
            Ok(db_entity) => {
                let result = json!({
                    "status": "ok",
                    "data": {
                        "id": db_entity.id_str(),
                        "entity": entity,
                        "data": data
                    }
                });
                serde_json::to_vec(&result).unwrap_or_default()
            }
            Err(super::db::DbDataStoreError::AlreadyExists) => {
                Self::json_error(409, "entity already exists")
            }
            Err(_) => Self::json_error(500, "internal error"),
        }
    }

    fn handle_json_read<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        entity: &str,
        id: &str,
    ) -> Vec<u8> {
        let partition = data_partition(entity, id);

        if !controller.is_local_partition(partition) {
            return Self::json_error(503, "partition not local, forwarding not yet implemented");
        }

        match controller.db_get(entity, id) {
            Some(db_entity) => {
                let data: Value =
                    serde_json::from_slice(&db_entity.data).unwrap_or(Value::Null);
                let result = json!({
                    "status": "ok",
                    "data": {
                        "id": db_entity.id_str(),
                        "entity": entity,
                        "data": data
                    }
                });
                serde_json::to_vec(&result).unwrap_or_default()
            }
            None => Self::json_error(404, &format!("entity not found: {entity} id={id}")),
        }
    }

    async fn handle_json_update<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        entity: &str,
        id: &str,
        payload: &[u8],
    ) -> Vec<u8> {
        let data: Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(_) => return Self::json_error(400, "invalid JSON payload"),
        };

        let partition = data_partition(entity, id);

        if !controller.is_local_partition(partition) {
            return Self::json_error(503, "partition not local, forwarding not yet implemented");
        }

        let data_bytes = serde_json::to_vec(&data).unwrap_or_default();
        let now_ms = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis()),
        )
        .unwrap_or(u64::MAX);

        match controller.db_update(entity, id, &data_bytes, now_ms).await {
            Ok(db_entity) => {
                let result = json!({
                    "status": "ok",
                    "data": {
                        "id": db_entity.id_str(),
                        "entity": entity,
                        "data": data
                    }
                });
                serde_json::to_vec(&result).unwrap_or_default()
            }
            Err(super::db::DbDataStoreError::NotFound) => {
                Self::json_error(404, &format!("entity not found: {entity} id={id}"))
            }
            Err(_) => Self::json_error(500, "internal error"),
        }
    }

    async fn handle_json_delete<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        entity: &str,
        id: &str,
    ) -> Vec<u8> {
        let partition = data_partition(entity, id);

        if !controller.is_local_partition(partition) {
            return Self::json_error(503, "partition not local, forwarding not yet implemented");
        }

        match controller.db_delete(entity, id).await {
            Ok(_) => {
                let result = json!({
                    "status": "ok",
                    "data": {
                        "id": id,
                        "entity": entity,
                        "deleted": true
                    }
                });
                serde_json::to_vec(&result).unwrap_or_default()
            }
            Err(super::db::DbDataStoreError::NotFound) => {
                Self::json_error(404, &format!("entity not found: {entity} id={id}"))
            }
            Err(_) => Self::json_error(500, "internal error"),
        }
    }

    fn handle_json_list<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        entity: &str,
        payload: &[u8],
    ) -> Vec<u8> {
        let filters: Vec<crate::Filter> = if payload.is_empty() {
            Vec::new()
        } else if let Ok(data) = serde_json::from_slice::<Value>(payload) {
            data.get("filters")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        let entities = controller.db_list(entity);
        let items: Vec<Value> = entities
            .iter()
            .filter_map(|e| {
                let data: Value = serde_json::from_slice(&e.data).ok()?;
                if Self::matches_filters(&data, &filters) {
                    Some(json!({
                        "id": e.id_str(),
                        "data": data
                    }))
                } else {
                    None
                }
            })
            .collect();

        let result = json!({
            "status": "ok",
            "data": items
        });
        serde_json::to_vec(&result).unwrap_or_default()
    }

    fn matches_filters(entity: &Value, filters: &[crate::Filter]) -> bool {
        for filter in filters {
            if let Some(field_value) = entity.get(&filter.field) {
                if !filter.matches(field_value) {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }

    fn json_error(code: u16, message: &str) -> Vec<u8> {
        let result = json!({
            "status": "error",
            "code": code,
            "message": message
        });
        serde_json::to_vec(&result).unwrap_or_default()
    }

    fn generate_id_for_partition(
        &self,
        entity: &str,
        partition: PartitionId,
        data: &[u8],
    ) -> String {
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

        async fn queue_local_publish(&self, _topic: String, _payload: Vec<u8>, _qos: u8) {}

        async fn queue_local_publish_retained(&self, _topic: String, _payload: Vec<u8>, _qos: u8) {}
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

        let partition = PartitionId::new(0).unwrap();
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

        let partition = PartitionId::new(0).unwrap();
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

        let partition = PartitionId::new(0).unwrap();
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
}
