use super::{
    ClusterMessage, ClusterTransport, NodeController, NodeId, PartitionId,
    UNIQUE_REQUEST_TIMEOUT_SECS, UniqueCommitRequest, UniqueCommitResponse, UniqueReleaseRequest,
    UniqueReleaseResponse, UniqueReserveRequest, UniqueReserveResponse, UniqueReserveStatus,
};
use tokio::sync::oneshot;

impl<T: ClusterTransport> NodeController<T> {
    /// # Errors
    /// Returns an error string if a unique constraint violation is detected.
    pub async fn check_unique_constraints(
        &mut self,
        entity: &str,
        id: &str,
        data: &serde_json::Value,
        data_partition: PartitionId,
        request_id: &str,
        now_ms: u64,
    ) -> Result<(), String> {
        let unique_fields = self.stores.constraint_get_unique_fields(entity);
        let mut local_reserved: Vec<(String, Vec<u8>)> = Vec::new();
        let mut remote_reserved: Vec<(String, Vec<u8>, NodeId)> = Vec::new();

        for field in &unique_fields {
            let value = match data.get(field) {
                Some(v) => serde_json::to_vec(v).unwrap_or_default(),
                None => continue,
            };

            let unique_part = super::super::db::unique_partition(entity, field, &value);
            let primary = self.partition_map.primary(unique_part);

            let is_conflict = if primary == Some(self.node_id) {
                let (result, write) = self.stores.unique_reserve_replicated(
                    entity,
                    field,
                    &value,
                    id,
                    request_id,
                    data_partition,
                    30_000,
                    now_ms,
                );

                match result {
                    super::super::db::ReserveResult::Reserved => {
                        if let Some(w) = write {
                            self.write_or_forward(w).await;
                        }
                        local_reserved.push((field.clone(), value));
                        false
                    }
                    super::super::db::ReserveResult::AlreadyReservedBySameRequest => {
                        local_reserved.push((field.clone(), value));
                        false
                    }
                    super::super::db::ReserveResult::Conflict => true,
                }
            } else if let Some(target_node) = primary {
                let result = self
                    .send_unique_reserve_request(
                        target_node,
                        entity,
                        field,
                        &value,
                        id,
                        request_id,
                        data_partition,
                        30_000,
                    )
                    .await;

                match result {
                    Ok(UniqueReserveStatus::Reserved | UniqueReserveStatus::AlreadyReserved) => {
                        remote_reserved.push((field.clone(), value, target_node));
                        false
                    }
                    Ok(UniqueReserveStatus::Conflict | UniqueReserveStatus::Error) | Err(_) => true,
                }
            } else {
                true
            };

            if is_conflict {
                for (f, v) in &local_reserved {
                    if let Some(w) = self
                        .stores
                        .unique_release_replicated(entity, f, v, request_id)
                    {
                        self.write_or_forward(w).await;
                    }
                }
                for (f, v, target) in &remote_reserved {
                    let _ = self
                        .send_unique_release_request(*target, entity, f, v, request_id)
                        .await;
                }
                return Err(field.clone());
            }
        }
        Ok(())
    }

    pub async fn commit_unique_constraints(
        &mut self,
        entity: &str,
        _id: &str,
        data: &serde_json::Value,
        _data_partition: PartitionId,
        request_id: &str,
        _now_ms: u64,
    ) {
        let unique_fields = self.stores.constraint_get_unique_fields(entity);

        for field in &unique_fields {
            let value = match data.get(field) {
                Some(v) => serde_json::to_vec(v).unwrap_or_default(),
                None => continue,
            };

            let unique_part = super::super::db::unique_partition(entity, field, &value);
            let primary = self.partition_map.primary(unique_part);

            if primary == Some(self.node_id) {
                if let Ok((_, w)) = self
                    .stores
                    .unique_commit_replicated(entity, field, &value, request_id)
                {
                    self.write_or_forward(w).await;
                }
            } else if let Some(target_node) = primary {
                let _ = self
                    .send_unique_commit_request(target_node, entity, field, &value, request_id)
                    .await;
            }
        }
    }

    pub async fn release_unique_constraints(
        &mut self,
        entity: &str,
        _id: &str,
        data: &serde_json::Value,
        _data_partition: PartitionId,
        request_id: &str,
        _now_ms: u64,
    ) {
        let unique_fields = self.stores.constraint_get_unique_fields(entity);

        for field in &unique_fields {
            let value = match data.get(field) {
                Some(v) => serde_json::to_vec(v).unwrap_or_default(),
                None => continue,
            };

            let unique_part = super::super::db::unique_partition(entity, field, &value);
            let primary = self.partition_map.primary(unique_part);

            if primary == Some(self.node_id) {
                if let Some(w) = self
                    .stores
                    .unique_release_replicated(entity, field, &value, request_id)
                {
                    self.write_or_forward(w).await;
                }
            } else if let Some(target_node) = primary {
                let _ = self
                    .send_unique_release_request(target_node, entity, field, &value, request_id)
                    .await;
            }
        }
    }

    pub(crate) async fn handle_unique_reserve_request(
        &mut self,
        from: NodeId,
        req: &UniqueReserveRequest,
    ) {
        let entity = req.entity_str();
        let field = req.field_str();
        let record_id = req.record_id_str();
        let idempotency_key = req.idempotency_key_str();

        let status = if let Some(data_partition) = req.data_partition() {
            let result = self.stores.unique_reserve(
                entity,
                field,
                &req.value,
                record_id,
                idempotency_key,
                data_partition,
                req.ttl_ms,
                Self::current_time_ms(),
            );
            match result {
                super::super::db::ReserveResult::Reserved => UniqueReserveStatus::Reserved,
                super::super::db::ReserveResult::AlreadyReservedBySameRequest => {
                    UniqueReserveStatus::AlreadyReserved
                }
                super::super::db::ReserveResult::Conflict => UniqueReserveStatus::Conflict,
            }
        } else {
            UniqueReserveStatus::Error
        };

        let response = UniqueReserveResponse::create(req.request_id, status);
        let _ = self
            .transport
            .send(from, ClusterMessage::UniqueReserveResponse(response))
            .await;
    }

    pub(crate) fn handle_unique_reserve_response(&mut self, resp: &UniqueReserveResponse) {
        if let Some(tx) = self.pending_unique_requests.remove(&resp.request_id) {
            let _ = tx.send(resp.status());
        }
    }

    pub(crate) async fn handle_unique_commit_request(
        &mut self,
        from: NodeId,
        req: &UniqueCommitRequest,
    ) {
        let entity = req.entity_str();
        let field = req.field_str();
        let idempotency_key = req.idempotency_key_str();

        let success = self
            .stores
            .unique_commit(entity, field, &req.value, idempotency_key);

        let response = UniqueCommitResponse::create(req.request_id, success);
        let _ = self
            .transport
            .send(from, ClusterMessage::UniqueCommitResponse(response))
            .await;
    }

    pub(crate) fn handle_unique_commit_response(&mut self, resp: &UniqueCommitResponse) {
        if let Some(tx) = self.pending_unique_commit_requests.remove(&resp.request_id) {
            let _ = tx.send(resp.is_success());
        }
    }

    pub(crate) async fn handle_unique_release_request(
        &mut self,
        from: NodeId,
        req: &UniqueReleaseRequest,
    ) {
        let entity = req.entity_str();
        let field = req.field_str();
        let idempotency_key = req.idempotency_key_str();

        let success = self
            .stores
            .unique_release(entity, field, &req.value, idempotency_key);

        let response = UniqueReleaseResponse::create(req.request_id, success);
        let _ = self
            .transport
            .send(from, ClusterMessage::UniqueReleaseResponse(response))
            .await;
    }

    pub(crate) fn handle_unique_release_response(&mut self, resp: &UniqueReleaseResponse) {
        if let Some(tx) = self
            .pending_unique_release_requests
            .remove(&resp.request_id)
        {
            let _ = tx.send(resp.is_success());
        }
    }

    fn allocate_unique_request_id(&mut self) -> u64 {
        let id = self.next_unique_request_id;
        self.next_unique_request_id += 1;
        id
    }

    /// # Errors
    /// Returns transport error if message send fails
    #[allow(clippy::too_many_arguments)]
    pub async fn send_unique_reserve_request(
        &mut self,
        target_node: NodeId,
        entity: &str,
        field: &str,
        value: &[u8],
        record_id: &str,
        idempotency_key: &str,
        data_partition: PartitionId,
        ttl_ms: u64,
    ) -> Result<UniqueReserveStatus, super::super::transport::TransportError> {
        let request_id = self.allocate_unique_request_id();
        let (tx, mut rx) = oneshot::channel();

        self.pending_unique_requests.insert(request_id, tx);

        let request = UniqueReserveRequest::create(
            request_id,
            entity,
            field,
            value,
            record_id,
            idempotency_key,
            data_partition,
            ttl_ms,
        );

        self.transport
            .send(target_node, ClusterMessage::UniqueReserveRequest(request))
            .await?;

        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_secs(UNIQUE_REQUEST_TIMEOUT_SECS);

        loop {
            match rx.try_recv() {
                Ok(status) => return Ok(status),
                Err(oneshot::error::TryRecvError::Closed) => return Ok(UniqueReserveStatus::Error),
                Err(oneshot::error::TryRecvError::Empty) => {}
            }

            let mut requeue_msgs = Vec::new();
            while let Some(msg) = self.transport.recv() {
                if let ClusterMessage::UniqueReserveResponse(ref resp) = msg.message {
                    self.handle_unique_reserve_response(resp);
                } else if let ClusterMessage::UniqueCommitResponse(ref resp) = msg.message {
                    self.handle_unique_commit_response(resp);
                } else if let ClusterMessage::UniqueReleaseResponse(ref resp) = msg.message {
                    self.handle_unique_release_response(resp);
                } else {
                    requeue_msgs.push(msg);
                }
            }
            for msg in requeue_msgs.into_iter().rev() {
                self.transport.requeue(msg);
            }

            if tokio::time::Instant::now() >= deadline {
                self.pending_unique_requests.remove(&request_id);
                return Ok(UniqueReserveStatus::Error);
            }

            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    /// # Errors
    /// Returns transport error if message send fails
    pub async fn send_unique_commit_request(
        &mut self,
        target_node: NodeId,
        entity: &str,
        field: &str,
        value: &[u8],
        idempotency_key: &str,
    ) -> Result<bool, super::super::transport::TransportError> {
        let request_id = self.allocate_unique_request_id();
        let (tx, mut rx) = oneshot::channel();

        self.pending_unique_commit_requests.insert(request_id, tx);

        let request =
            UniqueCommitRequest::create(request_id, entity, field, value, idempotency_key);

        self.transport
            .send(target_node, ClusterMessage::UniqueCommitRequest(request))
            .await?;

        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_secs(UNIQUE_REQUEST_TIMEOUT_SECS);

        loop {
            match rx.try_recv() {
                Ok(success) => return Ok(success),
                Err(oneshot::error::TryRecvError::Closed) => return Ok(false),
                Err(oneshot::error::TryRecvError::Empty) => {}
            }

            let mut requeue_msgs = Vec::new();
            while let Some(msg) = self.transport.recv() {
                if let ClusterMessage::UniqueReserveResponse(ref resp) = msg.message {
                    self.handle_unique_reserve_response(resp);
                } else if let ClusterMessage::UniqueCommitResponse(ref resp) = msg.message {
                    self.handle_unique_commit_response(resp);
                } else if let ClusterMessage::UniqueReleaseResponse(ref resp) = msg.message {
                    self.handle_unique_release_response(resp);
                } else {
                    requeue_msgs.push(msg);
                }
            }
            for msg in requeue_msgs.into_iter().rev() {
                self.transport.requeue(msg);
            }

            if tokio::time::Instant::now() >= deadline {
                self.pending_unique_commit_requests.remove(&request_id);
                return Ok(false);
            }

            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    /// # Errors
    /// Returns transport error if message send fails
    pub async fn send_unique_release_request(
        &mut self,
        target_node: NodeId,
        entity: &str,
        field: &str,
        value: &[u8],
        idempotency_key: &str,
    ) -> Result<bool, super::super::transport::TransportError> {
        let request_id = self.allocate_unique_request_id();
        let (tx, mut rx) = oneshot::channel();

        self.pending_unique_release_requests.insert(request_id, tx);

        let request =
            UniqueReleaseRequest::create(request_id, entity, field, value, idempotency_key);

        self.transport
            .send(target_node, ClusterMessage::UniqueReleaseRequest(request))
            .await?;

        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_secs(UNIQUE_REQUEST_TIMEOUT_SECS);

        loop {
            match rx.try_recv() {
                Ok(success) => return Ok(success),
                Err(oneshot::error::TryRecvError::Closed) => return Ok(false),
                Err(oneshot::error::TryRecvError::Empty) => {}
            }

            let mut requeue_msgs = Vec::new();
            while let Some(msg) = self.transport.recv() {
                if let ClusterMessage::UniqueReserveResponse(ref resp) = msg.message {
                    self.handle_unique_reserve_response(resp);
                } else if let ClusterMessage::UniqueCommitResponse(ref resp) = msg.message {
                    self.handle_unique_commit_response(resp);
                } else if let ClusterMessage::UniqueReleaseResponse(ref resp) = msg.message {
                    self.handle_unique_release_response(resp);
                } else {
                    requeue_msgs.push(msg);
                }
            }
            for msg in requeue_msgs.into_iter().rev() {
                self.transport.requeue(msg);
            }

            if tokio::time::Instant::now() >= deadline {
                self.pending_unique_release_requests.remove(&request_id);
                return Ok(false);
            }

            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
}
