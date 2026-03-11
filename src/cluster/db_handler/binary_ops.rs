// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::super::PartitionId;
use super::super::db::{self, IndexEntry, data_partition};
use super::super::db_protocol::{
    DbReadRequest, DbResponse, DbStatus, DbWriteRequest, FkValidateRequest, FkValidateResponse,
    FkValidateStatus, IndexUpdateRequest, UniqueCommitRequest, UniqueReleaseRequest,
    UniqueReserveRequest, UniqueReserveResponse, UniqueReserveStatus,
};
use super::super::db_topic::{DbTopicOperation, ParsedDbTopic};
use super::super::node_controller::NodeController;
use super::super::transport::ClusterTransport;
use super::DbRequestHandler;
use bebytes::BeBytes;

#[allow(clippy::unused_self)]
impl DbRequestHandler {
    pub(super) async fn handle_binary_operation<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        parsed: &ParsedDbTopic,
        payload: &[u8],
    ) -> Option<Vec<u8>> {
        let partition = parsed.partition?;
        Some(match &parsed.operation {
            DbTopicOperation::Create { entity } => {
                self.handle_create(controller, partition, entity, payload)
                    .await
            }
            DbTopicOperation::Read { entity, id } => {
                self.handle_read(controller, partition, entity, id, payload)
            }
            DbTopicOperation::Update { entity, id } => {
                self.handle_update(controller, partition, entity, id, payload)
                    .await
            }
            DbTopicOperation::Delete { entity, id } => {
                self.handle_delete(controller, partition, entity, id).await
            }
            DbTopicOperation::IndexUpdate => {
                self.handle_index_update(controller, partition, payload)
            }
            DbTopicOperation::UniqueReserve => {
                self.handle_unique_reserve(controller, partition, payload)
            }
            DbTopicOperation::UniqueCommit => {
                self.handle_unique_commit(controller, partition, payload)
            }
            DbTopicOperation::UniqueRelease => {
                self.handle_unique_release(controller, partition, payload)
            }
            DbTopicOperation::FkValidate => self.handle_fk_validate(controller, partition, payload),
            _ => return None,
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

        if !controller.is_primary_for_partition(partition) {
            return DbResponse::error(DbStatus::InvalidPartition).to_be_bytes();
        }

        let id = self.generate_id_for_partition(entity, partition, &request.data);

        match controller
            .db_create(entity, &id, &request.data, request.timestamp_ms)
            .await
        {
            Ok(db_entity) => DbResponse::ok(&db_entity.to_be_bytes()).to_be_bytes(),
            Err(db::DbDataStoreError::AlreadyExists) => {
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

        if !controller.can_serve_reads(partition) {
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

        if !controller.is_primary_for_partition(partition) {
            return DbResponse::error(DbStatus::InvalidPartition).to_be_bytes();
        }

        match controller
            .db_update(entity, id, &request.data, request.timestamp_ms)
            .await
        {
            Ok(db_entity) => DbResponse::ok(&db_entity.to_be_bytes()).to_be_bytes(),
            Err(db::DbDataStoreError::NotFound) => {
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

        if !controller.is_primary_for_partition(partition) {
            return DbResponse::error(DbStatus::InvalidPartition).to_be_bytes();
        }

        match controller.db_delete(entity, id).await {
            Ok(db_entity) => DbResponse::ok(&db_entity.to_be_bytes()).to_be_bytes(),
            Err(db::DbDataStoreError::NotFound) => {
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

        if !controller.is_primary_for_partition(partition) {
            return DbResponse::error(DbStatus::InvalidPartition).to_be_bytes();
        }

        let data_partition = PartitionId::new(request.data_partition).unwrap_or(PartitionId::ZERO);

        let entry = IndexEntry::create(
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

        if !controller.is_primary_for_partition(partition) {
            return UniqueReserveResponse::create(UniqueReserveStatus::Error).to_be_bytes();
        }

        let data_partition = PartitionId::new(request.data_partition).unwrap_or(PartitionId::ZERO);

        let reserve_params = db::UniqueReserveParams {
            entity: request.entity_str(),
            field: request.field_str(),
            value: &request.value,
            record_id: request.record_id_str(),
            request_id: request.request_id_str(),
            data_partition,
            ttl_ms: u64::from(request.ttl_ms),
        };
        let result = controller
            .stores_mut()
            .db_unique
            .reserve(&reserve_params, request.now_ms);

        let status = match result {
            db::ReserveResult::Reserved => UniqueReserveStatus::Reserved,
            db::ReserveResult::AlreadyReservedBySameRequest => {
                UniqueReserveStatus::AlreadyReservedBySameRequest
            }
            db::ReserveResult::Conflict => UniqueReserveStatus::Conflict,
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

        if !controller.is_primary_for_partition(partition) {
            return UniqueReserveResponse::create(UniqueReserveStatus::Error).to_be_bytes();
        }

        match controller.stores_mut().db_unique.commit(
            request.entity_str(),
            request.field_str(),
            &request.value,
            request.request_id_str(),
        ) {
            Ok(_) => UniqueReserveResponse::create(UniqueReserveStatus::Reserved).to_be_bytes(),
            Err(db::UniqueStoreError::NotFound) => {
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

        if !controller.is_primary_for_partition(partition) {
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

        if !controller.can_serve_reads(partition) {
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
}
