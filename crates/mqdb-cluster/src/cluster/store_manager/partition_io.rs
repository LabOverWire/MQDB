// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{StoreApplyError, StoreManager};
use crate::cluster::{PartitionId, entity};

impl StoreManager {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn export_partition(&self, partition: PartitionId) -> Vec<u8> {
        let mut buf = Vec::new();

        let store_data = [
            (
                entity::SESSIONS,
                self.sessions.export_for_partition(partition),
            ),
            (entity::QOS2, self.qos2.export_for_partition(partition)),
            (
                entity::SUBSCRIPTIONS,
                self.subscriptions.export_for_partition(partition),
            ),
            (
                entity::RETAINED,
                self.retained.export_for_partition(partition),
            ),
            (
                entity::TOPIC_INDEX,
                self.topics.export_for_partition(partition),
            ),
            (
                entity::WILDCARDS,
                self.wildcards.export_for_partition(partition),
            ),
            (
                entity::INFLIGHT,
                self.inflight.export_for_partition(partition),
            ),
            (
                entity::OFFSETS,
                self.offsets.export_for_partition(partition),
            ),
            (
                entity::IDEMPOTENCY,
                self.idempotency.export_for_partition(partition),
            ),
            (
                entity::DB_DATA,
                self.db_data.export_for_partition(partition),
            ),
            (
                entity::DB_SCHEMA,
                self.db_schema.export_for_partition(partition),
            ),
            (
                entity::DB_INDEX,
                self.db_index.export_for_partition(partition),
            ),
            (
                entity::DB_UNIQUE,
                self.db_unique.export_for_partition(partition),
            ),
            (entity::DB_FK, self.db_fk.export_for_partition(partition)),
            (
                entity::DB_CONSTRAINT,
                self.db_constraints.export_for_partition(partition),
            ),
        ];

        buf.extend_from_slice(&(store_data.len() as u8).to_be_bytes());

        for (entity_name, data) in store_data {
            let name_bytes = entity_name.as_bytes();
            buf.push(name_bytes.len() as u8);
            buf.extend_from_slice(name_bytes);
            buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
            buf.extend_from_slice(&data);
        }

        buf
    }

    /// # Errors
    /// Returns `StoreApplyError` if the snapshot data is malformed or a store import fails.
    pub fn import_partition(&self, data: &[u8]) -> Result<usize, StoreApplyError> {
        if data.is_empty() {
            return Ok(0);
        }

        let store_count = data[0] as usize;
        let mut offset = 1;
        let mut total_imported = 0;

        for _ in 0..store_count {
            if offset >= data.len() {
                break;
            }

            let name_len = data[offset] as usize;
            offset += 1;

            if offset + name_len > data.len() {
                break;
            }
            let entity_name = std::str::from_utf8(&data[offset..offset + name_len])
                .map_err(|_| StoreApplyError::UnknownEntity)?;
            offset += name_len;

            if offset + 4 > data.len() {
                break;
            }
            let data_len = u32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + data_len > data.len() {
                break;
            }
            let store_data = &data[offset..offset + data_len];
            offset += data_len;

            let imported = match entity_name {
                entity::SESSIONS => self
                    .sessions
                    .import_sessions(store_data)
                    .map_err(|_| StoreApplyError::SessionError)?,
                entity::QOS2 => self
                    .qos2
                    .import_states(store_data)
                    .map_err(|_| StoreApplyError::Qos2Error)?,
                entity::SUBSCRIPTIONS => self
                    .subscriptions
                    .import_subscriptions(store_data)
                    .map_err(|_| StoreApplyError::SubscriptionError)?,
                entity::RETAINED => self
                    .retained
                    .import_retained(store_data)
                    .map_err(|_| StoreApplyError::RetainedError)?,
                entity::TOPIC_INDEX => self
                    .topics
                    .import_entries(store_data)
                    .map_err(|_| StoreApplyError::TopicIndexError)?,
                entity::WILDCARDS => self
                    .wildcards
                    .import_wildcards(store_data)
                    .map_err(|_| StoreApplyError::WildcardError)?,
                entity::INFLIGHT => self
                    .inflight
                    .import_inflight(store_data)
                    .map_err(|_| StoreApplyError::InflightError)?,
                entity::OFFSETS => self
                    .offsets
                    .import_offsets(store_data)
                    .map_err(|_| StoreApplyError::OffsetError)?,
                entity::IDEMPOTENCY => self
                    .idempotency
                    .import_records(store_data)
                    .map_err(|_| StoreApplyError::IdempotencyError)?,
                entity::DB_DATA => self
                    .db_data
                    .import_entities(store_data)
                    .map_err(|_| StoreApplyError::DbDataError)?,
                entity::DB_SCHEMA => self
                    .db_schema
                    .import_schemas(store_data)
                    .map_err(|_| StoreApplyError::DbSchemaError)?,
                entity::DB_INDEX => self
                    .db_index
                    .import_entries(store_data)
                    .map_err(|_| StoreApplyError::DbIndexError)?,
                entity::DB_UNIQUE => self
                    .db_unique
                    .import_reservations(store_data)
                    .map_err(|_| StoreApplyError::DbUniqueError)?,
                entity::DB_FK => self
                    .db_fk
                    .import_requests(store_data)
                    .map_err(|_| StoreApplyError::DbFkError)?,
                entity::DB_CONSTRAINT => self
                    .db_constraints
                    .import_constraints(store_data)
                    .map_err(|_| StoreApplyError::DbConstraintError)?,
                _ => continue,
            };

            total_imported += imported;
        }

        self.rebuild_fk_indexes_after_import();

        Ok(total_imported)
    }

    fn rebuild_fk_indexes_after_import(&self) {
        for constraint in self.db_constraints.list_all() {
            if constraint.is_foreign_key() {
                self.rebuild_fk_index_for_constraint(&constraint);
            }
        }
    }

    pub fn clear_partition(&self, partition: PartitionId) -> usize {
        let mut total_cleared = 0;
        total_cleared += self.sessions.clear_partition(partition);
        total_cleared += self.qos2.clear_partition(partition);
        total_cleared += self.subscriptions.clear_partition(partition);
        total_cleared += self.retained.clear_partition(partition);
        total_cleared += self.topics.clear_partition(partition);
        total_cleared += self.wildcards.clear_partition(partition);
        total_cleared += self.inflight.clear_partition(partition);
        total_cleared += self.offsets.clear_partition(partition);
        total_cleared += self.idempotency.clear_partition(partition);
        total_cleared += self.db_data.clear_partition(partition);
        total_cleared += self.db_schema.clear_partition(partition);
        total_cleared += self.db_index.clear_partition(partition);
        total_cleared += self.db_unique.clear_partition(partition);
        total_cleared += self.db_fk.clear_partition(partition);
        total_cleared += self.db_constraints.clear_partition(partition);
        total_cleared
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::NodeId;

    fn node(id: u16) -> NodeId {
        NodeId::validated(id).unwrap()
    }

    #[test]
    fn export_import_roundtrip_preserves_db_data() {
        let src = StoreManager::new(node(1));
        let dst = StoreManager::new(node(2));

        src.db_data
            .create("notes", "n1", b"{\"title\":\"hello\"}", 1000)
            .unwrap();
        src.db_data
            .create("notes", "n2", b"{\"title\":\"world\"}", 2000)
            .unwrap();

        let partition = src.db_data.get("notes", "n1").unwrap().partition();

        let payload = src.export_partition(partition);
        assert!(!payload.is_empty(), "snapshot payload must not be empty");

        let imported = dst.import_partition(&payload).expect("import");
        assert!(
            imported >= 1,
            "at least one record for partition must survive the roundtrip"
        );

        let note = dst
            .db_data
            .get("notes", "n1")
            .expect("n1 must exist on destination after snapshot import");
        assert_eq!(note.data, b"{\"title\":\"hello\"}");
    }

    #[test]
    fn export_import_roundtrip_covers_all_db_stores() {
        let src = StoreManager::new(node(1));
        let dst = StoreManager::new(node(2));

        let entities = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta"];
        for entity in &entities {
            populate_all_db_stores(&src, entity);
        }

        let target = src.db_data.get("alpha", "rec-1").unwrap().partition();

        let payload = src.export_partition(target);
        assert!(!payload.is_empty());
        let total_imported = dst.import_partition(&payload).expect("import");
        assert!(total_imported >= 1);

        for entity in &entities {
            assert_per_store_partition_filter(&src, &dst, entity, target);
        }
    }

    fn populate_all_db_stores(store: &StoreManager, entity: &str) {
        use crate::cluster::db::{
            ClusterConstraint, FkValidationRequest, IndexEntry, UniqueReserveParams,
        };
        use mqdb_core::partition::data_partition;

        store.db_data.create(entity, "rec-1", b"{}", 1000).unwrap();
        store
            .db_schema
            .register(entity, b"{\"fields\":[]}")
            .unwrap();
        store
            .db_constraints
            .add(ClusterConstraint::unique(
                entity,
                &format!("uniq_{entity}"),
                "email",
            ))
            .unwrap();

        let value = format!("v-{entity}");
        let dp = data_partition(entity, "rec-1");
        store
            .db_index
            .add_entry(IndexEntry::create(
                entity,
                "email",
                value.as_bytes(),
                dp,
                "rec-1",
            ))
            .unwrap();
        store.db_unique.reserve(
            &UniqueReserveParams {
                entity,
                field: "email",
                value: value.as_bytes(),
                record_id: "rec-1",
                request_id: &format!("req-{entity}"),
                data_partition: dp,
                ttl_ms: 60_000,
            },
            1_000,
        );
        store
            .db_fk
            .add_request(FkValidationRequest::create(
                entity,
                "rec-1",
                &format!("fk-{entity}"),
                5000,
                1000,
            ))
            .unwrap();
    }

    fn assert_per_store_partition_filter(
        src: &StoreManager,
        dst: &StoreManager,
        entity: &str,
        target: PartitionId,
    ) {
        use crate::cluster::db::IndexEntry;

        let src_data_partition = src.db_data.get(entity, "rec-1").unwrap().partition();
        assert_eq!(
            dst.db_data.get(entity, "rec-1").is_some(),
            src_data_partition == target,
            "db_data for {entity}"
        );

        let value = format!("v-{entity}");
        if let Some(ip) = src
            .db_index
            .lookup(entity, "email", value.as_bytes())
            .first()
            .map(IndexEntry::index_partition)
        {
            let dst_has = !dst
                .db_index
                .lookup(entity, "email", value.as_bytes())
                .is_empty();
            assert_eq!(dst_has, ip == target, "db_index for {entity}");
        }

        if let Some(up) = src
            .db_unique
            .get(entity, "email", value.as_bytes())
            .map(|r| r.unique_partition())
        {
            let dst_has = dst
                .db_unique
                .get(entity, "email", value.as_bytes())
                .is_some();
            assert_eq!(dst_has, up == target, "db_unique for {entity}");
        }

        assert_eq!(
            dst.db_schema.get(entity).is_some(),
            src.db_schema.get(entity).unwrap().partition() == target,
            "db_schema for {entity}"
        );

        let constraint_name = format!("uniq_{entity}");
        assert_eq!(
            dst.db_constraints.get(entity, &constraint_name).is_some(),
            src.db_constraints
                .get(entity, &constraint_name)
                .unwrap()
                .partition()
                == target,
            "db_constraints for {entity}"
        );

        let fk_id = format!("fk-{entity}");
        assert_eq!(
            dst.db_fk.get_request(&fk_id).is_some(),
            src.db_fk.get_request(&fk_id).unwrap().target_partition() == target,
            "db_fk for {entity}"
        );
    }

    #[test]
    fn import_partition_rebuilds_fk_reverse_index() {
        use crate::cluster::db::{ClusterConstraint, OnDeleteAction};

        let src = StoreManager::new(node(1));
        let dst = StoreManager::new(node(2));

        let fk = ClusterConstraint::foreign_key(
            "comments",
            "fk_comment_post",
            "post_id",
            "posts",
            "id",
            OnDeleteAction::Cascade,
        );
        src.db_constraints.add(fk.clone()).unwrap();
        dst.db_constraints.add(fk).unwrap();

        src.db_data
            .create("posts", "p1", b"{\"title\":\"hello\"}", 1_000)
            .unwrap();
        src.db_data
            .create(
                "comments",
                "c1",
                b"{\"post_id\":\"p1\",\"text\":\"first\"}",
                1_000,
            )
            .unwrap();
        src.update_fk_reverse_index(
            crate::cluster::protocol::Operation::Insert,
            "comments",
            "c1",
            Some(b"{\"post_id\":\"p1\",\"text\":\"first\"}"),
            None,
        );

        assert_eq!(
            src.fk_reverse_lookup("posts", "p1", "comments", "post_id"),
            vec!["c1".to_string()],
            "src reverse index must hold c1 → p1 before export",
        );

        let child_partition = src.db_data.get("comments", "c1").unwrap().partition();

        let payload = src.export_partition(child_partition);
        assert!(!payload.is_empty(), "snapshot payload must not be empty");
        let imported = dst.import_partition(&payload).expect("import");
        assert!(imported >= 1, "child record must survive import");

        assert!(
            dst.db_data.get("comments", "c1").is_some(),
            "child must be present on destination after import",
        );

        let refs = dst.fk_reverse_lookup("posts", "p1", "comments", "post_id");
        assert_eq!(
            refs,
            vec!["c1".to_string()],
            "destination reverse index must hold c1 after import — without the rebuild step this would be empty",
        );
    }
}
