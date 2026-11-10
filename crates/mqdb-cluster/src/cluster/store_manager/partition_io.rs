// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::outbox::ClusterOutbox;
use super::snapshot_writes::{SnapshotSections, processing_claims, snapshot_writes};
use super::transfer_window::{GLOBAL_SECTIONS, TRACKED_SECTIONS};
use super::{StoreApplyError, StoreManager};
use crate::cluster::protocol::{Operation, ReplicationWrite};
use crate::cluster::{Epoch, PartitionId, entity};
use std::collections::HashSet;

const REPLACED_SECTIONS: [&str; 10] = [
    entity::SESSIONS,
    entity::QOS2,
    entity::SUBSCRIPTIONS,
    entity::RETAINED,
    entity::INFLIGHT,
    entity::OFFSETS,
    entity::IDEMPOTENCY,
    entity::DB_DATA,
    entity::DB_INDEX,
    entity::DB_FK,
];

const SNAPSHOT_SECTIONS: [&str; 16] = [
    entity::SESSIONS,
    entity::QOS2,
    entity::SUBSCRIPTIONS,
    entity::RETAINED,
    entity::TOPIC_INDEX,
    entity::WILDCARDS,
    entity::INFLIGHT,
    entity::OFFSETS,
    entity::IDEMPOTENCY,
    entity::DB_DATA,
    entity::DB_SCHEMA,
    entity::DB_INDEX,
    entity::DB_UNIQUE,
    entity::DB_FK,
    entity::DB_CONSTRAINT,
    entity::CHANGE_EVENTS,
];

fn removal(write: &ReplicationWrite) -> ReplicationWrite {
    ReplicationWrite::new(
        write.partition,
        Operation::Delete,
        Epoch::ZERO,
        0,
        write.entity.clone(),
        write.id.clone(),
        Vec::new(),
    )
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ImportCounts {
    pub(crate) total: usize,
    pub(crate) partition_records: usize,
}

impl StoreManager {
    /// # Errors
    /// Returns `SnapshotTooLarge` if a store's export does not fit the snapshot length fields.
    pub fn export_partition(&self, partition: PartitionId) -> Result<Vec<u8>, StoreApplyError> {
        let mut buf = Vec::new();

        let store_data = SNAPSHOT_SECTIONS
            .map(|entity_name| (entity_name, self.export_section(entity_name, partition)));

        let too_large = |_| StoreApplyError::SnapshotTooLarge;
        buf.push(u8::try_from(store_data.len()).map_err(too_large)?);

        for (entity_name, data) in store_data {
            let name_bytes = entity_name.as_bytes();
            buf.push(u8::try_from(name_bytes.len()).map_err(too_large)?);
            buf.extend_from_slice(name_bytes);
            buf.extend_from_slice(&u32::try_from(data.len()).map_err(too_large)?.to_be_bytes());
            buf.extend_from_slice(&data);
        }

        Ok(buf)
    }

    /// # Errors
    /// Returns `StoreApplyError` if the snapshot data is malformed or a store import fails.
    pub fn import_partition(
        &self,
        partition: PartitionId,
        data: &[u8],
    ) -> Result<usize, StoreApplyError> {
        self.import_partition_counted(partition, data)
            .map(|counts| counts.total)
    }

    pub(crate) fn import_partition_counted(
        &self,
        partition: PartitionId,
        data: &[u8],
    ) -> Result<ImportCounts, StoreApplyError> {
        let mut counts = ImportCounts::default();
        if data.is_empty() {
            return Ok(counts);
        }

        let protection = self.transfer_window_protection(partition);
        let held = if protection.overflowed() {
            self.tracked_keys_held(partition)?
        } else {
            HashSet::new()
        };
        let kept = |write: &ReplicationWrite| {
            protection.keeps(write)
                || (!held.is_empty() && held.contains(&(write.entity.clone(), write.id.clone())))
        };

        let mut incoming = Vec::new();
        let mut incoming_claims = Vec::new();
        for section in SnapshotSections::new(data) {
            let (entity_name, store_data) = section?;
            incoming.extend(snapshot_writes(entity_name, store_data)?);
            if entity_name == entity::IDEMPOTENCY {
                incoming_claims.extend(processing_claims(store_data)?);
            }
        }
        incoming.retain(|write| !kept(write));
        incoming_claims.retain(|write| !kept(write));

        let mut replaced = self.partition_writes(partition)?;
        replaced.retain(|write| !kept(write));
        let mut local_claims =
            processing_claims(&self.idempotency.export_for_partition(partition))?;
        local_claims.retain(|write| !kept(write));

        let incoming_keys: HashSet<(&str, &str)> = incoming
            .iter()
            .map(|write| (write.entity.as_str(), write.id.as_str()))
            .collect();
        let stale = replaced
            .iter()
            .filter(|write| !incoming_keys.contains(&(write.entity.as_str(), write.id.as_str())))
            .map(removal);
        let batch: Vec<ReplicationWrite> = stale.chain(incoming.iter().cloned()).collect();
        self.persist_writes_batch(&batch)?;

        for write in replaced.iter().chain(&local_claims) {
            self.apply_to_memory(&removal(write))?;
        }
        for write in incoming.iter().chain(&incoming_claims) {
            if write.entity == entity::DB_UNIQUE {
                self.apply_to_memory(&removal(write))?;
            }
            self.apply_to_memory(write)?;
            counts.total += 1;
            if !GLOBAL_SECTIONS.contains(&write.entity.as_str()) {
                counts.partition_records += 1;
            }
        }

        for section in SnapshotSections::new(data) {
            let (entity_name, store_data) = section?;
            let imported = self.import_merged_section(entity_name, store_data)?;
            counts.total += imported;
            if entity_name != entity::CHANGE_EVENTS {
                counts.partition_records += imported;
            }
        }

        self.close_transfer_window(partition);
        self.rebuild_fk_indexes_after_import();

        Ok(counts)
    }

    fn tracked_keys_held(
        &self,
        partition: PartitionId,
    ) -> Result<HashSet<(String, String)>, StoreApplyError> {
        let mut held = HashSet::new();
        for entity_name in TRACKED_SECTIONS {
            let exported = self.export_section(entity_name, partition);
            held.extend(
                snapshot_writes(entity_name, &exported)?
                    .into_iter()
                    .map(|write| (write.entity, write.id)),
            );
        }
        Ok(held)
    }

    fn partition_writes(
        &self,
        partition: PartitionId,
    ) -> Result<Vec<ReplicationWrite>, StoreApplyError> {
        let mut writes = Vec::new();
        for entity_name in REPLACED_SECTIONS {
            let exported = self.export_section(entity_name, partition);
            writes.extend(snapshot_writes(entity_name, &exported)?);
        }
        Ok(writes)
    }

    fn export_section(&self, entity_name: &str, partition: PartitionId) -> Vec<u8> {
        match entity_name {
            entity::SESSIONS => self.sessions.export_for_partition(partition),
            entity::QOS2 => self.qos2.export_for_partition(partition),
            entity::SUBSCRIPTIONS => self.subscriptions.export_for_partition(partition),
            entity::RETAINED => self.retained.export_for_partition(partition),
            entity::INFLIGHT => self.inflight.export_for_partition(partition),
            entity::OFFSETS => self.offsets.export_for_partition(partition),
            entity::IDEMPOTENCY => self.idempotency.export_for_partition(partition),
            entity::DB_DATA => self.db_data.export_for_partition(partition),
            entity::DB_INDEX => self.db_index.export_for_partition(partition),
            entity::DB_UNIQUE => self.db_unique.export_for_partition(partition),
            entity::DB_FK => self.db_fk.export_for_partition(partition),
            entity::TOPIC_INDEX => self.topics.export_for_partition(partition),
            entity::WILDCARDS => self.wildcards.export_for_partition(partition),
            entity::DB_SCHEMA => self.db_schema.export_all(),
            entity::DB_CONSTRAINT => self.db_constraints.export_all(),
            entity::CHANGE_EVENTS => self.export_change_events(partition),
            _ => Vec::new(),
        }
    }

    fn import_merged_section(
        &self,
        entity_name: &str,
        store_data: &[u8],
    ) -> Result<usize, StoreApplyError> {
        match entity_name {
            entity::TOPIC_INDEX => self
                .topics
                .import_entries(store_data)
                .map_err(|_| StoreApplyError::TopicIndexError),
            entity::WILDCARDS => self
                .wildcards
                .import_wildcards(store_data)
                .map_err(|_| StoreApplyError::WildcardError),
            entity::CHANGE_EVENTS => self.import_change_events(store_data),
            _ => Ok(0),
        }
    }

    fn export_change_events(&self, partition: PartitionId) -> Vec<u8> {
        self.cluster_outbox
            .as_ref()
            .and_then(|outbox| outbox.stored_change_events_for(partition).ok())
            .and_then(|events| ClusterOutbox::encode_change_events(&events))
            .unwrap_or_default()
    }

    fn import_change_events(&self, data: &[u8]) -> Result<usize, StoreApplyError> {
        let Some(outbox) = self.cluster_outbox.as_ref() else {
            return Ok(0);
        };
        if data.is_empty() {
            return Ok(0);
        }
        let events =
            ClusterOutbox::decode_change_events(data).ok_or(StoreApplyError::ChangeEventError)?;
        outbox
            .store_change_events(&events)
            .map_err(|_| StoreApplyError::PersistenceError)?;
        Ok(events.len())
    }

    fn rebuild_fk_indexes_after_import(&self) {
        for constraint in self.db_constraints.list_all() {
            if constraint.is_foreign_key() {
                self.rebuild_fk_index_for_constraint(&constraint);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::NodeId;
    use crate::cluster::test_support::put_record;

    fn node(id: u16) -> NodeId {
        NodeId::validated(id).unwrap()
    }

    #[test]
    fn export_import_roundtrip_preserves_db_data() {
        let src = StoreManager::new(node(1));
        let dst = StoreManager::new(node(2));

        put_record(&src.db_data, "notes", "n1", b"{\"title\":\"hello\"}", 1000);
        put_record(&src.db_data, "notes", "n2", b"{\"title\":\"world\"}", 2000);

        let partition = src.db_data.get("notes", "n1").unwrap().partition();

        let payload = src.export_partition(partition).unwrap();
        assert!(!payload.is_empty(), "snapshot payload must not be empty");

        let imported = dst.import_partition(partition, &payload).expect("import");
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
    fn a_snapshot_without_change_events_imports_into_a_node_that_keeps_them() {
        let src = StoreManager::new(node(1));
        let dst = StoreManager::new_with_storage(
            node(2),
            Some(std::sync::Arc::new(mqdb_core::storage::MemoryBackend::new())),
        );
        put_record(&src.db_data, "notes", "n1", b"{}", 1000);
        let partition = src.db_data.get("notes", "n1").unwrap().partition();

        let payload = src.export_partition(partition).unwrap();

        dst.import_partition(partition, &payload).expect("import");
        assert!(dst.db_data.get("notes", "n1").is_some());
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

        let payload = src.export_partition(target).unwrap();
        assert!(!payload.is_empty());
        let total_imported = dst.import_partition(target, &payload).expect("import");
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

        put_record(&store.db_data, entity, "rec-1", b"{}", 1000);
        store
            .db_schema
            .register(entity, b"{\"fields\":[]}")
            .unwrap();
        store
            .db_constraints
            .add(ClusterConstraint::unique(entity, &format!("uniq_{entity}"), "email").unwrap())
            .unwrap();

        let value = format!("v-{entity}");
        let dp = data_partition(entity, "rec-1");
        store
            .db_index
            .add_entry(IndexEntry::create(entity, "email", value.as_bytes(), dp, "rec-1").unwrap())
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

        assert!(
            dst.db_schema.get(entity).is_some(),
            "db_schema for {entity} must arrive via any partition snapshot \
             — schemas are broadcast state and need to be uniform on every node"
        );

        let constraint_name = format!("uniq_{entity}");
        assert!(
            dst.db_constraints.get(entity, &constraint_name).is_some(),
            "db_constraints {entity}:{constraint_name} must arrive via any \
             partition snapshot — constraints are broadcast state and need to \
             be uniform on every node"
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
        )
        .unwrap();
        src.db_constraints.add(fk.clone()).unwrap();
        dst.db_constraints.add(fk).unwrap();

        put_record(&src.db_data, "posts", "p1", b"{\"title\":\"hello\"}", 1_000);
        put_record(
            &src.db_data,
            "comments",
            "c1",
            b"{\"post_id\":\"p1\",\"text\":\"first\"}",
            1_000,
        );
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

        let payload = src.export_partition(child_partition).unwrap();
        assert!(!payload.is_empty(), "snapshot payload must not be empty");
        let imported = dst
            .import_partition(child_partition, &payload)
            .expect("import");
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

    #[test]
    fn partition_snapshot_carries_full_schema_and_constraint_catalog() {
        use crate::cluster::db::{ClusterConstraint, OnDeleteAction};

        let src = StoreManager::new(node(1));
        let dst = StoreManager::new(node(2));

        let entities = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta"];
        for entity in &entities {
            src.db_schema.register(entity, b"{\"fields\":[]}").unwrap();
            src.db_constraints
                .add(ClusterConstraint::unique(entity, &format!("uniq_{entity}"), "email").unwrap())
                .unwrap();
            src.db_constraints
                .add(
                    ClusterConstraint::foreign_key(
                        entity,
                        &format!("fk_{entity}"),
                        "parent_id",
                        "alpha",
                        "id",
                        OnDeleteAction::Cascade,
                    )
                    .unwrap(),
                )
                .unwrap();
        }

        put_record(&src.db_data, "alpha", "rec-1", b"{}", 1_000);
        let any_partition = src.db_data.get("alpha", "rec-1").unwrap().partition();

        let schema_partitions: std::collections::HashSet<_> = src
            .db_schema
            .list()
            .iter()
            .map(crate::cluster::db::ClusterSchema::partition)
            .collect();
        let constraint_partitions: std::collections::HashSet<_> = src
            .db_constraints
            .list_all()
            .iter()
            .map(ClusterConstraint::partition)
            .collect();
        assert!(
            schema_partitions.len() > 1,
            "test prerequisite: schemas must span more than one partition"
        );
        assert!(
            constraint_partitions.len() > 1,
            "test prerequisite: constraints must span more than one partition"
        );

        let payload = src.export_partition(any_partition).unwrap();
        dst.import_partition(any_partition, &payload)
            .expect("import");

        for entity in &entities {
            assert!(
                dst.db_schema.get(entity).is_some(),
                "every schema must arrive via any partition snapshot, regardless of partition: {entity}"
            );
            let uniq_name = format!("uniq_{entity}");
            assert!(
                dst.db_constraints.get(entity, &uniq_name).is_some(),
                "every unique constraint must arrive via any partition snapshot: {entity}:{uniq_name}"
            );
            let fk_name = format!("fk_{entity}");
            assert!(
                dst.db_constraints.get(entity, &fk_name).is_some(),
                "every fk constraint must arrive via any partition snapshot: {entity}:{fk_name}"
            );
        }

        assert_eq!(
            dst.db_schema.list().len(),
            entities.len(),
            "destination schema catalog count must match source"
        );
        assert_eq!(
            dst.db_constraints.list_all().len(),
            entities.len() * 2,
            "destination constraint catalog count must match source"
        );
    }
}
