// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::StoreManager;
use crate::cluster::db::{ClusterConstraint, ConstraintStore, ConstraintType};
use crate::cluster::node_controller::fk::extract_fk_string_value;
use crate::cluster::protocol::{Operation, ReplicationWrite};
use crate::cluster::{Epoch, PartitionId, entity};

impl StoreManager {
    /// # Errors
    /// Returns `ConstraintStoreError::AlreadyExists` if the constraint is already registered.
    pub fn constraint_add_replicated(
        &self,
        constraint: &ClusterConstraint,
    ) -> Result<ReplicationWrite, super::super::db::ConstraintStoreError> {
        let key = super::super::db::constraint_key(constraint.entity_str(), constraint.name_str());
        if self
            .db_constraints
            .exists(constraint.entity_str(), constraint.name_str())
        {
            return Err(super::super::db::ConstraintStoreError::AlreadyExists);
        }
        let serialized = ConstraintStore::serialize(constraint);

        let write = ReplicationWrite::new(
            PartitionId::ZERO,
            Operation::Insert,
            Epoch::ZERO,
            0,
            entity::DB_CONSTRAINT.to_string(),
            key,
            serialized,
        );

        Ok(write)
    }

    /// # Errors
    /// Returns `ConstraintStoreError::NotFound` if the constraint does not exist.
    pub fn constraint_remove_replicated(
        &self,
        entity_type: &str,
        name: &str,
    ) -> Result<ReplicationWrite, super::super::db::ConstraintStoreError> {
        let key = super::super::db::constraint_key(entity_type, name);
        if !self.db_constraints.exists(entity_type, name) {
            return Err(super::super::db::ConstraintStoreError::NotFound);
        }

        let write = ReplicationWrite::new(
            PartitionId::ZERO,
            Operation::Delete,
            Epoch::ZERO,
            0,
            entity::DB_CONSTRAINT.to_string(),
            key,
            Vec::new(),
        );

        Ok(write)
    }

    #[must_use]
    pub fn constraint_list(&self, entity_type: &str) -> Vec<ClusterConstraint> {
        self.db_constraints.list(entity_type)
    }

    #[must_use]
    pub fn constraint_list_all(&self) -> Vec<ClusterConstraint> {
        self.db_constraints.list_all()
    }

    #[must_use]
    pub fn constraint_get_unique_fields(&self, entity_type: &str) -> Vec<String> {
        self.db_constraints.get_unique_fields(entity_type)
    }

    #[must_use]
    pub fn constraint_get_fk_constraints(
        &self,
        entity_type: &str,
    ) -> Vec<super::super::db::ClusterConstraint> {
        self.db_constraints.get_fk_constraints(entity_type)
    }

    #[must_use]
    pub fn constraint_find_referencing(
        &self,
        target_entity: &str,
    ) -> Vec<super::super::db::ClusterConstraint> {
        self.db_constraints
            .find_referencing_constraints(target_entity)
    }

    #[must_use]
    pub fn fk_reverse_lookup(
        &self,
        target_entity: &str,
        target_id: &str,
        source_entity: &str,
        source_field: &str,
    ) -> Vec<String> {
        self.fk_reverse_index
            .lookup(target_entity, target_id, source_entity, source_field)
    }

    pub fn rebuild_fk_index_for_constraint(&self, constraint: &ClusterConstraint) {
        if constraint.constraint_type() != ConstraintType::ForeignKey {
            return;
        }
        let source_entity = constraint.entity_str();
        let source_field = constraint.field_str();
        let target_entity = constraint.target_entity_str();

        for record in self.db_data.list(source_entity) {
            if let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(&record.data)
                && let Some(fk_val) = parsed.get(source_field).and_then(extract_fk_string_value)
            {
                self.fk_reverse_index.insert(
                    target_entity,
                    &fk_val,
                    source_entity,
                    source_field,
                    record.id_str(),
                );
            }
        }
    }

    pub fn update_fk_reverse_index(
        &self,
        op: Operation,
        source_entity: &str,
        source_id: &str,
        new_data: Option<&[u8]>,
        old_data: Option<&[u8]>,
    ) {
        let fk_constraints = self.db_constraints.get_fk_constraints(source_entity);

        if fk_constraints.is_empty() {
            return;
        }

        if matches!(op, Operation::Delete | Operation::Update)
            && let Some(raw) = old_data
            && let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(raw)
        {
            for c in &fk_constraints {
                if let Some(fk_val) = parsed.get(c.field_str()).and_then(extract_fk_string_value) {
                    self.fk_reverse_index.remove(
                        c.target_entity_str(),
                        &fk_val,
                        source_entity,
                        c.field_str(),
                        source_id,
                    );
                }
            }
        }

        if matches!(op, Operation::Insert | Operation::Update)
            && let Some(raw) = new_data
            && let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(raw)
        {
            for c in &fk_constraints {
                if let Some(fk_val) = parsed.get(c.field_str()).and_then(extract_fk_string_value) {
                    self.fk_reverse_index.insert(
                        c.target_entity_str(),
                        &fk_val,
                        source_entity,
                        c.field_str(),
                        source_id,
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::NodeId;
    use crate::cluster::db::{ClusterConstraint, OnDeleteAction};

    fn node(id: u16) -> NodeId {
        NodeId::validated(id).unwrap()
    }

    fn fk_comments_to_posts() -> ClusterConstraint {
        ClusterConstraint::foreign_key(
            "comments",
            "fk_comment_post",
            "post_id",
            "posts",
            "id",
            OnDeleteAction::Cascade,
        )
    }

    #[test]
    fn update_fk_reverse_index_inserts_on_create() {
        let store = StoreManager::new(node(1));
        store.db_constraints.add(fk_comments_to_posts()).unwrap();

        store.update_fk_reverse_index(
            Operation::Insert,
            "comments",
            "c1",
            Some(b"{\"post_id\":\"p1\"}"),
            None,
        );

        assert_eq!(
            store.fk_reverse_lookup("posts", "p1", "comments", "post_id"),
            vec!["c1".to_string()],
        );
    }

    #[test]
    fn update_fk_reverse_index_is_noop_without_fk_constraint() {
        let store = StoreManager::new(node(1));

        store.update_fk_reverse_index(
            Operation::Insert,
            "comments",
            "c1",
            Some(b"{\"post_id\":\"p1\"}"),
            None,
        );

        assert!(
            store
                .fk_reverse_lookup("posts", "p1", "comments", "post_id")
                .is_empty(),
            "no constraint registered → no reverse index entry",
        );
    }

    #[test]
    fn update_fk_reverse_index_handles_update_repoint() {
        let store = StoreManager::new(node(1));
        store.db_constraints.add(fk_comments_to_posts()).unwrap();

        store.update_fk_reverse_index(
            Operation::Insert,
            "comments",
            "c1",
            Some(b"{\"post_id\":\"p1\"}"),
            None,
        );
        store.update_fk_reverse_index(
            Operation::Update,
            "comments",
            "c1",
            Some(b"{\"post_id\":\"p2\"}"),
            Some(b"{\"post_id\":\"p1\"}"),
        );

        assert!(
            store
                .fk_reverse_lookup("posts", "p1", "comments", "post_id")
                .is_empty(),
            "update must remove the old reference",
        );
        assert_eq!(
            store.fk_reverse_lookup("posts", "p2", "comments", "post_id"),
            vec!["c1".to_string()],
            "update must add the new reference",
        );
    }

    #[test]
    fn update_fk_reverse_index_removes_on_delete() {
        let store = StoreManager::new(node(1));
        store.db_constraints.add(fk_comments_to_posts()).unwrap();

        store.update_fk_reverse_index(
            Operation::Insert,
            "comments",
            "c1",
            Some(b"{\"post_id\":\"p1\"}"),
            None,
        );
        store.update_fk_reverse_index(
            Operation::Delete,
            "comments",
            "c1",
            None,
            Some(b"{\"post_id\":\"p1\"}"),
        );

        assert!(
            store
                .fk_reverse_lookup("posts", "p1", "comments", "post_id")
                .is_empty(),
            "delete must drop the reverse index entry",
        );
    }

    #[test]
    fn update_fk_reverse_index_skips_malformed_payloads() {
        let store = StoreManager::new(node(1));
        store.db_constraints.add(fk_comments_to_posts()).unwrap();

        store.update_fk_reverse_index(Operation::Insert, "comments", "c1", Some(b"not json"), None);

        assert!(
            store
                .fk_reverse_lookup("posts", "p1", "comments", "post_id")
                .is_empty(),
            "malformed JSON must be silently skipped, not panic",
        );
    }

    #[test]
    fn rebuild_fk_index_for_constraint_walks_existing_records() {
        let store = StoreManager::new(node(1));
        let fk = fk_comments_to_posts();

        store
            .db_data
            .create("comments", "c1", b"{\"post_id\":\"p1\"}", 1_000)
            .unwrap();
        store
            .db_data
            .create("comments", "c2", b"{\"post_id\":\"p1\"}", 1_000)
            .unwrap();
        store
            .db_data
            .create("comments", "c3", b"{\"post_id\":\"p2\"}", 1_000)
            .unwrap();

        store.rebuild_fk_index_for_constraint(&fk);

        let mut p1_refs = store.fk_reverse_lookup("posts", "p1", "comments", "post_id");
        p1_refs.sort();
        assert_eq!(p1_refs, vec!["c1".to_string(), "c2".to_string()]);

        assert_eq!(
            store.fk_reverse_lookup("posts", "p2", "comments", "post_id"),
            vec!["c3".to_string()],
        );
    }

    #[test]
    fn rebuild_fk_index_for_constraint_is_noop_for_unique_constraint() {
        let store = StoreManager::new(node(1));
        let unique = ClusterConstraint::unique("comments", "uniq_text", "text");

        store
            .db_data
            .create("comments", "c1", b"{\"post_id\":\"p1\"}", 1_000)
            .unwrap();

        store.rebuild_fk_index_for_constraint(&unique);

        assert!(
            store
                .fk_reverse_lookup("posts", "p1", "comments", "post_id")
                .is_empty(),
            "non-FK constraint must not populate the reverse index",
        );
    }
}
