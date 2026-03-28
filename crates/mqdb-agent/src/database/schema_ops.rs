// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::Database;
use mqdb_core::entity::Entity;
use mqdb_core::error::Result;
use mqdb_core::keys;
use mqdb_core::relationship::Relationship;
use mqdb_core::schema::Schema;

impl Database {
    /// # Errors
    /// Returns an error if persisting the schema fails.
    #[tracing::instrument(skip(self, schema), fields(entity = %schema.entity))]
    pub async fn add_schema(&self, schema: Schema) -> Result<()> {
        let mut batch = self.storage.batch();

        let schema_registry = self.schema_registry.read().await;
        schema_registry.persist_schema(&mut batch, &schema)?;
        drop(schema_registry);

        batch.commit()?;

        let mut schema_registry = self.schema_registry.write().await;
        schema_registry.add_schema(schema);

        Ok(())
    }

    pub async fn get_schema(&self, entity: &str) -> Option<Schema> {
        let registry = self.schema_registry.read().await;
        registry.get_schema(entity).cloned()
    }

    /// # Errors
    /// Returns an error if any index field doesn't exist in the entity's schema.
    pub async fn add_index(&self, entity: String, fields: Vec<String>) -> Result<()> {
        let schema_registry = self.schema_registry.read().await;
        let field_refs: Vec<&str> = fields.iter().map(String::as_str).collect();
        schema_registry.validate_fields_exist(&entity, &field_refs, "index")?;
        drop(schema_registry);

        {
            let mut manager = self.index_manager.write().await;
            manager.add_index(mqdb_core::index::IndexDefinition::new(
                entity.clone(),
                fields,
            ));
        }

        let prefix = format!("data/{entity}/");
        let mut after_key: Option<Vec<u8>> = None;
        loop {
            let batch_items =
                self.storage
                    .prefix_scan_batch(prefix.as_bytes(), 1000, after_key.as_deref())?;
            if batch_items.is_empty() {
                break;
            }
            let mut write_batch = self.storage.batch();
            let manager = self.index_manager.write().await;
            for (key, value) in &batch_items {
                if let Ok((_, id)) = keys::decode_data_key(key)
                    && let Ok(entity_obj) = Entity::deserialize(entity.clone(), id, value)
                {
                    manager.update_indexes(&mut write_batch, &entity_obj, None);
                }
            }
            drop(manager);
            write_batch.commit()?;
            after_key = batch_items.last().map(|(k, _)| k.clone());
        }

        Ok(())
    }

    pub async fn add_relationship(
        &self,
        source_entity: String,
        field: String,
        target_entity: String,
    ) {
        let mut registry = self.relationship_registry.write().await;
        let relationship = Relationship::new(source_entity, field, target_entity);
        registry.add(relationship);
    }

    pub async fn list_relationships(&self, entity: &str) -> Vec<Relationship> {
        let registry = self.relationship_registry.read().await;
        registry.get_all(entity).cloned().unwrap_or_default()
    }

    pub async fn list_constraints(&self, entity: &str) -> Vec<mqdb_core::constraint::Constraint> {
        let manager = self.constraint_manager.read().await;
        manager.get_constraints(entity).to_vec()
    }

    /// # Errors
    /// Returns an error if field validation or persisting the constraint fails.
    #[tracing::instrument(skip(self), fields(entity = %entity))]
    pub async fn add_unique_constraint(&self, entity: String, fields: Vec<String>) -> Result<()> {
        use mqdb_core::constraint::{Constraint, UniqueConstraint};

        self.add_index(entity.clone(), fields.clone()).await?;

        let constraint = Constraint::Unique(UniqueConstraint::new(entity, fields));

        let mut batch = self.storage.batch();

        let constraint_manager = self.constraint_manager.read().await;
        constraint_manager.persist_constraint(&mut batch, &constraint)?;
        drop(constraint_manager);

        batch.commit()?;

        let mut constraint_manager = self.constraint_manager.write().await;
        constraint_manager.add_constraint(constraint);

        Ok(())
    }

    /// # Errors
    /// Returns an error if field validation or persisting the constraint fails.
    #[tracing::instrument(skip(self), fields(entity = %entity, field = %field))]
    pub async fn add_not_null(&self, entity: String, field: String) -> Result<()> {
        use mqdb_core::constraint::{Constraint, NotNullConstraint};

        let schema_registry = self.schema_registry.read().await;
        schema_registry.validate_fields_exist(&entity, &[field.as_str()], "not-null constraint")?;
        drop(schema_registry);

        let constraint = Constraint::NotNull(NotNullConstraint::new(entity, field));

        let mut batch = self.storage.batch();

        let constraint_manager = self.constraint_manager.read().await;
        constraint_manager.persist_constraint(&mut batch, &constraint)?;
        drop(constraint_manager);

        batch.commit()?;

        let mut constraint_manager = self.constraint_manager.write().await;
        constraint_manager.add_constraint(constraint);

        Ok(())
    }

    /// # Errors
    /// Returns an error if field validation or persisting the constraint fails.
    #[tracing::instrument(skip(self), fields(source = %source_entity, target = %target_entity))]
    pub async fn add_foreign_key(
        &self,
        source_entity: String,
        source_field: String,
        target_entity: String,
        target_field: String,
        on_delete: mqdb_core::constraint::OnDeleteAction,
    ) -> Result<()> {
        use mqdb_core::constraint::{Constraint, ForeignKeyConstraint};

        let schema_registry = self.schema_registry.read().await;
        schema_registry.validate_fields_exist(
            &source_entity,
            &[source_field.as_str()],
            "foreign key",
        )?;
        schema_registry.validate_fields_exist(
            &target_entity,
            &[target_field.as_str()],
            "foreign key",
        )?;
        drop(schema_registry);

        let constraint = Constraint::ForeignKey(ForeignKeyConstraint::new(
            source_entity,
            source_field,
            target_entity,
            target_field,
            on_delete,
        ));

        let mut batch = self.storage.batch();

        let constraint_manager = self.constraint_manager.read().await;
        constraint_manager.persist_constraint(&mut batch, &constraint)?;
        drop(constraint_manager);

        batch.commit()?;

        let mut constraint_manager = self.constraint_manager.write().await;
        constraint_manager.add_constraint(constraint);

        Ok(())
    }
}
