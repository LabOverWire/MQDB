// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::Database;
use crate::error::Result;
use crate::relationship::Relationship;
use crate::schema::Schema;

impl Database {
    /// # Errors
    /// Returns an error if persisting the schema fails.
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

    pub async fn add_index(&self, entity: String, fields: Vec<String>) {
        let mut manager = self.index_manager.write().await;
        manager.add_index(crate::index::IndexDefinition::new(entity, fields));
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

    pub async fn list_constraints(&self, entity: &str) -> Vec<crate::constraint::Constraint> {
        let manager = self.constraint_manager.read().await;
        manager.get_constraints(entity).to_vec()
    }

    /// # Errors
    /// Returns an error if persisting the constraint fails.
    pub async fn add_unique_constraint(&self, entity: String, fields: Vec<String>) -> Result<()> {
        use crate::constraint::{Constraint, UniqueConstraint};

        self.add_index(entity.clone(), fields.clone()).await;

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
    /// Returns an error if persisting the constraint fails.
    pub async fn add_not_null(&self, entity: String, field: String) -> Result<()> {
        use crate::constraint::{Constraint, NotNullConstraint};

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
    /// Returns an error if persisting the constraint fails.
    pub async fn add_foreign_key(
        &self,
        source_entity: String,
        source_field: String,
        target_entity: String,
        target_field: String,
        on_delete: crate::constraint::OnDeleteAction,
    ) -> Result<()> {
        use crate::constraint::{Constraint, ForeignKeyConstraint};

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
