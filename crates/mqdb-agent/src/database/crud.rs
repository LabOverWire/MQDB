// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::Database;
use mqdb_core::entity::Entity;
use mqdb_core::error::{Error, Result};
use mqdb_core::events::ChangeEvent;
use mqdb_core::keys;
use mqdb_core::types::ScopeConfig;
use serde_json::Value;

use mqdb_core::types::OwnershipConfig;

pub struct CallerContext<'a> {
    pub sender: Option<&'a str>,
    pub client_id: Option<&'a str>,
    pub scope_config: &'a ScopeConfig,
}

impl Database {
    /// # Errors
    /// Returns an error if validation, constraint checks, or storage fails.
    pub async fn create(
        &self,
        entity_name: String,
        mut data: Value,
        constraint_data: Option<Value>,
        sender: Option<&str>,
        client_id: Option<&str>,
        scope_config: &ScopeConfig,
    ) -> Result<Value> {
        let schema_registry = self.schema_registry.read().await;
        schema_registry.apply_defaults(&entity_name, &mut data)?;
        drop(schema_registry);

        let id = if let Some(client_id) = data.get("id").and_then(Value::as_str) {
            client_id.to_string()
        } else {
            let payload_bytes = serde_json::to_vec(&data).unwrap_or_default();
            let generated = Self::generate_id(&entity_name, &payload_bytes);
            if let Value::Object(ref mut obj) = data {
                obj.insert("id".to_string(), Value::String(generated.clone()));
            }
            generated
        };

        if let Value::Object(ref mut obj) = data
            && let Some(ttl_secs) = obj.get("ttl_secs").and_then(Value::as_u64)
        {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| Error::SystemTime(format!("failed to get system time: {e}")))?
                .as_secs();
            let expires_at = now + ttl_secs;
            obj.insert("_expires_at".to_string(), Value::Number(expires_at.into()));
            obj.remove("ttl_secs");
        }

        if let Value::Object(ref mut obj) = data {
            obj.remove("_version");
            obj.insert("_version".to_string(), Value::Number(1.into()));
        }

        let schema_registry = self.schema_registry.read().await;
        schema_registry.validate_entity(&entity_name, &data)?;
        drop(schema_registry);

        let entity = Entity::new(entity_name.clone(), id.clone(), data.clone());

        let constraint_entity = if let Some(mut cd) = constraint_data {
            let schema_registry = self.schema_registry.read().await;
            let _ = schema_registry.apply_defaults(&entity_name, &mut cd);
            drop(schema_registry);
            if let Value::Object(ref mut obj) = cd {
                if !obj.contains_key("id") {
                    obj.insert("id".to_string(), Value::String(id.clone()));
                }
                obj.remove("_version");
                obj.insert("_version".to_string(), Value::Number(1.into()));
            }
            Entity::new(entity_name.clone(), id, cd)
        } else {
            entity.clone()
        };

        let mut batch = self.storage.batch();

        let constraint_manager = self.constraint_manager.read().await;
        constraint_manager.validate_create(&constraint_entity, &mut batch, &self.storage)?;
        drop(constraint_manager);

        batch.insert(entity.key(), entity.serialize()?);

        let index_manager = self.index_manager.read().await;
        index_manager.update_indexes(&mut batch, &constraint_entity, None);

        self.register_entity_name(&entity_name).await;

        let scope = scope_config.resolve_scope(&entity_name, &data);
        let event = ChangeEvent::create(entity_name, entity.id.clone(), data.clone())
            .with_sender(sender.map(String::from))
            .with_client_id(client_id.map(String::from))
            .with_scope(scope);
        let operation_id = uuid::Uuid::new_v4().to_string();
        self.outbox.enqueue_event(&mut batch, &operation_id, &event);

        batch.commit()?;

        self.dispatcher.dispatch(event).await?;
        self.outbox.mark_delivered(&operation_id)?;

        Ok(entity.to_json())
    }

    /// # Errors
    /// Returns an error if the entity is not found or deserialization fails.
    pub async fn read(
        &self,
        entity_name: String,
        id: String,
        includes: Vec<String>,
        projection: Option<Vec<String>>,
    ) -> Result<Value> {
        let key = keys::encode_data_key(&entity_name, &id);

        let data = self.storage.get(&key)?.ok_or_else(|| Error::NotFound {
            entity: entity_name.clone(),
            id: id.clone(),
        })?;

        let entity = Entity::deserialize(entity_name.clone(), id, &data)?;
        let mut result = entity.to_json();

        if !includes.is_empty() {
            self.load_includes(&mut result, &entity_name, &includes, 0)
                .await?;
        }

        let result = if let Some(ref fields) = projection {
            let schema_registry = self.schema_registry.read().await;
            let field_refs: Vec<&str> = fields.iter().map(String::as_str).collect();
            schema_registry.validate_fields_exist(&entity_name, &field_refs, "projection")?;
            drop(schema_registry);
            mqdb_core::types::project_fields(result, fields)
        } else {
            result
        };

        Ok(result)
    }

    /// # Errors
    /// Returns an error if the entity is not found, validation fails, or storage fails.
    pub async fn update(
        &self,
        entity_name: String,
        id: String,
        fields: Value,
        update_constraint_data: Option<(Value, Value)>,
        caller: &CallerContext<'_>,
    ) -> Result<Value> {
        let key = keys::encode_data_key(&entity_name, &id);

        let existing_data = self.storage.get(&key)?.ok_or_else(|| Error::NotFound {
            entity: entity_name.clone(),
            id: id.clone(),
        })?;

        let existing_entity = Entity::deserialize(entity_name.clone(), id.clone(), &existing_data)?;
        let mut updated_data = existing_entity.data.clone();

        if let (Value::Object(existing), Value::Object(updates)) = (&mut updated_data, fields) {
            for (key, value) in updates {
                existing.insert(key, value);
            }
        }

        let existing_version = existing_entity
            .data
            .get("_version")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        if let Value::Object(ref mut obj) = updated_data {
            obj.insert(
                "_version".to_string(),
                Value::Number((existing_version + 1).into()),
            );
        }

        let schema_registry = self.schema_registry.read().await;
        schema_registry.validate_entity(&entity_name, &updated_data)?;
        drop(schema_registry);

        let updated_entity = Entity::new(entity_name.clone(), id.clone(), updated_data);

        let (constraint_new, constraint_old) =
            if let Some((mut plaintext_merged, plaintext_existing)) = update_constraint_data {
                if let Value::Object(ref mut obj) = plaintext_merged {
                    obj.remove("_version");
                    obj.insert(
                        "_version".to_string(),
                        Value::Number((existing_version + 1).into()),
                    );
                }
                (
                    Entity::new(entity_name.clone(), id.clone(), plaintext_merged),
                    Entity::new(entity_name.clone(), id.clone(), plaintext_existing),
                )
            } else {
                (updated_entity.clone(), existing_entity.clone())
            };

        let mut batch = self.storage.batch();

        let constraint_manager = self.constraint_manager.read().await;
        constraint_manager.validate_update(
            &constraint_new,
            &constraint_old,
            &mut batch,
            &self.storage,
        )?;
        drop(constraint_manager);

        batch.expect_value(key, existing_data);
        batch.insert(updated_entity.key(), updated_entity.serialize()?);

        let index_manager = self.index_manager.read().await;
        index_manager.update_indexes(&mut batch, &constraint_new, Some(&constraint_old));

        let scope = caller
            .scope_config
            .resolve_scope(&entity_name, &updated_entity.data);
        let event = ChangeEvent::update(
            entity_name,
            updated_entity.id.clone(),
            updated_entity.data.clone(),
        )
        .with_sender(caller.sender.map(String::from))
        .with_client_id(caller.client_id.map(String::from))
        .with_scope(scope);
        let operation_id = uuid::Uuid::new_v4().to_string();
        self.outbox.enqueue_event(&mut batch, &operation_id, &event);

        batch.commit()?;

        self.dispatcher.dispatch(event).await?;
        self.outbox.mark_delivered(&operation_id)?;

        Ok(updated_entity.to_json())
    }

    /// # Errors
    /// Returns an error if the entity is not found or constraint validation fails.
    #[allow(clippy::too_many_lines)]
    pub async fn delete(
        &self,
        entity_name: String,
        id: String,
        sender: Option<&str>,
        client_id: Option<&str>,
        scope_config: &ScopeConfig,
        ownership: &OwnershipConfig,
    ) -> Result<()> {
        use mqdb_core::constraint::{DeleteOperation, OwnershipContext};

        let key = keys::encode_data_key(&entity_name, &id);

        let existing_data = self.storage.get(&key)?.ok_or_else(|| Error::NotFound {
            entity: entity_name.clone(),
            id: id.clone(),
        })?;

        let existing_entity = Entity::deserialize(entity_name.clone(), id.clone(), &existing_data)?;

        let ownership_ctx = sender
            .filter(|_| !ownership.is_empty())
            .map(|s| OwnershipContext {
                sender: s,
                ownership,
            });

        let constraint_manager = self.constraint_manager.read().await;
        let delete_ops = constraint_manager.validate_delete(
            &existing_entity,
            &self.storage,
            ownership_ctx.as_ref(),
        )?;
        drop(constraint_manager);

        let mut batch = self.storage.batch();

        batch.remove(key.clone());

        let index_manager = self.index_manager.read().await;
        index_manager.remove_indexes(&mut batch, &existing_entity);
        drop(index_manager);

        let mut deleted_entities: Vec<(String, String, Value)> = Vec::new();
        let mut set_null_entities: Vec<(String, String, Value)> = Vec::new();

        for operation in &delete_ops {
            match operation {
                DeleteOperation::Cascade(cascade_op) => {
                    let cascade_key = keys::encode_data_key(&cascade_op.entity, &cascade_op.id);
                    if let Some(cascade_data) = self.storage.get(&cascade_key)? {
                        let cascade_entity = Entity::deserialize(
                            cascade_op.entity.clone(),
                            cascade_op.id.clone(),
                            &cascade_data,
                        )?;

                        batch.remove(cascade_key);

                        let index_manager = self.index_manager.read().await;
                        index_manager.remove_indexes(&mut batch, &cascade_entity);
                        drop(index_manager);

                        deleted_entities.push((
                            cascade_op.entity.clone(),
                            cascade_op.id.clone(),
                            cascade_entity.data,
                        ));
                    }
                }
                DeleteOperation::SetNull(set_null_op) => {
                    let entity_key = keys::encode_data_key(&set_null_op.entity, &set_null_op.id);
                    if let Some(entity_data) = self.storage.get(&entity_key)? {
                        let mut entity = Entity::deserialize(
                            set_null_op.entity.clone(),
                            set_null_op.id.clone(),
                            &entity_data,
                        )?;

                        let old_entity = entity.clone();

                        if let Some(obj) = entity.data.as_object_mut() {
                            obj.insert(set_null_op.field.clone(), serde_json::Value::Null);
                            let v = obj
                                .get("_version")
                                .and_then(serde_json::Value::as_u64)
                                .unwrap_or(0);
                            obj.insert(
                                "_version".to_string(),
                                serde_json::Value::Number((v + 1).into()),
                            );
                        }

                        batch.insert(entity_key, entity.serialize()?);

                        let index_manager = self.index_manager.read().await;
                        index_manager.update_indexes(&mut batch, &entity, Some(&old_entity));
                        drop(index_manager);

                        set_null_entities.push((
                            set_null_op.entity.clone(),
                            set_null_op.id.clone(),
                            entity.data,
                        ));
                    }
                }
            }
        }

        let primary_scope = scope_config.resolve_scope(&entity_name, &existing_entity.data);
        let mut events = vec![
            ChangeEvent::delete(entity_name, id, existing_entity.data)
                .with_sender(sender.map(String::from))
                .with_client_id(client_id.map(String::from))
                .with_scope(primary_scope),
        ];
        for (cascade_entity, cascade_id, cascade_data) in deleted_entities {
            let cascade_scope = scope_config.resolve_scope(&cascade_entity, &cascade_data);
            events.push(
                ChangeEvent::delete(cascade_entity, cascade_id, cascade_data)
                    .with_sender(sender.map(String::from))
                    .with_client_id(client_id.map(String::from))
                    .with_scope(cascade_scope),
            );
        }
        for (sn_entity, sn_id, sn_data) in set_null_entities {
            let sn_scope = scope_config.resolve_scope(&sn_entity, &sn_data);
            events.push(
                ChangeEvent::update(sn_entity, sn_id, sn_data)
                    .with_sender(sender.map(String::from))
                    .with_client_id(client_id.map(String::from))
                    .with_scope(sn_scope),
            );
        }

        let operation_id = uuid::Uuid::new_v4().to_string();
        self.outbox
            .enqueue_events(&mut batch, &operation_id, &events);

        batch.commit()?;

        for event in events {
            self.dispatcher.dispatch(event).await?;
        }
        self.outbox.mark_delivered(&operation_id)?;

        Ok(())
    }

    /// # Errors
    /// Returns `Forbidden` if the sender doesn't own the entity, or `NotFound` if it doesn't exist.
    pub fn check_ownership(
        &self,
        entity_name: &str,
        id: &str,
        owner_field: &str,
        sender: &str,
    ) -> Result<()> {
        let key = keys::encode_data_key(entity_name, id);
        let existing_data = self.storage.get(&key)?.ok_or_else(|| Error::NotFound {
            entity: entity_name.to_string(),
            id: id.to_string(),
        })?;
        let entity = Entity::deserialize(entity_name.to_string(), id.to_string(), &existing_data)?;
        let owner_value = entity.data.get(owner_field).and_then(|v| v.as_str());
        if owner_value != Some(sender) {
            return Err(Error::Forbidden("permission denied".to_string()));
        }
        Ok(())
    }

    pub(super) fn generate_id(entity_name: &str, data: &[u8]) -> String {
        use std::sync::atomic::{AtomicU16, Ordering};
        static COUNTER: AtomicU16 = AtomicU16::new(0);

        let idx = COUNTER.fetch_add(1, Ordering::Relaxed) % mqdb_core::partition::NUM_PARTITIONS;
        let partition = mqdb_core::partition::PartitionId::new(idx)
            .unwrap_or(mqdb_core::partition::PartitionId::ZERO);
        mqdb_core::partition::generate_id_for_partition(1, entity_name, partition, data)
    }
}
