use super::Database;
use crate::entity::Entity;
use crate::error::{Error, Result};
use crate::events::ChangeEvent;
use crate::keys;
use crate::types::ScopeConfig;
use serde_json::Value;

impl Database {
    /// # Errors
    /// Returns an error if validation, constraint checks, or storage fails.
    pub async fn create(
        &self,
        entity_name: String,
        mut data: Value,
        sender: Option<&str>,
        scope_config: &ScopeConfig,
    ) -> Result<Value> {
        let schema_registry = self.schema_registry.read().await;
        schema_registry.apply_defaults(&entity_name, &mut data)?;
        drop(schema_registry);

        let id = if let Some(client_id) = data.get("id").and_then(Value::as_str) {
            client_id.to_string()
        } else {
            let generated = self.generate_id(&entity_name).await?;
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

        let schema_registry = self.schema_registry.read().await;
        schema_registry.validate_entity(&entity_name, &data)?;
        drop(schema_registry);

        let entity = Entity::new(entity_name.clone(), id, data.clone());

        let mut batch = self.storage.batch();

        let constraint_manager = self.constraint_manager.read().await;
        constraint_manager.validate_create(&entity, &mut batch, &self.storage)?;
        drop(constraint_manager);

        batch.insert(entity.key(), entity.serialize()?);

        let index_manager = self.index_manager.read().await;
        index_manager.update_indexes(&mut batch, &entity, None);

        let scope = scope_config.resolve_scope(&entity_name, &data);
        let event = ChangeEvent::create(entity_name, entity.id.clone(), data.clone())
            .with_sender(sender.map(String::from))
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
            Self::project_fields(result, fields)
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
        sender: Option<&str>,
        scope_config: &ScopeConfig,
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

        let schema_registry = self.schema_registry.read().await;
        schema_registry.validate_entity(&entity_name, &updated_data)?;
        drop(schema_registry);

        let updated_entity = Entity::new(entity_name.clone(), id.clone(), updated_data);

        let mut batch = self.storage.batch();

        let constraint_manager = self.constraint_manager.read().await;
        constraint_manager.validate_update(
            &updated_entity,
            &existing_entity,
            &mut batch,
            &self.storage,
        )?;
        drop(constraint_manager);

        batch.expect_value(key, existing_data);
        batch.insert(updated_entity.key(), updated_entity.serialize()?);

        let index_manager = self.index_manager.read().await;
        index_manager.update_indexes(&mut batch, &updated_entity, Some(&existing_entity));

        let scope = scope_config.resolve_scope(&entity_name, &updated_entity.data);
        let event = ChangeEvent::update(
            entity_name,
            updated_entity.id.clone(),
            updated_entity.data.clone(),
        )
        .with_sender(sender.map(String::from))
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
    pub async fn delete(
        &self,
        entity_name: String,
        id: String,
        sender: Option<&str>,
        scope_config: &ScopeConfig,
    ) -> Result<()> {
        use crate::constraint::DeleteOperation;

        let key = keys::encode_data_key(&entity_name, &id);

        let existing_data = self.storage.get(&key)?.ok_or_else(|| Error::NotFound {
            entity: entity_name.clone(),
            id: id.clone(),
        })?;

        let existing_entity = Entity::deserialize(entity_name.clone(), id.clone(), &existing_data)?;

        let constraint_manager = self.constraint_manager.read().await;
        let delete_ops = constraint_manager.validate_delete(&existing_entity, &self.storage)?;
        drop(constraint_manager);

        let mut batch = self.storage.batch();

        batch.remove(key.clone());

        let index_manager = self.index_manager.read().await;
        index_manager.remove_indexes(&mut batch, &existing_entity);
        drop(index_manager);

        let mut deleted_entities: Vec<(String, String, Value)> = Vec::new();

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
                        }

                        batch.insert(entity_key, entity.serialize()?);

                        let index_manager = self.index_manager.read().await;
                        index_manager.update_indexes(&mut batch, &entity, Some(&old_entity));
                        drop(index_manager);
                    }
                }
            }
        }

        let primary_scope = scope_config.resolve_scope(&entity_name, &existing_entity.data);
        let mut events = vec![
            ChangeEvent::delete(entity_name, id)
                .with_sender(sender.map(String::from))
                .with_scope(primary_scope),
        ];
        for (cascade_entity, cascade_id, cascade_data) in &deleted_entities {
            let cascade_scope = scope_config.resolve_scope(cascade_entity, cascade_data);
            events.push(
                ChangeEvent::delete(cascade_entity.clone(), cascade_id.clone())
                    .with_sender(sender.map(String::from))
                    .with_scope(cascade_scope),
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
        if let Some(owner_value) = entity.data.get(owner_field)
            && owner_value.as_str() != Some(sender)
        {
            return Err(Error::Forbidden(format!(
                "user '{sender}' does not own {entity_name}/{id}"
            )));
        }
        Ok(())
    }

    pub(super) async fn generate_id(&self, entity_name: &str) -> Result<String> {
        let _lock = self.id_gen_lock.lock().await;

        let counter_key = keys::encode_meta_key(&format!("seq:{entity_name}"));

        let current = self
            .storage
            .get(&counter_key)?
            .and_then(|v| String::from_utf8(v).ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        let next = current + 1;
        self.storage
            .insert(&counter_key, next.to_string().as_bytes())?;

        Ok(next.to_string())
    }
}
