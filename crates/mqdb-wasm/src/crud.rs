// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{
    ChangeEvent, JsValue, Relationship, WasmDatabase, deserialize_js, serialize_js, wasm_bindgen,
};

#[wasm_bindgen]
impl WasmDatabase {
    /// Creates a new record in the specified entity.
    ///
    /// # Errors
    /// Returns an error if validation fails or the storage operation fails.
    pub async fn create(&self, entity: String, data: JsValue) -> Result<JsValue, JsValue> {
        let mut value: serde_json::Value = deserialize_js(&data)?;

        let id = if let Some(existing_id) = value.get("id").and_then(|v| v.as_str()) {
            existing_id.to_string()
        } else {
            let mut inner = self.borrow_inner_mut()?;
            let counter = inner.id_counters.entry(entity.clone()).or_insert(0);
            *counter += 1;
            let generated_id = counter.to_string();
            if let serde_json::Value::Object(ref mut obj) = value {
                obj.insert(
                    "id".to_string(),
                    serde_json::Value::String(generated_id.clone()),
                );
            }
            generated_id
        };

        {
            let inner = self.borrow_inner()?;
            if let Some(schema) = inner.schemas.get(&entity) {
                schema
                    .apply_defaults(&mut value)
                    .map_err(|e| JsValue::from_str(&e.to_string()))?;
                schema
                    .validate(&value)
                    .map_err(|e| JsValue::from_str(&e.to_string()))?;
            }
        }

        self.validate_not_null_async(&entity, &value)?;
        self.validate_unique_async(&entity, &value, None).await?;
        self.validate_foreign_keys_async(&entity, &value).await?;

        if let serde_json::Value::Object(ref mut obj) = value {
            obj.remove("_version");
            obj.insert("_version".to_string(), serde_json::Value::Number(1.into()));
        }

        let key = format!("data/{entity}/{id}");
        let serialized = serde_json::to_vec(&value)
            .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))?;

        let stored = self
            .storage
            .encrypt_if_needed(key.as_bytes(), &serialized)
            .await?;
        let mut batch = self.storage.batch();
        batch.insert(key.as_bytes().to_vec(), stored);
        self.update_indexes_batch(&mut batch, &entity, &id, &value, None)?;
        batch.commit().await?;

        let event = ChangeEvent::create(entity, id, value.clone());
        self.dispatch_event(&event);

        serialize_js(&value)
    }

    /// Reads a record by entity and ID.
    ///
    /// # Errors
    /// Returns an error if the record is not found or deserialization fails.
    pub async fn read(&self, entity: String, id: String) -> Result<JsValue, JsValue> {
        let key = format!("data/{entity}/{id}");

        let data = self
            .storage
            .get(key.as_bytes())
            .await?
            .ok_or_else(|| JsValue::from_str(&format!("not found: {entity}/{id}")))?;

        let value: serde_json::Value = serde_json::from_slice(&data)
            .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;

        serialize_js(&value)
    }

    /// Reads a record with related entities eagerly loaded.
    ///
    /// # Errors
    /// Returns an error if the record is not found or includes cannot be loaded.
    pub async fn read_with_includes(
        &self,
        entity: String,
        id: String,
        includes: JsValue,
    ) -> Result<JsValue, JsValue> {
        let key = format!("data/{entity}/{id}");

        let include_list: Vec<String> = if includes.is_null() || includes.is_undefined() {
            Vec::new()
        } else {
            serde_wasm_bindgen::from_value(includes)
                .map_err(|e| JsValue::from_str(&format!("invalid includes: {e}")))?
        };

        let data = self
            .storage
            .get(key.as_bytes())
            .await?
            .ok_or_else(|| JsValue::from_str(&format!("not found: {entity}/{id}")))?;

        let mut value: serde_json::Value = serde_json::from_slice(&data)
            .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;

        if !include_list.is_empty() {
            self.load_includes_async(&entity, &mut value, &include_list, 0)
                .await?;
        }

        serialize_js(&value)
    }

    pub(crate) async fn load_includes_async(
        &self,
        entity: &str,
        value: &mut serde_json::Value,
        includes: &[String],
        depth: usize,
    ) -> Result<(), JsValue> {
        const MAX_DEPTH: usize = 3;
        if depth >= MAX_DEPTH {
            return Ok(());
        }

        let relationships: Vec<Relationship> = {
            let inner = self.borrow_inner()?;
            match inner.relationships.get(entity) {
                Some(rels) => rels.clone(),
                None => return Ok(()),
            }
        };

        for include in includes {
            let parts: Vec<&str> = include.splitn(2, '.').collect();
            let field_name = parts[0];
            let nested_includes: Vec<String> = if parts.len() > 1 {
                vec![parts[1].to_string()]
            } else {
                Vec::new()
            };

            if let Some(rel) = relationships.iter().find(|r| r.field == field_name) {
                let fk_value = value.get(&rel.field_suffix).cloned();
                if let Some(serde_json::Value::String(target_id)) = fk_value {
                    let target_key = format!("data/{}/{}", rel.target_entity, target_id);
                    if let Ok(Some(target_data)) = self.storage.get(target_key.as_bytes()).await
                        && let Ok(mut related) =
                            serde_json::from_slice::<serde_json::Value>(&target_data)
                    {
                        if !nested_includes.is_empty() {
                            Box::pin(self.load_includes_async(
                                &rel.target_entity,
                                &mut related,
                                &nested_includes,
                                depth + 1,
                            ))
                            .await?;
                        }
                        if let serde_json::Value::Object(obj) = value {
                            obj.insert(field_name.to_string(), related);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Updates an existing record with new field values.
    ///
    /// # Errors
    /// Returns an error if the record is not found or validation fails.
    pub async fn update(
        &self,
        entity: String,
        id: String,
        fields: JsValue,
    ) -> Result<JsValue, JsValue> {
        let key = format!("data/{entity}/{id}");
        let updates: serde_json::Value = deserialize_js(&fields)?;

        let existing_raw = self
            .storage
            .get_raw(key.as_bytes())
            .await?
            .ok_or_else(|| JsValue::from_str(&format!("not found: {entity}/{id}")))?;

        let existing_data = self
            .storage
            .get(key.as_bytes())
            .await?
            .ok_or_else(|| JsValue::from_str(&format!("not found: {entity}/{id}")))?;

        let existing: serde_json::Value = serde_json::from_slice(&existing_data)
            .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;

        let mut value = existing.clone();

        if let (serde_json::Value::Object(existing_obj), serde_json::Value::Object(new_fields)) =
            (&mut value, updates)
        {
            for (k, v) in new_fields {
                existing_obj.insert(k, v);
            }
        }

        let existing_version = existing
            .get("_version")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        if let serde_json::Value::Object(ref mut obj) = value {
            obj.insert(
                "_version".to_string(),
                serde_json::Value::Number((existing_version + 1).into()),
            );
        }

        {
            let inner = self.borrow_inner()?;
            if let Some(schema) = inner.schemas.get(&entity) {
                schema
                    .validate(&value)
                    .map_err(|e| JsValue::from_str(&e.to_string()))?;
            }
        }

        self.validate_not_null_async(&entity, &value)?;
        self.validate_unique_async(&entity, &value, Some(&id))
            .await?;
        self.validate_foreign_keys_async(&entity, &value).await?;

        let serialized = serde_json::to_vec(&value)
            .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))?;

        let stored = self
            .storage
            .encrypt_if_needed(key.as_bytes(), &serialized)
            .await?;
        let mut batch = self.storage.batch();
        batch.expect_value(key.as_bytes().to_vec(), existing_raw);
        batch.insert(key.as_bytes().to_vec(), stored);
        self.update_indexes_batch(&mut batch, &entity, &id, &value, Some(&existing))?;
        batch.commit().await?;

        let event = ChangeEvent::update(entity, id, value.clone());
        self.dispatch_event(&event);

        serialize_js(&value)
    }

    /// Deletes a record by entity and ID.
    ///
    /// # Errors
    /// Returns an error if the record is not found or foreign key constraints prevent deletion.
    pub async fn delete(&self, entity: String, id: String) -> Result<(), JsValue> {
        let key = format!("data/{entity}/{id}");

        let existing_data = self
            .storage
            .get(key.as_bytes())
            .await?
            .ok_or_else(|| JsValue::from_str(&format!("not found: {entity}/{id}")))?;

        let existing: serde_json::Value = serde_json::from_slice(&existing_data)
            .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;

        let cascade_deletes = self
            .check_foreign_key_constraints_async(&entity, &id)
            .await?;

        let mut batch = self.storage.batch();
        batch.remove(key.as_bytes().to_vec());
        self.remove_indexes_batch(&mut batch, &entity, &id, &existing)?;
        batch.commit().await?;

        for (cascade_entity, cascade_id) in cascade_deletes {
            let _ = Box::pin(self.delete(cascade_entity, cascade_id)).await;
        }

        let event = ChangeEvent::delete(entity, id, existing);
        self.dispatch_event(&event);

        Ok(())
    }

    #[must_use]
    pub fn is_memory_backend(&self) -> bool {
        self.storage.is_memory()
    }

    /// # Errors
    /// Returns an error if the record is not found or the backend is not memory-based.
    #[allow(clippy::needless_pass_by_value)]
    pub fn read_sync(&self, entity: String, id: String) -> Result<JsValue, JsValue> {
        let key = format!("data/{entity}/{id}");

        let data = self
            .storage
            .get_sync(key.as_bytes())?
            .ok_or_else(|| JsValue::from_str(&format!("not found: {entity}/{id}")))?;

        let value: serde_json::Value = serde_json::from_slice(&data)
            .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;

        serialize_js(&value)
    }

    /// # Errors
    /// Returns an error if validation fails or the backend is not memory-based.
    #[allow(clippy::needless_pass_by_value)]
    pub fn create_sync(&self, entity: String, data: JsValue) -> Result<JsValue, JsValue> {
        let mut value: serde_json::Value = deserialize_js(&data)?;

        let id = if let Some(existing_id) = value.get("id").and_then(|v| v.as_str()) {
            existing_id.to_string()
        } else {
            let mut inner = self.borrow_inner_mut()?;
            let counter = inner.id_counters.entry(entity.clone()).or_insert(0);
            *counter += 1;
            let generated_id = counter.to_string();
            if let serde_json::Value::Object(ref mut obj) = value {
                obj.insert(
                    "id".to_string(),
                    serde_json::Value::String(generated_id.clone()),
                );
            }
            generated_id
        };

        {
            let inner = self.borrow_inner()?;
            if let Some(schema) = inner.schemas.get(&entity) {
                schema
                    .apply_defaults(&mut value)
                    .map_err(|e| JsValue::from_str(&e.to_string()))?;
                schema
                    .validate(&value)
                    .map_err(|e| JsValue::from_str(&e.to_string()))?;
            }
        }

        self.validate_not_null_async(&entity, &value)?;
        self.validate_unique_sync(&entity, &value, None)?;
        self.validate_foreign_keys_sync(&entity, &value)?;

        if let serde_json::Value::Object(ref mut obj) = value {
            obj.remove("_version");
            obj.insert("_version".to_string(), serde_json::Value::Number(1.into()));
        }

        let key = format!("data/{entity}/{id}");
        let serialized = serde_json::to_vec(&value)
            .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))?;

        let mut batch = self.storage.batch();
        batch.insert(key.as_bytes().to_vec(), serialized);
        self.update_indexes_batch(&mut batch, &entity, &id, &value, None)?;
        batch.commit_sync()?;

        let event = ChangeEvent::create(entity, id, value.clone());
        self.dispatch_event(&event);

        serialize_js(&value)
    }

    /// # Errors
    /// Returns an error if the record is not found, validation fails, or the backend is not memory-based.
    #[allow(clippy::needless_pass_by_value)]
    pub fn update_sync(
        &self,
        entity: String,
        id: String,
        fields: JsValue,
    ) -> Result<JsValue, JsValue> {
        let key = format!("data/{entity}/{id}");
        let updates: serde_json::Value = deserialize_js(&fields)?;

        let existing_raw = self
            .storage
            .get_raw_sync(key.as_bytes())?
            .ok_or_else(|| JsValue::from_str(&format!("not found: {entity}/{id}")))?;

        let existing_data = self
            .storage
            .get_sync(key.as_bytes())?
            .ok_or_else(|| JsValue::from_str(&format!("not found: {entity}/{id}")))?;

        let existing: serde_json::Value = serde_json::from_slice(&existing_data)
            .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;

        let mut value = existing.clone();

        if let (serde_json::Value::Object(existing_obj), serde_json::Value::Object(new_fields)) =
            (&mut value, updates)
        {
            for (k, v) in new_fields {
                existing_obj.insert(k, v);
            }
        }

        let existing_version = existing
            .get("_version")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        if let serde_json::Value::Object(ref mut obj) = value {
            obj.insert(
                "_version".to_string(),
                serde_json::Value::Number((existing_version + 1).into()),
            );
        }

        {
            let inner = self.borrow_inner()?;
            if let Some(schema) = inner.schemas.get(&entity) {
                schema
                    .validate(&value)
                    .map_err(|e| JsValue::from_str(&e.to_string()))?;
            }
        }

        self.validate_not_null_async(&entity, &value)?;
        self.validate_unique_sync(&entity, &value, Some(&id))?;
        self.validate_foreign_keys_sync(&entity, &value)?;

        let serialized = serde_json::to_vec(&value)
            .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))?;

        let mut batch = self.storage.batch();
        batch.expect_value(key.as_bytes().to_vec(), existing_raw);
        batch.insert(key.as_bytes().to_vec(), serialized);
        self.update_indexes_batch(&mut batch, &entity, &id, &value, Some(&existing))?;
        batch.commit_sync()?;

        let event = ChangeEvent::update(entity, id, value.clone());
        self.dispatch_event(&event);

        serialize_js(&value)
    }

    /// # Errors
    /// Returns an error if the record is not found, foreign key constraints prevent deletion, or the backend is not memory-based.
    pub fn delete_sync(&self, entity: String, id: String) -> Result<(), JsValue> {
        let key = format!("data/{entity}/{id}");

        let existing_data = self
            .storage
            .get_sync(key.as_bytes())?
            .ok_or_else(|| JsValue::from_str(&format!("not found: {entity}/{id}")))?;

        let existing: serde_json::Value = serde_json::from_slice(&existing_data)
            .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;

        let cascade_deletes = self.check_foreign_key_constraints_sync(&entity, &id)?;

        let mut batch = self.storage.batch();
        batch.remove(key.as_bytes().to_vec());
        self.remove_indexes_batch(&mut batch, &entity, &id, &existing)?;
        batch.commit_sync()?;

        for (cascade_entity, cascade_id) in cascade_deletes {
            let _ = self.delete_sync(cascade_entity, cascade_id);
        }

        let event = ChangeEvent::delete(entity, id, existing);
        self.dispatch_event(&event);

        Ok(())
    }
}
