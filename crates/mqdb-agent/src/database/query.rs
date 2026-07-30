// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::Database;
use mqdb_core::entity::Entity;
use mqdb_core::error::{Error, Result};
use mqdb_core::keys;
use mqdb_core::query::RangeBound;
use mqdb_core::types::{Filter, FilterOp, MAX_FILTERS, MAX_SORT_FIELDS, Pagination, SortOrder};
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;

type RangeBounds = (String, Option<RangeBound>, Option<RangeBound>);

impl Database {
    /// # Errors
    /// Returns an error if scanning, filtering, or deserialization fails.
    #[tracing::instrument(skip(self, filters, sort, pagination, includes, projection), fields(entity = %entity_name))]
    pub async fn list(
        &self,
        entity_name: String,
        filters: Vec<Filter>,
        sort: Vec<SortOrder>,
        pagination: Option<Pagination>,
        includes: Vec<String>,
        projection: Option<Vec<String>>,
    ) -> Result<Vec<Value>> {
        self.validate_list_fields(&entity_name, &filters, &sort, projection.as_deref())
            .await?;

        self.list_core(entity_name, filters, sort, pagination, includes, projection)
            .await
    }

    /// # Errors
    /// Returns an error if any filter, sort, or projection field does not exist in the schema.
    pub(crate) async fn validate_list_fields(
        &self,
        entity_name: &str,
        filters: &[Filter],
        sort: &[SortOrder],
        projection: Option<&[String]>,
    ) -> Result<()> {
        let schema_registry = self.schema_registry.read().await;
        if !filters.is_empty() {
            let fields: Vec<&str> = filters.iter().map(|f| f.field.as_str()).collect();
            schema_registry.validate_fields_exist(entity_name, &fields, "filter")?;
        }
        if !sort.is_empty() {
            let fields: Vec<&str> = sort.iter().map(|s| s.field.as_str()).collect();
            schema_registry.validate_fields_exist(entity_name, &fields, "sort")?;
        }
        if let Some(fields) = projection {
            let field_refs: Vec<&str> = fields.iter().map(String::as_str).collect();
            schema_registry.validate_fields_exist(entity_name, &field_refs, "projection")?;
        }
        Ok(())
    }

    /// # Errors
    /// Returns an error if scanning, filtering, or deserialization fails.
    pub(crate) async fn list_core(
        &self,
        entity_name: String,
        filters: Vec<Filter>,
        sort: Vec<SortOrder>,
        pagination: Option<Pagination>,
        includes: Vec<String>,
        projection: Option<Vec<String>>,
    ) -> Result<Vec<Value>> {
        if filters.len() > MAX_FILTERS {
            return Err(Error::Validation(format!(
                "too many filters: {} exceeds maximum of {MAX_FILTERS}",
                filters.len()
            )));
        }
        if sort.len() > MAX_SORT_FIELDS {
            return Err(Error::Validation(format!(
                "too many sort fields: {} exceeds maximum of {MAX_SORT_FIELDS}",
                sort.len()
            )));
        }

        let (mut results, early_pagination_applied) = if filters.is_empty() && sort.is_empty() {
            (
                self.list_with_early_pagination(&entity_name, pagination.as_ref())?,
                true,
            )
        } else if filters.is_empty() {
            (self.scan_all_entities(&entity_name)?, false)
        } else {
            (self.list_with_filters(&entity_name, &filters).await?, false)
        };

        if !sort.is_empty() {
            Self::sort_results(&mut results, &sort);
        }

        let mut paginated = if early_pagination_applied {
            results
        } else {
            self.apply_pagination(&results, pagination.as_ref())
        };

        if !includes.is_empty() {
            for entity in &mut paginated {
                self.load_includes(entity, &entity_name, &includes, 0)
                    .await?;
            }
        }

        Ok(if let Some(ref fields) = projection {
            paginated
                .into_iter()
                .map(|e| mqdb_core::types::project_fields(e, fields))
                .collect()
        } else {
            paginated
        })
    }

    fn list_with_early_pagination(
        &self,
        entity_name: &str,
        pagination: Option<&Pagination>,
    ) -> Result<Vec<Value>> {
        let requested_limit = pagination.map_or(usize::MAX, |p| p.limit);
        let limit = self
            .config
            .max_list_results
            .map_or(requested_limit, |max| requested_limit.min(max));
        let offset = pagination.map_or(0, |p| p.offset);
        let prefix = format!("data/{entity_name}/");
        let batch_size = offset.saturating_add(limit);

        let mut results = Vec::new();
        let mut after_key: Option<Vec<u8>> = None;
        let mut skipped = 0;

        loop {
            let items = self.storage.prefix_scan_batch(
                prefix.as_bytes(),
                batch_size.min(1000),
                after_key.as_deref(),
            )?;
            if items.is_empty() {
                break;
            }
            for (key, value) in &items {
                if skipped < offset {
                    skipped += 1;
                    continue;
                }
                if results.len() >= limit {
                    return Ok(results);
                }
                let (_, id) = keys::decode_data_key(key)?;
                let entity = Entity::deserialize(entity_name.to_string(), id, value)?;
                results.push(entity.to_json());
            }
            if items.len() < batch_size.min(1000) {
                break;
            }
            after_key = items.last().map(|(k, _)| k.clone());
        }
        Ok(results)
    }

    async fn list_with_filters(&self, entity_name: &str, filters: &[Filter]) -> Result<Vec<Value>> {
        let index_manager = self.index_manager.read().await;

        for filter in filters {
            if filter.op == FilterOp::Eq
                && index_manager.is_field_indexed(entity_name, &filter.field)
            {
                let value_bytes = keys::encode_value_for_index(&filter.value)?;
                let ids = index_manager.lookup_by_field(
                    &self.storage,
                    entity_name,
                    &filter.field,
                    &value_bytes,
                )?;
                return if ids.is_empty() {
                    Ok(vec![])
                } else {
                    self.list_from_index_ids(entity_name, &ids, filters).await
                };
            }
        }

        if let Some((field, lower, upper)) =
            Self::collect_range_bounds(filters, |f| index_manager.is_field_indexed(entity_name, f))?
        {
            let ids = index_manager.lookup_by_range(
                &self.storage,
                entity_name,
                &field,
                lower.as_ref().map(|(v, inc)| (v.as_slice(), *inc)),
                upper.as_ref().map(|(v, inc)| (v.as_slice(), *inc)),
            )?;
            if !ids.is_empty() {
                return self.list_from_index_ids(entity_name, &ids, filters).await;
            }
        }

        self.scan_filtered_entities(entity_name, filters)
    }

    fn collect_range_bounds(
        filters: &[Filter],
        is_indexed: impl Fn(&str) -> bool,
    ) -> Result<Option<RangeBounds>> {
        let range_field = filters
            .iter()
            .find(|f| f.op.is_range() && is_indexed(&f.field))
            .map(|f| f.field.clone());

        let Some(field) = range_field else {
            return Ok(None);
        };

        let (lower, upper, _consumed) = mqdb_core::query::collect_range_bounds(filters, &field)?;

        Ok(Some((field, lower, upper)))
    }

    async fn list_from_index_ids(
        &self,
        entity_name: &str,
        ids: &[String],
        filters: &[Filter],
    ) -> Result<Vec<Value>> {
        let max_results = self.config.max_list_results.unwrap_or(usize::MAX);
        let mut results = Vec::new();
        for id in ids {
            if results.len() >= max_results {
                break;
            }
            match self
                .read(entity_name.to_string(), id.clone(), vec![], None)
                .await
            {
                Ok(entity_data) => {
                    if Self::matches_filters(&entity_data, filters) {
                        results.push(entity_data);
                    }
                }
                Err(Error::NotFound { .. }) => {
                    match self.purge_stale_index_entries(entity_name, id) {
                        Ok(0) => tracing::debug!(
                            entity = entity_name,
                            id = %id,
                            "stale index entry resolved by concurrent re-create",
                        ),
                        Ok(removed) => tracing::info!(
                            entity = entity_name,
                            id = %id,
                            removed,
                            "self-healed stale index entries pointing to deleted row",
                        ),
                        Err(err) => tracing::warn!(
                            entity = entity_name,
                            id = %id,
                            error = %err,
                            "failed to purge stale index entries",
                        ),
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Ok(results)
    }

    fn purge_stale_index_entries(&self, entity_name: &str, id: &str) -> Result<usize> {
        let data_key = keys::encode_data_key(entity_name, id);
        if self.storage.get(&data_key)?.is_some() {
            return Ok(0);
        }

        let mut entity_index_prefix =
            Vec::with_capacity(keys::INDEX_PREFIX.len() + 1 + entity_name.len() + 1);
        entity_index_prefix.extend_from_slice(keys::INDEX_PREFIX);
        entity_index_prefix.push(keys::SEPARATOR);
        entity_index_prefix.extend_from_slice(entity_name.as_bytes());
        entity_index_prefix.push(keys::SEPARATOR);

        let mut id_suffix = Vec::with_capacity(1 + id.len());
        id_suffix.push(keys::SEPARATOR);
        id_suffix.extend_from_slice(id.as_bytes());

        let candidate_keys = self.storage.prefix_scan_keys(&entity_index_prefix)?;
        let mut batch = self.storage.batch();
        let mut removed = 0usize;
        for key in candidate_keys {
            if key.ends_with(&id_suffix) {
                batch.remove(key);
                removed += 1;
            }
        }
        if removed > 0 {
            batch.commit()?;
        }
        Ok(removed)
    }

    /// # Errors
    /// Returns an error if cursor initialization or field validation fails.
    pub async fn cursor(
        &self,
        entity_name: String,
        filters: Vec<Filter>,
        sort: Vec<SortOrder>,
    ) -> Result<crate::cursor::Cursor> {
        {
            let schema_registry = self.schema_registry.read().await;
            if !filters.is_empty() {
                let fields: Vec<&str> = filters.iter().map(|f| f.field.as_str()).collect();
                schema_registry.validate_fields_exist(&entity_name, &fields, "filter")?;
            }
            if !sort.is_empty() {
                let fields: Vec<&str> = sort.iter().map(|s| s.field.as_str()).collect();
                schema_registry.validate_fields_exist(&entity_name, &fields, "sort")?;
            }
        }
        crate::cursor::Cursor::new(
            entity_name,
            &self.storage,
            filters,
            sort,
            self.config.max_cursor_buffer,
            self.config.max_sort_buffer,
        )
    }

    fn scan_all_entities(&self, entity_name: &str) -> Result<Vec<Value>> {
        let max_results = self.config.max_sort_buffer;
        let prefix = format!("data/{entity_name}/");
        let items = self.storage.prefix_scan(prefix.as_bytes())?;
        let mut results = Vec::new();
        for (key, value) in items {
            if results.len() >= max_results {
                break;
            }
            let (_, id) = keys::decode_data_key(&key)?;
            let entity = Entity::deserialize(entity_name.to_string(), id, &value)?;
            results.push(entity.to_json());
        }
        Ok(results)
    }

    fn scan_filtered_entities(&self, entity_name: &str, filters: &[Filter]) -> Result<Vec<Value>> {
        let max_results = self.config.max_list_results.unwrap_or(usize::MAX);
        let prefix = format!("data/{entity_name}/");
        let items = self.storage.prefix_scan(prefix.as_bytes())?;
        let mut results = Vec::new();
        for (key, value) in items {
            if results.len() >= max_results {
                break;
            }
            let (_, id) = keys::decode_data_key(&key)?;
            let entity = Entity::deserialize(entity_name.to_string(), id, &value)?;
            let entity_data = entity.to_json();
            if Self::matches_filters(&entity_data, filters) {
                results.push(entity_data);
            }
        }
        Ok(results)
    }

    fn apply_pagination(&self, results: &[Value], pagination: Option<&Pagination>) -> Vec<Value> {
        let offset = pagination.map_or(0, |p| p.offset);
        let limit = pagination.map_or(usize::MAX, |p| p.limit);
        let requested_end = offset.saturating_add(limit).min(results.len());

        let final_end = if let Some(max) = self.config.max_list_results {
            if requested_end > max {
                tracing::warn!(
                    "list operation would exceed max_list_results limit of {}, truncating",
                    max
                );
                max.min(results.len())
            } else {
                requested_end
            }
        } else {
            requested_end
        };

        if offset >= results.len() {
            return vec![];
        }
        results[offset..final_end].to_vec()
    }

    fn sort_results(results: &mut [Value], sort: &[SortOrder]) {
        mqdb_core::query::sort_results(results, sort);
    }

    pub(super) fn matches_filters(entity: &Value, filters: &[Filter]) -> bool {
        for filter in filters {
            if let Some(field_value) = entity.get(&filter.field) {
                if !filter.matches(field_value) {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }

    pub(super) fn load_includes<'a>(
        &'a self,
        entity: &'a mut Value,
        entity_name: &'a str,
        includes: &'a [String],
        depth: usize,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + 'a + Send>> {
        Box::pin(async move {
            const MAX_DEPTH: usize = 3;

            if depth >= MAX_DEPTH {
                return Ok(());
            }

            let registry = self.relationship_registry.read().await;

            for include_field in includes {
                if let Some(rel) = registry.get(entity_name, include_field)
                    && let Some(id_value) = entity.get(&rel.field_suffix)
                    && let Some(id_str) = id_value.as_str()
                {
                    match self
                        .read(rel.target_entity.clone(), id_str.to_string(), vec![], None)
                        .await
                    {
                        Ok(related_entity) => {
                            if let Value::Object(obj) = entity {
                                obj.insert(rel.field.clone(), related_entity);
                            }
                        }
                        Err(Error::NotFound { .. }) => {
                            tracing::warn!(
                                "related entity not found: {}/{}",
                                rel.target_entity,
                                id_str
                            );
                        }
                        Err(e) => return Err(e),
                    }
                }
            }

            Ok(())
        })
    }
}

#[cfg(test)]
mod stale_index_tests {
    use super::Database;
    use mqdb_core::keys;
    use mqdb_core::types::{Filter, FilterOp, ScopeConfig};
    use serde_json::json;
    use tempfile::TempDir;

    async fn test_db() -> (TempDir, Database) {
        let tmp = TempDir::new().unwrap();
        let db = Database::open_without_background_tasks(tmp.path())
            .await
            .unwrap();
        (tmp, db)
    }

    #[tokio::test]
    async fn list_purges_stale_index_entry_for_missing_row() {
        let (_tmp, db) = test_db().await;
        db.add_index("users".to_string(), vec!["email".to_string()])
            .await
            .unwrap();

        let scope = ScopeConfig::default();
        db.create(
            "users".to_string(),
            json!({ "id": "real", "email": "real@x.com" }),
            None,
            None,
            None,
            &scope,
        )
        .await
        .unwrap();

        let ghost_value = keys::encode_value_for_index(&json!("ghost@x.com")).unwrap();
        let ghost_key = keys::encode_index_key("users", "email", &ghost_value, "ghost");
        let mut batch = db.storage.batch();
        batch.insert(ghost_key.clone(), Vec::new());
        batch.commit().unwrap();

        assert!(
            db.storage.get(&ghost_key).unwrap().is_some(),
            "ghost index entry should exist before list",
        );

        let results = db
            .list(
                "users".to_string(),
                vec![Filter {
                    field: "email".to_string(),
                    op: FilterOp::Eq,
                    value: json!("ghost@x.com"),
                }],
                vec![],
                None,
                vec![],
                None,
            )
            .await
            .unwrap();
        assert!(results.is_empty(), "stale index must not return rows");

        assert!(
            db.storage.get(&ghost_key).unwrap().is_none(),
            "ghost index entry must be purged by self-heal",
        );

        let real_value = keys::encode_value_for_index(&json!("real@x.com")).unwrap();
        let real_key = keys::encode_index_key("users", "email", &real_value, "real");
        assert!(
            db.storage.get(&real_key).unwrap().is_some(),
            "live index entry must survive self-heal",
        );
    }

    #[tokio::test]
    async fn purge_skips_when_row_was_recreated_before_heal() {
        let (_tmp, db) = test_db().await;
        db.add_index("users".to_string(), vec!["email".to_string()])
            .await
            .unwrap();

        let scope = ScopeConfig::default();
        db.create(
            "users".to_string(),
            json!({ "id": "ghost", "email": "ghost@x.com" }),
            None,
            None,
            None,
            &scope,
        )
        .await
        .unwrap();

        let stray_value = keys::encode_value_for_index(&json!("stray@x.com")).unwrap();
        let stray_key = keys::encode_index_key("users", "email", &stray_value, "ghost");
        let mut batch = db.storage.batch();
        batch.insert(stray_key.clone(), Vec::new());
        batch.commit().unwrap();

        let removed = db.purge_stale_index_entries("users", "ghost").unwrap();
        assert_eq!(removed, 0, "must not purge when data row still exists");
        assert!(
            db.storage.get(&stray_key).unwrap().is_some(),
            "stray entry must remain when row exists (race-safe)",
        );
    }
}
