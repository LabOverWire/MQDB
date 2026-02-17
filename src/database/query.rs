// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::Database;
use crate::entity::Entity;
use crate::error::{Error, Result};
use crate::keys;
use crate::types::{
    Filter, FilterOp, MAX_FILTERS, MAX_SORT_FIELDS, Pagination, SortDirection, SortOrder,
};
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;

impl Database {
    /// # Errors
    /// Returns an error if scanning, filtering, or deserialization fails.
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
                .map(|e| Self::project_fields(e, fields))
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
        let items = self.storage.prefix_scan(prefix.as_bytes())?;

        let mut results = Vec::new();
        let mut skipped = 0;
        for (key, value) in items {
            if skipped < offset {
                skipped += 1;
                continue;
            }
            if results.len() >= limit {
                break;
            }
            let (_, id) = keys::decode_data_key(&key)?;
            let entity = Entity::deserialize(entity_name.to_string(), id, &value)?;
            results.push(entity.to_json());
        }
        Ok(results)
    }

    async fn list_with_filters(&self, entity_name: &str, filters: &[Filter]) -> Result<Vec<Value>> {
        let index_manager = self.index_manager.read().await;

        if let Some(filter) = filters.first()
            && filter.op == FilterOp::Eq
        {
            let value_bytes = keys::encode_value_for_index(&filter.value)?;
            let ids = index_manager.lookup_by_field(
                &self.storage,
                entity_name,
                &filter.field,
                &value_bytes,
            )?;
            if !ids.is_empty() {
                return self.list_from_index_ids(entity_name, &ids, filters).await;
            }
        }
        self.scan_filtered_entities(entity_name, filters)
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
                    tracing::warn!(
                        "index pointed to non-existent entity: {}/{}",
                        entity_name,
                        id
                    );
                }
                Err(e) => return Err(e),
            }
        }
        Ok(results)
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
        results.sort_by(|a, b| {
            for order in sort {
                let a_val = a.get(&order.field);
                let b_val = b.get(&order.field);

                let cmp = match (a_val, b_val) {
                    (Some(av), Some(bv)) => Self::compare_json_values(av, bv),
                    (Some(_), None) => std::cmp::Ordering::Greater,
                    (None, Some(_)) => std::cmp::Ordering::Less,
                    (None, None) => std::cmp::Ordering::Equal,
                };

                let cmp = match order.direction {
                    SortDirection::Asc => cmp,
                    SortDirection::Desc => cmp.reverse(),
                };

                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    fn compare_json_values(a: &Value, b: &Value) -> std::cmp::Ordering {
        match (a, b) {
            (Value::Number(a_num), Value::Number(b_num)) => {
                let a_f64 = a_num.as_f64().unwrap_or(0.0);
                let b_f64 = b_num.as_f64().unwrap_or(0.0);
                a_f64
                    .partial_cmp(&b_f64)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }
            (Value::String(a_str), Value::String(b_str)) => a_str.cmp(b_str),
            (Value::Bool(a_bool), Value::Bool(b_bool)) => a_bool.cmp(b_bool),
            _ => std::cmp::Ordering::Equal,
        }
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

    pub(super) fn project_fields(entity: Value, fields: &[String]) -> Value {
        if let Value::Object(obj) = entity {
            let mut projected = serde_json::Map::new();

            if let Some(id) = obj.get("id") {
                projected.insert("id".to_string(), id.clone());
            }

            for field in fields {
                if field != "id"
                    && let Some(value) = obj.get(field)
                {
                    projected.insert(field.clone(), value.clone());
                }
            }
            Value::Object(projected)
        } else {
            entity
        }
    }
}
