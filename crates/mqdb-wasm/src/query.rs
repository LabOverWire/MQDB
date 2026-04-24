// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::{
    CountOptions, FilterJs, JsValue, ListOptions, SortOrderJs, WasmDatabase, wasm_bindgen,
};
use mqdb_core::keys::{WASM_INDEX_PREFIX, encode_value_for_index};

const MAX_FILTERS: usize = mqdb_core::types::MAX_FILTERS;
const MAX_SORT_FIELDS: usize = mqdb_core::types::MAX_SORT_FIELDS;
const MAX_LIST_RESULTS: usize = mqdb_core::types::MAX_LIST_RESULTS;

type ScanResult = Option<(Vec<serde_json::Value>, Vec<FilterJs>)>;
type RangeBoundsResult = (
    Option<mqdb_core::query::RangeBound>,
    Option<mqdb_core::query::RangeBound>,
    Vec<usize>,
);

#[wasm_bindgen(js_class = "Database")]
impl WasmDatabase {
    /// Lists records from an entity with optional filtering, sorting, and pagination.
    ///
    /// # Errors
    /// Returns an error if options are invalid or the storage operation fails.
    pub async fn list(&self, entity: String, options: JsValue) -> Result<JsValue, JsValue> {
        let opts: ListOptions = if options.is_null() || options.is_undefined() {
            ListOptions::default()
        } else {
            serde_wasm_bindgen::from_value(options)
                .map_err(|e| JsValue::from_str(&format!("invalid options: {e}")))?
        };
        Self::validate_query_limits(&opts.filters, &opts.sort)?;
        self.validate_query_fields(
            &entity,
            &opts.filters,
            &opts.sort,
            opts.projection.as_deref(),
        )?;

        let mut results = if let Some((records, remaining)) =
            self.try_index_scans_async(&entity, &opts.filters).await?
        {
            Self::apply_remaining_filters(records, &remaining)
        } else {
            self.full_scan_async(&entity, &opts.filters).await?
        };

        if !opts.sort.is_empty() {
            Self::sort_results(&mut results, &opts.sort);
        }

        if let Some(pagination) = &opts.pagination {
            let offset = pagination.offset;
            let limit = pagination.limit;
            results = results.into_iter().skip(offset).take(limit).collect();
        }
        results.truncate(MAX_LIST_RESULTS);

        if let Some(ref includes) = opts.includes {
            for result in &mut results {
                self.load_includes_async(&entity, result, includes, 0)
                    .await?;
            }
        }

        if let Some(ref projection) = opts.projection {
            results = results
                .into_iter()
                .map(|v| Self::project_fields(v, projection))
                .collect();
        }

        let json_str = serde_json::to_string(&results)
            .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))?;
        js_sys::JSON::parse(&json_str)
            .map_err(|e| JsValue::from_str(&format!("JSON parse error: {e:?}")))
    }

    /// # Errors
    /// Returns an error if options are invalid or the backend is not memory-based.
    #[allow(clippy::needless_pass_by_value)]
    #[wasm_bindgen(js_name = "listSync")]
    pub fn list_sync(&self, entity: String, options: JsValue) -> Result<JsValue, JsValue> {
        let opts: ListOptions = if options.is_null() || options.is_undefined() {
            ListOptions::default()
        } else {
            serde_wasm_bindgen::from_value(options)
                .map_err(|e| JsValue::from_str(&format!("invalid options: {e}")))?
        };
        Self::validate_query_limits(&opts.filters, &opts.sort)?;
        self.validate_query_fields(
            &entity,
            &opts.filters,
            &opts.sort,
            opts.projection.as_deref(),
        )?;

        let mut results = if let Some((records, remaining)) =
            self.try_index_scans_sync(&entity, &opts.filters)?
        {
            Self::apply_remaining_filters(records, &remaining)
        } else {
            self.full_scan_sync(&entity, &opts.filters)?
        };

        if !opts.sort.is_empty() {
            Self::sort_results(&mut results, &opts.sort);
        }

        if let Some(pagination) = &opts.pagination {
            let offset = pagination.offset;
            let limit = pagination.limit;
            results = results.into_iter().skip(offset).take(limit).collect();
        }
        results.truncate(MAX_LIST_RESULTS);

        if let Some(ref projection) = opts.projection {
            results = results
                .into_iter()
                .map(|v| Self::project_fields(v, projection))
                .collect();
        }

        let json_str = serde_json::to_string(&results)
            .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))?;
        js_sys::JSON::parse(&json_str)
            .map_err(|e| JsValue::from_str(&format!("JSON parse error: {e:?}")))
    }

    /// Counts records matching optional filters, using indexes when available.
    ///
    /// # Errors
    /// Returns an error if options are invalid or the storage operation fails.
    pub async fn count(&self, entity: String, options: JsValue) -> Result<usize, JsValue> {
        let opts: CountOptions = if options.is_null() || options.is_undefined() {
            CountOptions::default()
        } else {
            serde_wasm_bindgen::from_value(options)
                .map_err(|e| JsValue::from_str(&format!("invalid options: {e}")))?
        };
        Self::validate_query_limits(&opts.filters, &[])?;
        self.validate_query_fields(&entity, &opts.filters, &[], None)?;

        if opts.filters.is_empty() {
            let prefix = format!("data/{entity}/");
            let items = self.storage.prefix_scan(prefix.as_bytes()).await?;
            return Ok(items.len());
        }

        if let Some((records, remaining)) =
            self.try_index_scans_async(&entity, &opts.filters).await?
        {
            if remaining.is_empty() {
                return Ok(records.len());
            }
            return Ok(Self::apply_remaining_filters(records, &remaining).len());
        }

        let core_filters: Vec<_> = opts.filters.iter().map(FilterJs::to_core_filter).collect();
        let prefix = format!("data/{entity}/");
        let items = self.storage.prefix_scan(prefix.as_bytes()).await?;
        let mut count = 0usize;
        for (_key, value) in items {
            let parsed: serde_json::Value = serde_json::from_slice(&value)
                .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;
            if mqdb_core::query::matches_all_filters(&parsed, &core_filters) {
                count += 1;
            }
        }
        Ok(count)
    }

    /// # Errors
    /// Returns an error if options are invalid or the backend is not memory-based.
    #[allow(clippy::needless_pass_by_value)]
    #[wasm_bindgen(js_name = "countSync")]
    pub fn count_sync(&self, entity: String, options: JsValue) -> Result<usize, JsValue> {
        let opts: CountOptions = if options.is_null() || options.is_undefined() {
            CountOptions::default()
        } else {
            serde_wasm_bindgen::from_value(options)
                .map_err(|e| JsValue::from_str(&format!("invalid options: {e}")))?
        };
        Self::validate_query_limits(&opts.filters, &[])?;
        self.validate_query_fields(&entity, &opts.filters, &[], None)?;

        if opts.filters.is_empty() {
            let prefix = format!("data/{entity}/");
            let items = self.storage.prefix_scan_sync(prefix.as_bytes())?;
            return Ok(items.len());
        }

        if let Some((records, remaining)) = self.try_index_scans_sync(&entity, &opts.filters)? {
            if remaining.is_empty() {
                return Ok(records.len());
            }
            return Ok(Self::apply_remaining_filters(records, &remaining).len());
        }

        let core_filters: Vec<_> = opts.filters.iter().map(FilterJs::to_core_filter).collect();
        let prefix = format!("data/{entity}/");
        let items = self.storage.prefix_scan_sync(prefix.as_bytes())?;
        let mut count = 0usize;
        for (_key, value) in items {
            let parsed: serde_json::Value = serde_json::from_slice(&value)
                .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;
            if mqdb_core::query::matches_all_filters(&parsed, &core_filters) {
                count += 1;
            }
        }
        Ok(count)
    }
}

impl WasmDatabase {
    fn is_equality_op(op: &str) -> bool {
        mqdb_core::FilterOp::from_js_op(op).is_some_and(|o| o.is_equality())
    }

    fn is_range_op(op: &str) -> bool {
        mqdb_core::FilterOp::from_js_op(op).is_some_and(|o| o.is_range())
    }

    pub(crate) fn apply_remaining_filters(
        records: Vec<serde_json::Value>,
        remaining: &[FilterJs],
    ) -> Vec<serde_json::Value> {
        let core_filters: Vec<_> = remaining.iter().map(FilterJs::to_core_filter).collect();
        records
            .into_iter()
            .filter(|r| mqdb_core::query::matches_all_filters(r, &core_filters))
            .collect()
    }

    pub(crate) async fn full_scan_async(
        &self,
        entity: &str,
        filters: &[FilterJs],
    ) -> Result<Vec<serde_json::Value>, JsValue> {
        let core_filters: Vec<_> = filters.iter().map(FilterJs::to_core_filter).collect();
        let prefix = format!("data/{entity}/");
        let items = self.storage.prefix_scan(prefix.as_bytes()).await?;
        let mut filtered = Vec::new();
        for (_key, value) in items {
            let parsed: serde_json::Value = serde_json::from_slice(&value)
                .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;
            if mqdb_core::query::matches_all_filters(&parsed, &core_filters) {
                filtered.push(parsed);
            }
        }
        Ok(filtered)
    }

    fn full_scan_sync(
        &self,
        entity: &str,
        filters: &[FilterJs],
    ) -> Result<Vec<serde_json::Value>, JsValue> {
        let core_filters: Vec<_> = filters.iter().map(FilterJs::to_core_filter).collect();
        let prefix = format!("data/{entity}/");
        let items = self.storage.prefix_scan_sync(prefix.as_bytes())?;
        let mut filtered = Vec::new();
        for (_key, value) in items {
            let parsed: serde_json::Value = serde_json::from_slice(&value)
                .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;
            if mqdb_core::query::matches_all_filters(&parsed, &core_filters) {
                filtered.push(parsed);
            }
        }
        Ok(filtered)
    }

    #[allow(clippy::type_complexity)]
    pub(crate) async fn try_index_scans_async(
        &self,
        entity: &str,
        filters: &[FilterJs],
    ) -> Result<ScanResult, JsValue> {
        if let Some(result) = self.equality_scan_async(entity, filters).await? {
            return Ok(Some(result));
        }
        if let Some(result) = self.range_scan_async(entity, filters).await? {
            return Ok(Some(result));
        }
        Ok(None)
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn try_index_scans_sync(
        &self,
        entity: &str,
        filters: &[FilterJs],
    ) -> Result<ScanResult, JsValue> {
        if let Some(result) = self.equality_scan_sync(entity, filters)? {
            return Ok(Some(result));
        }
        if let Some(result) = self.range_scan_sync(entity, filters)? {
            return Ok(Some(result));
        }
        Ok(None)
    }

    fn find_equality_index<'a>(
        &self,
        entity: &str,
        filters: &'a [FilterJs],
    ) -> Result<Option<(usize, &'a FilterJs)>, JsValue> {
        let inner = self.borrow_inner()?;
        let Some(index_defs) = inner.indexes.get(entity) else {
            return Ok(None);
        };

        for (i, filter) in filters.iter().enumerate() {
            if !Self::is_equality_op(&filter.op) {
                continue;
            }
            let target = vec![filter.field.clone()];
            if index_defs.contains(&target) {
                return Ok(Some((i, filter)));
            }
        }
        Ok(None)
    }

    fn find_range_field(
        &self,
        entity: &str,
        filters: &[FilterJs],
    ) -> Result<Option<String>, JsValue> {
        let inner = self.borrow_inner()?;
        let Some(index_defs) = inner.indexes.get(entity) else {
            return Ok(None);
        };

        for filter in filters {
            if !Self::is_range_op(&filter.op) {
                continue;
            }
            let target = vec![filter.field.clone()];
            if index_defs.contains(&target) {
                return Ok(Some(filter.field.clone()));
            }
        }
        Ok(None)
    }

    fn collect_range_bounds(
        filters: &[FilterJs],
        field: &str,
    ) -> Result<RangeBoundsResult, JsValue> {
        let core_filters: Vec<_> = filters.iter().map(FilterJs::to_core_filter).collect();
        mqdb_core::query::collect_range_bounds(&core_filters, field)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    async fn equality_scan_async(
        &self,
        entity: &str,
        filters: &[FilterJs],
    ) -> Result<ScanResult, JsValue> {
        let matched = self.find_equality_index(entity, filters)?;
        let Some((matched_idx, matched_filter)) = matched else {
            return Ok(None);
        };

        let encoded_value = encode_value_for_index(&matched_filter.value)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        let mut index_prefix = format!("index/{entity}/{}/", matched_filter.field).into_bytes();
        index_prefix.extend_from_slice(&encoded_value);
        index_prefix.push(b'/');

        let index_entries = self.storage.prefix_scan(&index_prefix).await?;
        let ids = mqdb_core::query::extract_ids_from_index_keys(&index_entries);

        let mut records = Vec::with_capacity(ids.len());
        for id in &ids {
            let data_key = format!("data/{entity}/{id}");
            if let Some(data) = self.storage.get(data_key.as_bytes()).await? {
                let parsed: serde_json::Value = serde_json::from_slice(&data)
                    .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;
                records.push(parsed);
            }
        }

        let remaining: Vec<FilterJs> = filters
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != matched_idx)
            .map(|(_, f)| f.clone())
            .collect();

        Ok(Some((records, remaining)))
    }

    fn equality_scan_sync(
        &self,
        entity: &str,
        filters: &[FilterJs],
    ) -> Result<ScanResult, JsValue> {
        let matched = self.find_equality_index(entity, filters)?;
        let Some((matched_idx, matched_filter)) = matched else {
            return Ok(None);
        };

        let encoded_value = encode_value_for_index(&matched_filter.value)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        let mut index_prefix = format!("index/{entity}/{}/", matched_filter.field).into_bytes();
        index_prefix.extend_from_slice(&encoded_value);
        index_prefix.push(b'/');

        let index_entries = self.storage.prefix_scan_sync(&index_prefix)?;
        let ids = mqdb_core::query::extract_ids_from_index_keys(&index_entries);

        let mut records = Vec::with_capacity(ids.len());
        for id in &ids {
            let data_key = format!("data/{entity}/{id}");
            if let Some(data) = self.storage.get_sync(data_key.as_bytes())? {
                let parsed: serde_json::Value = serde_json::from_slice(&data)
                    .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;
                records.push(parsed);
            }
        }

        let remaining: Vec<FilterJs> = filters
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != matched_idx)
            .map(|(_, f)| f.clone())
            .collect();

        Ok(Some((records, remaining)))
    }

    async fn range_scan_async(
        &self,
        entity: &str,
        filters: &[FilterJs],
    ) -> Result<ScanResult, JsValue> {
        let Some(field) = self.find_range_field(entity, filters)? else {
            return Ok(None);
        };

        let (lower, upper, consumed) = Self::collect_range_bounds(filters, &field)?;
        let (start, end) = mqdb_core::query::build_range_keys(
            entity,
            &field,
            lower.as_ref().map(|(v, inc)| (v.as_slice(), *inc)),
            upper.as_ref().map(|(v, inc)| (v.as_slice(), *inc)),
            WASM_INDEX_PREFIX,
        );

        let index_entries = self.storage.range_scan(&start, &end).await?;
        let ids = mqdb_core::query::extract_ids_from_index_keys(&index_entries);

        let mut records = Vec::with_capacity(ids.len());
        for id in &ids {
            let data_key = format!("data/{entity}/{id}");
            if let Some(data) = self.storage.get(data_key.as_bytes()).await? {
                let parsed: serde_json::Value = serde_json::from_slice(&data)
                    .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;
                records.push(parsed);
            }
        }

        let remaining: Vec<FilterJs> = filters
            .iter()
            .enumerate()
            .filter(|(i, _)| !consumed.contains(i))
            .map(|(_, f)| f.clone())
            .collect();

        Ok(Some((records, remaining)))
    }

    fn range_scan_sync(&self, entity: &str, filters: &[FilterJs]) -> Result<ScanResult, JsValue> {
        let Some(field) = self.find_range_field(entity, filters)? else {
            return Ok(None);
        };

        let (lower, upper, consumed) = Self::collect_range_bounds(filters, &field)?;
        let (start, end) = mqdb_core::query::build_range_keys(
            entity,
            &field,
            lower.as_ref().map(|(v, inc)| (v.as_slice(), *inc)),
            upper.as_ref().map(|(v, inc)| (v.as_slice(), *inc)),
            WASM_INDEX_PREFIX,
        );

        let index_entries = self.storage.range_scan_sync(&start, &end)?;
        let ids = mqdb_core::query::extract_ids_from_index_keys(&index_entries);

        let mut records = Vec::with_capacity(ids.len());
        for id in &ids {
            let data_key = format!("data/{entity}/{id}");
            if let Some(data) = self.storage.get_sync(data_key.as_bytes())? {
                let parsed: serde_json::Value = serde_json::from_slice(&data)
                    .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;
                records.push(parsed);
            }
        }

        let remaining: Vec<FilterJs> = filters
            .iter()
            .enumerate()
            .filter(|(i, _)| !consumed.contains(i))
            .map(|(_, f)| f.clone())
            .collect();

        Ok(Some((records, remaining)))
    }

    pub(crate) fn sort_results(results: &mut [serde_json::Value], sort: &[SortOrderJs]) {
        let core_sort: Vec<mqdb_core::SortOrder> = sort
            .iter()
            .map(|s| {
                if s.direction == "desc" {
                    mqdb_core::SortOrder::desc(s.field.clone())
                } else {
                    mqdb_core::SortOrder::asc(s.field.clone())
                }
            })
            .collect();
        mqdb_core::query::sort_results(results, &core_sort);
    }

    pub(crate) fn validate_query_fields(
        &self,
        entity: &str,
        filters: &[FilterJs],
        sort: &[SortOrderJs],
        projection: Option<&[String]>,
    ) -> Result<(), JsValue> {
        let inner = self.borrow_inner()?;
        let Some(schema) = inner.schemas.get(entity) else {
            return Ok(());
        };

        for filter in filters {
            if filter.field != "id" && !schema.fields.contains_key(&filter.field) {
                return Err(JsValue::from_str(&format!(
                    "unknown field in filter: '{}'",
                    filter.field
                )));
            }
        }
        for s in sort {
            if s.field != "id" && !schema.fields.contains_key(&s.field) {
                return Err(JsValue::from_str(&format!(
                    "unknown field in sort: '{}'",
                    s.field
                )));
            }
        }
        if let Some(fields) = projection {
            for f in fields {
                if f != "id" && !schema.fields.contains_key(f) {
                    return Err(JsValue::from_str(&format!(
                        "unknown field in projection: '{f}'"
                    )));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn validate_query_limits(
        filters: &[FilterJs],
        sort: &[SortOrderJs],
    ) -> Result<(), JsValue> {
        if filters.len() > MAX_FILTERS {
            return Err(JsValue::from_str(&format!(
                "too many filters: {} (max {MAX_FILTERS})",
                filters.len()
            )));
        }
        if sort.len() > MAX_SORT_FIELDS {
            return Err(JsValue::from_str(&format!(
                "too many sort fields: {} (max {MAX_SORT_FIELDS})",
                sort.len()
            )));
        }
        Ok(())
    }

    pub(crate) fn project_fields(value: serde_json::Value, fields: &[String]) -> serde_json::Value {
        if let serde_json::Value::Object(obj) = value {
            let mut projected = serde_json::Map::new();

            if let Some(id) = obj.get("id") {
                projected.insert("id".to_string(), id.clone());
            }

            for field in fields {
                if field != "id"
                    && let Some(v) = obj.get(field)
                {
                    projected.insert(field.clone(), v.clone());
                }
            }

            serde_json::Value::Object(projected)
        } else {
            value
        }
    }
}
