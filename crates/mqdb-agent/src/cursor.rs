// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use mqdb_core::entity::Entity;
use mqdb_core::error::Result;
use mqdb_core::storage::Storage;
use mqdb_core::types::{Filter, SortDirection, SortOrder};
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::Arc;

#[derive(Clone)]
pub struct Query {
    pub filters: Vec<Filter>,
    pub sort: Vec<SortOrder>,
    pub includes: Vec<String>,
    pub batch_size: usize,
}

impl Query {
    #[allow(clippy::must_use_candidate)]
    pub fn new() -> Self {
        Self {
            filters: Vec::new(),
            sort: Vec::new(),
            includes: Vec::new(),
            batch_size: 100,
        }
    }

    #[must_use]
    pub fn with_filters(mut self, filters: Vec<Filter>) -> Self {
        self.filters = filters;
        self
    }

    #[must_use]
    pub fn with_sort(mut self, sort: Vec<SortOrder>) -> Self {
        self.sort = sort;
        self
    }

    #[must_use]
    pub fn with_includes(mut self, includes: Vec<String>) -> Self {
        self.includes = includes;
        self
    }

    #[must_use]
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }
}

impl Default for Query {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Cursor {
    entity_name: String,
    filters: Vec<Filter>,
    sort_orders: Vec<SortOrder>,
    buffer: VecDeque<Value>,
    sorted_buffer: Option<Vec<Value>>,
    sorted_position: usize,
    fjall_items: Vec<(Vec<u8>, Vec<u8>)>,
    position: usize,
    done: bool,
    max_buffer_size: usize,
    max_sort_buffer: usize,
}

impl Cursor {
    pub(crate) fn new(
        entity_name: String,
        storage: &Arc<Storage>,
        filters: Vec<Filter>,
        sort_orders: Vec<SortOrder>,
        max_buffer_size: usize,
        max_sort_buffer: usize,
    ) -> Result<Self> {
        let prefix = format!("data/{entity_name}/");
        let fjall_items = storage.prefix_scan(prefix.as_bytes())?;

        Ok(Self {
            entity_name,
            filters,
            sort_orders,
            buffer: VecDeque::new(),
            sorted_buffer: None,
            sorted_position: 0,
            fjall_items,
            position: 0,
            done: false,
            max_buffer_size,
            max_sort_buffer,
        })
    }

    /// # Errors
    /// Returns an error if reading or deserializing entities fails.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<Value>> {
        if !self.sort_orders.is_empty() {
            if self.sorted_buffer.is_none() {
                self.load_and_sort()?;
            }

            if let Some(ref sorted) = self.sorted_buffer {
                if self.sorted_position < sorted.len() {
                    let result = sorted[self.sorted_position].clone();
                    self.sorted_position += 1;
                    return Ok(Some(result));
                }
                return Ok(None);
            }
        }

        if let Some(entity) = self.buffer.pop_front() {
            return Ok(Some(entity));
        }

        if self.done {
            return Ok(None);
        }

        self.fill_buffer()?;

        Ok(self.buffer.pop_front())
    }

    /// # Errors
    /// Returns an error if reading or deserializing entities fails.
    pub fn next_batch(&mut self, size: usize) -> Result<Vec<Value>> {
        let mut batch = Vec::with_capacity(size);

        while batch.len() < size {
            match self.next()? {
                Some(entity) => batch.push(entity),
                None => break,
            }
        }

        Ok(batch)
    }

    fn fill_buffer(&mut self) -> Result<()> {
        while self.buffer.len() < self.max_buffer_size && self.position < self.fjall_items.len() {
            let (key, value) = &self.fjall_items[self.position];
            self.position += 1;

            let (_, id) = mqdb_core::keys::decode_data_key(key)?;
            let entity = Entity::deserialize(self.entity_name.clone(), id, value)?;
            let entity_data = entity.to_json();

            if self.matches_filters(&entity_data) {
                self.buffer.push_back(entity_data);
            }
        }

        if self.position >= self.fjall_items.len() {
            self.done = true;
        }

        Ok(())
    }

    fn matches_filters(&self, entity: &Value) -> bool {
        use mqdb_core::types::FilterOp;

        for filter in &self.filters {
            let field_value = entity.get(&filter.field);

            let matches = match filter.op {
                FilterOp::Eq => field_value == Some(&filter.value),
                FilterOp::Neq => field_value != Some(&filter.value),
                FilterOp::Lt => {
                    if let (Some(a), Some(b)) = (field_value, Some(&filter.value)) {
                        Self::compare_values(a, b).is_some_and(std::cmp::Ordering::is_lt)
                    } else {
                        false
                    }
                }
                FilterOp::Lte => {
                    if let (Some(a), Some(b)) = (field_value, Some(&filter.value)) {
                        Self::compare_values(a, b).is_some_and(std::cmp::Ordering::is_le)
                    } else {
                        false
                    }
                }
                FilterOp::Gt => {
                    if let (Some(a), Some(b)) = (field_value, Some(&filter.value)) {
                        Self::compare_values(a, b).is_some_and(std::cmp::Ordering::is_gt)
                    } else {
                        false
                    }
                }
                FilterOp::Gte => {
                    if let (Some(a), Some(b)) = (field_value, Some(&filter.value)) {
                        Self::compare_values(a, b).is_some_and(std::cmp::Ordering::is_ge)
                    } else {
                        false
                    }
                }
                FilterOp::In => {
                    if let Some(arr) = filter.value.as_array() {
                        arr.contains(field_value.unwrap_or(&Value::Null))
                    } else {
                        false
                    }
                }
                FilterOp::Like => {
                    if let (Some(Value::String(text)), Some(Value::String(pattern))) =
                        (field_value, Some(&filter.value))
                    {
                        Self::glob_match(text, pattern)
                    } else {
                        false
                    }
                }
                FilterOp::IsNull => field_value.is_none() || field_value == Some(&Value::Null),
                FilterOp::IsNotNull => field_value.is_some() && field_value != Some(&Value::Null),
            };

            if !matches {
                return false;
            }
        }

        true
    }

    fn compare_values(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
        use std::cmp::Ordering;

        match (a, b) {
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            (Value::Bool(a), Value::Bool(b)) => a.partial_cmp(b),
            (Value::Number(a), Value::Number(b)) => {
                let a_f64 = a.as_f64().unwrap_or(0.0);
                let b_f64 = b.as_f64().unwrap_or(0.0);
                a_f64.partial_cmp(&b_f64)
            }
            (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }

    fn glob_match(text: &str, pattern: &str) -> bool {
        if pattern == "*" {
            return true;
        }

        let parts: Vec<&str> = pattern.split('*').collect();

        if parts.len() == 1 {
            return text == pattern;
        }

        let non_empty_parts: Vec<&str> = parts.iter().filter(|p| !p.is_empty()).copied().collect();

        if non_empty_parts.is_empty() {
            return true;
        }

        let starts_with_wildcard = pattern.starts_with('*');
        let ends_with_wildcard = pattern.ends_with('*');

        let mut text_pos = 0;

        for (idx, part) in non_empty_parts.iter().enumerate() {
            let is_first = idx == 0;
            let is_last = idx == non_empty_parts.len() - 1;

            if is_first && !starts_with_wildcard {
                if !text.starts_with(part) {
                    return false;
                }
                text_pos = part.len();
            } else if is_last && !ends_with_wildcard {
                return text[text_pos..].ends_with(part);
            } else if let Some(pos) = text[text_pos..].find(part) {
                text_pos += pos + part.len();
            } else {
                return false;
            }
        }

        true
    }

    fn load_and_sort(&mut self) -> Result<()> {
        let mut all_entities = Vec::new();

        while self.position < self.fjall_items.len() {
            let (key, value) = &self.fjall_items[self.position];
            self.position += 1;

            let (_, id) = mqdb_core::keys::decode_data_key(key)?;
            let entity = Entity::deserialize(self.entity_name.clone(), id, value)?;
            let entity_data = entity.to_json();

            if self.matches_filters(&entity_data) {
                all_entities.push(entity_data);

                if all_entities.len() >= self.max_sort_buffer {
                    tracing::warn!(
                        "cursor reached max_sort_buffer limit of {}, results may be incomplete",
                        self.max_sort_buffer
                    );
                    break;
                }
            }
        }

        self.sort_results(&mut all_entities);
        self.sorted_buffer = Some(all_entities);
        self.done = true;

        Ok(())
    }

    fn sort_results(&self, results: &mut [Value]) {
        results.sort_by(|a, b| {
            for order in &self.sort_orders {
                let a_val = a.get(&order.field);
                let b_val = b.get(&order.field);

                let cmp = match (a_val, b_val) {
                    (Some(av), Some(bv)) => {
                        Self::compare_values(av, bv).unwrap_or(std::cmp::Ordering::Equal)
                    }
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
}
