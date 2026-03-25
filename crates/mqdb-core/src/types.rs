// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

pub const MAX_LIST_RESULTS: usize = 10_000;
pub const MAX_FILTERS: usize = 16;
pub const MAX_SORT_FIELDS: usize = 4;

#[derive(Debug, Clone, Default)]
pub struct ScopeConfig {
    scope_entity: String,
    scope_field: String,
}

impl ScopeConfig {
    #[must_use]
    pub fn new(scope_entity: String, scope_field: String) -> Self {
        Self {
            scope_entity,
            scope_field,
        }
    }

    /// # Errors
    /// Returns an error if the spec is not in `entity=field` format.
    pub fn parse(spec: &str) -> Result<Self, String> {
        let spec = spec.trim();
        if spec.is_empty() {
            return Ok(Self::default());
        }
        let parts: Vec<&str> = spec.splitn(2, '=').collect();
        if parts.len() != 2 {
            return Err(format!(
                "invalid event-scope spec '{spec}': expected entity=field"
            ));
        }
        Ok(Self {
            scope_entity: parts[0].to_string(),
            scope_field: parts[1].to_string(),
        })
    }

    #[must_use]
    pub fn scope_entity(&self) -> &str {
        &self.scope_entity
    }

    #[must_use]
    pub fn scope_field(&self) -> &str {
        &self.scope_field
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.scope_entity.is_empty()
    }

    #[must_use]
    pub fn resolve_scope(&self, entity_name: &str, data: &Value) -> Option<(String, String)> {
        if self.is_empty() {
            return None;
        }
        if entity_name == self.scope_entity {
            let id = data.get("id").and_then(Value::as_str)?;
            return Some((self.scope_entity.clone(), id.to_string()));
        }
        let scope_value = data.get(&self.scope_field).and_then(Value::as_str)?;
        Some((self.scope_entity.clone(), scope_value.to_string()))
    }
}

#[derive(Debug, Clone, Default)]
pub struct OwnershipConfig {
    pub entity_owner_fields: HashMap<String, String>,
    admin_users: HashSet<String>,
}

impl OwnershipConfig {
    #[must_use]
    pub fn new(entity_owner_fields: HashMap<String, String>) -> Self {
        Self {
            entity_owner_fields,
            admin_users: HashSet::new(),
        }
    }

    /// # Errors
    /// Returns an error if any pair in the spec string is not in `entity=field` format.
    pub fn parse(spec: &str) -> Result<Self, String> {
        let mut fields = HashMap::new();
        for pair in spec.split(',') {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }
            let parts: Vec<&str> = pair.splitn(2, '=').collect();
            if parts.len() != 2 {
                return Err(format!(
                    "invalid ownership spec '{pair}': expected entity=field"
                ));
            }
            fields.insert(parts[0].to_string(), parts[1].to_string());
        }
        Ok(Self {
            entity_owner_fields: fields,
            admin_users: HashSet::new(),
        })
    }

    #[must_use]
    pub fn with_admin_users(mut self, users: HashSet<String>) -> Self {
        self.admin_users = users;
        self
    }

    pub fn add_admin_user(&mut self, user: String) {
        self.admin_users.insert(user);
    }

    #[must_use]
    pub fn owner_field(&self, entity: &str) -> Option<&str> {
        self.entity_owner_fields.get(entity).map(String::as_str)
    }

    #[must_use]
    pub fn is_admin(&self, user: &str) -> bool {
        self.admin_users.contains(user)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entity_owner_fields.is_empty()
    }

    #[must_use]
    pub fn evaluate<'a, 'b>(
        &'a self,
        entity: &str,
        sender: Option<&'b str>,
    ) -> OwnershipDecision<'a, 'b> {
        let Some(uid) = sender else {
            return OwnershipDecision::Allowed;
        };
        if self.is_admin(uid) {
            return OwnershipDecision::Allowed;
        }
        match self.owner_field(entity) {
            Some(field) => OwnershipDecision::Check {
                owner_field: field,
                sender: uid,
            },
            None => OwnershipDecision::Allowed,
        }
    }
}

pub enum OwnershipDecision<'a, 'b> {
    Allowed,
    Check {
        owner_field: &'a str,
        sender: &'b str,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortOrder {
    pub field: String,
    pub direction: SortDirection,
}

impl SortOrder {
    #[allow(clippy::must_use_candidate)]
    pub fn new(field: String, direction: SortDirection) -> Self {
        Self { field, direction }
    }

    #[must_use]
    pub fn asc(field: String) -> Self {
        Self::new(field, SortDirection::Asc)
    }

    #[must_use]
    pub fn desc(field: String) -> Self {
        Self::new(field, SortDirection::Desc)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pagination {
    pub limit: usize,
    pub offset: usize,
}

impl Pagination {
    #[allow(clippy::must_use_candidate)]
    pub fn new(limit: usize, offset: usize) -> Self {
        Self { limit, offset }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterOp {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    In,
    Like,
    IsNull,
    IsNotNull,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Filter {
    pub field: String,
    pub op: FilterOp,
    pub value: Value,
}

impl Filter {
    #[allow(clippy::must_use_candidate)]
    pub fn new(field: String, op: FilterOp, value: Value) -> Self {
        Self { field, op, value }
    }

    #[must_use]
    pub fn matches(&self, field_value: &Value) -> bool {
        match self.op {
            FilterOp::Eq => field_value == &self.value,
            FilterOp::Neq => field_value != &self.value,
            FilterOp::Lt => {
                Self::compare_values(field_value, &self.value) == Some(std::cmp::Ordering::Less)
            }
            FilterOp::Lte => matches!(
                Self::compare_values(field_value, &self.value),
                Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
            ),
            FilterOp::Gt => {
                Self::compare_values(field_value, &self.value) == Some(std::cmp::Ordering::Greater)
            }
            FilterOp::Gte => matches!(
                Self::compare_values(field_value, &self.value),
                Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
            ),
            FilterOp::In => {
                if let Value::Array(values) = &self.value {
                    values.contains(field_value)
                } else {
                    false
                }
            }
            FilterOp::Like => {
                if let (Value::String(field_str), Value::String(pattern)) =
                    (field_value, &self.value)
                {
                    Self::glob_match(field_str, pattern)
                } else {
                    false
                }
            }
            FilterOp::IsNull => field_value.is_null(),
            FilterOp::IsNotNull => !field_value.is_null(),
        }
    }

    fn compare_values(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
        match (a, b) {
            (Value::Number(a), Value::Number(b)) => {
                let a_f64 = a.as_f64()?;
                let b_f64 = b.as_f64()?;
                a_f64.partial_cmp(&b_f64)
            }
            (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }

    #[must_use]
    pub fn glob_match(text: &str, pattern: &str) -> bool {
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
}

#[must_use]
pub fn project_fields(entity: Value, fields: &[String]) -> Value {
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_filter_neq_deserialization() {
        let filter_json = json!({"field": "category", "op": "neq", "value": "furniture"});
        let filter: Filter = serde_json::from_value(filter_json).expect("should deserialize");
        assert_eq!(filter.field, "category");
        assert_eq!(filter.op, FilterOp::Neq);
        assert_eq!(filter.value, json!("furniture"));
    }

    #[test]
    fn test_filter_neq_matches() {
        let filter = Filter::new("category".to_string(), FilterOp::Neq, json!("furniture"));
        assert!(filter.matches(&json!("electronics")));
        assert!(!filter.matches(&json!("furniture")));
    }
}
