use serde::{Deserialize, Serialize};
use serde_json::Value;

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
