// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::error::Result;
use crate::keys;
use crate::types::{Filter, FilterOp, SortDirection, SortOrder};
use serde_json::Value;
use std::cmp::Ordering;

#[must_use]
pub fn compare_json_values(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Number(a_num), Value::Number(b_num)) => {
            let a_f64 = a_num.as_f64().unwrap_or(0.0);
            let b_f64 = b_num.as_f64().unwrap_or(0.0);
            a_f64.partial_cmp(&b_f64).unwrap_or(Ordering::Equal)
        }
        (Value::String(a_str), Value::String(b_str)) => a_str.cmp(b_str),
        (Value::Bool(a_bool), Value::Bool(b_bool)) => a_bool.cmp(b_bool),
        _ => Ordering::Equal,
    }
}

pub fn sort_results(results: &mut [Value], sort: &[SortOrder]) {
    results.sort_by(|a, b| {
        for order in sort {
            let a_val = a.get(&order.field);
            let b_val = b.get(&order.field);

            let cmp = match (a_val, b_val) {
                (Some(av), Some(bv)) => compare_json_values(av, bv),
                (Some(_), None) => Ordering::Greater,
                (None, Some(_)) => Ordering::Less,
                (None, None) => Ordering::Equal,
            };

            let cmp = match order.direction {
                SortDirection::Asc => cmp,
                SortDirection::Desc => cmp.reverse(),
            };

            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    });
}

#[must_use]
pub fn matches_all_filters(record: &Value, filters: &[Filter]) -> bool {
    filters.iter().all(|f| f.matches_document(record))
}

pub type RangeBound = (Vec<u8>, bool);

/// # Errors
/// Returns an error if a filter value cannot be encoded for index lookup.
pub fn collect_range_bounds(
    filters: &[Filter],
    field: &str,
) -> Result<(Option<RangeBound>, Option<RangeBound>, Vec<usize>)> {
    let mut lower: Option<RangeBound> = None;
    let mut upper: Option<RangeBound> = None;
    let mut consumed_indices = Vec::new();

    for (i, filter) in filters.iter().enumerate() {
        if filter.field != field {
            continue;
        }
        match filter.op {
            FilterOp::Gt => {
                lower = Some((keys::encode_value_for_index(&filter.value)?, false));
                consumed_indices.push(i);
            }
            FilterOp::Gte => {
                lower = Some((keys::encode_value_for_index(&filter.value)?, true));
                consumed_indices.push(i);
            }
            FilterOp::Lt => {
                upper = Some((keys::encode_value_for_index(&filter.value)?, false));
                consumed_indices.push(i);
            }
            FilterOp::Lte => {
                upper = Some((keys::encode_value_for_index(&filter.value)?, true));
                consumed_indices.push(i);
            }
            _ => {}
        }
    }

    Ok((lower, upper, consumed_indices))
}

#[must_use]
pub fn build_range_keys(
    entity: &str,
    field: &str,
    lower: Option<(&[u8], bool)>,
    upper: Option<(&[u8], bool)>,
    prefix: &str,
) -> (Vec<u8>, Vec<u8>) {
    let field_prefix = format!("{prefix}/{entity}/{field}");
    let field_prefix_bytes = field_prefix.as_bytes();

    let start = if let Some((value, inclusive)) = lower {
        let mut key = Vec::with_capacity(field_prefix_bytes.len() + 1 + value.len() + 2);
        key.extend_from_slice(field_prefix_bytes);
        key.push(b'/');
        key.extend_from_slice(value);
        key.push(b'/');
        if !inclusive {
            key.push(0xFF);
        }
        key
    } else {
        let mut key = Vec::with_capacity(field_prefix_bytes.len() + 1);
        key.extend_from_slice(field_prefix_bytes);
        key.push(b'/');
        key
    };

    let end = if let Some((value, inclusive)) = upper {
        let mut key = Vec::with_capacity(field_prefix_bytes.len() + 1 + value.len() + 2);
        key.extend_from_slice(field_prefix_bytes);
        key.push(b'/');
        key.extend_from_slice(value);
        key.push(b'/');
        if inclusive {
            key.push(0xFF);
        }
        key
    } else {
        let mut key = Vec::with_capacity(field_prefix_bytes.len() + 1);
        key.extend_from_slice(field_prefix_bytes);
        key.push(0xFF);
        key
    };

    (start, end)
}

#[must_use]
pub fn extract_ids_from_index_keys(entries: &[(Vec<u8>, Vec<u8>)]) -> Vec<String> {
    let mut ids = Vec::new();
    for (key, _) in entries {
        if let Some(id) = extract_id_from_index_key(key) {
            ids.push(id);
        }
    }
    ids
}

#[must_use]
pub fn extract_id_from_index_key(key: &[u8]) -> Option<String> {
    let last_slash = key.iter().rposition(|&b| b == b'/')?;
    String::from_utf8(key[last_slash + 1..].to_vec()).ok()
}

#[must_use]
pub fn extract_id_from_data_key(key: &[u8]) -> Option<String> {
    let key_str = std::str::from_utf8(key).ok()?;
    let rest = key_str.strip_prefix("data/")?;
    let slash_pos = rest.find('/')?;
    Some(rest[slash_pos + 1..].to_string())
}

/// # Errors
/// Returns an error if a field value cannot be encoded for index storage.
pub fn build_index_key(
    entity: &str,
    id: &str,
    fields: &[String],
    value: &Value,
    prefix: &str,
) -> Result<Option<Vec<u8>>> {
    let mut encoded_values = Vec::new();
    for field in fields {
        let Some(v) = value.get(field) else {
            return Ok(None);
        };
        let encoded = keys::encode_value_for_index(v)?;
        if !encoded_values.is_empty() {
            encoded_values.push(0x00);
        }
        encoded_values.extend_from_slice(&encoded);
    }

    let prefix_str = format!("{prefix}/{entity}/{}/", fields.join("_"));
    let mut key = Vec::with_capacity(prefix_str.len() + encoded_values.len() + 1 + id.len());
    key.extend_from_slice(prefix_str.as_bytes());
    key.extend_from_slice(&encoded_values);
    key.push(b'/');
    key.extend_from_slice(id.as_bytes());
    Ok(Some(key))
}

/// # Errors
/// Returns an error if a field value cannot be encoded for index storage.
pub fn build_unique_index_prefix(
    entity: &str,
    fields: &[String],
    value: &Value,
    prefix: &str,
) -> Result<Option<Vec<u8>>> {
    let mut encoded_values = Vec::new();
    for field in fields {
        let Some(v) = value.get(field) else {
            return Ok(None);
        };
        let encoded = keys::encode_value_for_index(v)?;
        if !encoded_values.is_empty() {
            encoded_values.push(0x00);
        }
        encoded_values.extend_from_slice(&encoded);
    }

    let prefix_str = format!("{prefix}/{entity}/{}/", fields.join("_"));
    let mut result = Vec::with_capacity(prefix_str.len() + encoded_values.len() + 1);
    result.extend_from_slice(prefix_str.as_bytes());
    result.extend_from_slice(&encoded_values);
    result.push(b'/');
    Ok(Some(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn compare_numbers() {
        assert_eq!(compare_json_values(&json!(1), &json!(2)), Ordering::Less);
        assert_eq!(compare_json_values(&json!(2), &json!(1)), Ordering::Greater);
        assert_eq!(compare_json_values(&json!(1), &json!(1)), Ordering::Equal);
    }

    #[test]
    fn compare_strings() {
        assert_eq!(
            compare_json_values(&json!("a"), &json!("b")),
            Ordering::Less
        );
        assert_eq!(
            compare_json_values(&json!("b"), &json!("a")),
            Ordering::Greater
        );
    }

    #[test]
    fn compare_bools() {
        assert_eq!(
            compare_json_values(&json!(false), &json!(true)),
            Ordering::Less
        );
    }

    #[test]
    fn sort_by_number_asc() {
        let mut results = vec![json!({"n": 3}), json!({"n": 1}), json!({"n": 2})];
        sort_results(&mut results, &[SortOrder::asc("n".into())]);
        assert_eq!(results[0]["n"], 1);
        assert_eq!(results[1]["n"], 2);
        assert_eq!(results[2]["n"], 3);
    }

    #[test]
    fn sort_by_string_desc() {
        let mut results = vec![json!({"s": "a"}), json!({"s": "c"}), json!({"s": "b"})];
        sort_results(&mut results, &[SortOrder::desc("s".into())]);
        assert_eq!(results[0]["s"], "c");
        assert_eq!(results[1]["s"], "b");
        assert_eq!(results[2]["s"], "a");
    }

    #[test]
    fn matches_all_filters_passes() {
        let record = json!({"name": "alice", "age": 30});
        let filters = vec![
            Filter::new("name".into(), FilterOp::Eq, json!("alice")),
            Filter::new("age".into(), FilterOp::Gte, json!(18)),
        ];
        assert!(matches_all_filters(&record, &filters));
    }

    #[test]
    fn matches_all_filters_fails() {
        let record = json!({"name": "alice", "age": 16});
        let filters = vec![
            Filter::new("name".into(), FilterOp::Eq, json!("alice")),
            Filter::new("age".into(), FilterOp::Gte, json!(18)),
        ];
        assert!(!matches_all_filters(&record, &filters));
    }

    #[test]
    fn extract_id_from_data_key_works() {
        assert_eq!(
            extract_id_from_data_key(b"data/users/abc123"),
            Some("abc123".into())
        );
        assert_eq!(extract_id_from_data_key(b"other/key"), None);
    }

    #[test]
    fn extract_id_from_index_key_works() {
        assert_eq!(
            extract_id_from_index_key(b"idx/users/email/test@example.com/abc123"),
            Some("abc123".into())
        );
    }

    #[test]
    fn build_index_key_with_prefix() {
        let key = build_index_key(
            "users",
            "abc",
            &["email".into()],
            &json!({"email": "test@x.com"}),
            "index",
        )
        .unwrap()
        .unwrap();
        assert_eq!(key, b"index/users/email/test@x.com/abc");
    }

    #[test]
    fn build_index_key_missing_field() {
        let key = build_index_key(
            "users",
            "abc",
            &["missing".into()],
            &json!({"email": "test@x.com"}),
            "index",
        )
        .unwrap();
        assert!(key.is_none());
    }

    #[test]
    fn build_unique_index_prefix_works() {
        let prefix = build_unique_index_prefix(
            "users",
            &["email".into()],
            &json!({"email": "test@x.com"}),
            "index",
        )
        .unwrap()
        .unwrap();
        assert_eq!(prefix, b"index/users/email/test@x.com/");
    }

    #[test]
    fn build_range_keys_bounded() {
        let (start, end) = build_range_keys(
            "users",
            "age",
            Some((b"18", true)),
            Some((b"65", false)),
            "index",
        );
        assert!(start.starts_with(b"index/users/age/"));
        assert!(end.starts_with(b"index/users/age/"));
    }

    #[test]
    fn build_range_keys_unbounded() {
        let (start, end) = build_range_keys("users", "age", None, None, "idx");
        assert!(start.starts_with(b"idx/users/age/"));
        assert!(end.starts_with(b"idx/users/age"));
        assert_eq!(*end.last().unwrap(), 0xFF);
    }

    #[test]
    fn extract_ids_from_index_keys_works() {
        let entries = vec![
            (b"index/users/email/a@b.com/id1".to_vec(), vec![]),
            (b"index/users/email/c@d.com/id2".to_vec(), vec![]),
        ];
        let ids = extract_ids_from_index_keys(&entries);
        assert_eq!(ids, vec!["id1", "id2"]);
    }

    #[test]
    fn matches_document_is_null() {
        let filter = Filter::new("x".into(), FilterOp::IsNull, json!(null));
        assert!(filter.matches_document(&json!({"y": 1})));
        assert!(filter.matches_document(&json!({"x": null})));
        assert!(!filter.matches_document(&json!({"x": 42})));
    }

    #[test]
    fn matches_document_is_not_null() {
        let filter = Filter::new("x".into(), FilterOp::IsNotNull, json!(null));
        assert!(!filter.matches_document(&json!({"y": 1})));
        assert!(!filter.matches_document(&json!({"x": null})));
        assert!(filter.matches_document(&json!({"x": 42})));
    }
}
