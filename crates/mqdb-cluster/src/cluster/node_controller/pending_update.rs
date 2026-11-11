// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use serde_json::{Map, Value};

const VERSION_FIELD: &str = "_version";

pub struct PendingUpdate {
    fields: Map<String, Value>,
    read_fields: Map<String, Value>,
    expected_version: Option<u64>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum UpdateRefusal {
    NotFound,
    VersionMoved { expected: u64, found: u64 },
    FieldChanged(String),
}

impl UpdateRefusal {
    #[must_use]
    pub(crate) fn code(&self) -> u16 {
        match self {
            Self::NotFound => 404,
            Self::VersionMoved { .. } => 412,
            Self::FieldChanged(_) => 409,
        }
    }

    #[must_use]
    pub(crate) fn message(&self, entity: &str, id: &str) -> String {
        match self {
            Self::NotFound => format!("entity not found: {entity} id={id}"),
            Self::VersionMoved { expected, found } => {
                format!("{entity}/{id}: expected _version {expected}, found {found}")
            }
            Self::FieldChanged(field) => {
                format!("{entity}/{id}: {field} was changed by another write, retry the update")
            }
        }
    }
}

fn record_version(record: &Value) -> u64 {
    record
        .get(VERSION_FIELD)
        .and_then(Value::as_u64)
        .unwrap_or(0)
}

impl PendingUpdate {
    #[must_use]
    pub(crate) fn new(read: &Value, updates: Value, expected_version: Option<u64>) -> Self {
        let mut fields = match updates {
            Value::Object(fields) => fields,
            _ => Map::new(),
        };
        fields.remove(VERSION_FIELD);
        let read_fields = fields
            .keys()
            .filter_map(|field| read.get(field).map(|value| (field.clone(), value.clone())))
            .collect();
        Self {
            fields,
            read_fields,
            expected_version,
        }
    }

    pub(crate) fn apply_to(&self, current: Option<Value>) -> Result<Value, UpdateRefusal> {
        let mut record = current.ok_or(UpdateRefusal::NotFound)?;
        let found = record_version(&record);
        if let Some(expected) = self.expected_version
            && expected != found
        {
            return Err(UpdateRefusal::VersionMoved { expected, found });
        }
        if let Some(field) = self
            .fields
            .keys()
            .find(|field| record.get(field.as_str()) != self.read_fields.get(field.as_str()))
        {
            return Err(UpdateRefusal::FieldChanged(field.clone()));
        }
        if let Value::Object(ref mut stored) = record {
            for (field, value) in &self.fields {
                stored.insert(field.clone(), value.clone());
            }
            stored.insert(VERSION_FIELD.to_string(), Value::from(found + 1));
        }
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn update(read: &Value, updates: Value, expected: Option<u64>) -> PendingUpdate {
        PendingUpdate::new(read, updates, expected)
    }

    #[test]
    fn writes_its_fields_onto_the_current_record_past_its_version() {
        let read = json!({"a": 1, "b": 1, "_version": 1});
        let current = json!({"a": 1, "b": 2, "_version": 2});

        let merged = update(&read, json!({"a": 5}), None).apply_to(Some(current));

        assert_eq!(merged, Ok(json!({"a": 5, "b": 2, "_version": 3})));
    }

    #[test]
    fn refuses_when_a_field_it_sets_was_changed() {
        let read = json!({"a": 1, "_version": 1});
        let current = json!({"a": 2, "_version": 2});

        let merged = update(&read, json!({"a": 5}), None).apply_to(Some(current));

        assert_eq!(merged, Err(UpdateRefusal::FieldChanged("a".to_string())));
    }

    #[test]
    fn refuses_when_a_field_it_adds_appeared() {
        let read = json!({"_version": 1});
        let current = json!({"a": 2, "_version": 2});

        let merged = update(&read, json!({"a": 5}), None).apply_to(Some(current));

        assert_eq!(merged, Err(UpdateRefusal::FieldChanged("a".to_string())));
    }

    #[test]
    fn refuses_a_moved_version_it_expected() {
        let read = json!({"a": 1, "_version": 1});
        let current = json!({"a": 1, "b": 2, "_version": 2});

        let merged = update(&read, json!({"a": 5}), Some(1)).apply_to(Some(current));

        assert_eq!(
            merged,
            Err(UpdateRefusal::VersionMoved {
                expected: 1,
                found: 2
            })
        );
    }

    #[test]
    fn refuses_a_missing_record() {
        let read = json!({"a": 1, "_version": 1});

        let merged = update(&read, json!({"a": 5}), None).apply_to(None);

        assert_eq!(merged, Err(UpdateRefusal::NotFound));
    }

    #[test]
    fn ignores_a_version_sent_as_a_field() {
        let read = json!({"a": 1, "_version": 1});
        let current = json!({"a": 1, "_version": 4});

        let merged = update(&read, json!({"a": 5, "_version": 99}), None).apply_to(Some(current));

        assert_eq!(merged, Ok(json!({"a": 5, "_version": 5})));
    }
}
