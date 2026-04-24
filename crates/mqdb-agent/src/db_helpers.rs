// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::database::{CallerContext, Database};
use mqdb_core::types::ScopeConfig;
use mqdb_core::{Filter, FilterOp};
use serde_json::Value;

pub async fn create_entity_db(db: &Database, entity: &str, data: &Value) -> bool {
    let scope = ScopeConfig::default();
    db.create(entity.to_string(), data.clone(), None, None, None, &scope)
        .await
        .is_ok()
}

pub async fn read_entity_db(db: &Database, entity: &str, id: &str) -> Option<Value> {
    db.read(entity.to_string(), id.to_string(), vec![], None)
        .await
        .ok()
}

pub async fn update_entity_db(db: &Database, entity: &str, id: &str, data: &Value) -> bool {
    let scope = ScopeConfig::default();
    let caller = CallerContext {
        sender: None,
        client_id: None,
        scope_config: &scope,
    };
    db.update(
        entity.to_string(),
        id.to_string(),
        data.clone(),
        None,
        &caller,
    )
    .await
    .is_ok()
}

pub async fn list_entities_db(db: &Database, entity: &str, filter: &str) -> Option<Vec<Value>> {
    let filters = if let Some((field, value)) = filter.split_once('=') {
        vec![Filter::new(
            field.to_string(),
            FilterOp::Eq,
            Value::String(value.to_string()),
        )]
    } else {
        vec![]
    };
    db.list(entity.to_string(), filters, vec![], None, vec![], None)
        .await
        .ok()
}
