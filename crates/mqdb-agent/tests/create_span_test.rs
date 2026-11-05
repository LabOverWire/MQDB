// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use mqdb_agent::Database;
use mqdb_core::types::ScopeConfig;
use serde_json::json;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use tracing::Subscriber;
use tracing::span::{Attributes, Id};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

#[derive(Clone, Default)]
struct SpanNames(Arc<Mutex<Vec<&'static str>>>);

impl SpanNames {
    fn take(&self) -> Vec<&'static str> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

impl<S: Subscriber> Layer<S> for SpanNames {
    fn on_new_span(&self, attrs: &Attributes<'_>, _: &Id, _: Context<'_, S>) {
        self.0.lock().unwrap().push(attrs.metadata().name());
    }
}

#[tokio::test]
async fn accepted_refused_and_restored_creates_open_a_span_named_create() {
    let names = SpanNames::default();
    let subscriber = tracing_subscriber::registry().with(names.clone());
    let _guard = tracing::subscriber::set_default(subscriber);
    let tmp = TempDir::new().unwrap();
    let db = Database::open_without_background_tasks(tmp.path().join("db"))
        .await
        .unwrap();
    let scope = ScopeConfig::default();
    names.take();

    db.create(
        "t".to_string(),
        json!({"id": "a"}),
        None,
        None,
        None,
        &scope,
    )
    .await
    .unwrap();
    assert_eq!(names.take(), ["create"], "accepted create");

    db.create(
        "t".to_string(),
        json!({"id": "events"}),
        None,
        None,
        None,
        &scope,
    )
    .await
    .unwrap_err();
    assert_eq!(names.take(), ["create"], "refused create");

    let backup = tmp.path().join("backup.jsonl");
    let line = json!({"_entity": "t", "_data": {"id": "events"}}).to_string();
    std::fs::write(&backup, line).unwrap();
    assert_eq!(db.restore_logical(&backup).await.unwrap(), 1);
    assert_eq!(names.take(), ["create"], "restored record");
}
