// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use mqdb_agent::Database;
use mqdb_core::types::{OwnershipConfig, ScopeConfig};
use serde_json::json;
use std::path::Path;
use tempfile::TempDir;

async fn create_user(db: &Database, id: &str) {
    db.create(
        "users".into(),
        json!({"id": id, "name": id}),
        None,
        None,
        None,
        &ScopeConfig::default(),
    )
    .await
    .unwrap();
}

async fn user_exists(db: &Database, id: &str) -> bool {
    db.read("users".into(), id.into(), vec![], None)
        .await
        .is_ok()
}

async fn backed_up_database(db_path: &Path, name: &str) -> Database {
    let db = Database::open_without_background_tasks(db_path)
        .await
        .unwrap();
    create_user(&db, "alice").await;
    db.backup(db_path.join("backups").join(name)).unwrap();
    db
}

#[tokio::test]
async fn a_backup_into_the_database_backups_dir_does_not_copy_the_backups_dir() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    let db = backed_up_database(&db_path, "first").await;

    db.backup(db_path.join("backups").join("second")).unwrap();

    let second = db_path.join("backups").join("second");
    assert!(second.join("version").is_file());
    assert!(!second.join("backups").exists());
}

#[tokio::test]
async fn restore_brings_back_the_backed_up_data_and_keeps_every_backup() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    let db = backed_up_database(&db_path, "snap").await;
    db.backup(db_path.join("backups").join("other")).unwrap();
    db.delete(
        "users".into(),
        "alice".into(),
        None,
        None,
        &ScopeConfig::default(),
        &OwnershipConfig::default(),
    )
    .await
    .unwrap();
    create_user(&db, "carol").await;
    drop(db);

    let restored = Database::restore_backup(&db_path, "snap").unwrap();
    assert!(restored > 0);

    let db = Database::open_without_background_tasks(&db_path)
        .await
        .unwrap();
    assert!(user_exists(&db, "alice").await);
    assert!(!user_exists(&db, "carol").await);
    assert!(
        db_path
            .join("backups")
            .join("snap")
            .join("version")
            .is_file()
    );
    assert!(
        db_path
            .join("backups")
            .join("other")
            .join("version")
            .is_file()
    );
    assert!(!db_path.join(".restore-staging").exists());
}

#[tokio::test]
async fn restore_refuses_a_database_that_is_open() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    let db = backed_up_database(&db_path, "snap").await;
    create_user(&db, "carol").await;

    let err = Database::restore_backup(&db_path, "snap").unwrap_err();
    assert!(err.to_string().contains("in use"), "{err}");
    assert!(user_exists(&db, "carol").await);
}

#[tokio::test]
async fn restore_refuses_a_missing_backup_and_an_invalid_name() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    drop(backed_up_database(&db_path, "snap").await);

    let missing = Database::restore_backup(&db_path, "nope").unwrap_err();
    assert!(
        missing.to_string().contains("no database backup"),
        "{missing}"
    );
    let invalid = Database::restore_backup(&db_path, "../snap").unwrap_err();
    assert!(
        invalid.to_string().contains("invalid backup name"),
        "{invalid}"
    );
}

#[tokio::test]
async fn restore_refuses_a_cluster_node_database() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    drop(backed_up_database(&db_path, "snap").await);
    std::fs::create_dir(db_path.join("raft")).unwrap();

    let err = Database::restore_backup(&db_path, "snap").unwrap_err();
    assert!(err.to_string().contains("cluster node"), "{err}");
    assert!(db_path.join("raft").is_dir());
}

#[tokio::test]
async fn an_interrupted_restore_blocks_opening_until_restore_runs_again() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    drop(backed_up_database(&db_path, "snap").await);
    for entry in std::fs::read_dir(&db_path).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        if name == "lock" || name == "backups" {
            continue;
        }
        if entry.file_type().unwrap().is_dir() {
            std::fs::remove_dir_all(entry.path()).unwrap();
        } else {
            std::fs::remove_file(entry.path()).unwrap();
        }
    }
    std::fs::create_dir(db_path.join(".restore-staging")).unwrap();

    let refused = Database::open_without_background_tasks(&db_path).await;
    let message = refused.err().map(|e| e.to_string()).unwrap_or_default();
    assert!(message.contains("did not finish"), "{message}");

    Database::restore_backup(&db_path, "snap").unwrap();
    let db = Database::open_without_background_tasks(&db_path)
        .await
        .unwrap();
    assert!(user_exists(&db, "alice").await);
}
