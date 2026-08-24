// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use mqdb_agent::Database;
use mqdb_core::SubscriptionMode;
use mqdb_core::config::{DatabaseConfig, SharedSubscriptionConfig};
use mqdb_core::types::OwnershipConfig;
use tempfile::tempdir;

async fn setup_db() -> Database {
    let dir = tempdir().unwrap();
    let config = DatabaseConfig::new(dir.path().to_path_buf()).with_shared_subscription(
        SharedSubscriptionConfig {
            num_partitions: 8,
            consumer_timeout_ms: 30_000,
        },
    );
    Database::open_with_config(config).await.unwrap()
}

#[tokio::test]
async fn test_subscribe_shared_ordered_returns_partitions() {
    let db = setup_db().await;

    let result = db
        .subscribe_shared(
            "orders/#".into(),
            Some("orders".into()),
            "order-processors".into(),
            SubscriptionMode::Ordered,
            None,
        )
        .await
        .unwrap();

    assert!(!result.id.is_empty());
    assert!(result.assigned_partitions.is_some());
    let partitions = result.assigned_partitions.unwrap();
    assert_eq!(partitions.len(), 8);

    db.shutdown();
}

#[tokio::test]
async fn test_subscribe_shared_load_balanced_no_partitions() {
    let db = setup_db().await;

    let result = db
        .subscribe_shared(
            "orders/#".into(),
            Some("orders".into()),
            "workers".into(),
            SubscriptionMode::LoadBalanced,
            None,
        )
        .await
        .unwrap();

    assert!(!result.id.is_empty());
    assert!(result.assigned_partitions.is_none());

    db.shutdown();
}

#[tokio::test]
async fn test_mixed_mode_rejected() {
    let db = setup_db().await;

    let r1 = db
        .subscribe_shared(
            "orders/#".into(),
            None,
            "workers".into(),
            SubscriptionMode::LoadBalanced,
            None,
        )
        .await
        .unwrap();

    let result = db
        .subscribe_shared(
            "orders/#".into(),
            None,
            "workers".into(),
            SubscriptionMode::Ordered,
            None,
        )
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("already uses"));

    db.unsubscribe(&r1.id, None, &OwnershipConfig::default())
        .await
        .unwrap();
    db.shutdown();
}

#[tokio::test]
async fn test_unsubscribe_cleans_consumer_group() {
    let db = setup_db().await;

    let r1 = db
        .subscribe_shared(
            "orders/#".into(),
            None,
            "workers".into(),
            SubscriptionMode::Ordered,
            None,
        )
        .await
        .unwrap();

    assert!(r1.assigned_partitions.is_some());
    assert_eq!(r1.assigned_partitions.unwrap().len(), 8);

    db.unsubscribe(&r1.id, None, &OwnershipConfig::default())
        .await
        .unwrap();

    let r2 = db
        .subscribe_shared(
            "orders/#".into(),
            None,
            "workers".into(),
            SubscriptionMode::Ordered,
            None,
        )
        .await
        .unwrap();

    assert!(r2.assigned_partitions.is_some());
    assert_eq!(r2.assigned_partitions.unwrap().len(), 8);

    db.shutdown();
}

#[tokio::test]
async fn test_ordered_partition_rebalance() {
    let db = setup_db().await;

    let r1 = db
        .subscribe_shared(
            "orders/#".into(),
            None,
            "processors".into(),
            SubscriptionMode::Ordered,
            None,
        )
        .await
        .unwrap();

    assert_eq!(r1.assigned_partitions.unwrap().len(), 8);

    let r2 = db
        .subscribe_shared(
            "orders/#".into(),
            None,
            "processors".into(),
            SubscriptionMode::Ordered,
            None,
        )
        .await
        .unwrap();

    assert_eq!(r2.assigned_partitions.unwrap().len(), 4);

    db.unsubscribe(&r1.id, None, &OwnershipConfig::default())
        .await
        .unwrap();
    db.unsubscribe(&r2.id, None, &OwnershipConfig::default())
        .await
        .unwrap();
    db.shutdown();
}

#[tokio::test]
async fn test_heartbeat_api() {
    let db = setup_db().await;

    let r1 = db
        .subscribe_shared(
            "orders/#".into(),
            None,
            "workers".into(),
            SubscriptionMode::Ordered,
            None,
        )
        .await
        .unwrap();

    let heartbeat_result = db
        .heartbeat(&r1.id, None, &OwnershipConfig::default())
        .await;
    assert!(heartbeat_result.is_ok());

    let invalid_heartbeat = db
        .heartbeat("non-existent-id", None, &OwnershipConfig::default())
        .await;
    assert!(invalid_heartbeat.is_err());

    db.shutdown();
}

#[tokio::test]
async fn test_same_mode_allowed_in_group() {
    let db = setup_db().await;

    let r1 = db
        .subscribe_shared(
            "orders/#".into(),
            None,
            "lb-workers".into(),
            SubscriptionMode::LoadBalanced,
            None,
        )
        .await
        .unwrap();

    let r2 = db
        .subscribe_shared(
            "orders/#".into(),
            None,
            "lb-workers".into(),
            SubscriptionMode::LoadBalanced,
            None,
        )
        .await;

    assert!(r2.is_ok());

    db.unsubscribe(&r1.id, None, &OwnershipConfig::default())
        .await
        .unwrap();
    db.unsubscribe(&r2.unwrap().id, None, &OwnershipConfig::default())
        .await
        .unwrap();
    db.shutdown();
}

#[tokio::test]
async fn test_subscription_ownership_blocks_cross_user_control() {
    let db = setup_db().await;
    let no_admins = OwnershipConfig::default();
    let mut carol_admin = OwnershipConfig::default();
    carol_admin.add_admin_user("carol".to_string());

    let sub_id = db
        .subscribe("orders/#".into(), Some("orders".into()), Some("alice"))
        .await
        .unwrap();

    db.unsubscribe(&sub_id, Some("bob"), &no_admins)
        .await
        .unwrap();
    assert!(
        db.heartbeat(&sub_id, Some("bob"), &no_admins)
            .await
            .is_err(),
        "heartbeat by a non-owner must report the subscription as not found"
    );
    assert!(
        db.get_subscription_info(&sub_id).await.is_some(),
        "a non-owner must not be able to cancel another user's subscription"
    );

    db.heartbeat(&sub_id, Some("carol"), &carol_admin)
        .await
        .unwrap();

    db.unsubscribe(&sub_id, Some("alice"), &no_admins)
        .await
        .unwrap();
    assert!(db.get_subscription_info(&sub_id).await.is_none());

    db.shutdown();
}

#[tokio::test]
async fn test_ownerless_subscription_is_not_ownership_restricted() {
    let db = setup_db().await;
    let no_admins = OwnershipConfig::default();

    let sub_id = db
        .subscribe("orders/#".into(), Some("orders".into()), None)
        .await
        .unwrap();

    db.heartbeat(&sub_id, Some("bob"), &no_admins)
        .await
        .unwrap();
    db.unsubscribe(&sub_id, Some("bob"), &no_admins)
        .await
        .unwrap();
    assert!(db.get_subscription_info(&sub_id).await.is_none());

    db.shutdown();
}
