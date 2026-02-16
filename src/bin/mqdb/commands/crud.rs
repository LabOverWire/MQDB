// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::cli_types::{ConnectionArgs, OutputFormat, SubscriptionModeArg};
use crate::common::{
    check_response_status, connect_client, execute_request, matches_filters, output_response,
    parse_filters,
};
use serde_json::{Value, json};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

pub(crate) async fn cmd_create(
    entity: String,
    data: String,
    conn: ConnectionArgs,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let payload: Value = serde_json::from_str(&data)?;
    let topic = format!("$DB/{entity}/create");
    let response = Box::pin(execute_request(&conn, &topic, payload)).await?;
    output_response(&response, &format);
    check_response_status(&response)?;
    Ok(())
}

pub(crate) async fn cmd_read(
    entity: String,
    id: String,
    projection: Option<String>,
    conn: ConnectionArgs,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let topic = format!("$DB/{entity}/{id}");
    let payload = if let Some(proj) = projection {
        let fields: Vec<&str> = proj.split(',').collect();
        json!({ "projection": fields })
    } else {
        json!({})
    };
    let response = Box::pin(execute_request(&conn, &topic, payload)).await?;
    output_response(&response, &format);
    check_response_status(&response)?;
    Ok(())
}

pub(crate) async fn cmd_update(
    entity: String,
    id: String,
    data: String,
    conn: ConnectionArgs,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let payload: Value = serde_json::from_str(&data)?;
    let topic = format!("$DB/{entity}/{id}/update");
    let response = Box::pin(execute_request(&conn, &topic, payload)).await?;
    output_response(&response, &format);
    check_response_status(&response)?;
    Ok(())
}

pub(crate) async fn cmd_delete(
    entity: String,
    id: String,
    conn: ConnectionArgs,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let topic = format!("$DB/{entity}/{id}/delete");
    let response = Box::pin(execute_request(&conn, &topic, json!({}))).await?;
    output_response(&response, &format);
    check_response_status(&response)?;
    Ok(())
}

pub(crate) async fn cmd_list(
    entity: String,
    filters: Vec<String>,
    sort: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
    conn: ConnectionArgs,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let topic = format!("$DB/{entity}/list");

    let mut payload = json!({});

    if !filters.is_empty() {
        let parsed_filters: Vec<Value> = filters.iter().flat_map(|f| parse_filters(f)).collect();
        payload["filters"] = json!(parsed_filters);
    }

    if let Some(sort_str) = sort {
        let sort_orders: Vec<Value> = sort_str
            .split(',')
            .map(|s| {
                let parts: Vec<&str> = s.split(':').collect();
                let field = parts[0];
                let direction = parts.get(1).unwrap_or(&"asc");
                json!({ "field": field, "direction": direction })
            })
            .collect();
        payload["sort"] = json!(sort_orders);
    }

    if limit.is_some() || offset.is_some() {
        payload["pagination"] = json!({
            "limit": limit.unwrap_or(100),
            "offset": offset.unwrap_or(0)
        });
    }

    let response = Box::pin(execute_request(&conn, &topic, payload)).await?;
    output_response(&response, &format);
    Ok(())
}

pub(crate) async fn cmd_watch(
    entity: String,
    filters: Vec<String>,
    conn: ConnectionArgs,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let parsed_filters: Vec<Value> = filters.iter().flat_map(|f| parse_filters(f)).collect();

    let client = Box::pin(connect_client(&conn)).await?;
    let topic = format!("$DB/{entity}/events/#");

    let (tx, rx) = flume::bounded::<Value>(100);

    client
        .subscribe(&topic, move |msg| {
            if let Ok(value) = serde_json::from_slice::<Value>(&msg.payload) {
                let _ = tx.try_send(value);
            }
        })
        .await?;

    eprintln!("Watching {entity} events (Ctrl+C to stop)...");

    while let Ok(event) = rx.recv_async().await {
        if !parsed_filters.is_empty() {
            let data = event.get("data").unwrap_or(&event);
            if !matches_filters(data, &parsed_filters) {
                continue;
            }
        }
        output_response(&event, &format);
    }

    Ok(())
}

pub(crate) async fn cmd_schema_set(
    entity: String,
    file: PathBuf,
    conn: ConnectionArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let content = tokio::fs::read_to_string(&file).await?;
    let schema: Value = serde_json::from_str(&content)?;
    let topic = format!("$DB/_admin/schema/{entity}/set");
    let response = Box::pin(execute_request(&conn, &topic, schema)).await?;
    output_response(&response, &OutputFormat::Json);
    Ok(())
}

pub(crate) async fn cmd_schema_get(
    entity: String,
    conn: ConnectionArgs,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let topic = format!("$DB/_admin/schema/{entity}/get");
    let response = Box::pin(execute_request(&conn, &topic, json!({}))).await?;
    output_response(&response, &format);
    Ok(())
}

pub(crate) async fn cmd_constraint_add(
    entity: String,
    name: Option<String>,
    unique: Option<String>,
    fk: Option<String>,
    not_null: Option<String>,
    conn: ConnectionArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let topic = format!("$DB/_admin/constraint/{entity}/add");

    let payload = if let Some(field) = unique {
        let constraint_name = name.unwrap_or_else(|| format!("unique_{entity}_{field}"));
        json!({ "type": "unique", "name": constraint_name, "field": field })
    } else if let Some(fk_spec) = fk {
        let parts: Vec<&str> = fk_spec.split(':').collect();
        if parts.len() < 3 {
            return Err("FK format: field:target_entity:target_field[:action]".into());
        }
        let constraint_name = name.unwrap_or_else(|| format!("fk_{entity}_{}", parts[0]));
        json!({
            "type": "foreign_key",
            "name": constraint_name,
            "field": parts[0],
            "target_entity": parts[1],
            "target_field": parts[2],
            "on_delete": parts.get(3).unwrap_or(&"restrict")
        })
    } else if let Some(field) = not_null {
        let constraint_name = name.unwrap_or_else(|| format!("not_null_{entity}_{field}"));
        json!({ "type": "not_null", "name": constraint_name, "field": field })
    } else {
        return Err("Must specify --unique, --fk, or --not-null".into());
    };

    let response = Box::pin(execute_request(&conn, &topic, payload)).await?;
    output_response(&response, &OutputFormat::Json);
    Ok(())
}

pub(crate) async fn cmd_constraint_list(
    entity: String,
    conn: ConnectionArgs,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let topic = format!("$DB/_admin/constraint/{entity}/list");
    let response = Box::pin(execute_request(&conn, &topic, json!({}))).await?;
    output_response(&response, &format);
    Ok(())
}

pub(crate) async fn cmd_backup_create(
    name: &str,
    conn: &ConnectionArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let topic = "$DB/_admin/backup";
    let payload = json!({"name": name});
    let response = Box::pin(execute_request(conn, topic, payload)).await?;

    let is_ok = response
        .get("status")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|s| s == "ok");

    if is_ok {
        if let Some(data) = response.get("data")
            && let Some(msg) = data.get("message").and_then(serde_json::Value::as_str)
        {
            println!("{msg}");
        }
    } else if let Some(error) = response.get("message").and_then(serde_json::Value::as_str) {
        eprintln!("Error: {error}");
    }

    Ok(())
}

pub(crate) async fn cmd_backup_list(
    conn: &ConnectionArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let topic = "$DB/_admin/backup/list";
    let payload = json!({});
    let response = Box::pin(execute_request(conn, topic, payload)).await?;

    let is_ok = response
        .get("status")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|s| s == "ok");

    if is_ok {
        if let Some(backups) = response.get("data").and_then(|v| v.as_array()) {
            if backups.is_empty() {
                println!("No backups found");
            } else {
                println!("Available backups:");
                for backup in backups {
                    if let Some(name) = backup.as_str() {
                        println!("  {name}");
                    }
                }
            }
        }
    } else if let Some(error) = response.get("message").and_then(serde_json::Value::as_str) {
        eprintln!("Error: {error}");
    }

    Ok(())
}

pub(crate) async fn cmd_restore(
    name: &str,
    conn: &ConnectionArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let topic = "$DB/_admin/restore";
    let payload = json!({"name": name});
    let response = Box::pin(execute_request(conn, topic, payload)).await?;

    let is_ok = response
        .get("status")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|s| s == "ok");

    if is_ok {
        println!("Restore initiated");
    } else if let Some(error) = response.get("message").and_then(serde_json::Value::as_str) {
        eprintln!("Error: {error}");
    }

    Ok(())
}

pub(crate) async fn cmd_subscribe(
    pattern: String,
    entity: Option<String>,
    group: Option<String>,
    mode: SubscriptionModeArg,
    heartbeat_interval: u64,
    conn: ConnectionArgs,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let topic = "$DB/_sub/subscribe";
    let mode_str = match mode {
        SubscriptionModeArg::Broadcast => "broadcast",
        SubscriptionModeArg::LoadBalanced => "load-balanced",
        SubscriptionModeArg::Ordered => "ordered",
    };

    let payload = json!({
        "pattern": pattern,
        "entity": entity,
        "group": group,
        "mode": mode_str
    });

    let response = Box::pin(execute_request(&conn, topic, payload)).await?;

    let is_ok = response
        .get("status")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|s| s == "ok");

    if is_ok {
        let data = response.get("data").unwrap_or(&Value::Null);
        let sub_id = data
            .get("id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();

        eprintln!("Subscription ID: {sub_id}");
        if let Some(partitions) = data.get("partitions").and_then(|v| v.as_array())
            && !partitions.is_empty()
        {
            eprintln!("Assigned partitions: {partitions:?}");
        }

        let event_pattern = if let Some(ref ent) = entity {
            format!("$DB/{ent}/events/#")
        } else {
            "$DB/+/events/#".to_string()
        };

        let client = Box::pin(connect_client(&conn)).await?;
        let (tx, rx) = flume::bounded::<Value>(100);

        client
            .subscribe(&event_pattern, move |msg| {
                if let Ok(value) = serde_json::from_slice::<Value>(&msg.payload) {
                    let _ = tx.try_send(value);
                }
            })
            .await?;

        eprintln!("Watching events (Ctrl+C to stop)...");

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();

        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            shutdown_clone.store(true, Ordering::SeqCst);
        });

        let heartbeat_conn = conn.clone();
        let heartbeat_sub_id = sub_id.to_string();
        let heartbeat_shutdown = shutdown.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(heartbeat_interval));
            while !heartbeat_shutdown.load(Ordering::SeqCst) {
                interval.tick().await;
                if heartbeat_shutdown.load(Ordering::SeqCst) {
                    break;
                }
                let topic = format!("$DB/_sub/{heartbeat_sub_id}/heartbeat");
                let _ = Box::pin(execute_request(&heartbeat_conn, &topic, json!({}))).await;
            }
        });

        while !shutdown.load(Ordering::SeqCst) {
            tokio::select! {
                event = rx.recv_async() => {
                    if let Ok(event) = event {
                        output_response(&event, &format);
                    }
                }
                () = tokio::time::sleep(Duration::from_millis(100)) => {
                    if shutdown.load(Ordering::SeqCst) {
                        break;
                    }
                }
            }
        }

        eprintln!("\nUnsubscribing...");
        let unsub_topic = format!("$DB/_sub/{sub_id}/unsubscribe");
        let _ = Box::pin(execute_request(&conn, &unsub_topic, json!({}))).await;
        client.disconnect().await?;
    } else if let Some(error) = response.get("message").and_then(serde_json::Value::as_str) {
        eprintln!("Error: {error}");
    }

    Ok(())
}
