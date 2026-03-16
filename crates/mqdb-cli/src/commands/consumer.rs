// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

#[cfg(feature = "cluster")]
use bebytes::BeBytes;
#[cfg(feature = "cluster")]
use mqdb_cluster::cluster::db::DbEntity;
#[cfg(feature = "cluster")]
use mqdb_cluster::cluster::db_protocol::{DbReadRequest, DbResponse, DbStatus, DbWriteRequest};
#[cfg(feature = "cluster")]
use mqtt5::types::{PublishOptions, PublishProperties};
use serde_json::json;
#[cfg(feature = "cluster")]
use std::time::Duration;

use crate::cli_types::{ConnectionArgs, OutputFormat};
#[cfg(feature = "cluster")]
use crate::common::connect_client;
use crate::common::{execute_request, output_response};

pub(crate) async fn cmd_consumer_group_list(
    conn: ConnectionArgs,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let topic = "$DB/_admin/consumer-groups";
    let response = Box::pin(execute_request(&conn, topic, json!({}))).await?;
    output_response(&response, &format);
    Ok(())
}

pub(crate) async fn cmd_consumer_group_show(
    name: String,
    conn: ConnectionArgs,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let topic = format!("$DB/_admin/consumer-groups/{name}");
    let response = Box::pin(execute_request(&conn, &topic, json!({}))).await?;
    output_response(&response, &format);
    Ok(())
}

#[cfg(feature = "cluster")]
pub(crate) async fn execute_db_request(
    conn: &ConnectionArgs,
    topic: &str,
    payload: Vec<u8>,
) -> Result<DbResponse, Box<dyn std::error::Error>> {
    let client = Box::pin(connect_client(conn)).await?;

    let response_topic = format!("$DB/_resp/mqdb-cli-{}", uuid::Uuid::new_v4());
    let (tx, rx) = flume::bounded::<Vec<u8>>(1);

    client
        .subscribe(&response_topic, move |msg| {
            let _ = tx.try_send(msg.payload.clone());
        })
        .await?;

    let opts = PublishOptions {
        properties: PublishProperties {
            response_topic: Some(response_topic.clone()),
            ..Default::default()
        },
        ..Default::default()
    };

    client.publish_with_options(topic, payload, opts).await?;

    let timeout = Duration::from_secs(conn.timeout);
    let response_bytes = tokio::time::timeout(timeout, rx.recv_async())
        .await
        .map_err(|_| "Request timed out")?
        .map_err(|_| "No response received")?;

    client.disconnect().await?;

    let (response, _) = DbResponse::try_from_be_bytes(&response_bytes)
        .map_err(|e| format!("Failed to parse response: {e}"))?;

    Ok(response)
}

#[cfg(feature = "cluster")]
#[allow(clippy::cast_possible_truncation)]
pub(crate) async fn cmd_db_create(
    partition: u16,
    entity: String,
    data: String,
    conn: ConnectionArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let timestamp_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as u64;

    let request = DbWriteRequest::create(data.as_bytes(), timestamp_ms);
    let topic = format!("$DB/p{partition}/{entity}/create");

    let response: DbResponse =
        Box::pin(execute_db_request(&conn, &topic, request.to_be_bytes())).await?;

    match response.status() {
        DbStatus::Ok => {
            if let Ok((db_entity, _)) = DbEntity::try_from_be_bytes(&response.data) {
                println!(
                    "Created: {} {} {}",
                    db_entity.entity_str(),
                    db_entity.id_str(),
                    String::from_utf8_lossy(&db_entity.data)
                );
            } else {
                println!("Created successfully");
            }
        }
        status => eprintln!("Error: {status:?}"),
    }

    Ok(())
}

#[cfg(feature = "cluster")]
pub(crate) async fn cmd_db_read(
    partition: u16,
    entity: String,
    id: String,
    conn: ConnectionArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let request = DbReadRequest::create();
    let topic = format!("$DB/p{partition}/{entity}/{id}");

    let response: DbResponse =
        Box::pin(execute_db_request(&conn, &topic, request.to_be_bytes())).await?;

    match response.status() {
        DbStatus::Ok => {
            if let Ok((db_entity, _)) = DbEntity::try_from_be_bytes(&response.data) {
                println!(
                    "{} {} {}",
                    db_entity.entity_str(),
                    db_entity.id_str(),
                    String::from_utf8_lossy(&db_entity.data)
                );
            } else {
                println!("Data: {:?}", response.data);
            }
        }
        DbStatus::NotFound => eprintln!("Not found"),
        status => eprintln!("Error: {status:?}"),
    }

    Ok(())
}

#[cfg(feature = "cluster")]
#[allow(clippy::cast_possible_truncation)]
pub(crate) async fn cmd_db_update(
    partition: u16,
    entity: String,
    id: String,
    data: String,
    conn: ConnectionArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let timestamp_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as u64;

    let request = DbWriteRequest::create(data.as_bytes(), timestamp_ms);
    let topic = format!("$DB/p{partition}/{entity}/{id}/update");

    let response: DbResponse =
        Box::pin(execute_db_request(&conn, &topic, request.to_be_bytes())).await?;

    match response.status() {
        DbStatus::Ok => {
            if let Ok((db_entity, _)) = DbEntity::try_from_be_bytes(&response.data) {
                println!(
                    "Updated: {} {} {}",
                    db_entity.entity_str(),
                    db_entity.id_str(),
                    String::from_utf8_lossy(&db_entity.data)
                );
            } else {
                println!("Updated successfully");
            }
        }
        DbStatus::NotFound => eprintln!("Not found"),
        status => eprintln!("Error: {status:?}"),
    }

    Ok(())
}

#[cfg(feature = "cluster")]
pub(crate) async fn cmd_db_delete(
    partition: u16,
    entity: String,
    id: String,
    conn: ConnectionArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let topic = format!("$DB/p{partition}/{entity}/{id}/delete");

    let response: DbResponse = Box::pin(execute_db_request(&conn, &topic, Vec::new())).await?;

    match response.status() {
        DbStatus::Ok => println!("Deleted: {entity}/{id}"),
        DbStatus::NotFound => eprintln!("Not found"),
        status => eprintln!("Error: {status:?}"),
    }

    Ok(())
}
