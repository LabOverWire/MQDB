// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use serde_json::json;

use crate::cli_types::{ConnectionArgs, OutputFormat};
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
