// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::broker_defaults::BROKER_MAX_PACKET_SIZE;
use mqdb_core::protocol::{AdminOperation, DbOp};
use mqdb_core::{ErrorCode, Response};
use mqtt5::PublishOptions;
use mqtt5::client::MqttClient;
use mqtt5::error::MqttError;
use serde_json::{Map, Value, json};

const PACKET_HEADER_RESERVE: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplyKind {
    Write,
    Read,
}

impl From<DbOp> for ReplyKind {
    fn from(op: DbOp) -> Self {
        match op {
            DbOp::Create | DbOp::Update | DbOp::Delete | DbOp::Share | DbOp::Unshare => Self::Write,
            DbOp::Read | DbOp::List | DbOp::Shares | DbOp::Shared => Self::Read,
        }
    }
}

impl From<&AdminOperation> for ReplyKind {
    fn from(op: &AdminOperation) -> Self {
        match op {
            AdminOperation::SchemaGet { .. }
            | AdminOperation::ConstraintList { .. }
            | AdminOperation::BackupList
            | AdminOperation::ConsumerGroupList
            | AdminOperation::ConsumerGroupShow { .. }
            | AdminOperation::Health
            | AdminOperation::UserList
            | AdminOperation::AclRuleList
            | AdminOperation::AclRoleList
            | AdminOperation::AclAssignmentList
            | AdminOperation::Catalog
            | AdminOperation::VaultStatus => Self::Read,
            _ => Self::Write,
        }
    }
}

#[must_use]
pub fn too_large_response(size: usize, max: usize) -> Vec<u8> {
    let refusal = Response::error(
        ErrorCode::PayloadTooLarge,
        format!("response too large: {size} bytes exceeds the {max} byte limit"),
    );
    serde_json::to_vec(&refusal).unwrap_or_default()
}

#[must_use]
pub fn fit_write_reply(
    payload: Vec<u8>,
    response_topic: &str,
    correlation_data: Option<&[u8]>,
) -> Vec<u8> {
    let packet_bound = payload.len()
        + response_topic.len()
        + correlation_data.map_or(0, <[u8]>::len)
        + PACKET_HEADER_RESERVE;
    if packet_bound <= BROKER_MAX_PACKET_SIZE {
        return payload;
    }
    write_acknowledgement(&payload).unwrap_or(payload)
}

fn write_acknowledgement(payload: &[u8]) -> Option<Vec<u8>> {
    let reply: Value = serde_json::from_slice(payload).ok()?;
    if reply.get("status")? != "ok" {
        return None;
    }
    let data = reply.get("data")?.as_object()?;
    let kept: Map<String, Value> = ["id", "_version"]
        .into_iter()
        .filter_map(|field| data.get(field).map(|v| (field.to_string(), v.clone())))
        .collect();
    serde_json::to_vec(&json!({"status": "ok", "data": kept})).ok()
}

pub async fn publish_reply(
    client: &MqttClient,
    topic: &str,
    payload: Vec<u8>,
    options: PublishOptions,
    kind: ReplyKind,
) {
    let payload = match kind {
        ReplyKind::Write => fit_write_reply(
            payload,
            topic,
            options.properties.correlation_data.as_deref(),
        ),
        ReplyKind::Read => payload,
    };
    let published = match client
        .publish_with_options(topic, payload, options.clone())
        .await
    {
        Err(MqttError::PacketTooLarge { size, max }) if kind == ReplyKind::Read => {
            tracing::warn!(topic, size, max, "reply too large for one packet");
            client
                .publish_with_options(topic, too_large_response(size, max), options)
                .await
        }
        other => other,
    };
    if let Err(e) = published {
        tracing::error!(error = %e, topic, "failed to publish reply");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record_reply(pad: usize) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "status": "ok",
            "data": {"id": "k", "_version": 4, "pad": "x".repeat(pad)}
        }))
        .unwrap()
    }

    #[test]
    fn the_refusal_names_the_size_and_the_limit() {
        let body: Value =
            serde_json::from_slice(&too_large_response(15_483_438, 10_485_760)).unwrap();
        assert_eq!(body["status"], "error");
        assert_eq!(body["code"], 413);
        let message = body["message"].as_str().unwrap();
        assert!(message.contains("15483438 bytes"), "{message}");
        assert!(message.contains("10485760 byte limit"), "{message}");
    }

    #[test]
    fn a_write_reply_that_fits_keeps_its_record() {
        let reply = record_reply(1024);
        assert_eq!(fit_write_reply(reply.clone(), "resp/t", None), reply);
    }

    #[test]
    fn a_write_reply_too_large_for_one_packet_keeps_only_id_and_version() {
        let fitted = fit_write_reply(record_reply(BROKER_MAX_PACKET_SIZE), "resp/t", None);
        let body: Value = serde_json::from_slice(&fitted).unwrap();
        assert_eq!(
            body,
            json!({"status": "ok", "data": {"id": "k", "_version": 4}})
        );
    }

    #[test]
    fn record_writes_and_admin_writes_are_writes() {
        assert_eq!(ReplyKind::from(DbOp::Update), ReplyKind::Write);
        assert_eq!(ReplyKind::from(DbOp::List), ReplyKind::Read);
        assert_eq!(ReplyKind::from(&AdminOperation::UserAdd), ReplyKind::Write);
        assert_eq!(ReplyKind::from(&AdminOperation::UserList), ReplyKind::Read);
    }
}
