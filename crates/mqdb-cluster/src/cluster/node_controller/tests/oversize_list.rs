// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::cluster::protocol::{JsonDbOp, JsonDbRequest, JsonDbResponse};
use crate::cluster::test_support::put_record;
use mqdb_agent::broker_defaults::BROKER_MAX_PACKET_SIZE;
use serde_json::{Value, json};

const RESPONSE_TOPIC: &str = "resp/client";
const RECORD_PAD: usize = 50 * 1024;

fn node(id: u16) -> NodeId {
    NodeId::validated(id).unwrap()
}

fn fill_past_the_packet_limit(ctrl: &NodeController<MockTransport>) {
    let records = BROKER_MAX_PACKET_SIZE / RECORD_PAD + 2;
    for index in 0..records {
        let data = serde_json::to_vec(&json!({"v": index, "pad": "x".repeat(RECORD_PAD)})).unwrap();
        put_record(
            &ctrl.stores.db_data,
            "bigdocs",
            &format!("r{index:05}"),
            &data,
            1000,
        );
    }
}

fn scatter_list_request(request_id: u64) -> JsonDbRequest {
    JsonDbRequest {
        request_id,
        op: JsonDbOp::List,
        entity: "bigdocs".to_string(),
        id: None,
        payload: b"{}".to_vec(),
        response_topic: format!("_mqdb/scatter/1/{request_id}"),
        correlation_data: None,
        sender: None,
        client_id: None,
        requested_owner: None,
    }
}

fn parse(payload: &[u8]) -> Value {
    serde_json::from_slice(payload).unwrap()
}

#[tokio::test]
async fn a_peer_answers_a_scatter_list_too_large_for_one_frame_with_a_size_refusal() {
    let mut ctrl = create_test_controller(node(2), MockTransport::new(node(2)));
    fill_past_the_packet_limit(&ctrl);

    ctrl.handle_json_db_request(node(1), PartitionId::ZERO, &scatter_list_request(7))
        .await;

    let replies: Vec<Vec<u8>> = ctrl
        .transport()
        .take_sent()
        .into_iter()
        .filter_map(|(to, message)| match message {
            ClusterMessage::JsonDbResponse(response) if to == node(1) => Some(response.payload),
            _ => None,
        })
        .collect();
    assert_eq!(replies.len(), 1);
    let body = parse(&replies[0]);
    assert_eq!(body["code"], 413, "{}", body["message"]);
    let message = body["message"].as_str().unwrap();
    assert!(
        message.contains(&format!(
            "{} byte limit",
            crate::cluster::quic_transport::MAX_MESSAGE_SIZE
        )),
        "{message}"
    );
}

#[tokio::test]
async fn a_peer_refusal_of_a_scatter_list_reaches_the_client() {
    let mut ctrl = create_test_controller(node(1), MockTransport::new(node(1)));
    ctrl.heartbeat.receive_heartbeat(
        node(2),
        &crate::cluster::protocol::Heartbeat::create(node(2), 0),
        0,
    );
    assert!(
        ctrl.start_scatter_list_query(
            "bigdocs",
            b"{}",
            RESPONSE_TOPIC.to_string(),
            Some(b"corr".to_vec()),
            Vec::new(),
            Vec::new(),
            None,
            None,
            None,
        )
        .await
    );
    let request_id = *ctrl.pending_scatter_requests.keys().next().unwrap();
    let refusal = NodeController::<MockTransport>::json_error(413, "response too large");

    ctrl.handle_json_db_response(&JsonDbResponse::new(
        request_id,
        refusal.clone(),
        format!("_mqdb/scatter/1/{request_id}"),
        None,
    ))
    .await;

    assert!(ctrl.pending_scatter_requests.is_empty());
    let replies: Vec<(String, String)> = ctrl
        .transport()
        .queued_publishes()
        .into_iter()
        .filter_map(|publish| match publish {
            QueuedPublish::Plain(topic, payload, _) => {
                Some((topic, String::from_utf8_lossy(&payload).into_owned()))
            }
            QueuedPublish::Routed(_) => None,
        })
        .collect();
    assert_eq!(
        replies,
        [(
            RESPONSE_TOPIC.to_string(),
            String::from_utf8_lossy(&refusal).into_owned()
        )]
    );
    assert_eq!(
        ctrl.transport().response_correlations(),
        [(RESPONSE_TOPIC.to_string(), Some(b"corr".to_vec()))]
    );
}

#[tokio::test]
async fn a_forwarded_update_too_large_to_echo_is_acknowledged_without_the_record() {
    let partition = crate::cluster::db::data_partition("grow", "k");
    let mut ctrl = create_test_controller(node(2), MockTransport::new(node(2)));
    ctrl.become_primary(partition, Epoch::new(1));
    let mut map = crate::cluster::PartitionMap::default();
    map.set(
        partition,
        crate::cluster::PartitionAssignment::new(node(2), Vec::new(), Epoch::new(1)),
    );
    ctrl.update_partition_map(map);
    let original = serde_json::to_vec(&json!({"a": "x".repeat(8 * 1024 * 1024)})).unwrap();
    put_record(&ctrl.stores.db_data, "grow", "k", &original, 1000);
    let request = JsonDbRequest {
        request_id: 9,
        op: JsonDbOp::Update,
        entity: "grow".to_string(),
        id: Some("k".to_string()),
        payload: serde_json::to_vec(&json!({"d": "y".repeat(3 * 1024 * 1024)})).unwrap(),
        response_topic: RESPONSE_TOPIC.to_string(),
        correlation_data: None,
        sender: None,
        client_id: None,
        requested_owner: None,
    };

    ctrl.handle_json_db_request(node(1), partition, &request)
        .await;

    let replies: Vec<Value> = ctrl
        .transport()
        .take_sent()
        .into_iter()
        .filter_map(|(to, message)| match message {
            ClusterMessage::JsonDbResponse(response) if to == node(1) => {
                Some(parse(&response.payload))
            }
            _ => None,
        })
        .collect();
    let stored: Value = serde_json::from_slice(&ctrl.db_get("grow", "k").unwrap().data).unwrap();
    assert!(stored.get("d").is_some(), "the update was not applied");
    assert_eq!(replies.len(), 1);
    assert_eq!(replies[0]["status"], "ok", "{}", replies[0]["message"]);
    assert_eq!(replies[0]["data"]["id"], "k");
    assert!(
        replies[0]["data"].get("_version").is_some(),
        "{}",
        replies[0]
    );
    assert!(replies[0]["data"].get("a").is_none());
}

#[tokio::test]
async fn a_scatter_list_sorts_by_id() {
    let mut ctrl = create_test_controller(node(1), MockTransport::new(node(1)));
    for id in ["a", "b", "c"] {
        put_record(&ctrl.stores.db_data, "rows", id, br#"{"n":1}"#, 1000);
    }
    ctrl.heartbeat.receive_heartbeat(
        node(2),
        &crate::cluster::protocol::Heartbeat::create(node(2), 0),
        0,
    );
    assert!(
        ctrl.start_scatter_list_query(
            "rows",
            b"{}",
            RESPONSE_TOPIC.to_string(),
            None,
            Vec::new(),
            vec![mqdb_core::SortOrder::new(
                "id".to_string(),
                mqdb_core::SortDirection::Desc
            )],
            None,
            None,
            None,
        )
        .await
    );
    let request_id = *ctrl.pending_scatter_requests.keys().next().unwrap();

    ctrl.handle_json_db_response(&JsonDbResponse::new(
        request_id,
        br#"{"status":"ok","data":[]}"#.to_vec(),
        format!("_mqdb/scatter/1/{request_id}"),
        None,
    ))
    .await;

    let ids: Vec<String> = ctrl
        .transport()
        .queued_publishes()
        .into_iter()
        .filter_map(|publish| match publish {
            QueuedPublish::Plain(topic, payload, _) if topic == RESPONSE_TOPIC => {
                Some(parse(&payload))
            }
            _ => None,
        })
        .flat_map(|reply| {
            reply["data"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(|item| item["id"].as_str().unwrap_or_default().to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(ids, ["c", "b", "a"]);
}
