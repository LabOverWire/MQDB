// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;

const RESPONSE_TOPIC: &str = "resp/client";

fn node(id: u16) -> NodeId {
    NodeId::validated(id).unwrap()
}

fn peer_hearing_node_two() -> NodeController<MockTransport> {
    let mut ctrl = create_test_controller(node(1), MockTransport::new(node(1)));
    ctrl.heartbeat.receive_heartbeat(
        node(2),
        &crate::cluster::protocol::Heartbeat::create(node(2), 0),
        0,
    );
    ctrl
}

async fn start_scatter_list(ctrl: &mut NodeController<MockTransport>, correlation: &[u8]) -> u64 {
    assert!(
        ctrl.start_scatter_list_query(
            "item",
            b"{}",
            RESPONSE_TOPIC.to_string(),
            Some(correlation.to_vec()),
            Vec::new(),
            Vec::new(),
            None,
            None,
            None,
        )
        .await
    );
    *ctrl.pending_scatter_requests.keys().next().unwrap()
}

fn expected(correlation: &[u8]) -> Vec<ResponseCorrelation> {
    vec![(RESPONSE_TOPIC.to_string(), Some(correlation.to_vec()))]
}

#[tokio::test]
async fn a_completed_scatter_list_reply_carries_the_correlation_data() {
    let mut ctrl = peer_hearing_node_two();
    let request_id = start_scatter_list(&mut ctrl, b"scatter-1").await;

    ctrl.handle_scatter_list_response(request_id, Vec::new())
        .await;

    assert_eq!(
        ctrl.transport().response_correlations(),
        expected(b"scatter-1")
    );
}

#[tokio::test]
async fn a_timed_out_scatter_list_reply_carries_the_correlation_data() {
    let mut ctrl = peer_hearing_node_two();
    start_scatter_list(&mut ctrl, b"scatter-2").await;
    let mut output = TickOutput::default();

    ctrl.collect_stale_scatter_responses(60_000, &mut output);

    let correlations: Vec<_> = output
        .local_publishes
        .into_iter()
        .map(|(topic, _, correlation)| (topic, correlation))
        .collect();
    assert_eq!(correlations, expected(b"scatter-2"));
}

#[cfg(feature = "http-api")]
#[tokio::test]
async fn a_superseded_share_reply_carries_the_correlation_data() {
    let mut ctrl = create_test_controller(node(1), MockTransport::new(node(1)));
    let continuation = ShareResolveContinuation {
        email_hash: "hash".to_string(),
        entity: "item".to_string(),
        id: "i1".to_string(),
        grantee_key: "hash".to_string(),
        grantee_email: None,
        permission: "view".to_string(),
        cascade: false,
        granted_by: "alice".to_string(),
        response_topic: RESPONSE_TOPIC.to_string(),
        correlation_data: Some(b"share-1".to_vec()),
        invalidated: true,
    };

    ctrl.complete_share_resolution(Vec::new(), continuation)
        .await;

    assert_eq!(
        ctrl.transport().response_correlations(),
        expected(b"share-1")
    );
}
