use super::*;

fn node(id: u16) -> NodeId {
    NodeId::validated(id).unwrap()
}

fn controller(id: u16) -> NodeController<MockTransport> {
    create_test_controller(node(id), MockTransport::new(node(id)))
}

fn location_write(client_id: &str, on: NodeId, operation: Operation) -> ClusterMessage {
    let entry = crate::cluster::ClientLocationEntry::create(client_id, on);
    ClusterMessage::WriteRequest(ReplicationWrite::new(
        session_partition(client_id),
        operation,
        Epoch::new(0),
        0,
        crate::cluster::entity::CLIENT_LOCATIONS.to_string(),
        crate::cluster::client_location_key(client_id),
        bebytes::BeBytes::to_be_bytes(&entry),
    ))
}

#[tokio::test]
async fn a_lost_disconnect_is_repaired_by_the_clients_next_connect() {
    let mut ctrl = controller(2);
    ctrl.transport
        .inject_message(node(1), location_write("mover", node(1), Operation::Insert));
    ctrl.process_messages().await;

    ctrl.transport
        .inject_message(node(3), location_write("mover", node(3), Operation::Insert));
    ctrl.process_messages().await;

    let mut forwarded: Vec<NodeId> = ctrl
        .remote_targets([crate::cluster::ForwardTarget::new("mover".to_string(), 1)])
        .into_keys()
        .collect();
    forwarded.sort_by_key(|n| n.get());
    assert_eq!(forwarded, [node(3)], "publishes still forwarded to node 1");
    assert_eq!(ctrl.connected_nodes("mover"), [node(3)]);
}

#[tokio::test]
async fn a_stale_claim_is_gone_after_later_connects_and_disconnects() {
    let mut ctrl = controller(2);
    ctrl.transport
        .inject_message(node(1), location_write("mover", node(1), Operation::Insert));
    ctrl.process_messages().await;
    for _ in 0..5 {
        ctrl.transport
            .inject_message(node(3), location_write("mover", node(3), Operation::Insert));
        ctrl.transport
            .inject_message(node(3), location_write("mover", node(3), Operation::Delete));
    }
    ctrl.process_messages().await;

    assert_eq!(ctrl.connected_nodes("mover"), Vec::<NodeId>::new());
}

#[tokio::test]
async fn a_reply_arriving_after_give_up_still_releases_stale_claims() {
    let mut peer = controller(1);
    let mut restarted = controller(2);
    restarted
        .stores()
        .client_locations
        .claim("gone-client", peer.node_id());

    restarted.transport.inject_message(
        peer.node_id(),
        ClusterMessage::Heartbeat(Heartbeat::create(peer.node_id(), 0)),
    );
    restarted.process_messages().await;

    let mut now = 0u64;
    let mut requests = Vec::new();
    for _ in 0..40 {
        restarted.drive_subscription_resyncs(now).await;
        requests.extend(
            restarted
                .transport
                .take_sent()
                .into_iter()
                .filter(|(to, m)| {
                    *to == peer.node_id()
                        && matches!(m, ClusterMessage::SubscriptionResyncRequest(_))
                }),
        );
        if restarted.resync_rounds.is_empty() {
            break;
        }
        now += 200_000;
    }
    assert!(restarted.resync_rounds.is_empty(), "round never given up");
    assert_eq!(requests.len(), 6, "every ask of the round");

    let (_, last_request) = requests.pop().unwrap();
    peer.transport
        .inject_message(restarted.node_id(), last_request);
    peer.process_messages().await;
    for (to, message) in peer.transport.take_sent() {
        if to == restarted.node_id() && matches!(message, ClusterMessage::SubscriptionResync(_)) {
            restarted.transport.inject_message(peer.node_id(), message);
        }
    }
    restarted.process_messages().await;

    assert_eq!(
        restarted.connected_nodes("gone-client"),
        Vec::<NodeId>::new(),
        "complete listing from the peer arrived but the stale claim was kept"
    );
}

fn indexed_controller(other_entries: u32) -> NodeController<MockTransport> {
    let ctrl = controller(1);
    ctrl.stores().client_locations.claim("resumer", node(1));
    ctrl.stores()
        .topics
        .subscribe("own/topic", "resumer", session_partition("resumer"), 1)
        .unwrap();
    ctrl.stores()
        .wildcards
        .subscribe_mqtt("own/+", "resumer", 1)
        .unwrap();
    for n in 0..other_entries {
        let client = format!("other-{n}");
        ctrl.stores()
            .topics
            .subscribe(
                &format!("other/{n}"),
                &client,
                session_partition(&client),
                1,
            )
            .unwrap();
        if n % 10 == 0 {
            ctrl.stores()
                .wildcards
                .subscribe_mqtt(&format!("other/{n}/+"), &client, 1)
                .unwrap();
        }
    }
    ctrl
}

#[tokio::test]
async fn a_resumed_client_announces_exactly_its_own_subscriptions() {
    let ctrl = indexed_controller(1_000);
    ctrl.announce_resumed_client("resumer").await;

    let announced: Vec<_> = ctrl
        .transport
        .take_sent()
        .into_iter()
        .filter_map(|(_, message)| match message {
            ClusterMessage::SubscriptionResync(resync) => Some(resync),
            _ => None,
        })
        .flat_map(|resync| resync.clients)
        .collect();

    assert_eq!(announced.len(), 1);
    assert_eq!(announced[0].client_id, "resumer");
    assert_eq!(
        announced[0]
            .topics
            .iter()
            .map(|t| t.topic.as_str())
            .collect::<Vec<_>>(),
        ["own/topic"]
    );
    assert_eq!(
        announced[0]
            .patterns
            .iter()
            .map(|p| p.pattern.as_str())
            .collect::<Vec<_>>(),
        ["own/+"]
    );
}

fn revive(ctrl: &NodeController<MockTransport>, peer: NodeId) {
    ctrl.transport
        .inject_message(peer, ClusterMessage::Heartbeat(Heartbeat::create(peer, 0)));
}

async fn answer_resyncs(
    asker: &mut NodeController<MockTransport>,
    peer: &mut NodeController<MockTransport>,
) {
    for (to, message) in asker.transport.take_sent() {
        if to == peer.node_id() && matches!(message, ClusterMessage::SubscriptionResyncRequest(_)) {
            peer.transport.inject_message(asker.node_id(), message);
        }
    }
    peer.process_messages().await;
    for (to, message) in peer.transport.take_sent() {
        if to == asker.node_id() && matches!(message, ClusterMessage::SubscriptionResync(_)) {
            asker.transport.inject_message(peer.node_id(), message);
        }
    }
    asker.process_messages().await;
}

const LATER_MS: u64 = 1_800_000_000_000;

async fn run_periodic_resync(ctrl: &mut NodeController<MockTransport>) {
    ctrl.drive_subscription_resyncs(LATER_MS).await;
    ctrl.drive_subscription_resyncs(
        LATER_MS + crate::cluster::node_controller::subscription_resync::ANTI_ENTROPY_INTERVAL_MS,
    )
    .await;
}

#[tokio::test]
async fn a_lost_disconnect_is_repaired_by_the_periodic_resync_without_a_reconnect() {
    let mut old = controller(1);
    let mut ctrl = controller(2);
    revive(&ctrl, node(1));
    ctrl.process_messages().await;
    answer_resyncs(&mut ctrl, &mut old).await;
    ctrl.transport
        .inject_message(node(1), location_write("idle", node(1), Operation::Insert));
    ctrl.process_messages().await;
    assert_eq!(ctrl.connected_nodes("idle"), [node(1)]);

    run_periodic_resync(&mut ctrl).await;
    answer_resyncs(&mut ctrl, &mut old).await;

    assert_eq!(ctrl.connected_nodes("idle"), Vec::<NodeId>::new());
}

#[tokio::test]
async fn a_late_connect_from_the_old_node_is_repaired_by_the_periodic_resync() {
    let mut new = controller(3);
    new.stores().client_locations.claim("hopper", node(3));
    let mut ctrl = controller(2);
    revive(&ctrl, node(3));
    ctrl.process_messages().await;
    answer_resyncs(&mut ctrl, &mut new).await;
    ctrl.transport.inject_message(
        node(3),
        location_write("hopper", node(3), Operation::Insert),
    );
    ctrl.transport.inject_message(
        node(1),
        location_write("hopper", node(1), Operation::Insert),
    );
    ctrl.transport.inject_message(
        node(1),
        location_write("hopper", node(1), Operation::Delete),
    );
    ctrl.process_messages().await;
    assert_eq!(ctrl.connected_nodes("hopper"), Vec::<NodeId>::new());

    run_periodic_resync(&mut ctrl).await;
    answer_resyncs(&mut ctrl, &mut new).await;

    assert_eq!(ctrl.connected_nodes("hopper"), [node(3)]);
}

#[tokio::test]
async fn the_periodic_resync_skips_a_peer_that_never_answered() {
    let mut ctrl = controller(2);
    revive(&ctrl, node(1));
    ctrl.process_messages().await;
    let step = crate::cluster::node_controller::subscription_resync::RESYNC_RETRY_MS;
    let mut now = LATER_MS;
    let mut asks = 0;
    for _ in 0..2_000 {
        ctrl.drive_subscription_resyncs(now).await;
        asks += ctrl
            .transport
            .take_sent()
            .iter()
            .filter(|(to, m)| {
                *to == node(1) && matches!(m, ClusterMessage::SubscriptionResyncRequest(_))
            })
            .count();
        now += step;
    }

    assert_eq!(
        asks,
        usize::try_from(crate::cluster::node_controller::subscription_resync::MAX_RESYNC_ATTEMPTS)
            .unwrap()
    );
}

#[tokio::test]
async fn a_resync_reasserting_an_old_connection_does_not_steal_the_single_location() {
    let client = "mover";
    let mut old = controller(1);
    old.stores().client_locations.claim(client, node(1));
    let mut ctrl = controller(2);
    ctrl.transport
        .inject_message(node(3), location_write(client, node(3), Operation::Insert));
    ctrl.process_messages().await;
    assert_eq!(ctrl.connected_node(client), Some(node(3)));

    ctrl.transport.inject_message(
        node(1),
        ClusterMessage::SubscriptionResyncRequest(crate::cluster::protocol::ResyncRequest {
            round: 7,
            ask_back: false,
            digest: crate::cluster::protocol::ListingDigest::default(),
        }),
    );
    let request = ctrl.transport.recv().unwrap();
    old.transport.inject_message(node(2), request.message);
    old.process_messages().await;
    for (to, message) in old.transport.take_sent() {
        if to == node(2) {
            ctrl.transport.inject_message(node(1), message);
        }
    }
    ctrl.process_messages().await;

    assert_eq!(
        ctrl.connected_node(client),
        Some(node(3)),
        "the newest connection is no longer the single answer used by $share and LWT routing"
    );
}

fn asked_rounds(ctrl: &NodeController<MockTransport>, peer: NodeId) -> Vec<u64> {
    ctrl.transport
        .take_sent()
        .into_iter()
        .filter_map(|(to, message)| match message {
            ClusterMessage::SubscriptionResyncRequest(request) if to == peer => Some(request.round),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn a_reply_to_a_round_asked_before_a_restart_does_not_complete_a_later_round() {
    let mut before_restart = controller(2);
    revive(&before_restart, node(1));
    before_restart.process_messages().await;
    let old_rounds = asked_rounds(&before_restart, node(1));
    assert_eq!(old_rounds.len(), 1);

    std::thread::sleep(std::time::Duration::from_millis(2));
    let mut after_restart = controller(2);
    revive(&after_restart, node(1));
    after_restart.process_messages().await;
    assert_eq!(asked_rounds(&after_restart, node(1)).len(), 1);

    after_restart.transport.inject_message(
        node(1),
        ClusterMessage::SubscriptionResync(crate::cluster::protocol::SubscriptionResync {
            round: old_rounds[0],
            part: 0,
            parts: 1,
            unchanged: false,
            clients: Vec::new(),
        }),
    );
    after_restart.process_messages().await;
    after_restart.drive_subscription_resyncs(LATER_MS).await;
    after_restart
        .drive_subscription_resyncs(
            LATER_MS + crate::cluster::node_controller::subscription_resync::RESYNC_RETRY_MS,
        )
        .await;

    assert_eq!(
        asked_rounds(&after_restart, node(1)).len(),
        1,
        "the round asked after the restart was completed by the old reply"
    );
}

fn resync_requests_to(ctrl: &NodeController<MockTransport>, peer: NodeId) -> usize {
    ctrl.transport
        .take_sent()
        .iter()
        .filter(|(to, m)| *to == peer && matches!(m, ClusterMessage::SubscriptionResyncRequest(_)))
        .count()
}

#[tokio::test]
async fn a_peer_that_answered_before_is_refreshed_again_after_one_unanswered_round() {
    let mut peer = controller(1);
    peer.stores().client_locations.claim("steady", node(1));
    let mut ctrl = controller(2);
    revive(&ctrl, node(1));
    ctrl.process_messages().await;
    answer_resyncs(&mut ctrl, &mut peer).await;
    assert!(ctrl.resync_rounds.is_empty(), "boot round not completed");

    let step = crate::cluster::node_controller::subscription_resync::RESYNC_RETRY_MS;
    let mut now = LATER_MS;
    let mut unanswered = 0;
    while now < LATER_MS + 800_000 {
        ctrl.drive_subscription_resyncs(now).await;
        unanswered += resync_requests_to(&ctrl, node(1));
        now += step;
    }
    assert_eq!(unanswered, 6, "the overloaded round was not given up");

    let mut later_asks = 0;
    while now < LATER_MS + 800_000 + 3_600_000 {
        ctrl.drive_subscription_resyncs(now).await;
        later_asks += resync_requests_to(&ctrl, node(1));
        now += step;
    }
    assert!(
        later_asks > 0,
        "a peer that answered before was never asked again after one unanswered round"
    );
}

#[tokio::test]
async fn a_periodic_resync_is_asked_five_minutes_after_the_first_tick() {
    let mut peer = controller(1);
    let mut ctrl = controller(2);
    revive(&ctrl, node(1));
    ctrl.process_messages().await;
    answer_resyncs(&mut ctrl, &mut peer).await;
    assert!(ctrl.resync_rounds.is_empty(), "boot round not completed");

    let mut first_ask = None;
    for second in 0..=400_u64 {
        ctrl.drive_subscription_resyncs(LATER_MS + second * 1_000)
            .await;
        if first_ask.is_none() && resync_requests_to(&ctrl, node(1)) > 0 {
            first_ask = Some(second);
        }
    }

    assert_eq!(first_ask, Some(300));
}

fn heavy_client(client_id: &str, topics: usize) -> Vec<String> {
    (0..topics)
        .map(|n| format!("{client_id}/{n:06}/{}", "x".repeat(1_000)))
        .collect()
}

#[tokio::test]
async fn a_client_split_across_parts_keeps_every_subscription_and_loses_stale_ones() {
    let mut peer = controller(1);
    let topics = heavy_client("heavy", 3_000);
    peer.stores().client_locations.claim("heavy", node(1));
    for topic in &topics {
        peer.stores()
            .topics
            .subscribe(topic, "heavy", session_partition("heavy"), 1)
            .unwrap();
    }
    peer.stores()
        .wildcards
        .subscribe_mqtt("heavy/+/tail", "heavy", 1)
        .unwrap();
    peer.stores().client_locations.claim("light", node(1));
    peer.stores()
        .topics
        .subscribe("light/t", "light", session_partition("light"), 0)
        .unwrap();

    let mut ctrl = controller(2);
    ctrl.stores()
        .topics
        .subscribe("heavy/stale", "heavy", session_partition("heavy"), 1)
        .unwrap();
    ctrl.transport
        .inject_message(node(1), location_write("heavy", node(1), Operation::Insert));
    ctrl.process_messages().await;
    revive(&ctrl, node(1));
    ctrl.process_messages().await;
    answer_resyncs(&mut ctrl, &mut peer).await;

    assert!(
        ctrl.resync_rounds.is_empty(),
        "the split round did not complete"
    );
    let mut indexed: Vec<String> = ctrl
        .stores()
        .topics
        .get_client_topics("heavy")
        .into_iter()
        .map(|(topic, _)| topic)
        .collect();
    indexed.sort();
    assert_eq!(indexed, topics);
    assert_eq!(
        ctrl.stores().wildcards.get_client_patterns("heavy"),
        [("heavy/+/tail".to_string(), 1)]
    );
    assert_eq!(
        ctrl.stores().topics.get_client_topics("light"),
        [("light/t".to_string(), 0)]
    );
    assert_eq!(ctrl.connected_nodes("light"), [node(1)]);
}

#[tokio::test]
async fn a_peer_whose_listing_matches_answers_without_sending_it() {
    let mut peer = controller(1);
    peer.stores().client_locations.claim("steady", node(1));
    peer.stores()
        .topics
        .subscribe("steady/t", "steady", session_partition("steady"), 1)
        .unwrap();
    let mut ctrl = controller(2);
    revive(&ctrl, node(1));
    ctrl.process_messages().await;
    answer_resyncs(&mut ctrl, &mut peer).await;
    assert_eq!(ctrl.connected_nodes("steady"), [node(1)]);

    run_periodic_resync(&mut ctrl).await;
    let requests: Vec<_> = ctrl
        .transport
        .take_sent()
        .into_iter()
        .filter(|(to, m)| {
            *to == node(1) && matches!(m, ClusterMessage::SubscriptionResyncRequest(_))
        })
        .collect();
    assert_eq!(requests.len(), 1);
    for (_, request) in requests {
        peer.transport.inject_message(node(2), request);
    }
    peer.process_messages().await;
    let replies: Vec<crate::cluster::protocol::SubscriptionResync> = peer
        .transport
        .take_sent()
        .into_iter()
        .filter_map(|(to, m)| match m {
            ClusterMessage::SubscriptionResync(reply) if to == node(2) => Some(reply),
            _ => None,
        })
        .collect();
    assert_eq!(replies.len(), 1);
    assert!(replies[0].unchanged);
    assert!(replies[0].clients.is_empty());

    for reply in replies {
        ctrl.transport
            .inject_message(node(1), ClusterMessage::SubscriptionResync(reply));
    }
    ctrl.process_messages().await;
    assert!(
        ctrl.resync_rounds.is_empty(),
        "the unchanged round did not complete"
    );
    assert_eq!(ctrl.connected_nodes("steady"), [node(1)]);
    assert_eq!(
        ctrl.stores().topics.get_client_topics("steady"),
        [("steady/t".to_string(), 1)]
    );
}

#[tokio::test]
async fn a_missing_subscription_makes_the_peer_send_its_full_listing() {
    let mut peer = controller(1);
    peer.stores().client_locations.claim("steady", node(1));
    peer.stores()
        .topics
        .subscribe("steady/t", "steady", session_partition("steady"), 1)
        .unwrap();
    let mut ctrl = controller(2);
    revive(&ctrl, node(1));
    ctrl.process_messages().await;
    answer_resyncs(&mut ctrl, &mut peer).await;
    ctrl.stores()
        .topics
        .unsubscribe("steady/t", "steady")
        .unwrap();

    run_periodic_resync(&mut ctrl).await;
    answer_resyncs(&mut ctrl, &mut peer).await;

    assert_eq!(
        ctrl.stores().topics.get_client_topics("steady"),
        [("steady/t".to_string(), 1)]
    );
}

#[tokio::test]
async fn the_digest_of_the_index_matches_the_digest_of_the_listing() {
    let ctrl = indexed_controller(50);
    ctrl.stores()
        .topics
        .subscribe(
            "resp/resumer/reply",
            "resumer",
            session_partition("resumer"),
            1,
        )
        .unwrap();
    let clients: std::collections::HashSet<String> = ["resumer", "other-3", "other-20", "absent"]
        .into_iter()
        .map(str::to_string)
        .collect();

    assert_eq!(
        ctrl.listing_digest(&clients),
        crate::cluster::protocol::ListingDigest::of(&ctrl.resynced_clients(&clients))
    );
}

#[tokio::test]
async fn a_reply_to_a_round_no_longer_tracked_only_adds_subscriptions() {
    let mut ctrl = controller(2);
    ctrl.transport
        .inject_message(node(1), location_write("kept", node(1), Operation::Insert));
    ctrl.process_messages().await;
    for topic in ["kept/a", "kept/b"] {
        ctrl.stores()
            .topics
            .subscribe(topic, "kept", session_partition("kept"), 1)
            .unwrap();
    }

    ctrl.transport.inject_message(
        node(1),
        ClusterMessage::SubscriptionResync(crate::cluster::protocol::SubscriptionResync {
            round: 12_345,
            part: 0,
            parts: 1,
            unchanged: false,
            clients: vec![crate::cluster::protocol::ResyncedClient {
                client_id: "kept".to_string(),
                topics: vec![crate::cluster::protocol::ResyncedTopic {
                    topic: "kept/a".to_string(),
                    qos: 1,
                }],
                patterns: Vec::new(),
            }],
        }),
    );
    ctrl.process_messages().await;

    let mut topics = ctrl.stores().topics.get_client_topics("kept");
    topics.sort();
    assert_eq!(
        topics,
        [("kept/a".to_string(), 1), ("kept/b".to_string(), 1)]
    );
}

#[tokio::test]
async fn an_unchanged_reply_to_a_view_that_changed_since_the_ask_is_asked_again() {
    let mut peer = controller(1);
    let mut ctrl = controller(2);
    revive(&ctrl, node(1));
    ctrl.process_messages().await;
    ctrl.transport
        .inject_message(node(1), location_write("idle", node(1), Operation::Insert));
    answer_resyncs(&mut ctrl, &mut peer).await;
    assert_eq!(ctrl.connected_nodes("idle"), [node(1)]);

    ctrl.drive_subscription_resyncs(LATER_MS).await;
    answer_resyncs(&mut ctrl, &mut peer).await;

    assert_eq!(ctrl.connected_nodes("idle"), Vec::<NodeId>::new());
    assert!(ctrl.resync_rounds.is_empty());
}
