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
            continued: false,
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
async fn a_reply_to_a_round_no_longer_tracked_changes_nothing() {
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
            continued: false,
            clients: vec![
                crate::cluster::protocol::ResyncedClient {
                    client_id: "kept".to_string(),
                    topics: vec![crate::cluster::protocol::ResyncedTopic {
                        topic: "kept/a".to_string(),
                        qos: 1,
                    }],
                    patterns: Vec::new(),
                },
                crate::cluster::protocol::ResyncedClient {
                    client_id: "listed-late".to_string(),
                    topics: vec![crate::cluster::protocol::ResyncedTopic {
                        topic: "late/t".to_string(),
                        qos: 1,
                    }],
                    patterns: Vec::new(),
                },
            ],
        }),
    );
    ctrl.process_messages().await;

    let mut topics = ctrl.stores().topics.get_client_topics("kept");
    topics.sort();
    assert_eq!(
        topics,
        [("kept/a".to_string(), 1), ("kept/b".to_string(), 1)]
    );
    assert_eq!(ctrl.connected_nodes("listed-late"), Vec::<NodeId>::new());
    assert!(
        ctrl.stores()
            .topics
            .get_client_topics("listed-late")
            .is_empty()
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

#[tokio::test]
async fn a_lost_later_part_drops_nothing_the_asker_already_had() {
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
    revive(&ctrl, node(1));
    ctrl.process_messages().await;
    answer_resyncs(&mut ctrl, &mut peer).await;
    assert!(ctrl.resync_rounds.is_empty(), "boot round not completed");
    assert_eq!(ctrl.stores().topics.get_client_topics("heavy").len(), 3_000);
    assert_eq!(
        ctrl.stores().wildcards.get_client_patterns("heavy").len(),
        1
    );

    ctrl.stores()
        .topics
        .unsubscribe("light/t", "light")
        .unwrap();
    run_periodic_resync(&mut ctrl).await;
    for (to, message) in ctrl.transport.take_sent() {
        if to == node(1) && matches!(message, ClusterMessage::SubscriptionResyncRequest(_)) {
            peer.transport.inject_message(node(2), message);
        }
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
    assert!(
        replies.len() >= 2,
        "expected a multi-part reply, got {}",
        replies.len()
    );
    let first = replies.into_iter().next().unwrap();
    assert_eq!(first.part, 0);
    ctrl.transport
        .inject_message(node(1), ClusterMessage::SubscriptionResync(first));
    ctrl.process_messages().await;

    assert_eq!(
        ctrl.stores().wildcards.get_client_patterns("heavy"),
        [("heavy/+/tail".to_string(), 1)],
        "pattern the asker already had was dropped by the first piece"
    );
    assert_eq!(
        ctrl.stores().topics.get_client_topics("heavy").len(),
        3_000,
        "topics the asker already had were dropped by the first piece"
    );
}

#[tokio::test]
async fn the_incremental_digest_matches_a_recomputation_after_random_changes() {
    use crate::cluster::{SubscriberLocation, TopicIndex, TopicIndexEntry, topic_partition};
    let ctrl = controller(1);
    let other = crate::cluster::TopicIndex::new(node(3));
    let clients = ["c0", "c1", "c2", "c3"];
    let topics = ["a/b", "a/c", "resp/c0/x", "$share/g/a/b", "x/y/z", "q"];
    let patterns = ["a/+", "a/#", "+/b", "$share/g/a/+", "#"];
    let mut seed: u64 = 0x1234_5678;
    let mut next = |bound: u64| {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (seed >> 33) % bound
    };
    let all: std::collections::HashSet<String> = clients.iter().map(|c| c.to_string()).collect();
    for step in 0..20_000 {
        let c = clients[next(4) as usize];
        let t = topics[next(6) as usize];
        let p = patterns[next(5) as usize];
        let q = next(3) as u8;
        match next(11) {
            0 | 1 => {
                let _ = ctrl
                    .stores()
                    .topics
                    .subscribe(t, c, session_partition(c), q);
            }
            2 => {
                let _ = ctrl.stores().topics.unsubscribe(t, c);
            }
            3 => {
                let subs: Vec<SubscriberLocation> = (0..next(4))
                    .map(|_| {
                        let sc = clients[next(4) as usize];
                        SubscriberLocation::create(sc, session_partition(sc), next(3) as u8)
                    })
                    .collect();
                let entry = TopicIndexEntry {
                    topic: t.as_bytes().to_vec(),
                    last_seq: 0,
                    subscribers: subs,
                };
                let data = TopicIndex::serialize(&entry).unwrap();
                let _ = ctrl
                    .stores()
                    .topics
                    .apply_replicated(Operation::Insert, t, &data);
            }
            4 => {
                let _ = ctrl
                    .stores()
                    .topics
                    .apply_replicated(Operation::Delete, t, &[]);
            }
            5 => {
                ctrl.stores().topics.clear_partition(topic_partition(t));
            }
            6 => {
                let _ = other.subscribe(t, c, session_partition(c), q);
                let _ = ctrl.stores().topics.import_entries(
                    &other.export_for_partition(topic_partition(t)),
                    &std::collections::HashSet::new(),
                );
            }
            7 | 8 => {
                let _ = if next(2) == 0 {
                    ctrl.stores().wildcards.subscribe_mqtt(p, c, q)
                } else {
                    ctrl.stores().wildcards.subscribe_db(p, c, q)
                };
            }
            9 => {
                let _ = ctrl.stores().wildcards.unsubscribe(p, c);
            }
            _ => {
                ctrl.stores()
                    .wildcards
                    .clear_partition(session_partition(c));
            }
        }
        assert_eq!(
            ctrl.listing_digest(&all),
            crate::cluster::protocol::ListingDigest::of(&ctrl.resynced_clients(&all)),
            "digest drifted at step {step}"
        );
    }
}

#[tokio::test]
async fn a_resumed_client_with_many_subscriptions_is_announced_in_parts() {
    let ctrl = controller(1);
    let topics = heavy_client("heavy", 3_000);
    ctrl.stores().client_locations.claim("heavy", node(1));
    for topic in &topics {
        ctrl.stores()
            .topics
            .subscribe(topic, "heavy", session_partition("heavy"), 1)
            .unwrap();
    }

    ctrl.announce_resumed_client("heavy").await;

    let announced: Vec<crate::cluster::protocol::SubscriptionResync> = ctrl
        .transport
        .take_sent()
        .into_iter()
        .filter_map(|(_, m)| match m {
            ClusterMessage::SubscriptionResync(resync) => Some(resync),
            _ => None,
        })
        .collect();
    assert!(announced.len() > 1);
    assert!(
        announced
            .iter()
            .all(|m| m.to_bytes().unwrap().len() <= 1024 * 1024)
    );
    let mut sent: Vec<String> = announced
        .into_iter()
        .flat_map(|m| m.clients)
        .flat_map(|c| c.topics)
        .map(|t| t.topic)
        .collect();
    sent.sort();
    assert_eq!(sent, topics);
}

#[tokio::test]
async fn a_snapshot_import_on_the_owner_does_not_spread_a_lost_subscription() {
    let owned = "owned";
    let topic = "t/x";
    let mut owner = controller(1);
    owner.stores().client_locations.claim(owned, node(1));
    owner
        .stores()
        .topics
        .subscribe(topic, owned, session_partition(owned), 1)
        .unwrap();
    let mut asker = controller(2);
    revive(&asker, node(1));
    asker.process_messages().await;
    answer_resyncs(&mut asker, &mut owner).await;
    assert_eq!(
        asker.stores().topics.get_client_topics(owned),
        [(topic.to_string(), 1)]
    );

    let source = controller(3);
    source
        .stores()
        .topics
        .subscribe(topic, "other", session_partition("other"), 0)
        .unwrap();
    let partition = crate::cluster::topic_partition(topic);
    let snapshot = source.stores().export_partition(partition).unwrap();
    owner
        .stores()
        .import_partition(partition, &snapshot)
        .unwrap();

    run_periodic_resync(&mut asker).await;
    answer_resyncs(&mut asker, &mut owner).await;

    assert_eq!(
        owner.stores().topics.get_client_topics(owned),
        [(topic.to_string(), 1)],
        "the owner's own index lost its local client's subscription"
    );
    assert_eq!(
        asker.stores().topics.get_client_topics(owned),
        [(topic.to_string(), 1)],
        "the asker dropped a subscription the client still holds on its own node"
    );
}

#[tokio::test]
async fn the_asker_keeps_a_subscription_the_owner_had_before_a_snapshot() {
    let owned = "owned";
    let topic = "t/x";
    let mut owner = controller(1);
    owner.stores().client_locations.claim(owned, node(1));
    owner
        .stores()
        .topics
        .subscribe(topic, owned, session_partition(owned), 1)
        .unwrap();
    let mut asker = controller(2);
    revive(&asker, node(1));
    asker.process_messages().await;
    answer_resyncs(&mut asker, &mut owner).await;
    assert_eq!(
        asker.stores().topics.get_client_topics(owned),
        [(topic.to_string(), 1)]
    );

    let source = controller(3);
    source
        .stores()
        .topics
        .subscribe(topic, "other", session_partition("other"), 0)
        .unwrap();
    let partition = crate::cluster::topic_partition(topic);
    let snapshot = source.stores().export_partition(partition).unwrap();
    owner
        .stores()
        .import_partition(partition, &snapshot)
        .unwrap();

    run_periodic_resync(&mut asker).await;
    answer_resyncs(&mut asker, &mut owner).await;

    assert_eq!(
        asker.stores().topics.get_client_topics(owned),
        [(topic.to_string(), 1)],
        "the asker dropped a subscription the client still holds on its own node"
    );
}

#[tokio::test]
async fn a_replicated_write_naming_this_node_does_not_make_it_list_a_client_it_does_not_hold() {
    let ghost = "ghost";
    let mut owner = controller(1);
    owner
        .stores()
        .topics
        .subscribe("g/t", ghost, session_partition(ghost), 1)
        .unwrap();
    let entry = crate::cluster::ClientLocationEntry::create(ghost, node(1));
    let replicated_insert = ReplicationWrite::new(
        session_partition(ghost),
        Operation::Insert,
        Epoch::new(0),
        0,
        crate::cluster::entity::CLIENT_LOCATIONS.to_string(),
        crate::cluster::client_location_key(ghost),
        bebytes::BeBytes::to_be_bytes(&entry),
    );
    owner.stores().apply_write(&replicated_insert).unwrap();

    let mut asker = controller(2);
    revive(&asker, node(1));
    asker.process_messages().await;
    answer_resyncs(&mut asker, &mut owner).await;

    assert_eq!(
        asker.connected_nodes(ghost),
        Vec::<NodeId>::new(),
        "node 1 lists a client that never connected to it"
    );
}

#[tokio::test]
async fn a_replicated_write_naming_this_node_does_not_outlive_the_clients_next_connect() {
    let ghost = "ghost";
    let mut owner = controller(1);
    let entry = crate::cluster::ClientLocationEntry::create(ghost, node(1));
    let replicated_insert = ReplicationWrite::new(
        session_partition(ghost),
        Operation::Insert,
        Epoch::new(0),
        0,
        crate::cluster::entity::CLIENT_LOCATIONS.to_string(),
        crate::cluster::client_location_key(ghost),
        bebytes::BeBytes::to_be_bytes(&entry),
    );
    owner.stores().apply_write(&replicated_insert).unwrap();

    owner
        .transport
        .inject_message(node(3), location_write(ghost, node(3), Operation::Insert));
    owner.process_messages().await;

    assert_eq!(
        owner.connected_node(ghost),
        Some(node(3)),
        "$share and will routing still pick this node for a client connected to node 3"
    );
}
