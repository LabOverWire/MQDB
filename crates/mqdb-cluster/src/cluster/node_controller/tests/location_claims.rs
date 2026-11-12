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

fn ordered_location_write(
    client_id: &str,
    on: NodeId,
    order: crate::cluster::LocationStamp,
    operation: Operation,
) -> ClusterMessage {
    let entry = crate::cluster::ClientLocationEntry::create(client_id, on);
    ClusterMessage::WriteRequest(ReplicationWrite::new(
        session_partition(client_id),
        operation,
        Epoch::new(0),
        0,
        crate::cluster::entity::CLIENT_LOCATIONS.to_string(),
        crate::cluster::client_location_key(client_id),
        entry.encode_ordered(order),
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
        .claim("gone-client", peer.node_id(), None);

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
    ctrl.stores().client_locations.connect_here("resumer");
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
    new.stores().client_locations.connect_here("hopper");
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
    old.stores().client_locations.connect_here(client);
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
    peer.stores().client_locations.connect_here("steady");
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
    peer.stores().client_locations.connect_here("heavy");
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
    peer.stores().client_locations.connect_here("light");
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
    peer.stores().client_locations.connect_here("steady");
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
    peer.stores().client_locations.connect_here("steady");
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
        ctrl.listing_digest(ctrl.node_id(), &clients),
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
                    order: 0,
                    instance: 0,
                    topics: vec![crate::cluster::protocol::ResyncedTopic {
                        topic: "kept/a".to_string(),
                        qos: 1,
                    }],
                    patterns: Vec::new(),
                },
                crate::cluster::protocol::ResyncedClient {
                    client_id: "listed-late".to_string(),
                    order: 0,
                    instance: 0,
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
    peer.stores().client_locations.connect_here("heavy");
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
    peer.stores().client_locations.connect_here("light");
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
            ctrl.listing_digest(ctrl.node_id(), &all),
            crate::cluster::protocol::ListingDigest::of(&ctrl.resynced_clients(&all)),
            "digest drifted at step {step}"
        );
    }
}

#[tokio::test]
async fn a_resumed_client_with_many_subscriptions_is_announced_in_parts() {
    let ctrl = controller(1);
    let topics = heavy_client("heavy", 3_000);
    ctrl.stores().client_locations.connect_here("heavy");
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
    owner.stores().client_locations.connect_here(owned);
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
    owner.stores().client_locations.connect_here(owned);
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

#[tokio::test]
async fn a_wildcard_import_does_not_restore_a_pattern_a_local_client_left() {
    let owned = "owned";
    let mut owner = controller(1);
    owner.stores().client_locations.connect_here(owned);
    let source = controller(3);
    source
        .stores()
        .wildcards
        .subscribe_mqtt("w/+", owned, 1)
        .unwrap();
    let partition = session_partition(owned);
    let snapshot = source.stores().export_partition(partition).unwrap();
    owner
        .stores()
        .wildcards
        .subscribe_mqtt("w/+", owned, 1)
        .unwrap();
    owner.stores().wildcards.unsubscribe("w/+", owned).unwrap();

    owner
        .stores()
        .import_partition(partition, &snapshot)
        .unwrap();

    let clients = std::collections::HashSet::from([owned.to_string()]);
    let mut asker = controller(2);
    revive(&asker, node(1));
    asker.process_messages().await;
    answer_resyncs(&mut asker, &mut owner).await;

    assert!(
        asker.stores().wildcards.patterns_of(&clients).is_empty(),
        "a pattern the local client left was restored by the import and spread to the asker"
    );
    assert!(
        owner.stores().wildcards.patterns_of(&clients).is_empty(),
        "the import restored a pattern the local client left"
    );
}

#[tokio::test]
async fn an_import_keeps_a_subscription_a_moved_client_made_on_its_new_node() {
    let mover = "mover";
    let topic = "t/x";
    let mut old = controller(1);
    old.stores().client_locations.connect_here(mover);
    old.transport
        .inject_message(node(3), location_write(mover, node(3), Operation::Insert));
    old.process_messages().await;
    assert_eq!(
        old.stores().client_locations.claimants(mover),
        [node(1), node(3)]
    );

    let source = controller(3);
    source
        .stores()
        .topics
        .subscribe(topic, mover, session_partition(mover), 1)
        .unwrap();
    let partition = crate::cluster::topic_partition(topic);
    let snapshot = source.stores().export_partition(partition).unwrap();
    old.stores().import_partition(partition, &snapshot).unwrap();

    assert_eq!(
        old.stores().topics.get_client_topics(mover),
        [(topic.to_string(), 1)],
        "the import dropped the subscription the client made on node 3"
    );
}

#[tokio::test]
async fn a_resync_from_the_new_node_reaches_a_client_still_held_here() {
    let mover = "mover";
    let topic = "t/x";
    let mut old = controller(1);
    old.stores().client_locations.connect_here(mover);
    let mut new = controller(3);
    new.stores().client_locations.connect_here(mover);
    new.stores()
        .topics
        .subscribe(topic, mover, session_partition(mover), 1)
        .unwrap();
    old.transport
        .inject_message(node(3), location_write(mover, node(3), Operation::Insert));
    old.process_messages().await;

    let partition = crate::cluster::topic_partition(topic);
    let snapshot = new.stores().export_partition(partition).unwrap();
    old.stores().import_partition(partition, &snapshot).unwrap();
    revive(&old, node(3));
    old.process_messages().await;
    answer_resyncs(&mut old, &mut new).await;
    run_periodic_resync(&mut old).await;
    answer_resyncs(&mut old, &mut new).await;

    assert_eq!(
        old.stores().topics.get_client_topics(mover),
        [(topic.to_string(), 1)],
        "node 1 never relearns the subscription while it still claims the client"
    );
}

#[tokio::test]
async fn a_resync_adds_what_a_client_still_held_here_subscribed_on_its_newer_node() {
    let mover = "mover";
    let mut old = controller(1);
    old.stores().client_locations.connect_here(mover);
    let mut new = controller(3);
    new.stores().client_locations.connect_here(mover);
    new.stores()
        .topics
        .subscribe("t/new", mover, session_partition(mover), 1)
        .unwrap();
    old.transport
        .inject_message(node(3), location_write(mover, node(3), Operation::Insert));
    old.process_messages().await;
    assert_eq!(
        old.stores().client_locations.claimants(mover),
        [node(1), node(3)]
    );
    assert!(old.stores().topics.get_client_topics(mover).is_empty());

    revive(&old, node(3));
    old.process_messages().await;
    answer_resyncs(&mut old, &mut new).await;

    assert_eq!(
        old.stores().topics.get_client_topics(mover),
        [("t/new".to_string(), 1)]
    );
}

#[tokio::test]
async fn two_close_connects_leave_one_authority() {
    let dup = "dup";
    let mut first = controller(1);
    let order_1 = first.stores().client_locations.connect_here(dup);
    let mut second = controller(3);
    let order_3 = second.stores().client_locations.connect_here(dup);
    first.transport.inject_message(
        node(3),
        ordered_location_write(dup, node(3), order_3, Operation::Insert),
    );
    first.process_messages().await;
    second.transport.inject_message(
        node(1),
        ordered_location_write(dup, node(1), order_1, Operation::Insert),
    );
    second.process_messages().await;

    let authorities = [
        first.stores().client_locations.is_authority_here(dup),
        second.stores().client_locations.is_authority_here(dup),
    ];
    assert_eq!(
        authorities.iter().filter(|here| **here).count(),
        1,
        "authority on nodes 1 and 3: {authorities:?}"
    );
}

#[tokio::test]
async fn an_import_on_the_newest_node_after_a_late_older_connect_keeps_its_subscription() {
    let dup = "dup";
    let order_1 = crate::cluster::LocationStamp {
        order: crate::cluster::LocationOrder(1),
        instance: 1,
    };
    let topic = "t/x";
    let mut latest = controller(3);
    let order_3 = latest.stores().client_locations.connect_here(dup);
    latest
        .stores()
        .topics
        .subscribe(topic, dup, session_partition(dup), 1)
        .unwrap();
    latest.transport.inject_message(
        node(1),
        ordered_location_write(dup, node(1), order_1, Operation::Insert),
    );
    latest.process_messages().await;

    let source = controller(4);
    source
        .stores()
        .topics
        .subscribe(topic, "other", session_partition("other"), 0)
        .unwrap();
    let partition = crate::cluster::topic_partition(topic);
    let snapshot = source.stores().export_partition(partition).unwrap();
    latest
        .stores()
        .import_partition(partition, &snapshot)
        .unwrap();

    let mut third = controller(2);
    third.transport.inject_message(
        node(1),
        ordered_location_write(dup, node(1), order_1, Operation::Insert),
    );
    third.transport.inject_message(
        node(3),
        ordered_location_write(dup, node(3), order_3, Operation::Insert),
    );
    third.process_messages().await;
    third
        .stores()
        .topics
        .subscribe(topic, dup, session_partition(dup), 1)
        .unwrap();
    revive(&third, node(3));
    third.process_messages().await;
    answer_resyncs(&mut third, &mut latest).await;

    assert_eq!(
        (
            latest.stores().topics.get_client_topics(dup),
            third.stores().topics.get_client_topics(dup),
        ),
        (vec![(topic.to_string(), 1)], vec![(topic.to_string(), 1)]),
        "(node 3 own index, node 2 index): publishes on node 2 to {topic} no longer reach the client on node 3"
    );
}

#[tokio::test]
async fn a_connect_heard_before_the_restarted_nodes_heartbeat_keeps_the_single_location() {
    let dup = "dup";
    let mut old = controller(1);
    let order_1 = old.stores().client_locations.connect_here(dup);
    let mut restarted = controller(3);
    let order_3 = restarted.stores().client_locations.connect_here(dup);

    let mut third = controller(2);
    third.transport.inject_message(
        node(1),
        ordered_location_write(dup, node(1), order_1, Operation::Insert),
    );
    third
        .transport
        .inject_message(node(1), ClusterMessage::DeathNotice { node_id: node(3) });
    third.process_messages().await;
    third.transport.inject_message(
        node(3),
        ordered_location_write(dup, node(3), order_3, Operation::Insert),
    );
    third.process_messages().await;
    revive(&third, node(3));
    third.process_messages().await;
    answer_resyncs(&mut third, &mut restarted).await;
    revive(&third, node(1));
    third.process_messages().await;
    answer_resyncs(&mut third, &mut old).await;

    assert_eq!(
        third.connected_node(dup),
        Some(node(3)),
        "$share and will routing on node 2 pick the older connection on node 1"
    );
}

#[tokio::test]
async fn control_import_without_the_late_older_connect() {
    let dup = "dup";
    let order_1 = crate::cluster::LocationStamp {
        order: crate::cluster::LocationOrder(1),
        instance: 1,
    };
    let topic = "t/x";
    let mut latest = controller(3);
    let order_3 = latest.stores().client_locations.connect_here(dup);
    latest
        .stores()
        .topics
        .subscribe(topic, dup, session_partition(dup), 1)
        .unwrap();

    let source = controller(4);
    source
        .stores()
        .topics
        .subscribe(topic, "other", session_partition("other"), 0)
        .unwrap();
    let partition = crate::cluster::topic_partition(topic);
    let snapshot = source.stores().export_partition(partition).unwrap();
    latest
        .stores()
        .import_partition(partition, &snapshot)
        .unwrap();

    let mut third = controller(2);
    third.transport.inject_message(
        node(1),
        ordered_location_write(dup, node(1), order_1, Operation::Insert),
    );
    third.transport.inject_message(
        node(3),
        ordered_location_write(dup, node(3), order_3, Operation::Insert),
    );
    third.process_messages().await;
    third
        .stores()
        .topics
        .subscribe(topic, dup, session_partition(dup), 1)
        .unwrap();
    revive(&third, node(3));
    third.process_messages().await;
    answer_resyncs(&mut third, &mut latest).await;

    assert_eq!(
        (
            latest.stores().topics.get_client_topics(dup),
            third.stores().topics.get_client_topics(dup),
        ),
        (vec![(topic.to_string(), 1)], vec![(topic.to_string(), 1)]),
        "(node 3 own index, node 2 index): publishes on node 2 to {topic} no longer reach the client on node 3"
    );
}

#[tokio::test]
async fn control_location_without_the_death_notice() {
    let dup = "dup";
    let mut old = controller(1);
    let order_1 = old.stores().client_locations.connect_here(dup);
    let mut restarted = controller(3);
    let order_3 = restarted.stores().client_locations.connect_here(dup);

    let mut third = controller(2);
    third.transport.inject_message(
        node(1),
        ordered_location_write(dup, node(1), order_1, Operation::Insert),
    );
    third.process_messages().await;
    third.transport.inject_message(
        node(3),
        ordered_location_write(dup, node(3), order_3, Operation::Insert),
    );
    third.process_messages().await;
    revive(&third, node(3));
    third.process_messages().await;
    answer_resyncs(&mut third, &mut restarted).await;
    revive(&third, node(1));
    third.process_messages().await;
    answer_resyncs(&mut third, &mut old).await;

    assert_eq!(
        third.connected_node(dup),
        Some(node(3)),
        "$share and will routing on node 2 pick the older connection on node 1"
    );
}

#[tokio::test]
async fn a_connect_heard_before_the_restarted_nodes_heartbeat_leaves_one_authority() {
    let dup = "dup";
    let mut old = controller(1);
    old.stores().client_locations.connect_here(dup);
    let mut restarted = controller(3);
    let order_3 = restarted.stores().client_locations.connect_here(dup);
    restarted
        .stores()
        .topics
        .subscribe("t/new", dup, session_partition(dup), 1)
        .unwrap();

    old.transport
        .inject_message(node(2), ClusterMessage::DeathNotice { node_id: node(3) });
    old.process_messages().await;
    old.transport.inject_message(
        node(3),
        ordered_location_write(dup, node(3), order_3, Operation::Insert),
    );
    old.process_messages().await;
    revive(&old, node(3));
    old.process_messages().await;
    answer_resyncs(&mut old, &mut restarted).await;
    revive(&restarted, node(1));
    restarted.process_messages().await;
    answer_resyncs(&mut restarted, &mut old).await;

    assert_eq!(
        (
            old.stores().client_locations.is_authority_here(dup),
            restarted.stores().client_locations.is_authority_here(dup),
            old.stores().topics.get_client_topics(dup),
        ),
        (false, true, vec![("t/new".to_string(), 1)]),
        "(node 1 authority, node 3 authority, node 1 index of the client)"
    );
}

#[tokio::test]
async fn a_resume_announced_by_the_restarted_node_reaches_a_node_that_missed_its_connect() {
    let dup = "dup";
    let mut old = controller(1);
    old.stores().client_locations.connect_here(dup);
    let restarted = controller(3);
    let order_3 = restarted.stores().client_locations.connect_here(dup);
    restarted
        .stores()
        .topics
        .subscribe("t/new", dup, session_partition(dup), 1)
        .unwrap();

    old.transport
        .inject_message(node(2), ClusterMessage::DeathNotice { node_id: node(3) });
    old.process_messages().await;
    old.transport.inject_message(
        node(3),
        ordered_location_write(dup, node(3), order_3, Operation::Insert),
    );
    restarted.announce_resumed_client(dup).await;
    for (_, message) in restarted.transport.take_sent() {
        if matches!(message, ClusterMessage::SubscriptionResync(_)) {
            old.transport.inject_message(node(3), message);
        }
    }
    old.process_messages().await;

    assert_eq!(
        old.stores().topics.get_client_topics(dup),
        [("t/new".to_string(), 1)],
        "node 1 ignored the resume announcement of the client's newest connection"
    );
}

#[tokio::test]
async fn control_resume_without_the_death_notice() {
    let dup = "dup";
    let mut old = controller(1);
    old.stores().client_locations.connect_here(dup);
    let restarted = controller(3);
    let order_3 = restarted.stores().client_locations.connect_here(dup);
    restarted
        .stores()
        .topics
        .subscribe("t/new", dup, session_partition(dup), 1)
        .unwrap();

    old.transport.inject_message(
        node(3),
        ordered_location_write(dup, node(3), order_3, Operation::Insert),
    );
    restarted.announce_resumed_client(dup).await;
    for (_, message) in restarted.transport.take_sent() {
        if matches!(message, ClusterMessage::SubscriptionResync(_)) {
            old.transport.inject_message(node(3), message);
        }
    }
    old.process_messages().await;

    assert_eq!(
        old.stores().topics.get_client_topics(dup),
        [("t/new".to_string(), 1)],
        "node 1 ignored the resume announcement of the client's newest connection"
    );
}

#[tokio::test]
async fn a_resync_listing_ranks_a_lower_numbered_node_by_its_connect_order() {
    let mover = "mover";
    let earlier = controller(3);
    let first_order = earlier.stores().client_locations.connect_here(mover);
    let mut later = controller(1);
    later.transport.inject_message(
        node(3),
        ordered_location_write(mover, node(3), first_order, Operation::Insert),
    );
    later.process_messages().await;
    let second_order = later.stores().client_locations.connect_here(mover);
    assert!(second_order.order.0 > first_order.order.0);

    let mut third = controller(2);
    third.transport.inject_message(
        node(3),
        ordered_location_write(mover, node(3), first_order, Operation::Insert),
    );
    third.process_messages().await;
    revive(&third, node(1));
    third.process_messages().await;
    answer_resyncs(&mut third, &mut later).await;

    assert_eq!(third.connected_node(mover), Some(node(1)));
    assert_eq!(third.connected_nodes(mover), [node(3), node(1)]);
}

#[tokio::test]
async fn a_late_copy_of_an_older_disconnect_keeps_the_nodes_reconnect() {
    let client = "flapper";
    let first = controller(1);
    let first_order = first.stores().client_locations.connect_here(client);
    first.stores().client_locations.disconnect_here(client);
    let second_order = first.stores().client_locations.connect_here(client);
    assert!(second_order.order.0 <= first_order.order.0);

    let mut ctrl = controller(2);
    ctrl.transport.inject_message(
        node(1),
        ordered_location_write(client, node(1), first_order, Operation::Insert),
    );
    ctrl.transport.inject_message(
        node(1),
        ordered_location_write(client, node(1), first_order, Operation::Delete),
    );
    ctrl.transport.inject_message(
        node(1),
        ordered_location_write(client, node(1), second_order, Operation::Insert),
    );
    ctrl.process_messages().await;
    ctrl.transport.inject_message(
        node(3),
        ordered_location_write(client, node(1), first_order, Operation::Delete),
    );
    ctrl.process_messages().await;

    assert_eq!(ctrl.connected_nodes(client), [node(1)]);
}

async fn share_target_after_restarted_node_connects(
    restarted_learned_first: bool,
) -> Option<NodeId> {
    let client = "roamer";
    let crashed = controller(1);
    let order_on_1 = crashed.stores().client_locations.connect_here(client);
    let mut moved_to = controller(3);
    moved_to.transport.inject_message(
        node(1),
        ordered_location_write(client, node(1), order_on_1, Operation::Insert),
    );
    moved_to.process_messages().await;
    let order_on_3 = moved_to.stores().client_locations.connect_here(client);

    let mut restarted = controller(1);
    if restarted_learned_first {
        restarted.transport.inject_message(
            node(3),
            ordered_location_write(client, node(3), order_on_3, Operation::Insert),
        );
        restarted.process_messages().await;
    }
    let order_after_restart = restarted.stores().client_locations.connect_here(client);

    let mut third = controller(2);
    third.transport.inject_message(
        node(1),
        ordered_location_write(client, node(1), order_on_1, Operation::Insert),
    );
    third.transport.inject_message(
        node(3),
        ordered_location_write(client, node(3), order_on_3, Operation::Insert),
    );
    third.process_messages().await;
    third.transport.inject_message(
        node(1),
        ordered_location_write(client, node(1), order_after_restart, Operation::Insert),
    );
    third.process_messages().await;
    third.connected_node(client)
}

#[tokio::test]
async fn a_restarted_node_that_knew_the_older_claim_outranks_it() {
    assert_eq!(
        share_target_after_restarted_node_connects(true).await,
        Some(node(1)),
        "$share and will routing on node 2 pick the client's older connection on node 3"
    );
}

async fn older_node_index_after_resync(restarted_learned_first: bool) -> Vec<(String, u8)> {
    let client = "roamer";
    let crashed = controller(1);
    let order_on_1 = crashed.stores().client_locations.connect_here(client);
    let mut older = controller(3);
    older.transport.inject_message(
        node(1),
        ordered_location_write(client, node(1), order_on_1, Operation::Insert),
    );
    older.process_messages().await;
    let order_on_3 = older.stores().client_locations.connect_here(client);

    let mut restarted = controller(1);
    if restarted_learned_first {
        restarted.transport.inject_message(
            node(3),
            ordered_location_write(client, node(3), order_on_3, Operation::Insert),
        );
        restarted.process_messages().await;
    }
    let order_after_restart = restarted.stores().client_locations.connect_here(client);
    restarted
        .stores()
        .topics
        .subscribe("t/new", client, session_partition(client), 1)
        .unwrap();

    older.transport.inject_message(
        node(1),
        ordered_location_write(client, node(1), order_after_restart, Operation::Insert),
    );
    older.process_messages().await;
    revive(&older, node(1));
    older.process_messages().await;
    answer_resyncs(&mut older, &mut restarted).await;
    older.stores().topics.get_client_topics(client)
}

#[tokio::test]
async fn a_resync_from_a_restarted_node_that_knew_the_older_claim_reaches_the_older_node() {
    assert_eq!(
        older_node_index_after_resync(true).await,
        [("t/new".to_string(), 1)],
        "node 3 kept its own view of the client and ignored the subscription made on its newest connection"
    );
}

async fn legacy_location_after_old_peer_goes_quiet(declared_dead: bool) -> Vec<NodeId> {
    let mut ctrl = controller(2);
    revive(&ctrl, node(1));
    ctrl.process_messages().await;
    ctrl.transport.inject_message(
        node(1),
        location_write("legacy", node(1), Operation::Insert),
    );
    ctrl.process_messages().await;
    assert_eq!(ctrl.connected_nodes("legacy"), [node(1)]);

    if declared_dead {
        ctrl.apply_dead_nodes(&[node(1)]);
        revive(&ctrl, node(1));
        ctrl.process_messages().await;
    }
    let mut now = LATER_MS;
    for _ in 0..2_000 {
        ctrl.drive_subscription_resyncs(now).await;
        ctrl.transport.take_sent();
        now += crate::cluster::node_controller::subscription_resync::RESYNC_RETRY_MS;
    }
    ctrl.connected_nodes("legacy")
}

#[tokio::test]
async fn an_old_version_peer_missing_heartbeats_keeps_its_clients_routed() {
    assert_eq!(
        legacy_location_after_old_peer_goes_quiet(true).await,
        [node(1)],
        "publishes on node 2 are no longer forwarded to the client still connected to old-version node 1"
    );
}

#[tokio::test]
async fn an_old_version_peer_without_missed_heartbeats_keeps_its_clients_routed() {
    assert_eq!(
        legacy_location_after_old_peer_goes_quiet(false).await,
        [node(1)],
        "publishes on node 2 are no longer forwarded to the client still connected to old-version node 1"
    );
}

fn location_writes_to(
    sender: &NodeController<MockTransport>,
    target: NodeId,
) -> Vec<ClusterMessage> {
    sender
        .transport
        .take_sent()
        .into_iter()
        .filter(|(to, message)| {
            *to == target
                && matches!(
                    message,
                    ClusterMessage::WriteRequest(_) | ClusterMessage::SubscriptionResync(_)
                )
        })
        .map(|(_, message)| message)
        .collect()
}

async fn share_target_after_the_restarted_nodes_boot_resync() -> Option<NodeId> {
    let client = "roamer";
    let crashed = controller(1);
    let order_on_1 = crashed.stores().client_locations.connect_here(client);
    let mut moved_to = controller(3);
    moved_to.transport.inject_message(
        node(1),
        ordered_location_write(client, node(1), order_on_1, Operation::Insert),
    );
    moved_to.process_messages().await;
    let order_on_3 = moved_to.stores().client_locations.connect_here(client);

    let mut restarted = controller(1);
    let order_after_restart = restarted.stores().client_locations.connect_here(client);
    restarted.note_local_connect(client);

    let mut third = controller(2);
    third.transport.inject_message(
        node(1),
        ordered_location_write(client, node(1), order_on_1, Operation::Insert),
    );
    third.transport.inject_message(
        node(3),
        ordered_location_write(client, node(3), order_on_3, Operation::Insert),
    );
    third.process_messages().await;
    third.transport.inject_message(
        node(1),
        ordered_location_write(client, node(1), order_after_restart, Operation::Insert),
    );
    third.process_messages().await;

    revive(&restarted, node(2));
    revive(&restarted, node(3));
    restarted.process_messages().await;
    answer_resyncs(&mut restarted, &mut moved_to).await;
    for message in location_writes_to(&restarted, node(2)) {
        third.transport.inject_message(node(1), message);
    }
    third.process_messages().await;
    third.connected_node(client)
}

#[tokio::test]
async fn a_connect_on_a_restarted_node_outranks_the_clients_older_open_connection() {
    assert_eq!(
        share_target_after_the_restarted_nodes_boot_resync().await,
        Some(node(1)),
        "$share and will routing on node 2 pick the client's older connection on node 3"
    );
}

async fn older_node_index_after_the_restarted_nodes_boot_resync() -> Vec<(String, u8)> {
    let client = "roamer";
    let crashed = controller(1);
    let order_on_1 = crashed.stores().client_locations.connect_here(client);
    let mut older = controller(3);
    older.transport.inject_message(
        node(1),
        ordered_location_write(client, node(1), order_on_1, Operation::Insert),
    );
    older.process_messages().await;
    older.stores().client_locations.connect_here(client);

    let mut restarted = controller(1);
    let order_after_restart = restarted.stores().client_locations.connect_here(client);
    restarted.note_local_connect(client);
    restarted
        .stores()
        .topics
        .subscribe("t/new", client, session_partition(client), 1)
        .unwrap();

    older.transport.inject_message(
        node(1),
        ordered_location_write(client, node(1), order_after_restart, Operation::Insert),
    );
    older.process_messages().await;

    revive(&restarted, node(3));
    restarted.process_messages().await;
    answer_resyncs(&mut restarted, &mut older).await;
    for message in location_writes_to(&restarted, node(3)) {
        older.transport.inject_message(node(1), message);
    }
    older.process_messages().await;

    revive(&older, node(1));
    older.process_messages().await;
    answer_resyncs(&mut older, &mut restarted).await;
    older.stores().topics.get_client_topics(client)
}

#[tokio::test]
async fn a_resync_from_a_restarted_node_reaches_the_node_holding_the_older_connection() {
    assert_eq!(
        older_node_index_after_the_restarted_nodes_boot_resync().await,
        [("t/new".to_string(), 1)],
        "node 3 kept its own view of the client and ignored the subscription made on its newest connection"
    );
}

#[tokio::test]
async fn a_connect_made_after_the_boot_resync_is_not_reranked_by_a_later_listing() {
    let client = "steady";
    let mut peer = controller(3);
    let mut local = controller(1);
    revive(&local, node(3));
    local.process_messages().await;
    answer_resyncs(&mut local, &mut peer).await;
    assert!(local.resync_rounds.is_empty(), "boot round not completed");

    let local_order = local.stores().client_locations.connect_here(client);
    local.note_local_connect(client);
    peer.transport.inject_message(
        node(1),
        ordered_location_write(client, node(1), local_order, Operation::Insert),
    );
    peer.process_messages().await;
    peer.stores().client_locations.connect_here(client);

    run_periodic_resync(&mut local).await;
    answer_resyncs(&mut local, &mut peer).await;

    assert_eq!(
        local.stores().client_locations.stamp_of(client, node(1)),
        Some(local_order)
    );
    assert!(!local.stores().client_locations.is_authority_here(client));
}

#[tokio::test]
async fn a_rerank_leaves_every_node_agreeing_on_the_newest_connection() {
    let client = "roamer";
    let crashed = controller(1);
    let order_on_1 = crashed.stores().client_locations.connect_here(client);
    let mut moved_to = controller(3);
    moved_to.transport.inject_message(
        node(1),
        ordered_location_write(client, node(1), order_on_1, Operation::Insert),
    );
    moved_to.process_messages().await;
    let order_on_3 = moved_to.stores().client_locations.connect_here(client);

    let mut restarted = controller(1);
    let order_after_restart = restarted.stores().client_locations.connect_here(client);
    restarted.note_local_connect(client);
    let mut third = controller(2);
    for (from, stamp) in [
        (node(1), order_on_1),
        (node(3), order_on_3),
        (node(1), order_after_restart),
    ] {
        third.transport.inject_message(
            from,
            ordered_location_write(client, from, stamp, Operation::Insert),
        );
        moved_to.transport.inject_message(
            from,
            ordered_location_write(client, from, stamp, Operation::Insert),
        );
    }
    third.process_messages().await;
    moved_to.process_messages().await;

    revive(&restarted, node(2));
    revive(&restarted, node(3));
    restarted.process_messages().await;
    answer_resyncs(&mut restarted, &mut moved_to).await;
    let sent = restarted.transport.take_sent();
    for (to, message) in sent {
        if matches!(message, ClusterMessage::WriteRequest(_)) {
            match to.get() {
                2 => third.transport.inject_message(node(1), message),
                3 => moved_to.transport.inject_message(node(1), message),
                _ => {}
            }
        }
    }
    third.process_messages().await;
    moved_to.process_messages().await;

    let views = [
        restarted.connected_node(client),
        third.connected_node(client),
        moved_to.stores().client_locations.get(client),
    ];
    assert_eq!(views, [Some(node(1)); 3]);
    let authorities = [
        restarted
            .stores()
            .client_locations
            .is_authority_here(client),
        third.stores().client_locations.is_authority_here(client),
        moved_to.stores().client_locations.is_authority_here(client),
    ];
    assert_eq!(authorities, [true, false, false]);
}

async fn restarted_after_boot_resync(
    restarted_learned_first: bool,
) -> (NodeController<MockTransport>, Vec<ClusterMessage>) {
    let client = "roamer";
    let crashed = controller(1);
    let order_on_1 = crashed.stores().client_locations.connect_here(client);
    let mut older = controller(3);
    older.transport.inject_message(
        node(1),
        ordered_location_write(client, node(1), order_on_1, Operation::Insert),
    );
    older.process_messages().await;
    let order_on_3 = older.stores().client_locations.connect_here(client);

    let mut restarted = controller(1);
    if restarted_learned_first {
        restarted.transport.inject_message(
            node(3),
            ordered_location_write(client, node(3), order_on_3, Operation::Insert),
        );
        restarted.process_messages().await;
    }
    restarted.stores().client_locations.connect_here(client);
    restarted.note_local_connect(client);
    restarted
        .stores()
        .topics
        .subscribe("t/new", client, session_partition(client), 1)
        .unwrap();
    revive(&restarted, node(3));
    restarted.process_messages().await;
    answer_resyncs(&mut restarted, &mut older).await;
    let sent = restarted
        .transport
        .take_sent()
        .into_iter()
        .filter(|(to, message)| match message {
            ClusterMessage::WriteRequest(_) => *to == node(3),
            ClusterMessage::SubscriptionResync(resync) => {
                resync.round == crate::cluster::protocol::SubscriptionResync::UNSOLICITED
            }
            _ => false,
        })
        .map(|(_, message)| message)
        .collect();
    (restarted, sent)
}

#[tokio::test]
async fn a_rerank_broadcasts_the_new_order_and_announces_the_clients_subscriptions() {
    let (restarted, sent) = restarted_after_boot_resync(false).await;
    let stamp = restarted
        .stores()
        .client_locations
        .stamp_of("roamer", node(1))
        .unwrap();
    assert_eq!(stamp.order, crate::cluster::LocationOrder(3));
    let orders: Vec<_> = sent
        .iter()
        .filter_map(|message| match message {
            ClusterMessage::WriteRequest(write) => {
                crate::cluster::ClientLocationEntry::decode_ordered(&write.data)
                    .and_then(|(_, stamp)| stamp)
            }
            _ => None,
        })
        .collect();
    assert_eq!(orders, [stamp]);
    let announced: Vec<(u64, Vec<String>)> = sent
        .iter()
        .filter_map(|message| match message {
            ClusterMessage::SubscriptionResync(resync)
                if resync.round == crate::cluster::protocol::SubscriptionResync::UNSOLICITED =>
            {
                Some(resync.clients.clone())
            }
            _ => None,
        })
        .flatten()
        .map(|client| {
            (
                client.order,
                client.topics.into_iter().map(|t| t.topic).collect(),
            )
        })
        .collect();
    assert_eq!(announced, [(3, vec!["t/new".to_string()])]);
}

#[tokio::test]
async fn a_boot_connect_that_already_ranks_newest_is_not_reranked() {
    let (restarted, sent) = restarted_after_boot_resync(true).await;
    assert_eq!(
        restarted
            .stores()
            .client_locations
            .stamp_of("roamer", node(1))
            .map(|stamp| stamp.order),
        Some(crate::cluster::LocationOrder(3))
    );
    assert!(sent.is_empty());
}

#[tokio::test]
async fn a_boot_connect_is_not_reranked_once_the_window_has_closed() {
    let client = "roamer";
    let mut older = controller(3);
    older.stores().client_locations.connect_here(client);
    let mut restarted = controller(1);
    restarted.stores().client_locations.connect_here(client);
    restarted.note_local_connect(client);
    older.transport.inject_message(
        node(1),
        ordered_location_write(
            client,
            node(1),
            restarted
                .stores()
                .client_locations
                .stamp_of(client, node(1))
                .unwrap(),
            Operation::Insert,
        ),
    );
    older.process_messages().await;
    restarted
        .stores()
        .client_locations
        .claim(client, node(3), None);
    restarted.drive_subscription_resyncs(LATER_MS).await;
    restarted
        .drive_subscription_resyncs(
            LATER_MS + crate::cluster::node_controller::subscription_resync::BOOT_WINDOW_MS,
        )
        .await;
    restarted.transport.take_sent();

    revive(&restarted, node(3));
    restarted.process_messages().await;
    answer_resyncs(&mut restarted, &mut older).await;

    assert!(
        !restarted
            .stores()
            .client_locations
            .is_authority_here(client)
    );
    assert!(location_writes_to(&restarted, node(3)).is_empty());
}

#[tokio::test]
async fn a_peer_given_up_on_closes_its_boot_window_without_a_rerank() {
    let client = "roamer";
    let mut older = controller(3);
    let mut restarted = controller(1);
    let restarted_order = restarted.stores().client_locations.connect_here(client);
    restarted.note_local_connect(client);
    older.transport.inject_message(
        node(1),
        ordered_location_write(client, node(1), restarted_order, Operation::Insert),
    );
    older.process_messages().await;
    older.stores().client_locations.connect_here(client);
    revive(&restarted, node(3));
    restarted.process_messages().await;
    let late_requests: Vec<ClusterMessage> = restarted
        .transport
        .take_sent()
        .into_iter()
        .filter(|(to, m)| {
            *to == node(3) && matches!(m, ClusterMessage::SubscriptionResyncRequest(_))
        })
        .map(|(_, m)| m)
        .collect();
    let mut now = LATER_MS;
    for _ in 0..70 {
        restarted.drive_subscription_resyncs(now).await;
        restarted.transport.take_sent();
        now += crate::cluster::node_controller::subscription_resync::RESYNC_RETRY_MS;
    }
    assert!(
        restarted.resync_rounds.is_empty(),
        "the asks were not given up"
    );
    assert!(now - LATER_MS < crate::cluster::node_controller::subscription_resync::BOOT_WINDOW_MS);
    for request in late_requests {
        older.transport.inject_message(node(1), request);
    }
    older.process_messages().await;
    for (to, message) in older.transport.take_sent() {
        if to == node(1) && matches!(message, ClusterMessage::SubscriptionResync(_)) {
            restarted.transport.inject_message(node(3), message);
        }
    }
    restarted.process_messages().await;

    assert!(
        !restarted
            .stores()
            .client_locations
            .is_authority_here(client)
    );
    assert!(location_writes_to(&restarted, node(3)).is_empty());
}

async fn newest_after_a_move_heard_live(
    boot_resync_first: bool,
) -> (Option<NodeId>, Option<NodeId>) {
    let client = "mover12";
    let mut restarted = controller(1);
    let mut later = controller(3);
    if boot_resync_first {
        revive(&restarted, node(3));
        restarted.process_messages().await;
        answer_resyncs(&mut restarted, &mut later).await;
    }
    let order_on_1 = restarted.stores().client_locations.connect_here(client);
    restarted.note_local_connect(client);
    later.transport.inject_message(
        node(1),
        ordered_location_write(client, node(1), order_on_1, Operation::Insert),
    );
    later.process_messages().await;
    let order_on_3 = later.stores().client_locations.connect_here(client);
    restarted.transport.inject_message(
        node(3),
        ordered_location_write(client, node(3), order_on_3, Operation::Insert),
    );
    restarted.process_messages().await;

    if !boot_resync_first {
        revive(&restarted, node(3));
        restarted.process_messages().await;
        answer_resyncs(&mut restarted, &mut later).await;
    }
    for message in location_writes_to(&restarted, node(3)) {
        later.transport.inject_message(node(1), message);
    }
    later.process_messages().await;
    (
        restarted.stores().client_locations.get(client),
        later.stores().client_locations.get(client),
    )
}

#[tokio::test]
async fn a_move_heard_live_during_the_boot_window_is_not_outranked_by_the_rerank() {
    assert_eq!(
        newest_after_a_move_heard_live(false).await,
        (Some(node(3)), Some(node(3))),
        "(node 1 view, node 3 view): the re-rank put the older connection on node 1 above the client's move to node 3"
    );
}

#[tokio::test]
async fn a_move_heard_live_after_the_boot_resync_is_the_newest_connection() {
    assert_eq!(
        newest_after_a_move_heard_live(true).await,
        (Some(node(3)), Some(node(3))),
        "(node 1 view, node 3 view)"
    );
}

#[tokio::test]
async fn a_move_heard_live_is_not_outranked_when_the_listing_also_differs() {
    let client = "mover12";
    let mut restarted = controller(1);
    let mut later = controller(3);
    let order_on_1 = restarted.stores().client_locations.connect_here(client);
    restarted.note_local_connect(client);
    later.transport.inject_message(
        node(1),
        ordered_location_write(client, node(1), order_on_1, Operation::Insert),
    );
    later.process_messages().await;
    let order_on_3 = later.stores().client_locations.connect_here(client);
    later
        .stores()
        .topics
        .subscribe("t/missed", client, session_partition(client), 1)
        .unwrap();
    restarted.transport.inject_message(
        node(3),
        ordered_location_write(client, node(3), order_on_3, Operation::Insert),
    );
    restarted.process_messages().await;

    revive(&restarted, node(3));
    restarted.process_messages().await;
    answer_resyncs(&mut restarted, &mut later).await;

    assert!(location_writes_to(&restarted, node(3)).is_empty());
    assert_eq!(
        restarted.stores().client_locations.get(client),
        Some(node(3))
    );
    assert_eq!(
        restarted.stores().topics.get_client_topics(client),
        [("t/missed".to_string(), 1)]
    );
}

async fn share_target_after_old_peer_restart(revived: bool) -> Option<NodeId> {
    let client = "r14-mover";
    let old = node(3);
    let mut moved_to = controller(1);
    let mut third = controller(2);
    revive(&moved_to, old);
    revive(&third, old);
    revive(&third, node(1));
    moved_to.process_messages().await;
    third.process_messages().await;
    moved_to.transport.take_sent();
    third.transport.take_sent();

    moved_to
        .transport
        .inject_message(old, location_write(client, old, Operation::Insert));
    third
        .transport
        .inject_message(old, location_write(client, old, Operation::Insert));
    moved_to.process_messages().await;
    third.process_messages().await;
    assert_eq!(third.connected_node(client), Some(old));

    moved_to.apply_dead_nodes(&[old]);
    third.apply_dead_nodes(&[old]);
    let order_on_1 = moved_to.stores().client_locations.connect_here(client);
    third.transport.inject_message(
        node(1),
        ordered_location_write(client, node(1), order_on_1, Operation::Insert),
    );
    third.process_messages().await;

    if revived {
        revive(&third, old);
        third.process_messages().await;
    }
    let mut now = LATER_MS;
    for _ in 0..200 {
        third.drive_subscription_resyncs(now).await;
        third.transport.take_sent();
        now += crate::cluster::node_controller::subscription_resync::RESYNC_RETRY_MS;
    }
    third.connected_node(client)
}

#[tokio::test]
async fn a_restarted_old_peer_does_not_get_a_claim_back_above_a_newer_connect() {
    assert_eq!(
        share_target_after_old_peer_restart(true).await,
        Some(node(1)),
        "after the old-version node 3 restarted, $share and will routing on node 2 pick node 3 for a client now connected only on node 1"
    );
}

#[tokio::test]
async fn a_newer_connect_outranks_a_dead_old_peers_claim() {
    assert_eq!(
        share_target_after_old_peer_restart(false).await,
        Some(node(1))
    );
}

async fn location_of_client_connected_on_resumed_old_peer(
    write_before_heartbeat: bool,
) -> Vec<NodeId> {
    let client = "r14-fresh";
    let old = node(1);
    let mut ctrl = controller(2);
    revive(&ctrl, old);
    ctrl.process_messages().await;
    ctrl.transport.take_sent();
    ctrl.apply_dead_nodes(&[old]);

    if write_before_heartbeat {
        ctrl.transport
            .inject_message(old, location_write(client, old, Operation::Insert));
        ctrl.process_messages().await;
        revive(&ctrl, old);
        ctrl.process_messages().await;
    } else {
        revive(&ctrl, old);
        ctrl.process_messages().await;
        ctrl.transport
            .inject_message(old, location_write(client, old, Operation::Insert));
        ctrl.process_messages().await;
    }
    let mut now = LATER_MS;
    for _ in 0..2_000 {
        ctrl.drive_subscription_resyncs(now).await;
        ctrl.transport.take_sent();
        now += crate::cluster::node_controller::subscription_resync::RESYNC_RETRY_MS;
    }
    ctrl.connected_nodes(client)
}

#[tokio::test]
async fn a_connect_on_a_resumed_old_peer_heard_before_its_heartbeat_is_kept() {
    assert_eq!(
        location_of_client_connected_on_resumed_old_peer(true).await,
        [node(1)],
        "publishes on node 2 are never forwarded to a client that connected to old-version node 1 right after it resumed"
    );
}

#[tokio::test]
async fn a_connect_on_a_resumed_old_peer_heard_after_its_heartbeat_is_kept() {
    assert_eq!(
        location_of_client_connected_on_resumed_old_peer(false).await,
        [node(1)]
    );
}

async fn newest_after_boot_rerank(moved_to_third_node: bool) -> [Option<NodeId>; 2] {
    let client = "r14-flaky";
    let mut older = controller(3);
    let order_on_3 = older.stores().client_locations.connect_here(client);

    let mut restarted = controller(1);
    let order_on_1 = restarted.stores().client_locations.connect_here(client);
    restarted.note_local_connect(client);

    let mut third = controller(2);
    third.transport.inject_message(
        node(3),
        ordered_location_write(client, node(3), order_on_3, Operation::Insert),
    );
    third.transport.inject_message(
        node(1),
        ordered_location_write(client, node(1), order_on_1, Operation::Insert),
    );
    third.process_messages().await;

    revive(&restarted, node(3));
    restarted.process_messages().await;
    let asks_to_older: Vec<ClusterMessage> = restarted
        .transport
        .take_sent()
        .into_iter()
        .filter(|(to, m)| {
            *to == node(3) && matches!(m, ClusterMessage::SubscriptionResyncRequest(_))
        })
        .map(|(_, m)| m)
        .collect();
    assert_eq!(asks_to_older.len(), 1);
    revive(&restarted, node(2));
    restarted.process_messages().await;
    answer_resyncs(&mut restarted, &mut third).await;
    for ask in asks_to_older {
        restarted
            .transport
            .outbox
            .lock()
            .unwrap()
            .push((node(3), ask));
    }

    if moved_to_third_node {
        let order_on_2 = third.stores().client_locations.connect_here(client);
        restarted.transport.inject_message(
            node(2),
            ordered_location_write(client, node(2), order_on_2, Operation::Insert),
        );
        restarted.process_messages().await;
    }

    answer_resyncs(&mut restarted, &mut older).await;
    for message in location_writes_to(&restarted, node(2)) {
        if matches!(message, ClusterMessage::WriteRequest(_)) {
            third.transport.inject_message(node(1), message);
        }
    }
    third.process_messages().await;
    [
        restarted.stores().client_locations.get(client),
        third.stores().client_locations.get(client),
    ]
}

#[tokio::test]
async fn a_boot_rerank_does_not_outrank_a_newer_connect_heard_live_from_a_third_node() {
    assert_eq!(
        newest_after_boot_rerank(true).await,
        [Some(node(2)); 2],
        "(node 1 view, node 2 view): the boot re-rank put node 1's connection above the client's newer connection on node 2"
    );
}

#[tokio::test]
async fn a_boot_rerank_without_a_third_node_connect_ranks_the_restarted_node_newest() {
    assert_eq!(newest_after_boot_rerank(false).await, [Some(node(1)); 2]);
}

#[tokio::test]
async fn a_reply_for_an_untracked_round_teaches_nothing_for_the_boot_rerank() {
    let client = "taught";
    let mut restarted = controller(1);
    let order_on_1 = restarted.stores().client_locations.connect_here(client);
    restarted.note_local_connect(client);
    let mut later = controller(3);
    later.transport.inject_message(
        node(1),
        ordered_location_write(client, node(1), order_on_1, Operation::Insert),
    );
    later.process_messages().await;
    let order_on_3 = later.stores().client_locations.connect_here(client);

    restarted.transport.inject_message(
        node(3),
        ClusterMessage::SubscriptionResync(crate::cluster::protocol::SubscriptionResync {
            round: 12_345,
            part: 0,
            parts: 1,
            unchanged: false,
            continued: false,
            clients: vec![crate::cluster::protocol::ResyncedClient {
                client_id: client.to_string(),
                order: order_on_3.order.0,
                instance: order_on_3.instance,
                topics: Vec::new(),
                patterns: Vec::new(),
            }],
        }),
    );
    restarted.process_messages().await;
    restarted.transport.inject_message(
        node(3),
        ordered_location_write(client, node(3), order_on_3, Operation::Insert),
    );
    restarted.process_messages().await;

    revive(&restarted, node(3));
    restarted.process_messages().await;
    answer_resyncs(&mut restarted, &mut later).await;

    assert!(location_writes_to(&restarted, node(3)).is_empty());
    assert_eq!(
        restarted.stores().client_locations.get(client),
        Some(node(3))
    );
}
