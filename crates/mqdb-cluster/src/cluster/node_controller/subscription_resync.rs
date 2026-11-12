// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::super::client_location::{ClientLocationEntry, client_location_key};
use super::super::heartbeat::NodeStatus;
use super::super::protocol::{
    Heartbeat, ListingDigest, Operation, ReplicationWrite, ResyncRequest, ResyncedClient,
    ResyncedPattern, ResyncedTopic, SubscriptionResync,
};
use super::super::transport::{ClusterMessage, ClusterTransport};
use super::super::{Epoch, entity};
use super::super::{
    LocationOrder, LocationStamp, NodeId, SubscriptionType, is_response_topic, session_partition,
};
use super::NodeController;
use std::collections::{HashMap, HashSet};

const RESYNC_MESSAGE_BUDGET: usize = 1024 * 1024;
pub(crate) const RESYNC_RETRY_MS: u64 = 5_000;
pub(crate) const MAX_RESYNC_ATTEMPTS: u32 = 6;
pub(crate) const ANTI_ENTROPY_INTERVAL_MS: u64 = 300_000;
const TRACKED_ROUNDS_PER_PEER: usize = 2 * MAX_RESYNC_ATTEMPTS as usize;

pub(super) fn first_resync_round() -> u64 {
    super::super::client_location::boot_seeded_counter()
}

#[derive(Debug, Default)]
pub(crate) struct RoundProgress {
    next_part: u32,
    listed: HashSet<String>,
    digest: ListingDigest,
    partial: Option<ResyncedClient>,
}

pub(crate) const BOOT_WINDOW_MS: u64 = 2 * ANTI_ENTROPY_INTERVAL_MS;

#[derive(Debug, Default)]
pub(crate) struct BootWindow {
    started_ms: Option<u64>,
    closed: bool,
    settled: HashSet<NodeId>,
    connects: HashSet<String>,
    taught: HashMap<NodeId, HashSet<String>>,
    due: Vec<(String, NodeId)>,
}

#[derive(Debug)]
pub(crate) struct ResyncRound {
    ask_back: bool,
    attempts: u32,
    next_ask_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IndexUpdate {
    Replace,
    Add,
    Keep,
}

fn retry_delay_ms(attempts: u32) -> u64 {
    RESYNC_RETRY_MS.saturating_mul(1 << attempts.saturating_sub(1).min(16))
}

fn chunk_by_budget<T>(
    items: impl IntoIterator<Item = T>,
    overhead: usize,
    mut len_of: impl FnMut(&T) -> usize,
) -> Vec<Vec<T>> {
    let mut chunks: Vec<Vec<T>> = Vec::new();
    let mut current: Vec<T> = Vec::new();
    let mut current_len = overhead;
    for item in items {
        let item_len = len_of(&item);
        if !current.is_empty() && current_len + item_len > RESYNC_MESSAGE_BUDGET {
            chunks.push(std::mem::take(&mut current));
            current_len = overhead;
        }
        current_len += item_len;
        current.push(item);
    }
    chunks.push(current);
    chunks
}

fn pieces_of(client: ResyncedClient) -> Vec<ResyncedClient> {
    let empty_message = SubscriptionResync::encoded_len(&[]);
    if empty_message + client.encoded_len() <= RESYNC_MESSAGE_BUDGET {
        return vec![client];
    }
    let ResyncedClient {
        client_id,
        order,
        instance,
        topics,
        patterns,
    } = client;
    let bare = ResyncedClient {
        client_id,
        order,
        instance,
        topics: Vec::new(),
        patterns: Vec::new(),
    };
    let overhead = empty_message + bare.encoded_len();
    let topic_pieces = chunk_by_budget(topics, overhead, ResyncedTopic::encoded_len)
        .into_iter()
        .filter(|piece| !piece.is_empty())
        .map(|topics| ResyncedClient {
            topics,
            ..bare.clone()
        });
    let pattern_pieces = chunk_by_budget(patterns, overhead, ResyncedPattern::encoded_len)
        .into_iter()
        .filter(|piece| !piece.is_empty())
        .map(|patterns| ResyncedClient {
            patterns,
            ..bare.clone()
        });
    topic_pieces.chain(pattern_pieces).collect()
}

fn split_into_messages(
    clients: Vec<ResyncedClient>,
    round: u64,
) -> Option<Vec<SubscriptionResync>> {
    let chunks = chunk_by_budget(
        clients.into_iter().flat_map(pieces_of),
        SubscriptionResync::encoded_len(&[]),
        ResyncedClient::encoded_len,
    );
    let parts = u32::try_from(chunks.len()).ok()?;
    let continued: Vec<bool> = chunks
        .windows(2)
        .map(|pair| {
            matches!(
                (pair[0].last(), pair[1].first()),
                (Some(last), Some(next)) if last.client_id == next.client_id
            )
        })
        .chain(std::iter::once(false))
        .collect();
    chunks
        .into_iter()
        .zip(continued)
        .enumerate()
        .map(|(index, (clients, continued))| {
            Some(SubscriptionResync {
                round,
                part: u32::try_from(index).ok()?,
                parts,
                unchanged: false,
                continued,
                clients,
            })
        })
        .collect()
}

fn merge_pieces(carried: Option<ResyncedClient>, pieces: &[ResyncedClient]) -> Vec<ResyncedClient> {
    pieces.iter().fold(
        carried.into_iter().collect(),
        |mut whole: Vec<ResyncedClient>, piece| {
            match whole.last_mut() {
                Some(last) if last.client_id == piece.client_id => {
                    last.topics.extend(piece.topics.iter().cloned());
                    last.patterns.extend(piece.patterns.iter().cloned());
                }
                _ => whole.push(piece.clone()),
            }
            whole
        },
    )
}

impl<T: ClusterTransport> NodeController<T> {
    pub(crate) fn receive_peer_heartbeat(
        &mut self,
        from: NodeId,
        heartbeat: &Heartbeat,
        received_at: u64,
    ) {
        let revived = self.heartbeat.node_status(from) != NodeStatus::Alive;
        self.heartbeat
            .receive_heartbeat(from, heartbeat, received_at);
        if revived {
            self.silent_peers.remove(&from);
            self.stores.client_locations.revive_node(from);
            self.pending_resync_requests.insert(from, true);
        }
    }

    pub(crate) fn forget_resync_peer(&mut self, peer: NodeId) {
        self.pending_resync_requests.remove(&peer);
        self.resync_rounds.remove(&peer);
        self.resync_progress.remove(&peer);
        self.silent_peers.remove(&peer);
        self.answered_peers.remove(&peer);
    }

    pub async fn request_subscription_resyncs(&mut self) {
        let pending: Vec<(NodeId, bool)> = self.pending_resync_requests.drain().collect();
        for (peer, ask_back) in pending {
            if self.heartbeat.node_status(peer) == NodeStatus::Dead {
                continue;
            }
            self.resync_rounds.insert(
                peer,
                ResyncRound {
                    ask_back,
                    attempts: 0,
                    next_ask_ms: None,
                },
            );
            self.ask_for_resync(peer).await;
        }
    }

    async fn ask_for_resync(&mut self, peer: NodeId) {
        let digest = self.listing_digest(peer, &self.stores.client_locations.clients_on(peer));
        let Some(state) = self.resync_rounds.get_mut(&peer) else {
            return;
        };
        let round = self.next_resync_round;
        self.next_resync_round += 1;
        state.attempts += 1;
        state.next_ask_ms = None;
        let request = ResyncRequest {
            round,
            ask_back: state.ask_back,
            digest,
        };
        let attempt = state.attempts;
        let tracked = self.resync_progress.entry(peer).or_default();
        tracked.insert(
            round,
            RoundProgress {
                digest,
                ..RoundProgress::default()
            },
        );
        while tracked.len() > TRACKED_ROUNDS_PER_PEER {
            tracked.pop_first();
        }
        match self
            .transport
            .send(peer, ClusterMessage::SubscriptionResyncRequest(request))
            .await
        {
            Ok(()) => tracing::debug!(
                peer = peer.get(),
                round,
                attempt,
                "asked peer for its connected subscribers"
            ),
            Err(error) => tracing::debug!(
                peer = peer.get(),
                attempt,
                %error,
                "subscription resync request not sent"
            ),
        }
    }

    fn queue_anti_entropy(&mut self, now_ms: u64) {
        let next = *self
            .next_anti_entropy_ms
            .get_or_insert(now_ms.saturating_add(ANTI_ENTROPY_INTERVAL_MS));
        if now_ms < next {
            return;
        }
        self.next_anti_entropy_ms = Some(now_ms.saturating_add(ANTI_ENTROPY_INTERVAL_MS));
        let peers: Vec<NodeId> = self
            .heartbeat
            .alive_nodes()
            .into_iter()
            .filter(|peer| {
                *peer != self.node_id
                    && !self.resync_rounds.contains_key(peer)
                    && !self.silent_peers.contains(peer)
            })
            .collect();
        tracing::debug!(peers = peers.len(), "queueing periodic subscription resync");
        for peer in peers {
            self.pending_resync_requests.entry(peer).or_insert(false);
        }
    }

    pub(crate) fn note_local_connect(&mut self, client_id: &str) {
        if !self.boot_window.closed {
            self.boot_window.connects.insert(client_id.to_string());
        }
    }

    pub(crate) fn forget_local_connect(&mut self, client_id: &str) {
        self.boot_window.connects.remove(client_id);
    }

    fn settle_boot_peer(&mut self, peer: NodeId, answered: bool) {
        if self.boot_window.closed || !self.boot_window.settled.insert(peer) {
            return;
        }
        if !answered {
            self.boot_window.taught.remove(&peer);
        }
        if answered {
            let taught = self.boot_window.taught.remove(&peer).unwrap_or_default();
            let due: Vec<(String, NodeId)> = self
                .boot_window
                .connects
                .intersection(&taught)
                .map(|client_id| (client_id.clone(), peer))
                .collect();
            self.boot_window.due.extend(due);
        }
    }

    fn close_boot_window_at(&mut self, now_ms: u64) {
        let started = *self.boot_window.started_ms.get_or_insert(now_ms);
        if !self.boot_window.closed && now_ms.saturating_sub(started) >= BOOT_WINDOW_MS {
            self.boot_window.closed = true;
            self.boot_window.connects.clear();
            self.boot_window.taught.clear();
            tracing::debug!("subscription resync boot window closed");
        }
    }

    fn note_taught_claims(&mut self, from: NodeId, clients: &[ResyncedClient]) {
        let taught: Vec<String> = clients
            .iter()
            .filter(|client| self.boot_window.connects.contains(&client.client_id))
            .filter(|client| {
                self.stores
                    .client_locations
                    .stamp_of(&client.client_id, from)
                    .is_none()
            })
            .map(|client| client.client_id.clone())
            .collect();
        self.boot_window
            .taught
            .entry(from)
            .or_default()
            .extend(taught);
    }

    pub(crate) async fn rerank_boot_connects(&mut self) {
        let due = std::mem::take(&mut self.boot_window.due);
        for (client_id, peer) in due {
            let Some(stamp) = self.stores.client_locations.rerank_here(&client_id, peer) else {
                continue;
            };
            tracing::info!(
                client_id,
                order = stamp.order.0,
                "re-ranked a connection made before this node had resynced with a peer"
            );
            let entry = ClientLocationEntry::create(&client_id, self.node_id);
            let write = ReplicationWrite::new(
                session_partition(&client_id),
                Operation::Insert,
                Epoch::new(0),
                0,
                entity::CLIENT_LOCATIONS.to_string(),
                client_location_key(&client_id),
                entry.encode_ordered(stamp),
            );
            self.write_or_forward(write).await;
            self.announce_resumed_client(&client_id).await;
        }
    }

    pub async fn drive_subscription_resyncs(&mut self, now_ms: u64) {
        self.close_boot_window_at(now_ms);
        self.queue_anti_entropy(now_ms);
        let due: Vec<NodeId> = self
            .resync_rounds
            .iter_mut()
            .filter_map(|(peer, state)| {
                let next = *state
                    .next_ask_ms
                    .get_or_insert(now_ms.saturating_add(retry_delay_ms(state.attempts)));
                (next <= now_ms).then_some(*peer)
            })
            .collect();
        for peer in due {
            let exhausted = self
                .resync_rounds
                .get(&peer)
                .is_some_and(|state| state.attempts >= MAX_RESYNC_ATTEMPTS);
            if exhausted || self.heartbeat.node_status(peer) == NodeStatus::Dead {
                self.resync_rounds.remove(&peer);
                if exhausted && !self.answered_peers.contains(&peer) {
                    self.silent_peers.insert(peer);
                }
                if exhausted {
                    self.settle_boot_peer(peer, false);
                }
                if exhausted {
                    tracing::info!(
                        peer = peer.get(),
                        attempts = MAX_RESYNC_ATTEMPTS,
                        "peer did not answer the subscription resync, giving up"
                    );
                }
                continue;
            }
            tracing::debug!(
                peer = peer.get(),
                "subscription resync incomplete, asking again"
            );
            self.ask_for_resync(peer).await;
        }
        self.request_subscription_resyncs().await;
    }

    pub(crate) async fn handle_subscription_resync_request(
        &mut self,
        from: NodeId,
        request: ResyncRequest,
    ) {
        let connected = self.stores.client_locations.clients_on(self.node_id);
        let unchanged = self.listing_digest(self.node_id, &connected) == request.digest;
        let reply = if unchanged {
            Some(vec![SubscriptionResync {
                round: request.round,
                part: 0,
                parts: 1,
                unchanged: true,
                continued: false,
                clients: Vec::new(),
            }])
        } else {
            split_into_messages(self.resynced_clients(&connected), request.round)
        };
        match reply {
            Some(messages) => {
                let clients: usize = messages.iter().map(|m| m.clients.len()).sum();
                tracing::info!(
                    to = from.get(),
                    round = request.round,
                    clients,
                    messages = messages.len(),
                    unchanged,
                    "sending connected subscribers to peer"
                );
                for message in messages {
                    if let Err(error) = self
                        .transport
                        .send(from, ClusterMessage::SubscriptionResync(message))
                        .await
                    {
                        tracing::warn!(to = from.get(), %error, "subscription resync not sent, the peer will ask again");
                        break;
                    }
                }
            }
            None => tracing::warn!(to = from.get(), "subscription resync too large to split"),
        }
        if request.ask_back {
            self.pending_resync_requests.entry(from).or_insert(false);
            self.request_subscription_resyncs().await;
        }
    }

    pub async fn announce_resumed_client(&self, client_id: &str) {
        let clients = self.resynced_clients(&HashSet::from([client_id.to_string()]));
        let Some(announcements) = split_into_messages(clients, SubscriptionResync::UNSOLICITED)
        else {
            tracing::warn!(client_id, "resumed client subscriptions too large to split");
            return;
        };
        for announcement in announcements {
            if let Err(error) = self
                .transport
                .broadcast(ClusterMessage::SubscriptionResync(announcement))
                .await
            {
                tracing::warn!(client_id, %error, "resumed client subscriptions not announced");
                return;
            }
        }
    }

    pub(super) fn listing_digest(&self, node: NodeId, clients: &HashSet<String>) -> ListingDigest {
        let mut digest = ListingDigest::default();
        for client_id in clients {
            let stamp = self.connect_stamp(client_id, node);
            digest.add_client(client_id, stamp.order.0, stamp.instance, 0);
        }
        digest.hash = digest
            .hash
            .wrapping_add(self.stores.topics.digest_of(clients))
            .wrapping_add(self.stores.wildcards.digest_of(clients));
        digest
    }

    pub(super) fn resynced_clients(&self, clients: &HashSet<String>) -> Vec<ResyncedClient> {
        let mut topics = self.stores.topics.subscriptions_of(clients);
        let mut patterns = self.stores.wildcards.patterns_of(clients);
        let mut resynced: Vec<ResyncedClient> = clients
            .iter()
            .map(|client_id| ResyncedClient {
                order: self.connect_stamp(client_id, self.node_id).order.0,
                instance: self.connect_stamp(client_id, self.node_id).instance,
                topics: topics
                    .remove(client_id)
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|(topic, _)| !is_response_topic(topic))
                    .map(|(topic, qos)| ResyncedTopic { topic, qos })
                    .collect(),
                patterns: patterns
                    .remove(client_id)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(pattern, qos, subscription_type)| ResyncedPattern {
                        pattern,
                        qos,
                        subscription_type: subscription_type.code(),
                    })
                    .collect(),
                client_id: client_id.clone(),
            })
            .collect();
        resynced.sort_by(|a, b| a.client_id.cmp(&b.client_id));
        resynced
    }

    fn connect_stamp(&self, client_id: &str, node: NodeId) -> LocationStamp {
        self.stores
            .client_locations
            .stamp_of(client_id, node)
            .unwrap_or(LocationStamp {
                order: LocationOrder(0),
                instance: 0,
            })
    }

    fn index_update_for(
        &self,
        from: NodeId,
        client: &ResyncedClient,
        replaceable: bool,
    ) -> IndexUpdate {
        let client_id = client.client_id.as_str();
        let claimants = self.stores.client_locations.claimants(client_id);
        if self
            .stores
            .client_locations
            .outranks_here(client_id, from, LocationOrder(client.order))
        {
            IndexUpdate::Keep
        } else if replaceable && claimants == [from] {
            IndexUpdate::Replace
        } else {
            IndexUpdate::Add
        }
    }

    pub(crate) fn handle_subscription_resync(&mut self, from: NodeId, resync: &SubscriptionResync) {
        let announced = resync.round == SubscriptionResync::UNSOLICITED;
        if !announced && !self.boot_window.settled.contains(&from) {
            self.note_taught_claims(from, &resync.clients);
        }
        let tracked = self
            .resync_progress
            .get(&from)
            .is_some_and(|rounds| rounds.contains_key(&resync.round));
        if !announced && !tracked {
            tracing::debug!(
                from = from.get(),
                round = resync.round,
                "subscription resync for a round this node is not tracking, ignored"
            );
            return;
        }
        for client in &resync.clients {
            self.stores.client_locations.claim_listed(
                &client.client_id,
                from,
                LocationStamp {
                    order: LocationOrder(client.order),
                    instance: client.instance,
                },
            );
        }
        let in_order = self
            .resync_progress
            .get(&from)
            .and_then(|tracked| tracked.get(&resync.round))
            .is_some_and(|progress| !announced && progress.next_part == resync.part);
        let carried = if in_order {
            self.resync_progress
                .get_mut(&from)
                .and_then(|tracked| tracked.get_mut(&resync.round))
                .and_then(|progress| progress.partial.take())
        } else {
            None
        };
        let continuing = carried.as_ref().map(|client| client.client_id.clone());
        let mut whole = merge_pieces(carried, &resync.clients);
        let held_back = if in_order && resync.continued && resync.part + 1 < resync.parts {
            whole.pop()
        } else {
            None
        };
        let listed_earlier = self
            .resync_progress
            .get(&from)
            .and_then(|tracked| tracked.get(&resync.round))
            .map(|progress| &progress.listed)
            .filter(|_| in_order);
        let updates: Vec<(&ResyncedClient, IndexUpdate)> = whole
            .iter()
            .map(|client| {
                let replaceable = listed_earlier.is_some_and(|listed| {
                    continuing.as_ref() == Some(&client.client_id)
                        || !listed.contains(&client.client_id)
                });
                (client, self.index_update_for(from, client, replaceable))
            })
            .collect();
        let replaced: HashSet<String> = updates
            .iter()
            .filter(|(_, update)| *update == IndexUpdate::Replace)
            .map(|(client, _)| client.client_id.clone())
            .collect();
        let mut indexed_topics = self.stores.topics.subscriptions_of(&replaced);
        let mut indexed_patterns = self.stores.wildcards.patterns_of(&replaced);
        for (client, update) in &updates {
            match update {
                IndexUpdate::Keep => {}
                IndexUpdate::Add => self.add_client_subscriptions(client),
                IndexUpdate::Replace => {
                    self.drop_unlisted_subscriptions(
                        client,
                        indexed_topics.remove(&client.client_id).unwrap_or_default(),
                        indexed_patterns
                            .remove(&client.client_id)
                            .unwrap_or_default(),
                    );
                    self.add_client_subscriptions(client);
                }
            }
        }
        tracing::debug!(
            from = from.get(),
            round = resync.round,
            part = resync.part,
            parts = resync.parts,
            clients = resync.clients.len(),
            replaced = replaced.len(),
            held_back = held_back.is_some(),
            "applied subscribers connected to peer"
        );
        if let Some(partial) = held_back
            && let Some(progress) = self
                .resync_progress
                .get_mut(&from)
                .and_then(|tracked| tracked.get_mut(&resync.round))
        {
            progress.partial = Some(partial);
        }
        if !announced {
            self.track_resync_part(from, resync);
        }
    }

    fn track_resync_part(&mut self, from: NodeId, resync: &SubscriptionResync) {
        let digest_now = resync
            .unchanged
            .then(|| self.listing_digest(from, &self.stores.client_locations.clients_on(from)));
        let Some(tracked) = self.resync_progress.get_mut(&from) else {
            return;
        };
        let Some(progress) = tracked.get_mut(&resync.round) else {
            return;
        };
        self.silent_peers.remove(&from);
        self.answered_peers.insert(from);
        let changed_since_asked = digest_now.is_some_and(|digest| digest != progress.digest);
        if resync.part != progress.next_part || changed_since_asked {
            tracing::debug!(
                from = from.get(),
                expected = progress.next_part,
                got = resync.part,
                changed_since_asked,
                "subscription resync part missing or view changed since asked, asking again"
            );
            tracked.remove(&resync.round);
            if let Some(state) = self.resync_rounds.get_mut(&from) {
                state.next_ask_ms = Some(0);
            }
            return;
        }
        progress.next_part += 1;
        progress
            .listed
            .extend(resync.clients.iter().map(|client| client.client_id.clone()));
        if progress.next_part == resync.parts {
            let listed = std::mem::take(&mut progress.listed);
            tracked.retain(|round, _| *round > resync.round);
            if tracked.is_empty() {
                self.resync_progress.remove(&from);
            }
            self.resync_rounds.remove(&from);
            self.settle_boot_peer(from, true);
            let released = if resync.unchanged {
                0
            } else {
                self.stores.client_locations.retain_claims_of(from, &listed)
            };
            tracing::debug!(
                from = from.get(),
                round = resync.round,
                clients = listed.len(),
                released,
                "subscription resync complete"
            );
        }
    }

    fn drop_unlisted_subscriptions(
        &self,
        client: &ResyncedClient,
        indexed_topics: Vec<(String, u8)>,
        indexed_patterns: Vec<(String, u8, SubscriptionType)>,
    ) {
        let client_id = client.client_id.as_str();
        let wanted_topics: HashMap<&str, u8> = client
            .topics
            .iter()
            .map(|t| (t.topic.as_str(), t.qos))
            .collect();
        for (topic, qos) in indexed_topics {
            if !is_response_topic(&topic) && wanted_topics.get(topic.as_str()) != Some(&qos) {
                let _ = self.stores.topics.unsubscribe(&topic, client_id);
            }
        }
        let wanted_patterns: HashMap<&str, (u8, SubscriptionType)> = client
            .patterns
            .iter()
            .map(|p| {
                (
                    p.pattern.as_str(),
                    (p.qos, SubscriptionType::from_code(p.subscription_type)),
                )
            })
            .collect();
        for (pattern, qos, subscription_type) in indexed_patterns {
            if wanted_patterns.get(pattern.as_str()) != Some(&(qos, subscription_type)) {
                let _ = self.stores.wildcards.unsubscribe(&pattern, client_id);
            }
        }
    }

    fn add_client_subscriptions(&self, client: &ResyncedClient) {
        let client_id = client.client_id.as_str();
        let partition = session_partition(client_id);
        for topic in &client.topics {
            if let Err(error) =
                self.stores
                    .topics
                    .subscribe(&topic.topic, client_id, partition, topic.qos)
            {
                tracing::warn!(
                    topic = topic.topic,
                    client_id,
                    ?error,
                    "resynced topic not indexed"
                );
            }
        }
        for pattern in &client.patterns {
            if let Err(error) = self.stores.wildcards.subscribe(
                &pattern.pattern,
                client_id,
                pattern.qos,
                SubscriptionType::from_code(pattern.subscription_type),
            ) {
                tracing::warn!(
                    pattern = pattern.pattern,
                    client_id,
                    ?error,
                    "resynced pattern not indexed"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client(id: &str, topic_len: usize) -> ResyncedClient {
        ResyncedClient {
            client_id: id.to_string(),
            order: 0,
            instance: 0,
            topics: vec![ResyncedTopic {
                topic: "t".repeat(topic_len),
                qos: 1,
            }],
            patterns: Vec::new(),
        }
    }

    #[test]
    fn large_resyncs_are_split_without_dropping_clients() {
        let clients: Vec<ResyncedClient> = (0..5)
            .map(|n| client(&format!("c{n}"), RESYNC_MESSAGE_BUDGET / 3))
            .collect();
        let messages = split_into_messages(clients.clone(), 4).unwrap();

        assert_eq!(messages.len(), 3);
        assert!(messages.iter().all(|m| m.round == 4 && m.parts == 3));
        assert_eq!(
            messages.iter().map(|m| m.part).collect::<Vec<_>>(),
            [0, 1, 2]
        );
        assert!(
            messages
                .iter()
                .all(|m| SubscriptionResync::encoded_len(&m.clients) <= RESYNC_MESSAGE_BUDGET)
        );
        let sent: Vec<ResyncedClient> = messages.into_iter().flat_map(|m| m.clients).collect();
        assert_eq!(sent, clients);
    }

    #[test]
    fn a_peer_without_connected_subscribers_still_completes_the_round() {
        let messages = split_into_messages(Vec::new(), 2).unwrap();
        assert_eq!(
            messages,
            vec![SubscriptionResync {
                round: 2,
                part: 0,
                parts: 1,
                unchanged: false,
                continued: false,
                clients: Vec::new()
            }]
        );
    }

    #[test]
    fn splitting_measures_each_item_once() {
        let mut measured = 0_usize;
        let chunks = chunk_by_budget(0..200_000_u32, 22, |_| {
            measured += 1;
            40
        });
        assert_eq!(measured, 200_000);
        assert_eq!(chunks.iter().map(Vec::len).sum::<usize>(), 200_000);
    }

    #[test]
    fn one_client_with_many_subscriptions_is_split_below_the_budget() {
        let topics: Vec<ResyncedTopic> = (0..200_000)
            .map(|n| ResyncedTopic {
                topic: format!("gateway/devices/{n:08}/telemetry/measurements/v1/raw"),
                qos: 1,
            })
            .collect();
        let patterns: Vec<ResyncedPattern> = (0..30_000)
            .map(|n| ResyncedPattern {
                pattern: format!("gateway/{n:08}/+/#"),
                qos: 0,
                subscription_type: 0,
            })
            .collect();
        let heavy = ResyncedClient {
            client_id: "a-gateway".to_string(),
            order: 0,
            instance: 0,
            topics: topics.clone(),
            patterns: patterns.clone(),
        };
        let light = client("b-sensor", 3);
        let messages = split_into_messages(vec![heavy, light.clone()], 3).unwrap();

        let sizes: Vec<usize> = messages
            .iter()
            .map(|m| m.to_bytes().unwrap().len())
            .collect();
        assert!(
            sizes.iter().all(|size| *size <= RESYNC_MESSAGE_BUDGET),
            "resync parts {sizes:?} exceed the budget"
        );
        let sent: Vec<ResyncedClient> = messages.into_iter().flat_map(|m| m.clients).collect();
        let heavy_topics: Vec<ResyncedTopic> = sent
            .iter()
            .filter(|c| c.client_id == "a-gateway")
            .flat_map(|c| c.topics.clone())
            .collect();
        let heavy_patterns: Vec<ResyncedPattern> = sent
            .iter()
            .filter(|c| c.client_id == "a-gateway")
            .flat_map(|c| c.patterns.clone())
            .collect();
        assert_eq!(heavy_topics, topics);
        assert_eq!(heavy_patterns, patterns);
        assert_eq!(sent.last(), Some(&light));
    }

    #[test]
    fn retries_back_off_and_stay_bounded() {
        let delays: Vec<u64> = (1..=MAX_RESYNC_ATTEMPTS).map(retry_delay_ms).collect();
        assert_eq!(delays, [5_000, 10_000, 20_000, 40_000, 80_000, 160_000]);
    }
}
