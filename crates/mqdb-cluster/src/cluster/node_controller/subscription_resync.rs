// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::super::heartbeat::NodeStatus;
use super::super::protocol::{
    Heartbeat, ResyncRequest, ResyncedClient, ResyncedPattern, ResyncedTopic, SubscriptionResync,
};
use super::super::transport::{ClusterMessage, ClusterTransport};
use super::super::{NodeId, SubscriptionType, is_response_topic, session_partition};
use super::NodeController;
use std::collections::{HashMap, HashSet};

const RESYNC_MESSAGE_BUDGET: usize = 1024 * 1024;
pub(crate) const RESYNC_RETRY_MS: u64 = 5_000;
pub(crate) const MAX_RESYNC_ATTEMPTS: u32 = 6;

#[derive(Debug, Default)]
struct RoundProgress {
    next_part: u32,
    listed: HashSet<String>,
}

#[derive(Debug)]
pub(crate) struct ResyncRound {
    ask_back: bool,
    attempts: u32,
    next_ask_ms: Option<u64>,
    progress: HashMap<u64, RoundProgress>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IndexUpdate {
    Replace,
    Add,
    Keep,
}

fn type_code(subscription_type: SubscriptionType) -> u8 {
    match subscription_type {
        SubscriptionType::Mqtt => 0,
        SubscriptionType::Db => 1,
    }
}

fn type_from_code(code: u8) -> SubscriptionType {
    match code {
        1 => SubscriptionType::Db,
        _ => SubscriptionType::Mqtt,
    }
}

fn retry_delay_ms(attempts: u32) -> u64 {
    RESYNC_RETRY_MS.saturating_mul(1 << attempts.saturating_sub(1).min(16))
}

fn split_into_messages(
    clients: Vec<ResyncedClient>,
    round: u64,
) -> Option<Vec<SubscriptionResync>> {
    let mut chunks: Vec<Vec<ResyncedClient>> = vec![Vec::new()];
    for client in clients {
        let fits = chunks.last().is_some_and(|chunk| {
            chunk.is_empty()
                || SubscriptionResync::encoded_len(chunk) + client.encoded_len()
                    <= RESYNC_MESSAGE_BUDGET
        });
        if !fits {
            chunks.push(Vec::new());
        }
        if let Some(chunk) = chunks.last_mut() {
            chunk.push(client);
        }
    }
    let parts = u32::try_from(chunks.len()).ok()?;
    chunks
        .into_iter()
        .enumerate()
        .map(|(index, clients)| {
            Some(SubscriptionResync {
                round,
                part: u32::try_from(index).ok()?,
                parts,
                clients,
            })
        })
        .collect()
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
            self.stores.client_locations.revive_node(from);
            self.pending_resync_requests.insert(from, true);
        }
    }

    pub(crate) fn forget_resync_peer(&mut self, peer: NodeId) {
        self.pending_resync_requests.remove(&peer);
        self.resync_rounds.remove(&peer);
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
                    progress: HashMap::new(),
                },
            );
            self.ask_for_resync(peer).await;
        }
    }

    async fn ask_for_resync(&mut self, peer: NodeId) {
        let Some(state) = self.resync_rounds.get_mut(&peer) else {
            return;
        };
        let round = self.next_resync_round;
        self.next_resync_round += 1;
        state.attempts += 1;
        state.next_ask_ms = None;
        state.progress.insert(round, RoundProgress::default());
        let request = ResyncRequest {
            round,
            ask_back: state.ask_back,
        };
        let attempt = state.attempts;
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

    pub async fn drive_subscription_resyncs(&mut self, now_ms: u64) {
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
        match split_into_messages(self.connected_subscribers(), request.round) {
            Some(messages) => {
                let clients: usize = messages.iter().map(|m| m.clients.len()).sum();
                tracing::info!(
                    to = from.get(),
                    round = request.round,
                    clients,
                    messages = messages.len(),
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
        let announcement = SubscriptionResync {
            round: SubscriptionResync::UNSOLICITED,
            part: 0,
            parts: 1,
            clients: self.resynced_clients(&HashSet::from([client_id.to_string()])),
        };
        if let Err(error) = self
            .transport
            .broadcast(ClusterMessage::SubscriptionResync(announcement))
            .await
        {
            tracing::warn!(client_id, %error, "resumed client subscriptions not announced");
        }
    }

    fn connected_subscribers(&self) -> Vec<ResyncedClient> {
        self.resynced_clients(&self.stores.client_locations.clients_on(self.node_id))
    }

    fn resynced_clients(&self, clients: &HashSet<String>) -> Vec<ResyncedClient> {
        let mut topics = self.stores.topics.subscriptions_of(clients);
        let mut patterns = self.stores.wildcards.patterns_of(clients);
        let mut resynced: Vec<ResyncedClient> = clients
            .iter()
            .map(|client_id| ResyncedClient {
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
                        subscription_type: type_code(subscription_type),
                    })
                    .collect(),
                client_id: client_id.clone(),
            })
            .collect();
        resynced.sort_by(|a, b| a.client_id.cmp(&b.client_id));
        resynced
    }

    fn index_update_for(&self, from: NodeId, client_id: &str, announced: bool) -> IndexUpdate {
        let claimants = self.stores.client_locations.claimants(client_id);
        if claimants.contains(&self.node_id) {
            IndexUpdate::Keep
        } else if !announced && claimants == [from] {
            IndexUpdate::Replace
        } else {
            IndexUpdate::Add
        }
    }

    pub(crate) fn handle_subscription_resync(&mut self, from: NodeId, resync: &SubscriptionResync) {
        let announced = resync.round == SubscriptionResync::UNSOLICITED;
        for client in &resync.clients {
            self.stores.client_locations.claim(&client.client_id, from);
        }
        let updates: Vec<(&ResyncedClient, IndexUpdate)> = resync
            .clients
            .iter()
            .map(|client| {
                (
                    client,
                    self.index_update_for(from, &client.client_id, announced),
                )
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
            "applied subscribers connected to peer"
        );
        if !announced {
            self.track_resync_part(from, resync);
        }
    }

    fn track_resync_part(&mut self, from: NodeId, resync: &SubscriptionResync) {
        let Some(state) = self.resync_rounds.get_mut(&from) else {
            return;
        };
        let Some(progress) = state.progress.get_mut(&resync.round) else {
            return;
        };
        if resync.part != progress.next_part {
            tracing::debug!(
                from = from.get(),
                expected = progress.next_part,
                got = resync.part,
                "subscription resync part missing, asking again"
            );
            state.progress.remove(&resync.round);
            state.next_ask_ms = Some(0);
            return;
        }
        progress.next_part += 1;
        progress
            .listed
            .extend(resync.clients.iter().map(|client| client.client_id.clone()));
        if progress.next_part == resync.parts {
            let listed = std::mem::take(&mut progress.listed);
            self.resync_rounds.remove(&from);
            let released = self.stores.client_locations.retain_claims_of(from, &listed);
            tracing::info!(
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
                    (p.qos, type_from_code(p.subscription_type)),
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
                type_from_code(pattern.subscription_type),
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
                clients: Vec::new()
            }]
        );
    }

    #[test]
    fn retries_back_off_and_stay_bounded() {
        let delays: Vec<u64> = (1..=MAX_RESYNC_ATTEMPTS).map(retry_delay_ms).collect();
        assert_eq!(delays, [5_000, 10_000, 20_000, 40_000, 80_000, 160_000]);
    }
}
