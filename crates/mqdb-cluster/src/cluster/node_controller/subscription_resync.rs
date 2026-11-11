// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::super::client_location::Located;
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

#[derive(Debug, Clone, Copy)]
pub(crate) struct ResyncRound {
    round: u64,
    ask_back: bool,
    asked_at_ms: Option<u64>,
    next_part: u32,
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
            self.send_resync_request(peer, ask_back).await;
        }
    }

    async fn send_resync_request(&mut self, peer: NodeId, ask_back: bool) {
        let round = self.next_resync_round;
        self.next_resync_round += 1;
        let request = ResyncRequest { round, ask_back };
        match self
            .transport
            .send(peer, ClusterMessage::SubscriptionResyncRequest(request))
            .await
        {
            Ok(()) => {
                self.resync_rounds.insert(
                    peer,
                    ResyncRound {
                        round,
                        ask_back,
                        asked_at_ms: None,
                        next_part: 0,
                    },
                );
                tracing::debug!(
                    peer = peer.get(),
                    round,
                    ask_back,
                    "asked peer for its connected subscribers"
                );
            }
            Err(error) => {
                let queued = self.pending_resync_requests.entry(peer).or_insert(false);
                *queued |= ask_back;
                tracing::warn!(peer = peer.get(), %error, "subscription resync request not sent");
            }
        }
    }

    pub async fn drive_subscription_resyncs(&mut self, now_ms: u64) {
        let due: Vec<(NodeId, bool)> = self
            .resync_rounds
            .iter_mut()
            .filter_map(|(peer, round)| {
                let asked_at = *round.asked_at_ms.get_or_insert(now_ms);
                (now_ms.saturating_sub(asked_at) >= RESYNC_RETRY_MS)
                    .then_some((*peer, round.ask_back))
            })
            .collect();
        for (peer, ask_back) in due {
            self.resync_rounds.remove(&peer);
            if self.heartbeat.node_status(peer) == NodeStatus::Dead {
                continue;
            }
            tracing::info!(
                peer = peer.get(),
                "subscription resync incomplete, asking again"
            );
            self.send_resync_request(peer, ask_back).await;
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

    pub async fn announce_resumed_client(&self, client_id: &str, since_ms: u64) {
        let clients = HashMap::from([(client_id.to_string(), since_ms)]);
        let announcement = SubscriptionResync {
            round: SubscriptionResync::UNSOLICITED,
            part: 0,
            parts: 1,
            clients: self.resynced_clients(clients),
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
        self.resynced_clients(self.stores.client_locations.clients_on(self.node_id))
    }

    fn resynced_clients(&self, clients: HashMap<String, u64>) -> Vec<ResyncedClient> {
        let names: HashSet<String> = clients.keys().cloned().collect();
        let mut topics = self.stores.topics.subscriptions_of(&names);
        let mut patterns = self.stores.wildcards.patterns_of(&names);
        let mut resynced: Vec<ResyncedClient> = clients
            .into_iter()
            .map(|(client_id, since_ms)| ResyncedClient {
                topics: topics
                    .remove(&client_id)
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|(topic, _)| !is_response_topic(topic))
                    .map(|(topic, qos)| ResyncedTopic { topic, qos })
                    .collect(),
                patterns: patterns
                    .remove(&client_id)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(pattern, qos, subscription_type)| ResyncedPattern {
                        pattern,
                        qos,
                        subscription_type: type_code(subscription_type),
                    })
                    .collect(),
                client_id,
                since_ms,
            })
            .collect();
        resynced.sort_by(|a, b| a.client_id.cmp(&b.client_id));
        resynced
    }

    fn takes_resynced_client(&self, client: &ResyncedClient) -> bool {
        self.stores
            .client_locations
            .located(&client.client_id)
            .is_none_or(|current| {
                current.node != self.node_id && current.since_ms <= client.since_ms
            })
    }

    pub(crate) fn handle_subscription_resync(&mut self, from: NodeId, resync: &SubscriptionResync) {
        let taken: Vec<&ResyncedClient> = resync
            .clients
            .iter()
            .filter(|client| self.takes_resynced_client(client))
            .collect();
        let names: HashSet<String> = taken
            .iter()
            .map(|client| client.client_id.clone())
            .collect();
        let mut indexed_topics = self.stores.topics.subscriptions_of(&names);
        let mut indexed_patterns = self.stores.wildcards.patterns_of(&names);
        for client in &taken {
            self.replace_client_subscriptions(
                from,
                client,
                indexed_topics.remove(&client.client_id).unwrap_or_default(),
                indexed_patterns
                    .remove(&client.client_id)
                    .unwrap_or_default(),
            );
        }
        tracing::info!(
            from = from.get(),
            round = resync.round,
            part = resync.part,
            parts = resync.parts,
            clients = taken.len(),
            skipped = resync.clients.len() - taken.len(),
            "applied subscribers connected to peer"
        );
        if resync.round != SubscriptionResync::UNSOLICITED {
            self.track_resync_part(from, resync);
        }
    }

    fn track_resync_part(&mut self, from: NodeId, resync: &SubscriptionResync) {
        let Some(round) = self.resync_rounds.get_mut(&from) else {
            return;
        };
        if round.round != resync.round {
            return;
        }
        if resync.part != round.next_part {
            tracing::warn!(
                from = from.get(),
                expected = round.next_part,
                got = resync.part,
                "subscription resync part missing, asking again"
            );
            round.asked_at_ms = Some(0);
            return;
        }
        round.next_part += 1;
        if round.next_part == resync.parts {
            self.resync_rounds.remove(&from);
            tracing::debug!(
                from = from.get(),
                round = resync.round,
                "subscription resync complete"
            );
        }
    }

    fn replace_client_subscriptions(
        &self,
        from: NodeId,
        client: &ResyncedClient,
        indexed_topics: Vec<(String, u8)>,
        indexed_patterns: Vec<(String, u8, SubscriptionType)>,
    ) {
        let client_id = client.client_id.as_str();
        let partition = session_partition(client_id);
        self.stores.client_locations.set_if_newer(
            client_id,
            Located {
                node: from,
                since_ms: client.since_ms,
            },
        );

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
            since_ms: 0,
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
}
