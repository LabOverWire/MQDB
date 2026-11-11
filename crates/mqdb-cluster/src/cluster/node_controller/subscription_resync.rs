// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::super::heartbeat::NodeStatus;
use super::super::protocol::{
    Heartbeat, ResyncedClient, ResyncedPattern, ResyncedTopic, SubscriptionResync,
};
use super::super::transport::{ClusterMessage, ClusterTransport};
use super::super::{NodeId, SubscriptionType, is_response_topic, session_partition};
use super::NodeController;
use std::collections::{HashMap, HashSet};

const RESYNC_MESSAGE_BUDGET: usize = 1024 * 1024;

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

fn split_into_messages(clients: Vec<ResyncedClient>) -> Vec<SubscriptionResync> {
    let mut messages = Vec::new();
    let mut current = Vec::new();
    let mut current_len = 0;
    for client in clients {
        let len = client.encoded_len();
        if !current.is_empty() && current_len + len > RESYNC_MESSAGE_BUDGET {
            messages.push(SubscriptionResync {
                clients: std::mem::take(&mut current),
            });
            current_len = 0;
        }
        current_len += len;
        current.push(client);
    }
    if !current.is_empty() {
        messages.push(SubscriptionResync { clients: current });
    }
    messages
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
            self.pending_resync_requests.insert(from);
        }
    }

    pub async fn request_subscription_resyncs(&mut self) {
        let peers: Vec<NodeId> = self.pending_resync_requests.drain().collect();
        for peer in peers {
            match self
                .transport
                .send(peer, ClusterMessage::SubscriptionResyncRequest)
                .await
            {
                Ok(()) => {
                    tracing::debug!(
                        peer = peer.get(),
                        "asked peer for its connected subscribers"
                    );
                }
                Err(error) => {
                    self.pending_resync_requests.insert(peer);
                    tracing::warn!(peer = peer.get(), %error, "subscription resync request not sent");
                }
            }
        }
    }

    pub(crate) async fn handle_subscription_resync_request(&self, from: NodeId) {
        let messages = split_into_messages(self.connected_subscribers());
        let clients: usize = messages.iter().map(|m| m.clients.len()).sum();
        tracing::info!(
            to = from.get(),
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
                tracing::warn!(to = from.get(), %error, "subscription resync not sent");
            }
        }
    }

    fn connected_subscribers(&self) -> Vec<ResyncedClient> {
        let clients = self.stores.client_locations.clients_on(self.node_id);
        let mut topics = self.stores.topics.subscriptions_of(&clients);
        let mut patterns = self.stores.wildcards.patterns_of(&clients);
        let mut resynced: Vec<ResyncedClient> = clients
            .into_iter()
            .map(|client_id| ResyncedClient {
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
            })
            .collect();
        resynced.sort_by(|a, b| a.client_id.cmp(&b.client_id));
        resynced
    }

    pub(crate) fn handle_subscription_resync(&self, from: NodeId, resync: &SubscriptionResync) {
        let clients: HashSet<String> = resync
            .clients
            .iter()
            .map(|client| client.client_id.clone())
            .filter(|client_id| self.stores.client_locations.get(client_id) != Some(self.node_id))
            .collect();
        let mut indexed_topics = self.stores.topics.subscriptions_of(&clients);
        let mut indexed_patterns = self.stores.wildcards.patterns_of(&clients);
        let mut replaced = 0usize;
        for client in resync
            .clients
            .iter()
            .filter(|client| clients.contains(&client.client_id))
        {
            replaced += 1;
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
            clients = replaced,
            skipped = resync.clients.len() - replaced,
            "applied subscribers connected to peer"
        );
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
        self.stores.client_locations.set(client_id, from);

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
        let messages = split_into_messages(clients.clone());

        assert_eq!(messages.len(), 3);
        assert!(messages.iter().all(|m| {
            m.clients
                .iter()
                .map(ResyncedClient::encoded_len)
                .sum::<usize>()
                <= RESYNC_MESSAGE_BUDGET
        }));
        let sent: Vec<ResyncedClient> = messages.into_iter().flat_map(|m| m.clients).collect();
        assert_eq!(sent, clients);
    }

    #[test]
    fn a_peer_without_connected_subscribers_sends_nothing() {
        assert!(split_into_messages(Vec::new()).is_empty());
    }
}
