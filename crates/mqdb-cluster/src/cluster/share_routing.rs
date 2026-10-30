// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{
    ForwardTarget, NodeId, SubscriberLocation, TopicIndex, WildcardStore, is_wildcard_pattern,
};
use mqtt5::broker::router::MessageRouter;
use mqtt5::broker::storage::{DynamicStorage, StorageBackend};
use mqtt5::{parse_shared_subscription, topic_matches_filter};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct ShareKey {
    pub(crate) group: String,
    pub(crate) filter: String,
}

impl ShareKey {
    fn parse(topic_filter: &str) -> Option<Self> {
        match parse_shared_subscription(topic_filter) {
            (filter, Some(group)) => Some(Self {
                group: group.to_string(),
                filter: filter.to_string(),
            }),
            (_, None) => None,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ShareMember {
    pub(crate) key: ShareKey,
    pub(crate) subscriber: SubscriberLocation,
}

pub(crate) fn share_members(
    topics: &TopicIndex,
    wildcards: &WildcardStore,
    topic: &str,
) -> Vec<ShareMember> {
    topics
        .shared_subscribers(topic)
        .into_iter()
        .chain(wildcards.match_shared(topic))
        .map(|(group, filter, subscriber)| ShareMember {
            key: ShareKey { group, filter },
            subscriber,
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ShareCandidate {
    pub(crate) client_id: String,
    pub(crate) key: ShareKey,
    version: u64,
}

#[derive(Debug, Default)]
pub(crate) struct LocalShareMembers {
    by_client: HashMap<String, HashMap<ShareKey, u64>>,
    by_filter: HashMap<String, HashSet<(String, String)>>,
    wildcard_filters: BTreeSet<String>,
    next_version: u64,
}

impl LocalShareMembers {
    pub(crate) fn connected(&mut self, client_id: &str, clean_start: bool) {
        if clean_start {
            self.remove_client(client_id);
        }
    }

    pub(crate) fn subscribed(&mut self, client_id: &str, topic_filter: &str) {
        if let Some(key) = ShareKey::parse(topic_filter) {
            self.add(client_id, key);
        }
    }

    pub(crate) fn unsubscribed(&mut self, client_id: &str, topic_filter: &str) {
        if let Some(key) = ShareKey::parse(topic_filter) {
            self.remove(client_id, &key);
        }
    }

    pub(crate) fn candidates(&self, topic: &str) -> Vec<ShareCandidate> {
        let exact = self.by_filter.get(topic).map(|members| (topic, members));
        let wildcard = self
            .wildcard_filters
            .iter()
            .filter(|filter| topic_matches_filter(topic, filter))
            .filter_map(|filter| {
                self.by_filter
                    .get(filter)
                    .map(|members| (filter.as_str(), members))
            });
        exact
            .into_iter()
            .chain(wildcard)
            .flat_map(|(filter, members)| {
                members.iter().filter_map(move |(client_id, group)| {
                    let key = ShareKey {
                        group: group.clone(),
                        filter: filter.to_string(),
                    };
                    let version = *self.by_client.get(client_id)?.get(&key)?;
                    Some(ShareCandidate {
                        client_id: client_id.clone(),
                        key,
                        version,
                    })
                })
            })
            .collect()
    }

    pub(crate) fn forget(&mut self, stale: &[ShareCandidate]) {
        for candidate in stale {
            let unchanged = self
                .by_client
                .get(&candidate.client_id)
                .and_then(|keys| keys.get(&candidate.key))
                .is_some_and(|version| *version == candidate.version);
            if unchanged {
                self.remove(&candidate.client_id, &candidate.key);
            }
        }
    }

    pub(crate) fn reconcile(&mut self, client_id: &str, stored: &HashSet<ShareKey>) {
        let tracked: HashSet<ShareKey> = self
            .by_client
            .get(client_id)
            .map(|keys| keys.keys().cloned().collect())
            .unwrap_or_default();
        for key in tracked.difference(stored) {
            self.remove(client_id, key);
        }
        for key in stored.difference(&tracked) {
            self.add(client_id, key.clone());
        }
    }

    fn add(&mut self, client_id: &str, key: ShareKey) {
        let member = (client_id.to_string(), key.group.clone());
        if is_wildcard_pattern(&key.filter) {
            self.wildcard_filters.insert(key.filter.clone());
        }
        self.by_filter
            .entry(key.filter.clone())
            .or_default()
            .insert(member);
        self.next_version += 1;
        self.by_client
            .entry(client_id.to_string())
            .or_default()
            .insert(key, self.next_version);
    }

    fn remove(&mut self, client_id: &str, key: &ShareKey) {
        if let Some(keys) = self.by_client.get_mut(client_id) {
            keys.remove(key);
            if keys.is_empty() {
                self.by_client.remove(client_id);
            }
        }
        if let Some(members) = self.by_filter.get_mut(&key.filter) {
            members.remove(&(client_id.to_string(), key.group.clone()));
            if members.is_empty() {
                self.by_filter.remove(&key.filter);
                self.wildcard_filters.remove(&key.filter);
            }
        }
    }

    fn remove_client(&mut self, client_id: &str) {
        let keys: Vec<ShareKey> = self
            .by_client
            .get(client_id)
            .map(|keys| keys.keys().cloned().collect())
            .unwrap_or_default();
        for key in &keys {
            self.remove(client_id, key);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LocalShare {
    pub(crate) client_id: String,
    pub(crate) key: ShareKey,
    pub(crate) online: bool,
}

pub(crate) fn served_locally(
    held: &[LocalShare],
    locate: impl Fn(&str) -> Option<NodeId>,
    local: NodeId,
) -> HashSet<ShareKey> {
    held.iter()
        .filter(|share| share.online || locate(&share.client_id).is_none_or(|node| node == local))
        .map(|share| share.key.clone())
        .collect()
}

#[derive(Debug, Default)]
pub(crate) struct HeldShares {
    pub(crate) held: Vec<LocalShare>,
    pub(crate) stale: Vec<ShareCandidate>,
}

pub(crate) struct BrokerView {
    router: Arc<MessageRouter>,
    storage: Option<Arc<DynamicStorage>>,
}

impl BrokerView {
    pub(crate) fn new(router: Arc<MessageRouter>, storage: Option<Arc<DynamicStorage>>) -> Self {
        Self { router, storage }
    }

    pub(crate) async fn held_shares(&self, candidates: Vec<ShareCandidate>) -> HeldShares {
        let mut held = Vec::new();
        let mut offline = Vec::new();
        for candidate in candidates {
            if self.router.is_connected(&candidate.client_id).await {
                held.push(LocalShare {
                    client_id: candidate.client_id,
                    key: candidate.key,
                    online: true,
                });
            } else {
                offline.push(candidate);
            }
        }
        let online_keys: HashSet<ShareKey> = held.iter().map(|share| share.key.clone()).collect();
        let mut stored_by_client: HashMap<String, HashSet<ShareKey>> = HashMap::new();
        let mut stale = Vec::new();
        for candidate in offline {
            if online_keys.contains(&candidate.key) {
                continue;
            }
            if !stored_by_client.contains_key(&candidate.client_id) {
                let stored = self.stored_shares(&candidate.client_id).await;
                stored_by_client.insert(candidate.client_id.clone(), stored);
            }
            if stored_by_client
                .get(&candidate.client_id)
                .is_some_and(|stored| stored.contains(&candidate.key))
            {
                held.push(LocalShare {
                    client_id: candidate.client_id,
                    key: candidate.key,
                    online: false,
                });
            } else {
                stale.push(candidate);
            }
        }
        HeldShares { held, stale }
    }

    pub(crate) async fn stored_shares(&self, client_id: &str) -> HashSet<ShareKey> {
        let Some(storage) = &self.storage else {
            return HashSet::new();
        };
        storage
            .get_session(client_id)
            .await
            .ok()
            .flatten()
            .map(|session| {
                session
                    .subscriptions
                    .keys()
                    .filter_map(|topic_filter| ShareKey::parse(topic_filter))
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[derive(Debug, Default)]
pub(crate) struct RemoteForward {
    pub(crate) targets: Vec<ForwardTarget>,
    pub(crate) share_groups: Vec<String>,
}

pub(crate) fn remote_forwards(
    remote_nodes: HashMap<NodeId, Vec<ForwardTarget>>,
    share_nodes: HashMap<NodeId, Vec<String>>,
) -> HashMap<NodeId, RemoteForward> {
    let mut forwards: HashMap<NodeId, RemoteForward> = remote_nodes
        .into_iter()
        .map(|(node, targets)| {
            (
                node,
                RemoteForward {
                    targets,
                    share_groups: Vec::new(),
                },
            )
        })
        .collect();
    for (node, share_groups) in share_nodes {
        forwards.entry(node).or_default().share_groups = share_groups;
    }
    forwards
}

pub(crate) fn live_remote_node(
    locate: impl Fn(&str) -> Option<NodeId>,
    local: NodeId,
    alive: HashSet<NodeId>,
) -> impl Fn(&str) -> Option<NodeId> {
    move |client_id| locate(client_id).filter(|node| *node != local && alive.contains(node))
}

pub(crate) fn assign_share_groups(
    members: &[ShareMember],
    served_locally: &HashSet<ShareKey>,
    remote_node: impl Fn(&str) -> Option<NodeId>,
    mut rotation: impl FnMut(&ShareKey) -> usize,
) -> HashMap<NodeId, Vec<String>> {
    let mut candidates: HashMap<&ShareKey, BTreeSet<NodeId>> = HashMap::new();
    for member in members {
        if let Some(node) = remote_node(member.subscriber.client_id_str()) {
            candidates.entry(&member.key).or_default().insert(node);
        }
    }

    let mut assigned: HashMap<NodeId, BTreeSet<&str>> = HashMap::new();
    for (key, nodes) in candidates {
        if served_locally.contains(key) {
            continue;
        }
        if let Some(node) = nodes.iter().nth(rotation(key) % nodes.len()) {
            assigned.entry(*node).or_default().insert(&key.group);
        }
    }
    assigned
        .into_iter()
        .map(|(node, groups)| (node, groups.into_iter().map(str::to_string).collect()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::PartitionId;

    fn node(id: u16) -> NodeId {
        NodeId::validated(id).expect("valid node id")
    }

    fn key(group: &str, filter: &str) -> ShareKey {
        ShareKey {
            group: group.to_string(),
            filter: filter.to_string(),
        }
    }

    fn member(group: &str, filter: &str, client_id: &str) -> ShareMember {
        ShareMember {
            key: key(group, filter),
            subscriber: SubscriberLocation::create(client_id, PartitionId::ZERO, 1),
        }
    }

    fn located<'a>(locations: &'a [(&'a str, u16)]) -> impl Fn(&str) -> Option<NodeId> + 'a {
        move |client_id| {
            locations
                .iter()
                .find(|(id, _)| *id == client_id)
                .map(|(_, n)| node(*n))
        }
    }

    fn remote<'a>(
        locations: &'a [(&'a str, u16)],
        local: u16,
        dead: &'a [u16],
    ) -> impl Fn(&str) -> Option<NodeId> + 'a {
        let alive: HashSet<NodeId> = (1..=5).filter(|n| !dead.contains(n)).map(node).collect();
        live_remote_node(located(locations), node(local), alive)
    }

    fn fixed(rotation: usize) -> impl FnMut(&ShareKey) -> usize {
        move |_| rotation
    }

    #[test]
    fn a_group_with_only_remote_members_goes_to_one_of_their_nodes() {
        let members = [member("g", "sh/x", "a"), member("g", "sh/x", "b")];
        let places = [("a", 2), ("b", 3)];
        let assigned =
            assign_share_groups(&members, &HashSet::new(), remote(&places, 1, &[]), fixed(0));
        assert_eq!(assigned, HashMap::from([(node(2), vec!["g".to_string()])]));
        let rotated =
            assign_share_groups(&members, &HashSet::new(), remote(&places, 1, &[]), fixed(1));
        assert_eq!(rotated, HashMap::from([(node(3), vec!["g".to_string()])]));
    }

    #[test]
    fn a_group_the_local_broker_serves_is_not_sent_anywhere() {
        let members = [member("g", "sh/x", "a"), member("g", "sh/x", "b")];
        let assigned = assign_share_groups(
            &members,
            &HashSet::from([key("g", "sh/x")]),
            remote(&[("a", 1), ("b", 2)], 1, &[]),
            fixed(0),
        );
        assert!(assigned.is_empty());
    }

    #[test]
    fn a_stale_local_location_does_not_stop_forwarding() {
        let members = [member("g", "sh/x", "gone"), member("g", "sh/x", "b")];
        let assigned = assign_share_groups(
            &members,
            &HashSet::new(),
            remote(&[("gone", 1), ("b", 2)], 1, &[]),
            fixed(0),
        );
        assert_eq!(assigned, HashMap::from([(node(2), vec!["g".to_string()])]));
    }

    #[test]
    fn members_on_dead_nodes_are_not_chosen() {
        let members = [member("g", "sh/x", "a"), member("g", "sh/x", "b")];
        for rotation in 0..4 {
            let assigned = assign_share_groups(
                &members,
                &HashSet::new(),
                remote(&[("a", 2), ("b", 3)], 1, &[2]),
                fixed(rotation),
            );
            assert_eq!(assigned, HashMap::from([(node(3), vec!["g".to_string()])]));
        }
    }

    #[test]
    fn each_filter_of_a_group_is_served_once() {
        let members = [member("g", "sh/+", "a"), member("g", "sh/x", "b")];
        let assigned = assign_share_groups(
            &members,
            &HashSet::from([key("g", "sh/+")]),
            remote(&[("b", 2)], 1, &[]),
            fixed(0),
        );
        assert_eq!(assigned, HashMap::from([(node(2), vec!["g".to_string()])]));
    }

    #[test]
    fn members_with_no_known_node_are_skipped() {
        let assigned = assign_share_groups(
            &[member("g", "sh/x", "a")],
            &HashSet::new(),
            remote(&[], 1, &[]),
            fixed(0),
        );
        assert!(assigned.is_empty());
    }

    #[test]
    fn local_candidates_follow_subscribe_unsubscribe_and_clean_start() {
        let mut local = LocalShareMembers::default();
        local.subscribed("a", "$share/g/sh/+");
        local.subscribed("a", "sh/plain");
        assert_eq!(
            pairs(&local.candidates("sh/x")),
            vec![("a".to_string(), key("g", "sh/+"))]
        );
        assert!(local.candidates("other/x").is_empty());

        local.connected("a", false);
        assert_eq!(local.candidates("sh/x").len(), 1);
        local.connected("a", true);
        assert!(local.candidates("sh/x").is_empty());

        local.subscribed("b", "$share/h/sh/x");
        let stale = local.candidates("sh/x");
        local.forget(&stale);
        assert!(local.candidates("sh/x").is_empty());

        local.subscribed("c", "$share/k/sh/x");
        local.unsubscribed("c", "$share/k/sh/x");
        assert!(local.candidates("sh/x").is_empty());
    }

    fn pairs(candidates: &[ShareCandidate]) -> Vec<(String, ShareKey)> {
        let mut pairs: Vec<_> = candidates
            .iter()
            .map(|candidate| (candidate.client_id.clone(), candidate.key.clone()))
            .collect();
        pairs.sort();
        pairs
    }

    fn local(client_id: &str, group: &str, online: bool) -> LocalShare {
        LocalShare {
            client_id: client_id.to_string(),
            key: key(group, "sh/x"),
            online,
        }
    }

    #[test]
    fn an_offline_local_member_now_connected_elsewhere_is_not_served_here() {
        let held = [
            local("online-here", "a", true),
            local("offline-unknown", "b", false),
            local("offline-here", "c", false),
            local("moved", "d", false),
        ];
        let served = served_locally(
            &held,
            located(&[("offline-here", 1), ("moved", 2)]),
            node(1),
        );
        assert_eq!(
            served,
            HashSet::from([key("a", "sh/x"), key("b", "sh/x"), key("c", "sh/x")])
        );
    }

    #[test]
    fn reconcile_keeps_only_the_shares_the_stored_session_still_has() {
        let mut members = LocalShareMembers::default();
        members.subscribed("a", "$share/g/sh/x");
        members.subscribed("a", "$share/h/sh/x");
        members.reconcile("a", &HashSet::from([key("h", "sh/x")]));
        assert_eq!(
            pairs(&members.candidates("sh/x")),
            vec![("a".to_string(), key("h", "sh/x"))]
        );
        members.reconcile("a", &HashSet::new());
        assert!(members.candidates("sh/x").is_empty());
        members.reconcile("b", &HashSet::from([key("k", "sh/+")]));
        assert_eq!(
            pairs(&members.candidates("sh/x")),
            vec![("b".to_string(), key("k", "sh/+"))]
        );
    }

    #[test]
    fn forgetting_a_stale_candidate_keeps_a_newer_subscription() {
        let mut members = LocalShareMembers::default();
        members.subscribed("a", "$share/g/sh/x");
        let stale = members.candidates("sh/x");
        members.unsubscribed("a", "$share/g/sh/x");
        members.subscribed("a", "$share/g/sh/x");
        members.forget(&stale);
        assert_eq!(
            pairs(&members.candidates("sh/x")),
            vec![("a".to_string(), key("g", "sh/x"))]
        );
        let current = members.candidates("sh/x");
        members.forget(&current);
        assert!(members.candidates("sh/x").is_empty());
    }

    async fn router_with_member(client_id: &str, topic_filter: &str) -> Arc<MessageRouter> {
        let router = Arc::new(MessageRouter::new());
        let receivers =
            crate::cluster::subscribe_test_client(&router, client_id, topic_filter, 8).await;
        drop(receivers);
        router
    }

    #[tokio::test]
    async fn the_broker_view_counts_an_offline_member_only_while_its_session_keeps_the_subscription()
     {
        use mqtt5::QoS;
        use mqtt5::broker::storage::{ClientSession, MemoryBackend, StoredSubscription};

        let storage = Arc::new(DynamicStorage::Memory(MemoryBackend::new()));
        let mut kept = ClientSession::new("kept", true, Some(300));
        kept.add_subscription("$share/g/sh/x", StoredSubscription::new(QoS::AtLeastOnce));
        storage
            .store_session(kept)
            .await
            .expect("store kept session");
        storage
            .store_session(ClientSession::new("dropped", true, Some(300)))
            .await
            .expect("store dropped session");
        let view = BrokerView::new(Arc::new(MessageRouter::new()), Some(storage));

        let mut members = LocalShareMembers::default();
        for (client_id, topic_filter) in [
            ("kept", "$share/g/sh/x"),
            ("kept", "$share/h/sh/x"),
            ("dropped", "$share/k/sh/x"),
            ("never-connected", "$share/m/sh/x"),
        ] {
            members.subscribed(client_id, topic_filter);
        }

        let checked = view.held_shares(members.candidates("sh/x")).await;

        assert_eq!(
            checked.held,
            vec![LocalShare {
                client_id: "kept".to_string(),
                key: key("g", "sh/x"),
                online: false,
            }]
        );
        assert_eq!(
            pairs(&checked.stale),
            vec![
                ("dropped".to_string(), key("k", "sh/x")),
                ("kept".to_string(), key("h", "sh/x")),
                ("never-connected".to_string(), key("m", "sh/x")),
            ]
        );
        assert_eq!(
            view.stored_shares("kept").await,
            HashSet::from([key("g", "sh/x")])
        );
        assert!(view.stored_shares("dropped").await.is_empty());
    }

    #[tokio::test]
    async fn the_broker_view_counts_a_connected_member_and_drops_an_unknown_one() {
        let router = router_with_member("a", "$share/g/sh/x").await;
        let view = BrokerView::new(router, None);
        let mut members = LocalShareMembers::default();
        members.subscribed("a", "$share/g/sh/x");
        members.subscribed("offline-same-group", "$share/g/sh/x");
        members.subscribed("gone", "$share/h/sh/x");

        let checked = view.held_shares(members.candidates("sh/x")).await;

        assert_eq!(checked.held, vec![local("a", "g", true)]);
        assert_eq!(
            pairs(&checked.stale),
            vec![("gone".to_string(), key("h", "sh/x"))]
        );
    }
}
