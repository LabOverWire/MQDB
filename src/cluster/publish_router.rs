use super::{PartitionId, SubscriberLocation, TopicIndex};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingTarget {
    pub client_id: String,
    pub client_partition: PartitionId,
    pub qos: u8,
}

impl RoutingTarget {
    #[must_use]
    pub fn from_subscriber(sub: &SubscriberLocation) -> Option<Self> {
        sub.partition().map(|partition| Self {
            client_id: sub.client_id_str().to_string(),
            client_partition: partition,
            qos: sub.qos,
        })
    }
}

#[derive(Debug, Clone)]
pub struct PublishRouteResult {
    pub targets: Vec<RoutingTarget>,
    pub target_partitions: Vec<PartitionId>,
    pub seq: Option<u64>,
}

impl PublishRouteResult {
    #[must_use]
    pub fn empty() -> Self {
        Self {
            targets: Vec::new(),
            target_partitions: Vec::new(),
            seq: None,
        }
    }
}

pub struct PublishRouter<'a> {
    topic_index: &'a TopicIndex,
}

impl<'a> PublishRouter<'a> {
    pub fn new(topic_index: &'a TopicIndex) -> Self {
        Self { topic_index }
    }

    #[must_use]
    pub fn route(&self, topic: &str) -> PublishRouteResult {
        let subscribers = self.topic_index.get_subscribers(topic);
        if subscribers.is_empty() {
            return PublishRouteResult::empty();
        }

        let seq = self.topic_index.next_seq(topic);

        let mut targets_by_client: HashMap<String, RoutingTarget> = HashMap::new();
        for sub in &subscribers {
            if let Some(target) = RoutingTarget::from_subscriber(sub) {
                let client_id = target.client_id.clone();
                targets_by_client
                    .entry(client_id)
                    .and_modify(|existing| {
                        existing.qos = existing.qos.max(target.qos);
                    })
                    .or_insert(target);
            }
        }

        let targets: Vec<RoutingTarget> = targets_by_client.into_values().collect();

        let mut partitions: Vec<PartitionId> = targets.iter().map(|t| t.client_partition).collect();
        partitions.sort_by_key(|p| p.get());
        partitions.dedup();

        PublishRouteResult {
            targets,
            target_partitions: partitions,
            seq,
        }
    }

    #[must_use]
    pub fn route_with_wildcards(
        &self,
        topic: &str,
        wildcard_matches: &[SubscriberLocation],
    ) -> PublishRouteResult {
        let exact_subscribers = self.topic_index.get_subscribers(topic);
        let seq = self.topic_index.next_seq(topic);

        let mut targets_by_client: HashMap<String, RoutingTarget> = HashMap::new();

        for sub in &exact_subscribers {
            if let Some(target) = RoutingTarget::from_subscriber(sub) {
                let client_id = target.client_id.clone();
                targets_by_client
                    .entry(client_id)
                    .and_modify(|existing| {
                        existing.qos = existing.qos.max(target.qos);
                    })
                    .or_insert(target);
            }
        }

        for sub in wildcard_matches {
            if let Some(target) = RoutingTarget::from_subscriber(sub) {
                let client_id = target.client_id.clone();
                targets_by_client
                    .entry(client_id)
                    .and_modify(|existing| {
                        existing.qos = existing.qos.max(target.qos);
                    })
                    .or_insert(target);
            }
        }

        let targets: Vec<RoutingTarget> = targets_by_client.into_values().collect();

        let mut partitions: Vec<PartitionId> = targets.iter().map(|t| t.client_partition).collect();
        partitions.sort_by_key(|p| p.get());
        partitions.dedup();

        PublishRouteResult {
            targets,
            target_partitions: partitions,
            seq,
        }
    }

    #[must_use]
    pub fn route_targets(
        &self,
        topic: &str,
        wildcard_matches: &[SubscriberLocation],
    ) -> Vec<RoutingTarget> {
        let exact_subscribers = self.topic_index.get_subscribers(topic);

        let mut targets_by_client: HashMap<String, RoutingTarget> = HashMap::new();

        for sub in &exact_subscribers {
            if let Some(target) = RoutingTarget::from_subscriber(sub) {
                let client_id = target.client_id.clone();
                targets_by_client
                    .entry(client_id)
                    .and_modify(|existing| {
                        existing.qos = existing.qos.max(target.qos);
                    })
                    .or_insert(target);
            }
        }

        for sub in wildcard_matches {
            if let Some(target) = RoutingTarget::from_subscriber(sub) {
                let client_id = target.client_id.clone();
                targets_by_client
                    .entry(client_id)
                    .and_modify(|existing| {
                        existing.qos = existing.qos.max(target.qos);
                    })
                    .or_insert(target);
            }
        }

        targets_by_client.into_values().collect()
    }
}

#[must_use]
pub fn effective_qos(publish_qos: u8, subscription_qos: u8) -> u8 {
    publish_qos.min(subscription_qos)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::NodeId;

    fn node(id: u16) -> NodeId {
        NodeId::validated(id).unwrap()
    }

    fn partition(n: u16) -> PartitionId {
        PartitionId::new(n).unwrap()
    }

    #[test]
    fn route_no_subscribers() {
        let index = TopicIndex::new(node(1));
        let router = PublishRouter::new(&index);

        let result = router.route("empty/topic");
        assert!(result.targets.is_empty());
        assert!(result.target_partitions.is_empty());
        assert!(result.seq.is_none());
    }

    #[test]
    fn route_single_subscriber() {
        let index = TopicIndex::new(node(1));
        index
            .subscribe("sensors/temp", "client1", partition(10), 1)
            .unwrap();

        let router = PublishRouter::new(&index);
        let result = router.route("sensors/temp");

        assert_eq!(result.targets.len(), 1);
        assert_eq!(result.targets[0].client_id, "client1");
        assert_eq!(result.targets[0].qos, 1);
        assert_eq!(result.target_partitions.len(), 1);
        assert_eq!(result.target_partitions[0].get(), 10);
        assert_eq!(result.seq, Some(1));
    }

    #[test]
    fn route_multiple_subscribers_same_partition() {
        let index = TopicIndex::new(node(1));
        index
            .subscribe("sensors/temp", "client1", partition(10), 1)
            .unwrap();
        index
            .subscribe("sensors/temp", "client2", partition(10), 2)
            .unwrap();

        let router = PublishRouter::new(&index);
        let result = router.route("sensors/temp");

        assert_eq!(result.targets.len(), 2);
        assert_eq!(result.target_partitions.len(), 1);
    }

    #[test]
    fn route_multiple_subscribers_different_partitions() {
        let index = TopicIndex::new(node(1));
        index
            .subscribe("sensors/temp", "client1", partition(10), 1)
            .unwrap();
        index
            .subscribe("sensors/temp", "client2", partition(20), 2)
            .unwrap();
        index
            .subscribe("sensors/temp", "client3", partition(30), 0)
            .unwrap();

        let router = PublishRouter::new(&index);
        let result = router.route("sensors/temp");

        assert_eq!(result.targets.len(), 3);
        assert_eq!(result.target_partitions.len(), 3);
    }

    #[test]
    fn route_deduplicates_by_client_uses_max_qos() {
        let index = TopicIndex::new(node(1));
        index
            .subscribe("sensors/temp", "client1", partition(10), 1)
            .unwrap();

        let router = PublishRouter::new(&index);

        let wildcard_matches = vec![SubscriberLocation::create("client1", partition(10), 2)];

        let result = router.route_with_wildcards("sensors/temp", &wildcard_matches);

        assert_eq!(result.targets.len(), 1);
        assert_eq!(result.targets[0].qos, 2);
    }

    #[test]
    fn effective_qos_is_minimum() {
        assert_eq!(effective_qos(0, 2), 0);
        assert_eq!(effective_qos(1, 1), 1);
        assert_eq!(effective_qos(2, 1), 1);
        assert_eq!(effective_qos(2, 2), 2);
    }

    #[test]
    fn sequence_increments() {
        let index = TopicIndex::new(node(1));
        index
            .subscribe("sensors/temp", "client1", partition(10), 1)
            .unwrap();

        let router = PublishRouter::new(&index);

        let r1 = router.route("sensors/temp");
        let r2 = router.route("sensors/temp");
        let r3 = router.route("sensors/temp");

        assert_eq!(r1.seq, Some(1));
        assert_eq!(r2.seq, Some(2));
        assert_eq!(r3.seq, Some(3));
    }
}
