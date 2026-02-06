// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{PartitionId, session_partition};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionType {
    Mqtt,
    Db,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WildcardSubscriber {
    pub client_id: String,
    pub client_partition: PartitionId,
    pub qos: u8,
    pub subscription_type: SubscriptionType,
}

impl WildcardSubscriber {
    #[must_use]
    pub fn mqtt(client_id: &str, qos: u8) -> Self {
        let partition = session_partition(client_id);
        Self {
            client_id: client_id.to_string(),
            client_partition: partition,
            qos,
            subscription_type: SubscriptionType::Mqtt,
        }
    }

    #[must_use]
    pub fn db(client_id: &str, qos: u8) -> Self {
        let partition = session_partition(client_id);
        Self {
            client_id: client_id.to_string(),
            client_partition: partition,
            qos,
            subscription_type: SubscriptionType::Db,
        }
    }
}

#[derive(Debug, Default)]
struct TrieNode {
    children: HashMap<String, TrieNode>,
    single_wildcard: Option<Box<TrieNode>>,
    multi_wildcard: Option<Box<TrieNode>>,
    subscribers: Vec<WildcardSubscriber>,
}

impl TrieNode {
    fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Default)]
pub struct TopicTrie {
    root: TrieNode,
    pattern_count: usize,
}

impl TopicTrie {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, pattern: &str, subscriber: WildcardSubscriber) {
        let levels: Vec<&str> = pattern.split('/').collect();
        let mut node = &mut self.root;

        for level in &levels {
            node = match *level {
                "+" => node
                    .single_wildcard
                    .get_or_insert_with(|| Box::new(TrieNode::new())),
                "#" => node
                    .multi_wildcard
                    .get_or_insert_with(|| Box::new(TrieNode::new())),
                _ => node.children.entry(level.to_string()).or_default(),
            };
        }

        if !node
            .subscribers
            .iter()
            .any(|s| s.client_id == subscriber.client_id)
        {
            node.subscribers.push(subscriber);
            self.pattern_count += 1;
        }
    }

    pub fn remove(&mut self, pattern: &str, client_id: &str) -> bool {
        let levels: Vec<&str> = pattern.split('/').collect();
        let removed = Self::remove_recursive(&mut self.root, &levels, 0, client_id);
        if removed {
            self.pattern_count = self.pattern_count.saturating_sub(1);
        }
        removed
    }

    fn remove_recursive(
        node: &mut TrieNode,
        levels: &[&str],
        depth: usize,
        client_id: &str,
    ) -> bool {
        if depth == levels.len() {
            let len_before = node.subscribers.len();
            node.subscribers.retain(|s| s.client_id != client_id);
            return node.subscribers.len() != len_before;
        }

        let level = levels[depth];
        match level {
            "+" => {
                if let Some(ref mut child) = node.single_wildcard {
                    Self::remove_recursive(child, levels, depth + 1, client_id)
                } else {
                    false
                }
            }
            "#" => {
                if let Some(ref mut child) = node.multi_wildcard {
                    Self::remove_recursive(child, levels, depth + 1, client_id)
                } else {
                    false
                }
            }
            _ => {
                if let Some(child) = node.children.get_mut(level) {
                    Self::remove_recursive(child, levels, depth + 1, client_id)
                } else {
                    false
                }
            }
        }
    }

    #[must_use]
    pub fn match_topic(&self, topic: &str) -> Vec<&WildcardSubscriber> {
        if topic.starts_with('$') {
            return Vec::new();
        }

        let levels: Vec<&str> = topic.split('/').collect();
        let mut matches = Vec::new();
        Self::match_recursive(&self.root, &levels, 0, &mut matches);
        matches
    }

    fn match_recursive<'a>(
        node: &'a TrieNode,
        levels: &[&str],
        depth: usize,
        matches: &mut Vec<&'a WildcardSubscriber>,
    ) {
        if let Some(ref multi) = node.multi_wildcard {
            matches.extend(multi.subscribers.iter());
        }

        if depth == levels.len() {
            matches.extend(node.subscribers.iter());
            return;
        }

        let level = levels[depth];

        if let Some(child) = node.children.get(level) {
            Self::match_recursive(child, levels, depth + 1, matches);
        }

        if let Some(ref single) = node.single_wildcard {
            Self::match_recursive(single, levels, depth + 1, matches);
        }
    }

    #[must_use]
    pub fn pattern_count(&self) -> usize {
        self.pattern_count
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pattern_count == 0
    }

    #[must_use]
    pub fn all_subscriptions(&self) -> Vec<(String, &WildcardSubscriber)> {
        let mut result = Vec::new();
        Self::collect_subscriptions(&self.root, "", &mut result);
        result
    }

    fn collect_subscriptions<'a>(
        node: &'a TrieNode,
        current_pattern: &str,
        result: &mut Vec<(String, &'a WildcardSubscriber)>,
    ) {
        for subscriber in &node.subscribers {
            result.push((current_pattern.to_string(), subscriber));
        }

        for (segment, child) in &node.children {
            let pattern = if current_pattern.is_empty() {
                segment.clone()
            } else {
                format!("{current_pattern}/{segment}")
            };
            Self::collect_subscriptions(child, &pattern, result);
        }

        if let Some(ref single) = node.single_wildcard {
            let pattern = if current_pattern.is_empty() {
                "+".to_string()
            } else {
                format!("{current_pattern}/+")
            };
            Self::collect_subscriptions(single, &pattern, result);
        }

        if let Some(ref multi) = node.multi_wildcard {
            let pattern = if current_pattern.is_empty() {
                "#".to_string()
            } else {
                format!("{current_pattern}/#")
            };
            Self::collect_subscriptions(multi, &pattern, result);
        }
    }

    pub fn clear_for_partition(&mut self, partition: PartitionId) -> usize {
        let subscriptions_to_remove: Vec<(String, String)> = self
            .all_subscriptions()
            .iter()
            .filter(|(_, sub)| sub.client_partition == partition)
            .map(|(pattern, sub)| (pattern.clone(), sub.client_id.clone()))
            .collect();

        let mut removed = 0;
        for (pattern, client_id) in subscriptions_to_remove {
            if self.remove(&pattern, &client_id) {
                removed += 1;
            }
        }
        removed
    }
}

#[must_use]
pub fn is_wildcard_pattern(pattern: &str) -> bool {
    pattern.contains('+') || pattern.contains('#')
}

#[must_use]
pub fn validate_pattern(pattern: &str) -> bool {
    if pattern.is_empty() {
        return false;
    }

    let levels: Vec<&str> = pattern.split('/').collect();
    for (i, level) in levels.iter().enumerate() {
        if *level == "#" && i != levels.len() - 1 {
            return false;
        }
        if level.contains('#') && *level != "#" {
            return false;
        }
        if level.contains('+') && *level != "+" {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_level_wildcard_match() {
        let mut trie = TopicTrie::new();
        trie.insert("sensors/+/temp", WildcardSubscriber::mqtt("client1", 1));

        let matches = trie.match_topic("sensors/building1/temp");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].client_id, "client1");

        let matches = trie.match_topic("sensors/building2/temp");
        assert_eq!(matches.len(), 1);

        let matches = trie.match_topic("sensors/temp");
        assert_eq!(matches.len(), 0);

        let matches = trie.match_topic("sensors/a/b/temp");
        assert_eq!(matches.len(), 0);
    }

    #[test]
    fn multi_level_wildcard_match() {
        let mut trie = TopicTrie::new();
        trie.insert("sensors/#", WildcardSubscriber::mqtt("client1", 1));

        let matches = trie.match_topic("sensors");
        assert_eq!(matches.len(), 1);

        let matches = trie.match_topic("sensors/temp");
        assert_eq!(matches.len(), 1);

        let matches = trie.match_topic("sensors/building1/temp");
        assert_eq!(matches.len(), 1);

        let matches = trie.match_topic("sensors/a/b/c/d");
        assert_eq!(matches.len(), 1);

        let matches = trie.match_topic("actuators/fan");
        assert_eq!(matches.len(), 0);
    }

    #[test]
    fn exact_match_takes_priority() {
        let mut trie = TopicTrie::new();
        trie.insert("sensors/temp", WildcardSubscriber::mqtt("exact", 1));
        trie.insert("sensors/+", WildcardSubscriber::mqtt("single", 1));
        trie.insert("sensors/#", WildcardSubscriber::mqtt("multi", 1));

        let matches = trie.match_topic("sensors/temp");
        assert_eq!(matches.len(), 3);

        let client_ids: Vec<&str> = matches.iter().map(|s| s.client_id.as_str()).collect();
        assert!(client_ids.contains(&"exact"));
        assert!(client_ids.contains(&"single"));
        assert!(client_ids.contains(&"multi"));
    }

    #[test]
    fn remove_subscription() {
        let mut trie = TopicTrie::new();
        trie.insert("sensors/+/temp", WildcardSubscriber::mqtt("client1", 1));
        trie.insert("sensors/+/temp", WildcardSubscriber::mqtt("client2", 1));

        assert_eq!(trie.pattern_count(), 2);

        let removed = trie.remove("sensors/+/temp", "client1");
        assert!(removed);
        assert_eq!(trie.pattern_count(), 1);

        let matches = trie.match_topic("sensors/building1/temp");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].client_id, "client2");
    }

    #[test]
    fn is_wildcard_pattern_detection() {
        assert!(is_wildcard_pattern("sensors/+/temp"));
        assert!(is_wildcard_pattern("sensors/#"));
        assert!(is_wildcard_pattern("+/temp"));
        assert!(is_wildcard_pattern("#"));

        assert!(!is_wildcard_pattern("sensors/temp"));
        assert!(!is_wildcard_pattern("sensors/building1/temp"));
    }

    #[test]
    fn validate_pattern_checks() {
        assert!(validate_pattern("sensors/+/temp"));
        assert!(validate_pattern("sensors/#"));
        assert!(validate_pattern("+/+/+"));
        assert!(validate_pattern("#"));
        assert!(validate_pattern("sensors/temp"));

        assert!(!validate_pattern("sensors/#/temp"));
        assert!(!validate_pattern("sensors/te+mp"));
        assert!(!validate_pattern("sensors/te#mp"));
        assert!(!validate_pattern(""));
    }

    #[test]
    fn complex_pattern_matching() {
        let mut trie = TopicTrie::new();
        trie.insert("home/+/temperature", WildcardSubscriber::mqtt("c1", 1));
        trie.insert("home/living/+", WildcardSubscriber::mqtt("c2", 1));
        trie.insert("home/#", WildcardSubscriber::mqtt("c3", 1));
        trie.insert("+/+/humidity", WildcardSubscriber::mqtt("c4", 1));

        let matches = trie.match_topic("home/living/temperature");
        let client_ids: Vec<&str> = matches.iter().map(|s| s.client_id.as_str()).collect();
        assert!(client_ids.contains(&"c1"));
        assert!(client_ids.contains(&"c2"));
        assert!(client_ids.contains(&"c3"));

        let matches = trie.match_topic("home/kitchen/humidity");
        let client_ids: Vec<&str> = matches.iter().map(|s| s.client_id.as_str()).collect();
        assert!(client_ids.contains(&"c3"));
        assert!(client_ids.contains(&"c4"));
    }

    #[test]
    fn sys_topic_protection() {
        let mut trie = TopicTrie::new();
        trie.insert("#", WildcardSubscriber::mqtt("client1", 1));

        let matches = trie.match_topic("$SYS/mqdb/stats");
        assert_eq!(matches.len(), 0);
    }
}
