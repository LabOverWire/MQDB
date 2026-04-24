// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct ConsumerMember {
    pub consumer_id: String,
    pub assigned_partitions: Vec<u8>,
    pub last_heartbeat: Instant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerMemberInfo {
    pub id: String,
    pub partitions: Vec<u8>,
    pub seconds_since_heartbeat: u64,
}

impl From<&ConsumerMember> for ConsumerMemberInfo {
    fn from(m: &ConsumerMember) -> Self {
        Self {
            id: m.consumer_id.clone(),
            partitions: m.assigned_partitions.clone(),
            seconds_since_heartbeat: m.last_heartbeat.elapsed().as_secs(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupInfo {
    pub name: String,
    pub member_count: usize,
    pub total_partitions: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupDetails {
    pub name: String,
    pub members: Vec<ConsumerMemberInfo>,
    pub total_partitions: u8,
}

#[derive(Debug)]
pub struct ConsumerGroup {
    pub name: String,
    pub num_partitions: u8,
    members: HashMap<String, ConsumerMember>,
    assignments: HashMap<u8, String>,
}

impl ConsumerGroup {
    #[allow(clippy::must_use_candidate)]
    pub fn new(name: String, num_partitions: u8) -> Self {
        Self {
            name,
            num_partitions,
            members: HashMap::new(),
            assignments: HashMap::new(),
        }
    }

    pub fn add_member(&mut self, consumer_id: &str) -> Vec<u8> {
        if self.members.contains_key(consumer_id) {
            return self
                .members
                .get(consumer_id)
                .map(|m| m.assigned_partitions.clone())
                .unwrap_or_default();
        }

        let consumer_id_owned = consumer_id.to_owned();
        self.members.insert(
            consumer_id_owned.clone(),
            ConsumerMember {
                consumer_id: consumer_id_owned,
                assigned_partitions: Vec::new(),
                last_heartbeat: Instant::now(),
            },
        );

        self.rebalance();

        self.members
            .get(consumer_id)
            .map(|m| m.assigned_partitions.clone())
            .unwrap_or_default()
    }

    pub fn remove_member(&mut self, consumer_id: &str) {
        if self.members.remove(consumer_id).is_some() {
            self.rebalance();
        }
    }

    pub fn update_heartbeat(&mut self, consumer_id: &str) {
        if let Some(member) = self.members.get_mut(consumer_id) {
            member.last_heartbeat = Instant::now();
        }
    }

    #[must_use]
    pub fn get_member(&self, consumer_id: &str) -> Option<&ConsumerMember> {
        self.members.get(consumer_id)
    }

    pub fn members(&self) -> impl Iterator<Item = &ConsumerMember> {
        self.members.values()
    }

    #[must_use]
    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    #[must_use]
    pub fn get_partition_owner(&self, partition: u8) -> Option<&str> {
        self.assignments.get(&partition).map(String::as_str)
    }

    pub fn rebalance(&mut self) {
        self.assignments.clear();

        let sorted_ids: Vec<String> = {
            let mut ids: Vec<_> = self.members.keys().cloned().collect();
            ids.sort();
            ids
        };

        if sorted_ids.is_empty() {
            for member in self.members.values_mut() {
                member.assigned_partitions.clear();
            }
            return;
        }

        for member in self.members.values_mut() {
            member.assigned_partitions.clear();
        }

        for partition in 0..self.num_partitions {
            let idx = partition as usize % sorted_ids.len();
            let member_id = &sorted_ids[idx];

            self.assignments.insert(partition, member_id.clone());

            if let Some(member) = self.members.get_mut(member_id) {
                member.assigned_partitions.push(partition);
            }
        }
    }

    pub fn remove_stale_members(&mut self, timeout_ms: u64) -> Vec<String> {
        let timeout = std::time::Duration::from_millis(timeout_ms);
        let now = Instant::now();

        let stale: Vec<String> = self
            .members
            .iter()
            .filter(|(_, m)| now.duration_since(m.last_heartbeat) > timeout)
            .map(|(id, _)| id.clone())
            .collect();

        for id in &stale {
            self.members.remove(id);
        }

        if !stale.is_empty() {
            self.rebalance();
        }

        stale
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_member_gets_all_partitions() {
        let mut group = ConsumerGroup::new("test".into(), 8);
        let partitions = group.add_member("consumer-1");

        assert_eq!(partitions.len(), 8);
        assert_eq!(partitions, vec![0, 1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn test_two_members_split_partitions() {
        let mut group = ConsumerGroup::new("test".into(), 8);
        group.add_member("consumer-1");
        let partitions_2 = group.add_member("consumer-2");

        let partitions_1 = group
            .get_member("consumer-1")
            .map(|m| m.assigned_partitions.clone())
            .unwrap();

        assert_eq!(partitions_1.len(), 4);
        assert_eq!(partitions_2.len(), 4);

        let mut all: Vec<u8> = partitions_1
            .iter()
            .chain(partitions_2.iter())
            .copied()
            .collect();
        all.sort_unstable();
        assert_eq!(all, vec![0, 1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn test_three_members_distribute_partitions() {
        let mut group = ConsumerGroup::new("test".into(), 8);
        group.add_member("a");
        group.add_member("b");
        group.add_member("c");

        let p_a = group.get_member("a").unwrap().assigned_partitions.clone();
        let p_b = group.get_member("b").unwrap().assigned_partitions.clone();
        let p_c = group.get_member("c").unwrap().assigned_partitions.clone();

        assert_eq!(p_a.len(), 3);
        assert_eq!(p_b.len(), 3);
        assert_eq!(p_c.len(), 2);

        let total = p_a.len() + p_b.len() + p_c.len();
        assert_eq!(total, 8);
    }

    #[test]
    fn test_member_removal_triggers_rebalance() {
        let mut group = ConsumerGroup::new("test".into(), 8);
        group.add_member("consumer-1");
        group.add_member("consumer-2");

        group.remove_member("consumer-2");

        let partitions = group
            .get_member("consumer-1")
            .map(|m| m.assigned_partitions.clone())
            .unwrap();

        assert_eq!(partitions.len(), 8);
    }

    #[test]
    fn test_partition_owner_lookup() {
        let mut group = ConsumerGroup::new("test".into(), 4);
        group.add_member("consumer-1");

        assert_eq!(group.get_partition_owner(0), Some("consumer-1"));
        assert_eq!(group.get_partition_owner(3), Some("consumer-1"));
        assert_eq!(group.get_partition_owner(4), None);
    }

    #[test]
    fn test_deterministic_assignment() {
        let mut group1 = ConsumerGroup::new("test".into(), 8);
        group1.add_member("a");
        group1.add_member("b");

        let mut group2 = ConsumerGroup::new("test".into(), 8);
        group2.add_member("b");
        group2.add_member("a");

        let p1_a = group1.get_member("a").unwrap().assigned_partitions.clone();
        let p2_a = group2.get_member("a").unwrap().assigned_partitions.clone();

        assert_eq!(p1_a, p2_a);
    }
}
