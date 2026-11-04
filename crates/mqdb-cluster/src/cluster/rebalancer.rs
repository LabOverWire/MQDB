// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{Epoch, NUM_PARTITIONS, NodeId, PartitionAssignment, PartitionId, PartitionMap};

pub struct RebalanceConfig {
    pub replication_factor: usize,
}

impl Default for RebalanceConfig {
    fn default() -> Self {
        Self {
            replication_factor: 2,
        }
    }
}

/// # Panics
/// Panics if partition creation fails (should not happen with valid partition numbers).
#[must_use]
pub fn compute_balanced_assignments(
    nodes: &[NodeId],
    config: &RebalanceConfig,
    current_epoch: Epoch,
) -> PartitionMap {
    let mut map = PartitionMap::new();

    if nodes.is_empty() {
        return map;
    }

    let node_count = nodes.len();
    let rf = config.replication_factor.min(node_count);

    for partition_num in 0..NUM_PARTITIONS {
        let partition = PartitionId::new(partition_num).unwrap();

        let primary_idx = (partition_num as usize) % node_count;
        let primary = nodes[primary_idx];

        let mut replicas = Vec::with_capacity(rf.saturating_sub(1));
        for r in 1..rf {
            let replica_idx = (primary_idx + r) % node_count;
            replicas.push(nodes[replica_idx]);
        }

        let epoch = Epoch::new(current_epoch.get() + 1);
        map.set(
            partition,
            PartitionAssignment::new(primary, replicas, epoch),
        );
    }

    map
}

#[must_use]
pub fn compute_incremental_assignments(
    current: &PartitionMap,
    all_nodes: &[NodeId],
    config: &RebalanceConfig,
) -> Vec<PartitionReassignment> {
    if all_nodes.is_empty() {
        return Vec::new();
    }

    let node_count = all_nodes.len();
    let rf = config.replication_factor.min(node_count);

    let mut reassignments = redistribute_primaries(current, all_nodes, rf);
    add_missing_replicas(current, all_nodes, rf, &mut reassignments);
    redistribute_replicas(current, all_nodes, rf, &mut reassignments);

    reassignments
}

fn redistribute_primaries(
    current: &PartitionMap,
    all_nodes: &[NodeId],
    rf: usize,
) -> Vec<PartitionReassignment> {
    let node_count = all_nodes.len();
    let ideal_per_node = NUM_PARTITIONS as usize / node_count;
    let max_per_node = ideal_per_node + 1;

    let mut primary_counts: std::collections::HashMap<NodeId, usize> = all_nodes
        .iter()
        .map(|n| (*n, current.primary_count(*n)))
        .collect();

    let mut reassignments = Vec::new();

    for partition in PartitionId::all() {
        let assignment = current.get(partition);
        let Some(current_primary) = assignment.primary else {
            continue;
        };

        let current_count = *primary_counts.get(&current_primary).unwrap_or(&0);

        let target_node = assignment
            .replicas
            .iter()
            .filter_map(|replica| primary_counts.get(replica).map(|count| (*replica, *count)))
            .filter(|(_, count)| {
                *count < ideal_per_node || (current_count > max_per_node && *count < current_count)
            })
            .min_by_key(|(_, count)| *count);

        let Some((target, target_count)) = target_node else {
            continue;
        };

        let should_move = current_count > max_per_node
            || (current_count > ideal_per_node && target_count < ideal_per_node);

        if !should_move {
            continue;
        }

        let new_epoch = Epoch::new(assignment.epoch.get() + 1);

        let mut new_replicas: Vec<NodeId> = assignment
            .replicas
            .iter()
            .filter(|r| **r != target)
            .copied()
            .collect();

        if !new_replicas.contains(&current_primary) && new_replicas.len() < rf.saturating_sub(1) {
            new_replicas.push(current_primary);
        }

        new_replicas.truncate(rf.saturating_sub(1));

        reassignments.push(PartitionReassignment {
            partition,
            old_primary: assignment.primary,
            new_primary: target,
            old_replicas: assignment.replicas.clone(),
            new_replicas,
            new_epoch,
        });

        primary_counts.insert(current_primary, current_count.saturating_sub(1));
        primary_counts.insert(target, target_count + 1);
    }

    reassignments
}

fn add_missing_replicas(
    current: &PartitionMap,
    all_nodes: &[NodeId],
    rf: usize,
    reassignments: &mut Vec<PartitionReassignment>,
) {
    let reassigned: std::collections::HashSet<u16> =
        reassignments.iter().map(|r| r.partition.get()).collect();
    let mut replica_counts: std::collections::HashMap<NodeId, usize> = all_nodes
        .iter()
        .map(|node| (*node, current.replica_count(*node)))
        .collect();

    for partition in PartitionId::all() {
        if reassigned.contains(&partition.get()) {
            continue;
        }

        let assignment = current.get(partition);
        let Some(primary) = assignment.primary.filter(|p| all_nodes.contains(p)) else {
            continue;
        };

        let desired_replica_count = rf.saturating_sub(1);
        if assignment.replicas.len() >= desired_replica_count {
            continue;
        }

        let mut new_replicas = assignment.replicas.clone();
        while new_replicas.len() < desired_replica_count {
            let Some(least_loaded) = all_nodes
                .iter()
                .copied()
                .filter(|node| *node != primary && !new_replicas.contains(node))
                .min_by_key(|node| replica_counts.get(node).copied().unwrap_or(0))
            else {
                break;
            };
            new_replicas.push(least_loaded);
            *replica_counts.entry(least_loaded).or_insert(0) += 1;
        }

        if new_replicas.len() > assignment.replicas.len() {
            reassignments.push(PartitionReassignment {
                partition,
                old_primary: Some(primary),
                new_primary: primary,
                old_replicas: assignment.replicas.clone(),
                new_replicas,
                new_epoch: Epoch::new(assignment.epoch.get() + 1),
            });
        }
    }
}

fn redistribute_replicas(
    current: &PartitionMap,
    all_nodes: &[NodeId],
    rf: usize,
    reassignments: &mut Vec<PartitionReassignment>,
) {
    let node_count = all_nodes.len();
    if node_count < 2 || rf < 2 {
        return;
    }

    let reassigned: std::collections::HashSet<u16> =
        reassignments.iter().map(|r| r.partition.get()).collect();

    let ideal_per_node = NUM_PARTITIONS as usize / node_count;
    let underweight_threshold = ideal_per_node / 2;

    let mut replica_counts: Vec<(NodeId, usize)> = all_nodes
        .iter()
        .map(|n| (*n, current.replica_count(*n)))
        .collect();

    let underweight_nodes: Vec<NodeId> = replica_counts
        .iter()
        .filter(|(_, count)| *count <= underweight_threshold)
        .map(|(n, _)| *n)
        .collect();

    if underweight_nodes.is_empty() {
        return;
    }

    let overloaded: Vec<(NodeId, usize)> = replica_counts
        .iter()
        .filter(|(_, count)| *count > ideal_per_node)
        .copied()
        .collect();

    let partitions_to_move = underweight_nodes.len() * ideal_per_node;
    let mut moved = 0;
    let mut target_idx = 0;

    for (overloaded_node, count) in &overloaded {
        if moved >= partitions_to_move || target_idx >= underweight_nodes.len() {
            break;
        }

        let excess = count.saturating_sub(ideal_per_node);
        let mut moved_from_node = 0;

        for partition in PartitionId::all() {
            if moved >= partitions_to_move || target_idx >= underweight_nodes.len() {
                break;
            }
            if moved_from_node >= excess {
                break;
            }
            if reassigned.contains(&partition.get()) {
                continue;
            }

            let assignment = current.get(partition);
            let Some(primary) = assignment.primary.filter(|p| all_nodes.contains(p)) else {
                continue;
            };

            if !assignment.replicas.contains(overloaded_node) {
                continue;
            }

            let target_node = underweight_nodes[target_idx];
            if target_node == primary {
                continue;
            }

            let new_replicas: Vec<NodeId> = assignment
                .replicas
                .iter()
                .map(|r| {
                    if *r == *overloaded_node {
                        target_node
                    } else {
                        *r
                    }
                })
                .collect();

            let new_epoch = Epoch::new(assignment.epoch.get() + 1);

            reassignments.push(PartitionReassignment {
                partition,
                old_primary: Some(primary),
                new_primary: primary,
                old_replicas: assignment.replicas.clone(),
                new_replicas,
                new_epoch,
            });

            moved += 1;
            moved_from_node += 1;

            for (node, c) in &mut replica_counts {
                if *node == target_node {
                    *c += 1;
                    if *c >= ideal_per_node {
                        target_idx += 1;
                    }
                }
            }
        }
    }
}

#[must_use]
pub fn compute_removal_assignments(
    current: &PartitionMap,
    remaining_nodes: &[NodeId],
    removed_node: NodeId,
    config: &RebalanceConfig,
) -> Vec<PartitionReassignment> {
    if remaining_nodes.is_empty() {
        return Vec::new();
    }

    let mut reassignments = Vec::new();
    let rf = config.replication_factor.min(remaining_nodes.len());

    let mut primary_counts: Vec<(NodeId, usize)> = remaining_nodes
        .iter()
        .map(|n| (*n, current.primary_count(*n)))
        .collect();

    for partition in PartitionId::all() {
        let assignment = current.get(partition);

        let primary_removed = assignment.primary == Some(removed_node);
        let replica_removed = assignment.replicas.contains(&removed_node);

        if primary_removed || replica_removed {
            let valid_replicas: Vec<NodeId> = assignment
                .replicas
                .iter()
                .filter(|r| **r != removed_node)
                .copied()
                .collect();

            let new_primary = if primary_removed {
                if valid_replicas.is_empty() {
                    let least = primary_counts
                        .iter()
                        .min_by_key(|(_, c)| *c)
                        .map(|(n, _)| *n);
                    least.unwrap_or(remaining_nodes[0])
                } else {
                    let Some(live_replica) = valid_replicas
                        .iter()
                        .find(|replica| remaining_nodes.contains(replica))
                    else {
                        continue;
                    };
                    *live_replica
                }
            } else {
                assignment.primary.unwrap_or(remaining_nodes[0])
            };

            let mut new_replicas: Vec<NodeId> = valid_replicas
                .into_iter()
                .filter(|r| *r != new_primary)
                .collect();

            while new_replicas.len() < rf.saturating_sub(1) {
                let primary_idx = remaining_nodes
                    .iter()
                    .position(|n| *n == new_primary)
                    .unwrap_or(0);
                for offset in 1..remaining_nodes.len() {
                    let candidate = remaining_nodes[(primary_idx + offset) % remaining_nodes.len()];
                    if candidate != new_primary && !new_replicas.contains(&candidate) {
                        new_replicas.push(candidate);
                        break;
                    }
                }
                if new_replicas.len() >= rf.saturating_sub(1) || remaining_nodes.len() <= 1 {
                    break;
                }
            }

            let new_epoch = Epoch::new(assignment.epoch.get() + 1);

            reassignments.push(PartitionReassignment {
                partition,
                old_primary: assignment.primary,
                new_primary,
                old_replicas: assignment.replicas.clone(),
                new_replicas,
                new_epoch,
            });

            for (node, count) in &mut primary_counts {
                if *node == new_primary && primary_removed {
                    *count += 1;
                }
            }
        }
    }

    reassignments
}

#[derive(Debug, Clone)]
pub struct PartitionReassignment {
    pub partition: PartitionId,
    pub old_primary: Option<NodeId>,
    pub new_primary: NodeId,
    pub old_replicas: Vec<NodeId>,
    pub new_replicas: Vec<NodeId>,
    pub new_epoch: Epoch,
}

impl PartitionReassignment {
    #[must_use]
    pub fn to_assignment(&self) -> PartitionAssignment {
        PartitionAssignment::new(self.new_primary, self.new_replicas.clone(), self.new_epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u16) -> NodeId {
        NodeId::validated(id).unwrap()
    }

    #[test]
    fn balanced_single_node() {
        let nodes = vec![node(1)];
        let config = RebalanceConfig::default();
        let map = compute_balanced_assignments(&nodes, &config, Epoch::ZERO);

        for p in PartitionId::all() {
            assert_eq!(map.primary(p), Some(node(1)));
            assert!(map.replicas(p).is_empty());
        }
    }

    #[test]
    fn balanced_three_nodes() {
        let nodes = vec![node(1), node(2), node(3)];
        let config = RebalanceConfig::default();
        let map = compute_balanced_assignments(&nodes, &config, Epoch::ZERO);

        let mut counts = [0usize; 3];
        for p in PartitionId::all() {
            let primary = map.primary(p).unwrap();
            let idx = nodes.iter().position(|n| *n == primary).unwrap();
            counts[idx] += 1;

            let replicas = map.replicas(p);
            assert_eq!(replicas.len(), 1);
            assert!(!replicas.contains(&primary));
        }

        let expected_min = NUM_PARTITIONS as usize / 3;
        let expected_max = expected_min + 1;
        for count in counts {
            assert!((expected_min..=expected_max).contains(&count));
        }
    }

    #[test]
    fn balanced_five_nodes_rf3() {
        let nodes = vec![node(1), node(2), node(3), node(4), node(5)];
        let config = RebalanceConfig {
            replication_factor: 3,
        };
        let map = compute_balanced_assignments(&nodes, &config, Epoch::ZERO);

        for p in PartitionId::all() {
            let replicas = map.replicas(p);
            assert_eq!(replicas.len(), 2);
        }
    }

    #[test]
    fn removal_promotes_replica() {
        let nodes = vec![node(1), node(2), node(3)];
        let config = RebalanceConfig::default();
        let map = compute_balanced_assignments(&nodes, &config, Epoch::ZERO);

        let partition_on_node1 = PartitionId::all()
            .find(|p| map.primary(*p) == Some(node(1)))
            .unwrap();

        let remaining = vec![node(2), node(3)];
        let reassignments = compute_removal_assignments(&map, &remaining, node(1), &config);

        assert!(!reassignments.is_empty());

        let reassignment = reassignments
            .iter()
            .find(|r| r.partition == partition_on_node1);
        assert!(reassignment.is_some());
        let r = reassignment.unwrap();
        assert!(r.new_primary == node(2) || r.new_primary == node(3));
    }

    #[test]
    fn incremental_rebalance() {
        let initial_nodes = vec![node(1), node(2)];
        let config = RebalanceConfig::default();
        let map = compute_balanced_assignments(&initial_nodes, &config, Epoch::ZERO);

        assert_eq!(map.primary_count(node(1)), NUM_PARTITIONS as usize / 2);
        assert_eq!(map.primary_count(node(2)), NUM_PARTITIONS as usize / 2);
        assert_eq!(map.primary_count(node(3)), 0);

        let all_nodes = vec![node(1), node(2), node(3)];

        let step1 = compute_incremental_assignments(&map, &all_nodes, &config);
        assert!(!step1.is_empty());

        let mut map2 = map.clone();
        for r in &step1 {
            map2.set(r.partition, r.to_assignment());
        }
        assert_eq!(map2.primary_count(node(3)), 0);
        assert!(map2.replica_count(node(3)) > 0);

        let step2 = compute_incremental_assignments(&map2, &all_nodes, &config);
        let mut map3 = map2.clone();
        for r in &step2 {
            map3.set(r.partition, r.to_assignment());
        }

        let expected_per_node = NUM_PARTITIONS as usize / 3;
        assert!(
            map3.primary_count(node(3)) >= expected_per_node,
            "expected ~{expected_per_node} primaries, got {}",
            map3.primary_count(node(3))
        );
    }

    #[test]
    fn new_node_added_as_replica_for_unmoved_partitions() {
        let config = RebalanceConfig::default();
        let mut map = PartitionMap::new();

        for partition_num in 0..NUM_PARTITIONS {
            let partition = PartitionId::new(partition_num).unwrap();
            map.set(
                partition,
                PartitionAssignment::new(node(1), vec![], Epoch::new(1)),
            );
        }

        assert_eq!(map.primary_count(node(1)), NUM_PARTITIONS as usize);
        assert_eq!(map.replica_count(node(1)), 0);

        let all_nodes = vec![node(1), node(2)];
        let reassignments = compute_incremental_assignments(&map, &all_nodes, &config);

        assert!(!reassignments.is_empty());
        assert_eq!(reassignments.len(), NUM_PARTITIONS as usize);

        let primary_moves: Vec<_> = reassignments
            .iter()
            .filter(|r| r.new_primary != r.old_primary.unwrap())
            .collect();
        assert_eq!(primary_moves.len(), 0);

        for r in &reassignments {
            assert!(
                r.new_replicas.contains(&node(2)),
                "partition {} should have node 2 as replica",
                r.partition.get()
            );
        }
    }

    #[test]
    fn redistribute_replicas_to_new_node() {
        let initial_nodes = vec![node(1), node(2)];
        let config = RebalanceConfig::default();
        let map = compute_balanced_assignments(&initial_nodes, &config, Epoch::ZERO);

        let half = NUM_PARTITIONS as usize / 2;
        assert_eq!(map.primary_count(node(1)), half);
        assert_eq!(map.primary_count(node(2)), half);
        assert_eq!(map.replica_count(node(1)), half);
        assert_eq!(map.replica_count(node(2)), half);
        assert_eq!(map.replica_count(node(3)), 0);

        let all_nodes = vec![node(1), node(2), node(3)];

        let step1 = compute_incremental_assignments(&map, &all_nodes, &config);
        let mut map2 = map.clone();
        for r in &step1 {
            map2.set(r.partition, r.to_assignment());
        }
        assert_eq!(map2.primary_count(node(3)), 0);
        assert!(
            map2.replica_count(node(3)) > 0,
            "node 3 should have replicas after step 1"
        );

        let step2 = compute_incremental_assignments(&map2, &all_nodes, &config);
        let mut map3 = map2.clone();
        for r in &step2 {
            map3.set(r.partition, r.to_assignment());
        }

        let expected_per_node = NUM_PARTITIONS as usize / 3;
        assert!(
            map3.primary_count(node(3)) >= expected_per_node,
            "expected ~{expected_per_node} primaries, got {}",
            map3.primary_count(node(3))
        );

        let step3 = compute_incremental_assignments(&map3, &all_nodes, &config);
        let mut map4 = map3.clone();
        for r in &step3 {
            map4.set(r.partition, r.to_assignment());
        }

        assert!(
            map4.replica_count(node(3)) >= expected_per_node,
            "expected ~{expected_per_node} replicas after 3 steps, got {}",
            map4.replica_count(node(3))
        );
    }

    fn apply(map: &mut PartitionMap, moves: &[PartitionReassignment]) {
        for change in moves {
            map.set(change.partition, change.to_assignment());
        }
    }

    fn after_a_failover_of(map: &PartitionMap, dead: NodeId) -> PartitionMap {
        let mut after = PartitionMap::new();
        for partition in PartitionId::all() {
            let assignment = map.get(partition);
            let replicas: Vec<NodeId> = assignment
                .replicas
                .iter()
                .copied()
                .filter(|replica| *replica != dead)
                .collect();
            let primary = assignment.primary.filter(|primary| *primary != dead);
            let (primary, replicas) = match primary {
                Some(primary) => (primary, replicas),
                None => (replicas[0], replicas[1..].to_vec()),
            };
            after.set(
                partition,
                PartitionAssignment::new(primary, replicas, Epoch::new(2)),
            );
        }
        after
    }

    fn rounds_until_returned(count: u16, done: impl Fn(&PartitionMap, &[NodeId]) -> bool) -> usize {
        let nodes: Vec<NodeId> = (1..=count).map(node).collect();
        let balanced =
            compute_balanced_assignments(&nodes, &RebalanceConfig::default(), Epoch::ZERO);
        let mut map = after_a_failover_of(&balanced, node(count));
        for round in 1..=10 {
            let moves = compute_incremental_assignments(&map, &nodes, &RebalanceConfig::default());
            apply(&mut map, &moves);
            if done(&map, &nodes) {
                return round;
            }
        }
        11
    }

    #[test]
    fn a_returned_node_is_a_replica_after_the_first_rebalance() {
        for count in [3u16, 5, 7] {
            let rounds = rounds_until_returned(count, |map, nodes| {
                nodes
                    .last()
                    .is_some_and(|last| map.replica_count(*last) > 0)
            });
            assert_eq!(rounds, 1, "{count} nodes");
        }
    }

    #[test]
    fn a_returned_node_nearly_holds_its_share_of_primaries_after_the_second_rebalance() {
        for count in [3u16, 5, 7] {
            let share = NUM_PARTITIONS as usize / usize::from(count);
            let rounds = rounds_until_returned(count, |map, nodes| {
                nodes
                    .last()
                    .is_some_and(|last| map.primary_count(*last) + 2 >= share)
            });
            assert_eq!(rounds, 2, "{count} nodes");
        }
    }

    #[test]
    fn primaries_are_within_one_after_the_fourth_rebalance_of_a_returned_node() {
        for (count, expected) in [(3u16, 2), (5, 4), (7, 4)] {
            let rounds = rounds_until_returned(count, |map, nodes| {
                let counts: Vec<usize> = nodes.iter().map(|n| map.primary_count(*n)).collect();
                counts
                    .iter()
                    .max()
                    .zip(counts.iter().min())
                    .is_some_and(|(max, min)| max - min <= 1)
            });
            assert_eq!(rounds, expected, "{count} nodes");
        }
    }

    #[test]
    fn a_departed_primary_with_no_replica_gets_no_replica_added() {
        let config = RebalanceConfig::default();
        let mut map = PartitionMap::default();
        for partition in PartitionId::all() {
            map.set(
                partition,
                crate::cluster::PartitionAssignment::new(node(1), Vec::new(), Epoch::new(1)),
            );
        }
        let reassignments = compute_incremental_assignments(&map, &[node(2), node(3)], &config);
        assert!(reassignments.iter().all(|r| r.new_primary != node(1)));
    }

    #[test]
    fn incremental_rebalance_with_a_departed_replica_does_not_panic() {
        let config = RebalanceConfig::default();
        let map = compute_balanced_assignments(&[node(1), node(2)], &config, Epoch::ZERO);
        let reassignments =
            compute_incremental_assignments(&map, &[node(2), node(3), node(4)], &config);
        assert!(reassignments.iter().all(|r| r.new_primary != node(1)));
    }
}
