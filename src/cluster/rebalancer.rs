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

    reassignments
}

fn redistribute_primaries(
    current: &PartitionMap,
    all_nodes: &[NodeId],
    rf: usize,
) -> Vec<PartitionReassignment> {
    let node_count = all_nodes.len();
    let ideal_per_node = NUM_PARTITIONS as usize / node_count;
    let remainder = NUM_PARTITIONS as usize % node_count;

    let mut primary_counts: Vec<(NodeId, usize)> = all_nodes
        .iter()
        .map(|n| (*n, current.primary_count(*n)))
        .collect();

    let nodes_with_zero: Vec<NodeId> = primary_counts
        .iter()
        .filter(|(_, count)| *count == 0)
        .map(|(n, _)| *n)
        .collect();

    if nodes_with_zero.is_empty() {
        return Vec::new();
    }

    let mut reassignments = Vec::new();
    let overloaded: Vec<(NodeId, usize)> = primary_counts
        .iter()
        .filter(|(_, count)| *count > ideal_per_node + usize::from(remainder > 0))
        .copied()
        .collect();

    let partitions_to_move = nodes_with_zero.len() * ideal_per_node;
    let mut moved = 0;
    let mut target_idx = 0;

    for (overloaded_node, count) in &overloaded {
        if moved >= partitions_to_move || target_idx >= nodes_with_zero.len() {
            break;
        }

        let excess = count.saturating_sub(ideal_per_node);
        let mut moved_from_node = 0;

        for partition in PartitionId::all() {
            if moved >= partitions_to_move || target_idx >= nodes_with_zero.len() {
                break;
            }
            if moved_from_node >= excess {
                break;
            }

            let assignment = current.get(partition);
            if assignment.primary != Some(*overloaded_node) {
                continue;
            }

            let target_node = nodes_with_zero[target_idx];
            let new_epoch = Epoch::new(assignment.epoch.get() + 1);

            let mut new_replicas: Vec<NodeId> = assignment
                .replicas
                .iter()
                .filter(|r| **r != target_node)
                .copied()
                .collect();

            if !new_replicas.contains(overloaded_node) && new_replicas.len() < rf.saturating_sub(1)
            {
                new_replicas.push(*overloaded_node);
            }

            new_replicas.truncate(rf.saturating_sub(1));

            reassignments.push(PartitionReassignment {
                partition,
                old_primary: assignment.primary,
                new_primary: target_node,
                old_replicas: assignment.replicas.clone(),
                new_replicas,
                new_epoch,
            });

            moved += 1;
            moved_from_node += 1;

            for (node, c) in &mut primary_counts {
                if *node == target_node {
                    *c += 1;
                    if *c >= ideal_per_node {
                        target_idx += 1;
                    }
                }
            }
        }
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

    for partition in PartitionId::all() {
        if reassigned.contains(&partition.get()) {
            continue;
        }

        let assignment = current.get(partition);
        let Some(primary) = assignment.primary else {
            continue;
        };

        let desired_replica_count = rf.saturating_sub(1);
        if assignment.replicas.len() >= desired_replica_count {
            continue;
        }

        let mut new_replicas = assignment.replicas.clone();
        for node in all_nodes {
            if *node == primary || new_replicas.contains(node) {
                continue;
            }
            new_replicas.push(*node);
            if new_replicas.len() >= desired_replica_count {
                break;
            }
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
                    valid_replicas[0]
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

        for count in counts {
            assert!((20..=23).contains(&count));
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

        assert_eq!(map.primary_count(node(1)), 32);
        assert_eq!(map.primary_count(node(2)), 32);
        assert_eq!(map.primary_count(node(3)), 0);

        let all_nodes = vec![node(1), node(2), node(3)];
        let reassignments = compute_incremental_assignments(&map, &all_nodes, &config);

        assert!(!reassignments.is_empty());
        assert!(reassignments.len() >= 20);

        let mut new_node3_count = 0;
        for r in &reassignments {
            if r.new_primary == node(3) {
                new_node3_count += 1;
            }
        }
        assert!(new_node3_count >= 20);
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

        assert_eq!(map.primary_count(node(1)), 64);
        assert_eq!(map.replica_count(node(1)), 0);

        let all_nodes = vec![node(1), node(2)];
        let reassignments = compute_incremental_assignments(&map, &all_nodes, &config);

        assert!(!reassignments.is_empty());
        assert_eq!(reassignments.len(), 64);

        let primary_moves: Vec<_> = reassignments
            .iter()
            .filter(|r| r.new_primary != r.old_primary.unwrap())
            .collect();
        assert_eq!(primary_moves.len(), 32);

        let replica_adds: Vec<_> = reassignments
            .iter()
            .filter(|r| {
                r.new_primary == r.old_primary.unwrap() && r.new_replicas.contains(&node(2))
            })
            .collect();
        assert_eq!(replica_adds.len(), 32);

        for r in &reassignments {
            let has_node2 = r.new_primary == node(2) || r.new_replicas.contains(&node(2));
            assert!(has_node2, "partition {} should have node 2", r.partition.get());
        }
    }
}
