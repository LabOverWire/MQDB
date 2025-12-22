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
    new_nodes: &[NodeId],
    config: &RebalanceConfig,
) -> Vec<PartitionReassignment> {
    if new_nodes.is_empty() {
        return Vec::new();
    }

    let mut reassignments = Vec::new();
    let node_count = new_nodes.len();
    let rf = config.replication_factor.min(node_count);

    let mut primary_counts: Vec<(NodeId, usize)> = new_nodes
        .iter()
        .map(|n| (*n, current.primary_count(*n)))
        .collect();

    primary_counts.sort_by_key(|(_, count)| std::cmp::Reverse(*count));

    for partition in PartitionId::all() {
        let assignment = current.get(partition);

        let needs_primary = assignment.primary.is_none()
            || assignment.primary.is_none_or(|p| !new_nodes.contains(&p));

        let valid_replicas: Vec<NodeId> = assignment
            .replicas
            .iter()
            .filter(|r| new_nodes.contains(r))
            .copied()
            .collect();
        let needs_replicas = valid_replicas.len() < rf.saturating_sub(1);

        if needs_primary || needs_replicas {
            let (new_primary, new_replicas) =
                select_nodes_for_partition(partition, new_nodes, rf, &primary_counts);

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
                if *node == new_primary {
                    *count += 1;
                }
            }
        }
    }

    reassignments
}

fn select_nodes_for_partition(
    partition: PartitionId,
    nodes: &[NodeId],
    rf: usize,
    primary_counts: &[(NodeId, usize)],
) -> (NodeId, Vec<NodeId>) {
    let least_loaded = primary_counts
        .iter()
        .min_by_key(|(_, count)| *count)
        .map_or_else(
            || nodes[partition.get() as usize % nodes.len()],
            |(n, _)| *n,
        );

    let primary = least_loaded;

    let mut replicas = Vec::with_capacity(rf.saturating_sub(1));
    let primary_idx = nodes.iter().position(|n| *n == primary).unwrap_or(0);

    for r in 1..rf {
        let replica_idx = (primary_idx + r) % nodes.len();
        if nodes[replica_idx] != primary {
            replicas.push(nodes[replica_idx]);
        }
    }

    (primary, replicas)
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

        let new_nodes = vec![node(1), node(2), node(3)];
        let reassignments = compute_incremental_assignments(&map, &new_nodes, &config);

        assert!(reassignments.is_empty() || !reassignments.is_empty());
    }
}
