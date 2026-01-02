use super::{Epoch, NUM_PARTITIONS, NodeId, PartitionId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionRole {
    None,
    Primary,
    Replica,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionAssignment {
    pub primary: Option<NodeId>,
    pub replicas: Vec<NodeId>,
    pub epoch: Epoch,
}

impl Default for PartitionAssignment {
    fn default() -> Self {
        Self {
            primary: None,
            replicas: Vec::new(),
            epoch: Epoch::ZERO,
        }
    }
}

impl PartitionAssignment {
    #[allow(clippy::must_use_candidate)]
    pub fn new(primary: NodeId, replicas: Vec<NodeId>, epoch: Epoch) -> Self {
        Self {
            primary: Some(primary),
            replicas,
            epoch,
        }
    }

    #[must_use]
    pub fn role_for(&self, node: NodeId) -> PartitionRole {
        if self.primary == Some(node) {
            PartitionRole::Primary
        } else if self.replicas.contains(&node) {
            PartitionRole::Replica
        } else {
            PartitionRole::None
        }
    }

    #[must_use]
    pub fn all_nodes(&self) -> Vec<NodeId> {
        let mut nodes = self.replicas.clone();
        if let Some(primary) = self.primary {
            nodes.insert(0, primary);
        }
        nodes
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionMap {
    version: u64,
    assignments: [PartitionAssignment; NUM_PARTITIONS as usize],
}

impl Default for PartitionMap {
    fn default() -> Self {
        Self::new()
    }
}

impl PartitionMap {
    #[allow(clippy::must_use_candidate)]
    pub fn new() -> Self {
        Self {
            version: 0,
            assignments: std::array::from_fn(|_| PartitionAssignment::default()),
        }
    }

    #[must_use]
    pub fn version(&self) -> u64 {
        self.version
    }

    #[must_use]
    pub fn get(&self, partition: PartitionId) -> &PartitionAssignment {
        &self.assignments[partition.get() as usize]
    }

    pub fn set(&mut self, partition: PartitionId, assignment: PartitionAssignment) {
        self.assignments[partition.get() as usize] = assignment;
        self.version += 1;
    }

    #[must_use]
    pub fn primary(&self, partition: PartitionId) -> Option<NodeId> {
        self.get(partition).primary
    }

    #[must_use]
    pub fn replicas(&self, partition: PartitionId) -> &[NodeId] {
        &self.get(partition).replicas
    }

    #[must_use]
    pub fn epoch(&self, partition: PartitionId) -> Epoch {
        self.get(partition).epoch
    }

    #[must_use]
    pub fn role_for(&self, partition: PartitionId, node: NodeId) -> PartitionRole {
        self.get(partition).role_for(node)
    }

    #[must_use]
    pub fn partitions_for_node(&self, node: NodeId) -> Vec<(PartitionId, PartitionRole)> {
        PartitionId::all()
            .filter_map(|p| {
                let role = self.role_for(p, node);
                if role == PartitionRole::None {
                    None
                } else {
                    Some((p, role))
                }
            })
            .collect()
    }

    #[must_use]
    pub fn primary_count(&self, node: NodeId) -> usize {
        PartitionId::all()
            .filter(|p| self.primary(*p) == Some(node))
            .count()
    }

    #[must_use]
    pub fn replica_count(&self, node: NodeId) -> usize {
        PartitionId::all()
            .filter(|p| self.replicas(*p).contains(&node))
            .count()
    }

    #[must_use]
    pub fn has_any_assignment(&self, node: NodeId) -> bool {
        PartitionId::all().any(|p| self.role_for(p, node) != PartitionRole::None)
    }

    pub fn apply_update(&mut self, update: &super::raft::PartitionUpdate) -> bool {
        let Some(partition) = PartitionId::new(u16::from(update.partition)) else {
            return false;
        };
        let Some(primary) = NodeId::validated(update.primary) else {
            return false;
        };

        let mut replicas = Vec::new();
        if let Some(r1) = NodeId::validated(update.replica1) {
            replicas.push(r1);
        }
        if let Some(r2) = NodeId::validated(update.replica2) {
            replicas.push(r2);
        }

        let epoch = Epoch::new(u64::from(update.epoch));
        self.set(
            partition,
            PartitionAssignment::new(primary, replicas, epoch),
        );
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u16) -> NodeId {
        NodeId::validated(id).unwrap()
    }

    fn partition(id: u16) -> PartitionId {
        PartitionId::new(id).unwrap()
    }

    #[test]
    fn partition_map_default_empty() {
        let map = PartitionMap::new();
        assert_eq!(map.version(), 0);
        assert!(map.primary(partition(0)).is_none());
    }

    #[test]
    fn partition_assignment() {
        let mut map = PartitionMap::new();

        let assignment = PartitionAssignment::new(node(1), vec![node(2), node(3)], Epoch::new(1));

        map.set(partition(0), assignment);

        assert_eq!(map.primary(partition(0)), Some(node(1)));
        assert_eq!(map.replicas(partition(0)), &[node(2), node(3)]);
        assert_eq!(map.epoch(partition(0)), Epoch::new(1));
        assert_eq!(map.version(), 1);
    }

    #[test]
    fn role_for_node() {
        let mut map = PartitionMap::new();

        let assignment = PartitionAssignment::new(node(1), vec![node(2), node(3)], Epoch::new(1));
        map.set(partition(0), assignment);

        assert_eq!(map.role_for(partition(0), node(1)), PartitionRole::Primary);
        assert_eq!(map.role_for(partition(0), node(2)), PartitionRole::Replica);
        assert_eq!(map.role_for(partition(0), node(3)), PartitionRole::Replica);
        assert_eq!(map.role_for(partition(0), node(4)), PartitionRole::None);
    }

    #[test]
    fn partitions_for_node() {
        let mut map = PartitionMap::new();

        map.set(
            partition(0),
            PartitionAssignment::new(node(1), vec![node(2)], Epoch::new(1)),
        );
        map.set(
            partition(1),
            PartitionAssignment::new(node(2), vec![node(1)], Epoch::new(1)),
        );
        map.set(
            partition(2),
            PartitionAssignment::new(node(1), vec![node(3)], Epoch::new(1)),
        );

        let node1_partitions = map.partitions_for_node(node(1));
        assert_eq!(node1_partitions.len(), 3);
        assert_eq!(map.primary_count(node(1)), 2);
        assert_eq!(map.replica_count(node(1)), 1);
    }
}
