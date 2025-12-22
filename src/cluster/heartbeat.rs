use super::protocol::Heartbeat;
use super::transport::{ClusterMessage, TransportConfig};
use super::{NodeId, PartitionMap, PartitionRole};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    Unknown,
    Alive,
    Suspected,
    Dead,
}

#[derive(Debug)]
struct NodeState {
    last_heartbeat: u64,
    status: NodeStatus,
    missed_count: u32,
}

#[derive(Debug)]
pub struct HeartbeatManager {
    local_node: NodeId,
    config: TransportConfig,
    nodes: HashMap<u16, NodeState>,
    last_sent: Option<u64>,
    partition_map: PartitionMap,
}

impl HeartbeatManager {
    #[must_use]
    pub fn new(local_node: NodeId, config: TransportConfig) -> Self {
        Self {
            local_node,
            config,
            nodes: HashMap::new(),
            last_sent: None,
            partition_map: PartitionMap::default(),
        }
    }

    pub fn register_node(&mut self, node_id: NodeId) {
        if node_id != self.local_node {
            self.nodes.insert(
                node_id.get(),
                NodeState {
                    last_heartbeat: 0,
                    status: NodeStatus::Unknown,
                    missed_count: 0,
                },
            );
        }
    }

    pub fn update_partition_map(&mut self, map: PartitionMap) {
        self.partition_map = map;
    }

    #[must_use]
    pub fn should_send(&self, now: u64) -> bool {
        match self.last_sent {
            None => true,
            Some(last) => now >= last + self.config.heartbeat_interval_ms,
        }
    }

    pub fn create_heartbeat(&mut self, now: u64) -> ClusterMessage {
        self.last_sent = Some(now);

        let mut hb = Heartbeat::create(self.local_node, now);

        for (partition, role) in self.partition_map.partitions_for_node(self.local_node) {
            match role {
                PartitionRole::Primary => hb.set_primary(partition),
                PartitionRole::Replica => hb.set_replica(partition),
                PartitionRole::None => {}
            }
        }

        ClusterMessage::Heartbeat(hb)
    }

    pub fn receive_heartbeat(&mut self, from: NodeId, heartbeat: &Heartbeat, received_at: u64) {
        let node_id = from.get();

        if let Some(state) = self.nodes.get_mut(&node_id) {
            state.last_heartbeat = received_at;
            state.status = NodeStatus::Alive;
            state.missed_count = 0;
        } else {
            self.nodes.insert(
                node_id,
                NodeState {
                    last_heartbeat: received_at,
                    status: NodeStatus::Alive,
                    missed_count: 0,
                },
            );
        }

        self.update_partition_map_from_heartbeat(from, heartbeat);
    }

    fn update_partition_map_from_heartbeat(&mut self, from: NodeId, heartbeat: &Heartbeat) {
        for partition in super::PartitionId::all() {
            let sender_claims_primary = heartbeat.is_primary(partition);
            let sender_claims_replica = heartbeat.is_replica(partition);

            if sender_claims_primary || sender_claims_replica {
                let our_role = self.partition_map.role_for(partition, from);
                let expected_primary =
                    sender_claims_primary && matches!(our_role, super::PartitionRole::Primary);
                let expected_replica =
                    sender_claims_replica && matches!(our_role, super::PartitionRole::Replica);

                if (sender_claims_primary && !expected_primary)
                    || (sender_claims_replica && !expected_replica)
                {
                    tracing::debug!(
                        ?partition,
                        ?from,
                        sender_claims_primary,
                        sender_claims_replica,
                        ?our_role,
                        "partition map discrepancy detected from heartbeat"
                    );
                }
            }
        }
    }

    pub fn check_timeouts(&mut self, now: u64) -> Vec<NodeId> {
        let mut dead_nodes = Vec::new();
        let timeout = self.config.heartbeat_timeout_ms;
        let suspect_threshold = timeout / 2;

        for (&node_id, state) in &mut self.nodes {
            if state.status == NodeStatus::Dead {
                continue;
            }

            let elapsed = now.saturating_sub(state.last_heartbeat);

            if elapsed > timeout {
                state.status = NodeStatus::Dead;
                if let Some(id) = NodeId::validated(node_id) {
                    dead_nodes.push(id);
                }
            } else if elapsed > suspect_threshold && state.status == NodeStatus::Alive {
                state.status = NodeStatus::Suspected;
                state.missed_count += 1;
            }
        }

        dead_nodes
    }

    pub fn handle_death_notice(&mut self, node_id: NodeId) {
        if let Some(state) = self.nodes.get_mut(&node_id.get()) {
            state.status = NodeStatus::Dead;
        }
    }

    #[must_use]
    pub fn node_status(&self, node_id: NodeId) -> NodeStatus {
        self.nodes
            .get(&node_id.get())
            .map_or(NodeStatus::Unknown, |s| s.status)
    }

    #[must_use]
    pub fn alive_nodes(&self) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|(_, state)| state.status == NodeStatus::Alive)
            .filter_map(|(&id, _)| NodeId::validated(id))
            .collect()
    }

    #[must_use]
    pub fn dead_nodes(&self) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|(_, state)| state.status == NodeStatus::Dead)
            .filter_map(|(&id, _)| NodeId::validated(id))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> TransportConfig {
        TransportConfig {
            heartbeat_interval_ms: 100,
            heartbeat_timeout_ms: 500,
            ack_timeout_ms: 50,
        }
    }

    #[test]
    fn register_and_track_nodes() {
        let local = NodeId::validated(1).unwrap();
        let mut mgr = HeartbeatManager::new(local, config());

        let node2 = NodeId::validated(2).unwrap();
        let node3 = NodeId::validated(3).unwrap();

        mgr.register_node(node2);
        mgr.register_node(node3);

        assert_eq!(mgr.node_status(node2), NodeStatus::Unknown);
        assert_eq!(mgr.node_status(node3), NodeStatus::Unknown);
    }

    #[test]
    fn heartbeat_marks_node_alive() {
        let local = NodeId::validated(1).unwrap();
        let mut mgr = HeartbeatManager::new(local, config());

        let node2 = NodeId::validated(2).unwrap();
        mgr.register_node(node2);

        let hb = Heartbeat::create(node2, 1000);
        mgr.receive_heartbeat(node2, &hb, 1000);

        assert_eq!(mgr.node_status(node2), NodeStatus::Alive);
        assert_eq!(mgr.alive_nodes(), vec![node2]);
    }

    #[test]
    fn timeout_marks_node_dead() {
        let local = NodeId::validated(1).unwrap();
        let mut mgr = HeartbeatManager::new(local, config());

        let node2 = NodeId::validated(2).unwrap();
        mgr.register_node(node2);

        let hb = Heartbeat::create(node2, 1000);
        mgr.receive_heartbeat(node2, &hb, 1000);

        let dead = mgr.check_timeouts(1300);
        assert!(dead.is_empty());
        assert_eq!(mgr.node_status(node2), NodeStatus::Suspected);

        let dead = mgr.check_timeouts(1600);
        assert_eq!(dead, vec![node2]);
        assert_eq!(mgr.node_status(node2), NodeStatus::Dead);
    }

    #[test]
    fn should_send_respects_interval() {
        let local = NodeId::validated(1).unwrap();
        let mut mgr = HeartbeatManager::new(local, config());

        assert!(mgr.should_send(0));

        mgr.create_heartbeat(0);
        assert!(!mgr.should_send(50));
        assert!(mgr.should_send(100));
    }

    #[test]
    fn death_notice_marks_dead() {
        let local = NodeId::validated(1).unwrap();
        let mut mgr = HeartbeatManager::new(local, config());

        let node2 = NodeId::validated(2).unwrap();
        mgr.register_node(node2);

        let hb = Heartbeat::create(node2, 1000);
        mgr.receive_heartbeat(node2, &hb, 1000);
        assert_eq!(mgr.node_status(node2), NodeStatus::Alive);

        mgr.handle_death_notice(node2);
        assert_eq!(mgr.node_status(node2), NodeStatus::Dead);
    }
}
