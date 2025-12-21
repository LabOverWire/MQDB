use super::clock::VirtualClock;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Message {
    pub from: u16,
    pub to: u16,
    pub payload: Vec<u8>,
    pub deliver_at: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum LinkState {
    Up,
    Down,
    Lossy { loss_rate_percent: u8 },
}

struct NetworkState {
    inboxes: HashMap<u16, VecDeque<Message>>,
    links: HashMap<(u16, u16), LinkState>,
    base_latency_ms: u64,
    rng_seed: u64,
}

#[derive(Clone)]
pub struct VirtualNetwork {
    clock: VirtualClock,
    state: Arc<Mutex<NetworkState>>,
}

impl std::fmt::Debug for VirtualNetwork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VirtualNetwork")
            .field("clock", &self.clock)
            .finish_non_exhaustive()
    }
}

impl VirtualNetwork {
    #[must_use]
    pub fn new(clock: VirtualClock) -> Self {
        Self {
            clock,
            state: Arc::new(Mutex::new(NetworkState {
                inboxes: HashMap::new(),
                links: HashMap::new(),
                base_latency_ms: 1,
                rng_seed: 0,
            })),
        }
    }

    pub fn register_node(&self, node_id: u16) {
        let mut state = self.state.lock().unwrap();
        state.inboxes.entry(node_id).or_default();
    }

    pub fn set_link(&self, from: u16, to: u16, link_state: LinkState) {
        let mut state = self.state.lock().unwrap();
        state.links.insert((from, to), link_state);
    }

    #[allow(dead_code)]
    pub fn set_base_latency_ms(&self, ms: u64) {
        let mut state = self.state.lock().unwrap();
        state.base_latency_ms = ms;
    }

    pub fn send(&self, from: u16, to: u16, payload: Vec<u8>) -> bool {
        let mut state = self.state.lock().unwrap();

        let link_state = state
            .links
            .get(&(from, to))
            .copied()
            .unwrap_or(LinkState::Up);

        match link_state {
            LinkState::Down => return false,
            LinkState::Lossy { loss_rate_percent } => {
                state.rng_seed = state
                    .rng_seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1);
                let rand = (state.rng_seed >> 33) as u8;
                if rand % 100 < loss_rate_percent {
                    return false;
                }
            }
            LinkState::Up => {}
        }

        let now = self.clock.now();
        let deliver_at = now + state.base_latency_ms * 1_000_000;

        let inbox = state.inboxes.entry(to).or_default();
        inbox.push_back(Message {
            from,
            to,
            payload,
            deliver_at,
        });

        true
    }

    pub fn receive(&self, node_id: u16) -> Option<Message> {
        let now = self.clock.now();
        let mut state = self.state.lock().unwrap();

        let inbox = state.inboxes.get_mut(&node_id)?;

        let idx = inbox.iter().position(|m| m.deliver_at <= now)?;
        inbox.remove(idx)
    }

    #[allow(dead_code)]
    pub fn receive_all(&self, node_id: u16) -> Vec<Message> {
        let now = self.clock.now();
        let mut state = self.state.lock().unwrap();

        let inbox = match state.inboxes.get_mut(&node_id) {
            Some(i) => i,
            None => return Vec::new(),
        };

        let mut delivered = Vec::new();
        let mut remaining = VecDeque::new();

        for msg in inbox.drain(..) {
            if msg.deliver_at <= now {
                delivered.push(msg);
            } else {
                remaining.push_back(msg);
            }
        }

        *inbox = remaining;
        delivered
    }

    #[allow(dead_code)]
    pub fn pending_count(&self, node_id: u16) -> usize {
        let state = self.state.lock().unwrap();
        state.inboxes.get(&node_id).map_or(0, VecDeque::len)
    }

    pub fn partition(&self, isolated_node: u16, other_nodes: &[u16]) {
        for &other in other_nodes {
            self.set_link(isolated_node, other, LinkState::Down);
            self.set_link(other, isolated_node, LinkState::Down);
        }
    }

    pub fn heal(&self, node_a: u16, node_b: u16) {
        self.set_link(node_a, node_b, LinkState::Up);
        self.set_link(node_b, node_a, LinkState::Up);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn send_and_receive() {
        let clock = VirtualClock::new();
        let net = VirtualNetwork::new(clock.clone());

        net.register_node(1);
        net.register_node(2);

        net.send(1, 2, b"hello".to_vec());

        assert!(net.receive(2).is_none());

        clock.advance(Duration::from_millis(1));

        let msg = net.receive(2).unwrap();
        assert_eq!(msg.from, 1);
        assert_eq!(msg.payload, b"hello");
    }

    #[test]
    fn link_down_drops_messages() {
        let clock = VirtualClock::new();
        let net = VirtualNetwork::new(clock.clone());

        net.register_node(1);
        net.register_node(2);
        net.set_link(1, 2, LinkState::Down);

        assert!(!net.send(1, 2, b"hello".to_vec()));
        clock.advance(Duration::from_millis(10));
        assert!(net.receive(2).is_none());
    }

    #[test]
    fn partition_isolates_node() {
        let clock = VirtualClock::new();
        let net = VirtualNetwork::new(clock.clone());

        net.register_node(1);
        net.register_node(2);
        net.register_node(3);

        net.partition(1, &[2, 3]);

        assert!(!net.send(1, 2, b"hello".to_vec()));
        assert!(!net.send(2, 1, b"world".to_vec()));
        assert!(net.send(2, 3, b"ok".to_vec()));
    }

    #[test]
    fn heal_restores_links() {
        let clock = VirtualClock::new();
        let net = VirtualNetwork::new(clock.clone());

        net.register_node(1);
        net.register_node(2);

        net.set_link(1, 2, LinkState::Down);
        assert!(!net.send(1, 2, b"hello".to_vec()));

        net.heal(1, 2);
        assert!(net.send(1, 2, b"hello".to_vec()));
    }
}
