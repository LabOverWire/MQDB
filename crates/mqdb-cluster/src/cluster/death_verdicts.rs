// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::NodeId;
use super::raft_task::RaftEvent;

#[derive(Default)]
pub(super) struct DeathVerdicts {
    undelivered: Vec<NodeId>,
}

impl DeathVerdicts {
    pub(super) fn report(&mut self, tx_raft_events: &flume::Sender<RaftEvent>, node: NodeId) {
        if !self.undelivered.contains(&node) {
            self.undelivered.push(node);
        }
        self.send_undelivered(tx_raft_events);
    }

    pub(super) fn retry(
        &mut self,
        tx_raft_events: &flume::Sender<RaftEvent>,
        is_back: impl Fn(NodeId) -> bool,
    ) {
        self.undelivered.retain(|node| !is_back(*node));
        self.send_undelivered(tx_raft_events);
    }

    fn send_undelivered(&mut self, tx_raft_events: &flume::Sender<RaftEvent>) {
        let refused = self.undelivered.iter().position(|node| {
            matches!(
                tx_raft_events.try_send(RaftEvent::NodeDead(*node)),
                Err(flume::TrySendError::Full(_))
            )
        });
        let delivered = refused.unwrap_or(self.undelivered.len());
        self.undelivered.drain(..delivered);
        if !self.undelivered.is_empty() {
            tracing::warn!(
                held = ?self.undelivered,
                "raft event queue is full, holding these death verdicts for the next tick"
            );
        }
    }
}
