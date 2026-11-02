// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::NodeId;
use std::collections::HashMap;

const FIRST_DELAY_MS: u64 = 1_000;
const MAX_DELAY_MS: u64 = 5_000;
const LONG_ABSENCE_MS: u64 = 300_000;
const LONG_ABSENCE_DELAY_MS: u64 = 60_000;

struct NextDial {
    unlinked_since_ms: u64,
    at_ms: u64,
    delay_ms: u64,
}

#[derive(Default)]
pub struct RedialBackoff {
    waiting: HashMap<NodeId, NextDial>,
}

impl RedialBackoff {
    pub fn due(&mut self, unlinked: &[NodeId], now_ms: u64) -> Vec<NodeId> {
        self.waiting.retain(|node, _| unlinked.contains(node));
        let mut due = Vec::new();
        for node in unlinked {
            let Some(next) = self.waiting.get_mut(node) else {
                self.waiting.insert(
                    *node,
                    NextDial {
                        unlinked_since_ms: now_ms,
                        at_ms: now_ms + FIRST_DELAY_MS,
                        delay_ms: FIRST_DELAY_MS,
                    },
                );
                due.push(*node);
                continue;
            };
            if now_ms >= next.at_ms {
                next.delay_ms = if now_ms.saturating_sub(next.unlinked_since_ms) >= LONG_ABSENCE_MS
                {
                    LONG_ABSENCE_DELAY_MS
                } else {
                    (next.delay_ms * 2).min(MAX_DELAY_MS)
                };
                next.at_ms = now_ms + next.delay_ms;
                due.push(*node);
            }
        }
        due
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u16) -> NodeId {
        NodeId::validated(id).unwrap()
    }

    #[test]
    fn a_peer_is_dialled_at_once_then_backs_off_to_the_cap() {
        let mut backoff = RedialBackoff::default();
        let peer = [node(2)];
        let attempts: Vec<u64> = (0..=20_000)
            .step_by(1_000)
            .filter(|now| !backoff.due(&peer, *now).is_empty())
            .collect();
        assert_eq!(
            attempts,
            vec![0, 1_000, 3_000, 7_000, 12_000, 17_000],
            "dials start at once and settle at the five second cap"
        );
    }

    #[test]
    fn a_peer_absent_for_minutes_is_dialled_once_a_minute() {
        let mut backoff = RedialBackoff::default();
        let peer = [node(2)];
        let attempts: Vec<u64> = (0..=500_000)
            .step_by(1_000)
            .filter(|now| !backoff.due(&peer, *now).is_empty())
            .filter(|now| *now >= 290_000)
            .collect();
        let gaps: Vec<u64> = attempts.windows(2).map(|pair| pair[1] - pair[0]).collect();
        assert_eq!(
            gaps.first(),
            Some(&5_000),
            "still fast just before the five minutes"
        );
        assert_eq!(gaps.last(), Some(&60_000));
    }

    #[test]
    fn a_relinked_peer_starts_over_when_it_goes_down_again() {
        let mut backoff = RedialBackoff::default();
        let peer = [node(2)];
        assert_eq!(backoff.due(&peer, 0), peer);
        assert_eq!(backoff.due(&peer, 1_000), peer);
        assert!(backoff.due(&[], 2_000).is_empty());
        assert_eq!(backoff.due(&peer, 2_500), peer);
    }

    #[test]
    fn peers_back_off_independently() {
        let mut backoff = RedialBackoff::default();
        assert_eq!(backoff.due(&[node(2)], 0), [node(2)]);
        assert_eq!(
            backoff.due(&[node(2), node(3)], 500),
            [node(3)],
            "the new peer is due now while the first still waits"
        );
    }
}
