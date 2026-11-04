// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::NodeId;
use std::collections::HashMap;
use std::time::{Duration, Instant};

const FIRST_DELAY: Duration = Duration::from_secs(1);
const MAX_DELAY: Duration = Duration::from_secs(5);
const LONG_ABSENCE: Duration = Duration::from_secs(300);
const LONG_ABSENCE_DELAY: Duration = Duration::from_secs(60);

struct NextDial {
    unlinked_since: Instant,
    at: Instant,
    delay: Duration,
}

#[derive(Default)]
pub struct RedialBackoff {
    waiting: HashMap<NodeId, NextDial>,
}

impl RedialBackoff {
    pub fn due(&mut self, unlinked: &[NodeId], now: Instant) -> Vec<NodeId> {
        self.waiting.retain(|node, _| unlinked.contains(node));
        let mut due = Vec::new();
        for node in unlinked {
            let Some(next) = self.waiting.get_mut(node) else {
                self.waiting.insert(
                    *node,
                    NextDial {
                        unlinked_since: now,
                        at: now + FIRST_DELAY,
                        delay: FIRST_DELAY,
                    },
                );
                due.push(*node);
                continue;
            };
            if now >= next.at {
                next.delay = if now.duration_since(next.unlinked_since) >= LONG_ABSENCE {
                    LONG_ABSENCE_DELAY
                } else {
                    (next.delay * 2).min(MAX_DELAY)
                };
                next.at = now + next.delay;
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

    fn after(start: Instant, elapsed_ms: u64) -> Instant {
        start + Duration::from_millis(elapsed_ms)
    }

    #[test]
    fn a_peer_is_dialled_at_once_then_backs_off_to_the_cap() {
        let mut backoff = RedialBackoff::default();
        let peer = [node(2)];
        let start = Instant::now();
        let attempts: Vec<u64> = (0..=20_000)
            .step_by(1_000)
            .filter(|elapsed| !backoff.due(&peer, after(start, *elapsed)).is_empty())
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
        let start = Instant::now();
        let attempts: Vec<u64> = (0..=500_000)
            .step_by(1_000)
            .filter(|elapsed| !backoff.due(&peer, after(start, *elapsed)).is_empty())
            .filter(|elapsed| *elapsed >= 290_000)
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
        let start = Instant::now();
        assert_eq!(backoff.due(&peer, after(start, 0)), peer);
        assert_eq!(backoff.due(&peer, after(start, 1_000)), peer);
        assert!(backoff.due(&[], after(start, 2_000)).is_empty());
        assert_eq!(backoff.due(&peer, after(start, 2_500)), peer);
    }

    #[test]
    fn peers_back_off_independently() {
        let mut backoff = RedialBackoff::default();
        let start = Instant::now();
        assert_eq!(backoff.due(&[node(2)], after(start, 0)), [node(2)]);
        assert_eq!(
            backoff.due(&[node(2), node(3)], after(start, 500)),
            [node(3)],
            "the new peer is due now while the first still waits"
        );
    }
}
