// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::wire::{WireReader, push_length_prefixed};

const VERSION: u8 = 2;
const HEADER_LEN: usize = 21;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResyncedTopic {
    pub topic: String,
    pub qos: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResyncedPattern {
    pub pattern: String,
    pub qos: u8,
    pub subscription_type: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResyncedClient {
    pub client_id: String,
    pub topics: Vec<ResyncedTopic>,
    pub patterns: Vec<ResyncedPattern>,
}

impl ResyncedClient {
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        let topics: usize = self.topics.iter().map(|t| 3 + t.topic.len()).sum();
        let patterns: usize = self.patterns.iter().map(|p| 4 + p.pattern.len()).sum();
        10 + self.client_id.len() + topics + patterns
    }

    fn encode(&self, buf: &mut Vec<u8>) -> Option<()> {
        push_length_prefixed(buf, self.client_id.as_bytes(), false)?;
        buf.extend_from_slice(&u32::try_from(self.topics.len()).ok()?.to_be_bytes());
        for topic in &self.topics {
            push_length_prefixed(buf, topic.topic.as_bytes(), false)?;
            buf.push(topic.qos);
        }
        buf.extend_from_slice(&u32::try_from(self.patterns.len()).ok()?.to_be_bytes());
        for pattern in &self.patterns {
            push_length_prefixed(buf, pattern.pattern.as_bytes(), false)?;
            buf.push(pattern.qos);
            buf.push(pattern.subscription_type);
        }
        Some(())
    }

    fn decode(reader: &mut WireReader<'_>) -> Option<Self> {
        let client_len = reader.length(true)?;
        let client_id = reader.text(client_len)?;
        let topic_count = reader.u32()?;
        let topics = (0..topic_count)
            .map(|_| {
                let len = reader.length(true)?;
                let topic = reader.text(len)?;
                let qos = reader.u8()?;
                Some(ResyncedTopic { topic, qos })
            })
            .collect::<Option<Vec<_>>>()?;
        let pattern_count = reader.u32()?;
        let patterns = (0..pattern_count)
            .map(|_| {
                let len = reader.length(true)?;
                let pattern = reader.text(len)?;
                let qos = reader.u8()?;
                let subscription_type = reader.u8()?;
                Some(ResyncedPattern {
                    pattern,
                    qos,
                    subscription_type,
                })
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Self {
            client_id,
            topics,
            patterns,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResyncRequest {
    pub round: u64,
    pub ask_back: bool,
}

impl ResyncRequest {
    #[must_use]
    pub fn to_bytes(self) -> Vec<u8> {
        let mut buf = vec![VERSION];
        buf.extend_from_slice(&self.round.to_be_bytes());
        buf.push(u8::from(self.ask_back));
        buf
    }

    #[must_use]
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        let mut reader = WireReader::new(data);
        if reader.u8()? != VERSION {
            return None;
        }
        let round = reader.u64()?;
        let ask_back = match reader.u8()? {
            0 => false,
            1 => true,
            _ => return None,
        };
        reader.is_exhausted().then_some(Self { round, ask_back })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionResync {
    pub round: u64,
    pub part: u32,
    pub parts: u32,
    pub clients: Vec<ResyncedClient>,
}

impl SubscriptionResync {
    pub const UNSOLICITED: u64 = 0;

    #[must_use]
    pub fn encoded_len(clients: &[ResyncedClient]) -> usize {
        HEADER_LEN
            + clients
                .iter()
                .map(ResyncedClient::encoded_len)
                .sum::<usize>()
    }

    #[must_use]
    pub fn to_bytes(&self) -> Option<Vec<u8>> {
        let mut buf = vec![VERSION];
        buf.extend_from_slice(&self.round.to_be_bytes());
        buf.extend_from_slice(&self.part.to_be_bytes());
        buf.extend_from_slice(&self.parts.to_be_bytes());
        buf.extend_from_slice(&u32::try_from(self.clients.len()).ok()?.to_be_bytes());
        for client in &self.clients {
            client.encode(&mut buf)?;
        }
        Some(buf)
    }

    #[must_use]
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        let mut reader = WireReader::new(data);
        if reader.u8()? != VERSION {
            return None;
        }
        let round = reader.u64()?;
        let part = reader.u32()?;
        let parts = reader.u32()?;
        if part >= parts {
            return None;
        }
        let count = reader.u32()?;
        let clients = (0..count)
            .map(|_| ResyncedClient::decode(&mut reader))
            .collect::<Option<Vec<_>>>()?;
        reader.is_exhausted().then_some(Self {
            round,
            part,
            parts,
            clients,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client(id: &str) -> ResyncedClient {
        ResyncedClient {
            client_id: id.to_string(),
            topics: vec![ResyncedTopic {
                topic: "a/b".to_string(),
                qos: 1,
            }],
            patterns: vec![ResyncedPattern {
                pattern: "a/+".to_string(),
                qos: 2,
                subscription_type: 0,
            }],
        }
    }

    fn resync(clients: Vec<ResyncedClient>) -> SubscriptionResync {
        SubscriptionResync {
            round: 7,
            part: 1,
            parts: 3,
            clients,
        }
    }

    #[test]
    fn a_resync_survives_the_wire() {
        let resync = resync(vec![client("c1"), client("c2")]);
        let bytes = resync.to_bytes().unwrap();
        assert_eq!(SubscriptionResync::from_bytes(&bytes), Some(resync.clone()));
        assert_eq!(
            bytes.len(),
            SubscriptionResync::encoded_len(&resync.clients)
        );
    }

    #[test]
    fn a_request_survives_the_wire() {
        let request = ResyncRequest {
            round: 9,
            ask_back: true,
        };
        assert_eq!(
            ResyncRequest::from_bytes(&request.to_bytes()),
            Some(request)
        );
    }

    #[test]
    fn a_truncated_resync_is_refused() {
        let bytes = resync(vec![client("c1")]).to_bytes().unwrap();
        assert_eq!(
            SubscriptionResync::from_bytes(&bytes[..bytes.len() - 1]),
            None
        );
    }

    #[test]
    fn a_part_past_the_last_is_refused() {
        let mut beyond = resync(Vec::new());
        beyond.part = 3;
        assert_eq!(
            SubscriptionResync::from_bytes(&beyond.to_bytes().unwrap()),
            None
        );
    }

    #[test]
    fn a_topic_too_long_for_its_length_field_is_not_encoded() {
        let mut long = client("c1");
        long.topics[0].topic = "x".repeat(usize::from(u16::MAX) + 1);
        assert_eq!(resync(vec![long]).to_bytes(), None);
    }

    #[test]
    fn a_huge_client_count_is_refused_without_allocating() {
        let mut bytes = resync(Vec::new()).to_bytes().unwrap();
        let count_at = bytes.len() - 4;
        bytes[count_at..].copy_from_slice(&u32::MAX.to_be_bytes());
        assert_eq!(SubscriptionResync::from_bytes(&bytes), None);
    }

    #[test]
    fn trailing_bytes_and_other_versions_are_refused() {
        let good = resync(Vec::new()).to_bytes().unwrap();
        let mut trailing = good.clone();
        trailing.push(0);
        assert_eq!(SubscriptionResync::from_bytes(&trailing), None);
        let mut older = good;
        older[0] = 1;
        assert_eq!(SubscriptionResync::from_bytes(&older), None);
        assert_eq!(SubscriptionResync::from_bytes(&[]), None);
    }
}
