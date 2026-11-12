// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::wire::{WireReader, push_length_prefixed};

const VERSION: u8 = 3;
const HEADER_LEN: usize = 22;
const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0100_0000_01b3;

fn fnv(fields: &[&[u8]]) -> u64 {
    fields.iter().fold(FNV_OFFSET, |hash, field| {
        field
            .iter()
            .chain(std::iter::once(&0xff))
            .fold(hash, |hash, byte| {
                (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
            })
    })
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ListingDigest {
    pub clients: u64,
    pub hash: u64,
}

impl ListingDigest {
    #[must_use]
    pub fn topic_term(client_id: &str, topic: &str, qos: u8) -> u64 {
        fnv(&[b"t", client_id.as_bytes(), topic.as_bytes(), &[qos]])
    }

    #[must_use]
    pub fn pattern_term(client_id: &str, pattern: &str, qos: u8, subscription_type: u8) -> u64 {
        fnv(&[
            b"p",
            client_id.as_bytes(),
            pattern.as_bytes(),
            &[qos, subscription_type],
        ])
    }

    pub fn add_client(&mut self, client_id: &str, order: u64, instance: u64, subscriptions: u64) {
        self.clients += 1;
        self.hash = self
            .hash
            .wrapping_add(fnv(&[
                b"c",
                client_id.as_bytes(),
                &order.to_be_bytes(),
                &instance.to_be_bytes(),
            ]))
            .wrapping_add(subscriptions);
    }

    #[cfg(test)]
    #[must_use]
    pub fn of(clients: &[ResyncedClient]) -> Self {
        clients.iter().fold(Self::default(), |mut digest, client| {
            let topics = client.topics.iter().fold(0_u64, |sum, topic| {
                sum.wrapping_add(Self::topic_term(&client.client_id, &topic.topic, topic.qos))
            });
            let subscriptions = client.patterns.iter().fold(topics, |sum, pattern| {
                sum.wrapping_add(Self::pattern_term(
                    &client.client_id,
                    &pattern.pattern,
                    pattern.qos,
                    pattern.subscription_type,
                ))
            });
            digest.add_client(
                &client.client_id,
                client.order,
                client.instance,
                subscriptions,
            );
            digest
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResyncedTopic {
    pub topic: String,
    pub qos: u8,
}

impl ResyncedTopic {
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        3 + self.topic.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResyncedPattern {
    pub pattern: String,
    pub qos: u8,
    pub subscription_type: u8,
}

impl ResyncedPattern {
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        4 + self.pattern.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResyncedClient {
    pub client_id: String,
    pub order: u64,
    pub instance: u64,
    pub topics: Vec<ResyncedTopic>,
    pub patterns: Vec<ResyncedPattern>,
}

impl ResyncedClient {
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        let topics: usize = self.topics.iter().map(ResyncedTopic::encoded_len).sum();
        let patterns: usize = self.patterns.iter().map(ResyncedPattern::encoded_len).sum();
        26 + self.client_id.len() + topics + patterns
    }

    fn encode(&self, buf: &mut Vec<u8>) -> Option<()> {
        push_length_prefixed(buf, self.client_id.as_bytes(), false)?;
        buf.extend_from_slice(&self.order.to_be_bytes());
        buf.extend_from_slice(&self.instance.to_be_bytes());
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
        let order = reader.u64()?;
        let instance = reader.u64()?;
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
            order,
            instance,
            topics,
            patterns,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResyncRequest {
    pub round: u64,
    pub ask_back: bool,
    pub digest: ListingDigest,
}

impl ResyncRequest {
    #[must_use]
    pub fn to_bytes(self) -> Vec<u8> {
        let mut buf = vec![VERSION];
        buf.extend_from_slice(&self.round.to_be_bytes());
        buf.push(u8::from(self.ask_back));
        buf.extend_from_slice(&self.digest.clients.to_be_bytes());
        buf.extend_from_slice(&self.digest.hash.to_be_bytes());
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
        let digest = ListingDigest {
            clients: reader.u64()?,
            hash: reader.u64()?,
        };
        reader.is_exhausted().then_some(Self {
            round,
            ask_back,
            digest,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionResync {
    pub round: u64,
    pub part: u32,
    pub parts: u32,
    pub unchanged: bool,
    pub continued: bool,
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
        buf.push(u8::from(self.unchanged) | (u8::from(self.continued) << 1));
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
        let flags = reader.u8()?;
        if flags > 0b11 {
            return None;
        }
        let unchanged = flags & 0b01 != 0;
        let continued = flags & 0b10 != 0;
        let count = reader.u32()?;
        let clients = (0..count)
            .map(|_| ResyncedClient::decode(&mut reader))
            .collect::<Option<Vec<_>>>()?;
        reader.is_exhausted().then_some(Self {
            round,
            part,
            parts,
            unchanged,
            continued,
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
            order: 0,
            instance: 0,
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
            unchanged: false,
            continued: false,
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
            digest: ListingDigest::of(&[client("c1")]),
        };
        assert_eq!(
            ResyncRequest::from_bytes(&request.to_bytes()),
            Some(request)
        );
    }

    #[test]
    fn an_unchanged_reply_survives_the_wire() {
        let mut unchanged = resync(Vec::new());
        unchanged.unchanged = true;
        let bytes = unchanged.to_bytes().unwrap();
        assert_eq!(SubscriptionResync::from_bytes(&bytes), Some(unchanged));
    }

    #[test]
    fn a_continued_part_survives_the_wire_and_unknown_flags_are_refused() {
        let mut continued = resync(vec![client("c1")]);
        continued.continued = true;
        let mut bytes = continued.to_bytes().unwrap();
        assert_eq!(SubscriptionResync::from_bytes(&bytes), Some(continued));
        bytes[17] = 0b100;
        assert_eq!(SubscriptionResync::from_bytes(&bytes), None);
    }

    #[test]
    fn a_digest_ignores_listing_order_and_sees_every_field() {
        let mut first = client("c1");
        let second = client("c2");
        let digest = ListingDigest::of(&[first.clone(), second.clone()]);
        assert_eq!(ListingDigest::of(&[second.clone(), first.clone()]), digest);
        first.topics[0].qos = 0;
        assert_ne!(ListingDigest::of(&[first.clone(), second.clone()]), digest);
        let mut moved = client("c1");
        moved.patterns[0].subscription_type = 1;
        assert_ne!(ListingDigest::of(&[moved, second.clone()]), digest);
        let mut renamed = client("c1");
        renamed.topics[0].topic = "a/c".to_string();
        assert_ne!(ListingDigest::of(&[renamed, second]), digest);
        assert_ne!(ListingDigest::of(&[client("c1")]), digest);
        let mut reconnected = client("c1");
        reconnected.order = 9;
        assert_ne!(
            ListingDigest::of(&[reconnected, client("c2")]),
            ListingDigest::of(&[client("c1"), client("c2")])
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
