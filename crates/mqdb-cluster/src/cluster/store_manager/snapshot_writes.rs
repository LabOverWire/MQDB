// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::StoreApplyError;
use crate::cluster::db::{
    ConstraintStore, DbDataStore, FkValidationStore, IndexStore, UniqueStore, constraint_key,
    index_key, unique_key,
};
use crate::cluster::idempotency_store::IdempotencyStore;
use crate::cluster::inflight_store::InflightStore;
use crate::cluster::offset_store::OffsetStore;
use crate::cluster::protocol::{Operation, ReplicationWrite};
use crate::cluster::qos2_store::Qos2Store;
use crate::cluster::retained_store::RetainedStore;
use crate::cluster::session::{SessionStore, session_partition};
use crate::cluster::subscription_cache::SubscriptionCache;
use crate::cluster::topic_index::topic_partition;
use crate::cluster::{Epoch, PartitionId, entity};

pub(super) struct SnapshotSections<'a> {
    data: &'a [u8],
    offset: usize,
    remaining: usize,
}

impl<'a> SnapshotSections<'a> {
    pub(super) fn new(data: &'a [u8]) -> Self {
        let remaining = data.first().map_or(0, |count| usize::from(*count));
        Self {
            data,
            offset: 1,
            remaining,
        }
    }
}

impl<'a> Iterator for SnapshotSections<'a> {
    type Item = Result<(&'a str, &'a [u8]), StoreApplyError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        let name_len = usize::from(*take(self.data, &mut self.offset, 1)?.first()?);
        let name = take(self.data, &mut self.offset, name_len)?;
        let body = take_u32_prefixed(self.data, &mut self.offset)?;
        Some(
            std::str::from_utf8(name)
                .map(|name| (name, body))
                .map_err(|_| StoreApplyError::UnknownEntity),
        )
    }
}

struct SectionEntries<'a> {
    data: &'a [u8],
    offset: usize,
    remaining: usize,
    keyed: bool,
}

impl<'a> SectionEntries<'a> {
    fn new(data: &'a [u8], keyed: bool) -> Self {
        let remaining = data
            .get(..4)
            .and_then(|count| <[u8; 4]>::try_from(count).ok())
            .map_or(0, |count| u32::from_be_bytes(count) as usize);
        Self {
            data,
            offset: 4,
            remaining,
            keyed,
        }
    }
}

impl<'a> Iterator for SectionEntries<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        let key: &[u8] = if self.keyed {
            let key_len = usize::from(u16::from_be_bytes(
                take(self.data, &mut self.offset, 2)?.try_into().ok()?,
            ));
            take(self.data, &mut self.offset, key_len)?
        } else {
            &[]
        };
        let value = take_u32_prefixed(self.data, &mut self.offset)?;
        Some((key, value))
    }
}

fn take<'a>(data: &'a [u8], offset: &mut usize, len: usize) -> Option<&'a [u8]> {
    let end = offset.checked_add(len)?;
    let slice = data.get(*offset..end)?;
    *offset = end;
    Some(slice)
}

fn take_u32_prefixed<'a>(data: &'a [u8], offset: &mut usize) -> Option<&'a [u8]> {
    let len = u32::from_be_bytes(take(data, offset, 4)?.try_into().ok()?) as usize;
    take(data, offset, len)
}

fn insert(partition: PartitionId, entity_name: &str, id: &str, data: Vec<u8>) -> ReplicationWrite {
    ReplicationWrite::new(
        partition,
        Operation::Insert,
        Epoch::ZERO,
        0,
        entity_name.to_string(),
        id.to_string(),
        data,
    )
}

fn keyed_writes(
    section: &[u8],
    error: StoreApplyError,
    to_write: impl Fn(&str, &[u8]) -> Result<Option<ReplicationWrite>, StoreApplyError>,
) -> Result<Vec<ReplicationWrite>, StoreApplyError> {
    let mut writes = Vec::new();
    for (key, value) in SectionEntries::new(section, true) {
        let key = std::str::from_utf8(key).map_err(|_| error)?;
        if let Some(write) = to_write(key, value)? {
            writes.push(write);
        }
    }
    Ok(writes)
}

fn client_and_packet(key: &str) -> Option<&str> {
    let (client_id, packet_id) = key.rsplit_once(':')?;
    packet_id.parse::<u16>().ok().map(|_| client_id)
}

pub(super) fn snapshot_writes(
    entity_name: &str,
    section: &[u8],
) -> Result<Vec<ReplicationWrite>, StoreApplyError> {
    match entity_name {
        entity::SESSIONS => keyed_writes(section, StoreApplyError::SessionError, |key, value| {
            Ok(SessionStore::deserialize(value).map(|session| {
                insert(
                    session_partition(key),
                    entity_name,
                    key,
                    SessionStore::serialize(&session),
                )
            }))
        }),
        entity::QOS2 => keyed_writes(section, StoreApplyError::Qos2Error, |key, value| {
            Ok(client_and_packet(key).and_then(|client_id| {
                Qos2Store::deserialize(value).map(|state| {
                    insert(
                        session_partition(client_id),
                        entity_name,
                        key,
                        Qos2Store::serialize(&state),
                    )
                })
            }))
        }),
        entity::SUBSCRIPTIONS => {
            keyed_writes(section, StoreApplyError::SubscriptionError, |key, value| {
                Ok(SubscriptionCache::deserialize(value).map(|snapshot| {
                    insert(
                        session_partition(key),
                        entity_name,
                        key,
                        SubscriptionCache::serialize(&snapshot),
                    )
                }))
            })
        }
        entity::RETAINED => keyed_writes(section, StoreApplyError::RetainedError, |key, value| {
            Ok(RetainedStore::deserialize(value).map(|message| {
                insert(
                    topic_partition(key),
                    entity_name,
                    key,
                    RetainedStore::serialize(&message),
                )
            }))
        }),
        entity::INFLIGHT => keyed_writes(section, StoreApplyError::InflightError, |key, value| {
            Ok(client_and_packet(key).and_then(|client_id| {
                InflightStore::deserialize(value).map(|message| {
                    insert(
                        session_partition(client_id),
                        entity_name,
                        key,
                        InflightStore::serialize(&message),
                    )
                })
            }))
        }),
        entity::OFFSETS => keyed_writes(section, StoreApplyError::OffsetError, |key, value| {
            let Some((consumer_id, partition_num)) = key.split_once(':') else {
                return Ok(None);
            };
            if partition_num.parse::<u16>().is_err() {
                return Ok(None);
            }
            Ok(OffsetStore::deserialize(value).map(|offset| {
                insert(
                    session_partition(consumer_id),
                    entity_name,
                    key,
                    OffsetStore::serialize(&offset),
                )
            }))
        }),
        entity::IDEMPOTENCY => {
            keyed_writes(section, StoreApplyError::IdempotencyError, |key, value| {
                let partition = key
                    .split_once(':')
                    .and_then(|(partition_num, _)| partition_num.parse::<u16>().ok())
                    .and_then(PartitionId::new);
                Ok(partition.and_then(|partition| {
                    IdempotencyStore::deserialize(value).map(|record| {
                        insert(
                            partition,
                            entity_name,
                            key,
                            IdempotencyStore::serialize(&record),
                        )
                    })
                }))
            })
        }
        entity::DB_DATA => keyed_writes(section, StoreApplyError::DbDataError, |_, value| {
            let record = DbDataStore::deserialize(value).ok_or(StoreApplyError::DbDataError)?;
            Ok(Some(insert(
                record.partition(),
                entity_name,
                &record.key(),
                DbDataStore::serialize(&record),
            )))
        }),
        entity::DB_SCHEMA => keyed_writes(section, StoreApplyError::DbSchemaError, |key, value| {
            let schema = crate::cluster::db::SchemaStore::deserialize(value)
                .ok_or(StoreApplyError::DbSchemaError)?;
            Ok(Some(insert(
                PartitionId::ZERO,
                entity_name,
                key,
                crate::cluster::db::SchemaStore::serialize(&schema),
            )))
        }),
        entity::DB_INDEX => SectionEntries::new(section, false)
            .map(|(_, value)| {
                let entry = IndexStore::deserialize(value).ok_or(StoreApplyError::DbIndexError)?;
                Ok(insert(
                    entry.index_partition(),
                    entity_name,
                    &index_key(&entry),
                    IndexStore::serialize(&entry),
                ))
            })
            .collect(),
        entity::DB_UNIQUE => keyed_writes(section, StoreApplyError::DbUniqueError, |_, value| {
            let reservation =
                UniqueStore::deserialize(value).ok_or(StoreApplyError::DbUniqueError)?;
            Ok(Some(insert(
                reservation.unique_partition(),
                entity_name,
                &unique_key(&reservation),
                UniqueStore::serialize(&reservation),
            )))
        }),
        entity::DB_FK => keyed_writes(section, StoreApplyError::DbFkError, |_, value| {
            let request =
                FkValidationStore::deserialize_request(value).ok_or(StoreApplyError::DbFkError)?;
            Ok(Some(insert(
                request.target_partition(),
                entity_name,
                request.request_id_str(),
                FkValidationStore::serialize_request(&request),
            )))
        }),
        entity::DB_CONSTRAINT => {
            keyed_writes(section, StoreApplyError::DbConstraintError, |_, value| {
                let constraint = ConstraintStore::deserialize(value)
                    .ok_or(StoreApplyError::DbConstraintError)?;
                Ok(Some(insert(
                    PartitionId::ZERO,
                    entity_name,
                    &constraint_key(constraint.entity_str(), constraint.name_str()),
                    ConstraintStore::serialize(&constraint),
                )))
            })
        }
        _ => Ok(Vec::new()),
    }
}
