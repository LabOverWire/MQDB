use super::{StoreApplyError, StoreManager};
use crate::cluster::{PartitionId, entity};

impl StoreManager {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn export_partition(&self, partition: PartitionId) -> Vec<u8> {
        let mut buf = Vec::new();

        let store_data = [
            (
                entity::SESSIONS,
                self.sessions.export_for_partition(partition),
            ),
            (entity::QOS2, self.qos2.export_for_partition(partition)),
            (
                entity::SUBSCRIPTIONS,
                self.subscriptions.export_for_partition(partition),
            ),
            (
                entity::RETAINED,
                self.retained.export_for_partition(partition),
            ),
            (
                entity::TOPIC_INDEX,
                self.topics.export_for_partition(partition),
            ),
            (
                entity::WILDCARDS,
                self.wildcards.export_for_partition(partition),
            ),
            (
                entity::INFLIGHT,
                self.inflight.export_for_partition(partition),
            ),
            (
                entity::OFFSETS,
                self.offsets.export_for_partition(partition),
            ),
            (
                entity::IDEMPOTENCY,
                self.idempotency.export_for_partition(partition),
            ),
        ];

        buf.extend_from_slice(&(store_data.len() as u8).to_be_bytes());

        for (entity_name, data) in store_data {
            let name_bytes = entity_name.as_bytes();
            buf.push(name_bytes.len() as u8);
            buf.extend_from_slice(name_bytes);
            buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
            buf.extend_from_slice(&data);
        }

        buf
    }

    /// # Errors
    /// Returns `StoreApplyError` if the snapshot data is malformed or a store import fails.
    pub fn import_partition(&self, data: &[u8]) -> Result<usize, StoreApplyError> {
        if data.is_empty() {
            return Ok(0);
        }

        let store_count = data[0] as usize;
        let mut offset = 1;
        let mut total_imported = 0;

        for _ in 0..store_count {
            if offset >= data.len() {
                break;
            }

            let name_len = data[offset] as usize;
            offset += 1;

            if offset + name_len > data.len() {
                break;
            }
            let entity_name = std::str::from_utf8(&data[offset..offset + name_len])
                .map_err(|_| StoreApplyError::UnknownEntity)?;
            offset += name_len;

            if offset + 4 > data.len() {
                break;
            }
            let data_len = u32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + data_len > data.len() {
                break;
            }
            let store_data = &data[offset..offset + data_len];
            offset += data_len;

            let imported = match entity_name {
                entity::SESSIONS => self
                    .sessions
                    .import_sessions(store_data)
                    .map_err(|_| StoreApplyError::SessionError)?,
                entity::QOS2 => self
                    .qos2
                    .import_states(store_data)
                    .map_err(|_| StoreApplyError::Qos2Error)?,
                entity::SUBSCRIPTIONS => self
                    .subscriptions
                    .import_subscriptions(store_data)
                    .map_err(|_| StoreApplyError::SubscriptionError)?,
                entity::RETAINED => self
                    .retained
                    .import_retained(store_data)
                    .map_err(|_| StoreApplyError::RetainedError)?,
                entity::TOPIC_INDEX => self
                    .topics
                    .import_entries(store_data)
                    .map_err(|_| StoreApplyError::TopicIndexError)?,
                entity::WILDCARDS => self
                    .wildcards
                    .import_wildcards(store_data)
                    .map_err(|_| StoreApplyError::WildcardError)?,
                entity::INFLIGHT => self
                    .inflight
                    .import_inflight(store_data)
                    .map_err(|_| StoreApplyError::InflightError)?,
                entity::OFFSETS => self
                    .offsets
                    .import_offsets(store_data)
                    .map_err(|_| StoreApplyError::OffsetError)?,
                entity::IDEMPOTENCY => self
                    .idempotency
                    .import_records(store_data)
                    .map_err(|_| StoreApplyError::IdempotencyError)?,
                _ => continue,
            };

            total_imported += imported;
        }

        Ok(total_imported)
    }

    pub fn clear_partition(&self, partition: PartitionId) -> usize {
        let mut total_cleared = 0;
        total_cleared += self.sessions.clear_partition(partition);
        total_cleared += self.qos2.clear_partition(partition);
        total_cleared += self.subscriptions.clear_partition(partition);
        total_cleared += self.retained.clear_partition(partition);
        total_cleared += self.topics.clear_partition(partition);
        total_cleared += self.wildcards.clear_partition(partition);
        total_cleared += self.inflight.clear_partition(partition);
        total_cleared += self.offsets.clear_partition(partition);
        total_cleared += self.idempotency.clear_partition(partition);
        total_cleared
    }
}
