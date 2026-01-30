use bebytes::BeBytes;

use super::{StoreApplyError, StoreManager};
use crate::cluster::db::DbDataStore;
use crate::cluster::entity;

impl StoreManager {
    #[allow(clippy::type_complexity)]
    pub fn query_entity(
        &self,
        entity: &str,
        filter: Option<&str>,
        limit: u32,
        cursor: Option<&[u8]>,
    ) -> Result<(Vec<u8>, bool, Option<Vec<u8>>), StoreApplyError> {
        match entity {
            entity::SESSIONS => {
                let (sessions, has_more, next_cursor) = self.sessions.query(filter, limit, cursor);
                let data = sessions.into_iter().flat_map(|s| s.to_be_bytes()).collect();
                Ok((data, has_more, next_cursor))
            }
            entity::RETAINED => {
                let (messages, has_more, next_cursor) = self.retained.query(filter, limit, cursor);
                let data = messages.into_iter().flat_map(|m| m.to_be_bytes()).collect();
                Ok((data, has_more, next_cursor))
            }
            entity::SUBSCRIPTIONS => {
                let (subs, has_more, next_cursor) = self.subscriptions.query(filter, limit, cursor);
                let data = subs.into_iter().flat_map(|s| s.to_be_bytes()).collect();
                Ok((data, has_more, next_cursor))
            }
            entity::TOPIC_INDEX => {
                let (entries, has_more, next_cursor) = self.topics.query(filter, limit, cursor);
                let data = entries.into_iter().flat_map(|e| e.to_be_bytes()).collect();
                Ok((data, has_more, next_cursor))
            }
            entity::WILDCARDS => {
                let (entries, has_more, next_cursor) = self.wildcards.query(filter, limit, cursor);
                let data = entries.into_iter().flat_map(|e| e.to_be_bytes()).collect();
                Ok((data, has_more, next_cursor))
            }
            entity::INFLIGHT => {
                let (messages, has_more, next_cursor) = self.inflight.query(filter, limit, cursor);
                let data = messages.into_iter().flat_map(|m| m.to_be_bytes()).collect();
                Ok((data, has_more, next_cursor))
            }
            entity::OFFSETS => {
                let (offsets, has_more, next_cursor) = self.offsets.query(filter, limit, cursor);
                let data = offsets.into_iter().flat_map(|o| o.to_be_bytes()).collect();
                Ok((data, has_more, next_cursor))
            }
            entity::DB_DATA => {
                let entity_type = Self::extract_db_entity_type(filter);
                let (entities, has_more, next_cursor) =
                    self.db_data.query(entity_type, filter, limit, cursor);
                let data = entities
                    .into_iter()
                    .flat_map(|e| DbDataStore::serialize(&e))
                    .collect();
                Ok((data, has_more, next_cursor))
            }
            _ if entity.starts_with("$DB/") => {
                let entity_type = entity.strip_prefix("$DB/");
                let (entities, has_more, next_cursor) =
                    self.db_data.query(entity_type, filter, limit, cursor);
                let data = entities
                    .into_iter()
                    .flat_map(|e| DbDataStore::serialize(&e))
                    .collect();
                Ok((data, has_more, next_cursor))
            }
            _ => Err(StoreApplyError::UnknownEntity),
        }
    }

    fn extract_db_entity_type(filter: Option<&str>) -> Option<&str> {
        filter.and_then(|f| {
            f.strip_prefix("entity_type=")
                .map(|v| v.trim().trim_matches('"').trim_matches('\''))
        })
    }

    #[must_use]
    pub fn get_entity(&self, entity: &str, id: &str) -> Option<Vec<u8>> {
        match entity {
            entity::SESSIONS => self.sessions.get(id).map(|s| s.to_be_bytes()),
            entity::RETAINED => self.retained.get(id).map(|m| m.to_be_bytes()),
            entity::SUBSCRIPTIONS => self.subscriptions.get_snapshot(id).map(|s| s.to_be_bytes()),
            entity::TOPIC_INDEX => self.topics.get_entry(id).map(|e| e.to_be_bytes()),
            entity::DB_DATA => {
                let parts: Vec<&str> = id.splitn(2, '/').collect();
                if parts.len() == 2 {
                    self.db_data
                        .get(parts[0], parts[1])
                        .map(|e| DbDataStore::serialize(&e))
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}
