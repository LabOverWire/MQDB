use super::super::PartitionId;
use super::super::db::data_partition;
use super::DbRequestHandler;
use serde_json::{Value, json};

impl DbRequestHandler {
    pub(super) fn json_error(code: u16, message: &str) -> Vec<u8> {
        let result = json!({
            "status": "error",
            "code": code,
            "message": message
        });
        serde_json::to_vec(&result).unwrap_or_default()
    }

    pub(super) fn json_success(entity: &str, id: &str, data: &Value) -> Vec<u8> {
        let result = json!({ "status": "ok", "id": id, "entity": entity, "data": data });
        serde_json::to_vec(&result).unwrap_or_default()
    }

    pub(super) fn current_time_ms() -> u64 {
        u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis()),
        )
        .unwrap_or(u64::MAX)
    }

    pub(super) fn generate_id_for_partition(
        &self,
        entity: &str,
        partition: PartitionId,
        data: &[u8],
    ) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        entity.hash(&mut hasher);
        data.hash(&mut hasher);
        self.node_id.get().hash(&mut hasher);
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
            .hash(&mut hasher);

        let base_id = hasher.finish();

        for suffix in 0..1000_u16 {
            let id = format!("{base_id:016x}-{suffix:04x}");
            if data_partition(entity, &id) == partition {
                return id;
            }
        }

        format!("{base_id:016x}-p{}", partition.get())
    }
}
