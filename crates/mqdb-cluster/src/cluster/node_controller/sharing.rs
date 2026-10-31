// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{ClusterTransport, NodeController};
use crate::cluster::db::{DbDataStoreError, data_partition, generate_id_for_partition};
use crate::cluster::db_handler::helpers::json_write_failure;
use mqdb_core::types::{AccessLevel, SHARES_ENTITY};
use serde_json::{Value, json};
use std::collections::{HashSet, VecDeque};

#[cfg(feature = "http-api")]
use super::db_ops::{EventOrigin, ForwardedJsonRequest};
#[cfg(feature = "http-api")]
use super::protocol::{JsonDbOp, JsonDbRequest};
#[cfg(feature = "http-api")]
use super::{ClusterMessage, NodeId, PartitionId, PendingScatterRequest, ShareResolveContinuation};

const MAX_CASCADE_RESOURCES: usize = 256;

#[derive(Clone, Copy)]
struct GrantWrite<'a> {
    grantee_key: &'a str,
    grantee: Option<&'a str>,
    grantee_email: Option<&'a str>,
    level: AccessLevel,
    granted_by: &'a str,
}

impl<T: ClusterTransport> NodeController<T> {
    fn share_owner_field(&self, entity: &str) -> Option<String> {
        self.ownership.owner_field(entity).map(str::to_string)
    }

    fn is_resource_owner(&self, entity: &str, id: &str, owner_field: &str, sender: &str) -> bool {
        let Some(record) = self.db_get(entity, id) else {
            return false;
        };
        let Ok(data) = serde_json::from_slice::<Value>(&record.data) else {
            return false;
        };
        data.get(owner_field).and_then(Value::as_str) == Some(sender)
    }

    fn require_owner_or_admin(
        &self,
        entity: &str,
        id: &str,
        sender: Option<&str>,
    ) -> Option<Vec<u8>> {
        let Some(owner_field) = self.share_owner_field(entity) else {
            return Some(Self::json_error(
                400,
                &format!("entity '{entity}' is not shareable"),
            ));
        };
        let uid = sender?;
        if self.ownership.is_admin(uid) || self.is_resource_owner(entity, id, &owner_field, uid) {
            None
        } else {
            Some(Self::json_error(403, "permission denied"))
        }
    }

    fn shares_for_resource(&self, entity: &str, id: &str) -> Vec<(String, Value)> {
        self.db_list(SHARES_ENTITY)
            .into_iter()
            .filter_map(|e| {
                let sid = e.id_str().to_string();
                let value: Value = serde_json::from_slice(&e.data).ok()?;
                let matches = value.get("resource_entity").and_then(Value::as_str) == Some(entity)
                    && value.get("resource_id").and_then(Value::as_str) == Some(id);
                if matches { Some((sid, value)) } else { None }
            })
            .collect()
    }

    #[must_use]
    pub(crate) fn cluster_share_level(
        &self,
        entity: &str,
        id: &str,
        sender: &str,
    ) -> Option<AccessLevel> {
        self.shares_for_resource(entity, id)
            .into_iter()
            .filter_map(|(_, value)| {
                let matches = value.get("grantee").and_then(Value::as_str) == Some(sender)
                    || value.get("grantee_key").and_then(Value::as_str) == Some(sender);
                if matches {
                    value
                        .get("permission")
                        .and_then(Value::as_str)
                        .and_then(AccessLevel::parse)
                } else {
                    None
                }
            })
            .max()
    }

    pub(crate) fn check_local_share_access(
        &self,
        entity: &str,
        id: &str,
        owner_field: &str,
        sender: Option<&str>,
        required: AccessLevel,
    ) -> Option<Vec<u8>> {
        let Some(uid) = sender else {
            return Some(Self::json_error(403, "permission denied"));
        };
        if self.is_resource_owner(entity, id, owner_field, uid) {
            return None;
        }
        if self
            .cluster_share_level(entity, id, uid)
            .is_some_and(|granted| granted >= required)
        {
            return None;
        }
        Some(Self::json_error(403, "permission denied"))
    }

    fn existing_grant_level(
        &self,
        entity: &str,
        id: &str,
        grantee_key: &str,
    ) -> Option<AccessLevel> {
        self.shares_for_resource(entity, id)
            .into_iter()
            .filter_map(|(_, value)| {
                if value.get("grantee_key").and_then(Value::as_str) == Some(grantee_key) {
                    value
                        .get("permission")
                        .and_then(Value::as_str)
                        .and_then(AccessLevel::parse)
                } else {
                    None
                }
            })
            .max()
    }

    async fn clear_grant(
        &mut self,
        entity: &str,
        id: &str,
        grantee_key: &str,
    ) -> Result<(), DbDataStoreError> {
        let victims = self
            .shares_for_resource(entity, id)
            .into_iter()
            .filter_map(|(sid, value)| {
                (value.get("grantee_key").and_then(Value::as_str) == Some(grantee_key))
                    .then_some(sid)
            })
            .collect();
        self.delete_grants(victims).await
    }

    pub(crate) async fn clear_all_resource_grants(
        &mut self,
        entity: &str,
        id: &str,
    ) -> Result<(), DbDataStoreError> {
        let victims: Vec<String> = self
            .shares_for_resource(entity, id)
            .into_iter()
            .map(|(sid, _)| sid)
            .collect();
        self.delete_grants(victims).await
    }

    async fn delete_grants(&mut self, share_ids: Vec<String>) -> Result<(), DbDataStoreError> {
        let mut first_failure = None;
        for sid in share_ids {
            match self.db_delete(SHARES_ENTITY, &sid).await {
                Ok(_) | Err(DbDataStoreError::NotFound) => {}
                Err(e) => {
                    first_failure.get_or_insert(e);
                }
            }
        }
        first_failure.map_or(Ok(()), Err)
    }

    async fn write_grant(
        &mut self,
        entity: &str,
        id: &str,
        grant: GrantWrite<'_>,
    ) -> Result<(), DbDataStoreError> {
        self.clear_grant(entity, id, grant.grantee_key).await?;
        if let Some(resolved) = grant.grantee
            && resolved != grant.grantee_key
        {
            self.clear_grant(entity, id, resolved).await?;
        }
        let partition = data_partition(entity, id);
        let share_id = generate_id_for_partition(SHARES_ENTITY, partition);
        let record = json!({
            "id": share_id,
            "resource_entity": entity,
            "resource_id": id,
            "grantee": grant.grantee,
            "grantee_key": grant.grantee_key,
            "grantee_email": grant.grantee_email,
            "permission": grant.level.as_str(),
            "granted_by": grant.granted_by,
        });
        let bytes =
            serde_json::to_vec(&record).map_err(|_| DbDataStoreError::SerializationError)?;
        let now = Self::current_time_ms();
        self.db_create(SHARES_ENTITY, &share_id, &bytes, now)
            .await?;
        Ok(())
    }

    fn self_ref_fields(&self, entity: &str) -> Vec<String> {
        self.stores
            .constraint_get_fk_constraints(entity)
            .into_iter()
            .filter(|c| c.is_foreign_key() && c.target_entity_str() == entity)
            .map(|c| c.field_str().to_string())
            .collect()
    }

    async fn cascade_share(
        &mut self,
        entity: &str,
        root_id: &str,
        grant: GrantWrite<'_>,
    ) -> Result<usize, usize> {
        let fields = self.self_ref_fields(entity);
        if fields.is_empty() {
            return Ok(0);
        }
        let mut visited: HashSet<String> = HashSet::from([root_id.to_string()]);
        let mut queue: VecDeque<String> = VecDeque::from([root_id.to_string()]);
        let mut granted = 0;
        let mut failed = 0;
        let mut skipped_remote = 0;
        while let Some(cur) = queue.pop_front() {
            if visited.len() > MAX_CASCADE_RESOURCES {
                break;
            }
            let children: Vec<String> = {
                let Some(rec) = self.db_get(entity, &cur) else {
                    continue;
                };
                let Ok(data) = serde_json::from_slice::<Value>(&rec.data) else {
                    continue;
                };
                fields
                    .iter()
                    .filter_map(|f| data.get(f).and_then(Value::as_str).map(str::to_string))
                    .collect()
            };
            for child in children {
                if !visited.insert(child.clone()) {
                    continue;
                }
                if self.holds_primary_role(data_partition(entity, &child)) {
                    let existing = self.existing_grant_level(entity, &child, grant.grantee_key);
                    if existing.is_none_or(|current| current < grant.level)
                        && let Err(e) = self.write_grant(entity, &child, grant).await
                    {
                        tracing::warn!(entity, child, error = %e, "cascade share grant not written");
                        failed += 1;
                        continue;
                    }
                    granted += 1;
                    queue.push_back(child);
                } else {
                    skipped_remote += 1;
                }
            }
        }
        if skipped_remote > 0 {
            tracing::warn!(
                entity,
                root_id,
                skipped_remote,
                "cascade share is incomplete: {skipped_remote} closure member(s) live on other \
                 partitions and were not granted (cross-partition cascade is tracked in #122)"
            );
        }
        if failed == 0 {
            Ok(granted)
        } else {
            Err(failed)
        }
    }

    pub(crate) async fn handle_share_local(
        &mut self,
        entity: &str,
        id: &str,
        payload: &[u8],
        sender: Option<&str>,
    ) -> Vec<u8> {
        if !self.holds_primary_role(data_partition(entity, id)) {
            return json_write_failure(DbDataStoreError::NotPrimary);
        }
        let payload: Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(_) => return Self::json_error(400, "invalid JSON payload"),
        };
        if let Some(err) = self.require_owner_or_admin(entity, id, sender) {
            return err;
        }
        if self.db_get(entity, id).is_none() {
            return Self::json_error(404, &format!("entity not found: {entity} id={id}"));
        }
        let grantee_key = payload
            .get("grantee_key")
            .and_then(Value::as_str)
            .or_else(|| payload.get("grantee").and_then(Value::as_str))
            .unwrap_or("")
            .to_string();
        if grantee_key.trim().is_empty() {
            return Self::json_error(400, "grantee is required");
        }
        let grantee = payload
            .get("grantee")
            .and_then(Value::as_str)
            .filter(|g| !g.is_empty())
            .map(str::to_string);
        let grantee_email = payload
            .get("grantee_email")
            .and_then(Value::as_str)
            .map(str::to_string);
        let permission = payload
            .get("permission")
            .and_then(Value::as_str)
            .unwrap_or("view");
        let Some(level) = AccessLevel::parse(permission) else {
            return Self::json_error(400, &format!("invalid permission '{permission}'"));
        };
        let granted_by = sender.unwrap_or("").to_string();

        let grant = GrantWrite {
            grantee_key: &grantee_key,
            grantee: grantee.as_deref(),
            grantee_email: grantee_email.as_deref(),
            level,
            granted_by: &granted_by,
        };
        if let Err(e) = self.write_grant(entity, id, grant).await {
            return json_write_failure(e);
        }
        let mut shared = 1;
        if payload.get("cascade").and_then(Value::as_bool) == Some(true) {
            match self.cascade_share(entity, id, grant).await {
                Ok(granted) => shared += granted,
                Err(failed) => {
                    return Self::json_error(
                        500,
                        &format!("cascade share incomplete: {failed} resource(s) not granted"),
                    );
                }
            }
        }
        let status = if grantee.is_none() {
            "pending"
        } else {
            "shared"
        };
        Self::share_response(status, grantee.as_deref(), level, shared)
    }

    fn share_response(
        status: &str,
        grantee: Option<&str>,
        level: AccessLevel,
        resources_shared: usize,
    ) -> Vec<u8> {
        let data = json!({
            "status": status,
            "grantee": grantee,
            "permission": level.as_str(),
            "resources_shared": resources_shared,
        });
        serde_json::to_vec(&json!({ "status": "ok", "data": data })).unwrap_or_default()
    }

    pub(crate) async fn handle_unshare_local(
        &mut self,
        entity: &str,
        id: &str,
        payload: &[u8],
        sender: Option<&str>,
    ) -> Vec<u8> {
        if !self.holds_primary_role(data_partition(entity, id)) {
            return json_write_failure(DbDataStoreError::NotPrimary);
        }
        let payload: Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(_) => return Self::json_error(400, "invalid JSON payload"),
        };
        if let Some(err) = self.require_owner_or_admin(entity, id, sender) {
            return err;
        }
        let primary_key = payload
            .get("grantee")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let resolved_key = payload
            .get("grantee_key")
            .and_then(Value::as_str)
            .map(str::to_string);
        if let Err(e) = self.clear_grant(entity, id, &primary_key).await {
            return json_write_failure(e);
        }
        if let Some(resolved) = resolved_key
            && resolved != primary_key
            && let Err(e) = self.clear_grant(entity, id, &resolved).await
        {
            return json_write_failure(e);
        }
        serde_json::to_vec(&json!({
            "status": "ok",
            "data": { "status": "unshared", "grantee": primary_key }
        }))
        .unwrap_or_default()
    }

    pub(crate) fn handle_shares_local(
        &self,
        entity: &str,
        id: &str,
        sender: Option<&str>,
    ) -> Vec<u8> {
        if let Some(err) = self.require_owner_or_admin(entity, id, sender) {
            return err;
        }
        let grants: Vec<Value> = self
            .shares_for_resource(entity, id)
            .into_iter()
            .map(|(_, value)| value)
            .collect();
        #[cfg(feature = "http-api")]
        let grants: Vec<Value> = match self.identity_crypto() {
            Some(crypto) => grants
                .into_iter()
                .map(|mut grant| {
                    crypto.decrypt_json_fields(SHARES_ENTITY, &mut grant, &["grantee_email"]);
                    grant
                })
                .collect(),
            None => grants,
        };
        serde_json::to_vec(&json!({ "status": "ok", "data": grants })).unwrap_or_default()
    }
}

#[cfg(feature = "http-api")]
impl<T: ClusterTransport> NodeController<T> {
    #[must_use]
    pub(crate) fn local_identity_canonical(&self, email_hash: &str) -> Option<String> {
        let payload = Self::email_hash_filter_payload(email_hash);
        let results = self.handle_json_list_local(crate::cluster::entity::IDENTITY_LINKS, &payload);
        let parsed: Value = serde_json::from_slice(&results).ok()?;
        parsed
            .get("data")?
            .as_array()?
            .first()?
            .get("data")?
            .get("canonical_id")
            .and_then(Value::as_str)
            .map(str::to_string)
    }

    fn email_hash_filter_payload(email_hash: &str) -> Vec<u8> {
        let filter = mqdb_core::Filter::new(
            "email_hash".to_string(),
            mqdb_core::FilterOp::Eq,
            Value::String(email_hash.to_string()),
        );
        serde_json::to_vec(&json!({ "filters": [filter] })).unwrap_or_default()
    }

    pub(crate) async fn begin_identity_scatter(&mut self, cont: ShareResolveContinuation) {
        let remote_nodes: Vec<NodeId> = self
            .heartbeat
            .alive_nodes()
            .into_iter()
            .filter(|&n| n != self.node_id)
            .collect();
        if remote_nodes.is_empty() {
            self.complete_share_resolution(Vec::new(), cont).await;
            return;
        }

        let request_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX));

        let filter = mqdb_core::Filter::new(
            "email_hash".to_string(),
            mqdb_core::FilterOp::Eq,
            Value::String(cont.email_hash.clone()),
        );
        let list_payload = Self::email_hash_filter_payload(&cont.email_hash);

        let pending = PendingScatterRequest {
            expected_count: remote_nodes.len(),
            received: Vec::new(),
            client_response_topic: cont.response_topic.clone(),
            created_at_ms: self.current_time,
            filters: vec![filter],
            sorts: Vec::new(),
            projection: None,
            pagination: None,
            entity: crate::cluster::entity::IDENTITY_LINKS.to_string(),
            vault_sender: None,
            continuation: Some(cont),
        };
        self.pending_scatter_requests.insert(request_id, pending);

        let scatter_response_topic = format!("_mqdb/scatter/{}/{request_id}", self.node_id.get());
        for &target_node in &remote_nodes {
            let request = JsonDbRequest {
                request_id,
                op: JsonDbOp::List,
                entity: crate::cluster::entity::IDENTITY_LINKS.to_string(),
                id: None,
                payload: list_payload.clone(),
                response_topic: scatter_response_topic.clone(),
                correlation_data: None,
                sender: None,
                client_id: None,
            };
            let msg = ClusterMessage::JsonDbRequest {
                partition: PartitionId::ZERO,
                request,
            };
            if let Err(e) = self.transport.send(target_node, msg).await {
                tracing::warn!(?target_node, ?e, "failed to send identity resolve request");
            }
        }
    }

    pub(crate) fn invalidate_share_resolutions(
        &mut self,
        entity: &str,
        id: &str,
        grantee_key: &str,
        sender: Option<&str>,
    ) {
        let Some(uid) = sender.filter(|s| !s.is_empty()) else {
            return;
        };
        let sender_authorized = self.ownership.is_admin(uid)
            || self
                .share_owner_field(entity)
                .is_some_and(|field| self.is_resource_owner(entity, id, &field, uid));
        for pending in self.pending_scatter_requests.values_mut() {
            if let Some(cont) = pending.continuation.as_mut()
                && cont.entity == entity
                && cont.id == id
                && cont.grantee_key == grantee_key
                && (sender_authorized || cont.granted_by == uid)
            {
                cont.invalidated = true;
            }
        }
    }

    pub(crate) async fn complete_share_resolution(
        &mut self,
        items: Vec<Value>,
        cont: ShareResolveContinuation,
    ) {
        if cont.invalidated {
            let body = serde_json::to_vec(&json!({
                "status": "ok",
                "data": { "status": "superseded", "resources_shared": 0 }
            }))
            .unwrap_or_default();
            self.transport
                .queue_local_publish(cont.response_topic, body, 0)
                .await;
            return;
        }
        let canonical = items.iter().find_map(|item| {
            let data = item.get("data")?;
            if data.get("email_hash").and_then(Value::as_str) == Some(cont.email_hash.as_str()) {
                data.get("canonical_id")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            } else {
                None
            }
        });

        let mut record = json!({
            "grantee_key": cont.grantee_key,
            "permission": cont.permission,
            "cascade": cont.cascade,
        });
        if let Some(ref email) = cont.grantee_email {
            record["grantee_email"] = json!(email);
        }
        if let Some(ref id) = canonical {
            record["grantee"] = json!(id);
        }
        let payload = serde_json::to_vec(&record).unwrap_or_default();

        let partition = data_partition(&cont.entity, &cont.id);
        if self.is_primary_for_partition(partition) {
            let out = self
                .handle_share_local(
                    &cont.entity,
                    &cont.id,
                    &payload,
                    Some(cont.granted_by.as_str()),
                )
                .await;
            self.transport
                .queue_local_publish(cont.response_topic, out, 0)
                .await;
        } else {
            self.forward_json_db_request(
                partition,
                ForwardedJsonRequest {
                    op: JsonDbOp::Share,
                    entity: &cont.entity,
                    id: Some(&cont.id),
                    payload: &payload,
                    response_topic: &cont.response_topic,
                    correlation_data: cont.correlation_data.as_deref(),
                    origin: EventOrigin::new(Some(cont.granted_by.as_str()), None),
                },
            )
            .await;
        }
    }
}
