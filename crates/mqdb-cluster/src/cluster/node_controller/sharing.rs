// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

//! Cluster-side diagram sharing (#75). Reimplements the agent's `_shares` grant
//! primitives (`crates/mqdb-agent/src/database/sharing.rs`) against the cluster
//! `StoreManager`. A grant for resource `(entity, id)` is co-located on the
//! resource's partition by minting its id with `generate_id_for_partition`, so
//! grant scans and access checks run on the resource's primary.

use super::{ClusterTransport, NodeController};
use crate::cluster::db::{data_partition, generate_id_for_partition};
use mqdb_core::types::{AccessLevel, SHARES_ENTITY};
use serde_json::{Value, json};
use std::collections::{HashSet, VecDeque};

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

    /// `None` when authorized; `Some(error_payload)` otherwise.
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

    /// Highest access level granted to `sender` on `(entity, id)` (owner-or-grant
    /// gate scans the co-located `_shares`). Matches on the resolved `grantee` or
    /// the stable `grantee_key`, so it works for resolved and password-mode grants.
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

    /// Owner-or-grant access gate for a resource whose partition THIS node is the
    /// primary of (grants are co-located and locally readable). `None` = allowed.
    /// Callers must only invoke this on the resource primary; a non-primary node
    /// forwards the request so the primary grades it (see #75 read-routing).
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

    async fn clear_grant(&mut self, entity: &str, id: &str, grantee_key: &str) {
        let victims: Vec<String> = self
            .shares_for_resource(entity, id)
            .into_iter()
            .filter_map(|(sid, value)| {
                (value.get("grantee_key").and_then(Value::as_str) == Some(grantee_key))
                    .then_some(sid)
            })
            .collect();
        for sid in victims {
            let _ = self.db_delete(SHARES_ENTITY, &sid).await;
        }
    }

    /// Remove every grant on a resource (called when the resource is deleted).
    pub(crate) async fn clear_all_resource_grants(&mut self, entity: &str, id: &str) {
        let victims: Vec<String> = self
            .shares_for_resource(entity, id)
            .into_iter()
            .map(|(sid, _)| sid)
            .collect();
        for sid in victims {
            let _ = self.db_delete(SHARES_ENTITY, &sid).await;
        }
    }

    async fn write_grant(
        &mut self,
        entity: &str,
        id: &str,
        grant: GrantWrite<'_>,
    ) -> Result<(), String> {
        self.clear_grant(entity, id, grant.grantee_key).await;
        if let Some(resolved) = grant.grantee
            && resolved != grant.grantee_key
        {
            self.clear_grant(entity, id, resolved).await;
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
        let bytes = serde_json::to_vec(&record).map_err(|e| e.to_string())?;
        let now = Self::current_time_ms();
        self.db_create(SHARES_ENTITY, &share_id, &bytes, now)
            .await
            .map_err(|e| format!("failed to write grant: {e:?}"))?;
        Ok(())
    }

    /// Self-reference fields on `entity` (FK constraints whose target is the same
    /// entity) — the edges a cascade share walks.
    fn self_ref_fields(&self, entity: &str) -> Vec<String> {
        self.stores
            .constraint_get_fk_constraints(entity)
            .into_iter()
            .filter(|c| c.is_foreign_key() && c.target_entity_str() == entity)
            .map(|c| c.field_str().to_string())
            .collect()
    }

    /// Grant `grant` across every resource reachable from `root_id` via
    /// self-references, at max-of-levels (never downgrading an existing grant).
    /// Walks only the locally-held closure — cross-partition closure members are a
    /// follow-up (they need cycle-safe cross-node fan-out); complete on a node that
    /// is primary for the whole closure.
    async fn cascade_share(&mut self, entity: &str, root_id: &str, grant: GrantWrite<'_>) -> usize {
        let fields = self.self_ref_fields(entity);
        if fields.is_empty() {
            return 0;
        }
        let mut visited: HashSet<String> = HashSet::from([root_id.to_string()]);
        let mut queue: VecDeque<String> = VecDeque::from([root_id.to_string()]);
        let mut granted = 0;
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
                if self.is_primary_for_partition(data_partition(entity, &child)) {
                    let existing = self.existing_grant_level(entity, &child, grant.grantee_key);
                    if existing.is_none_or(|current| current < grant.level) {
                        let _ = self.write_grant(entity, &child, grant).await;
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
        granted
    }

    /// Primary-side handler for `$DB/{entity}/{id}/share`. Grants the root resource
    /// (set-to-level, may demote) and, when `cascade` is set, the locally-held
    /// self-reference closure at max-of-levels.
    pub(crate) async fn handle_share_local(
        &mut self,
        entity: &str,
        id: &str,
        payload: &[u8],
        sender: Option<&str>,
    ) -> Vec<u8> {
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
            return Self::json_error(500, &e);
        }
        let mut shared = 1;
        if payload.get("cascade").and_then(Value::as_bool) == Some(true) {
            shared += self.cascade_share(entity, id, grant).await;
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

    /// Primary-side handler for `$DB/{entity}/{id}/unshare` (single resource).
    /// Revokes by every carried key so a grant is removed whether it was stored
    /// under the input identifier or a resolved canonical id.
    pub(crate) async fn handle_unshare_local(
        &mut self,
        entity: &str,
        id: &str,
        payload: &[u8],
        sender: Option<&str>,
    ) -> Vec<u8> {
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
        self.clear_grant(entity, id, &primary_key).await;
        if let Some(resolved) = resolved_key
            && resolved != primary_key
        {
            self.clear_grant(entity, id, &resolved).await;
        }
        serde_json::to_vec(&json!({
            "status": "ok",
            "data": { "status": "unshared", "grantee": primary_key }
        }))
        .unwrap_or_default()
    }

    /// Primary-side handler for `$DB/{entity}/{id}/shares` (owner/admin lists a
    /// resource's grants). `grantee_email` stays encrypted here; the client node
    /// decrypts it for display.
    pub(crate) fn handle_shares_local(
        &self,
        entity: &str,
        id: &str,
        sender: Option<&str>,
    ) -> Vec<u8> {
        if let Some(err) = self.require_owner_or_admin(entity, id, sender) {
            return err;
        }
        #[cfg_attr(not(feature = "http-api"), allow(unused_mut))]
        let mut grants: Vec<Value> = self
            .shares_for_resource(entity, id)
            .into_iter()
            .map(|(_, value)| value)
            .collect();
        #[cfg(feature = "http-api")]
        if let Some(crypto) = self.identity_crypto() {
            for grant in &mut grants {
                crypto.decrypt_json_fields(SHARES_ENTITY, grant, &["grantee_email"]);
            }
        }
        serde_json::to_vec(&json!({ "status": "ok", "data": grants })).unwrap_or_default()
    }
}

#[cfg(feature = "http-api")]
impl<T: ClusterTransport> NodeController<T> {
    /// Scan the locally-held `_identity_links` for the canonical id whose
    /// `email_hash` matches. Identity records are partitioned, so a miss here only
    /// means the link lives on another partition (resolve it via a scatter).
    #[must_use]
    pub(crate) fn local_identity_canonical(&self, email_hash: &str) -> Option<String> {
        self.db_list(crate::cluster::entity::IDENTITY_LINKS)
            .into_iter()
            .find_map(|e| {
                let value: Value = serde_json::from_slice(&e.data).ok()?;
                if value.get("email_hash").and_then(Value::as_str) == Some(email_hash) {
                    value
                        .get("canonical_id")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                } else {
                    None
                }
            })
    }

    /// Fan out a `_identity_links` LIST filtered by `email_hash` to resolve an
    /// email grant's canonical grantee across partitions; the grant is written
    /// when the scatter completes (`complete_share_resolution`).
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

        #[allow(clippy::cast_possible_truncation)]
        let request_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64);

        let filter = mqdb_core::Filter::new(
            "email_hash".to_string(),
            mqdb_core::FilterOp::Eq,
            Value::String(cont.email_hash.clone()),
        );
        let list_payload =
            serde_json::to_vec(&json!({ "filters": [filter.clone()] })).unwrap_or_default();

        let local_results =
            self.handle_json_list_local(crate::cluster::entity::IDENTITY_LINKS, &list_payload);
        let local_items: Vec<Value> = serde_json::from_slice::<Value>(&local_results)
            .ok()
            .and_then(|p| p.get("data").and_then(|d| d.as_array()).cloned())
            .unwrap_or_default();

        let pending = PendingScatterRequest {
            expected_count: remote_nodes.len(),
            received: local_items,
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

    /// Mark every in-flight resolution for `(entity, id, grantee_key)` as
    /// invalidated so its completing write is skipped. Called when an unshare for
    /// that grantee lands on this node while the share is still resolving — closes
    /// the revoked-grant resurrection race (`specs/ShareResolveRace.tla`).
    pub(crate) fn invalidate_share_resolutions(
        &mut self,
        entity: &str,
        id: &str,
        grantee_key: &str,
    ) {
        for pending in self.pending_scatter_requests.values_mut() {
            if let Some(cont) = pending.continuation.as_mut()
                && cont.entity == entity
                && cont.id == id
                && cont.grantee_key == grantee_key
            {
                cont.invalidated = true;
            }
        }
    }

    /// Complete a resolved (or unresolved) email share: pick the canonical id from
    /// the gathered `_identity_links`, build the grant payload, and route it to the
    /// resource primary. An unresolved grantee is written as a pending grant. A
    /// resolution invalidated by an intervening unshare writes nothing.
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
                JsonDbOp::Share,
                &cont.entity,
                Some(&cont.id),
                &payload,
                &cont.response_topic,
                cont.correlation_data.as_deref(),
                Some(cont.granted_by.as_str()),
            )
            .await;
        }
    }
}
