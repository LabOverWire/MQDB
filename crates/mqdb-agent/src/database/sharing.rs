// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::{CallerContext, Database};
use mqdb_core::entity::Entity;
use mqdb_core::error::{Error, Result};
use mqdb_core::keys;
use mqdb_core::types::{AccessLevel, OwnershipConfig, SHARES_ENTITY, ScopeConfig};
use mqdb_core::{Filter, FilterOp};
use serde_json::{Value, json};
use std::collections::{BTreeSet, HashSet, VecDeque};

const MAX_CASCADE_DIAGRAMS: usize = 256;

fn eq_filter(field: &str, value: &str) -> Filter {
    Filter::new(
        field.to_string(),
        FilterOp::Eq,
        Value::String(value.to_string()),
    )
}

impl Database {
    fn resource_filters(entity: &str, id: &str) -> Vec<Filter> {
        vec![
            eq_filter("resource_entity", entity),
            eq_filter("resource_id", id),
        ]
    }

    fn require_owner_or_admin(
        &self,
        ownership: &OwnershipConfig,
        entity: &str,
        id: &str,
        sender: Option<&str>,
    ) -> Result<()> {
        let Some(owner_field) = ownership.owner_field(entity) else {
            return Err(Error::Validation(format!(
                "entity '{entity}' is not shareable"
            )));
        };
        let Some(uid) = sender else {
            return Ok(());
        };
        if ownership.is_admin(uid) || self.is_owner(entity, id, owner_field, uid)? {
            Ok(())
        } else {
            Err(Error::Forbidden("permission denied".to_string()))
        }
    }

    async fn delete_grants(&self, filters: Vec<Filter>, ownership: &OwnershipConfig) -> Result<()> {
        let records = self
            .list_core(
                SHARES_ENTITY.to_string(),
                filters,
                vec![],
                None,
                vec![],
                None,
            )
            .await?;
        let scope = ScopeConfig::default();
        for rec in &records {
            if let Some(sid) = rec.get("id").and_then(Value::as_str) {
                self.delete(
                    SHARES_ENTITY.to_string(),
                    sid.to_string(),
                    None,
                    None,
                    &scope,
                    ownership,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn clear_grant(
        &self,
        entity: &str,
        id: &str,
        grantee_key: &str,
        ownership: &OwnershipConfig,
    ) -> Result<()> {
        let mut filters = Self::resource_filters(entity, id);
        filters.push(eq_filter("grantee_key", grantee_key));
        self.delete_grants(filters, ownership).await
    }

    /// Remove every grant on a resource. Called when the resource itself is deleted
    /// so stale grants cannot be inherited by a later record reusing the same id.
    ///
    /// # Errors
    /// Returns an error if scanning or deleting the share records fails.
    pub(crate) async fn clear_all_resource_grants(
        &self,
        entity: &str,
        id: &str,
        ownership: &OwnershipConfig,
    ) -> Result<()> {
        self.delete_grants(Self::resource_filters(entity, id), ownership)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn write_grant(
        &self,
        entity: &str,
        id: &str,
        grantee_key: &str,
        grantee: Option<&str>,
        level: AccessLevel,
        granted_by: &str,
        ownership: &OwnershipConfig,
    ) -> Result<()> {
        self.clear_grant(entity, id, grantee_key, ownership).await?;
        let record = json!({
            "resource_entity": entity,
            "resource_id": id,
            "grantee": grantee,
            "grantee_key": grantee_key,
            "permission": level.as_str(),
            "granted_by": granted_by,
        });
        self.create(
            SHARES_ENTITY.to_string(),
            record,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await?;
        Ok(())
    }

    async fn self_reference_fields(&self, entity: &str) -> Vec<String> {
        let mut fields = BTreeSet::new();
        for rel in self.list_relationships(entity).await {
            if rel.target_entity == entity {
                fields.insert(rel.field_suffix);
            }
        }
        for constraint in self.list_constraints(entity).await {
            if let mqdb_core::constraint::Constraint::ForeignKey(fk) = constraint
                && fk.target_entity == entity
            {
                fields.insert(fk.source_field);
            }
        }
        fields.into_iter().collect()
    }

    async fn referenced_closure(&self, entity: &str, root_id: &str) -> Result<Vec<String>> {
        let ref_fields = self.self_reference_fields(entity).await;
        let mut visited: HashSet<String> = HashSet::new();
        let mut queue: VecDeque<String> = VecDeque::new();
        queue.push_back(root_id.to_string());
        while let Some(id) = queue.pop_front() {
            if visited.len() >= MAX_CASCADE_DIAGRAMS {
                break;
            }
            if !visited.insert(id.clone()) {
                continue;
            }
            let key = keys::encode_data_key(entity, &id);
            let Some(bytes) = self.storage.get(&key)? else {
                continue;
            };
            let record = Entity::deserialize(entity.to_string(), id.clone(), &bytes)?;
            for field in &ref_fields {
                if let Some(ref_id) = record.data.get(field).and_then(Value::as_str)
                    && !visited.contains(ref_id)
                {
                    queue.push_back(ref_id.to_string());
                }
            }
        }
        Ok(visited.into_iter().collect())
    }

    /// Grant `grantee` access to a resource at `permission` (`view`/`edit`).
    /// The root resource is set-to-level (a re-share may demote it); when `cascade`
    /// is set, every diagram reachable via self-references is granted at max-of-levels
    /// (never downgrading an existing grant).
    ///
    /// # Errors
    /// Returns `Forbidden` if the sender is not the owner/admin, `Validation` for a
    /// bad permission or non-shareable entity, or `NotFound` if the resource is missing.
    #[allow(clippy::too_many_arguments)]
    pub async fn share_grant(
        &self,
        entity: &str,
        id: &str,
        grantee_key: &str,
        grantee: Option<&str>,
        permission: &str,
        sender: Option<&str>,
        ownership: &OwnershipConfig,
        cascade: bool,
    ) -> Result<Value> {
        self.require_owner_or_admin(ownership, entity, id, sender)?;
        if grantee_key.trim().is_empty() {
            return Err(Error::Validation("grantee is required".to_string()));
        }
        let level = AccessLevel::parse(permission)
            .ok_or_else(|| Error::Validation(format!("invalid permission '{permission}'")))?;
        let key = keys::encode_data_key(entity, id);
        if self.storage.get(&key)?.is_none() {
            return Err(Error::NotFound {
                entity: entity.to_string(),
                id: id.to_string(),
            });
        }
        let granted_by = sender.unwrap_or_default();
        self.write_grant(
            entity,
            id,
            grantee_key,
            grantee,
            level,
            granted_by,
            ownership,
        )
        .await?;
        let mut shared = 1usize;
        if cascade {
            for ref_id in self.referenced_closure(entity, id).await? {
                if ref_id == id {
                    continue;
                }
                let existing = self
                    .existing_grant_level(entity, &ref_id, grantee_key)
                    .await?;
                if existing.is_none_or(|current| current < level) {
                    self.write_grant(
                        entity,
                        &ref_id,
                        grantee_key,
                        grantee,
                        level,
                        granted_by,
                        ownership,
                    )
                    .await?;
                }
                shared += 1;
            }
        }
        Ok(json!({
            "status": if grantee.is_none() { "pending" } else { "shared" },
            "grantee": grantee,
            "grantee_key": grantee_key,
            "permission": level.as_str(),
            "resources_shared": shared,
        }))
    }

    /// Revoke a grantee's grant on a resource, and (when `cascade` is set) across
    /// every diagram reachable via self-references. Revokes by every known key so a
    /// grant survives regardless of whether it was stored under the input identifier
    /// (email/username) or a resolved canonical id.
    ///
    /// # Errors
    /// Returns `Forbidden` if the sender is not the owner/admin, or `Validation` for a
    /// non-shareable entity.
    #[allow(clippy::too_many_arguments)]
    pub async fn share_revoke(
        &self,
        entity: &str,
        id: &str,
        grantee_key: &str,
        resolved_key: Option<&str>,
        sender: Option<&str>,
        ownership: &OwnershipConfig,
        cascade: bool,
    ) -> Result<Value> {
        self.require_owner_or_admin(ownership, entity, id, sender)?;
        let mut keys: Vec<&str> = vec![grantee_key];
        if let Some(resolved) = resolved_key
            && resolved != grantee_key
        {
            keys.push(resolved);
        }
        for revoke_key in &keys {
            self.clear_grant(entity, id, revoke_key, ownership).await?;
        }
        if cascade {
            for ref_id in self.referenced_closure(entity, id).await? {
                if ref_id == id {
                    continue;
                }
                for revoke_key in &keys {
                    self.clear_grant(entity, &ref_id, revoke_key, ownership)
                        .await?;
                }
            }
        }
        Ok(json!({ "status": "unshared", "grantee": grantee_key }))
    }

    /// Highest access level already granted on a resource for a given input
    /// identifier (`grantee_key`), independent of whether the grant is resolved or
    /// still pending. Used by cascade to avoid downgrading an existing grant.
    async fn existing_grant_level(
        &self,
        entity: &str,
        id: &str,
        grantee_key: &str,
    ) -> Result<Option<AccessLevel>> {
        let mut filters = Self::resource_filters(entity, id);
        filters.push(eq_filter("grantee_key", grantee_key));
        let records = self
            .list_core(
                SHARES_ENTITY.to_string(),
                filters,
                vec![],
                None,
                vec![],
                None,
            )
            .await?;
        Ok(records
            .iter()
            .filter_map(|r| r.get("permission").and_then(Value::as_str))
            .filter_map(AccessLevel::parse)
            .max())
    }

    /// Fill pending grants for a now-verified identity. Sweeps `_shares` by
    /// `grantee_key` and sets `grantee = canonical_id` on every row whose `grantee`
    /// is still null. Idempotent — a second sweep fills nothing. Returns the count
    /// of grants filled.
    ///
    /// # Errors
    /// Returns an error if scanning or updating the share records fails.
    pub(crate) async fn resolve_pending_grants(
        &self,
        grantee_key: &str,
        canonical_id: &str,
    ) -> Result<usize> {
        let filters = vec![eq_filter("grantee_key", grantee_key)];
        let rows = self
            .list_core(
                SHARES_ENTITY.to_string(),
                filters,
                vec![],
                None,
                vec![],
                None,
            )
            .await?;
        let scope = ScopeConfig::default();
        let mut filled = 0;
        for row in &rows {
            let is_pending = row.get("grantee").is_none_or(Value::is_null);
            if !is_pending {
                continue;
            }
            if let Some(sid) = row.get("id").and_then(Value::as_str) {
                let caller = CallerContext {
                    sender: None,
                    client_id: None,
                    scope_config: &scope,
                };
                self.update(
                    SHARES_ENTITY.to_string(),
                    sid.to_string(),
                    json!({ "grantee": canonical_id }),
                    None,
                    &caller,
                )
                .await?;
                filled += 1;
            }
        }
        Ok(filled)
    }

    /// List the grants on a resource (owner/admin only).
    ///
    /// # Errors
    /// Returns `Forbidden` if the sender is not the owner/admin, or `Validation` for a
    /// non-shareable entity.
    pub async fn list_resource_shares(
        &self,
        entity: &str,
        id: &str,
        sender: Option<&str>,
        ownership: &OwnershipConfig,
    ) -> Result<Vec<Value>> {
        self.require_owner_or_admin(ownership, entity, id, sender)?;
        self.list_core(
            SHARES_ENTITY.to_string(),
            Self::resource_filters(entity, id),
            vec![],
            None,
            vec![],
            None,
        )
        .await
    }

    /// List the resources of `entity` shared with the caller.
    ///
    /// # Errors
    /// Returns an error if scanning the share records or reading a resource fails.
    pub async fn list_shared_with(&self, entity: &str, sender: Option<&str>) -> Result<Vec<Value>> {
        let Some(uid) = sender else {
            return Ok(vec![]);
        };
        let filters = vec![
            eq_filter("resource_entity", entity),
            eq_filter("grantee", uid),
        ];
        let grants = self
            .list_core(
                SHARES_ENTITY.to_string(),
                filters,
                vec![],
                None,
                vec![],
                None,
            )
            .await?;
        let mut resources = Vec::new();
        for grant in &grants {
            if let Some(rid) = grant.get("resource_id").and_then(Value::as_str)
                && let Ok(record) = self
                    .read(entity.to_string(), rid.to_string(), vec![], None)
                    .await
            {
                resources.push(record);
            }
        }
        Ok(resources)
    }

    /// All grantee identities with a grant on a resource. Every grant is at least
    /// `view`, so all qualify as event recipients.
    ///
    /// # Errors
    /// Returns an error if scanning the share records fails.
    pub(crate) async fn resource_grantees(&self, entity: &str, id: &str) -> Result<Vec<String>> {
        let records = self
            .list_core(
                SHARES_ENTITY.to_string(),
                Self::resource_filters(entity, id),
                vec![],
                None,
                vec![],
                None,
            )
            .await?;
        Ok(records
            .iter()
            .filter_map(|r| r.get("grantee").and_then(Value::as_str).map(String::from))
            .collect())
    }

    fn record_owner(
        &self,
        entity: &str,
        id: &str,
        ownership: &OwnershipConfig,
    ) -> Result<Option<String>> {
        let Some(owner_field) = ownership.owner_field(entity) else {
            return Ok(None);
        };
        let key = keys::encode_data_key(entity, id);
        let Some(bytes) = self.storage.get(&key)? else {
            return Ok(None);
        };
        let record = Entity::deserialize(entity.to_string(), id.to_string(), &bytes)?;
        Ok(record
            .data
            .get(owner_field)
            .and_then(Value::as_str)
            .map(String::from))
    }

    /// Recipients of a change event for confidentiality-scoped delivery: the owner
    /// plus every grantee of the governing resource. Ownership entities govern
    /// themselves; derived (child) entities govern through their parent. Returns
    /// `None` for entities with no ownership/derivation (Global — broadcast as before).
    ///
    /// # Errors
    /// Returns an error if reading the parent record or scanning grants fails.
    pub async fn event_recipients(
        &self,
        ownership: &OwnershipConfig,
        entity: &str,
        id: &str,
        data: Option<&Value>,
    ) -> Result<Option<Vec<String>>> {
        if entity == SHARES_ENTITY {
            let mut recipients: Vec<String> = Vec::new();
            if let Some(grantee) = data
                .and_then(|d| d.get("grantee"))
                .and_then(Value::as_str)
                .filter(|g| !g.is_empty())
            {
                recipients.push(grantee.to_string());
            }
            if let Some(res_entity) = data
                .and_then(|d| d.get("resource_entity"))
                .and_then(Value::as_str)
                && let Some(res_id) = data
                    .and_then(|d| d.get("resource_id"))
                    .and_then(Value::as_str)
                && let Some(owner) = self.record_owner(res_entity, res_id, ownership)?
            {
                let granted_by = data
                    .and_then(|d| d.get("granted_by"))
                    .and_then(Value::as_str);
                if granted_by != Some(owner.as_str()) && !recipients.contains(&owner) {
                    recipients.push(owner);
                }
            }
            return Ok(Some(recipients));
        }

        let (res_entity, res_id, owner) = if let Some(owner_field) = ownership.owner_field(entity) {
            let owner = data
                .and_then(|d| d.get(owner_field))
                .and_then(Value::as_str)
                .map(String::from);
            (entity.to_string(), id.to_string(), owner)
        } else if let Some((fk_field, parent_entity)) = ownership.derivation(entity) {
            let Some(parent_id) = data.and_then(|d| d.get(fk_field)).and_then(Value::as_str) else {
                return Ok(Some(vec![]));
            };
            let owner = self.record_owner(parent_entity, parent_id, ownership)?;
            (parent_entity.to_string(), parent_id.to_string(), owner)
        } else {
            return Ok(None);
        };

        let mut recipients: Vec<String> = Vec::new();
        if let Some(o) = owner {
            recipients.push(o);
        }
        for grantee in self.resource_grantees(&res_entity, &res_id).await? {
            if !recipients.contains(&grantee) {
                recipients.push(grantee);
            }
        }
        Ok(Some(recipients))
    }
}
