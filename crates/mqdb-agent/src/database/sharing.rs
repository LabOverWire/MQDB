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

#[derive(Clone, Copy)]
pub struct GranteeIdentity<'a> {
    pub key: &'a str,
    pub resolved: Option<&'a str>,
    pub display: Option<&'a str>,
}

#[derive(Clone, Copy)]
pub struct ShareActor<'a> {
    pub sender: Option<&'a str>,
    pub ownership: &'a OwnershipConfig,
}

struct ShareableClosure {
    members: Vec<String>,
    skipped: usize,
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

    async fn delete_grants(
        &self,
        filters: Vec<Filter>,
        actor: Option<&str>,
        ownership: &OwnershipConfig,
    ) -> Result<()> {
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
                    actor,
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
        actor: Option<&str>,
        ownership: &OwnershipConfig,
    ) -> Result<()> {
        let mut filters = Self::resource_filters(entity, id);
        filters.push(eq_filter("grantee_key", grantee_key));
        self.delete_grants(filters, actor, ownership).await
    }

    pub(crate) async fn clear_all_resource_grants(
        &self,
        entity: &str,
        id: &str,
        actor: Option<&str>,
        ownership: &OwnershipConfig,
    ) -> Result<()> {
        self.delete_grants(Self::resource_filters(entity, id), actor, ownership)
            .await
    }

    async fn write_grant(
        &self,
        entity: &str,
        id: &str,
        grantee: GranteeIdentity<'_>,
        level: AccessLevel,
        granted_by: &str,
        ownership: &OwnershipConfig,
    ) -> Result<()> {
        let actor = (!granted_by.is_empty()).then_some(granted_by);
        self.clear_grant(entity, id, grantee.key, actor, ownership)
            .await?;
        if let Some(resolved) = grantee.resolved
            && resolved != grantee.key
        {
            self.clear_grant(entity, id, resolved, actor, ownership)
                .await?;
        }
        let record = json!({
            "resource_entity": entity,
            "resource_id": id,
            "grantee": grantee.resolved,
            "grantee_key": grantee.key,
            "grantee_email": grantee.display,
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

    fn may_share(
        &self,
        entity: &str,
        id: &str,
        owner_field: &str,
        actor: ShareActor<'_>,
    ) -> Result<bool> {
        let key = keys::encode_data_key(entity, id);
        if self.storage.get(&key)?.is_none() {
            return Ok(false);
        }
        match actor.sender {
            Some(uid) if !actor.ownership.is_admin(uid) => {
                self.is_owner(entity, id, owner_field, uid)
            }
            _ => Ok(true),
        }
    }

    async fn shareable_closure(
        &self,
        entity: &str,
        root_id: &str,
        actor: ShareActor<'_>,
    ) -> Result<ShareableClosure> {
        let Some(owner_field) = actor.ownership.owner_field(entity) else {
            return Ok(ShareableClosure {
                members: vec![],
                skipped: 0,
            });
        };
        let ref_fields = self.self_reference_fields(entity).await;
        let mut visited: HashSet<String> = HashSet::from([root_id.to_string()]);
        let mut queue: VecDeque<String> = VecDeque::from([root_id.to_string()]);
        let mut members = Vec::new();
        let mut skipped = 0;
        while let Some(id) = queue.pop_front() {
            let key = keys::encode_data_key(entity, &id);
            let Some(bytes) = self.storage.get(&key)? else {
                continue;
            };
            let record = Entity::deserialize(entity.to_string(), id, &bytes)?;
            for field in &ref_fields {
                let Some(ref_id) = record.data.get(field).and_then(Value::as_str) else {
                    continue;
                };
                if visited.len() >= MAX_CASCADE_DIAGRAMS || !visited.insert(ref_id.to_string()) {
                    continue;
                }
                if self.may_share(entity, ref_id, owner_field, actor)? {
                    members.push(ref_id.to_string());
                    queue.push_back(ref_id.to_string());
                } else {
                    skipped += 1;
                }
            }
        }
        Ok(ShareableClosure { members, skipped })
    }

    pub async fn share_grant(
        &self,
        entity: &str,
        id: &str,
        grantee: GranteeIdentity<'_>,
        permission: &str,
        actor: ShareActor<'_>,
        cascade: bool,
    ) -> Result<Value> {
        let ShareActor { sender, ownership } = actor;
        self.require_owner_or_admin(ownership, entity, id, sender)?;
        if grantee.key.trim().is_empty() {
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
        self.write_grant(entity, id, grantee, level, granted_by, ownership)
            .await?;
        let closure = if cascade {
            self.shareable_closure(entity, id, actor).await?
        } else {
            ShareableClosure {
                members: vec![],
                skipped: 0,
            }
        };
        for ref_id in &closure.members {
            let existing = self
                .existing_grant_level(entity, ref_id, grantee.key)
                .await?;
            if existing.is_none_or(|current| current < level) {
                self.write_grant(entity, ref_id, grantee, level, granted_by, ownership)
                    .await?;
            }
        }
        Ok(json!({
            "status": if grantee.resolved.is_none() { "pending" } else { "shared" },
            "grantee": grantee.resolved,
            "permission": level.as_str(),
            "resources_shared": 1 + closure.members.len(),
            "resources_skipped": closure.skipped,
        }))
    }

    pub async fn share_revoke(
        &self,
        entity: &str,
        id: &str,
        grantee: GranteeIdentity<'_>,
        actor: ShareActor<'_>,
        cascade: bool,
    ) -> Result<Value> {
        let ShareActor { sender, ownership } = actor;
        self.require_owner_or_admin(ownership, entity, id, sender)?;
        let mut keys: Vec<&str> = vec![grantee.key];
        if let Some(resolved) = grantee.resolved
            && resolved != grantee.key
        {
            keys.push(resolved);
        }
        let mut revoked_ids = vec![id.to_string()];
        if cascade {
            revoked_ids.extend(self.shareable_closure(entity, id, actor).await?.members);
        }
        for revoked_id in &revoked_ids {
            for revoke_key in &keys {
                self.clear_grant(entity, revoked_id, revoke_key, sender, ownership)
                    .await?;
            }
        }
        Ok(json!({ "status": "unshared", "grantee": grantee.key }))
    }

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

    pub async fn event_recipients(
        &self,
        ownership: &OwnershipConfig,
        entity: &str,
        id: &str,
        data: Option<&Value>,
        actor: Option<&str>,
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
                let performed_by = actor.or_else(|| {
                    data.and_then(|d| d.get("granted_by"))
                        .and_then(Value::as_str)
                });
                if performed_by != Some(owner.as_str()) && !recipients.contains(&owner) {
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
