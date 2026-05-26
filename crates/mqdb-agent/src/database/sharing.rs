// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::Database;
use mqdb_core::error::{Error, Result};
use mqdb_core::keys;
use mqdb_core::types::{AccessLevel, OwnershipConfig, SHARES_ENTITY, ScopeConfig};
use mqdb_core::{Filter, FilterOp};
use serde_json::{Value, json};

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

    async fn clear_grant(&self, entity: &str, id: &str, grantee_key: &str) -> Result<()> {
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
        let scope = ScopeConfig::default();
        let ownership = OwnershipConfig::default();
        for rec in &records {
            if let Some(sid) = rec.get("id").and_then(Value::as_str) {
                self.delete(
                    SHARES_ENTITY.to_string(),
                    sid.to_string(),
                    None,
                    None,
                    &scope,
                    &ownership,
                )
                .await?;
            }
        }
        Ok(())
    }

    /// Grant `grantee` access to a resource at `permission` (`view`/`edit`).
    /// Direct share is set-to-level: any existing grant for the same grantee is replaced.
    ///
    /// # Errors
    /// Returns `Forbidden` if the sender is not the owner/admin, `Validation` for a
    /// bad permission or non-shareable entity, or `NotFound` if the resource is missing.
    pub async fn share_grant(
        &self,
        entity: &str,
        id: &str,
        grantee: &str,
        permission: &str,
        sender: Option<&str>,
        ownership: &OwnershipConfig,
    ) -> Result<Value> {
        self.require_owner_or_admin(ownership, entity, id, sender)?;
        if grantee.trim().is_empty() {
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
        self.clear_grant(entity, id, grantee).await?;
        let record = json!({
            "resource_entity": entity,
            "resource_id": id,
            "grantee": grantee,
            "grantee_key": grantee,
            "permission": level.as_str(),
            "granted_by": sender.unwrap_or_default(),
        });
        self.create(
            SHARES_ENTITY.to_string(),
            record,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
    }

    /// Revoke `grantee`'s grant on a resource.
    ///
    /// # Errors
    /// Returns `Forbidden` if the sender is not the owner/admin, or `Validation` for a
    /// non-shareable entity.
    pub async fn share_revoke(
        &self,
        entity: &str,
        id: &str,
        grantee: &str,
        sender: Option<&str>,
        ownership: &OwnershipConfig,
    ) -> Result<Value> {
        self.require_owner_or_admin(ownership, entity, id, sender)?;
        self.clear_grant(entity, id, grantee).await?;
        Ok(json!({ "status": "unshared", "grantee": grantee }))
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
}
