// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::database::{CallerContext, Database};
use mqdb_core::transport::{Request, Response, VaultConstraintData};
use mqdb_core::types::{OwnershipConfig, ScopeConfig};
use serde_json::Value;

fn value_from_unit(_: ()) -> Value {
    Value::Null
}

fn value_from_vec(v: Vec<Value>) -> Value {
    Value::Array(v)
}

fn value_from_string(s: String) -> Value {
    Value::String(s)
}

impl Database {
    pub async fn execute(&self, request: Request) -> Response {
        self.execute_with_sender(
            request,
            None,
            None,
            &OwnershipConfig::default(),
            &ScopeConfig::default(),
            None,
        )
        .await
    }

    #[allow(clippy::too_many_lines)]
    pub async fn execute_with_sender(
        &self,
        request: Request,
        sender: Option<&str>,
        client_id: Option<&str>,
        ownership: &OwnershipConfig,
        scope_config: &ScopeConfig,
        vault_constraint: Option<VaultConstraintData>,
    ) -> Response {
        match request {
            Request::Create { entity, data } => {
                let constraint_data = match vault_constraint {
                    Some(VaultConstraintData::Create(cd)) => Some(cd),
                    _ => None,
                };
                match self
                    .create(
                        entity,
                        data,
                        constraint_data,
                        sender,
                        client_id,
                        scope_config,
                    )
                    .await
                {
                    Ok(v) => Response::ok(v),
                    Err(e) => e.into(),
                }
            }
            Request::Read {
                entity,
                id,
                includes,
                projection,
            } => {
                if let Some(uid) = sender
                    && !ownership.is_admin(uid)
                    && let Some(owner_field) = ownership.owner_field(&entity)
                    && let Err(e) = self.check_ownership(&entity, &id, owner_field, uid)
                {
                    return e.into();
                }
                match self.read(entity, id, includes, projection).await {
                    Ok(v) => Response::ok(v),
                    Err(e) => e.into(),
                }
            }
            Request::Update {
                entity,
                id,
                mut fields,
            } => {
                if let Some(uid) = sender
                    && !ownership.is_admin(uid)
                    && let Some(owner_field) = ownership.owner_field(&entity)
                {
                    if let Err(e) = self.check_ownership(&entity, &id, owner_field, uid) {
                        return e.into();
                    }
                    if let Value::Object(ref mut map) = fields {
                        map.remove(owner_field);
                    }
                }
                let update_constraint = match vault_constraint {
                    Some(VaultConstraintData::Update(new_data, old_data)) => {
                        Some((new_data, old_data))
                    }
                    _ => None,
                };
                let caller = CallerContext {
                    sender,
                    client_id,
                    scope_config,
                };
                match self
                    .update(entity, id, fields, update_constraint, &caller)
                    .await
                {
                    Ok(v) => Response::ok(v),
                    Err(e) => e.into(),
                }
            }
            Request::Delete { entity, id } => {
                if let Some(uid) = sender
                    && !ownership.is_admin(uid)
                    && let Some(owner_field) = ownership.owner_field(&entity)
                    && let Err(e) = self.check_ownership(&entity, &id, owner_field, uid)
                {
                    return e.into();
                }
                let id_clone = id.clone();
                match self
                    .delete(entity, id, sender, client_id, scope_config)
                    .await
                {
                    Ok(()) => Response::ok(serde_json::json!({
                        "id": id_clone,
                        "deleted": true
                    })),
                    Err(e) => e.into(),
                }
            }
            Request::List {
                entity,
                mut filters,
                sort,
                pagination,
                includes,
                projection,
            } => {
                if let Err(e) = self
                    .validate_list_fields(&entity, &filters, &sort, projection.as_deref())
                    .await
                {
                    return e.into();
                }
                if let Some(uid) = sender
                    && !ownership.is_admin(uid)
                    && let Some(owner_field) = ownership.owner_field(&entity)
                {
                    filters.push(mqdb_core::Filter::new(
                        owner_field.to_string(),
                        mqdb_core::FilterOp::Eq,
                        Value::String(uid.to_string()),
                    ));
                }
                match self
                    .list_core(entity, filters, sort, pagination, includes, projection)
                    .await
                {
                    Ok(v) => Response::ok(value_from_vec(v)),
                    Err(e) => e.into(),
                }
            }
            Request::Subscribe {
                pattern,
                entity,
                share_group,
                mode,
            } => match (share_group, mode) {
                (Some(group), Some(m)) => {
                    match self.subscribe_shared(pattern, entity, group, m).await {
                        Ok(result) => Response::ok(serde_json::json!({
                            "id": result.id,
                            "assigned_partitions": result.assigned_partitions
                        })),
                        Err(e) => e.into(),
                    }
                }
                _ => match self.subscribe(pattern, entity).await {
                    Ok(id) => Response::ok(value_from_string(id)),
                    Err(e) => e.into(),
                },
            },
            Request::Unsubscribe { id } => match self.unsubscribe(&id).await {
                Ok(()) => Response::ok(value_from_unit(())),
                Err(e) => e.into(),
            },
        }
    }
}
