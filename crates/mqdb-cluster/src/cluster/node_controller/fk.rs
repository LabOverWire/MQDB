// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{
    ClusterMessage, ClusterTransport, NodeController, NodeId, PendingFkCheck,
    PendingFkReverseLookup,
};
use crate::cluster::db::OnDeleteAction;
use crate::cluster::protocol::{
    FkCheckRequest, FkCheckResponse, FkReverseLookupRequest, FkReverseLookupResponse,
};
use tokio::sync::oneshot;

const FK_CHECK_TIMEOUT_SECS: u64 = 5;
pub(crate) const MAX_CASCADE_DEPTH: usize = 16;
const MAX_CASCADE_WORK_ITEMS: usize = MAX_CASCADE_DEPTH * 1000;

pub struct FkExistenceResult {
    pub pending_remote: Vec<PendingFkCheck>,
}

#[derive(Debug)]
pub struct FkReverseLookupResult {
    pub constraint_name: String,
    pub source_entity: String,
    pub source_field: String,
    pub on_delete: OnDeleteAction,
    pub referencing_ids: Vec<String>,
    pub cross_owned_ids: Vec<String>,
    pub target_id: String,
}

struct FkLookupParams<'a> {
    constraint_name: &'a str,
    source_entity: &'a str,
    source_field: &'a str,
    on_delete: OnDeleteAction,
    target_id: &'a str,
    target_entity: &'a str,
}

pub(crate) fn extract_fk_string_value(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

pub async fn await_fk_checks(pending: Vec<PendingFkCheck>) -> Result<(), String> {
    for check in pending {
        let deadline =
            tokio::time::Instant::now() + std::time::Duration::from_secs(FK_CHECK_TIMEOUT_SECS);

        match tokio::time::timeout_at(deadline, check.receiver).await {
            Ok(Ok(true)) => {}
            Ok(Ok(false)) => {
                return Err(format!(
                    "FK constraint violation: referenced {} '{}' does not exist",
                    check.target_entity, check.target_id
                ));
            }
            _ => {
                return Err(format!(
                    "FK check timeout for {} '{}'",
                    check.target_entity, check.target_id
                ));
            }
        }
    }
    Ok(())
}

pub async fn await_fk_reverse_lookups(
    pending: Vec<PendingFkReverseLookup>,
) -> Result<Vec<FkReverseLookupResult>, String> {
    let mut results = Vec::new();

    for lookup in pending {
        let deadline =
            tokio::time::Instant::now() + std::time::Duration::from_secs(FK_CHECK_TIMEOUT_SECS);

        match tokio::time::timeout_at(deadline, lookup.receiver).await {
            Ok(Ok(ids)) if !ids.is_empty() => {
                results.push(FkReverseLookupResult {
                    constraint_name: lookup.constraint_name,
                    source_entity: lookup.source_entity,
                    source_field: lookup.source_field,
                    on_delete: lookup.on_delete,
                    referencing_ids: ids,
                    cross_owned_ids: Vec::new(),
                    target_id: lookup.target_id,
                });
            }
            Ok(Ok(_)) => {}
            _ => {
                return Err(format!(
                    "FK reverse lookup timeout for constraint '{}'",
                    lookup.constraint_name
                ));
            }
        }
    }

    Ok(results)
}

async fn resolve_single_level(
    local_results: Vec<FkReverseLookupResult>,
    pending_lookups: Vec<PendingFkReverseLookup>,
) -> (Option<String>, Vec<FkReverseLookupResult>) {
    for r in &local_results {
        if r.on_delete == OnDeleteAction::Restrict && !r.referencing_ids.is_empty() {
            return (
                Some(format!(
                    "FK constraint '{}' prevents deletion: {} referencing record(s) in '{}'",
                    r.constraint_name,
                    r.referencing_ids.len(),
                    r.source_entity
                )),
                Vec::new(),
            );
        }
    }

    let remote_results = match await_fk_reverse_lookups(pending_lookups).await {
        Ok(results) => results,
        Err(msg) => return (Some(msg), Vec::new()),
    };

    for r in &remote_results {
        if r.on_delete == OnDeleteAction::Restrict && !r.referencing_ids.is_empty() {
            return (
                Some(format!(
                    "FK constraint '{}' prevents deletion: {} referencing record(s) in '{}'",
                    r.constraint_name,
                    r.referencing_ids.len(),
                    r.source_entity
                )),
                Vec::new(),
            );
        }
    }

    let mut combined = local_results;
    combined.extend(remote_results);
    (None, combined)
}

fn filter_and_extract_cascade(
    results: Vec<FkReverseLookupResult>,
    visited: &mut std::collections::HashSet<(String, String)>,
) -> (Vec<FkReverseLookupResult>, Vec<(String, String)>) {
    let mut filtered = Vec::new();
    let mut queue = Vec::new();
    for mut r in results {
        if r.on_delete == OnDeleteAction::Cascade {
            r.referencing_ids.retain(|child_id| {
                if visited.insert((r.source_entity.clone(), child_id.clone())) {
                    queue.push((r.source_entity.clone(), child_id.clone()));
                    true
                } else {
                    false
                }
            });
            if r.referencing_ids.is_empty() {
                continue;
            }
        }
        filtered.push(r);
    }
    (filtered, queue)
}

#[allow(clippy::missing_errors_doc)]
pub async fn collect_recursive_cascade<T: ClusterTransport>(
    controller: &tokio::sync::RwLock<NodeController<T>>,
    deleted_entity: &str,
    deleted_id: &str,
    initial_local: Vec<FkReverseLookupResult>,
    initial_pending: Vec<PendingFkReverseLookup>,
    sender: Option<&str>,
) -> (Option<String>, Vec<FkReverseLookupResult>) {
    let (restrict_error, level_results) =
        resolve_single_level(initial_local, initial_pending).await;
    if restrict_error.is_some() {
        return (restrict_error, Vec::new());
    }

    let mut visited = std::collections::HashSet::new();
    visited.insert((deleted_entity.to_string(), deleted_id.to_string()));
    let (filtered, mut queue) = filter_and_extract_cascade(level_results, &mut visited);
    let mut total_work: usize = filtered.iter().map(|r| r.referencing_ids.len()).sum();
    if total_work > MAX_CASCADE_WORK_ITEMS {
        return (
            Some(format!(
                "cascade work limit exceeded ({MAX_CASCADE_WORK_ITEMS} items)"
            )),
            Vec::new(),
        );
    }
    let mut all_results = filtered;

    let mut depth = 0;
    while !queue.is_empty() {
        depth += 1;
        if depth > MAX_CASCADE_DEPTH {
            return (
                Some(format!(
                    "cascade depth limit exceeded ({MAX_CASCADE_DEPTH} levels)"
                )),
                Vec::new(),
            );
        }

        let mut next_local = Vec::new();
        let mut next_pending = Vec::new();

        {
            let mut ctrl = controller.write().await;
            let batch = std::mem::take(&mut queue);
            for (entity, id) in &batch {
                match ctrl.start_fk_reverse_lookup(entity, id, sender).await {
                    Ok((local, pending)) => {
                        next_local.extend(local);
                        next_pending.extend(pending);
                    }
                    Err(msg) => return (Some(msg), Vec::new()),
                }
            }
        }

        if next_local.is_empty() && next_pending.is_empty() {
            break;
        }

        let (restrict_error, level_results) = resolve_single_level(next_local, next_pending).await;
        if restrict_error.is_some() {
            return (restrict_error, Vec::new());
        }

        let (filtered, next_queue) = filter_and_extract_cascade(level_results, &mut visited);
        total_work += filtered
            .iter()
            .map(|r| r.referencing_ids.len())
            .sum::<usize>();
        if total_work > MAX_CASCADE_WORK_ITEMS {
            return (
                Some(format!(
                    "cascade work limit exceeded ({MAX_CASCADE_WORK_ITEMS} items)"
                )),
                Vec::new(),
            );
        }
        queue = next_queue;
        all_results.extend(filtered);
    }

    (None, all_results)
}

impl<T: ClusterTransport> NodeController<T> {
    #[allow(clippy::missing_errors_doc)]
    /// Checks that all FK references in `data` point to existing target records.
    ///
    /// # TOCTOU Trade-off
    ///
    /// FK existence checks are best-effort: between this check and the subsequent
    /// write commit, the referenced target could be concurrently deleted by another
    /// node. This is a deliberate trade-off — distributed two-phase locking across
    /// partitions would serialize all FK-related writes and severely impact throughput.
    ///
    /// The system converges to a consistent state because any subsequent delete of
    /// the target will trigger cascade/set-null actions on all referencing records.
    /// Stale FK references are therefore short-lived.
    ///
    /// Set-null cascades use a field=value CAS guard to prevent overwriting concurrent
    /// updates. This check is not ABA-safe (a field changed A→B→A would match), but
    /// the probability requires three concurrent operations in a precise interleaving.
    pub async fn start_fk_existence_check(
        &mut self,
        entity: &str,
        data: &serde_json::Value,
    ) -> Result<FkExistenceResult, String> {
        let fk_constraints = self.stores.constraint_get_fk_constraints(entity);
        let mut pending_remote: Vec<PendingFkCheck> = Vec::new();

        for constraint in &fk_constraints {
            let Some(fk_value) = data
                .get(constraint.field_str())
                .and_then(extract_fk_string_value)
            else {
                continue;
            };

            let target_entity = constraint.target_entity_str();
            let target_partition = super::super::db::data_partition(target_entity, &fk_value);
            let primary = self.partition_map.primary(target_partition);

            if primary == Some(self.node_id) {
                if self.stores.db_get(target_entity, &fk_value).is_none() {
                    return Err(format!(
                        "FK constraint violation: referenced {target_entity} '{fk_value}' does not exist"
                    ));
                }
            } else if let Some(target_node) = primary {
                let request_id = self.allocate_fk_request_id();
                let (tx, rx) = oneshot::channel();
                self.pending_constraints.insert_fk_check(request_id, tx);

                let req = FkCheckRequest::create(request_id, target_entity, &fk_value);
                let _ = self
                    .transport
                    .send(target_node, ClusterMessage::FkCheckRequest(req))
                    .await;

                pending_remote.push(PendingFkCheck {
                    target_entity: target_entity.to_string(),
                    target_id: fk_value,
                    receiver: rx,
                });
            } else {
                return Err(format!(
                    "FK check failed: no primary for partition {target_partition}"
                ));
            }
        }

        Ok(FkExistenceResult { pending_remote })
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn start_fk_reverse_lookup(
        &mut self,
        entity: &str,
        id: &str,
        sender: Option<&str>,
    ) -> Result<(Vec<FkReverseLookupResult>, Vec<PendingFkReverseLookup>), String> {
        let referencing = self.stores.constraint_find_referencing(entity);
        if referencing.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        let is_blind = sender.is_none() || sender.is_some_and(|s| self.ownership.is_admin(s));

        let mut local_results = Vec::new();
        let mut pending_remote: Vec<PendingFkReverseLookup> = Vec::new();

        for constraint in &referencing {
            let source_entity = constraint.entity_str();
            let source_field = constraint.field_str();
            let on_delete = constraint.on_delete_action();

            let all_refs = self
                .stores
                .fk_reverse_lookup(entity, id, source_entity, source_field);
            let local_refs: Vec<String> = all_refs
                .into_iter()
                .filter(|ref_id| {
                    self.is_primary_for_partition(super::super::db::data_partition(
                        source_entity,
                        ref_id,
                    ))
                })
                .collect();

            if !local_refs.is_empty() && on_delete == OnDeleteAction::Restrict {
                return Err(format!(
                    "FK constraint '{}' prevents deletion: {} referencing record(s) in '{source_entity}'",
                    constraint.name_str(),
                    local_refs.len()
                ));
            }

            if !local_refs.is_empty() {
                let (owned_refs, cross_owned) = if is_blind || on_delete != OnDeleteAction::Cascade
                {
                    (local_refs, Vec::new())
                } else {
                    self.partition_refs_by_ownership(
                        source_entity,
                        local_refs,
                        sender.unwrap_or(""),
                    )
                };

                if !owned_refs.is_empty() || !cross_owned.is_empty() {
                    local_results.push(FkReverseLookupResult {
                        constraint_name: constraint.name_str().to_string(),
                        source_entity: source_entity.to_string(),
                        source_field: source_field.to_string(),
                        on_delete,
                        referencing_ids: owned_refs,
                        cross_owned_ids: cross_owned,
                        target_id: id.to_string(),
                    });
                }
            }

            let fk_params = FkLookupParams {
                constraint_name: constraint.name_str(),
                source_entity,
                source_field,
                on_delete,
                target_id: id,
                target_entity: entity,
            };
            self.scatter_reverse_lookup_to_remote_nodes(&mut pending_remote, &fk_params)
                .await;
        }

        Ok((local_results, pending_remote))
    }

    fn partition_refs_by_ownership(
        &self,
        source_entity: &str,
        refs: Vec<String>,
        sender: &str,
    ) -> (Vec<String>, Vec<String>) {
        let Some(owner_field) = self.ownership.owner_field(source_entity) else {
            return (refs, Vec::new());
        };

        let mut owned = Vec::new();
        let mut cross_owned = Vec::new();
        for ref_id in refs {
            let is_owned = self
                .stores
                .db_get(source_entity, &ref_id)
                .and_then(|e| serde_json::from_slice::<serde_json::Value>(&e.data).ok())
                .and_then(|data| {
                    data.get(owner_field)
                        .and_then(|v| v.as_str())
                        .map(String::from)
                })
                .is_none_or(|owner| owner == sender);

            if is_owned {
                owned.push(ref_id);
            } else {
                cross_owned.push(ref_id);
            }
        }
        (owned, cross_owned)
    }

    async fn scatter_reverse_lookup_to_remote_nodes(
        &mut self,
        pending_remote: &mut Vec<PendingFkReverseLookup>,
        params: &FkLookupParams<'_>,
    ) {
        let constraint_name = params.constraint_name;
        let source_entity = params.source_entity;
        let source_field = params.source_field;
        let on_delete = params.on_delete;
        let target_id = params.target_id;
        let target_entity = params.target_entity;
        let alive_nodes = self.heartbeat.alive_nodes();
        for &node in &alive_nodes {
            if node == self.node_id {
                continue;
            }
            if self.partition_map.primary_count(node) == 0 {
                continue;
            }
            let request_id = self.allocate_fk_request_id();
            let (tx, rx) = oneshot::channel();
            self.pending_constraints.insert_fk_lookup(request_id, tx);

            let req = FkReverseLookupRequest::create(
                request_id,
                source_entity,
                source_field,
                target_id,
                target_entity,
            );
            let _ = self
                .transport
                .send(node, ClusterMessage::FkReverseLookupRequest(req))
                .await;

            pending_remote.push(PendingFkReverseLookup {
                constraint_name: constraint_name.to_string(),
                source_entity: source_entity.to_string(),
                source_field: source_field.to_string(),
                on_delete,
                target_id: target_id.to_string(),
                receiver: rx,
            });
        }
    }

    pub(crate) async fn handle_fk_check_request(&mut self, from: NodeId, req: &FkCheckRequest) {
        let exists = self.stores.db_get(req.entity_str(), req.id_str()).is_some();
        let response = FkCheckResponse::create(req.request_id, exists);
        let _ = self
            .transport
            .send(from, ClusterMessage::FkCheckResponse(response))
            .await;
    }

    pub(crate) async fn handle_fk_reverse_lookup_request(
        &mut self,
        from: NodeId,
        req: &FkReverseLookupRequest,
    ) {
        let all_refs = self.stores.fk_reverse_lookup(
            req.target_entity_str(),
            req.target_id_str(),
            req.source_entity_str(),
            req.source_field_str(),
        );
        let referencing_ids: Vec<String> = all_refs
            .into_iter()
            .filter(|ref_id| {
                self.is_primary_for_partition(super::super::db::data_partition(
                    req.source_entity_str(),
                    ref_id,
                ))
            })
            .collect();

        let response = FkReverseLookupResponse::create(req.request_id, &referencing_ids);
        let _ = self
            .transport
            .send(from, ClusterMessage::FkReverseLookupResponse(response))
            .await;
    }

    #[allow(clippy::missing_errors_doc)]
    pub(crate) fn collect_local_cascade(
        &self,
        deleted_entity: &str,
        deleted_id: &str,
        initial: Vec<FkReverseLookupResult>,
    ) -> Result<Vec<FkReverseLookupResult>, String> {
        let mut all_results = Vec::new();
        let mut visited = std::collections::HashSet::new();
        visited.insert((deleted_entity.to_string(), deleted_id.to_string()));
        let mut queue: Vec<(String, String)> = Vec::new();

        for r in &initial {
            if r.on_delete == OnDeleteAction::Cascade {
                for child_id in &r.referencing_ids {
                    visited.insert((r.source_entity.clone(), child_id.clone()));
                    queue.push((r.source_entity.clone(), child_id.clone()));
                }
            }
        }
        all_results.extend(initial);

        let mut depth = 0;
        while !queue.is_empty() {
            depth += 1;
            if depth > MAX_CASCADE_DEPTH {
                return Err(format!(
                    "cascade depth limit exceeded ({MAX_CASCADE_DEPTH} levels)"
                ));
            }
            let current_level = std::mem::take(&mut queue);
            for (entity, id) in current_level {
                if visited.len() > MAX_CASCADE_WORK_ITEMS {
                    return Err("cascade work limit exceeded".to_string());
                }
                let referencing = self.stores.constraint_find_referencing(&entity);
                for constraint in &referencing {
                    let source_entity = constraint.entity_str();
                    let source_field = constraint.field_str();
                    let on_delete = constraint.on_delete_action();

                    let all_refs =
                        self.stores
                            .fk_reverse_lookup(&entity, &id, source_entity, source_field);
                    let local_refs: Vec<String> = all_refs
                        .into_iter()
                        .filter(|ref_id| {
                            self.is_primary_for_partition(super::super::db::data_partition(
                                source_entity,
                                ref_id,
                            ))
                        })
                        .collect();

                    if !local_refs.is_empty() && on_delete == OnDeleteAction::Restrict {
                        return Err(format!(
                            "FK constraint '{}' prevents deletion: {} referencing record(s) in '{source_entity}'",
                            constraint.name_str(),
                            local_refs.len()
                        ));
                    }

                    if local_refs.is_empty() {
                        continue;
                    }

                    let filtered_refs: Vec<String> = if on_delete == OnDeleteAction::Cascade {
                        local_refs
                            .into_iter()
                            .filter(|child_id| {
                                if visited.insert((source_entity.to_string(), child_id.clone())) {
                                    queue.push((source_entity.to_string(), child_id.clone()));
                                    true
                                } else {
                                    false
                                }
                            })
                            .collect()
                    } else {
                        local_refs
                    };

                    if filtered_refs.is_empty() {
                        continue;
                    }

                    all_results.push(FkReverseLookupResult {
                        constraint_name: constraint.name_str().to_string(),
                        source_entity: source_entity.to_string(),
                        source_field: source_field.to_string(),
                        on_delete,
                        referencing_ids: filtered_refs,
                        cross_owned_ids: Vec::new(),
                        target_id: id.clone(),
                    });
                }
            }
        }

        Ok(all_results)
    }

    fn allocate_fk_request_id(&self) -> u64 {
        self.pending_constraints.allocate_fk_id()
    }
}
