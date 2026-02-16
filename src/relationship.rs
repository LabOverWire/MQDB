// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Relationship {
    pub source_entity: String,
    pub field: String,
    pub target_entity: String,
    pub field_suffix: String,
}

impl Relationship {
    #[allow(clippy::must_use_candidate)]
    pub fn new(source_entity: String, field: String, target_entity: String) -> Self {
        let field_suffix = if field.ends_with("_id") {
            field.clone()
        } else {
            format!("{field}_id")
        };

        Self {
            source_entity,
            field,
            target_entity,
            field_suffix,
        }
    }
}

#[derive(Default)]
pub struct RelationshipRegistry {
    relationships: HashMap<String, Vec<Relationship>>,
}

impl RelationshipRegistry {
    #[allow(clippy::must_use_candidate)]
    pub fn new() -> Self {
        Self {
            relationships: HashMap::new(),
        }
    }

    pub fn add(&mut self, relationship: Relationship) {
        let key = relationship.source_entity.clone();
        self.relationships
            .entry(key)
            .or_default()
            .push(relationship);
    }

    #[allow(clippy::must_use_candidate)]
    pub fn get(&self, entity: &str, field: &str) -> Option<&Relationship> {
        self.relationships
            .get(entity)?
            .iter()
            .find(|r| r.field == field)
    }

    #[allow(clippy::must_use_candidate)]
    pub fn get_all(&self, entity: &str) -> Option<&Vec<Relationship>> {
        self.relationships.get(entity)
    }
}
