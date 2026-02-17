// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::StoreManager;
use crate::cluster::db::ClusterSchema;

impl StoreManager {
    #[must_use]
    pub fn schema_get(&self, entity: &str) -> Option<ClusterSchema> {
        self.db_schema.get(entity)
    }

    #[must_use]
    pub fn schema_list(&self) -> Vec<ClusterSchema> {
        self.db_schema.list()
    }

    #[must_use]
    pub fn schema_is_valid_for_write(&self, entity: &str) -> bool {
        self.db_schema.is_valid_for_write(entity)
    }
}
