// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::HashMap;
use std::time::Instant;

const TTL_SECS: u64 = 300;

pub struct PkceCache {
    entries: HashMap<String, (String, String, Instant)>,
}

impl PkceCache {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn insert(&mut self, state: String, code_verifier: String, provider: String) {
        self.cleanup();
        self.entries
            .insert(state, (code_verifier, provider, Instant::now()));
    }

    pub fn take(&mut self, state: &str) -> Option<(String, String)> {
        self.cleanup();
        self.entries
            .remove(state)
            .map(|(verifier, provider, _)| (verifier, provider))
    }

    fn cleanup(&mut self) {
        let cutoff = Instant::now()
            .checked_sub(std::time::Duration::from_secs(TTL_SECS))
            .unwrap();
        self.entries.retain(|_, (_, _, created)| *created > cutoff);
    }
}
