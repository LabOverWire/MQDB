// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::events::ChangeEvent;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionMode {
    #[default]
    Broadcast,
    LoadBalanced,
    Ordered,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    pub id: String,
    pub pattern: String,
    pub entity: Option<String>,
    #[serde(default)]
    pub share_group: Option<String>,
    #[serde(default)]
    pub mode: SubscriptionMode,
}

impl Subscription {
    #[allow(clippy::must_use_candidate)]
    pub fn new(id: String, pattern: String, entity: Option<String>) -> Self {
        Self {
            id,
            pattern,
            entity,
            share_group: None,
            mode: SubscriptionMode::default(),
        }
    }

    #[must_use]
    pub fn with_share_group(mut self, group: String, mode: SubscriptionMode) -> Self {
        self.share_group = Some(group);
        self.mode = mode;
        self
    }

    #[must_use]
    pub fn matches(&self, event: &ChangeEvent) -> bool {
        if let Some(ref entity) = self.entity
            && entity != &event.entity
        {
            return false;
        }

        match_pattern(&self.pattern, &event.entity, &event.id)
    }
}

#[must_use]
pub fn match_pattern(pattern: &str, entity: &str, id: &str) -> bool {
    let path = format!("{entity}/{id}");
    match_wildcard(pattern, &path)
}

#[must_use]
pub fn match_wildcard(pattern: &str, path: &str) -> bool {
    let pattern_parts: Vec<&str> = pattern.split('/').collect();
    let path_parts: Vec<&str> = path.split('/').collect();

    match_parts(&pattern_parts, &path_parts, 0, 0)
}

fn match_parts(pattern: &[&str], path: &[&str], p_idx: usize, path_idx: usize) -> bool {
    if p_idx >= pattern.len() {
        return path_idx >= path.len();
    }

    let current = pattern[p_idx];

    if current == "#" {
        if p_idx == pattern.len() - 1 {
            return true;
        }
        for i in path_idx..=path.len() {
            if match_parts(pattern, path, p_idx + 1, i) {
                return true;
            }
        }
        return false;
    }

    if path_idx >= path.len() {
        return false;
    }

    if current == "+" || current == path[path_idx] {
        return match_parts(pattern, path, p_idx + 1, path_idx + 1);
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wildcard_matching() {
        assert!(match_wildcard("users/+", "users/123"));
        assert!(match_wildcard("users/#", "users/123"));
        assert!(match_wildcard("users/#", "users/123/profile"));
        assert!(match_wildcard("+/123", "users/123"));
        assert!(!match_wildcard("users/+", "users/123/profile"));
        assert!(!match_wildcard("posts/+", "users/123"));
    }

    #[test]
    fn test_subscription_matches() {
        let sub = Subscription::new("sub1".into(), "users/+".into(), Some("users".into()));

        let event = ChangeEvent::create(
            "users".into(),
            "123".into(),
            serde_json::json!({"name": "test"}),
        );

        assert!(sub.matches(&event));
    }
}
