// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{Database, SubscriptionResult};
use crate::consumer_group::{ConsumerGroupDetails, ConsumerGroupInfo, ConsumerMemberInfo};
use mqdb_core::error::{Error, Result};
use mqdb_core::subscription::{Subscription, SubscriptionMode};

impl Database {
    /// # Errors
    /// Returns an error if the subscription limit is reached or registration fails.
    pub async fn subscribe(&self, pattern: String, entity: Option<String>) -> Result<String> {
        if let Some(max_subs) = self.config.max_subscriptions {
            let current_count = self.registry.count().await;
            if current_count >= max_subs {
                return Err(Error::Internal(format!(
                    "maximum subscription limit reached: {current_count}/{max_subs}"
                )));
            }
        }

        let sub_id = uuid::Uuid::new_v4().to_string();
        let subscription = Subscription::new(sub_id.clone(), pattern, entity);

        self.registry.register(subscription).await?;

        Ok(sub_id)
    }

    /// # Errors
    /// Returns an error if the subscription limit is reached or mode conflicts.
    pub async fn subscribe_shared(
        &self,
        pattern: String,
        entity: Option<String>,
        group: String,
        mode: SubscriptionMode,
    ) -> Result<SubscriptionResult> {
        if let Some(max_subs) = self.config.max_subscriptions {
            let current_count = self.registry.count().await;
            if current_count >= max_subs {
                return Err(Error::Internal(format!(
                    "maximum subscription limit reached: {current_count}/{max_subs}"
                )));
            }
        }

        {
            let groups = self.consumer_groups.read().await;
            if let Some(existing_group) = groups.get(&group) {
                for member in existing_group.members() {
                    if let Some(existing_sub) = self.registry.get(&member.consumer_id).await {
                        if existing_sub.mode != mode {
                            return Err(Error::Validation(format!(
                                "share group '{}' already uses {:?} mode, cannot mix with {:?}",
                                group, existing_sub.mode, mode
                            )));
                        }
                        break;
                    }
                }
            }
        }

        let sub_id = uuid::Uuid::new_v4().to_string();
        let consumer_id = sub_id.clone();

        let assigned_partitions = {
            let mut groups = self.consumer_groups.write().await;
            let num_partitions = self.config.shared_subscription.num_partitions;

            let consumer_group = groups.entry(group.clone()).or_insert_with(|| {
                crate::consumer_group::ConsumerGroup::new(group.clone(), num_partitions)
            });

            let partitions = consumer_group.add_member(&consumer_id);

            if mode == SubscriptionMode::Ordered {
                Some(partitions)
            } else {
                None
            }
        };

        let subscription =
            Subscription::new(sub_id.clone(), pattern, entity).with_share_group(group, mode);

        self.registry.register(subscription).await?;

        Ok(SubscriptionResult {
            id: sub_id,
            assigned_partitions,
        })
    }

    /// # Errors
    /// Returns an error if unregistration fails.
    pub async fn unsubscribe(&self, sub_id: &str) -> Result<()> {
        if let Some(sub) = self.registry.get(sub_id).await
            && let Some(group) = &sub.share_group
        {
            let mut groups = self.consumer_groups.write().await;
            if let Some(cg) = groups.get_mut(group) {
                cg.remove_member(sub_id);
                if cg.member_count() == 0 {
                    groups.remove(group);
                }
            }
        }

        self.registry.unregister(sub_id).await?;
        self.dispatcher.remove_listener(sub_id).await;
        Ok(())
    }

    /// # Errors
    /// Returns an error if the subscription is not found.
    pub async fn heartbeat(&self, sub_id: &str) -> Result<()> {
        let sub = self
            .registry
            .get(sub_id)
            .await
            .ok_or_else(|| Error::NotFound {
                entity: "subscription".into(),
                id: sub_id.into(),
            })?;

        if let Some(group) = &sub.share_group {
            let mut groups = self.consumer_groups.write().await;
            if let Some(cg) = groups.get_mut(group) {
                cg.update_heartbeat(sub_id);
            }
        }
        Ok(())
    }

    pub async fn get_subscription_info(&self, sub_id: &str) -> Option<Subscription> {
        self.registry.get(sub_id).await
    }

    pub async fn list_consumer_groups(&self) -> Vec<ConsumerGroupInfo> {
        let groups = self.consumer_groups.read().await;
        groups
            .iter()
            .map(|(name, cg)| ConsumerGroupInfo {
                name: name.clone(),
                member_count: cg.member_count(),
                total_partitions: self.config.shared_subscription.num_partitions,
            })
            .collect()
    }

    pub async fn get_consumer_group(&self, name: &str) -> Option<ConsumerGroupDetails> {
        let groups = self.consumer_groups.read().await;
        groups.get(name).map(|cg| ConsumerGroupDetails {
            name: name.to_string(),
            members: cg.members().map(ConsumerMemberInfo::from).collect(),
            total_partitions: self.config.shared_subscription.num_partitions,
        })
    }
}
