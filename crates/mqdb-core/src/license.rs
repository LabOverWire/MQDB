// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

#[derive(Clone, Debug)]
pub struct LicenseInfo {
    pub customer: String,
    pub tier: LicenseTier,
    pub features: LicenseFeatures,
    pub expires_at: u64,
    pub trial: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LicenseTier {
    Pro,
    Enterprise,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct LicenseFeatures {
    pub vault: bool,
    pub cluster: bool,
}

impl LicenseInfo {
    #[must_use]
    pub fn check_runtime_expiry(expires_at: u64) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(u64::MAX, |d| d.as_secs());
        now > expires_at
    }

    #[must_use]
    pub fn is_expired(&self) -> bool {
        Self::check_runtime_expiry(self.expires_at)
    }

    #[must_use]
    pub fn days_remaining(&self) -> i64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        if now == 0 || now > self.expires_at {
            return 0;
        }
        i64::try_from((self.expires_at - now) / 86400).unwrap_or(i64::MAX)
    }
}

impl std::fmt::Display for LicenseTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pro => write!(f, "pro"),
            Self::Enterprise => write!(f, "enterprise"),
        }
    }
}

impl std::fmt::Display for LicenseFeatures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.vault, self.cluster) {
            (true, true) => write!(f, "vault, cluster"),
            (true, false) => write!(f, "vault"),
            (false, true) => write!(f, "cluster"),
            (false, false) => write!(f, "none"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn features_display_all_combinations() {
        let both = LicenseFeatures {
            vault: true,
            cluster: true,
        };
        assert_eq!(both.to_string(), "vault, cluster");

        let vault_only = LicenseFeatures {
            vault: true,
            cluster: false,
        };
        assert_eq!(vault_only.to_string(), "vault");

        let cluster_only = LicenseFeatures {
            vault: false,
            cluster: true,
        };
        assert_eq!(cluster_only.to_string(), "cluster");

        let none = LicenseFeatures {
            vault: false,
            cluster: false,
        };
        assert_eq!(none.to_string(), "none");
    }

    #[test]
    fn check_runtime_expiry_past_returns_true() {
        let past = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 3600;
        assert!(LicenseInfo::check_runtime_expiry(past));
    }

    #[test]
    fn check_runtime_expiry_future_returns_false() {
        let future = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        assert!(!LicenseInfo::check_runtime_expiry(future));
    }

    #[test]
    fn check_runtime_expiry_exact_now_returns_false() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!(!LicenseInfo::check_runtime_expiry(now));
    }

    #[test]
    fn is_expired_reflects_runtime_check() {
        let info_expired = LicenseInfo {
            customer: "x".into(),
            tier: LicenseTier::Pro,
            features: LicenseFeatures::default(),
            expires_at: 0,
            trial: false,
        };
        assert!(info_expired.is_expired());

        let future = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 86400;
        let info_valid = LicenseInfo {
            customer: "x".into(),
            tier: LicenseTier::Pro,
            features: LicenseFeatures::default(),
            expires_at: future,
            trial: false,
        };
        assert!(!info_valid.is_expired());
    }
}
