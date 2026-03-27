// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

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
}
