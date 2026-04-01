// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::{Algorithm, Argon2, Params, Version};
use ring::digest;
use ring::rand::{SecureRandom, SystemRandom};

use super::identity_crypto::IdentityCrypto;

#[derive(Debug)]
pub enum CredentialError {
    HashingFailed(String),
    VerificationFailed(String),
}

impl std::fmt::Display for CredentialError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HashingFailed(e) => write!(f, "password hashing failed: {e}"),
            Self::VerificationFailed(e) => write!(f, "password verification failed: {e}"),
        }
    }
}

impl std::error::Error for CredentialError {}

const MIN_PASSWORD_LENGTH: usize = 8;
const ARGON2_M_COST: u32 = 19456;
const ARGON2_T_COST: u32 = 2;
const ARGON2_P_COST: u32 = 1;

/// # Errors
/// Returns `CredentialError::HashingFailed` if Argon2id hashing fails.
pub fn hash_password(password: &str) -> Result<String, CredentialError> {
    let rng = SystemRandom::new();
    let mut salt_bytes = [0u8; 16];
    rng.fill(&mut salt_bytes)
        .map_err(|_| CredentialError::HashingFailed("salt generation failed".into()))?;
    let salt = SaltString::encode_b64(&salt_bytes)
        .map_err(|e| CredentialError::HashingFailed(e.to_string()))?;
    let params = Params::new(ARGON2_M_COST, ARGON2_T_COST, ARGON2_P_COST, None)
        .map_err(|e| CredentialError::HashingFailed(e.to_string()))?;
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let hash = argon2
        .hash_password(password.as_bytes(), &salt)
        .map_err(|e| CredentialError::HashingFailed(e.to_string()))?;
    Ok(hash.to_string())
}

/// # Errors
/// Returns `CredentialError::VerificationFailed` if the hash is malformed or verification fails.
pub fn verify_password(hash: &str, password: &str) -> Result<bool, CredentialError> {
    let parsed =
        PasswordHash::new(hash).map_err(|e| CredentialError::VerificationFailed(e.to_string()))?;
    Ok(Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok())
}

#[must_use]
pub fn compute_email_hash(crypto: Option<&IdentityCrypto>, email: &str) -> String {
    let lower = email.to_lowercase();
    if let Some(c) = crypto {
        c.blind_index("_credentials", &lower)
    } else {
        let d = digest::digest(&digest::SHA256, lower.as_bytes());
        hex_encode(d.as_ref())
    }
}

#[must_use]
pub fn validate_email(email: &str) -> bool {
    let Some((local, domain)) = email.split_once('@') else {
        return false;
    };
    !local.is_empty() && domain.contains('.') && domain.len() > 2
}

/// # Errors
/// Returns a static error message if the password is too short.
pub fn validate_password(password: &str) -> Result<(), &'static str> {
    if password.len() < MIN_PASSWORD_LENGTH {
        return Err("password must be at least 8 characters");
    }
    Ok(())
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(char::from(b"0123456789abcdef"[(b >> 4) as usize]));
        s.push(char::from(b"0123456789abcdef"[(b & 0x0F) as usize]));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_and_verify_roundtrip() {
        let hash = hash_password("test_password_123").unwrap();
        assert!(hash.starts_with("$argon2id$"));
        assert!(verify_password(&hash, "test_password_123").unwrap());
        assert!(!verify_password(&hash, "wrong_password").unwrap());
    }

    #[test]
    fn validate_email_cases() {
        assert!(validate_email("user@example.com"));
        assert!(validate_email("a@b.co"));
        assert!(!validate_email("no-at-sign"));
        assert!(!validate_email("@example.com"));
        assert!(!validate_email("user@"));
        assert!(!validate_email("user@x"));
        assert!(!validate_email(""));
    }

    #[test]
    fn validate_password_length() {
        assert!(validate_password("12345678").is_ok());
        assert!(validate_password("1234567").is_err());
        assert!(validate_password("").is_err());
    }

    #[test]
    fn compute_email_hash_without_crypto() {
        let h1 = compute_email_hash(None, "user@example.com");
        let h2 = compute_email_hash(None, "user@example.com");
        assert_eq!(h1, h2);
        let h3 = compute_email_hash(None, "USER@EXAMPLE.COM");
        assert_eq!(h1, h3);
        let h4 = compute_email_hash(None, "other@example.com");
        assert_ne!(h1, h4);
    }

    #[test]
    fn compute_email_hash_with_crypto() {
        let (crypto, _) = IdentityCrypto::generate().unwrap();
        let h1 = compute_email_hash(Some(&crypto), "user@example.com");
        let h2 = compute_email_hash(Some(&crypto), "user@example.com");
        assert_eq!(h1, h2);
        let h3 = compute_email_hash(Some(&crypto), "USER@EXAMPLE.COM");
        assert_eq!(h1, h3);
    }
}
