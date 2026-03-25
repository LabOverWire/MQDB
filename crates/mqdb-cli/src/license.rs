// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use mqdb_core::license::{LicenseFeature, LicenseInfo, LicenseTier};
use ring::digest;
use ring::signature;
use std::path::Path;

const OBFUSCATION_MASK: [u8; 65] = [
    0x24, 0x93, 0x65, 0x18, 0x62, 0x33, 0xa1, 0x56, 0xd3, 0x93, 0xfb, 0x87, 0x62, 0x9b, 0x68, 0x0b,
    0xa4, 0x0b, 0xac, 0x10, 0x32, 0xf8, 0x1d, 0x85, 0xc5, 0x17, 0x41, 0x4a, 0xaa, 0x63, 0xa7, 0xcc,
    0x94, 0xca, 0x63, 0x1a, 0x89, 0x51, 0x7a, 0xaa, 0x1b, 0x1a, 0xbd, 0xe1, 0xc7, 0xd3, 0x68, 0xfa,
    0x98, 0x22, 0x15, 0x96, 0xdc, 0x9b, 0xd5, 0x89, 0x86, 0xa7, 0x97, 0xae, 0xf8, 0xe1, 0x6d, 0x0c,
    0xa4,
];

const WRAPPED_PUBLIC_KEY: [u8; 65] = [
    0x20, 0x6c, 0xb9, 0x8b, 0xbd, 0xe5, 0xc9, 0x03, 0x8c, 0x69, 0x2d, 0x33, 0xde, 0x2f, 0xcf, 0x05,
    0x6b, 0x46, 0xe2, 0x09, 0x62, 0x3f, 0x48, 0x4e, 0x2c, 0x0a, 0xce, 0x72, 0x9d, 0x22, 0xaa, 0x37,
    0xd5, 0x3d, 0xe7, 0xfe, 0xa2, 0x7e, 0x48, 0x78, 0xd1, 0x6b, 0xc7, 0xe4, 0x78, 0x35, 0x62, 0x12,
    0x7a, 0x2e, 0xb0, 0xb6, 0xf9, 0x0b, 0x86, 0x9d, 0x62, 0x65, 0xc2, 0xe1, 0x3c, 0x28, 0x8b, 0x26,
    0x01,
];

const KEY_INTEGRITY_DIGEST: [u8; 32] = [
    0x3e, 0xb7, 0x9d, 0x91, 0x98, 0x00, 0x5b, 0xb4, 0xc0, 0xb3, 0x27, 0x63, 0xd2, 0xab, 0x5a, 0x1d,
    0x8c, 0xab, 0xeb, 0x4a, 0x18, 0x57, 0x4e, 0xaf, 0x68, 0x75, 0x36, 0xfa, 0xa8, 0xb5, 0x45, 0x50,
];

fn unwrap_licensing_key() -> Result<[u8; 65], String> {
    let mut key = [0u8; 65];
    for i in 0..65 {
        key[i] = WRAPPED_PUBLIC_KEY[i] ^ OBFUSCATION_MASK[i];
    }

    let mut ctx = digest::Context::new(&digest::SHA256);
    ctx.update(&OBFUSCATION_MASK);
    ctx.update(&key);
    let computed = ctx.finish();

    if computed.as_ref() != KEY_INTEGRITY_DIGEST {
        return Err(
            "license verification key integrity check failed — binary may be tampered".into(),
        );
    }

    Ok(key)
}

pub(crate) fn verify_license_file(path: &Path) -> Result<LicenseInfo, String> {
    let token = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read license file '{}': {e}", path.display()))?;
    let key = unwrap_licensing_key()?;
    verify_license_token_with_key(token.trim(), &key)
}

pub(crate) fn verify_license_token(token: &str) -> Result<LicenseInfo, String> {
    let key = unwrap_licensing_key()?;
    verify_license_token_with_key(token, &key)
}

fn verify_license_token_with_key(
    token: &str,
    public_key_bytes: &[u8],
) -> Result<LicenseInfo, String> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err("invalid license format: expected header.payload.signature".into());
    }

    let header_b64 = parts[0];
    let payload_b64 = parts[1];
    let sig_b64 = parts[2];

    let header_bytes = URL_SAFE_NO_PAD
        .decode(header_b64)
        .map_err(|e| format!("invalid header encoding: {e}"))?;
    let header: serde_json::Value =
        serde_json::from_slice(&header_bytes).map_err(|e| format!("invalid header JSON: {e}"))?;

    let alg = header
        .get("alg")
        .and_then(serde_json::Value::as_str)
        .ok_or("missing 'alg' in header")?;
    if alg != "ES256" {
        return Err(format!("unsupported algorithm: {alg}"));
    }

    let sig_bytes = URL_SAFE_NO_PAD
        .decode(sig_b64)
        .map_err(|e| format!("invalid signature encoding: {e}"))?;

    let signed_data = format!("{header_b64}.{payload_b64}");
    let public_key =
        signature::UnparsedPublicKey::new(&signature::ECDSA_P256_SHA256_ASN1, public_key_bytes);
    public_key
        .verify(signed_data.as_bytes(), &sig_bytes)
        .map_err(|_| "invalid license signature")?;

    let payload_bytes = URL_SAFE_NO_PAD
        .decode(payload_b64)
        .map_err(|e| format!("invalid payload encoding: {e}"))?;
    let payload: serde_json::Value =
        serde_json::from_slice(&payload_bytes).map_err(|e| format!("invalid payload JSON: {e}"))?;

    parse_license_payload(&payload)
}

fn parse_license_payload(payload: &serde_json::Value) -> Result<LicenseInfo, String> {
    let customer = payload
        .get("sub")
        .and_then(serde_json::Value::as_str)
        .ok_or("missing 'sub' in payload")?
        .to_string();

    let tier_str = payload
        .get("tier")
        .and_then(serde_json::Value::as_str)
        .ok_or("missing 'tier' in payload")?;
    let tier = match tier_str {
        "pro" => LicenseTier::Pro,
        "enterprise" => LicenseTier::Enterprise,
        other => return Err(format!("unknown tier: {other}")),
    };

    let expires_at = payload
        .get("exp")
        .and_then(serde_json::Value::as_u64)
        .ok_or("missing 'exp' in payload")?;

    let trial = payload
        .get("trial")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);

    let features = payload
        .get("features")
        .and_then(serde_json::Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|v| match v.as_str()? {
                    "vault" => Some(LicenseFeature::Vault),
                    "cluster" => Some(LicenseFeature::Cluster),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default();

    let info = LicenseInfo {
        customer,
        tier,
        features,
        expires_at,
        trial,
    };

    if info.is_expired() {
        return Err(format!(
            "license expired for '{}' — expired at {}",
            info.customer, info.expires_at
        ));
    }

    Ok(info)
}

pub(crate) fn enforce_license(
    license: Option<&LicenseInfo>,
    needs_vault: bool,
    needs_cluster: bool,
) -> Result<(), String> {
    if needs_vault {
        let Some(lic) = license else {
            return Err("vault encryption requires a Pro or Enterprise license (--license)".into());
        };
        if !lic.has_feature(LicenseFeature::Vault) {
            return Err("your license does not include vault encryption".into());
        }
    }
    if needs_cluster {
        let Some(lic) = license else {
            return Err("clustering requires an Enterprise license (--license)".into());
        };
        if !lic.has_feature(LicenseFeature::Cluster) {
            return Err("your license does not include clustering".into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::rand::SystemRandom;
    use ring::signature::{ECDSA_P256_SHA256_ASN1_SIGNING, EcdsaKeyPair, KeyPair};

    fn test_keypair() -> (EcdsaKeyPair, Vec<u8>) {
        let rng = SystemRandom::new();
        let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, &rng)
            .expect("keygen failed");
        let key_pair =
            EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, pkcs8.as_ref(), &rng)
                .expect("parse pkcs8 failed");
        let public_key = key_pair.public_key().as_ref().to_vec();
        (key_pair, public_key)
    }

    fn sign_token(key_pair: &EcdsaKeyPair, payload: &serde_json::Value) -> String {
        let rng = SystemRandom::new();
        let header = serde_json::json!({"alg": "ES256", "typ": "LIC"});
        let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
        let payload_b64 = URL_SAFE_NO_PAD.encode(payload.to_string().as_bytes());
        let signing_input = format!("{header_b64}.{payload_b64}");
        let sig = key_pair
            .sign(&rng, signing_input.as_bytes())
            .expect("signing failed");
        let sig_b64 = URL_SAFE_NO_PAD.encode(sig.as_ref());
        format!("{signing_input}.{sig_b64}")
    }

    fn valid_payload() -> serde_json::Value {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        serde_json::json!({
            "sub": "test@example.com",
            "iss": "laboverwire",
            "iat": now,
            "exp": now + 86400,
            "tier": "enterprise",
            "features": ["vault", "cluster"],
            "trial": false,
        })
    }

    #[test]
    fn roundtrip_sign_and_verify() {
        let (key_pair, public_key) = test_keypair();
        let token = sign_token(&key_pair, &valid_payload());
        let info =
            verify_license_token_with_key(&token, &public_key).expect("verify should succeed");
        assert_eq!(info.customer, "test@example.com");
        assert_eq!(info.tier, LicenseTier::Enterprise);
        assert!(info.has_feature(LicenseFeature::Vault));
        assert!(info.has_feature(LicenseFeature::Cluster));
        assert!(!info.trial);
        assert!(!info.is_expired());
    }

    #[test]
    fn expired_license_rejected() {
        let (key_pair, public_key) = test_keypair();
        let payload = serde_json::json!({
            "sub": "expired@example.com",
            "iss": "laboverwire",
            "iat": 1_000_000,
            "exp": 1_000_000,
            "tier": "pro",
            "features": ["vault"],
            "trial": false,
        });
        let token = sign_token(&key_pair, &payload);
        let result = verify_license_token_with_key(&token, &public_key);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("expired"));
    }

    #[test]
    fn invalid_signature_rejected() {
        let (key_pair, public_key) = test_keypair();
        let mut token = sign_token(&key_pair, &valid_payload());
        let last = token.pop().unwrap();
        let replacement = if last == 'A' { 'B' } else { 'A' };
        token.push(replacement);
        let result = verify_license_token_with_key(&token, &public_key);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("signature"));
    }

    #[test]
    fn wrong_key_rejected() {
        let (key_pair, _) = test_keypair();
        let (_, other_public_key) = test_keypair();
        let token = sign_token(&key_pair, &valid_payload());
        let result = verify_license_token_with_key(&token, &other_public_key);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("signature"));
    }

    #[test]
    fn wrong_algorithm_rejected() {
        let (key_pair, public_key) = test_keypair();
        let rng = SystemRandom::new();
        let header = serde_json::json!({"alg": "EdDSA", "typ": "LIC"});
        let payload_b64 = URL_SAFE_NO_PAD.encode(valid_payload().to_string().as_bytes());
        let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
        let signing_input = format!("{header_b64}.{payload_b64}");
        let sig = key_pair
            .sign(&rng, signing_input.as_bytes())
            .expect("signing failed");
        let sig_b64 = URL_SAFE_NO_PAD.encode(sig.as_ref());
        let token = format!("{signing_input}.{sig_b64}");
        let result = verify_license_token_with_key(&token, &public_key);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unsupported algorithm"));
    }

    #[test]
    fn unwrap_key_integrity_check() {
        let key = unwrap_licensing_key().expect("unwrap should succeed");
        assert_eq!(key[0], 0x04);
        assert_eq!(key.len(), 65);
    }

    #[test]
    fn production_key_verifies_format() {
        let key = unwrap_licensing_key().expect("unwrap should succeed");
        let public_key =
            signature::UnparsedPublicKey::new(&signature::ECDSA_P256_SHA256_ASN1, &key);
        let result = public_key.verify(b"test", b"not-a-real-sig");
        assert!(result.is_err());
    }

    #[test]
    fn enforce_free_tier_allows_no_vault_no_cluster() {
        assert!(enforce_license(None, false, false).is_ok());
    }

    #[test]
    fn enforce_vault_without_license_fails() {
        let result = enforce_license(None, true, false);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("vault"));
    }

    #[test]
    fn enforce_cluster_without_license_fails() {
        let result = enforce_license(None, false, true);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("cluster"));
    }

    #[test]
    fn enforce_vault_with_pro_license() {
        let info = LicenseInfo {
            customer: "test@example.com".into(),
            tier: LicenseTier::Pro,
            features: vec![LicenseFeature::Vault],
            expires_at: u64::MAX,
            trial: false,
        };
        assert!(enforce_license(Some(&info), true, false).is_ok());
    }

    #[test]
    fn enforce_cluster_without_feature_fails() {
        let info = LicenseInfo {
            customer: "test@example.com".into(),
            tier: LicenseTier::Pro,
            features: vec![LicenseFeature::Vault],
            expires_at: u64::MAX,
            trial: false,
        };
        let result = enforce_license(Some(&info), false, true);
        assert!(result.is_err());
    }
}
