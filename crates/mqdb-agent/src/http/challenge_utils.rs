// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use ring::digest;
use ring::rand::{SecureRandom, SystemRandom};

pub(crate) fn uuid_v4() -> String {
    let rng = SystemRandom::new();
    let mut bytes = [0u8; 16];
    rng.fill(&mut bytes)
        .expect("system RNG unavailable — OS CSPRNG failure");
    bytes[6] = (bytes[6] & 0x0F) | 0x40;
    bytes[8] = (bytes[8] & 0x3F) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    )
}

pub(crate) fn generate_verification_code() -> Option<String> {
    let rng = SystemRandom::new();
    let mut bytes = [0u8; 4];
    rng.fill(&mut bytes).ok()?;
    let num = u32::from_be_bytes(bytes) % 1_000_000;
    Some(format!("{num:06}"))
}

pub(crate) fn hash_code(code: &str) -> String {
    let d = digest::digest(&digest::SHA256, code.as_bytes());
    super::credentials::hex_encode(d.as_ref())
}

pub(crate) fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_v4_has_correct_format() {
        let id = uuid_v4();
        assert_eq!(id.len(), 36);
        assert_eq!(id.as_bytes()[8], b'-');
        assert_eq!(id.as_bytes()[13], b'-');
        assert_eq!(id.as_bytes()[14], b'4');
        assert_eq!(id.as_bytes()[18], b'-');
        assert_eq!(id.as_bytes()[23], b'-');
    }

    #[test]
    fn generate_verification_code_is_six_digits() {
        let code = generate_verification_code().unwrap();
        assert_eq!(code.len(), 6);
        assert!(code.chars().all(|c| c.is_ascii_digit()));
    }

    #[test]
    fn hash_code_produces_64_char_hex() {
        let h = hash_code("123456");
        assert_eq!(h.len(), 64);
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn hash_code_is_deterministic() {
        assert_eq!(hash_code("test"), hash_code("test"));
    }

    #[test]
    fn hash_code_differs_for_different_inputs() {
        assert_ne!(hash_code("abc"), hash_code("def"));
    }

    #[test]
    fn now_unix_returns_reasonable_timestamp() {
        let ts = now_unix();
        assert!(ts > 1_700_000_000);
    }
}
