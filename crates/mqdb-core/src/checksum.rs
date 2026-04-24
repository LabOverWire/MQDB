// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use crc32fast::Hasher;

#[derive(Debug, thiserror::Error)]
pub enum ChecksumError {
    #[error("data too short for checksum")]
    DataTooShort,
    #[error("checksum mismatch")]
    ChecksumMismatch,
}

#[must_use]
pub fn compute_checksum(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

#[must_use]
pub fn encode_with_checksum(data: &[u8]) -> Vec<u8> {
    let checksum = compute_checksum(data);
    let mut result = Vec::with_capacity(4 + data.len());
    result.extend_from_slice(&checksum.to_be_bytes());
    result.extend_from_slice(data);
    result
}

/// # Errors
/// Returns an error if data is too short or checksum doesn't match.
pub fn decode_and_verify(data: &[u8]) -> Result<&[u8], ChecksumError> {
    if data.len() < 4 {
        return Err(ChecksumError::DataTooShort);
    }

    let stored_checksum = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    let payload = &data[4..];
    let computed_checksum = compute_checksum(payload);

    if stored_checksum == computed_checksum {
        Ok(payload)
    } else {
        Err(ChecksumError::ChecksumMismatch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_checksum_consistency() {
        let data = b"hello world";
        let checksum1 = compute_checksum(data);
        let checksum2 = compute_checksum(data);
        assert_eq!(checksum1, checksum2);
    }

    #[test]
    fn test_encode_with_checksum() {
        let data = b"test data";
        let encoded = encode_with_checksum(data);

        assert_eq!(encoded.len(), 4 + data.len());
        assert_eq!(&encoded[4..], data);
    }

    #[test]
    fn test_decode_and_verify_valid() {
        let data = b"test data";
        let encoded = encode_with_checksum(data);
        let decoded = decode_and_verify(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_decode_and_verify_corrupted() {
        let data = b"test data";
        let mut encoded = encode_with_checksum(data);

        encoded[5] ^= 0xFF;

        let result = decode_and_verify(&encoded);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_and_verify_too_short() {
        let data = &[1, 2, 3];
        let result = decode_and_verify(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_bitflip_detection() {
        let data = b"important data that must not be corrupted";
        let encoded = encode_with_checksum(data);

        for i in 4..encoded.len() {
            let mut corrupted = encoded.clone();
            corrupted[i] ^= 0x01;

            let result = decode_and_verify(&corrupted);
            assert!(result.is_err(), "Failed to detect bitflip at position {i}");
        }
    }
}
