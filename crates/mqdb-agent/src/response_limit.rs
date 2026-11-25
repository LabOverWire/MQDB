// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use mqdb_core::{ErrorCode, Response};

#[must_use]
pub fn too_large_response(size: usize, max: usize) -> Vec<u8> {
    let refusal = Response::error(
        ErrorCode::PayloadTooLarge,
        format!("response too large: {size} bytes exceeds the {max} byte limit"),
    );
    serde_json::to_vec(&refusal).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_refusal_names_the_size_and_the_limit() {
        let body: serde_json::Value =
            serde_json::from_slice(&too_large_response(15_483_438, 10_485_760)).unwrap();
        assert_eq!(body["status"], "error");
        assert_eq!(body["code"], 413);
        let message = body["message"].as_str().unwrap();
        assert!(message.contains("15483438 bytes"), "{message}");
        assert!(message.contains("10485760 byte limit"), "{message}");
    }
}
