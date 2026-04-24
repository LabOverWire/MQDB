// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::fmt::Write;

pub const SESSION_COOKIE_NAME: &str = "mqdb_session";

pub fn build_set_cookie_header(session_id: &str, secure: bool, max_age_secs: u64) -> String {
    let mut cookie = format!("{SESSION_COOKIE_NAME}={session_id}; HttpOnly; SameSite=Lax; Path=/");
    if secure {
        cookie.push_str("; Secure");
    }
    let _ = write!(cookie, "; Max-Age={max_age_secs}");
    cookie
}

pub fn build_delete_cookie_header() -> String {
    format!("{SESSION_COOKIE_NAME}=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0")
}

pub fn parse_session_id(cookie_header: &str) -> Option<&str> {
    for cookie in cookie_header.split(';') {
        let cookie = cookie.trim();
        if let Some(value) = cookie
            .strip_prefix(SESSION_COOKIE_NAME)
            .and_then(|v| v.strip_prefix('='))
        {
            return Some(value.trim());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_session_id() {
        assert_eq!(parse_session_id("mqdb_session=abc123"), Some("abc123"));
        assert_eq!(
            parse_session_id("other=xyz; mqdb_session=def456; another=foo"),
            Some("def456")
        );
        assert_eq!(parse_session_id("other=xyz"), None);
    }

    #[test]
    fn test_build_set_cookie_header() {
        let header = build_set_cookie_header("test123", false, 3600);
        assert!(header.contains("mqdb_session=test123"));
        assert!(header.contains("HttpOnly"));
        assert!(header.contains("SameSite=Lax"));
        assert!(!header.contains("Secure"));

        let secure_header = build_set_cookie_header("test123", true, 3600);
        assert!(secure_header.contains("Secure"));
    }

    #[test]
    fn test_build_delete_cookie_header() {
        let header = build_delete_cookie_header();
        assert!(header.contains("mqdb_session="));
        assert!(header.contains("Max-Age=0"));
    }
}
