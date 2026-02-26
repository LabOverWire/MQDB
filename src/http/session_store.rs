// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use ring::rand::{SecureRandom, SystemRandom};
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, SystemTime};

const SESSION_ID_BYTES: usize = 32;
const SESSION_TTL_SECS: u64 = 86400;

pub struct Session {
    pub jwt: String,
    pub canonical_id: String,
    pub provider: String,
    pub email: Option<String>,
    pub name: Option<String>,
    pub picture: Option<String>,
    pub created_at: SystemTime,
}

pub struct SessionStore {
    sessions: RwLock<HashMap<String, Session>>,
}

impl Default for SessionStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create(
        &self,
        jwt: String,
        canonical_id: String,
        provider: String,
        email: Option<String>,
        name: Option<String>,
        picture: Option<String>,
    ) -> Option<String> {
        let rng = SystemRandom::new();
        let mut bytes = [0u8; SESSION_ID_BYTES];
        if rng.fill(&mut bytes).is_err() {
            return None;
        }

        let session_id = hex_encode(&bytes);
        let session = Session {
            jwt,
            canonical_id,
            provider,
            email,
            name,
            picture,
            created_at: SystemTime::now(),
        };

        let mut sessions = self.sessions.write().ok()?;
        cleanup_expired(&mut sessions);
        sessions.insert(session_id.clone(), session);
        Some(session_id)
    }

    #[must_use]
    pub fn get(&self, session_id: &str) -> Option<SessionRef> {
        let sessions = self.sessions.read().ok()?;
        let session = sessions.get(session_id)?;

        if is_expired(session) {
            return None;
        }

        Some(SessionRef {
            jwt: session.jwt.clone(),
            canonical_id: session.canonical_id.clone(),
            provider: session.provider.clone(),
            email: session.email.clone(),
            name: session.name.clone(),
            picture: session.picture.clone(),
        })
    }

    pub fn destroy(&self, session_id: &str) -> bool {
        let Ok(mut sessions) = self.sessions.write() else {
            return false;
        };
        sessions.remove(session_id).is_some()
    }

    pub fn cleanup(&self) {
        if let Ok(mut sessions) = self.sessions.write() {
            cleanup_expired(&mut sessions);
        }
    }
}

pub struct SessionRef {
    pub jwt: String,
    pub canonical_id: String,
    pub provider: String,
    pub email: Option<String>,
    pub name: Option<String>,
    pub picture: Option<String>,
}

const MAX_REVOKED_JTIS: usize = 10_000;
const JTI_TTL_SECS: u64 = 30 * 24 * 3600;

pub struct JtiRevocationStore {
    revoked: RwLock<HashMap<String, SystemTime>>,
}

impl Default for JtiRevocationStore {
    fn default() -> Self {
        Self::new()
    }
}

impl JtiRevocationStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            revoked: RwLock::new(HashMap::new()),
        }
    }

    pub fn revoke(&self, jti: &str) {
        let Ok(mut store) = self.revoked.write() else {
            return;
        };
        cleanup_revoked(&mut store);
        if store.len() < MAX_REVOKED_JTIS {
            store.insert(jti.to_string(), SystemTime::now());
        }
    }

    #[must_use]
    pub fn is_revoked(&self, jti: &str) -> bool {
        let Ok(store) = self.revoked.read() else {
            return true;
        };
        store.contains_key(jti)
    }

    #[must_use]
    pub fn generate_jti() -> String {
        let rng = SystemRandom::new();
        let mut bytes = [0u8; 16];
        if rng.fill(&mut bytes).is_err() {
            return hex_encode(
                &SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map_or(0, |d| d.as_nanos())
                    .to_le_bytes(),
            );
        }
        hex_encode(&bytes)
    }
}

fn cleanup_revoked(store: &mut HashMap<String, SystemTime>) {
    let ttl = Duration::from_secs(JTI_TTL_SECS);
    store.retain(|_, added_at| added_at.elapsed().map_or(true, |elapsed| elapsed < ttl));
}

fn is_expired(session: &Session) -> bool {
    let ttl = Duration::from_secs(SESSION_TTL_SECS);
    session
        .created_at
        .elapsed()
        .map_or(true, |elapsed| elapsed > ttl)
}

fn cleanup_expired(sessions: &mut HashMap<String, Session>) {
    sessions.retain(|_, session| !is_expired(session));
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
    fn test_create_and_get_session() {
        let store = SessionStore::new();
        let session_id = store
            .create(
                "jwt123".into(),
                "550e8400-e29b-41d4-a716-446655440000".into(),
                "google".into(),
                Some("user@example.com".into()),
                Some("Test User".into()),
                None,
            )
            .expect("create should succeed");

        assert_eq!(session_id.len(), 64);

        let session = store.get(&session_id).expect("get should succeed");
        assert_eq!(session.jwt, "jwt123");
        assert_eq!(session.canonical_id, "550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(session.provider, "google");
        assert_eq!(session.email.as_deref(), Some("user@example.com"));
    }

    #[test]
    fn test_destroy_session() {
        let store = SessionStore::new();
        let session_id = store
            .create(
                "jwt".into(),
                "canonical-1".into(),
                "google".into(),
                None,
                None,
                None,
            )
            .expect("create should succeed");

        assert!(store.get(&session_id).is_some());
        assert!(store.destroy(&session_id));
        assert!(store.get(&session_id).is_none());
    }

    #[test]
    fn test_invalid_session_id() {
        let store = SessionStore::new();
        assert!(store.get("nonexistent").is_none());
    }
}
