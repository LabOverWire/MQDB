// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

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
    pub provider_sub: String,
    pub email: Option<String>,
    pub name: Option<String>,
    pub picture: Option<String>,
    pub created_at: SystemTime,
    pub vault_unlocked: bool,
}

pub struct NewSession {
    pub jwt: String,
    pub canonical_id: String,
    pub provider: String,
    pub provider_sub: String,
    pub email: Option<String>,
    pub name: Option<String>,
    pub picture: Option<String>,
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

    pub fn create(&self, new: NewSession) -> Option<String> {
        let rng = SystemRandom::new();
        let mut bytes = [0u8; SESSION_ID_BYTES];
        if rng.fill(&mut bytes).is_err() {
            return None;
        }

        let session_id = hex_encode(&bytes);
        let session = Session {
            jwt: new.jwt,
            canonical_id: new.canonical_id,
            provider: new.provider,
            provider_sub: new.provider_sub,
            email: new.email,
            name: new.name,
            picture: new.picture,
            created_at: SystemTime::now(),
            vault_unlocked: false,
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
            provider_sub: session.provider_sub.clone(),
            email: session.email.clone(),
            name: session.name.clone(),
            picture: session.picture.clone(),
            vault_unlocked: session.vault_unlocked,
        })
    }

    pub fn destroy(&self, session_id: &str) -> bool {
        let Ok(mut sessions) = self.sessions.write() else {
            return false;
        };
        sessions.remove(session_id).is_some()
    }

    /// Destroys every session whose `canonical_id` matches, except `keep_session_id`
    /// (pass `None` to destroy all). Returns the JWTs of destroyed sessions so the
    /// caller can decode and revoke their JTIs.
    pub fn destroy_others_by_canonical_id(
        &self,
        canonical_id: &str,
        keep_session_id: Option<&str>,
    ) -> Vec<String> {
        let Ok(mut sessions) = self.sessions.write() else {
            return Vec::new();
        };
        let mut removed_jwts = Vec::new();
        sessions.retain(|sid, session| {
            let same_user = session.canonical_id == canonical_id;
            let is_kept = keep_session_id.is_some_and(|keep| sid.as_str() == keep);
            if same_user && !is_kept {
                removed_jwts.push(session.jwt.clone());
                false
            } else {
                true
            }
        });
        removed_jwts
    }

    pub fn set_vault_unlocked(&self, session_id: &str, unlocked: bool) -> bool {
        let Ok(mut sessions) = self.sessions.write() else {
            return false;
        };
        if let Some(session) = sessions.get_mut(session_id) {
            session.vault_unlocked = unlocked;
            return true;
        }
        false
    }

    pub fn set_vault_unlocked_by_canonical_id(&self, canonical_id: &str, unlocked: bool) {
        let Ok(mut sessions) = self.sessions.write() else {
            return;
        };
        for session in sessions.values_mut() {
            if session.canonical_id == canonical_id {
                session.vault_unlocked = unlocked;
            }
        }
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
    pub provider_sub: String,
    pub email: Option<String>,
    pub name: Option<String>,
    pub picture: Option<String>,
    pub vault_unlocked: bool,
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
            .create(NewSession {
                jwt: "jwt123".into(),
                canonical_id: "550e8400-e29b-41d4-a716-446655440000".into(),
                provider: "google".into(),
                provider_sub: "112233445566".into(),
                email: Some("user@example.com".into()),
                name: Some("Test User".into()),
                picture: None,
            })
            .expect("create should succeed");

        assert_eq!(session_id.len(), 64);

        let session = store.get(&session_id).expect("get should succeed");
        assert_eq!(session.jwt, "jwt123");
        assert_eq!(session.canonical_id, "550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(session.provider, "google");
        assert_eq!(session.provider_sub, "112233445566");
        assert_eq!(session.email.as_deref(), Some("user@example.com"));
    }

    #[test]
    fn test_destroy_session() {
        let store = SessionStore::new();
        let session_id = store
            .create(NewSession {
                jwt: "jwt".into(),
                canonical_id: "canonical-1".into(),
                provider: "google".into(),
                provider_sub: "sub-1".into(),
                email: None,
                name: None,
                picture: None,
            })
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

    fn make_session(store: &SessionStore, canonical_id: &str, jwt: &str) -> String {
        store
            .create(NewSession {
                jwt: jwt.into(),
                canonical_id: canonical_id.into(),
                provider: "email".into(),
                provider_sub: canonical_id.into(),
                email: None,
                name: None,
                picture: None,
            })
            .expect("create should succeed")
    }

    #[test]
    fn destroy_others_keeps_target_session_and_returns_other_jwts() {
        let store = SessionStore::new();
        let keep = make_session(&store, "user-a", "jwt-keep");
        let other_a = make_session(&store, "user-a", "jwt-a1");
        let other_b = make_session(&store, "user-a", "jwt-a2");
        let untouched = make_session(&store, "user-b", "jwt-b");

        let removed = store.destroy_others_by_canonical_id("user-a", Some(&keep));
        assert_eq!(removed.len(), 2);
        assert!(removed.contains(&"jwt-a1".to_string()));
        assert!(removed.contains(&"jwt-a2".to_string()));

        assert!(store.get(&keep).is_some());
        assert!(store.get(&other_a).is_none());
        assert!(store.get(&other_b).is_none());
        assert!(store.get(&untouched).is_some());
    }

    #[test]
    fn destroy_others_with_none_destroys_all_sessions_for_user() {
        let store = SessionStore::new();
        let a1 = make_session(&store, "user-a", "jwt-a1");
        let a2 = make_session(&store, "user-a", "jwt-a2");
        let b = make_session(&store, "user-b", "jwt-b");

        let removed = store.destroy_others_by_canonical_id("user-a", None);
        assert_eq!(removed.len(), 2);

        assert!(store.get(&a1).is_none());
        assert!(store.get(&a2).is_none());
        assert!(store.get(&b).is_some());
    }

    #[test]
    fn destroy_others_with_unknown_user_returns_empty() {
        let store = SessionStore::new();
        make_session(&store, "user-a", "jwt-a");

        let removed = store.destroy_others_by_canonical_id("user-x", None);
        assert!(removed.is_empty());
    }
}
