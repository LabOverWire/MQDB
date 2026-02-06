// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{SessionData, SessionStore};
use uuid::Uuid;

#[must_use]
pub fn generate_lwt_token() -> [u8; 16] {
    *Uuid::now_v7().as_bytes()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LwtAction {
    Publish,
    Skip,
    AlreadyPublished,
}

#[must_use]
pub fn determine_lwt_action(session: &SessionData) -> LwtAction {
    if session.lwt_published != 0 {
        return LwtAction::AlreadyPublished;
    }

    if session.has_will == 0 {
        return LwtAction::Skip;
    }

    if session.lwt_token_present != 0 {
        LwtAction::Skip
    } else {
        LwtAction::Publish
    }
}

pub struct LwtPublisher<'a> {
    session_store: &'a SessionStore,
}

impl<'a> LwtPublisher<'a> {
    pub fn new(session_store: &'a SessionStore) -> Self {
        Self { session_store }
    }

    /// # Errors
    /// Returns `SessionNotFound` if client has no session, `UpdateFailed` if update fails.
    pub fn prepare_lwt(&self, client_id: &str) -> Result<Option<LwtPrepared>, LwtError> {
        let session = self
            .session_store
            .get(client_id)
            .ok_or(LwtError::SessionNotFound)?;

        match determine_lwt_action(&session) {
            LwtAction::AlreadyPublished | LwtAction::Skip => Ok(None),
            LwtAction::Publish => {
                let token = generate_lwt_token();

                self.session_store
                    .update(client_id, |s| {
                        s.set_lwt_token(token);
                    })
                    .map_err(|_| LwtError::UpdateFailed)?;

                Ok(Some(LwtPrepared {
                    client_id: client_id.to_string(),
                    token,
                    topic: String::from_utf8_lossy(&session.will_topic).to_string(),
                    payload: session.will_payload.clone(),
                    qos: session.will_qos,
                    retain: session.will_retain != 0,
                }))
            }
        }
    }

    /// # Errors
    /// Returns `SessionNotFound`, `TokenMismatch`, or `UpdateFailed`.
    pub fn complete_lwt(&self, client_id: &str, token: [u8; 16]) -> Result<(), LwtError> {
        let session = self
            .session_store
            .get(client_id)
            .ok_or(LwtError::SessionNotFound)?;

        if let Some(stored_token) = session.get_lwt_token()
            && stored_token != token
        {
            return Err(LwtError::TokenMismatch);
        }

        self.session_store
            .update(client_id, |s| {
                s.mark_lwt_published();
            })
            .map_err(|_| LwtError::UpdateFailed)?;

        Ok(())
    }

    #[must_use]
    pub fn recover_pending_lwts(&self) -> Vec<LwtPrepared> {
        self.session_store
            .sessions_with_pending_lwt()
            .into_iter()
            .filter(SessionData::lwt_in_progress)
            .map(|session| LwtPrepared {
                client_id: session.client_id_str().to_string(),
                token: session.lwt_token,
                topic: String::from_utf8_lossy(&session.will_topic).to_string(),
                payload: session.will_payload.clone(),
                qos: session.will_qos,
                retain: session.will_retain != 0,
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct LwtPrepared {
    pub client_id: String,
    pub token: [u8; 16],
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub retain: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LwtError {
    SessionNotFound,
    UpdateFailed,
    TokenMismatch,
}

impl std::fmt::Display for LwtError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SessionNotFound => write!(f, "session not found"),
            Self::UpdateFailed => write!(f, "failed to update session"),
            Self::TokenMismatch => write!(f, "LWT token mismatch"),
        }
    }
}

impl std::error::Error for LwtError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::NodeId;

    fn node(id: u16) -> NodeId {
        NodeId::validated(id).unwrap()
    }

    #[test]
    fn generate_token_is_unique() {
        let t1 = generate_lwt_token();
        let t2 = generate_lwt_token();
        assert_ne!(t1, t2);
    }

    #[test]
    fn determine_action_no_will() {
        let session = SessionData::create("client1", node(1));
        assert_eq!(determine_lwt_action(&session), LwtAction::Skip);
    }

    #[test]
    fn determine_action_needs_publish() {
        let mut session = SessionData::create("client1", node(1));
        session.set_will(1, false, "lwt/topic", b"goodbye");
        assert_eq!(determine_lwt_action(&session), LwtAction::Publish);
    }

    #[test]
    fn determine_action_in_progress() {
        let mut session = SessionData::create("client1", node(1));
        session.set_will(1, false, "lwt/topic", b"goodbye");
        session.set_lwt_token([1u8; 16]);
        assert_eq!(determine_lwt_action(&session), LwtAction::Skip);
    }

    #[test]
    fn determine_action_already_published() {
        let mut session = SessionData::create("client1", node(1));
        session.set_will(1, false, "lwt/topic", b"goodbye");
        session.mark_lwt_published();
        assert_eq!(determine_lwt_action(&session), LwtAction::AlreadyPublished);
    }

    #[test]
    fn prepare_and_complete_lwt() {
        let store = SessionStore::new(node(1));
        store.create_session("client1").unwrap();
        store
            .update("client1", |s| {
                s.set_will(1, false, "lwt/topic", b"goodbye");
            })
            .unwrap();

        let publisher = LwtPublisher::new(&store);

        let prepared = publisher.prepare_lwt("client1").unwrap().unwrap();
        assert_eq!(prepared.topic, "lwt/topic");
        assert_eq!(prepared.payload, b"goodbye");

        let session = store.get("client1").unwrap();
        assert!(session.has_lwt_token());
        assert!(!session.lwt_published != 0);

        publisher.complete_lwt("client1", prepared.token).unwrap();

        let session = store.get("client1").unwrap();
        assert!(session.lwt_published != 0);
    }

    #[test]
    fn prepare_idempotent() {
        let store = SessionStore::new(node(1));
        store.create_session("client1").unwrap();
        store
            .update("client1", |s| {
                s.set_will(1, false, "lwt/topic", b"goodbye");
            })
            .unwrap();

        let publisher = LwtPublisher::new(&store);

        let first = publisher.prepare_lwt("client1").unwrap();
        assert!(first.is_some());

        let second = publisher.prepare_lwt("client1").unwrap();
        assert!(second.is_none());
    }

    #[test]
    fn token_mismatch_rejected() {
        let store = SessionStore::new(node(1));
        store.create_session("client1").unwrap();
        store
            .update("client1", |s| {
                s.set_will(1, false, "lwt/topic", b"goodbye");
            })
            .unwrap();

        let publisher = LwtPublisher::new(&store);
        publisher.prepare_lwt("client1").unwrap();

        let result = publisher.complete_lwt("client1", [99u8; 16]);
        assert_eq!(result, Err(LwtError::TokenMismatch));
    }
}
