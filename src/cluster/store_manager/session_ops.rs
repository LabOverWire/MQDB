use super::StoreManager;
use crate::cluster::protocol::{Operation, ReplicationWrite};
use crate::cluster::session::{SessionData, SessionError, session_partition};
use crate::cluster::{Epoch, entity};

impl StoreManager {
    /// # Errors
    /// Returns `SessionError` if the session already exists for this client ID.
    pub fn create_session_replicated(
        &self,
        client_id: &str,
    ) -> Result<(SessionData, ReplicationWrite), SessionError> {
        let (session, data) = self.sessions.create_session_with_data(client_id)?;
        let partition = session_partition(client_id);
        let write = ReplicationWrite::new(
            partition,
            Operation::Insert,
            Epoch::ZERO,
            0,
            entity::SESSIONS.to_string(),
            client_id.to_string(),
            data,
        );
        Ok((session, write))
    }

    /// # Errors
    /// Returns `SessionError` if no session exists for this client ID.
    pub fn update_session_replicated<F>(
        &self,
        client_id: &str,
        f: F,
    ) -> Result<(SessionData, ReplicationWrite), SessionError>
    where
        F: FnOnce(&mut SessionData),
    {
        let (session, data) = self.sessions.update_with_data(client_id, f)?;
        let partition = session_partition(client_id);
        let write = ReplicationWrite::new(
            partition,
            Operation::Update,
            Epoch::ZERO,
            0,
            entity::SESSIONS.to_string(),
            client_id.to_string(),
            data,
        );
        Ok((session, write))
    }

    /// # Errors
    /// Returns `SessionError` if no session exists for this client ID.
    pub fn remove_session_replicated(
        &self,
        client_id: &str,
    ) -> Result<(SessionData, ReplicationWrite), SessionError> {
        let (session, data) = self.sessions.remove_with_data(client_id)?;
        let partition = session_partition(client_id);
        let write = ReplicationWrite::new(
            partition,
            Operation::Delete,
            Epoch::ZERO,
            0,
            entity::SESSIONS.to_string(),
            client_id.to_string(),
            data,
        );
        Ok((session, write))
    }
}
