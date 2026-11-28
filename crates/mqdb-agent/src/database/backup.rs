// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::Database;
use mqdb_core::error::{Error, Result};
use serde_json::Value;
use std::fs::TryLockError;
use std::path::Path;

pub(crate) const BACKUPS_DIR: &str = "backups";
const LOCK_FILE: &str = "lock";
const VERSION_FILE: &str = "version";
const RESTORE_STAGING_DIR: &str = ".restore-staging";
const CLUSTER_NODE_DIRS: [&str; 2] = ["stores", "raft"];
const KEPT_ON_RESTORE: [&str; 3] = [LOCK_FILE, BACKUPS_DIR, RESTORE_STAGING_DIR];

pub(crate) fn is_valid_backup_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

impl Database {
    /// # Errors
    /// Returns an error if the backup fails.
    pub fn backup<P: AsRef<Path>>(&self, backup_path: P) -> Result<()> {
        self.storage.flush()?;

        let src = &self.config.path;
        let dst = backup_path.as_ref();

        if dst.exists() {
            std::fs::remove_dir_all(dst).map_err(|e| {
                Error::BackupFailed(format!("failed to remove existing backup: {e}"))
            })?;
        }

        copy_database(src, dst)
            .map_err(|e| Error::BackupFailed(format!("failed to create backup: {e}")))?;

        Ok(())
    }

    /// # Errors
    /// Returns an error if the name is invalid, the backup is missing, `db_path`
    /// is a cluster node database or in use, or a file operation fails.
    pub fn restore_backup<P: AsRef<Path>>(db_path: P, name: &str) -> Result<usize> {
        let db_path = db_path.as_ref();
        if !is_valid_backup_name(name) {
            return Err(Error::BackupFailed(format!(
                "invalid backup name: {name}: must be alphanumeric, underscore, or hyphen only"
            )));
        }
        let backup = db_path.join(BACKUPS_DIR).join(name);
        if !backup.join(VERSION_FILE).is_file() {
            return Err(Error::BackupFailed(format!(
                "no database backup at {}",
                backup.display()
            )));
        }
        if CLUSTER_NODE_DIRS
            .iter()
            .any(|dir| db_path.join(dir).exists())
        {
            return Err(Error::BackupFailed(format!(
                "{} is a cluster node database: restore supports agent databases only",
                db_path.display()
            )));
        }

        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(db_path.join(LOCK_FILE))
            .map_err(|e| Error::BackupFailed(format!("failed to open database lock: {e}")))?;
        lock.try_lock().map_err(|e| match e {
            TryLockError::WouldBlock => Error::BackupFailed(format!(
                "database at {} is in use: stop the agent before restoring",
                db_path.display()
            )),
            TryLockError::Error(e) => Error::BackupFailed(format!("failed to lock database: {e}")),
        })?;

        let restored = replace_with_backup(db_path, &backup)
            .map_err(|e| Error::BackupFailed(format!("failed to restore backup: {e}")))?;

        lock.unlock()
            .map_err(|e| Error::BackupFailed(format!("failed to unlock database: {e}")))?;

        tracing::info!(
            "restore completed: {restored} files from {} into {}",
            backup.display(),
            db_path.display()
        );
        Ok(restored)
    }

    /// # Errors
    /// Returns an error if the backup fails.
    pub fn backup_physical<P: AsRef<Path>>(&self, destination: P) -> Result<()> {
        let dest = destination.as_ref();

        self.storage.flush()?;

        if dest.exists() {
            return Err(Error::BackupFailed(format!(
                "destination already exists: {}",
                dest.display()
            )));
        }

        let src = &self.config.path;

        copy_database(src, dest)
            .map_err(|e| Error::BackupFailed(format!("failed to copy database: {e}")))?;

        tracing::info!(
            "physical backup completed: {} -> {}",
            src.display(),
            dest.display()
        );
        Ok(())
    }

    /// # Errors
    /// Returns an error if the backup fails.
    pub fn backup_logical<P: AsRef<Path>>(&self, destination: P) -> Result<()> {
        use std::fs::File;
        use std::io::BufWriter;

        let dest = destination.as_ref();

        if dest.exists() {
            return Err(Error::BackupFailed(format!(
                "destination already exists: {}",
                dest.display()
            )));
        }

        let file = File::create(dest)
            .map_err(|e| Error::BackupFailed(format!("failed to create backup file: {e}")))?;

        let mut writer = BufWriter::new(file);

        let prefix = b"data/";
        let items = self.storage.prefix_scan(prefix)?;

        let mut count = 0;
        for (key, value) in items {
            let (entity_name, id) = mqdb_core::keys::decode_data_key(&key)?;
            let entity = mqdb_core::entity::Entity::deserialize(entity_name.clone(), id, &value)?;

            let backup_record = serde_json::json!({
                "_entity": entity_name,
                "_data": entity.to_json()
            });

            serde_json::to_writer(&mut writer, &backup_record)
                .map_err(|e| Error::BackupFailed(format!("failed to write entity: {e}")))?;

            std::io::Write::write_all(&mut writer, b"\n")
                .map_err(|e| Error::BackupFailed(format!("failed to write newline: {e}")))?;

            count += 1;
        }

        std::io::Write::flush(&mut writer)
            .map_err(|e| Error::BackupFailed(format!("failed to flush backup: {e}")))?;

        tracing::info!(
            "logical backup completed: {count} entities written to {}",
            dest.display()
        );
        Ok(())
    }

    /// # Errors
    /// Returns an error if the restore fails.
    pub async fn restore_logical<P: AsRef<Path>>(&self, source: P) -> Result<usize> {
        use tokio::io::AsyncBufReadExt;

        let src = source.as_ref();

        if !src.exists() {
            return Err(Error::BackupFailed(format!(
                "backup file not found: {}",
                src.display()
            )));
        }

        let file = tokio::fs::File::open(src)
            .await
            .map_err(|e| Error::BackupFailed(format!("failed to open backup file: {e}")))?;

        let reader = tokio::io::BufReader::new(file);
        let mut lines = reader.lines();
        let mut count = 0;

        while let Some(line) = lines
            .next_line()
            .await
            .map_err(|e| Error::BackupFailed(format!("failed to read line: {e}")))?
        {
            if line.trim().is_empty() {
                continue;
            }

            let backup_record: Value = serde_json::from_str(&line)
                .map_err(|e| Error::BackupFailed(format!("failed to parse entity: {e}")))?;

            let entity_name = backup_record
                .get("_entity")
                .and_then(|v| v.as_str())
                .ok_or_else(|| Error::BackupFailed("entity missing _entity field".to_string()))?
                .to_string();

            let entity_data = backup_record
                .get("_data")
                .ok_or_else(|| Error::BackupFailed("entity missing _data field".to_string()))?
                .clone();

            self.restore_record(entity_name, entity_data).await?;
            count += 1;
        }

        tracing::info!(
            "logical restore completed: {count} entities restored from {}",
            src.display()
        );
        Ok(count)
    }
}

fn copy_database(src: &Path, dst: &Path) -> std::io::Result<usize> {
    std::fs::create_dir_all(dst)?;
    let dst = std::fs::canonicalize(dst)?;
    copy_tree(&std::fs::canonicalize(src)?, &dst, &|path| {
        dst.starts_with(path)
    })
}

fn copy_tree(src: &Path, dst: &Path, skip: &dyn Fn(&Path) -> bool) -> std::io::Result<usize> {
    std::fs::create_dir_all(dst)?;
    let mut copied = 0;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        if skip(&src_path) {
            continue;
        }
        let dst_path = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copied += copy_tree(&src_path, &dst_path, skip)?;
        } else {
            std::fs::copy(&src_path, &dst_path)?;
            copied += 1;
        }
    }
    Ok(copied)
}

fn replace_with_backup(db_path: &Path, backup: &Path) -> std::io::Result<usize> {
    let staging = db_path.join(RESTORE_STAGING_DIR);
    if staging.exists() {
        std::fs::remove_dir_all(&staging)?;
    }
    let restored = copy_tree(backup, &staging, &|path| {
        path.parent() == Some(backup) && is_kept_on_restore(path)
    })?;

    for entry in std::fs::read_dir(db_path)? {
        let entry = entry?;
        if is_kept_on_restore(&entry.path()) {
            continue;
        }
        if entry.file_type()?.is_dir() {
            std::fs::remove_dir_all(entry.path())?;
        } else {
            std::fs::remove_file(entry.path())?;
        }
    }

    for entry in std::fs::read_dir(&staging)? {
        let entry = entry?;
        std::fs::rename(entry.path(), db_path.join(entry.file_name()))?;
    }
    std::fs::remove_dir(&staging)?;
    Ok(restored)
}

fn is_kept_on_restore(path: &Path) -> bool {
    path.file_name()
        .is_some_and(|name| KEPT_ON_RESTORE.iter().any(|kept| name == *kept))
}
