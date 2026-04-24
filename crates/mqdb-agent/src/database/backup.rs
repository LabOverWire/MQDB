// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::Database;
use mqdb_core::error::{Error, Result};
use serde_json::Value;
use std::path::Path;

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

        copy_dir_recursive(src, dst)
            .map_err(|e| Error::BackupFailed(format!("failed to create backup: {e}")))?;

        Ok(())
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

        copy_dir_recursive(src, dest)
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

            self.create(
                entity_name,
                entity_data,
                None,
                None,
                None,
                &mqdb_core::types::ScopeConfig::default(),
            )
            .await?;
            count += 1;
        }

        tracing::info!(
            "logical restore completed: {count} entities restored from {}",
            src.display()
        );
        Ok(count)
    }
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            std::fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
}
