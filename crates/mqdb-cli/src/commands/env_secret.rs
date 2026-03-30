// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::io::Write;
use std::path::PathBuf;

#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};

fn secret_dir() -> PathBuf {
    std::env::temp_dir().join(format!("mqdb-env-secrets-{}", std::process::id()))
}

pub(crate) fn write_temp_file(
    name: &str,
    content: &str,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let dir = secret_dir();

    #[cfg(unix)]
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&dir)?;

    #[cfg(not(unix))]
    std::fs::create_dir_all(&dir)?;

    let path = dir.join(name);
    let normalized = if content.ends_with('\n') {
        content.to_string()
    } else {
        format!("{content}\n")
    };

    #[cfg(unix)]
    {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&path)?;
        file.write_all(normalized.as_bytes())?;
    }

    #[cfg(not(unix))]
    std::fs::write(&path, normalized)?;

    Ok(path)
}

pub(crate) fn resolve_path_or_data(
    file: Option<PathBuf>,
    data: Option<&str>,
    temp_name: &str,
) -> Result<Option<PathBuf>, Box<dyn std::error::Error>> {
    if let Some(content) = data {
        return Ok(Some(write_temp_file(temp_name, content)?));
    }
    Ok(file)
}

pub(crate) fn resolve_passphrase(
    file: Option<&PathBuf>,
    data: Option<&str>,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    if let Some(content) = data {
        return Ok(Some(content.trim().to_string()));
    }
    if let Some(pf) = file {
        let passphrase = std::fs::read_to_string(pf)
            .map_err(|e| format!("failed to read passphrase file: {e}"))?;
        return Ok(Some(passphrase.trim().to_string()));
    }
    Ok(None)
}

pub(crate) fn resolve_federated_jwt_content(
    data: Option<&str>,
    file: Option<&PathBuf>,
) -> Option<String> {
    data.map(String::from)
        .or_else(|| file.as_ref().and_then(|p| std::fs::read_to_string(p).ok()))
}
