// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use clap::Subcommand;
use std::path::PathBuf;

#[derive(Subcommand)]
pub(crate) enum LicenseAction {
    #[command(about = "Verify and display license key details")]
    Verify {
        #[arg(long, help = "Path to license key file")]
        license: PathBuf,
    },
}
