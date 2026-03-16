// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::path::PathBuf;

use crate::cli_types::{DevBaselineAction, DevBenchScenario};

pub(crate) fn cmd_dev_baseline(
    action: DevBaselineAction,
) -> Result<(), Box<dyn std::error::Error>> {
    let baselines_dir = PathBuf::from(".claude/benchmarks/baselines");

    match action {
        DevBaselineAction::Save { name, scenario } => {
            std::fs::create_dir_all(&baselines_dir)?;

            let scenario_name = match &scenario {
                DevBenchScenario::Pubsub { .. } => "pubsub",
                DevBenchScenario::Db { .. } => "db",
            };

            let filename = format!("{scenario_name}_{name}.json");
            let path = baselines_dir.join(&filename);

            println!("To save baseline, first run benchmark with --output:");
            println!(
                "  mqdb dev bench {scenario_name} --output {}",
                path.display()
            );
        }
        DevBaselineAction::List => {
            if !baselines_dir.exists() {
                println!("No baselines saved yet.");
                println!("Create with: mqdb dev baseline save <name> pubsub|db");
                return Ok(());
            }

            println!("Saved baselines:");
            for entry in std::fs::read_dir(&baselines_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.extension().is_some_and(|e| e == "json")
                    && let Some(name) = path.file_stem()
                {
                    println!("  {}", name.to_string_lossy());
                }
            }
        }
        DevBaselineAction::Compare { name, scenario } => {
            let scenario_name = match &scenario {
                DevBenchScenario::Pubsub { .. } => "pubsub",
                DevBenchScenario::Db { .. } => "db",
            };

            let filename = format!("{scenario_name}_{name}.json");
            let path = baselines_dir.join(&filename);

            if !path.exists() {
                return Err(format!("Baseline not found: {}", path.display()).into());
            }

            println!("To compare against baseline, run benchmark with --baseline:");
            println!(
                "  mqdb dev bench {scenario_name} --baseline {}",
                path.display()
            );
        }
    }

    Ok(())
}
