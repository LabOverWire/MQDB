// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use super::helpers::{chrono_timestamp, is_agent_running};
use super::runners::build_bench_args;
use crate::cli_types::DevBenchScenario;

pub(crate) fn cmd_dev_profile(
    scenario: &DevBenchScenario,
    tool: &str,
    duration: u64,
    output: Option<PathBuf>,
    db: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let exe = std::env::current_exe()?;

    if is_agent_running() {
        println!("Killing existing agent for profiling...");
        let _ = Command::new("pkill").args(["-f", "mqdb agent"]).status();
        std::thread::sleep(Duration::from_secs(1));
    }

    std::fs::create_dir_all(db)?;

    let timestamp = chrono_timestamp();
    let output_path = output.unwrap_or_else(|| {
        let dir = PathBuf::from(".claude/profiles");
        let _ = std::fs::create_dir_all(&dir);
        match tool {
            "flamegraph" => dir.join(format!("profile_{timestamp}.svg")),
            _ => dir.join(format!("profile_{timestamp}")),
        }
    });

    println!("Profiling with {tool}...");
    println!("Output will be saved to: {}", output_path.display());

    let bench_args = build_bench_args(scenario);

    match tool {
        "samply" => profile_with_samply(&exe, db, &output_path, &bench_args)?,
        "flamegraph" => profile_with_flamegraph(&exe, db, &output_path, &bench_args)?,
        "sample" => profile_with_sample(&exe, db, &output_path, &bench_args, duration)?,
        _ => {
            return Err(format!(
                "Unknown profiling tool: {tool}. Use 'sample', 'samply', or 'flamegraph'"
            )
            .into());
        }
    }

    Ok(())
}

fn profile_with_samply(
    _exe: &std::path::Path,
    db: &str,
    output_path: &std::path::Path,
    bench_args: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\nBuilding with profiling profile (full DWARF symbols)...");
    let build_status = Command::new("cargo")
        .args(["build", "--profile=profiling"])
        .status()?;
    if !build_status.success() {
        return Err("Failed to build with profiling profile".into());
    }

    let cwd = std::env::current_dir()?;
    let profiling_exe = cwd.join("target/profiling/mqdb");
    let symbol_dir = cwd.join("target/profiling");
    println!("Starting agent under samply profiler...");

    #[cfg(feature = "dev-insecure")]
    let agent_args = vec![
        "agent",
        "start",
        "--db",
        db,
        "--bind",
        "127.0.0.1:1883",
        "--anonymous",
    ];
    #[cfg(not(feature = "dev-insecure"))]
    let agent_args = vec!["agent", "start", "--db", db, "--bind", "127.0.0.1:1883"];

    let mut samply = Command::new("samply")
        .args([
            "record",
            "--save-only",
            "--unstable-presymbolicate",
            "--symbol-dir",
            symbol_dir.to_str().unwrap_or("."),
            "-o",
            output_path.to_str().unwrap_or("profile"),
            "--",
        ])
        .arg(&profiling_exe)
        .args(&agent_args)
        .spawn()?;

    std::thread::sleep(Duration::from_secs(3));

    println!("\nRunning benchmark...");
    let bench_status = Command::new(&profiling_exe).args(bench_args).status()?;
    if !bench_status.success() {
        println!("Benchmark failed with status: {bench_status}");
    }

    println!("\nStopping agent to save profile...");
    let _ = Command::new("pkill")
        .args(["-INT", "-f", "mqdb agent start"])
        .status();

    match samply.wait() {
        Ok(status) => println!("Samply exited with: {status}"),
        Err(e) => println!("Warning: Failed to wait for samply: {e}"),
    }

    println!("\nProfile saved: {}", output_path.display());
    println!("Load with: samply load {}", output_path.display());
    Ok(())
}

fn profile_with_flamegraph(
    exe: &std::path::Path,
    db: &str,
    output_path: &std::path::Path,
    bench_args: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\nBuilding with profiling profile...");
    let build_status = Command::new("cargo")
        .args(["build", "--profile=profiling"])
        .status()?;
    if !build_status.success() {
        return Err("Failed to build with profiling profile".into());
    }

    println!("Starting agent under flamegraph profiler...\n");

    let output_str = output_path.to_str().unwrap_or("flamegraph.svg");
    #[cfg(feature = "dev-insecure")]
    let flamegraph_args = vec![
        "flamegraph",
        "--profile=profiling",
        "-o",
        output_str,
        "--",
        "agent",
        "start",
        "--db",
        db,
        "--bind",
        "127.0.0.1:1883",
        "--anonymous",
    ];
    #[cfg(not(feature = "dev-insecure"))]
    let flamegraph_args = vec![
        "flamegraph",
        "--profile=profiling",
        "-o",
        output_str,
        "--",
        "agent",
        "start",
        "--db",
        db,
        "--bind",
        "127.0.0.1:1883",
    ];

    let mut flamegraph = Command::new("cargo").args(&flamegraph_args).spawn()?;

    std::thread::sleep(Duration::from_secs(2));

    println!("Running benchmark...");
    let _ = Command::new(exe).args(bench_args).status();

    println!("\nStopping agent to save flamegraph...");
    let _ = Command::new("pkill")
        .args(["-INT", "-f", "mqdb agent start"])
        .status();

    match flamegraph.wait() {
        Ok(status) => println!("Flamegraph exited with: {status}"),
        Err(e) => println!("Warning: Failed to wait for flamegraph: {e}"),
    }

    println!("\nFlamegraph saved: {}", output_path.display());
    Ok(())
}

fn profile_with_sample(
    _exe: &std::path::Path,
    db: &str,
    output_path: &std::path::Path,
    bench_args: &[String],
    duration: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\nBuilding with profiling profile (full DWARF symbols)...");
    let build_status = Command::new("cargo")
        .args(["build", "--profile=profiling"])
        .status()?;
    if !build_status.success() {
        return Err("Failed to build with profiling profile".into());
    }

    let cwd = std::env::current_dir()?;
    let profiling_exe = cwd.join("target/profiling/mqdb");

    println!("Starting agent...");
    #[cfg(feature = "dev-insecure")]
    let agent_args = vec![
        "agent",
        "start",
        "--db",
        db,
        "--bind",
        "127.0.0.1:1883",
        "--anonymous",
    ];
    #[cfg(not(feature = "dev-insecure"))]
    let agent_args = vec!["agent", "start", "--db", db, "--bind", "127.0.0.1:1883"];

    let mut agent = Command::new(&profiling_exe)
        .args(&agent_args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;

    std::thread::sleep(Duration::from_secs(2));

    let agent_pid = agent.id();
    println!("Agent started with PID: {agent_pid}");

    println!("\nRunning benchmark in background...");
    let mut bench = Command::new(&profiling_exe)
        .args(bench_args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;

    std::thread::sleep(Duration::from_millis(500));

    println!("Sampling for {duration} seconds...");
    let sample_output = output_path.with_extension("txt");
    let sample_status = Command::new("sample")
        .args([
            &agent_pid.to_string(),
            &duration.to_string(),
            "-f",
            sample_output.to_str().unwrap_or("profile.txt"),
        ])
        .status()?;

    if !sample_status.success() {
        println!("Warning: sample command failed with status: {sample_status}");
    }

    println!("\nStopping benchmark and agent...");
    let _ = bench.kill();
    let _ = agent.kill();
    let _ = bench.wait();
    let _ = agent.wait();

    println!("\nProfile saved: {}", sample_output.display());

    if sample_output.exists() {
        println!("\nGenerating analysis report...\n");
        analyze_sample_output(&sample_output)?;
    }

    Ok(())
}

fn analyze_sample_output(filepath: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    profile_analysis::analyze(filepath)
}

pub(super) fn demangle_rust_symbol(name: &str) -> String {
    let mut s = name.to_string();
    s = s.replace("::$u7b$$u7b$closure$u7d$$u7d$", "::{{closure}}");
    s = s.replace("$LT$", "<");
    s = s.replace("$GT$", ">");
    s = s.replace("$u20$", " ");
    if let Some(pos) = s.rfind("::h")
        && s.len() - pos == 19
        && s[pos + 3..].chars().all(|c| c.is_ascii_hexdigit())
    {
        s.truncate(pos);
    }
    s
}

mod profile_analysis {
    use std::collections::HashMap;

    type CategoryFilter = fn(&str) -> bool;

    fn is_waiting(f: &str) -> bool {
        f.contains("__psynch_cvwait") || f.contains("__psynch_mutexwait")
    }
    fn is_io_wait(f: &str) -> bool {
        f.contains("kevent")
    }
    fn is_network_io(f: &str) -> bool {
        f.contains("__sendto") || f.contains("__recvfrom")
    }
    fn is_mqdb(f: &str) -> bool {
        f.contains("[mqdb]") && f.contains("mqdb::")
    }
    fn is_mqtt5(f: &str) -> bool {
        f.contains("[mqdb]") && f.contains("mqtt5::")
    }
    fn is_allocation(f: &str) -> bool {
        f.contains("malloc") || f.contains("free") || f.contains("_xzm_")
    }
    fn is_sync(f: &str) -> bool {
        f.contains("pthread_mutex") || f.contains("pthread_cond") || f.contains("parking_lot::")
    }
    fn is_channel(f: &str) -> bool {
        f.contains("flume::")
    }

    const CATEGORIES: &[(&str, CategoryFilter)] = &[
        ("WAITING", is_waiting),
        ("IO_WAIT", is_io_wait),
        ("NETWORK_IO", is_network_io),
        ("MQDB", is_mqdb),
        ("MQTT5", is_mqtt5),
        ("ALLOCATION", is_allocation),
        ("SYNC", is_sync),
        ("CHANNEL", is_channel),
    ];

    fn pct(num: u64, denom: u64) -> f64 {
        #[allow(clippy::cast_precision_loss)]
        let result = (num as f64 / denom as f64) * 100.0;
        result
    }

    fn compute_exclusive(call_graph: &[(usize, u64, String)]) -> HashMap<String, u64> {
        let mut exclusive: HashMap<String, u64> = HashMap::new();
        for (i, (depth, count, key)) in call_graph.iter().enumerate() {
            let is_leaf = call_graph
                .iter()
                .skip(i + 1)
                .take_while(|(d, _, _)| *d > *depth)
                .next()
                .is_none();
            if is_leaf {
                *exclusive.entry(key.clone()).or_insert(0) += count;
            }
        }
        exclusive
    }

    fn print_category_breakdown(exclusive: &HashMap<String, u64>, total_excl: u64) {
        println!(
            "\n--------------------------------------------------------------------------------"
        );
        println!("EXCLUSIVE TIME BY CATEGORY");
        println!(
            "--------------------------------------------------------------------------------\n"
        );

        for (cat_name, filter) in CATEGORIES {
            let cat_total: u64 = exclusive
                .iter()
                .filter(|(k, _)| filter(k))
                .map(|(_, v)| v)
                .sum();
            if cat_total > 0 {
                println!(
                    "{cat_name:15} {cat_total:8} samples ({:5.1}%)",
                    pct(cat_total, total_excl)
                );
            }
        }
    }

    fn print_top_functions(inclusive: &HashMap<String, u64>, total_samples: u64) {
        println!(
            "\n--------------------------------------------------------------------------------"
        );
        println!("TOP MQTT5/MQDB FUNCTIONS (by inclusive time)");
        println!(
            "--------------------------------------------------------------------------------\n"
        );

        let mut mqtt_funcs: Vec<_> = inclusive
            .iter()
            .filter(|(k, v)| {
                (k.contains("mqtt5::") || k.contains("mqdb::"))
                    && !k.to_lowercase().contains("main")
                    && **v > total_samples / 100
            })
            .collect();
        mqtt_funcs.sort_by(|a, b| b.1.cmp(a.1));

        for (func, count) in mqtt_funcs.iter().take(12) {
            let short = if func.len() > 70 { &func[..70] } else { func };
            println!("{count:6} ({:5.2}%) {short}", pct(**count, total_samples));
        }
    }

    pub fn analyze(filepath: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
        use std::io::BufRead;

        let file = std::fs::File::open(filepath)?;
        let reader = std::io::BufReader::new(file);

        let mut inclusive: HashMap<String, u64> = HashMap::new();
        let mut thread_totals: Vec<u64> = Vec::new();
        let mut call_graph_lines: Vec<(usize, u64, String)> = Vec::new();
        let mut in_call_graph = false;

        let thread_re = regex::Regex::new(r"^\s*(\d+)\s+Thread_\d+")?;
        let call_re = regex::Regex::new(r"^(\s*[+!|:\s]*)(\d+)\s+(.+?)\s+\(in\s+([^)]+)\)")?;

        for line in reader.lines() {
            let line = line?;

            if line.contains("Call graph:") {
                in_call_graph = true;
                continue;
            }
            if in_call_graph && line.trim().starts_with("Total number") {
                break;
            }
            if let Some(caps) = thread_re.captures(&line) {
                if let Ok(count) = caps[1].parse::<u64>() {
                    thread_totals.push(count);
                }
                continue;
            }
            if !in_call_graph {
                continue;
            }
            if let Some(caps) = call_re.captures(&line) {
                let prefix = &caps[1];
                let count: u64 = caps[2].parse().unwrap_or(0);
                let func_name = super::demangle_rust_symbol(&caps[3]);
                let library = &caps[4];

                let key = format!("{func_name} [{library}]");
                let depth = prefix.replace(' ', "").len();

                call_graph_lines.push((depth, count, key.clone()));
                let entry = inclusive.entry(key).or_insert(0);
                *entry = (*entry).max(count);
            }
        }

        let exclusive = compute_exclusive(&call_graph_lines);
        let total_samples = thread_totals.iter().max().copied().unwrap_or(1);
        let total_excl: u64 = exclusive.values().sum();

        println!(
            "================================================================================"
        );
        println!("MQDB PROFILE ANALYSIS REPORT");
        println!(
            "================================================================================"
        );
        println!("\nTotal samples (per thread): {total_samples}");
        println!("Total exclusive samples: {total_excl}");

        print_category_breakdown(&exclusive, total_excl);
        print_top_functions(&inclusive, total_samples);

        let waiting: u64 = exclusive
            .iter()
            .filter(|(k, _)| {
                k.contains("__psynch_cvwait")
                    || k.contains("__psynch_mutexwait")
                    || k.contains("kevent")
            })
            .map(|(_, v)| v)
            .sum();
        let active = total_excl.saturating_sub(waiting);

        println!(
            "\n--------------------------------------------------------------------------------"
        );
        println!("SUMMARY");
        println!(
            "--------------------------------------------------------------------------------"
        );
        println!(
            "\nActive work: {active} samples ({:.1}%)",
            pct(active, total_excl)
        );
        println!(
            "Waiting/idle: {waiting} samples ({:.1}%)",
            pct(waiting, total_excl)
        );

        if active < total_excl / 10 {
            println!("\n[INFO] System is mostly idle - try longer benchmark or more clients");
        }

        println!(
            "\n================================================================================"
        );
        Ok(())
    }
}
