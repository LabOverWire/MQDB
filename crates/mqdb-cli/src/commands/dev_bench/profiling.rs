// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus};
use std::time::{Duration, Instant};

use super::helpers::{
    agent_args, broker_listening, chrono_timestamp, kill_processes_below, mqdb_agents_below,
    signal_pids,
};
use super::runners::build_bench_args;
use crate::cli_types::DevBenchScenario;

pub(crate) struct ProfileRun<'a> {
    pub(crate) tool: &'a str,
    pub(crate) duration: u64,
    pub(crate) output: Option<PathBuf>,
    pub(crate) db: &'a str,
    pub(crate) port: u16,
}

enum ProfileTool {
    Samply,
    Flamegraph,
    Sample,
}

impl ProfileTool {
    fn parse(tool: &str) -> Result<Self, String> {
        match tool {
            "samply" => Ok(Self::Samply),
            "flamegraph" => Ok(Self::Flamegraph),
            "sample" => Ok(Self::Sample),
            _ => Err(format!(
                "Unknown profiling tool: {tool}. Use 'sample', 'samply', or 'flamegraph'"
            )),
        }
    }
}

struct ProfileTarget<'a> {
    db: &'a str,
    port: u16,
    output_path: PathBuf,
    bench_args: Vec<String>,
}

pub(crate) fn cmd_dev_profile(
    scenario: &DevBenchScenario,
    run: &ProfileRun,
) -> Result<(), Box<dyn std::error::Error>> {
    let tool = ProfileTool::parse(run.tool)?;
    let port = run.port;
    refuse_busy_target(run.db, port)?;
    std::fs::create_dir_all(run.db)?;

    let timestamp = chrono_timestamp();
    let output_path = run.output.clone().unwrap_or_else(|| {
        let dir = PathBuf::from(".claude/profiles");
        let _ = std::fs::create_dir_all(&dir);
        match tool {
            ProfileTool::Flamegraph => dir.join(format!("profile_{timestamp}.svg")),
            ProfileTool::Samply | ProfileTool::Sample => dir.join(format!("profile_{timestamp}")),
        }
    });

    println!("Profiling with {}...", run.tool);
    println!("Output will be saved to: {}", output_path.display());

    let mut bench_args = build_bench_args(scenario);
    bench_args.extend(["--broker".to_string(), format!("127.0.0.1:{port}")]);
    let target = ProfileTarget {
        db: run.db,
        port,
        output_path,
        bench_args,
    };

    match tool {
        ProfileTool::Samply => profile_with_samply(&target),
        ProfileTool::Flamegraph => profile_with_flamegraph(&target),
        ProfileTool::Sample => profile_with_sample(&target, run.duration),
    }
}

const PROFILER_EXIT_TIMEOUT: Duration = Duration::from_secs(30);

fn refuse_busy_target(db: &str, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    if broker_listening(port) {
        return Err(
            format!("port {port} is already in use; profile on a free port with --port").into(),
        );
    }
    crate::commands::dev::refuse_db_in_use(db)?;
    Ok(())
}

fn build_profiling() -> Result<(), Box<dyn std::error::Error>> {
    println!("\nBuilding with profiling profile (full DWARF symbols)...");
    let build_status = Command::new("cargo")
        .args(["build", "--profile=profiling"])
        .status()?;
    if !build_status.success() {
        return Err("Failed to build with profiling profile".into());
    }
    Ok(())
}

fn build_profiling_binary() -> Result<PathBuf, Box<dyn std::error::Error>> {
    build_profiling()?;
    let profiling_exe = std::env::current_dir()?.join("target/profiling/mqdb");
    if !profiling_exe.is_file() {
        return Err(format!(
            "profiling build has no {}; run dev profile from the workspace root",
            profiling_exe.display()
        )
        .into());
    }
    Ok(profiling_exe)
}

fn wait_with_timeout(child: &mut Child, timeout: Duration) -> std::io::Result<Option<ExitStatus>> {
    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(Some(status));
        }
        if start.elapsed() > timeout {
            return Ok(None);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

struct OwnedChild {
    child: Child,
    name: &'static str,
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        let stopped = match self.child.try_wait() {
            Ok(Some(_)) => Ok(()),
            Ok(None) => self.child.kill().and_then(|()| self.child.wait().map(drop)),
            Err(e) => Err(e),
        };
        if let Err(e) = stopped {
            println!("Warning: could not stop the {}: {e}", self.name);
        }
    }
}

struct Profiler {
    child: Child,
    name: &'static str,
    db: String,
    stopped: bool,
}

impl Profiler {
    fn agents(&self) -> Result<Vec<u32>, String> {
        let mut agents = mqdb_agents_below(self.child.id())?;
        for pid in crate::commands::dev::pids_using_db(&self.db)? {
            if !agents.contains(&pid) {
                agents.push(pid);
            }
        }
        Ok(agents)
    }

    fn force_stop(&mut self) -> Vec<String> {
        let pid = self.child.id();
        let mut problems = Vec::new();
        if let Err(e) = kill_processes_below(pid) {
            problems.push(format!(
                "could not stop the processes under {}: {e}",
                self.name
            ));
        }
        if let Err(e) = self.child.kill().and_then(|()| self.child.wait().map(drop)) {
            problems.push(format!("could not stop {} (pid {pid}): {e}", self.name));
        }
        match crate::commands::dev::pids_using_db(&self.db)
            .and_then(|left| signal_pids("-KILL", &left))
        {
            Ok(()) => {}
            Err(e) => problems.push(format!("could not stop the agent on --db {}: {e}", self.db)),
        }
        problems
    }

    fn stop(&mut self) -> Result<(), String> {
        if self.stopped {
            return Ok(());
        }
        self.stopped = true;
        println!("\nStopping agent to save profile...");
        let agents = match self.agents() {
            Ok(agents) if agents.is_empty() => Err(format!(
                "no agent found under {} or on --db {}",
                self.name, self.db
            )),
            Ok(agents) => signal_pids("-INT", &agents)
                .map_err(|e| format!("could not interrupt the profiled agent: {e}")),
            Err(e) => Err(e),
        };
        let exited = match agents {
            Ok(()) => wait_with_timeout(&mut self.child, PROFILER_EXIT_TIMEOUT)
                .map_err(|e| format!("could not wait for {}: {e}", self.name))
                .and_then(|status| {
                    status.ok_or_else(|| {
                        format!(
                            "{} did not exit within {PROFILER_EXIT_TIMEOUT:?}",
                            self.name
                        )
                    })
                }),
            Err(e) => Err(e),
        };
        match exited {
            Ok(status) => {
                println!("{} exited with: {status}", self.name);
                let left = crate::commands::dev::pids_using_db(&self.db)?;
                if !left.is_empty() {
                    signal_pids("-KILL", &left)?;
                    println!(
                        "Warning: killed the agent still running on --db {}",
                        self.db
                    );
                }
                Ok(())
            }
            Err(e) => {
                let problems = self.force_stop();
                Err(std::iter::once(format!(
                    "{e}; killed {} before it finished, so its output may be missing",
                    self.name
                ))
                .chain(problems)
                .collect::<Vec<_>>()
                .join("; "))
            }
        }
    }

    fn finish(mut self) -> Result<(), String> {
        self.stop()
    }
}

impl Drop for Profiler {
    fn drop(&mut self) {
        if let Err(e) = self.stop() {
            println!("Warning: {e}");
        }
    }
}

fn run_bench(profiling_exe: &Path, target: &ProfileTarget) -> std::io::Result<()> {
    println!("\nRunning benchmark...");
    let bench_status = Command::new(profiling_exe)
        .args(&target.bench_args)
        .status()?;
    if !bench_status.success() {
        println!("Benchmark failed with status: {bench_status}");
    }
    Ok(())
}

fn profile_with_samply(target: &ProfileTarget) -> Result<(), Box<dyn std::error::Error>> {
    let profiling_exe = build_profiling_binary()?;
    refuse_busy_target(target.db, target.port)?;
    let symbol_dir = std::env::current_dir()?.join("target/profiling");
    println!("Starting agent under samply profiler...");

    let samply = Profiler {
        child: Command::new("samply")
            .args([
                "record",
                "--save-only",
                "--unstable-presymbolicate",
                "--symbol-dir",
            ])
            .arg(&symbol_dir)
            .arg("-o")
            .arg(&target.output_path)
            .arg("--")
            .arg(&profiling_exe)
            .args(agent_args(target.db, target.port))
            .spawn()?,
        name: "Samply",
        db: target.db.to_string(),
        stopped: false,
    };

    std::thread::sleep(Duration::from_secs(3));
    let bench = run_bench(&profiling_exe, target);
    let stopped = samply.finish();
    bench?;
    stopped?;

    println!("\nProfile saved: {}", target.output_path.display());
    println!("Load with: samply load {}", target.output_path.display());
    Ok(())
}

fn profile_with_flamegraph(target: &ProfileTarget) -> Result<(), Box<dyn std::error::Error>> {
    build_profiling()?;
    refuse_busy_target(target.db, target.port)?;
    let bench_exe = std::env::current_exe()?;

    println!("Starting agent under flamegraph profiler...\n");

    let flamegraph = Profiler {
        child: Command::new("cargo")
            .args(["flamegraph", "--profile=profiling", "-o"])
            .arg(&target.output_path)
            .arg("--")
            .args(agent_args(target.db, target.port))
            .spawn()?,
        name: "Flamegraph",
        db: target.db.to_string(),
        stopped: false,
    };

    std::thread::sleep(Duration::from_secs(2));
    let bench = run_bench(&bench_exe, target);
    let stopped = flamegraph.finish();
    bench?;
    stopped?;

    println!("\nFlamegraph saved: {}", target.output_path.display());
    Ok(())
}

fn profile_with_sample(
    target: &ProfileTarget,
    duration: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let profiling_exe = build_profiling_binary()?;
    refuse_busy_target(target.db, target.port)?;

    println!("Starting agent...");
    let agent = OwnedChild {
        child: Command::new(&profiling_exe)
            .args(agent_args(target.db, target.port))
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()?,
        name: "agent",
    };

    std::thread::sleep(Duration::from_secs(2));

    let agent_pid = agent.child.id();
    println!("Agent started with PID: {agent_pid}");

    println!("\nRunning benchmark in background...");
    let bench = OwnedChild {
        child: Command::new(&profiling_exe)
            .args(&target.bench_args)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()?,
        name: "benchmark",
    };

    std::thread::sleep(Duration::from_millis(500));

    println!("Sampling for {duration} seconds...");
    let sample_output = target.output_path.with_extension("txt");
    let sample_status = Command::new("sample")
        .args([&agent_pid.to_string(), &duration.to_string(), "-f"])
        .arg(&sample_output)
        .status();

    println!("\nStopping benchmark and agent...");
    drop(bench);
    drop(agent);

    let sample_status = sample_status?;
    if !sample_status.success() {
        println!("Warning: sample command failed with status: {sample_status}");
    }

    println!("\nProfile saved: {}", sample_output.display());

    if sample_output.exists() {
        println!("\nGenerating analysis report...\n");
        analyze_sample_output(&sample_output)?;
    }

    Ok(())
}

fn analyze_sample_output(filepath: &Path) -> Result<(), Box<dyn std::error::Error>> {
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
        let as_f64 = |n: u64| f64::from(u32::try_from(n).unwrap_or(u32::MAX));
        as_f64(num) / as_f64(denom) * 100.0
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
