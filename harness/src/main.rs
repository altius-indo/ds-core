//! dscore-harness: fault injection and verification for DS-CORE.
//!
//!   dscore-harness fault power-cut --trials 1000 --assert-zero-loss \
//!       [--writes 20] [--data-root DIR --lazyfs-fifo FIFO | --no-lazyfs]
//!       [--unsafe-no-fsync --expect-loss]
//!
//!   dscore-harness fault leader-kill --trials 100 --assert-p99-secs 10
//!
//!   dscore-harness jepsen --workload membership --duration 1h \
//!       --check single-leader,learner-promotion [--max-lag 1000] [--seed N]
//!
//!   dscore-harness jepsen --workload edges --duration 1h --check bidirectional \
//!       [--seed N] [--unsafe-split-writes --expect-violations]
//!
//!   dscore-harness jepsen --workload register --internal-retry on|off --duration 1h \
//!       --check lost-update,serializable
//!
//! See powercut.rs, leaderkill.rs and membership_churn.rs for the methods. `node-group` is
//! the child process the power-cut fault drives.

mod edges;
mod leaderkill;
mod membership_churn;
mod node_group;
mod powercut;
mod register;

/// Parse `90s`, `10m`, `1h` (or plain seconds).
fn parse_duration(s: &str) -> Option<std::time::Duration> {
    let (num, unit) = s.split_at(s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len()));
    let n: u64 = num.parse().ok()?;
    let secs = match unit {
        "" | "s" => n,
        "m" => n * 60,
        "h" => n * 3600,
        _ => return None,
    };
    Some(std::time::Duration::from_secs(secs))
}

async fn jepsen(args: &[String]) -> ExitCode {
    match flag_value(args, "--workload").as_deref() {
        Some("membership") => {}
        Some("edges") => return jepsen_edges(args).await,
        Some("register") => return jepsen_register(args).await,
        other => {
            eprintln!(
                "jepsen: unsupported workload {other:?}; available: membership, edges, register"
            );
            return ExitCode::from(2);
        }
    }
    let Some(duration) = flag_value(args, "--duration")
        .as_deref()
        .and_then(parse_duration)
    else {
        return usage();
    };
    let max_lag = flag_value(args, "--max-lag")
        .and_then(|v| v.parse().ok())
        .unwrap_or(dscore_server::raft::membership::DEFAULT_MAX_PROMOTE_LAG);
    let seed = flag_value(args, "--seed")
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| std::process::id() as u64);
    let checks =
        flag_value(args, "--check").unwrap_or_else(|| "single-leader,learner-promotion".into());
    match membership_churn::run(duration, max_lag, seed).await {
        Ok(r) => {
            let max_seen = r.promotions.iter().map(|(_, l)| *l).max().unwrap_or(0);
            println!(
                "membership: {:?} seed {seed}: {} writes, {} promotions (max lag {max_seen}; observer saw {}, \
                 {} unmeasured), {} removals, {} terms observed, checks [{checks}]",
                duration,
                r.writes_ok,
                r.promotions.len(),
                r.observed_promotions,
                r.unmeasured_promotions,
                r.removals,
                r.terms_observed
            );
            let relevant: Vec<_> = r
                .violations
                .iter()
                .filter(|v| {
                    (checks.contains("single-leader") && v.contains("AC1"))
                        || (checks.contains("learner-promotion") && v.contains("AC2"))
                })
                .collect();
            for v in &relevant {
                eprintln!("violation: {v}");
            }
            if !relevant.is_empty() {
                return ExitCode::FAILURE;
            }
            if r.promotions.is_empty() {
                eprintln!(
                    "FAIL: no membership change completed; the workload did not exercise anything"
                );
                return ExitCode::FAILURE;
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("jepsen: {e}");
            ExitCode::FAILURE
        }
    }
}

use std::path::PathBuf;
use std::process::ExitCode;

fn usage() -> ExitCode {
    eprintln!(
        "usage: dscore-harness fault power-cut --trials N [--writes N] [--assert-zero-loss]\n\
         \x20      [--data-root DIR --lazyfs-fifo FIFO | --no-lazyfs] [--unsafe-no-fsync --expect-loss]\n\
         \x20      dscore-harness --version"
    );
    ExitCode::from(2)
}

fn flag_value(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}

fn has(args: &[String], name: &str) -> bool {
    args.iter().any(|a| a == name)
}

async fn power_cut(args: &[String]) -> ExitCode {
    let trials = flag_value(args, "--trials")
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);
    let writes = flag_value(args, "--writes")
        .and_then(|v| v.parse().ok())
        .unwrap_or(20);
    let lazyfs_fifo = flag_value(args, "--lazyfs-fifo").map(PathBuf::from);
    if lazyfs_fifo.is_none() && !has(args, "--no-lazyfs") {
        eprintln!(
            "power-cut needs --lazyfs-fifo (and --data-root on the LazyFS mount) to drop unsynced \
             data; pass --no-lazyfs to run a process-kill-only trial that cannot satisfy STORY-0001 E2"
        );
        return ExitCode::from(2);
    }
    let data_root = flag_value(args, "--data-root")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::temp_dir().join(format!("dscore-powercut-{}", std::process::id()))
        });
    let opts = powercut::Options {
        trials,
        writes,
        data_root,
        lazyfs_fifo,
        lazyfs_done_fifo: flag_value(args, "--lazyfs-done-fifo").map(PathBuf::from),
        unsafe_no_fsync: has(args, "--unsafe-no-fsync"),
    };
    match powercut::run(&opts).await {
        Ok(o) => {
            println!(
                "power-cut: {} trials, {} acknowledged writes, {} lost ({})",
                o.trials,
                o.acknowledged,
                o.lost,
                if opts.lazyfs_fifo.is_some() {
                    "lazyfs clear-cache"
                } else {
                    "process kill only"
                }
            );
            if has(args, "--assert-zero-loss") && o.lost > 0 {
                eprintln!("FAIL: acknowledged writes were lost");
                return ExitCode::FAILURE;
            }
            if has(args, "--expect-loss") && o.lost == 0 {
                eprintln!(
                    "FAIL: expected losses with --unsafe-no-fsync; the harness is not detecting them"
                );
                return ExitCode::FAILURE;
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("power-cut: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn jepsen_register(args: &[String]) -> ExitCode {
    let Some(duration) = flag_value(args, "--duration")
        .as_deref()
        .and_then(parse_duration)
    else {
        return usage();
    };
    let internal_retry = flag_value(args, "--internal-retry").as_deref() != Some("off");
    let checks = flag_value(args, "--check").unwrap_or_else(|| "lost-update,serializable".into());
    match register::run(duration, internal_retry).await {
        Ok(r) => {
            println!(
                "register: {duration:?} internal-retry {}: {} acknowledged increments, {} internal retries, \
                 {} conflicts returned to clients, checks [{checks}]",
                if internal_retry { "on" } else { "off" },
                r.acknowledged,
                r.retries,
                r.conflicts_returned
            );
            let relevant: Vec<_> = r
                .violations
                .iter()
                .filter(|v| checks.split(',').any(|c| v.starts_with(c)))
                .collect();
            for v in &relevant {
                eprintln!("violation: {v}");
            }
            if !relevant.is_empty() || r.acknowledged == 0 {
                return ExitCode::FAILURE;
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("jepsen register: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn jepsen_edges(args: &[String]) -> ExitCode {
    let Some(duration) = flag_value(args, "--duration")
        .as_deref()
        .and_then(parse_duration)
    else {
        return usage();
    };
    let seed = flag_value(args, "--seed")
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| std::process::id() as u64);
    let unsafe_split = has(args, "--unsafe-split-writes");
    match edges::run(duration, seed, unsafe_split).await {
        Ok(r) => {
            println!(
                "edges: {duration:?} seed {seed}: {} committed ops, {} conflicts, {} snapshot checks \
                 (up to {} edges), {} violations{}",
                r.committed,
                r.conflicts,
                r.checks,
                r.max_edges_seen,
                r.violations.len(),
                if unsafe_split {
                    " [unsafe split writes]"
                } else {
                    ""
                }
            );
            for v in r.violations.iter().take(5) {
                eprintln!("violation: {v} (REQ-0010 AC2)");
            }
            if has(args, "--expect-violations") {
                if r.violations.is_empty() {
                    eprintln!(
                        "FAIL: expected one-sided edges with --unsafe-split-writes; the checker missed them"
                    );
                    return ExitCode::FAILURE;
                }
                return ExitCode::SUCCESS;
            }
            if !r.violations.is_empty() || r.committed == 0 || r.checks == 0 {
                return ExitCode::FAILURE;
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("jepsen edges: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn leader_kill(args: &[String]) -> ExitCode {
    let trials: usize = flag_value(args, "--trials")
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);
    let limit: Option<f64> = flag_value(args, "--assert-p99-secs").and_then(|v| v.parse().ok());
    let mut timing = dscore_server::raft::config::Timing::default();
    let ms = |name| {
        flag_value(args, name)
            .and_then(|v| v.parse().ok())
            .map(std::time::Duration::from_millis)
    };
    timing.heartbeat = ms("--heartbeat-ms").unwrap_or(timing.heartbeat);
    timing.election_min = ms("--election-min-ms").unwrap_or(timing.election_min);
    timing.election_max = ms("--election-max-ms").unwrap_or(timing.election_max);
    match leaderkill::run(trials, timing).await {
        Ok(o) if !o.samples.is_empty() => {
            let (p50, p99, max) = (o.percentile(50.0), o.percentile(99.0), o.percentile(100.0));
            println!(
                "leader-kill: {} trials, kill-to-first-write p50 {:.2}s p99 {:.2}s max {:.2}s \
                 (heartbeat {:?}, election {:?}..{:?})",
                o.samples.len(),
                p50.as_secs_f64(),
                p99.as_secs_f64(),
                max.as_secs_f64(),
                timing.heartbeat,
                timing.election_min,
                timing.election_max
            );
            match limit {
                Some(l) if p99.as_secs_f64() > l => {
                    eprintln!(
                        "FAIL: p99 {:.2}s exceeds {l}s (REQ-0018 AC1)",
                        p99.as_secs_f64()
                    );
                    ExitCode::FAILURE
                }
                _ => ExitCode::SUCCESS,
            }
        }
        Ok(_) => usage(),
        Err(e) => {
            eprintln!("leader-kill: {e}");
            ExitCode::FAILURE
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["--version"] | [] => {
            println!("dscore-harness {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        ["fault", "power-cut", ..] => power_cut(&args[2..]).await,
        ["fault", "leader-kill", ..] => leader_kill(&args[2..]).await,
        ["jepsen", ..] => jepsen(&args[1..]).await,
        ["node-group", ..] => {
            let Some(dir) = flag_value(&args, "--dir") else {
                return usage();
            };
            match node_group::run(&PathBuf::from(dir), has(&args, "--unsafe-no-fsync")).await {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("node-group: {e}");
                    ExitCode::FAILURE
                }
            }
        }
        _ => usage(),
    }
}
