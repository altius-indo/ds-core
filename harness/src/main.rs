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
//!   dscore-harness jepsen --workload list-append --nemesis partition,crash,clock,split \
//!       --duration 1h --check serializable [--rate 50] [--elle-jar PATH | ELLE_JAR]
//!       [--history FILE]
//!
//!   dscore-harness jepsen --workload splits --duration 1h --check serializable \
//!       [--split-budget 60] [--nemesis split,crash,partition] [--elle-jar PATH | ELLE_JAR]
//!   (list-append under continuous range splits, paced to last the run: REQ-0033 AC2)
//!
//!   dscore-harness scenario add-node --workload ycsb-a --assert-balance 0.10 --within 1h \
//!       --assert-zero-errors [--records 4000] [--max-range-bytes 16384] [--rate 200]
//!       [--settle 10s] [--move-interval 1s] [--seed N]
//!
//! See powercut.rs, leaderkill.rs, membership_churn.rs and add_node.rs for the methods. `node-group` is
//! the child process the power-cut fault drives.

mod add_node;
mod edges;
mod leaderkill;
mod list_append;
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
        Some("list-append") => return jepsen_list_append(args, false).await,
        Some("splits") => return jepsen_list_append(args, true).await,
        other => {
            eprintln!(
                "jepsen: unsupported workload {other:?}; available: membership, edges, register, list-append, splits"
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
use std::time::Duration;

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

async fn jepsen_list_append(args: &[String], splits_workload: bool) -> ExitCode {
    let Some(duration) = flag_value(args, "--duration")
        .as_deref()
        .and_then(parse_duration)
    else {
        return usage();
    };
    let default_nemeses = if splits_workload {
        "split,crash,partition"
    } else {
        ""
    };
    let nemeses = match list_append::parse_nemeses(
        &flag_value(args, "--nemesis").unwrap_or_else(|| default_nemeses.into()),
    ) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("jepsen list-append: {e}");
            return ExitCode::from(2);
        }
    };
    let consistency = flag_value(args, "--check").unwrap_or_else(|| "serializable".into());
    let jar = flag_value(args, "--elle-jar")
        .or_else(|| std::env::var("ELLE_JAR").ok())
        .map(PathBuf::from);
    let Some(jar) = jar else {
        eprintln!(
            "jepsen list-append: needs elle-cli (--elle-jar PATH or ELLE_JAR) to check the history"
        );
        return ExitCode::from(2);
    };
    let history = flag_value(args, "--history")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::temp_dir().join(format!("dscore-list-append-{}.edn", std::process::id()))
        });
    let seed = flag_value(args, "--seed")
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| std::process::id() as u64);
    let split_budget = splits_workload.then(|| {
        flag_value(args, "--split-budget")
            .and_then(|v| v.parse().ok())
            .unwrap_or(60)
    });
    if splits_workload && !nemeses.contains(&list_append::Nemesis::Split) {
        eprintln!("jepsen splits: the split nemesis is the point of this workload");
        return ExitCode::from(2);
    }
    let rate = flag_value(args, "--rate")
        .and_then(|v| v.parse().ok())
        .unwrap_or(50.0);
    let opts = list_append::Options {
        duration,
        nemeses: nemeses.clone(),
        split_budget,
        rate,
        history,
        seed,
    };
    let r = match list_append::run(&opts).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("jepsen list-append: {e}");
            return ExitCode::FAILURE;
        }
    };
    println!(
        "list-append: {duration:?} seed {seed} nemeses {nemeses:?}: {} ok, {} failed, {} unknown; {} faults injected ({} splits); history {}",
        r.ok,
        r.failed,
        r.unknown,
        r.faults.len(),
        r.splits,
        r.history.display()
    );
    // At least one split per 2 minutes of run, so a broken split path cannot pass silently.
    if splits_workload && r.splits < (duration.as_secs() / 120).max(1) as usize {
        eprintln!(
            "FAIL: only {} splits completed; the workload did not exercise continuous splits",
            r.splits
        );
        return ExitCode::FAILURE;
    }
    match list_append::elle(&jar, &r.history, &consistency) {
        Ok((true, _)) if r.ok > 0 => {
            println!("elle: history is {consistency}");
            ExitCode::SUCCESS
        }
        Ok((true, _)) => {
            eprintln!("FAIL: no transaction committed; the history proves nothing");
            ExitCode::FAILURE
        }
        Ok((false, text)) => {
            eprintln!("FAIL: elle found the history is not {consistency} (REQ-0014 AC2):\n{text}");
            ExitCode::FAILURE
        }
        Err(e) => {
            eprintln!("jepsen list-append: {e}");
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

async fn scenario_add_node(args: &[String]) -> ExitCode {
    if flag_value(args, "--workload").as_deref() != Some("ycsb-a") {
        eprintln!("scenario add-node: only --workload ycsb-a is supported");
        return ExitCode::from(2);
    }
    let num = |name: &str, default: f64| {
        flag_value(args, name)
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(default)
    };
    let dur = |name: &str, default: Duration| {
        flag_value(args, name)
            .as_deref()
            .and_then(parse_duration)
            .unwrap_or(default)
    };
    let opts = add_node::Options {
        records: num("--records", 4000.0) as u64,
        max_range_bytes: num("--max-range-bytes", 16384.0) as u64,
        balance: num("--assert-balance", 0.10),
        within: dur("--within", Duration::from_secs(3600)),
        rate: num("--rate", 200.0),
        settle: dur("--settle", Duration::from_secs(10)),
        move_interval: dur("--move-interval", Duration::from_secs(1)),
        seed: num("--seed", std::process::id() as f64) as u64,
    };
    let dir = std::env::temp_dir().join(format!("dscore-add-node-{}", std::process::id()));
    let r = add_node::run(&dir, &opts).await;
    let _ = std::fs::remove_dir_all(&dir);
    let r = match r {
        Ok(r) => r,
        Err(e) => {
            eprintln!("scenario add-node: {e}");
            return ExitCode::FAILURE;
        }
    };
    println!(
        "add-node: {} ranges, {} moves, balanced {} (replicas {:?}, max deviation {:.1}%); \
         {} YCSB-A requests, {} retried, {} failed; {} records lost",
        r.ranges,
        r.moves,
        r.balanced_after
            .map_or("never".to_string(), |d| format!("after {d:?}")),
        r.counts,
        r.deviation * 100.0,
        r.ops,
        r.retried,
        r.failed,
        r.lost
    );
    let mut ok = true;
    if r.balanced_after.is_none() || r.deviation > opts.balance {
        eprintln!(
            "FAIL: replica counts not within {:.0}% of the mean within {:?} (REQ-0033 AC1)",
            opts.balance * 100.0,
            opts.within
        );
        ok = false;
    }
    if has(args, "--assert-zero-errors") && r.failed > 0 {
        eprintln!(
            "FAIL: {} client requests failed (REQ-0033 AC1); first: {}",
            r.failed,
            r.first_failure.unwrap_or_default()
        );
        ok = false;
    }
    if r.lost > 0 {
        eprintln!("FAIL: {} preloaded records missing after the moves", r.lost);
        ok = false;
    }
    if r.moves == 0 {
        eprintln!("FAIL: no replica moved; the scenario exercised nothing");
        ok = false;
    }
    if ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
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
        ["scenario", "add-node", ..] => scenario_add_node(&args[2..]).await,
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
