//! `jepsen --workload list-append --nemesis partition,crash,clock --duration 1h --check serializable`
//! (REQ-0014 AC2, STORY-0005 E2).
//!
//! Workers run transactions of 1-4 micro-operations over 8 keys spread across 3 ranges: append
//! a unique value to a key's list, or read a key's list. Each key is retired after 128 appends
//! for a fresh one, and workers are paced (`--rate`, transactions per second), so lists and
//! the history stay small enough for Elle to check an hour-long run. Every invocation and completion is
//! recorded as a Jepsen history: `:ok` committed, `:fail` definitely not committed (a
//! serialization conflict, always raised before the commit point), `:info` unknown (the
//! cluster timed out, so the transaction may or may not have committed). A nemesis meanwhile
//! partitions a node away, crashes and restarts nodes, and skews clients' clocks. The history
//! goes to Elle (elle-cli, list-append model), which infers each key's version order from the
//! lists and searches the dependency graph for cycles: any G0, G1a/b/c, G-single or G2
//! anomaly fails the serializable check.
//!
//! The `split` nemesis splits the range owning a random key at (or just past) that key, under
//! load; splits are permanent, so the range count grows over the run and in-flight
//! transactions see their routing go stale mid-commit (REQ-0033, STORY-0004 E2). Not yet
//! available: the `membership` nemesis (per-range replica sets arrive with TASK-0008 step 2).

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dscore_server::txn::cluster::Cluster;
use dscore_server::txn::coordinator::{TxnClient, TxnConfig};
use dscore_server::txn::error::ErrorCode;
use openraft::Config;
use tokio::sync::Mutex;

const KEYS: usize = 8;
const WORKERS: u64 = 6;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Nemesis {
    Partition,
    Crash,
    Clock,
    Split,
}

pub fn parse_nemeses(s: &str) -> Result<Vec<Nemesis>, String> {
    let mut out = Vec::new();
    for n in s.split(',').filter(|n| !n.is_empty()) {
        out.push(match n {
            "partition" => Nemesis::Partition,
            "crash" => Nemesis::Crash,
            "clock" => Nemesis::Clock,
            "split" => Nemesis::Split,
            "membership" => {
                return Err("nemesis `membership` is not available yet: per-range replica sets arrive with TASK-0008".into());
            }
            other => return Err(format!("unknown nemesis `{other}`")),
        });
    }
    Ok(out)
}

pub struct Report {
    pub splits: usize,
    pub ok: usize,
    pub failed: usize,
    pub unknown: usize,
    pub faults: Vec<String>,
    pub history: PathBuf,
}

fn key(i: usize) -> Vec<u8> {
    // Key ids spread over the three initial ranges, split at "g" and "p".
    format!("{}/la/{i}", ["a", "h", "q"][i % 3]).into_bytes()
}

enum Op {
    Append(usize, i64),
    Read(usize, Option<Vec<i64>>),
}

fn edn(ops: &[Op], with_reads: bool) -> String {
    let mut s = String::from("[");
    for (i, op) in ops.iter().enumerate() {
        if i > 0 {
            s.push(' ');
        }
        match op {
            Op::Append(k, v) => {
                let _ = write!(s, "[:append {k} {v}]");
            }
            Op::Read(k, Some(list)) if with_reads => {
                let items: Vec<String> = list.iter().map(i64::to_string).collect();
                let _ = write!(s, "[:r {k} [{}]]", items.join(" "));
            }
            Op::Read(k, _) => {
                let _ = write!(s, "[:r {k} nil]");
            }
        }
    }
    s.push(']');
    s
}

struct History {
    lines: Mutex<Vec<String>>,
    start: Instant,
}

impl History {
    async fn push(&self, kind: &str, process: u64, value: String) {
        let mut l = self.lines.lock().await;
        let index = l.len();
        let time = self.start.elapsed().as_nanos();
        l.push(format!("{{:index {index} :type :{kind} :f :txn :process {process} :time {time} :value {value}}}"));
    }
}

fn decode(b: Option<Vec<u8>>) -> Vec<i64> {
    b.and_then(|b| postcard::from_bytes(&b).ok())
        .unwrap_or_default()
}

/// One transaction. `Ok(Some(ops))` committed with these reads, `Ok(None)` definitely failed,
/// `Err(())` unknown.
async fn run_txn(client: &TxnClient, mut ops: Vec<Op>) -> Result<Option<Vec<Op>>, ()> {
    let Ok(mut t) = client.begin().await else {
        return Ok(None);
    };
    for op in ops.iter_mut() {
        let r = match op {
            Op::Append(k, v) => match t.get(&key(*k)).await {
                Ok(cur) => {
                    let mut list = decode(cur);
                    list.push(*v);
                    t.put(
                        &key(*k),
                        &postcard::to_allocvec(&list).expect("list encodes"),
                    );
                    Ok(())
                }
                Err(e) => Err(e),
            },
            Op::Read(k, out) => t.get(&key(*k)).await.map(|v| *out = Some(decode(v))),
        };
        if let Err(e) = r {
            // Reads happen before commit: nothing was written.
            return if e.code == ErrorCode::Unavailable {
                Err(())
            } else {
                Ok(None)
            };
        }
    }
    match t.commit().await {
        Ok(_) => Ok(Some(ops)),
        Err(e) if e.code == ErrorCode::SerializationConflict => Ok(None),
        Err(_) => Err(()),
    }
}

static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

fn log_fault(faults: &mut Vec<String>, f: String) {
    eprintln!(
        "[{:?}] nemesis: {f}",
        START.get_or_init(Instant::now).elapsed()
    );
    faults.push(f);
}

/// The keys in play: `KEYS` slots, each holding a key id that is retired for a fresh one after
/// `MAX_WRITES_PER_KEY` appends, as Jepsen's list-append does. Bounded lists keep each read
/// (and so the history Elle must load) small however long the run.
struct Keys {
    slot: Vec<AtomicU64>,
    writes: Vec<AtomicU64>,
    next: AtomicU64,
}

const MAX_WRITES_PER_KEY: u64 = 128;

impl Keys {
    fn new() -> Self {
        Keys {
            slot: (0..KEYS as u64).map(AtomicU64::new).collect(),
            writes: (0..KEYS).map(|_| AtomicU64::new(0)).collect(),
            next: AtomicU64::new(KEYS as u64),
        }
    }

    fn for_read(&self, s: usize) -> usize {
        self.slot[s].load(Ordering::Relaxed) as usize
    }

    fn for_append(&self, s: usize) -> usize {
        let id = self.slot[s].load(Ordering::Relaxed);
        if self.writes[s].fetch_add(1, Ordering::Relaxed) + 1 == MAX_WRITES_PER_KEY {
            self.slot[s].store(self.next.fetch_add(1, Ordering::Relaxed), Ordering::Relaxed);
            self.writes[s].store(0, Ordering::Relaxed);
        }
        id as usize
    }

    /// A key id in use recently, for placing split points among live data.
    fn recent(&self, r: u64) -> usize {
        let n = self.next.load(Ordering::Relaxed);
        (n - 1 - r % n.min(2 * KEYS as u64)) as usize
    }
}

/// A split point next to a recent workload key, with a random suffix: a fresh boundary almost
/// every time, so later splits cut ranges that earlier splits created.
async fn split_near(cluster: &Cluster, keys: &Keys, r1: u64, r2: u64) -> (bool, String) {
    let mut at = key(keys.recent(r1));
    at.extend_from_slice(format!("/{}", r2 % 1000).as_bytes());
    let range = cluster.range_for(&at).id;
    let label = String::from_utf8_lossy(&at).into_owned();
    match cluster.split(range, &at).await {
        Ok(child) => (true, format!("split range {range} at {label} -> {child}")),
        // Already a boundary: not a fault.
        Err(e) => (
            false,
            format!("split range {range} at {label} skipped: {e}"),
        ),
    }
}

fn xorshift(mut rng: u64) -> impl FnMut() -> u64 {
    move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    }
}

pub struct Options {
    pub duration: Duration,
    pub nemeses: Vec<Nemesis>,
    /// `Some(n)`: the split nemesis runs on its own timer, `n` splits spread evenly over the
    /// run, concurrently with the other nemeses, so splits continue to the end without the
    /// range count growing unbounded (ranges never merge). `None`: a split is one of the
    /// sequential faults, like the others.
    pub split_budget: Option<usize>,
    /// Transactions per second across all workers; 0 is unlimited.
    pub rate: f64,
    pub history: PathBuf,
    pub seed: u64,
}

pub async fn run(o: &Options) -> Result<Report, String> {
    let dir = std::env::temp_dir().join(format!("dscore-list-append-{}", std::process::id()));
    let config = Config {
        heartbeat_interval: 50,
        election_timeout_min: 300,
        election_timeout_max: 600,
        ..Default::default()
    };
    let cluster = Arc::new(Cluster::start(&dir, 3, &[b"g", b"p"], config).await?);
    let base = TxnClient::new(
        cluster.clone(),
        TxnConfig {
            liveness_ttl: Duration::from_millis(1500),
            lock_wait: Duration::from_millis(1000),
        },
    );
    let history = Arc::new(History {
        lines: Mutex::new(Vec::new()),
        start: Instant::now(),
    });
    let keys = Arc::new(Keys::new());
    let stop = Arc::new(AtomicBool::new(false));
    let next_value = Arc::new(AtomicU64::new(1));
    let (ok, failed, unknown) = (
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
    );
    let clients: Vec<TxnClient> = (0..WORKERS).map(|_| base.fork_clock()).collect();
    let pace = (o.rate > 0.0).then(|| Duration::from_secs_f64(WORKERS as f64 / o.rate));

    let mut workers = Vec::new();
    for (p, client) in clients.iter().enumerate() {
        let (client, history, stop, next_value, keys) = (
            client.clone(),
            history.clone(),
            stop.clone(),
            next_value.clone(),
            keys.clone(),
        );
        let (ok, failed, unknown) = (ok.clone(), failed.clone(), unknown.clone());
        let mut next = xorshift(o.seed ^ ((p as u64 + 1) * 0x9E37_79B9_7F4A_7C15) | 1);
        workers.push(tokio::spawn(async move {
            let mut due = tokio::time::Instant::now();
            while !stop.load(Ordering::Relaxed) {
                if let Some(pace) = pace {
                    due += pace;
                    tokio::time::sleep_until(due).await;
                }
                let n = 1 + (next() % 4) as usize;
                let ops: Vec<Op> = (0..n)
                    .map(|_| {
                        let s = (next() % KEYS as u64) as usize;
                        if next().is_multiple_of(2) {
                            Op::Append(
                                keys.for_append(s),
                                next_value.fetch_add(1, Ordering::Relaxed) as i64,
                            )
                        } else {
                            Op::Read(keys.for_read(s), None)
                        }
                    })
                    .collect();
                let invoke = edn(&ops, false);
                history.push("invoke", p as u64, invoke.clone()).await;
                match run_txn(&client, ops).await {
                    Ok(Some(done)) => {
                        history.push("ok", p as u64, edn(&done, true)).await;
                        ok.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(None) => {
                        history.push("fail", p as u64, invoke).await;
                        failed.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(()) => {
                        history.push("info", p as u64, invoke).await;
                        unknown.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    let deadline = tokio::time::Instant::now() + o.duration;

    // Continuous splits on their own timer, overlapping the other faults.
    let splitter = o.split_budget.filter(|b| *b > 0).map(|budget| {
        let (cluster, keys) = (cluster.clone(), keys.clone());
        let every = o.duration / budget as u32;
        let mut next = xorshift(o.seed.rotate_left(17) | 1);
        tokio::spawn(async move {
            let (mut done, mut log) = (0, Vec::new());
            loop {
                tokio::time::sleep(every).await;
                if tokio::time::Instant::now() >= deadline {
                    break;
                }
                let (split, line) = split_near(&cluster, &keys, next(), next()).await;
                done += split as usize;
                log_fault(&mut log, line);
            }
            (done, log)
        })
    });

    // Sequential nemesis: one fault every few seconds, each healed before the next.
    let sequential: Vec<Nemesis> = o
        .nemeses
        .iter()
        .copied()
        .filter(|n| *n != Nemesis::Split || o.split_budget.is_none())
        .collect();
    let mut faults = Vec::new();
    let mut splits = 0;
    let mut next = xorshift(o.seed | 1);
    let nodes = cluster.node_ids();
    while tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_secs(2)).await;
        if sequential.is_empty() || tokio::time::Instant::now() >= deadline {
            continue;
        }
        let victim = nodes[(next() % nodes.len() as u64) as usize];
        match sequential[(next() % sequential.len() as u64) as usize] {
            Nemesis::Partition => {
                let rest: Vec<u64> = nodes.iter().copied().filter(|n| *n != victim).collect();
                cluster.partition(&[&[victim], &rest]);
                log_fault(&mut faults, format!("partition {victim} | {rest:?}"));
                tokio::time::sleep(Duration::from_secs(3)).await;
                cluster.heal();
            }
            Nemesis::Crash => {
                cluster.crash(victim).await;
                log_fault(&mut faults, format!("crash {victim}"));
                tokio::time::sleep(Duration::from_secs(2)).await;
                cluster.restart(victim).await?;
            }
            Nemesis::Clock => {
                let c = (next() % clients.len() as u64) as usize;
                let skew = (next() % 20_000) as i64 - 10_000;
                clients[c].set_clock_skew_ms(skew);
                log_fault(&mut faults, format!("clock client {c} skew {skew} ms"));
                tokio::time::sleep(Duration::from_secs(3)).await;
                clients[c].set_clock_skew_ms(0);
            }
            Nemesis::Split => {
                let (split, line) = split_near(&cluster, &keys, next(), next()).await;
                splits += split as usize;
                log_fault(&mut faults, line);
            }
        }
    }
    if let Some(s) = splitter {
        let (done, log) = s.await.map_err(|e| e.to_string())?;
        splits += done;
        faults.extend(log);
    }
    stop.store(true, Ordering::Relaxed);
    for w in workers {
        let _ = w.await;
    }

    // Final read of every key in play, healed, so Elle sees the final lists.
    cluster.heal();
    for c in &clients {
        c.set_clock_skew_ms(0);
    }
    let final_ops = || -> Vec<Op> {
        (0..KEYS)
            .map(|s| Op::Read(keys.for_read(s), None))
            .collect()
    };
    history
        .push("invoke", WORKERS, edn(&final_ops(), false))
        .await;
    let mut final_ok = false;
    for _ in 0..20 {
        if let Ok(Some(done)) = run_txn(&base, final_ops()).await {
            history.push("ok", WORKERS, edn(&done, true)).await;
            final_ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    if !final_ok {
        history
            .push("info", WORKERS, edn(&final_ops(), false))
            .await;
    }
    cluster.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);

    let lines = history.lines.lock().await;
    std::fs::write(&o.history, lines.join("\n") + "\n")
        .map_err(|e| format!("{}: {e}", o.history.display()))?;
    Ok(Report {
        splits,
        ok: ok.load(Ordering::Relaxed) as usize,
        failed: failed.load(Ordering::Relaxed) as usize,
        unknown: unknown.load(Ordering::Relaxed) as usize,
        faults,
        history: o.history.clone(),
    })
}

/// Run elle-cli on the history; Ok(true) when the history is serializable.
pub fn elle(jar: &Path, history: &Path, consistency: &str) -> Result<(bool, String), String> {
    let out = std::process::Command::new("java")
        .args(["-Xmx6g", "-jar"])
        .arg(jar)
        .args([
            "--model",
            "list-append",
            "--consistency-models",
            consistency,
        ])
        .arg(history)
        .output()
        .map_err(|e| format!("running elle-cli: {e}"))?;
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let verdict = text
        .lines()
        .rev()
        .find_map(|l| {
            l.split_whitespace()
                .last()
                .filter(|w| ["true", "false", "unknown"].contains(w))
        })
        .ok_or_else(|| format!("no verdict from elle-cli:\n{text}"))?;
    Ok((verdict == "true", text))
}
