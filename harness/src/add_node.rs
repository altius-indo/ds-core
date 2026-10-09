//! `scenario add-node --workload ycsb-a --assert-balance 0.10 --within 1h --assert-zero-errors`
//! (REQ-0033 AC1, STORY-0004 E1).
//!
//! Three nodes are preloaded with `records` YCSB records, and ranges split automatically
//! until none holds more than `max_range_bytes` of live data. YCSB workload A (50% reads, 50%
//! updates, zipfian keys) then runs while a fourth node joins and the rebalancer moves one
//! replica at a time towards balance. Passes when replica counts per node come within
//! `balance` of the mean inside `within`, and no client request failed. A client request is
//! one YCSB operation run as an auto-commit statement; like a real client library, it retries
//! errors that are retryable by contract (serialization conflict, range unavailable) for up to
//! `REQUEST_DEADLINE`, and fails after that.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dscore_server::txn::cluster::Cluster;
use dscore_server::txn::coordinator::{TxnClient, TxnConfig};
use dscore_server::txn::error::{DsError, ErrorCode};
use dscore_server::txn::retry::{RetryPolicy, autocommit};
use openraft::Config;

const WORKERS: u64 = 8;
const REQUEST_DEADLINE: Duration = Duration::from_secs(10);
const VALUE_BYTES: usize = 100;

pub struct Options {
    pub records: u64,
    pub max_range_bytes: u64,
    pub balance: f64,
    pub within: Duration,
    /// YCSB operations per second across all workers.
    pub rate: f64,
    /// How long the workload keeps running once balanced.
    pub settle: Duration,
    /// Pause between replica moves, as a production rebalancer throttles snapshot traffic;
    /// it also keeps the moves overlapping the workload rather than finishing in a burst.
    pub move_interval: Duration,
    pub seed: u64,
}

pub struct Report {
    pub ranges: usize,
    pub moves: usize,
    pub balanced_after: Option<Duration>,
    pub counts: BTreeMap<u64, usize>,
    pub deviation: f64,
    pub ops: u64,
    pub retried: u64,
    pub failed: u64,
    pub first_failure: Option<String>,
    /// Preloaded records missing at the end.
    pub lost: u64,
}

fn key(i: u64) -> Vec<u8> {
    format!("user{i:010}").into_bytes()
}

fn value(rng: &mut impl FnMut() -> u64) -> Vec<u8> {
    (0..VALUE_BYTES)
        .map(|_| b'a' + (rng() % 26) as u8)
        .collect()
}

fn xorshift(mut s: u64) -> impl FnMut() -> u64 {
    move || {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        s
    }
}

/// YCSB's zipfian generator (Gray et al., "Quickly generating billion-record synthetic
/// databases"), constant 0.99: a few keys are hot, most are cold.
struct Zipf {
    n: f64,
    theta: f64,
    alpha: f64,
    zetan: f64,
    eta: f64,
}

impl Zipf {
    fn new(n: u64) -> Self {
        let theta = 0.99;
        let zeta = |n: u64| (1..=n).map(|i| 1.0 / (i as f64).powf(theta)).sum::<f64>();
        let zetan = zeta(n);
        let zeta2 = zeta(2);
        let n = n as f64;
        Zipf {
            n,
            theta,
            alpha: 1.0 / (1.0 - theta),
            zetan,
            eta: (1.0 - (2.0 / n).powf(1.0 - theta)) / (1.0 - zeta2 / zetan),
        }
    }

    fn next(&self, u: f64) -> u64 {
        let uz = u * self.zetan;
        if uz < 1.0 {
            return 0;
        }
        if uz < 1.0 + 0.5f64.powf(self.theta) {
            return 1;
        }
        ((self.n * (self.eta * u - self.eta + 1.0).powf(self.alpha)) as u64).min(self.n as u64 - 1)
    }
}

fn deviation(counts: &BTreeMap<u64, usize>) -> f64 {
    let total: usize = counts.values().sum();
    let mean = total as f64 / counts.len().max(1) as f64;
    counts
        .values()
        .map(|c| (*c as f64 - mean).abs() / mean)
        .fold(0.0, f64::max)
}

/// One client request with the client-side retry contract; Ok(retried) or the final error.
async fn request(client: &TxnClient, read: bool, k: Vec<u8>, v: Vec<u8>) -> Result<bool, DsError> {
    let start = Instant::now();
    let mut retried = false;
    loop {
        let (k, v) = (k.clone(), v.clone());
        let r = autocommit(client, RetryPolicy::default(), move |mut t| {
            let (k, v) = (k.clone(), v.clone());
            async move {
                if read {
                    t.get(&k).await?;
                } else {
                    t.put(&k, &v);
                }
                Ok(((), t))
            }
        })
        .await;
        match r {
            Ok(_) => return Ok(retried),
            Err(e)
                if matches!(
                    e.code,
                    ErrorCode::SerializationConflict | ErrorCode::Unavailable
                ) && start.elapsed() < REQUEST_DEADLINE =>
            {
                retried = true;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

pub async fn run(dir: &Path, o: &Options) -> Result<Report, String> {
    let config = Config {
        heartbeat_interval: 50,
        election_timeout_min: 300,
        election_timeout_max: 600,
        ..Default::default()
    };
    let cluster: Arc<Cluster> = Cluster::start(dir, 3, &[], config).await?;
    let client = TxnClient::new(
        cluster.clone(),
        TxnConfig {
            liveness_ttl: Duration::from_millis(1500),
            lock_wait: Duration::from_millis(1000),
        },
    );
    let mut rng = xorshift(o.seed | 1);

    // Preload, then split until every range is within the size limit.
    for batch in (0..o.records).collect::<Vec<_>>().chunks(50) {
        let rows: Vec<(Vec<u8>, Vec<u8>)> =
            batch.iter().map(|i| (key(*i), value(&mut rng))).collect();
        autocommit(&client, RetryPolicy::default(), move |mut t| {
            let rows = rows.clone();
            async move {
                for (k, v) in &rows {
                    t.put(k, v);
                }
                Ok(((), t))
            }
        })
        .await
        .map_err(|e| format!("preload: {e}"))?;
    }
    loop {
        let made = cluster.split_oversized(o.max_range_bytes).await;
        if made.is_empty() {
            break;
        }
    }
    let ranges = cluster.ranges().len();
    eprintln!(
        "add-node: preloaded {} records into {ranges} ranges (max {} live bytes each); replicas {:?}",
        o.records,
        o.max_range_bytes,
        cluster.replica_counts()
    );

    // YCSB workload A.
    let zipf = Arc::new(Zipf::new(o.records));
    let stop = Arc::new(AtomicBool::new(false));
    let (ops, retried, failed) = (
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
    );
    let first_failure = Arc::new(std::sync::Mutex::new(None));
    let pace = Duration::from_secs_f64(WORKERS as f64 / o.rate);
    let mut workers = Vec::new();
    for w in 0..WORKERS {
        let (client, zipf, stop) = (client.clone(), zipf.clone(), stop.clone());
        let (ops, retried, failed, first_failure) = (
            ops.clone(),
            retried.clone(),
            failed.clone(),
            first_failure.clone(),
        );
        let mut rng = xorshift(o.seed ^ ((w + 1) * 0x9E37_79B9_7F4A_7C15) | 1);
        workers.push(tokio::spawn(async move {
            let mut due = tokio::time::Instant::now();
            while !stop.load(Ordering::Relaxed) {
                due += pace;
                tokio::time::sleep_until(due).await;
                let u = (rng() >> 11) as f64 / (1u64 << 53) as f64;
                let k = key(zipf.next(u));
                let read = rng().is_multiple_of(2);
                let v = value(&mut rng);
                match request(&client, read, k, v).await {
                    Ok(r) => {
                        retried.fetch_add(r as u64, Ordering::Relaxed);
                    }
                    Err(e) => {
                        failed.fetch_add(1, Ordering::Relaxed);
                        first_failure
                            .lock()
                            .expect("failure lock")
                            .get_or_insert_with(|| e.to_string());
                    }
                }
                ops.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // A fourth node joins; rebalance one move at a time.
    let new = cluster.add_node().await?;
    let start = Instant::now();
    let mut moves = 0;
    let mut balanced_after = None;
    while start.elapsed() < o.within {
        let dev = deviation(&cluster.replica_counts());
        if dev <= o.balance {
            balanced_after = Some(start.elapsed());
            break;
        }
        match cluster.rebalance_step().await {
            Ok(Some((r, from, to))) => {
                moves += 1;
                eprintln!(
                    "[{:?}] move range {r}: node {from} -> {to}; replicas {:?}",
                    start.elapsed(),
                    cluster.replica_counts()
                );
                tokio::time::sleep(o.move_interval).await;
            }
            Ok(None) => {
                balanced_after = Some(start.elapsed());
                break;
            }
            Err(e) => {
                eprintln!("[{:?}] move failed, retrying: {e}", start.elapsed());
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    eprintln!(
        "add-node: node {new} joined; settling under load for {:?}",
        o.settle
    );
    tokio::time::sleep(o.settle).await;
    stop.store(true, Ordering::Relaxed);
    for w in workers {
        let _ = w.await;
    }
    // No record may be lost: YCSB-A never deletes. Read through the new node's replicas,
    // which hold only what their snapshots and logs gave them.
    for d in cluster.ranges() {
        if d.replicas.contains(&new) {
            let _ = cluster.make_leader(d.id, new).await;
        }
    }
    let lo = key(0);
    let hi = key(o.records);
    let present = autocommit(&client, RetryPolicy::default(), move |mut t| {
        let (lo, hi) = (lo.clone(), hi.clone());
        async move {
            let n = t.scan(&lo, &hi).await?.len() as u64;
            Ok((n, t))
        }
    })
    .await
    .map_err(|e| format!("final scan: {e}"))?
    .value;
    let counts = cluster.replica_counts();
    cluster.shutdown().await;
    let first_failure = first_failure.lock().expect("failure lock").clone();
    Ok(Report {
        ranges,
        moves,
        balanced_after,
        deviation: deviation(&counts),
        counts,
        ops: ops.load(Ordering::Relaxed),
        retried: retried.load(Ordering::Relaxed),
        failed: failed.load(Ordering::Relaxed),
        lost: o.records - present.min(o.records),
        first_failure,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zipf_is_skewed_and_in_range() {
        let z = Zipf::new(1000);
        let mut rng = xorshift(7);
        let mut hits = vec![0u32; 1000];
        for _ in 0..100_000 {
            let u = (rng() >> 11) as f64 / (1u64 << 53) as f64;
            hits[z.next(u) as usize] += 1;
        }
        // The hottest key gets far more than a uniform share (100), the tail far less.
        assert!(hits[0] > 5_000, "{}", hits[0]);
        assert!(hits[900..].iter().all(|h| *h < 100));
    }

    #[test]
    fn deviation_is_relative_to_the_mean() {
        let c = BTreeMap::from([(1, 18), (2, 18), (3, 18), (4, 18)]);
        assert_eq!(deviation(&c), 0.0);
        let c = BTreeMap::from([(1, 24), (2, 24), (3, 24), (4, 0)]);
        assert_eq!(deviation(&c), 1.0);
    }
}
