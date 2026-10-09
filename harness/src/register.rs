//! `jepsen --workload register --internal-retry on|off --check lost-update,serializable`
//! (REQ-0015 AC2, STORY-0006 E4).
//!
//! Workers increment counters on 3 ranges with read-modify-write transactions, through the
//! internal auto-commit retry when it is on. Each acknowledged increment returns the value it
//! read. For read-modify-write increments the checks are exact:
//!   lost-update   final value = number of acknowledged increments;
//!   serializable  the values read by the acknowledged increments of a counter are exactly
//!                 0..N-1, each once (a total order); two commits reading the same value would
//!                 be a lost update or a non-serializable history.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use dscore_server::txn::cluster::Cluster;
use dscore_server::txn::coordinator::{TxnClient, TxnConfig};
use dscore_server::txn::error::ErrorCode;
use dscore_server::txn::retry::{RetryPolicy, autocommit};
use openraft::Config;
use tokio::sync::Mutex;

const COUNTERS: [&[u8]; 3] = [b"c/a", b"m/b", b"x/c"];
const WORKERS: usize = 8;

pub struct Report {
    pub acknowledged: usize,
    pub conflicts_returned: u64,
    pub retries: u64,
    pub violations: Vec<String>,
}

fn read_u64(b: Option<Vec<u8>>) -> u64 {
    b.map_or(0, |b| {
        u64::from_be_bytes(b.as_slice().try_into().unwrap_or([0; 8]))
    })
}

pub async fn run(duration: Duration, internal_retry: bool) -> Result<Report, String> {
    let dir = std::env::temp_dir().join(format!("dscore-register-{}", std::process::id()));
    let config = Config {
        heartbeat_interval: 50,
        election_timeout_min: 300,
        election_timeout_max: 600,
        ..Default::default()
    };
    let cluster = Cluster::start(&dir, 3, &[b"g", b"p"], config).await?;
    let client = TxnClient::new(
        cluster.clone(),
        TxnConfig {
            liveness_ttl: Duration::from_millis(500),
            lock_wait: Duration::from_millis(500),
        },
    );
    let policy = RetryPolicy {
        max_attempts: if internal_retry { 20 } else { 1 },
        ..RetryPolicy::default()
    };
    let observed: Arc<Mutex<BTreeMap<usize, Vec<u64>>>> = Arc::default();
    let stop = Arc::new(AtomicBool::new(false));
    let mut workers = Vec::new();
    for w in 0..WORKERS {
        let (client, observed, stop) = (client.clone(), observed.clone(), stop.clone());
        workers.push(tokio::spawn(async move {
            let (mut conflicts, mut retries) = (0u64, 0u64);
            let mut i = w;
            while !stop.load(Ordering::Relaxed) {
                let c = i % COUNTERS.len();
                i += 1;
                let key = COUNTERS[c];
                let res = autocommit(&client, policy, |mut t| async move {
                    let n = read_u64(t.get(key).await?);
                    t.put(key, &(n + 1).to_be_bytes());
                    Ok((n, t))
                })
                .await;
                match res {
                    Ok(done) => {
                        retries += u64::from(done.attempts - 1);
                        observed.lock().await.entry(c).or_default().push(done.value);
                    }
                    Err(e) if e.code == ErrorCode::SerializationConflict => conflicts += 1,
                    Err(_) => {}
                }
            }
            (conflicts, retries)
        }));
    }
    tokio::time::sleep(duration).await;
    stop.store(true, Ordering::Relaxed);
    let (mut conflicts, mut retries) = (0, 0);
    for w in workers {
        let (c, r) = w.await.map_err(|e| e.to_string())?;
        conflicts += c;
        retries += r;
    }

    let observed = observed.lock().await.clone();
    let mut violations = Vec::new();
    let mut acknowledged = 0;
    let mut t = client.begin().await.map_err(|e| e.to_string())?;
    for (c, key) in COUNTERS.iter().enumerate() {
        let reads = observed.get(&c).cloned().unwrap_or_default();
        acknowledged += reads.len();
        let fin = read_u64(t.get(key).await.map_err(|e| e.to_string())?);
        if fin != reads.len() as u64 {
            violations.push(format!(
                "lost-update: counter {} is {fin} after {} acknowledged increments",
                String::from_utf8_lossy(key),
                reads.len()
            ));
        }
        let mut sorted = reads.clone();
        sorted.sort_unstable();
        if sorted.iter().copied().ne(0..reads.len() as u64) {
            let dup = sorted.windows(2).find(|w| w[0] == w[1]).map(|w| w[0]);
            violations.push(format!(
                "serializable: increments of {} did not read a total order 0..{} (first duplicate read: {dup:?})",
                String::from_utf8_lossy(key),
                reads.len()
            ));
        }
    }
    cluster.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
    Ok(Report {
        acknowledged,
        conflicts_returned: conflicts,
        retries,
        violations,
    })
}
