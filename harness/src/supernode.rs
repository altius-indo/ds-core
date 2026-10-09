//! `supernode --edges 10000000 --assert-split --assert-p99-ms 50 --limit 100`
//! (REQ-0024, STORY-0009 E1).
//!
//! One hub node takes `edges` outgoing edges, spread over `dsts` destination nodes, loaded in
//! batches by concurrent workers while ranges split automatically once they exceed
//! `max_range_bytes`. AC1: no batch is rejected, and the hub's adjacency ends up spanning more
//! than one range. AC2: one-hop reads of the hub with LIMIT `limit` complete within the p99
//! bound; they read only the edges they return, however many the hub has.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dscore_server::graph::keys::{Direction, adjacency_span};
use dscore_server::graph::store::Graph;
use dscore_server::graph::value::Record;
use dscore_server::txn::cluster::Cluster;
use dscore_server::txn::coordinator::{TxnClient, TxnConfig};
use dscore_server::txn::retry::{RetryPolicy, autocommit};
use openraft::{Config, SnapshotPolicy};

const HUB: u64 = 1;
const GRAPH: u32 = 1;

pub struct Options {
    pub edges: u64,
    pub dsts: u64,
    pub batch: u64,
    pub workers: u64,
    pub max_range_bytes: u64,
    pub limit: usize,
    pub samples: usize,
}

pub struct Report {
    pub loaded: u64,
    pub rejected: u64,
    pub first_rejection: Option<String>,
    pub load_time: Duration,
    pub ranges: usize,
    pub hub_ranges: usize,
    pub p50: Duration,
    pub p99: Duration,
    pub short_reads: usize,
}

fn dst(i: u64) -> u64 {
    // Destination ids spread across the id space.
    2 + i * (u64::MAX / 4 / 1_000_000)
}

pub async fn run(dir: &std::path::Path, o: &Options) -> Result<Report, String> {
    let config = Config {
        heartbeat_interval: 50,
        election_timeout_min: 300,
        election_timeout_max: 600,
        // Bound the Raft logs a 10M-edge load leaves on disk.
        snapshot_policy: SnapshotPolicy::LogsSinceLast(500),
        max_in_snapshot_log_to_keep: 100,
        ..Default::default()
    };
    let cluster: Arc<Cluster> = Cluster::start(dir, 3, &[], config).await?;
    let client = TxnClient::new(
        cluster.clone(),
        TxnConfig {
            liveness_ttl: Duration::from_millis(3000),
            lock_wait: Duration::from_secs(2),
        },
    );
    let g = Arc::new(Graph::new(GRAPH));

    // The hub and its destinations.
    let ids: Vec<u64> = std::iter::once(HUB).chain((0..o.dsts).map(dst)).collect();
    for chunk in ids.chunks(200) {
        let (g, chunk) = (g.clone(), chunk.to_vec());
        autocommit(&client, RetryPolicy::default(), move |mut t| {
            let (g, chunk) = (g.clone(), chunk.clone());
            async move {
                for id in chunk {
                    g.insert_node(&mut t, id, &["N"], &Record::empty()).await?;
                }
                Ok(((), t))
            }
        })
        .await
        .map_err(|e| format!("creating nodes: {e}"))?;
    }

    // Split oversized ranges while the load runs.
    let splitting = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let splitter = {
        let (cluster, splitting) = (cluster.clone(), splitting.clone());
        let max = o.max_range_bytes;
        tokio::spawn(async move {
            while splitting.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(2)).await;
                cluster.split_oversized(max).await;
            }
        })
    };

    // Batches: (destination index, first rank), destination-major so each batch's keys are
    // contiguous on both sides of the edge.
    let per_dst = o.edges.div_ceil(o.dsts);
    let jobs: Vec<(u64, u64, u64)> = (0..o.dsts)
        .flat_map(|d| {
            (0..per_dst)
                .step_by(o.batch as usize)
                .map(move |r| (d, r, (r + o.batch).min(per_dst)))
        })
        .filter(|(d, r, _)| d * per_dst + r < o.edges)
        .collect();
    let jobs = Arc::new(std::sync::Mutex::new(jobs.into_iter()));
    let (loaded, rejected) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
    let first_rejection = Arc::new(std::sync::Mutex::new(None));
    let start = Instant::now();
    let total = o.edges;
    let mut workers = Vec::new();
    for _ in 0..o.workers {
        let (client, g, jobs) = (client.clone(), g.clone(), jobs.clone());
        let (loaded, rejected, first_rejection) =
            (loaded.clone(), rejected.clone(), first_rejection.clone());
        workers.push(tokio::spawn(async move {
            loop {
                let Some((d, lo, hi)) = jobs.lock().expect("jobs").next() else {
                    break;
                };
                // The last destination may take fewer than per_dst edges.
                let hi = hi.min(total.saturating_sub(d * per_dst));
                let policy = RetryPolicy {
                    max_attempts: 20,
                    base_backoff: Duration::from_millis(5),
                };
                let g2 = g.clone();
                let r = autocommit(&client, policy, move |mut t| {
                    let g = g2.clone();
                    async move {
                        g.insert_edges_bulk(&mut t, HUB, dst(d), "LINK", lo..hi, &Record::empty())
                            .await?;
                        Ok(((), t))
                    }
                })
                .await;
                match r {
                    Ok(_) => {
                        let before = loaded.fetch_add(hi - lo, Ordering::Relaxed);
                        let step = (total / 10).max(1);
                        if (before + hi - lo) / step > before / step {
                            eprintln!(
                                "supernode: {} edges loaded ({:?})",
                                before + hi - lo,
                                start.elapsed()
                            );
                        }
                    }
                    Err(e) => {
                        rejected.fetch_add(hi - lo, Ordering::Relaxed);
                        first_rejection
                            .lock()
                            .expect("rejection")
                            .get_or_insert_with(|| e.to_string());
                    }
                }
            }
        }));
    }
    for w in workers {
        let _ = w.await;
    }
    let load_time = start.elapsed();
    // A last pass so the final state is split too, then stop the splitter.
    cluster.split_oversized(o.max_range_bytes).await;
    splitting.store(false, Ordering::Relaxed);
    let _ = splitter.await;

    let (lo, hi) = adjacency_span(GRAPH, HUB, Direction::Out, None);
    let ranges = cluster.ranges();
    let hub_ranges = ranges
        .iter()
        .filter(|d| {
            d.start.as_slice() < hi.as_slice()
                && (d.end.is_empty() || d.end.as_slice() > lo.as_slice())
        })
        .count();

    // AC2: bounded one-hop reads of the hub.
    let mut lat = Vec::with_capacity(o.samples);
    let mut short_reads = 0;
    for _ in 0..o.samples {
        let t0 = Instant::now();
        let mut t = client.begin().await.map_err(|e| e.to_string())?;
        let got = g
            .edges_limit(&mut t, HUB, Direction::Out, None, o.limit)
            .await
            .map_err(|e| e.to_string())?;
        lat.push(t0.elapsed());
        if got.len() < o.limit.min(o.edges as usize) {
            short_reads += 1;
        }
    }
    lat.sort();
    let pct = |p: f64| lat[((lat.len() as f64 * p) as usize).min(lat.len() - 1)];
    let report = Report {
        loaded: loaded.load(Ordering::Relaxed),
        rejected: rejected.load(Ordering::Relaxed),
        first_rejection: first_rejection.lock().expect("rejection").clone(),
        load_time,
        ranges: ranges.len(),
        hub_ranges,
        p50: pct(0.50),
        p99: pct(0.99),
        short_reads,
    };
    cluster.shutdown().await;
    Ok(report)
}
