//! `jepsen --workload edges --check bidirectional` (REQ-0010 AC2, STORY-0007 E2).
//!
//! 30 nodes spread over 3 ranges; 8 workers insert, re-weight and delete random edges
//! concurrently (serialization conflicts are expected and counted). A checker repeatedly reads
//! every node's out- and in-adjacency in one transaction, i.e. one snapshot across all ranges,
//! and reports any edge visible from only one endpoint or with different properties on its
//! two entries.
//!
//! `--unsafe-split-writes` writes the two entries in separate transactions; the checker must
//! then find violations, which proves it can.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use dscore_server::graph::keys::{self, Direction};
use dscore_server::graph::store::{Edge, Graph};
use dscore_server::graph::value::{Record, Value};
use dscore_server::txn::cluster::Cluster;
use dscore_server::txn::coordinator::{TxnClient, TxnConfig};
use dscore_server::txn::error::ErrorCode;
use openraft::Config;

const NODES: u64 = 30;
const WORKERS: usize = 8;
const TYPES: [&str; 2] = ["PAID", "FOLLOWS"];

pub struct Report {
    pub committed: u64,
    pub conflicts: u64,
    pub checks: u64,
    pub max_edges_seen: usize,
    pub violations: Vec<String>,
}

fn node_id(i: u64) -> u64 {
    // Spread across the three ranges split at 1<<62 and 2<<62.
    (i % 3) * (1 << 62) + i
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn weight(w: u64) -> Record {
    Record::new(vec![("w".to_string(), Value::Int64(w as i64))]).expect("unique field")
}

type EdgeId = (u64, String, u64, u64);

async fn check(client: &TxnClient, g: &Graph) -> Result<(usize, Vec<String>), String> {
    let mut t = client.begin().await.map_err(|e| e.to_string())?;
    let mut outs: BTreeMap<EdgeId, Record> = BTreeMap::new();
    let mut ins: BTreeMap<EdgeId, Record> = BTreeMap::new();
    for i in 0..NODES {
        let n = node_id(i);
        for e in g
            .edges(&mut t, n, Direction::Out, None)
            .await
            .map_err(|e| e.to_string())?
        {
            outs.insert((e.src, e.edge_type, e.dst, e.rank), e.properties);
        }
        for e in g
            .edges(&mut t, n, Direction::In, None)
            .await
            .map_err(|e| e.to_string())?
        {
            ins.insert((e.src, e.edge_type, e.dst, e.rank), e.properties);
        }
    }
    let mut violations = Vec::new();
    for (id, p) in &outs {
        match ins.get(id) {
            None => violations.push(format!("edge {id:?} visible only from its source")),
            Some(q) if q != p => violations.push(format!(
                "edge {id:?} has different properties on its two entries"
            )),
            _ => {}
        }
    }
    for id in ins.keys().filter(|id| !outs.contains_key(*id)) {
        violations.push(format!("edge {id:?} visible only from its target"));
    }
    Ok((outs.len(), violations))
}

pub async fn run(
    duration: Duration,
    seed: u64,
    unsafe_split_writes: bool,
) -> Result<Report, String> {
    let dir = std::env::temp_dir().join(format!("dscore-edges-{}", std::process::id()));
    let config = Config {
        heartbeat_interval: 50,
        election_timeout_min: 300,
        election_timeout_max: 600,
        ..Default::default()
    };
    let s1 = keys::node_doc(1, 1 << 62);
    let s2 = keys::node_doc(1, 2 << 62);
    let cluster = Cluster::start(&dir, 3, &[&s1, &s2], config).await?;
    let client = TxnClient::new(
        cluster.clone(),
        TxnConfig {
            liveness_ttl: Duration::from_millis(500),
            lock_wait: Duration::from_millis(500),
        },
    );
    let g = Graph::new(1);
    let mut t = client.begin().await.map_err(|e| e.to_string())?;
    for i in 0..NODES {
        g.insert_node(&mut t, node_id(i), &["N"], &Record::empty())
            .await
            .map_err(|e| e.to_string())?;
    }
    t.commit().await.map_err(|e| e.to_string())?;

    let stop = Arc::new(AtomicBool::new(false));
    let committed = Arc::new(AtomicU64::new(0));
    let conflicts = Arc::new(AtomicU64::new(0));
    let mut workers = Vec::new();
    for w in 0..WORKERS {
        let (client, g, stop, committed, conflicts) = (
            client.clone(),
            g.clone(),
            stop.clone(),
            committed.clone(),
            conflicts.clone(),
        );
        workers.push(tokio::spawn(async move {
            let mut rng = Rng((seed ^ (w as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15)) | 1);
            while !stop.load(Ordering::Relaxed) {
                let e = Edge {
                    src: node_id(rng.below(NODES)),
                    dst: node_id(rng.below(NODES)),
                    edge_type: TYPES[rng.below(2) as usize].into(),
                    rank: rng.below(3),
                    properties: weight(rng.below(1000)),
                };
                let op = rng.below(3);
                let result = if unsafe_split_writes && op == 0 {
                    split_insert(&client, &e).await
                } else {
                    let mut t = match client.begin().await {
                        Ok(t) => t,
                        Err(_) => continue,
                    };
                    let staged = match op {
                        0 => g.insert_edge(&mut t, &e).await.map(|_| ()),
                        1 => g.set_edge_properties(&mut t, &e).await.map(|_| ()),
                        _ => g
                            .delete_edge(&mut t, e.src, &e.edge_type, e.dst, e.rank)
                            .await
                            .map(|_| ()),
                    };
                    match staged {
                        Ok(()) => t.commit().await.map(|_| ()),
                        Err(err) => Err(err),
                    }
                };
                match result {
                    Ok(()) => {
                        committed.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) if e.code == ErrorCode::SerializationConflict => {
                        conflicts.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {} // e.g. inserting an edge that exists, or updating a missing one
                }
            }
        }));
    }

    let mut checks = 0;
    let mut max_edges_seen = 0;
    let mut violations = Vec::new();
    let deadline = tokio::time::Instant::now() + duration;
    while tokio::time::Instant::now() < deadline {
        match check(&client, &g).await {
            Ok((n, v)) => {
                checks += 1;
                max_edges_seen = max_edges_seen.max(n);
                violations.extend(v);
            }
            Err(e) => eprintln!("edges: check skipped: {e}"),
        }
        if violations.len() > 50 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    stop.store(true, Ordering::Relaxed);
    for w in workers {
        let _ = w.await;
    }
    // One final check once writers are quiet.
    if let Ok((n, v)) = check(&client, &g).await {
        checks += 1;
        max_edges_seen = max_edges_seen.max(n);
        violations.extend(v);
    }
    cluster.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
    Ok(Report {
        committed: committed.load(Ordering::Relaxed),
        conflicts: conflicts.load(Ordering::Relaxed),
        checks,
        max_edges_seen,
        violations,
    })
}

/// The unsafe variant: out-entry and in-entry in two separate transactions.
async fn split_insert(
    client: &TxnClient,
    e: &Edge,
) -> Result<(), dscore_server::txn::error::DsError> {
    let g = Graph::new(1);
    let mut t = client.begin().await?;
    g.insert_edge(&mut t, e).await?;
    t.commit().await?;
    // Remove the in-entry again in its own transaction, leaving the edge one-sided until the
    // next operation on it: what a non-transactional two-write implementation risks.
    let mut t = client.begin().await?;
    for etype in 0..TYPES.len() as u32 {
        let k = keys::edge(1, e.dst, Direction::In, etype, e.src, e.rank);
        if t.get(&k).await?.is_some() {
            t.delete(&k);
        }
    }
    t.commit().await?;
    Ok(())
}
