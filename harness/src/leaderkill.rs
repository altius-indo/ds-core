//! `fault leader-kill`: time from a leader's death to the first successful write on its
//! replacement, p99 over many trials (REQ-0018 AC1, STORY-0003 E1).
//!
//! Each trial starts a fresh 3-voter group with production timing (`Timing::default()`),
//! confirms a write, then kills the leader: it is removed from the network and shut down, so
//! peers see silence exactly as after a crash. The clock starts at the kill and stops when a
//! surviving node commits a write.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use dscore_server::raft::config::Timing;
use dscore_server::raft::log_store::LogEngine;
use dscore_server::raft::network::{Router, RouterNetwork};
use dscore_server::raft::start_range;
use dscore_server::raft::state_machine::KvEngine;
use dscore_server::raft::types::{Command, NodeId, NodeInfo, Raft};

pub struct Outcome {
    pub samples: Vec<Duration>,
}

impl Outcome {
    pub fn percentile(&self, p: f64) -> Duration {
        let mut s = self.samples.clone();
        s.sort();
        let idx = ((p / 100.0) * s.len() as f64).ceil() as usize;
        s[idx.saturating_sub(1).min(s.len() - 1)]
    }
}

fn info(id: NodeId) -> NodeInfo {
    NodeInfo {
        addr: format!("node{id}"),
        zone: format!("az{id}"),
    }
}

fn put(key: &str) -> Command {
    Command::Put {
        key: key.as_bytes().to_vec(),
        value: b"1".to_vec(),
    }
}

async fn wait_leader(
    nodes: &[(NodeId, Raft)],
    deadline: Duration,
) -> Result<(NodeId, Raft), String> {
    let start = Instant::now();
    while start.elapsed() < deadline {
        for (id, r) in nodes {
            if r.metrics().borrow().current_leader == Some(*id) {
                return Ok((*id, r.clone()));
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err("no leader elected".into())
}

async fn trial(n: usize, timing: Timing) -> Result<Duration, String> {
    let dir = std::env::temp_dir().join(format!("dscore-leaderkill-{}-{n}", std::process::id()));
    let router = Router::new();
    let mut nodes = Vec::new();
    for id in 1..=3 {
        let base = dir.join(format!("n{id}"));
        let logs = LogEngine::open(&base.join("raftdb")).map_err(|e| e.to_string())?;
        let kv = KvEngine::open(&base.join("kvdb")).map_err(|e| e.to_string())?;
        let net = RouterNetwork {
            router: router.clone(),
            from: id,
        };
        let raft = start_range(
            id,
            1,
            Vec::new(),
            Vec::new(),
            timing.to_openraft(),
            &logs,
            &kv,
            net,
        )
        .await
        .map_err(|e| e.to_string())?;
        router.add(id, raft.clone());
        nodes.push((id, raft));
    }
    let members: BTreeMap<_, _> = (1..=3).map(|id| (id, info(id))).collect();
    nodes[0]
        .1
        .initialize(members)
        .await
        .map_err(|e| e.to_string())?;
    let (old, leader) = wait_leader(&nodes, Duration::from_secs(30)).await?;
    leader
        .client_write(put("before"))
        .await
        .map_err(|e| e.to_string())?;

    // Kill the leader.
    let killed_at = Instant::now();
    router.remove(old);
    leader.shutdown().await.map_err(|e| e.to_string())?;
    let survivors: Vec<_> = nodes.iter().filter(|(id, _)| *id != old).cloned().collect();

    // First successful write after the kill.
    let elapsed = loop {
        if killed_at.elapsed() > Duration::from_secs(60) {
            return Err("no write succeeded within 60 s of the leader kill".into());
        }
        match wait_leader(&survivors, Duration::from_secs(60)).await {
            Ok((_, new_leader)) => {
                let attempt = tokio::time::timeout(
                    Duration::from_secs(2),
                    new_leader.client_write(put("after")),
                )
                .await;
                if matches!(attempt, Ok(Ok(_))) {
                    break killed_at.elapsed();
                }
            }
            Err(e) => return Err(e),
        }
    };
    for (_, r) in survivors {
        let _ = r.shutdown().await;
    }
    let _ = std::fs::remove_dir_all(&dir);
    Ok(elapsed)
}

pub async fn run(trials: usize, timing: Timing) -> Result<Outcome, String> {
    let mut samples = Vec::with_capacity(trials);
    for n in 0..trials {
        let t = trial(n, timing).await?;
        samples.push(t);
        if (n + 1) % 10 == 0 {
            println!(
                "leader-kill: {} trials, last {:.2}s",
                n + 1,
                t.as_secs_f64()
            );
        }
    }
    Ok(Outcome { samples })
}
