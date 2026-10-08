//! `jepsen --workload membership`: continuous replica add/remove under write load
//! (REQ-0017, STORY-0002 E1).
//!
//! A pool of 5 nodes runs one range with 3 voters. Until the deadline the churn loop adds a
//! non-member as a learner, promotes it once caught up (`promote_when_caught_up`), then
//! removes a voter (sometimes the leader) so the group returns to 3 voters. A writer keeps
//! writing to whoever leads, and an observer samples every node's metrics every 5 ms.
//!
//! A removed replica is garbage-collected: stopped, its data deleted, and a fresh empty
//! replica started in its place, which is what a node does after leaving a range. Without
//! this, a removed voter that never saw the configuration excluding it keeps campaigning,
//! raises its term, and later rejects the leader when re-added.
//!
//! Checks:
//!   single-leader      no term has two different nodes acting as leader (AC1);
//!   learner-promotion  every promotion happened with lag <= the bound (AC2).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dscore_server::raft::log_store::LogEngine;
use dscore_server::raft::membership::{
    PromotionPolicy, add_learner, promote_when_caught_up, remove_voter, replication_lag, voters,
};
use dscore_server::raft::network::{Router, RouterNetwork};
use dscore_server::raft::start_range;
use dscore_server::raft::state_machine::KvEngine;
use dscore_server::raft::types::{Command, NodeId, NodeInfo, Raft};
use openraft::{Config, ServerState};

const POOL: NodeId = 5;

/// Nodes seen acting as leader, per term.
type LeadersByTerm = Arc<Mutex<BTreeMap<u64, BTreeSet<NodeId>>>>;
/// (node, the leader's lag figure for it when it first appeared as a voter).
type ObservedPromotions = Arc<Mutex<Vec<(NodeId, Option<u64>)>>>;

pub struct Report {
    pub writes_ok: u64,
    pub promotions: Vec<(NodeId, u64)>,
    /// Promotions the observer saw, and how many it could not measure (no leader progress yet).
    pub observed_promotions: usize,
    pub unmeasured_promotions: usize,
    pub removals: usize,
    pub terms_observed: usize,
    pub violations: Vec<String>,
}

fn info(id: NodeId) -> NodeInfo {
    NodeInfo {
        addr: format!("node{id}"),
        zone: format!("az{id}"),
    }
}

fn config() -> Config {
    Config {
        heartbeat_interval: 50,
        election_timeout_min: 300,
        election_timeout_max: 600,
        ..Default::default()
    }
}

/// Small deterministic PRNG so runs are reproducible from `--seed`.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn pick<T: Copy>(&mut self, xs: &[T]) -> Option<T> {
        (!xs.is_empty()).then(|| xs[(self.next() % xs.len() as u64) as usize])
    }
}

fn leader_of(nodes: &[(NodeId, Raft)]) -> Option<Raft> {
    nodes.iter().find_map(|(id, r)| {
        let m = r.metrics().borrow().clone();
        (m.current_leader == Some(*id) && m.state == ServerState::Leader).then(|| r.clone())
    })
}

async fn start_replica(
    dir: &std::path::Path,
    router: &Arc<Router>,
    id: NodeId,
    generation: u32,
) -> Result<Raft, String> {
    let base = dir.join(format!("n{id}-g{generation}"));
    let logs = LogEngine::open(&base.join("raftdb")).map_err(|e| e.to_string())?;
    let kv = KvEngine::open(&base.join("kvdb")).map_err(|e| e.to_string())?;
    let net = RouterNetwork {
        router: router.clone(),
        from: id,
    };
    let raft = start_range(id, 1, Vec::new(), Vec::new(), config(), &logs, &kv, net)
        .await
        .map_err(|e| e.to_string())?;
    router.add(id, raft.clone());
    Ok(raft)
}

/// Replica GC: stop the removed replica, delete its data, start a fresh empty one.
async fn replace_replica(
    dir: &std::path::Path,
    router: &Arc<Router>,
    nodes: &Arc<Mutex<Vec<(NodeId, Raft)>>>,
    id: NodeId,
    generation: u32,
) -> Result<(), String> {
    router.remove(id);
    let old = {
        let n = nodes.lock().unwrap();
        n.iter().find(|(i, _)| *i == id).map(|(_, r)| r.clone())
    };
    if let Some(r) = old {
        let _ = r.shutdown().await;
    }
    let _ = std::fs::remove_dir_all(dir.join(format!("n{id}-g{}", generation - 1)));
    let fresh = start_replica(dir, router, id, generation).await?;
    let mut n = nodes.lock().unwrap();
    if let Some(slot) = n.iter_mut().find(|(i, _)| *i == id) {
        slot.1 = fresh;
    }
    Ok(())
}

fn snapshot(nodes: &Arc<Mutex<Vec<(NodeId, Raft)>>>) -> Vec<(NodeId, Raft)> {
    nodes.lock().unwrap().clone()
}

pub async fn run(duration: Duration, max_lag: u64, seed: u64) -> Result<Report, String> {
    let dir = std::env::temp_dir().join(format!("dscore-churn-{}", std::process::id()));
    let router = Router::new();
    let mut initial_nodes = Vec::new();
    for id in 1..=POOL {
        initial_nodes.push((id, start_replica(&dir, &router, id, 0).await?));
    }
    let nodes = Arc::new(Mutex::new(initial_nodes));
    let mut generation: BTreeMap<NodeId, u32> = (1..=POOL).map(|id| (id, 0)).collect();
    let initial: BTreeMap<_, _> = (1..=3).map(|id| (id, info(id))).collect();
    snapshot(&nodes)[0]
        .1
        .initialize(initial)
        .await
        .map_err(|e| e.to_string())?;

    let stop = Arc::new(AtomicBool::new(false));
    let leaders_by_term: LeadersByTerm = Arc::default();
    // (node, lag the leader reported the moment the node first appeared as a voter).
    let observed_promotions: ObservedPromotions = Arc::default();

    // Observer, independent of the churn code: who acts as leader in which term, and how far
    // behind each node was when it became a voter.
    let observer = {
        let (nodes, stop, seen, promoted) = (
            nodes.clone(),
            stop.clone(),
            leaders_by_term.clone(),
            observed_promotions.clone(),
        );
        tokio::spawn(async move {
            // Compare voter sets only within one leader's term: a new leader starts with no
            // replication progress for anyone, so its lag figures for nodes the previous
            // leader promoted would be meaningless.
            let mut prev: Option<((NodeId, u64), BTreeSet<NodeId>)> = None;
            while !stop.load(Ordering::Relaxed) {
                for (id, r) in &snapshot(&nodes) {
                    let m = r.metrics().borrow().clone();
                    if m.state == ServerState::Leader && m.current_leader == Some(*id) {
                        seen.lock()
                            .unwrap()
                            .entry(m.current_term)
                            .or_default()
                            .insert(*id);
                        let reign = (*id, m.current_term);
                        let now = voters(r);
                        if let Some((prev_reign, prev_voters)) = &prev
                            && *prev_reign == reign
                        {
                            for new in now.difference(prev_voters) {
                                promoted
                                    .lock()
                                    .unwrap()
                                    .push((*new, replication_lag(r, *new)));
                            }
                        }
                        prev = Some((reign, now));
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };

    // Writer: continuous load on whoever leads.
    let writes_ok = Arc::new(AtomicU64::new(0));
    let writer = {
        let (nodes, stop, ok) = (nodes.clone(), stop.clone(), writes_ok.clone());
        tokio::spawn(async move {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                if let Some(l) = leader_of(&snapshot(&nodes)) {
                    let cmd = Command::Put {
                        key: format!("k{}", i % 1000).into_bytes(),
                        value: i.to_le_bytes().to_vec(),
                    };
                    if let Ok(Ok(_)) =
                        tokio::time::timeout(Duration::from_secs(2), l.client_write(cmd)).await
                    {
                        ok.fetch_add(1, Ordering::Relaxed);
                    }
                    i += 1;
                } else {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        })
    };

    // Churn.
    let mut rng = Rng(seed | 1);
    let policy = PromotionPolicy {
        max_lag,
        ..PromotionPolicy::default()
    };
    let mut promotions = Vec::new();
    let mut removals = 0;
    let deadline = tokio::time::Instant::now() + duration;
    let pause = || tokio::time::sleep(Duration::from_millis(50));
    while tokio::time::Instant::now() < deadline {
        let Some(leader) = leader_of(&snapshot(&nodes)) else {
            pause().await;
            continue;
        };
        let current = voters(&leader);
        let outsiders: Vec<NodeId> = (1..=POOL).filter(|id| !current.contains(id)).collect();
        let Some(new) = rng.pick(&outsiders) else {
            pause().await;
            continue;
        };
        if add_learner(&leader, new, info(new)).await.is_err() {
            pause().await;
            continue;
        }
        match promote_when_caught_up(&leader, new, policy).await {
            Ok(lag) => promotions.push((new, lag)),
            Err(_) => {
                pause().await;
                continue;
            }
        }
        let after: Vec<NodeId> = voters(&leader).into_iter().filter(|&v| v != new).collect();
        if let Some(victim) = rng.pick(&after)
            && remove_voter(&leader, victim).await.is_ok()
        {
            removals += 1;
            let g = generation.entry(victim).or_default();
            *g += 1;
            replace_replica(&dir, &router, &nodes, victim, *g).await?;
        }
        pause().await;
    }

    stop.store(true, Ordering::Relaxed);
    let _ = writer.await;
    let _ = observer.await;
    for (_, r) in &snapshot(&nodes) {
        let _ = r.shutdown().await;
    }
    let _ = std::fs::remove_dir_all(&dir);

    let seen = leaders_by_term.lock().unwrap().clone();
    let mut violations = Vec::new();
    for (term, leaders) in &seen {
        if leaders.len() > 1 {
            violations.push(format!(
                "term {term} had leaders {leaders:?} (REQ-0017 AC1)"
            ));
        }
    }
    let observed = observed_promotions.lock().unwrap().clone();
    for (node, lag) in &observed {
        if let Some(l) = lag
            && *l > max_lag
        {
            violations.push(format!(
                "node {node} became a voter at lag {l} > {max_lag} (REQ-0017 AC2)"
            ));
        }
    }
    Ok(Report {
        writes_ok: writes_ok.load(Ordering::Relaxed),
        promotions,
        observed_promotions: observed.len(),
        unmeasured_promotions: observed.iter().filter(|(_, l)| l.is_none()).count(),
        removals,
        terms_observed: seen.len(),
        violations,
    })
}
