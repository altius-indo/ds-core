//! An in-process multi-range cluster: every node hosts a replica of every range, and all of a
//! node's ranges share its `raftdb` and `kvdb` (multi-Raft, design/raft-ranges.md §2).
//!
//! Range 0 is the meta range: it owns no user keys and holds the timestamp oracle's mark.
//! Data ranges split the keyspace at fixed points (dynamic splits come with TASK-0008).
//! Commands go to the current leader of a range and are retried across leader changes;
//! reads run on the leader after a linearizability check.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use openraft::Config;

use super::mvcc::{TxnCommand, TxnResponse};
use crate::raft::log_store::LogEngine;
use crate::raft::network::{Router, RouterNetwork};
use crate::raft::start_range;
use crate::raft::state_machine::KvEngine;
use crate::raft::types::{Command, CommandResult, NodeId, NodeInfo, Raft, RangeId};

pub const META_RANGE: RangeId = 0;
/// Sentinel interval for the meta range: `[x, x)` owns no user keys.
const META_INTERVAL: &[u8] = b"\xff\xffmeta";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeDesc {
    pub id: RangeId,
    pub start: Vec<u8>,
    /// Exclusive; empty means +∞.
    pub end: Vec<u8>,
}

impl RangeDesc {
    pub fn contains(&self, key: &[u8]) -> bool {
        key >= self.start.as_slice() && (self.end.is_empty() || key < self.end.as_slice())
    }
}

pub struct ClusterNode {
    pub id: NodeId,
    pub kv: Arc<KvEngine>,
    pub ranges: BTreeMap<RangeId, Raft>,
}

pub struct Cluster {
    pub nodes: Vec<ClusterNode>,
    pub ranges: Vec<RangeDesc>,
    pub routers: HashMap<RangeId, Arc<Router>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unavailable(pub String);

impl std::fmt::Display for Unavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "range unavailable: {}", self.0)
    }
}

impl std::error::Error for Unavailable {}

const LEADER_WAIT: Duration = Duration::from_secs(10);

impl Cluster {
    /// Start `nodes` voters hosting the meta range plus one data range per interval between
    /// `split_points` (sorted).
    pub async fn start(
        dir: &Path,
        nodes: u64,
        split_points: &[&[u8]],
        config: Config,
    ) -> Result<Self, String> {
        let mut ranges = Vec::new();
        let mut start: Vec<u8> = Vec::new();
        for (i, p) in split_points.iter().enumerate() {
            ranges.push(RangeDesc {
                id: i as RangeId + 1,
                start: start.clone(),
                end: p.to_vec(),
            });
            start = p.to_vec();
        }
        ranges.push(RangeDesc {
            id: split_points.len() as RangeId + 1,
            start,
            end: Vec::new(),
        });

        let mut routers = HashMap::new();
        let all_ranges: Vec<RangeId> = std::iter::once(META_RANGE)
            .chain(ranges.iter().map(|r| r.id))
            .collect();
        for r in &all_ranges {
            routers.insert(*r, Router::new());
        }
        let mut cluster_nodes = Vec::new();
        for id in 1..=nodes {
            let base = dir.join(format!("n{id}"));
            let logs = LogEngine::open(&base.join("raftdb")).map_err(|e| e.to_string())?;
            let kv = KvEngine::open(&base.join("kvdb")).map_err(|e| e.to_string())?;
            let mut rafts = BTreeMap::new();
            for &r in &all_ranges {
                let (s, e) = match ranges.iter().find(|d| d.id == r) {
                    Some(d) => (d.start.clone(), d.end.clone()),
                    None => (META_INTERVAL.to_vec(), META_INTERVAL.to_vec()),
                };
                let net = RouterNetwork {
                    router: routers[&r].clone(),
                    from: id,
                };
                let raft = start_range(id, r, s, e, config.clone(), &logs, &kv, net)
                    .await
                    .map_err(|e| e.to_string())?;
                routers[&r].add(id, raft.clone());
                rafts.insert(r, raft);
            }
            cluster_nodes.push(ClusterNode {
                id,
                kv,
                ranges: rafts,
            });
        }
        let members: BTreeMap<NodeId, NodeInfo> = (1..=nodes)
            .map(|id| {
                (
                    id,
                    NodeInfo {
                        addr: format!("node{id}"),
                        zone: format!("az{id}"),
                    },
                )
            })
            .collect();
        for &r in &all_ranges {
            // Spread initial leadership: range r starts its election on node (r mod n) + 1.
            let first = &cluster_nodes[(r as usize) % cluster_nodes.len()];
            let _ = first.ranges[&r].initialize(members.clone()).await;
        }
        let cluster = Self {
            nodes: cluster_nodes,
            ranges,
            routers,
        };
        for &r in &all_ranges {
            cluster.leader(r).await.map_err(|e| e.to_string())?;
        }
        Ok(cluster)
    }

    pub fn range_for(&self, key: &[u8]) -> &RangeDesc {
        self.ranges
            .iter()
            .find(|r| r.contains(key))
            .expect("data ranges cover the whole keyspace")
    }

    /// The node currently leading `range`, waiting for an election if needed.
    pub async fn leader(&self, range: RangeId) -> Result<(&Raft, &Arc<KvEngine>), Unavailable> {
        let deadline = tokio::time::Instant::now() + LEADER_WAIT;
        loop {
            for n in &self.nodes {
                if let Some(r) = n.ranges.get(&range) {
                    let m = r.metrics().borrow().clone();
                    if m.current_leader == Some(n.id) && m.state == openraft::ServerState::Leader {
                        return Ok((r, &n.kv));
                    }
                }
            }
            if tokio::time::Instant::now() > deadline {
                return Err(Unavailable(format!("no leader for range {range}")));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Replicate a transaction command on `range` and return its result.
    pub async fn propose(
        &self,
        range: RangeId,
        cmd: TxnCommand,
    ) -> Result<TxnResponse, Unavailable> {
        let deadline = tokio::time::Instant::now() + LEADER_WAIT;
        loop {
            let (raft, _) = self.leader(range).await?;
            match raft.client_write(Command::Txn(cmd.clone())).await {
                Ok(resp) => {
                    return match resp.data {
                        CommandResult::Txn(r) => Ok(r),
                        CommandResult::Ok => Err(Unavailable("unexpected non-txn result".into())),
                    };
                }
                Err(e)
                    if tokio::time::Instant::now() < deadline
                        && e.forward_to_leader::<NodeInfo>().is_some() =>
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(e) => return Err(Unavailable(e.to_string())),
            }
        }
    }

    /// Run `f` against the leader's store for `range` after confirming leadership, so the read
    /// sees every write committed before it started.
    pub async fn read<T>(
        &self,
        range: RangeId,
        f: impl Fn(&rocksdb::DB) -> T,
    ) -> Result<T, Unavailable> {
        let deadline = tokio::time::Instant::now() + LEADER_WAIT;
        loop {
            let (raft, kv) = self.leader(range).await?;
            match raft.ensure_linearizable().await {
                Ok(_) => return Ok(f(kv.db())),
                Err(_) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(e) => return Err(Unavailable(e.to_string())),
            }
        }
    }

    pub async fn shutdown(&self) {
        for n in &self.nodes {
            for r in n.ranges.values() {
                let _ = r.shutdown().await;
            }
        }
    }
}
