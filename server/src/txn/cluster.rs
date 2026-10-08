//! An in-process multi-range cluster: every node hosts a replica of every range, and all of a
//! node's ranges share its `raftdb` and `kvdb` (multi-Raft, design/raft-ranges.md §2).
//!
//! Range 0 is the meta range: it owns no user keys and holds the timestamp oracle's mark.
//! Data ranges split the keyspace at fixed points (dynamic splits come with TASK-0008).
//! Commands go to the current leader of a range, with a timeout, and are retried across leader
//! changes; reads run on the leader after a linearizability check. A timed-out proposal may
//! still commit, so every command is idempotent and its outcome is reported as unknown.
//!
//! Fault hooks for the harness: `crash` / `restart` a node, `partition` / `heal` the network.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
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

/// A running node's store and its replica of every range.
type NodeHandles = (Arc<KvEngine>, BTreeMap<RangeId, Raft>);

#[derive(Clone)]
struct ClusterNode {
    id: NodeId,
    /// None while the node is crashed.
    up: Option<NodeHandles>,
}

pub struct Cluster {
    nodes: RwLock<Vec<ClusterNode>>,
    pub ranges: Vec<RangeDesc>,
    routers: HashMap<RangeId, Arc<Router>>,
    dir: PathBuf,
    config: Config,
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
/// A proposal or read that takes longer is retried on whoever leads then.
const RPC_TIMEOUT: Duration = Duration::from_secs(2);

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
        for r in std::iter::once(META_RANGE).chain(ranges.iter().map(|r| r.id)) {
            routers.insert(r, Router::new());
        }
        let cluster = Self {
            nodes: RwLock::new((1..=nodes).map(|id| ClusterNode { id, up: None }).collect()),
            ranges,
            routers,
            dir: dir.to_path_buf(),
            config,
        };
        for id in 1..=nodes {
            cluster.restart(id).await?;
        }
        let members: BTreeMap<NodeId, NodeInfo> = (1..=nodes).map(|id| (id, info(id))).collect();
        let snapshot = cluster.snapshot();
        for r in cluster.all_ranges() {
            // Spread initial leadership: range r starts its election on node (r mod n) + 1.
            let (_, (_, rafts)) = &snapshot[(r as usize) % snapshot.len()];
            let _ = rafts[&r].initialize(members.clone()).await;
        }
        for r in cluster.all_ranges() {
            cluster.leader(r).await.map_err(|e| e.to_string())?;
        }
        Ok(cluster)
    }

    fn all_ranges(&self) -> Vec<RangeId> {
        std::iter::once(META_RANGE)
            .chain(self.ranges.iter().map(|r| r.id))
            .collect()
    }

    /// Live nodes: (id, (kv, ranges)).
    fn snapshot(&self) -> Vec<(NodeId, NodeHandles)> {
        self.nodes
            .read()
            .expect("cluster lock")
            .iter()
            .filter_map(|n| n.up.clone().map(|u| (n.id, u)))
            .collect()
    }

    pub fn node_ids(&self) -> Vec<NodeId> {
        self.nodes
            .read()
            .expect("cluster lock")
            .iter()
            .map(|n| n.id)
            .collect()
    }

    pub fn range_for(&self, key: &[u8]) -> &RangeDesc {
        self.ranges
            .iter()
            .find(|r| r.contains(key))
            .expect("data ranges cover the whole keyspace")
    }

    /// The node currently leading `range` (the claimant with the highest term, in case a
    /// partitioned old leader has not noticed yet), waiting for an election if needed.
    pub async fn leader(&self, range: RangeId) -> Result<(Raft, Arc<KvEngine>), Unavailable> {
        let deadline = tokio::time::Instant::now() + LEADER_WAIT;
        loop {
            let mut best: Option<(u64, Raft, Arc<KvEngine>)> = None;
            for (id, (kv, rafts)) in self.snapshot() {
                if let Some(r) = rafts.get(&range) {
                    let m = r.metrics().borrow().clone();
                    if m.current_leader == Some(id)
                        && m.state == openraft::ServerState::Leader
                        && best.as_ref().is_none_or(|(t, _, _)| m.current_term > *t)
                    {
                        best = Some((m.current_term, r.clone(), kv.clone()));
                    }
                }
            }
            if let Some((_, r, kv)) = best {
                return Ok((r, kv));
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
            match tokio::time::timeout(RPC_TIMEOUT, raft.client_write(Command::Txn(cmd.clone())))
                .await
            {
                Ok(Ok(resp)) => {
                    return match resp.data {
                        CommandResult::Txn(r) => Ok(r),
                        CommandResult::Ok => Err(Unavailable("unexpected non-txn result".into())),
                    };
                }
                Ok(Err(e)) if e.forward_to_leader::<NodeInfo>().is_none() => {
                    return Err(Unavailable(e.to_string()));
                }
                // Not the leader any more, or no quorum within the timeout: retry elsewhere.
                // The command may still commit; every TxnCommand is idempotent.
                _ if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
                _ => {
                    return Err(Unavailable(format!(
                        "range {range}: no quorum within {LEADER_WAIT:?}"
                    )));
                }
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
            match tokio::time::timeout(RPC_TIMEOUT, raft.ensure_linearizable()).await {
                Ok(Ok(_)) => return Ok(f(kv.db())),
                _ if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
                _ => {
                    return Err(Unavailable(format!(
                        "range {range}: no linearizable read within {LEADER_WAIT:?}"
                    )));
                }
            }
        }
    }

    // ------------------------------------------------------------------ fault hooks

    /// Stop every replica on `node` and drop its stores, as a process crash would.
    pub async fn crash(&self, node: NodeId) {
        for router in self.routers.values() {
            router.remove(node);
        }
        let up = {
            let mut nodes = self.nodes.write().expect("cluster lock");
            nodes
                .iter_mut()
                .find(|n| n.id == node)
                .and_then(|n| n.up.take())
        };
        if let Some((_, rafts)) = up {
            for r in rafts.values() {
                let _ = r.shutdown().await;
            }
        }
    }

    /// (Re)start `node` from its on-disk stores.
    pub async fn restart(&self, node: NodeId) -> Result<(), String> {
        let base = self.dir.join(format!("n{node}"));
        // The previous incarnation's handles drop asynchronously; RocksDB's lock frees then.
        let mut attempt = 0;
        let (logs, kv) = loop {
            match (
                LogEngine::open(&base.join("raftdb")),
                KvEngine::open(&base.join("kvdb")),
            ) {
                (Ok(l), Ok(k)) => break (l, k),
                (l, k) if attempt < 200 => {
                    drop((l, k));
                    attempt += 1;
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                (Err(e), _) | (_, Err(e)) => return Err(format!("node {node}: {e}")),
            }
        };
        let mut rafts = BTreeMap::new();
        for r in self.all_ranges() {
            let (s, e) = match self.ranges.iter().find(|d| d.id == r) {
                Some(d) => (d.start.clone(), d.end.clone()),
                None => (META_INTERVAL.to_vec(), META_INTERVAL.to_vec()),
            };
            let net = RouterNetwork {
                router: self.routers[&r].clone(),
                from: node,
            };
            let raft = start_range(node, r, s, e, self.config.clone(), &logs, &kv, net)
                .await
                .map_err(|e| e.to_string())?;
            self.routers[&r].add(node, raft.clone());
            rafts.insert(r, raft);
        }
        let mut nodes = self.nodes.write().expect("cluster lock");
        if let Some(n) = nodes.iter_mut().find(|n| n.id == node) {
            n.up = Some((kv, rafts));
        }
        Ok(())
    }

    /// Cut links between nodes in different `groups`, on every range.
    pub fn partition(&self, groups: &[&[NodeId]]) {
        for router in self.routers.values() {
            router.partition(groups);
        }
    }

    pub fn heal(&self) {
        for router in self.routers.values() {
            router.heal();
        }
    }

    pub async fn shutdown(&self) {
        for (_, (_, rafts)) in self.snapshot() {
            for r in rafts.values() {
                let _ = r.shutdown().await;
            }
        }
    }
}

fn info(id: NodeId) -> NodeInfo {
    NodeInfo {
        addr: format!("node{id}"),
        zone: format!("az{id}"),
    }
}
