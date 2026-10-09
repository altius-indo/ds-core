//! An in-process multi-range cluster: every node hosts a replica of every range, and all of a
//! node's ranges share its `raftdb` and `kvdb` (multi-Raft, design/raft-ranges.md §2).
//!
//! Range 0 is the meta range: it owns no user keys and holds the timestamp oracle's mark.
//! Data ranges start at fixed split points and split further with `split` (REQ-0033).
//! Commands go to the current leader of a range, with a timeout, and are retried across leader
//! changes; a timed-out proposal may still commit, so every command is idempotent. Data reads
//! (`read_key`, `read_span`) run on the leader after a linearizability check and then confirm
//! the range still owns the key, so a read never misses writes a split moved to another range.
//!
//! Fault hooks for the harness: `crash` / `restart` a node, `partition` / `heal` the network.

// reqforge: implements REQ-0033

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use openraft::Config;

use super::mvcc::{Span, TxnCommand, TxnResponse, owns, owns_span};
use crate::raft::log_store::LogEngine;
use crate::raft::network::{Router, RouterNetwork};
use crate::raft::start_range;
use crate::raft::state_machine::{Interval, KvEngine};
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
        owns(&self.start, &self.end, key)
    }

    pub fn overlaps(&self, s: &Span) -> bool {
        self.start.as_slice() < s.end.as_slice()
            && (self.end.is_empty() || self.end.as_slice() > s.start.as_slice())
    }
}

/// A running node's store and its replica of every range.
type NodeHandles = (Arc<KvEngine>, BTreeMap<RangeId, Raft>);

#[derive(Clone)]
struct ClusterNode {
    id: NodeId,
    /// None while the node is crashed.
    up: Option<NodeHandles>,
    logs: Option<Arc<LogEngine>>,
}

pub struct Cluster {
    nodes: RwLock<Vec<ClusterNode>>,
    ranges: RwLock<Vec<RangeDesc>>,
    routers: RwLock<HashMap<RangeId, Arc<Router>>>,
    next_range: AtomicU64,
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

fn info(id: NodeId) -> NodeInfo {
    NodeInfo {
        addr: format!("node{id}"),
        zone: format!("az{id}"),
    }
}

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
            nodes: RwLock::new(
                (1..=nodes)
                    .map(|id| ClusterNode {
                        id,
                        up: None,
                        logs: None,
                    })
                    .collect(),
            ),
            next_range: AtomicU64::new(ranges.len() as u64 + 1),
            ranges: RwLock::new(ranges),
            routers: RwLock::new(routers),
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
            .chain(self.ranges().into_iter().map(|r| r.id))
            .collect()
    }

    /// Current data-range descriptors, sorted by start key.
    pub fn ranges(&self) -> Vec<RangeDesc> {
        self.ranges.read().expect("cluster lock").clone()
    }

    fn router(&self, range: RangeId) -> Option<Arc<Router>> {
        self.routers
            .read()
            .expect("cluster lock")
            .get(&range)
            .cloned()
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

    /// The range that owns user key `key` according to the current descriptors.
    pub fn range_for(&self, key: &[u8]) -> RangeDesc {
        self.ranges
            .read()
            .expect("cluster lock")
            .iter()
            .find(|r| r.contains(key))
            .cloned()
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

    async fn write(&self, range: RangeId, cmd: Command) -> Result<CommandResult, Unavailable> {
        let deadline = tokio::time::Instant::now() + LEADER_WAIT;
        loop {
            let (raft, _) = self.leader(range).await?;
            match tokio::time::timeout(RPC_TIMEOUT, raft.client_write(cmd.clone())).await {
                Ok(Ok(resp)) => return Ok(resp.data),
                Ok(Err(e)) if e.forward_to_leader::<NodeInfo>().is_none() => {
                    return Err(Unavailable(e.to_string()));
                }
                // Not the leader any more, or no quorum within the timeout: retry elsewhere.
                // The command may still commit; every command is idempotent.
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

    /// Replicate a transaction command on `range` and return its result.
    pub async fn propose(
        &self,
        range: RangeId,
        cmd: TxnCommand,
    ) -> Result<TxnResponse, Unavailable> {
        match self.write(range, Command::Txn(cmd)).await? {
            CommandResult::Txn(r) => Ok(r),
            CommandResult::Ok => Err(Unavailable("unexpected non-txn result".into())),
        }
    }

    /// Linearizable read on `range`'s leader, provided the range's applied interval passes
    /// `owned`; `Ok(None)` if a split moved the data away.
    async fn read_owned<T>(
        &self,
        range: RangeId,
        owned: impl Fn(&Interval) -> bool,
        f: impl Fn(&rocksdb::DB) -> T,
    ) -> Result<Option<T>, Unavailable> {
        let deadline = tokio::time::Instant::now() + LEADER_WAIT;
        loop {
            let (raft, kv) = self.leader(range).await?;
            match tokio::time::timeout(RPC_TIMEOUT, raft.ensure_linearizable()).await {
                Ok(Ok(_)) => {
                    if range != META_RANGE {
                        match kv.range_interval(range) {
                            Ok(Some(i)) if owned(&i) => {}
                            _ => return Ok(None),
                        }
                    }
                    return Ok(Some(f(kv.db())));
                }
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

    /// Linearizable read on the meta range (or any range, ignoring splits).
    pub async fn read<T>(
        &self,
        range: RangeId,
        f: impl Fn(&rocksdb::DB) -> T,
    ) -> Result<T, Unavailable> {
        self.read_owned(range, |_| true, f)
            .await?
            .ok_or_else(|| Unavailable(format!("range {range} moved")))
    }

    /// Linearizable read of user key `key` on whichever range owns it, following splits. `f`
    /// also gets the owning range's id (transaction records are keyed by range).
    pub async fn read_key<T>(
        &self,
        key: &[u8],
        f: impl Fn(&rocksdb::DB, RangeId) -> T,
    ) -> Result<T, Unavailable> {
        let deadline = tokio::time::Instant::now() + LEADER_WAIT;
        loop {
            let range = self.range_for(key).id;
            if let Some(v) = self
                .read_owned(range, |(s, e)| owns(s, e, key), |db| f(db, range))
                .await?
            {
                return Ok(v);
            }
            if tokio::time::Instant::now() > deadline {
                return Err(Unavailable(format!(
                    "key kept moving between ranges for {LEADER_WAIT:?}"
                )));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Linearizable read of `span`, which lies within `range` as the caller saw it; `None` if a
    /// split moved part of it, so the caller re-splits the span by the current descriptors.
    pub async fn read_span<T>(
        &self,
        range: RangeId,
        span: &Span,
        f: impl Fn(&rocksdb::DB) -> T,
    ) -> Result<Option<T>, Unavailable> {
        self.read_owned(range, |(s, e)| owns_span(s, e, span), f)
            .await
    }

    /// Split `range` at user key `at`; returns the new range's id (REQ-0033). The new range
    /// has the same replicas and serves once it has elected a leader. Requests routed with the
    /// old descriptor get `RangeMismatch` meanwhile and retry.
    pub async fn split(&self, range: RangeId, at: &[u8]) -> Result<RangeId, String> {
        let parent = self
            .ranges()
            .into_iter()
            .find(|r| r.id == range)
            .ok_or_else(|| format!("no range {range}"))?;
        if !parent.contains(at) || parent.start.as_slice() == at {
            return Err(format!("split key is not strictly inside range {range}"));
        }
        let child = self.next_range.fetch_add(1, Ordering::Relaxed);
        let cmd = Command::Split {
            at: at.to_vec(),
            new_range: child,
        };
        match self.write(range, cmd).await.map_err(|e| e.to_string())? {
            CommandResult::Ok => {}
            other => return Err(format!("range {range} refused the split: {other:?}")),
        }
        // Bring up the child group on every live node. A node that applies the split later,
        // or restarts, finds the same interval persisted by the parent's log.
        self.routers
            .write()
            .expect("cluster lock")
            .insert(child, Router::new());
        let members: BTreeMap<NodeId, NodeInfo> = self
            .node_ids()
            .into_iter()
            .map(|id| (id, info(id)))
            .collect();
        let mut first = None;
        for (id, (kv, _)) in self.snapshot() {
            let raft = self
                .start_replica(id, child, at.to_vec(), parent.end.clone(), &kv)
                .await?;
            first.get_or_insert(raft);
        }
        if let Some(r) = first {
            let _ = r.initialize(members).await;
        }
        {
            let mut ranges = self.ranges.write().expect("cluster lock");
            if let Some(p) = ranges.iter_mut().find(|r| r.id == range) {
                p.end = at.to_vec();
            }
            ranges.push(RangeDesc {
                id: child,
                start: at.to_vec(),
                end: parent.end,
            });
            ranges.sort_by(|a, b| a.start.cmp(&b.start));
        }
        self.leader(child).await.map_err(|e| e.to_string())?;
        Ok(child)
    }

    async fn start_replica(
        &self,
        node: NodeId,
        range: RangeId,
        start: Vec<u8>,
        end: Vec<u8>,
        kv: &Arc<KvEngine>,
    ) -> Result<Raft, String> {
        let logs = {
            let nodes = self.nodes.read().expect("cluster lock");
            nodes
                .iter()
                .find(|n| n.id == node)
                .and_then(|n| n.logs.clone())
        }
        .ok_or_else(|| format!("node {node} is down"))?;
        let router = self
            .router(range)
            .ok_or_else(|| format!("no router for range {range}"))?;
        let net = RouterNetwork {
            router: router.clone(),
            from: node,
        };
        let raft = start_range(node, range, start, end, self.config.clone(), &logs, kv, net)
            .await
            .map_err(|e| e.to_string())?;
        router.add(node, raft.clone());
        let mut nodes = self.nodes.write().expect("cluster lock");
        if let Some((_, rafts)) = nodes
            .iter_mut()
            .find(|n| n.id == node)
            .and_then(|n| n.up.as_mut())
        {
            rafts.insert(range, raft.clone());
        }
        Ok(raft)
    }

    // ------------------------------------------------------------------ fault hooks

    /// Stop every replica on `node` and drop its stores, as a process crash would.
    pub async fn crash(&self, node: NodeId) {
        for router in self.routers.read().expect("cluster lock").values() {
            router.remove(node);
        }
        let up = {
            let mut nodes = self.nodes.write().expect("cluster lock");
            nodes.iter_mut().find(|n| n.id == node).and_then(|n| {
                n.logs = None;
                n.up.take()
            })
        };
        if let Some((_, rafts)) = up {
            for r in rafts.values() {
                let _ = r.shutdown().await;
            }
        }
    }

    /// (Re)start `node` from its on-disk stores, with a replica of every current range.
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
        {
            let mut nodes = self.nodes.write().expect("cluster lock");
            if let Some(n) = nodes.iter_mut().find(|n| n.id == node) {
                n.logs = Some(logs);
                n.up = Some((kv.clone(), BTreeMap::new()));
            }
        }
        for r in self.all_ranges() {
            let (s, e) = match self.ranges().into_iter().find(|d| d.id == r) {
                Some(d) => (d.start, d.end),
                None => (META_INTERVAL.to_vec(), META_INTERVAL.to_vec()),
            };
            self.start_replica(node, r, s, e, &kv).await?;
        }
        Ok(())
    }

    /// Cut links between nodes in different `groups`, on every range.
    pub fn partition(&self, groups: &[&[NodeId]]) {
        for router in self.routers.read().expect("cluster lock").values() {
            router.partition(groups);
        }
    }

    pub fn heal(&self) {
        for router in self.routers.read().expect("cluster lock").values() {
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
