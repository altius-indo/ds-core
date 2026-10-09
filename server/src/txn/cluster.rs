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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use openraft::Config;

use super::mvcc::{RangeStats, Span, TxnCommand, TxnResponse, live_stats, owns, owns_span};
use crate::raft::log_store::LogEngine;
use crate::raft::network::{Router, RouterNetwork};
use crate::raft::placement::next_move;
use crate::raft::start_range;
use crate::raft::state_machine::{Interval, KvEngine};
use crate::raft::types::{Command, CommandResult, NodeId, NodeInfo, Raft, RangeId};

pub const META_RANGE: RangeId = 0;
/// How long `split` keeps re-proposing an undecided split before giving up.
const SPLIT_WAIT: Duration = Duration::from_secs(60);
/// Sentinel interval for the meta range: `[x, x)` owns no user keys.
const META_INTERVAL: &[u8] = b"\xff\xffmeta";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeDesc {
    pub id: RangeId,
    pub start: Vec<u8>,
    /// Exclusive; empty means +∞.
    pub end: Vec<u8>,
    /// Nodes holding a voting replica.
    pub replicas: BTreeSet<NodeId>,
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
    /// Serializes topology changes (split, crash, restart): a node restarting mid-split must
    /// see the child range either in the descriptors or in the split's replica start-up.
    topology: tokio::sync::Mutex<()>,
    /// Replicas waiting for their node's parent replica to apply the split that created them.
    pending: std::sync::Mutex<std::collections::BTreeSet<(NodeId, RangeId)>>,
    /// Ranges with ids up to this were created at start; higher ids came from splits.
    initial_ranges: RangeId,
    /// Nodes holding a replica of the meta range.
    meta_replicas: BTreeSet<NodeId>,
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
    ) -> Result<Arc<Self>, String> {
        let mut ranges = Vec::new();
        let mut start: Vec<u8> = Vec::new();
        for (i, p) in split_points.iter().enumerate() {
            ranges.push(RangeDesc {
                id: i as RangeId + 1,
                start: start.clone(),
                end: p.to_vec(),
                replicas: (1..=nodes).collect(),
            });
            start = p.to_vec();
        }
        ranges.push(RangeDesc {
            id: split_points.len() as RangeId + 1,
            start,
            end: Vec::new(),
            replicas: (1..=nodes).collect(),
        });
        let mut routers = HashMap::new();
        for r in std::iter::once(META_RANGE).chain(ranges.iter().map(|r| r.id)) {
            routers.insert(r, Router::new());
        }
        let cluster = Arc::new(Self {
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
            topology: tokio::sync::Mutex::new(()),
            pending: std::sync::Mutex::new(Default::default()),
            initial_ranges: ranges.len() as RangeId,
            meta_replicas: (1..=nodes).collect(),
            ranges: RwLock::new(ranges),
            routers: RwLock::new(routers),
            dir: dir.to_path_buf(),
            config,
        });
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
        let weak = Arc::downgrade(&cluster);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let Some(c) = weak.upgrade() else { break };
                c.start_pending().await;
            }
        });
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
        let topology = self.topology.lock().await;
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
        // Keep proposing until the outcome is definite: a proposal that timed out may still
        // commit, and giving up then would leave the parent shrunk with no child group. The
        // split is idempotent, so a repeat of a committed split reports success.
        let deadline = tokio::time::Instant::now() + SPLIT_WAIT;
        loop {
            match self.write(range, cmd.clone()).await {
                Ok(CommandResult::Ok) => break,
                Ok(other) => return Err(format!("range {range} refused the split: {other:?}")),
                Err(e) if tokio::time::Instant::now() >= deadline => {
                    // Still uncertain: the child id stays reserved and unrouted. The keys it
                    // would own are unavailable until an operator retries, but never wrong.
                    return Err(format!("split of range {range} undecided: {e}"));
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
        // Publish the child, then bring up its replica on every live node whose parent replica
        // has applied the split; those bootstrap the group. The rest start from the reconciler
        // once their parent replica has applied the split too.
        self.routers
            .write()
            .expect("cluster lock")
            .insert(child, Router::new());
        {
            let mut ranges = self.ranges.write().expect("cluster lock");
            if let Some(p) = ranges.iter_mut().find(|r| r.id == range) {
                p.end = at.to_vec();
            }
            ranges.push(RangeDesc {
                id: child,
                start: at.to_vec(),
                end: parent.end,
                replicas: parent.replicas.clone(),
            });
            ranges.sort_by(|a, b| a.start.cmp(&b.start));
        }
        for id in parent.replicas {
            self.start_or_defer(id, child, true).await?;
        }
        // The child may need a deferred replica for a quorum, and the reconciler that starts
        // it takes the topology lock: release it before waiting for the election.
        drop(topology);
        self.leader(child).await.map_err(|e| e.to_string())?;
        Ok(child)
    }

    /// Live keys, bytes and median key of `range`, read on its leader.
    pub async fn range_stats(&self, range: RangeId) -> Result<RangeStats, Unavailable> {
        let desc = self
            .ranges()
            .into_iter()
            .find(|d| d.id == range)
            .ok_or_else(|| Unavailable(format!("no range {range}")))?;
        self.read(range, |db| live_stats(db, &desc.start, &desc.end))
            .await?
            .map_err(Unavailable)
    }

    /// Split every range holding more than `max_bytes` of live data at its median key
    /// (REQ-0033: automatic split of ranges exceeding a configurable size). Returns the
    /// splits made.
    pub async fn split_oversized(&self, max_bytes: u64) -> Vec<(RangeId, RangeId)> {
        let mut done = Vec::new();
        for d in self.ranges() {
            let Ok(stats) = self.range_stats(d.id).await else {
                continue;
            };
            if stats.bytes <= max_bytes {
                continue;
            }
            if let Some(at) = stats.median
                && at.as_slice() > d.start.as_slice()
                && let Ok(child) = self.split(d.id, &at).await
            {
                done.push((d.id, child));
            }
        }
        done
    }

    /// Make one replica move towards balance, if any is needed (REQ-0033 AC1). Returns the
    /// move made.
    pub async fn rebalance_step(&self) -> Result<Option<(RangeId, NodeId, NodeId)>, String> {
        let ranges: Vec<(RangeId, BTreeSet<NodeId>)> = self
            .ranges()
            .into_iter()
            .map(|d| (d.id, d.replicas))
            .collect();
        let Some(m @ (range, from, to)) = next_move(&ranges, &self.node_ids()) else {
            return Ok(None);
        };
        self.move_replica(range, from, to).await?;
        Ok(Some(m))
    }

    /// Replicas per node, over the data ranges.
    pub fn replica_counts(&self) -> BTreeMap<NodeId, usize> {
        let mut count: BTreeMap<NodeId, usize> =
            self.node_ids().into_iter().map(|n| (n, 0)).collect();
        for d in self.ranges() {
            for n in d.replicas {
                *count.entry(n).or_default() += 1;
            }
        }
        count
    }

    /// Add an empty node, in a zone of its own; it holds no replicas until the rebalancer moves
    /// some to it.
    pub async fn add_node(&self) -> Result<NodeId, String> {
        let _topology = self.topology.lock().await;
        let id = self.node_ids().into_iter().max().unwrap_or(0) + 1;
        self.nodes.write().expect("cluster lock").push(ClusterNode {
            id,
            up: None,
            logs: None,
        });
        self.restart_locked(id).await?;
        Ok(id)
    }

    /// Move `range`'s replica from node `from` to node `to` (REQ-0033 rebalancing). The new
    /// replica starts from a snapshot of the leader, off the network until installed (the
    /// range's log may not reach back to data it never had), joins as a learner, and replaces
    /// `from` through joint consensus. `from`'s replica is then stopped and its data and log
    /// deleted, so a later replica there starts clean.
    pub async fn move_replica(
        &self,
        range: RangeId,
        from: NodeId,
        to: NodeId,
    ) -> Result<(), String> {
        let _topology = self.topology.lock().await;
        let desc = self
            .ranges()
            .into_iter()
            .find(|d| d.id == range)
            .ok_or_else(|| format!("no range {range}"))?;
        if !desc.replicas.contains(&from) || desc.replicas.contains(&to) {
            return Err(format!(
                "range {range}: cannot move {from} -> {to}, replicas {:?}",
                desc.replicas
            ));
        }
        let (kv_to, logs_to) = self
            .handles(to)
            .ok_or_else(|| format!("node {to} is down"))?;
        let (leader, leader_kv) = self.leader(range).await.map_err(|e| e.to_string())?;
        let vote = leader.metrics().borrow().vote;
        let snapshot = leader_kv
            .range(range, desc.start.clone(), desc.end.clone())
            .snapshot_now()
            .map_err(|e| e.to_string())?;
        kv_to
            .wipe_range(range, &desc.start, &desc.end)
            .map_err(|e| e.to_string())?;
        logs_to.wipe_range(range).map_err(|e| e.to_string())?;
        let raft = self
            .start_unrouted(to, range, desc.start.clone(), desc.end.clone(), &kv_to)
            .await?;
        raft.install_full_snapshot(vote, snapshot)
            .await
            .map_err(|e| e.to_string())?;
        self.route(to, range, &raft)?;

        let mut voters = desc.replicas.clone();
        voters.remove(&from);
        voters.insert(to);
        let changed = self.change_voters(range, to, &voters).await;
        if let Err(e) = changed {
            // Back out: the old voters still serve; drop the half-joined replica.
            self.drop_replica(to, range, &desc).await;
            return Err(e);
        }
        {
            let mut ranges = self.ranges.write().expect("cluster lock");
            if let Some(d) = ranges.iter_mut().find(|d| d.id == range) {
                d.replicas = voters;
            }
        }
        self.drop_replica(from, range, &desc).await;
        Ok(())
    }

    /// Add `learner` to `range` and make `voters` the voting set, retrying across leader
    /// changes until a leader confirms it.
    async fn change_voters(
        &self,
        range: RangeId,
        learner: NodeId,
        voters: &BTreeSet<NodeId>,
    ) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + SPLIT_WAIT;
        loop {
            let (leader, _) = self.leader(range).await.map_err(|e| e.to_string())?;
            let attempt = async {
                leader
                    .add_learner(learner, info(learner), true)
                    .await
                    .map_err(|e| e.to_string())?;
                leader
                    .change_membership(
                        openraft::ChangeMembers::ReplaceAllVoters(voters.clone()),
                        false,
                    )
                    .await
                    .map_err(|e| e.to_string())
            };
            let error = match tokio::time::timeout(SPLIT_WAIT, attempt).await {
                Ok(Ok(_)) => return Ok(()),
                Ok(Err(e)) => e,
                Err(_) => "timed out".to_string(),
            };
            if tokio::time::Instant::now() >= deadline {
                return Err(format!("range {range}: voter change failed: {error}"));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Stop `node`'s replica of `range` and delete its data and log there.
    async fn drop_replica(&self, node: NodeId, range: RangeId, desc: &RangeDesc) {
        if let Some(router) = self.router(range) {
            router.remove(node);
        }
        let raft = {
            let mut nodes = self.nodes.write().expect("cluster lock");
            nodes
                .iter_mut()
                .find(|n| n.id == node)
                .and_then(|n| n.up.as_mut())
                .and_then(|(_, rafts)| rafts.remove(&range))
        };
        if let Some(r) = raft {
            let _ = r.shutdown().await;
        }
        if let Some((kv, logs)) = self.handles(node) {
            let _ = kv.wipe_range(range, &desc.start, &desc.end);
            let _ = logs.wipe_range(range);
        }
    }

    /// A live node's stores.
    fn handles(&self, node: NodeId) -> Option<(Arc<KvEngine>, Arc<LogEngine>)> {
        let nodes = self.nodes.read().expect("cluster lock");
        let n = nodes.iter().find(|n| n.id == node)?;
        Some((n.up.as_ref()?.0.clone(), n.logs.clone()?))
    }

    /// Start `node`'s replica of `range` if it is ready, else queue it for the reconciler.
    async fn start_or_defer(
        &self,
        node: NodeId,
        range: RangeId,
        bootstrap: bool,
    ) -> Result<(), String> {
        if !self.try_start(node, range, bootstrap).await? {
            self.pending
                .lock()
                .expect("cluster lock")
                .insert((node, range));
        }
        Ok(())
    }

    /// Start `node`'s replica of `range` unless another replica on that node still owns part
    /// of its interval (a parent that has not applied the split yet). Ok(true) when the
    /// replica is running, or the node is down (its restart will start it).
    async fn try_start(
        &self,
        node: NodeId,
        range: RangeId,
        bootstrap: bool,
    ) -> Result<bool, String> {
        let Some((kv, rafts)) = self
            .nodes
            .read()
            .expect("cluster lock")
            .iter()
            .find(|n| n.id == node)
            .and_then(|n| n.up.clone())
        else {
            return Ok(true);
        };
        if rafts.contains_key(&range) || !self.replicas_of(range).contains(&node) {
            return Ok(true);
        }
        let (start, end) = match self.ranges().into_iter().find(|d| d.id == range) {
            Some(d) => (d.start, d.end),
            None => {
                // The meta range owns no user keys.
                return self
                    .start_replica(
                        node,
                        range,
                        META_INTERVAL.to_vec(),
                        META_INTERVAL.to_vec(),
                        &kv,
                    )
                    .await
                    .map(|_| true);
            }
        };
        let others = self
            .ranges()
            .into_iter()
            .filter(|d| d.id != range && d.replicas.contains(&node))
            .map(|d| d.id);
        if kv
            .overlapped(others, &start, &end)
            .map_err(|e| e.to_string())?
        {
            return Ok(false);
        }
        if !self.is_split_child(range) {
            self.start_replica(node, range, start, end, &kv).await?;
            return Ok(true);
        }
        if kv
            .range_interval(range)
            .map_err(|e| e.to_string())?
            .is_some()
        {
            // This node's parent replica applied the split: the child's state here is exactly
            // the parent's at the split, which is where the child's log begins. Replicas
            // started with the split bootstrap the group; one that starts later joins when the
            // leader contacts it and replays the child's log from the start.
            let raft = self.start_replica(node, range, start, end, &kv).await?;
            if bootstrap {
                let _ = raft.initialize(self.members_of(range)).await;
            }
            return Ok(true);
        }
        // This node's parent replica skipped the split (it installed a later parent snapshot),
        // so the child's keys here hold stale parent data, and replaying the child's log onto
        // them would be wrong. Start the replica empty and off the network, install a snapshot
        // of the child's leader, and only then connect it.
        let Some((leader_kv, vote)) = self.leader_store(range) else {
            return Ok(false);
        };
        let snapshot = leader_kv
            .range(range, start.clone(), end.clone())
            .snapshot_now()
            .map_err(|e| e.to_string())?;
        kv.wipe_range(range, &start, &end)
            .map_err(|e| e.to_string())?;
        let raft = self.start_unrouted(node, range, start, end, &kv).await?;
        raft.install_full_snapshot(vote, snapshot)
            .await
            .map_err(|e| e.to_string())?;
        self.route(node, range, &raft)?;
        Ok(true)
    }

    /// The store and vote of `range`'s current leader.
    fn leader_store(&self, range: RangeId) -> Option<(Arc<KvEngine>, openraft::Vote<NodeId>)> {
        self.snapshot().into_iter().find_map(|(id, (kv, rafts))| {
            let m = rafts.get(&range)?.metrics().borrow().clone();
            (m.state == openraft::ServerState::Leader && m.current_leader == Some(id))
                .then_some((kv, m.vote))
        })
    }

    /// The nodes holding a replica of `range`, as a Raft membership.
    fn members_of(&self, range: RangeId) -> BTreeMap<NodeId, NodeInfo> {
        self.replicas_of(range)
            .into_iter()
            .map(|id| (id, info(id)))
            .collect()
    }

    pub fn replicas_of(&self, range: RangeId) -> BTreeSet<NodeId> {
        if range == META_RANGE {
            return self.meta_replicas.clone();
        }
        self.ranges()
            .into_iter()
            .find(|d| d.id == range)
            .map(|d| d.replicas)
            .unwrap_or_default()
    }

    /// Whether `range` was created by a split rather than at cluster start.
    fn is_split_child(&self, range: RangeId) -> bool {
        range > self.initial_ranges
    }

    /// Start every queued replica that has become ready.
    async fn start_pending(&self) {
        let _topology = self.topology.lock().await;
        let queued: Vec<(NodeId, RangeId)> = self
            .pending
            .lock()
            .expect("cluster lock")
            .iter()
            .copied()
            .collect();
        for (node, range) in queued {
            if let Ok(true) = self.try_start(node, range, false).await {
                self.pending
                    .lock()
                    .expect("cluster lock")
                    .remove(&(node, range));
            }
        }
    }

    async fn start_replica(
        &self,
        node: NodeId,
        range: RangeId,
        start: Vec<u8>,
        end: Vec<u8>,
        kv: &Arc<KvEngine>,
    ) -> Result<Raft, String> {
        let raft = self.start_unrouted(node, range, start, end, kv).await?;
        self.route(node, range, &raft)?;
        Ok(raft)
    }

    /// Start `node`'s replica of `range` without connecting it to the range's network yet.
    async fn start_unrouted(
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
        let net = RouterNetwork { router, from: node };
        start_range(node, range, start, end, self.config.clone(), &logs, kv, net)
            .await
            .map_err(|e| e.to_string())
    }

    /// Connect a started replica to its range's network and record it on its node.
    fn route(&self, node: NodeId, range: RangeId, raft: &Raft) -> Result<(), String> {
        self.router(range)
            .ok_or_else(|| format!("no router for range {range}"))?
            .add(node, raft.clone());
        let mut nodes = self.nodes.write().expect("cluster lock");
        if let Some((_, rafts)) = nodes
            .iter_mut()
            .find(|n| n.id == node)
            .and_then(|n| n.up.as_mut())
        {
            rafts.insert(range, raft.clone());
        }
        Ok(())
    }

    // ------------------------------------------------------------------ fault hooks

    /// Stop every replica on `node` and drop its stores, as a process crash would.
    pub async fn crash(&self, node: NodeId) {
        let _topology = self.topology.lock().await;
        // Restart re-queues whatever this node still has to start.
        self.pending
            .lock()
            .expect("cluster lock")
            .retain(|(n, _)| *n != node);
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
        let _topology = self.topology.lock().await;
        self.restart_locked(node).await
    }

    async fn restart_locked(&self, node: NodeId) -> Result<(), String> {
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
        // Replicas moved away while this node was down: delete what they left behind.
        for d in self.ranges() {
            if !d.replicas.contains(&node)
                && kv
                    .range_interval(d.id)
                    .map_err(|e| e.to_string())?
                    .is_some()
            {
                kv.wipe_range(d.id, &d.start, &d.end)
                    .map_err(|e| e.to_string())?;
                logs.wipe_range(d.id).map_err(|e| e.to_string())?;
            }
        }
        {
            let mut nodes = self.nodes.write().expect("cluster lock");
            if let Some(n) = nodes.iter_mut().find(|n| n.id == node) {
                n.logs = Some(logs);
                n.up = Some((kv.clone(), BTreeMap::new()));
            }
        }
        for r in self.all_ranges() {
            self.start_or_defer(node, r, false).await?;
        }
        Ok(())
    }

    /// Make `node`'s replica the leader of `range` by having it campaign until it wins (it
    /// can only win with an up-to-date log). For tests and the harness.
    pub async fn make_leader(&self, range: RangeId, node: NodeId) -> Result<(), String> {
        let raft = self
            .nodes
            .read()
            .expect("cluster lock")
            .iter()
            .find(|n| n.id == node)
            .and_then(|n| n.up.as_ref()?.1.get(&range).cloned())
            .ok_or_else(|| format!("node {node} has no replica of range {range}"))?;
        let deadline = tokio::time::Instant::now() + LEADER_WAIT;
        while tokio::time::Instant::now() < deadline {
            let m = raft.metrics().borrow().clone();
            if m.state == openraft::ServerState::Leader {
                return Ok(());
            }
            let _ = raft.trigger().elect().await;
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Err(format!("node {node} did not win range {range}"))
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
