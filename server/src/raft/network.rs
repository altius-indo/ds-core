//! In-process Raft transport for tests and the single-process harness. Links between nodes
//! can be cut to simulate partitions. The TLS peer transport replaces it in a deployment.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use openraft::AnyError;
use openraft::error::{
    InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use super::types::{NodeId, NodeInfo, Raft, TypeConfig};

/// Routes RPCs between the Raft instances of one group, all in this process.
#[derive(Default)]
pub struct Router {
    nodes: Mutex<HashMap<NodeId, Raft>>,
    cut: Mutex<HashSet<(NodeId, NodeId)>>,
}

impl Router {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn add(&self, id: NodeId, raft: Raft) {
        self.nodes.lock().expect("router lock").insert(id, raft);
    }

    /// Take a node off the network, as if its process had died.
    pub fn remove(&self, id: NodeId) -> Option<Raft> {
        self.nodes.lock().expect("router lock").remove(&id)
    }

    /// Cut every link between nodes in different `groups`; links within a group stay up.
    pub fn partition(&self, groups: &[&[NodeId]]) {
        let mut cut = self.cut.lock().expect("router lock");
        cut.clear();
        for (i, a) in groups.iter().enumerate() {
            for b in &groups[i + 1..] {
                for &x in *a {
                    for &y in *b {
                        cut.insert((x, y));
                        cut.insert((y, x));
                    }
                }
            }
        }
    }

    pub fn heal(&self) {
        self.cut.lock().expect("router lock").clear();
    }

    fn target(&self, from: NodeId, to: NodeId) -> Result<Raft, Unreachable> {
        if self.cut.lock().expect("router lock").contains(&(from, to)) {
            return Err(Unreachable::new(&AnyError::error(format!(
                "link {from}->{to} is cut"
            ))));
        }
        self.nodes
            .lock()
            .expect("router lock")
            .get(&to)
            .cloned()
            .ok_or_else(|| Unreachable::new(&AnyError::error(format!("node {to} is not running"))))
    }
}

/// Network factory for one node: every client it creates sends from `from`.
#[derive(Clone)]
pub struct RouterNetwork {
    pub router: Arc<Router>,
    pub from: NodeId,
}

pub struct RouterConn {
    router: Arc<Router>,
    from: NodeId,
    to: NodeId,
}

impl RaftNetworkFactory<TypeConfig> for RouterNetwork {
    type Network = RouterConn;

    async fn new_client(&mut self, target: NodeId, _node: &NodeInfo) -> RouterConn {
        RouterConn {
            router: self.router.clone(),
            from: self.from,
            to: target,
        }
    }
}

type RpcErr<E = RaftError<NodeId>> = RPCError<NodeId, NodeInfo, E>;

impl RaftNetwork<TypeConfig> for RouterConn {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RpcErr> {
        let raft = self
            .router
            .target(self.from, self.to)
            .map_err(RPCError::Unreachable)?;
        let resp = raft
            .append_entries(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.to, e)))?;
        // The reply crosses the same link; a link cut mid-call loses it.
        self.router
            .target(self.to, self.from)
            .map_err(RPCError::Unreachable)?;
        Ok(resp)
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, RpcErr<RaftError<NodeId, InstallSnapshotError>>>
    {
        let raft = self
            .router
            .target(self.from, self.to)
            .map_err(RPCError::Unreachable)?;
        raft.install_snapshot(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.to, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RpcErr> {
        let raft = self
            .router
            .target(self.from, self.to)
            .map_err(RPCError::Unreachable)?;
        let resp = raft
            .vote(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.to, e)))?;
        self.router
            .target(self.to, self.from)
            .map_err(|e| RPCError::Network(NetworkError::new(&AnyError::error(e.to_string()))))?;
        Ok(resp)
    }
}
