//! Raft replication per range (DEC-0002) on openraft, with logs and state on RocksDB.

pub mod config;
pub mod log_store;
pub mod network;
pub mod placement;
pub mod state_machine;
pub mod types;

use std::fmt;
use std::sync::Arc;

use openraft::{Config, RaftNetworkFactory};

use log_store::LogEngine;
use state_machine::KvEngine;
use types::{NodeId, Raft, RangeId, TypeConfig};

/// Production timing (design/raft-ranges.md §7): failover within the 10 s p99 of REQ-0018.
pub fn default_config() -> Config {
    config::Timing::default().to_openraft()
}

#[derive(Debug)]
pub enum StartError {
    Config(openraft::ConfigError),
    Raft(openraft::error::Fatal<NodeId>),
}

impl fmt::Display for StartError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(e) => write!(f, "invalid raft config: {e}"),
            Self::Raft(e) => write!(f, "raft failed to start: {e}"),
        }
    }
}

impl std::error::Error for StartError {}

/// Start this node's replica of `range`, owning user keys `[start, end)`.
#[allow(clippy::too_many_arguments)]
pub async fn start_range<N>(
    node_id: NodeId,
    range: RangeId,
    start: Vec<u8>,
    end: Vec<u8>,
    config: Config,
    logs: &Arc<LogEngine>,
    kv: &Arc<KvEngine>,
    network: N,
) -> Result<Raft, StartError>
where
    N: RaftNetworkFactory<TypeConfig>,
{
    let config = Arc::new(config.validate().map_err(StartError::Config)?);
    Raft::new(
        node_id,
        config,
        network,
        logs.range(range),
        kv.range(range, start, end),
    )
    .await
    .map_err(StartError::Raft)
}
