//! Raft type configuration shared by every range group.

use std::fmt;
use std::io::Cursor;

use serde::{Deserialize, Serialize};

/// Identifies a node (store) within every Raft group it belongs to.
pub type NodeId = u64;
pub type RangeId = u64;

/// Where a replica lives: its peer address and availability zone (REQ-0001 AC2).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    pub addr: String,
    pub zone: String,
}

impl fmt::Display for NodeInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.addr, self.zone)
    }
}

/// A state-machine command replicated through a range's Raft log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    /// Applied atomically, in order.
    Batch(Vec<Command>),
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandResult;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Command,
        R = CommandResult,
        NodeId = NodeId,
        Node = NodeInfo,
        SnapshotData = Cursor<Vec<u8>>,
);

pub type Raft = openraft::Raft<TypeConfig>;
pub type Entry = openraft::Entry<TypeConfig>;
pub type LogId = openraft::LogId<NodeId>;
pub type Vote = openraft::Vote<NodeId>;
pub type StoredMembership = openraft::StoredMembership<NodeId, NodeInfo>;
pub type SnapshotMeta = openraft::SnapshotMeta<NodeId, NodeInfo>;
pub type StorageError = openraft::StorageError<NodeId>;
pub type StorageIOError = openraft::StorageIOError<NodeId>;
