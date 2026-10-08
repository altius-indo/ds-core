//! Replica membership changes (REQ-0017, design/raft-ranges.md §8).
//!
//! New replicas join as learners. A learner is promoted to voter only after its replication
//! lag has stayed within `max_lag` entries for `checks` consecutive samples (AC2); the voter
//! change itself goes through openraft's joint consensus, so two majorities can never form
//! in one term (AC1).

// reqforge: implements REQ-0017

use std::collections::BTreeSet;
use std::fmt;
use std::time::Duration;

use openraft::ChangeMembers;

use super::types::{NodeId, NodeInfo, Raft};

/// Default promotion bound: the learner must be within this many log entries of the leader.
pub const DEFAULT_MAX_PROMOTE_LAG: u64 = 1000;

#[derive(Debug, Clone, Copy)]
pub struct PromotionPolicy {
    pub max_lag: u64,
    /// Consecutive in-bound samples required before promoting.
    pub checks: u32,
    pub interval: Duration,
    pub give_up_after: Duration,
}

impl Default for PromotionPolicy {
    fn default() -> Self {
        Self {
            max_lag: DEFAULT_MAX_PROMOTE_LAG,
            checks: 3,
            interval: Duration::from_millis(100),
            give_up_after: Duration::from_secs(60),
        }
    }
}

#[derive(Debug)]
pub enum MembershipError {
    NotLeader,
    LagTooHigh { node: NodeId, lag: Option<u64> },
    Raft(String),
}

impl fmt::Display for MembershipError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotLeader => f.write_str("membership changes must go through the leader"),
            Self::LagTooHigh { node, lag } => {
                write!(f, "learner {node} did not catch up (lag {lag:?})")
            }
            Self::Raft(e) => write!(f, "raft: {e}"),
        }
    }
}

impl std::error::Error for MembershipError {}

/// Entries `node` is behind the leader, from the leader's replication progress; `None` if
/// `raft` is not the leader or has no progress for `node` yet.
pub fn replication_lag(raft: &Raft, node: NodeId) -> Option<u64> {
    let m = raft.metrics().borrow().clone();
    let last = m.last_log_index?;
    let matched = m
        .replication
        .as_ref()?
        .get(&node)?
        .as_ref()
        .map_or(0, |l| l.index);
    Some(last.saturating_sub(matched))
}

pub fn voters(raft: &Raft) -> BTreeSet<NodeId> {
    raft.metrics()
        .borrow()
        .membership_config
        .membership()
        .voter_ids()
        .collect()
}

fn is_leader(raft: &Raft) -> bool {
    let m = raft.metrics().borrow().clone();
    m.current_leader == Some(m.id)
}

/// Add `node` as a learner (non-voting; never counted towards a quorum).
pub async fn add_learner(
    leader: &Raft,
    node: NodeId,
    info: NodeInfo,
) -> Result<(), MembershipError> {
    if !is_leader(leader) {
        return Err(MembershipError::NotLeader);
    }
    leader
        .add_learner(node, info, false)
        .await
        .map(|_| ())
        .map_err(|e| MembershipError::Raft(e.to_string()))
}

/// Promote a learner once it has caught up (REQ-0017 AC2). Returns the lag it was promoted at.
pub async fn promote_when_caught_up(
    leader: &Raft,
    node: NodeId,
    policy: PromotionPolicy,
) -> Result<u64, MembershipError> {
    let start = tokio::time::Instant::now();
    let mut in_bound = 0;
    let mut lag = None;
    while in_bound < policy.checks {
        if !is_leader(leader) {
            return Err(MembershipError::NotLeader);
        }
        if start.elapsed() > policy.give_up_after {
            return Err(MembershipError::LagTooHigh { node, lag });
        }
        lag = replication_lag(leader, node);
        in_bound = match lag {
            Some(l) if l <= policy.max_lag => in_bound + 1,
            _ => 0,
        };
        if in_bound < policy.checks {
            tokio::time::sleep(policy.interval).await;
        }
    }
    let at = lag.expect("in bound implies a sample");
    leader
        .change_membership(ChangeMembers::AddVoterIds(BTreeSet::from([node])), true)
        .await
        .map_err(|e| MembershipError::Raft(e.to_string()))?;
    Ok(at)
}

/// Remove a voter. `retain = false` drops it from the group entirely.
///
/// The removed replica must then be garbage-collected: stopped and its data deleted. It may
/// never see the configuration that excludes it, so left running it keeps campaigning and
/// raising its term; if the node rejoins later it must do so as a fresh, empty replica.
pub async fn remove_voter(leader: &Raft, node: NodeId) -> Result<(), MembershipError> {
    if !is_leader(leader) {
        return Err(MembershipError::NotLeader);
    }
    leader
        .change_membership(ChangeMembers::RemoveVoters(BTreeSet::from([node])), false)
        .await
        .map(|_| ())
        .map_err(|e| MembershipError::Raft(e.to_string()))
}
