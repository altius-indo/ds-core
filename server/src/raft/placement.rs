//! Voter placement across availability zones (REQ-0001).
//!
//! A range has 3 or 5 voters (AC1), each in a different zone (AC2). Within a zone the
//! least-loaded store is chosen; zones are taken in order of their least-loaded store.

// reqforge: implements REQ-0001

use std::collections::BTreeMap;
use std::fmt;

use super::types::NodeId;

pub const ALLOWED_VOTER_COUNTS: [usize; 2] = [3, 5];
pub const DEFAULT_VOTERS: usize = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreInfo {
    pub node_id: NodeId,
    pub zone: String,
    /// Replicas already on the store; the load measure for placement.
    pub replicas: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlacementError {
    InvalidVoterCount(usize),
    NotEnoughZones { needed: usize, available: usize },
}

impl fmt::Display for PlacementError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidVoterCount(n) => write!(
                f,
                "a range needs 3 or 5 voting replicas, got {n} (even counts and other sizes are rejected)"
            ),
            Self::NotEnoughZones { needed, available } => write!(
                f,
                "{needed} voters need {needed} distinct zones; only {available} have a store"
            ),
        }
    }
}

impl std::error::Error for PlacementError {}

/// REQ-0001 AC1: only 3 or 5 voters.
pub fn validate_voter_count(n: usize) -> Result<(), PlacementError> {
    if ALLOWED_VOTER_COUNTS.contains(&n) {
        Ok(())
    } else {
        Err(PlacementError::InvalidVoterCount(n))
    }
}

/// Pick `voters` stores in distinct zones (REQ-0001 AC2).
pub fn place_voters(stores: &[StoreInfo], voters: usize) -> Result<Vec<NodeId>, PlacementError> {
    validate_voter_count(voters)?;
    let mut best_per_zone: BTreeMap<&str, &StoreInfo> = BTreeMap::new();
    for s in stores {
        let better = match best_per_zone.get(s.zone.as_str()) {
            Some(cur) => (s.replicas, s.node_id) < (cur.replicas, cur.node_id),
            None => true,
        };
        if better {
            best_per_zone.insert(&s.zone, s);
        }
    }
    if best_per_zone.len() < voters {
        return Err(PlacementError::NotEnoughZones {
            needed: voters,
            available: best_per_zone.len(),
        });
    }
    let mut picks: Vec<&StoreInfo> = best_per_zone.into_values().collect();
    picks.sort_by_key(|s| (s.replicas, s.node_id));
    Ok(picks.into_iter().take(voters).map(|s| s.node_id).collect())
}
