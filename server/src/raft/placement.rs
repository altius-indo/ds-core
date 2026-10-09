//! Voter placement across availability zones (REQ-0001).
//!
//! A range has 3 or 5 voters (AC1), each in a different zone (AC2). Within a zone the
//! least-loaded store is chosen; zones are taken in order of their least-loaded store.

// reqforge: implements REQ-0001

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use super::types::{NodeId, RangeId};

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

/// The next replica move towards balance (REQ-0033 AC1): from the node holding the most
/// replicas to the node holding the fewest, of a range the latter does not hold yet. `None`
/// once replica counts differ by at most one, the best any placement can do.
pub fn next_move(
    ranges: &[(RangeId, BTreeSet<NodeId>)],
    nodes: &[NodeId],
) -> Option<(RangeId, NodeId, NodeId)> {
    let mut count: BTreeMap<NodeId, usize> = nodes.iter().map(|n| (*n, 0)).collect();
    for (_, replicas) in ranges {
        for n in replicas {
            *count.entry(*n).or_default() += 1;
        }
    }
    let (&to, &low) = count.iter().min_by_key(|(n, c)| (**c, **n))?;
    let (&from, &high) = count
        .iter()
        .max_by_key(|(n, c)| (**c, std::cmp::Reverse(**n)))?;
    if high <= low + 1 {
        return None;
    }
    ranges
        .iter()
        .find(|(_, r)| r.contains(&from) && !r.contains(&to))
        .map(|(id, _)| (*id, from, to))
}

#[cfg(test)]
mod balance_tests {
    use super::*;

    fn apply(ranges: &mut [(RangeId, BTreeSet<NodeId>)], (r, from, to): (RangeId, NodeId, NodeId)) {
        let set = &mut ranges.iter_mut().find(|(id, _)| *id == r).unwrap().1;
        assert!(set.remove(&from) && set.insert(to));
    }

    #[test]
    fn adding_a_node_converges_within_one_replica() {
        // 24 ranges on 3 nodes, then a 4th joins: 72 replicas settle at 18 each.
        let mut ranges: Vec<(RangeId, BTreeSet<NodeId>)> =
            (1..=24).map(|r| (r, BTreeSet::from([1, 2, 3]))).collect();
        let nodes = [1, 2, 3, 4];
        let mut moves = 0;
        while let Some(m) = next_move(&ranges, &nodes) {
            apply(&mut ranges, m);
            moves += 1;
            assert!(moves <= 24, "does not converge");
        }
        assert_eq!(moves, 18);
        for n in nodes {
            let c = ranges.iter().filter(|(_, r)| r.contains(&n)).count();
            assert_eq!(c, 18, "node {n}");
            // Every range keeps three distinct replicas.
        }
        assert!(ranges.iter().all(|(_, r)| r.len() == 3));
    }

    #[test]
    fn balanced_cluster_needs_no_move() {
        let ranges = vec![
            (1, BTreeSet::from([1, 2, 3])),
            (2, BTreeSet::from([2, 3, 4])),
        ];
        assert_eq!(next_move(&ranges, &[1, 2, 3, 4]), None);
    }
}
