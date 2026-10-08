//! Internal retry of auto-commit statements (REQ-0015, design/transactions.md §8).
//!
//! An auto-commit statement may be retried after a serialization conflict because nothing it
//! read has reached the client: each attempt re-runs the whole statement in a fresh
//! transaction at a new snapshot, so no stale read survives into a retry and retrying cannot
//! lose an update. An interactive transaction is never retried: the client has seen its
//! reads, so the conflict goes back to the client (REQ-0015 AC1).

// reqforge: implements REQ-0015

use std::future::Future;
use std::time::Duration;

use super::coordinator::{Txn, TxnClient};
use super::error::{DsError, ErrorCode};
use super::mvcc::Ts;

#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_backoff: Duration::from_millis(2),
        }
    }
}

/// Outcome of an auto-commit statement: its result and commit timestamp, plus how many
/// attempts it took.
#[derive(Debug)]
pub struct Committed<T> {
    pub value: T,
    pub commit_ts: Ts,
    pub attempts: u32,
}

/// Run one auto-commit statement. `statement` stages reads and writes on a fresh transaction
/// and returns the statement's result; it is re-run from scratch on a serialization conflict,
/// up to `policy.max_attempts` times. Any other error is returned at once.
pub async fn autocommit<T, F, Fut>(
    client: &TxnClient,
    policy: RetryPolicy,
    mut statement: F,
) -> Result<Committed<T>, DsError>
where
    F: FnMut(Txn) -> Fut,
    Fut: Future<Output = Result<(T, Txn), DsError>>,
{
    let mut attempt = 0;
    loop {
        attempt += 1;
        let txn = client.begin().await?;
        let outcome = match statement(txn).await {
            Ok((value, txn)) => txn.commit().await.map(|ts| (value, ts)),
            Err(e) => Err(e),
        };
        match outcome {
            Ok((value, commit_ts)) => {
                return Ok(Committed {
                    value,
                    commit_ts,
                    attempts: attempt,
                });
            }
            Err(e)
                if e.code == ErrorCode::SerializationConflict && attempt < policy.max_attempts =>
            {
                tokio::time::sleep(policy.base_backoff * 2u32.pow(attempt - 1)).await;
            }
            Err(e) => return Err(e),
        }
    }
}
