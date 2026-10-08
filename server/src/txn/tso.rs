//! Timestamp oracle (DEC-0012, design/transactions.md §3).
//!
//! Timestamps are `physical_ms << 18 | logical`, strictly increasing. The oracle serves them
//! from memory below a high-water mark persisted through the meta range's Raft log, raising
//! the mark a window at a time. After a meta-range leadership change it resumes at the
//! persisted mark, which is above everything already served, so timestamps never repeat or
//! go backwards. No correctness argument depends on clock synchronisation: the clock only
//! keeps timestamps near wall time.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::Mutex;

use super::cluster::{Cluster, META_RANGE, Unavailable};
use super::mvcc::{Ts, TxnCommand, TxnResponse, tso_hwm};

pub const LOGICAL_BITS: u32 = 18;
/// Mark advance per Raft write: 3 s of timestamps.
pub const WINDOW: Ts = 3_000 << LOGICAL_BITS;

struct State {
    next: Ts,
    hwm: Ts,
    /// (leader node, term) of the meta range when `hwm` was last confirmed.
    reign: Option<(u64, u64)>,
}

pub struct Tso {
    cluster: Arc<Cluster>,
    state: Mutex<State>,
}

fn physical_now() -> Ts {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as Ts);
    ms << LOGICAL_BITS
}

impl Tso {
    pub fn new(cluster: Arc<Cluster>) -> Self {
        Self {
            cluster,
            state: Mutex::new(State {
                next: 0,
                hwm: 0,
                reign: None,
            }),
        }
    }

    pub async fn next(&self) -> Result<Ts, Unavailable> {
        let mut s = self.state.lock().await;
        let (raft, _) = self.cluster.leader(META_RANGE).await?;
        let m = raft.metrics().borrow().clone();
        let reign = (m.id, m.current_term);
        if s.reign != Some(reign) {
            // New meta leader (or first call): everything served so far is below the persisted
            // mark, so resume from it.
            let hwm = self
                .cluster
                .read(META_RANGE, |db| tso_hwm(db, META_RANGE))
                .await?
                .map_err(Unavailable)?;
            s.next = s.next.max(hwm);
            s.hwm = hwm;
            s.reign = Some(reign);
        }
        let ts = s.next.max(physical_now());
        if ts >= s.hwm {
            match self
                .cluster
                .propose(META_RANGE, TxnCommand::TsoAdvance { hwm: ts + WINDOW })
                .await?
            {
                TxnResponse::TsoHwm(h) => s.hwm = h,
                other => return Err(Unavailable(format!("tso advance: {other:?}"))),
            }
        }
        s.next = ts + 1;
        Ok(ts)
    }
}
