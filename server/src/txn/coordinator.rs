//! Transaction coordinator (design/transactions.md §5, §7, §8).
//!
//! A transaction reads at `start_ts`, buffers its writes and commits:
//!   - read-only: nothing to do; a snapshot at `start_ts` is serializable (§6.2);
//!   - one range touched: prewrite (intents + record), take `commit_ts`, then a single
//!     `CommitLocal` entry validates, commits and resolves;
//!   - several ranges: Percolator-style 2PC. Prewrite the anchor range (creating the PENDING
//!     record), then the others; take `commit_ts`; validate every read span; flip the record
//!     to COMMITTED (the commit point); resolve intents. A heartbeat keeps the record alive.
//!
//! A reader that meets another transaction's intent consults that transaction's record and
//! resolves, waits, or pushes it to ABORTED once its heartbeat is stale (§7).
//!
//! Isolation is always serializable and cannot be lowered (REQ-0014 AC1). A failed commit is
//! never retried here (REQ-0015 AC1); retry policy lives with the caller (TASK-0011).

// reqforge: implements REQ-0014
// reqforge: implements REQ-0028
// reqforge: implements REQ-0015

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::cluster::Cluster;
use super::error::{DsError, ErrorCode};
use super::mvcc::{
    self, Conflict, Intent, Read, Span, Ts, TxnCommand, TxnId, TxnMeta, TxnRecord, TxnResponse,
    TxnStatus,
};
use super::tso::Tso;
use crate::raft::types::RangeId;

#[derive(Debug, Clone, Copy)]
pub struct TxnConfig {
    /// A PENDING record whose heartbeat is older than this may be pushed to ABORTED.
    pub liveness_ttl: Duration,
    /// How long to wait on another transaction's live intent before giving up with a conflict.
    pub lock_wait: Duration,
}

impl Default for TxnConfig {
    fn default() -> Self {
        Self {
            liveness_ttl: Duration::from_secs(5),
            lock_wait: Duration::from_secs(1),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Isolation {
    Serializable,
    SnapshotIsolation,
    RepeatableRead,
    ReadCommitted,
    ReadUncommitted,
}

/// Test hook: stop the coordinator at a commit phase without any cleanup, as a crash would.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashPoint {
    AfterAnchorPrewrite,
    AfterAllPrewrites,
    AfterValidation,
    AfterCommitRecord,
    AfterFirstResolve,
}

impl CrashPoint {
    pub const ALL: [CrashPoint; 5] = [
        CrashPoint::AfterAnchorPrewrite,
        CrashPoint::AfterAllPrewrites,
        CrashPoint::AfterValidation,
        CrashPoint::AfterCommitRecord,
        CrashPoint::AfterFirstResolve,
    ];
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

fn internal(e: impl std::fmt::Display) -> DsError {
    DsError::new(ErrorCode::Internal, e.to_string())
}

fn conflict_message(c: &Conflict) -> String {
    match c {
        Conflict::NewerVersion { key, commit_ts } => {
            format!(
                "key {:?} was changed by a transaction committed at {commit_ts}",
                String::from_utf8_lossy(key)
            )
        }
        Conflict::Intent { key, holder } => format!(
            "key {:?} is held by concurrent transaction {:x}",
            String::from_utf8_lossy(key),
            holder.id
        ),
        Conflict::Aborted => "transaction was aborted by a concurrent transaction".into(),
    }
}

#[derive(Clone)]
pub struct TxnClient {
    cluster: Arc<Cluster>,
    tso: Arc<Tso>,
    cfg: TxnConfig,
    seq: Arc<AtomicU64>,
}

impl TxnClient {
    pub fn new(cluster: Arc<Cluster>, cfg: TxnConfig) -> Self {
        Self {
            tso: Arc::new(Tso::new(cluster.clone())),
            cluster,
            cfg,
            seq: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn cluster(&self) -> &Arc<Cluster> {
        &self.cluster
    }

    /// Sessions and clients may only ask for serializable isolation (REQ-0014 AC1).
    pub fn set_isolation(&self, level: Isolation) -> Result<(), DsError> {
        match level {
            Isolation::Serializable => Ok(()),
            other => Err(DsError::new(
                ErrorCode::InvalidConfiguration,
                format!(
                    "isolation level {other:?} is not supported: every transaction runs at SERIALIZABLE"
                ),
            )),
        }
    }

    pub async fn begin(&self) -> Result<Txn, DsError> {
        let start_ts = self.tso.next().await?;
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let id = (nanos << 40)
            ^ ((std::process::id() as u128) << 24)
            ^ self.seq.fetch_add(1, Ordering::Relaxed) as u128;
        Ok(Txn {
            client: self.clone(),
            meta: TxnMeta {
                id,
                start_ts,
                anchor: Vec::new(),
            },
            reads: Vec::new(),
            writes: BTreeMap::new(),
        })
    }

    /// Resolve or wait on another transaction's intent on `key` (§7). `Ok(())` means: read
    /// again. Gives up with a conflict once `give_up` passes and the holder is still live.
    async fn handle_intent(
        &self,
        intent: &Intent,
        key: &[u8],
        give_up: tokio::time::Instant,
    ) -> Result<(), DsError> {
        let id = intent.txn.id;
        let anchor_range = self.cluster.range_for(&intent.txn.anchor).id;
        let key_range = self.cluster.range_for(key).id;
        let record = self
            .cluster
            .read(anchor_range, |db| mvcc::get_record(db, anchor_range, id))
            .await?
            .map_err(internal)?;
        let ttl = self.cfg.liveness_ttl.as_millis() as u64;
        let decided: Option<Option<Ts>> = match record.map(|r| r.status) {
            Some(TxnStatus::Committed { commit_ts }) => Some(Some(commit_ts)),
            Some(TxnStatus::Aborted) => Some(None),
            Some(TxnStatus::Pending { heartbeat_ms }) if heartbeat_ms + ttl >= now_ms() => None,
            _ => {
                let push = TxnCommand::Abort {
                    id,
                    stale_before_ms: now_ms().saturating_sub(ttl),
                };
                match self.cluster.propose(anchor_range, push).await? {
                    TxnResponse::Record(Some(TxnRecord {
                        status: TxnStatus::Aborted,
                        ..
                    })) => Some(None),
                    TxnResponse::Record(Some(TxnRecord {
                        status: TxnStatus::Committed { commit_ts },
                        ..
                    })) => Some(Some(commit_ts)),
                    _ => None,
                }
            }
        };
        match decided {
            Some(commit_ts) => {
                self.cluster
                    .propose(
                        key_range,
                        TxnCommand::Resolve {
                            id,
                            keys: vec![key.to_vec()],
                            commit_ts,
                        },
                    )
                    .await?;
                Ok(())
            }
            None if tokio::time::Instant::now() >= give_up => Err(DsError::conflict(format!(
                "lock wait timeout: key {:?} is held by live transaction {id:x}",
                String::from_utf8_lossy(key)
            ))),
            None => {
                tokio::time::sleep(Duration::from_millis(10)).await;
                Ok(())
            }
        }
    }
}

pub struct Txn {
    client: TxnClient,
    meta: TxnMeta,
    reads: Vec<Span>,
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl Txn {
    pub fn id(&self) -> TxnId {
        self.meta.id
    }

    pub fn start_ts(&self) -> Ts {
        self.meta.start_ts
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, DsError> {
        if let Some(v) = self.writes.get(key) {
            return Ok(v.clone());
        }
        self.reads.push(Span::point(key));
        let range = self.client.cluster.range_for(key).id;
        let give_up = tokio::time::Instant::now() + self.client.cfg.lock_wait;
        let (ts, me) = (self.meta.start_ts, self.meta.id);
        loop {
            let r = self
                .client
                .cluster
                .read(range, |db| mvcc::read(db, key, ts, Some(me)))
                .await?
                .map_err(internal)?;
            match r {
                Read::Value(v) => return Ok(v),
                Read::Blocked(intent) => self.client.handle_intent(&intent, key, give_up).await?,
            }
        }
    }

    /// Snapshot scan of `[start, end)`, including this transaction's own writes.
    pub async fn scan(
        &mut self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, DsError> {
        let span = Span {
            start: start.to_vec(),
            end: end.to_vec(),
        };
        self.reads.push(span.clone());
        let mut out = Vec::new();
        let ranges: Vec<_> = self.client.cluster.ranges.clone();
        for r in ranges
            .iter()
            .filter(|r| r.start.as_slice() < end && (r.end.is_empty() || r.end.as_slice() > start))
        {
            let part = Span {
                start: span.start.clone().max(r.start.clone()),
                end: if r.end.is_empty() {
                    span.end.clone()
                } else {
                    span.end.clone().min(r.end.clone())
                },
            };
            let give_up = tokio::time::Instant::now() + self.client.cfg.lock_wait;
            let (ts, me) = (self.meta.start_ts, self.meta.id);
            loop {
                let res = self
                    .client
                    .cluster
                    .read(r.id, |db| mvcc::scan(db, &part, ts, Some(me)))
                    .await?
                    .map_err(internal)?;
                match res {
                    Ok(pairs) => {
                        out.extend(pairs);
                        break;
                    }
                    Err((key, intent)) => self.client.handle_intent(&intent, &key, give_up).await?,
                }
            }
        }
        // Overlay buffered writes.
        for (k, v) in self.writes.range(span.start.clone()..span.end.clone()) {
            out.retain(|(kk, _)| kk != k);
            if let Some(v) = v {
                out.push((k.clone(), v.clone()));
            }
        }
        out.sort();
        Ok(out)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.writes.insert(key.to_vec(), Some(value.to_vec()));
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.writes.insert(key.to_vec(), None);
    }

    /// Insert `key` only if it does not exist; a constraint violation otherwise (e.g. a
    /// duplicate node identifier, REQ-0006 AC2). The existence check is a serializable read.
    pub async fn insert_new(&mut self, key: &[u8], value: &[u8]) -> Result<(), DsError> {
        if self.get(key).await?.is_some() {
            return Err(DsError::new(
                ErrorCode::ConstraintViolation,
                format!("key {:?} already exists", String::from_utf8_lossy(key)),
            ));
        }
        self.put(key, value);
        Ok(())
    }

    pub async fn commit(self) -> Result<Ts, DsError> {
        self.commit_with(None).await
    }

    /// Commit, optionally stopping dead at `crash` (test hook for REQ-0028).
    pub async fn commit_with(mut self, crash: Option<CrashPoint>) -> Result<Ts, DsError> {
        if self.writes.is_empty() {
            return Ok(self.meta.start_ts);
        }
        let cluster = self.client.cluster.clone();
        let mut writes_by_range: BTreeMap<RangeId, Vec<mvcc::Write>> = BTreeMap::new();
        for (k, v) in &self.writes {
            writes_by_range
                .entry(cluster.range_for(k).id)
                .or_default()
                .push((k.clone(), v.clone()));
        }
        let mut reads_by_range: BTreeMap<RangeId, Vec<Span>> = BTreeMap::new();
        for s in &self.reads {
            for r in cluster.ranges.iter().filter(|r| {
                r.start.as_slice() < s.end.as_slice()
                    && (r.end.is_empty() || r.end.as_slice() > s.start.as_slice())
            }) {
                reads_by_range.entry(r.id).or_default().push(s.clone());
            }
        }
        let touched: BTreeSet<RangeId> = writes_by_range
            .keys()
            .chain(reads_by_range.keys())
            .copied()
            .collect();

        // 2PC. The anchor is the first written key; its range holds the record.
        self.meta.anchor = self
            .writes
            .keys()
            .next()
            .expect("writes is non-empty")
            .clone();
        let anchor_range = cluster.range_for(&self.meta.anchor).id;
        let heartbeat_stop = Arc::new(AtomicBool::new(false));
        {
            let (cluster, stop, id) = (cluster.clone(), heartbeat_stop.clone(), self.meta.id);
            let every = self.client.cfg.liveness_ttl / 3;
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(every).await;
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    let _ = cluster
                        .propose(
                            anchor_range,
                            TxnCommand::Heartbeat {
                                id,
                                now_ms: now_ms(),
                            },
                        )
                        .await;
                }
            });
        }
        let crash_here = |at: CrashPoint| {
            if crash == Some(at) {
                heartbeat_stop.store(true, Ordering::Relaxed);
                Some(Err(DsError::new(
                    ErrorCode::Internal,
                    format!("coordinator crashed at {at:?} (test hook)"),
                )))
            } else {
                None
            }
        };

        let mut prewritten: Vec<RangeId> = Vec::new();
        let order: Vec<RangeId> = std::iter::once(anchor_range)
            .chain(
                writes_by_range
                    .keys()
                    .copied()
                    .filter(|r| *r != anchor_range),
            )
            .collect();
        for (i, range) in order.iter().enumerate() {
            let cmd = TxnCommand::Prewrite {
                txn: self.meta.clone(),
                writes: writes_by_range[range].clone(),
                record: *range == anchor_range,
                now_ms: now_ms(),
            };
            match cluster.propose(*range, cmd).await {
                Ok(TxnResponse::Ok) => prewritten.push(*range),
                Ok(TxnResponse::Conflict(c)) => {
                    self.rollback(&prewritten, &writes_by_range, anchor_range, &heartbeat_stop)
                        .await;
                    return Err(DsError::conflict(conflict_message(&c)));
                }
                Ok(other) => {
                    self.rollback(&prewritten, &writes_by_range, anchor_range, &heartbeat_stop)
                        .await;
                    return Err(internal(format!("prewrite: {other:?}")));
                }
                Err(e) => {
                    self.rollback(&prewritten, &writes_by_range, anchor_range, &heartbeat_stop)
                        .await;
                    return Err(e.into());
                }
            }
            if i == 0
                && let Some(r) = crash_here(CrashPoint::AfterAnchorPrewrite)
            {
                return r;
            }
        }
        if let Some(r) = crash_here(CrashPoint::AfterAllPrewrites) {
            return r;
        }

        // The commit timestamp is taken strictly after every intent is durable (§6.1).
        let commit_ts = self.client.tso.next().await?;

        // One range: validate, commit and resolve in a single entry.
        if touched.len() == 1 && crash.is_none() {
            let cmd = TxnCommand::CommitLocal {
                id: self.meta.id,
                start_ts: self.meta.start_ts,
                commit_ts,
                reads: reads_by_range.remove(&anchor_range).unwrap_or_default(),
                keys: writes_by_range[&anchor_range]
                    .iter()
                    .map(|(k, _)| k.clone())
                    .collect(),
            };
            let result = cluster.propose(anchor_range, cmd).await;
            heartbeat_stop.store(true, Ordering::Relaxed);
            return match result? {
                TxnResponse::Ok => Ok(commit_ts),
                TxnResponse::Conflict(c) => {
                    self.rollback(&prewritten, &writes_by_range, anchor_range, &heartbeat_stop)
                        .await;
                    Err(DsError::conflict(conflict_message(&c)))
                }
                other => Err(internal(format!("local commit: {other:?}"))),
            };
        }
        for (range, spans) in &reads_by_range {
            let cmd = TxnCommand::Validate {
                id: self.meta.id,
                start_ts: self.meta.start_ts,
                commit_ts,
                spans: spans.clone(),
            };
            match cluster.propose(*range, cmd).await? {
                TxnResponse::Ok => {}
                TxnResponse::Conflict(c) => {
                    self.rollback(&prewritten, &writes_by_range, anchor_range, &heartbeat_stop)
                        .await;
                    return Err(DsError::conflict(conflict_message(&c)));
                }
                other => {
                    self.rollback(&prewritten, &writes_by_range, anchor_range, &heartbeat_stop)
                        .await;
                    return Err(internal(format!("validate: {other:?}")));
                }
            }
        }
        if let Some(r) = crash_here(CrashPoint::AfterValidation) {
            return r;
        }

        // Commit point.
        match cluster
            .propose(
                anchor_range,
                TxnCommand::Commit {
                    id: self.meta.id,
                    commit_ts,
                },
            )
            .await?
        {
            TxnResponse::Ok => {}
            TxnResponse::Conflict(c) => {
                self.rollback(&prewritten, &writes_by_range, anchor_range, &heartbeat_stop)
                    .await;
                return Err(DsError::conflict(conflict_message(&c)));
            }
            other => return Err(internal(format!("commit record: {other:?}"))),
        }
        heartbeat_stop.store(true, Ordering::Relaxed);
        if let Some(r) = crash_here(CrashPoint::AfterCommitRecord) {
            return r;
        }

        for (i, range) in order.iter().enumerate() {
            let keys = writes_by_range[range]
                .iter()
                .map(|(k, _)| k.clone())
                .collect();
            let _ = cluster
                .propose(
                    *range,
                    TxnCommand::Resolve {
                        id: self.meta.id,
                        keys,
                        commit_ts: Some(commit_ts),
                    },
                )
                .await;
            if i == 0
                && let Some(r) = crash_here(CrashPoint::AfterFirstResolve)
            {
                return r;
            }
        }
        Ok(commit_ts)
    }

    /// Abort before the commit point: mark the record ABORTED, drop our intents.
    async fn rollback(
        &self,
        prewritten: &[RangeId],
        writes_by_range: &BTreeMap<RangeId, Vec<mvcc::Write>>,
        anchor_range: RangeId,
        heartbeat_stop: &AtomicBool,
    ) {
        heartbeat_stop.store(true, Ordering::Relaxed);
        let cluster = &self.client.cluster;
        let _ = cluster
            .propose(
                anchor_range,
                TxnCommand::Abort {
                    id: self.meta.id,
                    stale_before_ms: u64::MAX,
                },
            )
            .await;
        for range in prewritten {
            let keys = writes_by_range[range]
                .iter()
                .map(|(k, _)| k.clone())
                .collect();
            let _ = cluster
                .propose(
                    *range,
                    TxnCommand::Resolve {
                        id: self.meta.id,
                        keys,
                        commit_ts: None,
                    },
                )
                .await;
        }
    }
}
