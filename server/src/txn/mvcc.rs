//! Multi-version storage, intents and transaction records (design/transactions.md §4–§7).
//!
//! Everything here runs inside a range's Raft state machine, so each operation is applied in
//! the same order on every replica. Inputs that depend on time (heartbeats, staleness) arrive
//! in the command, never from the local clock, so replicas stay identical.
//!
//! Layout in the store's `kvdb` (user keys escaped so ordering is preserved):
//!   0x14 · esc(key) · !commit_ts   committed version (value or tombstone), newest first
//!   0x15 · key                      intent (at most one per key) -> Intent
//!   0x16 · range · txn_id           transaction record (anchor range) -> TxnRecord
//!   0x17 · range                    timestamp-oracle high-water mark (meta range)

// reqforge: implements REQ-0014
// reqforge: implements REQ-0028

use rocksdb::{DB, Direction, IteratorMode, WriteBatch};
use serde::{Deserialize, Serialize};

use crate::raft::types::RangeId;

pub type Ts = u64;
pub type TxnId = u128;

const VERSION: u8 = 0x14;
const INTENT: u8 = 0x15;
const RECORD: u8 = 0x16;
const TSO_HWM: u8 = 0x17;

/// A key range `[start, end)`; a point read of `k` is `[k, k\0)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Span {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
}

impl Span {
    pub fn point(key: &[u8]) -> Self {
        let mut end = key.to_vec();
        end.push(0);
        Self {
            start: key.to_vec(),
            end,
        }
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        key >= self.start.as_slice() && key < self.end.as_slice()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxnMeta {
    pub id: TxnId,
    pub start_ts: Ts,
    /// The key whose range holds the transaction record.
    pub anchor: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Intent {
    pub txn: TxnMeta,
    /// `None` is a delete.
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TxnStatus {
    Pending { heartbeat_ms: u64 },
    Committed { commit_ts: Ts },
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxnRecord {
    pub status: TxnStatus,
    pub start_ts: Ts,
    /// The anchor key; a range split moves the record to whichever range owns it.
    pub anchor: Vec<u8>,
}

pub type Write = (Vec<u8>, Option<Vec<u8>>);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TxnCommand {
    /// Lay down intents; with `record`, also create the PENDING record (anchor range only).
    Prewrite {
        txn: TxnMeta,
        writes: Vec<Write>,
        record: bool,
        now_ms: u64,
    },
    Heartbeat {
        id: TxnId,
        anchor: Vec<u8>,
        now_ms: u64,
    },
    /// PENDING -> COMMITTED. The single write that decides a 2PC transaction.
    Commit {
        id: TxnId,
        anchor: Vec<u8>,
        commit_ts: Ts,
    },
    /// Push: PENDING (heartbeat older than `stale_before_ms`) or absent -> ABORTED.
    Abort {
        id: TxnId,
        anchor: Vec<u8>,
        stale_before_ms: u64,
    },
    /// Turn this transaction's intents on `keys` into versions at `commit_ts`, or drop them.
    Resolve {
        id: TxnId,
        keys: Vec<Vec<u8>>,
        commit_ts: Option<Ts>,
    },
    /// Serializability check (§6): nothing in `spans` changed in `(start_ts, commit_ts]` and no
    /// other transaction holds an intent there.
    Validate {
        id: TxnId,
        start_ts: Ts,
        commit_ts: Ts,
        spans: Vec<Span>,
    },
    /// Single-range commit, after `Prewrite` (with record) on the same range: validate the
    /// reads, confirm the record is still PENDING, mark it COMMITTED and turn the intents on
    /// `keys` into versions, all in one entry. The intents were durable before `commit_ts` was
    /// taken, which is what makes a later reader see (and wait for) this transaction (§6.1).
    CommitLocal {
        id: TxnId,
        start_ts: Ts,
        commit_ts: Ts,
        reads: Vec<Span>,
        keys: Vec<Vec<u8>>,
    },
    /// Raise the timestamp oracle's persisted high-water mark (never lowers it).
    TsoAdvance { hwm: Ts },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Conflict {
    /// A version newer than the transaction's start (write-write) or inside the validation
    /// window (read-write).
    NewerVersion { key: Vec<u8>, commit_ts: Ts },
    /// Another transaction's intent.
    Intent { key: Vec<u8>, holder: TxnMeta },
    /// The transaction record is (or became) ABORTED, or is missing at commit.
    Aborted,
}

impl TxnCommand {
    /// Keys and spans this command reads or writes; the range applying it must own all of
    /// them, or it answers `RangeMismatch` (the sender routed with a stale descriptor).
    pub fn footprint(&self) -> (Vec<&[u8]>, Vec<&Span>) {
        match self {
            TxnCommand::Prewrite {
                txn,
                writes,
                record,
                ..
            } => {
                let mut keys: Vec<&[u8]> = writes.iter().map(|(k, _)| k.as_slice()).collect();
                if *record {
                    keys.push(&txn.anchor);
                }
                (keys, Vec::new())
            }
            TxnCommand::Heartbeat { anchor, .. }
            | TxnCommand::Commit { anchor, .. }
            | TxnCommand::Abort { anchor, .. } => (vec![anchor.as_slice()], Vec::new()),
            TxnCommand::Resolve { keys, .. } => {
                (keys.iter().map(Vec::as_slice).collect(), Vec::new())
            }
            TxnCommand::Validate { spans, .. } => (Vec::new(), spans.iter().collect()),
            TxnCommand::CommitLocal { keys, reads, .. } => (
                keys.iter().map(Vec::as_slice).collect(),
                reads.iter().collect(),
            ),
            TxnCommand::TsoAdvance { .. } => (Vec::new(), Vec::new()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TxnResponse {
    /// The range does not own every key the command touches: it was routed with a stale
    /// descriptor (e.g. across a split). Nothing was applied.
    RangeMismatch,
    Ok,
    Conflict(Conflict),
    Record(Option<TxnRecord>),
    TsoHwm(Ts),
}

// ---------------------------------------------------------------------------------- keys

fn escape(out: &mut Vec<u8>, key: &[u8]) {
    for &b in key {
        out.push(b);
        if b == 0 {
            out.push(0xFF);
        }
    }
    out.extend_from_slice(&[0, 1]);
}

fn version_prefix(key: &[u8]) -> Vec<u8> {
    let mut k = vec![VERSION];
    escape(&mut k, key);
    k
}

fn version_key(key: &[u8], ts: Ts) -> Vec<u8> {
    let mut k = version_prefix(key);
    k.extend_from_slice(&(!ts).to_be_bytes());
    k
}

/// Split an encoded version key into (user key, commit ts).
fn decode_version_key(k: &[u8]) -> Option<(Vec<u8>, Ts)> {
    let body = k.strip_prefix(&[VERSION])?;
    let (enc, ts) = body.split_at(body.len().checked_sub(8)?);
    let mut key = Vec::with_capacity(enc.len());
    let mut i = 0;
    while i < enc.len() {
        match (enc[i], enc.get(i + 1)) {
            (0, Some(0xFF)) => {
                key.push(0);
                i += 2;
            }
            (0, Some(1)) if i + 2 == enc.len() => {
                return Some((key, !Ts::from_be_bytes(ts.try_into().ok()?)));
            }
            (b, _) => {
                key.push(b);
                i += 1;
            }
        }
    }
    None
}

fn intent_key(key: &[u8]) -> Vec<u8> {
    let mut k = vec![INTENT];
    k.extend_from_slice(key);
    k
}

fn record_key(range: RangeId, id: TxnId) -> Vec<u8> {
    let mut k = vec![RECORD];
    k.extend_from_slice(&range.to_be_bytes());
    k.extend_from_slice(&id.to_be_bytes());
    k
}

fn tso_key(range: RangeId) -> Vec<u8> {
    let mut k = vec![TSO_HWM];
    k.extend_from_slice(&range.to_be_bytes());
    k
}

/// Every raw kvdb key interval owned by `range` (user keys `[start, end)`, `end` empty = +∞),
/// for snapshots: versions, intents, the range's transaction records and its TSO mark.
pub fn owned_spans(range: RangeId, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let bounded = |prefix: u8, key_fn: &dyn Fn(&[u8]) -> Vec<u8>| {
        let lo = key_fn(start);
        let hi = if end.is_empty() {
            vec![prefix + 1]
        } else {
            key_fn(end)
        };
        (lo, hi)
    };
    let mut next_range = vec![RECORD];
    next_range.extend_from_slice(&(range + 1).to_be_bytes());
    let mut next_tso = vec![TSO_HWM];
    next_tso.extend_from_slice(&(range + 1).to_be_bytes());
    let mut rec_lo = vec![RECORD];
    rec_lo.extend_from_slice(&range.to_be_bytes());
    vec![
        bounded(VERSION, &version_prefix),
        bounded(INTENT, &intent_key),
        (rec_lo, next_range),
        (tso_key(range), next_tso),
    ]
}

fn enc<T: Serialize>(v: &T) -> Vec<u8> {
    postcard::to_allocvec(v).expect("txn types always serialize")
}

fn dec<T: for<'de> Deserialize<'de>>(b: &[u8]) -> Result<T, String> {
    postcard::from_bytes(b).map_err(|e| e.to_string())
}

/// Committed versions are stored as `Option<Vec<u8>>` so a delete is a tombstone.
fn dec_version(b: &[u8]) -> Result<Option<Vec<u8>>, String> {
    dec(b)
}

// ---------------------------------------------------------------------------------- reads

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Read {
    Value(Option<Vec<u8>>),
    /// Another transaction's intent sits on the key; the reader must resolve or wait (§7).
    Blocked(Intent),
}

pub fn get_intent(db: &DB, key: &[u8]) -> Result<Option<Intent>, String> {
    match db.get(intent_key(key)).map_err(|e| e.to_string())? {
        Some(b) => Ok(Some(dec(&b)?)),
        None => Ok(None),
    }
}

pub fn get_record(db: &DB, range: RangeId, id: TxnId) -> Result<Option<TxnRecord>, String> {
    match db.get(record_key(range, id)).map_err(|e| e.to_string())? {
        Some(b) => Ok(Some(dec(&b)?)),
        None => Ok(None),
    }
}

/// Newest committed version of `key` with `commit_ts <= ts`, and the newest commit ts overall.
fn newest_version(db: &DB, key: &[u8], at_or_below: Ts) -> Result<VersionLookup, String> {
    let prefix = version_prefix(key);
    let mut newest = None;
    for item in db.iterator(IteratorMode::From(&prefix, Direction::Forward)) {
        let (k, v) = item.map_err(|e| e.to_string())?;
        if !k.starts_with(&prefix) {
            break;
        }
        let ts = !Ts::from_be_bytes(
            k[prefix.len()..]
                .try_into()
                .map_err(|_| "bad version key")?,
        );
        newest.get_or_insert(ts);
        if ts <= at_or_below {
            return Ok((Some((ts, dec_version(&v)?)), newest));
        }
    }
    Ok((None, newest))
}

/// Snapshot read at `ts`. `own` lets a transaction see its own intent.
pub fn read(db: &DB, key: &[u8], ts: Ts, own: Option<TxnId>) -> Result<Read, String> {
    if let Some(intent) = get_intent(db, key)? {
        if Some(intent.txn.id) == own {
            return Ok(Read::Value(intent.value));
        }
        return Ok(Read::Blocked(intent));
    }
    Ok(Read::Value(
        newest_version(db, key, ts)?.0.and_then(|(_, v)| v),
    ))
}

/// A key and its value.
pub type Pair = (Vec<u8>, Vec<u8>);

/// The pairs visible to a scan, or the first blocking intent and the key it sits on.
pub type ScanResult = Result<Vec<Pair>, (Vec<u8>, Intent)>;

/// (newest version at or below a timestamp as (commit ts, value), newest commit ts overall).
type VersionLookup = (Option<(Ts, Option<Vec<u8>>)>, Option<Ts>);

/// Snapshot scan of `span` at `ts`: committed pairs, or the first blocking intent and its key.
pub fn scan(db: &DB, span: &Span, ts: Ts, own: Option<TxnId>) -> Result<ScanResult, String> {
    let ik_start = intent_key(&span.start);
    let ik_end = intent_key(&span.end);
    for item in db.iterator(IteratorMode::From(&ik_start, Direction::Forward)) {
        let (k, v) = item.map_err(|e| e.to_string())?;
        if k.as_ref() >= ik_end.as_slice() {
            break;
        }
        let intent: Intent = dec(&v)?;
        if Some(intent.txn.id) != own {
            return Ok(Err((k[1..].to_vec(), intent)));
        }
    }
    let mut out: Vec<Pair> = Vec::new();
    let from = version_prefix(&span.start);
    let to = version_prefix(&span.end);
    let mut current: Option<Vec<u8>> = None;
    let mut done_with_current = false;
    for item in db.iterator(IteratorMode::From(&from, Direction::Forward)) {
        let (k, v) = item.map_err(|e| e.to_string())?;
        if k.as_ref() >= to.as_slice() {
            break;
        }
        let Some((key, cts)) = decode_version_key(&k) else {
            continue;
        };
        if current.as_deref() != Some(key.as_slice()) {
            current = Some(key.clone());
            done_with_current = false;
        }
        if done_with_current || cts > ts {
            continue;
        }
        done_with_current = true;
        if let Some(val) = dec_version(&v)? {
            out.push((key, val));
        }
    }
    // Overlay this transaction's own intents.
    if let Some(me) = own {
        for item in db.iterator(IteratorMode::From(&ik_start, Direction::Forward)) {
            let (k, v) = item.map_err(|e| e.to_string())?;
            if k.as_ref() >= ik_end.as_slice() {
                break;
            }
            let intent: Intent = dec(&v)?;
            if intent.txn.id == me {
                let key = k[1..].to_vec();
                out.retain(|(kk, _)| kk != &key);
                if let Some(val) = intent.value {
                    out.push((key, val));
                }
            }
        }
        out.sort();
    }
    Ok(Ok(out))
}

// ---------------------------------------------------------------------------------- apply

fn validate_spans(
    db: &DB,
    id: Option<TxnId>,
    start_ts: Ts,
    commit_ts: Ts,
    spans: &[Span],
) -> Result<Option<Conflict>, String> {
    for span in spans {
        // Another transaction's intent anywhere in the span.
        let ik_start = intent_key(&span.start);
        let ik_end = intent_key(&span.end);
        for item in db.iterator(IteratorMode::From(&ik_start, Direction::Forward)) {
            let (k, v) = item.map_err(|e| e.to_string())?;
            if k.as_ref() >= ik_end.as_slice() {
                break;
            }
            let intent: Intent = dec(&v)?;
            if Some(intent.txn.id) != id {
                return Ok(Some(Conflict::Intent {
                    key: k[1..].to_vec(),
                    holder: intent.txn,
                }));
            }
        }
        // A version committed inside (start_ts, commit_ts], including inserts (phantoms).
        // Versions sort by user key, newest first, so per key one seek to commit_ts finds the
        // newest version at or below it, and a second skips the key's older versions: the
        // cost is per distinct key, not per version ever written.
        let to = version_prefix(&span.end);
        let mut it = db.raw_iterator();
        it.seek(version_prefix(&span.start));
        while let Some(k) = it.key() {
            if k >= to.as_slice() {
                break;
            }
            let (key, _) = decode_version_key(k).ok_or("bad version key")?;
            let prefix = version_prefix(&key);
            it.seek(version_key(&key, commit_ts));
            if let Some(k) = it.key()
                && k.starts_with(&prefix)
                && let Some((_, cts)) = decode_version_key(k)
                && cts > start_ts
            {
                return Ok(Some(Conflict::NewerVersion {
                    key,
                    commit_ts: cts,
                }));
            }
            // The last possible version key of `key` is version_key(key, 0); seek just past it.
            let mut next = version_key(&key, 0);
            next.push(0);
            it.seek(next);
        }
        it.status().map_err(|e| e.to_string())?;
    }
    Ok(None)
}

/// Live data in `[start, end)`: the number of keys whose newest version is not a delete, their
/// bytes (key plus newest value), and the median such key. One seek per key, so the cost
/// does not grow with old versions. Drives size-based splits (REQ-0033).
pub fn live_stats(db: &DB, start: &[u8], end: &[u8]) -> Result<RangeStats, String> {
    let to = (!end.is_empty()).then(|| version_prefix(end));
    let mut keys = Vec::new();
    let mut bytes = 0u64;
    let mut it = db.raw_iterator();
    it.seek(version_prefix(start));
    while let Some(k) = it.key() {
        if !k.starts_with(&[VERSION]) || to.as_ref().is_some_and(|to| k >= to.as_slice()) {
            break;
        }
        let (key, _) = decode_version_key(k).ok_or("bad version key")?;
        let value = it.value().map(dec_version).transpose()?.flatten();
        if let Some(v) = value {
            bytes += (key.len() + v.len()) as u64;
            keys.push(key.clone());
        }
        let mut next = version_key(&key, 0);
        next.push(0);
        it.seek(next);
    }
    it.status().map_err(|e| e.to_string())?;
    Ok(RangeStats {
        keys: keys.len() as u64,
        bytes,
        median: keys.get(keys.len() / 2).cloned(),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeStats {
    pub keys: u64,
    pub bytes: u64,
    pub median: Option<Vec<u8>>,
}

/// Whether user key `k` falls in `[start, end)` (`end` empty = +∞).
pub fn owns(start: &[u8], end: &[u8], k: &[u8]) -> bool {
    k >= start && (end.is_empty() || k < end)
}

/// Whether span `s` lies wholly inside `[start, end)`.
pub fn owns_span(start: &[u8], end: &[u8], s: &Span) -> bool {
    s.start.as_slice() >= start && (end.is_empty() || s.end.as_slice() <= end)
}

/// Apply one transaction command for the range owning user keys `[start, end)`: read `db`,
/// stage writes in `batch`. A command touching keys outside the interval changes nothing and
/// answers `RangeMismatch`.
pub fn apply(
    db: &DB,
    batch: &mut WriteBatch,
    range: RangeId,
    interval: (&[u8], &[u8]),
    cmd: &TxnCommand,
) -> Result<TxnResponse, String> {
    let (start, end) = interval;
    let (keys, spans) = cmd.footprint();
    if !keys.iter().all(|k| owns(start, end, k)) || !spans.iter().all(|s| owns_span(start, end, s))
    {
        return Ok(TxnResponse::RangeMismatch);
    }
    Ok(match cmd {
        TxnCommand::Prewrite {
            txn,
            writes,
            record,
            now_ms,
        } => {
            if *record {
                match get_record(db, range, txn.id)? {
                    Some(TxnRecord {
                        status: TxnStatus::Aborted,
                        ..
                    }) => return Ok(TxnResponse::Conflict(Conflict::Aborted)),
                    Some(_) => {}
                    None => batch.put(
                        record_key(range, txn.id),
                        enc(&TxnRecord {
                            status: TxnStatus::Pending {
                                heartbeat_ms: *now_ms,
                            },
                            start_ts: txn.start_ts,
                            anchor: txn.anchor.clone(),
                        }),
                    ),
                }
            }
            for (key, _) in writes {
                if let Some(other) = get_intent(db, key)?
                    && other.txn.id != txn.id
                {
                    return Ok(TxnResponse::Conflict(Conflict::Intent {
                        key: key.clone(),
                        holder: other.txn,
                    }));
                }
                if let (_, Some(newest)) = newest_version(db, key, Ts::MAX)?
                    && newest > txn.start_ts
                {
                    return Ok(TxnResponse::Conflict(Conflict::NewerVersion {
                        key: key.clone(),
                        commit_ts: newest,
                    }));
                }
            }
            for (key, value) in writes {
                batch.put(
                    intent_key(key),
                    enc(&Intent {
                        txn: txn.clone(),
                        value: value.clone(),
                    }),
                );
            }
            TxnResponse::Ok
        }
        TxnCommand::Heartbeat { id, now_ms, .. } => match get_record(db, range, *id)? {
            Some(
                mut r @ TxnRecord {
                    status: TxnStatus::Pending { .. },
                    ..
                },
            ) => {
                r.status = TxnStatus::Pending {
                    heartbeat_ms: *now_ms,
                };
                batch.put(record_key(range, *id), enc(&r));
                TxnResponse::Ok
            }
            other => TxnResponse::Record(other),
        },
        TxnCommand::Commit { id, commit_ts, .. } => match get_record(db, range, *id)? {
            Some(
                mut r @ TxnRecord {
                    status: TxnStatus::Pending { .. },
                    ..
                },
            ) => {
                r.status = TxnStatus::Committed {
                    commit_ts: *commit_ts,
                };
                batch.put(record_key(range, *id), enc(&r));
                TxnResponse::Ok
            }
            Some(TxnRecord {
                status: TxnStatus::Committed { commit_ts: c },
                ..
            }) if c == *commit_ts => TxnResponse::Ok,
            _ => TxnResponse::Conflict(Conflict::Aborted),
        },
        TxnCommand::Abort {
            id,
            anchor,
            stale_before_ms,
        } => match get_record(db, range, *id)? {
            Some(TxnRecord {
                status: TxnStatus::Pending { heartbeat_ms },
                start_ts,
                anchor,
            }) if heartbeat_ms < *stale_before_ms => {
                let r = TxnRecord {
                    status: TxnStatus::Aborted,
                    start_ts,
                    anchor,
                };
                batch.put(record_key(range, *id), enc(&r));
                TxnResponse::Record(Some(r))
            }
            None => {
                // No record yet: block it from ever being created PENDING.
                let r = TxnRecord {
                    status: TxnStatus::Aborted,
                    start_ts: 0,
                    anchor: anchor.clone(),
                };
                batch.put(record_key(range, *id), enc(&r));
                TxnResponse::Record(Some(r))
            }
            other => TxnResponse::Record(other),
        },
        TxnCommand::Resolve {
            id,
            keys,
            commit_ts,
        } => {
            for key in keys {
                if let Some(intent) = get_intent(db, key)?
                    && intent.txn.id == *id
                {
                    batch.delete(intent_key(key));
                    if let Some(ts) = commit_ts {
                        batch.put(version_key(key, *ts), enc(&intent.value));
                    }
                }
            }
            TxnResponse::Ok
        }
        TxnCommand::Validate {
            id,
            start_ts,
            commit_ts,
            spans,
        } => match validate_spans(db, Some(*id), *start_ts, *commit_ts, spans)? {
            Some(c) => TxnResponse::Conflict(c),
            None => TxnResponse::Ok,
        },
        TxnCommand::CommitLocal {
            id,
            start_ts,
            commit_ts,
            reads,
            keys,
        } => {
            let mut record = match get_record(db, range, *id)? {
                Some(
                    r @ TxnRecord {
                        status: TxnStatus::Pending { .. },
                        ..
                    },
                ) => r,
                // A retried proposal of a commit that already applied (idempotent).
                Some(TxnRecord {
                    status: TxnStatus::Committed { commit_ts: c },
                    ..
                }) if c == *commit_ts => return Ok(TxnResponse::Ok),
                _ => return Ok(TxnResponse::Conflict(Conflict::Aborted)),
            };
            if let Some(c) = validate_spans(db, Some(*id), *start_ts, *commit_ts, reads)? {
                return Ok(TxnResponse::Conflict(c));
            }
            record.status = TxnStatus::Committed {
                commit_ts: *commit_ts,
            };
            batch.put(record_key(range, *id), enc(&record));
            for key in keys {
                if let Some(intent) = get_intent(db, key)?
                    && intent.txn.id == *id
                {
                    batch.delete(intent_key(key));
                    batch.put(version_key(key, *commit_ts), enc(&intent.value));
                }
            }
            TxnResponse::Ok
        }
        TxnCommand::TsoAdvance { hwm } => {
            let cur = tso_hwm(db, range)?;
            let next = cur.max(*hwm);
            batch.put(tso_key(range), next.to_be_bytes());
            TxnResponse::TsoHwm(next)
        }
    })
}

/// A transaction record moving between ranges: (old key, new key, encoded record).
pub type RecordMove = (Vec<u8>, Vec<u8>, Vec<u8>);

/// Records of `range` whose anchor is at or after `at`, re-keyed for `new_range`. Used by a split.
pub fn records_to_move(
    db: &DB,
    range: RangeId,
    new_range: RangeId,
    at: &[u8],
) -> Result<Vec<RecordMove>, openraft::AnyError> {
    let mut lo = vec![RECORD];
    lo.extend_from_slice(&range.to_be_bytes());
    let mut hi = vec![RECORD];
    hi.extend_from_slice(&(range + 1).to_be_bytes());
    let mut out = Vec::new();
    for item in db.iterator(IteratorMode::From(&lo, Direction::Forward)) {
        let (k, v) = item.map_err(|e| openraft::AnyError::new(&e))?;
        if k.as_ref() >= hi.as_slice() {
            break;
        }
        let rec: TxnRecord = dec(&v).map_err(openraft::AnyError::error)?;
        if rec.anchor.as_slice() >= at {
            let id = TxnId::from_be_bytes(
                k[1 + 8..]
                    .try_into()
                    .map_err(|_| openraft::AnyError::error("record key"))?,
            );
            out.push((k.to_vec(), record_key(new_range, id), v.to_vec()));
        }
    }
    Ok(out)
}

pub fn tso_hwm(db: &DB, range: RangeId) -> Result<Ts, String> {
    Ok(match db.get(tso_key(range)).map_err(|e| e.to_string())? {
        Some(b) => Ts::from_be_bytes(b.as_slice().try_into().map_err(|_| "bad tso hwm")?),
        None => 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_keys_order_by_user_key_then_newest_first() {
        assert!(version_key(b"a", 5) < version_key(b"a", 3), "newer first");
        assert!(version_key(b"a", 1) < version_key(b"a\0", 9));
        assert!(version_key(b"a\0", 9) < version_key(b"b", 9));
        assert!(version_prefix(b"a") < version_prefix(b"aa"));
        for key in [&b"a"[..], b"a\0b", b"\0", b"", b"\xff\x00"] {
            assert_eq!(
                decode_version_key(&version_key(key, 42)),
                Some((key.to_vec(), 42))
            );
        }
    }

    #[test]
    fn span_point_contains_only_the_key() {
        let s = Span::point(b"k");
        assert!(s.contains(b"k"));
        assert!(!s.contains(b"k\0"));
        assert!(!s.contains(b"j"));
    }
}
