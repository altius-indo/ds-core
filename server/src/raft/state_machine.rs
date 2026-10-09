//! A range's replicated state machine on the store's `kvdb` RocksDB instance.
//!
//! User keys are stored as-is under a data prefix, so a range is a key interval rather than a
//! separate keyspace and a split moves no data (design/raft-ranges.md §3). Each apply writes
//! the data, the last applied log id and the membership in one batch, without sync: after a
//! crash the Raft log, which is synced, replays whatever the batch lost.

use std::io::Cursor;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot};
use openraft::{AnyError, EntryPayload, OptionalSend};
use rocksdb::{DB, Direction, IteratorMode, Options, WriteBatch};
use serde::{Deserialize, Serialize};

use crate::txn::mvcc;

use super::types::{
    Command, CommandResult, Entry, LogId, RangeId, SnapshotMeta, StorageError, StorageIOError,
    StoredMembership, TypeConfig,
};

const DATA: u8 = 0x10;
const META: u8 = 0x11;
const APPLIED: u8 = b'a';
const MEMBERSHIP: u8 = b'm';
const INTERVAL: u8 = b'i';

/// A range's user-key interval `[start, end)` (`end` empty = +∞).
pub type Interval = (Vec<u8>, Vec<u8>);

fn data_key(user_key: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(1 + user_key.len());
    k.push(DATA);
    k.extend_from_slice(user_key);
    k
}

fn meta_key(range: RangeId, kind: u8) -> Vec<u8> {
    let mut k = vec![META];
    k.extend_from_slice(&range.to_be_bytes());
    k.push(kind);
    k
}

/// The store-wide key-value database shared by every range's state machine.
pub struct KvEngine {
    db: Arc<DB>,
}

impl KvEngine {
    pub fn open(path: &Path) -> Result<Arc<Self>, rocksdb::Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        Ok(Arc::new(Self {
            db: Arc::new(DB::open(&opts, path)?),
        }))
    }

    /// Read a user key as applied on this replica.
    pub fn get(&self, user_key: &[u8]) -> Result<Option<Vec<u8>>, rocksdb::Error> {
        self.db.get(data_key(user_key))
    }

    /// The underlying database, for MVCC reads (txn::mvcc) on this replica.
    pub fn db(&self) -> &DB {
        &self.db
    }

    /// The interval `range` owns on this store, as last applied (splits included).
    pub fn range_interval(&self, range: RangeId) -> Result<Option<Interval>, rocksdb::Error> {
        Ok(self
            .db
            .get(meta_key(range, INTERVAL))?
            .and_then(|b| postcard::from_bytes(&b).ok()))
    }

    /// Every range this store holds, with its interval (for restarting after splits).
    pub fn ranges(&self) -> Result<Vec<(RangeId, Interval)>, rocksdb::Error> {
        let mut out = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(&[META], Direction::Forward))
        {
            let (k, v) = item?;
            if k.first() != Some(&META) {
                break;
            }
            if k.len() == 10 && k[9] == INTERVAL {
                let range = RangeId::from_be_bytes(k[1..9].try_into().expect("8-byte range id"));
                if let Ok(i) = postcard::from_bytes(&v) {
                    out.push((range, i));
                }
            }
        }
        Ok(out)
    }

    /// Whether any of `others` has an applied interval on this store overlapping `[start, end)`.
    /// A replica may only start on a store when no other local replica still owns part of its
    /// interval: a lagging parent that has not applied the split would otherwise write into the
    /// child's keys underneath it.
    pub fn overlapped(
        &self,
        others: impl IntoIterator<Item = RangeId>,
        start: &[u8],
        end: &[u8],
    ) -> Result<bool, rocksdb::Error> {
        for r in others {
            if let Some((s, e)) = self.range_interval(r)?
                && (e.is_empty() || s < e)
                && (end.is_empty() || s.as_slice() < end)
                && (e.is_empty() || start < e.as_slice())
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Delete `range`'s data in `[start, end)` and its applied state, leaving an empty replica
    /// that can only be brought up to date by a snapshot.
    pub fn wipe_range(
        &self,
        range: RangeId,
        start: &[u8],
        end: &[u8],
    ) -> Result<(), rocksdb::Error> {
        let mut batch = WriteBatch::default();
        let data_hi = if end.is_empty() {
            vec![DATA + 1]
        } else {
            data_key(end)
        };
        batch.delete_range(data_key(start), data_hi);
        for (lo, hi) in mvcc::owned_spans(range, start, end) {
            batch.delete_range(lo, hi);
        }
        for kind in [APPLIED, MEMBERSHIP, INTERVAL] {
            batch.delete(meta_key(range, kind));
        }
        self.db.write(batch)
    }

    /// The state machine for `range`. A range created by a split, or restarted, uses its
    /// persisted interval; otherwise `[start, end)` is recorded as its initial interval.
    pub fn range(
        self: &Arc<Self>,
        range: RangeId,
        start: Vec<u8>,
        end: Vec<u8>,
    ) -> RangeStateMachine {
        let interval = match self.range_interval(range) {
            Ok(Some(i)) => i,
            _ => {
                let i = (start, end);
                let _ = self.db.put(
                    meta_key(range, INTERVAL),
                    postcard::to_allocvec(&i).expect("interval encodes"),
                );
                i
            }
        };
        RangeStateMachine {
            engine: self.clone(),
            range,
            interval: Arc::new(RwLock::new(interval)),
            snapshot: Arc::new(Mutex::new(None)),
        }
    }
}

/// The kvdb key spans `range` owns given its interval: its user data, its MVCC keys, and its
/// interval record (which travels with snapshots, so a replica rebuilt after a split learns
/// its bounds).
fn owned_spans(range: RangeId, (start, end): &Interval) -> Vec<(Vec<u8>, Vec<u8>)> {
    let data_lo = data_key(start);
    let data_hi = if end.is_empty() {
        vec![DATA + 1]
    } else {
        data_key(end)
    };
    let mut spans = vec![(data_lo, data_hi)];
    spans.extend(mvcc::owned_spans(range, start, end));
    let mut hi = meta_key(range, INTERVAL);
    hi.push(0);
    spans.push((meta_key(range, INTERVAL), hi));
    spans
}

/// A raw kvdb key and its value.
type KvPair = (Vec<u8>, Vec<u8>);

#[derive(Serialize, Deserialize)]
struct SnapshotData {
    pairs: Vec<KvPair>,
}

#[derive(Clone)]
struct StoredSnapshot {
    meta: SnapshotMeta,
    data: Vec<u8>,
}

#[derive(Clone)]
pub struct RangeStateMachine {
    engine: Arc<KvEngine>,
    range: RangeId,
    interval: Arc<RwLock<Interval>>,
    snapshot: Arc<Mutex<Option<StoredSnapshot>>>,
}

fn encode<T: Serialize>(v: &T) -> Result<Vec<u8>, AnyError> {
    postcard::to_allocvec(v).map_err(|e| AnyError::error(e.to_string()))
}

fn decode<T: for<'de> Deserialize<'de>>(b: &[u8]) -> Result<T, AnyError> {
    postcard::from_bytes(b).map_err(|e| AnyError::error(e.to_string()))
}

impl RangeStateMachine {
    fn get_meta<T: for<'de> Deserialize<'de>>(&self, kind: u8) -> Result<Option<T>, AnyError> {
        match self
            .engine
            .db
            .get(meta_key(self.range, kind))
            .map_err(|e| AnyError::new(&e))?
        {
            Some(b) => Ok(Some(decode(&b)?)),
            None => Ok(None),
        }
    }

    fn apply_command(batch: &mut WriteBatch, cmd: &Command) {
        match cmd {
            Command::Put { key, value } => batch.put(data_key(key), value),
            Command::Delete { key } => batch.delete(data_key(key)),
            Command::Batch(cmds) => cmds.iter().for_each(|c| Self::apply_command(batch, c)),
            // Transaction commands and splits are applied at top level (see `apply`).
            Command::Txn(_) | Command::Split { .. } => {}
        }
    }

    /// Raw kvdb intervals this range owns: plain data plus MVCC versions, intents, records and
    /// the TSO mark. Snapshots carry exactly these.
    fn owned(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        owned_spans(self.range, &self.interval.read().expect("interval lock"))
    }

    /// Split at `at` (see Command::Split); false if `at` is not strictly inside the interval.
    fn split(
        &self,
        batch: &mut WriteBatch,
        at: &[u8],
        new_range: RangeId,
    ) -> Result<bool, AnyError> {
        let (start, end) = self.interval.read().expect("interval lock").clone();
        // A retried split (its first proposal timed out but committed) is a no-op success.
        if end.as_slice() == at
            && let Ok(Some((child_start, _))) = self.engine.range_interval(new_range)
            && child_start.as_slice() == at
        {
            return Ok(true);
        }
        if at <= start.as_slice() || (!end.is_empty() && at >= end.as_slice()) {
            return Ok(false);
        }
        batch.put(
            meta_key(self.range, INTERVAL),
            encode(&(start, at.to_vec()))?,
        );
        batch.put(meta_key(new_range, INTERVAL), encode(&(at.to_vec(), end))?);
        for (old_key, new_key, record) in
            mvcc::records_to_move(&self.engine.db, self.range, new_range, at)?
        {
            batch.delete(old_key);
            batch.put(new_key, record);
        }
        Ok(true)
    }

    fn reload_interval(&self) {
        if let Ok(Some(i)) = self.engine.range_interval(self.range) {
            *self.interval.write().expect("interval lock") = i;
        }
    }

    /// The applied log id, membership and owned pairs, all read at one point in time. Applies
    /// run while a snapshot is built, and a snapshot whose data is newer than its log id would
    /// have the entries in between applied a second time by whoever installs it.
    fn consistent_state(&self) -> Result<(Option<LogId>, StoredMembership, Vec<KvPair>), AnyError> {
        let snap = self.engine.db.snapshot();
        let meta = |kind| {
            snap.get(meta_key(self.range, kind))
                .map_err(|e| AnyError::new(&e))
        };
        let applied: Option<LogId> = match meta(APPLIED)? {
            Some(b) => decode::<Option<LogId>>(&b)?,
            None => None,
        };
        let membership: StoredMembership = match meta(MEMBERSHIP)? {
            Some(b) => decode(&b)?,
            None => StoredMembership::default(),
        };
        let interval: Interval = match meta(INTERVAL)? {
            Some(b) => decode(&b)?,
            None => self.interval.read().expect("interval lock").clone(),
        };
        let mut pairs = Vec::new();
        for (lo, hi) in owned_spans(self.range, &interval) {
            for item in snap.iterator(IteratorMode::From(&lo, Direction::Forward)) {
                let (k, v) = item.map_err(|e| AnyError::new(&e))?;
                if k.as_ref() >= hi.as_slice() {
                    break;
                }
                pairs.push((k.to_vec(), v.to_vec()));
            }
        }
        Ok((applied, membership, pairs))
    }
}

impl RaftSnapshotBuilder<TypeConfig> for RangeStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError> {
        let (applied, membership, pairs) = self
            .consistent_state()
            .map_err(StorageIOError::read_state_machine)?;
        let data = encode(&SnapshotData { pairs }).map_err(StorageIOError::read_state_machine)?;
        let snapshot_id = match applied {
            Some(l) => format!("{}-{}-{}", self.range, l.leader_id, l.index),
            None => format!("{}-empty", self.range),
        };
        let meta = SnapshotMeta {
            last_log_id: applied,
            last_membership: membership,
            snapshot_id,
        };
        *self.snapshot.lock().expect("snapshot lock") = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for RangeStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), StorageError> {
        let applied = self
            .get_meta::<Option<LogId>>(APPLIED)
            .map_err(StorageIOError::read_state_machine)?
            .flatten();
        let membership = self
            .get_meta(MEMBERSHIP)
            .map_err(StorageIOError::read_state_machine)?
            .unwrap_or_default();
        Ok((applied, membership))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<CommandResult>, StorageError>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        // One batch per entry, applied index included: transaction commands read the state
        // the previous entry left, so their conflict checks must see its writes.
        let mut results = Vec::new();
        for e in entries {
            let mut batch = WriteBatch::default();
            let result = match &e.payload {
                EntryPayload::Blank => CommandResult::Ok,
                EntryPayload::Normal(Command::Txn(cmd)) => {
                    let (start, end) = self.interval.read().expect("interval lock").clone();
                    CommandResult::Txn(
                        mvcc::apply(&self.engine.db, &mut batch, self.range, (&start, &end), cmd)
                            .map_err(|err| StorageIOError::apply(e.log_id, AnyError::error(err)))?,
                    )
                }
                EntryPayload::Normal(Command::Split { at, new_range }) => {
                    if self
                        .split(&mut batch, at, *new_range)
                        .map_err(|err| StorageIOError::apply(e.log_id, err))?
                    {
                        CommandResult::Ok
                    } else {
                        CommandResult::Txn(crate::txn::mvcc::TxnResponse::RangeMismatch)
                    }
                }
                EntryPayload::Normal(cmd) => {
                    Self::apply_command(&mut batch, cmd);
                    CommandResult::Ok
                }
                EntryPayload::Membership(m) => {
                    batch.put(
                        meta_key(self.range, MEMBERSHIP),
                        encode(&StoredMembership::new(Some(e.log_id), m.clone()))
                            .map_err(StorageIOError::write_state_machine)?,
                    );
                    CommandResult::Ok
                }
            };
            batch.put(
                meta_key(self.range, APPLIED),
                encode(&Some(e.log_id)).map_err(StorageIOError::write_state_machine)?,
            );
            self.engine
                .db
                .write(batch)
                .map_err(|err| StorageIOError::write_state_machine(&err))?;
            if matches!(&e.payload, EntryPayload::Normal(Command::Split { .. })) {
                self.reload_interval();
            }
            results.push(result);
        }
        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError> {
        let data = snapshot.into_inner();
        let parsed: SnapshotData =
            decode(&data).map_err(|e| StorageIOError::read_snapshot(None, e))?;
        let mut batch = WriteBatch::default();
        for (lo, hi) in self.owned() {
            batch.delete_range(lo, hi);
        }
        for (k, v) in &parsed.pairs {
            batch.put(k, v);
        }
        batch.put(
            meta_key(self.range, APPLIED),
            encode(&meta.last_log_id).map_err(StorageIOError::write_state_machine)?,
        );
        batch.put(
            meta_key(self.range, MEMBERSHIP),
            encode(&meta.last_membership).map_err(StorageIOError::write_state_machine)?,
        );
        self.engine
            .db
            .write(batch)
            .map_err(|e| StorageIOError::write_state_machine(&e))?;
        self.reload_interval();
        *self.snapshot.lock().expect("snapshot lock") = Some(StoredSnapshot {
            meta: meta.clone(),
            data,
        });
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, StorageError> {
        Ok(self
            .snapshot
            .lock()
            .expect("snapshot lock")
            .clone()
            .map(|s| Snapshot {
                meta: s.meta,
                snapshot: Box::new(Cursor::new(s.data)),
            }))
    }
}

#[cfg(test)]
mod tests {
    use openraft::CommittedLeaderId;

    use super::*;

    fn entry(index: u64, cmd: Command) -> Entry {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(cmd),
        }
    }

    #[tokio::test]
    async fn repeated_split_is_idempotent() {
        let dir = tempfile::TempDir::new().unwrap();
        let kv = KvEngine::open(dir.path()).unwrap();
        let mut sm = kv.range(1, b"a".to_vec(), b"z".to_vec());
        let split = || Command::Split {
            at: b"m".to_vec(),
            new_range: 7,
        };
        // The first proposal and its retry both commit: the retry reports success and changes
        // nothing, so the caller goes on to start the child group.
        let r = sm
            .apply([entry(1, split()), entry(2, split())])
            .await
            .unwrap();
        assert!(
            matches!(r[..], [CommandResult::Ok, CommandResult::Ok]),
            "{r:?}"
        );
        // The applied log id reads back exactly (it is stored as an Option<LogId>).
        let (applied, _) = sm.applied_state().await.unwrap();
        assert_eq!(applied, Some(entry(2, split()).log_id));
        assert_eq!(
            kv.range_interval(1).unwrap(),
            Some((b"a".to_vec(), b"m".to_vec()))
        );
        assert_eq!(
            kv.range_interval(7).unwrap(),
            Some((b"m".to_vec(), b"z".to_vec()))
        );
        // A different split at the parent's new boundary is still refused.
        let other = Command::Split {
            at: b"m".to_vec(),
            new_range: 8,
        };
        let r = sm.apply([entry(3, other)]).await.unwrap();
        assert!(matches!(r[..], [CommandResult::Txn(_)]), "{r:?}");
    }
}
