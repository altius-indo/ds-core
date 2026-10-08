//! A range's replicated state machine on the store's `kvdb` RocksDB instance.
//!
//! User keys are stored as-is under a data prefix, so a range is a key interval rather than a
//! separate keyspace and a split moves no data (design/raft-ranges.md §3). Each apply writes
//! the data, the last applied log id and the membership in one batch, without sync: after a
//! crash the Raft log, which is synced, replays whatever the batch lost.

use std::io::Cursor;
use std::path::Path;
use std::sync::{Arc, Mutex};

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot};
use openraft::{AnyError, EntryPayload, OptionalSend};
use rocksdb::{DB, Direction, IteratorMode, Options, WriteBatch};
use serde::{Deserialize, Serialize};

use super::types::{
    Command, CommandResult, Entry, LogId, RangeId, SnapshotMeta, StorageError, StorageIOError,
    StoredMembership, TypeConfig,
};

const DATA: u8 = 0x10;
const META: u8 = 0x11;
const APPLIED: u8 = b'a';
const MEMBERSHIP: u8 = b'm';

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

    /// The state machine for `range`, owning user keys in `[start, end)` (`end` empty = +∞).
    pub fn range(
        self: &Arc<Self>,
        range: RangeId,
        start: Vec<u8>,
        end: Vec<u8>,
    ) -> RangeStateMachine {
        RangeStateMachine {
            engine: self.clone(),
            range,
            start,
            end,
            snapshot: Arc::new(Mutex::new(None)),
        }
    }
}

/// A user key and its value.
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
    start: Vec<u8>,
    end: Vec<u8>,
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
        }
    }

    fn interval(&self) -> (Vec<u8>, Vec<u8>) {
        let start = data_key(&self.start);
        let end = if self.end.is_empty() {
            vec![DATA + 1]
        } else {
            data_key(&self.end)
        };
        (start, end)
    }

    fn pairs(&self) -> Result<Vec<KvPair>, AnyError> {
        let (start, end) = self.interval();
        let mut out = Vec::new();
        for item in self
            .engine
            .db
            .iterator(IteratorMode::From(&start, Direction::Forward))
        {
            let (k, v) = item.map_err(|e| AnyError::new(&e))?;
            if k.as_ref() >= end.as_slice() {
                break;
            }
            out.push((k[1..].to_vec(), v.to_vec()));
        }
        Ok(out)
    }
}

impl RaftSnapshotBuilder<TypeConfig> for RangeStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError> {
        let applied: Option<LogId> = self
            .get_meta(APPLIED)
            .map_err(StorageIOError::read_state_machine)?;
        let membership: StoredMembership = self
            .get_meta(MEMBERSHIP)
            .map_err(StorageIOError::read_state_machine)?
            .unwrap_or_default();
        let data = encode(&SnapshotData {
            pairs: self.pairs().map_err(StorageIOError::read_state_machine)?,
        })
        .map_err(StorageIOError::read_state_machine)?;
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
            .get_meta(APPLIED)
            .map_err(StorageIOError::read_state_machine)?;
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
        let mut batch = WriteBatch::default();
        let mut results = Vec::new();
        let mut last = None;
        for e in entries {
            match &e.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(cmd) => Self::apply_command(&mut batch, cmd),
                EntryPayload::Membership(m) => batch.put(
                    meta_key(self.range, MEMBERSHIP),
                    encode(&StoredMembership::new(Some(e.log_id), m.clone()))
                        .map_err(StorageIOError::write_state_machine)?,
                ),
            }
            last = Some(e.log_id);
            results.push(CommandResult);
        }
        if let Some(l) = last {
            batch.put(
                meta_key(self.range, APPLIED),
                encode(&Some(l)).map_err(StorageIOError::write_state_machine)?,
            );
        }
        self.engine
            .db
            .write(batch)
            .map_err(|e| StorageIOError::write_state_machine(&e))?;
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
        let (start, end) = self.interval();
        let mut batch = WriteBatch::default();
        batch.delete_range(start, end);
        for (k, v) in &parsed.pairs {
            batch.put(data_key(k), v);
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
