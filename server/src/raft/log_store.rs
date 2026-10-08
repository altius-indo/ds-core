//! Raft log storage on RocksDB with group-committed fsync (REQ-0016, design/raft-ranges.md §6).
//!
//! One `LogEngine` per store holds the Raft logs of every range on that store (the `raftdb`
//! instance). An append is written to RocksDB without sync, so it is readable as soon as
//! `append` returns, as openraft requires; it is then queued to the engine's writer thread.
//! The writer drains every queued append from every group, syncs the WAL once
//! (`flush_wal(true)`), and only then completes their `LogFlushed` callbacks. openraft counts
//! this node towards a quorum only for flushed entries, so an entry is acknowledged only
//! after a majority of voters have it on stable storage.

// reqforge: implements REQ-0016

use std::fmt::Debug;
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::Arc;
use std::sync::mpsc;
use std::thread;

use openraft::storage::{LogFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::{AnyError, OptionalSend};
use rocksdb::{DB, Direction, IteratorMode, Options, WriteBatch, WriteOptions};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::types::{Entry, LogId, RangeId, StorageError, StorageIOError, TypeConfig, Vote};

const ENTRY: u8 = 0x01;
const VOTE: u8 = 0x02;
const COMMITTED: u8 = 0x03;
const PURGED: u8 = 0x04;

fn key(range: RangeId, kind: u8) -> Vec<u8> {
    let mut k = Vec::with_capacity(17);
    k.extend_from_slice(&range.to_be_bytes());
    k.push(kind);
    k
}

fn entry_key(range: RangeId, index: u64) -> Vec<u8> {
    let mut k = key(range, ENTRY);
    k.extend_from_slice(&index.to_be_bytes());
    k
}

fn encode<T: Serialize>(v: &T) -> Result<Vec<u8>, AnyError> {
    postcard::to_allocvec(v).map_err(|e| AnyError::error(e.to_string()))
}

fn decode<T: DeserializeOwned>(b: &[u8]) -> Result<T, AnyError> {
    postcard::from_bytes(b).map_err(|e| AnyError::error(e.to_string()))
}

/// Work for the writer thread: sync the WAL, then complete these callbacks.
enum Flush {
    Log(LogFlushed<TypeConfig>),
    Notify(tokio::sync::oneshot::Sender<Result<(), String>>),
}

/// The per-store Raft log database and its group-commit writer.
pub struct LogEngine {
    db: Arc<DB>,
    flushes: mpsc::Sender<Flush>,
}

impl LogEngine {
    pub fn open(path: &Path) -> Result<Arc<Self>, rocksdb::Error> {
        Self::open_with(path, true)
    }

    /// Like `open`, but appends are acknowledged without fsync. Exists only so the power-cut
    /// harness can prove it detects lost writes; never enabled in a release build.
    #[cfg(feature = "fault-injection")]
    pub fn open_without_fsync(path: &Path) -> Result<Arc<Self>, rocksdb::Error> {
        Self::open_with(path, false)
    }

    fn open_with(path: &Path, sync_wal: bool) -> Result<Arc<Self>, rocksdb::Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = Arc::new(DB::open(&opts, path)?);
        let (tx, rx) = mpsc::channel::<Flush>();
        let writer_db = db.clone();
        thread::Builder::new()
            .name("raft-log-fsync".into())
            .spawn(move || {
                // Block for one request, take everything else queued, fsync once, reply to all.
                while let Ok(first) = rx.recv() {
                    let mut pending = vec![first];
                    pending.extend(rx.try_iter());
                    let result = if sync_wal {
                        writer_db.flush_wal(true)
                    } else {
                        Ok(())
                    };
                    for p in pending {
                        match p {
                            Flush::Log(cb) => cb.log_io_completed(
                                result
                                    .clone()
                                    .map_err(|e| std::io::Error::other(e.to_string())),
                            ),
                            Flush::Notify(tx) => {
                                let _ = tx.send(result.clone().map_err(|e| e.to_string()));
                            }
                        }
                    }
                }
            })
            .expect("spawn raft-log-fsync thread");
        Ok(Arc::new(Self { db, flushes: tx }))
    }

    /// The log store for one range's Raft group on this store.
    pub fn range(self: &Arc<Self>, range: RangeId) -> RangeLogStore {
        RangeLogStore {
            engine: self.clone(),
            range,
        }
    }

    fn write(&self, batch: WriteBatch, sync: bool) -> Result<(), rocksdb::Error> {
        let mut opts = WriteOptions::default();
        opts.set_sync(sync);
        self.db.write_opt(batch, &opts)
    }

    async fn sync(&self) -> Result<(), String> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.flushes
            .send(Flush::Notify(tx))
            .map_err(|_| "log writer stopped".to_string())?;
        rx.await.map_err(|_| "log writer stopped".to_string())?
    }
}

/// One range's view of the store's Raft log. Cheap to clone; clones share the engine.
#[derive(Clone)]
pub struct RangeLogStore {
    engine: Arc<LogEngine>,
    range: RangeId,
}

impl RangeLogStore {
    fn get<T: DeserializeOwned>(&self, kind: u8) -> Result<Option<T>, AnyError> {
        match self
            .engine
            .db
            .get(key(self.range, kind))
            .map_err(|e| AnyError::new(&e))?
        {
            Some(b) => Ok(Some(decode(&b)?)),
            None => Ok(None),
        }
    }

    fn last_entry(&self) -> Result<Option<Entry>, AnyError> {
        let prefix = key(self.range, ENTRY);
        let upper = entry_key(self.range, u64::MAX);
        let mut it = self
            .engine
            .db
            .iterator(IteratorMode::From(&upper, Direction::Reverse));
        match it.next() {
            Some(Ok((k, v))) if k.starts_with(&prefix) => Ok(Some(decode(&v)?)),
            Some(Err(e)) => Err(AnyError::new(&e)),
            _ => Ok(None),
        }
    }
}

impl RaftLogReader<TypeConfig> for RangeLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, StorageError> {
        let start = match range.start_bound() {
            Bound::Included(&s) => s,
            Bound::Excluded(&s) => s + 1,
            Bound::Unbounded => 0,
        };
        let prefix = key(self.range, ENTRY);
        let from = entry_key(self.range, start);
        let mut out = Vec::new();
        for item in self
            .engine
            .db
            .iterator(IteratorMode::From(&from, Direction::Forward))
        {
            let (k, v) = item.map_err(|e| StorageIOError::read_logs(&e))?;
            if !k.starts_with(&prefix) {
                break;
            }
            let index = u64::from_be_bytes(k[prefix.len()..].try_into().expect("8-byte index"));
            if !range.contains(&index) {
                break;
            }
            out.push(decode(&v).map_err(StorageIOError::read_logs)?);
        }
        Ok(out)
    }
}

impl RaftLogStorage<TypeConfig> for RangeLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError> {
        let last_purged: Option<LogId> = self.get(PURGED).map_err(StorageIOError::read_logs)?;
        let last = self.last_entry().map_err(StorageIOError::read_logs)?;
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last.map(|e| e.log_id).or(last_purged),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote) -> Result<(), StorageError> {
        // A vote must be durable before it is acted on: written and synced before returning.
        let mut b = WriteBatch::default();
        b.put(
            key(self.range, VOTE),
            encode(vote).map_err(StorageIOError::write_vote)?,
        );
        self.engine
            .write(b, true)
            .map_err(|e| StorageIOError::write_vote(&e))?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, StorageError> {
        Ok(self.get(VOTE).map_err(StorageIOError::read_vote)?)
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), StorageError> {
        let mut b = WriteBatch::default();
        b.put(
            key(self.range, COMMITTED),
            encode(&committed).map_err(StorageIOError::write)?,
        );
        self.engine
            .write(b, false)
            .map_err(|e| StorageIOError::write(&e))?;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, StorageError> {
        Ok(self
            .get::<Option<LogId>>(COMMITTED)
            .map_err(StorageIOError::read)?
            .flatten())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut b = WriteBatch::default();
        for e in entries {
            b.put(
                entry_key(self.range, e.log_id.index),
                encode(&e).map_err(StorageIOError::write_logs)?,
            );
        }
        // Visible to readers now; durable once the writer thread has synced the WAL.
        self.engine
            .write(b, false)
            .map_err(|e| StorageIOError::write_logs(&e))?;
        self.engine
            .flushes
            .send(Flush::Log(callback))
            .map_err(|_| StorageIOError::write_logs(AnyError::error("log writer stopped")))?;
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId) -> Result<(), StorageError> {
        let mut b = WriteBatch::default();
        b.delete_range(
            entry_key(self.range, log_id.index),
            key(self.range, ENTRY + 1),
        );
        self.engine
            .write(b, true)
            .map_err(|e| StorageIOError::write_logs(&e))?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), StorageError> {
        let mut b = WriteBatch::default();
        b.put(
            key(self.range, PURGED),
            encode(&log_id).map_err(StorageIOError::write_logs)?,
        );
        b.delete_range(
            entry_key(self.range, 0),
            entry_key(self.range, log_id.index + 1),
        );
        self.engine
            .write(b, false)
            .map_err(|e| StorageIOError::write_logs(&e))?;
        self.engine
            .sync()
            .await
            .map_err(|e| StorageIOError::write_logs(AnyError::error(e)))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_keys_sort_by_range_then_index() {
        assert!(entry_key(1, u64::MAX) < key(1, ENTRY + 1));
        assert!(entry_key(1, 2) < entry_key(1, 10));
        assert!(entry_key(1, u64::MAX) < entry_key(2, 0));
    }
}
