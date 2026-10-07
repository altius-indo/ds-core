# Range / Raft architecture and key layout

- **Task:** TASK-0003 (STORY-0001; feeds TASK-0004, 0006, 0007, 0008, 0012, 0014)
- **Status:** Draft — pending review
- **Satisfies:** REQ-0001, REQ-0016, REQ-0017, REQ-0018, REQ-0033, REQ-0010, REQ-0024
- **Bound by:** DEC-0001 (KV store modelling a property graph), DEC-0002 (range-sharded, Raft per range), DEC-0004 (non-voting analytics replicas), DEC-0011 (Rust; native allow-list RocksDB, aws-lc)

> **Verification note.** This draft was written without web access. Statements about
> third-party projects (raft-rs, openraft, TiKV, CockroachDB) come from the author's
> background knowledge and are marked **[verify]**. Check them before the review
> closes; none of the design's correctness arguments depend on them.

## 1. Goals

| Goal | Source | Measured by |
|---|---|---|
| Every range is a Raft group with 3 or 5 voters, each in a distinct AZ | REQ-0001 AC1/AC2 | STORY-0001 E1 |
| A write is acknowledged only after a majority of *voters* fsynced it; 0 losses across 1,000 all-node power cuts | REQ-0016 AC1 | STORY-0001 E2 |
| One voter plus any number of learners can never acknowledge a write | REQ-0016 AC2 | STORY-0001 E3 |
| ≤ 1 leader per term during membership change; learners promoted only below a lag bound | REQ-0017 | STORY-0002 E1 |
| New leader serving writes ≤ 10 s p99 after leader node death | REQ-0018 | STORY-0003 |
| Automatic size-based split and replica rebalance without rejecting requests | REQ-0033 | STORY-0004 |
| Both edge directions committed together; hub nodes can span ranges | REQ-0010, REQ-0024 | STORY-0007, STORY-0009 |

Non-goals here: the transaction protocol and MVCC (see `transactions.md`), analytics routing (TASK-0024).

## 2. Architecture overview

```
            client / GQL executor
                    │  key → range lookup (cached RangeDescriptor)
                    ▼
 ┌──────────── node (dscore-server) ────────────┐
 │  store 0..n (one per disk)                    │
 │   ├── RaftRouter  ── many Raft groups (one per range replica on this store)
 │   ├── raft log engine  (RocksDB instance "raftdb", sync WAL)
 │   └── state machine    (RocksDB instance "kvdb")
 │  transport: gRPC/TLS between nodes, batched per peer node
 └───────────────────────────────────────────────┘
```

- The **ordered keyspace** (DEC-0001) is cut into contiguous **ranges** `[start, end)`.
- Each range is one **Raft group**. Its replicas are **voters** (3 or 5) and optional
  **learners**: catching-up replicas and analytics replicas (DEC-0004).
- A node hosts thousands of range replicas. This is a *multi-Raft* system, so per-group
  overhead (tasks, timers, fsyncs) must be amortised. That requirement drives the
  library choice in §4.

## 3. Range descriptor and addressing

```rust
struct RangeDescriptor {
    range_id: u64,          // never reused
    start_key: Vec<u8>,     // inclusive
    end_key: Vec<u8>,       // exclusive; empty = +∞
    generation: u64,        // bumped on every split/merge; stale-routing guard
    conf_epoch: u64,        // bumped on every membership change
    replicas: Vec<ReplicaDescriptor>,
}
struct ReplicaDescriptor {
    node_id: u64, store_id: u64, replica_id: u64,
    kind: ReplicaKind,      // Voter | Learner | Analytics
}
```

- **Stale-request guard.** Every request carries the `(range_id, generation, conf_epoch)`
  it was routed with. A replica rejects the request with `RangeMismatch { current
  descriptor }` when its descriptor is newer, and the client refreshes its cache. This
  is what keeps continuous splits safe for in-flight requests (REQ-0033 AC2).
- **Addressing.** Descriptors live in a dedicated **meta range** (range 1, keys under
  `0x01 'm'`). It is itself a 5-voter Raft group, so loss of the meta range is the
  cluster's worst case. Nodes cache descriptors and look them up by
  `seek_for_prev(key)` on meta keys of the form `m/{end_key}`. v1 uses a single meta
  range. When descriptor count makes that range too large to cache, move to two-level
  meta (meta1 → meta2, CockroachDB-style **[verify]**). Open question Q4.
- **Split** is a Raft command on the parent range. It writes two descriptors and bumps
  the generation, atomically in one log entry. The right half reuses the same replica
  set, so no data moves. **Merge** is out of scope for v1.

## 4. Raft library: raft-rs vs openraft

| Criterion | raft-rs (`tikv/raft-rs`) | openraft |
|---|---|---|
| Model | Core state machine only. The app drives a `Ready` loop and owns I/O, timers and transport **[verify]** | Async (tokio) framework: owns the runtime loop; the app implements `RaftLogStorage`, `RaftStateMachine` and `RaftNetwork` **[verify]** |
| Production multi-Raft use | TiKV runs very many groups per store through raft-rs with batching **[verify]** | Used by Databend's meta service and others, typically a small number of groups **[verify]** |
| Membership | Joint consensus (`ConfChangeV2`) and learners **[verify]** | Joint consensus and learners **[verify]** |
| Safety features | PreVote, CheckQuorum, ReadIndex, lease read **[verify]** | Leader lease and linearizable read API **[verify]** |
| API stability | Long-lived 0.x line, slow-moving **[verify]** | Has broken APIs between 0.x minors **[verify]** |
| Per-group cost | One struct plus user-scheduled ticks. Many groups can share one thread and one fsync | Per-group tokio tasks and channels **[verify]** |
| Pure Rust (DEC-0011) | Yes; protobuf codegen is Rust **[verify]** | Yes |

**Recommendation: raft-rs.** DS-CORE is multi-Raft at the scale of thousands of ranges
per store. With raft-rs we control batching: one event loop per store drains `Ready`
from many groups, writes all their log entries in **one** RocksDB write batch, and
fsyncs once. That is the main lever on write latency and on the 1,000-trial power-cut
test. Owning the I/O loop also lets us prove the fsync-before-ack ordering in §6 in our
own code, not inside a framework's scheduler.

The cost is that we write more code ourselves: tick scheduling, snapshots, and message
batching. openraft is the better choice only if the group count stays small, which the
split policy in §8 rules out. Recorded as open question Q1 for the reviewer to confirm;
if accepted, record it as a DEC.

## 5. Replica placement (REQ-0001, REQ-0016 AC2)

- **Locality.** Each node starts with `--locality region=…,zone=…`. The placement code
  (`server/src/placement/`) treats zone as a hard constraint for voters.
- **Voter count** is `num_voters ∈ {3, 5}`. Configuration validation rejects any other
  value (REQ-0001 AC1). The 5-voter option needs ≥ 5 zones for strict
  distinct-AZ placement; with 3 zones, validation rejects 5 voters unless
  `allow_shared_zone=true`. Open question Q2.
- **Voter placement.** Place voters in distinct zones (REQ-0001 AC2), choosing the
  least-loaded store within each zone (replica count, then disk usage).
- **Learners never vote.** Learners and analytics replicas hold the Raft learner role,
  so they are excluded from election and commit quorums by construction. E3 checks
  this: partition so that one voter plus N learners is reachable, then assert that no
  ack arrives within the request timeout.
- **Leaseholder.** The Raft leader also holds the range lease, so reads are served at
  the leader under a lease bounded by the election timeout. Lease reads depend on
  clock drift staying below the configured maximum offset. Read-index reads are the
  fallback when the lease is in doubt.

## 6. Write path and fsync ordering (REQ-0016)

Two RocksDB instances per store:

| Engine | Holds | WAL | Sync |
|---|---|---|---|
| `raftdb` | Raft log entries, `HardState` (term, vote, commit) | on | **every append batch fsynced** |
| `kvdb` | applied state machine data plus `RaftApplyState` (`applied_index`) | on | not synced on apply |

**Write path:**

1. The client request reaches the leaseholder (leader). The leader proposes the entry
   to its group.
2. The store loop collects `Ready` from all groups. It writes the new entries and the
   new `HardState` from every group into **one `raftdb` WriteBatch with `sync=true`**.
3. **Only after step 2 returns** does the loop send this tick's `MsgAppend`/`MsgAppendResponse` messages
   and advance the group (`on_persist_ready`). The leader therefore counts itself
   toward the majority only for entries it has fsynced, and followers acknowledge only
   fsynced entries.
4. Once the commit index passes the entry (a majority of voters fsynced it), apply it
   to `kvdb`. `applied_index` goes in the same `kvdb` WriteBatch as the data, without
   sync.
5. Acknowledge the client after the entry is **committed and applied** on the leader.

**Crash recovery.** `kvdb` may lose its unsynced tail. On restart we read
`applied_index` from `kvdb` and re-apply entries `(applied_index, commit]` from
`raftdb`. Apply must therefore be idempotent at the entry level. It is, because
`applied_index` advances in the same batch as the data.

**Hardware assumptions (to state in operator docs):**
- Linux only (DEC-0008).
- fsync must reach stable media. Drives need power-loss protection, or the volatile
  write cache must be flushed on fsync. Many cloud volumes flush on fsync; local NVMe
  without power-loss protection may not **[verify per target platform]**.
- The power-cut harness (TASK-0005) must cut power at the VM or hypervisor level. A
  process kill doesn't test this.

**Tuning.** Group commit across ranges (step 2) means the fsync rate is per store, not
per range. One open question (Q3) is whether to use Raft Engine (TiKV's log-structured
Raft log store) in place of `raftdb`. It is pure Rust **[verify]**, but it is extra
surface area, so the recommendation is to defer it until benchmarks show the `raftdb`
write path is the bottleneck.

## 7. Elections and failover (REQ-0018)

| Setting | Default | Rule |
|---|---|---|
| `heartbeat_interval` | 500 ms | — |
| `election_timeout` | 5 s (randomised over [5 s, 10 s)) | Must be ≥ 10 × measured heartbeat RTT (REQ-0018 AC2); validated at startup and on change |
| PreVote and CheckQuorum | on | Prevents a partitioned node from disrupting the group |
| Lease duration | `election_timeout` minus max clock offset | The leader stops serving lease reads before a new leader can be elected |

**Failover budget:**

| Step | Time |
|---|---|
| Detection | ≤ 10 s worst case (top of the randomised election range) |
| PreVote and election | ~1–2 RTT |
| New leader applies its log tail and acquires the lease | — |

That leaves no headroom against the 10 s p99 target at the top of the timeout range.
The recommendation is therefore an election timeout randomised over [3 s, 6 s) with a
300 ms heartbeat. TASK-0007 must measure this in the harness (leader-kill × 100);
open question Q5.

## 8. Membership change, split and rebalance (REQ-0017, REQ-0033)

**Membership change** (TASK-0006):
1. Add the new replica as a **learner**.
2. Stream a snapshot to it, then let it catch up on the log.
3. Promote it once `match_index ≥ leader.commit − max_promote_lag` (default 1,000
   entries) for 3 consecutive checks.
4. All voter changes go through **joint consensus** (`ConfChangeV2` entering and
   leaving the joint configuration). Learner add/remove can be a simple change.

**Split** (TASK-0008):
- A range splits when its size exceeds `range_max_bytes` (default 512 MiB; open
  question Q6) or its load passes a hot-range QPS threshold.
- Split keys are chosen at a key boundary. Inside a node prefix, the split may only
  fall between *edge entries*, never between the node document key and the node's
  first edge entry (§9).

**Rebalancer:**
- It runs on the meta-range leaseholder.
- Goal: replica count per store within ±10% of the mean (REQ-0033 AC1), and leaders
  balanced per store.
- Each move is add learner → promote → remove old voter, so voter count never drops
  below `num_voters` during a move.
- Moves are rate-limited per store, and snapshot bandwidth is capped so OLTP latency
  is preserved.

## 9. Node and edge key encoding (REQ-0010, REQ-0024, DEC-0001)

### Top-level layout
All keys are byte strings compared in lexicographic (memcmp) order. RocksDB uses the
default bytewise comparator.

```
0x01 'm' …                              meta range (descriptors, liveness, catalog)
0x02 'g' {graph:u32}  …                 graph data
     … 'n' {node:u64}  0x00              node document (labels + properties)
     … 'n' {node:u64}  'o' {etype:u32} {dst:u64} {rank:u64}   out-edge
     … 'n' {node:u64}  'i' {etype:u32} {src:u64} {rank:u64}   in-edge
0x02 'g' {graph:u32} 'x' {index:u32} {encoded value…} {node:u64}   secondary index (TASK-0020)
```

- **Integers** are fixed-width big-endian, so byte order equals numeric order.
- **`node` ids are 64-bit and generated uniformly at random** (or from a hash), not
  sequentially. Sequential ids would put every insert on the last range (a write
  hotspot). Adjacency locality doesn't need ordered ids, because a node's edges share
  its prefix whatever the id is. Applications look up nodes by their own keys through
  a unique property index (REQ-0031).
- **`etype` (edge type) and label names are interned** to `u32` through a per-graph
  dictionary in the meta range. The graph is schemaless (DEC-0007), so the dictionary
  only grows. This keeps edge keys at a fixed 1+4+8+8 bytes after the node prefix.
- **`rank`** separates parallel edges with the same `(src, etype, dst)` (DEC-0001).
  It is allocated by the writing transaction (TASK-0012 owns the scheme). An edge's
  identity is `(src, etype, dst, rank)`.
- **Variable-length values** (only in index keys) use memcomparable escaping: `0x00`
  becomes `0x00 0xFF`, and the value ends with `0x00 0x01`. This is the
  FoundationDB-tuple / TiDB-codec style **[verify exact scheme]**.
- **MVCC timestamps** are appended as a suffix by the transaction layer; see
  `transactions.md`.

### Adjacency invariants
- **Every edge writes both its out-entry and its in-entry in one transaction**
  (REQ-0010). They usually sit in different ranges, so the transaction is cross-range,
  and atomicity comes from the transaction protocol, not from this layer.
- **Edge properties are stored in both entries**, so a reverse traversal needs no
  second lookup. REQ-0010 AC1 requires identical properties; a property update writes
  both entries in the same transaction. The cost is twice the bytes for edge
  properties. The alternative, an in-entry that points at the out-entry, makes reverse
  traversal a possibly cross-range point read. Open question Q7.
- **`DETACH DELETE`** (DEC-0006) scans `n/{node}/o…` and `n/{node}/i…` and deletes each
  entry together with its mirror entry in one transaction.

### Supernodes (REQ-0024)
- A hub node's `o` and `i` sub-ranges can exceed `range_max_bytes`. Because the split
  key can fall **between edge entries inside a node prefix** (§8), adjacency spills
  across several ranges.
- A one-hop traversal with `LIMIT 100` reads the first range of the prefix and stops,
  which meets AC2 (≤ 50 ms p99) without fan-out.
- A full traversal fans out over the ranges in key order.
- The node document key (`… n {node} 0x00`) sorts before all of its edges, so it always
  stays with the first adjacency range.

## 10. Failure modes and how they are verified

| Failure | Expected behaviour | Eval |
|---|---|---|
| All nodes lose power right after an ack | Acknowledged writes present on restart | STORY-0001 E2 (power-cut × 1,000) |
| Only 1 voter and N learners reachable | No ack; request times out | STORY-0001 E3 |
| Leader node killed | New leader writes within 10 s p99 | STORY-0003 E1 (leader-kill × 100) |
| Continuous add/remove of replicas | ≤ 1 leader per term | STORY-0002 E1 |
| Continuous splits under load | 0 anomalies; stale routing rejected with `RangeMismatch` | REQ-0033 AC2 via the Jepsen suite (`--nemesis split`) |
| Clock skew beyond max offset | Node self-terminates rather than serve stale lease reads | Jepsen `--nemesis clock` |

## 11. Open questions for the reviewer

1. **Q1. Raft library.** Confirm raft-rs over openraft (§4). If accepted, record a DEC.
2. **Q2. 5 voters with only 3 zones.** Reject, or allow shared zones?
3. **Q3. Raft log store.** Separate `raftdb` RocksDB for v1, with Raft Engine deferred until benchmarked?
4. **Q4. Meta addressing.** A single meta range in v1; at what descriptor count do we move to two-level?
5. **Q5. Election timeout defaults.** Are [3 s, 6 s) with a 300 ms heartbeat acceptable given the 10 s p99 failover target?
6. **Q6. `range_max_bytes` default.** 512 MiB proposed. Smaller ranges speed rebalancing but mean more Raft groups.
7. **Q7. Edge properties.** Duplicate them in both entries (proposed) or use pointer in-entries?
8. **Q8. Node ids.** Confirm random 64-bit ids. Applications look nodes up by their own keys through unique indexes, never by insertion order.
