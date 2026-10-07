# Serializable transaction protocol

- **Task:** TASK-0009 (STORY-0005; feeds TASK-0010, TASK-0011, TASK-0012, TASK-0020)
- **Status:** Draft — pending review
- **Satisfies:** REQ-0014, REQ-0028, REQ-0015, REQ-0005, REQ-0010 (both edge entries atomically)
- **Bound by:** DEC-0002 (Raft per range), DEC-0003 (serializable by default), DEC-0004 (eventually consistent analytics), DEC-0006 (DETACH DELETE in one transaction)
- **Proposed decision:** a Raft-replicated timestamp oracle (TSO). See §3 and the DEC recorded as *proposed* in reqforge.

> **Verification note.** Written without web access. Claims about CockroachDB, TiDB,
> Percolator, FoundationDB and Jepsen results come from background knowledge and are
> marked **[verify]**. The correctness argument in §6 depends only on this document.

## 1. Requirements in one table

| Need | Source | Eval |
|---|---|---|
| Every transaction is serializable; no knob lowers isolation | REQ-0014 AC1 | STORY-0005 E1 `isolation_locked` |
| 0 anomalies under partitions, crashes, **clock skew**, membership changes and splits, 1 h per release | REQ-0014 AC2 | STORY-0005 E2 (Elle, list-append) |
| A multi-shard write is all-or-nothing when the coordinator crashes at any phase | REQ-0028 AC1 | STORY-0005 E3 `coordinator_crash_atomic` |
| No silent retry once the read set has changed | REQ-0015 | STORY-0006 |
| A distinct, retryable `SERIALIZATION_CONFLICT` code, used only for conflicts | REQ-0005 | STORY-0006 |

## 2. Summary of the design

- **Timestamps:** a TSO hands out strictly increasing 64-bit timestamps. Its state lives in the meta range, and allocation is batched.
- **Storage:** multi-version (MVCC) on the ordered keyspace. Writes first land as **intents**, which are provisional writes owned by a transaction.
- **Commit:** Percolator-style two-phase commit. Each transaction has a **transaction record** on its *anchor* (primary) key. Commit is a single Raft write of that record. Secondary intents are resolved asynchronously.
- **Serializability:** **commit-time read validation**. The transaction reads at `start_ts`, gets `commit_ts`, then checks that nothing it read changed in `(start_ts, commit_ts]`. If something did, it aborts with `SERIALIZATION_CONFLICT`.
- **Fast path:** a transaction whose writes all fall in one range commits in a single Raft entry (1PC).

## 3. Timestamp source: TSO vs HLC

| | **TSO** (TiDB Placement Driver / Percolator style **[verify]**) | **HLC + uncertainty** (CockroachDB style **[verify]**) |
|---|---|---|
| How timestamps are made | A single Raft-replicated allocator; strictly monotonic | Each node's hybrid logical clock; ordering relies on max clock offset |
| Dependence on clock sync | **None for correctness**. Clocks only drive lease timing | Reads within the uncertainty window restart. Correctness relies on the bound, so nodes must self-terminate when it is exceeded **[verify]** |
| Guarantee reached with read validation | Strict serializability (commit order matches real time) | Serializable; real-time ordering is only within the clock bound **[verify]** |
| Latency cost | One TSO round trip at begin and at commit. Same AZ: sub-millisecond; cross-AZ: ~1–2 ms; batching amortises it | No extra round trip; uncertainty restarts appear under skew |
| Failure mode | TSO leader failover pauses allocation for every transaction (cluster-wide blip ≤ election timeout) | No single component; skew beyond the bound becomes an **anomaly risk** |
| Under REQ-0014's clock nemesis | Skew affects leases, not ordering. Expected anomalies: 0 | Depends on self-termination and the offset bound holding during the nemesis |
| Multi-region | Poor: every transaction crosses to the TSO's region | Good |

**Recommendation: TSO.** Three reasons:
- **The clock nemesis is a release gate.** REQ-0014 AC2 runs Elle with clock skew on
  every release. A TSO takes clock skew out of the correctness argument altogether, so
  the nemesis can only cause availability problems, not anomalies.
- **v1 is one region.** The AZ-level placement in REQ-0001 makes the TSO round trip a
  same-region cost. It can be batched: one RPC hands out a block of timestamps to many
  waiting transactions.
- **It's the simpler correctness argument (§6),** and that argument is what the Jepsen
  suite will test.

**Revisit if** multi-region writes become a requirement. Moving to HLC would then mean
replacing the timestamp provider and adding uncertainty-interval reads. MVCC, intents
and the commit protocol carry over unchanged.

**TSO design:**
- It runs on the meta-range leaseholder.
- It persists a high-water mark `max_ts` through Raft in steps of, for example, 3 s
  worth of logical time.
- It serves timestamps from memory below `max_ts`.
- A new leader resumes at the persisted `max_ts`, which is strictly above anything
  already served, so timestamps never repeat or go backwards across failover.
- Timestamp layout: physical milliseconds (46 bits) followed by a logical counter
  (18 bits).

## 4. MVCC layout

The keys below follow the encodings in `raft-ranges.md` §9.

```
{user_key} 0x00 {!commit_ts:u64}    committed version (ts bit-inverted → newest first)
{user_key} 0x00 0x00…00             intent slot (sorts before every version) → IntentMeta
{anchor_key} 0x01 'txn' {txn_id}    transaction record (on the anchor key's range)
```

- **IntentMeta** is `{ txn_id, start_ts, anchor_key, value | tombstone }`. A key has at most one intent at a time.
- **Reading at ts `t`:** seek to `{user_key} 0x00`. If an intent is there and belongs to
  another transaction, follow §7. Otherwise return the first version with
  `commit_ts ≤ t`.
- **Garbage collection:** versions older than `gc_ttl` (default 4 h; must cover the
  longest read and the PITR granularity in REQ-0019) are removed by a compaction filter.
  The newest version at or below the GC threshold is always kept.

## 5. Commit protocol

**Begin.** `start_ts ← TSO`. The client-side coordinator, inside the gateway node,
keeps the read set and the write buffer.

**Reads.** Read at `start_ts`. Record each **read span** in the read set: a point key
or a scanned range `[a, b)`, for example the adjacency prefix of a one-hop traversal.

**Writes.** Buffer them in the coordinator. The first written key becomes the **anchor**.

**Commit.**

1. **Prewrite.** For each written key, write an intent through Raft on that key's
   range. Prewrite fails with `SERIALIZATION_CONFLICT` if the key has a committed
   version with `commit_ts > start_ts` (write-write conflict) or another transaction's
   live intent. Live means its record is PENDING with a fresh heartbeat; §7 says how
   to push it.
   The **transaction record** `{status: PENDING, heartbeat_ts, start_ts}` is written
   together with the anchor's intent in the same Raft entry.
2. **Get `commit_ts ← TSO`.** This happens strictly after every intent from step 1 is
   durable.
3. **Validate the read set.** For each read span, the span's range leaseholder checks
   for a committed version with `start_ts < commit_ts' ≤ commit_ts`, or for another
   transaction's intent whose record isn't ABORTED (§7). Either one aborts with
   `SERIALIZATION_CONFLICT`. Validation of point keys and scanned spans runs in
   parallel across ranges. Spans the transaction also wrote are skipped.
4. **Commit point.** A conditional Raft write on the anchor's range sets the record
   from PENDING to `COMMITTED{commit_ts}`. **This single write decides the transaction.**
   Once it's applied, the client is acknowledged.
5. **Resolve.** Asynchronously, turn each intent into a committed version at
   `commit_ts`, then garbage-collect the record.

**Single-range fast path (1PC).** When every write and every read span is in one
range, the leaseholder does steps 1–4 as one Raft entry. It validates against its own
data under the range latch, so there are no intents and no record.

**Read-only transactions** read at `start_ts` and commit without validation (§6.2).

**Edges.** An edge insert writes `n/{src}/o/…` and `n/{dst}/i/…` (REQ-0010). These
usually sit in two ranges, so the transaction takes the 2PC path. Both entries become
visible at the same `commit_ts` because readers resolve intents through the anchor
record, so no reader sees a one-sided edge (REQ-0010 AC2).

## 6. Why this is serializable

Serialization order is `commit_ts`; read-only transactions are ordered by `start_ts`.

### 6.1 Read-write transactions

Take a read-write transaction T with `start_ts = s` and `commit_ts = c`. T's reads
observe the state at `s`. It is enough to show that no transaction U with
`s < u = commit_ts(U) < c` wrote into T's read set. If no such U exists, the state at
`s` equals the state just before `c` on every span T read, so T is equivalent to
executing atomically at `c`.

- **Case 1: U's commit point applied before T's validation read the span.** Then
  validation (step 3) sees U's committed version in `(s, c]`, or U's intent pointing to
  a COMMITTED record. T aborts.
- **Case 2: U's commit point had not applied when T validated.** U obtained
  `u < c` from the TSO before T obtained `c`. Because the TSO is strictly monotonic,
  U's step 2 happened before T's step 2. U's step 2 follows completion of U's
  prewrites (U's step 1). So U's intents on every key U writes were durable before
  T's step 2, and therefore before T's validation. T's validation finds U's intent,
  whose record is PENDING or COMMITTED, so T aborts. T may push U first (§7), but it
  never ignores the intent.

Writes between `c` and T's own commit point don't matter: they are ordered after T.
Write-write conflicts are caught at prewrite. **This argument doesn't mention
wall-clock time anywhere**, which is why REQ-0014's clock nemesis can't create
anomalies (§3).

**Phantoms.** A read span is a key range, not a key set. An insert of a new key inside
`[a, b)` with `commit_ts ∈ (s, c]` is a committed version, or an intent, inside the
span, so validation catches it.

### 6.2 Read-only transactions

They read at `s` and must see every transaction with `commit_ts ≤ s`, all of it or
none of it.

- A writer U with `u < s` got `u` from the TSO before the reader got `s`, so U's
  intents were durable before the read began (prewrite precedes step 2).
- U may not have reached its commit point yet. The reader still meets U's intent and
  must **wait or push** (§7), never skip it.
- Once U's record is decided, the reader sees all of U at `u` (COMMITTED) or none of
  it (ABORTED).

That makes every read a consistent snapshot at `s`.

## 7. Conflicts, pushes and coordinator recovery (REQ-0028)

**When a reader or writer meets another transaction's intent,** it looks up the
intent's transaction record at the anchor:

| Record | Action |
|---|---|
| `COMMITTED{c}` | Resolve the intent to a version at `c`, then continue |
| `ABORTED` or missing (and past the creation grace period) | Remove the intent, then continue |
| `PENDING`, heartbeat fresh | Wait up to `lock_wait_timeout`, then retry. A writer gives up with `SERIALIZATION_CONFLICT`. Deadlocks are broken by a wait-for graph on the anchor leaseholders, or v1 settles for timeout plus abort (open question Q3) |
| `PENDING`, heartbeat older than `txn_liveness_ttl` (default 5 s) | **Push to abort.** A conditional Raft write PENDING → ABORTED on the anchor. That write races with the coordinator's own commit; exactly one wins |

**Coordinator crash at each phase (REQ-0028 AC1, E3):**

| Crash point | State left | Outcome |
|---|---|---|
| Before or during prewrite | Some intents and possibly a PENDING record | Heartbeat expires and it is pushed to ABORTED: **none visible** |
| After prewrite, before validation | All intents and a PENDING record | Same: **none visible** |
| After validation, before commit point | Same | Same: **none visible** |
| After commit point, before resolve | COMMITTED record and intents | Readers resolve forward: **all visible** |
| During resolve | Mix of versions and intents | Intents still point at COMMITTED: **all visible** |

The decision is the single anchor-record write in every case, so the outcome is always
all or none. E3 injects a crash at each row, 100 trials.

**Liveness.** The coordinator heartbeats the record every `txn_liveness_ttl / 3`.
Long-running transactions stay alive; transactions on a crashed gateway are aborted
within one TTL.

## 8. Retry and error contract (REQ-0005, REQ-0015)

- **Error code.** `SERIALIZATION_CONFLICT` is returned only for: a write-write conflict
  at prewrite, failed read validation, being pushed to ABORTED, or lock-wait timeout.
  Syntax, permission and constraint errors have their own codes (REQ-0005 AC2). Unique
  index violations (REQ-0031 AC2) are constraint errors, not conflicts.
- **Internal retry** (TASK-0011) is allowed **only** when no result row has been
  returned to the client and the transaction is a single auto-commit statement. The
  server then re-runs the whole statement at a fresh `start_ts`. In an interactive
  multi-statement transaction, the client has already seen reads, so the server never
  retries and returns `SERIALIZATION_CONFLICT` (REQ-0015 AC1).
- **Read refresh, which skips the retry entirely.** If validation fails only because
  `start_ts` is too old, but the values read are unchanged in `(s, c]`, there is no
  conflict, since validation compares versions, not timestamps. This is already the
  §5 step 3 rule.
- **Isolation is fixed.** The protocol accepts no isolation setting. Any session or
  client attempt to set a lower level returns a configuration error (REQ-0014 AC1,
  E1 `isolation_locked`).

## 9. Interaction with other subsystems

- **Raft leases:** intents, records and validation go through the range leaseholder.
  A lease transfer carries the range latch state. The TSO's monotonicity doesn't depend
  on leases.
- **Splits:** a span validated across a split boundary is re-routed per range. A stale
  routing attempt gets `RangeMismatch` and retries the *validation step*, not the
  transaction.
- **Analytics learners (DEC-0004):**
  - They apply the same Raft log, so they hold intents too.
  - They serve reads at a **resolved timestamp**: the highest ts below which every
    intent in the range is resolved, published by the leader.
  - Those reads are eventually consistent and need no validation, which REQ-0025..27
    allows.
- **Indexes (REQ-0031):** index entries are ordinary keys written in the same
  transaction. They get intents and validation like data keys, so reads through an
  index are serializable.

## 10. How the harness verifies it

- **E2:** Elle list-append for 1 h with the nemeses `partition, crash, clock,
  membership, split`. It checks serializable (G0, G1a/b/c, G-single, G2).
  Plan: also run with `--check strict-serializable` to confirm the TSO's stronger
  guarantee **[verify Elle flag name]**.
- **E3:** a crash-injection point at each row of the §7 table.
- **Extra edge test:** concurrent edge insert, update and delete across shards. A
  checker reads both endpoints at one snapshot and asserts 0 one-sided edges
  (REQ-0010 AC2).

## 11. Open questions for the reviewer

1. **Q1.** Accept **TSO** over HLC for v1 (proposed DEC)? It knowingly gives up cheap
   multi-region writes.
2. **Q2.** Defaults: TSO batch window, `txn_liveness_ttl` (5 s), `lock_wait_timeout`
   (proposed 1 s) and `gc_ttl` (4 h).
3. **Q3.** Deadlock handling in v1: timeout plus abort (proposed), or wait-for-graph
   detection?
4. **Q4.** Parallel commits (CockroachDB-style: commit without waiting on a separate
   record write **[verify]**). Defer to after v1? It saves one Raft round trip on the
   commit path of multi-range transactions.
5. **Q5.** Read-set size cap: above N spans or M bytes, collapse spans to coarser
   ranges (more false conflicts), or reject the transaction?
