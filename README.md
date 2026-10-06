# DS-CORE

Distributed graph database engine: range-sharded ordered key-value storage on
RocksDB, replicated per range with Raft, serializable transactions, GQL.

Requirements, plan and decisions live in the reqforge project `DS-CORE`.

## Layout

| Crate | Path | Purpose |
|---|---|---|
| `dscore-server` | `server/` | Storage, Raft, transactions, GQL engine |
| `dscore-importer` | `importer/` | Bulk import CLI |
| `dscore-harness` | `harness/` | Fault injection, benchmarks, probes |

## Build

```sh
cargo build --workspace
cargo test --workspace
```

The toolchain is pinned in `rust-toolchain.toml` (applied by rustup).
