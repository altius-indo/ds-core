//! STORY-0005 (E1, E3) and STORY-0006 (E1-E3): serializable cross-shard transactions.
//! The cluster has 3 nodes and 3 data ranges: [-∞, g), [g, p), [p, +∞).

use std::sync::Arc;
use std::time::Duration;

use dscore_server::txn::cluster::Cluster;
use dscore_server::txn::coordinator::{CrashPoint, Isolation, TxnClient, TxnConfig};
use dscore_server::txn::error::ErrorCode;
use openraft::Config;
use tempfile::TempDir;

struct Env {
    client: TxnClient,
    _dir: TempDir,
}

async fn env() -> Env {
    let dir = TempDir::new().unwrap();
    let config = Config {
        heartbeat_interval: 50,
        election_timeout_min: 300,
        election_timeout_max: 600,
        ..Default::default()
    };
    let cluster = Cluster::start(dir.path(), 3, &[b"g", b"p"], config)
        .await
        .unwrap();
    let client = TxnClient::new(
        Arc::new(cluster),
        TxnConfig {
            liveness_ttl: Duration::from_millis(300),
            lock_wait: Duration::from_secs(3),
        },
    );
    Env { client, _dir: dir }
}

async fn read_value(client: &TxnClient, key: &str) -> Option<String> {
    let mut t = client.begin().await.unwrap();
    let v = t.get(key.as_bytes()).await.unwrap();
    v.map(|b| String::from_utf8(b).unwrap())
}

async fn write(client: &TxnClient, pairs: &[(&str, &str)]) {
    let mut t = client.begin().await.unwrap();
    for (k, v) in pairs {
        t.put(k.as_bytes(), v.as_bytes());
    }
    t.commit().await.unwrap();
}

// reqforge: verifies REQ-0014#AC1
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isolation_locked_to_serializable() {
    let e = env().await;
    assert!(e.client.set_isolation(Isolation::Serializable).is_ok());
    for level in [
        Isolation::SnapshotIsolation,
        Isolation::RepeatableRead,
        Isolation::ReadCommitted,
        Isolation::ReadUncommitted,
    ] {
        let err = e.client.set_isolation(level).unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidConfiguration, "{level:?}");
        assert!(!err.code.is_retryable());
    }
}

// reqforge: verifies REQ-0005#AC1
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forced_write_write_conflict_code() {
    let e = env().await;
    // Single range (one-phase commit).
    let mut t1 = e.client.begin().await.unwrap();
    let mut t2 = e.client.begin().await.unwrap();
    t1.put(b"a", b"1");
    t2.put(b"a", b"2");
    t1.commit().await.unwrap();
    let err = t2.commit().await.unwrap_err();
    assert_eq!(err.code, ErrorCode::SerializationConflict, "{err}");
    assert_eq!(err.code.sqlstate(), "40001");
    assert!(err.code.is_retryable());
    assert_eq!(read_value(&e.client, "a").await.as_deref(), Some("1"));

    // Across ranges (two-phase commit).
    let mut t3 = e.client.begin().await.unwrap();
    let mut t4 = e.client.begin().await.unwrap();
    t3.put(b"b", b"3");
    t3.put(b"x", b"3");
    t4.put(b"x", b"4");
    t4.put(b"h", b"4");
    t3.commit().await.unwrap();
    let err = t4.commit().await.unwrap_err();
    assert_eq!(err.code, ErrorCode::SerializationConflict, "{err}");
    assert_eq!(read_value(&e.client, "x").await.as_deref(), Some("3"));
    assert_eq!(
        read_value(&e.client, "h").await,
        None,
        "the conflicting transaction left a write behind"
    );
}

// reqforge: verifies REQ-0005#AC2
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn syntax_permission_constraint_not_conflict() {
    let e = env().await;
    write(&e.client, &[("node/1", "a")]).await;
    let mut t = e.client.begin().await.unwrap();
    let err = t.insert_new(b"node/1", b"b").await.unwrap_err();
    assert_eq!(err.code, ErrorCode::ConstraintViolation);
    assert_ne!(err.code, ErrorCode::SerializationConflict);

    for code in [
        ErrorCode::Syntax,
        ErrorCode::Permission,
        ErrorCode::ConstraintViolation,
        ErrorCode::InvalidConfiguration,
        ErrorCode::Unavailable,
        ErrorCode::Internal,
    ] {
        assert!(!code.is_retryable(), "{code:?} must not be retryable");
        assert_ne!(code.sqlstate(), ErrorCode::SerializationConflict.sqlstate());
    }
}

// reqforge: verifies REQ-0015#AC1
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interactive_readset_changed_no_retry() {
    let e = env().await;
    write(&e.client, &[("balance", "100")]).await;

    let mut t1 = e.client.begin().await.unwrap();
    assert_eq!(
        t1.get(b"balance").await.unwrap().as_deref(),
        Some(&b"100"[..])
    );
    // Another transaction changes what t1 read.
    write(&e.client, &[("balance", "50")]).await;
    t1.put(b"zz-audit", b"saw 100");
    let err = t1.commit().await.unwrap_err();
    assert_eq!(err.code, ErrorCode::SerializationConflict, "{err}");
    // Not silently retried: none of t1's writes landed.
    assert_eq!(read_value(&e.client, "zz-audit").await, None);
    assert_eq!(
        read_value(&e.client, "balance").await.as_deref(),
        Some("50")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_skew_prevented_across_ranges() {
    let e = env().await;
    write(&e.client, &[("doc-a", "on"), ("doc-z", "on")]).await;
    // Classic write skew: each transaction reads both flags and turns off a different one.
    let mut t1 = e.client.begin().await.unwrap();
    let mut t2 = e.client.begin().await.unwrap();
    for t in [&mut t1, &mut t2] {
        assert!(t.get(b"doc-a").await.unwrap().is_some());
        assert!(t.get(b"doc-z").await.unwrap().is_some());
    }
    t1.put(b"doc-a", b"off");
    t2.put(b"doc-z", b"off");
    let r1 = t1.commit().await;
    let r2 = t2.commit().await;
    assert!(
        r1.is_ok() != r2.is_ok(),
        "exactly one may commit: {r1:?} {r2:?}"
    );
    let failed = r1.err().or(r2.err()).unwrap();
    assert_eq!(failed.code, ErrorCode::SerializationConflict);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_reads_and_own_writes() {
    let e = env().await;
    write(&e.client, &[("k1", "v1"), ("k2", "v2"), ("q1", "x")]).await;
    let mut t = e.client.begin().await.unwrap();
    t.put(b"k3", b"v3");
    t.delete(b"k1");
    let seen = t.scan(b"k", b"l").await.unwrap();
    assert_eq!(
        seen,
        vec![
            (b"k2".to_vec(), b"v2".to_vec()),
            (b"k3".to_vec(), b"v3".to_vec())
        ]
    );
    // A later write is invisible to t's snapshot.
    write(&e.client, &[("k4", "v4")]).await;
    assert_eq!(t.get(b"k4").await.unwrap(), None);
}

// reqforge: verifies REQ-0028#AC1
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_crash_atomic() {
    let e = env().await;
    for trial in 0..100 {
        let point = CrashPoint::ALL[trial % CrashPoint::ALL.len()];
        // A node and two edges on three different shards.
        let keys = [
            format!("a/node/{trial}"),
            format!("h/edge/{trial}"),
            format!("q/edge/{trial}"),
        ];
        let mut t = e.client.begin().await.unwrap();
        for k in &keys {
            t.put(k.as_bytes(), b"v");
        }
        assert!(
            t.commit_with(Some(point)).await.is_err(),
            "crash hook did not fire"
        );

        // Recovery: readers resolve forward or push the abandoned transaction after its TTL.
        let mut visible = Vec::new();
        for k in &keys {
            visible.push(read_value(&e.client, k).await.is_some());
        }
        let committed = matches!(
            point,
            CrashPoint::AfterCommitRecord | CrashPoint::AfterFirstResolve
        );
        assert!(
            visible.iter().all(|&v| v == committed),
            "trial {trial} crash {point:?}: visibility {visible:?}, expected all {committed}"
        );
    }
}
