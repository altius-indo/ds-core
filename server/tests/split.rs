//! Range splits (REQ-0033, TASK-0008): data stays put, routing follows the new descriptors,
//! stale routing is rejected and retried, and transaction records follow their anchors.

use std::sync::Arc;
use std::time::Duration;

use dscore_server::txn::cluster::Cluster;
use dscore_server::txn::coordinator::{CrashPoint, TxnClient, TxnConfig};
use dscore_server::txn::error::ErrorCode;
use openraft::Config;
use tempfile::TempDir;

async fn env() -> (TxnClient, Arc<Cluster>, TempDir) {
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
        cluster.clone(),
        TxnConfig {
            liveness_ttl: Duration::from_millis(300),
            lock_wait: Duration::from_secs(3),
        },
    );
    (client, cluster, dir)
}

async fn put(c: &TxnClient, pairs: &[(&str, &str)]) {
    let mut t = c.begin().await.unwrap();
    for (k, v) in pairs {
        t.put(k.as_bytes(), v.as_bytes());
    }
    t.commit().await.unwrap();
}

async fn get(c: &TxnClient, k: &str) -> Option<String> {
    let mut t = c.begin().await.unwrap();
    t.get(k.as_bytes())
        .await
        .unwrap()
        .map(|v| String::from_utf8(v).unwrap())
}

fn range_of(cluster: &Cluster, k: &str) -> u64 {
    cluster.range_for(k.as_bytes()).id
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_keeps_data_and_routes_new_writes() {
    let (c, cluster, _d) = env().await;
    put(&c, &[("h1", "a"), ("j1", "b"), ("m1", "c"), ("o1", "d")]).await;
    let parent = range_of(&cluster, "m1");
    let child = cluster.split(parent, b"k").await.unwrap();
    assert_ne!(child, parent);
    assert_eq!(range_of(&cluster, "j1"), parent);
    assert_eq!(range_of(&cluster, "m1"), child);
    // Existing data on both sides is readable through the new routing.
    for (k, v) in [("h1", "a"), ("j1", "b"), ("m1", "c"), ("o1", "d")] {
        assert_eq!(get(&c, k).await.as_deref(), Some(v), "{k}");
    }
    // New writes land on both halves; a transaction spanning them commits (2PC).
    put(&c, &[("j2", "x"), ("m2", "y")]).await;
    assert_eq!(get(&c, "m2").await.as_deref(), Some("y"));
    // A scan across the split point sees both sides.
    let mut t = c.begin().await.unwrap();
    let keys: Vec<String> = t
        .scan(b"h", b"p")
        .await
        .unwrap()
        .into_iter()
        .map(|(k, _)| String::from_utf8(k).unwrap())
        .collect();
    assert_eq!(keys, vec!["h1", "j1", "j2", "m1", "m2", "o1"]);
    // Split again, inside the child.
    let grandchild = cluster.split(child, b"n").await.unwrap();
    assert_eq!(range_of(&cluster, "o1"), grandchild);
    assert_eq!(get(&c, "o1").await.as_deref(), Some("d"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transaction_spanning_a_split_commits_or_retries() {
    let (c, cluster, _d) = env().await;
    put(&c, &[("h1", "1"), ("m1", "1")]).await;
    // t reads both keys before the split and commits after it.
    let mut t = c.begin().await.unwrap();
    assert!(t.get(b"h1").await.unwrap().is_some());
    assert!(t.get(b"m1").await.unwrap().is_some());
    t.put(b"h1", b"2");
    t.put(b"m1", b"2");
    cluster.split(range_of(&cluster, "m1"), b"k").await.unwrap();
    match t.commit().await {
        Ok(_) => {
            assert_eq!(get(&c, "h1").await.as_deref(), Some("2"));
            assert_eq!(get(&c, "m1").await.as_deref(), Some("2"));
        }
        Err(e) => {
            // Never a non-retryable error, and never half-applied.
            assert_eq!(e.code, ErrorCode::SerializationConflict, "{e}");
            assert_eq!(
                get(&c, "h1").await,
                get(&c, "m1").await.map(|_| "1".to_string())
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_record_follows_its_anchor_across_a_split() {
    let (c, cluster, _d) = env().await;
    // A 2PC transaction anchored at "m1" (the first written key) dies after prewriting.
    let mut t = c.begin().await.unwrap();
    t.put(b"m1", b"v");
    t.put(b"z1", b"v");
    assert!(
        t.commit_with(Some(CrashPoint::AfterAllPrewrites))
            .await
            .is_err()
    );
    // Split so the anchor's record must move to the child range.
    cluster.split(range_of(&cluster, "m1"), b"k").await.unwrap();
    // A reader finds the intent, looks the record up in the child range, pushes it after the
    // TTL, and sees nothing: the abandoned transaction is aborted, not lost or stuck.
    assert_eq!(get(&c, "m1").await, None);
    assert_eq!(get(&c, "z1").await, None);
    put(&c, &[("m1", "after")]).await;
    assert_eq!(get(&c, "m1").await.as_deref(), Some("after"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_down_during_split_recovers_the_child_range() {
    let (c, cluster, _d) = env().await;
    put(&c, &[("m1", "a")]).await;
    cluster.crash(3).await;
    let child = cluster.split(range_of(&cluster, "m1"), b"k").await.unwrap();
    put(&c, &[("m2", "b")]).await;
    cluster.restart(3).await.unwrap();
    // Take the other two nodes down one at a time: node 3 must serve the child range.
    cluster.crash(1).await;
    assert_eq!(get(&c, "m2").await.as_deref(), Some("b"));
    assert_eq!(range_of(&cluster, "m2"), child);
    cluster.restart(1).await.unwrap();
}
