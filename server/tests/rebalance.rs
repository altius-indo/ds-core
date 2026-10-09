//! Replica moves (REQ-0033, TASK-0008): a replica moved to a new node starts from a snapshot of
//! the leader, so it holds data its range's log no longer reaches; the old replica is removed
//! and its data deleted.

use std::sync::Arc;
use std::time::Duration;

use dscore_server::txn::cluster::Cluster;
use dscore_server::txn::coordinator::{TxnClient, TxnConfig};
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn moved_split_child_serves_from_the_new_node() {
    let (c, cluster, _d) = env().await;
    put(&c, &[("m1", "a"), ("m2", "b"), ("j1", "c")]).await;
    // A split child: its log begins at the split, after "m1" and "m2" were written.
    let child = cluster
        .split(cluster.range_for(b"m1").id, b"k")
        .await
        .unwrap();
    put(&c, &[("m3", "d")]).await;

    let new = cluster.add_node().await.unwrap();
    cluster.move_replica(child, 1, new).await.unwrap();
    let replicas = cluster.replicas_of(child);
    assert!(
        replicas.contains(&new) && !replicas.contains(&1),
        "{replicas:?}"
    );

    // Reads are served by the leader: with the new node leading, every acknowledged write,
    // including those from before the split, must be in the snapshot it started from.
    cluster.make_leader(child, new).await.unwrap();
    put(&c, &[("m4", "e")]).await;
    for (k, v) in [("m1", "a"), ("m2", "b"), ("m3", "d"), ("m4", "e")] {
        assert_eq!(get(&c, k).await.as_deref(), Some(v), "{k}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removed_replica_is_deleted_and_writes_continue() {
    let (c, cluster, _d) = env().await;
    put(&c, &[("h1", "a")]).await;
    let range = cluster.range_for(b"h1").id;
    let new = cluster.add_node().await.unwrap();
    // Keep writing while the replica moves; every write must commit.
    let writer = {
        let c = c.clone();
        tokio::spawn(async move {
            for i in 0..40 {
                put(&c, &[(&format!("h{i}"), "w")]).await;
            }
        })
    };
    cluster.move_replica(range, 2, new).await.unwrap();
    writer.await.unwrap();
    assert_eq!(get(&c, "h39").await.as_deref(), Some("w"));
    // A move back starts again from a snapshot, onto a node whose old copy was deleted.
    cluster.move_replica(range, new, 2).await.unwrap();
    cluster.make_leader(range, 2).await.unwrap();
    put(&c, &[("h40", "x")]).await;
    for i in 0..40 {
        assert_eq!(
            get(&c, &format!("h{i}")).await.as_deref(),
            Some("w"),
            "h{i}"
        );
    }
}
