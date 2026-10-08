//! STORY-0013 E1: DELETE and DETACH DELETE across shards (REQ-0029, REQ-0030, DEC-0006).
//! Exercised through the graph store; GQL `DELETE` / `DETACH DELETE` statements route to the
//! same operations once the GQL engine lands (TASK-0016).

use std::sync::Arc;
use std::time::Duration;

use dscore_server::graph::keys::{Direction, node_doc};
use dscore_server::graph::store::{Edge, Graph};
use dscore_server::graph::value::Record;
use dscore_server::txn::cluster::Cluster;
use dscore_server::txn::coordinator::{TxnClient, TxnConfig};
use dscore_server::txn::error::ErrorCode;
use openraft::Config;
use tempfile::TempDir;

// One node per range: A and D in range 1, B in range 2, C in range 3.
const A: u64 = 7;
const D: u64 = 9;
const B: u64 = (1 << 62) + 7;
const C: u64 = (3 << 62) + 7;

async fn env() -> (TxnClient, Graph, TempDir) {
    let dir = TempDir::new().unwrap();
    let config = Config {
        heartbeat_interval: 50,
        election_timeout_min: 300,
        election_timeout_max: 600,
        ..Default::default()
    };
    let s1 = node_doc(1, 1 << 62);
    let s2 = node_doc(1, 2 << 62);
    let cluster = Cluster::start(dir.path(), 3, &[&s1, &s2], config)
        .await
        .unwrap();
    let client = TxnClient::new(
        Arc::new(cluster),
        TxnConfig {
            liveness_ttl: Duration::from_millis(300),
            lock_wait: Duration::from_secs(3),
        },
    );
    let g = Graph::new(1);
    let mut t = client.begin().await.unwrap();
    for n in [A, B, C, D] {
        g.insert_node(&mut t, n, &["N"], &Record::empty())
            .await
            .unwrap();
    }
    for (src, dst, rank) in [(A, B, 0), (A, C, 0), (D, A, 0), (A, A, 0), (B, C, 1)] {
        let e = Edge {
            src,
            dst,
            edge_type: "LINK".into(),
            rank,
            properties: Record::empty(),
        };
        g.insert_edge(&mut t, &e).await.unwrap();
    }
    t.commit().await.unwrap();
    (client, g, dir)
}

async fn edges_touching(client: &TxnClient, g: &Graph, node: u64) -> usize {
    let mut t = client.begin().await.unwrap();
    let mut n = 0;
    for other in [A, B, C, D] {
        for dir in [Direction::Out, Direction::In] {
            n += g
                .edges(&mut t, other, dir, None)
                .await
                .unwrap()
                .iter()
                .filter(|e| e.src == node || e.dst == node)
                .count();
        }
    }
    n
}

// reqforge: verifies REQ-0029#AC1
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reject_connected_node_delete() {
    let (client, g, _dir) = env().await;
    let before = edges_touching(&client, &g, A).await;
    assert!(before > 0);

    let mut t = client.begin().await.unwrap();
    let err = g.delete_node(&mut t, A).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::ConstraintViolation, "{err}");
    drop(t);

    let mut t = client.begin().await.unwrap();
    assert!(
        g.get_node(&mut t, A).await.unwrap().is_some(),
        "the node must survive"
    );
    assert_eq!(
        edges_touching(&client, &g, A).await,
        before,
        "nothing may change"
    );

    // A node without edges deletes normally.
    let mut t = client.begin().await.unwrap();
    g.insert_node(&mut t, 12345, &["Lonely"], &Record::empty())
        .await
        .unwrap();
    t.commit().await.unwrap();
    let mut t = client.begin().await.unwrap();
    assert!(g.delete_node(&mut t, 12345).await.unwrap());
    t.commit().await.unwrap();
}

// reqforge: verifies REQ-0030#AC1
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn detach_atomic_across_three_shards() {
    let (client, g, _dir) = env().await;
    let mut t = client.begin().await.unwrap();
    // A->B, A->C, D->A and the self-loop A->A.
    assert_eq!(g.detach_delete_node(&mut t, A).await.unwrap(), Some(4));
    t.commit().await.unwrap();

    let mut t = client.begin().await.unwrap();
    assert!(g.get_node(&mut t, A).await.unwrap().is_none());
    assert_eq!(
        edges_touching(&client, &g, A).await,
        0,
        "no edge to the deleted node may remain"
    );
    // Unrelated edges are untouched.
    assert_eq!(
        g.edges(&mut t, B, Direction::Out, None)
            .await
            .unwrap()
            .len(),
        1
    );
}

// reqforge: verifies REQ-0030#AC1
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn detach_atomic_against_concurrent_edge_inserts() {
    let (client, g, _dir) = env().await;
    // Writers keep adding edges to A while DETACH DELETE runs; whichever commits second must
    // conflict or find A gone, so no edge to A can survive.
    let mut writers = Vec::new();
    for w in 0..4u64 {
        let (client, g) = (client.clone(), g.clone());
        writers.push(tokio::spawn(async move {
            for i in 0..20u64 {
                let mut t = client.begin().await.unwrap();
                let e = Edge {
                    src: [B, C, D][(i % 3) as usize],
                    dst: A,
                    edge_type: "LATE".into(),
                    rank: w * 100 + i,
                    properties: Record::empty(),
                };
                if g.insert_edge(&mut t, &e).await.is_ok() {
                    let _ = t.commit().await;
                }
            }
        }));
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
    loop {
        let mut t = client.begin().await.unwrap();
        g.detach_delete_node(&mut t, A).await.unwrap();
        match t.commit().await {
            Ok(_) => break,
            Err(e) => assert_eq!(e.code, ErrorCode::SerializationConflict, "{e}"),
        }
    }
    for w in writers {
        w.await.unwrap();
    }
    assert_eq!(edges_touching(&client, &g, A).await, 0);
}
