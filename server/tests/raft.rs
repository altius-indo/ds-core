//! STORY-0001: Raft group per range.
//!   E1 (`placement`): voter count and AZ placement (REQ-0001).
//!   E3 (`partition_one_voter_plus_learners_no_ack`): learners never form a quorum (REQ-0016 AC2).
//! E2 (power-cut durability) runs in dscore-harness.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use dscore_server::raft::log_store::LogEngine;
use dscore_server::raft::network::{Router, RouterNetwork};
use dscore_server::raft::placement::{
    PlacementError, StoreInfo, place_voters, validate_voter_count,
};
use dscore_server::raft::start_range;
use dscore_server::raft::state_machine::KvEngine;
use dscore_server::raft::types::{Command, NodeId, NodeInfo, Raft};
use openraft::Config;
use tempfile::TempDir;

// ---------------------------------------------------------------------------------- E1

fn store(id: NodeId, zone: &str, replicas: usize) -> StoreInfo {
    StoreInfo {
        node_id: id,
        zone: zone.into(),
        replicas,
    }
}

// reqforge: verifies REQ-0001#AC1
#[test]
fn placement_rejects_voter_counts_other_than_3_or_5() {
    for n in [0, 1, 2, 4, 6, 7] {
        assert_eq!(
            validate_voter_count(n),
            Err(PlacementError::InvalidVoterCount(n))
        );
    }
    assert!(validate_voter_count(3).is_ok());
    assert!(validate_voter_count(5).is_ok());
    let stores: Vec<_> = (1..=6).map(|i| store(i, &format!("az{i}"), 0)).collect();
    assert_eq!(
        place_voters(&stores, 4),
        Err(PlacementError::InvalidVoterCount(4))
    );
}

// reqforge: verifies REQ-0001#AC2
#[test]
fn placement_three_voters_in_three_distinct_zones() {
    // Two stores per zone across 3 zones; the busier store in each zone must be skipped.
    let stores = vec![
        store(1, "az-a", 5),
        store(2, "az-a", 1),
        store(3, "az-b", 0),
        store(4, "az-b", 9),
        store(5, "az-c", 2),
        store(6, "az-c", 3),
    ];
    let picked = place_voters(&stores, 3).unwrap();
    let zones: BTreeSet<_> = picked
        .iter()
        .map(|id| {
            stores
                .iter()
                .find(|s| s.node_id == *id)
                .unwrap()
                .zone
                .clone()
        })
        .collect();
    assert_eq!(zones.len(), 3, "two voters share a zone: {picked:?}");
    assert_eq!(
        picked.iter().copied().collect::<BTreeSet<_>>(),
        BTreeSet::from([2, 3, 5])
    );
}

// reqforge: verifies REQ-0001#AC2
#[test]
fn placement_never_puts_two_voters_in_one_zone() {
    // Only 3 zones: 3 voters fit, 5 do not, even with 9 stores available.
    let stores: Vec<_> = (1..=9)
        .map(|i| store(i, ["az-a", "az-b", "az-c"][(i % 3) as usize], 0))
        .collect();
    assert_eq!(place_voters(&stores, 3).unwrap().len(), 3);
    assert_eq!(
        place_voters(&stores, 5),
        Err(PlacementError::NotEnoughZones {
            needed: 5,
            available: 3
        })
    );
    let five: Vec<_> = (1..=5).map(|i| store(i, &format!("az{i}"), 0)).collect();
    assert_eq!(place_voters(&five, 5).unwrap().len(), 5);
}

// ---------------------------------------------------------------------------------- cluster

struct Node {
    id: NodeId,
    raft: Raft,
    kv: Arc<KvEngine>,
    _dir: TempDir,
}

fn test_config() -> Config {
    Config {
        heartbeat_interval: 50,
        election_timeout_min: 300,
        election_timeout_max: 600,
        ..Default::default()
    }
}

fn info(id: NodeId) -> NodeInfo {
    NodeInfo {
        addr: format!("node{id}"),
        zone: format!("az{}", (id - 1) % 3),
    }
}

async fn start_node(id: NodeId, router: &Arc<Router>) -> Node {
    let dir = TempDir::new().unwrap();
    let logs = LogEngine::open(&dir.path().join("raftdb")).unwrap();
    let kv = KvEngine::open(&dir.path().join("kvdb")).unwrap();
    let net = RouterNetwork {
        router: router.clone(),
        from: id,
    };
    let raft = start_range(
        id,
        1,
        Vec::new(),
        Vec::new(),
        test_config(),
        &logs,
        &kv,
        net,
    )
    .await
    .unwrap();
    router.add(id, raft.clone());
    Node {
        id,
        raft,
        kv,
        _dir: dir,
    }
}

/// Voters 1..=3 and learners 4..=`n`.
async fn cluster(n: NodeId) -> (Arc<Router>, Vec<Node>) {
    let router = Router::new();
    let mut nodes = Vec::new();
    for id in 1..=n {
        nodes.push(start_node(id, &router).await);
    }
    let voters: BTreeMap<_, _> = (1..=3).map(|id| (id, info(id))).collect();
    nodes[0].raft.initialize(voters).await.unwrap();
    let leader = wait_leader(&nodes, &[1, 2, 3]).await;
    for id in 4..=n {
        nodes[(leader - 1) as usize]
            .raft
            .add_learner(id, info(id), true)
            .await
            .unwrap();
    }
    (router, nodes)
}

async fn wait_leader(nodes: &[Node], among: &[NodeId]) -> NodeId {
    for _ in 0..200 {
        for n in nodes.iter().filter(|n| among.contains(&n.id)) {
            let m = n.raft.metrics().borrow().clone();
            if m.current_leader == Some(n.id) {
                return n.id;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("no leader among {among:?}");
}

fn put(k: &str, v: &str) -> Command {
    Command::Put {
        key: k.as_bytes().to_vec(),
        value: v.as_bytes().to_vec(),
    }
}

async fn wait_value(node: &Node, key: &str, want: Option<&str>) {
    for _ in 0..200 {
        let got = node.kv.get(key.as_bytes()).unwrap();
        if got.as_deref() == want.map(str::as_bytes) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("node {}: {key} never became {want:?}", node.id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replication_reaches_voters_and_learners() {
    let (_router, nodes) = cluster(5).await;
    let leader = wait_leader(&nodes, &[1, 2, 3]).await;
    nodes[(leader - 1) as usize]
        .raft
        .client_write(put("a", "1"))
        .await
        .unwrap();
    for n in &nodes {
        wait_value(n, "a", Some("1")).await;
    }
}

// reqforge: verifies REQ-0016#AC2
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_one_voter_plus_learners_no_ack() {
    let (router, nodes) = cluster(5).await;
    let old = wait_leader(&nodes, &[1, 2, 3]).await;
    let leader = &nodes[(old - 1) as usize];
    leader.raft.client_write(put("before", "1")).await.unwrap();

    // The leader keeps both learners but loses both other voters.
    let others: Vec<NodeId> = [1, 2, 3].into_iter().filter(|&v| v != old).collect();
    router.partition(&[&[old, 4, 5], &others]);

    // One voter plus any number of learners must not acknowledge a write.
    let attempt = tokio::time::timeout(
        Duration::from_secs(3),
        leader.raft.client_write(put("lost", "x")),
    )
    .await;
    assert!(
        !matches!(attempt, Ok(Ok(_))),
        "write acknowledged by one voter plus learners"
    );
    for n in &nodes {
        assert_eq!(
            n.kv.get(b"lost").unwrap(),
            None,
            "node {} applied an unacknowledged write",
            n.id
        );
    }

    // The two connected voters form a quorum, elect a leader and keep serving.
    let new = wait_leader(&nodes, &others).await;
    nodes[(new - 1) as usize]
        .raft
        .client_write(put("after", "2"))
        .await
        .unwrap();

    // After healing, the old leader's uncommitted entry is discarded everywhere.
    router.heal();
    for n in &nodes {
        wait_value(n, "after", Some("2")).await;
        assert_eq!(
            n.kv.get(b"lost").unwrap(),
            None,
            "node {} kept the lost write",
            n.id
        );
        assert_eq!(n.kv.get(b"before").unwrap().as_deref(), Some(&b"1"[..]));
    }
}

/// Reopen a database after `shutdown()`. openraft's tasks drop their storage handles shortly
/// after shutdown returns, and RocksDB keeps its file lock until the last handle is gone.
async fn reopen<T, E: std::fmt::Debug>(open: impl Fn() -> Result<T, E>) -> T {
    for _ in 0..200 {
        if let Ok(v) = open() {
            return v;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    open().expect("database still locked 5 s after shutdown")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn state_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let router = Router::new();
    {
        let logs = LogEngine::open(&dir.path().join("raftdb")).unwrap();
        let kv = KvEngine::open(&dir.path().join("kvdb")).unwrap();
        let net = RouterNetwork {
            router: router.clone(),
            from: 1,
        };
        let raft = start_range(1, 1, Vec::new(), Vec::new(), test_config(), &logs, &kv, net)
            .await
            .unwrap();
        raft.initialize(BTreeMap::from([(1, info(1))]))
            .await
            .unwrap();
        for _ in 0..200 {
            if raft.metrics().borrow().current_leader == Some(1) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        raft.client_write(put("k", "v")).await.unwrap();
        raft.shutdown().await.unwrap();
    }
    // Reopen both databases: the log and applied state come back, and the node leads again.
    let logs = reopen(|| LogEngine::open(&dir.path().join("raftdb"))).await;
    let kv = reopen(|| KvEngine::open(&dir.path().join("kvdb"))).await;
    assert_eq!(kv.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
    let net = RouterNetwork { router, from: 1 };
    let raft = start_range(1, 1, Vec::new(), Vec::new(), test_config(), &logs, &kv, net)
        .await
        .unwrap();
    for _ in 0..200 {
        if raft.metrics().borrow().current_leader == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    raft.client_write(put("k2", "v2")).await.unwrap();
    assert_eq!(kv.get(b"k2").unwrap().as_deref(), Some(&b"v2"[..]));
}
