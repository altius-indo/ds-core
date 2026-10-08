//! `node-group`: a 3-voter Raft group in one process, driven over stdin/stdout by the
//! harness. Line protocol:
//!
//!   put KEY VALUE  ->  ack KEY            (after the leader's client_write commits)
//!                      err KEY MESSAGE
//!   get KEY        ->  val KEY VALUE | none KEY   (read on the leader after a write barrier)
//!   quit
//!
//! Prints `ready` once a leader is elected. Data for node N lives in `<dir>/nN`.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use dscore_server::raft::log_store::LogEngine;
use dscore_server::raft::network::{Router, RouterNetwork};
use dscore_server::raft::start_range;
use dscore_server::raft::state_machine::KvEngine;
use dscore_server::raft::types::{Command, NodeId, NodeInfo, Raft};
use openraft::Config;
use tokio::io::{AsyncBufReadExt, BufReader};

const VOTERS: NodeId = 3;

struct Node {
    id: NodeId,
    raft: Raft,
    kv: Arc<KvEngine>,
}

fn info(id: NodeId) -> NodeInfo {
    NodeInfo {
        addr: format!("node{id}"),
        zone: format!("az{id}"),
    }
}

fn config() -> Config {
    Config {
        heartbeat_interval: 50,
        election_timeout_min: 300,
        election_timeout_max: 600,
        ..Default::default()
    }
}

async fn leader(nodes: &[Node]) -> Result<&Node, String> {
    for _ in 0..400 {
        for n in nodes {
            if n.raft.metrics().borrow().current_leader == Some(n.id) {
                return Ok(n);
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Err("no leader".into())
}

pub async fn run(dir: &Path, unsafe_no_fsync: bool) -> Result<(), String> {
    let router = Router::new();
    let mut nodes = Vec::new();
    for id in 1..=VOTERS {
        let base = dir.join(format!("n{id}"));
        let logs = if unsafe_no_fsync {
            LogEngine::open_without_fsync(&base.join("raftdb"))
        } else {
            LogEngine::open(&base.join("raftdb"))
        }
        .map_err(|e| e.to_string())?;
        let kv = KvEngine::open(&base.join("kvdb")).map_err(|e| e.to_string())?;
        let net = RouterNetwork {
            router: router.clone(),
            from: id,
        };
        let raft = start_range(id, 1, Vec::new(), Vec::new(), config(), &logs, &kv, net)
            .await
            .map_err(|e| e.to_string())?;
        router.add(id, raft.clone());
        nodes.push(Node { id, raft, kv });
    }
    // Fails with NotAllowed once the log holds a membership, i.e. on every restart.
    let members: BTreeMap<_, _> = (1..=VOTERS).map(|id| (id, info(id))).collect();
    let _ = nodes[0].raft.initialize(members).await;
    leader(&nodes).await?;
    println!("ready");

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let mut parts = line.splitn(3, ' ');
        match (parts.next(), parts.next(), parts.next()) {
            (Some("put"), Some(key), Some(value)) => {
                let cmd = Command::Put {
                    key: key.as_bytes().to_vec(),
                    value: value.as_bytes().to_vec(),
                };
                match leader(&nodes).await?.raft.client_write(cmd).await {
                    Ok(_) => println!("ack {key}"),
                    Err(e) => println!("err {key} {e}"),
                }
            }
            (Some("get"), Some(key), None) => {
                let l = leader(&nodes).await?;
                // A committed no-op on the current leader orders this read after every prior
                // committed write, including ones replayed from the log after a restart.
                if let Err(e) = l.raft.client_write(Command::Batch(Vec::new())).await {
                    println!("err {key} {e}");
                    continue;
                }
                match l.kv.get(key.as_bytes()).map_err(|e| e.to_string())? {
                    Some(v) => println!("val {key} {}", String::from_utf8_lossy(&v)),
                    None => println!("none {key}"),
                }
            }
            (Some("quit"), None, None) => break,
            _ => println!("err ? unknown request: {line}"),
        }
    }
    for n in nodes {
        let _ = n.raft.shutdown().await;
    }
    Ok(())
}
