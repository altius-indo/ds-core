//! GQL statements executed end to end through a session on a 3-range cluster.

use std::collections::BTreeMap;
use std::time::Duration;

use dscore_server::gql::session::Session;
use dscore_server::graph::keys::node_doc;
use dscore_server::graph::store::Graph;
use dscore_server::graph::value::{Decimal128, Value};
use dscore_server::txn::cluster::Cluster;
use dscore_server::txn::coordinator::{TxnClient, TxnConfig};
use dscore_server::txn::error::ErrorCode;
use openraft::Config;
use tempfile::TempDir;

async fn session() -> (Session, TxnClient, TempDir) {
    let dir = TempDir::new().unwrap();
    let config = Config {
        heartbeat_interval: 50,
        election_timeout_min: 300,
        election_timeout_max: 600,
        ..Default::default()
    };
    // GQL node ids are non-negative INT64, so split inside [0, 2^63).
    let s1 = node_doc(1, 1 << 61);
    let s2 = node_doc(1, 1 << 62);
    let cluster = Cluster::start(dir.path(), 3, &[&s1, &s2], config)
        .await
        .unwrap();
    let client = TxnClient::new(
        cluster,
        TxnConfig {
            liveness_ttl: Duration::from_millis(300),
            lock_wait: Duration::from_secs(3),
        },
    );
    (Session::new(client.clone(), Graph::new(1)), client, dir)
}

fn no_params() -> BTreeMap<String, Value> {
    BTreeMap::new()
}

async fn rows(s: &mut Session, gql: &str) -> Vec<Vec<Value>> {
    s.execute(gql, &no_params())
        .await
        .unwrap_or_else(|e| panic!("{gql}: {e}"))
        .rows
}

fn int(i: i64) -> Value {
    Value::Int64(i)
}

fn s(x: &str) -> Value {
    Value::String(x.into())
}

/// Node ids on three different ranges.
const A: i64 = 1;
const B: i64 = (1 << 61) + 1;
const C: i64 = (1 << 62) + 1;

async fn people(sess: &mut Session) {
    rows(sess, &format!("INSERT (:Person {{id: {A}, name: 'Ada', born: 1815}}), (:Person {{id: {B}, name: 'Babbage', born: 1791}}), (:Person&Admin {{id: {C}, name: 'Carl'}})")).await;
    rows(
        sess,
        &format!(
            "MATCH (a {{id: {A}}}), (b {{id: {B}}}), (c {{id: {C}}}) INSERT (a)-[:KNOWS {{since: 1833}}]->(b), (a)-[:KNOWS]->(c), (b)-[:KNOWS]->(c)"
        ),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn point_read_by_identifier() {
    let (mut sess, _c, _d) = session().await;
    people(&mut sess).await;
    let mut params = BTreeMap::new();
    params.insert("id".to_string(), int(B));
    let r = sess
        .execute("MATCH (n {id: $id}) RETURN n.name, n.born", &params)
        .await
        .unwrap();
    assert_eq!(r.columns, vec!["n.name", "n.born"]);
    assert_eq!(r.rows, vec![vec![s("Babbage"), int(1791)]]);
    assert!(
        rows(&mut sess, "MATCH (n {id: 424242}) RETURN n")
            .await
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_hop_and_bounded_hops_across_ranges() {
    let (mut sess, _c, _d) = session().await;
    people(&mut sess).await;
    assert_eq!(
        rows(
            &mut sess,
            &format!("MATCH (a {{id: {A}}})-[e:KNOWS]->(b) RETURN b.name ORDER BY b.name")
        )
        .await,
        vec![vec![s("Babbage")], vec![s("Carl")]]
    );
    // Reverse direction from the target.
    assert_eq!(
        rows(
            &mut sess,
            &format!("MATCH (c {{id: {C}}})<-[:KNOWS]-(x) RETURN x.name ORDER BY x.name")
        )
        .await,
        vec![vec![s("Ada")], vec![s("Babbage")]]
    );
    // Edge properties and the identical view from both endpoints.
    assert_eq!(
        rows(
            &mut sess,
            &format!("MATCH (a {{id: {A}}})-[e:KNOWS]->(b {{id: {B}}}) RETURN e.since")
        )
        .await,
        vec![vec![int(1833)]]
    );
    // {1,2} hops from Ada reaches Babbage, Carl (1 hop) and Carl again via Babbage (2 hops).
    assert_eq!(
        rows(
            &mut sess,
            &format!("MATCH (a {{id: {A}}})-[:KNOWS]->{{1,2}}(x) RETURN x.name ORDER BY x.name")
        )
        .await,
        vec![vec![s("Babbage")], vec![s("Carl")], vec![s("Carl")]]
    );
    assert_eq!(
        rows(
            &mut sess,
            &format!(
                "MATCH (a {{id: {A}}})-[:KNOWS]->{{1,2}}(x) RETURN DISTINCT x.name ORDER BY x.name"
            )
        )
        .await,
        vec![vec![s("Babbage")], vec![s("Carl")]]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn labels_filters_and_ordering() {
    let (mut sess, _c, _d) = session().await;
    people(&mut sess).await;
    assert_eq!(
        rows(&mut sess, "MATCH (n:Person&Admin) RETURN n.name").await,
        vec![vec![s("Carl")]]
    );
    assert_eq!(
        rows(
            &mut sess,
            "MATCH (n:Person) WHERE n.born < 1800 OR n.born IS NULL RETURN n.name ORDER BY n.name"
        )
        .await,
        vec![vec![s("Babbage")], vec![s("Carl")]]
    );
    // NULLS FIRST / LAST (GA03), OFFSET / LIMIT.
    assert_eq!(
        rows(
            &mut sess,
            "MATCH (n:Person) RETURN n.born ORDER BY n.born NULLS FIRST"
        )
        .await,
        vec![vec![Value::Null], vec![int(1791)], vec![int(1815)]]
    );
    assert_eq!(
        rows(
            &mut sess,
            "MATCH (n:Person) RETURN n.born ORDER BY n.born DESC NULLS LAST OFFSET 1 LIMIT 1"
        )
        .await,
        vec![vec![int(1791)]]
    );
    assert_eq!(
        rows(&mut sess, "MATCH (n:Person) FILTER n.born IS NOT NULL LET age = 2026 - n.born RETURN n.name, age ORDER BY age").await,
        vec![vec![s("Ada"), int(211)], vec![s("Babbage"), int(235)]]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aggregates_and_expressions() {
    let (mut sess, _c, _d) = session().await;
    people(&mut sess).await;
    assert_eq!(
        rows(
            &mut sess,
            "MATCH (n:Person) RETURN COUNT(*), COUNT(n.born), MIN(n.born), MAX(n.born), SUM(n.born)"
        )
        .await,
        vec![vec![int(3), int(2), int(1791), int(1815), int(3606)]]
    );
    let err = sess
        .execute("MATCH (n:Person) RETURN n.name, COUNT(*)", &no_params())
        .await
        .unwrap_err();
    assert!(err.message.contains("GQ15"), "{err}");
    let r = rows(&mut sess, "RETURN 1 + 2 * 3, 12.50 + 0.25, 7 / 2, 'a' || 'b', CAST('9' AS INT64) + 1, UPPER('x'), 0x10").await;
    assert_eq!(
        r,
        vec![vec![
            int(7),
            Value::Decimal(Decimal128::parse("12.75").unwrap()),
            int(3),
            s("ab"),
            int(10),
            s("X"),
            int(16)
        ]]
    );
    let err = sess
        .execute("RETURN 9223372036854775807 + 1", &no_params())
        .await
        .unwrap_err();
    assert_eq!(
        err.code,
        ErrorCode::ConstraintViolation,
        "overflow is an error, not a wrap: {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_remove_and_parallel_edges() {
    let (mut sess, _c, _d) = session().await;
    people(&mut sess).await;
    rows(
        &mut sess,
        &format!("MATCH (n {{id: {A}}}) SET n.born = 1816, n:Mathematician REMOVE n:Person"),
    )
    .await;
    assert_eq!(
        rows(&mut sess, &format!("MATCH (n {{id: {A}}}) RETURN n.born, n IS LABELED Mathematician, n IS LABELED Person")).await,
        vec![vec![int(1816), Value::Bool(true), Value::Bool(false)]]
    );
    // A second KNOWS edge between the same nodes is a parallel edge with the next rank.
    rows(
        &mut sess,
        &format!("MATCH (a {{id: {A}}}), (b {{id: {B}}}) INSERT (a)-[:KNOWS {{since: 1840}}]->(b)"),
    )
    .await;
    assert_eq!(
        rows(
            &mut sess,
            &format!(
                "MATCH (a {{id: {A}}})-[e:KNOWS]->(b {{id: {B}}}) RETURN e.since ORDER BY e.since"
            )
        )
        .await,
        vec![vec![int(1833)], vec![int(1840)]]
    );
    rows(
        &mut sess,
        &format!("MATCH (a {{id: {A}}})-[e:KNOWS {{since: 1840}}]->(b) SET e.since = 1841"),
    )
    .await;
    assert_eq!(
        rows(
            &mut sess,
            &format!("MATCH (b {{id: {B}}})<-[e:KNOWS]-(a) RETURN e.since ORDER BY e.since")
        )
        .await,
        vec![vec![int(1833)], vec![int(1841)]],
        "the change is visible from the target's in-edges too"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gql_delete_and_detach_delete() {
    let (mut sess, _c, _d) = session().await;
    people(&mut sess).await;
    let err = sess
        .execute(&format!("MATCH (n {{id: {C}}}) DELETE n"), &no_params())
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::ConstraintViolation, "{err}");
    assert_eq!(
        rows(&mut sess, "MATCH (n:Person) RETURN COUNT(*)").await,
        vec![vec![int(3)]]
    );
    rows(&mut sess, &format!("MATCH (n {{id: {C}}}) DETACH DELETE n")).await;
    assert_eq!(
        rows(&mut sess, "MATCH (n:Person) RETURN COUNT(*)").await,
        vec![vec![int(2)]]
    );
    assert_eq!(
        rows(&mut sess, "MATCH (a)-[e]->(b) RETURN COUNT(*)").await,
        vec![vec![int(1)]]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_transactions_and_batch_insert() {
    let (mut sess, _c, _d) = session().await;
    rows(&mut sess, "START TRANSACTION").await;
    rows(&mut sess, "FOR i IN [10, 11, 12] INSERT (:Item {id: i})").await;
    rows(&mut sess, "ROLLBACK").await;
    assert_eq!(
        rows(&mut sess, "MATCH (n:Item) RETURN COUNT(*)").await,
        vec![vec![int(0)]]
    );

    rows(&mut sess, "START TRANSACTION").await;
    rows(&mut sess, "FOR i IN [10, 11, 12] INSERT (:Item {id: i})").await;
    assert_eq!(
        rows(&mut sess, "MATCH (n:Item) RETURN COUNT(*)").await,
        vec![vec![int(3)]],
        "own writes visible"
    );
    rows(&mut sess, "COMMIT").await;
    assert_eq!(
        rows(&mut sess, "MATCH (n:Item) RETURN COUNT(*)").await,
        vec![vec![int(3)]]
    );

    rows(&mut sess, "START TRANSACTION READ ONLY").await;
    let err = sess
        .execute("INSERT (:Item {id: 13})", &no_params())
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::InvalidConfiguration);
    assert!(
        !sess.in_transaction(),
        "the read-only violation rolled the transaction back"
    );
    let err = sess
        .execute("INSERT (:Item {id: 10})", &no_params())
        .await
        .unwrap_err();
    assert_eq!(
        err.code,
        ErrorCode::ConstraintViolation,
        "duplicate node id"
    );
}
