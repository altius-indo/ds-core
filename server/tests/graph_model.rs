//! STORY-0008 E1: typed values, duplicate property names, schemaless writes.

use dscore_server::graph::catalog::Catalog;
use dscore_server::graph::limits::Limits;
use dscore_server::graph::model::{EdgeWrite, NodeWrite, resolve_edge, resolve_node};
use dscore_server::graph::value::{Decimal128, MAX_DEPTH, Record, Timestamp, Value, ValueError};

fn record(fields: Vec<(&str, Value)>) -> Record {
    Record::new(
        fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
    )
    .unwrap()
}

fn roundtrip(v: &Value) -> Value {
    Value::decode(&v.encode()).unwrap()
}

// reqforge: verifies REQ-0009#AC1
#[test]
fn typed_values_round_trip_exactly() {
    let values = [
        Value::Int64(i64::MAX),
        Value::Int64(-i64::MAX),
        Value::Int64(i64::MIN),
        Value::Int64(0),
        Value::Decimal(Decimal128::parse("9999999999999999999999999999999999").unwrap()),
        Value::Decimal(Decimal128::parse("-9999999999999999999999999999999999").unwrap()),
        Value::Decimal(Decimal128::parse("0.1").unwrap()),
        Value::Decimal(Decimal128::parse("1.00").unwrap()),
        Value::Decimal(Decimal128::parse("1E-6176").unwrap()),
        Value::Decimal(Decimal128::parse("1E6111").unwrap()),
        Value::Double(0.1),
        Value::Double(-0.0),
        Value::Double(f64::INFINITY),
        Value::Timestamp(Timestamp::new(1_759_800_000_123_456, 330).unwrap()),
        Value::Binary(vec![0, 255, 7]),
        Value::String("ü€𝄞".into()),
        Value::Bool(true),
        Value::Null,
    ];
    for v in &values {
        let back = roundtrip(v);
        assert_eq!(&back, v, "value changed on round trip");
        assert_eq!(
            std::mem::discriminant(&back),
            std::mem::discriminant(v),
            "type changed"
        );
    }

    // 2^53 + 1 is where JSON numbers start losing integers.
    let beyond_json = Value::Int64((1 << 53) + 1);
    assert_eq!(roundtrip(&beyond_json), beyond_json);

    // Decimal scale is preserved: 1.0 and 1.00 stay distinct values.
    assert_ne!(
        Decimal128::parse("1.0").unwrap(),
        Decimal128::parse("1.00").unwrap()
    );

    // NaN does not compare equal; check the bits instead.
    let Value::Double(back) = roundtrip(&Value::Double(f64::NAN)) else {
        panic!("type changed")
    };
    assert_eq!(back.to_bits(), f64::NAN.to_bits());
}

// reqforge: verifies REQ-0009#AC2
#[test]
fn typed_values_nested_100_levels_round_trip() {
    // Alternate records and lists: records within lists within records (REQ-0006 AC1 shape).
    let mut v = Value::Int64(42);
    for depth in 0..100 {
        v = if depth % 2 == 0 {
            Value::Record(record(vec![("level", Value::Int64(depth)), ("child", v)]))
        } else {
            Value::List(vec![v, Value::String(format!("d{depth}"))])
        };
    }
    assert_eq!(roundtrip(&v), v);

    // The decoder refuses absurd nesting rather than recursing without bound.
    let mut deep = Value::Null;
    for _ in 0..=MAX_DEPTH {
        deep = Value::List(vec![deep]);
    }
    assert_eq!(Value::decode(&deep.encode()), Err(ValueError::TooDeep));
}

// reqforge: verifies REQ-0011#AC1
#[test]
fn duplicate_keys_rejected_at_every_level() {
    let dup = Record::new(vec![
        ("a".into(), Value::Int64(1)),
        ("a".into(), Value::Int64(2)),
    ]);
    assert_eq!(dup, Err(ValueError::DuplicateKey("a".into())));

    // Hand-encode a document with a duplicate nested inside a list inside a record; the
    // decoder must reject it even though no `Record::new` call was made.
    let inner_ok = record(vec![("x", Value::Int64(1)), ("y", Value::Int64(2))]);
    let doc = record(vec![("items", Value::List(vec![Value::Record(inner_ok)]))]);
    let mut bytes = doc.encode();
    let y = bytes.iter().rposition(|&b| b == b'y').unwrap();
    bytes[y] = b'x';
    assert_eq!(
        Value::decode(&bytes),
        Err(ValueError::DuplicateKey("x".into()))
    );
}

// reqforge: verifies REQ-0011#AC1
#[test]
fn duplicate_keys_write_stores_nothing() {
    // A write can only carry a `Record`, and a `Record` with duplicates cannot be built, so
    // the write fails before validation touches the catalog.
    let catalog = Catalog::default();
    let props = Record::new(vec![("k".into(), Value::Null), ("k".into(), Value::Null)]);
    assert!(props.is_err());
    assert!(catalog.labels.is_empty() && catalog.edge_types.is_empty());
}

// reqforge: verifies REQ-0008#AC1
#[test]
fn schemaless_unseen_labels_and_edge_type_need_no_ddl() {
    let catalog = Catalog::default();
    let limits = Limits::default();
    assert!(catalog.labels.is_empty() && catalog.edge_types.is_empty());

    let a = NodeWrite {
        id: 1,
        labels: vec!["Account".into(), "Premium".into(), "Account".into()],
        properties: record(vec![("name", Value::String("a".into()))]),
    };
    let b = NodeWrite {
        id: 2,
        labels: vec!["Merchant".into()],
        properties: Record::empty(),
    };
    let ra = resolve_node(&a, &catalog, &limits).unwrap();
    let rb = resolve_node(&b, &catalog, &limits).unwrap();
    assert_eq!(ra.label_ids.len(), 2, "duplicate labels collapse to a set");
    assert_eq!(catalog.labels.len(), 3);
    assert!(!rb.label_ids.iter().any(|id| ra.label_ids.contains(id)));

    let e = EdgeWrite {
        src: 1,
        dst: 2,
        edge_type: "PAID".into(),
        rank: 0,
        properties: record(vec![(
            "amount",
            Value::Decimal(Decimal128::parse("12.50").unwrap()),
        )]),
    };
    let re = resolve_edge(&e, &catalog, &limits).unwrap();
    assert_eq!(
        catalog.edge_types.name(re.edge_type_id).as_deref(),
        Some("PAID")
    );

    // Any label may link to any label: a second edge type between the same nodes, and a
    // parallel edge of the same type (rank 1), are both accepted.
    let back = EdgeWrite {
        src: 2,
        dst: 1,
        edge_type: "REFUNDED".into(),
        rank: 0,
        properties: Record::empty(),
    };
    let parallel = EdgeWrite {
        rank: 1,
        ..e.clone()
    };
    resolve_edge(&back, &catalog, &limits).unwrap();
    let rp = resolve_edge(&parallel, &catalog, &limits).unwrap();
    assert_eq!(rp.edge_type_id, re.edge_type_id);
    assert_eq!(catalog.edge_types.len(), 2);

    // The stored property bytes decode to the written record.
    assert_eq!(Record::decode(&ra.properties).unwrap(), a.properties);
}

// ------------------------------------------------------------------------------------------
// STORY-0007: nodes and edges stored transactionally on a 3-range cluster whose split points
// fall inside the node-id space, so an edge's endpoints live on different ranges.

mod stored {
    use std::time::Duration;

    use dscore_server::graph::keys::{Direction, node_doc};
    use dscore_server::graph::store::{Edge, Graph};
    use dscore_server::graph::value::{Decimal128, Record, Value};
    use dscore_server::txn::cluster::Cluster;
    use dscore_server::txn::coordinator::{TxnClient, TxnConfig};
    use dscore_server::txn::error::ErrorCode;
    use openraft::Config;
    use tempfile::TempDir;

    const A: u64 = 1; // range 1
    const B: u64 = (1 << 62) + 5; // range 2
    const C: u64 = 3 << 62; // range 3

    async fn env() -> (TxnClient, TempDir) {
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
            cluster,
            TxnConfig {
                liveness_ttl: Duration::from_millis(300),
                lock_wait: Duration::from_secs(3),
            },
        );
        (client, dir)
    }

    fn rec(fields: Vec<(&str, Value)>) -> Record {
        Record::new(
            fields
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        )
        .unwrap()
    }

    /// Records within lists within records, five levels deep (REQ-0006 AC1).
    fn nested() -> Record {
        let mut v = Value::String("leaf".into());
        for level in 0..5 {
            v = if level % 2 == 0 {
                Value::List(vec![v, Value::Int64(level)])
            } else {
                Value::Record(rec(vec![("level", Value::Int64(level)), ("child", v)]))
            };
        }
        rec(vec![
            ("profile", v),
            ("score", Value::Decimal(Decimal128::parse("99.95").unwrap())),
        ])
    }

    // reqforge: verifies REQ-0006#AC1
    // reqforge: verifies REQ-0007#AC1
    // reqforge: verifies REQ-0007#AC2
    // reqforge: verifies REQ-0010#AC1
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn node_edge_roundtrip() {
        let (client, _dir) = env().await;
        let g = Graph::new(1);

        let mut t = client.begin().await.unwrap();
        g.insert_node(&mut t, A, &["Account", "Customer", "Premium"], &nested())
            .await
            .unwrap();
        g.insert_node(&mut t, B, &["Merchant"], &Record::empty())
            .await
            .unwrap();
        g.insert_node(&mut t, C, &["Bank"], &Record::empty())
            .await
            .unwrap();
        t.commit().await.unwrap();

        // Node with 3 labels and 5-level nesting comes back unchanged.
        let mut t = client.begin().await.unwrap();
        let a = g.get_node(&mut t, A).await.unwrap().unwrap();
        assert_eq!(a.labels, vec!["Account", "Customer", "Premium"]);
        assert_eq!(a.properties, nested());

        // Two parallel edges of the same type between the same nodes, with nested properties.
        let paid = |rank: u64, amount: &str| Edge {
            src: A,
            dst: B,
            edge_type: "PAID".into(),
            rank,
            properties: rec(vec![
                ("amount", Value::Decimal(Decimal128::parse(amount).unwrap())),
                (
                    "meta",
                    Value::Record(rec(vec![(
                        "channel",
                        Value::List(vec![Value::String("card".into())]),
                    )])),
                ),
            ]),
        };
        let mut t = client.begin().await.unwrap();
        g.insert_edge(&mut t, &paid(0, "12.50")).await.unwrap();
        g.insert_edge(&mut t, &paid(1, "7.25")).await.unwrap();
        g.insert_edge(
            &mut t,
            &Edge {
                src: B,
                dst: C,
                edge_type: "BANKS_WITH".into(),
                rank: 0,
                properties: Record::empty(),
            },
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        let mut t = client.begin().await.unwrap();
        let from_a = g
            .edges(&mut t, A, Direction::Out, Some("PAID"))
            .await
            .unwrap();
        let into_b = g
            .edges(&mut t, B, Direction::In, Some("PAID"))
            .await
            .unwrap();
        assert_eq!(
            from_a,
            vec![paid(0, "12.50"), paid(1, "7.25")],
            "both parallel edges, unchanged"
        );
        assert_eq!(
            into_b, from_a,
            "traversal from either endpoint sees identical edges"
        );
        assert_eq!(
            g.edges(&mut t, B, Direction::Out, None)
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(
            g.edges(&mut t, A, Direction::In, None)
                .await
                .unwrap()
                .is_empty()
        );

        // Updating and deleting change both entries together.
        let mut t = client.begin().await.unwrap();
        g.set_edge_properties(&mut t, &paid(0, "13.00"))
            .await
            .unwrap();
        assert!(g.delete_edge(&mut t, A, "PAID", B, 1).await.unwrap());
        t.commit().await.unwrap();
        let mut t = client.begin().await.unwrap();
        assert_eq!(
            g.edges(&mut t, A, Direction::Out, Some("PAID"))
                .await
                .unwrap(),
            vec![paid(0, "13.00")]
        );
        assert_eq!(
            g.edges(&mut t, B, Direction::In, Some("PAID"))
                .await
                .unwrap(),
            vec![paid(0, "13.00")]
        );
    }

    // reqforge: verifies REQ-0006#AC2
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn duplicate_node_id_rejected() {
        let (client, _dir) = env().await;
        let g = Graph::new(1);
        let mut t = client.begin().await.unwrap();
        g.insert_node(&mut t, B, &["First"], &Record::empty())
            .await
            .unwrap();
        t.commit().await.unwrap();

        let mut t = client.begin().await.unwrap();
        let err = g
            .insert_node(&mut t, B, &["Second"], &Record::empty())
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::ConstraintViolation, "{err}");

        let mut t = client.begin().await.unwrap();
        assert_eq!(
            g.get_node(&mut t, B).await.unwrap().unwrap().labels,
            vec!["First"]
        );

        // An edge to a missing node is refused rather than left dangling.
        let mut t = client.begin().await.unwrap();
        let err = g
            .insert_edge(
                &mut t,
                &Edge {
                    src: B,
                    dst: 424242,
                    edge_type: "X".into(),
                    rank: 0,
                    properties: Record::empty(),
                },
            )
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::ConstraintViolation);
    }
}
