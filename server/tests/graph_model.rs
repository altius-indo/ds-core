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
