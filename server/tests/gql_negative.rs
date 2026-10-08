//! STORY-0010 E3: unsupported GQL names its feature; SQL is rejected as not GQL (REQ-0003 AC3).

use dscore_server::gql::ast::*;
use dscore_server::gql::error::ParseError;
use dscore_server::gql::parse;
use dscore_server::txn::error::{DsError, ErrorCode};

const CORPUS: &str = include_str!("gql/corpus.toml");

#[derive(serde::Deserialize)]
struct Corpus {
    case: Vec<Case>,
}

#[derive(serde::Deserialize)]
struct Case {
    feature: String,
    gql: String,
    expect: String,
}

// reqforge: verifies REQ-0003#AC3
#[test]
fn unsupported_feature_named() {
    let corpus: Corpus = toml::from_str(CORPUS).unwrap();
    let cases: Vec<_> = corpus
        .case
        .iter()
        .filter(|c| c.expect == "unsupported")
        .collect();
    assert!(cases.len() >= 40, "the negative corpus is too small");
    for c in cases {
        let err = parse(&c.gql).expect_err(&c.gql);
        let ParseError::Unsupported { feature, name, .. } = &err else {
            panic!("{}: expected a feature-named error, got {err}", c.gql);
        };
        assert_eq!(*feature, c.feature, "{}", c.gql);
        let msg = err.to_string();
        assert!(
            msg.contains(feature) && msg.contains(name),
            "message must name the feature: {msg}"
        );
        // Clients see a non-retryable syntax error, never a serialization conflict.
        let client: DsError = err.into();
        assert_eq!(client.code, ErrorCode::Syntax);
        assert!(!client.code.is_retryable());
    }
}

// reqforge: verifies REQ-0003#AC3
#[test]
fn sql_input_rejected() {
    for sql in [
        "SELECT * FROM person",
        "select name from people where id = 1",
        "INSERT INTO person (id) VALUES (1)",
        "DELETE FROM person WHERE id = 1",
        "UPDATE person SET name = 'x' WHERE id = 1",
        "CREATE TABLE person (id INT)",
    ] {
        match parse(sql) {
            Err(e @ ParseError::NotGql { .. }) => {
                assert!(e.to_string().contains("GQL"), "{e}");
                assert_eq!(DsError::from(e).code, ErrorCode::Syntax);
            }
            other => panic!("{sql}: expected a not-GQL error, got {other:?}"),
        }
    }
    // Plain syntax errors are syntax errors, with an offset.
    let e = parse("MATCH (n RETURN n").unwrap_err();
    assert!(matches!(e, ParseError::Syntax { .. }), "{e}");
    assert_eq!(e.at(), 9);
}

#[test]
fn parse_tree_shapes() {
    let Statement::Query(q) = parse("MATCH (a:Person&!Bot {id: $id})-[e:KNOWS]->{1,3}(b) WHERE b.age >= 18 RETURN b.name AS name ORDER BY name LIMIT 10").unwrap() else {
        panic!("not a query");
    };
    let Clause::Match {
        pattern, filter, ..
    } = &q.clauses[0]
    else {
        panic!()
    };
    let path = &pattern[0];
    assert_eq!(path.nodes.len(), 2);
    assert_eq!(path.edges[0].dir, Dir::Right);
    assert_eq!(path.edges[0].hops, Some((1, 3)));
    assert_eq!(
        path.nodes[0].labels,
        Some(LabelExpr::And(
            Box::new(LabelExpr::Name("Person".into())),
            Box::new(LabelExpr::Not(Box::new(LabelExpr::Name("Bot".into()))))
        ))
    );
    assert_eq!(
        path.nodes[0].props,
        vec![("id".to_string(), Expr::Param("id".into()))]
    );
    assert!(matches!(filter, Some(Expr::Binary(BinOp::Ge, _, _))));
    let Clause::Return(r) = &q.clauses[1] else {
        panic!()
    };
    assert!(matches!(r.limit, Some(Expr::Lit(_))));

    // Precedence: AND binds tighter than OR, comparison tighter than AND, * tighter than +.
    let Statement::Query(q) = parse("RETURN 1 + 2 * 3 = 7 OR FALSE AND TRUE").unwrap() else {
        panic!()
    };
    let Clause::Return(r) = &q.clauses[0] else {
        panic!()
    };
    let (e, _) = &r.items.as_ref().unwrap()[0];
    let Expr::Binary(BinOp::Or, l, rr) = e else {
        panic!("{e:?}")
    };
    assert!(matches!(**rr, Expr::Binary(BinOp::And, _, _)));
    let Expr::Binary(BinOp::Eq, sum, _) = &**l else {
        panic!()
    };
    let Expr::Binary(BinOp::Add, _, prod) = &**sum else {
        panic!()
    };
    assert!(matches!(**prod, Expr::Binary(BinOp::Mul, _, _)));

    // A RETURN ends a linear query.
    assert!(parse("MATCH (n) RETURN n MATCH (m) RETURN m").is_err());
    // Duplicate property names are rejected at parse time (REQ-0011).
    assert!(parse("INSERT (:N {a: 1, a: 2})").is_err());
}
