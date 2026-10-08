//! STORY-0010 E2: GQL conformance suite.
//!
//!   cargo test -p dscore-server --test gql_conformance -- --matrix docs/gql-conformance.toml
//!
//! For every feature the matrix marks supported or partial, the corpus must hold at least one
//! statement that parses; every corpus statement must have its expected outcome (parse, or a
//! parse error naming its feature). Execution of supported statements joins this suite as the
//! executor lands (TASK-0018).

// reqforge: verifies REQ-0003#AC2

use std::process::ExitCode;

use dscore_server::gql::error::ParseError;
use dscore_server::gql::parse;

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

#[derive(serde::Deserialize)]
struct Matrix {
    feature: Vec<Feature>,
}

#[derive(serde::Deserialize)]
struct Feature {
    id: String,
    status: String,
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let matrix_path = args
        .iter()
        .position(|a| a == "--matrix")
        .and_then(|i| args.get(i + 1))
        .cloned()
        .unwrap_or_else(|| {
            concat!(env!("CARGO_MANIFEST_DIR"), "/../docs/gql-conformance.toml").into()
        });
    // cargo runs tests from the package directory; accept workspace-relative paths too.
    let text = std::fs::read_to_string(&matrix_path)
        .or_else(|_| {
            std::fs::read_to_string(format!("{}/../{matrix_path}", env!("CARGO_MANIFEST_DIR")))
        })
        .unwrap_or_else(|e| panic!("{matrix_path}: {e}"));
    let matrix: Matrix = toml::from_str(&text).expect("matrix parses");
    let corpus: Corpus = toml::from_str(CORPUS).expect("corpus parses");

    let mut failures = Vec::new();
    let mut passed = 0;
    for c in &corpus.case {
        let got = parse(&c.gql);
        let ok = match (c.expect.as_str(), &got) {
            ("ok", Ok(_)) => true,
            ("unsupported", Err(ParseError::Unsupported { feature, .. })) => *feature == c.feature,
            _ => false,
        };
        if ok {
            passed += 1;
        } else {
            failures.push(format!(
                "{} [{}] expected {}: {:?}\n    got {:?}",
                c.feature, c.gql, c.expect, c.gql, got
            ));
        }
    }
    for f in matrix.feature.iter().filter(|f| f.status != "unsupported") {
        if !corpus
            .case
            .iter()
            .any(|c| c.feature == f.id && c.expect == "ok")
        {
            failures.push(format!(
                "{} is {} in the matrix but has no passing corpus statement",
                f.id, f.status
            ));
        }
    }
    let supported = matrix
        .feature
        .iter()
        .filter(|f| f.status != "unsupported")
        .count();
    println!(
        "gql_conformance: {passed}/{} corpus statements as expected; {supported} supported/partial features covered",
        corpus.case.len()
    );
    for f in &failures {
        println!("FAIL {f}");
    }
    if failures.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
