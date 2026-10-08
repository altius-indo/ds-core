//! GQL parse errors. Unsupported syntax names the ISO/IEC 39075 feature (REQ-0003 AC3).

// reqforge: implements REQ-0003

use std::fmt;

use crate::txn::error::{DsError, ErrorCode};

/// Optional features the parser recognises and rejects by name. Each id and name must match
/// docs/gql-conformance.toml, where its status is `unsupported` or `partial` (checked by a
/// unit test).
pub const FEATURES: &[(&str, &str)] = &[
    ("G002", "Different-edges match mode"),
    ("G003", "Explicit REPEATABLE ELEMENTS keyword"),
    ("G004", "Path variables"),
    ("G006", "Graph pattern KEEP clause: path mode prefix"),
    ("G011", "Advanced path modes: TRAIL"),
    ("G012", "Advanced path modes: SIMPLE"),
    ("G013", "Advanced path modes: ACYCLIC"),
    ("G015", "All path search: explicit ALL keyword"),
    ("G016", "Any path search"),
    ("G017", "All shortest path search"),
    ("G018", "Any shortest path search"),
    ("G019", "Counted shortest path search"),
    ("G020", "Counted shortest group search"),
    ("G035", "Quantified paths"),
    ("G037", "Questioned paths"),
    ("G038", "Parenthesized path pattern expression"),
    ("G043", "Complete full edge patterns"),
    ("G045", "Complete abbreviated edge patterns"),
    ("G061", "Unbounded graph pattern quantifiers"),
    ("GA09", "Comparison of paths"),
    ("GC01", "Graph schema management"),
    ("GC04", "Graph management"),
    ("GD03", "DELETE statement: subquery support"),
    ("GE03", "Let-binding of variables in expressions"),
    ("GE06", "Path value construction"),
    ("GF04", "Enhanced path functions"),
    (
        "GF10",
        "Advanced aggregate functions: general set functions",
    ),
    ("GF11", "Advanced aggregate functions: binary set functions"),
    ("GG03", "Graph type inline specification"),
    ("GH02", "Undirected edge patterns"),
    (
        "GL05",
        "Exact number in common notation or as decimal integer with suffix",
    ),
    ("GL06", "Exact number in scientific notation with suffix"),
    ("GL09", "Optional float number suffix"),
    ("GL10", "Optional double number suffix"),
    ("GL12", "SQL datetime and interval formats"),
    ("GP01", "Inline procedure"),
    ("GP04", "Named procedure calls"),
    ("GQ02", "Composite query: OTHERWISE"),
    ("GQ03", "Composite query: UNION"),
    ("GQ04", "Composite query: EXCEPT DISTINCT"),
    ("GQ06", "Composite query: INTERSECT DISTINCT"),
    ("GQ11", "FOR statement: WITH ORDINALITY"),
    ("GQ15", "GROUP BY clause"),
    ("GQ18", "Scalar subqueries"),
    ("GQ19", "Graph pattern YIELD clause"),
    ("GQ20", "Advanced linear composition with NEXT"),
    ("GQ21", "OPTIONAL: Multiple MATCH statements"),
    ("GQ24", "FOR statement: WITH OFFSET"),
    (
        "GS03",
        "SESSION SET command: session-local value parameters",
    ),
    (
        "GS08",
        "SESSION RESET command: reset all session parameters",
    ),
    ("GT03", "Use of multiple graphs in a transaction"),
    ("GV01", "8 bit unsigned integer numbers"),
    ("GV02", "8 bit signed integer numbers"),
    ("GV03", "16 bit unsigned integer numbers"),
    ("GV04", "16 bit signed integer numbers"),
    ("GV05", "Small unsigned integer numbers"),
    ("GV06", "32 bit unsigned integer numbers"),
    ("GV07", "32 bit signed integer numbers"),
    ("GV08", "Regular unsigned integer numbers"),
    ("GV10", "Big unsigned integer numbers"),
    ("GV11", "64 bit unsigned integer numbers"),
    ("GV13", "128 bit unsigned integer numbers"),
    ("GV14", "128 bit signed integer numbers"),
    ("GV15", "256 bit unsigned integer numbers"),
    ("GV16", "256 bit signed integer numbers"),
    ("GV17", "Decimal numbers"),
    ("GV18", "Small signed integer numbers"),
    ("GV19", "Big signed integer numbers"),
    ("GV20", "16 bit floating point numbers"),
    ("GV21", "32 bit floating point numbers"),
    ("GV23", "Floating point type name synonyms"),
    ("GV25", "128 bit floating point numbers"),
    ("GV26", "256 bit floating point numbers"),
    (
        "GV39",
        "Temporal types: date, local datetime and local time support",
    ),
    (
        "GV40",
        "Temporal types: zoned datetime and zoned time support",
    ),
    ("GV41", "Temporal types: duration support"),
    ("GV55", "Path value types"),
    ("GV60", "Graph reference value types"),
    ("GV61", "Binding table reference value types"),
    ("GV65", "Dynamic union types"),
    ("GV71", "Immaterial value types: null type support"),
    ("GV72", "Immaterial value types: empty type support"),
];

pub fn feature_name(id: &str) -> &'static str {
    FEATURES
        .iter()
        .find(|(f, _)| *f == id)
        .map(|(_, n)| *n)
        .unwrap_or_else(|| panic!("feature {id} is not in gql::error::FEATURES"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    Syntax {
        at: usize,
        message: String,
    },
    /// Valid GQL that this release does not support (REQ-0003 AC3).
    Unsupported {
        at: usize,
        feature: &'static str,
        name: &'static str,
    },
    /// Not GQL at all, e.g. SQL (STORY-0010 E3).
    NotGql {
        at: usize,
        message: String,
    },
}

impl ParseError {
    pub fn at(&self) -> usize {
        match self {
            Self::Syntax { at, .. } | Self::Unsupported { at, .. } | Self::NotGql { at, .. } => *at,
        }
    }

    pub fn feature(&self) -> Option<&'static str> {
        match self {
            Self::Unsupported { feature, .. } => Some(feature),
            _ => None,
        }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Syntax { at, message } => write!(f, "syntax error at offset {at}: {message}"),
            Self::Unsupported { at, feature, name } => write!(
                f,
                "GQL feature {feature} ({name}) is not supported in this release (offset {at}); see docs/gql-conformance.toml"
            ),
            Self::NotGql { at, message } => {
                write!(f, "not a GQL statement (offset {at}): {message}")
            }
        }
    }
}

impl std::error::Error for ParseError {}

impl From<ParseError> for DsError {
    fn from(e: ParseError) -> Self {
        DsError::new(ErrorCode::Syntax, e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every feature the parser can name is listed as unsupported or partial in the published
    /// matrix, under the standard's name.
    #[test]
    fn feature_table_matches_conformance_matrix() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../docs/gql-conformance.toml");
        let text = std::fs::read_to_string(path).expect("docs/gql-conformance.toml");
        for (id, name) in FEATURES {
            let at = text
                .find(&format!("id = \"{id}\""))
                .unwrap_or_else(|| panic!("{id} is not in the matrix"));
            let entry = &text[at..text[at..].find("\n\n").map_or(text.len(), |e| at + e)];
            assert!(
                entry.contains(&format!("name = \"{name}\"")),
                "{id}: name differs from matrix:\n{entry}"
            );
            assert!(
                entry.contains("status = \"unsupported\"")
                    || entry.contains("status = \"partial\""),
                "{id} is rejected by the parser but the matrix marks it supported:\n{entry}"
            );
        }
    }
}
