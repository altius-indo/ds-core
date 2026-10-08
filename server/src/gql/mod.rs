//! GQL front end (DEC-0005, design/gql-parser.md): lexer, AST, parser with feature-named
//! errors for unsupported syntax (REQ-0003).

pub mod ast;
pub mod error;
pub mod lexer;
pub mod parser;

pub use parser::parse;
