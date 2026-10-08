//! GQL front end and executor (DEC-0005, design/gql-parser.md): lexer, AST, parser with
//! feature-named errors for unsupported syntax (REQ-0003), executor and sessions.

pub mod ast;
pub mod error;
pub mod exec;
pub mod lexer;
pub mod parser;
pub mod session;

pub use parser::parse;
