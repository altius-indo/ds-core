//! GQL abstract syntax tree for the v1 subset (docs/gql-conformance.toml).

use crate::graph::value::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Query(Query),
    /// `START TRANSACTION [READ ONLY | READ WRITE]` (GT01, GT02).
    StartTransaction {
        read_only: bool,
    },
    Commit,
    Rollback,
}

/// A linear composite query: clauses applied in order.
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    /// `USE graph` (GQ01).
    pub graph: Option<String>,
    pub clauses: Vec<Clause>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Clause {
    Match {
        optional: bool,
        pattern: Vec<Path>,
        filter: Option<Expr>,
    },
    /// `FILTER expr` (GQ08).
    Filter(Expr),
    /// `LET x = expr, ...` (GQ09).
    Let(Vec<(String, Expr)>),
    /// `FOR x IN list` (GQ10).
    For {
        var: String,
        list: Expr,
    },
    Insert(Vec<Path>),
    Set(Vec<SetItem>),
    Remove(Vec<RemoveItem>),
    Delete {
        detach: bool,
        items: Vec<Expr>,
    },
    Return(Return),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Path {
    /// Alternating node, edge, node, ... (always starts and ends with a node).
    pub nodes: Vec<NodePattern>,
    pub edges: Vec<EdgePattern>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct NodePattern {
    pub var: Option<String>,
    pub labels: Option<LabelExpr>,
    pub props: Vec<(String, Expr)>,
    pub filter: Option<Expr>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dir {
    /// `-[ ]->` / `->`
    Right,
    /// `<-[ ]-` / `<-`
    Left,
    /// `-[ ]-` / `-` (either direction, G043 partial)
    Any,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EdgePattern {
    pub var: Option<String>,
    pub labels: Option<LabelExpr>,
    pub props: Vec<(String, Expr)>,
    pub filter: Option<Expr>,
    pub dir: Dir,
    /// `{m,n}` bounded quantifier (G036, G060).
    pub hops: Option<(u32, u32)>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LabelExpr {
    Name(String),
    /// `%` (G074)
    Wildcard,
    Not(Box<LabelExpr>),
    And(Box<LabelExpr>, Box<LabelExpr>),
    Or(Box<LabelExpr>, Box<LabelExpr>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetItem {
    Property {
        var: String,
        key: String,
        value: Expr,
    },
    /// `SET n = {...}`: replace all properties.
    AllProperties {
        var: String,
        value: Expr,
    },
    Label {
        var: String,
        label: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum RemoveItem {
    Property { var: String, key: String },
    Label { var: String, label: String },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Return {
    pub distinct: bool,
    /// `None` is `RETURN *`.
    pub items: Option<Vec<(Expr, Option<String>)>>,
    pub order_by: Vec<SortKey>,
    pub offset: Option<Expr>,
    pub limit: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortKey {
    pub expr: Expr,
    pub descending: bool,
    /// `NULLS FIRST` / `NULLS LAST` (GA03).
    pub nulls_first: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Or,
    Xor,
    And,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Concat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnOp {
    Not,
    Neg,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Lit(Value),
    Param(String),
    Var(String),
    Prop(Box<Expr>, String),
    List(Vec<Expr>),
    Record(Vec<(String, Expr)>),
    Unary(UnOp, Box<Expr>),
    Binary(BinOp, Box<Expr>, Box<Expr>),
    /// Function call; `name` is upper-cased. `distinct` for aggregates.
    Call {
        name: String,
        args: Vec<Expr>,
        distinct: bool,
    },
    /// `count(*)`
    CountStar,
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    /// `x IS [NOT] LABELED [label]` (G111)
    IsLabeled {
        expr: Box<Expr>,
        label: Option<LabelExpr>,
        negated: bool,
    },
    /// `e IS [NOT] DIRECTED` (G110)
    IsDirected {
        expr: Box<Expr>,
        negated: bool,
    },
    /// `n IS [NOT] SOURCE OF e` / `DESTINATION OF` (G112)
    IsEndpoint {
        node: Box<Expr>,
        edge: Box<Expr>,
        source: bool,
        negated: bool,
    },
    /// `x IS [NOT] TYPED type` (GA06)
    IsTyped {
        expr: Box<Expr>,
        ty: TypeName,
        negated: bool,
    },
    Cast {
        expr: Box<Expr>,
        ty: TypeName,
    },
    Case {
        operand: Option<Box<Expr>>,
        arms: Vec<(Expr, Expr)>,
        otherwise: Option<Box<Expr>>,
    },
    In {
        expr: Box<Expr>,
        list: Box<Expr>,
        negated: bool,
    },
    Exists(Vec<Path>),
}

/// The value types v1 stores (REQ-0009).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeName {
    Bool,
    Int64,
    Float64,
    Decimal,
    String,
    Bytes,
    ZonedDateTime,
    List,
    Record,
}
