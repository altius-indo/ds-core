//! GQL executor: runs a parsed query against the graph store inside one serializable
//! transaction.
//!
//! Node identity: an integer `id` property in a node pattern is the node identifier, so
//! `MATCH (n {id: $id})` is a single-key point read (REQ-0022) and `INSERT (:N {id: 5})`
//! creates node 5; nodes inserted without an `id` get a random 64-bit identifier
//! (design/raft-ranges.md §9). Each edge expansion is one adjacency prefix scan (REQ-0023).

// reqforge: implements REQ-0003
// reqforge: implements REQ-0022
// reqforge: implements REQ-0023

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::{SystemTime, UNIX_EPOCH};

use super::ast::*;
use crate::graph::keys::{Direction, NodeId};
use crate::graph::store::{Edge, Graph, Node};
use crate::graph::value::{Decimal128, Record, Timestamp, Value};
use crate::txn::coordinator::Txn;
use crate::txn::error::{DsError, ErrorCode};

/// The property that doubles as a node's identifier.
pub const ID_PROPERTY: &str = "id";

#[derive(Debug, Clone, PartialEq)]
pub enum Binding {
    Node(Node),
    Edge(Edge),
    /// A bounded variable-length edge (`-[e]->{m,n}`) binds the list of edges it walked.
    Edges(Vec<Edge>),
    Value(Value),
}

type Row = BTreeMap<String, Binding>;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    /// Nodes and edges created, changed or deleted.
    pub mutations: usize,
}

fn err(code: ErrorCode, msg: impl Into<String>) -> DsError {
    DsError::new(code, msg)
}

fn semantic(msg: impl Into<String>) -> DsError {
    err(ErrorCode::Syntax, msg)
}

fn data(msg: impl Into<String>) -> DsError {
    err(ErrorCode::ConstraintViolation, msg)
}

pub fn node_value(n: &Node) -> Value {
    let rec = Record::new(vec![
        ("id".into(), Value::Int64(n.id as i64)),
        (
            "labels".into(),
            Value::List(n.labels.iter().cloned().map(Value::String).collect()),
        ),
        ("properties".into(), Value::Record(n.properties.clone())),
    ])
    .expect("fixed field names");
    Value::Record(rec)
}

pub fn edge_value(e: &Edge) -> Value {
    let rec = Record::new(vec![
        ("source".into(), Value::Int64(e.src as i64)),
        ("target".into(), Value::Int64(e.dst as i64)),
        ("type".into(), Value::String(e.edge_type.clone())),
        ("rank".into(), Value::Int64(e.rank as i64)),
        ("properties".into(), Value::Record(e.properties.clone())),
    ])
    .expect("fixed field names");
    Value::Record(rec)
}

fn binding_value(b: &Binding) -> Value {
    match b {
        Binding::Node(n) => node_value(n),
        Binding::Edge(e) => edge_value(e),
        Binding::Edges(es) => Value::List(es.iter().map(edge_value).collect()),
        Binding::Value(v) => v.clone(),
    }
}

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

/// A fresh random node identifier (splitmix64 over time and a counter), below 2^63 so GQL's
/// signed INT64 can express it.
fn random_node_id() -> NodeId {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);
    let mut z = nanos ^ NEXT_ID.fetch_add(0x9E37_79B9_7F4A_7C15, AtomicOrdering::Relaxed);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    (z ^ (z >> 31)) & (u64::MAX >> 1)
}

// ------------------------------------------------------------------------------ values

fn truthy(v: &Value) -> Option<bool> {
    match v {
        Value::Bool(b) => Some(*b),
        _ => None,
    }
}

fn dec_from_i64(i: i64) -> Decimal128 {
    Decimal128::new(i as i128, 0).expect("int64 fits decimal128")
}

fn dec_to_f64(d: Decimal128) -> f64 {
    d.coefficient() as f64 * 10f64.powi(d.exponent() as i32)
}

/// Align two decimals to the smaller exponent; None if that overflows i128.
fn align(a: Decimal128, b: Decimal128) -> Option<(i128, i128, i16)> {
    let e = a.exponent().min(b.exponent());
    let scale = |d: Decimal128| -> Option<i128> {
        let shift = (d.exponent() - e) as u32;
        d.coefficient().checked_mul(10i128.checked_pow(shift)?)
    };
    Some((scale(a)?, scale(b)?, e))
}

enum Num {
    I(i64),
    D(Decimal128),
    F(f64),
}

fn num(v: &Value) -> Option<Num> {
    match v {
        Value::Int64(i) => Some(Num::I(*i)),
        Value::Decimal(d) => Some(Num::D(*d)),
        Value::Double(f) => Some(Num::F(*f)),
        _ => None,
    }
}

fn as_f64(n: &Num) -> f64 {
    match n {
        Num::I(i) => *i as f64,
        Num::D(d) => dec_to_f64(*d),
        Num::F(f) => *f,
    }
}

fn as_dec(n: &Num) -> Option<Decimal128> {
    match n {
        Num::I(i) => Some(dec_from_i64(*i)),
        Num::D(d) => Some(*d),
        Num::F(_) => None,
    }
}

/// Total order used by ORDER BY and comparisons; None when the values are not comparable.
pub fn compare(a: &Value, b: &Value) -> Option<Ordering> {
    if let (Some(x), Some(y)) = (num(a), num(b)) {
        return match (&x, &y) {
            (Num::I(p), Num::I(q)) => Some(p.cmp(q)),
            (Num::F(_), _) | (_, Num::F(_)) => as_f64(&x).partial_cmp(&as_f64(&y)),
            _ => {
                let (p, q) = (as_dec(&x)?, as_dec(&y)?);
                match align(p, q) {
                    Some((p, q, _)) => Some(p.cmp(&q)),
                    None => dec_to_f64(p).partial_cmp(&dec_to_f64(q)),
                }
            }
        };
    }
    match (a, b) {
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        (Value::Bool(x), Value::Bool(y)) => Some(x.cmp(y)),
        (Value::Binary(x), Value::Binary(y)) => Some(x.cmp(y)),
        (Value::Timestamp(x), Value::Timestamp(y)) => Some(x.micros_utc.cmp(&y.micros_utc)),
        (Value::List(x), Value::List(y)) => {
            for (p, q) in x.iter().zip(y) {
                match compare(p, q)? {
                    Ordering::Equal => continue,
                    o => return Some(o),
                }
            }
            Some(x.len().cmp(&y.len()))
        }
        (Value::Record(x), Value::Record(y)) => (x == y).then_some(Ordering::Equal),
        _ => None,
    }
}

fn arith(op: BinOp, a: &Value, b: &Value) -> Result<Value, DsError> {
    if matches!(a, Value::Null) || matches!(b, Value::Null) {
        return Ok(Value::Null);
    }
    let (Some(x), Some(y)) = (num(a), num(b)) else {
        return Err(data(format!("cannot apply {op:?} to {a:?} and {b:?}")));
    };
    let overflow = || data("int64 arithmetic overflow");
    Ok(match (&x, &y) {
        (Num::I(p), Num::I(q)) => match op {
            BinOp::Add => Value::Int64(p.checked_add(*q).ok_or_else(overflow)?),
            BinOp::Sub => Value::Int64(p.checked_sub(*q).ok_or_else(overflow)?),
            BinOp::Mul => Value::Int64(p.checked_mul(*q).ok_or_else(overflow)?),
            BinOp::Div if *q == 0 => return Err(data("division by zero")),
            BinOp::Div => Value::Int64(p.checked_div(*q).ok_or_else(overflow)?),
            BinOp::Mod if *q == 0 => return Err(data("division by zero")),
            BinOp::Mod => Value::Int64(p.checked_rem(*q).ok_or_else(overflow)?),
            _ => unreachable!(),
        },
        (Num::F(_), _) | (_, Num::F(_)) => {
            let (p, q) = (as_f64(&x), as_f64(&y));
            Value::Double(match op {
                BinOp::Add => p + q,
                BinOp::Sub => p - q,
                BinOp::Mul => p * q,
                BinOp::Div => p / q,
                BinOp::Mod => p % q,
                _ => unreachable!(),
            })
        }
        _ => {
            let (p, q) = (
                as_dec(&x).expect("not float"),
                as_dec(&y).expect("not float"),
            );
            let exact = match op {
                BinOp::Add | BinOp::Sub => align(p, q).and_then(|(m, n, e)| {
                    let c = if op == BinOp::Add {
                        m.checked_add(n)?
                    } else {
                        m.checked_sub(n)?
                    };
                    Decimal128::new(c, e).ok()
                }),
                BinOp::Mul => p
                    .coefficient()
                    .checked_mul(q.coefficient())
                    .and_then(|c| Decimal128::new(c, p.exponent().checked_add(q.exponent())?).ok()),
                _ => None,
            };
            match exact {
                Some(d) => Value::Decimal(d),
                None => {
                    // Division and anything beyond decimal128 precision: approximate.
                    let (pf, qf) = (dec_to_f64(p), dec_to_f64(q));
                    if op == BinOp::Div && qf == 0.0 {
                        return Err(data("division by zero"));
                    }
                    Value::Double(match op {
                        BinOp::Add => pf + qf,
                        BinOp::Sub => pf - qf,
                        BinOp::Mul => pf * qf,
                        BinOp::Div => pf / qf,
                        BinOp::Mod => pf % qf,
                        _ => unreachable!(),
                    })
                }
            }
        }
    })
}

fn label_match(expr: &LabelExpr, labels: &[String]) -> bool {
    match expr {
        LabelExpr::Name(n) => labels.iter().any(|l| l == n),
        LabelExpr::Wildcard => !labels.is_empty(),
        LabelExpr::Not(e) => !label_match(e, labels),
        LabelExpr::And(a, b) => label_match(a, labels) && label_match(b, labels),
        LabelExpr::Or(a, b) => label_match(a, labels) || label_match(b, labels),
    }
}

/// Labels named by an INSERT label expression (`:A&B` or `:A`).
fn insert_labels(expr: &Option<LabelExpr>) -> Result<Vec<String>, DsError> {
    fn walk(e: &LabelExpr, out: &mut Vec<String>) -> Result<(), DsError> {
        match e {
            LabelExpr::Name(n) => {
                out.push(n.clone());
                Ok(())
            }
            LabelExpr::And(a, b) => {
                walk(a, out)?;
                walk(b, out)
            }
            _ => Err(semantic("INSERT takes label names joined with &")),
        }
    }
    let mut out = Vec::new();
    if let Some(e) = expr {
        walk(e, &mut out)?;
    }
    Ok(out)
}

fn is_aggregate(e: &Expr) -> bool {
    matches!(e, Expr::CountStar)
        || matches!(e, Expr::Call { name, .. } if matches!(name.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"))
}

fn expr_name(e: &Expr) -> String {
    match e {
        Expr::Var(v) => v.clone(),
        Expr::Prop(base, key) => format!("{}.{key}", expr_name(base)),
        Expr::Call { name, .. } => name.to_ascii_lowercase(),
        Expr::CountStar => "count".into(),
        _ => "expr".into(),
    }
}

// ------------------------------------------------------------------------------ executor

pub struct Executor<'a> {
    graph: &'a Graph,
    params: &'a BTreeMap<String, Value>,
    node_cache: HashMap<NodeId, Option<Node>>,
    all_nodes: Option<Vec<Node>>,
    mutations: usize,
}

impl<'a> Executor<'a> {
    pub fn new(graph: &'a Graph, params: &'a BTreeMap<String, Value>) -> Self {
        Self {
            graph,
            params,
            node_cache: HashMap::new(),
            all_nodes: None,
            mutations: 0,
        }
    }

    async fn node(&mut self, txn: &mut Txn, id: NodeId) -> Result<Option<Node>, DsError> {
        if let Some(n) = self.node_cache.get(&id) {
            return Ok(n.clone());
        }
        let n = self.graph.get_node(txn, id).await?;
        self.node_cache.insert(id, n.clone());
        Ok(n)
    }

    fn invalidate(&mut self, id: NodeId) {
        self.node_cache.remove(&id);
        self.all_nodes = None;
    }

    pub async fn run(&mut self, txn: &mut Txn, q: &Query) -> Result<QueryResult, DsError> {
        if let Some(g) = &q.graph
            && g != "default"
            && g.parse::<u32>().ok() != Some(self.graph.id)
        {
            return Err(semantic(format!("unknown graph `{g}`")));
        }
        let mut rows: Vec<Row> = vec![Row::new()];
        for clause in &q.clauses {
            match clause {
                Clause::Match {
                    optional,
                    pattern,
                    filter,
                } => {
                    let mut out = Vec::new();
                    for row in rows {
                        let mut matched = vec![row.clone()];
                        for path in pattern {
                            let mut next = Vec::new();
                            for r in matched {
                                next.extend(self.match_path(txn, path, r).await?);
                            }
                            matched = next;
                        }
                        if let Some(f) = filter {
                            let mut kept = Vec::new();
                            for r in matched {
                                if truthy(&self.eval(txn, f, &r).await?) == Some(true) {
                                    kept.push(r);
                                }
                            }
                            matched = kept;
                        }
                        if matched.is_empty() && *optional {
                            let mut r = row;
                            for path in pattern {
                                for v in path
                                    .nodes
                                    .iter()
                                    .filter_map(|n| n.var.clone())
                                    .chain(path.edges.iter().filter_map(|e| e.var.clone()))
                                {
                                    r.entry(v).or_insert(Binding::Value(Value::Null));
                                }
                            }
                            out.push(r);
                        } else {
                            out.extend(matched);
                        }
                    }
                    rows = out;
                }
                Clause::Filter(f) => {
                    let mut kept = Vec::new();
                    for r in rows {
                        if truthy(&self.eval(txn, f, &r).await?) == Some(true) {
                            kept.push(r);
                        }
                    }
                    rows = kept;
                }
                Clause::Let(binds) => {
                    for r in &mut rows {
                        for (var, e) in binds {
                            let v = self.eval(txn, e, r).await?;
                            r.insert(var.clone(), Binding::Value(v));
                        }
                    }
                }
                Clause::For { var, list } => {
                    let mut out = Vec::new();
                    for r in rows {
                        match self.eval(txn, list, &r).await? {
                            Value::List(items) => {
                                for item in items {
                                    let mut r2 = r.clone();
                                    r2.insert(var.clone(), Binding::Value(item));
                                    out.push(r2);
                                }
                            }
                            Value::Null => {}
                            other => return Err(data(format!("FOR needs a list, got {other:?}"))),
                        }
                    }
                    rows = out;
                }
                Clause::Insert(paths) => {
                    for r in &mut rows {
                        for p in paths {
                            self.insert_path(txn, p, r).await?;
                        }
                    }
                }
                Clause::Set(items) => {
                    for r in &rows {
                        for item in items {
                            self.set_item(txn, item, r).await?;
                        }
                    }
                }
                Clause::Remove(items) => {
                    for r in &rows {
                        for item in items {
                            self.remove_item(txn, item, r).await?;
                        }
                    }
                }
                Clause::Delete { detach, items } => {
                    let mut done = BTreeSet::new();
                    for r in &rows {
                        for item in items {
                            let Expr::Var(v) = item else { continue };
                            match r.get(v) {
                                Some(Binding::Node(n))
                                    if done.insert(("n", n.id, String::new(), 0, 0)) =>
                                {
                                    if *detach {
                                        self.graph.detach_delete_node(txn, n.id).await?;
                                    } else {
                                        self.graph.delete_node(txn, n.id).await?;
                                    }
                                    self.invalidate(n.id);
                                    self.mutations += 1;
                                }
                                Some(Binding::Edge(e))
                                    if done.insert((
                                        "e",
                                        e.src,
                                        e.edge_type.clone(),
                                        e.dst,
                                        e.rank,
                                    )) =>
                                {
                                    if self
                                        .graph
                                        .delete_edge(txn, e.src, &e.edge_type, e.dst, e.rank)
                                        .await?
                                    {
                                        self.mutations += 1;
                                    }
                                }
                                Some(Binding::Value(Value::Null)) | Some(_) => {}
                                None => {
                                    return Err(semantic(format!("DELETE: unbound variable `{v}`")));
                                }
                            }
                        }
                    }
                }
                Clause::Return(r) => return self.project(txn, r, rows).await,
            }
        }
        Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            mutations: self.mutations,
        })
    }

    // -------------------------------------------------------------------------- MATCH

    async fn node_ok(
        &mut self,
        txn: &mut Txn,
        pat: &NodePattern,
        n: &Node,
        row: &Row,
    ) -> Result<bool, DsError> {
        if let Some(l) = &pat.labels
            && !label_match(l, &n.labels)
        {
            return Ok(false);
        }
        for (k, e) in &pat.props {
            let want = self.eval(txn, e, row).await?;
            let have = if k == ID_PROPERTY && n.properties.get(k).is_none() {
                Value::Int64(n.id as i64)
            } else {
                n.properties.get(k).cloned().unwrap_or(Value::Null)
            };
            if compare(&want, &have) != Some(Ordering::Equal) {
                return Ok(false);
            }
        }
        if let Some(var) = &pat.var
            && let Some(Binding::Node(bound)) = row.get(var)
            && bound.id != n.id
        {
            return Ok(false);
        }
        if let Some(f) = &pat.filter {
            let mut r = row.clone();
            if let Some(v) = &pat.var {
                r.insert(v.clone(), Binding::Node(n.clone()));
            }
            if truthy(&self.eval(txn, f, &r).await?) != Some(true) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn edge_ok(
        &mut self,
        txn: &mut Txn,
        pat: &EdgePattern,
        e: &Edge,
        row: &Row,
    ) -> Result<bool, DsError> {
        if let Some(l) = &pat.labels
            && !label_match(l, std::slice::from_ref(&e.edge_type))
        {
            return Ok(false);
        }
        for (k, ex) in &pat.props {
            let want = self.eval(txn, ex, row).await?;
            let have = e.properties.get(k).cloned().unwrap_or(Value::Null);
            if compare(&want, &have) != Some(Ordering::Equal) {
                return Ok(false);
            }
        }
        if let Some(f) = &pat.filter {
            let mut r = row.clone();
            if let Some(v) = &pat.var {
                r.insert(v.clone(), Binding::Edge(e.clone()));
            }
            if truthy(&self.eval(txn, f, &r).await?) != Some(true) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Candidates for a path's first node: bound variable, id point lookup, or full scan.
    async fn start_nodes(
        &mut self,
        txn: &mut Txn,
        pat: &NodePattern,
        row: &Row,
    ) -> Result<Vec<Node>, DsError> {
        if let Some(v) = &pat.var {
            match row.get(v) {
                Some(Binding::Node(n)) => return Ok(vec![n.clone()]),
                Some(Binding::Value(Value::Null)) => return Ok(Vec::new()),
                Some(_) => return Err(semantic(format!("`{v}` is not a node"))),
                None => {}
            }
        }
        if let Some((_, e)) = pat.props.iter().find(|(k, _)| k == ID_PROPERTY) {
            // Point read by identifier (REQ-0022).
            return Ok(match self.eval(txn, e, row).await? {
                Value::Int64(id) => self.node(txn, id as NodeId).await?.into_iter().collect(),
                _ => Vec::new(),
            });
        }
        if self.all_nodes.is_none() {
            self.all_nodes = Some(self.graph.scan_nodes(txn).await?);
        }
        Ok(self.all_nodes.clone().unwrap_or_default())
    }

    /// One hop from `at` along `pat`: (edge, neighbour id) pairs, one adjacency scan per
    /// direction (REQ-0023).
    async fn hop(
        &mut self,
        txn: &mut Txn,
        at: NodeId,
        pat: &EdgePattern,
    ) -> Result<Vec<(Edge, NodeId)>, DsError> {
        let etype = match &pat.labels {
            Some(LabelExpr::Name(n)) => Some(n.clone()),
            _ => None,
        };
        let mut out = Vec::new();
        if matches!(pat.dir, Dir::Right | Dir::Any) {
            for e in self
                .graph
                .edges(txn, at, Direction::Out, etype.as_deref())
                .await?
            {
                let other = e.dst;
                out.push((e, other));
            }
        }
        if matches!(pat.dir, Dir::Left | Dir::Any) {
            for e in self
                .graph
                .edges(txn, at, Direction::In, etype.as_deref())
                .await?
            {
                // A self-loop already came back from the out-scan when matching either way.
                if pat.dir == Dir::Any && e.src == e.dst {
                    continue;
                }
                let other = e.src;
                out.push((e, other));
            }
        }
        Ok(out)
    }

    fn match_path<'b>(
        &'b mut self,
        txn: &'b mut Txn,
        path: &'b Path,
        row: Row,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<Row>, DsError>> + Send + 'b>>
    {
        Box::pin(async move {
            let mut out = Vec::new();
            for start in self.start_nodes(txn, &path.nodes[0], &row).await? {
                if !self.node_ok(txn, &path.nodes[0], &start, &row).await? {
                    continue;
                }
                let mut r = row.clone();
                if let Some(v) = &path.nodes[0].var {
                    r.insert(v.clone(), Binding::Node(start.clone()));
                }
                self.extend(txn, path, 0, start, r, &mut out).await?;
            }
            Ok(out)
        })
    }

    fn extend<'b>(
        &'b mut self,
        txn: &'b mut Txn,
        path: &'b Path,
        i: usize,
        at: Node,
        row: Row,
        out: &'b mut Vec<Row>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), DsError>> + Send + 'b>> {
        Box::pin(async move {
            if i == path.edges.len() {
                out.push(row);
                return Ok(());
            }
            let epat = &path.edges[i];
            let npat = &path.nodes[i + 1];
            let (lo, hi) = epat.hops.unwrap_or((1, 1));
            // Frontier of walks: (current node, edges walked so far).
            let mut frontier: Vec<(NodeId, Vec<Edge>)> = vec![(at.id, Vec::new())];
            for depth in 1..=hi.max(1) {
                let mut next = Vec::new();
                for (node, walked) in &frontier {
                    for (e, other) in self.hop(txn, *node, epat).await? {
                        if !self.edge_ok(txn, epat, &e, &row).await? {
                            continue;
                        }
                        let mut w = walked.clone();
                        w.push(e);
                        next.push((other, w));
                    }
                }
                if depth >= lo {
                    for (other, walked) in &next {
                        let Some(n) = self.node(txn, *other).await? else {
                            continue;
                        };
                        if !self.node_ok(txn, npat, &n, &row).await? {
                            continue;
                        }
                        let mut r = row.clone();
                        if let Some(v) = &epat.var {
                            let b = if epat.hops.is_some() {
                                Binding::Edges(walked.clone())
                            } else {
                                Binding::Edge(walked[0].clone())
                            };
                            if let Some(bound) = r.get(v)
                                && *bound != b
                            {
                                continue;
                            }
                            r.insert(v.clone(), b);
                        }
                        if let Some(v) = &npat.var {
                            r.insert(v.clone(), Binding::Node(n.clone()));
                        }
                        self.extend(txn, path, i + 1, n, r, out).await?;
                    }
                }
                frontier = next;
                if frontier.is_empty() {
                    break;
                }
            }
            if lo == 0 {
                // Zero hops: the next node is this node.
                if self.node_ok(txn, npat, &at, &row).await? {
                    let mut r = row.clone();
                    if let Some(v) = &epat.var {
                        r.insert(v.clone(), Binding::Edges(Vec::new()));
                    }
                    if let Some(v) = &npat.var {
                        r.insert(v.clone(), Binding::Node(at.clone()));
                    }
                    self.extend(txn, path, i + 1, at, r, out).await?;
                }
            }
            Ok(())
        })
    }

    // -------------------------------------------------------------------------- updates

    async fn props(
        &mut self,
        txn: &mut Txn,
        props: &[(String, Expr)],
        row: &Row,
    ) -> Result<Record, DsError> {
        let mut fields = Vec::with_capacity(props.len());
        for (k, e) in props {
            let v = self.eval(txn, e, row).await?;
            if !matches!(v, Value::Null) {
                fields.push((k.clone(), v));
            }
        }
        Record::new(fields).map_err(|e| data(e.to_string()))
    }

    async fn insert_path(
        &mut self,
        txn: &mut Txn,
        path: &Path,
        row: &mut Row,
    ) -> Result<(), DsError> {
        let mut ids = Vec::with_capacity(path.nodes.len());
        for npat in &path.nodes {
            if let Some(v) = &npat.var
                && let Some(Binding::Node(n)) = row.get(v)
            {
                ids.push(n.id);
                continue;
            }
            let props = self.props(txn, &npat.props, row).await?;
            let id = match props.get(ID_PROPERTY) {
                Some(Value::Int64(i)) if *i >= 0 => *i as NodeId,
                Some(other) => {
                    return Err(data(format!(
                        "node `id` must be a non-negative INT64, got {other:?}"
                    )));
                }
                None => random_node_id(),
            };
            let labels = insert_labels(&npat.labels)?;
            let label_refs: Vec<&str> = labels.iter().map(String::as_str).collect();
            self.graph.insert_node(txn, id, &label_refs, &props).await?;
            self.invalidate(id);
            self.mutations += 1;
            let node = Node {
                id,
                labels,
                properties: props,
            };
            if let Some(v) = &npat.var {
                row.insert(v.clone(), Binding::Node(node));
            }
            ids.push(id);
        }
        for (i, epat) in path.edges.iter().enumerate() {
            let Some(LabelExpr::Name(etype)) = &epat.labels else {
                return Err(semantic(
                    "INSERT edges need exactly one type, e.g. -[:KNOWS]->",
                ));
            };
            if epat.hops.is_some() {
                return Err(semantic("INSERT edges cannot have quantifiers"));
            }
            let (src, dst) = match epat.dir {
                Dir::Right => (ids[i], ids[i + 1]),
                Dir::Left => (ids[i + 1], ids[i]),
                Dir::Any => return Err(semantic("INSERT edges need a direction (-> or <-)")),
            };
            let props = self.props(txn, &epat.props, row).await?;
            // Parallel edges get the next free rank (REQ-0007).
            let rank = self
                .graph
                .edges(txn, src, Direction::Out, Some(etype))
                .await?
                .iter()
                .filter(|e| e.dst == dst)
                .map(|e| e.rank + 1)
                .max()
                .unwrap_or(0);
            let edge = Edge {
                src,
                dst,
                edge_type: etype.clone(),
                rank,
                properties: props,
            };
            self.graph.insert_edge(txn, &edge).await?;
            self.mutations += 1;
            if let Some(v) = &epat.var {
                row.insert(v.clone(), Binding::Edge(edge));
            }
        }
        Ok(())
    }

    async fn set_item(&mut self, txn: &mut Txn, item: &SetItem, row: &Row) -> Result<(), DsError> {
        let var = match item {
            SetItem::Property { var, .. }
            | SetItem::AllProperties { var, .. }
            | SetItem::Label { var, .. } => var,
        };
        match row.get(var) {
            Some(Binding::Node(n)) => {
                let mut node = self
                    .node(txn, n.id)
                    .await?
                    .ok_or_else(|| data(format!("node {} no longer exists", n.id)))?;
                match item {
                    SetItem::Property { key, value, .. } => {
                        let v = self.eval(txn, value, row).await?;
                        node.properties = with_field(&node.properties, key, v)?;
                    }
                    SetItem::AllProperties { value, .. } => match self.eval(txn, value, row).await?
                    {
                        Value::Record(r) => node.properties = r,
                        other => {
                            return Err(data(format!("SET {var} = needs a record, got {other:?}")));
                        }
                    },
                    SetItem::Label { label, .. } => {
                        if !node.labels.contains(label) {
                            node.labels.push(label.clone());
                        }
                    }
                }
                self.graph
                    .update_node(txn, node.id, &node.labels, &node.properties)
                    .await?;
                self.invalidate(node.id);
                self.mutations += 1;
                Ok(())
            }
            Some(Binding::Edge(e)) => {
                let mut edge = e.clone();
                match item {
                    SetItem::Property { key, value, .. } => {
                        let v = self.eval(txn, value, row).await?;
                        edge.properties = with_field(&edge.properties, key, v)?;
                    }
                    SetItem::AllProperties { value, .. } => match self.eval(txn, value, row).await?
                    {
                        Value::Record(r) => edge.properties = r,
                        other => {
                            return Err(data(format!("SET {var} = needs a record, got {other:?}")));
                        }
                    },
                    SetItem::Label { .. } => {
                        return Err(semantic("edges have exactly one type; it cannot be SET"));
                    }
                }
                self.graph.set_edge_properties(txn, &edge).await?;
                self.mutations += 1;
                Ok(())
            }
            Some(Binding::Value(Value::Null)) => Ok(()),
            _ => Err(semantic(format!("SET: `{var}` is not a node or edge"))),
        }
    }

    async fn remove_item(
        &mut self,
        txn: &mut Txn,
        item: &RemoveItem,
        row: &Row,
    ) -> Result<(), DsError> {
        let var = match item {
            RemoveItem::Property { var, .. } | RemoveItem::Label { var, .. } => var,
        };
        match row.get(var) {
            Some(Binding::Node(n)) => {
                let mut node = self
                    .node(txn, n.id)
                    .await?
                    .ok_or_else(|| data(format!("node {} no longer exists", n.id)))?;
                match item {
                    RemoveItem::Property { key, .. } => {
                        node.properties = without_field(&node.properties, key)
                    }
                    RemoveItem::Label { label, .. } => node.labels.retain(|l| l != label),
                }
                self.graph
                    .update_node(txn, node.id, &node.labels, &node.properties)
                    .await?;
                self.invalidate(node.id);
                self.mutations += 1;
                Ok(())
            }
            Some(Binding::Edge(e)) => match item {
                RemoveItem::Property { key, .. } => {
                    let mut edge = e.clone();
                    edge.properties = without_field(&edge.properties, key);
                    self.graph.set_edge_properties(txn, &edge).await?;
                    self.mutations += 1;
                    Ok(())
                }
                RemoveItem::Label { .. } => Err(semantic("an edge's type cannot be removed")),
            },
            Some(Binding::Value(Value::Null)) => Ok(()),
            _ => Err(semantic(format!("REMOVE: `{var}` is not a node or edge"))),
        }
    }

    // -------------------------------------------------------------------------- RETURN

    async fn project(
        &mut self,
        txn: &mut Txn,
        r: &Return,
        rows: Vec<Row>,
    ) -> Result<QueryResult, DsError> {
        let items: Vec<(Expr, String)> = match &r.items {
            Some(items) => items
                .iter()
                .map(|(e, alias)| (e.clone(), alias.clone().unwrap_or_else(|| expr_name(e))))
                .collect(),
            None => {
                let vars: BTreeSet<String> = rows.iter().flat_map(|r| r.keys().cloned()).collect();
                vars.into_iter()
                    .map(|v| (Expr::Var(v.clone()), v))
                    .collect()
            }
        };
        let columns: Vec<String> = items.iter().map(|(_, n)| n.clone()).collect();
        let aggregates = items.iter().filter(|(e, _)| is_aggregate(e)).count();
        let mut out: Vec<(Vec<Value>, Row)> = Vec::new();
        if aggregates > 0 {
            if aggregates != items.len() {
                return Err(semantic(
                    "mixing aggregates and non-aggregates needs GROUP BY (GQ15), which this release does not support",
                ));
            }
            let mut vals = Vec::new();
            for (e, _) in &items {
                vals.push(self.aggregate(txn, e, &rows).await?);
            }
            out.push((vals, Row::new()));
        } else {
            for row in rows {
                let mut vals = Vec::with_capacity(items.len());
                for (e, _) in &items {
                    vals.push(self.eval(txn, e, &row).await?);
                }
                let mut ext = row;
                for (v, c) in vals.iter().zip(&columns) {
                    ext.entry(c.clone()).or_insert(Binding::Value(v.clone()));
                }
                out.push((vals, ext));
            }
        }
        if r.distinct {
            let mut seen: Vec<Vec<Value>> = Vec::new();
            out.retain(|(v, _)| {
                if seen.contains(v) {
                    false
                } else {
                    seen.push(v.clone());
                    true
                }
            });
        }
        if !r.order_by.is_empty() {
            let mut keyed = Vec::with_capacity(out.len());
            for (vals, row) in out {
                let mut keys = Vec::with_capacity(r.order_by.len());
                for k in &r.order_by {
                    keys.push(self.eval(txn, &k.expr, &row).await?);
                }
                keyed.push((keys, vals, row));
            }
            keyed.sort_by(|a, b| {
                for (i, k) in r.order_by.iter().enumerate() {
                    let (x, y) = (&a.0[i], &b.0[i]);
                    // Nulls sort last ascending and first descending unless stated (GA03).
                    let nulls_first = k.nulls_first.unwrap_or(k.descending);
                    let o = match (matches!(x, Value::Null), matches!(y, Value::Null)) {
                        (true, true) => Ordering::Equal,
                        (true, false) => {
                            if nulls_first {
                                Ordering::Less
                            } else {
                                Ordering::Greater
                            }
                        }
                        (false, true) => {
                            if nulls_first {
                                Ordering::Greater
                            } else {
                                Ordering::Less
                            }
                        }
                        _ => {
                            let o = compare(x, y).unwrap_or(Ordering::Equal);
                            if k.descending { o.reverse() } else { o }
                        }
                    };
                    if o != Ordering::Equal {
                        return o;
                    }
                }
                Ordering::Equal
            });
            out = keyed.into_iter().map(|(_, v, r)| (v, r)).collect();
        }
        let empty = Row::new();
        let count = |v: Value| -> Result<usize, DsError> {
            match v {
                Value::Int64(n) if n >= 0 => Ok(n as usize),
                other => Err(data(format!(
                    "OFFSET/LIMIT need a non-negative integer, got {other:?}"
                ))),
            }
        };
        let offset = match &r.offset {
            Some(e) => count(self.eval(txn, e, &empty).await?)?,
            None => 0,
        };
        let limit = match &r.limit {
            Some(e) => count(self.eval(txn, e, &empty).await?)?,
            None => usize::MAX,
        };
        let rows = out
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|(v, _)| v)
            .collect();
        Ok(QueryResult {
            columns,
            rows,
            mutations: self.mutations,
        })
    }

    async fn aggregate(&mut self, txn: &mut Txn, e: &Expr, rows: &[Row]) -> Result<Value, DsError> {
        if matches!(e, Expr::CountStar) {
            return Ok(Value::Int64(rows.len() as i64));
        }
        let Expr::Call {
            name,
            args,
            distinct,
        } = e
        else {
            unreachable!()
        };
        let arg = args
            .first()
            .ok_or_else(|| semantic(format!("{name} needs an argument")))?;
        let mut vals = Vec::new();
        for r in rows {
            let v = self.eval(txn, arg, r).await?;
            if !matches!(v, Value::Null) && (!*distinct || !vals.contains(&v)) {
                vals.push(v);
            }
        }
        Ok(match name.as_str() {
            "COUNT" => Value::Int64(vals.len() as i64),
            "MIN" | "MAX" => {
                let mut best: Option<Value> = None;
                for v in vals {
                    best = match best {
                        None => Some(v),
                        Some(b) => {
                            let o = compare(&v, &b)
                                .ok_or_else(|| data("MIN/MAX over incomparable values"))?;
                            let take = if name == "MIN" {
                                o == Ordering::Less
                            } else {
                                o == Ordering::Greater
                            };
                            Some(if take { v } else { b })
                        }
                    };
                }
                best.unwrap_or(Value::Null)
            }
            "SUM" | "AVG" => {
                if vals.is_empty() {
                    return Ok(Value::Null);
                }
                let n = vals.len();
                let mut acc = Value::Int64(0);
                for v in vals {
                    acc = arith(BinOp::Add, &acc, &v)?;
                }
                if name == "AVG" {
                    arith(
                        BinOp::Div,
                        &Value::Double(as_f64(&num(&acc).expect("numeric sum"))),
                        &Value::Int64(n as i64),
                    )?
                } else {
                    acc
                }
            }
            _ => unreachable!(),
        })
    }

    // -------------------------------------------------------------------------- expressions

    fn eval<'b>(
        &'b mut self,
        txn: &'b mut Txn,
        e: &'b Expr,
        row: &'b Row,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, DsError>> + Send + 'b>>
    {
        Box::pin(async move {
            Ok(match e {
                Expr::Lit(v) => v.clone(),
                Expr::Param(p) => self
                    .params
                    .get(p)
                    .cloned()
                    .ok_or_else(|| semantic(format!("missing parameter ${p}")))?,
                Expr::Var(v) => match row.get(v) {
                    Some(b) => binding_value(b),
                    None => return Err(semantic(format!("unbound variable `{v}`"))),
                },
                Expr::Prop(base, key) => {
                    if let Expr::Var(v) = &**base {
                        match row.get(v) {
                            Some(Binding::Node(n)) => {
                                return Ok(n.properties.get(key).cloned().unwrap_or(
                                    if key == ID_PROPERTY {
                                        Value::Int64(n.id as i64)
                                    } else {
                                        Value::Null
                                    },
                                ));
                            }
                            Some(Binding::Edge(ed)) => {
                                return Ok(ed.properties.get(key).cloned().unwrap_or(Value::Null));
                            }
                            _ => {}
                        }
                    }
                    match self.eval(txn, base, row).await? {
                        Value::Record(r) => r.get(key).cloned().unwrap_or(Value::Null),
                        Value::Null => Value::Null,
                        other => return Err(data(format!("{other:?} has no property `{key}`"))),
                    }
                }
                Expr::List(items) => {
                    let mut out = Vec::with_capacity(items.len());
                    for i in items {
                        out.push(self.eval(txn, i, row).await?);
                    }
                    Value::List(out)
                }
                Expr::Record(fields) => {
                    let mut out = Vec::with_capacity(fields.len());
                    for (k, v) in fields {
                        out.push((k.clone(), self.eval(txn, v, row).await?));
                    }
                    Value::Record(Record::new(out).map_err(|e| data(e.to_string()))?)
                }
                Expr::Unary(op, x) => {
                    let v = self.eval(txn, x, row).await?;
                    match (op, v) {
                        (_, Value::Null) => Value::Null,
                        (UnOp::Not, Value::Bool(b)) => Value::Bool(!b),
                        (UnOp::Neg, Value::Int64(i)) => {
                            Value::Int64(i.checked_neg().ok_or_else(|| data("int64 overflow"))?)
                        }
                        (UnOp::Neg, Value::Double(f)) => Value::Double(-f),
                        (UnOp::Neg, Value::Decimal(d)) => Value::Decimal(
                            Decimal128::new(-d.coefficient(), d.exponent())
                                .map_err(|e| data(e.to_string()))?,
                        ),
                        (op, v) => return Err(data(format!("cannot apply {op:?} to {v:?}"))),
                    }
                }
                Expr::Binary(op, a, b) => {
                    let x = self.eval(txn, a, row).await?;
                    // Short-circuit three-valued logic.
                    match op {
                        BinOp::And if truthy(&x) == Some(false) => return Ok(Value::Bool(false)),
                        BinOp::Or if truthy(&x) == Some(true) => return Ok(Value::Bool(true)),
                        _ => {}
                    }
                    let y = self.eval(txn, b, row).await?;
                    match op {
                        BinOp::And | BinOp::Or | BinOp::Xor => match (truthy(&x), truthy(&y), op) {
                            (Some(p), Some(q), BinOp::And) => Value::Bool(p && q),
                            (Some(p), Some(q), BinOp::Or) => Value::Bool(p || q),
                            (Some(p), Some(q), _) => Value::Bool(p != q),
                            (_, Some(false), BinOp::And) => Value::Bool(false),
                            (_, Some(true), BinOp::Or) => Value::Bool(true),
                            _ => Value::Null,
                        },
                        BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge => {
                            if matches!(x, Value::Null) || matches!(y, Value::Null) {
                                Value::Null
                            } else {
                                match compare(&x, &y) {
                                    Some(o) => Value::Bool(match op {
                                        BinOp::Eq => o == Ordering::Equal,
                                        BinOp::Ne => o != Ordering::Equal,
                                        BinOp::Lt => o == Ordering::Less,
                                        BinOp::Le => o != Ordering::Greater,
                                        BinOp::Gt => o == Ordering::Greater,
                                        _ => o != Ordering::Less,
                                    }),
                                    None if *op == BinOp::Eq => Value::Bool(false),
                                    None if *op == BinOp::Ne => Value::Bool(true),
                                    None => {
                                        return Err(data(format!("cannot compare {x:?} and {y:?}")));
                                    }
                                }
                            }
                        }
                        BinOp::Concat => match (x, y) {
                            (Value::Null, _) | (_, Value::Null) => Value::Null,
                            (Value::String(p), Value::String(q)) => Value::String(p + &q),
                            (Value::List(mut p), Value::List(q)) => {
                                p.extend(q);
                                Value::List(p)
                            }
                            (Value::Binary(mut p), Value::Binary(q)) => {
                                p.extend(q);
                                Value::Binary(p)
                            }
                            (p, q) => {
                                return Err(data(format!("cannot concatenate {p:?} and {q:?}")));
                            }
                        },
                        _ => arith(*op, &x, &y)?,
                    }
                }
                Expr::Call { name, args, .. } => {
                    if is_aggregate(e) {
                        return Err(semantic(format!(
                            "{name} is an aggregate; use it in RETURN"
                        )));
                    }
                    if name == "ELEMENT_ID" {
                        return Ok(match args.first() {
                            Some(Expr::Var(v)) => match row.get(v) {
                                Some(Binding::Node(n)) => Value::Int64(n.id as i64),
                                Some(Binding::Edge(ed)) => Value::String(format!(
                                    "{}-{}-{}-{}",
                                    ed.src, ed.edge_type, ed.dst, ed.rank
                                )),
                                _ => Value::Null,
                            },
                            _ => return Err(semantic("ELEMENT_ID takes an element variable")),
                        });
                    }
                    if name == "PROPERTY_EXISTS" {
                        let (Some(Expr::Var(v)), Some(Expr::Var(k))) = (args.first(), args.get(1))
                        else {
                            return Err(semantic("PROPERTY_EXISTS(element, property_name)"));
                        };
                        return Ok(Value::Bool(match row.get(v) {
                            Some(Binding::Node(n)) => n.properties.get(k).is_some(),
                            Some(Binding::Edge(ed)) => ed.properties.get(k).is_some(),
                            _ => false,
                        }));
                    }
                    if name == "SAME" || name == "ALL_DIFFERENT" {
                        let mut ids = Vec::new();
                        for a in args {
                            ids.push(self.eval(txn, a, row).await?);
                        }
                        let distinct = ids
                            .iter()
                            .enumerate()
                            .all(|(i, x)| ids[..i].iter().all(|y| y != x));
                        let same = ids.windows(2).all(|w| w[0] == w[1]);
                        return Ok(Value::Bool(if name == "SAME" { same } else { distinct }));
                    }
                    let mut vals = Vec::with_capacity(args.len());
                    for a in args {
                        vals.push(self.eval(txn, a, row).await?);
                    }
                    call(name, &vals)?
                }
                Expr::CountStar => {
                    return Err(semantic("count(*) is an aggregate; use it in RETURN"));
                }
                Expr::IsNull { expr, negated } => {
                    let v = self.eval(txn, expr, row).await?;
                    Value::Bool(matches!(v, Value::Null) != *negated)
                }
                Expr::IsLabeled {
                    expr,
                    label,
                    negated,
                } => {
                    let labels = match &**expr {
                        Expr::Var(v) => match row.get(v) {
                            Some(Binding::Node(n)) => n.labels.clone(),
                            Some(Binding::Edge(ed)) => vec![ed.edge_type.clone()],
                            _ => return Ok(Value::Null),
                        },
                        _ => return Err(semantic("IS LABELED takes an element variable")),
                    };
                    let m = match label {
                        Some(l) => label_match(l, &labels),
                        None => !labels.is_empty(),
                    };
                    Value::Bool(m != *negated)
                }
                Expr::IsDirected { expr, negated } => match &**expr {
                    Expr::Var(v) if matches!(row.get(v), Some(Binding::Edge(_))) => {
                        Value::Bool(!*negated)
                    }
                    _ => return Err(semantic("IS DIRECTED takes an edge variable")),
                },
                Expr::IsEndpoint {
                    node,
                    edge,
                    source,
                    negated,
                } => {
                    let (Expr::Var(nv), Expr::Var(ev)) = (&**node, &**edge) else {
                        return Err(semantic("IS SOURCE/DESTINATION OF takes element variables"));
                    };
                    match (row.get(nv), row.get(ev)) {
                        (Some(Binding::Node(n)), Some(Binding::Edge(ed))) => {
                            let hit = if *source {
                                ed.src == n.id
                            } else {
                                ed.dst == n.id
                            };
                            Value::Bool(hit != *negated)
                        }
                        _ => Value::Null,
                    }
                }
                Expr::IsTyped { expr, ty, negated } => {
                    let v = self.eval(txn, expr, row).await?;
                    let is = matches!(
                        (ty, &v),
                        (TypeName::Bool, Value::Bool(_))
                            | (TypeName::Int64, Value::Int64(_))
                            | (TypeName::Float64, Value::Double(_))
                            | (TypeName::Decimal, Value::Decimal(_))
                            | (TypeName::String, Value::String(_))
                            | (TypeName::Bytes, Value::Binary(_))
                            | (TypeName::ZonedDateTime, Value::Timestamp(_))
                            | (TypeName::List, Value::List(_))
                            | (TypeName::Record, Value::Record(_))
                    );
                    Value::Bool(is != *negated)
                }
                Expr::Cast { expr, ty } => cast(self.eval(txn, expr, row).await?, *ty)?,
                Expr::Case {
                    operand,
                    arms,
                    otherwise,
                } => {
                    let subject = match operand {
                        Some(o) => Some(self.eval(txn, o, row).await?),
                        None => None,
                    };
                    for (w, t) in arms {
                        let hit = match &subject {
                            Some(s) => {
                                compare(s, &self.eval(txn, w, row).await?) == Some(Ordering::Equal)
                            }
                            None => truthy(&self.eval(txn, w, row).await?) == Some(true),
                        };
                        if hit {
                            return self.eval(txn, t, row).await;
                        }
                    }
                    match otherwise {
                        Some(o) => self.eval(txn, o, row).await?,
                        None => Value::Null,
                    }
                }
                Expr::In {
                    expr,
                    list,
                    negated,
                } => {
                    let v = self.eval(txn, expr, row).await?;
                    match self.eval(txn, list, row).await? {
                        Value::List(items) => {
                            if matches!(v, Value::Null) {
                                Value::Null
                            } else {
                                let found = items
                                    .iter()
                                    .any(|i| compare(i, &v) == Some(Ordering::Equal));
                                Value::Bool(found != *negated)
                            }
                        }
                        Value::Null => Value::Null,
                        other => return Err(data(format!("IN needs a list, got {other:?}"))),
                    }
                }
                Expr::Exists(paths) => {
                    let mut rows = vec![row.clone()];
                    for p in paths {
                        let mut next = Vec::new();
                        for r in rows {
                            next.extend(self.match_path(txn, p, r).await?);
                        }
                        rows = next;
                    }
                    Value::Bool(!rows.is_empty())
                }
            })
        })
    }
}

fn with_field(r: &Record, key: &str, v: Value) -> Result<Record, DsError> {
    let mut fields: Vec<(String, Value)> = r
        .fields()
        .iter()
        .filter(|(k, _)| k != key)
        .cloned()
        .collect();
    if !matches!(v, Value::Null) {
        fields.push((key.to_string(), v));
    }
    Record::new(fields).map_err(|e| data(e.to_string()))
}

fn without_field(r: &Record, key: &str) -> Record {
    Record::new(
        r.fields()
            .iter()
            .filter(|(k, _)| k != key)
            .cloned()
            .collect(),
    )
    .expect("subset of unique fields")
}

fn cast(v: Value, ty: TypeName) -> Result<Value, DsError> {
    let bad = |v: &Value| data(format!("cannot cast {v:?} to {ty:?}"));
    Ok(match (ty, v) {
        (_, Value::Null) => Value::Null,
        (TypeName::Int64, Value::Int64(i)) => Value::Int64(i),
        (TypeName::Int64, Value::Double(f)) if f.is_finite() && f.abs() < 9.2e18 => {
            Value::Int64(f.trunc() as i64)
        }
        (TypeName::Int64, Value::Decimal(d)) => Value::Int64(dec_to_f64(d).trunc() as i64),
        (TypeName::Int64, Value::String(s)) => Value::Int64(
            s.trim()
                .parse()
                .map_err(|_| data(format!("'{s}' is not an INT64")))?,
        ),
        (TypeName::Float64, v @ (Value::Int64(_) | Value::Decimal(_) | Value::Double(_))) => {
            Value::Double(as_f64(&num(&v).expect("numeric")))
        }
        (TypeName::Float64, Value::String(s)) => Value::Double(
            s.trim()
                .parse()
                .map_err(|_| data(format!("'{s}' is not a FLOAT64")))?,
        ),
        (TypeName::Decimal, Value::Int64(i)) => Value::Decimal(dec_from_i64(i)),
        (TypeName::Decimal, Value::Decimal(d)) => Value::Decimal(d),
        (TypeName::Decimal, Value::String(s)) => {
            Value::Decimal(Decimal128::parse(s.trim()).map_err(|e| data(e.to_string()))?)
        }
        (TypeName::Decimal, Value::Double(f)) => {
            Value::Decimal(Decimal128::parse(&format!("{f}")).map_err(|e| data(e.to_string()))?)
        }
        (TypeName::String, Value::String(s)) => Value::String(s),
        (TypeName::String, Value::Int64(i)) => Value::String(i.to_string()),
        (TypeName::String, Value::Double(f)) => Value::String(f.to_string()),
        (TypeName::String, Value::Bool(b)) => Value::String(b.to_string().to_uppercase()),
        (TypeName::String, Value::Decimal(d)) => Value::String(d.to_string()),
        (TypeName::Bool, Value::Bool(b)) => Value::Bool(b),
        (TypeName::Bool, Value::String(s)) => match s.to_ascii_uppercase().as_str() {
            "TRUE" => Value::Bool(true),
            "FALSE" => Value::Bool(false),
            _ => return Err(data(format!("'{s}' is not a BOOL"))),
        },
        (TypeName::Bytes, Value::Binary(b)) => Value::Binary(b),
        (TypeName::Bytes, Value::String(s)) => Value::Binary(s.into_bytes()),
        (TypeName::ZonedDateTime, Value::Timestamp(t)) => Value::Timestamp(t),
        (TypeName::ZonedDateTime, Value::Int64(micros)) => {
            Value::Timestamp(Timestamp::new(micros, 0).map_err(|e| data(e.to_string()))?)
        }
        (TypeName::List, v @ Value::List(_)) => v,
        (TypeName::Record, v @ Value::Record(_)) => v,
        (_, v) => return Err(bad(&v)),
    })
}

fn f64_arg(v: &Value) -> Result<f64, DsError> {
    num(v)
        .map(|n| as_f64(&n))
        .ok_or_else(|| data(format!("expected a number, got {v:?}")))
}

fn trim(s: &str, chars: &str, leading: bool, trailing: bool) -> String {
    let set: Vec<char> = chars.chars().collect();
    let mut out = s;
    if leading {
        out = out.trim_start_matches(|c| set.contains(&c));
    }
    if trailing {
        out = out.trim_end_matches(|c| set.contains(&c));
    }
    out.to_string()
}

fn trim_bytes(b: &[u8], set: &[u8]) -> Vec<u8> {
    let start = b.iter().position(|x| !set.contains(x)).unwrap_or(b.len());
    let end = b
        .iter()
        .rposition(|x| !set.contains(x))
        .map_or(start, |e| e + 1);
    b[start..end.max(start)].to_vec()
}

/// Scalar functions (GF01–GF03, GF05–GF07, GF12, GF13 and the mandatory core).
fn call(name: &str, a: &[Value]) -> Result<Value, DsError> {
    if a.iter().any(|v| matches!(v, Value::Null)) && name != "COALESCE" {
        return Ok(Value::Null);
    }
    let arity = |n: usize| -> Result<(), DsError> {
        if a.len() == n {
            Ok(())
        } else {
            Err(semantic(format!(
                "{name} takes {n} argument(s), got {}",
                a.len()
            )))
        }
    };
    let f1 = |f: fn(f64) -> f64| -> Result<Value, DsError> {
        arity(1)?;
        Ok(Value::Double(f(f64_arg(&a[0])?)))
    };
    Ok(match name {
        "ABS" => {
            arity(1)?;
            match &a[0] {
                Value::Int64(i) => {
                    Value::Int64(i.checked_abs().ok_or_else(|| data("int64 overflow"))?)
                }
                Value::Decimal(d) => Value::Decimal(
                    Decimal128::new(d.coefficient().abs(), d.exponent())
                        .map_err(|e| data(e.to_string()))?,
                ),
                v => Value::Double(f64_arg(v)?.abs()),
            }
        }
        "CEIL" | "CEILING" => f1(f64::ceil)?,
        "FLOOR" => f1(f64::floor)?,
        "SQRT" => f1(f64::sqrt)?,
        "EXP" => f1(f64::exp)?,
        "LN" => f1(f64::ln)?,
        "LOG10" => f1(f64::log10)?,
        "LOG" => {
            arity(2)?;
            Value::Double(f64_arg(&a[1])?.log(f64_arg(&a[0])?))
        }
        "POWER" => {
            arity(2)?;
            Value::Double(f64_arg(&a[0])?.powf(f64_arg(&a[1])?))
        }
        "MOD" => {
            arity(2)?;
            arith(BinOp::Mod, &a[0], &a[1])?
        }
        "SIN" => f1(f64::sin)?,
        "COS" => f1(f64::cos)?,
        "TAN" => f1(f64::tan)?,
        "ASIN" => f1(f64::asin)?,
        "ACOS" => f1(f64::acos)?,
        "ATAN" => f1(f64::atan)?,
        "COT" => f1(|x| 1.0 / x.tan())?,
        "SINH" => f1(f64::sinh)?,
        "COSH" => f1(f64::cosh)?,
        "TANH" => f1(f64::tanh)?,
        "DEGREES" => f1(f64::to_degrees)?,
        "RADIANS" => f1(f64::to_radians)?,
        "UPPER" | "LOWER" => match a {
            [Value::String(s)] => Value::String(if name == "UPPER" {
                s.to_uppercase()
            } else {
                s.to_lowercase()
            }),
            _ => return Err(data(format!("{name} takes a string"))),
        },
        "CHAR_LENGTH" | "CHARACTER_LENGTH" => match a {
            [Value::String(s)] => Value::Int64(s.chars().count() as i64),
            _ => return Err(data(format!("{name} takes a string"))),
        },
        "BYTE_LENGTH" | "OCTET_LENGTH" => match a {
            [Value::Binary(b)] => Value::Int64(b.len() as i64),
            [Value::String(s)] => Value::Int64(s.len() as i64),
            _ => return Err(data(format!("{name} takes a byte string"))),
        },
        "LEFT" | "RIGHT" => match a {
            [Value::String(s), Value::Int64(n)] if *n >= 0 => {
                let chars: Vec<char> = s.chars().collect();
                let n = (*n as usize).min(chars.len());
                Value::String(if name == "LEFT" {
                    chars[..n].iter().collect()
                } else {
                    chars[chars.len() - n..].iter().collect()
                })
            }
            _ => return Err(data(format!("{name}(string, length)"))),
        },
        "TRIM" => match a {
            // Explicit form from the parser: (mode, source[, chars]).
            [Value::String(mode), Value::String(s), rest @ ..] => {
                let chars = match rest.first() {
                    Some(Value::String(c)) => c.clone(),
                    None => " ".into(),
                    Some(v) => {
                        return Err(data(format!("TRIM characters must be a string, got {v:?}")));
                    }
                };
                trim(s, &chars, mode != "TRAILING", mode != "LEADING")
            }
            .into(),
            [Value::String(mode), Value::Binary(b), Value::Binary(set)] => {
                Value::Binary(match mode.as_str() {
                    "BOTH" => trim_bytes(b, set),
                    _ => return Err(data("byte string TRIM supports BOTH")),
                })
            }
            [Value::String(s)] => Value::String(s.trim().to_string()),
            _ => return Err(data("TRIM needs a string")),
        },
        "BTRIM" | "LTRIM" | "RTRIM" => match a {
            [Value::String(s)] => Value::String(trim(s, " ", name != "RTRIM", name != "LTRIM")),
            [Value::String(s), Value::String(c)] => {
                Value::String(trim(s, c, name != "RTRIM", name != "LTRIM"))
            }
            [Value::Binary(b), Value::Binary(set)] if name == "BTRIM" => {
                Value::Binary(trim_bytes(b, set))
            }
            _ => return Err(data(format!("{name}(string[, characters])"))),
        },
        "SIZE" | "CARDINALITY" => match a {
            [Value::List(l)] => Value::Int64(l.len() as i64),
            [Value::Record(r)] => Value::Int64(r.len() as i64),
            _ => return Err(data(format!("{name} takes a list"))),
        },
        "COALESCE" => a
            .iter()
            .find(|v| !matches!(v, Value::Null))
            .cloned()
            .unwrap_or(Value::Null),
        "NULLIF" => {
            arity(2)?;
            if compare(&a[0], &a[1]) == Some(Ordering::Equal) {
                Value::Null
            } else {
                a[0].clone()
            }
        }
        other => return Err(semantic(format!("function {other} is not implemented"))),
    })
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s)
    }
}
