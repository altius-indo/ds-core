//! Nodes and edges stored transactionally (REQ-0006, REQ-0007, REQ-0010).
//!
//! Every write goes through a serializable transaction, so an edge's out-entry under its
//! source and in-entry under its target are written, changed and deleted together: no
//! committed state has an edge visible from only one endpoint (REQ-0010). Both entries carry
//! the edge's properties, so a traversal from either side needs no second read.
//! Label and edge-type names are interned per graph through a transactional catalog, so two
//! transactions creating the same new name serialize instead of allocating two ids.

// reqforge: implements REQ-0006
// reqforge: implements REQ-0007
// reqforge: implements REQ-0010

use serde::{Deserialize, Serialize};

use super::keys::{self, Direction, EDGE_TYPE, GraphId, LABEL, NodeId, TypeId};
use super::limits::Limits;
use super::value::Record;
use crate::txn::coordinator::Txn;
use crate::txn::error::{DsError, ErrorCode};

#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    pub id: NodeId,
    pub labels: Vec<String>,
    pub properties: Record,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Edge {
    pub src: NodeId,
    pub dst: NodeId,
    pub edge_type: String,
    pub rank: u64,
    pub properties: Record,
}

#[derive(Serialize, Deserialize)]
struct NodeDoc {
    labels: Vec<TypeId>,
    properties: Vec<u8>,
}

fn bad_data(e: impl std::fmt::Display) -> DsError {
    DsError::new(ErrorCode::Internal, format!("corrupt graph data: {e}"))
}

fn invalid(e: impl std::fmt::Display) -> DsError {
    DsError::new(ErrorCode::ConstraintViolation, e.to_string())
}

/// A graph within the database.
#[derive(Debug, Clone)]
pub struct Graph {
    pub id: GraphId,
    pub limits: Limits,
}

impl Graph {
    pub fn new(id: GraphId) -> Self {
        Self {
            id,
            limits: Limits::default(),
        }
    }

    // ------------------------------------------------------------------ catalog

    async fn lookup(&self, txn: &mut Txn, kind: u8, name: &str) -> Result<Option<TypeId>, DsError> {
        Ok(
            match txn.get(&keys::catalog_name(self.id, kind, name)).await? {
                Some(b) => Some(TypeId::from_be_bytes(
                    b.as_slice().try_into().map_err(bad_data)?,
                )),
                None => None,
            },
        )
    }

    /// Id for `name`, allocating one in this transaction if the name is new.
    async fn intern(&self, txn: &mut Txn, kind: u8, name: &str) -> Result<TypeId, DsError> {
        if name.is_empty() {
            return Err(invalid("labels and edge types must be non-empty"));
        }
        if let Some(id) = self.lookup(txn, kind, name).await? {
            return Ok(id);
        }
        let counter = keys::catalog_counter(self.id, kind);
        let next = match txn.get(&counter).await? {
            Some(b) => TypeId::from_be_bytes(b.as_slice().try_into().map_err(bad_data)?),
            None => 0,
        };
        txn.put(&counter, &(next + 1).to_be_bytes());
        txn.put(
            &keys::catalog_name(self.id, kind, name),
            &next.to_be_bytes(),
        );
        txn.put(&keys::catalog_id(self.id, kind, next), name.as_bytes());
        Ok(next)
    }

    async fn name_of(&self, txn: &mut Txn, kind: u8, id: TypeId) -> Result<String, DsError> {
        let b = txn
            .get(&keys::catalog_id(self.id, kind, id))
            .await?
            .ok_or_else(|| bad_data(format!("unknown catalog id {id}")))?;
        String::from_utf8(b).map_err(bad_data)
    }

    // ------------------------------------------------------------------ nodes

    /// Insert a node; a node with the same id must not exist (REQ-0006 AC2).
    pub async fn insert_node(
        &self,
        txn: &mut Txn,
        id: NodeId,
        labels: &[&str],
        properties: &Record,
    ) -> Result<(), DsError> {
        self.limits.check_document(properties).map_err(invalid)?;
        let mut label_ids = Vec::with_capacity(labels.len());
        for l in labels {
            label_ids.push(self.intern(txn, LABEL, l).await?);
        }
        label_ids.sort_unstable();
        label_ids.dedup();
        let doc = NodeDoc {
            labels: label_ids,
            properties: properties.encode(),
        };
        let key = keys::node_doc(self.id, id);
        let bytes = postcard::to_allocvec(&doc).map_err(bad_data)?;
        txn.insert_new(&key, &bytes)
            .await
            .map_err(|e| match e.code {
                ErrorCode::ConstraintViolation => DsError::new(
                    ErrorCode::ConstraintViolation,
                    format!("node {id} already exists"),
                ),
                _ => e,
            })
    }

    pub async fn get_node(&self, txn: &mut Txn, id: NodeId) -> Result<Option<Node>, DsError> {
        let Some(b) = txn.get(&keys::node_doc(self.id, id)).await? else {
            return Ok(None);
        };
        let doc: NodeDoc = postcard::from_bytes(&b).map_err(bad_data)?;
        let mut labels = Vec::with_capacity(doc.labels.len());
        for l in doc.labels {
            labels.push(self.name_of(txn, LABEL, l).await?);
        }
        labels.sort();
        Ok(Some(Node {
            id,
            labels,
            properties: Record::decode(&doc.properties).map_err(bad_data)?,
        }))
    }

    // ------------------------------------------------------------------ edges

    /// Insert an edge; both endpoints must exist. Writes the out-entry under `src` and the
    /// in-entry under `dst` in the same transaction (REQ-0010).
    pub async fn insert_edge(&self, txn: &mut Txn, e: &Edge) -> Result<(), DsError> {
        self.limits.check_document(&e.properties).map_err(invalid)?;
        for endpoint in [e.src, e.dst] {
            if txn.get(&keys::node_doc(self.id, endpoint)).await?.is_none() {
                return Err(invalid(format!(
                    "edge endpoint node {endpoint} does not exist"
                )));
            }
        }
        let t = self.intern(txn, EDGE_TYPE, &e.edge_type).await?;
        let out = keys::edge(self.id, e.src, Direction::Out, t, e.dst, e.rank);
        let inn = keys::edge(self.id, e.dst, Direction::In, t, e.src, e.rank);
        if txn.get(&out).await?.is_some() {
            return Err(invalid(format!(
                "edge {}-[{}#{}]->{} already exists",
                e.src, e.edge_type, e.rank, e.dst
            )));
        }
        let props = e.properties.encode();
        txn.put(&out, &props);
        txn.put(&inn, &props);
        Ok(())
    }

    /// Replace an existing edge's properties on both entries.
    pub async fn set_edge_properties(&self, txn: &mut Txn, e: &Edge) -> Result<(), DsError> {
        self.limits.check_document(&e.properties).map_err(invalid)?;
        let Some(t) = self.lookup(txn, EDGE_TYPE, &e.edge_type).await? else {
            return Err(invalid(format!("no edges of type {}", e.edge_type)));
        };
        let out = keys::edge(self.id, e.src, Direction::Out, t, e.dst, e.rank);
        if txn.get(&out).await?.is_none() {
            return Err(invalid("edge does not exist"));
        }
        let props = e.properties.encode();
        txn.put(&out, &props);
        txn.put(
            &keys::edge(self.id, e.dst, Direction::In, t, e.src, e.rank),
            &props,
        );
        Ok(())
    }

    /// Delete an edge's two entries. Returns false if it did not exist.
    pub async fn delete_edge(
        &self,
        txn: &mut Txn,
        src: NodeId,
        edge_type: &str,
        dst: NodeId,
        rank: u64,
    ) -> Result<bool, DsError> {
        let Some(t) = self.lookup(txn, EDGE_TYPE, edge_type).await? else {
            return Ok(false);
        };
        let out = keys::edge(self.id, src, Direction::Out, t, dst, rank);
        if txn.get(&out).await?.is_none() {
            return Ok(false);
        }
        txn.delete(&out);
        txn.delete(&keys::edge(self.id, dst, Direction::In, t, src, rank));
        Ok(true)
    }

    /// Edges of `node` in one direction, optionally of one type: a single prefix scan.
    pub async fn edges(
        &self,
        txn: &mut Txn,
        node: NodeId,
        dir: Direction,
        edge_type: Option<&str>,
    ) -> Result<Vec<Edge>, DsError> {
        let t = match edge_type {
            Some(name) => match self.lookup(txn, EDGE_TYPE, name).await? {
                Some(t) => Some(t),
                None => return Ok(Vec::new()),
            },
            None => None,
        };
        let (start, end) = keys::adjacency_span(self.id, node, dir, t);
        let pairs = txn.scan(&start, &end).await?;
        let mut out = Vec::with_capacity(pairs.len());
        for (k, v) in pairs {
            let (at, d, etype, other, rank) =
                keys::decode_edge(self.id, &k).ok_or_else(|| bad_data("edge key"))?;
            let (src, dst) = match d {
                Direction::Out => (at, other),
                Direction::In => (other, at),
            };
            out.push(Edge {
                src,
                dst,
                edge_type: self.name_of(txn, EDGE_TYPE, etype).await?,
                rank,
                properties: Record::decode(&v).map_err(bad_data)?,
            });
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------- node deletion

impl Graph {
    /// Plain DELETE: refused while the node has any incident edge (REQ-0029, DEC-0006).
    /// Returns false if the node does not exist.
    pub async fn delete_node(&self, txn: &mut Txn, id: NodeId) -> Result<bool, DsError> {
        let doc = keys::node_doc(self.id, id);
        if txn.get(&doc).await?.is_none() {
            return Ok(false);
        }
        let (start, end) = keys::node_span(self.id, id);
        // Everything under the node prefix except the document itself is an edge entry. The
        // scan is a read span, so a concurrent edge insert to this node conflicts with us.
        let incident = txn
            .scan(&start, &end)
            .await?
            .into_iter()
            .filter(|(k, _)| *k != doc)
            .count();
        if incident > 0 {
            return Err(DsError::new(
                ErrorCode::ConstraintViolation,
                format!("node {id} has {incident} incident edge entries; use DETACH DELETE"),
            ));
        }
        txn.delete(&doc);
        Ok(true)
    }

    /// DETACH DELETE: remove the node and every incident edge, both entries of each, in this
    /// transaction (REQ-0030, DEC-0006). Returns the number of edges removed.
    pub async fn detach_delete_node(
        &self,
        txn: &mut Txn,
        id: NodeId,
    ) -> Result<Option<usize>, DsError> {
        let doc = keys::node_doc(self.id, id);
        if txn.get(&doc).await?.is_none() {
            return Ok(None);
        }
        let (start, end) = keys::node_span(self.id, id);
        let mut removed = 0;
        for (k, _) in txn.scan(&start, &end).await? {
            if k == doc {
                continue;
            }
            let (at, dir, etype, other, rank) =
                keys::decode_edge(self.id, &k).ok_or_else(|| bad_data("edge key"))?;
            let mirror = match dir {
                Direction::Out => keys::edge(self.id, other, Direction::In, etype, at, rank),
                Direction::In => keys::edge(self.id, other, Direction::Out, etype, at, rank),
            };
            txn.delete(&k);
            txn.delete(&mirror);
            if dir == Direction::Out || other != at {
                removed += 1;
            }
        }
        txn.delete(&doc);
        Ok(Some(removed))
    }
}

// ---------------------------------------------------------------------- node updates and scans

impl Graph {
    /// Replace a node's labels and properties (SET / REMOVE). The node must exist.
    pub async fn update_node(
        &self,
        txn: &mut Txn,
        id: NodeId,
        labels: &[String],
        properties: &Record,
    ) -> Result<(), DsError> {
        self.limits.check_document(properties).map_err(invalid)?;
        let key = keys::node_doc(self.id, id);
        if txn.get(&key).await?.is_none() {
            return Err(invalid(format!("node {id} does not exist")));
        }
        let mut label_ids = Vec::with_capacity(labels.len());
        for l in labels {
            label_ids.push(self.intern(txn, LABEL, l).await?);
        }
        label_ids.sort_unstable();
        label_ids.dedup();
        let doc = NodeDoc {
            labels: label_ids,
            properties: properties.encode(),
        };
        txn.put(&key, &postcard::to_allocvec(&doc).map_err(bad_data)?);
        Ok(())
    }

    /// Every node of the graph, in id order. A full scan: v1 has no label index yet
    /// (property indexes arrive with TASK-0020).
    pub async fn scan_nodes(&self, txn: &mut Txn) -> Result<Vec<Node>, DsError> {
        let (start, end) = keys::graph_nodes_span(self.id);
        let mut out = Vec::new();
        for (k, v) in txn.scan(&start, &end).await? {
            let Some(id) = keys::decode_node_doc(self.id, &k) else {
                continue;
            };
            let doc: NodeDoc = postcard::from_bytes(&v).map_err(bad_data)?;
            let mut labels = Vec::with_capacity(doc.labels.len());
            for l in doc.labels {
                labels.push(self.name_of(txn, LABEL, l).await?);
            }
            labels.sort();
            out.push(Node {
                id,
                labels,
                properties: Record::decode(&doc.properties).map_err(bad_data)?,
            });
        }
        Ok(out)
    }
}
