//! Node and edge writes, validated and resolved against a graph's catalog and limits.
//!
//! Validation happens before anything is stored: names are interned (schemaless, REQ-0008),
//! the property document is size-checked (REQ-0013) and encoded. `Record` already guarantees
//! unique property names (REQ-0011).

// reqforge: implements REQ-0006
// reqforge: implements REQ-0007

use std::fmt;

use super::catalog::{Catalog, CatalogError};
use super::limits::{LimitError, Limits};
use super::value::Record;

#[derive(Debug, Clone, PartialEq)]
pub struct NodeWrite {
    pub id: u64,
    pub labels: Vec<String>,
    pub properties: Record,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EdgeWrite {
    pub src: u64,
    pub dst: u64,
    pub edge_type: String,
    /// Distinguishes parallel edges with the same (src, type, dst) (REQ-0007, DEC-0001).
    pub rank: u64,
    pub properties: Record,
}

/// A node ready to store: label ids sorted and deduplicated, properties encoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedNode {
    pub id: u64,
    pub label_ids: Vec<u32>,
    pub properties: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedEdge {
    pub src: u64,
    pub dst: u64,
    pub edge_type_id: u32,
    pub rank: u64,
    pub properties: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteError {
    EmptyName,
    Limit(LimitError),
    Catalog(CatalogError),
}

impl fmt::Display for WriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyName => f.write_str("labels and edge types must be non-empty"),
            Self::Limit(e) => e.fmt(f),
            Self::Catalog(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for WriteError {}

impl From<LimitError> for WriteError {
    fn from(e: LimitError) -> Self {
        Self::Limit(e)
    }
}

impl From<CatalogError> for WriteError {
    fn from(e: CatalogError) -> Self {
        Self::Catalog(e)
    }
}

pub fn resolve_node(
    w: &NodeWrite,
    catalog: &Catalog,
    limits: &Limits,
) -> Result<ResolvedNode, WriteError> {
    limits.check_document(&w.properties)?;
    let mut label_ids = Vec::with_capacity(w.labels.len());
    for label in &w.labels {
        if label.is_empty() {
            return Err(WriteError::EmptyName);
        }
        label_ids.push(catalog.labels.intern(label)?);
    }
    label_ids.sort_unstable();
    label_ids.dedup();
    Ok(ResolvedNode {
        id: w.id,
        label_ids,
        properties: w.properties.encode(),
    })
}

pub fn resolve_edge(
    w: &EdgeWrite,
    catalog: &Catalog,
    limits: &Limits,
) -> Result<ResolvedEdge, WriteError> {
    limits.check_document(&w.properties)?;
    if w.edge_type.is_empty() {
        return Err(WriteError::EmptyName);
    }
    Ok(ResolvedEdge {
        src: w.src,
        dst: w.dst,
        edge_type_id: catalog.edge_types.intern(&w.edge_type)?,
        rank: w.rank,
        properties: w.properties.encode(),
    })
}
