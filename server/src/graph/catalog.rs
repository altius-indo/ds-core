//! Per-graph dictionaries that intern label and edge-type names to compact ids.
//!
//! The graph is schemaless (DEC-0007, REQ-0008): a name is interned the first time a write uses
//! it, with no DDL. Ids are never reused, so the dictionaries only grow. Persisting them in the
//! meta range comes with storage; this is the in-memory form.

// reqforge: implements REQ-0008

use std::collections::HashMap;
use std::fmt;
use std::sync::RwLock;

#[derive(Debug, Default)]
pub struct Dictionary {
    inner: RwLock<DictInner>,
}

#[derive(Debug, Default)]
struct DictInner {
    ids: HashMap<String, u32>,
    names: Vec<String>,
}

impl Dictionary {
    pub fn get(&self, name: &str) -> Option<u32> {
        self.inner
            .read()
            .expect("dictionary lock poisoned")
            .ids
            .get(name)
            .copied()
    }

    pub fn name(&self, id: u32) -> Option<String> {
        self.inner
            .read()
            .expect("dictionary lock poisoned")
            .names
            .get(id as usize)
            .cloned()
    }

    /// Id for `name`, assigning the next id if it has not been seen.
    pub fn intern(&self, name: &str) -> Result<u32, CatalogError> {
        if let Some(id) = self.get(name) {
            return Ok(id);
        }
        let mut inner = self.inner.write().expect("dictionary lock poisoned");
        if let Some(&id) = inner.ids.get(name) {
            return Ok(id);
        }
        let id = u32::try_from(inner.names.len()).map_err(|_| CatalogError::Full)?;
        inner.names.push(name.to_string());
        inner.ids.insert(name.to_string(), id);
        Ok(id)
    }

    pub fn len(&self) -> usize {
        self.inner
            .read()
            .expect("dictionary lock poisoned")
            .names
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Name dictionaries for one graph.
#[derive(Debug, Default)]
pub struct Catalog {
    pub labels: Dictionary,
    pub edge_types: Dictionary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogError {
    Full,
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("name dictionary is full (2^32 names)")
    }
}

impl std::error::Error for CatalogError {}
