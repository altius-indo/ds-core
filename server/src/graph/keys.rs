//! Node and edge keys in the ordered keyspace (design/raft-ranges.md §9, DEC-0001).
//!
//!   0x02 'g' graph:u32 'n' node:u64 0x00                          node document
//!   0x02 'g' graph:u32 'n' node:u64 'i' etype:u32 src:u64 rank:u64 in-edge
//!   0x02 'g' graph:u32 'n' node:u64 'o' etype:u32 dst:u64 rank:u64 out-edge
//!   0x02 'g' graph:u32 'c' kind:u8 ...                            name catalog
//!
//! Integers are big-endian, so byte order is numeric order, and everything about a node sorts
//! under its prefix with the document first: one-hop reads are prefix scans (REQ-0023), and a
//! hub node's adjacency can be split across ranges between edge entries (REQ-0024).

// reqforge: implements REQ-0010

pub type GraphId = u32;
pub type NodeId = u64;
pub type TypeId = u32;

const SPACE: u8 = 0x02;
const GRAPH: u8 = b'g';
const NODE: u8 = b'n';
const DOC: u8 = 0x00;
const IN: u8 = b'i';
const OUT: u8 = b'o';
const CATALOG: u8 = b'c';

/// Catalog kinds.
pub const LABEL: u8 = b'l';
pub const EDGE_TYPE: u8 = b'e';

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Out,
    In,
}

fn graph_prefix(g: GraphId) -> Vec<u8> {
    let mut k = Vec::with_capacity(32);
    k.extend_from_slice(&[SPACE, GRAPH]);
    k.extend_from_slice(&g.to_be_bytes());
    k
}

pub fn node_prefix(g: GraphId, node: NodeId) -> Vec<u8> {
    let mut k = graph_prefix(g);
    k.push(NODE);
    k.extend_from_slice(&node.to_be_bytes());
    k
}

pub fn node_doc(g: GraphId, node: NodeId) -> Vec<u8> {
    let mut k = node_prefix(g, node);
    k.push(DOC);
    k
}

/// The key of one direction of an edge stored under `at`: for `Out`, `other` is the target;
/// for `In`, `other` is the source.
pub fn edge(
    g: GraphId,
    at: NodeId,
    dir: Direction,
    etype: TypeId,
    other: NodeId,
    rank: u64,
) -> Vec<u8> {
    let mut k = node_prefix(g, at);
    k.push(match dir {
        Direction::Out => OUT,
        Direction::In => IN,
    });
    k.extend_from_slice(&etype.to_be_bytes());
    k.extend_from_slice(&other.to_be_bytes());
    k.extend_from_slice(&rank.to_be_bytes());
    k
}

/// `[start, end)` of `at`'s edges in one direction, optionally of one type.
pub fn adjacency_span(
    g: GraphId,
    at: NodeId,
    dir: Direction,
    etype: Option<TypeId>,
) -> (Vec<u8>, Vec<u8>) {
    let mut start = node_prefix(g, at);
    start.push(match dir {
        Direction::Out => OUT,
        Direction::In => IN,
    });
    if let Some(t) = etype {
        start.extend_from_slice(&t.to_be_bytes());
    }
    let end = prefix_end(&start);
    (start, end)
}

/// Every key of node `at`: its document and both adjacency lists.
pub fn node_span(g: GraphId, at: NodeId) -> (Vec<u8>, Vec<u8>) {
    let start = node_prefix(g, at);
    let end = prefix_end(&start);
    (start, end)
}

/// The smallest key greater than every key with `prefix`.
pub fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xFF {
            end.push(last + 1);
            return end;
        }
    }
    Vec::new()
}

/// Decoded edge key: (at, direction, type, other, rank).
pub fn decode_edge(g: GraphId, key: &[u8]) -> Option<(NodeId, Direction, TypeId, NodeId, u64)> {
    let p = graph_prefix(g);
    let rest = key.strip_prefix(p.as_slice())?.strip_prefix(&[NODE])?;
    if rest.len() != 8 + 1 + 4 + 8 + 8 {
        return None;
    }
    let at = NodeId::from_be_bytes(rest[0..8].try_into().ok()?);
    let dir = match rest[8] {
        OUT => Direction::Out,
        IN => Direction::In,
        _ => return None,
    };
    let etype = TypeId::from_be_bytes(rest[9..13].try_into().ok()?);
    let other = NodeId::from_be_bytes(rest[13..21].try_into().ok()?);
    let rank = u64::from_be_bytes(rest[21..29].try_into().ok()?);
    Some((at, dir, etype, other, rank))
}

/// Catalog entry mapping a name to its id.
pub fn catalog_name(g: GraphId, kind: u8, name: &str) -> Vec<u8> {
    let mut k = graph_prefix(g);
    k.extend_from_slice(&[CATALOG, kind, b'n']);
    k.extend_from_slice(name.as_bytes());
    k
}

/// Catalog entry mapping an id back to its name.
pub fn catalog_id(g: GraphId, kind: u8, id: TypeId) -> Vec<u8> {
    let mut k = graph_prefix(g);
    k.extend_from_slice(&[CATALOG, kind, b'i']);
    k.extend_from_slice(&id.to_be_bytes());
    k
}

/// The catalog's next-id counter for `kind`.
pub fn catalog_counter(g: GraphId, kind: u8) -> Vec<u8> {
    let mut k = graph_prefix(g);
    k.extend_from_slice(&[CATALOG, kind, b'#']);
    k
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_document_sorts_before_its_edges_and_nodes_stay_together() {
        let doc = node_doc(1, 7);
        let inn = edge(1, 7, Direction::In, 3, 9, 0);
        let out = edge(1, 7, Direction::Out, 3, 9, 0);
        assert!(doc < inn && inn < out);
        let (s, e) = node_span(1, 7);
        for k in [&doc, &inn, &out] {
            assert!(k.as_slice() >= s.as_slice() && k.as_slice() < e.as_slice());
        }
        assert!(out < node_doc(1, 8), "node 7's keys all precede node 8's");
        assert!(
            node_doc(1, u64::MAX) < node_doc(2, 0),
            "graphs are disjoint"
        );
    }

    #[test]
    fn adjacency_span_selects_one_direction_and_type() {
        let (s, e) = adjacency_span(1, 7, Direction::Out, Some(3));
        let inside = edge(1, 7, Direction::Out, 3, u64::MAX, u64::MAX);
        let other_type = edge(1, 7, Direction::Out, 4, 0, 0);
        let other_dir = edge(1, 7, Direction::In, 3, 0, 0);
        assert!(inside.as_slice() >= s.as_slice() && inside.as_slice() < e.as_slice());
        assert!(other_type.as_slice() >= e.as_slice());
        assert!(other_dir.as_slice() < s.as_slice());
    }

    #[test]
    fn edge_keys_decode() {
        let k = edge(2, 10, Direction::In, 5, 11, 3);
        assert_eq!(decode_edge(2, &k), Some((10, Direction::In, 5, 11, 3)));
        assert_eq!(decode_edge(2, &node_doc(2, 10)), None);
    }

    #[test]
    fn prefix_end_handles_ff() {
        assert_eq!(prefix_end(&[1, 2]), vec![1, 3]);
        assert_eq!(prefix_end(&[1, 0xFF]), vec![2]);
        assert_eq!(prefix_end(&[0xFF, 0xFF]), Vec::<u8>::new());
    }
}
