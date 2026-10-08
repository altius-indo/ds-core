//! STORY-0008 E2: property-document size limit and live reconfiguration.

use dscore_server::graph::catalog::Catalog;
use dscore_server::graph::limits::{DEFAULT_MAX_PROPERTY_BYTES, LimitError, Limits};
use dscore_server::graph::model::{NodeWrite, WriteError, resolve_node};
use dscore_server::graph::value::{Record, Value};

/// A record whose encoded size is exactly `size` bytes.
fn document_of_size(size: usize) -> Record {
    let doc = |n: usize| Record::new(vec![("blob".into(), Value::Binary(vec![0xab; n]))]).unwrap();
    let overhead = doc(0).encoded_len();
    // The binary length prefix grows with n; shrink the payload until the total fits exactly.
    let mut n = size - overhead;
    while doc(n).encoded_len() > size {
        n -= 1;
    }
    let d = doc(n);
    assert_eq!(
        d.encoded_len(),
        size,
        "cannot build a document of exactly {size} bytes"
    );
    d
}

fn write(props: Record) -> NodeWrite {
    NodeWrite {
        id: 7,
        labels: vec!["Doc".into()],
        properties: props,
    }
}

// reqforge: verifies REQ-0013#AC1
#[test]
fn size_16mib_accepted_16mib_plus_1_rejected() {
    let limits = Limits::default();
    let catalog = Catalog::default();
    assert_eq!(limits.max_property_bytes(), 16 * 1024 * 1024);

    let at_limit = document_of_size(DEFAULT_MAX_PROPERTY_BYTES as usize);
    let over = document_of_size(DEFAULT_MAX_PROPERTY_BYTES as usize + 1);

    let stored = resolve_node(&write(at_limit), &catalog, &limits).unwrap();
    assert_eq!(stored.properties.len() as u64, DEFAULT_MAX_PROPERTY_BYTES);

    let err = resolve_node(&write(over), &catalog, &limits).unwrap_err();
    assert_eq!(
        err,
        WriteError::Limit(LimitError::DocumentTooLarge {
            size: DEFAULT_MAX_PROPERTY_BYTES + 1,
            max: DEFAULT_MAX_PROPERTY_BYTES,
        })
    );
}

// reqforge: verifies REQ-0013#AC2
#[test]
fn live_reconfigure_applies_without_restart() {
    let limits = Limits::default();
    // A clone stands in for another component holding the same handle (e.g. a request worker).
    let worker = limits.clone();
    let catalog = Catalog::default();
    let doc = document_of_size(4096);

    resolve_node(&write(doc.clone()), &catalog, &worker).unwrap();

    limits.set_max_property_bytes(4095).unwrap();
    assert_eq!(worker.max_property_bytes(), 4095);
    assert!(matches!(
        resolve_node(&write(doc.clone()), &catalog, &worker),
        Err(WriteError::Limit(LimitError::DocumentTooLarge {
            size: 4096,
            max: 4095
        }))
    ));

    limits.set_max_property_bytes(4096).unwrap();
    resolve_node(&write(doc), &catalog, &worker).unwrap();

    assert_eq!(
        limits.set_max_property_bytes(0),
        Err(LimitError::InvalidSetting(0))
    );
    assert_eq!(
        worker.max_property_bytes(),
        4096,
        "a rejected setting leaves the limit unchanged"
    );
}
