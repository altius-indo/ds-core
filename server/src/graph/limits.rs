//! Property-document size limit (REQ-0013).
//!
//! The limit is held in a shared atomic so that a configuration change applies to the next
//! write on every holder of the handle, with no restart (REQ-0013 AC2). Distributing a change
//! across the cluster goes through the meta range once it exists; this is the node-local half.

// reqforge: implements REQ-0013

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::value::Record;

/// 16 MiB (REQ-0013).
pub const DEFAULT_MAX_PROPERTY_BYTES: u64 = 16 * 1024 * 1024;

/// Upper bound on the configurable limit: a document must still fit comfortably in one Raft
/// log entry alongside the rest of its transaction.
pub const MAX_CONFIGURABLE_PROPERTY_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct Limits {
    max_property_bytes: Arc<AtomicU64>,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_property_bytes: Arc::new(AtomicU64::new(DEFAULT_MAX_PROPERTY_BYTES)),
        }
    }
}

impl Limits {
    pub fn max_property_bytes(&self) -> u64 {
        self.max_property_bytes.load(Ordering::Acquire)
    }

    /// Change the limit live. Every clone of this handle sees the new value immediately.
    pub fn set_max_property_bytes(&self, bytes: u64) -> Result<(), LimitError> {
        if bytes == 0 || bytes > MAX_CONFIGURABLE_PROPERTY_BYTES {
            return Err(LimitError::InvalidSetting(bytes));
        }
        self.max_property_bytes.store(bytes, Ordering::Release);
        Ok(())
    }

    /// Check an encoded property document; returns its size in bytes.
    pub fn check_document(&self, doc: &Record) -> Result<u64, LimitError> {
        let size = doc.encoded_len() as u64;
        let max = self.max_property_bytes();
        if size > max {
            return Err(LimitError::DocumentTooLarge { size, max });
        }
        Ok(size)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LimitError {
    DocumentTooLarge { size: u64, max: u64 },
    InvalidSetting(u64),
}

impl fmt::Display for LimitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DocumentTooLarge { size, max } => {
                write!(
                    f,
                    "property document is {size} bytes; the limit is {max} bytes"
                )
            }
            Self::InvalidSetting(b) => write!(
                f,
                "max_property_bytes must be between 1 and {MAX_CONFIGURABLE_PROPERTY_BYTES}, got {b}"
            ),
        }
    }
}

impl std::error::Error for LimitError {}
