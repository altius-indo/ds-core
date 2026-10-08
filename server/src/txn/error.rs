//! Client-visible error codes (REQ-0005).
//!
//! `SERIALIZATION_CONFLICT` is the only retryable code and is returned only for transaction
//! conflicts (write-write at prewrite, failed read validation, being pushed, lock-wait
//! timeout). Syntax, permission and constraint errors have their own codes (REQ-0005 AC2).
//! Codes follow SQLSTATE classes so drivers can map them uniformly.

// reqforge: implements REQ-0005

use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    /// 40001: retry the whole transaction.
    SerializationConflict,
    /// 42000: the statement is not valid GQL.
    Syntax,
    /// 42501: the principal lacks a privilege.
    Permission,
    /// 23000: a uniqueness or other integrity constraint would be violated.
    ConstraintViolation,
    /// 22023: an invalid configuration or option value.
    InvalidConfiguration,
    /// 08006: the cluster could not serve the request (no leader, network).
    Unavailable,
    /// XX000: unexpected internal failure.
    Internal,
}

impl ErrorCode {
    pub fn sqlstate(self) -> &'static str {
        match self {
            Self::SerializationConflict => "40001",
            Self::Syntax => "42000",
            Self::Permission => "42501",
            Self::ConstraintViolation => "23000",
            Self::InvalidConfiguration => "22023",
            Self::Unavailable => "08006",
            Self::Internal => "XX000",
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::SerializationConflict => "SERIALIZATION_CONFLICT",
            Self::Syntax => "SYNTAX_ERROR",
            Self::Permission => "PERMISSION_DENIED",
            Self::ConstraintViolation => "CONSTRAINT_VIOLATION",
            Self::InvalidConfiguration => "INVALID_CONFIGURATION",
            Self::Unavailable => "UNAVAILABLE",
            Self::Internal => "INTERNAL",
        }
    }

    /// Only a serialization conflict is safe to retry blindly (REQ-0005 AC2).
    pub fn is_retryable(self) -> bool {
        self == Self::SerializationConflict
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DsError {
    pub code: ErrorCode,
    pub message: String,
}

impl DsError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::SerializationConflict, message)
    }
}

impl fmt::Display for DsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} ({}): {}",
            self.code.name(),
            self.code.sqlstate(),
            self.message
        )
    }
}

impl std::error::Error for DsError {}

impl From<super::cluster::Unavailable> for DsError {
    fn from(e: super::cluster::Unavailable) -> Self {
        Self::new(ErrorCode::Unavailable, e.to_string())
    }
}
