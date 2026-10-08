//! Security events. Refused connections are recorded here (REQ-0036 AC1); the audit log
//! (TASK-0036) becomes a sink for the same events.

use std::fmt;
use std::net::SocketAddr;
use std::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Port {
    Client,
    Peer,
}

impl fmt::Display for Port {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Port::Client => "client",
            Port::Peer => "peer",
        })
    }
}

/// Why a TLS handshake was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    NoCertificate,
    ExpiredCertificate,
    UntrustedIssuer,
    BadCertificate,
    ProtocolViolation,
    Timeout,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecurityEvent {
    TlsRejected {
        port: Port,
        remote: SocketAddr,
        reason: RejectReason,
        detail: String,
    },
}

pub trait SecurityEventSink: Send + Sync {
    fn record(&self, event: SecurityEvent);
}

/// Writes one line per event to stderr until the audit log exists.
#[derive(Debug, Default)]
pub struct StderrSink;

impl SecurityEventSink for StderrSink {
    fn record(&self, event: SecurityEvent) {
        match event {
            SecurityEvent::TlsRejected {
                port,
                remote,
                reason,
                detail,
            } => eprintln!(
                "security: tls_rejected port={port} remote={remote} reason={reason:?} detail={detail:?}"
            ),
        }
    }
}

/// Keeps events in memory; for tests.
#[derive(Debug, Default)]
pub struct MemorySink {
    events: Mutex<Vec<SecurityEvent>>,
}

impl MemorySink {
    pub fn events(&self) -> Vec<SecurityEvent> {
        self.events.lock().expect("sink lock poisoned").clone()
    }
}

impl SecurityEventSink for MemorySink {
    fn record(&self, event: SecurityEvent) {
        self.events.lock().expect("sink lock poisoned").push(event);
    }
}
