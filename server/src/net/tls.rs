//! TLS for the client port and mutual TLS for the peer port.
//!
//! - Crypto comes from aws-lc (via rustls' aws-lc-rs provider), the allow-listed native
//!   library (deny.native.toml, DEC-0011).
//! - Only TLS 1.3 and TLS 1.2 with AEAD cipher suites are offered (REQ-0035).
//! - The peer port requires a client certificate issued by the cluster CA; no certificate, an
//!   expired one or one from another CA fails the handshake (REQ-0036).

// reqforge: implements REQ-0035
// reqforge: implements REQ-0036

use std::fmt;
use std::io;
use std::path::Path;
use std::sync::Arc;

use rustls::crypto::CryptoProvider;
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig, SupportedCipherSuite};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

use crate::security::RejectReason;

/// A certificate chain and its private key.
#[derive(Debug)]
pub struct Identity {
    pub cert_chain: Vec<CertificateDer<'static>>,
    pub key: PrivateKeyDer<'static>,
}

impl Clone for Identity {
    fn clone(&self) -> Self {
        Self {
            cert_chain: self.cert_chain.clone(),
            key: self.key.clone_key(),
        }
    }
}

impl Identity {
    pub fn from_pem_files(cert: &Path, key: &Path) -> Result<Self, TlsError> {
        let cert_chain = CertificateDer::pem_file_iter(cert)
            .and_then(|it| it.collect::<Result<Vec<_>, _>>())
            .map_err(|e| TlsError(format!("{}: {e}", cert.display())))?;
        if cert_chain.is_empty() {
            return Err(TlsError(format!("{}: no certificates", cert.display())));
        }
        let key = PrivateKeyDer::from_pem_file(key)
            .map_err(|e| TlsError(format!("{}: {e}", key.display())))?;
        Ok(Self { cert_chain, key })
    }
}

pub fn load_ca_pem(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let certs = CertificateDer::pem_file_iter(path)
        .and_then(|it| it.collect::<Result<Vec<_>, _>>())
        .map_err(|e| TlsError(format!("{}: {e}", path.display())))?;
    if certs.is_empty() {
        return Err(TlsError(format!("{}: no CA certificates", path.display())));
    }
    Ok(certs)
}

fn is_aead(suite: &SupportedCipherSuite) -> bool {
    let name = format!("{:?}", suite.suite());
    name.contains("_GCM_") || name.contains("CHACHA20_POLY1305")
}

/// The aws-lc-rs provider restricted to AEAD cipher suites.
pub fn provider() -> Arc<CryptoProvider> {
    let mut p = rustls::crypto::aws_lc_rs::default_provider();
    p.cipher_suites.retain(is_aead);
    Arc::new(p)
}

const VERSIONS: &[&rustls::SupportedProtocolVersion] =
    &[&rustls::version::TLS13, &rustls::version::TLS12];

fn roots(ca: &[CertificateDer<'static>]) -> Result<Arc<RootCertStore>, TlsError> {
    let mut store = RootCertStore::empty();
    for c in ca {
        store
            .add(c.clone())
            .map_err(|e| TlsError(format!("cluster CA: {e}")))?;
    }
    Ok(Arc::new(store))
}

/// Client port: server authentication only. Client identity is established after the
/// handshake (SCRAM, x.509 or OIDC; TASK-0034).
pub fn client_port_config(identity: Identity) -> Result<Arc<ServerConfig>, TlsError> {
    let cfg = ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(VERSIONS)
        .map_err(|e| TlsError(e.to_string()))?
        .with_no_client_auth()
        .with_single_cert(identity.cert_chain, identity.key)
        .map_err(|e| TlsError(e.to_string()))?;
    Ok(Arc::new(cfg))
}

/// Peer port: both sides present certificates issued by the cluster CA.
pub fn peer_server_config(
    identity: Identity,
    cluster_ca: &[CertificateDer<'static>],
) -> Result<Arc<ServerConfig>, TlsError> {
    let provider = provider();
    let verifier =
        WebPkiClientVerifier::builder_with_provider(roots(cluster_ca)?, provider.clone())
            .build()
            .map_err(|e| TlsError(e.to_string()))?;
    let cfg = ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(VERSIONS)
        .map_err(|e| TlsError(e.to_string()))?
        .with_client_cert_verifier(verifier)
        .with_single_cert(identity.cert_chain, identity.key)
        .map_err(|e| TlsError(e.to_string()))?;
    Ok(Arc::new(cfg))
}

/// Outbound peer connections: present this node's certificate, trust only the cluster CA.
pub fn peer_client_config(
    identity: Identity,
    cluster_ca: &[CertificateDer<'static>],
) -> Result<Arc<ClientConfig>, TlsError> {
    let cfg = ClientConfig::builder_with_provider(provider())
        .with_protocol_versions(VERSIONS)
        .map_err(|e| TlsError(e.to_string()))?
        .with_root_certificates(roots(cluster_ca)?)
        .with_client_auth_cert(identity.cert_chain, identity.key)
        .map_err(|e| TlsError(e.to_string()))?;
    Ok(Arc::new(cfg))
}

/// Classify a failed server-side handshake for the security event log.
pub fn reject_reason(err: &io::Error) -> RejectReason {
    use rustls::{CertificateError, Error};
    match err.get_ref().and_then(|e| e.downcast_ref::<Error>()) {
        Some(Error::NoCertificatesPresented) => RejectReason::NoCertificate,
        Some(Error::InvalidCertificate(c)) => match c {
            CertificateError::Expired | CertificateError::ExpiredContext { .. } => {
                RejectReason::ExpiredCertificate
            }
            CertificateError::UnknownIssuer | CertificateError::BadSignature => {
                RejectReason::UntrustedIssuer
            }
            _ => RejectReason::BadCertificate,
        },
        Some(_) => RejectReason::ProtocolViolation,
        None if err.kind() == io::ErrorKind::TimedOut => RejectReason::Timeout,
        None => RejectReason::Other,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsError(pub String);

impl fmt::Display for TlsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "tls: {}", self.0)
    }
}

impl std::error::Error for TlsError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_aead_suites_offered() {
        let p = provider();
        assert!(!p.cipher_suites.is_empty());
        assert!(p.cipher_suites.iter().all(is_aead));
    }
}
