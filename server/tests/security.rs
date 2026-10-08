//! STORY-0021 E2: the peer port refuses connections without a valid cluster certificate and
//! records each refusal (REQ-0036 AC1).

use std::sync::Arc;
use std::time::Duration;

use dscore_server::net::listener::serve;
use dscore_server::net::tls::{Identity, peer_client_config, peer_server_config, provider};
use dscore_server::security::{MemorySink, Port, RejectReason, SecurityEvent, SecurityEventSink};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose, date_time_ymd,
};
use rustls::ClientConfig;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsConnector};

fn ca(name: &str) -> CertifiedIssuer<'static, KeyPair> {
    let mut p = CertificateParams::new(Vec::<String>::new()).unwrap();
    p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    p.distinguished_name.push(DnType::CommonName, name);
    p.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    CertifiedIssuer::self_signed(p, KeyPair::generate().unwrap()).unwrap()
}

fn node(ca: &CertifiedIssuer<'static, KeyPair>, name: &str, expired: bool) -> Identity {
    let key = KeyPair::generate().unwrap();
    let mut p = CertificateParams::new(vec![name.to_string(), "localhost".to_string()]).unwrap();
    p.distinguished_name.push(DnType::CommonName, name);
    p.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    if expired {
        p.not_before = date_time_ymd(2019, 1, 1);
        p.not_after = date_time_ymd(2020, 1, 1);
    }
    let cert = p.signed_by(&key, ca).unwrap();
    Identity {
        cert_chain: vec![cert.der().clone()],
        key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
    }
}

/// Connect, then read until EOF. A refused handshake surfaces as an error or as no data.
async fn attempt(
    addr: std::net::SocketAddr,
    cfg: Arc<ClientConfig>,
) -> Result<Vec<u8>, std::io::Error> {
    let tcp = TcpStream::connect(addr).await?;
    let mut tls = TlsConnector::from(cfg)
        .connect(ServerName::try_from("localhost").unwrap(), tcp)
        .await?;
    let mut buf = Vec::new();
    tls.read_to_end(&mut buf).await?;
    Ok(buf)
}

async fn wait_for_events(sink: &MemorySink, n: usize) -> Vec<SecurityEvent> {
    for _ in 0..200 {
        let ev = sink.events();
        if ev.len() >= n {
            return ev;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    sink.events()
}

// reqforge: verifies REQ-0036#AC1
#[tokio::test]
async fn mtls_reject_invalid_peer() {
    let cluster = ca("dscore cluster CA");
    let other = ca("some other CA");
    let cluster_roots: Vec<CertificateDer<'static>> = vec![cluster.der().clone()];

    let sink = Arc::new(MemorySink::default());
    let server_cfg = peer_server_config(node(&cluster, "node1", false), &cluster_roots).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let events: Arc<dyn SecurityEventSink> = sink.clone();
    tokio::spawn(serve(
        listener,
        TlsAcceptor::from(server_cfg),
        Port::Peer,
        events,
        |mut s| async move {
            let _ = s.write_all(b"ok").await;
            let _ = s.shutdown().await;
        },
    ));

    // A node with a valid cluster certificate is accepted.
    let valid = peer_client_config(node(&cluster, "node2", false), &cluster_roots).unwrap();
    assert_eq!(attempt(addr, valid).await.unwrap(), b"ok");
    assert!(
        sink.events().is_empty(),
        "a valid peer must not be logged as rejected"
    );

    // No certificate.
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cluster.der().clone()).unwrap();
    let no_cert = Arc::new(
        ClientConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );
    // Expired certificate from the cluster CA, and a valid certificate from another CA.
    let expired = peer_client_config(node(&cluster, "node3", true), &cluster_roots).unwrap();
    let foreign = peer_client_config(node(&other, "rogue", false), &cluster_roots).unwrap();

    let cases = [
        (no_cert, RejectReason::NoCertificate),
        (expired, RejectReason::ExpiredCertificate),
        (foreign, RejectReason::UntrustedIssuer),
    ];
    for (i, (cfg, want)) in cases.into_iter().enumerate() {
        let got = attempt(addr, cfg).await;
        assert!(
            !matches!(&got, Ok(data) if data == b"ok"),
            "{want:?}: connection was accepted"
        );
        let events = wait_for_events(&sink, i + 1).await;
        assert_eq!(events.len(), i + 1, "{want:?}: refusal was not recorded");
        let SecurityEvent::TlsRejected { port, reason, .. } = &events[i];
        assert_eq!((*port, *reason), (Port::Peer, want));
    }
}
