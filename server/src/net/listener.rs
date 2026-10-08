//! TLS accept loop. Every connection on a client or peer port must complete a TLS handshake
//! within `HANDSHAKE_TIMEOUT`; anything else, plaintext included, is dropped and recorded as a
//! security event (REQ-0035 AC2, REQ-0036 AC1).

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;

use super::tls::reject_reason;
use crate::security::{Port, RejectReason, SecurityEvent, SecurityEventSink};

pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Accept connections forever, handing each established TLS stream to `handler`.
pub async fn serve<H, F>(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    port: Port,
    events: Arc<dyn SecurityEventSink>,
    handler: H,
) -> std::io::Result<()>
where
    H: Fn(TlsStream<TcpStream>) -> F + Send + Sync + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    let handler = Arc::new(handler);
    loop {
        let (tcp, remote) = listener.accept().await?;
        let acceptor = acceptor.clone();
        let events = events.clone();
        let handler = handler.clone();
        tokio::spawn(async move {
            match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(tcp)).await {
                Ok(Ok(stream)) => handler(stream).await,
                Ok(Err(e)) => events.record(SecurityEvent::TlsRejected {
                    port,
                    remote,
                    reason: reject_reason(&e),
                    detail: e.to_string(),
                }),
                Err(_) => events.record(SecurityEvent::TlsRejected {
                    port,
                    remote,
                    reason: RejectReason::Timeout,
                    detail: "handshake timed out".into(),
                }),
            }
        });
    }
}
