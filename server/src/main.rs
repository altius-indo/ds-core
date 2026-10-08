//! dscore-server entry point.
//!
//!   dscore-server serve --client-addr 0.0.0.0:7400 --peer-addr 0.0.0.0:7401 \
//!       --tls-cert node.pem --tls-key node.key --cluster-ca ca.pem
//!
//! Binds the TLS client port and the mutual-TLS peer port (REQ-0035, REQ-0036). Request
//! handling arrives with the client protocol (TASK-0017); for now an accepted connection is
//! greeted and closed.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use dscore_server::net::listener::serve;
use dscore_server::net::tls::{Identity, client_port_config, load_ca_pem, peer_server_config};
use dscore_server::security::{Port, SecurityEventSink, StderrSink};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

struct ServeArgs {
    client_addr: SocketAddr,
    peer_addr: SocketAddr,
    tls_cert: PathBuf,
    tls_key: PathBuf,
    cluster_ca: PathBuf,
}

fn parse_serve(mut args: impl Iterator<Item = String>) -> Result<ServeArgs, String> {
    let (mut client, mut peer, mut cert, mut key, mut ca) = (None, None, None, None, None);
    while let Some(flag) = args.next() {
        let value = args.next().ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--client-addr" => {
                client = Some(value.parse().map_err(|e| format!("--client-addr: {e}"))?)
            }
            "--peer-addr" => peer = Some(value.parse().map_err(|e| format!("--peer-addr: {e}"))?),
            "--tls-cert" => cert = Some(PathBuf::from(value)),
            "--tls-key" => key = Some(PathBuf::from(value)),
            "--cluster-ca" => ca = Some(PathBuf::from(value)),
            other => return Err(format!("unknown flag {other}")),
        }
    }
    Ok(ServeArgs {
        client_addr: client.ok_or("--client-addr is required")?,
        peer_addr: peer.ok_or("--peer-addr is required")?,
        tls_cert: cert.ok_or("--tls-cert is required")?,
        tls_key: key.ok_or("--tls-key is required")?,
        cluster_ca: ca.ok_or("--cluster-ca is required")?,
    })
}

async fn run(a: ServeArgs) -> Result<(), Box<dyn std::error::Error>> {
    let identity = Identity::from_pem_files(&a.tls_cert, &a.tls_key)?;
    let ca = load_ca_pem(&a.cluster_ca)?;
    let client_tls = TlsAcceptor::from(client_port_config(identity.clone())?);
    let peer_tls = TlsAcceptor::from(peer_server_config(identity, &ca)?);
    let client = TcpListener::bind(a.client_addr).await?;
    let peer = TcpListener::bind(a.peer_addr).await?;
    println!(
        "listening client={} peer={}",
        client.local_addr()?,
        peer.local_addr()?
    );

    let events: Arc<dyn SecurityEventSink> = Arc::new(StderrSink);
    let greet = |mut s: tokio_rustls::server::TlsStream<tokio::net::TcpStream>| async move {
        let _ = s.write_all(b"dscore\n").await;
        let _ = s.shutdown().await;
    };
    tokio::select! {
        r = serve(client, client_tls, Port::Client, events.clone(), greet) => r?,
        r = serve(peer, peer_tls, Port::Peer, events, greet) => r?,
        _ = tokio::signal::ctrl_c() => {}
    }
    Ok(())
}

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("serve") => {
            let parsed = match parse_serve(args) {
                Ok(a) => a,
                Err(e) => {
                    eprintln!("dscore-server serve: {e}");
                    return ExitCode::from(2);
                }
            };
            let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
            match rt.block_on(run(parsed)) {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("dscore-server: {e}");
                    ExitCode::FAILURE
                }
            }
        }
        Some("--version") | None => {
            println!("dscore-server {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        Some(other) => {
            eprintln!("dscore-server: unknown command {other}; try `serve` or `--version`");
            ExitCode::from(2)
        }
    }
}
