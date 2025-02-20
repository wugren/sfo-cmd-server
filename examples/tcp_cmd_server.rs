use std::any::Any;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use as_any::AsAny;
use rcgen::{generate_simple_self_signed};
use rustls::crypto::{ring};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, UnixTime};
use rustls::{DigitallySignedStruct, DistinguishedName, Error, ServerConfig, SignatureScheme};
use rustls::client::danger::HandshakeSignatureValid;
use rustls::version::TLS13;
use tokio::net::TcpListener;
use sfo_cmd_server::{CmdHeader, CmdTunnel, CmdTunnelRead, CmdTunnelWrite, PeerId};
use sfo_cmd_server::errors::{into_cmd_err, CmdErrorCode, CmdResult};
use rustls::pki_types::pem::PemObject;
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use sha2::Digest;
use tokio::io::{split, AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::TlsAcceptor;
use sfo_cmd_server::server::{CmdServer, CmdTunnelListener, DefaultCmdServer};

struct TlsStreamRead {
    remote_id: PeerId,
    read: Option<tokio::io::ReadHalf<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>>,
}

impl TlsStreamRead {
    pub fn new(remote_id: PeerId, read: tokio::io::ReadHalf<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>) -> Self {
        Self { remote_id, read: Some(read) }
    }
}

impl AsyncRead for TlsStreamRead {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if let Some(read) = this.read.as_mut() {
            Pin::new(read).poll_read(cx, buf)
        } else {
            Poll::Ready(Ok(()))
        }
    }
}


impl CmdTunnelRead for TlsStreamRead {
    fn get_remote_peer_id(&self) -> PeerId {
        self.remote_id.clone()
    }
}

struct TlsStreamWrite {
    remote_id: PeerId,
    write: Option<tokio::io::WriteHalf<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>>,
}

impl TlsStreamWrite {
    pub fn new(remote_id: PeerId, write: tokio::io::WriteHalf<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>) -> Self {
        Self { remote_id, write: Some(write) }
    }
}

impl AsyncWrite for TlsStreamWrite {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        if let Some(write) = this.write.as_mut() {
            Pin::new(write).poll_write(cx, buf)
        } else {
            Poll::Ready(Ok(0))
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if let Some(write) = this.write.as_mut() {
            Pin::new(write).poll_flush(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if let Some(write) = this.write.as_mut() {
            Pin::new(write).poll_shutdown(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

impl CmdTunnelWrite for TlsStreamWrite {
    fn get_remote_peer_id(&self) -> PeerId {
        self.remote_id.clone()
    }
}

pub struct TlsClientCertVerifier {
    pub subjects: Vec<DistinguishedName>,
}

impl Debug for TlsClientCertVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsClientCertVerifier")
            .finish()
    }
}

impl ClientCertVerifier for TlsClientCertVerifier {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        self.subjects.as_slice()
    }

    fn verify_client_cert(&self, end_entity: &CertificateDer<'_>, intermediates: &[CertificateDer<'_>], now: UnixTime) -> Result<ClientCertVerified, Error> {
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![SignatureScheme::ECDSA_NISTP256_SHA256]
    }
}
struct TunnelListener {
    tls_acceptor: TlsAcceptor,
    tcp_listener: TcpListener,
}

fn generate_cert() -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
    let subject_alt_names = vec!["127.0.0.1".to_string()];
    let cert_key = generate_simple_self_signed(subject_alt_names).unwrap();
    (vec![CertificateDer::from_pem_slice(cert_key.cert.pem().as_bytes()).unwrap()], PrivateKeyDer::from_pem_slice(cert_key.key_pair.serialize_pem().as_bytes()).unwrap())
}

impl TunnelListener {
    pub async fn bind(addr: &str) -> CmdResult<Self> {
        let listener = TcpListener::bind(addr).await.map_err(into_cmd_err!(CmdErrorCode::IoError, "bind failed"))?;
        let (certs, key) = generate_cert();
        let config = ServerConfig::builder_with_provider(ring::default_provider().into())
            .with_protocol_versions(&[&TLS13]).unwrap()
            .with_client_cert_verifier(Arc::new(TlsClientCertVerifier { subjects: vec![] }))
            .with_single_cert(certs, key)
            .map_err(into_cmd_err!(CmdErrorCode::TlsError, "create tls config failed"))?;
        Ok(Self {
            tls_acceptor: TlsAcceptor::from(Arc::new(config)),
            tcp_listener: listener,
        })
    }
}
#[async_trait::async_trait]
impl CmdTunnelListener<TlsStreamRead, TlsStreamWrite> for TunnelListener {
    async fn accept(&self) -> sfo_cmd_server::errors::CmdResult<CmdTunnel<TlsStreamRead, TlsStreamWrite>> {
        let (stream, _) = self.tcp_listener.accept().await.map_err(into_cmd_err!(CmdErrorCode::IoError, "accept failed"))?;
        let tls_stream = self.tls_acceptor.accept(stream).await.map_err(into_cmd_err!(CmdErrorCode::TlsError, "tls accept failed"))?;
        let tls_key = tls_stream.get_ref().1.peer_certificates().unwrap().get(0).unwrap().to_vec();
        let mut sha256 = sha2::Sha256::new();
        sha256.update(tls_key.as_slice());
        let peer_id = PeerId::from(sha256.finalize().as_slice().to_vec());
        let (r, w) = split(tls_stream);
        Ok(CmdTunnel::new(TlsStreamRead::new(peer_id.clone(), r), TlsStreamWrite::new(peer_id, w)))
    }
}

#[tokio::main]
async fn main() {
    let listener = TunnelListener::bind("127.0.0.1:4453").await.unwrap();
    let server = DefaultCmdServer::<TlsStreamRead, TlsStreamWrite, u16, u8, _>::new(listener);
    let sender = server.clone();
    server.register_cmd_handler(0x01, move |peer_id, tunnel_id, header: CmdHeader<u16, u8>, body_read| {
        let sender = sender.clone();
        async move {
            println!("recv cmd {}", header.cmd_code());
            sender.send(&peer_id, 0x02, "server".as_bytes()).await
        }
    });
    server.start();
    tokio::signal::ctrl_c().await.unwrap();
}
