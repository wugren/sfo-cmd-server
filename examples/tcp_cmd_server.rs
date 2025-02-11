use std::any::Any;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
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
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::TlsAcceptor;
use sfo_cmd_server::server::{CmdServer, CmdTunnelListener, DefaultCmdServer};

struct TlsStreamRead {
    read: Option<tokio::io::ReadHalf<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>>,
}

impl TlsStreamRead {
    pub fn new(read: tokio::io::ReadHalf<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>) -> Self {
        Self { read: Some(read) }
    }

    pub fn take(&mut self) -> Option<tokio::io::ReadHalf<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>> {
        self.read.take()
    }
}

impl CmdTunnelRead for TlsStreamRead {
    fn get_any(&self) -> &dyn Any {
        self
    }

    fn get_any_mut(&mut self) -> &mut dyn Any {
        self
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

struct TlsStreamWrite {
    write: Option<tokio::io::WriteHalf<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>>,
}

impl TlsStreamWrite {
    pub fn new(write: tokio::io::WriteHalf<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>) -> Self {
        Self { write: Some(write) }
    }

    pub fn take(&mut self) -> Option<tokio::io::WriteHalf<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>> {
        self.write.take()
    }
}

impl CmdTunnelWrite for TlsStreamWrite {
    fn get_any(&self) -> &dyn Any {
        self
    }

    fn get_any_mut(&mut self) -> &mut dyn Any {
        self
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

struct TlsConnection {
    tls_key: Vec<u8>,
    stream: Mutex<Option<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>>,
}

impl TlsConnection {
    pub fn new(stream: tokio_rustls::server::TlsStream<tokio::net::TcpStream>) -> Self {
        let tls_key = stream.get_ref().1.peer_certificates().unwrap().get(0).unwrap().to_vec();
        Self { tls_key, stream: Mutex::new(Some(stream)) }
    }
}

#[async_trait::async_trait]
impl CmdTunnel for TlsConnection {
    fn get_remote_peer_id(&self) -> PeerId {
        let mut sha256 = sha2::Sha256::new();
        sha256.update(self.tls_key.as_slice());
        PeerId::from(sha256.finalize().as_slice().to_vec())
    }

    fn split(&self) -> CmdResult<(Box<dyn CmdTunnelRead>, Box<dyn CmdTunnelWrite>)> {
        let stream = self.stream.lock().unwrap().take().unwrap();
        let (read, write) = tokio::io::split(stream);
        Ok((Box::new(TlsStreamRead::new(read)), Box::new(TlsStreamWrite::new(write))))
    }

    fn unsplit(&self, mut read: Box<dyn CmdTunnelRead>, mut write: Box<dyn CmdTunnelWrite>) -> CmdResult<()> {
        let mut socket = self.stream.lock().unwrap();
        let read = unsafe { std::mem::transmute::<_, &mut dyn Any>(read.get_any_mut())};
        if let Some(read) = read.downcast_mut::<TlsStreamRead>() {
            if let Some(read) = read.take() {
                let write = unsafe { std::mem::transmute::<_, &mut dyn Any>(write.get_any_mut())};
                if let Some(write) = write.downcast_mut::<TlsStreamWrite>() {
                    if let Some(write) = write.take() {
                        *socket = Some(read.unsplit(write));
                    }
                }
            }
        }
        Ok(())
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
impl CmdTunnelListener<TlsConnection> for TunnelListener {
    async fn accept(&self) -> sfo_cmd_server::errors::CmdResult<Arc<TlsConnection>> {
        let (stream, _) = self.tcp_listener.accept().await.map_err(into_cmd_err!(CmdErrorCode::IoError, "accept failed"))?;
        let tls_stream = self.tls_acceptor.accept(stream).await.map_err(into_cmd_err!(CmdErrorCode::TlsError, "tls accept failed"))?;
        Ok(Arc::new(TlsConnection::new(tls_stream)))
    }
}

#[tokio::main]
async fn main() {
    let listener = TunnelListener::bind("127.0.0.1:4453").await.unwrap();
    let server = DefaultCmdServer::<u16, u8, TlsConnection, _>::new(listener);
    let sender = server.clone();
    server.register_cmd_handler(0x01, move |peer_id, tunnel_id, header: CmdHeader<u16, u8>, body_read| {
        let sender = sender.clone();
        async move {
            println!("recv cmd {}", header.cmd_code());
            sender.send(&peer_id, 0x02, vec![].as_slice()).await
        }
    });
    server.start();
    tokio::signal::ctrl_c().await.unwrap();
}
