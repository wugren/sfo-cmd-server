use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use rcgen::generate_simple_self_signed;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::{ClientConfig, DigitallySignedStruct, Error, SignatureScheme};
use rustls::crypto::ring;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::pki_types::pem::PemObject;
use rustls::version::TLS13;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::TlsConnector;
use sfo_cmd_server::{CmdHeader, CmdTunnel, CmdTunnelRead, CmdTunnelRef, CmdTunnelWrite};
use sfo_cmd_server::client::{CmdClient, CmdTunnelFactory};
use sfo_cmd_server::errors::{into_cmd_err, CmdErrorCode, CmdResult};

struct TlsStreamRead {
    read: Option<tokio::io::ReadHalf<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>>,
}

impl TlsStreamRead {
    pub fn new(read: tokio::io::ReadHalf<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>) -> Self {
        Self { read: Some(read) }
    }

    pub fn take(&mut self) -> Option<tokio::io::ReadHalf<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>> {
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
    write: Option<tokio::io::WriteHalf<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>>,
}

impl TlsStreamWrite {
    pub fn new(write: tokio::io::WriteHalf<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>) -> Self {
        Self { write: Some(write) }
    }

    pub fn take(&mut self) -> Option<tokio::io::WriteHalf<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>> {
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
    stream: Mutex<Option<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>>,
}

impl TlsConnection {
    pub fn new(stream: tokio_rustls::client::TlsStream<tokio::net::TcpStream>) -> Self {
        let tls_key = stream.get_ref().1.peer_certificates().unwrap().get(0).unwrap().to_vec();
        Self { tls_key, stream: Mutex::new(Some(stream)) }
    }
}

#[async_trait::async_trait]
impl CmdTunnel for TlsConnection {
    fn get_tls_key(&self) -> Vec<u8> {
        self.tls_key.clone()
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

fn generate_cert() -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
    let subject_alt_names = vec!["127.0.0.1".to_string()];
    let cert_key = generate_simple_self_signed(subject_alt_names).unwrap();
    (vec![CertificateDer::from_pem_slice(cert_key.cert.pem().as_bytes()).unwrap()], PrivateKeyDer::from_pem_slice(cert_key.key_pair.serialize_pem().as_bytes()).unwrap())
}

pub struct TlsServerCertVerifier {

}

impl Debug for TlsServerCertVerifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("TlsServerCertVerifier")
    }
}

impl ServerCertVerifier for TlsServerCertVerifier {
    fn verify_server_cert(&self, end_entity: &CertificateDer<'_>, intermediates: &[CertificateDer<'_>], server_name: &ServerName<'_>, ocsp_response: &[u8], now: UnixTime) -> Result<ServerCertVerified, Error> {
        Ok(ServerCertVerified::assertion())
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
pub struct TlsConnectionFactory {
    tls_connector: TlsConnector,
}

impl TlsConnectionFactory {
    pub fn new() -> Self {
        let (certs, key) = generate_cert();
        let config = ClientConfig::builder_with_provider(ring::default_provider().into())
            .with_protocol_versions(&[&TLS13]).unwrap()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(TlsServerCertVerifier {}))
            .with_client_auth_cert(certs, key).unwrap();
        Self {
            tls_connector: TlsConnector::from(Arc::new(config))
        }
    }
}

#[async_trait::async_trait]
impl CmdTunnelFactory for TlsConnectionFactory {
    async fn create_tunnel(&self) -> CmdResult<CmdTunnelRef> {
        let socket = tokio::net::TcpStream::connect("127.0.0.1:4453").await.map_err(into_cmd_err!(CmdErrorCode::IoError, "connect to server failed"))?;
        let tls_stream = self.tls_connector.connect("127.0.0.1".to_string().try_into().unwrap(), socket).await.map_err(into_cmd_err!(CmdErrorCode::IoError, "tls handshake failed"))?;
        Ok(Arc::new(TlsConnection::new(tls_stream)))
    }
}
#[tokio::main]
async fn main() {
    let client = CmdClient::<_, u16, u8>::new(TlsConnectionFactory::new(), 5);

    let sender = client.clone();
    client.attach_cmd_handler(0x02, move |peer_id, header: CmdHeader<u16, u8>, body| {
        let sender = sender.clone();
        async move {
            println!("recv cmd {}", header.cmd_code());
            Ok(())
        }
    });

    let mut send = client.get_send().await.unwrap();
    send.send::<u16, u8>(0x01, vec![].as_slice()).await.unwrap();

    tokio::time::sleep(Duration::from_secs(1)).await;
}
