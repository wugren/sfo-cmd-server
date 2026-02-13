use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use sfo_cmd_server::client::{
    ClassifiedCmdClient, ClassifiedCmdTunnelFactory, ClassifiedCmdTunnelRead,
    ClassifiedCmdTunnelWrite, CmdClient, CmdSend, CmdTunnelFactory, DefaultClassifiedCmdClient,
    DefaultCmdClient,
};
use sfo_cmd_server::errors::{CmdErrorCode, CmdResult, cmd_err};
use sfo_cmd_server::server::{CmdServer, CmdTunnelListener, DefaultCmdServer};
use sfo_cmd_server::{
    ClassifiedCmdNode, ClassifiedCmdNodeTunnelFactory, CmdBody, CmdHeader, CmdNode,
    CmdNodeTunnelClassification, CmdNodeTunnelFactory, CmdTunnel, DefaultClassifiedCmdNode,
    DefaultCmdNode, PeerId, TunnelId,
};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf, split};
use tokio::sync::{Mutex, mpsc};
use tokio::time::timeout;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum TestClass {
    Alpha,
    Beta,
}

fn peer(seed: u8) -> PeerId {
    PeerId::from(vec![seed; 32])
}

struct MockRead {
    remote_id: PeerId,
    read: tokio::io::ReadHalf<DuplexStream>,
}

impl AsyncRead for MockRead {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.read).poll_read(cx, buf)
    }
}

impl sfo_cmd_server::CmdTunnelRead<()> for MockRead {
    fn get_remote_peer_id(&self) -> PeerId {
        self.remote_id.clone()
    }
}

struct MockWrite {
    remote_id: PeerId,
    write: tokio::io::WriteHalf<DuplexStream>,
}

impl AsyncWrite for MockWrite {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.write).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.write).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.write).poll_shutdown(cx)
    }
}

impl sfo_cmd_server::CmdTunnelWrite<()> for MockWrite {
    fn get_remote_peer_id(&self) -> PeerId {
        self.remote_id.clone()
    }
}

struct MockClassifiedRead {
    remote_id: PeerId,
    class: TestClass,
    read: tokio::io::ReadHalf<DuplexStream>,
}

impl AsyncRead for MockClassifiedRead {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.read).poll_read(cx, buf)
    }
}

impl sfo_cmd_server::CmdTunnelRead<()> for MockClassifiedRead {
    fn get_remote_peer_id(&self) -> PeerId {
        self.remote_id.clone()
    }
}

impl ClassifiedCmdTunnelRead<TestClass, ()> for MockClassifiedRead {
    fn get_classification(&self) -> TestClass {
        self.class
    }
}

struct MockClassifiedWrite {
    remote_id: PeerId,
    class: TestClass,
    write: tokio::io::WriteHalf<DuplexStream>,
}

impl AsyncWrite for MockClassifiedWrite {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.write).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.write).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.write).poll_shutdown(cx)
    }
}

impl sfo_cmd_server::CmdTunnelWrite<()> for MockClassifiedWrite {
    fn get_remote_peer_id(&self) -> PeerId {
        self.remote_id.clone()
    }
}

impl ClassifiedCmdTunnelWrite<TestClass, ()> for MockClassifiedWrite {
    fn get_classification(&self) -> TestClass {
        self.class
    }
}

type NormalTunnel = CmdTunnel<MockRead, MockWrite>;
type ClassifiedTunnel = CmdTunnel<MockClassifiedRead, MockClassifiedWrite>;

#[derive(Clone)]
struct NormalEndpoint {
    rx: std::sync::Arc<Mutex<mpsc::Receiver<NormalTunnel>>>,
}

impl NormalEndpoint {
    fn new() -> (Self, mpsc::Sender<NormalTunnel>) {
        let (tx, rx) = mpsc::channel(16);
        (
            Self {
                rx: std::sync::Arc::new(Mutex::new(rx)),
            },
            tx,
        )
    }

    async fn next_tunnel(&self) -> CmdResult<NormalTunnel> {
        self.rx
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| cmd_err!(CmdErrorCode::Failed, "mock tunnel queue closed"))
    }
}

#[async_trait::async_trait]
impl CmdTunnelListener<(), MockRead, MockWrite> for NormalEndpoint {
    async fn accept(&self) -> CmdResult<NormalTunnel> {
        self.next_tunnel().await
    }
}

#[async_trait::async_trait]
impl CmdTunnelFactory<(), MockRead, MockWrite> for NormalEndpoint {
    async fn create_tunnel(&self) -> CmdResult<NormalTunnel> {
        self.next_tunnel().await
    }
}

#[async_trait::async_trait]
impl CmdNodeTunnelFactory<(), MockRead, MockWrite> for NormalEndpoint {
    async fn create_tunnel(&self, _remote_id: &PeerId) -> CmdResult<NormalTunnel> {
        self.next_tunnel().await
    }
}

#[derive(Clone)]
struct ClassifiedEndpoint {
    rx: std::sync::Arc<Mutex<mpsc::Receiver<ClassifiedTunnel>>>,
}

impl ClassifiedEndpoint {
    fn new() -> (Self, mpsc::Sender<ClassifiedTunnel>) {
        let (tx, rx) = mpsc::channel(16);
        (
            Self {
                rx: std::sync::Arc::new(Mutex::new(rx)),
            },
            tx,
        )
    }

    async fn next_tunnel(&self) -> CmdResult<ClassifiedTunnel> {
        self.rx
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| cmd_err!(CmdErrorCode::Failed, "mock classified tunnel queue closed"))
    }
}

#[async_trait::async_trait]
impl CmdTunnelListener<(), MockClassifiedRead, MockClassifiedWrite> for ClassifiedEndpoint {
    async fn accept(&self) -> CmdResult<ClassifiedTunnel> {
        self.next_tunnel().await
    }
}

#[async_trait::async_trait]
impl ClassifiedCmdTunnelFactory<TestClass, (), MockClassifiedRead, MockClassifiedWrite>
    for ClassifiedEndpoint
{
    async fn create_tunnel(
        &self,
        classification: Option<TestClass>,
    ) -> CmdResult<ClassifiedTunnel> {
        let tunnel = self.next_tunnel().await?;
        if let Some(expect) = classification {
            if tunnel.get_classification() != expect {
                return Err(cmd_err!(
                    CmdErrorCode::InvalidParam,
                    "classification mismatch"
                ));
            }
        }
        Ok(tunnel)
    }
}

#[async_trait::async_trait]
impl ClassifiedCmdNodeTunnelFactory<TestClass, (), MockClassifiedRead, MockClassifiedWrite>
    for ClassifiedEndpoint
{
    async fn create_tunnel(
        &self,
        classification: Option<CmdNodeTunnelClassification<TestClass>>,
    ) -> CmdResult<ClassifiedTunnel> {
        let tunnel = self.next_tunnel().await?;
        if let Some(expect) = classification.and_then(|c| c.classification) {
            if tunnel.get_classification() != expect {
                return Err(cmd_err!(
                    CmdErrorCode::InvalidParam,
                    "node classification mismatch"
                ));
            }
        }
        Ok(tunnel)
    }
}

async fn wire_normal_connection(
    server_tx: &mpsc::Sender<NormalTunnel>,
    client_tx: &mpsc::Sender<NormalTunnel>,
    client_id: PeerId,
    server_id: PeerId,
) {
    let (client_stream, server_stream) = tokio::io::duplex(8 * 1024);
    let (client_read, client_write) = split(client_stream);
    let (server_read, server_write) = split(server_stream);

    let client_tunnel = CmdTunnel::new(
        MockRead {
            remote_id: server_id.clone(),
            read: client_read,
        },
        MockWrite {
            remote_id: server_id,
            write: client_write,
        },
    );
    let server_tunnel = CmdTunnel::new(
        MockRead {
            remote_id: client_id.clone(),
            read: server_read,
        },
        MockWrite {
            remote_id: client_id,
            write: server_write,
        },
    );

    server_tx.send(server_tunnel).await.unwrap();
    client_tx.send(client_tunnel).await.unwrap();
}

async fn wire_classified_connection(
    server_tx: &mpsc::Sender<ClassifiedTunnel>,
    client_tx: &mpsc::Sender<ClassifiedTunnel>,
    class: TestClass,
    client_id: PeerId,
    server_id: PeerId,
) {
    let (client_stream, server_stream) = tokio::io::duplex(8 * 1024);
    let (client_read, client_write) = split(client_stream);
    let (server_read, server_write) = split(server_stream);

    let client_tunnel = CmdTunnel::new(
        MockClassifiedRead {
            remote_id: server_id.clone(),
            class,
            read: client_read,
        },
        MockClassifiedWrite {
            remote_id: server_id,
            class,
            write: client_write,
        },
    );
    let server_tunnel = CmdTunnel::new(
        MockClassifiedRead {
            remote_id: client_id.clone(),
            class,
            read: server_read,
        },
        MockClassifiedWrite {
            remote_id: client_id,
            class,
            write: server_write,
        },
    );

    server_tx.send(server_tunnel).await.unwrap();
    client_tx.send(client_tunnel).await.unwrap();
}

async fn wait_until_true<F, Fut>(mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    tokio::time::timeout(Duration::from_secs(2), async move {
        loop {
            if check().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("condition not met in time");
}

async fn recv_text(rx: &mut mpsc::UnboundedReceiver<String>) -> String {
    timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("recv timeout")
        .expect("channel closed")
}

async fn recv_tunnel_id(rx: &mut mpsc::UnboundedReceiver<TunnelId>) -> TunnelId {
    timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("recv tunnel id timeout")
        .expect("tunnel id channel closed")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_client_server_bidirectional_request_response_and_timeout() {
    let client_id = peer(1);
    let server_id = peer(2);

    let (server_listener, server_tx) = NormalEndpoint::new();
    let (client_factory, client_tx) = NormalEndpoint::new();

    let server = DefaultCmdServer::<(), MockRead, MockWrite, u16, u8, _>::new(server_listener);
    let (server_recv_tx, mut server_recv_rx) = mpsc::unbounded_channel::<String>();
    server.register_cmd_handler(
        0x11,
        move |_peer_id, _tunnel_id, _header: CmdHeader<u16, u8>, body: CmdBody| {
            let server_recv_tx = server_recv_tx.clone();
            async move {
                server_recv_tx.send(body.into_string().await?).unwrap();
                Ok(Some(CmdBody::from_string("server-reply".to_owned())))
            }
        },
    );

    let client = DefaultCmdClient::<(), MockRead, MockWrite, _, u16, u8>::new(client_factory, 4);
    let (client_recv_tx, mut client_recv_rx) = mpsc::unbounded_channel::<String>();
    client.register_cmd_handler(
        0x12,
        move |_peer_id, _tunnel_id, _header: CmdHeader<u16, u8>, body: CmdBody| {
            let client_recv_tx = client_recv_tx.clone();
            async move {
                client_recv_tx.send(body.into_string().await?).unwrap();
                Ok(Some(CmdBody::from_string("client-reply".to_owned())))
            }
        },
    );

    server.start();
    wire_normal_connection(&server_tx, &client_tx, client_id.clone(), server_id.clone()).await;

    let resp = client
        .send_with_resp(0x11, 1, b"hello-server", Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(resp.into_string().await.unwrap(), "server-reply");
    assert_eq!(server_recv_rx.recv().await.unwrap(), "hello-server");

    wait_until_true(|| async { !server.get_peer_tunnels(&client_id).await.is_empty() }).await;

    let resp = server
        .send_with_resp(&client_id, 0x12, 1, b"hello-client", Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(resp.into_string().await.unwrap(), "client-reply");
    assert_eq!(client_recv_rx.recv().await.unwrap(), "hello-client");

    let timeout_err = client
        .send_with_resp(0x99, 1, b"no-handler", Duration::from_millis(80))
        .await;
    assert!(timeout_err.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn classified_client_server_communication() {
    let client_id = peer(11);
    let server_id = peer(12);

    let (server_listener, server_tx) = ClassifiedEndpoint::new();
    let (client_factory, client_tx) = ClassifiedEndpoint::new();

    let server = DefaultCmdServer::<(), MockClassifiedRead, MockClassifiedWrite, u16, u8, _>::new(
        server_listener,
    );
    server.register_cmd_handler(
        0x21,
        move |_peer_id, _tunnel_id, _header: CmdHeader<u16, u8>, _body| async move {
            Ok(Some(CmdBody::from_string(
                "server-classified-reply".to_owned(),
            )))
        },
    );

    let client = DefaultClassifiedCmdClient::<
        TestClass,
        (),
        MockClassifiedRead,
        MockClassifiedWrite,
        _,
        u16,
        u8,
    >::new(client_factory, 2);
    client.register_cmd_handler(
        0x22,
        move |_peer_id, _tunnel_id, _header: CmdHeader<u16, u8>, _body| async move {
            Ok(Some(CmdBody::from_string(
                "client-classified-reply".to_owned(),
            )))
        },
    );

    server.start();
    wire_classified_connection(
        &server_tx,
        &client_tx,
        TestClass::Alpha,
        client_id.clone(),
        server_id,
    )
    .await;

    let resp = client
        .send_by_classified_tunnel_with_resp(
            TestClass::Alpha,
            0x21,
            1,
            b"classified-req",
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(resp.into_string().await.unwrap(), "server-classified-reply");

    wait_until_true(|| async { !server.get_peer_tunnels(&client_id).await.is_empty() }).await;

    let resp = server
        .send_with_resp(
            &client_id,
            0x22,
            1,
            b"server->classified-client",
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(resp.into_string().await.unwrap(), "client-classified-reply");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_node_server_bidirectional_request_response() {
    let node_id = peer(21);
    let server_id = peer(22);

    let (server_listener, server_tx) = NormalEndpoint::new();
    let (node_factory, node_factory_tx) = NormalEndpoint::new();
    let (node_listener, _node_listener_tx) = NormalEndpoint::new();

    let server = DefaultCmdServer::<(), MockRead, MockWrite, u16, u8, _>::new(server_listener);
    server.register_cmd_handler(
        0x31,
        move |_peer_id, _tunnel_id, _header: CmdHeader<u16, u8>, _body| async move {
            Ok(Some(CmdBody::from_string("server-node-reply".to_owned())))
        },
    );

    let node = DefaultCmdNode::<(), MockRead, MockWrite, _, u16, u8, _>::new(
        node_listener,
        node_factory,
        4,
    );
    node.register_cmd_handler(
        0x32,
        move |_peer_id, _tunnel_id, _header: CmdHeader<u16, u8>, _body| async move {
            Ok(Some(CmdBody::from_string("node-reply".to_owned())))
        },
    );

    server.start();
    wire_normal_connection(
        &server_tx,
        &node_factory_tx,
        node_id.clone(),
        server_id.clone(),
    )
    .await;

    let resp = node
        .send_with_resp(&server_id, 0x31, 1, b"node->server", Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(resp.into_string().await.unwrap(), "server-node-reply");

    wait_until_true(|| async { !server.get_peer_tunnels(&node_id).await.is_empty() }).await;

    let resp = server
        .send_with_resp(&node_id, 0x32, 1, b"server->node", Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(resp.into_string().await.unwrap(), "node-reply");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn classified_node_server_communication() {
    let node_id = peer(31);
    let server_id = peer(32);

    let (server_listener, server_tx) = ClassifiedEndpoint::new();
    let (node_factory, node_factory_tx) = ClassifiedEndpoint::new();
    let (node_listener, _node_listener_tx) = ClassifiedEndpoint::new();

    let server = DefaultCmdServer::<(), MockClassifiedRead, MockClassifiedWrite, u16, u8, _>::new(
        server_listener,
    );
    server.register_cmd_handler(
        0x41,
        move |_peer_id, _tunnel_id, _header: CmdHeader<u16, u8>, _body| async move {
            Ok(Some(CmdBody::from_string(
                "server-class-node-reply".to_owned(),
            )))
        },
    );

    let node = DefaultClassifiedCmdNode::<
        TestClass,
        (),
        MockClassifiedRead,
        MockClassifiedWrite,
        _,
        u16,
        u8,
        _,
    >::new(node_listener, node_factory, 4);
    node.register_cmd_handler(
        0x42,
        move |_peer_id, _tunnel_id, _header: CmdHeader<u16, u8>, _body| async move {
            Ok(Some(CmdBody::from_string(
                "classified-node-reply".to_owned(),
            )))
        },
    );

    server.start();
    wire_classified_connection(
        &server_tx,
        &node_factory_tx,
        TestClass::Beta,
        node_id.clone(),
        server_id.clone(),
    )
    .await;

    let resp = node
        .send_by_classified_tunnel_with_resp(
            TestClass::Beta,
            0x41,
            1,
            b"classified-node->server",
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(resp.into_string().await.unwrap(), "server-class-node-reply");

    wait_until_true(|| async { !server.get_peer_tunnels(&node_id).await.is_empty() }).await;

    let resp = server
        .send_with_resp(
            &node_id,
            0x42,
            1,
            b"server->classified-node",
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(resp.into_string().await.unwrap(), "classified-node-reply");

    let tunnel_id = node
        .find_tunnel_id_by_peer_classified(&server_id, TestClass::Beta)
        .await
        .unwrap();
    let resp = node
        .send_by_specify_tunnel_with_resp(
            &server_id,
            tunnel_id,
            0x41,
            1,
            b"node-by-tunnel",
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(resp.into_string().await.unwrap(), "server-class-node-reply");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_client_interfaces_covered() {
    let client_id = peer(41);
    let server_id = peer(42);
    let (server_listener, server_tx) = NormalEndpoint::new();
    let (client_factory, client_tx) = NormalEndpoint::new();

    let server = DefaultCmdServer::<(), MockRead, MockWrite, u16, u8, _>::new(server_listener);
    let (server_recv_tx, mut server_recv_rx) = mpsc::unbounded_channel::<String>();
    for cmd in [0x01u8, 0x02, 0x03, 0x04, 0x05, 0x06, 0x77] {
        let tx = server_recv_tx.clone();
        server.register_cmd_handler(
            cmd,
            move |_peer_id, _tunnel_id, _header: CmdHeader<u16, u8>, body: CmdBody| {
                let tx = tx.clone();
                async move {
                    tx.send(body.into_string().await?).unwrap();
                    Ok(None)
                }
            },
        );
    }
    for (cmd, resp) in [
        (0x11u8, "resp-11"),
        (0x12, "resp-12"),
        (0x13, "resp-13"),
        (0x14, "resp-14"),
        (0x15, "resp-15"),
        (0x16, "resp-16"),
    ] {
        server.register_cmd_handler(
            cmd,
            move |_peer_id, _tunnel_id, _header: CmdHeader<u16, u8>, _body| async move {
                Ok(Some(CmdBody::from_string(resp.to_owned())))
            },
        );
    }

    let client = DefaultCmdClient::<(), MockRead, MockWrite, _, u16, u8>::new(client_factory, 4);
    let (client_tunnel_tx, mut client_tunnel_rx) = mpsc::unbounded_channel::<TunnelId>();
    client.register_cmd_handler(
        0x70,
        move |_peer_id, tunnel_id, _header: CmdHeader<u16, u8>, _body| {
            let client_tunnel_tx = client_tunnel_tx.clone();
            async move {
                client_tunnel_tx.send(tunnel_id).unwrap();
                Ok(Some(CmdBody::from_string(
                    "client-handler-reply".to_owned(),
                )))
            }
        },
    );

    server.start();
    wire_normal_connection(&server_tx, &client_tx, client_id.clone(), server_id).await;

    let r = client
        .send_with_resp(0x11, 1, b"boot", Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "resp-11");

    wait_until_true(|| async { !server.get_peer_tunnels(&client_id).await.is_empty() }).await;
    let _ = server
        .send_with_resp(&client_id, 0x70, 1, b"who-am-i", Duration::from_secs(1))
        .await
        .unwrap();
    let client_tunnel_id = recv_tunnel_id(&mut client_tunnel_rx).await;

    client.send(0x01, 1, b"send").await.unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "send");

    client.send2(0x02, 1, &[b"A", b"B"]).await.unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "AB");

    client
        .send_cmd(0x03, 1, CmdBody::from_string("cmd-body".to_owned()))
        .await
        .unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "cmd-body");

    let r = client
        .send2_with_resp(0x12, 1, &[b"X", b"Y"], Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "resp-12");

    let r = client
        .send_cmd_with_resp(
            0x13,
            1,
            CmdBody::from_string("need-resp".to_owned()),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "resp-13");

    client
        .send_by_specify_tunnel(client_tunnel_id, 0x04, 1, b"spec-send")
        .await
        .unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "spec-send");

    client
        .send2_by_specify_tunnel(client_tunnel_id, 0x05, 1, &[b"S", b"2"])
        .await
        .unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "S2");

    client
        .send_cmd_by_specify_tunnel(
            client_tunnel_id,
            0x06,
            1,
            CmdBody::from_string("spec-cmd".to_owned()),
        )
        .await
        .unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "spec-cmd");

    let r = client
        .send_by_specify_tunnel_with_resp(
            client_tunnel_id,
            0x14,
            1,
            b"spec-r",
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "resp-14");

    let r = client
        .send2_by_specify_tunnel_with_resp(
            client_tunnel_id,
            0x15,
            1,
            &[b"S", b"R"],
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "resp-15");

    let r = client
        .send_cmd_by_specify_tunnel_with_resp(
            client_tunnel_id,
            0x16,
            1,
            CmdBody::from_string("spec-cmd-r".to_owned()),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "resp-16");

    let guard = client.get_send(client_tunnel_id).await.unwrap();
    assert_eq!(guard.get_remote_peer_id(), peer(42));
    drop(guard);

    client.clear_all_tunnel().await;
    wire_normal_connection(&server_tx, &client_tx, client_id, peer(43)).await;
    client.send(0x01, 1, b"after-clear").await.unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "after-clear");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_interfaces_covered() {
    let client_id = peer(51);
    let server_id = peer(52);
    let (server_listener, server_tx) = NormalEndpoint::new();
    let (client_factory, client_tx) = NormalEndpoint::new();

    let server = DefaultCmdServer::<(), MockRead, MockWrite, u16, u8, _>::new(server_listener);
    let client = DefaultCmdClient::<(), MockRead, MockWrite, _, u16, u8>::new(client_factory, 4);
    let (client_recv_tx, mut client_recv_rx) = mpsc::unbounded_channel::<String>();

    for cmd in [0x21u8, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28] {
        let tx = client_recv_tx.clone();
        client.register_cmd_handler(
            cmd,
            move |_peer_id, _tunnel_id, _header: CmdHeader<u16, u8>, body: CmdBody| {
                let tx = tx.clone();
                async move {
                    tx.send(body.into_string().await?).unwrap();
                    Ok(None)
                }
            },
        );
    }

    for (cmd, resp) in [
        (0x31u8, "c31"),
        (0x32, "c32"),
        (0x33, "c33"),
        (0x34, "c34"),
        (0x35, "c35"),
        (0x36, "c36"),
    ] {
        client.register_cmd_handler(
            cmd,
            move |_peer_id, _tunnel_id, _header: CmdHeader<u16, u8>, _body| async move {
                Ok(Some(CmdBody::from_string(resp.to_owned())))
            },
        );
    }

    server.start();
    wire_normal_connection(&server_tx, &client_tx, client_id.clone(), server_id).await;

    client.send(0x00, 1, b"boot").await.unwrap();
    wait_until_true(|| async { !server.get_peer_tunnels(&client_id).await.is_empty() }).await;
    let tunnel_id = server.get_peer_tunnels(&client_id).await[0].conn_id;

    server.send(&client_id, 0x21, 1, b"s-send").await.unwrap();
    assert_eq!(recv_text(&mut client_recv_rx).await, "s-send");

    let r = server
        .send_with_resp(&client_id, 0x31, 1, b"s-r", Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "c31");

    server
        .send2(&client_id, 0x22, 1, &[b"S", b"2"])
        .await
        .unwrap();
    assert_eq!(recv_text(&mut client_recv_rx).await, "S2");

    let r = server
        .send2_with_resp(&client_id, 0x32, 1, &[b"R", b"2"], Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "c32");

    server
        .send_cmd(
            &client_id,
            0x23,
            1,
            CmdBody::from_string("s-cmd".to_owned()),
        )
        .await
        .unwrap();
    assert_eq!(recv_text(&mut client_recv_rx).await, "s-cmd");

    let r = server
        .send_cmd_with_resp(
            &client_id,
            0x33,
            1,
            CmdBody::from_string("s-cmd-r".to_owned()),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "c33");

    server
        .send_by_specify_tunnel(&client_id, tunnel_id, 0x24, 1, b"s-spec")
        .await
        .unwrap();
    assert_eq!(recv_text(&mut client_recv_rx).await, "s-spec");

    let r = server
        .send_by_specify_tunnel_with_resp(
            &client_id,
            tunnel_id,
            0x34,
            1,
            b"s-spec-r",
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "c34");

    server
        .send2_by_specify_tunnel(&client_id, tunnel_id, 0x25, 1, &[b"P", b"2"])
        .await
        .unwrap();
    assert_eq!(recv_text(&mut client_recv_rx).await, "P2");

    let r = server
        .send2_by_specify_tunnel_with_resp(
            &client_id,
            tunnel_id,
            0x35,
            1,
            &[b"P", b"R"],
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "c35");

    server
        .send_cmd_by_specify_tunnel(
            &client_id,
            tunnel_id,
            0x26,
            1,
            CmdBody::from_string("spec-cmd".to_owned()),
        )
        .await
        .unwrap();
    assert_eq!(recv_text(&mut client_recv_rx).await, "spec-cmd");

    let r = server
        .send_cmd_by_specify_tunnel_with_resp(
            &client_id,
            tunnel_id,
            0x36,
            1,
            CmdBody::from_string("spec-cmd-r".to_owned()),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "c36");

    server
        .send_by_all_tunnels(&client_id, 0x27, 1, b"all-send")
        .await
        .unwrap();
    assert_eq!(recv_text(&mut client_recv_rx).await, "all-send");

    server
        .send2_by_all_tunnels(&client_id, 0x28, 1, &[b"A", b"L"])
        .await
        .unwrap();
    assert_eq!(recv_text(&mut client_recv_rx).await, "AL");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_node_interfaces_covered() {
    let node_id = peer(61);
    let server_id = peer(62);
    let (server_listener, server_tx) = NormalEndpoint::new();
    let (node_factory, node_factory_tx) = NormalEndpoint::new();
    let (node_listener, _node_listener_tx) = NormalEndpoint::new();

    let server = DefaultCmdServer::<(), MockRead, MockWrite, u16, u8, _>::new(server_listener);
    let (server_recv_tx, mut server_recv_rx) = mpsc::unbounded_channel::<String>();
    for cmd in [0x51u8, 0x52, 0x53, 0x54, 0x55, 0x56, 0x97] {
        let tx = server_recv_tx.clone();
        server.register_cmd_handler(
            cmd,
            move |_peer_id, _tunnel_id, _header: CmdHeader<u16, u8>, body: CmdBody| {
                let tx = tx.clone();
                async move {
                    tx.send(body.into_string().await?).unwrap();
                    Ok(None)
                }
            },
        );
    }
    for (cmd, resp) in [
        (0x61u8, "n61"),
        (0x62, "n62"),
        (0x63, "n63"),
        (0x64, "n64"),
        (0x65, "n65"),
        (0x66, "n66"),
    ] {
        server.register_cmd_handler(
            cmd,
            move |_peer_id, _tunnel_id, _header: CmdHeader<u16, u8>, _body| async move {
                Ok(Some(CmdBody::from_string(resp.to_owned())))
            },
        );
    }

    let node = DefaultCmdNode::<(), MockRead, MockWrite, _, u16, u8, _>::new(
        node_listener,
        node_factory,
        4,
    );
    let (node_tunnel_tx, mut node_tunnel_rx) = mpsc::unbounded_channel::<TunnelId>();
    node.register_cmd_handler(
        0x70,
        move |_peer_id, tunnel_id, _header: CmdHeader<u16, u8>, _body| {
            let node_tunnel_tx = node_tunnel_tx.clone();
            async move {
                node_tunnel_tx.send(tunnel_id).unwrap();
                Ok(Some(CmdBody::from_string("node-handler-reply".to_owned())))
            }
        },
    );

    server.start();
    wire_normal_connection(
        &server_tx,
        &node_factory_tx,
        node_id.clone(),
        server_id.clone(),
    )
    .await;

    let r = node
        .send_with_resp(&server_id, 0x61, 1, b"boot", Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "n61");

    wait_until_true(|| async { !server.get_peer_tunnels(&node_id).await.is_empty() }).await;
    let _ = server
        .send_with_resp(&node_id, 0x70, 1, b"nid", Duration::from_secs(1))
        .await
        .unwrap();
    let node_tunnel_id = recv_tunnel_id(&mut node_tunnel_rx).await;

    node.send(&server_id, 0x51, 1, b"n-send").await.unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "n-send");

    node.send2(&server_id, 0x52, 1, &[b"N", b"2"])
        .await
        .unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "N2");

    node.send_cmd(
        &server_id,
        0x53,
        1,
        CmdBody::from_string("n-cmd".to_owned()),
    )
    .await
    .unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "n-cmd");

    let r = node
        .send2_with_resp(&server_id, 0x62, 1, &[b"R", b"2"], Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "n62");

    let r = node
        .send_cmd_with_resp(
            &server_id,
            0x63,
            1,
            CmdBody::from_string("n-cmd-r".to_owned()),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "n63");

    node.send_by_specify_tunnel(&server_id, node_tunnel_id, 0x54, 1, b"n-spec")
        .await
        .unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "n-spec");

    node.send2_by_specify_tunnel(&server_id, node_tunnel_id, 0x55, 1, &[b"S", b"2"])
        .await
        .unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "S2");

    node.send_cmd_by_specify_tunnel(
        &server_id,
        node_tunnel_id,
        0x56,
        1,
        CmdBody::from_string("n-spec-cmd".to_owned()),
    )
    .await
    .unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "n-spec-cmd");

    let r = node
        .send_by_specify_tunnel_with_resp(
            &server_id,
            node_tunnel_id,
            0x64,
            1,
            b"n-spec-r",
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "n64");

    let r = node
        .send2_by_specify_tunnel_with_resp(
            &server_id,
            node_tunnel_id,
            0x65,
            1,
            &[b"S", b"R"],
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "n65");

    let r = node
        .send_cmd_by_specify_tunnel_with_resp(
            &server_id,
            node_tunnel_id,
            0x66,
            1,
            CmdBody::from_string("n-spec-cmd-r".to_owned()),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "n66");

    let guard = node.get_send(&server_id, node_tunnel_id).await.unwrap();
    assert_eq!(guard.get_remote_peer_id(), server_id);
    drop(guard);

    node.clear_all_tunnel().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn classified_interfaces_covered() {
    let endpoint_id = peer(71);
    let remote_id = peer(72);
    let (server_listener, server_tx) = ClassifiedEndpoint::new();
    let (client_factory, client_tx) = ClassifiedEndpoint::new();
    let (node_factory, node_factory_tx) = ClassifiedEndpoint::new();
    let (node_listener, node_listener_tx) = ClassifiedEndpoint::new();
    let (inbound_factory, inbound_tx) = ClassifiedEndpoint::new();

    let server = DefaultCmdServer::<(), MockClassifiedRead, MockClassifiedWrite, u16, u8, _>::new(
        server_listener,
    );
    let (server_recv_tx, mut server_recv_rx) = mpsc::unbounded_channel::<String>();
    for cmd in [
        0x81u8, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8A, 0x8B, 0x8C, 0x8D, 0x8E, 0x8F,
    ] {
        let tx = server_recv_tx.clone();
        server.register_cmd_handler(
            cmd,
            move |_peer_id, _tunnel_id, _header: CmdHeader<u16, u8>, body: CmdBody| {
                let tx = tx.clone();
                async move {
                    tx.send(body.into_string().await?).unwrap();
                    Ok(None)
                }
            },
        );
    }
    for (cmd, resp) in [
        (0x91u8, "k91"),
        (0x92, "k92"),
        (0x93, "k93"),
        (0x94, "k94"),
        (0x95, "k95"),
        (0x96, "k96"),
        (0x97, "k97"),
        (0x98, "k98"),
        (0x99, "k99"),
        (0x9A, "k9A"),
        (0x9B, "k9B"),
        (0x9C, "k9C"),
        (0x9D, "k9D"),
    ] {
        server.register_cmd_handler(
            cmd,
            move |_peer_id, _tunnel_id, _header: CmdHeader<u16, u8>, _body| async move {
                Ok(Some(CmdBody::from_string(resp.to_owned())))
            },
        );
    }

    let client = DefaultClassifiedCmdClient::<
        TestClass,
        (),
        MockClassifiedRead,
        MockClassifiedWrite,
        _,
        u16,
        u8,
    >::new(client_factory, 2);

    let node = DefaultClassifiedCmdNode::<
        TestClass,
        (),
        MockClassifiedRead,
        MockClassifiedWrite,
        _,
        u16,
        u8,
        _,
    >::new(node_listener, node_factory, 2);
    node.register_cmd_handler(
        0xA0,
        move |_peer_id, _tunnel_id, _header: CmdHeader<u16, u8>, body: CmdBody| async move {
            let text = body.into_string().await?;
            Ok(Some(CmdBody::from_string(format!("node-{}", text))))
        },
    );

    let inbound_client = DefaultClassifiedCmdClient::<
        TestClass,
        (),
        MockClassifiedRead,
        MockClassifiedWrite,
        _,
        u16,
        u8,
    >::new(inbound_factory, 1);

    server.start();
    wire_classified_connection(
        &server_tx,
        &client_tx,
        TestClass::Alpha,
        endpoint_id.clone(),
        remote_id.clone(),
    )
    .await;
    wire_classified_connection(
        &server_tx,
        &node_factory_tx,
        TestClass::Beta,
        peer(73),
        remote_id.clone(),
    )
    .await;
    wire_classified_connection(
        &node_listener_tx,
        &inbound_tx,
        TestClass::Alpha,
        peer(74),
        peer(75),
    )
    .await;

    let inbound_resp = inbound_client
        .send_by_classified_tunnel_with_resp(
            TestClass::Alpha,
            0xA0,
            1,
            b"inbound",
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(inbound_resp.into_string().await.unwrap(), "node-inbound");

    let r = client
        .send_with_resp(0x95, 1, b"c-base-r", Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "k95");
    client.send(0x85, 1, b"c-base").await.unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "c-base");
    client.send2(0x86, 1, &[b"B", b"2"]).await.unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "B2");
    client
        .send_cmd(0x87, 1, CmdBody::from_string("c-base-cmd".to_owned()))
        .await
        .unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "c-base-cmd");
    let r = client
        .send2_with_resp(0x96, 1, &[b"B", b"R"], Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "k96");
    let r = client
        .send_cmd_with_resp(
            0x97,
            1,
            CmdBody::from_string("c-base-cmd-r".to_owned()),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "k97");

    let r = client
        .send_by_classified_tunnel_with_resp(
            TestClass::Alpha,
            0x91,
            1,
            b"c-req",
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "k91");
    client
        .send_by_classified_tunnel(TestClass::Alpha, 0x81, 1, b"c-send")
        .await
        .unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "c-send");
    client
        .send2_by_classified_tunnel(TestClass::Alpha, 0x82, 1, &[b"C", b"2"])
        .await
        .unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "C2");
    client
        .send_cmd_by_classified_tunnel(
            TestClass::Alpha,
            0x83,
            1,
            CmdBody::from_string("c-cmd".to_owned()),
        )
        .await
        .unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "c-cmd");
    let r = client
        .send2_by_classified_tunnel_with_resp(
            TestClass::Alpha,
            0x92,
            1,
            &[b"R", b"2"],
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "k92");
    let r = client
        .send_cmd_by_classified_tunnel_with_resp(
            TestClass::Alpha,
            0x93,
            1,
            CmdBody::from_string("c-cmd-r".to_owned()),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "k93");
    let tunnel_id = client
        .find_tunnel_id_by_classified(TestClass::Alpha)
        .await
        .unwrap();
    client
        .send_by_specify_tunnel(tunnel_id, 0x84, 1, b"c-spec")
        .await
        .unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "c-spec");
    let r = client
        .send_by_specify_tunnel_with_resp(tunnel_id, 0x98, 1, b"c-spec-r", Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "k98");
    client
        .send2_by_specify_tunnel(tunnel_id, 0x88, 1, &[b"S", b"2"])
        .await
        .unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "S2");
    let r = client
        .send2_by_specify_tunnel_with_resp(
            tunnel_id,
            0x99,
            1,
            &[b"S", b"R"],
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "k99");
    client
        .send_cmd_by_specify_tunnel(
            tunnel_id,
            0x89,
            1,
            CmdBody::from_string("c-spec-cmd".to_owned()),
        )
        .await
        .unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "c-spec-cmd");
    let r = client
        .send_cmd_by_specify_tunnel_with_resp(
            tunnel_id,
            0x9A,
            1,
            CmdBody::from_string("c-spec-cmd-r".to_owned()),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "k9A");
    let guard0 = client.get_send(tunnel_id).await.unwrap();
    drop(guard0);
    let _guard = client
        .get_send_by_classified(TestClass::Alpha)
        .await
        .unwrap();

    let node_peer = remote_id.clone();
    let r = node
        .send_with_resp(&node_peer, 0x9B, 1, b"n-base-r", Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "k9B");
    node.send(&node_peer, 0x8A, 1, b"n-base").await.unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "n-base");
    node.send2(&node_peer, 0x8B, 1, &[b"N", b"2"])
        .await
        .unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "N2");
    node.send_cmd(
        &node_peer,
        0x8C,
        1,
        CmdBody::from_string("n-base-cmd".to_owned()),
    )
    .await
    .unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "n-base-cmd");
    let r = node
        .send2_with_resp(&node_peer, 0x9C, 1, &[b"N", b"R"], Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "k9C");
    let r = node
        .send_cmd_with_resp(
            &node_peer,
            0x9D,
            1,
            CmdBody::from_string("n-base-cmd-r".to_owned()),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "k9D");

    let r = node
        .send_by_classified_tunnel_with_resp(
            TestClass::Beta,
            0x94,
            1,
            b"n-req",
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "k94");
    node.send_by_peer_classified_tunnel(&node_peer, TestClass::Beta, 0x81, 1, b"pn-send")
        .await
        .unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "pn-send");
    let r = node
        .send_by_peer_classified_tunnel_with_resp(
            &node_peer,
            TestClass::Beta,
            0x91,
            1,
            b"pn-send-r",
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "k91");
    node.send2_by_peer_classified_tunnel(&node_peer, TestClass::Beta, 0x82, 1, &[b"P", b"2"])
        .await
        .unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "P2");
    let r = node
        .send2_by_peer_classified_tunnel_with_resp(
            &node_peer,
            TestClass::Beta,
            0x92,
            1,
            &[b"P", b"R"],
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "k92");
    node.send_cmd_by_peer_classified_tunnel(
        &node_peer,
        TestClass::Beta,
        0x83,
        1,
        CmdBody::from_string("p-cmd".to_owned()),
    )
    .await
    .unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "p-cmd");
    let r = node
        .send_cmd_by_peer_classified_tunnel_with_resp(
            &node_peer,
            TestClass::Beta,
            0x93,
            1,
            CmdBody::from_string("p-cmd-r".to_owned()),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "k93");
    let id1 = node
        .find_tunnel_id_by_classified(TestClass::Beta)
        .await
        .unwrap();
    let id2 = node
        .find_tunnel_id_by_peer_classified(&node_peer, TestClass::Beta)
        .await
        .unwrap();
    assert_eq!(id1, id2);
    let g1 = node.get_send_by_classified(TestClass::Beta).await.unwrap();
    drop(g1);
    let g2 = node
        .get_send_by_peer_classified(&node_peer, TestClass::Beta)
        .await
        .unwrap();
    drop(g2);

    node.send_by_specify_tunnel(&node_peer, id1, 0x8D, 1, b"n-spec")
        .await
        .unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "n-spec");
    let r = node
        .send_by_specify_tunnel_with_resp(
            &node_peer,
            id1,
            0x94,
            1,
            b"n-spec-r",
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "k94");
    node.send2_by_specify_tunnel(&node_peer, id1, 0x8E, 1, &[b"T", b"2"])
        .await
        .unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "T2");
    let r = node
        .send2_by_specify_tunnel_with_resp(
            &node_peer,
            id1,
            0x91,
            1,
            &[b"T", b"R"],
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "k91");
    node.send_cmd_by_specify_tunnel(
        &node_peer,
        id1,
        0x8F,
        1,
        CmdBody::from_string("n-spec-cmd".to_owned()),
    )
    .await
    .unwrap();
    assert_eq!(recv_text(&mut server_recv_rx).await, "n-spec-cmd");
    let r = node
        .send_cmd_by_specify_tunnel_with_resp(
            &node_peer,
            id1,
            0x92,
            1,
            CmdBody::from_string("n-spec-cmd-r".to_owned()),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(r.into_string().await.unwrap(), "k92");

    let node_guard = node.get_send(&node_peer, id1).await.unwrap();
    drop(node_guard);
    node.clear_all_tunnel().await;
}
