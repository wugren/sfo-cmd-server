use super::CmdServer;
use super::peer_manager::{PeerManager, PeerManagerRef};
use crate::client::{RespWaiter, RespWaiterRef, gen_resp_id, gen_seq};
use crate::cmd::{CmdBodyRead, CmdHandler, CmdHandlerMap, CmdHeader};
use crate::errors::{CmdErrorCode, CmdResult, cmd_err, into_cmd_err};
use crate::peer_connection::PeerConnection;
use crate::peer_id::PeerId;
use crate::{CmdBody, CmdTunnelMeta, CmdTunnelRead, CmdTunnelWrite, TunnelId};
use async_named_locker::{NamedStateHolder, ObjectHolder};
use bucky_raw_codec::{RawConvertTo, RawDecode, RawEncode, RawFixedBytes, RawFrom};
use num::{FromPrimitive, ToPrimitive};
use sfo_split::Splittable;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};

#[async_trait::async_trait]
pub trait CmdTunnelListener<M: CmdTunnelMeta, R: CmdTunnelRead<M>, W: CmdTunnelWrite<M>>:
    Send + Sync + 'static
{
    async fn accept(&self) -> CmdResult<Splittable<R, W>>;
}

#[async_trait::async_trait]
pub trait CmdTunnelService<M: CmdTunnelMeta, R: CmdTunnelRead<M>, W: CmdTunnelWrite<M>>:
    Send + Sync + 'static
{
    async fn handle_tunnel(&self, tunnel: Splittable<R, W>) -> CmdResult<()>;
}

#[async_trait::async_trait]
pub trait CmdServerEventListener: Send + Sync + 'static {
    async fn on_peer_connected(&self, peer_id: &PeerId) -> CmdResult<()>;
    async fn on_peer_disconnected(&self, peer_id: &PeerId) -> CmdResult<()>;
}

#[derive(Clone)]
struct CmdServerEventListenerEmit {
    listeners: Arc<Mutex<Vec<Arc<dyn CmdServerEventListener>>>>,
}

impl CmdServerEventListenerEmit {
    pub fn new() -> Self {
        Self {
            listeners: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn attach_event_listener(&self, event_listener: Arc<dyn CmdServerEventListener>) {
        self.listeners.lock().unwrap().push(event_listener);
    }
}

#[async_trait::async_trait]
impl CmdServerEventListener for CmdServerEventListenerEmit {
    async fn on_peer_connected(&self, peer_id: &PeerId) -> CmdResult<()> {
        let listeners = { self.listeners.lock().unwrap().clone() };
        for listener in listeners.iter() {
            if let Err(e) = listener.on_peer_connected(peer_id).await {
                log::error!("on_peer_connected error: {:?}", e);
            }
        }
        Ok(())
    }

    async fn on_peer_disconnected(&self, peer_id: &PeerId) -> CmdResult<()> {
        let listeners = { self.listeners.lock().unwrap().clone() };
        for listener in listeners.iter() {
            if let Err(e) = listener.on_peer_disconnected(peer_id).await {
                log::error!("on_peer_disconnected error: {:?}", e);
            }
        }
        Ok(())
    }
}

pub struct DefaultCmdServerService<
    M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    LEN,
    CMD,
> {
    cmd_handler_map: Arc<CmdHandlerMap<LEN, CMD>>,
    peer_manager: PeerManagerRef<M, R, W>,
    event_emit: CmdServerEventListenerEmit,
    resp_waiter: RespWaiterRef,
    state_holder: Arc<NamedStateHolder<tokio::task::Id>>,
    _p: PhantomData<fn() -> (M, R, W, LEN, CMD)>,
}

impl<
    M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    LEN: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + RawFixedBytes
        + Sync
        + Send
        + 'static
        + FromPrimitive
        + ToPrimitive,
    CMD: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + RawFixedBytes
        + Sync
        + Send
        + 'static
        + Eq
        + Hash
        + Debug,
> DefaultCmdServerService<M, R, W, LEN, CMD>
{
    fn encode_pkg_len(len: u64) -> CmdResult<LEN> {
        LEN::from_u64(len).ok_or_else(|| {
            cmd_err!(
                CmdErrorCode::InvalidParam,
                "body len {} exceeds header len type {}",
                len,
                std::any::type_name::<LEN>()
            )
        })
    }

    pub fn new() -> Arc<Self> {
        let event_emit = CmdServerEventListenerEmit::new();
        Arc::new(Self {
            cmd_handler_map: Arc::new(CmdHandlerMap::new()),
            peer_manager: PeerManager::new(Arc::new(event_emit.clone())),
            event_emit,
            resp_waiter: Arc::new(RespWaiter::new()),
            state_holder: NamedStateHolder::new(),
            _p: PhantomData,
        })
    }

    pub fn attach_event_listener(&self, event_listener: Arc<dyn CmdServerEventListener>) {
        self.event_emit.attach_event_listener(event_listener);
    }

    pub async fn get_peer_tunnels(&self, peer_id: &PeerId) -> Vec<Arc<PeerConnection<R, W>>> {
        self.peer_manager.find_connections(peer_id)
    }

    pub async fn serve_tunnel(&self, tunnel: Splittable<R, W>) -> CmdResult<()> {
        let peer_id = tunnel.get_remote_peer_id();
        let tunnel_id = self.peer_manager.generate_conn_id();
        let cmd_handler_map = self.cmd_handler_map.clone();
        let resp_waiter = self.resp_waiter.clone();
        let state_holder = self.state_holder.clone();
        let (mut reader, writer) = tunnel.split();
        let local_id = reader.get_local_peer_id();
        let writer = ObjectHolder::new(writer);
        let resp_write = writer.clone();
        let remote_id = peer_id.clone();
        let recv_handle = tokio::spawn(async move {
            let ret: CmdResult<()> = async move {
                loop {
                    let header_len = reader
                        .read_u8()
                        .await
                        .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                    let mut header = vec![0u8; header_len as usize];
                    let n = reader
                        .read_exact(&mut header)
                        .await
                        .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                    if n == 0 {
                        break;
                    }
                    let header = CmdHeader::<LEN, CMD>::clone_from_slice(header.as_slice())
                        .map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
                    sfo_log::debug!("recv cmd {:?}", header.cmd_code());
                    let body_len = header.pkg_len().to_u64().unwrap();
                    let cmd_read = CmdBodyRead::new(reader, body_len as usize);
                    let waiter = cmd_read.get_waiter();
                    let future = waiter
                        .create_result_future()
                        .map_err(into_cmd_err!(CmdErrorCode::Failed))?;
                    {
                        let body = CmdBody::from_reader(BufReader::new(cmd_read), body_len);
                        if header.is_resp() && header.seq().is_some() {
                            let resp_id =
                                gen_resp_id(tunnel_id, header.cmd_code(), header.seq().unwrap());
                            let _ = resp_waiter.set_result(resp_id, body);
                        } else if let Some(handler) = cmd_handler_map.get(header.cmd_code()) {
                            let version = header.version();
                            let seq = header.seq();
                            let cmd_code = header.cmd_code();
                            match {
                                let _handle_state = state_holder.new_state(tokio::task::id());
                                handler
                                    .handle(
                                        local_id.clone(),
                                        remote_id.clone(),
                                        tunnel_id,
                                        header,
                                        body,
                                    )
                                    .await
                            } {
                                Ok(Some(mut body)) => {
                                    let mut write = resp_write.get().await;
                                    let header = CmdHeader::<LEN, CMD>::new(
                                        version,
                                        true,
                                        seq,
                                        cmd_code,
                                        Self::encode_pkg_len(body.len())?,
                                    );
                                    let buf = header
                                        .to_vec()
                                        .map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
                                    if buf.len() > 255 {
                                        return Err(cmd_err!(
                                            CmdErrorCode::RawCodecError,
                                            "header len too large"
                                        ));
                                    }
                                    write
                                        .write_u8(buf.len() as u8)
                                        .await
                                        .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                                    write
                                        .write_all(buf.as_slice())
                                        .await
                                        .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                                    tokio::io::copy(&mut body, write.deref_mut().deref_mut())
                                        .await
                                        .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                                    write
                                        .flush()
                                        .await
                                        .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                                }
                                Err(e) => {
                                    log::error!("handle cmd error: {:?}", e);
                                }
                                Ok(None) => {}
                            }
                        }
                    }
                    reader = future
                        .await
                        .map_err(into_cmd_err!(CmdErrorCode::Failed))??;
                }
                Ok(())
            }
            .await;
            if let Err(e) = ret.as_ref() {
                log::error!("recv cmd error: {:?}", e);
            }
            ret
        });

        let peer_conn = PeerConnection {
            conn_id: tunnel_id,
            peer_id,
            send: writer,
            handle: Some(recv_handle),
        };
        self.peer_manager.add_peer_connection(peer_conn).await;
        Ok(())
    }
}

#[async_trait::async_trait]
impl<
    M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    LEN: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + RawFixedBytes
        + Sync
        + Send
        + 'static
        + FromPrimitive
        + ToPrimitive,
    CMD: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + RawFixedBytes
        + Sync
        + Send
        + 'static
        + Eq
        + Hash
        + Debug,
> CmdTunnelService<M, R, W> for DefaultCmdServerService<M, R, W, LEN, CMD>
{
    async fn handle_tunnel(&self, tunnel: Splittable<R, W>) -> CmdResult<()> {
        self.serve_tunnel(tunnel).await
    }
}

#[async_trait::async_trait]
impl<
    M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    LEN: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + RawFixedBytes
        + Sync
        + Send
        + 'static
        + FromPrimitive
        + ToPrimitive,
    CMD: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + RawFixedBytes
        + Sync
        + Send
        + 'static
        + Eq
        + Hash
        + Debug,
> CmdServer<LEN, CMD> for DefaultCmdServerService<M, R, W, LEN, CMD>
{
    fn register_cmd_handler(&self, cmd: CMD, handler: impl CmdHandler<LEN, CMD>) {
        self.cmd_handler_map.insert(cmd, handler);
    }

    async fn send(&self, peer_id: &PeerId, cmd: CMD, version: u8, body: &[u8]) -> CmdResult<()> {
        let connections = self.peer_manager.find_connections(peer_id);
        if connections.is_empty() {
            return Err(cmd_err!(
                CmdErrorCode::PeerConnectionNotFound,
                "peer_id: {}",
                peer_id
            ));
        }
        let mut last_err = None;
        for conn in connections {
            let ret: CmdResult<()> = async move {
                log::debug!(
                    "send peer_id: {}, tunnel_id {:?}, cmd: {:?}, len: {} data: {}",
                    peer_id,
                    conn.conn_id,
                    cmd,
                    body.len(),
                    hex::encode(body)
                );
                let header = CmdHeader::<LEN, CMD>::new(
                    version,
                    false,
                    None,
                    cmd,
                    Self::encode_pkg_len(body.len() as u64)?,
                );
                let buf = header
                    .to_vec()
                    .map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
                let mut send = conn.send.get().await;
                if buf.len() > 255 {
                    return Err(cmd_err!(CmdErrorCode::InvalidParam, "header len too large"));
                }
                send.write_u8(buf.len() as u8)
                    .await
                    .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                send.write_all(buf.as_slice())
                    .await
                    .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                send.write_all(body)
                    .await
                    .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                send.flush()
                    .await
                    .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                Ok(())
            }
            .await;
            if ret.is_ok() {
                return Ok(());
            }
            last_err = ret.err();
        }
        Err(last_err
            .unwrap_or_else(|| cmd_err!(CmdErrorCode::Failed, "send to peer_id: {}", peer_id)))
    }

    async fn send_with_resp(
        &self,
        peer_id: &PeerId,
        cmd: CMD,
        version: u8,
        body: &[u8],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        let connections = self.peer_manager.find_connections(peer_id);
        for conn in connections {
            if let Some(id) = tokio::task::try_id() {
                if self.state_holder.has_state(id) {
                    continue;
                }
            }
            let ret: CmdResult<CmdBody> = async move {
                log::debug!(
                    "send peer_id: {}, tunnel_id {:?}, cmd: {:?}, len: {} data: {}",
                    peer_id,
                    conn.conn_id,
                    cmd,
                    body.len(),
                    hex::encode(body)
                );
                let seq = gen_seq();
                let header = CmdHeader::<LEN, CMD>::new(
                    version,
                    false,
                    Some(seq),
                    cmd,
                    Self::encode_pkg_len(body.len() as u64)?,
                );
                let buf = header
                    .to_vec()
                    .map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
                let resp_id = gen_resp_id(conn.conn_id, cmd, seq);
                let waiter = self
                    .resp_waiter
                    .create_timeout_result_future(resp_id, timeout)
                    .map_err(into_cmd_err!(CmdErrorCode::Failed))?;
                {
                    let mut send = conn.send.get().await;
                    if buf.len() > 255 {
                        return Err(cmd_err!(CmdErrorCode::InvalidParam, "header len too large"));
                    }
                    send.write_u8(buf.len() as u8)
                        .await
                        .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                    send.write_all(buf.as_slice())
                        .await
                        .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                    send.write_all(body)
                        .await
                        .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                    send.flush()
                        .await
                        .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                }
                let body =
                    waiter
                        .await
                        .map_err(into_cmd_err!(CmdErrorCode::Timeout, "cmd {:?}", cmd))?;
                Ok(body)
            }
            .await;
            if ret.is_ok() {
                return ret;
            } else {
                sfo_log::error!("send err {:?}", ret.unwrap_err());
            }
        }
        Err(cmd_err!(
            CmdErrorCode::Failed,
            "send to peer_id: {}",
            peer_id
        ))
    }

    async fn send2(
        &self,
        peer_id: &PeerId,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
    ) -> CmdResult<()> {
        let connections = self.peer_manager.find_connections(peer_id);
        if connections.is_empty() {
            return Err(cmd_err!(
                CmdErrorCode::PeerConnectionNotFound,
                "peer_id: {}",
                peer_id
            ));
        }
        let mut last_err = None;
        for conn in connections {
            let ret: CmdResult<()> = async move {
                let mut len = 0;
                for b in body.iter() {
                    len += b.len();
                    log::debug!(
                        "send2 peer_id: {}, tunnel_id: {:?}, cmd: {:?} body: {}",
                        peer_id,
                        conn.conn_id,
                        cmd,
                        hex::encode(b)
                    );
                }
                log::debug!(
                    "send2 peer_id: {}, tunnel_id: {:?}, cmd: {:?} len: {}",
                    peer_id,
                    conn.conn_id,
                    cmd,
                    len
                );
                let header = CmdHeader::<LEN, CMD>::new(
                    version,
                    false,
                    None,
                    cmd,
                    Self::encode_pkg_len(len as u64)?,
                );
                let buf = header
                    .to_vec()
                    .map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
                let mut send = conn.send.get().await;
                if buf.len() > 255 {
                    return Err(cmd_err!(CmdErrorCode::InvalidParam, "header len too large"));
                }
                send.write_u8(buf.len() as u8)
                    .await
                    .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                send.write_all(buf.as_slice())
                    .await
                    .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                for b in body.iter() {
                    send.write_all(b)
                        .await
                        .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                }
                send.flush()
                    .await
                    .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                Ok(())
            }
            .await;
            if ret.is_ok() {
                return Ok(());
            }
            last_err = ret.err();
        }
        Err(last_err
            .unwrap_or_else(|| cmd_err!(CmdErrorCode::Failed, "send to peer_id: {}", peer_id)))
    }

    async fn send2_with_resp(
        &self,
        peer_id: &PeerId,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        let connections = self.peer_manager.find_connections(peer_id);
        for conn in connections {
            if let Some(id) = tokio::task::try_id() {
                if self.state_holder.has_state(id) {
                    continue;
                }
            }
            let ret: CmdResult<CmdBody> = async move {
                let mut len = 0;
                for b in body.iter() {
                    len += b.len();
                    log::debug!(
                        "send2 peer_id: {}, tunnel_id: {:?}, cmd: {:?} body: {}",
                        peer_id,
                        conn.conn_id,
                        cmd,
                        hex::encode(b)
                    );
                }
                log::debug!(
                    "send2 peer_id: {}, tunnel_id: {:?}, cmd: {:?} len: {}",
                    peer_id,
                    conn.conn_id,
                    cmd,
                    len
                );
                let seq = gen_seq();
                let header = CmdHeader::<LEN, CMD>::new(
                    version,
                    false,
                    Some(seq),
                    cmd,
                    Self::encode_pkg_len(len as u64)?,
                );
                let buf = header
                    .to_vec()
                    .map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
                let resp_id = gen_resp_id(conn.conn_id, cmd, seq);
                let waiter = self
                    .resp_waiter
                    .create_timeout_result_future(resp_id, timeout)
                    .map_err(into_cmd_err!(CmdErrorCode::Failed))?;
                {
                    let mut send = conn.send.get().await;
                    if buf.len() > 255 {
                        return Err(cmd_err!(CmdErrorCode::InvalidParam, "header len too large"));
                    }
                    send.write_u8(buf.len() as u8)
                        .await
                        .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                    send.write_all(buf.as_slice())
                        .await
                        .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                    for b in body.iter() {
                        send.write_all(b)
                            .await
                            .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                    }
                    send.flush()
                        .await
                        .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                }
                let body = waiter.await.map_err(into_cmd_err!(CmdErrorCode::Timeout))?;
                Ok(body)
            }
            .await;
            if ret.is_ok() {
                return ret;
            }
        }
        Err(cmd_err!(
            CmdErrorCode::Failed,
            "send to peer_id: {}",
            peer_id
        ))
    }

    async fn send_cmd(
        &self,
        peer_id: &PeerId,
        cmd: CMD,
        version: u8,
        mut body: CmdBody,
    ) -> CmdResult<()> {
        let connections = self.peer_manager.find_connections(peer_id);
        if connections.is_empty() {
            return Err(cmd_err!(
                CmdErrorCode::PeerConnectionNotFound,
                "peer_id: {}",
                peer_id
            ));
        }
        let mut last_err = None;
        for conn in connections {
            let ret: CmdResult<()> = async {
                log::debug!(
                    "send peer_id: {}, tunnel_id {:?}, cmd: {:?}, len: {} data: {}",
                    peer_id,
                    conn.conn_id,
                    cmd,
                    body.len(),
                    "<streaming>"
                );
                let header = CmdHeader::<LEN, CMD>::new(
                    version,
                    false,
                    None,
                    cmd,
                    Self::encode_pkg_len(body.len() as u64)?,
                );
                let buf = header
                    .to_vec()
                    .map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
                let mut send = conn.send.get().await;
                if buf.len() > 255 {
                    return Err(cmd_err!(CmdErrorCode::InvalidParam, "header len too large"));
                }
                send.write_u8(buf.len() as u8)
                    .await
                    .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                send.write_all(buf.as_slice())
                    .await
                    .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                tokio::io::copy(&mut body, send.deref_mut().deref_mut())
                    .await
                    .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                send.flush()
                    .await
                    .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                Ok(())
            }
            .await;
            if ret.is_ok() {
                return Ok(());
            }
            last_err = ret.err();
        }
        Err(last_err
            .unwrap_or_else(|| cmd_err!(CmdErrorCode::Failed, "send to peer_id: {}", peer_id)))
    }

    async fn send_cmd_with_resp(
        &self,
        peer_id: &PeerId,
        cmd: CMD,
        version: u8,
        mut body: CmdBody,
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        let connections = self.peer_manager.find_connections(peer_id);
        if connections.is_empty() {
            return Err(cmd_err!(
                CmdErrorCode::PeerConnectionNotFound,
                "peer_id: {}",
                peer_id
            ));
        }
        let mut last_err = None;
        for conn in connections {
            if let Some(id) = tokio::task::try_id() {
                if self.state_holder.has_state(id) {
                    continue;
                }
            }
            let ret: CmdResult<CmdBody> = async {
                log::debug!(
                    "send peer_id: {}, tunnel_id {:?}, cmd: {:?}, len: {}",
                    peer_id,
                    conn.conn_id,
                    cmd,
                    body.len()
                );
                let seq = gen_seq();
                let header = CmdHeader::<LEN, CMD>::new(
                    version,
                    false,
                    Some(seq),
                    cmd,
                    Self::encode_pkg_len(body.len())?,
                );
                let buf = header
                    .to_vec()
                    .map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
                let resp_id = gen_resp_id(conn.conn_id, cmd, seq);
                let waiter = self
                    .resp_waiter
                    .create_timeout_result_future(resp_id, timeout)
                    .map_err(into_cmd_err!(CmdErrorCode::Failed))?;
                {
                    let mut send = conn.send.get().await;
                    if buf.len() > 255 {
                        return Err(cmd_err!(CmdErrorCode::InvalidParam, "header len too large"));
                    }
                    send.write_u8(buf.len() as u8)
                        .await
                        .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                    send.write_all(buf.as_slice())
                        .await
                        .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                    tokio::io::copy(&mut body, send.deref_mut().deref_mut())
                        .await
                        .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                    send.flush()
                        .await
                        .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                }
                let body = waiter.await.map_err(into_cmd_err!(CmdErrorCode::Timeout))?;
                Ok(body)
            }
            .await;
            if ret.is_ok() {
                return ret;
            }
            last_err = ret.err();
        }
        Err(last_err
            .unwrap_or_else(|| cmd_err!(CmdErrorCode::Failed, "send to peer_id: {}", peer_id)))
    }

    async fn send_by_specify_tunnel(
        &self,
        peer_id: &PeerId,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: &[u8],
    ) -> CmdResult<()> {
        let conn = self.peer_manager.find_connection(tunnel_id);
        if conn.is_none() {
            return Err(cmd_err!(
                CmdErrorCode::PeerConnectionNotFound,
                "tunnel_id: {:?}",
                tunnel_id
            ));
        }
        let conn = conn.unwrap();
        assert_eq!(tunnel_id, conn.conn_id);
        log::trace!(
            "send_by_specify_tunnel peer_id: {}, tunnel_id: {:?}, cmd: {:?}, len: {} data: {}",
            peer_id,
            conn.conn_id,
            cmd,
            body.len(),
            hex::encode(body)
        );
        let header = CmdHeader::<LEN, CMD>::new(
            version,
            false,
            None,
            cmd,
            Self::encode_pkg_len(body.len() as u64)?,
        );
        let buf = header
            .to_vec()
            .map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
        let mut send = conn.send.get().await;
        if buf.len() > 255 {
            return Err(cmd_err!(CmdErrorCode::InvalidParam, "header len too large"));
        }
        send.write_u8(buf.len() as u8)
            .await
            .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        send.write_all(buf.as_slice())
            .await
            .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        send.write_all(body)
            .await
            .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        send.flush()
            .await
            .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        Ok(())
    }

    async fn send_by_specify_tunnel_with_resp(
        &self,
        peer_id: &PeerId,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: &[u8],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        let conn = self.peer_manager.find_connection(tunnel_id);
        if conn.is_none() {
            return Err(cmd_err!(
                CmdErrorCode::PeerConnectionNotFound,
                "tunnel_id: {:?}",
                tunnel_id
            ));
        }
        let conn = conn.unwrap();
        if let Some(id) = tokio::task::try_id() {
            if self.state_holder.has_state(id) {
                return Err(cmd_err!(
                    CmdErrorCode::Failed,
                    "can't send msg with resp in tunnel {:?} msg handle",
                    conn.conn_id
                ));
            }
        }
        assert_eq!(tunnel_id, conn.conn_id);
        log::trace!(
            "send_by_specify_tunnel peer_id: {}, tunnel_id: {:?}, cmd: {:?}, len: {} data: {}",
            peer_id,
            conn.conn_id,
            cmd,
            body.len(),
            hex::encode(body)
        );
        let seq = gen_seq();
        let header = CmdHeader::<LEN, CMD>::new(
            version,
            false,
            Some(seq),
            cmd,
            Self::encode_pkg_len(body.len() as u64)?,
        );
        let buf = header
            .to_vec()
            .map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
        let resp_id = gen_resp_id(conn.conn_id, cmd, seq);
        let waiter = self
            .resp_waiter
            .create_timeout_result_future(resp_id, timeout)
            .map_err(into_cmd_err!(CmdErrorCode::Failed))?;
        {
            let mut send = conn.send.get().await;
            if buf.len() > 255 {
                return Err(cmd_err!(CmdErrorCode::InvalidParam, "header len too large"));
            }
            send.write_u8(buf.len() as u8)
                .await
                .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
            send.write_all(buf.as_slice())
                .await
                .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
            send.write_all(body)
                .await
                .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
            send.flush()
                .await
                .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        }
        let body = waiter.await.map_err(into_cmd_err!(CmdErrorCode::Timeout))?;
        Ok(body)
    }

    async fn send2_by_specify_tunnel(
        &self,
        peer_id: &PeerId,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
    ) -> CmdResult<()> {
        let conn = self.peer_manager.find_connection(tunnel_id);
        if conn.is_none() {
            return Err(cmd_err!(
                CmdErrorCode::PeerConnectionNotFound,
                "tunnel_id: {:?}",
                tunnel_id
            ));
        }
        let conn = conn.unwrap();
        assert_eq!(tunnel_id, conn.conn_id);
        let mut len = 0;
        for b in body.iter() {
            len += b.len();
            log::debug!(
                "send2_by_specify_tunnel peer_id: {}, tunnel_id: {:?}, cmd: {:?} body: {}",
                peer_id,
                conn.conn_id,
                cmd,
                hex::encode(b)
            );
        }
        log::debug!(
            "send2_by_specify_tunnel peer_id: {}, tunnel_id: {:?}, cmd: {:?} len: {}",
            peer_id,
            conn.conn_id,
            cmd,
            len
        );
        let header = CmdHeader::<LEN, CMD>::new(
            version,
            false,
            None,
            cmd,
            Self::encode_pkg_len(len as u64)?,
        );
        let buf = header
            .to_vec()
            .map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
        if buf.len() > 255 {
            return Err(cmd_err!(CmdErrorCode::InvalidParam, "header len too large"));
        }
        let mut send = conn.send.get().await;
        send.write_u8(buf.len() as u8)
            .await
            .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        send.write_all(buf.as_slice())
            .await
            .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        for b in body.iter() {
            send.write_all(b)
                .await
                .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        }
        send.flush()
            .await
            .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        Ok(())
    }

    async fn send2_by_specify_tunnel_with_resp(
        &self,
        peer_id: &PeerId,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        let conn = self.peer_manager.find_connection(tunnel_id);
        if conn.is_none() {
            return Err(cmd_err!(
                CmdErrorCode::PeerConnectionNotFound,
                "tunnel_id: {:?}",
                tunnel_id
            ));
        }
        let conn = conn.unwrap();
        if let Some(id) = tokio::task::try_id() {
            if self.state_holder.has_state(id) {
                return Err(cmd_err!(
                    CmdErrorCode::Failed,
                    "can't send msg with resp in tunnel {:?} msg handle",
                    conn.conn_id
                ));
            }
        }
        assert_eq!(tunnel_id, conn.conn_id);
        let mut len = 0;
        for b in body.iter() {
            len += b.len();
            log::debug!(
                "send2_by_specify_tunnel peer_id: {}, tunnel_id: {:?}, cmd: {:?} body: {}",
                peer_id,
                conn.conn_id,
                cmd,
                hex::encode(b)
            );
        }
        log::debug!(
            "send2_by_specify_tunnel peer_id: {}, tunnel_id: {:?}, cmd: {:?} len: {}",
            peer_id,
            conn.conn_id,
            cmd,
            len
        );
        let seq = gen_seq();
        let header = CmdHeader::<LEN, CMD>::new(
            version,
            false,
            Some(seq),
            cmd,
            Self::encode_pkg_len(len as u64)?,
        );
        let buf = header
            .to_vec()
            .map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
        let resp_id = gen_resp_id(conn.conn_id, cmd, seq);
        let waiter = self
            .resp_waiter
            .create_timeout_result_future(resp_id, timeout)
            .map_err(into_cmd_err!(CmdErrorCode::Failed))?;
        if buf.len() > 255 {
            return Err(cmd_err!(CmdErrorCode::InvalidParam, "header len too large"));
        }
        {
            let mut send = conn.send.get().await;
            send.write_u8(buf.len() as u8)
                .await
                .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
            send.write_all(buf.as_slice())
                .await
                .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
            for b in body.iter() {
                send.write_all(b)
                    .await
                    .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
            }
            send.flush()
                .await
                .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        }
        let body = waiter.await.map_err(into_cmd_err!(CmdErrorCode::Timeout))?;
        Ok(body)
    }

    async fn send_cmd_by_specify_tunnel(
        &self,
        peer_id: &PeerId,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        mut body: CmdBody,
    ) -> CmdResult<()> {
        let conn = self.peer_manager.find_connection(tunnel_id);
        if conn.is_none() {
            return Err(cmd_err!(
                CmdErrorCode::PeerConnectionNotFound,
                "tunnel_id: {:?}",
                tunnel_id
            ));
        }
        let conn = conn.unwrap();
        assert_eq!(tunnel_id, conn.conn_id);
        log::debug!(
            "send_by_specify_tunnel peer_id: {}, tunnel_id: {:?}, cmd: {:?}, len: {}",
            peer_id,
            conn.conn_id,
            cmd,
            body.len()
        );
        let header = CmdHeader::<LEN, CMD>::new(
            version,
            false,
            None,
            cmd,
            Self::encode_pkg_len(body.len())?,
        );
        let buf = header
            .to_vec()
            .map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
        let mut send = conn.send.get().await;
        if buf.len() > 255 {
            return Err(cmd_err!(CmdErrorCode::InvalidParam, "header len too large"));
        }
        send.write_u8(buf.len() as u8)
            .await
            .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        send.write_all(buf.as_slice())
            .await
            .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        tokio::io::copy(&mut body, send.deref_mut().deref_mut())
            .await
            .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        send.flush()
            .await
            .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        Ok(())
    }

    async fn send_cmd_by_specify_tunnel_with_resp(
        &self,
        peer_id: &PeerId,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        mut body: CmdBody,
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        let conn = self.peer_manager.find_connection(tunnel_id);
        if conn.is_none() {
            return Err(cmd_err!(
                CmdErrorCode::PeerConnectionNotFound,
                "tunnel_id: {:?}",
                tunnel_id
            ));
        }
        let conn = conn.unwrap();
        if let Some(id) = tokio::task::try_id() {
            if self.state_holder.has_state(id) {
                return Err(cmd_err!(
                    CmdErrorCode::Failed,
                    "can't send msg with resp in tunnel {:?} msg handle",
                    conn.conn_id
                ));
            }
        }
        assert_eq!(tunnel_id, conn.conn_id);
        log::debug!(
            "send_by_specify_tunnel peer_id: {}, tunnel_id: {:?}, cmd: {:?}, len: {}",
            peer_id,
            conn.conn_id,
            cmd,
            body.len()
        );
        let seq = gen_seq();
        let header = CmdHeader::<LEN, CMD>::new(
            version,
            false,
            Some(seq),
            cmd,
            Self::encode_pkg_len(body.len())?,
        );
        let buf = header
            .to_vec()
            .map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
        let resp_id = gen_resp_id(conn.conn_id, cmd, seq);
        let waiter = self
            .resp_waiter
            .create_timeout_result_future(resp_id, timeout)
            .map_err(into_cmd_err!(CmdErrorCode::Failed))?;
        {
            let mut send = conn.send.get().await;
            if buf.len() > 255 {
                return Err(cmd_err!(CmdErrorCode::InvalidParam, "header len too large"));
            }
            send.write_u8(buf.len() as u8)
                .await
                .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
            send.write_all(buf.as_slice())
                .await
                .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
            tokio::io::copy(&mut body, send.deref_mut().deref_mut())
                .await
                .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
            send.flush()
                .await
                .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        }
        let body = waiter.await.map_err(into_cmd_err!(CmdErrorCode::Timeout))?;
        Ok(body)
    }

    async fn send_by_all_tunnels(
        &self,
        peer_id: &PeerId,
        cmd: CMD,
        version: u8,
        body: &[u8],
    ) -> CmdResult<()> {
        let connections = self.peer_manager.find_connections(peer_id);
        let header = CmdHeader::<LEN, CMD>::new(
            version,
            false,
            None,
            cmd,
            Self::encode_pkg_len(body.len() as u64)?,
        );
        let buf = header
            .to_vec()
            .map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
        if buf.len() > 255 {
            return Err(cmd_err!(CmdErrorCode::InvalidParam, "header len too large"));
        }
        for conn in connections {
            let ret: CmdResult<()> = async {
                let mut send = conn.send.get().await;
                send.write_u8(buf.len() as u8)
                    .await
                    .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                send.write_all(buf.as_slice())
                    .await
                    .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                send.write_all(body)
                    .await
                    .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                send.flush()
                    .await
                    .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                Ok(())
            }
            .await;
            if let Err(e) = ret {
                log::error!(
                    "broadcast send failed peer_id: {}, tunnel_id: {:?}, cmd: {:?}, err: {:?}",
                    peer_id,
                    conn.conn_id,
                    cmd,
                    e
                );
            }
        }
        Ok(())
    }

    async fn send2_by_all_tunnels(
        &self,
        peer_id: &PeerId,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
    ) -> CmdResult<()> {
        let connections = self.peer_manager.find_connections(peer_id);
        let mut len = 0;
        for b in body.iter() {
            len += b.len();
        }
        let header = CmdHeader::<LEN, CMD>::new(
            version,
            false,
            None,
            cmd,
            Self::encode_pkg_len(len as u64)?,
        );
        let buf = header
            .to_vec()
            .map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
        if buf.len() > 255 {
            return Err(cmd_err!(CmdErrorCode::InvalidParam, "header len too large"));
        }
        for conn in connections {
            let ret: CmdResult<()> = async {
                let mut send = conn.send.get().await;
                send.write_u8(buf.len() as u8)
                    .await
                    .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                send.write_all(buf.as_slice())
                    .await
                    .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                for b in body.iter() {
                    send.write_all(b)
                        .await
                        .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                }
                send.flush()
                    .await
                    .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                Ok(())
            }
            .await;
            if let Err(e) = ret {
                log::error!(
                    "broadcast send2 failed peer_id: {}, tunnel_id: {:?}, cmd: {:?}, err: {:?}",
                    peer_id,
                    conn.conn_id,
                    cmd,
                    e
                );
            }
        }
        Ok(())
    }
}

pub struct DefaultCmdServerIncoming<
    M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    LISTENER,
> {
    tunnel_listener: LISTENER,
    tunnel_service: Arc<dyn CmdTunnelService<M, R, W>>,
    _p: PhantomData<fn() -> (M, R, W)>,
}

impl<
    M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    LISTENER: CmdTunnelListener<M, R, W>,
> DefaultCmdServerIncoming<M, R, W, LISTENER>
{
    pub fn new(
        tunnel_listener: LISTENER,
        tunnel_service: Arc<dyn CmdTunnelService<M, R, W>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            tunnel_listener,
            tunnel_service,
            _p: PhantomData,
        })
    }

    pub fn start(self: &Arc<Self>) {
        let this = self.clone();
        tokio::spawn(async move {
            if let Err(e) = this.run().await {
                log::error!("cmd server error: {:?}", e);
            }
        });
    }

    pub async fn run(&self) -> CmdResult<()> {
        loop {
            let tunnel = self.tunnel_listener.accept().await?;
            let tunnel_service = self.tunnel_service.clone();
            tokio::spawn(async move {
                if let Err(e) = tunnel_service.handle_tunnel(tunnel).await {
                    log::error!("peer connection error: {:?}", e);
                }
            });
        }
    }
}

pub struct DefaultCmdServer<
    M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    LEN,
    CMD,
    LISTENER,
> {
    incoming: Arc<DefaultCmdServerIncoming<M, R, W, LISTENER>>,
    service: Arc<DefaultCmdServerService<M, R, W, LEN, CMD>>,
}

impl<
    M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    LEN: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + RawFixedBytes
        + Sync
        + Send
        + 'static
        + FromPrimitive
        + ToPrimitive,
    CMD: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + RawFixedBytes
        + Sync
        + Send
        + 'static
        + Eq
        + Hash
        + Debug,
    LISTENER: CmdTunnelListener<M, R, W>,
> DefaultCmdServer<M, R, W, LEN, CMD, LISTENER>
{
    pub fn new(tunnel_listener: LISTENER) -> Arc<Self> {
        let service = DefaultCmdServerService::new();
        let incoming = DefaultCmdServerIncoming::new(tunnel_listener, service.clone());
        Arc::new(Self { incoming, service })
    }

    pub fn incoming(&self) -> Arc<DefaultCmdServerIncoming<M, R, W, LISTENER>> {
        self.incoming.clone()
    }

    pub fn service(&self) -> Arc<DefaultCmdServerService<M, R, W, LEN, CMD>> {
        self.service.clone()
    }

    pub fn attach_event_listener(&self, event_listener: Arc<dyn CmdServerEventListener>) {
        self.service.attach_event_listener(event_listener);
    }

    pub async fn get_peer_tunnels(&self, peer_id: &PeerId) -> Vec<Arc<PeerConnection<R, W>>> {
        self.service.get_peer_tunnels(peer_id).await
    }

    pub fn start(self: &Arc<Self>) {
        self.incoming.start();
    }
}

impl<M: CmdTunnelMeta, R: CmdTunnelRead<M>, W: CmdTunnelWrite<M>, LEN, CMD, LISTENER> Deref
    for DefaultCmdServer<M, R, W, LEN, CMD, LISTENER>
{
    type Target = DefaultCmdServerService<M, R, W, LEN, CMD>;

    fn deref(&self) -> &Self::Target {
        self.service.as_ref()
    }
}

#[async_trait::async_trait]
impl<
    M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    LEN: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + RawFixedBytes
        + Sync
        + Send
        + 'static
        + FromPrimitive
        + ToPrimitive,
    CMD: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + RawFixedBytes
        + Sync
        + Send
        + 'static
        + Eq
        + Hash
        + Debug,
    LISTENER: CmdTunnelListener<M, R, W>,
> CmdServer<LEN, CMD> for DefaultCmdServer<M, R, W, LEN, CMD, LISTENER>
{
    fn register_cmd_handler(&self, cmd: CMD, handler: impl CmdHandler<LEN, CMD>) {
        self.service.register_cmd_handler(cmd, handler);
    }

    async fn send(&self, peer_id: &PeerId, cmd: CMD, version: u8, body: &[u8]) -> CmdResult<()> {
        self.service.send(peer_id, cmd, version, body).await
    }

    async fn send_with_resp(
        &self,
        peer_id: &PeerId,
        cmd: CMD,
        version: u8,
        body: &[u8],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        self.service
            .send_with_resp(peer_id, cmd, version, body, timeout)
            .await
    }

    async fn send2(
        &self,
        peer_id: &PeerId,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
    ) -> CmdResult<()> {
        self.service.send2(peer_id, cmd, version, body).await
    }

    async fn send2_with_resp(
        &self,
        peer_id: &PeerId,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        self.service
            .send2_with_resp(peer_id, cmd, version, body, timeout)
            .await
    }

    async fn send_cmd(
        &self,
        peer_id: &PeerId,
        cmd: CMD,
        version: u8,
        body: CmdBody,
    ) -> CmdResult<()> {
        self.service.send_cmd(peer_id, cmd, version, body).await
    }

    async fn send_cmd_with_resp(
        &self,
        peer_id: &PeerId,
        cmd: CMD,
        version: u8,
        body: CmdBody,
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        self.service
            .send_cmd_with_resp(peer_id, cmd, version, body, timeout)
            .await
    }

    async fn send_by_specify_tunnel(
        &self,
        peer_id: &PeerId,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: &[u8],
    ) -> CmdResult<()> {
        self.service
            .send_by_specify_tunnel(peer_id, tunnel_id, cmd, version, body)
            .await
    }

    async fn send_by_specify_tunnel_with_resp(
        &self,
        peer_id: &PeerId,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: &[u8],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        self.service
            .send_by_specify_tunnel_with_resp(peer_id, tunnel_id, cmd, version, body, timeout)
            .await
    }

    async fn send2_by_specify_tunnel(
        &self,
        peer_id: &PeerId,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
    ) -> CmdResult<()> {
        self.service
            .send2_by_specify_tunnel(peer_id, tunnel_id, cmd, version, body)
            .await
    }

    async fn send2_by_specify_tunnel_with_resp(
        &self,
        peer_id: &PeerId,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        self.service
            .send2_by_specify_tunnel_with_resp(peer_id, tunnel_id, cmd, version, body, timeout)
            .await
    }

    async fn send_cmd_by_specify_tunnel(
        &self,
        peer_id: &PeerId,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: CmdBody,
    ) -> CmdResult<()> {
        self.service
            .send_cmd_by_specify_tunnel(peer_id, tunnel_id, cmd, version, body)
            .await
    }

    async fn send_cmd_by_specify_tunnel_with_resp(
        &self,
        peer_id: &PeerId,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: CmdBody,
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        self.service
            .send_cmd_by_specify_tunnel_with_resp(peer_id, tunnel_id, cmd, version, body, timeout)
            .await
    }

    async fn send_by_all_tunnels(
        &self,
        peer_id: &PeerId,
        cmd: CMD,
        version: u8,
        body: &[u8],
    ) -> CmdResult<()> {
        self.service
            .send_by_all_tunnels(peer_id, cmd, version, body)
            .await
    }

    async fn send2_by_all_tunnels(
        &self,
        peer_id: &PeerId,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
    ) -> CmdResult<()> {
        self.service
            .send2_by_all_tunnels(peer_id, cmd, version, body)
            .await
    }
}
