use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use bucky_raw_codec::{RawConvertTo, RawDecode, RawEncode, RawFixedBytes, RawFrom};
use num::{FromPrimitive, ToPrimitive};
use sfo_split::Splittable;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::cmd::{CmdBodyReadImpl, CmdHandler, CmdHandlerMap, CmdHeader};
use crate::{CmdTunnelRead, CmdTunnelWrite, TunnelId};
use crate::errors::{cmd_err, into_cmd_err, CmdErrorCode, CmdResult};
use crate::peer_connection::PeerConnection;
use crate::peer_id::PeerId;
use crate::server::CmdServer;
use super::peer_manager::{PeerManager, PeerManagerRef};

#[async_trait::async_trait]
pub trait CmdTunnelListener<R: CmdTunnelRead, W: CmdTunnelWrite>: Send + Sync + 'static {
    async fn accept(&self) -> CmdResult<Splittable<R, W>>;
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
        let listeners = {
            self.listeners.lock().unwrap().clone()
        };
        for listener in listeners.iter() {
            if let Err(e) = listener.on_peer_connected(peer_id).await {
                log::error!("on_peer_connected error: {:?}", e);
            }
        }
        Ok(())
    }

    async fn on_peer_disconnected(&self, peer_id: &PeerId) -> CmdResult<()> {
        let listeners = {
            self.listeners.lock().unwrap().clone()
        };
        for listener in listeners.iter() {
            if let Err(e) = listener.on_peer_disconnected(peer_id).await {
                log::error!("on_peer_disconnected error: {:?}", e);
            }
        }
        Ok(())
    }
}

pub struct DefaultCmdServer<R: CmdTunnelRead, W: CmdTunnelWrite, LEN, CMD, LISTENER> {
    tunnel_listener: LISTENER,
    cmd_handler_map: Arc<CmdHandlerMap<LEN, CMD>>,
    peer_manager: PeerManagerRef<R, W>,
    event_emit: CmdServerEventListenerEmit,
    _l: Mutex<std::marker::PhantomData<(R, W, LEN, CMD)>>,
}

impl<R: CmdTunnelRead,
    W: CmdTunnelWrite,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send + 'static + Eq + Hash,
    LISTENER: CmdTunnelListener<R, W>> DefaultCmdServer<R, W, LEN, CMD, LISTENER> {
    pub fn new(tunnel_listener: LISTENER) -> Arc<Self> {
        let event_emit = CmdServerEventListenerEmit::new();
        Arc::new(Self {
            tunnel_listener,
            cmd_handler_map: Arc::new(CmdHandlerMap::new()),
            peer_manager: PeerManager::new(Arc::new(event_emit.clone())),
            event_emit,
            _l: Default::default(),
        })
    }

    pub fn attach_event_listener(&self, event_listener: Arc<dyn CmdServerEventListener>) {
        self.event_emit.attach_event_listener(event_listener);
    }

    pub async fn get_peer_tunnels(&self, peer_id: &PeerId) -> Vec<Arc<tokio::sync::Mutex<PeerConnection<R, W>>>> {
        let connections = self.peer_manager.find_connections(peer_id);
        connections
    }

    pub fn start(self: &Arc<Self>) {
        let this = self.clone();
        tokio::spawn(async move {
            if let Err(e) = this.run().await {
                log::error!("cmd server error: {:?}", e);
            }
        });
    }

    async fn run(self: &Arc<Self>) -> CmdResult<()> {
        loop {
            let tunnel = self.tunnel_listener.accept().await?;
            let peer_id = tunnel.get_remote_peer_id();
            let tunnel_id = self.peer_manager.generate_conn_id();
            let this = self.clone();
            tokio::spawn(async move {
                let remote_id = peer_id.clone();
                let ret: CmdResult<()> = async move {
                    let this = this.clone();
                    let cmd_handler_map = this.cmd_handler_map.clone();
                    let (mut reader, writer) = tunnel.split();
                    let recv_handle = tokio::spawn(async move {
                        let ret: CmdResult<()> = async move {
                            loop {
                                let mut header = vec![0u8; CmdHeader::<LEN, CMD>::raw_bytes().unwrap()];
                                let n = reader.read_exact(&mut header).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                                if n == 0 {
                                    break;
                                }
                                let header = CmdHeader::<LEN, CMD>::clone_from_slice(header.as_slice()).map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
                                let cmd_read = Box::new(CmdBodyReadImpl::new(reader, header.pkg_len().to_u64().unwrap() as usize));
                                let waiter = cmd_read.get_waiter();
                                let future = waiter.create_result_future();
                                {
                                    let body_read = cmd_read;
                                    if let Some(handler) = cmd_handler_map.get(header.cmd_code()) {
                                        if let Err(e) = handler.handle(remote_id.clone(), tunnel_id, header, body_read).await {
                                            log::error!("handle cmd error: {:?}", e);
                                        }
                                    }
                                };
                                reader = future.await.map_err(into_cmd_err!(CmdErrorCode::Failed))??;
                                // }
                            }
                            Ok(())
                        }.await;
                        ret
                    });

                    let peer_conn = PeerConnection {
                        conn_id: tunnel_id,
                        peer_id: peer_id.clone(),
                        send: writer,
                        handle: Some(recv_handle),
                    };
                    this.peer_manager.add_peer_connection(peer_conn).await;
                    Ok(())
                }.await;
                if let Err(e) = ret {
                    log::error!("peer connection error: {:?}", e);
                }
            });
        }
    }
}

#[async_trait::async_trait]
impl<R: CmdTunnelRead,
    W: CmdTunnelWrite,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send + 'static + Eq + Hash + Debug,
    LISTENER: CmdTunnelListener<R, W>> CmdServer<LEN, CMD> for DefaultCmdServer<R, W, LEN, CMD, LISTENER> {
    fn register_cmd_handler(&self, cmd: CMD, handler: impl CmdHandler<LEN, CMD>) {
        self.cmd_handler_map.insert(cmd, handler);
    }

    async fn send(&self, peer_id: &PeerId, cmd: CMD, version: u8, body: &[u8]) -> CmdResult<()> {
        let connections = self.peer_manager.find_connections(peer_id);
        for conn in connections {
            let ret: CmdResult<()> = async move {
                let mut conn = conn.lock().await;
                log::trace!("send peer_id: {}, tunnel_id {:?}, cmd: {:?}, len: {} data: {}", peer_id, conn.conn_id, cmd, body.len(), hex::encode(body));
                let header = CmdHeader::<LEN, CMD>::new(version, cmd, LEN::from_u64(body.len() as u64).unwrap());
                let buf = header.to_vec().map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
                conn.send.write_all(buf.as_slice()).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                conn.send.write_all(body).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                conn.send.flush().await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                Ok(())
            }.await;
            if ret.is_ok() {
                break;
            }
        }
        Ok(())
    }

    async fn send2(&self, peer_id: &PeerId, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()> {
        let connections = self.peer_manager.find_connections(peer_id);
        for conn in connections {
            let ret: CmdResult<()> = async move {
                let mut conn = conn.lock().await;
                let mut len = 0;
                for b in body.iter() {
                    len += b.len();
                    log::trace!("send2 peer_id: {}, tunnel_id: {:?}, cmd: {:?} body: {}", peer_id, conn.conn_id, cmd, hex::encode(b));
                }
                log::trace!("send2 peer_id: {}, tunnel_id: {:?}, cmd: {:?} len: {}", peer_id, conn.conn_id, cmd, len);
                let header = CmdHeader::<LEN, CMD>::new(version, cmd, LEN::from_u64(len as u64).unwrap());
                let buf = header.to_vec().map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
                conn.send.write_all(buf.as_slice()).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                for b in body.iter() {
                    conn.send.write_all(b).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                }
                conn.send.flush().await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                Ok(())
            }.await;
            if ret.is_ok() {
                break;
            }
        }
        Ok(())
    }

    async fn send_by_specify_tunnel(&self, peer_id: &PeerId, tunnel_id: TunnelId, cmd: CMD, version: u8, body: &[u8]) -> CmdResult<()> {
        let conn = self.peer_manager.find_connection(tunnel_id);
        if conn.is_none() {
            return Err(cmd_err!(CmdErrorCode::PeerConnectionNotFound, "tunnel_id: {:?}", tunnel_id));
        }
        let conn = conn.unwrap();
        let mut conn = conn.lock().await;
        assert_eq!(tunnel_id, conn.conn_id);
        log::trace!("send_by_specify_tunnel peer_id: {}, tunnel_id: {:?}, cmd: {:?}, len: {} data: {}", peer_id, conn.conn_id, cmd, body.len(), hex::encode(body));
        let header = CmdHeader::<LEN, CMD>::new(version, cmd, LEN::from_u64(body.len() as u64).unwrap());
        let buf = header.to_vec().map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
        conn.send.write_all(buf.as_slice()).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        conn.send.write_all(body).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        conn.send.flush().await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        Ok(())
    }

    async fn send2_by_specify_tunnel(&self, peer_id: &PeerId, tunnel_id: TunnelId, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()> {
        let conn = self.peer_manager.find_connection(tunnel_id);
        if conn.is_none() {
            return Err(cmd_err!(CmdErrorCode::PeerConnectionNotFound, "tunnel_id: {:?}", tunnel_id));
        }
        let conn = conn.unwrap();
        let mut conn = conn.lock().await;
        assert_eq!(tunnel_id, conn.conn_id);
        let mut len = 0;
        for b in body.iter() {
            len += b.len();
            log::trace!("send2_by_specify_tunnel peer_id: {}, tunnel_id: {:?}, cmd: {:?} body: {}", peer_id, conn.conn_id, cmd, hex::encode(b));
        }
        log::trace!("send2_by_specify_tunnel peer_id: {}, tunnel_id: {:?}, cmd: {:?} len: {}", peer_id, conn.conn_id, cmd, len);
        let header = CmdHeader::<LEN, CMD>::new(version, cmd, LEN::from_u64(len as u64).unwrap());
        let buf = header.to_vec().map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
        conn.send.write_all(buf.as_slice()).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        for b in body.iter() {
            conn.send.write_all(b).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        }
        conn.send.flush().await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        Ok(())
    }

    async fn send_by_all_tunnels(&self, peer_id: &PeerId, cmd: CMD, version: u8, body: &[u8]) -> CmdResult<()> {
        let connections = self.peer_manager.find_connections(peer_id);
        for conn in connections {
            let _ret: CmdResult<()> = async move {
                let mut conn = conn.lock().await;
                let header = CmdHeader::<LEN, CMD>::new(version, cmd, LEN::from_u64(body.len() as u64).unwrap());
                let buf = header.to_vec().map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
                conn.send.write_all(buf.as_slice()).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                conn.send.write_all(body).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                conn.send.flush().await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                Ok(())
            }.await;
        }
        Ok(())
    }

    async fn send2_by_all_tunnels(&self, peer_id: &PeerId, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()> {
        let connections = self.peer_manager.find_connections(peer_id);
        let mut len = 0;
        for b in body.iter() {
            len += b.len();
        }
        for conn in connections {
            let _ret: CmdResult<()> = async move {
                let mut conn = conn.lock().await;
                let header = CmdHeader::<LEN, CMD>::new(version, cmd, LEN::from_u64(len as u64).unwrap());
                let buf = header.to_vec().map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
                conn.send.write_all(buf.as_slice()).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                for b in body.iter() {
                    conn.send.write_all(b).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                }
                conn.send.flush().await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                Ok(())
            }.await;
        }
        Ok(())
    }
}
