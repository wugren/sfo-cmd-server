use std::hash::Hash;
use std::sync::{Arc, Mutex};
use bucky_raw_codec::{RawConvertTo, RawDecode, RawEncode, RawFixedBytes, RawFrom};
use num::{FromPrimitive, ToPrimitive};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::cmd::{CmdHandler, CmdHandlerMap, CmdHeader};
use crate::{CmdTunnel, TunnelId};
use crate::errors::{cmd_err, into_cmd_err, CmdErrorCode, CmdResult};
use crate::peer_connection::PeerConnection;
use crate::peer_id::PeerId;
use super::peer_manager::{PeerManager, PeerManagerRef};

#[async_trait::async_trait]
pub trait CmdTunnelListener<T: CmdTunnel>: Send + Sync + 'static {
    async fn accept(&self) -> CmdResult<Arc<T>>;
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

pub struct CmdServer<LEN, CMD, T: CmdTunnel, LISTENER> {
    tunnel_listener: LISTENER,
    cmd_handler_map: Arc<CmdHandlerMap<LEN, CMD>>,
    peer_manager: PeerManagerRef<T>,
    event_emit: CmdServerEventListenerEmit,
    _l: std::marker::PhantomData<(LEN, CMD, T)>,
}

impl<LEN: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send + 'static + Eq + Hash,
    T: CmdTunnel,
    LISTENER: CmdTunnelListener<T>> CmdServer<LEN, CMD, T, LISTENER> {
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

    pub fn register_cmd_handler(&self, cmd: CMD, handler: impl CmdHandler<LEN, CMD>) {
        self.cmd_handler_map.insert(cmd, handler);
    }

    pub async fn send_to_peer(&self, peer_id: &PeerId, cmd: CMD, body: &[u8]) -> CmdResult<()> {
        let connections = self.peer_manager.find_connections(peer_id);
        for conn in connections {
            let ret: CmdResult<()> = async move {
                let mut conn = conn.lock().await;
                let header = CmdHeader::<LEN, CMD>::new(cmd, LEN::from_u64(body.len() as u64).unwrap());
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

    pub async fn send_to_peer_from_tunnel(&self, peer_id: &PeerId, tunnel_id: TunnelId, cmd: CMD, body: &[u8]) -> CmdResult<()> {
        let conn = self.peer_manager.find_connection(tunnel_id);
        if conn.is_none() {
            return Err(cmd_err!(CmdErrorCode::PeerConnectionNotFound, "tunnel_id: {:?}", tunnel_id));
        }
        let conn = conn.unwrap();
        let mut conn = conn.lock().await;
        let header = CmdHeader::<LEN, CMD>::new(cmd, LEN::from_u64(body.len() as u64).unwrap());
        let buf = header.to_vec().map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
        conn.send.write_all(buf.as_slice()).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        conn.send.write_all(body).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        conn.send.flush().await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        Ok(())
    }

    pub async fn send_to_peer_from_all_tunnels(&self, peer_id: &PeerId, cmd: CMD, body: &[u8]) -> CmdResult<()> {
        let connections = self.peer_manager.find_connections(peer_id);
        for conn in connections {
            let _ret: CmdResult<()> = async move {
                let mut conn = conn.lock().await;
                let header = CmdHeader::<LEN, CMD>::new(cmd, LEN::from_u64(body.len() as u64).unwrap());
                let buf = header.to_vec().map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
                conn.send.write_all(buf.as_slice()).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                conn.send.write_all(body).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                conn.send.flush().await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                Ok(())
            }.await;
        }
        Ok(())
    }

    pub async fn get_peer_tunnels(&self, peer_id: &PeerId) -> Vec<Arc<T>> {
        let connections = self.peer_manager.find_connections(peer_id);
        let mut tunnels = Vec::new();
        for conn in connections {
            let conn = conn.lock().await;
            tunnels.push(conn.tunnel.clone());
        }
        tunnels
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
                    let (mut reader, writer) = tunnel.split()?;
                    let recv_handle = tokio::spawn(async move {
                        let ret: CmdResult<()> = async move {
                            loop {
                                let mut header = vec![0u8; CmdHeader::<LEN, CMD>::raw_bytes().unwrap()];
                                let n = reader.read_exact(&mut header).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                                if n == 0 {
                                    break;
                                }
                                let header = CmdHeader::<LEN, CMD>::clone_from_slice(&header).unwrap();
                                let mut buf = vec![0u8; header.pkg_len().to_u64().unwrap_or(0) as usize];
                                if buf.len() > 0 {
                                    let n = reader.read_exact(&mut buf).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                                    if n == 0 {
                                        break;
                                    }
                                }

                                if let Some(handler) = cmd_handler_map.get(header.cmd_code()) {
                                    if let Err(e) = handler.handle(remote_id.clone(), tunnel_id, header, buf).await {
                                        log::error!("handle cmd error: {:?}", e);
                                    }
                                }
                                // if let Err(e) = this.handle_cmd(tls_key.as_slice(), header, buf).await {
                                //     log::error!("handle cmd error: {:?}", e);
                                // }
                            }
                            Ok(())
                        }.await;
                        ret
                    });

                    let peer_conn = PeerConnection {
                        conn_id: tunnel_id,
                        peer_id: peer_id.clone(),
                        tunnel,
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

