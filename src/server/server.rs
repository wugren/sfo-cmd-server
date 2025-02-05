use std::hash::Hash;
use std::sync::{Arc, Mutex};
use bucky_raw_codec::{RawConvertTo, RawDecode, RawEncode, RawFixedBytes, RawFrom};
use num::{FromPrimitive, ToPrimitive};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::cmd::{CmdHandler, CmdHandlerMap, CmdHeader};
use crate::{CmdTunnel};
use crate::errors::{into_cmd_err, CmdErrorCode, CmdResult};
use crate::peer_connection::PeerConnection;
use crate::peer_id::PeerId;
use super::peer_manager::{PeerManager, PeerManagerRef};

#[async_trait::async_trait]
pub trait CmdTunnelListener: Send + Sync + 'static {
    async fn accept(&self) -> CmdResult<Arc<dyn CmdTunnel>>;
}

pub struct CmdServer<LEN, CMD, LISTENER> {
    tunnel_listener: LISTENER,
    cmd_handler_map: Arc<CmdHandlerMap<LEN, CMD>>,
    peer_manager: PeerManagerRef,
    _l: std::marker::PhantomData<LEN>,
    _c: std::marker::PhantomData<CMD>,
}

impl<LEN: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send + 'static + Eq + Hash,
    LISTENER: CmdTunnelListener> CmdServer<LEN, CMD, LISTENER> {
    pub fn new(tunnel_listener: LISTENER) -> Arc<Self> {
        Arc::new(Self {
            tunnel_listener,
            cmd_handler_map: Arc::new(CmdHandlerMap::new()),
            peer_manager: PeerManager::new(),
            _l: Default::default(),
            _c: Default::default(),
        })
    }

    pub fn attach_cmd_handler(&self, cmd: CMD, handler: impl CmdHandler<LEN, CMD>) {
        self.cmd_handler_map.insert(cmd, handler);
    }

    pub async fn send_to_peer(&self, peer_id: &PeerId, cmd: CMD, body: &[u8]) -> CmdResult<()> {
        let connections = self.peer_manager.find_connections(peer_id);
        for conn in connections {
            let mut conn = conn.lock().await;
            let header = CmdHeader::<LEN, CMD>::new(cmd, LEN::from_u64(body.len() as u64).unwrap());
            let buf = header.to_vec().map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
            conn.send.write_all(buf.as_slice()).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
            conn.send.write_all(body).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        }
        Ok(())
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
            let tls_key = tunnel.get_tls_key();
            let peer_id = PeerId::from(tls_key.as_slice());
            let this = self.clone();
            tokio::spawn(async move {
                let remote_id = peer_id.clone();
                let ret: CmdResult<()> = async move {
                    let this = this.clone();
                    let cmd_handler_map = this.cmd_handler_map.clone();
                    let (mut reader, mut writer) = tunnel.split()?;
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
                                    if let Err(e) = handler.handle(remote_id.clone(), header, buf).await {
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
                        conn_id: Default::default(),
                        peer_id: peer_id.clone(),
                        tunnel,
                        send: writer,
                        handle: Some(recv_handle),
                    };
                    this.peer_manager.add_peer_connection(peer_conn);
                    Ok(())
                }.await;
                if let Err(e) = ret {
                    log::error!("peer connection error: {:?}", e);
                }
            });
        }
    }
}

