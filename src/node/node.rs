use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use bucky_raw_codec::{RawDecode, RawEncode, RawFixedBytes};
use num::{FromPrimitive, ToPrimitive};
use sfo_pool::{ClassifiedWorker, ClassifiedWorkerFactory, ClassifiedWorkerGuard, ClassifiedWorkerPool, ClassifiedWorkerPoolRef, PoolErrorCode, PoolResult};
use sfo_split::{Splittable};
use crate::{into_pool_err, pool_err, CmdHandler, CmdHeader, CmdNode, CmdTunnelRead, CmdTunnelWrite, PeerId, TunnelId, TunnelIdGenerator};
use crate::client::{CmdSend};
use crate::cmd::{CmdHandlerMap};
use crate::errors::{into_cmd_err, CmdErrorCode, CmdResult};
use crate::node::create_recv_handle;
use crate::server::{CmdTunnelListener};

#[async_trait::async_trait]
pub trait CmdNodeTunnelFactory<R: CmdTunnelRead, W: CmdTunnelWrite>: Send + Sync + 'static {
    async fn create_tunnel(&self, remote_id: &PeerId) -> CmdResult<Splittable<R, W>>;
}


impl<R, W, LEN, CMD> ClassifiedWorker<(PeerId, Option<TunnelId>)> for CmdSend<R, W, LEN, CMD>
where R: CmdTunnelRead,
      W: CmdTunnelWrite,
      LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
      CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug{
    fn is_work(&self) -> bool {
        self.is_work && !self.recv_handle.is_finished()
    }

    fn is_valid(&self, c: (PeerId, Option<TunnelId>)) -> bool {
        let (peer_id, tunnel_id) = c;
        if tunnel_id.is_some() {
            self.tunnel_id == tunnel_id.unwrap() && peer_id == self.write.get_remote_peer_id()
        } else {
            peer_id == self.write.get_remote_peer_id()
        }
    }

    fn classification(&self) -> (PeerId, Option<TunnelId>) {
        (self.write.get_remote_peer_id().clone(), Some(self.tunnel_id))
    }
}

struct CmdWriteFactoryImpl<R: CmdTunnelRead,
    W: CmdTunnelWrite,
    F: CmdNodeTunnelFactory<R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug,
    LISTENER: CmdTunnelListener<R, W>> {
    tunnel_listener: LISTENER,
    tunnel_factory: F,
    cmd_handler: Arc<dyn CmdHandler<LEN, CMD>>,
    tunnel_id_generator: TunnelIdGenerator,
    send_cache: Arc<Mutex<HashMap<PeerId, Vec<CmdSend<R, W, LEN, CMD>>>>>,
}


impl<R: CmdTunnelRead,
    W: CmdTunnelWrite,
    F: CmdNodeTunnelFactory<R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive + RawFixedBytes,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug + RawFixedBytes,
    LISTENER: CmdTunnelListener<R, W>> CmdWriteFactoryImpl<R, W, F, LEN, CMD, LISTENER> {
    pub fn new(tunnel_factory: F,
               tunnel_listener: LISTENER,
               cmd_handler: impl CmdHandler<LEN, CMD>) -> Self {
        Self {
            tunnel_listener,
            tunnel_factory,
            cmd_handler: Arc::new(cmd_handler),
            tunnel_id_generator: TunnelIdGenerator::new(),
            send_cache: Arc::new(Mutex::new(Default::default())),
        }
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
            let tunnel_id = self.tunnel_id_generator.generate();
            let this = self.clone();
            tokio::spawn(async move {
                let ret: CmdResult<()> = async move {
                    let this = this.clone();
                    let cmd_handler = this.cmd_handler.clone();
                    let (reader, writer) = tunnel.split();
                    let recv_handle = create_recv_handle::<R, W, LEN, CMD>(reader, tunnel_id, cmd_handler);
                    {
                        let mut send_cache = this.send_cache.lock().unwrap();
                        let send_list = send_cache.entry(peer_id).or_insert(Vec::new());
                        send_list.push(CmdSend::new(tunnel_id, recv_handle, writer));
                    }
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
    F: CmdNodeTunnelFactory<R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive + RawFixedBytes,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + RawFixedBytes + Debug,
    LISTENER: CmdTunnelListener<R, W>> ClassifiedWorkerFactory<(PeerId, Option<TunnelId>), CmdSend<R, W, LEN, CMD>> for CmdWriteFactoryImpl<R, W, F, LEN, CMD, LISTENER> {
    async fn create(&self, c: Option<(PeerId, Option<TunnelId>)>) -> PoolResult<CmdSend<R, W, LEN, CMD>> {
        if c.is_some() {
            let (peer_id, tunnel_id) = c.unwrap();
            if tunnel_id.is_some() {
                let mut send_cache = self.send_cache.lock().unwrap();
                if let Some(send_list) = send_cache.get_mut(&peer_id) {
                    let mut send_index = None;
                    for (index, send) in send_list.iter().enumerate() {
                        if send.get_tunnel_id() == tunnel_id.unwrap() {
                            send_index = Some(index);
                            break;
                        }
                    }
                    if let Some(send_index) = send_index {
                        let send = send_list.remove(send_index);
                        Ok(send)
                    } else {
                        Err(pool_err!(PoolErrorCode::Failed, "tunnel {:?} not found", tunnel_id.unwrap()))
                    }
                } else {
                    Err(pool_err!(PoolErrorCode::Failed, "tunnel {:?} not found", tunnel_id.unwrap()))
                }
            } else {
                {
                    let mut send_cache = self.send_cache.lock().unwrap();
                    if let Some(send_list) = send_cache.get_mut(&peer_id) {
                        if !send_list.is_empty() {
                            let send = send_list.pop().unwrap();
                            if send_list.is_empty() {
                                send_cache.remove(&peer_id);
                            }
                            return Ok(send);
                        }
                    }
                }
                let tunnel = self.tunnel_factory.create_tunnel(&peer_id).await.map_err(into_pool_err!(PoolErrorCode::Failed))?;
                let tunnel_id = self.tunnel_id_generator.generate();
                let (recv, write) = tunnel.split();
                let cmd_handler = self.cmd_handler.clone();
                let handle = create_recv_handle::<R, W, LEN, CMD>(recv, tunnel_id, cmd_handler);
                Ok(CmdSend::new(tunnel_id, handle, write))
            }
        } else {
            Err(pool_err!(PoolErrorCode::Failed, "peer id is none"))
        }
    }
}

struct CmdWriteFactory<R: CmdTunnelRead,
    W: CmdTunnelWrite,
    F: CmdNodeTunnelFactory<R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug,
    LISTENER: CmdTunnelListener<R, W>> {
    inner: Arc<CmdWriteFactoryImpl<R, W, F, LEN, CMD, LISTENER>>
}


impl<R: CmdTunnelRead,
    W: CmdTunnelWrite,
    F: CmdNodeTunnelFactory<R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive + RawFixedBytes,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug + RawFixedBytes,
    LISTENER: CmdTunnelListener<R, W>> CmdWriteFactory<R, W, F, LEN, CMD, LISTENER> {
    pub fn new(tunnel_factory: F,
               tunnel_listener: LISTENER,
               cmd_handler: impl CmdHandler<LEN, CMD>) -> Self {
        Self {
            inner: Arc::new(CmdWriteFactoryImpl::new(tunnel_factory, tunnel_listener, cmd_handler)),
        }
    }

    pub fn start(&self) {
        self.inner.start();
    }
}

#[async_trait::async_trait]
impl<R: CmdTunnelRead,
    W: CmdTunnelWrite,
    F: CmdNodeTunnelFactory<R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive + RawFixedBytes,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + RawFixedBytes + Debug,
    LISTENER: CmdTunnelListener<R, W>> ClassifiedWorkerFactory<(PeerId, Option<TunnelId>), CmdSend<R, W, LEN, CMD>> for CmdWriteFactory<R, W, F, LEN, CMD, LISTENER> {
    async fn create(&self, c: Option<(PeerId, Option<TunnelId>)>) -> PoolResult<CmdSend<R, W, LEN, CMD>> {
        self.inner.create(c).await
    }
}
pub struct DefaultCmdNode<R: CmdTunnelRead,
    W: CmdTunnelWrite,
    F: CmdNodeTunnelFactory<R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive + RawFixedBytes,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + RawFixedBytes + Eq + Hash + Debug,
    LISTENER: CmdTunnelListener<R, W>> {
    tunnel_pool: ClassifiedWorkerPoolRef<(PeerId, Option<TunnelId>), CmdSend<R, W, LEN, CMD>, CmdWriteFactory<R, W, F, LEN, CMD, LISTENER>>,
    cmd_handler_map: Arc<CmdHandlerMap<LEN, CMD>>,
}

impl<R: CmdTunnelRead,
    W: CmdTunnelWrite,
    F: CmdNodeTunnelFactory<R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync +'static + FromPrimitive + ToPrimitive + RawFixedBytes,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync +'static + RawFixedBytes + Eq + Hash + Debug,
    LISTENER: CmdTunnelListener<R, W>> DefaultCmdNode<R, W, F, LEN, CMD, LISTENER> {
    pub fn new(listener: LISTENER, factory: F, tunnel_count: u16) -> Arc<Self> {
        let cmd_handler_map = Arc::new(CmdHandlerMap::new());
        let handler_map = cmd_handler_map.clone();
        let write_factory = CmdWriteFactory::<R, W, _, LEN, CMD, LISTENER>::new(factory, listener, move |peer_id: PeerId, tunnel_id: TunnelId, header: CmdHeader<LEN, CMD>, body_read| {
            let handler_map = handler_map.clone();
            async move {
                if let Some(handler) = handler_map.get(header.cmd_code()) {
                    handler.handle(peer_id, tunnel_id, header, body_read).await?;
                }
                Ok(())
            }
        });
        write_factory.start();
        Arc::new(Self {
            tunnel_pool: ClassifiedWorkerPool::new(tunnel_count, write_factory),
            cmd_handler_map,
        })
    }

    async fn get_send(&self, peer_id: PeerId) -> CmdResult<ClassifiedWorkerGuard<(PeerId, Option<TunnelId>), CmdSend<R, W, LEN, CMD>, CmdWriteFactory<R, W, F, LEN, CMD, LISTENER>>> {
        self.tunnel_pool.get_classified_worker((peer_id, None)).await.map_err(into_cmd_err!(CmdErrorCode::Failed, "get worker failed"))
    }

    async fn get_send_of_tunnel_id(&self, peer_id: PeerId, tunnel_id: TunnelId) -> CmdResult<ClassifiedWorkerGuard<(PeerId, Option<TunnelId>), CmdSend<R, W, LEN, CMD>, CmdWriteFactory<R, W, F, LEN, CMD, LISTENER>>> {
        self.tunnel_pool.get_classified_worker((peer_id, Some(tunnel_id))).await.map_err(into_cmd_err!(CmdErrorCode::Failed, "get worker failed"))
    }

}

#[async_trait::async_trait]
impl<R: CmdTunnelRead,
    W: CmdTunnelWrite,
    F: CmdNodeTunnelFactory<R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send + 'static + Eq + Hash + Debug,
    LISTENER: CmdTunnelListener<R, W>> CmdNode<LEN, CMD> for DefaultCmdNode<R, W, F, LEN, CMD, LISTENER> {
    fn register_cmd_handler(&self, cmd: CMD, handler: impl CmdHandler<LEN, CMD>) {
        self.cmd_handler_map.insert(cmd, handler);
    }

    async fn send(&self, peer_id: &PeerId, cmd: CMD, version: u8, body: &[u8]) -> CmdResult<()> {
        let mut send = self.get_send(peer_id.clone()).await?;
        send.send(cmd, version, body).await
    }

    async fn send2(&self, peer_id: &PeerId, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()> {
        let mut send = self.get_send(peer_id.clone()).await?;
        send.send2(cmd, version, body).await
    }

    async fn send_by_specify_tunnel(&self, peer_id: &PeerId, tunnel_id: TunnelId, cmd: CMD, version: u8, body: &[u8]) -> CmdResult<()> {
        let mut send = self.get_send_of_tunnel_id(peer_id.clone(), tunnel_id).await?;
        send.send(cmd, version, body).await
    }

    async fn send2_by_specify_tunnel(&self, peer_id: &PeerId, tunnel_id: TunnelId, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()> {
        let mut send = self.get_send_of_tunnel_id(peer_id.clone(), tunnel_id).await?;
        send.send2(cmd, version, body).await
    }

    async fn clear_all_tunnel(&self) {
        self.tunnel_pool.clear_all_worker().await
    }
}
