use std::fmt::Debug;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use bucky_raw_codec::{RawConvertTo, RawDecode, RawEncode, RawFixedBytes, RawFrom};
use num::{FromPrimitive, ToPrimitive};
use sfo_pool::{into_pool_err, pool_err, ClassifiedWorker, ClassifiedWorkerFactory, ClassifiedWorkerGuard, ClassifiedWorkerPool, ClassifiedWorkerPoolRef, PoolErrorCode, PoolResult, WorkerClassification};
use sfo_split::{Splittable, WHalf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::spawn;
use tokio::task::JoinHandle;
use crate::{CmdTunnelRead, CmdTunnelWrite, TunnelId, TunnelIdGenerator};
use crate::client::{CmdClient, SendGuard};
use crate::cmd::{CmdBodyReadImpl, CmdHandler, CmdHandlerMap, CmdHeader};
use crate::errors::{into_cmd_err, CmdErrorCode, CmdResult};
use crate::peer_id::PeerId;

#[async_trait::async_trait]
pub trait CmdTunnelFactory<R: CmdTunnelRead, W: CmdTunnelWrite>: Send + Sync + 'static {
    async fn create_tunnel(&self) -> CmdResult<Splittable<R, W>>;
}

pub struct CmdSend<R: CmdTunnelRead, W: CmdTunnelWrite, LEN, CMD>
where LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
      CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug {
    pub(crate) recv_handle: JoinHandle<CmdResult<()>>,
    pub(crate) write: WHalf<R, W>,
    pub(crate) is_work: bool,
    pub(crate) tunnel_id: TunnelId,
    _p: std::marker::PhantomData<(LEN, CMD)>,

}

impl<R, W, LEN, CMD> Deref for CmdSend<R, W, LEN, CMD>
where R: CmdTunnelRead,
      W: CmdTunnelWrite,
      LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
      CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug {
    type Target = W;

    fn deref(&self) -> &Self::Target {
        self.write.deref()
    }
}

impl<R, W, LEN, CMD> CmdSend<R, W, LEN, CMD>
where R: CmdTunnelRead,
      W: CmdTunnelWrite,
      LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
      CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug {
    pub fn new(tunnel_id: TunnelId, recv_handle: JoinHandle<CmdResult<()>>, write: WHalf<R, W>) -> Self {
        Self {
            recv_handle,
            write,
            is_work: true,
            tunnel_id,
            _p: Default::default(),
        }
    }

    pub fn get_tunnel_id(&self) -> TunnelId {
        self.tunnel_id
    }

    pub fn set_disable(&mut self) {
        self.is_work = false;
        self.recv_handle.abort();
    }

    pub async fn send(&mut self, cmd: CMD, version: u8, body: &[u8]) -> CmdResult<()> {
        log::trace!("client {:?} send cmd: {:?}, len: {} data:{}", self.tunnel_id, cmd, body.len(), hex::encode(body));
        let header = CmdHeader::<LEN, CMD>::new(version, cmd, LEN::from_u64(body.len() as u64).unwrap());
        let buf = header.to_vec().map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
        let ret = self.send_inner(buf.as_slice(), body).await;
        if let Err(e) = ret {
            self.set_disable();
            return Err(e);
        }
        Ok(())
    }

    pub async fn send2(&mut self, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()> {
        let mut len = 0;
        for b in body.iter() {
            len += b.len();
            log::trace!("client {:?} send2 cmd: {:?}, data {}", self.tunnel_id, cmd, hex::encode(b));
        }
        log::trace!("client {:?} send2 cmd: {:?}, len {}", self.tunnel_id, cmd, len);
        let header = CmdHeader::<LEN, CMD>::new(version, cmd, LEN::from_u64(len as u64).unwrap());
        let buf = header.to_vec().map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
        let ret = self.send_inner2(buf.as_slice(), body).await;
        if let Err(e) = ret {
            self.set_disable();
            return Err(e);
        }
        Ok(())
    }

    async fn send_inner(&mut self, header: &[u8], body: &[u8]) -> CmdResult<()> {
        self.write.write_all(header).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        self.write.write_all(body).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        self.write.flush().await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        Ok(())
    }

    async fn send_inner2(&mut self, header: &[u8], body: &[&[u8]]) -> CmdResult<()> {
        self.write.write_all(header).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        for b in body.iter() {
            self.write.write_all(b).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        }
        self.write.flush().await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        Ok(())
    }
}

impl<R, W, LEN, CMD> Drop for CmdSend<R, W, LEN, CMD>
where R: CmdTunnelRead,
      W: CmdTunnelWrite,
      LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
      CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug {
    fn drop(&mut self) {
        self.set_disable();
    }
}

impl<R, W, LEN, CMD> ClassifiedWorker<TunnelId> for CmdSend<R, W, LEN, CMD>
where R: CmdTunnelRead,
      W: CmdTunnelWrite,
      LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
      CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug{
    fn is_work(&self) -> bool {
        self.is_work && !self.recv_handle.is_finished()
    }

    fn is_valid(&self, c: TunnelId) -> bool {
        self.tunnel_id == c
    }

    fn classification(&self) -> TunnelId {
        self.tunnel_id
    }
}

pub struct ClassifiedSendGuard<
    C: WorkerClassification,
    W: CmdTunnelWrite,
    CW: ClassifiedWorker<C> + Deref<Target=W>,
    F: ClassifiedWorkerFactory<C, CW>> {
    pub(crate) worker_guard: ClassifiedWorkerGuard<C, CW, F>
}

impl<
    C: WorkerClassification,
    W: CmdTunnelWrite,
    CW: ClassifiedWorker<C> + Deref<Target=W>,
    F: ClassifiedWorkerFactory<C, CW>> Deref for ClassifiedSendGuard<C, W, CW, F> {
    type Target = W;

    fn deref(&self) -> &Self::Target {
        &self.worker_guard.deref()
    }
}

impl<
    C: WorkerClassification,
    W: CmdTunnelWrite,
    CW: ClassifiedWorker<C> + Deref<Target=W>,
    F: ClassifiedWorkerFactory<C, CW>> SendGuard<W> for ClassifiedSendGuard<C, W, CW, F> {

}
pub struct CmdWriteFactory<R: CmdTunnelRead,
    W: CmdTunnelWrite,
    F: CmdTunnelFactory<R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug> {
    tunnel_factory: F,
    cmd_handler: Arc<dyn CmdHandler<LEN, CMD>>,
    tunnel_id_generator: TunnelIdGenerator,
    p: std::marker::PhantomData<Mutex<(R, W)>>,
}

impl<R: CmdTunnelRead,
    W: CmdTunnelWrite,
    F: CmdTunnelFactory<R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug> CmdWriteFactory<R, W, F, LEN, CMD> {
    pub(crate) fn new(tunnel_factory: F, cmd_handler: impl CmdHandler<LEN, CMD>) -> Self {
        Self {
            tunnel_factory,
            cmd_handler: Arc::new(cmd_handler),
            tunnel_id_generator: TunnelIdGenerator::new(),
            p: Default::default(),
        }
    }
}

#[async_trait::async_trait]
impl<R: CmdTunnelRead,
    W: CmdTunnelWrite,
    F: CmdTunnelFactory<R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive + RawFixedBytes,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + RawFixedBytes + Debug> ClassifiedWorkerFactory<TunnelId, CmdSend<R, W, LEN, CMD>> for CmdWriteFactory<R, W, F, LEN, CMD> {
    async fn create(&self, c: Option<TunnelId>) -> PoolResult<CmdSend<R, W, LEN, CMD>> {
        if c.is_some() {
            return Err(pool_err!(PoolErrorCode::Failed, "tunnel {:?} not found", c.unwrap()));
        }
        let tunnel = self.tunnel_factory.create_tunnel().await.map_err(into_pool_err!(PoolErrorCode::Failed))?;
        let peer_id = tunnel.get_remote_peer_id();
        let tunnel_id = self.tunnel_id_generator.generate();
        let (mut recv, write) = tunnel.split();
        let cmd_handler = self.cmd_handler.clone();
        let handle = spawn(async move {
            let ret: CmdResult<()> = async move {
                loop {
                    let mut header = vec![0u8; CmdHeader::<LEN, CMD>::raw_bytes().unwrap()];
                    let n = recv.read_exact(header.as_mut()).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                    if n == 0 {
                        break;
                    }
                    let header = CmdHeader::<LEN, CMD>::clone_from_slice(header.as_slice()).map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
                    log::trace!("recv cmd {:?} from {} len {}", header.cmd_code(), peer_id.to_base58(), header.pkg_len().to_u64().unwrap());
                    let cmd_read = Box::new(CmdBodyReadImpl::new(recv, header.pkg_len().to_u64().unwrap() as usize));
                    let waiter = cmd_read.get_waiter();
                    let future = waiter.create_result_future();
                    if let Err(e) = cmd_handler.handle(peer_id.clone(), tunnel_id, header, cmd_read).await {
                        log::error!("handle cmd error: {:?}", e);
                    }
                    recv = future.await.map_err(into_cmd_err!(CmdErrorCode::Failed))??;
                }
                Ok(())
            }.await;
            ret
        });
        Ok(CmdSend::new(tunnel_id, handle, write))
    }
}

pub struct DefaultCmdClient<R: CmdTunnelRead,
    W: CmdTunnelWrite,
    F: CmdTunnelFactory<R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive + RawFixedBytes,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + RawFixedBytes + Eq + Hash + Debug> {
    tunnel_pool: ClassifiedWorkerPoolRef<TunnelId, CmdSend<R, W, LEN, CMD>, CmdWriteFactory<R, W, F, LEN, CMD>>,
    cmd_handler_map: Arc<CmdHandlerMap<LEN, CMD>>,
}

impl<R: CmdTunnelRead,
    W: CmdTunnelWrite,
    F: CmdTunnelFactory<R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive + RawFixedBytes,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + RawFixedBytes + Eq + Hash + Debug> DefaultCmdClient<R, W, F, LEN, CMD> {
    pub fn new(factory: F, tunnel_count: u16) -> Arc<Self> {
        let cmd_handler_map = Arc::new(CmdHandlerMap::new());
        let handler_map = cmd_handler_map.clone();
        Arc::new(Self {
            tunnel_pool: ClassifiedWorkerPool::new(tunnel_count, CmdWriteFactory::<R, W, _, LEN, CMD>::new(factory, move |peer_id: PeerId, tunnel_id: TunnelId, header: CmdHeader<LEN, CMD>, body_read| {
                let handler_map = handler_map.clone();
                async move {
                    if let Some(handler) = handler_map.get(header.cmd_code()) {
                        handler.handle(peer_id, tunnel_id, header, body_read).await?;
                    }
                    Ok(())
                }
            })),
            cmd_handler_map,
        })
    }

    async fn get_send(&self) -> CmdResult<ClassifiedWorkerGuard<TunnelId, CmdSend<R, W, LEN, CMD>, CmdWriteFactory<R, W, F, LEN, CMD>>> {
        self.tunnel_pool.get_worker().await.map_err(into_cmd_err!(CmdErrorCode::Failed, "get worker failed"))
    }

    async fn get_send_of_tunnel_id(&self, tunnel_id: TunnelId) -> CmdResult<ClassifiedWorkerGuard<TunnelId, CmdSend<R, W, LEN, CMD>, CmdWriteFactory<R, W, F, LEN, CMD>>> {
        self.tunnel_pool.get_classified_worker(tunnel_id).await.map_err(into_cmd_err!(CmdErrorCode::Failed, "get worker failed"))
    }
}

pub type CmdClientSendGuard<R, W, F, LEN, CMD> = ClassifiedSendGuard<TunnelId, W, CmdSend<R, W, LEN, CMD>, CmdWriteFactory<R, W, F, LEN, CMD>>;
#[async_trait::async_trait]
impl<R: CmdTunnelRead,
    W: CmdTunnelWrite,
    F: CmdTunnelFactory<R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive + RawFixedBytes,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + RawFixedBytes + Eq + Hash + Debug,
    > CmdClient<LEN, CMD, W, CmdClientSendGuard<R, W, F, LEN, CMD>, > for DefaultCmdClient<R, W, F, LEN, CMD> {
    fn register_cmd_handler(&self, cmd: CMD, handler: impl CmdHandler<LEN, CMD>) {
        self.cmd_handler_map.insert(cmd, handler);
    }

    async fn send(&self, cmd: CMD, version: u8, body: &[u8]) -> CmdResult<()> {
        let mut send = self.get_send().await?;
        send.send(cmd, version, body).await
    }

    async fn send2(&self, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()> {
        let mut send = self.get_send().await?;
        send.send2(cmd, version, body).await
    }

    async fn send_by_specify_tunnel(&self, tunnel_id: TunnelId, cmd: CMD, version: u8, body: &[u8]) -> CmdResult<()> {
        let mut send = self.get_send_of_tunnel_id(tunnel_id).await?;
        send.send(cmd, version, body).await
    }

    async fn send2_by_specify_tunnel(&self, tunnel_id: TunnelId, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()> {
        let mut send = self.get_send_of_tunnel_id(tunnel_id).await?;
        send.send2(cmd, version, body).await
    }

    async fn clear_all_tunnel(&self) {
        self.tunnel_pool.clear_all_worker().await;
    }

    async fn get_send(&self, tunnel_id: TunnelId) -> CmdResult<CmdClientSendGuard<R, W, F, LEN, CMD>> {
        Ok(ClassifiedSendGuard {
            worker_guard: self.get_send_of_tunnel_id(tunnel_id).await?
        })
    }
}
