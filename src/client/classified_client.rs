use std::hash::Hash;
use std::sync::{Arc, Mutex};
use bucky_raw_codec::{RawConvertTo, RawDecode, RawEncode, RawFixedBytes, RawFrom};
use num::{FromPrimitive, ToPrimitive};
use sfo_pool::{into_pool_err, pool_err, ClassifiedWorker, ClassifiedWorkerFactory, ClassifiedWorkerGuard, ClassifiedWorkerPool, ClassifiedWorkerPoolRef, PoolErrorCode, PoolResult, WorkerClassification};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::spawn;
use tokio::task::JoinHandle;
use crate::{CmdTunnelRead, CmdTunnelWrite, TunnelId, TunnelIdGenerator};
use crate::client::{ClassifiedCmdClient, ClassifiedSendGuard, CmdClient};
use crate::cmd::{CmdBodyReadImpl, CmdHandler, CmdHandlerMap, CmdHeader};
use crate::errors::{into_cmd_err, CmdErrorCode, CmdResult};
use crate::peer_id::PeerId;
use std::fmt::Debug;
use std::ops::Deref;
use sfo_split::{RHalf, Splittable, WHalf};

pub trait ClassifiedCmdTunnelRead<C: WorkerClassification>: CmdTunnelRead + 'static + Send {
    fn get_classification(&self) -> C;
}

pub trait ClassifiedCmdTunnelWrite<C: WorkerClassification>: CmdTunnelWrite + 'static + Send {
    fn get_classification(&self) -> C;
}

pub type ClassifiedCmdTunnel<R, W> = Splittable<R, W>;
pub type ClassifiedCmdTunnelRHalf<R, W> = RHalf<R, W>;
pub type ClassifiedCmdTunnelWHalf<R, W> = WHalf<R, W>;

#[derive(Debug, Clone, Copy, Eq, Hash)]
pub struct CmdClientTunnelClassification<C: WorkerClassification> {
    tunnel_id: Option<TunnelId>,
    classification: Option<C>,
}

impl<C: WorkerClassification> PartialEq for CmdClientTunnelClassification<C> {
    fn eq(&self, other: &Self) -> bool {
        self.tunnel_id == other.tunnel_id && self.classification == other.classification
    }
}


#[async_trait::async_trait]
pub trait ClassifiedCmdTunnelFactory<C: WorkerClassification, R: ClassifiedCmdTunnelRead<C>, W: ClassifiedCmdTunnelWrite<C>>: Send + Sync + 'static {
    async fn create_tunnel(&self, classification: Option<C>) -> CmdResult<Splittable<R, W>>;
}

pub struct ClassifiedCmdSend<C, R, W, LEN, CMD>
where
    C: WorkerClassification,
    R: ClassifiedCmdTunnelRead<C>,
    W: ClassifiedCmdTunnelWrite<C>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug,
{
    pub(crate) recv_handle: JoinHandle<CmdResult<()>>,
    pub(crate) write: ClassifiedCmdTunnelWHalf<R, W>,
    pub(crate) is_work: bool,
    pub(crate) classification: C,
    pub(crate) tunnel_id: TunnelId,
    _p: std::marker::PhantomData<(LEN, CMD)>,

}

impl<C, R, W, LEN, CMD> Deref for ClassifiedCmdSend<C, R, W, LEN, CMD>
where C: WorkerClassification,
      R: ClassifiedCmdTunnelRead<C>,
      W: ClassifiedCmdTunnelWrite<C>,
      LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
      CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug {
    type Target = W;

    fn deref(&self) -> &Self::Target {
        self.write.deref()
    }
}

impl<C, R, W, LEN, CMD> ClassifiedCmdSend<C, R, W, LEN, CMD>
where C: WorkerClassification,
      R: ClassifiedCmdTunnelRead<C>,
      W: ClassifiedCmdTunnelWrite<C>,
      LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
      CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug {
    pub(crate) fn new(tunnel_id: TunnelId, classification: C, recv_handle: JoinHandle<CmdResult<()>>, write: ClassifiedCmdTunnelWHalf<R, W>) -> Self {
        Self {
            recv_handle,
            write,
            is_work: true,
            classification,
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
        log::trace!("client {:?} send cmd: {:?}, len: {}, data: {}", self.tunnel_id, cmd, body.len(), hex::encode(body));
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
            log::trace!("client {:?} send2 cmd {:?} body: {}", self.tunnel_id, cmd, hex::encode(b));
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

impl<C, R, W, LEN, CMD> Drop for ClassifiedCmdSend<C, R, W, LEN, CMD>
where C: WorkerClassification,
      R: ClassifiedCmdTunnelRead<C>,
      W: ClassifiedCmdTunnelWrite<C>,
      LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
      CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug {
    fn drop(&mut self) {
        self.set_disable();
    }
}

impl<C, R, W, LEN, CMD> ClassifiedWorker<CmdClientTunnelClassification<C>> for ClassifiedCmdSend<C, R, W, LEN, CMD>
where C: WorkerClassification,
      R: ClassifiedCmdTunnelRead<C>,
      W: ClassifiedCmdTunnelWrite<C>,
      LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
      CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug {
    fn is_work(&self) -> bool {
        self.is_work && !self.recv_handle.is_finished()
    }

    fn is_valid(&self, c: CmdClientTunnelClassification<C>) -> bool {
        if c.tunnel_id.is_some() {
            self.tunnel_id == c.tunnel_id.unwrap()
        } else {
            if c.classification.is_some() {
                self.classification == c.classification.unwrap()
            } else {
                true
            }
        }
    }

    fn classification(&self) -> CmdClientTunnelClassification<C> {
        CmdClientTunnelClassification {
            tunnel_id: Some(self.tunnel_id),
            classification: Some(self.classification.clone()),
        }
    }
}

pub struct ClassifiedCmdWriteFactory<C: WorkerClassification,
    R: ClassifiedCmdTunnelRead<C>,
    W: ClassifiedCmdTunnelWrite<C>,
    F: ClassifiedCmdTunnelFactory<C, R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug> {
    tunnel_factory: F,
    cmd_handler: Arc<dyn CmdHandler<LEN, CMD>>,
    tunnel_id_generator: TunnelIdGenerator,
    _p: std::marker::PhantomData<Mutex<(C, R, W)>>,
}

impl<
    C: WorkerClassification,
    R: ClassifiedCmdTunnelRead<C>,
    W: ClassifiedCmdTunnelWrite<C>,
    F: ClassifiedCmdTunnelFactory<C, R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug
> ClassifiedCmdWriteFactory<C, R, W, F, LEN, CMD> {
    pub(crate) fn new(tunnel_factory: F, cmd_handler: impl CmdHandler<LEN, CMD>) -> Self {
        Self {
            tunnel_factory,
            cmd_handler: Arc::new(cmd_handler),
            tunnel_id_generator: TunnelIdGenerator::new(),
            _p: Default::default(),
        }
    }
}

#[async_trait::async_trait]
impl<C: WorkerClassification,
    R: ClassifiedCmdTunnelRead<C>,
    W: ClassifiedCmdTunnelWrite<C>,
    F: ClassifiedCmdTunnelFactory<C, R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive + RawFixedBytes,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + RawFixedBytes + Debug> ClassifiedWorkerFactory<CmdClientTunnelClassification<C>, ClassifiedCmdSend<C, R, W, LEN, CMD>
> for ClassifiedCmdWriteFactory<C, R, W, F, LEN, CMD> {
    async fn create(&self, classification: Option<CmdClientTunnelClassification<C>>) -> PoolResult<ClassifiedCmdSend<C, R, W, LEN, CMD>> {
        if classification.is_some() && classification.as_ref().unwrap().tunnel_id.is_some() {
            return Err(pool_err!(PoolErrorCode::Failed, "tunnel {:?} not found", classification.as_ref().unwrap().tunnel_id.unwrap()));
        }

        let classification = if classification.is_some() && classification.as_ref().unwrap().classification.is_some() {
            classification.unwrap().classification
        } else {
            None
        };
        let tunnel = self.tunnel_factory.create_tunnel(classification).await.map_err(into_pool_err!(PoolErrorCode::Failed))?;
        let classification = tunnel.get_classification();
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
                    log::trace!("recv cmd {:?} from {} len {} tunnel {:?}", header.cmd_code(), peer_id, header.pkg_len().to_u64().unwrap(), tunnel_id);
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
        Ok(ClassifiedCmdSend::new(tunnel_id, classification, handle, write))
    }
}

pub struct DefaultClassifiedCmdClient<C: WorkerClassification,
    R: ClassifiedCmdTunnelRead<C>,
    W: ClassifiedCmdTunnelWrite<C>,
    F: ClassifiedCmdTunnelFactory<C, R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive + RawFixedBytes,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + RawFixedBytes + Eq + Hash + Debug> {
    tunnel_pool: ClassifiedWorkerPoolRef<CmdClientTunnelClassification<C>, ClassifiedCmdSend<C, R, W, LEN, CMD>, ClassifiedCmdWriteFactory<C, R, W, F, LEN, CMD>>,
    cmd_handler_map: Arc<CmdHandlerMap<LEN, CMD>>,
}

impl<C: WorkerClassification,
    R: ClassifiedCmdTunnelRead<C>,
    W: ClassifiedCmdTunnelWrite<C>,
    F: ClassifiedCmdTunnelFactory<C, R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive + RawFixedBytes,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + RawFixedBytes + Eq + Hash + Debug> DefaultClassifiedCmdClient<C, R, W, F, LEN, CMD> {
    pub fn new(factory: F, tunnel_count: u16) -> Arc<Self> {
        let cmd_handler_map = Arc::new(CmdHandlerMap::new());
        let handler_map = cmd_handler_map.clone();
        Arc::new(Self {
            tunnel_pool: ClassifiedWorkerPool::new(tunnel_count, ClassifiedCmdWriteFactory::<C, R, W, _, LEN, CMD>::new(factory, move |peer_id: PeerId, tunnel_id, header: CmdHeader<LEN, CMD>, body_read| {
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

    async fn get_send(&self) -> CmdResult<ClassifiedWorkerGuard<CmdClientTunnelClassification<C>, ClassifiedCmdSend<C, R, W, LEN, CMD>, ClassifiedCmdWriteFactory<C, R, W, F, LEN, CMD>>> {
        self.tunnel_pool.get_worker().await.map_err(into_cmd_err!(CmdErrorCode::Failed, "get worker failed"))
    }

    async fn get_send_of_tunnel_id(&self, tunnel_id: TunnelId) -> CmdResult<ClassifiedWorkerGuard<CmdClientTunnelClassification<C>, ClassifiedCmdSend<C, R, W, LEN, CMD>, ClassifiedCmdWriteFactory<C, R, W, F, LEN, CMD>>> {
        self.tunnel_pool.get_classified_worker(CmdClientTunnelClassification {
            tunnel_id: Some(tunnel_id),
            classification: None,
        }).await.map_err(into_cmd_err!(CmdErrorCode::Failed, "get worker failed"))
    }

    async fn get_classified_send(&self, classification: C) -> CmdResult<ClassifiedWorkerGuard<CmdClientTunnelClassification<C>, ClassifiedCmdSend<C, R, W, LEN, CMD>, ClassifiedCmdWriteFactory<C, R, W, F, LEN, CMD>>> {
        self.tunnel_pool.get_classified_worker(CmdClientTunnelClassification {
            tunnel_id: None,
            classification: Some(classification),
        }).await.map_err(into_cmd_err!(CmdErrorCode::Failed, "get worker failed"))
    }
}

pub type ClassifiedClientSendGuard<C, R, W, F, LEN, CMD> = ClassifiedSendGuard<CmdClientTunnelClassification<C>, W, ClassifiedCmdSend<C, R, W, LEN, CMD>, ClassifiedCmdWriteFactory<C, R, W, F, LEN, CMD>>;
#[async_trait::async_trait]
impl<C: WorkerClassification,
    R: ClassifiedCmdTunnelRead<C>,
    W: ClassifiedCmdTunnelWrite<C>,
    F: ClassifiedCmdTunnelFactory<C, R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive + RawFixedBytes,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + RawFixedBytes + Eq + Hash + Debug,
> CmdClient<LEN, CMD, W, ClassifiedClientSendGuard<C, R, W, F, LEN, CMD>> for DefaultClassifiedCmdClient<C, R, W, F, LEN, CMD> {
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

    async fn get_send(&self, tunnel_id: TunnelId) -> CmdResult<ClassifiedClientSendGuard<C, R, W, F, LEN, CMD>> {
        Ok(ClassifiedSendGuard {
            worker_guard: self.get_send_of_tunnel_id(tunnel_id).await?,
        })
    }
}

#[async_trait::async_trait]
impl<C: WorkerClassification,
    R: ClassifiedCmdTunnelRead<C>,
    W: ClassifiedCmdTunnelWrite<C>,
    F: ClassifiedCmdTunnelFactory<C, R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive + RawFixedBytes,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + RawFixedBytes + Eq + Hash + Debug
> ClassifiedCmdClient<LEN, CMD, C, W, ClassifiedClientSendGuard<C, R, W, F, LEN, CMD>> for DefaultClassifiedCmdClient<C, R, W, F, LEN, CMD> {
    async fn send_by_classified_tunnel(&self, classification: C, cmd: CMD, version: u8, body: &[u8]) -> CmdResult<()> {
        let mut send = self.get_classified_send(classification).await?;
        send.send(cmd, version, body).await
    }

    async fn send2_by_classified_tunnel(&self, classification: C, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()> {
        let mut send = self.get_classified_send(classification).await?;
        send.send2(cmd, version, body).await
    }

    async fn find_tunnel_id_by_classified(&self, classification: C) -> CmdResult<TunnelId> {
        let send = self.get_classified_send(classification).await?;
        Ok(send.get_tunnel_id())
    }

    async fn get_send_by_classified(&self, classification: C) -> CmdResult<ClassifiedSendGuard<CmdClientTunnelClassification<C>, W, ClassifiedCmdSend<C, R, W, LEN, CMD>, ClassifiedCmdWriteFactory<C, R, W, F, LEN, CMD>>> {
        Ok(ClassifiedSendGuard {
            worker_guard: self.get_classified_send(classification).await?,
        })
    }
}
