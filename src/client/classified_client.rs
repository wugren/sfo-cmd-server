use std::hash::Hash;
use std::sync::{Arc};
use bucky_raw_codec::{RawConvertTo, RawDecode, RawEncode, RawFixedBytes, RawFrom};
use num::{FromPrimitive, ToPrimitive};
use sfo_pool::{into_pool_err, pool_err, ClassifiedWorker, ClassifiedWorkerFactory, ClassifiedWorkerGuard, ClassifiedWorkerPool, ClassifiedWorkerPoolRef, PoolErrorCode, PoolResult, Worker, WorkerClassification};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::spawn;
use tokio::task::JoinHandle;
use crate::{CmdTunnel, CmdTunnelWrite, TunnelId, TunnelIdGenerator};
use crate::client::{ClassifiedCmdClient, CmdClient};
use crate::cmd::{CmdHandler, CmdHandlerMap, CmdHeader};
use crate::errors::{into_cmd_err, CmdErrorCode, CmdResult};
use crate::peer_id::PeerId;

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

pub trait ClassifiedCmdTunnel<C: WorkerClassification>: CmdTunnel {
    fn get_classification(&self) -> C;
}

#[async_trait::async_trait]
pub trait ClassifiedCmdTunnelFactory<C: WorkerClassification,T: ClassifiedCmdTunnel<C>>: Send + Sync + 'static {
    async fn create_tunnel(&self, classification: Option<C>) -> CmdResult<Arc<T>>;
}

pub struct ClassifiedCmdSend<T, C, LEN, CMD>
where T: CmdTunnel,
      C: WorkerClassification,
      LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
      CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static {
    recv_handle: JoinHandle<CmdResult<()>>,
    write: Box<dyn CmdTunnelWrite>,
    is_work: bool,
    classification: C,
    tunnel_id: TunnelId,
    tunnel: Arc<T>,
    _p: std::marker::PhantomData<(LEN, CMD)>,

}

impl<T, C, LEN, CMD> ClassifiedCmdSend<T, C, LEN, CMD>
where T: CmdTunnel,
      C: WorkerClassification,
      LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
      CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static {
    pub(crate) fn new(tunnel_id: TunnelId, tunnel: Arc<T>, classification: C, recv_handle: JoinHandle<CmdResult<()>>, write: Box<dyn CmdTunnelWrite>) -> Self {
        Self {
            recv_handle,
            write,
            is_work: true,
            classification,
            tunnel_id,
            tunnel,
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

    pub async fn send(&mut self, cmd: CMD, body: &[u8]) -> CmdResult<()> {
        let header = CmdHeader::<LEN, CMD>::new(cmd, LEN::from_u64(body.len() as u64).unwrap());
        let buf = header.to_vec().map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
        let ret = self.send_inner(buf.as_slice(), body).await;
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
}

impl<T, C, LEN, CMD> Drop for ClassifiedCmdSend<T, C, LEN, CMD>
where T: CmdTunnel,
      C: WorkerClassification,
      LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
      CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static {
    fn drop(&mut self) {
        self.set_disable();
    }
}

impl<T, C, LEN, CMD> ClassifiedWorker<CmdClientTunnelClassification<C>> for ClassifiedCmdSend<T, C, LEN, CMD>
where T: CmdTunnel,
      C: WorkerClassification,
      LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
      CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static{
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

pub struct CmdWriteFactory<C: WorkerClassification,
    T: ClassifiedCmdTunnel<C>,
    F: ClassifiedCmdTunnelFactory<C, T>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static> {
    tunnel_factory: F,
    cmd_handler: Arc<dyn CmdHandler<LEN, CMD>>,
    tunnel_id_generator: TunnelIdGenerator,
    _p: std::marker::PhantomData<(C, T)>,
}

impl<C: WorkerClassification,
    T: ClassifiedCmdTunnel<C>,
    F: ClassifiedCmdTunnelFactory<C, T>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static> CmdWriteFactory<C, T, F, LEN, CMD> {
    pub fn new(tunnel_factory: F, cmd_handler: impl CmdHandler<LEN, CMD>) -> Self {
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
    T: ClassifiedCmdTunnel<C>,
    F: ClassifiedCmdTunnelFactory<C, T>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive + RawFixedBytes,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + RawFixedBytes> ClassifiedWorkerFactory<CmdClientTunnelClassification<C>, ClassifiedCmdSend<T, C, LEN, CMD>> for CmdWriteFactory<C, T, F, LEN, CMD> {
    async fn create(&self, classification: Option<CmdClientTunnelClassification<C>>) -> PoolResult<ClassifiedCmdSend<T, C, LEN, CMD>> {
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
        let (mut recv, write) = tunnel.split().map_err(into_pool_err!(PoolErrorCode::Failed))?;
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
                    let mut buf = vec![0u8; header.pkg_len().to_u64().unwrap() as usize];
                    if buf.len() > 0 {
                        let n = recv.read_exact(&mut buf).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                        if n == 0 {
                            break;
                        }
                    }
                    if let Err(e) = cmd_handler.handle(peer_id.clone(), tunnel_id, header, buf).await {
                        log::error!("handle cmd error: {:?}", e);
                    }
                }
                Ok(())
            }.await;
            ret
        });
        Ok(ClassifiedCmdSend::new(tunnel_id, tunnel, classification, handle, write))
    }
}

pub struct DefaultClassifiedCmdClient<C: WorkerClassification,
    T: ClassifiedCmdTunnel<C>,
    F: ClassifiedCmdTunnelFactory<C, T>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive + RawFixedBytes,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + RawFixedBytes + Eq + Hash> {
    tunnel_pool: ClassifiedWorkerPoolRef<CmdClientTunnelClassification<C>, ClassifiedCmdSend<T, C, LEN, CMD>, CmdWriteFactory<C, T, F, LEN, CMD>>,
    cmd_handler_map: Arc<CmdHandlerMap<LEN, CMD>>,
}

impl<C: WorkerClassification,
    T: ClassifiedCmdTunnel<C>,
    F: ClassifiedCmdTunnelFactory<C, T>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive + RawFixedBytes,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + RawFixedBytes + Eq + Hash> DefaultClassifiedCmdClient<C, T, F, LEN, CMD> {
    pub fn new(factory: F, tunnel_count: u16) -> Arc<Self> {
        let cmd_handler_map = Arc::new(CmdHandlerMap::new());
        let handler_map = cmd_handler_map.clone();
        Arc::new(Self {
            tunnel_pool: ClassifiedWorkerPool::new(tunnel_count, CmdWriteFactory::<C, _, _, LEN, CMD>::new(factory, move |peer_id: PeerId, tunnel_id, header: CmdHeader<LEN, CMD>, buf| {
                let handler_map = handler_map.clone();
                async move {
                    if let Some(handler) = handler_map.get(header.cmd_code()) {
                        handler.handle(peer_id, tunnel_id, header, buf).await?;
                    }
                    Ok(())
                }
            })),
            cmd_handler_map,
        })
    }

    async fn get_send(&self) -> CmdResult<ClassifiedWorkerGuard<CmdClientTunnelClassification<C>, ClassifiedCmdSend<T, C, LEN, CMD>, CmdWriteFactory<C, T, F, LEN, CMD>>> {
        self.tunnel_pool.get_worker().await.map_err(into_cmd_err!(CmdErrorCode::Failed, "get worker failed"))
    }

    async fn get_send_of_tunnel_id(&self, tunnel_id: TunnelId) -> CmdResult<ClassifiedWorkerGuard<CmdClientTunnelClassification<C>, ClassifiedCmdSend<T, C, LEN, CMD>, CmdWriteFactory<C, T, F, LEN, CMD>>> {
        self.tunnel_pool.get_classified_worker(CmdClientTunnelClassification {
            tunnel_id: Some(tunnel_id),
            classification: None,
        }).await.map_err(into_cmd_err!(CmdErrorCode::Failed, "get worker failed"))
    }

    async fn get_classified_send(&self, classification: C) -> CmdResult<ClassifiedWorkerGuard<CmdClientTunnelClassification<C>, ClassifiedCmdSend<T, C, LEN, CMD>, CmdWriteFactory<C, T, F, LEN, CMD>>> {
        self.tunnel_pool.get_classified_worker(CmdClientTunnelClassification {
            tunnel_id: None,
            classification: Some(classification),
        }).await.map_err(into_cmd_err!(CmdErrorCode::Failed, "get worker failed"))
    }
}

#[async_trait::async_trait]
impl<C: WorkerClassification,
    T: ClassifiedCmdTunnel<C>,
    F: ClassifiedCmdTunnelFactory<C, T>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive + RawFixedBytes,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + RawFixedBytes + Eq + Hash> CmdClient<LEN, CMD> for DefaultClassifiedCmdClient<C, T, F, LEN, CMD> {
    fn register_cmd_handler(&self, cmd: CMD, handler: impl CmdHandler<LEN, CMD>) {
        self.cmd_handler_map.insert(cmd, handler);
    }

    async fn send(&self, cmd: CMD, body: &[u8]) -> CmdResult<()> {
        let mut send = self.get_send().await?;
        send.send(cmd, body).await
    }

    async fn send_by_specify_tunnel(&self, tunnel_id: TunnelId, cmd: CMD, body: &[u8]) -> CmdResult<()> {
        let mut send = self.get_send_of_tunnel_id(tunnel_id).await?;
        send.send(cmd, body).await
    }
}

#[async_trait::async_trait]
impl<C: WorkerClassification,
    T: ClassifiedCmdTunnel<C>,
    F: ClassifiedCmdTunnelFactory<C, T>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive + RawFixedBytes,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + RawFixedBytes + Eq + Hash> ClassifiedCmdClient<LEN, CMD, C> for DefaultClassifiedCmdClient<C, T, F, LEN, CMD> {
    async fn send_by_classified_tunnel(&self, classification: C, cmd: CMD, body: &[u8]) -> CmdResult<()> {
        let mut send = self.get_classified_send(classification).await?;
        send.send(cmd, body).await
    }

    async fn find_tunnel_id_by_classified(&self, classification: C) -> CmdResult<TunnelId> {
        let send = self.get_classified_send(classification).await?;
        Ok(send.get_tunnel_id())
    }
}
