use std::hash::Hash;
use std::sync::{Arc};
use bucky_raw_codec::{RawConvertTo, RawDecode, RawEncode, RawFixedBytes, RawFrom};
use num::{FromPrimitive, ToPrimitive};
use sfo_pool::{into_pool_err, PoolErrorCode, PoolResult, Worker, WorkerFactory, WorkerGuard, WorkerPool, WorkerPoolRef};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::spawn;
use tokio::task::JoinHandle;
use crate::{CmdTunnel, CmdTunnelWrite, TunnelId, TunnelIdGenerator};
use crate::cmd::{CmdHandler, CmdHandlerMap, CmdHeader};
use crate::errors::{into_cmd_err, CmdErrorCode, CmdResult};
use crate::peer_id::PeerId;

#[async_trait::async_trait]
pub trait CmdTunnelFactory<T: CmdTunnel>: Send + Sync + 'static {
    async fn create_tunnel(&self) -> CmdResult<Arc<T>>;
}

pub struct CmdSend<LEN, CMD>
where LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
      CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static {
    recv_handle: JoinHandle<CmdResult<()>>,
    write: Box<dyn CmdTunnelWrite>,
    is_work: bool,
    tunnel_id: TunnelId,
    _p: std::marker::PhantomData<(LEN, CMD)>,

}

impl<LEN, CMD> CmdSend<LEN, CMD>
where LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
      CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static {
    pub fn new(tunnel_id: TunnelId, recv_handle: JoinHandle<CmdResult<()>>, write: Box<dyn CmdTunnelWrite>) -> Self {
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

impl<LEN, CMD> Drop for CmdSend<LEN, CMD>
where LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
      CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static {
    fn drop(&mut self) {
        self.set_disable();
    }
}

impl<LEN, CMD> Worker for CmdSend<LEN, CMD>
where LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
      CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static{
    fn is_work(&self) -> bool {
        self.is_work && !self.recv_handle.is_finished()
    }
}


pub struct CmdWriteFactory<T: CmdTunnel,
    F: CmdTunnelFactory<T>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static> {
    tunnel_factory: F,
    cmd_handler: Arc<dyn CmdHandler<LEN, CMD>>,
    tunnel_id_generator: TunnelIdGenerator,
    _p: std::marker::PhantomData<T>,
}

impl<T: CmdTunnel,
    F: CmdTunnelFactory<T>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static> CmdWriteFactory<T, F, LEN, CMD> {
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
impl<T: CmdTunnel,
    F: CmdTunnelFactory<T>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive + RawFixedBytes,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + RawFixedBytes> WorkerFactory<CmdSend<LEN, CMD>> for CmdWriteFactory<T, F, LEN, CMD> {
    async fn create(&self) -> PoolResult<CmdSend<LEN, CMD>> {
        let tunnel = self.tunnel_factory.create_tunnel().await.map_err(into_pool_err!(PoolErrorCode::Failed))?;
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
        Ok(CmdSend::new(tunnel_id, handle, write))
    }
}

pub struct CmdClient<T: CmdTunnel,
    F: CmdTunnelFactory<T>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive + RawFixedBytes,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + RawFixedBytes + Eq + Hash> {
    tunnel_pool: WorkerPoolRef<CmdSend<LEN, CMD>, CmdWriteFactory<T, F, LEN, CMD>>,
    cmd_handler_map: Arc<CmdHandlerMap<LEN, CMD>>,
}

impl<T: CmdTunnel,
    F: CmdTunnelFactory<T>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive + RawFixedBytes,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + RawFixedBytes + Eq + Hash> CmdClient<T, F, LEN, CMD> {
    pub fn new(factory: F, tunnel_count: u16) -> Arc<Self> {
        let cmd_handler_map = Arc::new(CmdHandlerMap::new());
        let handler_map = cmd_handler_map.clone();
        Arc::new(Self {
            tunnel_pool: WorkerPool::new(tunnel_count, CmdWriteFactory::<_, _, LEN, CMD>::new(factory, move |peer_id: PeerId, tunnel_id: TunnelId, header: CmdHeader<LEN, CMD>, buf| {
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

    pub fn register_cmd_handler(&self, cmd: CMD, handler: impl CmdHandler<LEN, CMD>) {
        self.cmd_handler_map.insert(cmd, handler);
    }

    pub async fn get_send(&self) -> CmdResult<WorkerGuard<CmdSend<LEN, CMD>, CmdWriteFactory<T, F, LEN, CMD>>> {
        self.tunnel_pool.get_worker().await.map_err(into_cmd_err!(CmdErrorCode::Failed, "get worker failed"))
    }
}


