use crate::client::{
    CmdClient, CmdSend, RespWaiter, RespWaiterRef, SendGuard, gen_resp_id, gen_seq,
};
use crate::cmd::{CmdBodyRead, CmdHandler, CmdHandlerMap, CmdHeader};
use crate::errors::{CmdErrorCode, CmdResult, cmd_err, into_cmd_err};
use crate::peer_id::PeerId;
use crate::{CmdBody, CmdTunnelMeta, CmdTunnelRead, CmdTunnelWrite, TunnelId, TunnelIdGenerator};
use async_named_locker::ObjectHolder;
use bucky_raw_codec::{RawConvertTo, RawDecode, RawEncode, RawFixedBytes, RawFrom};
use num::{FromPrimitive, ToPrimitive};
use sfo_pool::{
    ClassifiedWorker, ClassifiedWorkerFactory, ClassifiedWorkerGuard, ClassifiedWorkerPool,
    ClassifiedWorkerPoolRef, PoolErrorCode, PoolResult, WorkerClassification, into_pool_err,
    pool_err,
};
use sfo_split::{Splittable, WHalf};
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::spawn;
use tokio::task::JoinHandle;

#[async_trait::async_trait]
pub trait CmdTunnelFactory<M: CmdTunnelMeta, R: CmdTunnelRead<M>, W: CmdTunnelWrite<M>>:
    Send + Sync + 'static
{
    async fn create_tunnel(&self) -> CmdResult<Splittable<R, W>>;
}

pub struct CommonCmdSend<M: CmdTunnelMeta, R: CmdTunnelRead<M>, W: CmdTunnelWrite<M>, LEN, CMD>
where
    LEN: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + Send
        + Sync
        + 'static
        + FromPrimitive
        + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug + RawFixedBytes,
{
    pub(crate) recv_handle: JoinHandle<CmdResult<()>>,
    pub(crate) write: ObjectHolder<WHalf<R, W>>,
    pub(crate) is_work: bool,
    pub(crate) tunnel_id: TunnelId,
    pub(crate) remote_id: PeerId,
    pub(crate) resp_waiter: RespWaiterRef,
    pub(crate) tunnel_meta: Option<Arc<M>>,
    _p: std::marker::PhantomData<(LEN, CMD)>,
}

// impl<R, W, LEN, CMD> Deref for CmdSend<R, W, LEN, CMD>
// where R: CmdTunnelRead,
//       W: CmdTunnelWrite,
//       LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
//       CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug + RawFixedBytes {
//     type Target = W;
//
//     fn deref(&self) -> &Self::Target {
//         self.write.deref()
//     }
// }

impl<M, R, W, LEN, CMD> CommonCmdSend<M, R, W, LEN, CMD>
where
    M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    LEN: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + Send
        + Sync
        + 'static
        + FromPrimitive
        + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug + RawFixedBytes,
{
    pub fn new(
        tunnel_id: TunnelId,
        recv_handle: JoinHandle<CmdResult<()>>,
        write: ObjectHolder<WHalf<R, W>>,
        resp_waiter: RespWaiterRef,
        remote_id: PeerId,
        tunnel_meta: Option<Arc<M>>,
    ) -> Self {
        Self {
            recv_handle,
            write,
            is_work: true,
            tunnel_id,
            remote_id,
            resp_waiter,
            tunnel_meta,
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
        log::trace!(
            "client {:?} send cmd: {:?}, len: {} data:{}",
            self.tunnel_id,
            cmd,
            body.len(),
            hex::encode(body)
        );
        let header = CmdHeader::<LEN, CMD>::new(
            version,
            false,
            None,
            cmd,
            LEN::from_u64(body.len() as u64).unwrap(),
        );
        let buf = header
            .to_vec()
            .map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
        let ret = self.send_inner(buf.as_slice(), body).await;
        if let Err(e) = ret {
            self.set_disable();
            return Err(e);
        }
        Ok(())
    }

    pub async fn send_with_resp(
        &mut self,
        cmd: CMD,
        version: u8,
        body: &[u8],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        if let Some(id) = tokio::task::try_id() {
            if id == self.recv_handle.id() {
                return Err(cmd_err!(
                    CmdErrorCode::Failed,
                    "can't send with resp in recv task"
                ));
            }
        }
        log::trace!(
            "client {:?} send cmd: {:?}, len: {}, data: {}",
            self.tunnel_id,
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
            LEN::from_u64(body.len() as u64).unwrap(),
        );
        let buf = header
            .to_vec()
            .map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
        let resp_id = gen_resp_id(self.tunnel_id, cmd, seq);
        let waiter = self.resp_waiter.clone();
        let resp_waiter = waiter
            .create_timeout_result_future(resp_id, timeout)
            .map_err(into_cmd_err!(
                CmdErrorCode::Failed,
                "create timeout result future error"
            ))?;
        let ret = self.send_inner(buf.as_slice(), body).await;
        if let Err(e) = ret {
            self.set_disable();
            return Err(e);
        }
        let resp = resp_waiter
            .await
            .map_err(into_cmd_err!(CmdErrorCode::Timeout, "recv resp error"))?;
        Ok(resp)
    }

    pub async fn send2(&mut self, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()> {
        let mut len = 0;
        for b in body.iter() {
            len += b.len();
            log::trace!(
                "client {:?} send2 cmd: {:?}, data {}",
                self.tunnel_id,
                cmd,
                hex::encode(b)
            );
        }
        log::trace!(
            "client {:?} send2 cmd: {:?}, len {}",
            self.tunnel_id,
            cmd,
            len
        );
        let header = CmdHeader::<LEN, CMD>::new(
            version,
            false,
            None,
            cmd,
            LEN::from_u64(len as u64).unwrap(),
        );
        let buf = header
            .to_vec()
            .map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
        let ret = self.send_inner2(buf.as_slice(), body).await;
        if let Err(e) = ret {
            self.set_disable();
            return Err(e);
        }
        Ok(())
    }

    pub async fn send2_with_resp(
        &mut self,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        if let Some(id) = tokio::task::try_id() {
            if id == self.recv_handle.id() {
                return Err(cmd_err!(
                    CmdErrorCode::Failed,
                    "can't send with resp in recv task"
                ));
            }
        }
        let mut len = 0;
        for b in body.iter() {
            len += b.len();
            log::trace!(
                "client {:?} send2 cmd {:?} body: {}",
                self.tunnel_id,
                cmd,
                hex::encode(b)
            );
        }
        log::trace!(
            "client {:?} send2 cmd: {:?}, len {}",
            self.tunnel_id,
            cmd,
            len
        );
        let seq = gen_seq();
        let header = CmdHeader::<LEN, CMD>::new(
            version,
            false,
            Some(seq),
            cmd,
            LEN::from_u64(len as u64).unwrap(),
        );
        let buf = header
            .to_vec()
            .map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
        let resp_id = gen_resp_id(self.tunnel_id, cmd, seq);
        let waiter = self.resp_waiter.clone();
        let resp_waiter = waiter
            .create_timeout_result_future(resp_id, timeout)
            .map_err(into_cmd_err!(
                CmdErrorCode::Failed,
                "create timeout result future error"
            ))?;
        let ret = self.send_inner2(buf.as_slice(), body).await;
        if let Err(e) = ret {
            self.set_disable();
            return Err(e);
        }
        let resp = resp_waiter
            .await
            .map_err(into_cmd_err!(CmdErrorCode::Timeout, "recv resp error"))?;
        Ok(resp)
    }

    pub async fn send_cmd(&mut self, cmd: CMD, version: u8, body: CmdBody) -> CmdResult<()> {
        log::trace!(
            "client {:?} send cmd: {:?}, len: {}",
            self.tunnel_id,
            cmd,
            body.len()
        );
        let header = CmdHeader::<LEN, CMD>::new(
            version,
            false,
            None,
            cmd,
            LEN::from_u64(body.len()).unwrap(),
        );
        let buf = header
            .to_vec()
            .map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
        let ret = self.send_inner_cmd(buf.as_slice(), body).await;
        if let Err(e) = ret {
            self.set_disable();
            return Err(e);
        }
        Ok(())
    }

    pub async fn send_cmd_with_resp(
        &mut self,
        cmd: CMD,
        version: u8,
        body: CmdBody,
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        if let Some(id) = tokio::task::try_id() {
            if id == self.recv_handle.id() {
                return Err(cmd_err!(
                    CmdErrorCode::Failed,
                    "can't send with resp in recv task"
                ));
            }
        }
        log::trace!(
            "client {:?} send cmd: {:?}, len: {}",
            self.tunnel_id,
            cmd,
            body.len()
        );
        let seq = gen_seq();
        let header = CmdHeader::<LEN, CMD>::new(
            version,
            false,
            Some(seq),
            cmd,
            LEN::from_u64(body.len()).unwrap(),
        );
        let buf = header
            .to_vec()
            .map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
        let resp_id = gen_resp_id(self.tunnel_id, cmd, seq);
        let waiter = self.resp_waiter.clone();
        let resp_waiter = waiter
            .create_timeout_result_future(resp_id, timeout)
            .map_err(into_cmd_err!(
                CmdErrorCode::Failed,
                "create timeout result future error"
            ))?;
        let ret = self.send_inner_cmd(buf.as_slice(), body).await;
        if let Err(e) = ret {
            self.set_disable();
            return Err(e);
        }
        let resp = resp_waiter
            .await
            .map_err(into_cmd_err!(CmdErrorCode::Timeout, "recv resp error"))?;
        Ok(resp)
    }

    async fn send_inner(&mut self, header: &[u8], body: &[u8]) -> CmdResult<()> {
        let mut write = self.write.get().await;
        if header.len() > 255 {
            return Err(cmd_err!(CmdErrorCode::InvalidParam, "header len too long"));
        }
        write
            .write_u8(header.len() as u8)
            .await
            .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        write
            .write_all(header)
            .await
            .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        write
            .write_all(body)
            .await
            .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        write
            .flush()
            .await
            .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        Ok(())
    }

    async fn send_inner2(&mut self, header: &[u8], body: &[&[u8]]) -> CmdResult<()> {
        let mut write = self.write.get().await;
        if header.len() > 255 {
            return Err(cmd_err!(CmdErrorCode::InvalidParam, "header len too long"));
        }
        write
            .write_u8(header.len() as u8)
            .await
            .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        write
            .write_all(header)
            .await
            .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        for b in body.iter() {
            write
                .write_all(b)
                .await
                .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        }
        write
            .flush()
            .await
            .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        Ok(())
    }

    async fn send_inner_cmd(&mut self, header: &[u8], mut body: CmdBody) -> CmdResult<()> {
        let mut write = self.write.get().await;
        if header.len() > 255 {
            return Err(cmd_err!(CmdErrorCode::InvalidParam, "header len too large"));
        }
        write
            .write_u8(header.len() as u8)
            .await
            .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        write
            .write_all(header)
            .await
            .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        tokio::io::copy(&mut body, write.deref_mut().deref_mut())
            .await
            .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        write
            .flush()
            .await
            .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
        Ok(())
    }
}

impl<M, R, W, LEN, CMD> Drop for CommonCmdSend<M, R, W, LEN, CMD>
where
    M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    LEN: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + Send
        + Sync
        + 'static
        + FromPrimitive
        + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug + RawFixedBytes,
{
    fn drop(&mut self) {
        self.set_disable();
    }
}

impl<M, R, W, LEN, CMD> CmdSend<M> for CommonCmdSend<M, R, W, LEN, CMD>
where
    M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    LEN: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + Send
        + Sync
        + 'static
        + FromPrimitive
        + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug + RawFixedBytes,
{
    fn get_tunnel_meta(&self) -> Option<Arc<M>> {
        self.tunnel_meta.clone()
    }

    fn get_remote_peer_id(&self) -> PeerId {
        self.remote_id.clone()
    }
}

impl<M, R, W, LEN, CMD> ClassifiedWorker<TunnelId> for CommonCmdSend<M, R, W, LEN, CMD>
where
    M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    LEN: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + Send
        + Sync
        + 'static
        + FromPrimitive
        + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug + RawFixedBytes,
{
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
    M: CmdTunnelMeta,
    CW: ClassifiedWorker<C> + CmdSend<M>,
    F: ClassifiedWorkerFactory<C, CW>,
> {
    pub(crate) worker_guard: ClassifiedWorkerGuard<C, CW, F>,
    pub(crate) _p: PhantomData<M>,
}

impl<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    CW: ClassifiedWorker<C> + CmdSend<M>,
    F: ClassifiedWorkerFactory<C, CW>,
> Deref for ClassifiedSendGuard<C, M, CW, F>
{
    type Target = CW;

    fn deref(&self) -> &Self::Target {
        &self.worker_guard.deref()
    }
}

impl<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    CW: ClassifiedWorker<C> + CmdSend<M>,
    F: ClassifiedWorkerFactory<C, CW>,
> SendGuard<M, CW> for ClassifiedSendGuard<C, M, CW, F>
{
}

pub struct CmdWriteFactory<
    M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    F: CmdTunnelFactory<M, R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug,
> {
    tunnel_factory: F,
    cmd_handler: Arc<dyn CmdHandler<LEN, CMD>>,
    resp_waiter: RespWaiterRef,
    tunnel_id_generator: TunnelIdGenerator,
    p: std::marker::PhantomData<Mutex<(R, W, M)>>,
}

impl<
    M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    F: CmdTunnelFactory<M, R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug,
> CmdWriteFactory<M, R, W, F, LEN, CMD>
{
    pub(crate) fn new(
        tunnel_factory: F,
        cmd_handler: impl CmdHandler<LEN, CMD>,
        resp_waiter: RespWaiterRef,
    ) -> Self {
        Self {
            tunnel_factory,
            cmd_handler: Arc::new(cmd_handler),
            resp_waiter,
            tunnel_id_generator: TunnelIdGenerator::new(),
            p: Default::default(),
        }
    }
}

#[async_trait::async_trait]
impl<
    M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    F: CmdTunnelFactory<M, R, W>,
    LEN: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + Send
        + Sync
        + 'static
        + FromPrimitive
        + ToPrimitive
        + RawFixedBytes,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + RawFixedBytes + Debug,
> ClassifiedWorkerFactory<TunnelId, CommonCmdSend<M, R, W, LEN, CMD>>
    for CmdWriteFactory<M, R, W, F, LEN, CMD>
{
    async fn create(&self, c: Option<TunnelId>) -> PoolResult<CommonCmdSend<M, R, W, LEN, CMD>> {
        if c.is_some() {
            return Err(pool_err!(
                PoolErrorCode::Failed,
                "tunnel {:?} not found",
                c.unwrap()
            ));
        }
        let tunnel = self
            .tunnel_factory
            .create_tunnel()
            .await
            .map_err(into_pool_err!(PoolErrorCode::Failed))?;
        let peer_id = tunnel.get_remote_peer_id();
        let tunnel_id = self.tunnel_id_generator.generate();
        let (mut recv, write) = tunnel.split();
        let remote_id = write.get_remote_peer_id();
        let meta = write.get_tunnel_meta();
        let write = ObjectHolder::new(write);
        let resp_write = write.clone();
        let cmd_handler = self.cmd_handler.clone();
        let handle = spawn(async move {
            let ret: CmdResult<()> = async move {
                loop {
                    let header_len = recv
                        .read_u8()
                        .await
                        .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                    let mut header = vec![0u8; header_len as usize];
                    let n = recv
                        .read_exact(header.as_mut())
                        .await
                        .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                    if n == 0 {
                        break;
                    }
                    let header = CmdHeader::<LEN, CMD>::clone_from_slice(header.as_slice())
                        .map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
                    log::trace!(
                        "recv cmd {:?} from {} len {}",
                        header.cmd_code(),
                        peer_id.to_base58(),
                        header.pkg_len().to_u64().unwrap()
                    );
                    let body_len = header.pkg_len().to_u64().unwrap();
                    let cmd_read =
                        CmdBodyRead::new(recv, header.pkg_len().to_u64().unwrap() as usize);
                    let waiter = cmd_read.get_waiter();
                    let future = waiter
                        .create_result_future()
                        .map_err(into_cmd_err!(CmdErrorCode::Failed))?;
                    let version = header.version();
                    let seq = header.seq();
                    let cmd_code = header.cmd_code();
                    match cmd_handler
                        .handle(
                            peer_id.clone(),
                            tunnel_id,
                            header,
                            CmdBody::from_reader(BufReader::new(cmd_read), body_len),
                        )
                        .await
                    {
                        Ok(Some(mut body)) => {
                            let mut write = resp_write.get().await;
                            let header = CmdHeader::<LEN, CMD>::new(
                                version,
                                true,
                                seq,
                                cmd_code,
                                LEN::from_u64(body.len()).unwrap(),
                            );
                            let buf = header
                                .to_vec()
                                .map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
                            if buf.len() > 255 {
                                return Err(cmd_err!(
                                    CmdErrorCode::InvalidParam,
                                    "header len too long"
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
                        Ok(None) => {}
                        Err(e) => {
                            log::error!("handle cmd error: {:?}", e);
                        }
                    }
                    recv = future
                        .await
                        .map_err(into_cmd_err!(CmdErrorCode::Failed))??;
                }
                Ok(())
            }
            .await;
            ret
        });
        Ok(CommonCmdSend::new(
            tunnel_id,
            handle,
            write,
            self.resp_waiter.clone(),
            remote_id,
            meta,
        ))
    }
}

pub struct DefaultCmdClient<
    M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    F: CmdTunnelFactory<M, R, W>,
    LEN: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + Send
        + Sync
        + 'static
        + FromPrimitive
        + ToPrimitive
        + RawFixedBytes,
    CMD: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + Send
        + Sync
        + 'static
        + RawFixedBytes
        + Eq
        + Hash
        + Debug,
> {
    tunnel_pool: ClassifiedWorkerPoolRef<
        TunnelId,
        CommonCmdSend<M, R, W, LEN, CMD>,
        CmdWriteFactory<M, R, W, F, LEN, CMD>,
    >,
    cmd_handler_map: Arc<CmdHandlerMap<LEN, CMD>>,
}

impl<
    M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    F: CmdTunnelFactory<M, R, W>,
    LEN: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + Send
        + Sync
        + 'static
        + FromPrimitive
        + ToPrimitive
        + RawFixedBytes,
    CMD: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + Send
        + Sync
        + 'static
        + RawFixedBytes
        + Eq
        + Hash
        + Debug,
> DefaultCmdClient<M, R, W, F, LEN, CMD>
{
    pub fn new(factory: F, tunnel_count: u16) -> Arc<Self> {
        let cmd_handler_map = Arc::new(CmdHandlerMap::new());
        let handler_map = cmd_handler_map.clone();
        let resp_waiter = Arc::new(RespWaiter::new());
        let waiter = resp_waiter.clone();
        Arc::new(Self {
            tunnel_pool: ClassifiedWorkerPool::new(
                tunnel_count,
                CmdWriteFactory::<M, R, W, _, LEN, CMD>::new(
                    factory,
                    move |peer_id: PeerId,
                          tunnel_id: TunnelId,
                          header: CmdHeader<LEN, CMD>,
                          body_read: CmdBody| {
                        let handler_map = handler_map.clone();
                        let waiter = waiter.clone();
                        async move {
                            if header.is_resp() && header.seq().is_some() {
                                let resp_id = gen_resp_id(
                                    tunnel_id,
                                    header.cmd_code(),
                                    header.seq().unwrap(),
                                );
                                let _ = waiter.set_result(resp_id, body_read);
                                Ok(None)
                            } else {
                                if let Some(handler) = handler_map.get(header.cmd_code()) {
                                    handler.handle(peer_id, tunnel_id, header, body_read).await
                                } else {
                                    Ok(None)
                                }
                            }
                        }
                    },
                    resp_waiter.clone(),
                ),
            ),
            cmd_handler_map,
        })
    }

    async fn get_send(
        &self,
    ) -> CmdResult<
        ClassifiedWorkerGuard<
            TunnelId,
            CommonCmdSend<M, R, W, LEN, CMD>,
            CmdWriteFactory<M, R, W, F, LEN, CMD>,
        >,
    > {
        self.tunnel_pool
            .get_worker()
            .await
            .map_err(into_cmd_err!(CmdErrorCode::Failed, "get worker failed"))
    }

    async fn get_send_of_tunnel_id(
        &self,
        tunnel_id: TunnelId,
    ) -> CmdResult<
        ClassifiedWorkerGuard<
            TunnelId,
            CommonCmdSend<M, R, W, LEN, CMD>,
            CmdWriteFactory<M, R, W, F, LEN, CMD>,
        >,
    > {
        self.tunnel_pool
            .get_classified_worker(tunnel_id)
            .await
            .map_err(into_cmd_err!(CmdErrorCode::Failed, "get worker failed"))
    }
}

pub type CmdClientSendGuard<M, R, W, F, LEN, CMD> = ClassifiedSendGuard<
    TunnelId,
    M,
    CommonCmdSend<M, R, W, LEN, CMD>,
    CmdWriteFactory<M, R, W, F, LEN, CMD>,
>;
#[async_trait::async_trait]
impl<
    M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    F: CmdTunnelFactory<M, R, W>,
    LEN: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + Send
        + Sync
        + 'static
        + FromPrimitive
        + ToPrimitive
        + RawFixedBytes,
    CMD: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + Send
        + Sync
        + 'static
        + RawFixedBytes
        + Eq
        + Hash
        + Debug,
> CmdClient<LEN, CMD, M, CommonCmdSend<M, R, W, LEN, CMD>, CmdClientSendGuard<M, R, W, F, LEN, CMD>>
    for DefaultCmdClient<M, R, W, F, LEN, CMD>
{
    fn register_cmd_handler(&self, cmd: CMD, handler: impl CmdHandler<LEN, CMD>) {
        self.cmd_handler_map.insert(cmd, handler);
    }

    async fn send(&self, cmd: CMD, version: u8, body: &[u8]) -> CmdResult<()> {
        let mut send = self.get_send().await?;
        send.send(cmd, version, body).await
    }

    async fn send_with_resp(
        &self,
        cmd: CMD,
        version: u8,
        body: &[u8],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        let mut send = self.get_send().await?;
        send.send_with_resp(cmd, version, body, timeout).await
    }

    async fn send2(&self, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()> {
        let mut send = self.get_send().await?;
        send.send2(cmd, version, body).await
    }

    async fn send2_with_resp(
        &self,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        let mut send = self.get_send().await?;
        send.send2_with_resp(cmd, version, body, timeout).await
    }

    async fn send_cmd(&self, cmd: CMD, version: u8, body: CmdBody) -> CmdResult<()> {
        let mut send = self.get_send().await?;
        send.send_cmd(cmd, version, body).await
    }

    async fn send_cmd_with_resp(
        &self,
        cmd: CMD,
        version: u8,
        body: CmdBody,
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        let mut send = self.get_send().await?;
        send.send_cmd_with_resp(cmd, version, body, timeout).await
    }

    async fn send_by_specify_tunnel(
        &self,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: &[u8],
    ) -> CmdResult<()> {
        let mut send = self.get_send_of_tunnel_id(tunnel_id).await?;
        send.send(cmd, version, body).await
    }

    async fn send_by_specify_tunnel_with_resp(
        &self,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: &[u8],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        let mut send = self.get_send_of_tunnel_id(tunnel_id).await?;
        send.send_with_resp(cmd, version, body, timeout).await
    }

    async fn send2_by_specify_tunnel(
        &self,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
    ) -> CmdResult<()> {
        let mut send = self.get_send_of_tunnel_id(tunnel_id).await?;
        send.send2(cmd, version, body).await
    }

    async fn send2_by_specify_tunnel_with_resp(
        &self,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        let mut send = self.get_send_of_tunnel_id(tunnel_id).await?;
        send.send2_with_resp(cmd, version, body, timeout).await
    }

    async fn send_cmd_by_specify_tunnel(
        &self,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: CmdBody,
    ) -> CmdResult<()> {
        let mut send = self.get_send_of_tunnel_id(tunnel_id).await?;
        send.send_cmd(cmd, version, body).await
    }

    async fn send_cmd_by_specify_tunnel_with_resp(
        &self,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: CmdBody,
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        let mut send = self.get_send_of_tunnel_id(tunnel_id).await?;
        send.send_cmd_with_resp(cmd, version, body, timeout).await
    }

    async fn clear_all_tunnel(&self) {
        self.tunnel_pool.clear_all_worker().await;
    }

    async fn get_send(
        &self,
        tunnel_id: TunnelId,
    ) -> CmdResult<CmdClientSendGuard<M, R, W, F, LEN, CMD>> {
        Ok(ClassifiedSendGuard {
            worker_guard: self.get_send_of_tunnel_id(tunnel_id).await?,
            _p: Default::default(),
        })
    }
}
