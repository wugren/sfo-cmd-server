use crate::client::{
    ClassifiedCmdClient, CmdClient, CmdSend, RespWaiter, RespWaiterRef, TrackedSendGuard,
    TunnelReserveResult, TunnelRuntimeRegistry, gen_resp_id, gen_seq,
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
use sfo_split::{RHalf, Splittable, WHalf};
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::spawn;
use tokio::task::JoinHandle;
use tokio::task::yield_now;

pub trait ClassifiedCmdTunnelRead<C: WorkerClassification, M: CmdTunnelMeta>:
    CmdTunnelRead<M> + 'static + Send
{
    fn get_classification(&self) -> C;
}

pub trait ClassifiedCmdTunnelWrite<C: WorkerClassification, M: CmdTunnelMeta>:
    CmdTunnelWrite<M> + 'static + Send
{
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
pub trait ClassifiedCmdTunnelFactory<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
>: Send + Sync + 'static
{
    async fn create_tunnel(&self, classification: Option<C>) -> CmdResult<Splittable<R, W>>;
}

pub struct ClassifiedCmdSend<C, M, R, W, LEN, CMD>
where
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
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
    pub(crate) write: ObjectHolder<ClassifiedCmdTunnelWHalf<R, W>>,
    pub(crate) is_work: bool,
    pub(crate) classification: C,
    pub(crate) pool_tunnel_id: Option<TunnelId>,
    pub(crate) pool_classification: Option<C>,
    pub(crate) tunnel_id: TunnelId,
    pub(crate) resp_waiter: RespWaiterRef,
    pub(crate) remote_id: PeerId,
    pub(crate) pool_peer_id: Option<PeerId>,
    pub(crate) tunnel_meta: Option<Arc<M>>,
    _p: std::marker::PhantomData<(LEN, CMD)>,
}

// impl<C, R, W, LEN, CMD> Deref for ClassifiedCmdSend<C, R, W, LEN, CMD>
// where C: WorkerClassification,
//       R: ClassifiedCmdTunnelRead<C>,
//       W: ClassifiedCmdTunnelWrite<C>,
//       LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
//       CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug + RawFixedBytes {
//     type Target = W;
//
//     fn deref(&self) -> &Self::Target {
//         self.write.deref()
//     }
// }

impl<C, M, R, W, LEN, CMD> ClassifiedCmdSend<C, M, R, W, LEN, CMD>
where
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
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
    pub(crate) fn new(
        tunnel_id: TunnelId,
        classification: C,
        pool_peer_id: Option<PeerId>,
        pool_tunnel_id: Option<TunnelId>,
        pool_classification: Option<C>,
        recv_handle: JoinHandle<CmdResult<()>>,
        write: ObjectHolder<ClassifiedCmdTunnelWHalf<R, W>>,
        resp_waiter: RespWaiterRef,
        remote_id: PeerId,
        tunnel_meta: Option<Arc<M>>,
    ) -> Self {
        Self {
            recv_handle,
            write,
            is_work: true,
            classification,
            pool_tunnel_id,
            pool_classification,
            tunnel_id,
            resp_waiter,
            remote_id,
            pool_peer_id,
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

    pub(crate) fn set_pool_key(
        &mut self,
        peer_id: Option<PeerId>,
        tunnel_id: Option<TunnelId>,
        classification: Option<C>,
    ) {
        self.pool_peer_id = peer_id;
        self.pool_tunnel_id = tunnel_id;
        self.pool_classification = classification;
    }

    pub async fn send(&mut self, cmd: CMD, version: u8, body: &[u8]) -> CmdResult<()> {
        log::trace!(
            "client {:?} send cmd: {:?}, len: {}, data: {}",
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

    pub async fn send_parts(&mut self, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()> {
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

    pub async fn send_parts_with_resp(
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

    #[allow(deprecated)]
    #[deprecated(note = "use send_parts instead")]
    pub async fn send2(&mut self, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()> {
        self.send_parts(cmd, version, body).await
    }

    #[allow(deprecated)]
    #[deprecated(note = "use send_parts_with_resp instead")]
    pub async fn send2_with_resp(
        &mut self,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        self.send_parts_with_resp(cmd, version, body, timeout).await
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

impl<C, M, R, W, LEN, CMD> Drop for ClassifiedCmdSend<C, M, R, W, LEN, CMD>
where
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
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

impl<C, M, R, W, LEN, CMD> CmdSend<M> for ClassifiedCmdSend<C, M, R, W, LEN, CMD>
where
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
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

impl<C, M, R, W, LEN, CMD> ClassifiedWorker<CmdClientTunnelClassification<C>>
    for ClassifiedCmdSend<C, M, R, W, LEN, CMD>
where
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
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
            tunnel_id: self.pool_tunnel_id,
            classification: self.pool_classification.clone(),
        }
    }
}

pub struct ClassifiedCmdWriteFactory<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
    F: ClassifiedCmdTunnelFactory<C, M, R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug + RawFixedBytes,
> {
    tunnel_factory: F,
    cmd_handler: Arc<dyn CmdHandler<LEN, CMD>>,
    resp_waiter: RespWaiterRef,
    tunnel_id_generator: TunnelIdGenerator,
    _p: std::marker::PhantomData<Mutex<(C, M, R, W)>>,
}

impl<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
    F: ClassifiedCmdTunnelFactory<C, M, R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug + RawFixedBytes,
> ClassifiedCmdWriteFactory<C, M, R, W, F, LEN, CMD>
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
            _p: Default::default(),
        }
    }
}

#[async_trait::async_trait]
impl<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
    F: ClassifiedCmdTunnelFactory<C, M, R, W>,
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
> ClassifiedWorkerFactory<CmdClientTunnelClassification<C>, ClassifiedCmdSend<C, M, R, W, LEN, CMD>>
    for ClassifiedCmdWriteFactory<C, M, R, W, F, LEN, CMD>
{
    async fn create(
        &self,
        classification: Option<CmdClientTunnelClassification<C>>,
    ) -> PoolResult<ClassifiedCmdSend<C, M, R, W, LEN, CMD>> {
        if classification.is_some() && classification.as_ref().unwrap().tunnel_id.is_some() {
            return Err(pool_err!(
                PoolErrorCode::Failed,
                "tunnel {:?} not found",
                classification.as_ref().unwrap().tunnel_id.unwrap()
            ));
        }

        let requested_classification = classification;
        let classification = requested_classification
            .as_ref()
            .and_then(|key| key.classification.clone());
        let tunnel = self
            .tunnel_factory
            .create_tunnel(classification)
            .await
            .map_err(into_pool_err!(PoolErrorCode::Failed))?;
        let classification = tunnel.get_classification();
        let pool_tunnel_id = requested_classification
            .as_ref()
            .and_then(|key| key.tunnel_id);
        let pool_classification = requested_classification
            .as_ref()
            .and_then(|key| key.classification.clone())
            .or(Some(classification.clone()));
        let peer_id = tunnel.get_remote_peer_id();
        let tunnel_id = self.tunnel_id_generator.generate();
        let (mut recv, write) = tunnel.split();
        let local_id = recv.get_local_peer_id();
        let remote_id = peer_id.clone();
        let tunnel_meta = recv.get_tunnel_meta();
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
                        "recv cmd {:?} from {} len {} tunnel {:?}",
                        header.cmd_code(),
                        peer_id,
                        header.pkg_len().to_u64().unwrap(),
                        tunnel_id
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
                            local_id.clone(),
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
                        _ => {}
                    }
                    recv = future
                        .await
                        .map_err(into_cmd_err!(CmdErrorCode::Failed))??;
                    log::debug!(
                        "handle cmd {:?} from {} len {} tunnel {:?} complete",
                        cmd_code,
                        peer_id,
                        body_len,
                        tunnel_id
                    );
                }
                Ok(())
            }
            .await;
            if ret.is_err() {
                log::error!("recv cmd error: {:?}", ret.as_ref().err().unwrap());
            }
            ret
        });
        Ok(ClassifiedCmdSend::new(
            tunnel_id,
            classification,
            None,
            pool_tunnel_id,
            pool_classification,
            handle,
            write,
            self.resp_waiter.clone(),
            remote_id,
            tunnel_meta,
        ))
    }
}

pub struct DefaultClassifiedCmdClient<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
    F: ClassifiedCmdTunnelFactory<C, M, R, W>,
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
        CmdClientTunnelClassification<C>,
        ClassifiedCmdSend<C, M, R, W, LEN, CMD>,
        ClassifiedCmdWriteFactory<C, M, R, W, F, LEN, CMD>,
    >,
    runtime_tunnels: Arc<TunnelRuntimeRegistry>,
    cmd_handler_map: Arc<CmdHandlerMap<LEN, CMD>>,
}

impl<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
    F: ClassifiedCmdTunnelFactory<C, M, R, W>,
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
> DefaultClassifiedCmdClient<C, M, R, W, F, LEN, CMD>
{
    fn tunnel_not_found(tunnel_id: TunnelId) -> sfo_result::Error<CmdErrorCode> {
        cmd_err!(CmdErrorCode::Failed, "tunnel {:?} not found", tunnel_id)
    }

    pub fn new(factory: F, tunnel_count: u16) -> Arc<Self> {
        let cmd_handler_map = Arc::new(CmdHandlerMap::new());
        let resp_waiter = Arc::new(RespWaiter::new());
        let handler_map = cmd_handler_map.clone();
        let waiter = resp_waiter.clone();
        Arc::new(Self {
            tunnel_pool: ClassifiedWorkerPool::new(
                tunnel_count,
                ClassifiedCmdWriteFactory::<C, M, R, W, _, LEN, CMD>::new(
                    factory,
                    move |local_id: PeerId,
                          peer_id: PeerId,
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
                                    handler
                                        .handle(local_id, peer_id, tunnel_id, header, body_read)
                                        .await
                                } else {
                                    Ok(None)
                                }
                            }
                        }
                    },
                    resp_waiter.clone(),
                ),
            ),
            runtime_tunnels: TunnelRuntimeRegistry::new(),
            cmd_handler_map,
        })
    }

    fn tracked_send_guard(
        &self,
        worker_guard: ClassifiedWorkerGuard<
            CmdClientTunnelClassification<C>,
            ClassifiedCmdSend<C, M, R, W, LEN, CMD>,
            ClassifiedCmdWriteFactory<C, M, R, W, F, LEN, CMD>,
        >,
    ) -> ClassifiedClientSendGuard<C, M, R, W, F, LEN, CMD> {
        let tunnel_id = worker_guard.get_tunnel_id();
        TrackedSendGuard::new(worker_guard, self.runtime_tunnels.clone(), tunnel_id)
    }

    async fn reserve_tunnel(&self, tunnel_id: TunnelId) -> CmdResult<()> {
        loop {
            match self.runtime_tunnels.reserve_existing(tunnel_id) {
                TunnelReserveResult::Acquired => return Ok(()),
                TunnelReserveResult::Wait(waiter) => waiter.notified().await,
                TunnelReserveResult::Missing => return Err(Self::tunnel_not_found(tunnel_id)),
            }
        }
    }

    async fn get_send(&self) -> CmdResult<ClassifiedClientSendGuard<C, M, R, W, F, LEN, CMD>> {
        loop {
            let worker_guard = self
                .tunnel_pool
                .get_worker()
                .await
                .map_err(into_cmd_err!(CmdErrorCode::Failed, "get worker failed"))?;
            let tunnel_id = worker_guard.get_tunnel_id();
            if self.runtime_tunnels.mark_borrowed(tunnel_id) {
                return Ok(self.tracked_send_guard(worker_guard));
            }
            drop(worker_guard);
            yield_now().await;
        }
    }

    async fn get_send_of_tunnel_id(
        &self,
        tunnel_id: TunnelId,
    ) -> CmdResult<ClassifiedClientSendGuard<C, M, R, W, F, LEN, CMD>> {
        self.reserve_tunnel(tunnel_id).await?;
        match self
            .tunnel_pool
            .get_classified_worker(CmdClientTunnelClassification {
                tunnel_id: Some(tunnel_id),
                classification: None,
            })
            .await
        {
            Ok(worker_guard) => Ok(self.tracked_send_guard(worker_guard)),
            Err(_) => {
                self.runtime_tunnels.remove(tunnel_id);
                Err(Self::tunnel_not_found(tunnel_id))
            }
        }
    }

    async fn get_classified_send(
        &self,
        classification: C,
    ) -> CmdResult<ClassifiedClientSendGuard<C, M, R, W, F, LEN, CMD>> {
        loop {
            let worker_guard = self
                .tunnel_pool
                .get_classified_worker(CmdClientTunnelClassification {
                    tunnel_id: None,
                    classification: Some(classification.clone()),
                })
                .await
                .map_err(into_cmd_err!(CmdErrorCode::Failed, "get worker failed"))?;
            let tunnel_id = worker_guard.get_tunnel_id();
            if self.runtime_tunnels.mark_borrowed(tunnel_id) {
                return Ok(self.tracked_send_guard(worker_guard));
            }
            drop(worker_guard);
            yield_now().await;
        }
    }
}

pub type ClassifiedClientSendGuard<C, M, R, W, F, LEN, CMD> = TrackedSendGuard<
    CmdClientTunnelClassification<C>,
    M,
    ClassifiedCmdSend<C, M, R, W, LEN, CMD>,
    ClassifiedCmdWriteFactory<C, M, R, W, F, LEN, CMD>,
>;
#[async_trait::async_trait]
impl<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
    F: ClassifiedCmdTunnelFactory<C, M, R, W>,
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
>
    CmdClient<
        LEN,
        CMD,
        M,
        ClassifiedCmdSend<C, M, R, W, LEN, CMD>,
        ClassifiedClientSendGuard<C, M, R, W, F, LEN, CMD>,
    > for DefaultClassifiedCmdClient<C, M, R, W, F, LEN, CMD>
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

    async fn send_parts(&self, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()> {
        let mut send = self.get_send().await?;
        send.send_parts(cmd, version, body).await
    }

    async fn send_parts_with_resp(
        &self,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        let mut send = self.get_send().await?;
        send.send_parts_with_resp(cmd, version, body, timeout).await
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

    async fn send_parts_by_specify_tunnel(
        &self,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
    ) -> CmdResult<()> {
        let mut send = self.get_send_of_tunnel_id(tunnel_id).await?;
        send.send_parts(cmd, version, body).await
    }

    async fn send_parts_by_specify_tunnel_with_resp(
        &self,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        let mut send = self.get_send_of_tunnel_id(tunnel_id).await?;
        send.send_parts_with_resp(cmd, version, body, timeout).await
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
        self.runtime_tunnels.clear();
    }

    async fn get_send(
        &self,
        tunnel_id: TunnelId,
    ) -> CmdResult<ClassifiedClientSendGuard<C, M, R, W, F, LEN, CMD>> {
        self.get_send_of_tunnel_id(tunnel_id).await
    }
}

#[async_trait::async_trait]
impl<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
    F: ClassifiedCmdTunnelFactory<C, M, R, W>,
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
>
    ClassifiedCmdClient<
        LEN,
        CMD,
        C,
        M,
        ClassifiedCmdSend<C, M, R, W, LEN, CMD>,
        ClassifiedClientSendGuard<C, M, R, W, F, LEN, CMD>,
    > for DefaultClassifiedCmdClient<C, M, R, W, F, LEN, CMD>
{
    async fn send_by_classified_tunnel(
        &self,
        classification: C,
        cmd: CMD,
        version: u8,
        body: &[u8],
    ) -> CmdResult<()> {
        let mut send = self.get_classified_send(classification).await?;
        send.send(cmd, version, body).await
    }

    async fn send_by_classified_tunnel_with_resp(
        &self,
        classification: C,
        cmd: CMD,
        version: u8,
        body: &[u8],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        let mut send = self.get_classified_send(classification).await?;
        send.send_with_resp(cmd, version, body, timeout).await
    }

    async fn send_parts_by_classified_tunnel(
        &self,
        classification: C,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
    ) -> CmdResult<()> {
        let mut send = self.get_classified_send(classification).await?;
        send.send_parts(cmd, version, body).await
    }

    async fn send_parts_by_classified_tunnel_with_resp(
        &self,
        classification: C,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        let mut send = self.get_classified_send(classification).await?;
        send.send_parts_with_resp(cmd, version, body, timeout).await
    }

    async fn send_cmd_by_classified_tunnel(
        &self,
        classification: C,
        cmd: CMD,
        version: u8,
        body: CmdBody,
    ) -> CmdResult<()> {
        let mut send = self.get_classified_send(classification).await?;
        send.send_cmd(cmd, version, body).await
    }

    async fn send_cmd_by_classified_tunnel_with_resp(
        &self,
        classification: C,
        cmd: CMD,
        version: u8,
        body: CmdBody,
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        let mut send = self.get_classified_send(classification).await?;
        send.send_cmd_with_resp(cmd, version, body, timeout).await
    }

    async fn find_tunnel_id_by_classified(&self, classification: C) -> CmdResult<TunnelId> {
        let send = self.get_classified_send(classification).await?;
        Ok(send.get_tunnel_id())
    }

    async fn get_send_by_classified(
        &self,
        classification: C,
    ) -> CmdResult<ClassifiedClientSendGuard<C, M, R, W, F, LEN, CMD>> {
        self.get_classified_send(classification).await
    }
}
