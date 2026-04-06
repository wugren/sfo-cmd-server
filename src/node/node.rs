use crate::client::{
    CommonCmdSend, RespWaiter, RespWaiterRef, TrackedSendGuard, TunnelReserveResult,
    TunnelRuntimeRegistry, gen_resp_id,
};
use crate::cmd::CmdHandlerMap;
use crate::errors::{CmdErrorCode, CmdResult, into_cmd_err};
use crate::node::create_recv_handle;
use crate::server::CmdTunnelListener;
use crate::{
    CmdBody, CmdHandler, CmdHeader, CmdNode, CmdTunnelMeta, CmdTunnelRead, CmdTunnelWrite, PeerId,
    TunnelId, TunnelIdGenerator, into_pool_err, pool_err,
};
use async_named_locker::ObjectHolder;
use bucky_raw_codec::{RawDecode, RawEncode, RawFixedBytes};
use num::{FromPrimitive, ToPrimitive};
use sfo_pool::{
    ClassifiedWorker, ClassifiedWorkerFactory, ClassifiedWorkerGuard, ClassifiedWorkerPool,
    ClassifiedWorkerPoolRef, PoolErrorCode, PoolResult,
};
use sfo_split::Splittable;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::yield_now;

#[async_trait::async_trait]
pub trait CmdNodeTunnelFactory<M: CmdTunnelMeta, R: CmdTunnelRead<M>, W: CmdTunnelWrite<M>>:
    Send + Sync + 'static
{
    async fn create_tunnel(&self, remote_id: &PeerId) -> CmdResult<Splittable<R, W>>;
}

impl<M, R, W, LEN, CMD> ClassifiedWorker<(PeerId, Option<TunnelId>)>
    for CommonCmdSend<M, R, W, LEN, CMD>
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

    fn is_valid(&self, c: (PeerId, Option<TunnelId>)) -> bool {
        let (peer_id, tunnel_id) = c;
        if tunnel_id.is_some() {
            self.tunnel_id == tunnel_id.unwrap() && peer_id == self.remote_id
        } else {
            peer_id == self.remote_id
        }
    }

    fn classification(&self) -> (PeerId, Option<TunnelId>) {
        (self.remote_id.clone(), None)
    }
}

struct CmdWriteFactoryImpl<
    M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    F: CmdNodeTunnelFactory<M, R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug + RawFixedBytes,
    LISTENER: CmdTunnelListener<M, R, W>,
> {
    tunnel_listener: LISTENER,
    tunnel_factory: F,
    cmd_handler: Arc<dyn CmdHandler<LEN, CMD>>,
    tunnel_id_generator: TunnelIdGenerator,
    resp_waiter: RespWaiterRef,
    send_cache: Arc<Mutex<HashMap<PeerId, Vec<CommonCmdSend<M, R, W, LEN, CMD>>>>>,
}

impl<
    M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    F: CmdNodeTunnelFactory<M, R, W>,
    LEN: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + Send
        + Sync
        + 'static
        + FromPrimitive
        + ToPrimitive
        + RawFixedBytes,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug + RawFixedBytes,
    LISTENER: CmdTunnelListener<M, R, W>,
> CmdWriteFactoryImpl<M, R, W, F, LEN, CMD, LISTENER>
{
    pub fn new(
        tunnel_factory: F,
        tunnel_listener: LISTENER,
        cmd_handler: impl CmdHandler<LEN, CMD>,
        resp_waiter: RespWaiterRef,
    ) -> Self {
        Self {
            tunnel_listener,
            tunnel_factory,
            cmd_handler: Arc::new(cmd_handler),
            tunnel_id_generator: TunnelIdGenerator::new(),
            resp_waiter,
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
            let resp_waiter = self.resp_waiter.clone();
            let this = self.clone();
            tokio::spawn(async move {
                let ret: CmdResult<()> = async move {
                    let this = this.clone();
                    let cmd_handler = this.cmd_handler.clone();
                    let (reader, writer) = tunnel.split();
                    let remote_id = reader.get_remote_peer_id();
                    let tunnel_meta = reader.get_tunnel_meta();
                    let writer = ObjectHolder::new(writer);
                    let recv_handle = create_recv_handle::<M, R, W, LEN, CMD>(
                        reader,
                        writer.clone(),
                        tunnel_id,
                        cmd_handler,
                    );
                    {
                        let mut send_cache = this.send_cache.lock().unwrap();
                        let send_list = send_cache.entry(peer_id).or_insert(Vec::new());
                        send_list.push(CommonCmdSend::new(
                            tunnel_id,
                            recv_handle,
                            writer,
                            resp_waiter,
                            remote_id,
                            tunnel_meta,
                        ));
                    }
                    Ok(())
                }
                .await;
                if let Err(e) = ret {
                    log::error!("peer connection error: {:?}", e);
                }
            });
        }
    }
}

#[async_trait::async_trait]
impl<
    M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    F: CmdNodeTunnelFactory<M, R, W>,
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
    LISTENER: CmdTunnelListener<M, R, W>,
> ClassifiedWorkerFactory<(PeerId, Option<TunnelId>), CommonCmdSend<M, R, W, LEN, CMD>>
    for CmdWriteFactoryImpl<M, R, W, F, LEN, CMD, LISTENER>
{
    async fn create(
        &self,
        c: Option<(PeerId, Option<TunnelId>)>,
    ) -> PoolResult<CommonCmdSend<M, R, W, LEN, CMD>> {
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
                        Err(pool_err!(
                            PoolErrorCode::Failed,
                            "tunnel {:?} not found",
                            tunnel_id.unwrap()
                        ))
                    }
                } else {
                    Err(pool_err!(
                        PoolErrorCode::Failed,
                        "tunnel {:?} not found",
                        tunnel_id.unwrap()
                    ))
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
                let tunnel = self
                    .tunnel_factory
                    .create_tunnel(&peer_id)
                    .await
                    .map_err(into_pool_err!(PoolErrorCode::Failed))?;
                let tunnel_id = self.tunnel_id_generator.generate();
                let (recv, write) = tunnel.split();
                let remote_id = recv.get_remote_peer_id();
                let tunnel_meta = recv.get_tunnel_meta();
                let write = ObjectHolder::new(write);
                let cmd_handler = self.cmd_handler.clone();
                let handle = create_recv_handle::<M, R, W, LEN, CMD>(
                    recv,
                    write.clone(),
                    tunnel_id,
                    cmd_handler,
                );
                Ok(CommonCmdSend::new(
                    tunnel_id,
                    handle,
                    write,
                    self.resp_waiter.clone(),
                    remote_id,
                    tunnel_meta,
                ))
            }
        } else {
            Err(pool_err!(PoolErrorCode::Failed, "peer id is none"))
        }
    }
}

pub struct CmdNodeWriteFactory<
    M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    F: CmdNodeTunnelFactory<M, R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug + RawFixedBytes,
    LISTENER: CmdTunnelListener<M, R, W>,
> {
    inner: Arc<CmdWriteFactoryImpl<M, R, W, F, LEN, CMD, LISTENER>>,
}

impl<
    M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    F: CmdNodeTunnelFactory<M, R, W>,
    LEN: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + Send
        + Sync
        + 'static
        + FromPrimitive
        + ToPrimitive
        + RawFixedBytes,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug + RawFixedBytes,
    LISTENER: CmdTunnelListener<M, R, W>,
> CmdNodeWriteFactory<M, R, W, F, LEN, CMD, LISTENER>
{
    pub(crate) fn new(
        tunnel_factory: F,
        tunnel_listener: LISTENER,
        cmd_handler: impl CmdHandler<LEN, CMD>,
        resp_waiter: RespWaiterRef,
    ) -> Self {
        Self {
            inner: Arc::new(CmdWriteFactoryImpl::new(
                tunnel_factory,
                tunnel_listener,
                cmd_handler,
                resp_waiter,
            )),
        }
    }

    pub fn start(&self) {
        self.inner.start();
    }
}

#[async_trait::async_trait]
impl<
    M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    F: CmdNodeTunnelFactory<M, R, W>,
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
    LISTENER: CmdTunnelListener<M, R, W>,
> ClassifiedWorkerFactory<(PeerId, Option<TunnelId>), CommonCmdSend<M, R, W, LEN, CMD>>
    for CmdNodeWriteFactory<M, R, W, F, LEN, CMD, LISTENER>
{
    async fn create(
        &self,
        c: Option<(PeerId, Option<TunnelId>)>,
    ) -> PoolResult<CommonCmdSend<M, R, W, LEN, CMD>> {
        self.inner.create(c).await
    }
}
pub struct DefaultCmdNode<
    M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    F: CmdNodeTunnelFactory<M, R, W>,
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
    LISTENER: CmdTunnelListener<M, R, W>,
> {
    tunnel_pool: ClassifiedWorkerPoolRef<
        (PeerId, Option<TunnelId>),
        CommonCmdSend<M, R, W, LEN, CMD>,
        CmdNodeWriteFactory<M, R, W, F, LEN, CMD, LISTENER>,
    >,
    runtime_tunnels: Arc<TunnelRuntimeRegistry>,
    cmd_handler_map: Arc<CmdHandlerMap<LEN, CMD>>,
}

impl<
    M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    F: CmdNodeTunnelFactory<M, R, W>,
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
    LISTENER: CmdTunnelListener<M, R, W>,
> DefaultCmdNode<M, R, W, F, LEN, CMD, LISTENER>
{
    fn tunnel_not_found(tunnel_id: TunnelId) -> sfo_result::Error<CmdErrorCode> {
        crate::errors::cmd_err!(CmdErrorCode::Failed, "tunnel {:?} not found", tunnel_id)
    }

    pub fn new(listener: LISTENER, factory: F, tunnel_count: u16) -> Arc<Self> {
        let cmd_handler_map = Arc::new(CmdHandlerMap::new());
        let handler_map = cmd_handler_map.clone();
        let resp_waiter = Arc::new(RespWaiter::new());
        let waiter = resp_waiter.clone();
        let write_factory = CmdNodeWriteFactory::<M, R, W, _, LEN, CMD, LISTENER>::new(
            factory,
            listener,
            move |local_id: PeerId,
                  peer_id: PeerId,
                  tunnel_id: TunnelId,
                  header: CmdHeader<LEN, CMD>,
                  body_read: CmdBody| {
                let handler_map = handler_map.clone();
                let waiter = waiter.clone();
                async move {
                    if header.is_resp() && header.seq().is_some() {
                        let resp_id =
                            gen_resp_id(tunnel_id, header.cmd_code(), header.seq().unwrap());
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
        );
        write_factory.start();
        Arc::new(Self {
            tunnel_pool: ClassifiedWorkerPool::new(tunnel_count, write_factory),
            runtime_tunnels: TunnelRuntimeRegistry::new(),
            cmd_handler_map,
        })
    }

    fn tracked_send_guard(
        &self,
        worker_guard: ClassifiedWorkerGuard<
            (PeerId, Option<TunnelId>),
            CommonCmdSend<M, R, W, LEN, CMD>,
            CmdNodeWriteFactory<M, R, W, F, LEN, CMD, LISTENER>,
        >,
    ) -> CmdNodeSendGuard<M, R, W, F, LEN, CMD, LISTENER> {
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

    async fn get_send(
        &self,
        peer_id: PeerId,
    ) -> CmdResult<CmdNodeSendGuard<M, R, W, F, LEN, CMD, LISTENER>> {
        loop {
            let worker_guard = self
                .tunnel_pool
                .get_classified_worker((peer_id.clone(), None))
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
        peer_id: PeerId,
        tunnel_id: TunnelId,
    ) -> CmdResult<CmdNodeSendGuard<M, R, W, F, LEN, CMD, LISTENER>> {
        self.reserve_tunnel(tunnel_id).await?;
        match self
            .tunnel_pool
            .get_classified_worker((peer_id, Some(tunnel_id)))
            .await
        {
            Ok(worker_guard) => Ok(self.tracked_send_guard(worker_guard)),
            Err(_) => {
                self.runtime_tunnels.remove(tunnel_id);
                Err(Self::tunnel_not_found(tunnel_id))
            }
        }
    }
}

pub type CmdNodeSendGuard<M, R, W, F, LEN, CMD, LISTENER> = TrackedSendGuard<
    (PeerId, Option<TunnelId>),
    M,
    CommonCmdSend<M, R, W, LEN, CMD>,
    CmdNodeWriteFactory<M, R, W, F, LEN, CMD, LISTENER>,
>;
#[async_trait::async_trait]
impl<
    M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    F: CmdNodeTunnelFactory<M, R, W>,
    LEN: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + RawFixedBytes
        + Sync
        + Send
        + 'static
        + FromPrimitive
        + ToPrimitive,
    CMD: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + RawFixedBytes
        + Sync
        + Send
        + 'static
        + Eq
        + Hash
        + Debug,
    LISTENER: CmdTunnelListener<M, R, W>,
>
    CmdNode<
        LEN,
        CMD,
        M,
        CommonCmdSend<M, R, W, LEN, CMD>,
        CmdNodeSendGuard<M, R, W, F, LEN, CMD, LISTENER>,
    > for DefaultCmdNode<M, R, W, F, LEN, CMD, LISTENER>
{
    fn register_cmd_handler(&self, cmd: CMD, handler: impl CmdHandler<LEN, CMD>) {
        self.cmd_handler_map.insert(cmd, handler);
    }

    async fn send(&self, peer_id: &PeerId, cmd: CMD, version: u8, body: &[u8]) -> CmdResult<()> {
        let mut send = self.get_send(peer_id.clone()).await?;
        send.send(cmd, version, body).await
    }

    async fn send_with_resp(
        &self,
        peer_id: &PeerId,
        cmd: CMD,
        version: u8,
        body: &[u8],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        let mut send = self.get_send(peer_id.clone()).await?;
        send.send_with_resp(cmd, version, body, timeout).await
    }

    async fn send_parts(
        &self,
        peer_id: &PeerId,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
    ) -> CmdResult<()> {
        let mut send = self.get_send(peer_id.clone()).await?;
        send.send_parts(cmd, version, body).await
    }

    async fn send_parts_with_resp(
        &self,
        peer_id: &PeerId,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        let mut send = self.get_send(peer_id.clone()).await?;
        send.send_parts_with_resp(cmd, version, body, timeout).await
    }

    async fn send_cmd(
        &self,
        peer_id: &PeerId,
        cmd: CMD,
        version: u8,
        body: CmdBody,
    ) -> CmdResult<()> {
        let mut send = self.get_send(peer_id.clone()).await?;
        send.send_cmd(cmd, version, body).await
    }

    async fn send_cmd_with_resp(
        &self,
        peer_id: &PeerId,
        cmd: CMD,
        version: u8,
        body: CmdBody,
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        let mut send = self.get_send(peer_id.clone()).await?;
        send.send_cmd_with_resp(cmd, version, body, timeout).await
    }

    async fn send_by_specify_tunnel(
        &self,
        peer_id: &PeerId,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: &[u8],
    ) -> CmdResult<()> {
        let mut send = self
            .get_send_of_tunnel_id(peer_id.clone(), tunnel_id)
            .await?;
        send.send(cmd, version, body).await
    }

    async fn send_by_specify_tunnel_with_resp(
        &self,
        peer_id: &PeerId,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: &[u8],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        let mut send = self
            .get_send_of_tunnel_id(peer_id.clone(), tunnel_id)
            .await?;
        send.send_with_resp(cmd, version, body, timeout).await
    }

    async fn send_parts_by_specify_tunnel(
        &self,
        peer_id: &PeerId,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
    ) -> CmdResult<()> {
        let mut send = self
            .get_send_of_tunnel_id(peer_id.clone(), tunnel_id)
            .await?;
        send.send_parts(cmd, version, body).await
    }

    async fn send_parts_by_specify_tunnel_with_resp(
        &self,
        peer_id: &PeerId,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        let mut send = self
            .get_send_of_tunnel_id(peer_id.clone(), tunnel_id)
            .await?;
        send.send_parts_with_resp(cmd, version, body, timeout).await
    }

    async fn send_cmd_by_specify_tunnel(
        &self,
        peer_id: &PeerId,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: CmdBody,
    ) -> CmdResult<()> {
        let mut send = self
            .get_send_of_tunnel_id(peer_id.clone(), tunnel_id)
            .await?;
        send.send_cmd(cmd, version, body).await
    }

    async fn send_cmd_by_specify_tunnel_with_resp(
        &self,
        peer_id: &PeerId,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: CmdBody,
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        let mut send = self
            .get_send_of_tunnel_id(peer_id.clone(), tunnel_id)
            .await?;
        send.send_cmd_with_resp(cmd, version, body, timeout).await
    }

    async fn clear_all_tunnel(&self) {
        self.tunnel_pool.clear_all_worker().await;
        self.runtime_tunnels.clear();
    }

    async fn get_send(
        &self,
        peer_id: &PeerId,
        tunnel_id: TunnelId,
    ) -> CmdResult<CmdNodeSendGuard<M, R, W, F, LEN, CMD, LISTENER>> {
        self.get_send_of_tunnel_id(peer_id.clone(), tunnel_id).await
    }
}
