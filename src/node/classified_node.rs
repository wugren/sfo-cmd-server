use crate::client::{
    ClassifiedCmdSend, ClassifiedCmdTunnelRead, ClassifiedCmdTunnelWrite, RespWaiter,
    RespWaiterRef, TrackedSendGuard, TunnelReserveResult, TunnelRuntimeRegistry, gen_resp_id,
};
use crate::cmd::CmdHandlerMap;
use crate::errors::{CmdErrorCode, CmdResult, into_cmd_err};
use crate::node::create_recv_handle;
use crate::server::{CmdTunnelListener, CmdTunnelService};
use crate::{
    ClassifiedCmdNode, CmdBody, CmdHandler, CmdHeader, CmdNode, CmdTunnelMeta, CmdTunnelRead,
    CmdTunnelWrite, PeerId, TunnelId, TunnelIdGenerator, into_pool_err, pool_err,
};
use async_named_locker::ObjectHolder;
use bucky_raw_codec::{RawDecode, RawEncode, RawFixedBytes};
use num::{FromPrimitive, ToPrimitive};
use sfo_pool::{
    ClassifiedWorker, ClassifiedWorkerFactory, ClassifiedWorkerGuard, ClassifiedWorkerPool,
    ClassifiedWorkerPoolRef, PoolErrorCode, PoolResult, WorkerClassification,
};
use sfo_split::Splittable;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::yield_now;

#[derive(Debug, Clone, Eq, Hash)]
pub struct CmdNodeTunnelClassification<C: WorkerClassification> {
    pub peer_id: Option<PeerId>,
    pub tunnel_id: Option<TunnelId>,
    pub classification: Option<C>,
}

impl<C: WorkerClassification> PartialEq for CmdNodeTunnelClassification<C> {
    fn eq(&self, other: &Self) -> bool {
        self.peer_id == other.peer_id
            && self.tunnel_id == other.tunnel_id
            && self.classification == other.classification
    }
}

impl<C, M, R, W, LEN, CMD> ClassifiedWorker<CmdNodeTunnelClassification<C>>
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

    fn is_valid(&self, c: CmdNodeTunnelClassification<C>) -> bool {
        if c.peer_id.is_some() && c.peer_id.as_ref().unwrap() != &self.remote_id {
            return false;
        }

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

    fn classification(&self) -> CmdNodeTunnelClassification<C> {
        CmdNodeTunnelClassification {
            peer_id: self.pool_peer_id.clone(),
            tunnel_id: self.pool_tunnel_id,
            classification: self.pool_classification.clone(),
        }
    }
}

#[async_trait::async_trait]
pub trait ClassifiedCmdNodeTunnelFactory<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
>: Send + Sync + 'static
{
    async fn create_tunnel(
        &self,
        classification: Option<CmdNodeTunnelClassification<C>>,
    ) -> CmdResult<Splittable<R, W>>;
}

struct CmdWriteFactoryImpl<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
    F: ClassifiedCmdNodeTunnelFactory<C, M, R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug + RawFixedBytes,
> {
    tunnel_factory: F,
    cmd_handler: Arc<dyn CmdHandler<LEN, CMD>>,
    tunnel_id_generator: TunnelIdGenerator,
    resp_waiter: RespWaiterRef,
    send_cache: Arc<Mutex<HashMap<PeerId, Vec<ClassifiedCmdSend<C, M, R, W, LEN, CMD>>>>>,
}

impl<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
    F: ClassifiedCmdNodeTunnelFactory<C, M, R, W>,
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
> CmdWriteFactoryImpl<C, M, R, W, F, LEN, CMD>
{
    pub fn new(
        tunnel_factory: F,
        cmd_handler: impl CmdHandler<LEN, CMD>,
        resp_waiter: RespWaiterRef,
    ) -> Self {
        Self {
            tunnel_factory,
            cmd_handler: Arc::new(cmd_handler),
            tunnel_id_generator: TunnelIdGenerator::new(),
            resp_waiter,
            send_cache: Arc::new(Mutex::new(Default::default())),
        }
    }

    pub async fn serve_tunnel(&self, tunnel: Splittable<R, W>) -> CmdResult<()> {
        let peer_id = tunnel.get_remote_peer_id();
        let classification = tunnel.get_classification();
        let tunnel_id = self.tunnel_id_generator.generate();
        let cmd_handler = self.cmd_handler.clone();
        let (reader, writer) = tunnel.split();
        let tunnel_meta = reader.get_tunnel_meta();
        let remote_id = writer.get_remote_peer_id();
        let writer = ObjectHolder::new(writer);
        let recv_handle =
            create_recv_handle::<M, R, W, LEN, CMD>(reader, writer.clone(), tunnel_id, cmd_handler);
        {
            let mut send_cache = self.send_cache.lock().unwrap();
            let send_list = send_cache.entry(peer_id).or_insert(Vec::new());
            send_list.push(ClassifiedCmdSend::new(
                tunnel_id,
                classification.clone(),
                Some(remote_id.clone()),
                Some(tunnel_id),
                Some(classification),
                recv_handle,
                writer,
                self.resp_waiter.clone(),
                remote_id,
                tunnel_meta,
            ));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
    F: ClassifiedCmdNodeTunnelFactory<C, M, R, W>,
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
> ClassifiedWorkerFactory<CmdNodeTunnelClassification<C>, ClassifiedCmdSend<C, M, R, W, LEN, CMD>>
    for CmdWriteFactoryImpl<C, M, R, W, F, LEN, CMD>
{
    async fn create(
        &self,
        c: Option<CmdNodeTunnelClassification<C>>,
    ) -> PoolResult<ClassifiedCmdSend<C, M, R, W, LEN, CMD>> {
        if c.is_some() {
            let classification = c.unwrap();
            if classification.peer_id.is_some() {
                let peer_id = classification.peer_id.clone().unwrap();
                let tunnel_id = classification.tunnel_id;
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
                            let mut send = send_list.remove(send_index);
                            send.set_pool_key(
                                Some(peer_id.clone()),
                                tunnel_id,
                                classification.classification.clone(),
                            );
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
                                let mut send = send_list.pop().unwrap();
                                send.set_pool_key(
                                    Some(peer_id.clone()),
                                    tunnel_id,
                                    classification.classification.clone(),
                                );
                                if send_list.is_empty() {
                                    send_cache.remove(&peer_id);
                                }
                                return Ok(send);
                            }
                        }
                    }
                    let tunnel = self
                        .tunnel_factory
                        .create_tunnel(Some(classification.clone()))
                        .await
                        .map_err(into_pool_err!(PoolErrorCode::Failed))?;
                    let actual_classification = tunnel.get_classification();
                    let pool_peer_id = classification.peer_id.clone();
                    let pool_tunnel_id = classification.tunnel_id;
                    let pool_classification = classification.classification.clone();
                    let tunnel_id = self.tunnel_id_generator.generate();
                    let (recv, write) = tunnel.split();
                    let remote_id = write.get_remote_peer_id();
                    let tunnel_meta = recv.get_tunnel_meta();
                    let write = ObjectHolder::new(write);
                    let cmd_handler = self.cmd_handler.clone();
                    let handle = create_recv_handle::<M, R, W, LEN, CMD>(
                        recv,
                        write.clone(),
                        tunnel_id,
                        cmd_handler,
                    );
                    Ok(ClassifiedCmdSend::new(
                        tunnel_id,
                        actual_classification,
                        pool_peer_id,
                        pool_tunnel_id,
                        pool_classification,
                        handle,
                        write,
                        self.resp_waiter.clone(),
                        remote_id,
                        tunnel_meta,
                    ))
                }
            } else {
                if classification.tunnel_id.is_some() {
                    Err(pool_err!(
                        PoolErrorCode::Failed,
                        "must set peer id when set tunnel id"
                    ))
                } else {
                    let tunnel = self
                        .tunnel_factory
                        .create_tunnel(Some(classification.clone()))
                        .await
                        .map_err(into_pool_err!(PoolErrorCode::Failed))?;
                    let actual_classification = tunnel.get_classification();
                    let pool_peer_id = classification.peer_id.clone();
                    let pool_tunnel_id = classification.tunnel_id;
                    let pool_classification = classification.classification.clone();
                    let tunnel_id = self.tunnel_id_generator.generate();
                    let (recv, write) = tunnel.split();
                    let remote_id = write.get_remote_peer_id();
                    let tunnel_meta = write.get_tunnel_meta();
                    let write = ObjectHolder::new(write);
                    let cmd_handler = self.cmd_handler.clone();
                    let handle = create_recv_handle::<M, R, W, LEN, CMD>(
                        recv,
                        write.clone(),
                        tunnel_id,
                        cmd_handler,
                    );
                    Ok(ClassifiedCmdSend::new(
                        tunnel_id,
                        actual_classification,
                        pool_peer_id,
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
        } else {
            Err(pool_err!(PoolErrorCode::Failed, "peer id is none"))
        }
    }
}

pub struct ClassifiedCmdNodeWriteFactory<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
    F: ClassifiedCmdNodeTunnelFactory<C, M, R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug + RawFixedBytes,
> {
    inner: Arc<CmdWriteFactoryImpl<C, M, R, W, F, LEN, CMD>>,
}

impl<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
    F: ClassifiedCmdNodeTunnelFactory<C, M, R, W>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug + RawFixedBytes,
> Clone for ClassifiedCmdNodeWriteFactory<C, M, R, W, F, LEN, CMD>
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
    F: ClassifiedCmdNodeTunnelFactory<C, M, R, W>,
    LEN: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + Send
        + Sync
        + 'static
        + FromPrimitive
        + ToPrimitive
        + RawFixedBytes
        + RawFixedBytes,
    CMD: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + Send
        + Sync
        + 'static
        + Debug
        + RawFixedBytes
        + RawFixedBytes,
> ClassifiedCmdNodeWriteFactory<C, M, R, W, F, LEN, CMD>
{
    pub(crate) fn new(
        tunnel_factory: F,
        cmd_handler: impl CmdHandler<LEN, CMD>,
        resp_waiter: RespWaiterRef,
    ) -> Self {
        Self {
            inner: Arc::new(CmdWriteFactoryImpl::new(
                tunnel_factory,
                cmd_handler,
                resp_waiter,
            )),
        }
    }

    pub async fn serve_tunnel(&self, tunnel: Splittable<R, W>) -> CmdResult<()> {
        self.inner.serve_tunnel(tunnel).await
    }
}

#[async_trait::async_trait]
impl<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
    F: ClassifiedCmdNodeTunnelFactory<C, M, R, W>,
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
> ClassifiedWorkerFactory<CmdNodeTunnelClassification<C>, ClassifiedCmdSend<C, M, R, W, LEN, CMD>>
    for ClassifiedCmdNodeWriteFactory<C, M, R, W, F, LEN, CMD>
{
    async fn create(
        &self,
        c: Option<CmdNodeTunnelClassification<C>>,
    ) -> PoolResult<ClassifiedCmdSend<C, M, R, W, LEN, CMD>> {
        self.inner.create(c).await
    }
}

pub struct DefaultClassifiedCmdNodeService<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
    F: ClassifiedCmdNodeTunnelFactory<C, M, R, W>,
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
        CmdNodeTunnelClassification<C>,
        ClassifiedCmdSend<C, M, R, W, LEN, CMD>,
        ClassifiedCmdNodeWriteFactory<C, M, R, W, F, LEN, CMD>,
    >,
    write_factory: ClassifiedCmdNodeWriteFactory<C, M, R, W, F, LEN, CMD>,
    runtime_tunnels: Arc<TunnelRuntimeRegistry>,
    cmd_handler_map: Arc<CmdHandlerMap<LEN, CMD>>,
}

impl<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
    F: ClassifiedCmdNodeTunnelFactory<C, M, R, W>,
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
> DefaultClassifiedCmdNodeService<C, M, R, W, F, LEN, CMD>
{
    fn tunnel_not_found(tunnel_id: TunnelId) -> sfo_result::Error<CmdErrorCode> {
        crate::errors::cmd_err!(CmdErrorCode::Failed, "tunnel {:?} not found", tunnel_id)
    }

    pub fn new(factory: F, tunnel_count: u16) -> Arc<Self> {
        let cmd_handler_map = Arc::new(CmdHandlerMap::new());
        let handler_map = cmd_handler_map.clone();
        let resp_waiter = Arc::new(RespWaiter::new());
        let waiter = resp_waiter.clone();
        let write_factory = ClassifiedCmdNodeWriteFactory::<C, M, R, W, _, LEN, CMD>::new(
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
        Arc::new(Self {
            tunnel_pool: ClassifiedWorkerPool::new(tunnel_count, write_factory.clone()),
            write_factory,
            runtime_tunnels: TunnelRuntimeRegistry::new(),
            cmd_handler_map,
        })
    }

    pub async fn serve_tunnel(&self, tunnel: Splittable<R, W>) -> CmdResult<()> {
        self.write_factory.serve_tunnel(tunnel).await
    }

    fn tracked_send_guard(
        &self,
        worker_guard: ClassifiedWorkerGuard<
            CmdNodeTunnelClassification<C>,
            ClassifiedCmdSend<C, M, R, W, LEN, CMD>,
            ClassifiedCmdNodeWriteFactory<C, M, R, W, F, LEN, CMD>,
        >,
    ) -> ClassifiedCmdNodeSendGuard<C, M, R, W, F, LEN, CMD> {
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
    ) -> CmdResult<ClassifiedCmdNodeSendGuard<C, M, R, W, F, LEN, CMD>> {
        loop {
            let worker_guard = self
                .tunnel_pool
                .get_classified_worker(CmdNodeTunnelClassification {
                    peer_id: Some(peer_id.clone()),
                    tunnel_id: None,
                    classification: None,
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

    async fn get_send_of_tunnel_id(
        &self,
        peer_id: PeerId,
        tunnel_id: TunnelId,
    ) -> CmdResult<ClassifiedCmdNodeSendGuard<C, M, R, W, F, LEN, CMD>> {
        self.reserve_tunnel(tunnel_id).await?;
        match self
            .tunnel_pool
            .get_classified_worker(CmdNodeTunnelClassification {
                peer_id: Some(peer_id),
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
    ) -> CmdResult<ClassifiedCmdNodeSendGuard<C, M, R, W, F, LEN, CMD>> {
        loop {
            let worker_guard = self
                .tunnel_pool
                .get_classified_worker(CmdNodeTunnelClassification {
                    peer_id: None,
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

    async fn get_peer_classified_send(
        &self,
        peer_id: PeerId,
        classification: C,
    ) -> CmdResult<ClassifiedCmdNodeSendGuard<C, M, R, W, F, LEN, CMD>> {
        loop {
            let worker_guard = self
                .tunnel_pool
                .get_classified_worker(CmdNodeTunnelClassification {
                    peer_id: Some(peer_id.clone()),
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

pub type ClassifiedCmdNodeSendGuard<C, M, R, W, F, LEN, CMD> = TrackedSendGuard<
    CmdNodeTunnelClassification<C>,
    M,
    ClassifiedCmdSend<C, M, R, W, LEN, CMD>,
    ClassifiedCmdNodeWriteFactory<C, M, R, W, F, LEN, CMD>,
>;
#[async_trait::async_trait]
impl<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
    F: ClassifiedCmdNodeTunnelFactory<C, M, R, W>,
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
    CmdNode<
        LEN,
        CMD,
        M,
        ClassifiedCmdSend<C, M, R, W, LEN, CMD>,
        ClassifiedCmdNodeSendGuard<C, M, R, W, F, LEN, CMD>,
    > for DefaultClassifiedCmdNodeService<C, M, R, W, F, LEN, CMD>
{
    fn register_cmd_handler(&self, cmd: CMD, handler: impl CmdHandler<LEN, CMD>) {
        self.cmd_handler_map.insert(cmd, handler)
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
    ) -> CmdResult<ClassifiedCmdNodeSendGuard<C, M, R, W, F, LEN, CMD>> {
        self.get_send_of_tunnel_id(peer_id.clone(), tunnel_id).await
    }
}

#[async_trait::async_trait]
impl<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
    F: ClassifiedCmdNodeTunnelFactory<C, M, R, W>,
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
    ClassifiedCmdNode<
        LEN,
        CMD,
        C,
        M,
        ClassifiedCmdSend<C, M, R, W, LEN, CMD>,
        ClassifiedCmdNodeSendGuard<C, M, R, W, F, LEN, CMD>,
    > for DefaultClassifiedCmdNodeService<C, M, R, W, F, LEN, CMD>
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

    async fn send_by_peer_classified_tunnel(
        &self,
        peer_id: &PeerId,
        classification: C,
        cmd: CMD,
        version: u8,
        body: &[u8],
    ) -> CmdResult<()> {
        let mut send = self
            .get_peer_classified_send(peer_id.clone(), classification)
            .await?;
        send.send(cmd, version, body).await
    }

    async fn send_by_peer_classified_tunnel_with_resp(
        &self,
        peer_id: &PeerId,
        classification: C,
        cmd: CMD,
        version: u8,
        body: &[u8],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        let mut send = self
            .get_peer_classified_send(peer_id.clone(), classification)
            .await?;
        send.send_with_resp(cmd, version, body, timeout).await
    }

    async fn send_parts_by_peer_classified_tunnel(
        &self,
        peer_id: &PeerId,
        classification: C,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
    ) -> CmdResult<()> {
        let mut send = self
            .get_peer_classified_send(peer_id.clone(), classification)
            .await?;
        send.send_parts(cmd, version, body).await
    }

    async fn send_parts_by_peer_classified_tunnel_with_resp(
        &self,
        peer_id: &PeerId,
        classification: C,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        let mut send = self
            .get_peer_classified_send(peer_id.clone(), classification)
            .await?;
        send.send_parts_with_resp(cmd, version, body, timeout).await
    }

    async fn send_cmd_by_peer_classified_tunnel(
        &self,
        peer_id: &PeerId,
        classification: C,
        cmd: CMD,
        version: u8,
        body: CmdBody,
    ) -> CmdResult<()> {
        let mut send = self
            .get_peer_classified_send(peer_id.clone(), classification)
            .await?;
        send.send_cmd(cmd, version, body).await
    }

    async fn send_cmd_by_peer_classified_tunnel_with_resp(
        &self,
        peer_id: &PeerId,
        classification: C,
        cmd: CMD,
        version: u8,
        body: CmdBody,
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        let mut send = self
            .get_peer_classified_send(peer_id.clone(), classification)
            .await?;
        send.send_cmd_with_resp(cmd, version, body, timeout).await
    }

    async fn find_tunnel_id_by_classified(&self, classification: C) -> CmdResult<TunnelId> {
        let send = self.get_classified_send(classification).await?;
        Ok(send.get_tunnel_id())
    }

    async fn find_tunnel_id_by_peer_classified(
        &self,
        peer_id: &PeerId,
        classification: C,
    ) -> CmdResult<TunnelId> {
        let send = self
            .get_peer_classified_send(peer_id.clone(), classification)
            .await?;
        Ok(send.get_tunnel_id())
    }

    async fn get_send_by_classified(
        &self,
        classification: C,
    ) -> CmdResult<ClassifiedCmdNodeSendGuard<C, M, R, W, F, LEN, CMD>> {
        self.get_classified_send(classification).await
    }

    async fn get_send_by_peer_classified(
        &self,
        peer_id: &PeerId,
        classification: C,
    ) -> CmdResult<ClassifiedCmdNodeSendGuard<C, M, R, W, F, LEN, CMD>> {
        self.get_peer_classified_send(peer_id.clone(), classification)
            .await
    }
}

#[async_trait::async_trait]
impl<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
    F: ClassifiedCmdNodeTunnelFactory<C, M, R, W>,
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
> CmdTunnelService<M, R, W> for DefaultClassifiedCmdNodeService<C, M, R, W, F, LEN, CMD>
{
    async fn handle_tunnel(&self, tunnel: Splittable<R, W>) -> CmdResult<()> {
        self.serve_tunnel(tunnel).await
    }
}

pub struct DefaultClassifiedCmdNodeIncoming<
    M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    LISTENER,
> {
    tunnel_listener: LISTENER,
    tunnel_service: Arc<dyn CmdTunnelService<M, R, W>>,
    started: AtomicBool,
    _p: PhantomData<fn() -> (M, R, W)>,
}

impl<
    M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    LISTENER: CmdTunnelListener<M, R, W>,
> DefaultClassifiedCmdNodeIncoming<M, R, W, LISTENER>
{
    pub fn new(
        tunnel_listener: LISTENER,
        tunnel_service: Arc<dyn CmdTunnelService<M, R, W>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            tunnel_listener,
            tunnel_service,
            started: AtomicBool::new(false),
            _p: PhantomData,
        })
    }

    pub fn start(self: &Arc<Self>) {
        if self
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let this = self.clone();
        tokio::spawn(async move {
            if let Err(e) = this.run().await {
                log::error!("classified cmd node error: {:?}", e);
            }
        });
    }

    pub async fn run(&self) -> CmdResult<()> {
        loop {
            let tunnel = self.tunnel_listener.accept().await?;
            let tunnel_service = self.tunnel_service.clone();
            tokio::spawn(async move {
                if let Err(e) = tunnel_service.handle_tunnel(tunnel).await {
                    log::error!("peer connection error: {:?}", e);
                }
            });
        }
    }
}

pub struct DefaultClassifiedCmdNode<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
    F: ClassifiedCmdNodeTunnelFactory<C, M, R, W>,
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
    LISTENER,
> {
    incoming: Arc<DefaultClassifiedCmdNodeIncoming<M, R, W, LISTENER>>,
    service: Arc<DefaultClassifiedCmdNodeService<C, M, R, W, F, LEN, CMD>>,
}

impl<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
    F: ClassifiedCmdNodeTunnelFactory<C, M, R, W>,
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
> DefaultClassifiedCmdNode<C, M, R, W, F, LEN, CMD, LISTENER>
{
    pub fn new(listener: LISTENER, factory: F, tunnel_count: u16) -> Arc<Self> {
        let service = DefaultClassifiedCmdNodeService::new(factory, tunnel_count);
        let incoming = DefaultClassifiedCmdNodeIncoming::new(listener, service.clone());
        incoming.start();
        Arc::new(Self { incoming, service })
    }

    pub fn incoming(&self) -> Arc<DefaultClassifiedCmdNodeIncoming<M, R, W, LISTENER>> {
        self.incoming.clone()
    }

    pub fn service(&self) -> Arc<DefaultClassifiedCmdNodeService<C, M, R, W, F, LEN, CMD>> {
        self.service.clone()
    }

    pub async fn serve_tunnel(&self, tunnel: Splittable<R, W>) -> CmdResult<()> {
        self.service.serve_tunnel(tunnel).await
    }

    pub fn start(self: &Arc<Self>) {
        self.incoming.start();
    }
}

impl<C, M, R, W, F, LEN, CMD, LISTENER> Deref
    for DefaultClassifiedCmdNode<C, M, R, W, F, LEN, CMD, LISTENER>
where
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
    F: ClassifiedCmdNodeTunnelFactory<C, M, R, W>,
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
{
    type Target = DefaultClassifiedCmdNodeService<C, M, R, W, F, LEN, CMD>;

    fn deref(&self) -> &Self::Target {
        self.service.as_ref()
    }
}

#[async_trait::async_trait]
impl<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
    F: ClassifiedCmdNodeTunnelFactory<C, M, R, W>,
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
>
    CmdNode<
        LEN,
        CMD,
        M,
        ClassifiedCmdSend<C, M, R, W, LEN, CMD>,
        ClassifiedCmdNodeSendGuard<C, M, R, W, F, LEN, CMD>,
    > for DefaultClassifiedCmdNode<C, M, R, W, F, LEN, CMD, LISTENER>
{
    fn register_cmd_handler(&self, cmd: CMD, handler: impl CmdHandler<LEN, CMD>) {
        self.service.register_cmd_handler(cmd, handler);
    }

    async fn send(&self, peer_id: &PeerId, cmd: CMD, version: u8, body: &[u8]) -> CmdResult<()> {
        self.service.send(peer_id, cmd, version, body).await
    }

    async fn send_with_resp(
        &self,
        peer_id: &PeerId,
        cmd: CMD,
        version: u8,
        body: &[u8],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        self.service
            .send_with_resp(peer_id, cmd, version, body, timeout)
            .await
    }

    async fn send_parts(
        &self,
        peer_id: &PeerId,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
    ) -> CmdResult<()> {
        self.service.send_parts(peer_id, cmd, version, body).await
    }

    async fn send_parts_with_resp(
        &self,
        peer_id: &PeerId,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        self.service
            .send_parts_with_resp(peer_id, cmd, version, body, timeout)
            .await
    }

    async fn send_cmd(
        &self,
        peer_id: &PeerId,
        cmd: CMD,
        version: u8,
        body: CmdBody,
    ) -> CmdResult<()> {
        self.service.send_cmd(peer_id, cmd, version, body).await
    }

    async fn send_cmd_with_resp(
        &self,
        peer_id: &PeerId,
        cmd: CMD,
        version: u8,
        body: CmdBody,
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        self.service
            .send_cmd_with_resp(peer_id, cmd, version, body, timeout)
            .await
    }

    async fn send_by_specify_tunnel(
        &self,
        peer_id: &PeerId,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: &[u8],
    ) -> CmdResult<()> {
        self.service
            .send_by_specify_tunnel(peer_id, tunnel_id, cmd, version, body)
            .await
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
        self.service
            .send_by_specify_tunnel_with_resp(peer_id, tunnel_id, cmd, version, body, timeout)
            .await
    }

    async fn send_parts_by_specify_tunnel(
        &self,
        peer_id: &PeerId,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
    ) -> CmdResult<()> {
        self.service
            .send_parts_by_specify_tunnel(peer_id, tunnel_id, cmd, version, body)
            .await
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
        self.service
            .send_parts_by_specify_tunnel_with_resp(peer_id, tunnel_id, cmd, version, body, timeout)
            .await
    }

    async fn send_cmd_by_specify_tunnel(
        &self,
        peer_id: &PeerId,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: CmdBody,
    ) -> CmdResult<()> {
        self.service
            .send_cmd_by_specify_tunnel(peer_id, tunnel_id, cmd, version, body)
            .await
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
        self.service
            .send_cmd_by_specify_tunnel_with_resp(peer_id, tunnel_id, cmd, version, body, timeout)
            .await
    }

    async fn clear_all_tunnel(&self) {
        self.service.clear_all_tunnel().await;
    }

    async fn get_send(
        &self,
        peer_id: &PeerId,
        tunnel_id: TunnelId,
    ) -> CmdResult<ClassifiedCmdNodeSendGuard<C, M, R, W, F, LEN, CMD>> {
        self.service
            .get_send_of_tunnel_id(peer_id.clone(), tunnel_id)
            .await
    }
}

#[async_trait::async_trait]
impl<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    R: ClassifiedCmdTunnelRead<C, M>,
    W: ClassifiedCmdTunnelWrite<C, M>,
    F: ClassifiedCmdNodeTunnelFactory<C, M, R, W>,
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
>
    ClassifiedCmdNode<
        LEN,
        CMD,
        C,
        M,
        ClassifiedCmdSend<C, M, R, W, LEN, CMD>,
        ClassifiedCmdNodeSendGuard<C, M, R, W, F, LEN, CMD>,
    > for DefaultClassifiedCmdNode<C, M, R, W, F, LEN, CMD, LISTENER>
{
    async fn send_by_classified_tunnel(
        &self,
        classification: C,
        cmd: CMD,
        version: u8,
        body: &[u8],
    ) -> CmdResult<()> {
        self.service
            .send_by_classified_tunnel(classification, cmd, version, body)
            .await
    }

    async fn send_by_classified_tunnel_with_resp(
        &self,
        classification: C,
        cmd: CMD,
        version: u8,
        body: &[u8],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        self.service
            .send_by_classified_tunnel_with_resp(classification, cmd, version, body, timeout)
            .await
    }

    async fn send_parts_by_classified_tunnel(
        &self,
        classification: C,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
    ) -> CmdResult<()> {
        self.service
            .send_parts_by_classified_tunnel(classification, cmd, version, body)
            .await
    }

    async fn send_parts_by_classified_tunnel_with_resp(
        &self,
        classification: C,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        self.service
            .send_parts_by_classified_tunnel_with_resp(classification, cmd, version, body, timeout)
            .await
    }

    async fn send_cmd_by_classified_tunnel(
        &self,
        classification: C,
        cmd: CMD,
        version: u8,
        body: CmdBody,
    ) -> CmdResult<()> {
        self.service
            .send_cmd_by_classified_tunnel(classification, cmd, version, body)
            .await
    }

    async fn send_cmd_by_classified_tunnel_with_resp(
        &self,
        classification: C,
        cmd: CMD,
        version: u8,
        body: CmdBody,
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        self.service
            .send_cmd_by_classified_tunnel_with_resp(classification, cmd, version, body, timeout)
            .await
    }

    async fn send_by_peer_classified_tunnel(
        &self,
        peer_id: &PeerId,
        classification: C,
        cmd: CMD,
        version: u8,
        body: &[u8],
    ) -> CmdResult<()> {
        self.service
            .send_by_peer_classified_tunnel(peer_id, classification, cmd, version, body)
            .await
    }

    async fn send_by_peer_classified_tunnel_with_resp(
        &self,
        peer_id: &PeerId,
        classification: C,
        cmd: CMD,
        version: u8,
        body: &[u8],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        self.service
            .send_by_peer_classified_tunnel_with_resp(
                peer_id,
                classification,
                cmd,
                version,
                body,
                timeout,
            )
            .await
    }

    async fn send_parts_by_peer_classified_tunnel(
        &self,
        peer_id: &PeerId,
        classification: C,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
    ) -> CmdResult<()> {
        self.service
            .send_parts_by_peer_classified_tunnel(peer_id, classification, cmd, version, body)
            .await
    }

    async fn send_parts_by_peer_classified_tunnel_with_resp(
        &self,
        peer_id: &PeerId,
        classification: C,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        self.service
            .send_parts_by_peer_classified_tunnel_with_resp(
                peer_id,
                classification,
                cmd,
                version,
                body,
                timeout,
            )
            .await
    }

    async fn send_cmd_by_peer_classified_tunnel(
        &self,
        peer_id: &PeerId,
        classification: C,
        cmd: CMD,
        version: u8,
        body: CmdBody,
    ) -> CmdResult<()> {
        self.service
            .send_cmd_by_peer_classified_tunnel(peer_id, classification, cmd, version, body)
            .await
    }

    async fn send_cmd_by_peer_classified_tunnel_with_resp(
        &self,
        peer_id: &PeerId,
        classification: C,
        cmd: CMD,
        version: u8,
        body: CmdBody,
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        self.service
            .send_cmd_by_peer_classified_tunnel_with_resp(
                peer_id,
                classification,
                cmd,
                version,
                body,
                timeout,
            )
            .await
    }

    async fn find_tunnel_id_by_classified(&self, classification: C) -> CmdResult<TunnelId> {
        self.service
            .find_tunnel_id_by_classified(classification)
            .await
    }

    async fn find_tunnel_id_by_peer_classified(
        &self,
        peer_id: &PeerId,
        classification: C,
    ) -> CmdResult<TunnelId> {
        self.service
            .find_tunnel_id_by_peer_classified(peer_id, classification)
            .await
    }

    async fn get_send_by_classified(
        &self,
        classification: C,
    ) -> CmdResult<ClassifiedCmdNodeSendGuard<C, M, R, W, F, LEN, CMD>> {
        self.service.get_send_by_classified(classification).await
    }

    async fn get_send_by_peer_classified(
        &self,
        peer_id: &PeerId,
        classification: C,
    ) -> CmdResult<ClassifiedCmdNodeSendGuard<C, M, R, W, F, LEN, CMD>> {
        self.service
            .get_send_by_peer_classified(peer_id, classification)
            .await
    }
}
