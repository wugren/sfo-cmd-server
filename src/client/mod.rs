mod client;

use bucky_raw_codec::{RawDecode, RawEncode, RawFixedBytes};
use callback_result::CallbackWaiter;
pub use client::*;
use num::{FromPrimitive, ToPrimitive};
use sfo_pool::{
    ClassifiedWorker, ClassifiedWorkerFactory, ClassifiedWorkerGuard, WorkerClassification,
};
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

mod classified_client;
pub use classified_client::*;

use crate::errors::CmdResult;
use crate::{CmdBody, CmdHandler, CmdTunnelMeta, PeerId, TunnelId};

pub trait CmdSend<M: CmdTunnelMeta>: Send + 'static {
    fn get_tunnel_meta(&self) -> Option<Arc<M>>;
    fn get_remote_peer_id(&self) -> PeerId;
}

pub trait SendGuard<M: CmdTunnelMeta, S: CmdSend<M>>: Send + 'static + Deref<Target = S> {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TunnelBorrowState {
    Idle,
    Borrowed,
}

struct TunnelRuntimeEntry {
    state: TunnelBorrowState,
    waiters: VecDeque<Arc<Notify>>,
}

pub(crate) enum TunnelReserveResult {
    Acquired,
    Wait(Arc<Notify>),
    Missing,
}

#[derive(Default)]
pub(crate) struct TunnelRuntimeRegistry {
    state: Mutex<HashMap<TunnelId, TunnelRuntimeEntry>>,
}

impl TunnelRuntimeRegistry {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub(crate) fn reserve_existing(&self, tunnel_id: TunnelId) -> TunnelReserveResult {
        let mut state = self.state.lock().unwrap();
        match state.get_mut(&tunnel_id) {
            Some(entry) => match entry.state {
                TunnelBorrowState::Idle => {
                    entry.state = TunnelBorrowState::Borrowed;
                    TunnelReserveResult::Acquired
                }
                TunnelBorrowState::Borrowed => {
                    let notify = Arc::new(Notify::new());
                    entry.waiters.push_back(notify.clone());
                    TunnelReserveResult::Wait(notify)
                }
            },
            None => TunnelReserveResult::Missing,
        }
    }

    pub(crate) fn mark_borrowed(&self, tunnel_id: TunnelId) -> bool {
        let mut state = self.state.lock().unwrap();
        match state.get_mut(&tunnel_id) {
            Some(entry) => match entry.state {
                TunnelBorrowState::Idle => {
                    entry.state = TunnelBorrowState::Borrowed;
                    true
                }
                TunnelBorrowState::Borrowed => false,
            },
            None => {
                state.insert(
                    tunnel_id,
                    TunnelRuntimeEntry {
                        state: TunnelBorrowState::Borrowed,
                        waiters: VecDeque::new(),
                    },
                );
                true
            }
        }
    }

    pub(crate) fn remove(&self, tunnel_id: TunnelId) {
        let waiters = {
            let mut state = self.state.lock().unwrap();
            state
                .remove(&tunnel_id)
                .map(|entry| entry.waiters.into_iter().collect::<Vec<_>>())
                .unwrap_or_default()
        };
        for waiter in waiters {
            waiter.notify_one();
        }
    }

    pub(crate) fn release(&self, tunnel_id: TunnelId, alive: bool) {
        let (waiter, waiters) = {
            let mut state = self.state.lock().unwrap();
            match state.get_mut(&tunnel_id) {
                Some(entry) if alive => {
                    entry.state = TunnelBorrowState::Idle;
                    (entry.waiters.pop_front(), Vec::new())
                }
                Some(_) => {
                    let entry = state.remove(&tunnel_id).unwrap();
                    (None, entry.waiters.into_iter().collect::<Vec<_>>())
                }
                None => (None, Vec::new()),
            }
        };
        if let Some(waiter) = waiter {
            waiter.notify_one();
        }
        for waiter in waiters {
            waiter.notify_one();
        }
    }

    pub(crate) fn clear(&self) {
        let waiters = {
            let mut state = self.state.lock().unwrap();
            state
                .drain()
                .flat_map(|(_, entry)| entry.waiters.into_iter())
                .collect::<Vec<_>>()
        };
        for waiter in waiters {
            waiter.notify_one();
        }
    }
}

pub struct TrackedSendGuard<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    S: ClassifiedWorker<C> + CmdSend<M>,
    F: ClassifiedWorkerFactory<C, S>,
> {
    worker_guard: Option<ClassifiedWorkerGuard<C, S, F>>,
    runtime_tunnels: Arc<TunnelRuntimeRegistry>,
    tunnel_id: TunnelId,
    _p: PhantomData<M>,
}

impl<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    S: ClassifiedWorker<C> + CmdSend<M>,
    F: ClassifiedWorkerFactory<C, S>,
> TrackedSendGuard<C, M, S, F>
{
    pub(crate) fn new(
        worker_guard: ClassifiedWorkerGuard<C, S, F>,
        runtime_tunnels: Arc<TunnelRuntimeRegistry>,
        tunnel_id: TunnelId,
    ) -> Self {
        Self {
            worker_guard: Some(worker_guard),
            runtime_tunnels,
            tunnel_id,
            _p: PhantomData,
        }
    }
}

impl<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    S: ClassifiedWorker<C> + CmdSend<M>,
    F: ClassifiedWorkerFactory<C, S>,
> Deref for TrackedSendGuard<C, M, S, F>
{
    type Target = S;

    fn deref(&self) -> &Self::Target {
        self.worker_guard.as_ref().unwrap().deref()
    }
}

impl<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    S: ClassifiedWorker<C> + CmdSend<M>,
    F: ClassifiedWorkerFactory<C, S>,
> DerefMut for TrackedSendGuard<C, M, S, F>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.worker_guard.as_mut().unwrap().deref_mut()
    }
}

impl<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    S: ClassifiedWorker<C> + CmdSend<M>,
    F: ClassifiedWorkerFactory<C, S>,
> Drop for TrackedSendGuard<C, M, S, F>
{
    fn drop(&mut self) {
        if let Some(worker_guard) = self.worker_guard.take() {
            let alive = worker_guard.is_work();
            drop(worker_guard);
            self.runtime_tunnels.release(self.tunnel_id, alive);
        }
    }
}

impl<
    C: WorkerClassification,
    M: CmdTunnelMeta,
    S: ClassifiedWorker<C> + CmdSend<M>,
    F: ClassifiedWorkerFactory<C, S>,
> SendGuard<M, S> for TrackedSendGuard<C, M, S, F>
{
}

#[async_trait::async_trait]
pub trait CmdClient<
    LEN: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + RawFixedBytes
        + Sync
        + Send
        + 'static
        + FromPrimitive
        + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send + 'static + Eq + Hash,
    M: CmdTunnelMeta,
    S: CmdSend<M>,
    G: SendGuard<M, S>,
>: Send + Sync + 'static
{
    fn register_cmd_handler(&self, cmd: CMD, handler: impl CmdHandler<LEN, CMD>);
    async fn send(&self, cmd: CMD, version: u8, body: &[u8]) -> CmdResult<()>;
    async fn send_with_resp(
        &self,
        cmd: CMD,
        version: u8,
        body: &[u8],
        timeout: Duration,
    ) -> CmdResult<CmdBody>;
    async fn send_parts(&self, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()>;
    async fn send_parts_with_resp(
        &self,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
        timeout: Duration,
    ) -> CmdResult<CmdBody>;
    #[deprecated(note = "use send_parts instead")]
    async fn send2(&self, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()> {
        self.send_parts(cmd, version, body).await
    }
    #[deprecated(note = "use send_parts_with_resp instead")]
    async fn send2_with_resp(
        &self,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        self.send_parts_with_resp(cmd, version, body, timeout).await
    }
    async fn send_cmd(&self, cmd: CMD, version: u8, body: CmdBody) -> CmdResult<()>;
    async fn send_cmd_with_resp(
        &self,
        cmd: CMD,
        version: u8,
        body: CmdBody,
        timeout: Duration,
    ) -> CmdResult<CmdBody>;
    async fn send_by_specify_tunnel(
        &self,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: &[u8],
    ) -> CmdResult<()>;
    async fn send_by_specify_tunnel_with_resp(
        &self,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: &[u8],
        timeout: Duration,
    ) -> CmdResult<CmdBody>;
    async fn send_parts_by_specify_tunnel(
        &self,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
    ) -> CmdResult<()>;
    async fn send_parts_by_specify_tunnel_with_resp(
        &self,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
        timeout: Duration,
    ) -> CmdResult<CmdBody>;
    #[deprecated(note = "use send_parts_by_specify_tunnel instead")]
    async fn send2_by_specify_tunnel(
        &self,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
    ) -> CmdResult<()> {
        self.send_parts_by_specify_tunnel(tunnel_id, cmd, version, body)
            .await
    }
    #[deprecated(note = "use send_parts_by_specify_tunnel_with_resp instead")]
    async fn send2_by_specify_tunnel_with_resp(
        &self,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        self.send_parts_by_specify_tunnel_with_resp(tunnel_id, cmd, version, body, timeout)
            .await
    }
    async fn send_cmd_by_specify_tunnel(
        &self,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: CmdBody,
    ) -> CmdResult<()>;
    async fn send_cmd_by_specify_tunnel_with_resp(
        &self,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: CmdBody,
        timeout: Duration,
    ) -> CmdResult<CmdBody>;
    async fn clear_all_tunnel(&self);
    async fn get_send(&self, tunnel_id: TunnelId) -> CmdResult<G>;
}

#[async_trait::async_trait]
pub trait ClassifiedCmdClient<
    LEN: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + RawFixedBytes
        + Sync
        + Send
        + 'static
        + FromPrimitive
        + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send + 'static + Eq + Hash,
    C: WorkerClassification,
    M: CmdTunnelMeta,
    S: CmdSend<M>,
    G: SendGuard<M, S>,
>: CmdClient<LEN, CMD, M, S, G>
{
    async fn send_by_classified_tunnel(
        &self,
        classification: C,
        cmd: CMD,
        version: u8,
        body: &[u8],
    ) -> CmdResult<()>;
    async fn send_by_classified_tunnel_with_resp(
        &self,
        classification: C,
        cmd: CMD,
        version: u8,
        body: &[u8],
        timeout: Duration,
    ) -> CmdResult<CmdBody>;
    async fn send_parts_by_classified_tunnel(
        &self,
        classification: C,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
    ) -> CmdResult<()>;
    async fn send_parts_by_classified_tunnel_with_resp(
        &self,
        classification: C,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
        timeout: Duration,
    ) -> CmdResult<CmdBody>;
    #[deprecated(note = "use send_parts_by_classified_tunnel instead")]
    async fn send2_by_classified_tunnel(
        &self,
        classification: C,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
    ) -> CmdResult<()> {
        self.send_parts_by_classified_tunnel(classification, cmd, version, body)
            .await
    }
    #[deprecated(note = "use send_parts_by_classified_tunnel_with_resp instead")]
    async fn send2_by_classified_tunnel_with_resp(
        &self,
        classification: C,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
        timeout: Duration,
    ) -> CmdResult<CmdBody> {
        self.send_parts_by_classified_tunnel_with_resp(classification, cmd, version, body, timeout)
            .await
    }
    async fn send_cmd_by_classified_tunnel(
        &self,
        classification: C,
        cmd: CMD,
        version: u8,
        body: CmdBody,
    ) -> CmdResult<()>;
    async fn send_cmd_by_classified_tunnel_with_resp(
        &self,
        classification: C,
        cmd: CMD,
        version: u8,
        body: CmdBody,
        timeout: Duration,
    ) -> CmdResult<CmdBody>;
    async fn find_tunnel_id_by_classified(&self, classification: C) -> CmdResult<TunnelId>;
    async fn get_send_by_classified(&self, classification: C) -> CmdResult<G>;
}

pub(crate) type RespWaiter = CallbackWaiter<u128, CmdBody>;
pub(crate) type RespWaiterRef = Arc<RespWaiter>;

pub(crate) fn gen_resp_id<
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send + 'static,
>(
    tunnel_id: TunnelId,
    cmd: CMD,
    seq: u32,
) -> u128 {
    let cmd_buf = cmd.raw_encode_to_buffer().unwrap();
    let mut cmd = cmd_buf.len() as u64;
    for chunk in cmd_buf.chunks(8) {
        let mut buf = [0u8; 8];
        buf[..chunk.len()].copy_from_slice(chunk);
        cmd = cmd.rotate_left(13) ^ u64::from_be_bytes(buf);
    }
    ((tunnel_id.value() as u128) << 96) | ((seq as u128) << 64) | (cmd as u128)
}

pub(crate) fn gen_seq() -> u32 {
    rand::random::<u32>()
}

#[cfg(test)]
mod tests {
    use super::gen_resp_id;
    use crate::TunnelId;

    #[test]
    fn resp_id_changes_with_seq() {
        let id1 = gen_resp_id(TunnelId::from(7), 0x11u8, 1);
        let id2 = gen_resp_id(TunnelId::from(7), 0x11u8, 2);
        assert_ne!(id1, id2);
    }

    #[test]
    fn resp_id_changes_with_cmd() {
        let id1 = gen_resp_id(TunnelId::from(7), 0x11u8, 5);
        let id2 = gen_resp_id(TunnelId::from(7), 0x12u8, 5);
        assert_ne!(id1, id2);
    }

    #[test]
    fn resp_id_changes_with_tunnel() {
        let id1 = gen_resp_id(TunnelId::from(7), 0x11u8, 5);
        let id2 = gen_resp_id(TunnelId::from(8), 0x11u8, 5);
        assert_ne!(id1, id2);
    }

    #[test]
    fn resp_id_changes_with_long_cmd_suffix() {
        let id1 = gen_resp_id(
            TunnelId::from(7),
            0x1122_3344_5566_7788_0000_0000_0000_0001u128,
            5,
        );
        let id2 = gen_resp_id(
            TunnelId::from(7),
            0x1122_3344_5566_7788_0000_0000_0000_0002u128,
            5,
        );
        assert_ne!(id1, id2);
    }
}
