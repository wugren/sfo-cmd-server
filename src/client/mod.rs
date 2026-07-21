mod client;

use bucky_raw_codec::{RawDecode, RawEncode, RawFixedBytes};
use callback_result::CallbackWaiter;
pub use client::*;
use sfo_pool::{
    ClassifiedWorker, ClassifiedWorkerFactory, ClassifiedWorkerGuard, WorkerClassification,
};
use sha2::{Digest, Sha256};
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

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct TunnelRuntimeKey {
    peer_id: PeerId,
    tunnel_id: TunnelId,
}

impl TunnelRuntimeKey {
    fn new(peer_id: PeerId, tunnel_id: TunnelId) -> Self {
        Self { peer_id, tunnel_id }
    }
}

pub(crate) enum TunnelReserveResult {
    Acquired(TunnelReservationGuard),
    Wait(TunnelWaiterGuard),
    Missing,
}

pub(crate) struct TunnelReservationGuard {
    runtime_tunnels: Arc<TunnelRuntimeRegistry>,
    key: TunnelRuntimeKey,
    active: bool,
}

impl TunnelReservationGuard {
    fn new(runtime_tunnels: Arc<TunnelRuntimeRegistry>, key: TunnelRuntimeKey) -> Self {
        Self {
            runtime_tunnels,
            key,
            active: true,
        }
    }

    pub(crate) fn commit(mut self) {
        self.active = false;
    }

    pub(crate) fn invalidate(mut self) {
        self.runtime_tunnels.remove(&self.key);
        self.active = false;
    }
}

impl Drop for TunnelReservationGuard {
    fn drop(&mut self) {
        if self.active {
            self.runtime_tunnels.release(&self.key, true);
        }
    }
}

pub(crate) struct TunnelWaiterGuard {
    runtime_tunnels: Arc<TunnelRuntimeRegistry>,
    key: TunnelRuntimeKey,
    notify: Arc<Notify>,
}

impl TunnelWaiterGuard {
    fn new(
        runtime_tunnels: Arc<TunnelRuntimeRegistry>,
        key: TunnelRuntimeKey,
        notify: Arc<Notify>,
    ) -> Self {
        Self {
            runtime_tunnels,
            key,
            notify,
        }
    }

    pub(crate) async fn wait(self) {
        self.notify.notified().await;
    }
}

impl Drop for TunnelWaiterGuard {
    fn drop(&mut self) {
        self.runtime_tunnels.cancel_waiter(&self.key, &self.notify);
    }
}

#[derive(Default)]
pub(crate) struct TunnelRuntimeRegistry {
    state: Mutex<HashMap<TunnelRuntimeKey, TunnelRuntimeEntry>>,
}

impl TunnelRuntimeRegistry {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn reserve_key(
        self: &Arc<Self>,
        state: &mut HashMap<TunnelRuntimeKey, TunnelRuntimeEntry>,
        key: TunnelRuntimeKey,
    ) -> TunnelReserveResult {
        match state.get_mut(&key) {
            Some(entry) => match entry.state {
                TunnelBorrowState::Idle => {
                    entry.state = TunnelBorrowState::Borrowed;
                    TunnelReserveResult::Acquired(TunnelReservationGuard::new(self.clone(), key))
                }
                TunnelBorrowState::Borrowed => {
                    let notify = Arc::new(Notify::new());
                    entry.waiters.push_back(notify.clone());
                    TunnelReserveResult::Wait(TunnelWaiterGuard::new(self.clone(), key, notify))
                }
            },
            None => TunnelReserveResult::Missing,
        }
    }

    pub(crate) fn reserve_existing(
        self: &Arc<Self>,
        peer_id: &PeerId,
        tunnel_id: TunnelId,
    ) -> TunnelReserveResult {
        let mut state = self.state.lock().unwrap();
        self.reserve_key(
            &mut state,
            TunnelRuntimeKey::new(peer_id.clone(), tunnel_id),
        )
    }

    pub(crate) fn reserve_existing_tunnel(
        self: &Arc<Self>,
        tunnel_id: TunnelId,
    ) -> TunnelReserveResult {
        let mut state = self.state.lock().unwrap();
        let Some(key) = state.keys().find(|key| key.tunnel_id == tunnel_id).cloned() else {
            return TunnelReserveResult::Missing;
        };
        self.reserve_key(&mut state, key)
    }

    fn cancel_waiter(&self, key: &TunnelRuntimeKey, notify: &Arc<Notify>) {
        let mut state = self.state.lock().unwrap();
        if let Some(entry) = state.get_mut(key) {
            entry.waiters.retain(|waiter| !Arc::ptr_eq(waiter, notify));
        }
    }

    pub(crate) fn mark_borrowed(&self, peer_id: PeerId, tunnel_id: TunnelId) -> bool {
        let key = TunnelRuntimeKey::new(peer_id, tunnel_id);
        let mut state = self.state.lock().unwrap();
        match state.get_mut(&key) {
            Some(entry) => match entry.state {
                TunnelBorrowState::Idle => {
                    entry.state = TunnelBorrowState::Borrowed;
                    true
                }
                TunnelBorrowState::Borrowed => false,
            },
            None => {
                state.insert(
                    key,
                    TunnelRuntimeEntry {
                        state: TunnelBorrowState::Borrowed,
                        waiters: VecDeque::new(),
                    },
                );
                true
            }
        }
    }

    fn remove(&self, key: &TunnelRuntimeKey) {
        let waiters = {
            let mut state = self.state.lock().unwrap();
            state
                .remove(key)
                .map(|entry| entry.waiters.into_iter().collect::<Vec<_>>())
                .unwrap_or_default()
        };
        for waiter in waiters {
            waiter.notify_one();
        }
    }

    fn release(&self, key: &TunnelRuntimeKey, alive: bool) {
        let waiters = {
            let mut state = self.state.lock().unwrap();
            match state.get_mut(key) {
                Some(entry) if alive => {
                    entry.state = TunnelBorrowState::Idle;
                    entry.waiters.drain(..).collect::<Vec<_>>()
                }
                Some(_) => {
                    let entry = state.remove(key).unwrap();
                    entry.waiters.into_iter().collect::<Vec<_>>()
                }
                None => Vec::new(),
            }
        };
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
    key: TunnelRuntimeKey,
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
        peer_id: PeerId,
        tunnel_id: TunnelId,
    ) -> Self {
        Self {
            worker_guard: Some(worker_guard),
            runtime_tunnels,
            key: TunnelRuntimeKey::new(peer_id, tunnel_id),
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
            self.runtime_tunnels.release(&self.key, alive);
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

#[cfg(test)]
mod tunnel_runtime_tests {
    use super::{TunnelReserveResult, TunnelRuntimeRegistry};
    use crate::{PeerId, TunnelId};
    use std::time::Duration;

    fn peer(value: u8) -> PeerId {
        PeerId::from(vec![value; 32])
    }

    #[tokio::test]
    async fn canceled_reservation_returns_tunnel_to_idle() {
        let registry = TunnelRuntimeRegistry::new();
        let tunnel_id = TunnelId::from(1);
        let peer_id = peer(1);
        assert!(registry.mark_borrowed(peer_id.clone(), tunnel_id));
        registry.release(
            &super::TunnelRuntimeKey::new(peer_id.clone(), tunnel_id),
            true,
        );

        let reservation = match registry.reserve_existing(&peer_id, tunnel_id) {
            TunnelReserveResult::Acquired(reservation) => reservation,
            _ => panic!("expected reservation"),
        };
        drop(reservation);

        assert!(matches!(
            registry.reserve_existing(&peer_id, tunnel_id),
            TunnelReserveResult::Acquired(_)
        ));
    }

    #[tokio::test]
    async fn canceled_waiter_does_not_block_following_waiter() {
        let registry = TunnelRuntimeRegistry::new();
        let tunnel_id = TunnelId::from(2);
        let peer_id = peer(2);
        assert!(registry.mark_borrowed(peer_id.clone(), tunnel_id));

        let canceled = match registry.reserve_existing(&peer_id, tunnel_id) {
            TunnelReserveResult::Wait(waiter) => waiter,
            _ => panic!("expected waiter"),
        };
        let following = match registry.reserve_existing(&peer_id, tunnel_id) {
            TunnelReserveResult::Wait(waiter) => waiter,
            _ => panic!("expected waiter"),
        };
        drop(canceled);

        registry.release(&super::TunnelRuntimeKey::new(peer_id, tunnel_id), true);
        tokio::time::timeout(Duration::from_secs(1), following.wait())
            .await
            .expect("following waiter should be notified");
    }

    #[test]
    fn same_tunnel_id_is_scoped_by_peer_id() {
        let registry = TunnelRuntimeRegistry::new();
        let tunnel_id = TunnelId::from(3);
        let peer_a = peer(3);
        let peer_b = peer(4);

        assert!(registry.mark_borrowed(peer_a.clone(), tunnel_id));
        assert!(registry.mark_borrowed(peer_b.clone(), tunnel_id));
        assert!(matches!(
            registry.reserve_existing(&peer_a, tunnel_id),
            TunnelReserveResult::Wait(_)
        ));
        assert!(matches!(
            registry.reserve_existing(&peer_b, tunnel_id),
            TunnelReserveResult::Wait(_)
        ));
    }

    #[test]
    fn wrong_peer_does_not_invalidate_existing_tunnel() {
        let registry = TunnelRuntimeRegistry::new();
        let tunnel_id = TunnelId::from(4);
        let correct_peer = peer(5);
        let wrong_peer = peer(6);

        assert!(registry.mark_borrowed(correct_peer.clone(), tunnel_id));
        registry.release(
            &super::TunnelRuntimeKey::new(correct_peer.clone(), tunnel_id),
            true,
        );
        assert!(matches!(
            registry.reserve_existing(&wrong_peer, tunnel_id),
            TunnelReserveResult::Missing
        ));
        assert!(matches!(
            registry.reserve_existing(&correct_peer, tunnel_id),
            TunnelReserveResult::Acquired(_)
        ));
    }
}

#[async_trait::async_trait]
pub trait CmdClient<
    LEN: crate::CmdPkgLen,
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
    LEN: crate::CmdPkgLen,
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
    peer_id: &PeerId,
    tunnel_id: TunnelId,
    cmd: CMD,
    seq: u32,
) -> u128 {
    let cmd_buf = cmd.raw_encode_to_buffer().unwrap();
    let mut hasher = Sha256::new();
    hasher.update((peer_id.as_slice().len() as u64).to_be_bytes());
    hasher.update(peer_id.as_slice());
    hasher.update(tunnel_id.value().to_be_bytes());
    hasher.update(seq.to_be_bytes());
    hasher.update((cmd_buf.len() as u64).to_be_bytes());
    hasher.update(cmd_buf);
    let digest = hasher.finalize();
    u128::from_be_bytes(digest[..16].try_into().unwrap())
}

pub(crate) fn gen_seq() -> u32 {
    rand::random::<u32>()
}

#[cfg(test)]
mod tests {
    use super::gen_resp_id;
    use crate::{PeerId, TunnelId};

    fn peer(value: u8) -> PeerId {
        PeerId::from(vec![value; 32])
    }

    #[test]
    fn resp_id_changes_with_seq() {
        let peer_id = peer(1);
        let id1 = gen_resp_id(&peer_id, TunnelId::from(7), 0x11u8, 1);
        let id2 = gen_resp_id(&peer_id, TunnelId::from(7), 0x11u8, 2);
        assert_ne!(id1, id2);
    }

    #[test]
    fn resp_id_changes_with_cmd() {
        let peer_id = peer(1);
        let id1 = gen_resp_id(&peer_id, TunnelId::from(7), 0x11u8, 5);
        let id2 = gen_resp_id(&peer_id, TunnelId::from(7), 0x12u8, 5);
        assert_ne!(id1, id2);
    }

    #[test]
    fn resp_id_changes_with_tunnel() {
        let peer_id = peer(1);
        let id1 = gen_resp_id(&peer_id, TunnelId::from(7), 0x11u8, 5);
        let id2 = gen_resp_id(&peer_id, TunnelId::from(8), 0x11u8, 5);
        assert_ne!(id1, id2);
    }

    #[test]
    fn resp_id_changes_with_long_cmd_suffix() {
        let id1 = gen_resp_id(
            &peer(1),
            TunnelId::from(7),
            0x1122_3344_5566_7788_0000_0000_0000_0001u128,
            5,
        );
        let id2 = gen_resp_id(
            &peer(1),
            TunnelId::from(7),
            0x1122_3344_5566_7788_0000_0000_0000_0002u128,
            5,
        );
        assert_ne!(id1, id2);
    }

    #[test]
    fn resp_id_changes_with_peer() {
        let id1 = gen_resp_id(&peer(1), TunnelId::from(7), 0x11u8, 5);
        let id2 = gen_resp_id(&peer(2), TunnelId::from(7), 0x11u8, 5);
        assert_ne!(id1, id2);
    }
}
