mod client;

use bucky_raw_codec::{RawDecode, RawEncode, RawFixedBytes};
use callback_result::CallbackWaiter;
pub use client::*;
use num::{FromPrimitive, ToPrimitive};
use sfo_pool::WorkerClassification;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

mod classified_client;
pub use classified_client::*;

use crate::errors::CmdResult;
use crate::{CmdBody, CmdHandler, CmdTunnelMeta, PeerId, TunnelId};

pub trait CmdSend<M: CmdTunnelMeta>: Send + 'static {
    fn get_tunnel_meta(&self) -> Option<Arc<M>>;
    fn get_remote_peer_id(&self) -> PeerId;
}

pub trait SendGuard<M: CmdTunnelMeta, S: CmdSend<M>>: Send + 'static + Deref<Target = S> {}

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
    async fn send2(&self, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()>;
    async fn send2_with_resp(
        &self,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
        timeout: Duration,
    ) -> CmdResult<CmdBody>;
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
    async fn send2_by_specify_tunnel(
        &self,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
    ) -> CmdResult<()>;
    async fn send2_by_specify_tunnel_with_resp(
        &self,
        tunnel_id: TunnelId,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
        timeout: Duration,
    ) -> CmdResult<CmdBody>;
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
    async fn send2_by_classified_tunnel(
        &self,
        classification: C,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
    ) -> CmdResult<()>;
    async fn send2_by_classified_tunnel_with_resp(
        &self,
        classification: C,
        cmd: CMD,
        version: u8,
        body: &[&[u8]],
        timeout: Duration,
    ) -> CmdResult<CmdBody>;
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
    let cmd = if cmd_buf.len() > 8 {
        u64::from_be_bytes(cmd_buf[..8].try_into().unwrap())
    } else {
        let mut buf = [0u8; 8];
        buf[..cmd_buf.len()].copy_from_slice(&cmd_buf);
        u64::from_be_bytes(buf)
    };
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
}
