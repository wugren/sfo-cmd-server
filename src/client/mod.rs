mod client;

use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use bucky_raw_codec::{RawDecode, RawEncode, RawFixedBytes};
use callback_result::CallbackWaiter;
use num::{FromPrimitive, ToPrimitive};
use sfo_pool::WorkerClassification;
pub use client::*;

mod classified_client;
pub use classified_client::*;

use crate::{CmdBody, CmdHandler, CmdTunnelMeta, PeerId, TunnelId};
use crate::errors::CmdResult;

pub trait CmdSend<M: CmdTunnelMeta>: Send + 'static {
    fn get_tunnel_meta(&self) -> Option<Arc<M>>;
    fn get_remote_peer_id(&self) -> PeerId;
}

pub trait SendGuard<M: CmdTunnelMeta, S: CmdSend<M>>: Send + 'static + Deref<Target = S>  {
}

#[async_trait::async_trait]
pub trait CmdClient<LEN: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send + 'static + Eq + Hash,
    M: CmdTunnelMeta,
    S: CmdSend<M>,
    G: SendGuard<M, S>>: Send + Sync + 'static {
    fn register_cmd_handler(&self, cmd: CMD, handler: impl CmdHandler<LEN, CMD>);
    async fn send(&self, cmd: CMD, version: u8, body: &[u8]) -> CmdResult<()>;
    async fn send_with_resp(&self, cmd: CMD, version: u8, body: &[u8], timeout: Duration) -> CmdResult<CmdBody>;
    async fn send2(&self, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()>;
    async fn send2_with_resp(&self, cmd: CMD, version: u8, body: &[&[u8]], timeout: Duration) -> CmdResult<CmdBody>;
    async fn send_cmd(&self, cmd: CMD, version: u8, body: CmdBody) -> CmdResult<()>;
    async fn send_cmd_with_resp(&self, cmd: CMD, version: u8, body: CmdBody, timeout: Duration) -> CmdResult<CmdBody>;
    async fn send_by_specify_tunnel(&self, tunnel_id: TunnelId, cmd: CMD, version: u8, body: &[u8]) -> CmdResult<()>;
    async fn send_by_specify_tunnel_with_resp(&self, tunnel_id: TunnelId, cmd: CMD, version: u8, body: &[u8], timeout: Duration) -> CmdResult<CmdBody>;
    async fn send2_by_specify_tunnel(&self, tunnel_id: TunnelId, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()>;
    async fn send2_by_specify_tunnel_with_resp(&self, tunnel_id: TunnelId, cmd: CMD, version: u8, body: &[&[u8]], timeout: Duration) -> CmdResult<CmdBody>;
    async fn send_cmd_by_specify_tunnel(&self, tunnel_id: TunnelId, cmd: CMD, version: u8, body: CmdBody) -> CmdResult<()>;
    async fn send_cmd_by_specify_tunnel_with_resp(&self, tunnel_id: TunnelId, cmd: CMD, version: u8, body: CmdBody, timeout: Duration) -> CmdResult<CmdBody>;
    async fn clear_all_tunnel(&self);
    async fn get_send(&self, tunnel_id: TunnelId) -> CmdResult<G>;
}

#[async_trait::async_trait]
pub trait ClassifiedCmdClient<LEN: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send + 'static + Eq + Hash,
    C: WorkerClassification,
    M: CmdTunnelMeta,
    S: CmdSend<M>,
    G: SendGuard<M, S>>: CmdClient<LEN, CMD, M, S, G> {
    async fn send_by_classified_tunnel(&self, classification: C, cmd: CMD, version: u8, body: &[u8]) -> CmdResult<()>;
    async fn send_by_classified_tunnel_with_resp(&self, classification: C, cmd: CMD, version: u8, body: &[u8], timeout: Duration) -> CmdResult<CmdBody>;
    async fn send2_by_classified_tunnel(&self, classification: C, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()>;
    async fn send2_by_classified_tunnel_with_resp(&self, classification: C, cmd: CMD, version: u8, body: &[&[u8]], timeout: Duration) -> CmdResult<CmdBody>;
    async fn send_cmd_by_classified_tunnel(&self, classification: C, cmd: CMD, version: u8, body: CmdBody) -> CmdResult<()>;
    async fn send_cmd_by_classified_tunnel_with_resp(&self, classification: C, cmd: CMD, version: u8, body: CmdBody, timeout: Duration) -> CmdResult<CmdBody>;
    async fn find_tunnel_id_by_classified(&self, classification: C) -> CmdResult<TunnelId>;
    async fn get_send_by_classified(&self, classification: C) -> CmdResult<G>;
}


pub(crate) type RespWaiter = CallbackWaiter<u64, CmdBody>;
pub(crate) type RespWaiterRef = Arc<RespWaiter>;

pub(crate) fn gen_resp_id<CMD: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send + 'static>(cmd: CMD, seq: u32) -> u64 {
    let cmd_buf = cmd.raw_encode_to_buffer().unwrap();
    let mut id = seq as u64;
    let cmd = if cmd_buf.len() > 4 {
        u32::from_be_bytes(cmd_buf[..4].try_into().unwrap())
    } else {
        let mut buf = [0u8; 4];
        buf[..cmd_buf.len()].copy_from_slice(&cmd_buf);
        u32::from_be_bytes(buf)
    };
    id |= (cmd as u64) << 32;
    id
}

pub(crate) fn gen_seq() -> u32 {
    rand::random::<u32>()
}
