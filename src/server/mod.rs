mod server;
mod peer_manager;

use std::hash::Hash;
use std::time::Duration;
use bucky_raw_codec::{RawDecode, RawEncode, RawFixedBytes};
use num::{FromPrimitive, ToPrimitive};
pub use server::*;
use crate::{CmdBody, CmdHandler, PeerId, TunnelId};
use crate::errors::CmdResult;

#[async_trait::async_trait]
pub trait CmdServer<LEN: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send + 'static + Eq + Hash>: 'static + Send + Sync {
    fn register_cmd_handler(&self, cmd: CMD, handler: impl CmdHandler<LEN, CMD>);
    async fn send(&self, peer_id: &PeerId, cmd: CMD, version: u8, body: &[u8]) -> CmdResult<()>;
    async fn send_with_resp(&self, peer_id: &PeerId, cmd: CMD, version: u8, body: &[u8], timeout: Duration) -> CmdResult<CmdBody>;
    async fn send2(&self, peer_id: &PeerId, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()>;
    async fn send2_with_resp(&self, peer_id: &PeerId, cmd: CMD, version: u8, body: &[&[u8]], timeout: Duration) -> CmdResult<CmdBody>;
    async fn send_cmd(&self, peer_id: &PeerId, cmd: CMD, version: u8, body: CmdBody) -> CmdResult<()>;
    async fn send_cmd_with_resp(&self, peer_id: &PeerId, cmd: CMD, version: u8, body: CmdBody, timeout: Duration) -> CmdResult<CmdBody>;
    async fn send_by_specify_tunnel(&self, peer_id: &PeerId, tunnel_id: TunnelId, cmd: CMD, version: u8, body: &[u8]) -> CmdResult<()>;
    async fn send_by_specify_tunnel_with_resp(&self, peer_id: &PeerId, tunnel_id: TunnelId, cmd: CMD, version: u8, body: &[u8], timeout: Duration) -> CmdResult<CmdBody>;
    async fn send2_by_specify_tunnel(&self, peer_id: &PeerId, tunnel_id: TunnelId, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()>;
    async fn send2_by_specify_tunnel_with_resp(&self, peer_id: &PeerId, tunnel_id: TunnelId, cmd: CMD, version: u8, body: &[&[u8]], timeout: Duration) -> CmdResult<CmdBody>;
    async fn send_cmd_by_specify_tunnel(&self, peer_id: &PeerId, tunnel_id: TunnelId, cmd: CMD, version: u8, body: CmdBody) -> CmdResult<()>;
    async fn send_cmd_by_specify_tunnel_with_resp(&self, peer_id: &PeerId, tunnel_id: TunnelId, cmd: CMD, version: u8, body: CmdBody, timeout: Duration) -> CmdResult<CmdBody>;
    async fn send_by_all_tunnels(&self, peer_id: &PeerId, cmd: CMD, version: u8, body: &[u8]) -> CmdResult<()>;
    async fn send2_by_all_tunnels(&self, peer_id: &PeerId, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()>;
}
