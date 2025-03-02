mod client;

use std::hash::Hash;
use bucky_raw_codec::{RawDecode, RawEncode, RawFixedBytes};
use num::{FromPrimitive, ToPrimitive};
use sfo_pool::WorkerClassification;
pub use client::*;

mod classified_client;
pub use classified_client::*;

use crate::{CmdHandler, TunnelId};
use crate::errors::CmdResult;

#[async_trait::async_trait]
pub trait CmdClient<LEN: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send + 'static + Eq + Hash,>: Send + Sync + 'static {
    fn register_cmd_handler(&self, cmd: CMD, handler: impl CmdHandler<LEN, CMD>);
    async fn send(&self, cmd: CMD, version: u8, body: &[u8]) -> CmdResult<()>;
    async fn send2(&self, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()>;
    async fn send_by_specify_tunnel(&self, tunnel_id: TunnelId, cmd: CMD, version: u8, body: &[u8]) -> CmdResult<()>;
    async fn send2_by_specify_tunnel(&self, tunnel_id: TunnelId, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()>;
}

#[async_trait::async_trait]
pub trait ClassifiedCmdClient<LEN: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send + 'static + Eq + Hash, C: WorkerClassification>: CmdClient<LEN, CMD> {
    async fn send_by_classified_tunnel(&self, classification: C, cmd: CMD, version: u8, body: &[u8]) -> CmdResult<()>;
    async fn send2_by_classified_tunnel(&self, classification: C, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()>;
    async fn find_tunnel_id_by_classified(&self, classification: C) -> CmdResult<TunnelId>;
}
