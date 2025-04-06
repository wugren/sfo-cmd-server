mod node;
mod classified_node;

use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use bucky_raw_codec::{RawDecode, RawEncode, RawFixedBytes, RawFrom};
use num::{FromPrimitive, ToPrimitive};
use sfo_pool::WorkerClassification;
use sfo_split::RHalf;
use tokio::io::AsyncReadExt;
use tokio::task::JoinHandle;
pub use node::*;
pub use classified_node::*;
use crate::{CmdHandler, CmdHeader, CmdTunnelRead, CmdTunnelWrite, PeerId, TunnelId};
use crate::cmd::CmdBodyReadImpl;
use crate::errors::{into_cmd_err, CmdErrorCode, CmdResult};

#[async_trait::async_trait]
pub trait CmdNode<LEN: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send + 'static + Eq + Hash> {
    fn register_cmd_handler(&self, cmd: CMD, handler: impl CmdHandler<LEN, CMD>);
    async fn send(&self, peer_id: &PeerId, cmd: CMD, version: u8, body: &[u8]) -> CmdResult<()>;
    async fn send2(&self, peer_id: &PeerId, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()>;
    async fn send_by_specify_tunnel(&self, peer_id: &PeerId, tunnel_id: TunnelId, cmd: CMD, version: u8, body: &[u8]) -> CmdResult<()>;
    async fn send2_by_specify_tunnel(&self, peer_id: &PeerId, tunnel_id: TunnelId, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()>;
    async fn clear_all_tunnel(&self);
}

#[async_trait::async_trait]
pub trait ClassifiedCmdNode<LEN: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send +'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send +'static + Eq + Hash, C: WorkerClassification>: CmdNode<LEN, CMD> {
    async fn send_by_classified_tunnel(&self, classification: C, cmd: CMD, version: u8, body: &[u8]) -> CmdResult<()>;
    async fn send2_by_classified_tunnel(&self, classification: C, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()>;
    async fn find_tunnel_id_by_classified(&self, classification: C) -> CmdResult<TunnelId>;
}


pub(crate) fn create_recv_handle<R: CmdTunnelRead,
    W: CmdTunnelWrite,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive + RawFixedBytes,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug + RawFixedBytes,
>(mut reader: RHalf<R, W>, tunnel_id: TunnelId, cmd_handler: Arc<dyn CmdHandler<LEN, CMD>>) -> JoinHandle<CmdResult<()>> {
    let recv_handle = tokio::spawn(async move {
        let ret: CmdResult<()> = async move {
            let remote_id = reader.get_remote_peer_id();
            loop {
                let mut header = vec![0u8; CmdHeader::<LEN, CMD>::raw_bytes().unwrap()];
                let n = reader.read_exact(&mut header).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                if n == 0 {
                    break;
                }
                let header = CmdHeader::<LEN, CMD>::clone_from_slice(header.as_slice()).map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
                log::trace!("recv cmd {:?} from {} len {}", header.cmd_code(), remote_id.to_base58(), header.pkg_len().to_u64().unwrap());
                let cmd_read = Box::new(CmdBodyReadImpl::new(reader, header.pkg_len().to_u64().unwrap() as usize));
                let waiter = cmd_read.get_waiter();
                let future = waiter.create_result_future();
                {
                    let body_read = cmd_read;
                    if let Err(e) = cmd_handler.handle(remote_id.clone(), tunnel_id, header, body_read).await {
                        log::error!("handle cmd error: {:?}", e);
                    }
                };
                reader = future.await.map_err(into_cmd_err!(CmdErrorCode::Failed))??;
                // }
            }
            Ok(())
        }.await;
        ret
    });
    recv_handle
}
