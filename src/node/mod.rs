mod node;
mod classified_node;

use std::fmt::Debug;
use std::hash::Hash;
use std::ops::DerefMut;
use std::sync::Arc;
use std::time::Duration;
use async_named_locker::ObjectHolder;
use bucky_raw_codec::{RawConvertTo, RawDecode, RawEncode, RawFixedBytes, RawFrom};
use num::{FromPrimitive, ToPrimitive};
use sfo_pool::WorkerClassification;
use sfo_split::{RHalf, WHalf};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::task::JoinHandle;
pub use node::*;
pub use classified_node::*;
use crate::{CmdBody, CmdHandler, CmdHeader, CmdTunnelMeta, CmdTunnelRead, CmdTunnelWrite, PeerId, TunnelId};
use crate::client::{CmdSend, SendGuard};
use crate::cmd::CmdBodyRead;
use crate::errors::{cmd_err, into_cmd_err, CmdErrorCode, CmdResult};

#[async_trait::async_trait]
pub trait CmdNode<LEN: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send + 'static + Eq + Hash,
    M: CmdTunnelMeta,
    S: CmdSend<M>,
    G: SendGuard<M, S>> {
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
    async fn clear_all_tunnel(&self);
    async fn get_send(&self, peer_id: &PeerId, tunnel_id: TunnelId) -> CmdResult<G>;
}

#[async_trait::async_trait]
pub trait ClassifiedCmdNode<LEN: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send +'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes + Sync + Send +'static + Eq + Hash,
    C: WorkerClassification,
    M: CmdTunnelMeta,
    S: CmdSend<M>,
    G: SendGuard<M, S>>: CmdNode<LEN, CMD, M, S, G> {
    async fn send_by_classified_tunnel(&self, classification: C, cmd: CMD, version: u8, body: &[u8]) -> CmdResult<()>;
    async fn send_by_classified_tunnel_with_resp(&self, classification: C, cmd: CMD, version: u8, body: &[u8], timeout: Duration) -> CmdResult<CmdBody>;
    async fn send2_by_classified_tunnel(&self, classification: C, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()>;
    async fn send2_by_classified_tunnel_with_resp(&self, classification: C, cmd: CMD, version: u8, body: &[&[u8]], timeout: Duration) -> CmdResult<CmdBody>;
    async fn send_cmd_by_classified_tunnel(&self, classification: C, cmd: CMD, version: u8, body: CmdBody) -> CmdResult<()>;
    async fn send_cmd_by_classified_tunnel_with_resp(&self, classification: C, cmd: CMD, version: u8, body: CmdBody, timeout: Duration) -> CmdResult<CmdBody>;
    async fn send_by_peer_classified_tunnel(&self, peer_id: &PeerId, classification: C, cmd: CMD, version: u8, body: &[u8]) -> CmdResult<()>;
    async fn send_by_peer_classified_tunnel_with_resp(&self, peer_id: &PeerId, classification: C, cmd: CMD, version: u8, body: &[u8], timeout: Duration) -> CmdResult<CmdBody>;
    async fn send2_by_peer_classified_tunnel(&self, peer_id: &PeerId, classification: C, cmd: CMD, version: u8, body: &[&[u8]]) -> CmdResult<()>;
    async fn send2_by_peer_classified_tunnel_with_resp(&self, peer_id: &PeerId, classification: C, cmd: CMD, version: u8, body: &[&[u8]], timeout: Duration) -> CmdResult<CmdBody>;
    async fn send_cmd_by_peer_classified_tunnel(&self, peer_id: &PeerId, classification: C, cmd: CMD, version: u8, body: CmdBody) -> CmdResult<()>;
    async fn send_cmd_by_peer_classified_tunnel_with_resp(&self, peer_id: &PeerId, classification: C, cmd: CMD, version: u8, body: CmdBody, timeout: Duration) -> CmdResult<CmdBody>;
    async fn find_tunnel_id_by_classified(&self, classification: C) -> CmdResult<TunnelId>;
    async fn find_tunnel_id_by_peer_classified(&self, peer_id: &PeerId,classification: C) -> CmdResult<TunnelId>;
    async fn get_send_by_classified(&self, classification: C) -> CmdResult<G>;
    async fn get_send_by_peer_classified(&self, peer_id: &PeerId, classification: C) -> CmdResult<G>;
}


pub(crate) fn create_recv_handle<M: CmdTunnelMeta,
    R: CmdTunnelRead<M>,
    W: CmdTunnelWrite<M>,
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive + RawFixedBytes,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Debug + RawFixedBytes,
>(mut reader: RHalf<R, W>, write: ObjectHolder<WHalf<R, W>>, tunnel_id: TunnelId, cmd_handler: Arc<dyn CmdHandler<LEN, CMD>>) -> JoinHandle<CmdResult<()>> {
    let recv_handle = tokio::spawn(async move {
        let ret: CmdResult<()> = async move {
            let remote_id = reader.get_remote_peer_id();
            loop {
                log::trace!("tunnel {:?} enter recv proc", tunnel_id);
                let header_len = reader.read_u8().await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                log::trace!("tunnel {:?} recv cmd len {}", tunnel_id, header_len);
                let mut header = vec![0u8; header_len as usize];
                let n = reader.read_exact(&mut header).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                if n == 0 {
                    break;
                }
                let header = CmdHeader::<LEN, CMD>::clone_from_slice(header.as_slice()).map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
                log::trace!("tunnel {:?} recv cmd {:?} from {} len {}", tunnel_id, header.cmd_code(), remote_id.to_base36(), header.pkg_len().to_u64().unwrap());
                let body_len = header.pkg_len().to_u64().unwrap();
                let cmd_read = CmdBodyRead::new(reader, header.pkg_len().to_u64().unwrap() as usize);
                let waiter = cmd_read.get_waiter();
                let future = waiter.create_result_future().map_err(into_cmd_err!(CmdErrorCode::Failed))?;
                {
                    let version = header.version();
                    let seq = header.seq();
                    let cmd_code = header.cmd_code();
                    let body_read = cmd_read;
                    match cmd_handler.handle(remote_id.clone(), tunnel_id, header, CmdBody::from_reader(BufReader::new(body_read), body_len)).await {
                        Ok(Some(mut body)) => {
                            let mut write = write.get().await;
                            let header = CmdHeader::<LEN, CMD>::new(version, true, seq, cmd_code, LEN::from_u64(body.len()).unwrap());
                            let buf = header.to_vec().map_err(into_cmd_err!(CmdErrorCode::RawCodecError))?;
                            if buf.len() > 255 {
                                return Err(cmd_err!(CmdErrorCode::RawCodecError, "header too long"));
                            }
                            write.write_u8(buf.len() as u8).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                            write.write_all(buf.as_slice()).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                            tokio::io::copy(&mut body, write.deref_mut().deref_mut()).await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                            write.flush().await.map_err(into_cmd_err!(CmdErrorCode::IoError))?;
                        }
                        Err(e) => {
                            log::error!("handle cmd error: {:?}", e);
                        }
                        _ => {}
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
