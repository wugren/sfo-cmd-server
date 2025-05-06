use async_named_locker::ObjectHolder;
use sfo_split::WHalf;
use tokio::task::JoinHandle;
use crate::errors::CmdResult;
use crate::peer_id::PeerId;
use crate::tunnel_id::TunnelId;

pub struct PeerConnection<R, W> {
    pub conn_id: TunnelId,
    pub peer_id: PeerId,
    pub send: ObjectHolder<WHalf<R, W>>,
    pub handle: Option<JoinHandle<CmdResult<()>>>,
}
