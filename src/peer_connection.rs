use tokio::task::JoinHandle;
use crate::{CmdTunnelRef, CmdTunnelWrite};
use crate::errors::CmdResult;
use crate::peer_id::PeerId;
use crate::tunnel_id::TunnelId;

pub struct PeerConnection {
    pub conn_id: TunnelId,
    pub peer_id: PeerId,
    pub tunnel: CmdTunnelRef,
    pub send: Box<dyn CmdTunnelWrite>,
    pub handle: Option<JoinHandle<CmdResult<()>>>,
}
