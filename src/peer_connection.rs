use std::sync::Arc;
use tokio::task::JoinHandle;
use crate::{CmdTunnel, CmdTunnelWrite};
use crate::errors::CmdResult;
use crate::peer_id::PeerId;
use crate::tunnel_id::TunnelId;

pub struct PeerConnection<T: CmdTunnel> {
    pub conn_id: TunnelId,
    pub peer_id: PeerId,
    pub tunnel: Arc<T>,
    pub send: Box<dyn CmdTunnelWrite>,
    pub handle: Option<JoinHandle<CmdResult<()>>>,
}
