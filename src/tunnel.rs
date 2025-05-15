use std::any::Any;
use std::sync::Arc;
use as_any::AsAny;
use sfo_split::{RHalf, Splittable, WHalf};
use tokio::io::{AsyncRead, AsyncWrite};
use crate::PeerId;

pub trait CmdTunnelMeta: Send + Sync + 'static {

}

impl<T: Send + Sync + 'static> CmdTunnelMeta for T {
    
}

pub trait CmdTunnelRead<M: CmdTunnelMeta>: Send + AsyncRead + 'static + Unpin + Any + AsAny {
    fn get_remote_peer_id(&self) -> PeerId;
    fn get_tunnel_meta(&self) -> Option<Arc<M>> {
        None
    }
}

pub trait CmdTunnelWrite<M: CmdTunnelMeta>: AsyncWrite + Send + 'static + Unpin + Any + AsAny {
    fn get_remote_peer_id(&self) -> PeerId;
    fn get_tunnel_meta(&self) -> Option<Arc<M>> {
        None
    }
}

pub type CmdTunnel<R, W> = Splittable<R, W>;
pub type CmdTunnelRHalf<R, W> = RHalf<R, W>;
pub type CmdTunnelWHalf<R, W> = WHalf<R, W>;

