use std::any::Any;
use as_any::AsAny;
use sfo_split::{RHalf, Splittable, WHalf};
use tokio::io::{AsyncRead, AsyncWrite};
use crate::PeerId;

pub trait CmdTunnelRead: Send + AsyncRead + 'static + Unpin + Any + AsAny {
    fn get_remote_peer_id(&self) -> PeerId;
}

pub trait CmdTunnelWrite: AsyncWrite + Send + 'static + Unpin + Any + AsAny {
    fn get_remote_peer_id(&self) -> PeerId;
}

pub type CmdTunnel<R, W> = Splittable<R, W>;
pub type CmdTunnelRHalf<R, W> = RHalf<R, W>;
pub type CmdTunnelWHalf<R, W> = WHalf<R, W>;

