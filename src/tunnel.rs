use std::any::Any;
use tokio::io::{AsyncRead, AsyncWrite};
use crate::errors::CmdResult;
use crate::PeerId;

pub trait CmdTunnelRead: Send + AsyncRead + 'static + Unpin + Any {
    fn get_any(&self) -> &dyn Any;
    fn get_any_mut(&mut self) -> &mut dyn Any;
}

pub trait CmdTunnelWrite: AsyncWrite + Send + 'static + Unpin + Any {
    fn get_any(&self) -> &dyn Any;
    fn get_any_mut(&mut self) -> &mut dyn Any;
}

pub trait CmdTunnel: Send + Sync + 'static {
    fn get_remote_peer_id(&self) -> PeerId;
    fn split(&self) -> CmdResult<(Box<dyn CmdTunnelRead>, Box<dyn CmdTunnelWrite>)>;
    fn unsplit(&self, read: Box<dyn CmdTunnelRead>, write: Box<dyn CmdTunnelWrite>) -> CmdResult<()>;
}
