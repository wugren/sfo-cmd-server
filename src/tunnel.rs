use std::any::Any;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use crate::errors::CmdResult;

pub trait CmdTunnelRead: Send + Sync + AsyncRead + 'static + Unpin + Any {
    fn get_any(&self) -> &dyn Any;
    fn get_any_mut(&mut self) -> &mut dyn Any;
}

pub trait CmdTunnelWrite: AsyncWrite + Send + Sync + 'static + Unpin + Any {
    fn get_any(&self) -> &dyn Any;
    fn get_any_mut(&mut self) -> &mut dyn Any;
}

pub trait CmdTunnel: Send + Sync + 'static {
    fn get_tls_key(&self) -> Vec<u8>;
    fn split(&self) -> CmdResult<(Box<dyn CmdTunnelRead>, Box<dyn CmdTunnelWrite>)>;
    fn unsplit(&self, read: Box<dyn CmdTunnelRead>, write: Box<dyn CmdTunnelWrite>) -> CmdResult<()>;
}
pub type CmdTunnelRef = Arc<dyn CmdTunnel>;
