use std::collections::HashMap;
use std::hash::Hash;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use bucky_raw_codec::{RawDecode, RawEncode, RawFixedBytes};
use callback_result::{SingleCallbackWaiter};
use num::{FromPrimitive, ToPrimitive};
use sfo_split::RHalf;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
use crate::errors::{cmd_err, into_cmd_err, CmdErrorCode, CmdResult};
use crate::peer_id::PeerId;
use crate::{TunnelId};

#[derive(RawEncode, RawDecode)]
pub struct CmdHeader<LEN, CMD> {
    pkg_len: LEN,
    version: u8,
    cmd_code: CMD,
}

impl<LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static> CmdHeader<LEN, CMD> {
    pub fn new(version: u8, cmd_code: CMD, pkg_len: LEN) -> Self {
        Self {
            pkg_len,
            version,
            cmd_code,
        }
    }

    pub fn pkg_len(&self) -> LEN {
        self.pkg_len
    }

    pub fn version(&self) -> u8 {
        self.version
    }

    pub fn cmd_code(&self) -> CMD {
        self.cmd_code
    }

    pub fn set_pkg_len(&mut self, pkg_len: LEN) {
        self.pkg_len = pkg_len;
    }
}

impl<LEN: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + RawFixedBytes> RawFixedBytes for CmdHeader<LEN, CMD> {
    fn raw_bytes() -> Option<usize> {
        Some(LEN::raw_bytes().unwrap() + u8::raw_bytes().unwrap() + CMD::raw_bytes().unwrap())
    }
}

#[async_trait::async_trait]
pub trait CmdBodyReadAll: tokio::io::AsyncRead + Send + 'static {
    async fn read_all(&mut self) -> CmdResult<Vec<u8>>;
}
pub type CmdBodyRead = Box<dyn CmdBodyReadAll>;

pub(crate) struct CmdBodyReadImpl<R: AsyncRead + Send + 'static + Unpin, W: AsyncWrite + Send + 'static + Unpin> {
    recv: Option<RHalf<R, W>>,
    len: usize,
    offset: usize,
    waiter: Arc<SingleCallbackWaiter<CmdResult<RHalf<R, W>>>>,
}

impl<R: AsyncRead + Send + 'static + Unpin, W: AsyncWrite + Send + 'static + Unpin> CmdBodyReadImpl<R, W> {
    pub fn new(recv: RHalf<R, W>, len: usize) -> Self {
        Self {
            recv: Some(recv),
            len,
            offset: 0,
            waiter: Arc::new(SingleCallbackWaiter::new()),
        }
    }


    pub(crate) fn get_waiter(&self) -> Arc<SingleCallbackWaiter<CmdResult<RHalf<R, W>>>> {
        self.waiter.clone()
    }
}

#[async_trait::async_trait]
impl<R: AsyncRead + Send + 'static + Unpin, W: AsyncWrite + Send + 'static + Unpin> CmdBodyReadAll for CmdBodyReadImpl<R, W> {
    async fn read_all(&mut self) -> CmdResult<Vec<u8>> {
        if self.offset == self.len {
            return Ok(Vec::new());
        }
        let mut buf = vec![0u8; self.len - self.offset];
        let ret = self.recv.as_mut().unwrap().read_exact(&mut buf).await.map_err(into_cmd_err!(CmdErrorCode::IoError));
        if ret.is_ok() {
            self.offset = self.len;
            self.waiter.set_result_with_cache(Ok(self.recv.take().unwrap()));
            Ok(buf)
        } else {
            self.recv.take();
            self.waiter.set_result_with_cache(Err(cmd_err!(CmdErrorCode::IoError, "read body error")));
            Err(ret.err().unwrap())
        }
    }
}

impl<R: AsyncRead + Send + 'static + Unpin, W: AsyncWrite + Send + 'static + Unpin> Drop for CmdBodyReadImpl<R, W> {
    fn drop(&mut self) {
        if self.recv.is_none() || self.len == self.offset {
            return;
        }
        let mut recv = self.recv.take().unwrap();
        let len = self.len - self.offset;
        let waiter = self.waiter.clone();
        if len == 0 {
            waiter.set_result_with_cache(Ok(recv));
            return;
        }

        tokio::spawn(async move {
            let mut buf = vec![0u8; len];
            if let Err(e) = recv.read_exact(&mut buf).await {
                waiter.set_result_with_cache(Err(cmd_err!(CmdErrorCode::IoError, "read body error {}", e)));
            } else {
                waiter.set_result_with_cache(Ok(recv));
            }
        });
    }
}

impl<R: AsyncRead + Send + 'static + Unpin, W: AsyncWrite + Send + 'static + Unpin> tokio::io::AsyncRead for CmdBodyReadImpl<R, W> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let this = Pin::into_inner(self);
        let len = this.len - this.offset;
        if len == 0 {
            return Poll::Ready(Ok(()));
        }
        let buf = buf.initialize_unfilled();
        let mut buf = ReadBuf::new(&mut buf[..len]);
        let recv = Pin::new(this.recv.as_mut().unwrap().deref_mut());
        let fut = recv.poll_read(cx, &mut buf);
        match fut {
            Poll::Ready(Ok(())) => {
                this.offset += buf.filled().len();
                if this.offset == this.len {
                    this.waiter.set_result_with_cache(Ok(this.recv.take().unwrap()));
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => {
                this.recv.take();
                this.waiter.set_result_with_cache(Err(cmd_err!(CmdErrorCode::IoError, "read body error")));
                Poll::Ready(Err(e))
            },
            Poll::Pending => Poll::Pending,
        }
    }
}


#[callback_trait::callback_trait]
pub trait CmdHandler<LEN, CMD>: Send + Sync + 'static
where LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
      CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static {
    async fn handle(&self, peer_id: PeerId, tunnel_id: TunnelId, header: CmdHeader<LEN, CMD>, body: CmdBodyRead) -> CmdResult<()>;
}

pub(crate) struct CmdHandlerMap<LEN, CMD> {
    map: Mutex<HashMap<CMD, Arc<dyn CmdHandler<LEN, CMD>>>>,
}

impl <LEN, CMD> CmdHandlerMap<LEN, CMD>
where LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
      CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Eq + Hash {
    pub fn new() -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
        }
    }

    pub fn insert(&self, cmd: CMD, handler: impl CmdHandler<LEN, CMD>) {
        self.map.lock().unwrap().insert(cmd, Arc::new(handler));
    }

    pub fn get(&self, cmd: CMD) -> Option<Arc<dyn CmdHandler<LEN, CMD>>> {
        self.map.lock().unwrap().get(&cmd).map(|v| v.clone())
    }
}
