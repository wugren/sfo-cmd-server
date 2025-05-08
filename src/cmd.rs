use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::{fmt, io};
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use bucky_raw_codec::{RawDecode, RawEncode};
use callback_result::{SingleCallbackWaiter};
use futures_lite::ready;
use num::{FromPrimitive, ToPrimitive};
use sfo_split::RHalf;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
use crate::errors::{cmd_err, into_cmd_err, CmdErrorCode, CmdResult};
use crate::peer_id::PeerId;
use crate::{TunnelId};

#[derive(RawEncode, RawDecode)]
pub struct CmdHeader<LEN, CMD> {
    pkg_len: LEN,
    version: u8,
    cmd_code: CMD,
    is_resp: bool,
    seq: Option<u32>,
}

impl<LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static> CmdHeader<LEN, CMD> {
    pub fn new(version: u8, is_resp: bool, seq: Option<u32>, cmd_code: CMD, pkg_len: LEN) -> Self {
        Self {
            pkg_len,
            version,
            seq,
            cmd_code,
            is_resp,
        }
    }

    pub fn pkg_len(&self) -> LEN {
        self.pkg_len
    }

    pub fn version(&self) -> u8 {
        self.version
    }

    pub fn seq(&self) -> Option<u32> {
        self.seq
    }

    pub fn is_resp(&self) -> bool {
        self.is_resp
    }

    pub fn cmd_code(&self) -> CMD {
        self.cmd_code
    }

    pub fn set_pkg_len(&mut self, pkg_len: LEN) {
        self.pkg_len = pkg_len;
    }
}

#[async_trait::async_trait]
pub trait CmdBodyReadAll: tokio::io::AsyncRead + Send + 'static {
    async fn read_all(&mut self) -> CmdResult<Vec<u8>>;
}

pub(crate) struct CmdBodyRead<R: AsyncRead + Send + 'static + Unpin, W: AsyncWrite + Send + 'static + Unpin> {
    recv: Option<RHalf<R, W>>,
    len: usize,
    offset: usize,
    waiter: Arc<SingleCallbackWaiter<CmdResult<RHalf<R, W>>>>,
}

impl<R: AsyncRead + Send + 'static + Unpin, W: AsyncWrite + Send + 'static + Unpin> CmdBodyRead<R, W> {
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
impl<R: AsyncRead + Send + 'static + Unpin, W: AsyncWrite + Send + 'static + Unpin> CmdBodyReadAll for CmdBodyRead<R, W> {
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

impl<R: AsyncRead + Send + 'static + Unpin, W: AsyncWrite + Send + 'static + Unpin> Drop for CmdBodyRead<R, W> {
    fn drop(&mut self) {
        if self.recv.is_none() || (self.len == self.offset && self.len != 0) {
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

impl<R: AsyncRead + Send + 'static + Unpin, W: AsyncWrite + Send + 'static + Unpin> tokio::io::AsyncRead for CmdBodyRead<R, W> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let this = Pin::into_inner(self);
        let len = this.len - this.offset;
        if len == 0 {
            return Poll::Ready(Ok(()));
        }
        let recv = Pin::new(this.recv.as_mut().unwrap().deref_mut());
        let fut = recv.poll_read(cx, buf);
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
    async fn handle(&self, peer_id: PeerId, tunnel_id: TunnelId, header: CmdHeader<LEN, CMD>, body: CmdBody) -> CmdResult<Option<CmdBody>>;
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
pin_project_lite::pin_project! {
pub struct CmdBody {
        #[pin]
        reader: Box<dyn AsyncBufRead + Unpin + Send  + 'static>,
        length: u64,
        bytes_read: u64,
    }
}

impl CmdBody {
    pub fn empty() -> Self {
        Self {
            reader: Box::new(tokio::io::empty()),
            length: 0,
            bytes_read: 0,
        }
    }

    pub fn from_reader(
        reader: impl AsyncBufRead + Unpin + Send + 'static,
        length: u64,
    ) -> Self {
        Self {
            reader: Box::new(reader),
            length,
            bytes_read: 0,
        }
    }

    pub fn into_reader(self) -> Box<dyn AsyncBufRead + Unpin + Send + 'static> {
        self.reader
    }

    pub async fn read_all(&mut self) -> CmdResult<Vec<u8>> {
        let mut buf = Vec::with_capacity(1024);
        self.read_to_end(&mut buf).await.map_err(into_cmd_err!(CmdErrorCode::Failed, "read to end failed"))?;
        Ok(buf)
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self {
            length: bytes.len() as u64,
            reader: Box::new(io::Cursor::new(bytes)),
            bytes_read: 0,
        }
    }

    pub async fn into_bytes(mut self) -> CmdResult<Vec<u8>> {
        let mut buf = Vec::with_capacity(1024);
        self.read_to_end(&mut buf)
            .await.map_err(into_cmd_err!(CmdErrorCode::Failed, "read to end failed"))?;
        Ok(buf)
    }

    pub fn from_string(s: String) -> Self {
        Self {
            length: s.len() as u64,
            reader: Box::new(io::Cursor::new(s.into_bytes())),
            bytes_read: 0,
        }
    }

    pub async fn into_string(mut self) -> CmdResult<String> {
        let mut result = String::with_capacity(self.len() as usize);
        self.read_to_string(&mut result)
            .await.map_err(into_cmd_err!(CmdErrorCode::Failed, "read to string failed"))?;
        Ok(result)
    }

    pub async fn from_path<P>(path: P) -> io::Result<Self>
    where
        P: AsRef<std::path::Path>,
    {
        let path = path.as_ref();
        let file = tokio::fs::File::open(path).await?;
        Self::from_file(file).await
    }

    pub async fn from_file(
        file: tokio::fs::File,
    ) -> io::Result<Self> {
        let len = file.metadata().await?.len();

        Ok(Self {
            length: len,
            reader: Box::new(tokio::io::BufReader::new(file)),
            bytes_read: 0,
        })
    }

    pub fn len(&self) -> u64 {
        self.length
    }

    /// Returns `true` if the body has a length of zero, and `false` otherwise.
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub fn chain(self, other: CmdBody) -> Self {
        let length = (self.length - self.bytes_read).checked_add(other.length - other.bytes_read).unwrap_or(0);
        Self {
            length,
            reader: Box::new(tokio::io::AsyncReadExt::chain(self, other)),
            bytes_read: 0,
        }
    }
}

impl Debug for CmdBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CmdResponse")
            .field("reader", &"<hidden>")
            .field("length", &self.length)
            .field("bytes_read", &self.bytes_read)
            .finish()
    }
}


impl From<String> for CmdBody {
    fn from(s: String) -> Self {
        Self::from_string(s)
    }
}

impl<'a> From<&'a str> for CmdBody {
    fn from(s: &'a str) -> Self {
        Self::from_string(s.to_owned())
    }
}

impl From<Vec<u8>> for CmdBody {
    fn from(b: Vec<u8>) -> Self {
        Self::from_bytes(b)
    }
}

impl<'a> From<&'a [u8]> for CmdBody {
    fn from(b: &'a [u8]) -> Self {
        Self::from_bytes(b.to_owned())
    }
}

impl AsyncRead for CmdBody {
    #[allow(rustdoc::missing_doc_code_examples)]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let buf = if self.length == self.bytes_read {
            return Poll::Ready(Ok(()));
        } else {
            buf
        };

        ready!(Pin::new(&mut self.reader).poll_read(cx, buf))?;
        self.bytes_read += buf.filled().len() as u64;
        Poll::Ready(Ok(()))
    }
}

impl AsyncBufRead for CmdBody {
    #[allow(rustdoc::missing_doc_code_examples)]
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&'_ [u8]>> {
        self.project().reader.poll_fill_buf(cx)
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        Pin::new(&mut self.reader).consume(amt)
    }
}
