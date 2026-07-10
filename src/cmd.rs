use crate::TunnelId;
use crate::errors::{CmdErrorCode, CmdResult, cmd_err, into_cmd_err};
use crate::peer_id::PeerId;
use bucky_raw_codec::{RawDecode, RawEncode};
use num::{FromPrimitive, ToPrimitive};
use sfo_split::RHalf;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::{fmt, io};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
use tokio::sync::oneshot;

pub const MAX_PKG_LEN: u64 = 1024 * 1024;

fn validate_pkg_len(len: u64) -> CmdResult<()> {
    if len > MAX_PKG_LEN {
        return Err(cmd_err!(
            CmdErrorCode::InvalidParam,
            "body len {} exceeds max package len {}",
            len,
            MAX_PKG_LEN
        ));
    }
    Ok(())
}

pub(crate) fn encode_pkg_len<LEN: FromPrimitive>(len: u64) -> CmdResult<LEN> {
    validate_pkg_len(len)?;
    LEN::from_u64(len).ok_or_else(|| {
        cmd_err!(
            CmdErrorCode::InvalidParam,
            "body len {} exceeds header len type {}",
            len,
            std::any::type_name::<LEN>()
        )
    })
}

pub(crate) fn decode_pkg_len<LEN: ToPrimitive>(len: LEN) -> CmdResult<(u64, usize)> {
    let len = len.to_u64().ok_or_else(|| {
        cmd_err!(
            CmdErrorCode::InvalidParam,
            "invalid body len for header len type {}",
            std::any::type_name::<LEN>()
        )
    })?;
    validate_pkg_len(len)?;
    let usize_len = usize::try_from(len).map_err(|_| {
        cmd_err!(
            CmdErrorCode::InvalidParam,
            "body len {} exceeds platform usize",
            len
        )
    })?;
    Ok((len, usize_len))
}

#[derive(RawEncode, RawDecode)]
pub struct CmdHeader<LEN, CMD> {
    pkg_len: LEN,
    version: u8,
    cmd_code: CMD,
    is_resp: bool,
    seq: Option<u32>,
}

impl<
    LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static,
> CmdHeader<LEN, CMD>
{
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

pub(crate) struct CmdBodyRead<
    R: AsyncRead + Send + 'static + Unpin,
    W: AsyncWrite + Send + 'static + Unpin,
> {
    recv: Option<RHalf<R, W>>,
    len: usize,
    offset: usize,
    completion: Option<oneshot::Sender<CmdResult<RHalf<R, W>>>>,
}

pub(crate) struct CmdBodyReadCompletion<
    R: AsyncRead + Send + 'static + Unpin,
    W: AsyncWrite + Send + 'static + Unpin,
> {
    receiver: oneshot::Receiver<CmdResult<RHalf<R, W>>>,
}

impl<R: AsyncRead + Send + 'static + Unpin, W: AsyncWrite + Send + 'static + Unpin>
    CmdBodyReadCompletion<R, W>
{
    pub async fn into_reader(self) -> CmdResult<RHalf<R, W>> {
        self.receiver
            .await
            .map_err(|e| cmd_err!(CmdErrorCode::Failed, "body read completion canceled: {}", e))?
    }
}

impl<R: AsyncRead + Send + 'static + Unpin, W: AsyncWrite + Send + 'static + Unpin>
    CmdBodyRead<R, W>
{
    pub fn new(recv: RHalf<R, W>, len: usize) -> (Self, CmdBodyReadCompletion<R, W>) {
        let (completion, receiver) = oneshot::channel();
        (
            Self {
                recv: Some(recv),
                len,
                offset: 0,
                completion: Some(completion),
            },
            CmdBodyReadCompletion { receiver },
        )
    }

    fn complete(&mut self, result: CmdResult<RHalf<R, W>>) {
        if let Some(completion) = self.completion.take() {
            let _ = completion.send(result);
        }
    }
}

#[async_trait::async_trait]
impl<R: AsyncRead + Send + 'static + Unpin, W: AsyncWrite + Send + 'static + Unpin> CmdBodyReadAll
    for CmdBodyRead<R, W>
{
    async fn read_all(&mut self) -> CmdResult<Vec<u8>> {
        if self.offset == self.len {
            return Ok(Vec::new());
        }
        let mut buf = vec![0u8; self.len - self.offset];
        let ret = self
            .recv
            .as_mut()
            .unwrap()
            .read_exact(&mut buf)
            .await
            .map_err(into_cmd_err!(CmdErrorCode::IoError));
        if ret.is_ok() {
            self.offset = self.len;
            let recv = self.recv.take().unwrap();
            self.complete(Ok(recv));
            Ok(buf)
        } else {
            self.recv.take();
            self.complete(Err(cmd_err!(CmdErrorCode::IoError, "read body error")));
            Err(ret.err().unwrap())
        }
    }
}

impl<R: AsyncRead + Send + 'static + Unpin, W: AsyncWrite + Send + 'static + Unpin> Drop
    for CmdBodyRead<R, W>
{
    fn drop(&mut self) {
        if self.recv.is_none() || (self.len == self.offset && self.len != 0) {
            return;
        }
        let mut recv = self.recv.take().unwrap();
        let len = self.len - self.offset;
        let completion = self.completion.take();
        if len == 0 {
            if let Some(completion) = completion {
                let _ = completion.send(Ok(recv));
            }
            return;
        }

        tokio::spawn(async move {
            let mut buf = vec![0u8; len];
            if let Some(completion) = completion {
                if let Err(e) = recv.read_exact(&mut buf).await {
                    let _ = completion.send(Err(cmd_err!(
                        CmdErrorCode::IoError,
                        "read body error {}",
                        e
                    )));
                } else {
                    let _ = completion.send(Ok(recv));
                }
            }
        });
    }
}

impl<R: AsyncRead + Send + 'static + Unpin, W: AsyncWrite + Send + 'static + Unpin>
    tokio::io::AsyncRead for CmdBodyRead<R, W>
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = Pin::into_inner(self);
        let len = this.len - this.offset;
        if len == 0 {
            return Poll::Ready(Ok(()));
        }
        let recv = Pin::new(this.recv.as_mut().unwrap().deref_mut());
        let read_len = std::cmp::min(len, buf.remaining());
        let mut read_buf = ReadBuf::new(buf.initialize_unfilled_to(read_len));
        let fut = recv.poll_read(cx, &mut read_buf);
        match fut {
            Poll::Ready(Ok(())) => {
                let len = read_buf.filled().len();
                drop(read_buf);
                this.offset += len;
                buf.advance(len);
                if this.offset == this.len {
                    let recv = this.recv.take().unwrap();
                    this.complete(Ok(recv));
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => {
                this.recv.take();
                this.complete(Err(cmd_err!(CmdErrorCode::IoError, "read body error")));
                Poll::Ready(Err(e))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[callback_trait::callback_trait]
pub trait CmdHandler<LEN, CMD>: Send + Sync + 'static
where
    LEN: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + Send
        + Sync
        + 'static
        + FromPrimitive
        + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static,
{
    async fn handle(
        &self,
        local_id: PeerId,
        peer_id: PeerId,
        tunnel_id: TunnelId,
        header: CmdHeader<LEN, CMD>,
        body: CmdBody,
    ) -> CmdResult<Option<CmdBody>>;
}

pub(crate) struct CmdHandlerMap<LEN, CMD> {
    map: Mutex<HashMap<CMD, Arc<dyn CmdHandler<LEN, CMD>>>>,
}

impl<LEN, CMD> CmdHandlerMap<LEN, CMD>
where
    LEN: RawEncode
        + for<'a> RawDecode<'a>
        + Copy
        + Send
        + Sync
        + 'static
        + FromPrimitive
        + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + Eq + Hash,
{
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

    pub fn from_reader(reader: impl AsyncBufRead + Unpin + Send + 'static, length: u64) -> Self {
        Self {
            reader: Box::new(reader),
            length,
            bytes_read: 0,
        }
    }

    pub fn into_reader(self) -> Box<dyn AsyncBufRead + Unpin + Send + 'static> {
        Box::new(self)
    }

    pub async fn read_all(&mut self) -> CmdResult<Vec<u8>> {
        let mut buf = Vec::with_capacity(1024);
        self.read_to_end(&mut buf)
            .await
            .map_err(into_cmd_err!(CmdErrorCode::Failed, "read to end failed"))?;
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
            .await
            .map_err(into_cmd_err!(CmdErrorCode::Failed, "read to end failed"))?;
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
            .await
            .map_err(into_cmd_err!(CmdErrorCode::Failed, "read to string failed"))?;
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

    pub async fn from_file(file: tokio::fs::File) -> io::Result<Self> {
        let len = file.metadata().await?.len();

        Ok(Self {
            length: len,
            reader: Box::new(tokio::io::BufReader::new(file)),
            bytes_read: 0,
        })
    }

    pub fn len(&self) -> u64 {
        self.length.saturating_sub(self.bytes_read)
    }

    /// Returns `true` if the body has a length of zero, and `false` otherwise.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn chain(self, other: CmdBody) -> CmdResult<Self> {
        let length = self.len().checked_add(other.len()).ok_or_else(|| {
            cmd_err!(
                CmdErrorCode::InvalidParam,
                "chained body length exceeds u64"
            )
        })?;
        validate_pkg_len(length)?;
        Ok(Self {
            length,
            reader: Box::new(tokio::io::AsyncReadExt::chain(self, other)),
            bytes_read: 0,
        })
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
        let remaining = self.len();
        if remaining == 0 {
            return Poll::Ready(Ok(()));
        }
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        let read_len = std::cmp::min(remaining, buf.remaining() as u64) as usize;
        let mut read_buf = ReadBuf::new(buf.initialize_unfilled_to(read_len));
        match Pin::new(&mut self.reader).poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(())) => {
                let read = read_buf.filled().len();
                if read == 0 {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("cmd body ended with {} bytes remaining", remaining),
                    )));
                }
                self.bytes_read += read as u64;
                buf.advance(read);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncBufRead for CmdBody {
    #[allow(rustdoc::missing_doc_code_examples)]
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&'_ [u8]>> {
        let this = self.project();
        let remaining = this.length.saturating_sub(*this.bytes_read);
        if remaining == 0 {
            return Poll::Ready(Ok(&[]));
        }
        match this.reader.poll_fill_buf(cx) {
            Poll::Ready(Ok(buf)) => {
                if buf.is_empty() {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("cmd body ended with {} bytes remaining", remaining),
                    )));
                }
                let len = std::cmp::min(remaining, buf.len() as u64) as usize;
                Poll::Ready(Ok(&buf[..len]))
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        let this = self.project();
        let remaining = this.length.saturating_sub(*this.bytes_read);
        let consumed = std::cmp::min(remaining, amt as u64) as usize;
        this.reader.consume(consumed);
        *this.bytes_read += consumed as u64;
    }
}

pub(crate) async fn copy_cmd_body<W: AsyncWrite + Unpin>(
    body: &mut CmdBody,
    writer: &mut W,
) -> CmdResult<()> {
    let expected = body.len();
    let copied = tokio::io::copy(body, writer)
        .await
        .map_err(into_cmd_err!(CmdErrorCode::IoError))?;
    if copied != expected {
        return Err(cmd_err!(
            CmdErrorCode::IoError,
            "cmd body length mismatch: expected {}, copied {}",
            expected,
            copied
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        CmdBody, CmdBodyRead, CmdBodyReadAll, CmdHeader, MAX_PKG_LEN, decode_pkg_len,
        encode_pkg_len,
    };
    use crate::errors::CmdErrorCode;
    use crate::{CmdTunnel, CmdTunnelRead, CmdTunnelWrite, PeerId};
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{
        AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf, split,
    };

    #[test]
    fn pkg_len_overflow_returns_invalid_param() {
        assert_eq!(encode_pkg_len::<u8>(u8::MAX as u64).unwrap(), u8::MAX);
        assert_eq!(
            encode_pkg_len::<u8>(u8::MAX as u64 + 1).unwrap_err().code(),
            CmdErrorCode::InvalidParam
        );
        assert_eq!(
            encode_pkg_len::<u32>(MAX_PKG_LEN).unwrap(),
            MAX_PKG_LEN as u32
        );
        assert_eq!(
            encode_pkg_len::<u32>(MAX_PKG_LEN + 1).unwrap_err().code(),
            CmdErrorCode::InvalidParam
        );
        assert_eq!(
            decode_pkg_len((MAX_PKG_LEN + 1) as u32).unwrap_err().code(),
            CmdErrorCode::InvalidParam
        );
    }

    struct TestRead {
        read: tokio::io::ReadHalf<DuplexStream>,
    }

    impl AsyncRead for TestRead {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.read).poll_read(cx, buf)
        }
    }

    impl CmdTunnelRead<()> for TestRead {
        fn get_local_peer_id(&self) -> PeerId {
            PeerId::from(vec![9; 32])
        }

        fn get_remote_peer_id(&self) -> PeerId {
            PeerId::from(vec![1; 32])
        }
    }

    struct TestWrite {
        write: tokio::io::WriteHalf<DuplexStream>,
    }

    impl AsyncWrite for TestWrite {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Pin::new(&mut self.write).poll_write(cx, buf)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.write).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.write).poll_shutdown(cx)
        }
    }

    impl CmdTunnelWrite<()> for TestWrite {
        fn get_local_peer_id(&self) -> PeerId {
            PeerId::from(vec![9; 32])
        }

        fn get_remote_peer_id(&self) -> PeerId {
            PeerId::from(vec![2; 32])
        }
    }

    #[tokio::test]
    async fn cmd_body_bytes_round_trip() {
        let body = CmdBody::from_bytes(b"hello-body".to_vec());
        let data = body.into_bytes().await.unwrap();
        assert_eq!(data, b"hello-body");
    }

    #[tokio::test]
    async fn cmd_body_string_round_trip() {
        let body = CmdBody::from_string("hello-string".to_owned());
        let s = body.into_string().await.unwrap();
        assert_eq!(s, "hello-string");
    }

    #[tokio::test]
    async fn cmd_body_chain_respects_consumed_prefix() {
        let mut first = CmdBody::from_bytes(b"abc".to_vec());
        let mut buf = [0u8; 1];
        first.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"a");

        let chained = first.chain(CmdBody::from_bytes(b"XYZ".to_vec())).unwrap();
        let s = chained.into_string().await.unwrap();
        assert_eq!(s, "bcXYZ");
    }

    #[test]
    fn cmd_body_chain_rejects_package_larger_than_limit() {
        let first = CmdBody::from_reader(io::Cursor::new(Vec::<u8>::new()), MAX_PKG_LEN);
        let second = CmdBody::from_reader(io::Cursor::new(Vec::<u8>::new()), 1);

        assert_eq!(
            first.chain(second).unwrap_err().code(),
            CmdErrorCode::InvalidParam
        );
    }

    #[tokio::test]
    async fn cmd_body_limits_reader_to_declared_length() {
        let body = CmdBody::from_reader(io::Cursor::new(b"abcdef".to_vec()), 3);
        assert_eq!(body.into_bytes().await.unwrap(), b"abc");
    }

    #[tokio::test]
    async fn cmd_body_rejects_reader_shorter_than_declared_length() {
        let body = CmdBody::from_reader(io::Cursor::new(b"ab".to_vec()), 3);
        assert_eq!(
            body.into_bytes().await.unwrap_err().code(),
            CmdErrorCode::Failed
        );
    }

    #[tokio::test]
    async fn cmd_body_len_tracks_remaining_output() {
        let mut body = CmdBody::from_bytes(b"abcd".to_vec());
        let mut prefix = [0u8; 1];
        body.read_exact(&mut prefix).await.unwrap();

        assert_eq!(&prefix, b"a");
        assert_eq!(body.len(), 3);
        assert_eq!(body.into_bytes().await.unwrap(), b"bcd");
    }

    #[test]
    fn cmd_body_empty_and_len() {
        let empty = CmdBody::empty();
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);

        let body = CmdBody::from_bytes(vec![1, 2, 3, 4]);
        assert!(!body.is_empty());
        assert_eq!(body.len(), 4);
    }

    #[tokio::test]
    async fn cmd_body_into_reader_and_read_all() {
        let mut body = CmdBody::from_string("reader-body".to_owned());
        let all = body.read_all().await.unwrap();
        assert_eq!(all, b"reader-body");

        let body = CmdBody::from_string("reader-body2".to_owned());
        let mut reader = body.into_reader();
        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"reader-body2");
    }

    #[test]
    fn cmd_header_set_pkg_len() {
        let mut header = CmdHeader::<u16, u8>::new(1, false, Some(7), 0x11, 3);
        assert_eq!(header.pkg_len(), 3);
        header.set_pkg_len(9);
        assert_eq!(header.pkg_len(), 9);
    }

    #[tokio::test]
    async fn cmd_body_read_all_success_and_empty_after_read() {
        let (side_a, side_b) = tokio::io::duplex(128);
        let (a_read, a_write) = split(side_a);
        let (_b_read, mut b_write) = split(side_b);
        b_write.write_all(b"abcdef").await.unwrap();
        b_write.flush().await.unwrap();

        let tunnel = CmdTunnel::new(TestRead { read: a_read }, TestWrite { write: a_write });
        let (reader, _writer) = tunnel.split();
        let (mut body_read, _completion) = CmdBodyRead::new(reader, 6);

        let first = body_read.read_all().await.unwrap();
        assert_eq!(first, b"abcdef");
        let second = body_read.read_all().await.unwrap();
        assert!(second.is_empty());
    }

    #[tokio::test]
    async fn cmd_body_read_all_error_when_source_short() {
        let (side_a, side_b) = tokio::io::duplex(128);
        let (a_read, a_write) = split(side_a);
        let (_b_read, mut b_write) = split(side_b);
        b_write.write_all(b"ab").await.unwrap();
        b_write.shutdown().await.unwrap();

        let tunnel = CmdTunnel::new(TestRead { read: a_read }, TestWrite { write: a_write });
        let (reader, _writer) = tunnel.split();
        let (mut body_read, _completion) = CmdBodyRead::new(reader, 5);
        assert!(body_read.read_all().await.is_err());
    }
}
