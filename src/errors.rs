pub use sfo_result::err as cmd_err;
pub use sfo_result::into_err as into_cmd_err;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Default)]
pub enum CmdErrorCode {
    #[default]
    Failed,
    InvalidParam,
    IoError,
    TlsError,
    RawCodecError,
    PeerConnectionNotFound,
}

pub type CmdResult<T> = sfo_result::Result<T, CmdErrorCode>;
pub type CmdError = sfo_result::Error<CmdErrorCode>;
