use std::fmt::{Debug, Display, Formatter};
use base58::{FromBase58, ToBase58};
use crate::errors::{cmd_err, CmdError, CmdErrorCode, CmdResult};

pub trait ToBase36 {
    fn to_base36(&self) -> String;
}

pub trait FromBase36 {
    fn from_base36(&self) -> CmdResult<Vec<u8>>;
}

const ALPHABET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";

impl ToBase36 for [u8] {
    fn to_base36(&self) -> String {
        base_x::encode(ALPHABET, self)
    }
}

impl FromBase36 for str {
    fn from_base36(&self) -> CmdResult<Vec<u8>> {
        base_x::decode(ALPHABET, &self.to_ascii_lowercase()).map_err(|e| {
            let msg = format!("convert string to base36 error! {self}, {e}");
            CmdError::new(CmdErrorCode::Failed, msg)
        })
    }
}

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct PeerId(Vec<u8>);
impl PeerId {
    pub fn to_base58(&self) -> String {
        self.0.as_slice().to_base58()
    }

    pub fn from_base58(base58: &str) -> CmdResult<Self> {
        Ok(Self(base58.from_base58().map_err(|_| cmd_err!(CmdErrorCode::InvalidParam, "invalid peer id {}", base58))?))
    }

    pub fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }

    pub fn to_base36(&self) -> String {
        self.0.to_base36()
    }

    pub fn from_base36(base36: &str) -> CmdResult<Self> {
        Ok(Self(base36.from_base36()?))
    }
}

impl Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_base36())
    }
}

impl Debug for PeerId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_base36())
    }
}

impl From<&[u8]> for PeerId {
    fn from(key: &[u8]) -> Self {
        Self(key.to_vec())
    }
}

impl From<Vec<u8>> for PeerId {
    fn from(key: Vec<u8>) -> Self {
        Self(key)
    }
}
