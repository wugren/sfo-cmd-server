use base58::{FromBase58, ToBase58};
use sha2::Digest;
use crate::errors::{cmd_err, CmdErrorCode, CmdResult};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct PeerId(Vec<u8>);
impl PeerId {
    pub fn to_base58(&self) -> String {
        self.0.as_slice().to_base58()
    }

    pub fn from_base58(base58: &str) -> CmdResult<Self> {
        Ok(Self(base58.from_base58().map_err(|e| cmd_err!(CmdErrorCode::InvalidParam, "invalid peer id {}", base58))?))
    }

    pub fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
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
