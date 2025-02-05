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

}

impl From<&[u8]> for PeerId {
    fn from(key: &[u8]) -> Self {
        let mut sha256 = sha2::Sha256::new();
        sha256.update(key);
        Self(sha256.finalize().as_slice().to_vec())
    }
}
