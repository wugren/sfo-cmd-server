use crate::errors::{CmdError, CmdErrorCode, CmdResult, cmd_err};
use base58::{FromBase58, ToBase58};
use bucky_raw_codec::{RawDecode, RawEncode};
use std::fmt::{Debug, Display, Formatter};

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

#[derive(Clone, Eq, PartialEq, Hash, RawDecode, RawEncode)]
pub struct PeerId(Vec<u8>);
impl PeerId {
    pub fn to_base58(&self) -> String {
        self.0.as_slice().to_base58()
    }

    pub fn from_base58(base58: &str) -> CmdResult<Self> {
        Ok(Self(base58.from_base58().map_err(|_| {
            cmd_err!(CmdErrorCode::InvalidParam, "invalid peer id {}", base58)
        })?))
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

#[cfg(test)]
mod tests {
    use super::PeerId;

    #[test]
    fn base58_round_trip() {
        let raw = vec![1u8, 2, 3, 4, 5, 250, 251, 252];
        let id = PeerId::from(raw.clone());
        let decoded = PeerId::from_base58(&id.to_base58()).unwrap();
        assert_eq!(decoded.as_slice(), raw.as_slice());
    }

    #[test]
    fn base36_round_trip() {
        let raw = vec![9u8, 8, 7, 6, 5, 4, 3, 2];
        let id = PeerId::from(raw.clone());
        let decoded = PeerId::from_base36(&id.to_base36()).unwrap();
        assert_eq!(decoded.as_slice(), raw.as_slice());
    }

    #[test]
    fn invalid_base58_rejected() {
        assert!(PeerId::from_base58("***invalid***").is_err());
    }

    #[test]
    fn invalid_base36_rejected() {
        assert!(PeerId::from_base36("not@base36").is_err());
    }
}
