use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use bucky_raw_codec::{RawDecode, RawEncode, RawFixedBytes};
use num::{FromPrimitive, ToPrimitive};
use crate::errors::CmdResult;
use crate::peer_id::PeerId;

#[derive(RawEncode, RawDecode)]
pub struct CmdHeader<LEN, CMD> {
    pkg_len: LEN,
    cmd_code: CMD,
}

impl<LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
    CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static> CmdHeader<LEN, CMD> {
    pub fn new(cmd_code: CMD, pkg_len: LEN) -> Self {
        Self {
            pkg_len,
            cmd_code,
        }
    }

    pub fn pkg_len(&self) -> LEN {
        self.pkg_len
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
        Some(LEN::raw_bytes().unwrap() + u8::raw_bytes().unwrap())
    }
}

#[callback_trait::callback_trait]
pub trait CmdHandler<LEN, CMD>: Send + Sync + 'static
where LEN: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static + FromPrimitive + ToPrimitive,
      CMD: RawEncode + for<'a> RawDecode<'a> + Copy + Send + Sync + 'static {
    async fn handle(&self, peer_id: PeerId, header: CmdHeader<LEN, CMD>, buf: Vec<u8>) -> CmdResult<()>;
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
