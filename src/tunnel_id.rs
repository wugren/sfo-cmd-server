use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use bucky_raw_codec::{RawDecode, RawEncode};

#[derive(Clone, Copy, Ord, PartialEq, Eq, Debug, RawEncode, RawDecode)]
pub struct TunnelId(u32);

impl TunnelId {
    pub fn value(&self) -> u32 {
        self.0
    }

    fn now(_now: u64) -> u32 {
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() as u32;
        let since_2021 = Duration::from_secs((50 * 365 + 9) * 24 * 3600).as_secs() as u32;
        // TODO: 用10年？
        (now - since_2021) * 10
    }

    // fn time_bits() -> usize {
    //     20
    // }
}

impl PartialOrd for TunnelId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.0 == 0 || other.0 == 0 {
            self.0.partial_cmp(&other.0)
        } else if (std::cmp::max(self.0, other.0) - std::cmp::min(self.0, other.0)) > (u32::MAX / 2)
        {
            Some(if self.0 > other.0 {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            })
        } else {
            self.0.partial_cmp(&other.0)
        }
    }
}

impl Default for TunnelId {
    fn default() -> Self {
        Self(0)
    }
}

impl From<u32> for TunnelId {
    fn from(v: u32) -> Self {
        Self(v)
    }
}

impl Hash for TunnelId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u32(self.0)
    }
}

pub struct TunnelIdGenerator {
    cur: AtomicU32,
}


impl From<TunnelId> for TunnelIdGenerator {
    fn from(init: TunnelId) -> Self {
        Self {
            cur: AtomicU32::new(init.value())
        }
    }
}


impl TunnelIdGenerator {
    pub fn new() -> Self {
        let now = TunnelId::now(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64);
        Self {
            cur: AtomicU32::new(now),
        }
    }

    pub fn generate(&self) -> TunnelId {
        let v = self.cur.fetch_add(1, Ordering::SeqCst);
        if v == 0 {
            TunnelId(self.cur.fetch_add(1, Ordering::SeqCst))
        } else {
            TunnelId(v)
        }
    }
}
